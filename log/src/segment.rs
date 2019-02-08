use if_chain::if_chain;
use log::{debug, warn};
use matches::matches;
use std::borrow::Borrow;
use std::cmp;
use std::io::prelude::*;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::entry::{BufEntry, Update, ValidBody};
use crate::entry::format;
use crate::error::*;
use crate::file::*;
use crate::index::{self as index, Index};
use crate::message::{Id, Timestamp};
use rcommon::bytes::*;

pub const HARD_MAX_SEGMENT_LEN: u32 = u32::max_value() as u32 - 1024;

// This must be high enough so the mmap len of index is sane.
const MIN_INDEX_EACH_BYTES: u32 = 1024;
const MAX_INDEX_EACH_BYTES: u32 = HARD_MAX_SEGMENT_LEN;

pub type IdIndex = Index<u32, u32>;
pub type TimestampIndex = Index<Timestamp, u32, index::DupAllowed>;

#[derive(Clone, Copy, Debug, Eq, Fail, PartialEq)]
pub enum ErrorId {
    #[fail(display = "invalid segment path")]
    BadPath,

    #[fail(display = "error opening segment data file")]
    Open,

    #[fail(display = "error creating new segment data file")]
    CreateNew,

    #[fail(display = "segment unexpectedly truncated during iteration")]
    SegmentTruncated,

    #[fail(display = "fsync'ing of segment file failed")]
    Fsync,

    #[fail(display = "timestamp index is empty for existing segment")]
    TimestampIndexEmpty,

    #[fail(display = "IO error")]
    Io,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileKind {
    Segment,
    IdIndex,
    TimestampIndex,
}

impl FileKind {
    const SEGMENT: &'static str = ".segment";
    const ID_INDEX: &'static str = ".i.id";
    const TIMESTAMP_INDEX: &'static str = ".i.timestamp";

    pub(in crate) fn from_path(p: impl AsRef<Path>) -> Option<Self> {
        let p = p.as_ref().file_name()?.to_str()?;
        Some(match () {
            _ if p.ends_with(Self::SEGMENT) => FileKind::Segment,
            _ if p.ends_with(Self::ID_INDEX) => FileKind::IdIndex,
            _ if p.ends_with(Self::TIMESTAMP_INDEX) => FileKind::TimestampIndex,
            _ => return None,
        })
    }

    fn suffix(&self) -> &'static str {
        match self {
            FileKind::Segment => Self::SEGMENT,
            FileKind::IdIndex => Self::ID_INDEX,
            FileKind::TimestampIndex => Self::TIMESTAMP_INDEX,
        }
    }
}

pub fn make_file_path(dir: impl AsRef<Path>, start_id: Id, kind: FileKind) -> PathBuf {
    dir.as_ref().to_owned().join(format!("{:020}{}", start_id, kind.suffix()))
}

struct SegFile {
    path: PathBuf,
    file: File,
}

impl Borrow<File> for Arc<SegFile> {
    fn borrow(&self) -> &File {
        &self.file
    }
}

pub struct Options {
    pub read_only: bool,
    pub index_preallocate: u32,
    pub index_each_bytes: u32,
    pub fsync_each_bytes: Option<u32>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            read_only: false,
            index_preallocate: 1_000_000,
            index_each_bytes: 4096,
            fsync_each_bytes: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Push {
    pub dense: bool,
    pub timestamp: Option<Timestamp>,
}

impl Default for Push {
    fn default() -> Self {
        Self {
            dense: true,
            timestamp: None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Open,
    CreateNew { max_timestamp: Timestamp },
}

pub struct Segment {
    file: Arc<SegFile>,
    start_id: Id,
    next_id: Id,
    last_pos: Option<u32>,
    max_timestamp: Timestamp,
    id_index:  IdIndex,
    timestamp_index: TimestampIndex,
    index_each_bytes: u32,
    bytes_since_last_index_push: u32,
    fsync_each_bytes: Option<u32>,
    bytes_sync_last_fsync: u32,
}

impl Segment {
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        let path = path.as_ref();

        let dir = path.parent()
            .ok_or_else(|| Error::new(ErrorId::BadPath, format!(
                "couldn't get dir of {:?}", path)))?;

        let file_name = path.file_name()
            .ok_or_else(|| Error::new(ErrorId::BadPath, format!(
                "couldn't get segment filename from {:?}", path)))?
            .to_str()
            .ok_or_else(|| Error::new(ErrorId::BadPath, format!(
                "segment file name {:?} contains non-unicode character",
                path.file_name().unwrap())))?;
        let suffix = FileKind::Segment.suffix();
        if !file_name.ends_with(suffix) {
            return Err(Error::new(ErrorId::BadPath, format!(
                "segment data file name \"{}\" doesn't end with {}",
                file_name, suffix)));
        }
        if file_name.len() != 20 + suffix.len() {
            return Err(Error::new(ErrorId::BadPath, format!(
                "segment data file name \"{}\" doesn't specify start id",
                file_name)));
        }

        let start_id = (&file_name[..20]).parse()
            .ok()
            .map(Id::new)
            .ok_or_else(|| Error::new(ErrorId::BadPath, format!(
                "couldn't parse start id from filename \"{}\"", file_name)))?;

        Self::new(dir, start_id, Mode::Open, options)
    }

    pub fn create_new(path: impl AsRef<Path>, start_id: Id, max_timestamp: Timestamp,
            options: Options) -> Result<Self> {
        Self::new(path, start_id, Mode::CreateNew { max_timestamp }, options)
    }

    fn new(dir: impl AsRef<Path>, start_id: Id, mode: Mode, options: Options) -> Result<Self> {
        assert!(options.index_each_bytes >= MIN_INDEX_EACH_BYTES);
        assert!(options.index_each_bytes <= MAX_INDEX_EACH_BYTES);
        assert!(options.index_preallocate >= 1);

        let entries_path = make_file_path(dir.as_ref(), start_id, FileKind::Segment);

        let create_new = matches!(mode, Mode::CreateNew { .. });
        let file = OpenOptions::new()
            .create_new(create_new)
            .open(&entries_path)
            .wrap_err_with(|_| (if create_new {
                ErrorId::CreateNew
            } else {
                ErrorId::Open
            }, format!("{:?}", entries_path)))?;
        let file = Arc::new(SegFile {
            path: entries_path,
            file,
        });

        // FIXME repair data file: truncate to the entry boundary (either using index or during
        // index rebuild if it's damaged)

        // TODO rebuild indexes if corrupted.

        let index_mode = if options.read_only {
            index::Mode::ReadOnly
        } else {
            let index_max_capacity = cast::usize(HARD_MAX_SEGMENT_LEN / options.index_each_bytes);
            let index_preallocate = cmp::min(cast::usize(options.index_preallocate),
                index_max_capacity);
            index::Mode::ReadWrite {
                preallocate: index_preallocate,
                max_capacity: index_max_capacity,
            }
        };

        let id_index_path = make_file_path(dir.as_ref(), start_id, FileKind::IdIndex);
        let id_index = if create_new {
            Index::create_new(&id_index_path, index_mode)
        } else {
            Index::open(&id_index_path, index_mode)
        }.context_with(|_| format!("opening id index file {:?}", id_index_path))?;

        let timestamp_index_path = make_file_path(dir.as_ref(), start_id, FileKind::TimestampIndex);
        let timestamp_index = if create_new {
            Index::create_new(&timestamp_index_path, index_mode)
        } else {
            match Index::open(&timestamp_index_path, index_mode) {
                Ok(idx) => {
                    if idx.is_empty() {
                        Err(Error::without_details(ErrorId::TimestampIndexEmpty))
                    } else {
                        Ok(idx)
                    }
                }
                Err(e) => Err(e),
            }
        }.context_with(|_| format!("opening timestamp index file {:?}", timestamp_index_path))?;

        let (next_id, last_pos) = {
            let id = Self::local_to_global_id0(start_id, id_index.last_key().unwrap_or(0));
            let mut it = Self::iter0(&file, start_id, Id::max_value(), &id_index, id..);
            let mut last_id = None;
            for entry in &mut it {
                let entry = entry?;
                last_id = Some(entry.end_id());
            }
            let last_pos = it.last_pos();

            let file_len = file.file.len();
            let valid_file_len = cast::u64(it.next_pos());
            if file_len > valid_file_len {
                warn!("detected trailing garbage in segment {:?} length {}, truncating to {}",
                    file.path, file_len, it.next_pos());
                file.file.truncate(valid_file_len)
                    .wrap_err_id(ErrorId::Io)
                    .context_with(|_| format!("while truncating file {:?}", file.path))?;
            }

            (last_id.map(|v| v + 1).unwrap_or(start_id), last_pos)
        };

        let max_timestamp = if let Mode::CreateNew { max_timestamp } = mode {
            timestamp_index.push(max_timestamp, 0)
                .context("pushing checkpoint entry into timestamp index")?;
            max_timestamp
        } else {
            let (mut max_timestamp, lid) = timestamp_index.last_entry()
                .unwrap_or((Timestamp::min_value(), 0));
            let id = Self::local_to_global_id0(start_id, lid);
            let it = Self::iter0(&file, start_id, Id::max_value(), &id_index, id..);
            for entry in it {
                let entry = entry?;
                if entry.max_timestamp() > max_timestamp {
                    max_timestamp = entry.max_timestamp();
                }
            }
            max_timestamp
        };

        Ok(Self {
            file,
            start_id,
            next_id,
            last_pos,
            max_timestamp,
            id_index,
            timestamp_index,
            index_each_bytes: options.index_each_bytes,
            bytes_since_last_index_push: 0,
            fsync_each_bytes: options.fsync_each_bytes,
            bytes_sync_last_fsync: 0,
        })
    }

    pub fn path(&self) -> &PathBuf {
        &self.file.path
    }

    pub fn len_bytes(&self) -> u32 {
        cast::u32(self.file.file.len()).unwrap()
    }

    pub fn start_id(&self) -> Id {
        self.start_id
    }

    pub fn next_id(&self) -> Id {
        self.next_id
    }

    pub fn last_pos(&self) -> Option<u32> {
        self.last_pos
    }

    pub fn is_empty(&self) -> bool {
        self.next_id == self.start_id
    }

    pub fn max_timestamp(&self) -> Timestamp {
        self.max_timestamp
    }

    pub fn push(&mut self, entry: &mut BufEntry, buf: &mut impl BufMut, options: Push) -> Result<()> {
        assert!(self.file.file.len() + buf.len() as u64 <= HARD_MAX_SEGMENT_LEN as u64);

        entry.validate_body(buf, ValidBody {
            dense: options.dense,
            without_timestamp: options.timestamp.is_some(),
            ..Default::default()
        }).context("validating body")?;

        entry.update(buf, Update {
            start_id: if options.dense { Some(self.next_id) } else { None },
            first_timestamp: options.timestamp,
        });

        assert!(entry.start_id() >= self.start_id);

        self.next_id = entry.end_id() + 1;

        let pos = self.len_bytes();
        self.last_pos = Some(pos);

        self.file.file.writer().write_all(buf.as_slice()).wrap_err_id(ErrorId::Io)?;

        if entry.max_timestamp() > self.max_timestamp {
            self.max_timestamp = entry.max_timestamp();
        }

        self.bytes_since_last_index_push = self.bytes_since_last_index_push
            .saturating_add(cast::u32(buf.len()).unwrap());

        if self.bytes_since_last_index_push >= self.index_each_bytes {
            let local_start_id = self.global_to_local_id(entry.start_id());
            self.id_index.push(local_start_id, pos)
                .context("pushing to id index")?;

            let local_end_id = self.global_to_local_id(entry.end_id());
            self.timestamp_index.push(self.max_timestamp, local_end_id)
                .context("pushing to timestamp index")?;

            self.bytes_since_last_index_push = 0;
        }

        self.bytes_sync_last_fsync = self.bytes_sync_last_fsync
            .saturating_add(cast::u32(buf.len()).unwrap());

        if_chain! {
            if let Some(fsync_each_bytes) = self.fsync_each_bytes;
            if self.bytes_sync_last_fsync >= fsync_each_bytes;
            then {
                self.fsync()?;
            }
        }

        Ok(())
    }

    pub fn fsync(&mut self) -> Result<()> {
        debug!("fsyncing: bytes_sync_last_fsync={} path={:?}",
            self.bytes_sync_last_fsync, self.path());
        self.file.file.sync_all().wrap_err_id(ErrorId::Io)?;
        self.bytes_sync_last_fsync = 0;
        Ok(())
    }

    pub fn make_read_only(&mut self) -> Result<()> {
        debug!("making segment read-only: {:?}", self.path());
        if let Some(last_pos) = self.last_pos {
            let next_lid = self.global_to_local_id(self.next_id);

            if self.id_index.last_value().map(|v| v < last_pos).unwrap_or(true) {
                let last_lid = next_lid - 1;
                debug!("pushing final id checkpoint: ({}, {})", last_lid, last_pos);
                self.id_index.push(last_lid, last_pos)
                    .context("pushing final checkpoint to id index")?;
            }

            if self.timestamp_index.last_value().map(|v| v < next_lid).unwrap_or(true) {
                debug!("pushing final timestamp checkpoint: ({}, {})",
                    self.max_timestamp, next_lid);
                self.timestamp_index.push(self.max_timestamp, next_lid)
                    .context("pushing to timestamp index (final checkpoint)")?;
            }
        }
        self.id_index.make_read_only()
            .context("making id index read-only")?;
        self.timestamp_index.make_read_only()
            .context("making timestamp index read-only")?;
        Ok(())
    }

    pub fn iter(&self, range: impl RangeBounds<Id>) -> Iter {
        Self::iter0(&self.file, self.start_id, self.next_id, &self.id_index, range)
    }

    fn iter0(file: &Arc<SegFile>, seg_start_id: Id, next_id: Id, id_index: &IdIndex,
            range: impl RangeBounds<Id>) -> Iter {
        let start_id = match range.start_bound() {
            Bound::Excluded(_) => unreachable!(),
            Bound::Included(v) => *v,
            Bound::Unbounded => Id::min_value(),
        };
        let end_id_excl = match range.end_bound() {
            Bound::Excluded(v) => {
                assert!(*v >= start_id);
                *v
            },
            Bound::Included(v) => {
                assert!(*v >= start_id);
                v.checked_add(1).unwrap()
            },
            Bound::Unbounded => Id::max_value(),
        };

        let (start_pos, force_eof) = if end_id_excl > start_id &&
                start_id < next_id && end_id_excl > seg_start_id {
            let start_id = cmp::max(start_id, seg_start_id);
            let local_id = Self::global_to_local_id0(seg_start_id, start_id);
            (id_index.value_by_key(local_id)
                 .unwrap_or(0) as u64, false)
        } else {
            (0, true)
        };
        let mut rd: Reader<Arc<SegFile>> = file.clone().into();
        rd.set_position(start_pos);
        Iter::new(rd, start_id, end_id_excl, force_eof)
    }

    fn global_to_local_id(&self, global_id: Id) -> u32 {
        Self::global_to_local_id0(self.start_id, global_id)
    }

    fn global_to_local_id0(segment_start_id: Id, global_id: Id) -> u32 {
        cast::u32(global_id.checked_delta(segment_start_id).unwrap()).unwrap()
    }

    fn local_to_global_id0(segment_start_id: Id, local_id: u32) -> Id {
        segment_start_id.checked_add(cast::u64(local_id)).unwrap()
    }
}

impl cmp::PartialEq<Id> for Segment {
    fn eq(&self, other: &Id) -> bool {
        *other >= self.start_id && *other < self.next_id
    }
}

impl cmp::PartialOrd<Id> for Segment {
    fn partial_cmp(&self, other: &Id) -> Option<cmp::Ordering> {
        let r = self.start_id.cmp(other);
        Some(if r == cmp::Ordering::Less && self.next_id > *other {
            cmp::Ordering::Equal
        } else {
            r
        })
    }
}

pub struct Iter {
    rd: Reader<Arc<SegFile>>,
    start_id: Id,
    end_id_excl: Id,
    // If Some then the last read entry was partial (prolog only).
    // Thus before reading next entry it has to skip the unread part of the frame.
    frame_len: Option<usize>,
    eof: bool,
    buf: Vec<u8>,
    file_grow_check: Option<u64>,
    last_pos: Option<u32>,
}

impl Iter {
    fn new(rd: Reader<Arc<SegFile>>, start_id: Id, end_id_excl: Id,
            force_eof: bool) -> Self {
        assert!(end_id_excl > start_id || end_id_excl == start_id && force_eof);
        Self {
            rd,
            start_id,
            end_id_excl,
            frame_len: None,
            eof: force_eof,
            buf: Vec::new(),
            file_grow_check: None,
            last_pos: None,
        }
    }

    pub fn with_buf(mut self, buf: Vec<u8>) -> Self {
        self.buf = buf;
        self.buf.clear();
        self
    }

    pub fn is_eof(&self) -> bool {
        self.eof
    }

    pub fn buf(&self) -> &Vec<u8> {
        &self.buf
    }

    pub fn buf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }

    pub fn into_buf(self) -> Vec<u8> {
        self.buf
    }

    /// File position where the last entry was read at.
    pub fn last_pos(&self) -> Option<u32> {
        self.last_pos
    }

    /// File position where the next entry will be read at.
    pub fn next_pos(&self) -> u32 {
        cast::u32(self.rd.position()).unwrap() +
            self.frame_len.map(|v| cast::u32(v).unwrap()).unwrap_or(0)
    }

    pub fn complete_read(&mut self) -> Result<()> {
        if let Some(frame_len) = self.frame_len {
            self.buf.set_len_zeroed(frame_len);
            let r = self.rd.read_exact(&mut self.buf[format::FRAME_PROLOG_LEN..frame_len])
                .wrap_err_id(ErrorId::Io);
            if r.is_ok() {
                self.frame_len = None;
            }
            r?;
        } else {
            panic!("wrong state");
        }
        Ok(())
    }
}

impl Iterator for Iter {
    type Item = Result<BufEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.buf.clear();

        if self.eof {
            return None;
        }

        let file_len = self.rd.file().len();
        if let Some(last_file_len) = self.file_grow_check {
            if file_len < last_file_len {
                return Some(Err(Error::new(ErrorId::SegmentTruncated, format!(
                    "segment {:?} unexpectedly truncated while being iterated",
                    self.rd.inner().path))));
            }
            if file_len == last_file_len {
                return None;
            }
            self.file_grow_check = None;
        }

        loop {
            if let Some(frame_len) = self.frame_len {
                self.rd.advance((frame_len - format::FRAME_PROLOG_LEN) as u64);
                self.frame_len = None;
            }
            let last_position = cast::u32(self.rd.position()).unwrap();
            break match BufEntry::read_prolog(&mut self.rd, &mut self.buf) {
                Ok(Some(entry)) => {
                    self.frame_len = Some(entry.frame_len());

                    let entry_start_id = entry.start_id();
                    if entry_start_id >= self.end_id_excl {
                        self.eof = true;
                        break None;
                    }
                    let entry_end_id = entry.end_id();
                    if entry_end_id < self.start_id {
                        continue;
                    }

                    self.eof = entry_end_id >= self.end_id_excl;
                    self.last_pos = Some(last_position);
                    Some(Ok(entry))
                }
                Ok(None) => {
                    self.file_grow_check = Some(file_len);
                    None
                }
                Err(e) => {
                    Some(Err(e))
                }
            };
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::ErrorId;
    use std::fs;
    use crate::entry::{BufEntryBuilder, ValidBody};
    use crate::message::MessageBuilder;
    
    #[test]
    fn push_and_iter() {
        let dir = mktemp::Temp::new_dir().unwrap();
        let seg_path;
        {
            let mut seg = Segment::create_new(&dir, Id::new(100), Timestamp::min_value(),
                Default::default()).unwrap();

            seg_path = seg.path().clone();

            assert_eq!(seg.timestamp_index.last_entry().unwrap(), (Timestamp::min_value(), 0));
            assert!(seg.iter(..).next().is_none());

            // first entry

            let timestamp1 = Timestamp::now() - 10000;

            let (mut entry, mut buf) = BufEntryBuilder::from(vec![
                MessageBuilder {
                    timestamp: Some(timestamp1),
                    value: Some("msg1".into()),
                    ..Default::default()
                },
                MessageBuilder {
                    timestamp: Some(timestamp1 + 1),
                    value: Some("msg2".into()),
                    ..Default::default()
                },
            ]).build();
            entry.validate_body(&buf, Default::default()).unwrap();
            seg.push(&mut entry, &mut buf, Default::default()).unwrap();

            let mut it = seg.iter(..);

            let act_entry = it.next().unwrap().unwrap();
            it.complete_read().unwrap();
            act_entry.validate_body(it.buf(), Default::default()).unwrap();

            assert_eq!(act_entry.start_id(), Id::new(100));
            assert_eq!(act_entry.end_id(), Id::new(101));
            assert_eq!(act_entry.first_timestamp(), timestamp1);
            assert_eq!(act_entry.max_timestamp(), timestamp1 + 1);

            let act_msgs: Vec<_> = act_entry.iter(it.buf())
                .map(|m| m.unwrap())
                .collect();
            assert_eq!(act_msgs, vec![
                MessageBuilder {
                    id: Some(Id::new(100)),
                    timestamp: Some(timestamp1),
                    value: Some("msg1".into()),
                    ..Default::default()
                }.build(),
                MessageBuilder {
                    id: Some(Id::new(101)),
                    timestamp: Some(timestamp1 + 1),
                    value: Some("msg2".into()),
                    ..Default::default()
                }.build(),
            ]);

            assert!(it.next().is_none());

            // second entry

            let (mut entry, mut buf) = BufEntryBuilder::from(vec![
                MessageBuilder {
                    value: Some("msg3".into()),
                    ..Default::default()
                },
            ]).build();
            entry.validate_body(&buf, ValidBody { without_timestamp: true,
                ..Default::default() }).unwrap();
            seg.push(&mut entry, &mut buf, Push { timestamp: Some(Timestamp::now()),
                ..Default::default() }).unwrap();
            let timestamp = entry.first_timestamp();
            assert_eq!(entry.start_id(), Id::new(102));
            assert_eq!(entry.end_id(), Id::new(102));
            assert_eq!(entry.first_timestamp(), timestamp);
            assert_eq!(entry.max_timestamp(), timestamp);

            assert_eq!(seg.max_timestamp(), timestamp);
        }
        // Reopen the segment and check the content.
        {
            let seg = Segment::open(&seg_path, Default::default()).unwrap();

            let mut it = seg.iter(..);

            let e = it.next().unwrap().unwrap();
            it.complete_read().unwrap();
            assert_eq!(e.start_id(), Id::new(100));
            assert_eq!(e.iter(it.buf()).count(), 2);

            let e = it.next().unwrap().unwrap();
            it.complete_read().unwrap();
            assert_eq!(e.start_id(), Id::new(102));
            assert_eq!(e.iter(it.buf()).count(), 1);
        }
    }

    #[test]
    fn index_each_bytes() {
        let dir = mktemp::Temp::new_dir().unwrap();
        let mut seg = Segment::create_new(&dir, Id::new(10), Timestamp::min_value(), Options {
            index_each_bytes: MIN_INDEX_EACH_BYTES,
            .. Default::default()
        }).unwrap();

        let (mut entry, mut buf) = BufEntryBuilder::sparse(
            Id::new(10), Id::new(11)).build();
        seg.push(&mut entry, &mut buf, Push { dense: false, ..Default::default() }).unwrap();
        assert_eq!(seg.id_index.entry_by_key(10), None);

        let mut b = BufEntryBuilder::dense();
        while b.get_frame_len() < cast::usize(MIN_INDEX_EACH_BYTES) {
            b.message(Default::default());
        }
        let (mut entry, mut buf) = b.build();
        let pos = seg.len_bytes();
        seg.push(&mut entry, &mut buf, Default::default()).unwrap();
        assert_eq!(seg.id_index.entry_by_key(12), Some((2, pos)));
    }

    #[test]
    fn fails_if_timestamp_index_empty() {
        let dir = mktemp::Temp::new_dir().unwrap();
        let (path, start_id) = {
            let seg = Segment::create_new(&dir, Id::min_value(), Timestamp::min_value(),
                Default::default()).unwrap();
            (seg.path().clone(), seg.start_id())
        };
        let idx_path = make_file_path(dir.as_ref(), start_id, FileKind::TimestampIndex);
        fs::OpenOptions::new().write(true).create(true).truncate(true).open(idx_path).unwrap();
        assert_eq!(Segment::open(path, Default::default()).err().unwrap().id(),
            &ErrorId::TimestampIndexEmpty.into());
    }

    #[test]
    fn truncate_trailing_garbage() {
        let dir = mktemp::Temp::new_dir().unwrap();
        let (path, expected_len) = {
            let mut seg = Segment::create_new(&dir, Id::min_value(), Timestamp::min_value(),
                Default::default()).unwrap();
            let (mut entry, mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();
            seg.push(&mut entry, &mut buf, Default::default()).unwrap();
            (seg.path().clone(), seg.len_bytes())
        };

        fs::OpenOptions::new().append(true).open(&path).unwrap().write_all(&[42]).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), expected_len as u64 + 1);

        let mut seg = Segment::open(&path, Default::default()).unwrap();
        seg.fsync().unwrap();
        assert_eq!(seg.len_bytes(), expected_len);
        assert_eq!(fs::metadata(&path).unwrap().len(), expected_len as u64);
    }

    #[test]
    fn make_read_only() {
        let dir = mktemp::Temp::new_dir().unwrap();

        let mut seg = Segment::create_new(&dir, Id::min_value(), Timestamp::min_value(),
            Default::default()).unwrap();
        let (mut entry, mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();
        assert_eq!(seg.last_pos(), None);

        seg.push(&mut entry, &mut buf, Default::default()).unwrap();
        assert_eq!(seg.last_pos(), Some(0));
        let expected_last_pos = seg.len_bytes();

        let (mut entry, mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();
        seg.push(&mut entry, &mut buf, Default::default()).unwrap();
        assert!(seg.len_bytes() > expected_last_pos);
        assert_eq!(seg.last_pos(), Some(expected_last_pos));
        assert!(seg.id_index.is_empty());
        assert_eq!(seg.timestamp_index.len(), 1);

        seg.make_read_only().unwrap();
        seg.make_read_only().unwrap();

        let idx = &seg.id_index;
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.capacity(), 1);
        assert_eq!(idx.max_capacity(), 1);
        assert_eq!(fs::metadata(idx.path()).unwrap().len(), IdIndex::ENTRY_LEN as u64);
        assert_eq!(idx.entry_by_key(1), Some((1, expected_last_pos)));

        let idx = &seg.timestamp_index;
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.capacity(), 2);
        assert_eq!(idx.max_capacity(), 2);
        assert_eq!(fs::metadata(idx.path()).unwrap().len(), TimestampIndex::ENTRY_LEN as u64 * 2);
        assert_eq!(idx.entry_by_key(seg.max_timestamp()), Some((seg.max_timestamp(), 2)));
    }

    #[test]
    fn takes_next_id_and_max_timestamp_from_index() {
        let dir = mktemp::Temp::new_dir().unwrap();
        let timestamp = Timestamp::now();
        let path;
        {
            let mut seg = Segment::create_new(&dir, Id::min_value(), Timestamp::min_value(),
                Default::default()).unwrap();
            path = seg.path().clone();
            let (ref mut e, ref mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();
            seg.push(e, buf, Push { timestamp: Some(timestamp), ..Default::default() }).unwrap();

            seg.make_read_only().unwrap();

            assert_eq!(seg.next_id(), Id::new(1));
            assert_eq!(seg.max_timestamp(), timestamp);
            assert_eq!(seg.id_index.last_entry(), Some((0, 0)));
            assert_eq!(seg.timestamp_index.last_entry(), Some((timestamp, 1)));
        }
        let seg = Segment::open(path, Options { read_only: true, ..Default::default() }).unwrap();
        assert_eq!(seg.next_id(), Id::new(1));
        assert_eq!(seg.max_timestamp(), timestamp);
    }
}