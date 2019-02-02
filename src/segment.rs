use if_chain::if_chain;
use std::borrow::Cow;
use std::cmp;
use std::io::prelude::*;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::bytes::*;
use crate::entry::BufEntry;
use crate::entry::format;
use crate::entry::message::{Id, Timestamp};
use crate::error::*;
use crate::file::*;
use crate::index::{self as index, Index};
use std::borrow::Borrow;

pub const DATA_FILE_SUFFIX: &'static str = ".data";
pub const ID_INDEX_FILE_SUFFIX: &'static str = ".idx.id";
pub const TIMESTAMP_INDEX_FILE_SUFFIX: &'static str = ".idx.ts";

pub const HARD_MAX_SEGMENT_LEN: u32 = u32::max_value() as u32 - 1024;

// This must be high enough so the mmap len of index is sane.
const MIN_INDEX_EACH_BYTES: u32 = 1024;
const MAX_INDEX_EACH_BYTES: u32 = HARD_MAX_SEGMENT_LEN;

pub type IdIndex = Index<u32, u32>;
pub type TimestampIndex = Index<Timestamp, u32, index::DupIgnored>;

#[derive(Clone, Debug, Eq, Fail, PartialEq)]
pub enum Error {
    #[fail(display = "{}", _0)]
    BadPath(Cow<'static, str>),

    #[fail(display = "error opening segment data file {:?}", _0)]
    Open(PathBuf),

    #[fail(display = "error creating segment data file {:?}", _0)]
    Create(PathBuf),

    #[fail(display = "{}", _0)]
    SegmentTruncated(Cow<'static, str>),

    #[fail(display = "fsync of segment file failed")]
    Fsync,

    #[fail(display = "IO error")]
    Io,
}

impl Error {
    pub fn into_error(self) -> crate::error::Error {
        self.into()
    }
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

pub struct Segment {
    file: Arc<SegFile>,
    base_id: Id,
    next_id: Id,
    min_timestamp: Timestamp,
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
            .ok_or_else(|| Error::BadPath(format!(
                "couldn't get dir of {:?}", path).into()).into_error())?;

        let file_name = path.file_name()
            .ok_or_else(|| Error::BadPath(format!(
                "couldn't get segment filename from path {:?}",
                path).into()).into_error())?
            .to_str()
            .ok_or_else(|| Error::BadPath(format!(
                "segment file name {:?} contains non-unicode character",
                path.file_name().unwrap())
                .into()).into_error())?;
        if !file_name.ends_with(DATA_FILE_SUFFIX) {
            return Err(Error::BadPath(format!(
                "segment data file name \"{}\" doesn't end with {}",
                file_name, DATA_FILE_SUFFIX)
                .into()).into());
        }
        if file_name.len() != 20 + DATA_FILE_SUFFIX.len() {
            return Err(Error::BadPath(format!(
                "segment data file name \"{}\" doesn't specify base id",
                file_name).into()).into());
        }

        let base_id = (&file_name[..20]).parse()
            .ok()
            .and_then(Id::new)
            .ok_or_else(|| Error::BadPath(
                format!("couldn't parse base id from filename \"{}\"", file_name)
                    .into()).into_error())?;

        Self::new(dir, base_id, false, options)
    }

    pub fn create(path: impl AsRef<Path>, base_id: Id, options: Options) -> Result<Self> {
        Self::new(path, base_id, true, options)
    }

    fn new(path: impl AsRef<Path>, base_id: Id, create_and_overwrite: bool,
            options: Options) -> Result<Self> {
        assert!(options.index_each_bytes >= MIN_INDEX_EACH_BYTES);
        assert!(options.index_each_bytes <= MAX_INDEX_EACH_BYTES);
        assert!(options.index_preallocate >= 1);

        let base_name = format!("{:020}", base_id);
        let base_path = PathBuf::from(path.as_ref());

        let path = base_path.join(format!("{}{}", base_name, DATA_FILE_SUFFIX));
        let file = OpenOptions::new()
            .create(create_and_overwrite)
            .truncate(create_and_overwrite)
            .open(&path)
            .with_context(|_| if create_and_overwrite {
                Error::Create(path.clone())
            } else {
                Error::Open(path.clone())
            })?;

        // FIXME repair data file: truncate to the entry boundary (either using index or during
        // index rebuild if it's damaged)

        // TODO rebuild indexes if corrupted.

        let index_mode = if options.read_only {
            index::Mode::Static
        } else {
            let index_max_capacity = cast::usize(HARD_MAX_SEGMENT_LEN / options.index_each_bytes);
            let index_preallocate = cmp::min(cast::usize(options.index_preallocate),
                index_max_capacity);
            index::Mode::Growable {
                preallocate: index_preallocate,
                max_capacity: index_max_capacity,
            }
        };

        let id_index_path = base_path.join(format!("{}{}", base_name, ID_INDEX_FILE_SUFFIX));
        let id_index = Index::open_or_create(&id_index_path, index_mode)
            .with_more_context(|_| format!("opening id index file {:?}", id_index_path))?;

        let timestamp_index_path = base_path.join(format!("{}{}", base_name, TIMESTAMP_INDEX_FILE_SUFFIX));
        let timestamp_index = Index::open_or_create(&timestamp_index_path, index_mode)
            .with_more_context(|_| format!("opening timestamp index file {:?}", timestamp_index_path))?;

        let (next_id, min_timestamp) = if let Some(pos) = id_index.last_value() {
            let ref mut rd = file.reader();
            rd.set_position(pos as u64);
            let ref mut buf = BytesMut::new();
            let mut next_id = None;
            let mut last_timestamp = None;
            while let Some(e) = BufEntry::read_prolog(rd, buf)? {
                next_id = Some(e.end_id().checked_add(1).unwrap());
                last_timestamp = Some(e.last_timestamp());
            }
            (next_id.unwrap(), last_timestamp.unwrap())
        } else {
            (base_id, Timestamp::epoch())
        };

        Ok(Self {
            file: Arc::new(SegFile {
                path,
                file,
            }),
            base_id,
            next_id,
            min_timestamp,
            id_index,
            timestamp_index,
            index_each_bytes: options.index_each_bytes,
            bytes_since_last_index_push: 0,
            fsync_each_bytes: options.fsync_each_bytes,
            bytes_sync_last_fsync: 0,
        })
    }

    pub fn len(&self) -> u32 {
        cast::u32(self.file.file.len()).unwrap()
    }

    pub fn base_id(&self) -> Id {
        self.base_id
    }

    pub fn next_id(&self) -> Id {
        self.next_id
    }

    pub fn push(&mut self, entry: &mut BufEntry, buf: &mut BytesMut) -> Result<()> {
        assert!(self.file.file.len() + buf.len() as u64 <= HARD_MAX_SEGMENT_LEN as u64);

        entry.set_start_id(buf, self.next_id);
        let timestamp = cmp::max(Timestamp::now(), self.min_timestamp);
        entry.set_timestamp(buf, timestamp);
        self.next_id = self.next_id.checked_add(1).unwrap();
        self.min_timestamp = timestamp;

        let pos = self.len();

        self.file.file.writer().write_all(&buf[..]).context(Error::Io)?;

        self.bytes_since_last_index_push = self.bytes_since_last_index_push
            .saturating_add(cast::u32(buf.len()).unwrap());

        if self.bytes_since_last_index_push >= self.index_each_bytes {
            let id_delta = cast::u32(entry.start_id().checked_delta(self.base_id)
                .unwrap()).unwrap();
            self.id_index.push(id_delta, pos)
                .more_context("pushing to id index")?;
            self.timestamp_index.push(entry.first_timestamp(), pos)
                .more_context("pushing to timestamp index")?;
            self.bytes_since_last_index_push = 0;
        }

        self.bytes_sync_last_fsync = self.bytes_sync_last_fsync
            .saturating_add(cast::u32(buf.len()).unwrap());

        if_chain! {
            if let Some(fsync_each_bytes) = self.fsync_each_bytes;
            if self.bytes_sync_last_fsync >= fsync_each_bytes;
            then {
                self.force_fsync()?;
            }
        }

        Ok(())
    }

    pub fn force_fsync(&mut self) -> Result<()> {
        self.file.file.sync_all().context(Error::Io)?;
        self.bytes_sync_last_fsync = 0;
        Ok(())
    }

    pub fn get(&self, range: impl RangeBounds<Id>) -> Iter {
        let start_id = match range.start_bound() {
            Bound::Excluded(_) => unreachable!(),
            Bound::Included(v) => *v,
            Bound::Unbounded => Id::min_value(),
        };
        let end_id = match range.end_bound() {
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
        let (start_pos, force_eof) = if end_id > start_id &&
                start_id >= self.base_id && start_id < self.next_id {
            let local_id = cast::u32(start_id - self.base_id).unwrap();
            (self.id_index.value_by_key(local_id)
                 .unwrap_or(0) as u64, false)
        } else {
            (0, true)
        };
        let mut rd: Reader<Arc<SegFile>> = self.file.clone().into();
        rd.set_position(start_pos);
        Iter::new(rd, start_id, end_id, force_eof)
    }
}

impl cmp::PartialEq<Id> for Segment {
    fn eq(&self, other: &Id) -> bool {
        *other >= self.base_id && *other < self.next_id
    }
}

impl cmp::PartialOrd<Id> for Segment {
    fn partial_cmp(&self, other: &Id) -> Option<cmp::Ordering> {
        let r = self.base_id.cmp(other);
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
    end_id: Id,
    // If Some then the last read entry was partial (prolog only).
    // Thus before reading next entry it has to skip the unread part of the frame.
    frame_len: Option<usize>,
    eof: bool,
    buf: BytesMut,
    file_grow_check: Option<u64>,
}

impl Iter {
    fn new(rd: Reader<Arc<SegFile>>, start_id: Id, end_id: Id,
            force_eof: bool) -> Self {
        assert!(end_id > start_id || end_id == start_id && force_eof);
        Self {
            rd,
            start_id,
            end_id,
            frame_len: None,
            eof: force_eof,
            buf: BytesMut::new(),
            file_grow_check: None,
        }
    }

    pub fn is_eof(&self) -> bool {
        self.eof
    }

    pub fn buf_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    pub fn complete_read(&mut self) -> Result<()> {
        if let Some(frame_len) = self.frame_len {
            self.buf.set_len(frame_len);
            let r = self.rd.read_exact(&mut self.buf[format::FRAME_PROLOG_LEN..frame_len - format::FRAME_PROLOG_LEN])
                .context(Error::Io);
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
                return Some(Err(Error::SegmentTruncated(format!(
                    "segment {:?} unexpectedly truncated while being iterated",
                    self.rd.inner().path)
                    .into()).into()));
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
            break match BufEntry::read_prolog(&mut self.rd, &mut self.buf) {
                Ok(Some(entry)) => {
                    let start_id = entry.start_id();
                    if start_id >= self.end_id {
                        self.eof = true;
                        break None;
                    }
                    self.frame_len = Some(entry.frame_len());
                    if start_id < self.start_id {
                        continue;
                    }
                    self.eof = start_id == self.end_id - 1;
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
    use crate::entry::BufEntryBuilder;

    #[test]
    fn index_each_bytes() {
        let dir = mktemp::Temp::new_dir().unwrap();
        let mut seg = Segment::create(&dir, Id::new(10).unwrap(), Options {
            index_each_bytes: MIN_INDEX_EACH_BYTES,
            .. Default::default()
        }).unwrap();

        let (mut entry, mut buf) = BufEntryBuilder::sparse(
            Id::new(1).unwrap(), Id::new(1).unwrap()).build();
        seg.push(&mut entry, &mut buf).unwrap();
        assert_eq!(seg.id_index.entry_by_key(10), None);

        let mut b = BufEntryBuilder::dense();
        while b.get_encoded_len() < cast::usize(MIN_INDEX_EACH_BYTES) {
            b.message(Default::default());
        }
        let (mut entry, mut buf) = b.build();
        let pos = seg.len();
        seg.push(&mut entry, &mut buf).unwrap();
        assert_eq!(seg.id_index.entry_by_key(1), Some((1, pos)));
    }
}