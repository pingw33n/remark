use itertools::Itertools;
use log::{debug, info};
use num_traits::cast::ToPrimitive;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::fs;
use std::mem;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::result::{Result as StdResult};
use std::sync::Arc;
use std::time::Duration;

use crate::bytes::*;
use crate::entry::BufEntry;
use crate::entry::format::MIN_FRAME_LEN;
use crate::error::*;
use crate::message::{Id, Timestamp};
use crate::segment::{self, Iter as SegIter, Segment};
use crate::util::file_mutex::FileMutex;

#[derive(Clone, Copy, Debug, Eq, Fail, PartialEq)]
pub enum ErrorId {
    #[fail(display = "unknown files/directories found under a log directory")]
    UnknownDirEntries,

    #[fail(display = "can't push entry because it's too big")]
    PushEntryTooBig,

    #[fail(display = "couldn't lock log directory")]
    CantLockDir,

    #[fail(display = "max. timestamp decreases between segments")]
    MaxTimestampNonMonotonic,

    #[fail(display = "segment gone while being iterated over")]
    IterSegmentGone,

    #[fail(display = "IO error")]
    Io,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileKind {
    Lock,
    Segment(segment::FileKind),
}

impl FileKind {
    const LOCK: &'static str = ".lock";

    pub fn from_path(p: impl AsRef<Path>) -> Option<Self> {
        if let Some(kind) = segment::FileKind::from_path(p.as_ref()) {
            return Some(FileKind::Segment(kind));
        }
        let p = p.as_ref().file_name()?.to_str()?;
        Some(match () {
            _ if p == Self::LOCK => FileKind::Lock,
            _ => return None,
        })
    }
}

pub struct Options {
    pub max_segment_len: u32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_segment_len: 1024 * 1024 * 1024,
        }
    }
}

pub struct Log {
    _lock: FileMutex,
    inner: Arc<Mutex<Inner>>,
}

impl Log {
    pub fn open_or_create(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        const UNKNOWN_DIR_ENTRIES_LIMIT: usize = 10;

        assert!(options.max_segment_len >= cast::u32(MIN_FRAME_LEN).unwrap(),
            "max_segment_len must be at least {} (MIN_FRAME_LEN)", MIN_FRAME_LEN);
        assert!(options.max_segment_len <= segment::HARD_MAX_SEGMENT_LEN,
            "max_segment_len must be at most {} (HARD_MAX_SEGMENT_LEN)",
            segment::HARD_MAX_SEGMENT_LEN);

        let path = path.as_ref().to_path_buf();

        let exists = path.exists();
        if !exists {
            fs::create_dir_all(&path).wrap_err_id(ErrorId::Io)?;
        }

        let _lock = FileMutex::try_lock(&path.join(FileKind::LOCK))
            .wrap_err_with(|_| (ErrorId::CantLockDir, format!("{:?}", path)))?;

        let mut segments = if exists {
            let mut segment_paths = Vec::new();
            let mut unknown = Vec::new();
            for dir_entry in fs::read_dir(&path).wrap_err_id(ErrorId::Io)? {
                let dir_entry = dir_entry.wrap_err_id(ErrorId::Io)?;
                let path = dir_entry.path();
                let file_kind = FileKind::from_path(&path);
                if !dir_entry.file_type().wrap_err_id(ErrorId::Io)?.is_file() || file_kind.is_none() {
                    unknown.push(path.clone());
                    if unknown.len() == UNKNOWN_DIR_ENTRIES_LIMIT + 1 {
                        break;
                    }
                }
                if !unknown.is_empty() {
                    let mut s: String = unknown.iter().map(|p| p.to_string_lossy()).join(", ");
                    if unknown.len() > UNKNOWN_DIR_ENTRIES_LIMIT {
                        s += ", ..."
                    }
                    return Err(Error::new(ErrorId::UnknownDirEntries, s));
                }
                if file_kind.unwrap() == FileKind::Segment(segment::FileKind::Segment) {
                    segment_paths.push(path.clone());
                }
            }

            segment_paths.sort();

            let mut segments = Vec::with_capacity(segment_paths.len());
            for (i, path) in segment_paths.iter().enumerate() {
                let segment = Segment::open(path, segment::Options {
                    read_only: i < segment_paths.len() - 1,
                    .. Default::default()
                }).context_with(|_| format!("opening segment {:?}", path))?;
                segments.push(segment);
            }

            for (i, segment) in segments.iter().enumerate() {
                if i > 0 && segment.max_timestamp() < segments[i - 1].max_timestamp() {
                    return Err(Error::new(ErrorId::MaxTimestampNonMonotonic, format!(
                        "{} in {:?} and {} in {:?}",
                        segments[i - 1].max_timestamp(), segments[i - 1].path(),
                        segment.max_timestamp(), segment.path())));
                }
            }

            segments.into()
        } else {
            VecDeque::new()
        };

        if segments.is_empty() {
            segments.push_back(Segment::create_new(&path, Id::min_value(), Timestamp::min_value(),
                Default::default())
                .context_with(|_| format!("creating first segment in {:?}", path))?);
        }

        Ok(Self {
            _lock,
            inner: Arc::new(Mutex::new(Inner {
                path,
                max_segment_len: options.max_segment_len,
                segments,
                stable_start_idx: 0,
            })),
        })
    }

    pub fn push(&self, entry: &mut BufEntry, buf: &mut BytesMut) -> Result<()> {
        self.inner.lock().push(entry, buf)
    }

    pub fn roll_over_if_idle(&self, max_idle: Duration, now: Timestamp) -> Result<bool> {
        self.inner.lock().roll_over_if_idle(max_idle, now)
    }

    pub fn iter(&self, range: impl RangeBounds<Id>) -> Iter {
        let start_id = match range.start_bound() {
            Bound::Excluded(_) => unreachable!(),
            Bound::Included(v) => *v,
            Bound::Unbounded => Id::min_value(),
        };
        let end_id_excl = match range.end_bound() {
            Bound::Excluded(v) => {
                assert!(*v >= start_id);
                Some(*v)
            },
            Bound::Included(v) => {
                assert!(*v >= start_id);
                Some(v.checked_add(1).unwrap())
            },
            Bound::Unbounded => None,
        };
        let (idx, end_excl) = {
            let inner = self.inner.lock();
            (inner.segment_idx_bound(start_id),
                end_id_excl.map(|id| (inner.segment_idx_bound(id - 1) + 1, id)))
        };
        Iter {
            log: self.inner.clone(),
            idx,
            start_id,
            end_excl,
            seg_iter: SegIterState::Buf(BytesMut::new()),
            dirty: false,
        }
    }
}

enum SegIterState {
    Empty,
    Buf(BytesMut),
    Iter(SegIter)
}

impl SegIterState {
    pub fn take_buf(&mut self) -> Option<BytesMut> {
        if let SegIterState::Buf(_) = self {
            if let SegIterState::Buf(v) = mem::replace(self, SegIterState::Empty) {
                Some(v)
            } else {
                unreachable!();
            }
        } else {
            None
        }
    }

    pub fn recycle_buf(&mut self) {
        if let SegIterState::Iter(iter) = mem::replace(self, SegIterState::Empty) {
            *self = SegIterState::Buf(iter.into_buf());
        } else {
            panic!("not in Iter state");
        }
    }

    pub fn clear(&mut self) {
        *self = SegIterState::Empty;
    }

    pub fn iter(&self) -> Option<&SegIter> {
        if let SegIterState::Iter(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn iter_mut(&mut self) -> Option<&mut SegIter> {
        if let SegIterState::Iter(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

pub struct Iter {
    log: Arc<Mutex<Inner>>,
    idx: u64,
    start_id: Id,
    end_excl: Option<(u64, Id)>,
    seg_iter: SegIterState,
    /// `true` if this iterator produced at least one entry.
    dirty: bool,
}

impl Iter {
    pub fn segment_iter(&self) -> &SegIter {
        self.seg_iter.iter().expect("wrong state")
    }

    pub fn buf(&self) -> &BytesMut {
        self.segment_iter().buf()
    }

    pub fn buf_mut(&mut self) -> &mut BytesMut {
        self.segment_iter_mut().buf_mut()
    }

    pub fn complete_read(&mut self) -> Result<()> {
        self.segment_iter_mut().complete_read()
    }

    fn segment_iter_mut(&mut self) -> &mut SegIter {
        self.seg_iter.iter_mut().expect("wrong state")
    }
}

impl Iterator for Iter {
    type Item = Result<BufEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.end_excl.is_some() && self.idx >= self.end_excl.unwrap().0 {
                self.seg_iter.clear();
                return None;
            }
            if let Some(buf) = self.seg_iter.take_buf() {
                let log = self.log.lock();
                let seg = match log.segment(self.idx) {
                    Ok(seg) => seg,
                    Err(idx) => {
                        self.idx = idx;
                        if self.dirty {
                            return Some(Err(Error::without_details(ErrorId::IterSegmentGone)));
                        }
                        // Since nothing has been produced by this iterator so far we can ignore
                        // disappearing segments.
                        continue;
                    }
                };
                let seg_iter = if let Some((_, end_id_excl)) = self.end_excl {
                    seg.iter(self.start_id..end_id_excl)
                } else {
                    seg.iter(self.start_id..)
                }.with_buf(buf);
                self.seg_iter = SegIterState::Iter(seg_iter);
            }
            let r = self.seg_iter.iter_mut().unwrap().next();
            if r.is_none() {
                if self.end_excl.is_some() ||
                        self.idx < self.log.lock().last_stable_idx() {
                    self.idx += 1;
                    self.seg_iter.recycle_buf();
                    continue;
                } else {
                    return None;
                }
            }
            self.dirty = true;
            break r;
        }
    }
}

/// Stable index provide indirect reference to the same segment even if segments are added or removed from the vec.
struct Inner {
    path: PathBuf,
    segments: VecDeque<Segment>,
    /// This should increase when tail segments are removed from `segments`.
    stable_start_idx: u64,
    max_segment_len: u32,
}

impl Inner {
    pub fn first_id(&self) -> Id {
        self.segments.front().unwrap().start_id()
    }

    pub fn next_id(&self) -> Id {
        self.segments.back().unwrap().next_id()
    }

    pub fn segment(&self, stable_idx: u64) -> StdResult<&Segment, u64> {
        self.idx_to_unstable(stable_idx)
            .map(|i| &self.segments[i])
    }

    pub fn next_stable_idx(&self) -> u64 {
        self.idx_from_unstable(self.segments.len())
    }

    pub fn last_stable_idx(&self) -> u64 {
        self.next_stable_idx() - 1
    }

    fn idx_from_unstable(&self, unstable_idx: usize) -> u64 {
        (unstable_idx as u64).checked_add(self.stable_start_idx)
            .expect("stable index overflow")
    }

    fn idx_to_unstable(&self, stable_idx: u64) -> StdResult<usize, u64> {
        stable_idx.checked_sub(self.stable_start_idx)
            .map(|i| i.to_usize().expect("stable index overflow"))
            .ok_or(self.stable_start_idx)
    }

    pub fn push(&mut self, entry: &mut BufEntry, buf: &mut BytesMut) -> Result<()> {
        let entry_len = cast::u32(buf.len()).unwrap();
        if entry_len > self.max_segment_len {
            return Err(Error::new(ErrorId::PushEntryTooBig, format!(
                "{} > {}",
                buf.len(), self.max_segment_len)));
        }

        self.roll_over_if_max_len(entry_len)?;

        self.segments.back_mut().unwrap().push(entry, buf)
    }

    pub fn roll_over_if_idle(&mut self, max_idle: Duration, now: Timestamp) -> Result<bool> {
        let (empty, max_timestamp) = {
            let seg = self.segments.back().unwrap();
            (seg.is_empty(), seg.max_timestamp())
        };
        if empty {
            return Ok(false);
        }
        if let Some(dur) = now.duration_since(max_timestamp) {
            if dur > max_idle {
                info!("rolling over segment based on max. idle time: max_idle={} now={} path={:?}",
                    humantime::format_duration(max_idle), now, self.path);
                self.roll_over()?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn head(&self) -> &Segment {
        self.segments.back().unwrap()
    }

    fn roll_over_if_max_len(&mut self, new_entry_len: u32) -> Result<()> {
        if self.head().len_bytes() + new_entry_len > self.max_segment_len {
            info!("rolling over segment based on max. length {}: {:?}",
                self.max_segment_len, self.path);
            debug!("start_id={} next_id={} max_timestamp={}",
                    self.head().start_id(), self.head().next_id(), self.head().max_timestamp());
            self.roll_over()?;
        }
        Ok(())
    }

    fn roll_over(&mut self) -> Result<()> {
        if self.head().is_empty() {
            return Ok(());
        }
        let start_id = {
            let cur_seg = self.segments.back_mut().unwrap();
            cur_seg.make_read_only()
                .context_with(|_| format!("making segment {:?} read-only", cur_seg.path()))?;
            cur_seg.next_id()
        };
        let max_timestamp = self.segments.back().unwrap().max_timestamp();
        assert!(max_timestamp > Timestamp::min_value());
        debug!("creating new segment in {:?} start_id={} max_timestamp={}",
            self.path, start_id, max_timestamp);
        self.segments.push_back(Segment::create_new(&self.path, start_id,
            max_timestamp, Default::default())?);
        Ok(())
    }

    fn segment_idx(&self, id: Id) -> Option<u64> {
        let (a, b) = self.segments.as_slices();
        let mut i = a.binary_search_by(|s| s.partial_cmp(&id).unwrap());
        if i.is_err() {
            i = b.binary_search_by(|s| s.partial_cmp(&id).unwrap());
        }
        i.ok().map(|i| self.idx_from_unstable(i))
    }

    fn segment_idx_bound(&self, id: Id) -> u64 {
        let idx = self.segment_idx(id);
        idx.unwrap_or_else(|| {
            if id < self.first_id() {
                0
            } else if id >= self.next_id() {
                self.next_stable_idx()
            } else {
                self.segments.iter().for_each(|s| println!("{} {}", s.start_id(), s.next_id()));
                unreachable!("id={} first_id={} next_id={}", id, self.first_id(), self.next_id());
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::ErrorId;
    use num_traits::cast::ToPrimitive;
    use rand::Rng;
    use rayon::prelude::*;
    use std::mem;
    use std::ops::{Bound, Range};
    use crate::entry::BufEntryBuilder;
    use crate::message::{Message, MessageBuilder};
    use crate::util::test_common::*;

    #[test]
    fn lock() {
        let dir = mktemp::Temp::new_dir().unwrap();
        let log = Log::open_or_create(&dir, Default::default()).unwrap();
        assert_eq!(Log::open_or_create(&dir, Default::default()).err().unwrap().id(),
            &ErrorId::CantLockDir.into());

        mem::drop(log);
        Log::open_or_create(&dir, Default::default()).unwrap();
    }

    #[test]
    fn roll_over_on_len() {
        let dir = mktemp::Temp::new_dir().unwrap();

        let (ref mut e, ref mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();

        let log = Log::open_or_create(&dir, Options {
            max_segment_len: buf.len().to_u32().unwrap(),
            ..Default::default()
        }).unwrap();
        assert_eq!(log.inner.lock().segments.len(), 1);

        log.push(e, buf).unwrap();
        assert_eq!(log.inner.lock().segments.len(), 1);

        let (ref mut e, ref mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();
        log.push(e, buf).unwrap();
        assert_eq!(log.inner.lock().segments.len(), 2);
        assert_eq!(log.inner.lock().segments[0].len_bytes(), buf.len().to_u32().unwrap());
        assert_eq!(log.inner.lock().segments[1].len_bytes(), buf.len().to_u32().unwrap());
    }

    #[test]
    fn roll_over_on_idle() {
        let dir = mktemp::Temp::new_dir().unwrap();
        let log = Log::open_or_create(&dir, Default::default()).unwrap();

        // Don't roll over empty.
        assert!(!log.roll_over_if_idle(Duration::from_millis(0), Timestamp::max_value()).unwrap());
        assert_eq!(log.inner.lock().segments.len(), 1);

        let (ref mut e, ref mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();
        log.push(e, buf).unwrap();
        assert_eq!(log.inner.lock().segments.len(), 1);
        assert!(log.roll_over_if_idle(Duration::from_millis(0), e.max_timestamp() + 1).unwrap());
        assert_eq!(log.inner.lock().segments.len(), 2);
        assert_eq!(log.inner.lock().segments[0].len_bytes(), buf.len().to_u32().unwrap());
        assert_eq!(log.inner.lock().segments[1].len_bytes(), 0);
    }

    #[test]
    fn iter() {
        let dir = mktemp::Temp::new_dir().unwrap();

        const ENTRY_COUNT: usize = 50;
        const MSG_COUNT: Range<usize> = 10..21;

        let mut entries = Vec::new();
        let mut entry_msgs = Vec::new();

        let it = {
            let log = Log::open_or_create(&dir, Options {
                max_segment_len: 1024,
                ..Default::default()
            }).unwrap();

            let mut id = Id::min_value();
            for _ in 0..ENTRY_COUNT {
                let msg_count = thread_rng().gen_range(MSG_COUNT.start, MSG_COUNT.end);
                let msgs: Vec<_> = (0..msg_count)
                    .map(|_| {
                        let r = MessageBuilder {
                            id: Some(id),
                            key: Some(random_vec(0..=20)),
                            value: Some(random_vec(0..=20)),
                            ..Default::default()
                        };
                        id += 1;
                        r
                    })
                    .collect();

                let (mut e, mut buf) = BufEntryBuilder::from(msgs.clone()).build();
                log.push(&mut e, &mut buf).unwrap();

                entry_msgs.push(msgs.iter().cloned().map(|mut b| {
                    b.timestamp = Some(e.first_timestamp());
                    b.clone().build()
                }).collect::<Vec<_>>());

                entries.push(e);
            }

            log.iter(..)
        };

        fn get(mut it: Iter) -> (Vec<BufEntry>, Vec<Vec<Message>>) {
            let mut entries = Vec::new();
            let mut entry_msgs = Vec::new();
            while let Some(e) = it.next() {
                let e = e.unwrap();
                it.complete_read().unwrap();
                entry_msgs.push(e.iter(it.buf()).map(|m| m.unwrap()).collect::<Vec<_>>());
                entries.push(e);
            }
            (entries, entry_msgs)
        }

        let (act_entries, act_entry_msgs) = get(it);
        assert_eq!(act_entries, entries);
        assert_eq!(act_entry_msgs, entry_msgs);

        // Verify parallel iteration over ranges.

        let log = Log::open_or_create(&dir, Default::default()).unwrap();

        let ranges: Vec<((usize, usize), (Bound<Id>, Bound<Id>))> = vec![
            ((0, 15), (Bound::Unbounded, Bound::Excluded(entries[15].start_id()))),
            ((12, 43), (Bound::Included(entries[12].start_id() + MSG_COUNT.start as u64 / 2),
                Bound::Included(entries[42].start_id()))),
            ((30, 50), (Bound::Included(entries[30].start_id()), Bound::Unbounded)),
        ];
        let act = ranges.par_iter()
            .map(|(idx, bounds)| {
                let mut r = None;
                for _ in 0..100 {
                    r = Some((idx, get(log.iter(*bounds))));
                }
                r.unwrap()
            }).collect::<Vec<_>>();

        for (idx_range, (act_entries, act_entry_msgs)) in act {
            assert_eq!(act_entries.len(), idx_range.1 - idx_range.0);
            assert_eq!(act_entry_msgs.len(), idx_range.1 - idx_range.0);
            for i in idx_range.0..idx_range.1 {
                assert_eq!(&act_entries[i - idx_range.0], &entries[i]);
                assert_eq!(&act_entry_msgs[i - idx_range.0], &entry_msgs[i]);
            }
        }
    }

    #[test]
    fn reopen() {
        let dir = mktemp::Temp::new_dir().unwrap();
        {
            let log = Log::open_or_create(&dir, Options {
                max_segment_len: 500,
            }).unwrap();
            for _ in 0..100 {
                let (ref mut e, ref mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();
                log.push(e, buf).unwrap();
            }
        }
        let log = Log::open_or_create(&dir, Default::default()).unwrap();
        for _ in 0..100 {
            let (ref mut e, ref mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();
            log.push(e, buf).unwrap();
        }
    }
}