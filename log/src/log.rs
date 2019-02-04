use itertools::Itertools;
use log::info;
use num_traits::cast::ToPrimitive;
use std::fs;
use std::path::{Path, PathBuf};
use std::collections::VecDeque;
use std::time::Duration;

use crate::bytes::*;
use crate::entry::BufEntry;
use crate::entry::format::MIN_FRAME_LEN;
use crate::error::*;
use crate::message::{Id, Timestamp};
use crate::segment::{self, Segment};
use crate::util::file_mutex::FileMutex;

const LOCK_FILE_NAME: &'static str = ".remark_lock";

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

    #[fail(display = "IO error")]
    Io,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileType {
    Data,
    IdIndex,
    Lock,
    TimestampIndex,
}

impl FileType {
    pub fn from_path(p: impl AsRef<Path>) -> Option<Self> {
        let p = p.as_ref().file_name()?.to_str()?;
        Some(match () {
            _ if p.ends_with(segment::DATA_FILE_SUFFIX) => FileType::Data,
            _ if p.ends_with(segment::ID_INDEX_FILE_SUFFIX) => FileType::IdIndex,
            _ if p == LOCK_FILE_NAME => FileType::Lock,
            _ if p.ends_with(segment::TIMESTAMP_INDEX_FILE_SUFFIX) => FileType::TimestampIndex,
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
    path: PathBuf,
    segments: VecDeque<Segment>,
    max_segment_len: u32,
    _lock: FileMutex,
    max_timestamp: Timestamp,
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

        let _lock = FileMutex::try_lock(&path.join(LOCK_FILE_NAME))
            .wrap_err_with(|_| (ErrorId::CantLockDir, format!("{:?}", path)))?;

        let (mut segments, max_timestamp) = if exists {
            let mut segment_paths = Vec::new();
            let mut unknown = Vec::new();
            for dir_entry in fs::read_dir(&path).wrap_err_id(ErrorId::Io)? {
                let dir_entry = dir_entry.wrap_err_id(ErrorId::Io)?;
                let path = dir_entry.path();
                let file_type = FileType::from_path(&path);
                if !dir_entry.file_type().wrap_err_id(ErrorId::Io)?.is_file() || file_type.is_none() {
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
                if file_type.unwrap() == FileType::Data {
                    segment_paths.push(path.clone());
                }
            }

            let mut segments = Vec::with_capacity(segment_paths.len());
            for path in &segment_paths {
                let segment = Segment::open(path, segment::Options {
                    read_only: true,
                    .. Default::default()
                }).context_with(|_| format!("opening segment {:?}", path))?;
                segments.push(segment);
            }
            segments.sort_by_key(|s| s.base_id());

            for (i, segment) in segments.iter().enumerate() {
                if i > 0 && segment.max_timestamp() < segments[i - 1].max_timestamp() {
                    return Err(Error::new(ErrorId::MaxTimestampNonMonotonic, format!(
                        "{:?} and {:?}",
                        segments[i - 1].path(), segment.path())));
                }
            }
            let max_timestamp = segments.last().map(|s| s.max_timestamp());

            (segments.into(), max_timestamp)
        } else {
            (VecDeque::new(), None)
        };
        let max_timestamp = max_timestamp.unwrap_or(Timestamp::min_value());

        if segments.is_empty() {
            segments.push_back(Segment::create_new(&path, Id::min_value(), max_timestamp,
                Default::default())
                .context_with(|_| format!("creating first segment in {:?}", path))?);
        }

        Ok(Self {
            path,
            segments,
            max_segment_len: options.max_segment_len,
            _lock,
            max_timestamp,
        })
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

    fn roll_over_if_max_len(&mut self, new_entry_len: u32) -> Result<()> {
        if self.segments.back().unwrap().len_bytes() + new_entry_len > self.max_segment_len {
            info!("rolling over segment based on max. length {}: {:?}",
                self.max_segment_len, self.path);
            self.roll_over()?;
        }
        Ok(())
    }

    fn roll_over(&mut self) -> Result<()> {
        let base_id = {
            let cur_seg = self.segments.back_mut().unwrap();
            cur_seg.make_read_only()
                .context_with(|_| format!("making segment {:?} read-only", cur_seg.path()))?;
            cur_seg.next_id()
        };
        self.segments.push_back(Segment::create_new(&self.path, base_id,
            self.max_timestamp, Default::default())?);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use super::ErrorId;
    use std::mem;
    use crate::entry::BufEntryBuilder;
    use crate::message::MessageBuilder;

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

        let mut log = Log::open_or_create(&dir, Options {
            max_segment_len: buf.len().to_u32().unwrap(),
            ..Default::default()
        }).unwrap();
        assert_eq!(log.segments.len(), 1);

        log.push(e, buf).unwrap();
        assert_eq!(log.segments.len(), 1);

        let (ref mut e, ref mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();
        log.push(e, buf).unwrap();
        assert_eq!(log.segments.len(), 2);
        assert_eq!(log.segments[0].len_bytes(), buf.len().to_u32().unwrap());
        assert_eq!(log.segments[1].len_bytes(), buf.len().to_u32().unwrap());
    }

    #[test]
    fn roll_over_on_idle() {
        let dir = mktemp::Temp::new_dir().unwrap();
        let mut log = Log::open_or_create(&dir, Default::default()).unwrap();

        // Don't roll over empty.
        assert!(!log.roll_over_if_idle(Duration::from_millis(0), Timestamp::max_value()).unwrap());
        assert_eq!(log.segments.len(), 1);

        let (ref mut e, ref mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();
        log.push(e, buf).unwrap();
        assert_eq!(log.segments.len(), 1);
        assert!(log.roll_over_if_idle(Duration::from_millis(0), e.max_timestamp() + 1).unwrap());
        assert_eq!(log.segments.len(), 2);
        assert_eq!(log.segments[0].len_bytes(), buf.len().to_u32().unwrap());
        assert_eq!(log.segments[1].len_bytes(), 0);
    }
}