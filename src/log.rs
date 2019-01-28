use itertools::Itertools;
use std::borrow::Cow;
use std::fs;
use std::path::{Path, PathBuf};
use std::collections::VecDeque;

use crate::bytes::*;
use crate::entry::BufEntry;
use crate::entry::format::MIN_FRAME_LEN;
use crate::error::*;
use crate::segment::{self, Segment};

#[derive(Debug, Eq, Fail, PartialEq)]
pub enum Error {
    #[fail(display = "unknown files/directories found under a log directory: {}", _0)]
    UnknownDirEntries(Cow<'static, str>),

    #[fail(display = "{}", _0)]
    EntryTooBig(Cow<'static, str>),

    #[fail(display = "IO error")]
    Io,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileType {
    Data,
    IdIndex,
    TimestampIndex,
}

impl FileType {
    pub fn from_path(p: impl AsRef<Path>) -> Option<Self> {
        let p = p.as_ref().file_name()?.to_str()?;
        Some(match () {
            _ if p.ends_with(segment::DATA_FILE_SUFFIX) => FileType::Data,
            _ if p.ends_with(segment::ID_INDEX_FILE_SUFFIX) => FileType::IdIndex,
            _ if p.ends_with(segment::TIMESTAMP_INDEX_FILE_SUFFIX) => FileType::TimestampIndex,
            _ => return None,
        })
    }
}

pub struct Options {
    pub max_segment_len: u32,
}

pub struct Log {
    path: PathBuf,
    segments: VecDeque<Segment>,
    max_segment_len: u32,
}

impl Log {
    pub fn open_or_create(path: impl Into<PathBuf>, options: Options) -> Result<Self> {
        const UNKNOWN_DIR_ENTRIES_LIMIT: usize = 10;

        assert!(options.max_segment_len >= cast::u32(MIN_FRAME_LEN).unwrap(),
            "max_segment_len must be at least {} (MIN_FRAME_LEN)", MIN_FRAME_LEN);
        assert!(options.max_segment_len <= segment::HARD_MAX_SEGMENT_LEN,
            "max_segment_len must be at most {} (HARD_MAX_SEGMENT_LEN)",
            segment::HARD_MAX_SEGMENT_LEN);

        let path = path.into();
        let mut segments = if path.exists() {
            let mut segment_paths = Vec::new();
            let mut unknown = Vec::new();
            for dir_entry in fs::read_dir(&path).context(Error::Io)? {
                let dir_entry = dir_entry.context(Error::Io)?;
                let path = dir_entry.path();
                let file_type = FileType::from_path(&path);
                if !dir_entry.file_type().context(Error::Io)?.is_file() || file_type.is_none() {
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
                    return Err(Error::UnknownDirEntries(s.into())
                        .into());
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
                })
                    .with_more_context(|_| format!("opening segment {:?}", path))?;
                segments.push(segment);
            }
            segments.sort_by_key(|s| s.base_id());

            segments.into()
        } else {
            fs::create_dir_all(&path).context(Error::Io)?;
            VecDeque::new()
        };

        if segments.is_empty() {
            segments.push_back(Segment::create(&path, 0, Default::default())
                .with_more_context(|_| format!("creating segment 0 in {:?}", path))?);
        }

        Ok(Self {
            path,
            segments,
            max_segment_len: options.max_segment_len,
        })
    }

    pub fn push(&mut self, entry: &mut BufEntry, buf: &mut BytesMut) -> Result<()> {
        let entry_len = cast::u32(buf.len()).unwrap();
        if entry_len > self.max_segment_len {
            return Err(Error::EntryTooBig(format!(
                "entry is bigger than the max segment len: {} > {}",
                buf.len(), self.max_segment_len)
                .into()).into());
        }
        if self.segments.back_mut().unwrap().len() + entry_len > self.max_segment_len {
            let base_id = self.segments.back_mut().unwrap().next_id();
            self.segments.push_back(Segment::create(&self.path, base_id, Default::default())?);
        }

        self.segments.back_mut().unwrap().push(entry, buf)
    }

    fn find_segment_idx(&self, id: u64) -> Option<usize> {
        let (a, b) = self.segments.as_slices();
        let mut i = a.binary_search_by(|s| s.partial_cmp(&id).unwrap());
        if i.is_err() {
            i = b.binary_search_by(|s| s.partial_cmp(&id).unwrap());
        }
        i.ok()
    }
}