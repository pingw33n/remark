pub mod bytes;
pub mod entry;
pub mod error;
pub mod file;
pub mod index;
pub mod log;
pub mod segment;
pub mod util;

use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use memmap::{Mmap, MmapMut};
use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::marker::PhantomData;
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::error::*;
use crate::index::Index;
use crate::segment::Segment;
use crate::util::DurationExt;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct Timestamp(u64);

impl Timestamp {
    pub fn now() -> Self {
        let dur = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
            .expect("system time couldn't be converted to unix epoch");
        Self::from_duration(dur).unwrap()
    }

    pub fn epoch() -> Self {
        Self(0)
    }

    pub fn from_duration(v: Duration) -> Option<Self> {
        Some(Self(v.as_millis_u64()?))
    }

    pub fn millis(&self) -> u64 {
        self.0
    }

    pub fn checked_add(&self, duration: Duration) -> Option<Self> {
        self.checked_add_millis(duration.as_millis_u64()?)
    }

    pub fn checked_add_millis(&self, millis: u64) -> Option<Self> {
        self.0.checked_add(millis).map(|v| v.into())
    }

    pub fn duration_since(&self, since: Timestamp) -> Option<Duration> {
        self.0.checked_sub(since.0).map(Duration::from_millis)
    }
}

impl From<u64> for Timestamp {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl Into<Duration> for Timestamp {
    fn into(self) -> Duration {
        Duration::from_millis(self.0)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::entry::BufEntryBuilder;
    use crate::entry::BufEntry;
    use crate::entry::message::MessageBuilder;
    use crate::log::Log;

    #[test]
    fn ttt() {
        let mut b = BufEntryBuilder::new(10, Timestamp::now());
//        b.message(MessageBuilder {
//            //        value: Some("test".into()),
//            ..Default::default()
//            //    }).message(MessageBuilder {
//            //        key: Some("key2".into()),
//            //        value: Some("value2".into()),
//            //        .. Default::default()
//        });
        let (ref mut entry, ref mut buf) = b.build();

//        {
//            let mut log = Log::open_or_create("/tmp/my_log", log::Options { max_segment_len: 10_000_000 }).unwrap();
//
//            for _ in 0..100 {
//                log.push(entry, buf).unwrap();
//            }
//        }

//        {
//            use std::fs::OpenOptions;
//            let f = OpenOptions::new()
//                .write(true)
//                .open("/tmp/00000000000000000000.id").unwrap();
//            f.set_len(8).unwrap();
//
//            let f = OpenOptions::new()
//                .write(true)
//                .open("/tmp/00000000000000000000.timestamp").unwrap();
//            f.set_len(12).unwrap();
//        }
        let mut seg = Segment::open("/tmp/my_log/00000000000000000000.data", Default::default()).unwrap();
//        for entry in seg.get(..) {
//            dbg!(entry);
//        }
        let mut it = seg.get(8..=20);
        let mut i = 0;
        while let Some(entry) = it.next() {
            if i == 5 {
                break;
            }
            dbg!(entry.unwrap().first_id());
//            dbg!(it.buf_mut().len());
//            i += 1;
        }

        panic!();
    }
}
