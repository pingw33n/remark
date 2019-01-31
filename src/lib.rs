#![deny(non_snake_case)]
#![deny(unused_must_use)]

#[macro_use]
mod macros;

pub mod bytes;
pub mod entry;
pub mod error;
pub mod file;
pub mod index;
pub mod log;
pub mod segment;
pub mod util;

use std::time::{Duration, SystemTime};

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
}
