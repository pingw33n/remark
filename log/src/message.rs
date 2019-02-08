use chrono::{DateTime, NaiveDateTime, Utc};
use num_integer::Integer;
use num_traits::cast::ToPrimitive;
use std::fmt;
use std::io::prelude::*;
use std::ops;
use std::time::{Duration, SystemTime};

use crate::error::*;
use crate::util::DurationExt;
use rcommon::varint::{self, ReadExt, WriteExt};

#[derive(Clone, Copy, Debug, Eq, Fail, PartialEq)]
pub enum ErrorId {
    #[fail(display = "stored message id overflows max value")]
    IdOverflow,

    #[fail(display = "stored message timestamp overflows max value")]
    TimestampOverflow,

    #[fail(display = "message's stored len and actual len differ")]
    LenMismatch,

    #[fail(display = "stored message header name is null")]
    HeaderNameIsNull,

    #[fail(display = "stored message header value is null")]
    HeaderValueIsNull,

    #[fail(display = "malformed UTF-8 string")]
    MalformedUtf8,

    #[fail(display = "IO error")]
    Io,
}


#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Id(u64);

impl Id {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn min_value() -> Self {
        Self::new(0)
    }

    pub const fn max_value() -> Self {
        Self::new(u64::max_value())
    }

    pub fn get(self) -> u64 {
        self.0
    }

    pub fn checked_add(self, rhs: u64) -> Option<Self> {
        self.0.checked_add(rhs).map(Self::new)
    }

    pub fn checked_sub(self, rhs: u64) -> Option<Self> {
        self.0.checked_sub(rhs).map(Self::new)
    }

    pub fn checked_delta(self, rhs: Self) -> Option<u64> {
        self.0.checked_sub(rhs.get())
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Id({})", self.0)
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl ops::Add<u64> for Id {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        self.checked_add(rhs).unwrap()
    }
}

impl ops::AddAssign<u64> for Id {
    fn add_assign(&mut self, rhs: u64) {
        *self = *self + rhs;
    }
}

impl ops::Sub for Id {
    type Output = u64;

    fn sub(self, rhs: Self) -> Self::Output {
        self.get().checked_sub(rhs.get()).unwrap()
    }
}

impl ops::Sub<u64> for Id {
    type Output = Self;

    fn sub(self, rhs: u64) -> Self::Output {
        self.checked_sub(rhs).unwrap()
    }
}

impl ops::SubAssign<u64> for Id {
    fn sub_assign(&mut self, rhs: u64) {
        *self = *self - rhs;
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct Timestamp(i64);

impl Timestamp {
    pub fn now() -> Self {
        let dur = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
            .expect("system time couldn't be converted to Timestamp");
        Self::from_duration_since_epoch(dur).unwrap()
    }

    pub fn from_millis(millis: i64) -> Option<Self> {
        if millis >= Self::min_value().0 && millis <= Self::max_value().0 {
            Some(Self(millis))
        } else {
            None
        }
    }

    pub const fn epoch() -> Self {
        Self(0)
    }

    pub const fn min_value() -> Self {
        Self(-62167219200000) // 0000-01-01T00:00:00.000Z
    }

    pub const fn max_value() -> Self {
        Self(32503680000000) // 3000-01-01T00:00:00.000Z
    }

    pub fn millis(&self) -> i64 {
        self.0
    }

    pub fn checked_add(&self, duration: Duration) -> Option<Self> {
        self.checked_add_millis(Self::from_duration_since_epoch(duration)?.0)
    }

    pub fn checked_add_millis(&self, millis: i64) -> Option<Self> {
        self.0.checked_add(millis).and_then(Self::from_millis)
    }

    pub fn checked_sub(&self, duration: Duration) -> Option<Self> {
        self.checked_sub_millis(Self::from_duration_since_epoch(duration)?.0)
    }

    pub fn checked_sub_millis(&self, millis: i64) -> Option<Self> {
        self.0.checked_sub(millis).and_then(Self::from_millis)
    }

    pub fn checked_delta(&self, other: Self) -> Option<i64> {
        self.0.checked_sub(other.0)
    }

    pub fn duration_since(&self, other: Self) -> Option<Duration> {
        if self.0 >= other.0 {
            (self.0 as i128 - other.0 as i128).to_u64()
                .map(Duration::from_millis)
        } else {
            None
        }
    }

    pub fn to_date_time(&self) -> DateTime<Utc> {
        let millis = (self.0 % 1000 + 1000) % 1000;
        let secs = self.0.div_floor(&1000);
        let ndt = NaiveDateTime::from_timestamp(secs, (millis * 1_000_000) as u32);
        DateTime::from_utc(ndt, Utc)
    }

    fn from_duration_since_epoch(v: Duration) -> Option<Self> {
        Some(Self(v.as_millis_u64().and_then(|v| cast::i64(v).ok())?))
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_date_time().format("%Y-%m-%dT%H:%M:%S%.3fZ"))
    }
}

impl ops::Add<i64> for Timestamp {
    type Output = Self;

    fn add(self, rhs: i64) -> Self::Output {
        self.checked_add_millis(rhs).unwrap()
    }
}

impl ops::AddAssign<i64> for Timestamp {
    fn add_assign(&mut self, rhs: i64) {
        *self = *self + rhs;
    }
}

impl ops::Add<Duration> for Timestamp {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        self.checked_add(rhs).unwrap()
    }
}

impl ops::AddAssign<Duration> for Timestamp {
    fn add_assign(&mut self, rhs: Duration) {
        *self = *self + rhs;
    }
}

impl ops::Sub for Timestamp {
    type Output = i64;

    fn sub(self, rhs: Self) -> Self::Output {
        self.checked_delta(rhs).unwrap()
    }
}

impl ops::Sub<i64> for Timestamp {
    type Output = Self;

    fn sub(self, rhs: i64) -> Self::Output {
        self.checked_sub_millis(rhs).unwrap()
    }
}

impl ops::SubAssign<i64> for Timestamp {
    fn sub_assign(&mut self, rhs: i64) {
        *self = *self - rhs;
    }
}

impl ops::Sub<Duration> for Timestamp {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        self.checked_sub(rhs).unwrap()
    }
}

impl ops::SubAssign<Duration> for Timestamp {
    fn sub_assign(&mut self, rhs: Duration) {
        *self = *self - rhs;
    }
}

impl Into<DateTime<Utc>> for Timestamp {
    fn into(self) -> DateTime<Utc> {
        self.to_date_time()
    }
}

#[derive(Clone, Debug, Default)]
pub struct MessageBuilder {
    pub id: Option<Id>,
    pub timestamp: Option<Timestamp>,
    pub headers: Headers,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}

impl MessageBuilder {
    pub fn build(self) -> Message {
        Message {
            id: self.id.unwrap(),
            timestamp: self.timestamp.unwrap(),
            headers: self.headers,
            key: self.key,
            value: self.value,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub id: Id,
    pub timestamp: Timestamp,
    pub headers: Headers,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}

impl Message {
    pub fn read(rd: &mut impl Read, next_id: Id, next_timestamp: Timestamp) -> Result<Self> {
        let len = rd.read_u32_varint().wrap_err_id(ErrorId::Io)?;
        let id_delta = rd.read_u32_varint().wrap_err_id(ErrorId::Io)?;
        let timestamp_delta = rd.read_i64_varint().wrap_err_id(ErrorId::Io)?;
        let headers = Headers::read(rd)?;
        let key = read_opt_bstring(rd)?;
        let value = read_opt_bstring(rd)?;

        let id = next_id.checked_add(id_delta as u64).ok_or_else(||
            Error::without_details(ErrorId::IdOverflow))?;
        let timestamp = next_timestamp.checked_add_millis(timestamp_delta).ok_or_else(||
            Error::without_details(ErrorId::TimestampOverflow))?;

        let r = Self {
            id,
            timestamp,
            headers,
            key,
            value,
        };

        // TODO should be already able to tell how much bytes was actually read from rd.
        if r.writer(next_id, next_timestamp).encoded_len() != cast::usize(len) {
            return Err(Error::without_details(ErrorId::LenMismatch));
        }

        Ok(r)
    }

    pub fn writer(&self, next_id: Id, next_timestamp: Timestamp) -> MessageWriter {
        assert!(self.id.checked_delta(next_id)
            .filter(|&v| v <= u32::max_value() as u64)
            .is_some());
        assert!(self.timestamp.checked_delta(next_timestamp).is_some());
        MessageWriter {
            msg: self,
            next_id,
            next_timestamp,
        }
    }
}

pub struct MessageWriter<'a> {
    msg: &'a Message,
    next_id: Id,
    next_timestamp: Timestamp,
}

impl MessageWriter<'_> {
    pub fn id_delta(&self) -> u32 {
        cast::u32(self.msg.id.checked_delta(self.next_id).unwrap()).unwrap()
    }

    pub fn timestamp_delta(&self) -> i64 {
        self.msg.timestamp.millis().checked_sub(self.next_timestamp.millis()).unwrap()
    }

    pub fn encoded_len(&self) -> usize {
        let l = varint::encoded_len(self.id_delta() as u64) as usize +
            varint::encoded_len_i64(self.timestamp_delta()) as usize +
            self.msg.headers.encoded_len() +
            encoded_len_opt_bstring(self.msg.key.as_ref()) +
            encoded_len_opt_bstring(self.msg.value.as_ref());
        varint::encoded_len(l as u64) as usize + l
    }

    pub fn write(&self, wr: &mut impl Write) -> Result<()> {
        wr.write_u32_varint(cast::u32(self.encoded_len()).unwrap()).wrap_err_id(ErrorId::Io)?;
        wr.write_u32_varint(self.id_delta()).wrap_err_id(ErrorId::Io)?;
        wr.write_i64_varint(self.timestamp_delta()).wrap_err_id(ErrorId::Io)?;
        self.msg.headers.write(wr)?;
        write_opt_bstring(wr, self.msg.key.as_ref())?;
        write_opt_bstring(wr, self.msg.value.as_ref())?;
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Headers {
    pub vec: Vec<Header>,
}

impl Headers {
    pub fn encoded_len(&self) -> usize {
        varint::encoded_len(self.vec.len() as u64) as usize +
            self.vec.iter().map(|h| h.encoded_len()).sum::<usize>()
    }

    pub fn read(rd: &mut impl Read) -> Result<Self> {
        let count = cast::usize(rd.read_u32_varint().wrap_err_id(ErrorId::Io)?);
        let mut vec = Vec::with_capacity(count);
        for _ in 0..count {
            vec.push(Header::read(rd)?);
        }
        Ok(Self {
            vec,
        })
    }

    pub fn write(&self, wr: &mut impl Write) -> Result<()> {
        wr.write_u32_varint(self.vec.len() as u32).wrap_err_id(ErrorId::Io)?;
        for h in &self.vec {
            h.write(wr)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Header {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

impl Header {
    pub fn encoded_len(&self) -> usize {
        encoded_len_bstring(self.name.as_ref()) +
            encoded_len_bstring(self.value.as_ref())
    }

    pub fn read(rd: &mut impl Read) -> Result<Self> {
        let name = read_opt_bstring(rd)?
            .ok_or_else(|| Error::without_details(ErrorId::HeaderNameIsNull))?;
        let value = read_opt_bstring(rd)?
            .ok_or_else(|| Error::without_details(ErrorId::HeaderValueIsNull))?;
        Ok(Self {
            name,
            value,
        })
    }

    pub fn write(&self, wr: &mut impl Write) -> Result<()> {
        write_bstring(wr, &self.name)?;
        write_bstring(wr, &self.value)
    }
}

fn encoded_len_bstring(buf: &[u8]) -> usize {
    varint::encoded_len(buf.len() as u64) as usize +
        buf.len()
}

fn encoded_len_opt_bstring<T: AsRef<[u8]>>(buf: Option<T>) -> usize {
    if let Some(buf) = buf {
        encoded_len_bstring(buf.as_ref())
    } else {
        varint::encoded_len(0) as usize
    }
}

fn read_opt_bstring(rd: &mut impl Read) -> Result<Option<Vec<u8>>> {
    let len = rd.read_u32_varint().wrap_err_id(ErrorId::Io)?;
    if len == 0 {
        Ok(None)
    } else {
        let mut vec = Vec::with_capacity(cast::usize(len) - 1);
        vec.resize(vec.capacity(), 0);
        rd.read_exact(&mut vec).wrap_err_id(ErrorId::Io)?;
        Ok(Some(vec))
    }
}

fn write_bstring(wr: &mut impl Write, buf: &[u8]) -> Result<()> {
    wr.write_u32_varint(cast::u32(buf.len()).unwrap().checked_add(1).unwrap())
        .wrap_err_id(ErrorId::Io)?;
    wr.write_all(buf).wrap_err_id(ErrorId::Io)?;
    Ok(())
}

fn write_opt_bstring<T: AsRef<[u8]>>(wr: &mut impl Write, buf: Option<T>) -> Result<()> {
    if let Some(buf) = buf {
        write_bstring(wr, buf.as_ref())
    } else {
        wr.write_u32_varint(0).wrap_err_id(ErrorId::Io)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    mod id {
        use super::*;

        #[test]
        fn checked_delta() {
            assert_eq!(Id::min_value().checked_delta(Id::min_value()), Some(0));
            assert_eq!(Id::new(2).checked_delta(Id::new(1)), Some(1));
            assert_eq!(Id::min_value().checked_delta(Id::new(2)), None);
            assert_eq!(Id::min_value().checked_delta(Id::max_value()), None);
        }
    }

    mod timestamp {
        use super::*;

        #[test]
        fn display() {
            assert_eq!(Timestamp::epoch().to_string(), "1970-01-01T00:00:00.000Z".to_owned());
            assert_eq!(Timestamp::from_millis(1549306753_123).unwrap().to_string(),
                "2019-02-04T18:59:13.123Z".to_owned());
            assert_eq!(Timestamp::from_millis(-1).unwrap().to_string(), "1969-12-31T23:59:59.999Z".to_owned());
            assert_eq!(Timestamp::from_millis(-386380800_000).unwrap().to_string(),
                "1957-10-04T00:00:00.000Z".to_owned());
            assert_eq!(Timestamp::from_millis(-386380799_999).unwrap().to_string(),
                "1957-10-04T00:00:00.001Z".to_owned());
            assert_eq!(Timestamp::min_value().to_string(), "0000-01-01T00:00:00.000Z".to_owned());
            assert_eq!(Timestamp::max_value().to_string(), "3000-01-01T00:00:00.000Z".to_owned());
        }
    }

    mod message {
        use super::*;
        use std::io::Cursor;

        #[test]
        fn write_read() {
            let now = Timestamp::now();
            let d = &[
                MessageBuilder {
                    id: Some(Id::min_value()),
                    timestamp: Some(Timestamp::epoch()),
                    .. Default::default()
                }.build(),
                Message {
                    id: Id::new(12345),
                    timestamp: now,
                    headers: Headers {
                        vec: vec![
                            Header { name: "".into(), value: vec![] },
                            Header { name: "header ❤".into(), value: vec![0, 1, 128, 255] },
                        ],
                    },
                    key: Some(vec![0, 1, 128, 255]),
                    value: Some(vec![0, 42, 128, 255]),
                },
            ];

            let ref mut cur = Cursor::new(Vec::new());
            for msg in d {
                for &(next_id, next_timestamp) in &[
                    (Id::min_value(), Timestamp::epoch()),
                    (Id::new(12345), now),
                ] {
                    cur.set_position(0);
                    cur.get_mut().clear();

                    if msg.id < next_id || msg.timestamp < next_timestamp {
                        continue;
                    }
                    let wr = msg.writer(next_id, next_timestamp);

                    wr.write(cur).unwrap();
                    assert_eq!(cur.get_ref().len(), wr.encoded_len());

                    cur.set_position(0);
                    let actual = Message::read(cur,next_id, next_timestamp)
                        .unwrap();
                    assert_eq!(&actual, msg);
                }
            }
        }
    }
}