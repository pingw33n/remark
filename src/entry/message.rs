use std::fmt;
use std::io::prelude::*;
use std::num::NonZeroU64;
use std::ops;

use super::*;
use super::Error;
use crate::util::DurationExt;
use crate::util::varint::{self, ReadExt, WriteExt};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Id(NonZeroU64);

impl Id {
    pub fn new(id: u64) -> Option<Self> {
        NonZeroU64::new(id).map(Self)
    }

    pub fn min_value() -> Self {
        Self::new(1).unwrap()
    }

    pub fn max_value() -> Self {
        Self::new(u64::max_value()).unwrap()
    }

    pub fn as_u64(self) -> u64 {
        self.0.get()
    }

    pub fn checked_add(self, rhs: u64) -> Option<Self> {
        self.0.get().checked_add(rhs).map(|v| Self::new(v).unwrap())
    }

    pub fn checked_sub(self, rhs: u64) -> Option<Self> {
        self.0.get().checked_sub(rhs).and_then(Self::new)
    }

    pub fn checked_delta(self, rhs: Self) -> Option<u64> {
        self.0.get().checked_sub(rhs.as_u64())
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl ops::Sub for Id {
    type Output = u64;

    fn sub(self, rhs: Self) -> Self::Output {
        self.as_u64().checked_sub(rhs.as_u64()).unwrap()
    }
}

impl ops::Sub<u64> for Id {
    type Output = Self;

    fn sub(self, rhs: u64) -> Self::Output {
        self.checked_sub(rhs).unwrap()
    }
}

#[derive(Default)]
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
        let len = rd.read_u32_varint().context(Error::Io)?;
        let id_delta = rd.read_u32_varint().context(Error::Io)?;
        let timestamp_delta = rd.read_u64_varint().context(Error::Io)?;
        let headers = Headers::read(rd)?;
        let key = read_opt_bstring(rd)?;
        let value = read_opt_bstring(rd)?;

        let id = next_id.checked_add(id_delta as u64).ok_or_else(||
            Error::BadMessage("stored message id overflows u64".into()).into_error())?;
        let timestamp = next_timestamp.checked_add_millis(timestamp_delta).ok_or_else(||
            Error::BadMessage("stored message timestamp overflows u64".into()).into_error())?;

        let r = Self {
            id,
            timestamp,
            headers,
            key,
            value,
        };

        // TODO should be already able to tell how much bytes was actually read from rd.
        if r.writer(next_id, next_timestamp).encoded_len() != cast::usize(len) {
            return Err(Error::BadMessage("message's stored len and actual len differ"
                .into()).into());
        }

        Ok(r)
    }

    pub fn writer(&self, next_id: Id, next_timestamp: Timestamp) -> MessageWriter {
        assert!(self.id.checked_delta(next_id)
            .filter(|&v| v <= u32::max_value() as u64)
            .is_some());
        assert!(self.timestamp >= next_timestamp);
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

    pub fn timestamp_delta(&self) -> u64 {
        self.msg.timestamp.duration_since(self.next_timestamp).unwrap()
            .as_millis_u64().unwrap()
    }

    pub fn encoded_len(&self) -> usize {
        let l = varint::encoded_len(self.id_delta() as u64) as usize +
            varint::encoded_len(self.timestamp_delta()) as usize +
            self.msg.headers.encoded_len() +
            encoded_len_opt_bstring(self.msg.key.as_ref()) +
            encoded_len_opt_bstring(self.msg.value.as_ref());
        varint::encoded_len(l as u64) as usize + l
    }

    pub fn write(&self, wr: &mut impl Write) -> Result<()> {
        wr.write_u32_varint(cast::u32(self.encoded_len()).unwrap()).context(Error::Io)?;
        wr.write_u32_varint(self.id_delta()).context(Error::Io)?;
        wr.write_u64_varint(self.timestamp_delta()).context(Error::Io)?;
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
        let count = cast::usize(rd.read_u32_varint().context(Error::Io)?);
        let mut vec = Vec::with_capacity(count);
        for _ in 0..count {
            vec.push(Header::read(rd)?);
        }
        Ok(Self {
            vec,
        })
    }

    pub fn write(&self, wr: &mut impl Write) -> Result<()> {
        wr.write_u32_varint(self.vec.len() as u32).context(Error::Io)?;
        for h in &self.vec {
            h.write(wr)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Header {
    pub name: String,
    pub value: Vec<u8>,
}

impl Header {
    pub fn encoded_len(&self) -> usize {
        encoded_len_bstring(self.name.as_bytes()) +
            encoded_len_bstring(self.value.as_ref())
    }

    pub fn read(rd: &mut impl Read) -> Result<Self> {
        let name = read_opt_string(rd)?.ok_or_else(||
            Error::BadMessage("stored message header name is null".into()).into_error())?;
        let value = read_opt_bstring(rd)?.ok_or_else(||
            Error::BadMessage("stored message header value is null".into()).into_error())?;
        Ok(Self {
            name,
            value,
        })
    }

    pub fn write(&self, wr: &mut impl Write) -> Result<()> {
        write_bstring(wr, self.name.as_bytes())?;
        write_bstring(wr, &self.value)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    mod message_id {
        use super::*;

        #[test]
        fn checked_delta() {
            assert_eq!(Id::min_value().checked_delta(Id::min_value()), Some(0));
            assert_eq!(Id::new(2).unwrap().checked_delta(Id::new(1).unwrap()), Some(1));
            assert_eq!(Id::min_value().checked_delta(Id::new(2).unwrap()), None);
            assert_eq!(Id::min_value().checked_delta(Id::max_value()), None);
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
                    id: Id::new(12345).unwrap(),
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
                    (Id::new(12345).unwrap(), now),
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