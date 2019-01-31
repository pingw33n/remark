use std::io::prelude::*;

use super::*;
use super::Error;
use crate::util::DurationExt;
use crate::util::varint::{self, ReadExt, WriteExt};

#[derive(Default)]
pub struct MessageBuilder {
    pub id: Option<u64>,
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

pub struct Message {
    pub id: u64,
    pub timestamp: Timestamp,
    pub headers: Headers,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}

impl Message {
    pub fn read(rd: &mut impl Read, base_id: u64, base_timestamp: Timestamp) -> Result<Self> {
        let _len = rd.read_u32_varint().context(Error::Io)?;
        let id_delta = rd.read_u32_varint().context(Error::Io)?;
        let timestamp_delta = rd.read_u64_varint().context(Error::Io)?;
        let headers = Headers::read(rd)?;
        let key = read_opt_bstring(rd)?;
        let value = read_opt_bstring(rd)?;

        let id = base_id.checked_add(id_delta as u64).ok_or_else(||
            Error::BadMessage("stored message id_delta overflows u64".into()).into_error())?;
        let timestamp = base_timestamp.checked_add_millis(timestamp_delta).ok_or_else(||
            Error::BadMessage("stored message timestamp_delta overflows u64".into()).into_error())?;

        Ok(Self {
            id,
            timestamp,
            headers,
            key,
            value,
        })
    }

    pub fn writer(&self, base_id: u64, base_timestamp: Timestamp) -> MessageWriter {
        assert!(base_id <= self.id && self.id - base_id <= u32::max_value() as u64);
        assert!(base_timestamp <= self.timestamp);
        MessageWriter {
            msg: self,
            base_id,
            base_timestamp,
        }
    }
}

pub struct MessageWriter<'a> {
    msg: &'a Message,
    base_id: u64,
    base_timestamp: Timestamp,
}

impl MessageWriter<'_> {
    pub fn id_delta(&self) -> u32 {
        cast::u32(self.msg.id - self.base_id).unwrap()
    }

    pub fn timestamp_delta(&self) -> u64 {
        self.msg.timestamp.duration_since(self.base_timestamp).unwrap()
            .as_millis_u64().unwrap()
    }

    pub fn encoded_len(&self) -> usize {
        varint::encoded_len(self.id_delta() as u64) as usize +
            varint::encoded_len(self.timestamp_delta()) as usize +
            self.msg.headers.encoded_len() +
            encoded_len_opt_bstring(self.msg.key.as_ref()) +
            encoded_len_opt_bstring(self.msg.value.as_ref())
    }

    pub fn write(&self, wr: &mut impl Write) -> Result<()> {
        wr.write_u32_varint(self.id_delta()).context(Error::Io)?;
        wr.write_u64_varint(self.timestamp_delta()).context(Error::Io)?;
        self.msg.headers.write(wr)?;
        write_opt_bstring(wr, self.msg.key.as_ref())?;
        write_opt_bstring(wr, self.msg.value.as_ref())?;
        Ok(())
    }
}

#[derive(Default)]
pub struct Headers {
    pub vec: Vec<Header>,
}

impl Headers {
    pub fn encoded_len(&self) -> usize {
        self.vec.iter().map(|h| h.encoded_len()).sum()
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