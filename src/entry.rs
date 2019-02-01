pub(in crate) mod format;
pub mod message;

use byteorder::{BigEndian, ReadBytesExt};
use std::borrow::Cow;
use std::io::SeekFrom;
use std::io::prelude::*;
use std::ops::Range;

use crate::error::*;
use crate::file::FileRead;
use crate::bytes::*;
use crate::Timestamp;
use crate::util::varint::{self, ReadExt, WriteExt};
use message::{Id, Message, MessageBuilder};

#[derive(Clone, Debug, Eq, Fail, PartialEq)]
pub enum Error {
    #[fail(display = "{}", _0)]
    BadFraming(Cow<'static, str>),

    #[fail(display = "{}", _0)]
    BadVersion(Cow<'static, str>),

    #[fail(display = "{}", _0)]
    BadHeader(Cow<'static, str>),

    #[fail(display = "{}", _0)]
    BadBody(Cow<'static, str>),

    #[fail(display = "{}", _0)]
    BadMessage(Cow<'static, str>),

    #[fail(display = "IO error")]
    Io,
}

impl Error {
    pub fn into_error(self) -> crate::error::Error {
        self.into()
    }
}

pub type BufRange = Range<usize>;

#[derive(Debug)]
pub struct BufEntry {
    frame_len: usize,
    header_crc: u32,
    first_id: Id,
    last_id_delta: u32,
    first_timestamp: Timestamp,
    last_timestamp: Timestamp,
    flags: u16,
    term: u64,
    body_crc: u32,
    message_count: u32,
    body: BufRange,
}

// Some sane value.
const MAX_ENTRY_LEN: usize = 1024 * 1024 * 1024;

impl BufEntry {
    fn read_frame_buf(rd: &mut impl FileRead, buf: &mut BytesMut, prolog_only: bool) -> Result<bool> {
        if rd.available() < format::FRAME_PROLOG_FIXED_LEN as u64 {
            return Ok(false);
        }

        let len = cast::usize(rd.read_u32::<BigEndian>().context(Error::Io)?);
        BufEntry::check_frame_len(len)?;

        if rd.available() < (len - format::FRAME_LEN.next) as u64 {
            return Ok(false);
        }

        let len = if prolog_only {
            format::FRAME_PROLOG_LEN
        } else {
            len
        };

        buf.set_len(len);
        format::FRAME_LEN.set(&mut buf[..], len as u32);
        rd.read_exact(&mut buf[format::FRAME_LEN.next..len])
            .context(Error::Io)?;

        Ok(true)
    }

    fn read0(rd: &mut impl FileRead, buf: &mut BytesMut, prolog_only: bool) -> Result<Option<Self>> {
        if Self::read_frame_buf(rd, buf, prolog_only)? {
            Self::decode(buf)
        } else {
            Ok(None)
        }
    }

    pub fn read_full(rd: &mut impl FileRead, buf: &mut BytesMut) -> Result<Option<Self>> {
        Self::read0(rd, buf, false)
    }

    pub fn read_prolog(rd: &mut impl FileRead, buf: &mut BytesMut) -> Result<Option<Self>> {
        Self::read0(rd, buf, true)
    }

    pub fn decode(buf: &impl Buf) -> Result<Option<Self>> {
        use format::*;

        if buf.len() < FRAME_PROLOG_FIXED_LEN {
            return Ok(None);
        }

        let frame_len = cast::usize(FRAME_LEN.get(buf));
        Self::check_frame_len(frame_len)?;
        if buf.len() < frame_len {
            return Ok(None);
        }

        let header_crc = HEADER_CRC.get(buf);

        let version = VERSION.get(buf);
        if version != CURRENT_VERSION {
            return Err(Error::BadVersion(format!(
                "unsupported entry version: {}", version).into()).into());
        }
        if buf.len() < FRAME_PROLOG_LEN {
            return Ok(None);
        }

        let first_id = FIRST_ID.get(buf).ok_or_else(||
            Error::BadHeader("invalid first_id (must not be 0)".into()).into_error())?;
        let last_id_delta = LAST_ID_DELTA.get(buf);
        let first_timestamp = FIRST_TIMESTAMP.get(buf);
        let last_timestamp = LAST_TIMESTAMP.get(buf);

        let flags = FLAGS.get(buf);
        let term = TERM.get(buf);
        let body_crc = BODY_CRC.get(buf);
        let message_count = MESSAGE_COUNT.get(buf);
        let messages = FRAME_PROLOG_LEN..frame_len;

        Ok(Some(Self {
            frame_len,
            header_crc,
            first_id,
            last_id_delta,
            first_timestamp,
            last_timestamp,
            flags,
            term,
            body_crc,
            message_count,
            body: messages,
        }))
    }

//    pub fn complete_read(&self, rd: &mut impl FileRead, buf: &mut BytesMut) -> Result<()> {
//        assert_eq!(buf.len(), format::FRAME_PROLOG_LEN);
//        buf.set_len(self.frame_len);
//        rd.read_exact(&mut buf[format::FRAME_PROLOG_LEN..]).context(Error::Io)?;
//        Ok(())
//    }

    pub fn encoded_frame_len(&self) -> usize {
        format::FRAME_PROLOG_FIXED_LEN + self.body.len()
    }

    pub fn iter(&self, buf: &Bytes) -> BufEntryIter<Cursor<Bytes>> {
        BufEntryIter {
            next_id: self.first_id,
            next_timestamp: self.first_timestamp,
            left: self.message_count,
            rd: Cursor::new(buf.clone())
        }
    }

    fn check_frame_len(len: usize) -> Result<()> {
        if len < format::FRAME_PROLOG_FIXED_LEN {
            return Err(Error::BadFraming(format!("frame len stored entry appears truncated ({} < {})",
                len, format::FRAME_PROLOG_FIXED_LEN).into()).into());
        }
        if len > MAX_ENTRY_LEN {
            return Err(Error::BadFraming(format!("stored entry len is too big ({} > {})",
                len, MAX_ENTRY_LEN).into()).into());
        }
        Ok(())
    }

    pub fn frame_len(&self) -> usize {
        self.frame_len
    }

    pub fn first_id(&self) -> Id {
        self.first_id
    }

    pub fn last_id(&self) -> Id {
        self.first_id.checked_add(cast::u64(self.last_id_delta)).unwrap()
    }

    pub fn set_first_id(&mut self, buf: &mut BytesMut, first_id: Id) {
        self.first_id = first_id;
        format::FIRST_ID.set(&mut *buf, Some(first_id))
    }

    pub fn first_timestamp(&self) -> Timestamp {
        self.first_timestamp
    }

    pub fn last_timestamp(&self) -> Timestamp {
        self.last_timestamp
    }

    pub fn set_timestamp(&mut self, buf: &mut BytesMut, timestamp: Timestamp) {
        let delta = self.last_timestamp.duration_since(self.first_timestamp).unwrap();
        self.first_timestamp = timestamp;
        self.last_timestamp = timestamp.checked_add(delta).unwrap();

        format::FIRST_TIMESTAMP.set(&mut *buf, self.first_timestamp);
        format::LAST_TIMESTAMP.set(&mut *buf, self.last_timestamp);
    }
}

pub struct BufEntryIter<R> {
    next_id: Id,
    next_timestamp: Timestamp,
    left: u32,
    rd: R,
}

impl<R: Read> Iterator for BufEntryIter<R> {
    type Item = Result<Message>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        Some(match Message::read(&mut self.rd, self.next_id, self.next_timestamp) {
            Ok(msg) => {
                self.left -= 1;
                if self.left > 0 {
                    match msg.id.checked_add(1) {
                        Some(next_id) => self.next_id = next_id,
                        None => return Some(Err(
                            Error::BadMessage("message ID overflowed u64".into()).into())),
                    }
                }
                Ok(msg)
            }
            Err(e) => Err(e),
        })
    }
}

pub struct BufHeader {
    pub name: BufRange,
    pub value: BufRange,
}

pub struct BufEntryBuilder {
    buf: BytesMut,
    first_id: Id,
    next_id: Id,
    first_timestamp: Timestamp,
    last_timestamp: Timestamp,
    flags: u16,
    term: u64,
    message_count: u32,
}

impl BufEntryBuilder {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::new(),
            first_id: Id::min_value(),
            next_id: Id::min_value(),
            first_timestamp: Timestamp::epoch(),
            last_timestamp: Timestamp::epoch(),
            flags: 0,
            term: 0,
            message_count: 0,
        }
    }

    pub fn get_last_id(&self) -> Option<Id> {
        Self::last_id_from_next(self.next_id)
    }

    pub fn get_timestamp_range(&self) -> Option<(Timestamp, Timestamp)> {
        if self.message_count > 0 {
            Some((self.first_timestamp, self.last_timestamp))
        } else {
            None
        }
    }

    pub fn get_message_count(&self) -> u32 {
        self.message_count
    }

    pub fn get_encoded_len(&self) -> usize {
        self.buf.len()
    }

    pub fn term(&mut self, term: u64) -> &mut Self {
        self.term = term;
        self
    }

    pub fn message(&mut self, mut msg: MessageBuilder) -> &mut Self {
        let expected_next_id = self.next_id;
        if msg.id.is_none() {
            msg.id = Some(self.next_id);
        } else {
            let msg_id = msg.id.unwrap();
            assert!(msg_id >= self.next_id);
            self.next_id = msg_id;
        }
        self.next_id = self.next_id.checked_add(1).unwrap();

        if msg.timestamp.is_none() {
            msg.timestamp = Some(self.last_timestamp);
        } else {
            assert!(msg.timestamp.unwrap() >= self.last_timestamp);
        }

        let msg = msg.build();

        let next_timestamp = if self.message_count == 0 {
            self.first_id = msg.id;
            self.first_timestamp = msg.timestamp;
            self.first_timestamp
        } else {
            self.last_timestamp
        };
        self.last_timestamp = msg.timestamp;
        self.message_count = self.message_count.checked_add(1).unwrap();

        let msg_wr = msg.writer(expected_next_id, next_timestamp);

        if self.buf.capacity() == 0 {
            let len = format::FRAME_PROLOG_LEN + msg_wr.encoded_len();
            self.buf.ensure_capacity(len);
            self.buf.ensure_len(format::FRAME_PROLOG_LEN);
        } else {
            self.buf.reserve(msg_wr.encoded_len());
        }

        let ref mut wr = Cursor::new(&mut self.buf);
        wr.seek(SeekFrom::End(0)).unwrap();
        msg_wr.write(wr).unwrap();

        self
    }

    fn last_id_from_next(next_id: Id) -> Option<Id> {
        next_id.checked_sub(1)
    }

    fn last_id_delta(first_id: Id, next_id: Id) -> u32 {
        if let Some(last_id) = Self::last_id_from_next(next_id) {
            cast::u32(last_id - first_id).unwrap()
        } else {
            0
        }
    }

    fn write_prolog(&mut self) {
        use format::*;

        self.buf.ensure_len(format::FRAME_PROLOG_LEN);

        let ref mut wr = Cursor::new(&mut self.buf);

        let len = cast::u32(wr.get_ref().len()).unwrap();
        FRAME_LEN.write(wr, len).unwrap();

        // skip header crc
        wr.set_position(format::HEADER_CRC.next);

        VERSION.write(wr, CURRENT_VERSION).unwrap();
        FIRST_ID.write(wr, Some(self.first_id)).unwrap();
        LAST_ID_DELTA.write(wr, Self::last_id_delta(self.first_id, self.next_id)).unwrap();
        FIRST_TIMESTAMP.write(wr, self.first_timestamp).unwrap();
        LAST_TIMESTAMP.write(wr, self.last_timestamp).unwrap();
        FLAGS.write(wr, self.flags).unwrap();
        TERM.write(wr, self.term).unwrap();

        // skip body crc
        wr.set_position(format::BODY_CRC.next);

        MESSAGE_COUNT.write(wr, self.message_count).unwrap();

        let header_crc = crc(&wr.get_ref()[format::HEADER_CRC_RANGE]);
        format::HEADER_CRC.set(wr.get_mut(), header_crc);

        let body_crc = crc(&wr.get_ref()[format::BODY_CRC_RANGE]);
        format::BODY_CRC.set(wr.get_mut(), body_crc);
    }

    pub fn build(mut self) -> (BufEntry, BytesMut) {
        self.write_prolog();
        let buf_entry = BufEntry::decode(&self.buf).unwrap().unwrap();
        (buf_entry, self.buf)
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
    let len = rd.read_u32_varint().context(Error::Io)?;
    if len == 0 {
        Ok(None)
    } else {
        let mut vec = Vec::with_capacity(cast::usize(len) - 1);
        vec.resize(vec.capacity(), 0);
        rd.read_exact(&mut vec).context(Error::Io)?;
        Ok(Some(vec))
    }
}

fn read_opt_string(rd: &mut impl Read) -> Result<Option<String>> {
    if let Some(s) = read_opt_bstring(rd)? {
        String::from_utf8(s).map(Some).map_err(|_|
            Error::BadMessage("malformed UTF-8 string".into()).into_error())
    } else {
        Ok(None)
    }
}

fn write_bstring(wr: &mut impl Write, buf: &[u8]) -> Result<()> {
    wr.write_u32_varint(cast::u32(buf.len()).unwrap().checked_add(1).unwrap())
        .context(Error::Io)?;
    wr.write_all(buf).context(Error::Io)?;
    Ok(())
}

fn write_opt_bstring<T: AsRef<[u8]>>(wr: &mut impl Write, buf: Option<T>) -> Result<()> {
    if let Some(buf) = buf {
        write_bstring(wr, buf.as_ref())
    } else {
        wr.write_u32_varint(0).context(Error::Io)?;
        Ok(())
    }
}

fn crc(buf: &[u8]) -> u32 {
    crc::crc32::checksum_castagnoli(buf)
}