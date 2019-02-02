pub(in crate) mod format;

use byteorder::{BigEndian, ReadBytesExt};
use std::borrow::Cow;
use std::io::SeekFrom;
use std::io::prelude::*;
use std::ops::Range;

use crate::error::*;
use crate::file::FileRead;
use crate::bytes::*;
use crate::message::{Id, Message, MessageBuilder, Timestamp};

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
    start_id: Id,
    end_id_delta: u32,
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

        let start_id = START_ID.get(buf).ok_or_else(||
            Error::BadHeader("invalid start_id (must not be 0)".into()).into_error())?;
        let end_id_delta = END_ID_DELTA.get(buf);
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
            start_id,
            end_id_delta,
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
            next_id: self.start_id,
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

    pub fn start_id(&self) -> Id {
        self.start_id
    }

    pub fn end_id(&self) -> Id {
        self.start_id.checked_add(cast::u64(self.end_id_delta)).unwrap()
    }

    pub fn set_start_id(&mut self, buf: &mut BytesMut, start_id: Id) {
        self.start_id = start_id;
        format::START_ID.set(&mut *buf, Some(start_id))
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
                        None => return Some(Err(crate::message::Error::IdOverflow.into())),
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
    start_id: Option<Id>,
    end_id: Option<Id>,
    next_id: Id,
    first_timestamp: Timestamp,
    last_timestamp: Timestamp,
    flags: u16,
    term: u64,
    message_count: u32,
}

impl BufEntryBuilder {
    pub fn dense() -> Self {
        Self::new(None, None)
    }

    pub fn sparse(start_id: Id, end_id: Id) -> Self {
        Self::new(Some(start_id), Some(end_id))
    }

    fn new(start_id: Option<Id>, end_id: Option<Id>) -> Self {
        assert!(start_id.is_none() || end_id.is_none() ||
            start_id.unwrap() <= end_id.unwrap());
        let next_id = start_id.unwrap_or(Id::min_value());
        Self {
            buf: BytesMut::new(),
            start_id,
            end_id,
            next_id,
            first_timestamp: Timestamp::epoch(),
            last_timestamp: Timestamp::epoch(),
            flags: 0,
            term: 0,
            message_count: 0,
        }
    }

    pub fn get_encoded_len(&self) -> usize {
        self.buf.len()
    }

    pub fn get_id_range(&self) -> Option<(Id, Id)> {
        if let Some(start_id) = self.start_id {
            let end_id = self.end_id
                .unwrap_or_else(|| Self::end_id_from_next(self.next_id).unwrap());
            Some((start_id, end_id))
        } else {
            None
        }
    }

    pub fn get_timestamp_range(&self) -> Option<(Timestamp, Timestamp)> {
        if self.message_count > 0 {
            Some((self.first_timestamp, self.last_timestamp))
        } else {
            None
        }
    }

    pub fn get_flags(&self) -> u16 {
        self.flags
    }

    pub fn get_term(&self) -> u64 {
        self.term
    }

    pub fn get_message_count(&self) -> u32 {
        self.message_count
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
            assert!(self.end_id.is_none() || msg_id <= self.end_id.unwrap());
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
            if self.start_id.is_none() {
                self.start_id = Some(msg.id);
            }
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

    fn id_delta(id_range: (Id, Id)) -> u32 {
        cast::u32(id_range.1 - id_range.0).unwrap()
    }

    pub fn build(mut self) -> (BufEntry, BytesMut) {
        let id_range = self.get_id_range().expect("can't build dense entry without messages");
        dbg!(id_range);
        let (header_crc, body_crc) = self.write_prolog(id_range);
        let frame_len = self.get_encoded_len();
        (BufEntry {
            frame_len,
            header_crc,
            start_id: id_range.0,
            end_id_delta: Self::id_delta(id_range),
            first_timestamp: self.first_timestamp,
            last_timestamp: self.last_timestamp,
            flags: self.flags,
            term: self.term,
            body_crc,
            message_count: self.message_count,
            body: format::FRAME_PROLOG_LEN..frame_len,
        }, self.buf)
    }

    fn end_id_from_next(next_id: Id) -> Option<Id> {
        next_id.checked_sub(1)
    }

    /// Returns (header_crc, body_crc).
    fn write_prolog(&mut self, id_range: (Id, Id)) -> (u32, u32) {
        use format::*;

        self.buf.ensure_len(format::FRAME_PROLOG_LEN);

        let ref mut wr = Cursor::new(&mut self.buf);

        let len = cast::u32(wr.get_ref().len()).unwrap();
        FRAME_LEN.write(wr, len).unwrap();

        // skip header crc
        wr.set_position(format::HEADER_CRC.next);

        VERSION.write(wr, CURRENT_VERSION).unwrap();
        START_ID.write(wr, Some(id_range.0)).unwrap();
        END_ID_DELTA.write(wr, Self::id_delta(id_range)).unwrap();
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

        (header_crc, body_crc)
    }
}
