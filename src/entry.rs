pub(in crate) mod format;

use byteorder::{BigEndian, ReadBytesExt};
use std::borrow::Cow;
use std::io::SeekFrom;
use std::io::prelude::*;

use crate::error::*;
use crate::file::FileRead;
use crate::bytes::*;
use crate::message::{Id, Message, MessageBuilder, Timestamp};

#[derive(Clone, Debug, Eq, Fail, PartialEq)]
pub enum Error {
    #[fail(display = "{}", _0)]
    BadBody(BadBody),

    #[fail(display = "{}", _0)]
    BadFraming(Cow<'static, str>),

    #[fail(display = "{}", _0)]
    BadHeader(Cow<'static, str>),

    #[fail(display = "{}", _0)]
    BadMessages(BadMessages),

    #[fail(display = "{}", _0)]
    BadVersion(Cow<'static, str>),

    #[fail(display = "IO error")]
    Io,
}

impl Error {
    pub fn into_error(self) -> crate::error::Error {
        self.into()
    }
}

#[derive(Clone, Debug, Eq, Fail, PartialEq)]
pub enum BadBody {
    #[fail(display = "entry body CRC check failed")]
    BadCrc,

    #[fail(display = "found garbage after message sequence")]
    TrailingGarbage,
}

#[derive(Clone, Debug, Eq, Fail, PartialEq)]
pub enum BadMessages {
    #[fail(display = "message IDs must have no gaps")]
    DenseRequired,

    #[fail(display = "all message timestamps must be equal to the first_timestamp")]
    SingleTimestampRequired,

    #[fail(display = "last message timestamp differs from the one in the entry header")]
    FirstTimestampMismatch,

    #[fail(display = "last message timestamp differs from the one in the entry header")]
    LastTimestampMismatch,

    #[fail(display = "message ID overflow")]
    IdOverflow,
}

pub struct ValidBody {
    pub dense: bool,

    /// Effectively this requires all `timestamp_delta`s are zero.
    pub single_timestamp: bool,
}

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

        let header_crc = HEADER_CRC.get(buf);
        let actual_header_crc = crc(&buf.as_slice()[format::HEADER_CRC_RANGE]);
        if header_crc != actual_header_crc {
            return Err(Error::BadHeader("header CRC check failed".into()).into());
        }

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
        let end_id = start_id.checked_add(cast::u64(end_id_delta)).ok_or_else(||
            Error::BadHeader("end_id overflows u64".into()).into_error())?;

        let first_timestamp = FIRST_TIMESTAMP.get(buf);
        let last_timestamp = LAST_TIMESTAMP.get(buf);
        if last_timestamp < first_timestamp {
            return Err(Error::BadHeader("last_timestamp is less than first_timestamp"
                .into()).into());
        }

        let flags = FLAGS.get(buf);
        let term = TERM.get(buf);
        let body_crc = BODY_CRC.get(buf);

        let message_count = MESSAGE_COUNT.get(buf);
        let id_count = cast::u64(end_id_delta) + 1;
        if cast::u64(message_count) > id_count  {
            return Err(Error::BadHeader(format!(
                "(start_id, end_id) range is inconsistent with message_count: \
                {} ID(s) in ({}, {}) range while message_count is {}",
                id_count, start_id, end_id, message_count)
                .into()).into());
        }

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
        }))
    }

//    pub fn complete_read(&self, rd: &mut impl FileRead, buf: &mut BytesMut) -> Result<()> {
//        assert_eq!(buf.len(), format::FRAME_PROLOG_LEN);
//        buf.set_len(self.frame_len);
//        rd.read_exact(&mut buf[format::FRAME_PROLOG_LEN..]).context(Error::Io)?;
//        Ok(())
//    }

    pub fn iter<T: Buf>(&self, buf: T) -> BufEntryIter<Cursor<T>> {
        assert!(buf.len() >= self.frame_len);
        let mut rd = Cursor::new(buf);
        rd.set_position(format::MESSAGES_START);
        BufEntryIter {
            next_id: self.start_id,
            next_timestamp: self.first_timestamp,
            left: self.message_count,
            rd,
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
        let delta = self.last_timestamp - self.first_timestamp;
        self.first_timestamp = timestamp;
        self.last_timestamp = timestamp + delta;

        format::FIRST_TIMESTAMP.set(&mut *buf, self.first_timestamp);
        format::LAST_TIMESTAMP.set(&mut *buf, self.last_timestamp);
    }

    pub fn validate_body(&self, buf: &impl Buf, options: ValidBody) -> Result<()> {
        assert!(buf.len() >= self.frame_len, "invalid buf");

        if self.body_crc != crc(&buf.as_slice()[format::BODY_CRC_START..self.frame_len]) {
            return Err(BadBody::BadCrc.into());
        }

        if options.dense && cast::u64(self.message_count) !=
                cast::u64(self.end_id_delta) + 1 {
            return Err(BadMessages::DenseRequired.into());
        }

        let buf = &buf.as_slice()[..self.frame_len];
        let mut rd = Cursor::new(buf);
        rd.set_position(format::MESSAGES_START);
        let mut next_id = self.start_id;
        let mut next_timestamp = self.first_timestamp;
        for i in 0..self.message_count {
            // TODO implement more efficient reading of message specifically for validation.
            let msg = Message::read(&mut rd, next_id, next_timestamp)
                .more_context("reading message for validation")?;
            if i == 0 && msg.timestamp != self.first_timestamp {
                return Err(BadMessages::FirstTimestampMismatch.into());
            }
            if options.dense && msg.id - next_id != 0 {
                return Err(BadMessages::DenseRequired.into());
            }
            if options.single_timestamp && msg.timestamp != self.first_timestamp {
                return Err(BadMessages::SingleTimestampRequired.into());
            }
            next_id += 1;
            next_timestamp = msg.timestamp;
        }
        if next_timestamp != self.last_timestamp {
            return Err(BadMessages::LastTimestampMismatch.into());
        }
        if rd.available() > 0 {
            return Err(BadBody::TrailingGarbage.into())
        }
        Ok(())
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
                        None => return Some(Err(BadMessages::IdOverflow.into())),
                    }
                }
                Ok(msg)
            }
            Err(e) => Err(e),
        })
    }
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

    pub fn messages(&mut self, vec: &mut Vec<MessageBuilder>) -> &mut Self {
        for m in vec.drain(..) {
            self.message(m);
        }

        self
    }

    fn id_delta(id_range: (Id, Id)) -> u32 {
        cast::u32(id_range.1 - id_range.0).unwrap()
    }

    pub fn build(mut self) -> (BufEntry, BytesMut) {
        let id_range = self.get_id_range().expect("can't build dense entry without messages");
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

        let header_crc = Self::set_header_crc(wr.get_mut());

        let body_crc = crc(&wr.get_ref()[format::BODY_CRC_START..]);
        format::BODY_CRC.set(wr.get_mut(), body_crc);

        (header_crc, body_crc)
    }

    fn set_header_crc(buf: &mut [u8]) -> u32 {
        let header_crc = crc(&buf[format::HEADER_CRC_RANGE]);
        format::HEADER_CRC.set(buf, header_crc);
        header_crc
    }
}

impl From<Vec<MessageBuilder>> for BufEntryBuilder {
    fn from(mut v: Vec<MessageBuilder>) -> Self {
        let mut b = BufEntryBuilder::dense();
        b.messages(&mut v);
        b
    }
}

impl From<MessageBuilder> for BufEntryBuilder {
    fn from(v: MessageBuilder) -> Self {
        let mut b = BufEntryBuilder::dense();
        b.message(v);
        b
    }
}

fn crc(buf: &[u8]) -> u32 {
    crc::crc32::checksum_castagnoli(buf)
}

#[cfg(test)]
mod test {
    use super::*;
    use super::Error;
    use assert_matches::assert_matches;

    mod buf_entry {
        use super::*;

        mod validate_body {
            use super::*;

            fn val_err_opts(e: &BufEntry, buf: &impl Buf,
                    dense: bool, single_timestamp: bool) -> ErrorKind {
                e.validate_body(buf, ValidBody {
                    dense: dense,
                    single_timestamp: single_timestamp
                })
                    .err().unwrap().kind().clone()
            }

            fn val_err(e: &BufEntry, buf: &impl Buf) -> ErrorKind {
                val_err_opts(e, buf, false, false)
            }

            #[test]
            fn bad_body_crc_field() {
                // Check corruption in the body_crc field itself.
                let (_, mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();
                buf[format::BODY_CRC.pos] = !buf[format::BODY_CRC.pos];
                let e = BufEntry::decode(&buf).unwrap().unwrap();

                assert_matches!(val_err(&e, &buf),
                        ErrorKind::Entry(Error::BadBody(BadBody::BadCrc)));
            }

            #[test]
            fn bad_body_crc_content() {
                // Check corruption is first and last bytes of the CRC'ed body range.
                for &i in &[format::BODY_CRC_START as isize, -1] {
                    let (e, mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();

                    let i = if i < 0 { buf.len() as isize + i } else { i } as usize;
                    buf[i] = !buf[i];

                    assert_matches!(val_err(&e, &buf),
                        ErrorKind::Entry(Error::BadBody(BadBody::BadCrc)));
                }
            }

            #[test]
            fn last_timestamp_mismatch() {
                let (_, mut buf) = BufEntryBuilder::from(
                    MessageBuilder {
                        timestamp: Some(Timestamp::epoch().checked_add_millis(1).unwrap()),
                        ..Default::default()
                    }
                ).build();

                format::LAST_TIMESTAMP.set(&mut buf, Timestamp::epoch().checked_add_millis(2).unwrap());
                BufEntryBuilder::set_header_crc(&mut buf);
                let e = BufEntry::decode(&buf).unwrap().unwrap();
                assert_matches!(val_err(&e, &buf),
                        ErrorKind::Entry(Error::BadMessages(BadMessages::LastTimestampMismatch)));
            }

            #[test]
            fn not_single_timestamp() {
                let (e, buf) = BufEntryBuilder::from(vec![
                    MessageBuilder {
                        timestamp: Some(Timestamp::epoch()),
                        ..Default::default()
                    },
                    MessageBuilder {
                        timestamp: Some(Timestamp::epoch().checked_add_millis(100).unwrap()),
                        ..Default::default()
                    },
                ]).build();

                assert_matches!(val_err_opts(&e, &buf, false, true),
                        ErrorKind::Entry(Error::BadMessages(BadMessages::SingleTimestampRequired)));
            }

            #[test]
            fn not_dense_inner() {
                // No gaps between message ids.
                let (e, buf) = BufEntryBuilder::from(vec![
                    MessageBuilder::default(),
                    MessageBuilder {
                        id: Id::new(100),
                        ..Default::default()
                    },
                ]).build();
                assert_matches!(val_err_opts(&e, &buf, true, false),
                    ErrorKind::Entry(Error::BadMessages(BadMessages::DenseRequired)));
            }

            #[test]
            fn not_dense_outer() {
                // No gaps after start_id and before end_id.
                let mut b = BufEntryBuilder::sparse(Id::new(50).unwrap(), Id::new(100).unwrap());
                b.message(MessageBuilder { id: Id::new(75), .. Default::default() });
                let (e, buf) = b.build();
                assert_matches!(val_err_opts(&e, &buf, true, false),
                    ErrorKind::Entry(Error::BadMessages(BadMessages::DenseRequired)));
            }
        }
    }
}