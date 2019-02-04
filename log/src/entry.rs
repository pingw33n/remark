pub(in crate) mod format;

use byteorder::{BigEndian, ReadBytesExt};
use if_chain::if_chain;
use std::io::SeekFrom;
use std::io::prelude::*;

use crate::error::*;
use crate::file::FileRead;
use crate::bytes::*;
use crate::message::{Id, Message, MessageBuilder, Timestamp};

#[derive(Clone, Copy, Debug, Eq, Fail, PartialEq)]
pub enum ErrorId {
    #[fail(display = "{}", _0)]
    BadBody(BadBody),

    #[fail(display = "invalid entry framing")]
    BadFraming,

    #[fail(display = "invalid entry header")]
    BadHeader,

    #[fail(display = "{}", _0)]
    BadMessages(BadMessages),

    #[fail(display = "invalid entry version")]
    BadVersion,

    #[fail(display = "message IDs must have no gaps")]
    DenseRequired,

    #[fail(display = "all message timestamps must be zero")]
    WithoutTimestampRequired,

    #[fail(display = "IO error")]
    Io,
}

#[derive(Clone, Copy, Debug, Eq, Fail, PartialEq)]
pub enum BadBody {
    #[fail(display = "entry body CRC check failed")]
    BadCrc,

    #[fail(display = "found garbage after message sequence")]
    TrailingGarbage,
}

#[derive(Clone, Copy, Debug, Eq, Fail, PartialEq)]
pub enum BadMessages {
    #[fail(display = "first message timestamp differs from the one in the entry header")]
    FirstTimestampMismatch,

    #[fail(display = "max message timestamp differs from the one in the entry header")]
    MaxTimestampMismatch,

    #[fail(display = "message ID overflow")]
    IdOverflow,
}

#[derive(Clone, Debug, Default)]
pub struct ValidBody {
    pub dense: bool,

    /// Requires that all timestamps are zero: `first_timestamp`, `max_timestamp` and
    /// `timestamp_delta`s of all messages.
    pub without_timestamp: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Update {
    pub start_id: Option<Id>,
    pub first_timestamp: Option<Timestamp>,
}

#[derive(Debug)]
pub struct BufEntry {
    frame_len: usize,
    header_crc: u32,
    start_id: Id,
    end_id_delta: u32,
    first_timestamp: Timestamp,
    max_timestamp: Timestamp,
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

        let len = cast::usize(rd.read_u32::<BigEndian>().wrap_err_id(ErrorId::Io)?);
        BufEntry::check_frame_len(len)?;

        if rd.available() < (len - format::FRAME_LEN.next) as u64 {
            return Ok(false);
        }

        let read_len = if prolog_only {
            format::FRAME_PROLOG_LEN
        } else {
            len
        };

        buf.set_len(read_len);
        format::FRAME_LEN.set(&mut buf[..], len as u32);
        rd.read_exact(&mut buf[format::FRAME_LEN.next..read_len])
            .wrap_err_id(ErrorId::Io)?;

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
            return Err(Error::new(ErrorId::BadHeader, "header CRC check failed"));
        }

        let version = VERSION.get(buf);
        if version != CURRENT_VERSION {
            return Err(Error::new(ErrorId::BadVersion, format!("{}", version)));
        }
        if buf.len() < FRAME_PROLOG_LEN {
            return Ok(None);
        }

        let start_id = START_ID.get(buf).ok_or_else(||
            Error::new(ErrorId::BadHeader, "start_id must not be 0"))?;
        let end_id_delta = END_ID_DELTA.get(buf);
        let end_id = start_id.checked_add(cast::u64(end_id_delta)).ok_or_else(||
            Error::new(ErrorId::BadHeader, "end_id overflows u64"))?;

        let first_timestamp = FIRST_TIMESTAMP.get(buf).ok_or_else(||
            Error::new(ErrorId::BadHeader, "first_timestamp is out of range"))?;
        let max_timestamp = MAX_TIMESTAMP.get(buf).ok_or_else(||
            Error::new(ErrorId::BadHeader, "max_timestamp is out of range"))?;
        if max_timestamp < first_timestamp {
            return Err(Error::new(ErrorId::BadHeader, "max_timestamp is before first_timestamp"));
        }

        let flags = FLAGS.get(buf);
        let term = TERM.get(buf);
        let body_crc = BODY_CRC.get(buf);

        let message_count = MESSAGE_COUNT.get(buf);

        let id_count = cast::u64(end_id_delta) + 1;
        if cast::u64(message_count) > id_count  {
            return Err(Error::new(ErrorId::BadHeader, format!(
                "(start_id, end_id) range is inconsistent with message_count: \
                {} ID(s) in ({}, {}) range while message_count is {}",
                id_count, start_id, end_id, message_count)));
        }

        if message_count <= 1 && (first_timestamp != max_timestamp) {
            return Err(Error::new(ErrorId::BadHeader,
                "first_timestamp must be the same as max_timestamp when message_count <= 1"));
        }

        Ok(Some(Self {
            frame_len,
            header_crc,
            start_id,
            end_id_delta,
            first_timestamp,
            max_timestamp,
            flags,
            term,
            body_crc,
            message_count,
        }))
    }

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
            return Err(Error::new(ErrorId::BadFraming, format!(
                "frame len stored entry appears truncated ({} < {})",
                len, format::FRAME_PROLOG_FIXED_LEN)));
        }
        if len > MAX_ENTRY_LEN {
            return Err(Error::new(ErrorId::BadFraming, format!(
                "stored entry len is too big ({} > {})",
                len, MAX_ENTRY_LEN)));
        }
        Ok(())
    }

    pub fn frame_len(&self) -> usize {
        self.frame_len
    }

    pub fn header_crc(&self) -> u32 {
        self.header_crc
    }

    pub fn start_id(&self) -> Id {
        self.start_id
    }

    pub fn end_id(&self) -> Id {
        self.start_id.checked_add(cast::u64(self.end_id_delta)).unwrap()
    }

    pub fn first_timestamp(&self) -> Timestamp {
        self.first_timestamp
    }

    pub fn max_timestamp(&self) -> Timestamp {
        self.max_timestamp
    }

    pub fn flags(&self) -> u16 {
        self.flags
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn body_crc(&self) -> u32 {
        self.body_crc
    }

    pub fn message_count(&self) -> u32 {
        self.message_count
    }

    pub fn update(&mut self, buf: &mut BytesMut, update: Update) {
        let mut dirty = false;
        let buf = buf.as_mut_slice();
        if_chain! {
            if let Some(start_id) = update.start_id;
            if start_id != self.start_id;
            then {
                assert!(start_id.checked_add(cast::u64(self.end_id_delta)).is_some());
                self.start_id = start_id;
                format::START_ID.set(buf, Some(start_id));
                dirty = true;
            }
        }
        if_chain! {
            if let Some(first_timestamp) = update.first_timestamp;
            if first_timestamp != self.first_timestamp;
            then {
                let delta = first_timestamp - self.first_timestamp;
                let max_timestamp = self.max_timestamp + delta;

                self.first_timestamp = first_timestamp;
                format::FIRST_TIMESTAMP.set(buf, Some(first_timestamp));

                self.max_timestamp = max_timestamp;
                format::MAX_TIMESTAMP.set(buf, Some(max_timestamp));

                dirty = true;
            }
        }
        if dirty {
            BufEntryBuilder::set_header_crc(buf);
        }
    }

    pub fn validate_body(&self, buf: &impl Buf, options: ValidBody) -> Result<()> {
        assert!(buf.len() >= self.frame_len, "invalid buf");

        if self.body_crc != crc(&buf.as_slice()[format::BODY_CRC_START..self.frame_len]) {
            return Err(Error::without_details(BadBody::BadCrc));
        }

        if options.dense && cast::u64(self.message_count) !=
                cast::u64(self.end_id_delta) + 1 {
            return Err(Error::without_details(ErrorId::DenseRequired));
        }

        if options.without_timestamp && (self.first_timestamp != Timestamp::epoch() ||
                self.max_timestamp != Timestamp::epoch()) {
            return Err(Error::without_details(ErrorId::WithoutTimestampRequired));
        }

        let buf = &buf.as_slice()[..self.frame_len];
        let mut rd = Cursor::new(buf);
        rd.set_position(format::MESSAGES_START);
        let mut next_id = self.start_id;
        let mut next_timestamp = self.first_timestamp;
        let mut max_timestamp = None;
        for i in 0..self.message_count {
            // TODO implement more efficient reading of message specifically for validation.
            let msg = Message::read(&mut rd, next_id, next_timestamp)
                .context("reading message for validation")?;
            if i == 0 && msg.timestamp != self.first_timestamp {
                return Err(Error::without_details(BadMessages::FirstTimestampMismatch));
            }
            if options.dense && msg.id - next_id != 0 {
                return Err(Error::without_details(ErrorId::DenseRequired));
            }
            if options.without_timestamp && msg.timestamp != self.first_timestamp {
                return Err(Error::without_details(ErrorId::WithoutTimestampRequired));
            }
            if max_timestamp.is_none() || msg.timestamp > max_timestamp.unwrap() {
                max_timestamp = Some(msg.timestamp);
            }
            next_id += 1;
            next_timestamp = msg.timestamp;
        }
        if max_timestamp.is_some() && max_timestamp.unwrap() != self.max_timestamp {
            return Err(Error::without_details(BadMessages::MaxTimestampMismatch));
        }
        if rd.available() > 0 {
            return Err(Error::without_details(BadBody::TrailingGarbage))
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
                        None => return Some(Err(Error::without_details(BadMessages::IdOverflow))),
                    }
                }
                self.next_timestamp = msg.timestamp;
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
    max_timestamp: Timestamp,
    next_timestamp: Timestamp,
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
            max_timestamp: Timestamp::epoch(),
            next_timestamp: Timestamp::epoch(),
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
            Some((self.first_timestamp, self.max_timestamp))
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
            msg.timestamp = Some(self.next_timestamp);
        } else {
            assert!(msg.timestamp.unwrap().checked_delta(self.next_timestamp).is_some());
        }

        let msg = msg.build();

        if self.message_count == 0 {
            if self.start_id.is_none() {
                self.start_id = Some(msg.id);
            }
            self.first_timestamp = msg.timestamp;
            self.next_timestamp = msg.timestamp;
        }

        let expected_next_timestamp = self.next_timestamp;
        self.next_timestamp = msg.timestamp;
        if msg.timestamp > self.max_timestamp {
            self.max_timestamp = msg.timestamp;
        }

        self.message_count = self.message_count.checked_add(1).unwrap();

        let msg_wr = msg.writer(expected_next_id, expected_next_timestamp);

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
            max_timestamp: self.max_timestamp,
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
        FIRST_TIMESTAMP.write(wr, Some(self.first_timestamp)).unwrap();
        MAX_TIMESTAMP.write(wr, Some(self.max_timestamp)).unwrap();
        FLAGS.write(wr, self.flags).unwrap();
        TERM.write(wr, self.term).unwrap();

        // skip body crc
        wr.set_position(format::BODY_CRC.next);

        MESSAGE_COUNT.write(wr, self.message_count).unwrap();

        let header_crc = Self::set_header_crc(wr.get_mut());
        let body_crc = Self::set_body_crc(wr.get_mut());

        (header_crc, body_crc)
    }

    fn set_header_crc(buf: &mut [u8]) -> u32 {
        let r = crc(&buf[format::HEADER_CRC_RANGE]);
        format::HEADER_CRC.set(buf, r);
        r
    }

    fn set_body_crc(buf: &mut [u8]) -> u32 {
        let r = crc(&buf[format::BODY_CRC_START..]);
        format::BODY_CRC.set(buf, r);
        r
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
    use super::ErrorId;

    mod buf_entry_build {
        use super::*;

        #[test]
        fn non_monotonic_timestamps() {
            let ts = &[
                Timestamp::epoch(),
                Timestamp::epoch() + 1000,
                Timestamp::epoch() - 1000,
            ];
            let mut b = BufEntryBuilder::dense();
            b.message(MessageBuilder {
                timestamp: Some(ts[0]),
                .. Default::default()
            });
            b.message(MessageBuilder {
                timestamp: Some(ts[1]),
                .. Default::default()
            });
            b.message(MessageBuilder {
                timestamp: Some(ts[2]),
                .. Default::default()
            });
            let (e, buf) = b.build();
            assert_eq!(e.first_timestamp(), ts[0]);
            assert_eq!(e.max_timestamp(), ts[1]);

            let act_ts: Vec<_> = e.iter(&buf).map(|v| v.unwrap().timestamp).collect();
            assert_eq!(&act_ts, ts);
        }
    }

    mod buf_entry {
        use super::*;

        mod validate_body {
            use super::*;

            fn val_err_opts(e: &BufEntry, buf: &impl Buf,
                    dense: bool, without_timestamp: bool) -> crate::error::ErrorId {
                e.validate_body(buf, ValidBody {
                    dense,
                    without_timestamp,
                }).err().unwrap().id().clone()
            }

            fn val_err(e: &BufEntry, buf: &impl Buf) -> crate::error::ErrorId {
                val_err_opts(e, buf, false, false)
            }

            #[test]
            fn bad_body_crc_field() {
                // Check corruption in the body_crc field itself.
                let (_, mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();
                buf[format::BODY_CRC.pos] = !buf[format::BODY_CRC.pos];
                let e = BufEntry::decode(&buf).unwrap().unwrap();

                assert_eq!(val_err(&e, &buf), BadBody::BadCrc.into());
            }

            #[test]
            fn bad_body_crc_content() {
                // Check corruption is first and last bytes of the CRC'ed body range.
                for &i in &[format::BODY_CRC_START as isize, -1] {
                    let (e, mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();

                    let i = if i < 0 { buf.len() as isize + i } else { i } as usize;
                    buf[i] = !buf[i];

                    assert_eq!(val_err(&e, &buf), BadBody::BadCrc.into());
                }
            }

            #[test]
            fn first_timestamp_mismatch() {
                let b = BufEntryBuilder::sparse(Id::new(10).unwrap(), Id::new(20).unwrap());
                let (_, buf) = b.build();

                // Have to craft an entry with bad first_timestamp since BufEntryBuilder
                // always sets valid value for the field.
                let msg = MessageBuilder {
                    id: Id::new(10),
                    timestamp: Some(Timestamp::epoch().checked_add_millis(1).unwrap()),
                    ..Default::default()
                }.build();
                let mut c = Cursor::new(buf);
                c.set_position(format::MESSAGES_START);
                msg.writer(Id::new(10).unwrap(), Timestamp::epoch()).write(&mut c).unwrap();
                let ref mut buf = c.into_inner();

                let frame_len = buf.len() as u32;
                format::FRAME_LEN.set(buf, frame_len);
                BufEntryBuilder::set_header_crc(buf);
                format::MESSAGE_COUNT.set(buf, 1);
                BufEntryBuilder::set_body_crc(buf);

                let e = BufEntry::decode(&buf).unwrap().unwrap();
                assert_eq!(val_err(&e, &buf), BadMessages::FirstTimestampMismatch.into());
            }

            #[test]
            fn max_timestamp_mismatch() {
                let (_, mut buf) = BufEntryBuilder::from(vec![
                    MessageBuilder {
                        timestamp: Some(Timestamp::epoch().checked_add_millis(1).unwrap()),
                        ..Default::default()
                    },
                    MessageBuilder {
                        timestamp: Some(Timestamp::epoch().checked_add_millis(2).unwrap()),
                        ..Default::default()
                    },
                ]).build();

                let mismatching_timestamp = Timestamp::epoch().checked_add_millis(3).unwrap();
                format::MAX_TIMESTAMP.set(&mut buf, Some(mismatching_timestamp));
                BufEntryBuilder::set_header_crc(&mut buf);

                let e = BufEntry::decode(&buf).unwrap().unwrap();
                assert_eq!(val_err(&e, &buf), BadMessages::MaxTimestampMismatch.into());
            }

            #[test]
            fn without_timestamp_violated_in_messages() {
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

                assert_eq!(val_err_opts(&e, &buf, false, true),
                    ErrorId::WithoutTimestampRequired.into());
            }

            #[test]
            fn without_timestamp_violated_in_header() {
                let (mut e, mut buf) = BufEntryBuilder::sparse(Id::new(10).unwrap(),
                    Id::new(100).unwrap()).build();
                e.update(&mut buf, Update {
                    first_timestamp: Some(Timestamp::min_value()),
                    ..Default::default()
                });

                assert_eq!(val_err_opts(&e, &buf, false, true),
                    ErrorId::WithoutTimestampRequired.into());
            }

            #[test]
            fn dense_violated_inner() {
                // No gaps between message ids.
                let (e, buf) = BufEntryBuilder::from(vec![
                    MessageBuilder::default(),
                    MessageBuilder {
                        id: Id::new(100),
                        ..Default::default()
                    },
                ]).build();

                assert_eq!(val_err_opts(&e, &buf, true, false),
                    ErrorId::DenseRequired.into());
            }

            #[test]
            fn dense_violated_outer() {
                // No gaps after start_id and before end_id.
                let mut b = BufEntryBuilder::sparse(Id::new(50).unwrap(), Id::new(100).unwrap());
                b.message(MessageBuilder { id: Id::new(75), .. Default::default() });
                let (e, buf) = b.build();

                assert_eq!(val_err_opts(&e, &buf, true, false),
                    ErrorId::DenseRequired.into());
            }
        }
    }
}