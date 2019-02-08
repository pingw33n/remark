pub(in crate) mod format;

use byteorder::{BigEndian, ReadBytesExt};
use if_chain::if_chain;
use matches::matches;
use num_traits::cast::{FromPrimitive, ToPrimitive};
use std::io::prelude::*;
use std::mem;

pub use crate::util::compress::Codec;

use crate::error::*;
use crate::file::FileRead;
use crate::message::{Id, Message, MessageBuilder, Timestamp};
use crate::util::compress::*;
use rcommon::bytes::*;

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

    #[fail(display = "unknown compression codec")]
    BadCompression,

    #[fail(display = "message IDs must have no gaps")]
    DenseRequired,

    #[fail(display = "all message timestamps must be zero")]
    WithoutTimestampRequired,

    #[fail(display = "IO error")]
    Io,
}

#[derive(Clone, Copy, Debug, Eq, Fail, PartialEq)]
pub enum BadBody {
    #[fail(display = "entry body checksum check failed")]
    BadChecksum,

    #[fail(display = "found less messages than declared in the entry header")]
    TooFewMessages,

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BufEntry {
    frame_len: usize,
    header_checksum: u32,
    start_id: Id,
    end_id_delta: u32,
    first_timestamp: Timestamp,
    max_timestamp: Timestamp,
    compression: Codec,
    term: u64,
    body_checksum: u32,
    message_count: u32,
}

// Some sane value.
const MAX_ENTRY_LEN: usize = 1024 * 1024 * 1024;

impl BufEntry {
    fn read_frame_buf(rd: &mut impl FileRead, buf: &mut impl GrowableBuf, prolog_only: bool) -> Result<bool> {
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

        buf.set_len_zeroed(read_len);
        format::FRAME_LEN.set(buf.as_mut_slice(), len as u32);
        rd.read_exact(&mut buf.as_mut_slice()[format::FRAME_LEN.next..read_len])
            .wrap_err_id(ErrorId::Io)?;

        Ok(true)
    }

    fn read0(rd: &mut impl FileRead, buf: &mut impl GrowableBuf,
            prolog_only: bool) -> Result<Option<Self>> {
        if Self::read_frame_buf(rd, buf, prolog_only)? {
            Self::decode(buf)
        } else {
            Ok(None)
        }
    }

    pub fn read_full(rd: &mut impl FileRead, buf: &mut impl GrowableBuf) -> Result<Option<Self>> {
        Self::read0(rd, buf, false)
    }

    pub fn read_prolog(rd: &mut impl FileRead, buf: &mut impl GrowableBuf) -> Result<Option<Self>> {
        Self::read0(rd, buf, true)
    }

    pub fn decode(buf: &impl Buf) -> Result<Option<Self>> {
        use format::*;

        if buf.len() < FRAME_PROLOG_FIXED_LEN {
            return Ok(None);
        }

        let frame_len = cast::usize(FRAME_LEN.get(buf));
        Self::check_frame_len(frame_len)?;

        let header_checksum = HEADER_CHECKSUM.get(buf);
        let actual_header_checksum = checksum(&buf.as_slice()[format::HEADER_CHECKSUM_RANGE]);
        if header_checksum != actual_header_checksum {
            return Err(Error::new(ErrorId::BadHeader, "header checksum check failed"));
        }

        let version = VERSION.get(buf);
        if version != CURRENT_VERSION {
            return Err(Error::new(ErrorId::BadVersion, format!("{}", version)));
        }
        if buf.len() < FRAME_PROLOG_LEN {
            return Ok(None);
        }

        let start_id = START_ID.get(buf);
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
        let compress_num = flags & 0b111;
        let compression = Codec::from_u16(compress_num).ok_or_else(||
            Error::new(ErrorId::BadCompression, compress_num.to_string()))?;

        let term = TERM.get(buf);
        let message_count = MESSAGE_COUNT.get(buf);

        let body_checksum = BODY_CHECKSUM.get(buf);

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
            header_checksum,
            start_id,
            end_id_delta,
            first_timestamp,
            max_timestamp,
            compression,
            term,
            body_checksum,
            message_count,
        }))
    }

    pub fn iter<T: Buf>(&self, buf: T) -> BufEntryIter<Cursor<T>> {
        if self.message_count > 0 && buf.len() == format::FRAME_PROLOG_LEN {
            panic!("`buf` seems to be incomplete, did you forget to call complete_read()?");
        }
        assert!(buf.len() >= self.frame_len);
        let mut rd = Cursor::new(buf);
        rd.set_position(format::MESSAGES_START);
        BufEntryIter {
            next_id: self.start_id,
            next_timestamp: self.first_timestamp,
            left: self.message_count,
            rd: ReadState::Init { rd, compression: self.compression },
            last_pos: None,
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

    pub fn header_checksum(&self) -> u32 {
        self.header_checksum
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

    pub fn compression(&self) -> Codec {
        self.compression
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn body_checksum(&self) -> u32 {
        self.body_checksum
    }

    pub fn message_count(&self) -> u32 {
        self.message_count
    }

    pub fn update(&mut self, buf: &mut impl BufMut, update: Update) {
        let mut dirty = false;
        let buf = buf.as_mut_slice();
        if_chain! {
            if let Some(start_id) = update.start_id;
            if start_id != self.start_id;
            then {
                assert!(start_id.checked_add(cast::u64(self.end_id_delta)).is_some());
                self.start_id = start_id;
                format::START_ID.set(buf, start_id);
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
            self.header_checksum = BufEntryBuilder::set_header_checksum(buf);
        }
    }

    pub fn validate_body(&self, buf: &impl Buf, options: ValidBody) -> Result<()> {
        assert!(buf.len() >= self.frame_len, "invalid buf");

        if self.body_checksum != format::checksum(&buf.as_slice()
                [format::MESSAGES_START..self.frame_len]) {
            return Err(Error::without_details(BadBody::BadChecksum));
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
        let mut prev_id = self.start_id;
        let mut max_timestamp = None;
        // TODO implement more efficient reading of message specifically for validation.
        let mut it = self.iter(buf);
        let mut i = 0;
        while i < self.message_count {
            let msg = if let Some(msg) = it.next() {
                msg.context("reading message for validation")?
            } else {
                return Err(Error::without_details(BadBody::TooFewMessages));
            };
            if i == 0 && msg.timestamp != self.first_timestamp {
                return Err(Error::without_details(BadMessages::FirstTimestampMismatch));
            }
            if options.dense && msg.id - prev_id != (i > 0) as u64 {
                return Err(Error::without_details(ErrorId::DenseRequired));
            }
            if options.without_timestamp && msg.timestamp != self.first_timestamp {
                return Err(Error::without_details(ErrorId::WithoutTimestampRequired));
            }
            prev_id = msg.id;
            if max_timestamp.is_none() || msg.timestamp > max_timestamp.unwrap() {
                max_timestamp = Some(msg.timestamp);
            }
            i += 1;
        }
        if max_timestamp.is_some() && max_timestamp.unwrap() != self.max_timestamp {
            return Err(Error::without_details(BadMessages::MaxTimestampMismatch));
        }
        if it.next_pos() < self.frame_len.to_u32().unwrap() {
            return Err(Error::without_details(BadBody::TrailingGarbage))
        }
        Ok(())
    }
}

enum ReadState<R: BufRead> {
    Empty,
    Init { rd: R, compression: Codec },
    Decoder(Decoder<R>),
}

pub struct BufEntryIter<R: BufRead> {
    next_id: Id,
    next_timestamp: Timestamp,
    left: u32,
    rd: ReadState<R>,
    last_pos: Option<u32>,
}

impl<T: Buf> BufEntryIter<Cursor<T>> {
    pub fn last_pos(&self) -> Option<u32> {
        self.last_pos
    }

    pub fn next_pos(&self) -> u32 {
        match &self.rd {
            ReadState::Init { rd, .. } => rd.position().to_u32().unwrap(),
            ReadState::Decoder(dec) => dec.get_ref().position().to_u32().unwrap(),
            ReadState::Empty => unreachable!(),
        }
    }
}

impl<T: Buf> Iterator for BufEntryIter<Cursor<T>> {
    type Item = Result<Message>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        if matches!(self.rd, ReadState::Init {..}) {
            if let ReadState::Init { rd, compression } = mem::replace(&mut self.rd, ReadState::Empty) {
                let rd = Decoder::new(rd, compression);
                if rd.is_err() {
                    return Some(rd.map(|_| unreachable!()).wrap_err_id(ErrorId::Io));
                }
                self.rd = ReadState::Decoder(rd.unwrap());
            } else {
                unreachable!()
            }
        }
        let rd = if let ReadState::Decoder(rd) = &mut self.rd {
            rd
        } else {
            unreachable!()
        };
        let last_pos = rd.get_ref().position().to_u32().unwrap();
        Some(match Message::read(rd, self.next_id, self.next_timestamp) {
            Ok(msg) => {
                self.left -= 1;
                if self.left > 0 {
                    match msg.id.checked_add(1) {
                        Some(next_id) => self.next_id = next_id,
                        None => return Some(Err(Error::without_details(BadMessages::IdOverflow))),
                    }
                }
                self.next_timestamp = msg.timestamp;
                self.last_pos = Some(last_pos);
                Ok(msg)
            }
            Err(e) => Err(e),
        })
    }
}

pub struct BufEntryBuilder {
    compression: Codec,
    encoder: Option<Encoder<Cursor<Vec<u8>>>>,
    start_id: Option<Id>,
    end_id: Option<Id>,
    next_id: Id,
    first_timestamp: Timestamp,
    max_timestamp: Timestamp,
    next_timestamp: Timestamp,
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
            compression: Codec::Uncompressed,
            encoder: None,
            start_id,
            end_id,
            next_id,
            first_timestamp: Timestamp::epoch(),
            max_timestamp: Timestamp::epoch(),
            next_timestamp: Timestamp::epoch(),
            term: 0,
            message_count: 0,
        }
    }

    pub fn compression(&mut self, codec: Codec) -> &mut Self {
        assert!(self.encoder.is_none(),
            "can't change compression codec after some messages have been already written");
        self.compression = codec;
        self
    }

    pub fn get_frame_len(&self) -> usize {
        self.encoder.as_ref().map(|enc| enc.get_ref().get_ref().len())
            .unwrap_or(format::FRAME_PROLOG_LEN)
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
        if msg.id.is_none() {
            msg.id = Some(self.next_id);
        } else {
            let msg_id = msg.id.unwrap();
            assert!(msg_id >= self.next_id);
            assert!(self.end_id.is_none() || msg_id <= self.end_id.unwrap());
            self.next_id = msg_id;
        }

        if msg.timestamp.is_none() {
            msg.timestamp = Some(self.next_timestamp);
        } else {
            assert!(msg.timestamp.unwrap().checked_delta(self.next_timestamp).is_some());
        }

        let msg = msg.build();

        let expected_next_id = if self.message_count == 0 {
            if self.start_id.is_none() {
                self.start_id = Some(msg.id);
            }
            self.first_timestamp = msg.timestamp;
            self.next_timestamp = msg.timestamp;
            self.start_id.unwrap()
        } else {
            self.next_id
        };
        self.next_id = self.next_id.checked_add(1).unwrap();

        let expected_next_timestamp = self.next_timestamp;
        self.next_timestamp = msg.timestamp;
        if msg.timestamp > self.max_timestamp {
            self.max_timestamp = msg.timestamp;
        }

        self.message_count = self.message_count.checked_add(1).unwrap();

        let msg_wr = msg.writer(expected_next_id, expected_next_timestamp);

        if self.encoder.is_none() {
            let estimated_len = format::FRAME_PROLOG_LEN + msg_wr.encoded_len();
            let mut buf = Vec::with_capacity(estimated_len);
            buf.ensure_len_zeroed(format::FRAME_PROLOG_LEN);

            let mut cursor = Cursor::new(buf);
            cursor.set_position(format::MESSAGES_START);
            self.encoder = Some(Encoder::new(cursor, self.compression)
                .expect("couldn't create encoder")); // TODO when can this happen?
        }

        msg_wr.write(self.encoder.as_mut().unwrap())
            .expect("error writing message into encoder"); // TODO when can this happen?

        self
    }

    pub fn messages(&mut self, iter: impl IntoIterator<Item=MessageBuilder>) -> &mut Self {
        for m in iter {
            self.message(m);
        }

        self
    }

    fn id_delta(id_range: (Id, Id)) -> u32 {
        cast::u32(id_range.1 - id_range.0).unwrap()
    }

    pub fn build(mut self) -> (BufEntry, Vec<u8>) {
        let id_range = self.get_id_range().expect("can't build dense entry without messages");
        let mut buf = self.encoder.take().map(|e| e.finish()
            .expect("error finishing encoder")
            .into_inner())
            .unwrap_or_else(|| Vec::new());
        let (header_checksum, body_checksum) = self.fill_prolog(&mut buf, id_range);
        let frame_len = buf.len();
        (BufEntry {
            frame_len,
            header_checksum,
            start_id: id_range.0,
            end_id_delta: Self::id_delta(id_range),
            first_timestamp: self.first_timestamp,
            max_timestamp: self.max_timestamp,
            compression: self.compression,
            term: self.term,
            body_checksum,
            message_count: self.message_count,
        }, buf)
    }

    fn end_id_from_next(next_id: Id) -> Option<Id> {
        next_id.checked_sub(1)
    }

    /// Returns (header_checksum, body_checksum).
    fn fill_prolog(&self, buf: &mut impl GrowableBuf, id_range: (Id, Id)) -> (u32, u32) {
        use format::*;

        buf.ensure_len_zeroed(format::FRAME_PROLOG_LEN);
        let buf = buf.as_mut_slice();

        let len = cast::u32(buf.len()).unwrap();
        FRAME_LEN.set(buf, len);

        VERSION.set(buf, CURRENT_VERSION);
        START_ID.set(buf, id_range.0);
        END_ID_DELTA.set(buf, Self::id_delta(id_range));
        FIRST_TIMESTAMP.set(buf, Some(self.first_timestamp));
        MAX_TIMESTAMP.set(buf, Some(self.max_timestamp));
        FLAGS.set(buf, self.compression as u16);
        TERM.set(buf, self.term);
        MESSAGE_COUNT.set(buf, self.message_count);

        let header_checksum = Self::set_header_checksum(buf);
        let body_checksum = Self::set_body_checksum(buf);

        (header_checksum, body_checksum)
    }

    fn set_header_checksum(buf: &mut [u8]) -> u32 {
        let r = format::checksum(&buf[format::HEADER_CHECKSUM_RANGE]);
        format::HEADER_CHECKSUM.set(buf, r);
        r
    }

    fn set_body_checksum(buf: &mut [u8]) -> u32 {
        let r = format::checksum(&buf[format::BODY_CHECKSUM_START..]);
        format::BODY_CHECKSUM.set(buf, r);
        r
    }
}

impl From<Vec<MessageBuilder>> for BufEntryBuilder {
    fn from(mut v: Vec<MessageBuilder>) -> Self {
        let mut b = BufEntryBuilder::dense();
        b.messages(v.drain(..));
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

#[cfg(test)]
mod test {
    use super::*;
    use super::ErrorId;

    mod buf_entry_builder {
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

        #[test]
        fn compression() {
            let msgs_b: Vec<_> = (0..100)
                .map(|i| MessageBuilder {
                    id: Some(Id::new(i)),
                    timestamp: Some(Timestamp::now()),
                    key: Some("key1".into()),
                    value: Some("value1".into()),
                    ..Default::default()
                })
                .collect();
            let msgs: Vec<_> = msgs_b.iter().map(|v| v.clone().build()).collect();

            let build = |codec| {
                let mut b = BufEntryBuilder::dense();
                b.compression(codec)
                    .messages(msgs_b.clone());
                b.build()
            };

            let all = &[Codec::Uncompressed, Codec::Lz4, Codec::Zstd];
            let encoded: Vec<_> = all.iter().map(|&c| (c, build(c))).collect();

            let uncompressed_len = (encoded[0].1).1.len();

            for (codec, (entry, buf)) in encoded {
                entry.validate_body(&buf, ValidBody { dense: false, without_timestamp: false }).unwrap();

                assert_eq!(entry.compression(), codec);
                if codec != Codec::Uncompressed {
                    assert!(buf.len() < uncompressed_len);
                }

                let actual: Vec<_> = entry.iter(&buf).map(|v| v.unwrap()).collect();
                assert_eq!(actual, msgs);
            }
        }

        #[test]
        fn expected_next_id_is_first_msg_id() {
            let mut b = BufEntryBuilder::dense();
            b.message(MessageBuilder { id: Some(Id::new(1000)), ..Default::default() });
            let (mut e, mut buf) = b.build();

            e.update(&mut buf, Update { start_id: Some(Id::new(0)), ..Default::default() });
            assert_eq!(e.iter(&buf).next().unwrap().unwrap().id, Id::new(0));
        }
    }

    mod buf_entry {
        use super::*;

        #[test]
        fn update() {
            let (mut e, mut buf) = BufEntryBuilder::sparse(Id::new(123), Id::new(223)).build();
            assert_eq!(e.start_id(), Id::new(123));
            assert_eq!(e.end_id(), Id::new(223));

            let now = Timestamp::now();

            e.update(&mut buf, Update {
                start_id: Some(Id::new(12345)),
                first_timestamp: Some(now),
            });

            let e2 = BufEntry::decode(&buf).unwrap().unwrap();

            assert_eq!(e, e2);

            assert_eq!(e.start_id(), Id::new(12345));
            assert_eq!(e.end_id(), Id::new(12345 + 100));
            assert_eq!(e.first_timestamp(), now);
            assert_eq!(e.max_timestamp(), now);
        }

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
            fn bad_body_checksum_field() {
                // Check corruption in the body_checksum field itself.
                let (_, mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();
                buf[format::BODY_CHECKSUM.pos] = !buf[format::BODY_CHECKSUM.pos];
                let e = BufEntry::decode(&buf).unwrap().unwrap();

                assert_eq!(val_err(&e, &buf), BadBody::BadChecksum.into());
            }

            #[test]
            fn bad_body_checksum_content() {
                // Check corruption is first and last bytes of the CHECKSUM'ed body range.
                for &i in &[format::BODY_CHECKSUM_START as isize, -1] {
                    let (e, mut buf) = BufEntryBuilder::from(MessageBuilder::default()).build();

                    let i = if i < 0 { buf.len() as isize + i } else { i } as usize;
                    buf[i] = !buf[i];

                    assert_eq!(val_err(&e, &buf), BadBody::BadChecksum.into());
                }
            }

            #[test]
            fn first_timestamp_mismatch() {
                let b = BufEntryBuilder::sparse(Id::new(10), Id::new(20));
                let (_, buf) = b.build();

                // Have to craft an entry with bad first_timestamp since BufEntryBuilder
                // always sets valid value for the field.
                let msg = MessageBuilder {
                    id: Some(Id::new(10)),
                    timestamp: Some(Timestamp::epoch().checked_add_millis(1).unwrap()),
                    ..Default::default()
                }.build();
                let mut c = Cursor::new(buf);
                c.set_position(format::MESSAGES_START);
                msg.writer(Id::new(10), Timestamp::epoch()).write(&mut c).unwrap();
                let ref mut buf = c.into_inner();
                BufEntryBuilder::set_body_checksum(buf);

                let frame_len = buf.len() as u32;
                format::FRAME_LEN.set(buf, frame_len);
                format::MESSAGE_COUNT.set(buf, 1);
                BufEntryBuilder::set_header_checksum(buf);

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
                BufEntryBuilder::set_header_checksum(&mut buf);

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
                let (mut e, mut buf) = BufEntryBuilder::sparse(Id::new(10),
                    Id::new(100)).build();
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
                        id: Some(Id::new(100)),
                        ..Default::default()
                    },
                ]).build();

                assert_eq!(val_err_opts(&e, &buf, true, false),
                    ErrorId::DenseRequired.into());
            }

            #[test]
            fn dense_violated_outer() {
                // No gaps after start_id and before end_id.
                let mut b = BufEntryBuilder::sparse(Id::new(50), Id::new(100));
                b.message(MessageBuilder { id: Some(Id::new(75)), .. Default::default() });
                let (e, buf) = b.build();

                assert_eq!(val_err_opts(&e, &buf, true, false),
                    ErrorId::DenseRequired.into());
            }
        }
    }
}