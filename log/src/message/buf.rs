use super::*;
use super::ErrorId;
use rcommon::io::{BoundedRead, BoundedReader};
use rcommon::varint::ReadExt;

/// Message backed by a flat buffer. Stores only scalar fields of message (like `id` and `timestamp`)
/// and provides indirect references for non-scalar fields (headers, `key`, `value`).
#[derive(Clone, Debug)]
pub struct BufMessage {
    id: Id,
    timestamp: Timestamp,
    header_count: usize,
    headers_pos: usize,
    key: Option<Range<usize>>,
    value: Option<Range<usize>>,
}

impl BufMessage {
    /// Reads message from `rd` appending non-scalar fields to `buf`. Later this buffer can be
    /// used to retrieve these fields using the provided methods on this struct.
    /// Note the `buf` is never cleared and it's up to the caller to clear the buffer if data
    /// for previously read messages is not needed.
    /// Contents of the `buf` is opaque and non-versioned, meaning it can't be used across different
    /// versions of `BufMessage`.
    pub fn read(rd: &mut impl Read, buf: &mut impl GrowableBuf,
            next_id: Id, next_timestamp: Timestamp) -> Result<Self> {
        let ref mut rd = BoundedReader::new(rd, u64::max_value());

        let len = rd.read_u32_varint().wrap_err_id(ErrorId::Io)?;
        if len < 4 {
            return Err(Error::without_details(ErrorId::LenMismatch));
        }
        rd.set_len(len as u64);

        let id_delta = rd.read_u32_varint().wrap_err_id(ErrorId::Io)?;
        let timestamp_delta = rd.read_i64_varint().wrap_err_id(ErrorId::Io)?;

        let header_count = rd.read_u32_varint().wrap_err_id(ErrorId::Io)? as usize;
        let headers_pos = buf.len();
        for _ in 0..header_count {
            read_opt_bstring_item(rd, buf)?
                .ok_or_else(|| Error::without_details(ErrorId::HeaderNameIsNull))?;
            read_opt_bstring_item(rd, buf)?
                .ok_or_else(|| Error::without_details(ErrorId::HeaderValueIsNull))?;
        }

        let key = read_opt_bstring_single(rd, buf)?;
        let value = read_opt_bstring_single(rd, buf)?;

        let id = next_id.checked_add(id_delta as u64).ok_or_else(||
            Error::without_details(ErrorId::IdOverflow))?;
        let timestamp = next_timestamp.checked_add_millis(timestamp_delta).ok_or_else(||
            Error::without_details(ErrorId::TimestampOverflow))?;

        if !rd.is_eof().unwrap() {
            return Err(Error::without_details(ErrorId::LenMismatch));
        }

        Ok(Self {
            id,
            timestamp,
            header_count,
            headers_pos,
            key,
            value,
        })
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    pub fn header_count(&self) -> usize {
        self.header_count
    }

    pub fn headers<'a, B: 'a + Buf>(&self, buf: &'a B) -> BufHeaders<'a, B> {
        BufHeaders {
            buf,
            left: self.header_count,
            pos: self.headers_pos,
        }
    }

    pub fn key<'a>(&self, buf: &'a (impl 'a + Buf)) -> Option<&'a [u8]> {
        self.key.clone().map(|r| &buf.as_slice()[r])
    }

    pub fn value<'a>(&self, buf: &'a (impl 'a + Buf)) -> Option<&'a [u8]> {
        self.value.clone().map(|r| &buf.as_slice()[r])
    }

    pub fn to_message(&self, buf: &impl Buf) -> Message {
        Message {
            id: self.id,
            timestamp: self.timestamp,
            headers: Headers {
                vec: self.headers(buf)
                    .map(|(n, v)| Header { name: n.into(), value: v.into() })
                    .collect(),
            },
            key: self.key(buf).map(|v| v.into()),
            value: self.value(buf).map(|v| v.into()),
        }
    }
}

pub struct BufHeaders<'a, B: 'a> {
    buf: &'a B,
    left: usize,
    pos: usize,
}

impl<'a, B: 'a + Buf> Iterator for BufHeaders<'a, B> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.left > 0 {
            self.left -= 1;
            let name = get_opt_bstring_item(self.buf, &mut self.pos).unwrap();
            let value = get_opt_bstring_item(self.buf, &mut self.pos).unwrap();
            Some((name, value))
        } else {
            None
        }
    }
}

fn get_opt_bstring_item<'a>(buf: &'a (impl 'a + Buf), pos: &mut usize) -> Option<&'a [u8]> {
    let len = buf.get_u32::<BigEndian>(*pos);
    *pos += 4;
    if len == 0 {
        None
    } else {
        let start = *pos;
        *pos += len as usize - 1;
        Some(&buf.as_slice()[start..*pos])
    }
}

fn read_opt_bstring_single(rd: &mut impl Read, buf: &mut impl GrowableBuf) -> Result<Option<Range<usize>>> {
    let len = rd.read_u32_varint().wrap_err_id(ErrorId::Io)?;
    if len == 0 {
        Ok(None)
    } else {
        let pos = buf.len();
        buf.grow(len as usize - 1);
        rd.read_exact(&mut buf.as_mut_slice()[pos..]).wrap_err_id(ErrorId::Io)?;
        Ok(Some(pos..buf.len()))
    }
}

fn read_opt_bstring_item(rd: &mut impl Read, buf: &mut impl GrowableBuf) -> Result<Option<usize>> {
    let len = rd.read_u32_varint().wrap_err_id(ErrorId::Io)?;

    let pos = buf.len();
    buf.grow(4 + (len as usize).checked_sub(1).unwrap_or(0));
    buf.set_u32::<BigEndian>(pos, len);

    if len == 0 {
        Ok(None)
    } else {
        rd.read_exact(&mut buf.as_mut_slice()[pos + 4..]).wrap_err_id(ErrorId::Io)?;
        Ok(Some(pos))
    }
}