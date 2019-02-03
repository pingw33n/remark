pub use byteorder::{BigEndian, ByteOrder, LittleEndian};
use std::io::{self, SeekFrom};
use std::io::prelude::*;
use std::sync::Arc;
use std::ops;
use std::cmp;

macro_rules! impl_get {
    ($g:ident, $r:ident: $ty:ty) => {
        fn $g<B: ByteOrder>(&self, i: usize) -> $ty {
            B::$r(&self.as_slice()[i..])
        }
    };
}

pub trait Buf: AsRef<[u8]>  {
    fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }

    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn get_u8(&self, i: usize) -> u8 {
        self.as_slice()[i]
    }

    fn get_i8(&self, i: usize) -> i8 {
        self.get_u8(i) as i8
    }

    impl_get!(get_u16, read_u16: u16);
    impl_get!(get_i16, read_i16: i16);
    impl_get!(get_u32, read_u32: u32);
    impl_get!(get_i32, read_i32: i32);
    impl_get!(get_u64, read_u64: u64);
    impl_get!(get_i64, read_i64: i64);
}

impl<T: AsRef<[u8]>> Buf for T {}


#[derive(Clone)]
pub struct Bytes {
    range: ops::Range<usize>,
    vec: Arc<Vec<u8>>,
}

impl Bytes {
    pub fn new() -> Self {
        Self {
            range: 0..0,
            vec: Arc::new(Vec::new()),
        }
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        &self.vec[self.range.start..self.range.end]
    }
}

impl ops::Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl Default for Bytes {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(v: Vec<u8>) -> Self {
        Self {
            range: 0..v.len(),
            vec: Arc::new(v),
        }
    }
}

impl From<BytesMut> for Bytes {
    fn from(v: BytesMut) -> Self {
        v.vec.into()
    }
}

#[derive(Default)]
pub struct BytesMut {
    vec: Vec<u8>,
}

macro_rules! impl_set {
    ($s:ident, $w:ident: $ty:ty) => {
        pub fn $s<B: ByteOrder>(&mut self, i: usize, v: $ty) {
            B::$w(&mut self.vec[i..], v);
        }
    };
}

impl BytesMut {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_len(len: usize) -> Self {
        let mut vec = Vec::with_capacity(len);
        vec.resize(len, 0);
        vec.into()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Vec::with_capacity(capacity).into()
    }

    pub fn capacity(&self) -> usize {
        self.vec.capacity()
    }

    pub fn reserve(&mut self, extra_capacity: usize) {
        self.vec.reserve(extra_capacity);
    }

    pub fn ensure_capacity(&mut self, min_capacity: usize) {
        if min_capacity > self.vec.capacity() {
            let extra = min_capacity - self.vec.capacity();
            self.vec.reserve(extra);
        }
    }

    pub fn len(&self) -> usize {
        self.vec.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vec.is_empty()
    }

    pub fn grow(&mut self, extra_len: usize) {
        let new_len = self.vec.len().checked_add(extra_len).unwrap();
        self.vec.resize(new_len, 0);
    }

    pub fn ensure_len(&mut self, min_len: usize) {
        if self.len() < min_len {
            self.vec.resize(min_len, 0);
        }
    }

    pub fn set_len(&mut self, len: usize) {
        self.vec.resize(len, 0);
    }

    pub fn clear(&mut self) {
        self.vec.clear();
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.vec
    }

    pub fn set_u8(&mut self, i: usize, v: u8) {
        self.as_mut_slice()[i] = v;
    }

    pub fn set_i8(&mut self, i: usize, v: i8) {
        self.set_u8(i, v as u8);
    }

    impl_set!(set_u16, write_u16: u16);
    impl_set!(set_i16, write_i16: i16);
    impl_set!(set_u32, write_u32: u32);
    impl_set!(set_i32, write_i32: i32);
    impl_set!(set_u64, write_u64: u64);
    impl_set!(set_i64, write_i64: i64);
}

impl AsRef<[u8]> for BytesMut {
    fn as_ref(&self) -> &[u8] {
        &self.vec
    }
}

impl AsMut<[u8]> for BytesMut {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl ops::Deref for BytesMut {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl ops::DerefMut for BytesMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl From<Vec<u8>> for BytesMut {
    fn from(vec: Vec<u8>) -> Self {
        Self {
            vec,
        }
    }
}

pub struct Cursor<T> {
    inner: T,
    pos: usize,
}

impl<T> Cursor<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            pos: 0,
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn set_position(&mut self, pos: usize) {
        self.pos = pos;
    }
}

impl<T: AsRef<[u8]>> Seek for Cursor<T> {
    fn seek(&mut self, style: SeekFrom) -> io::Result<u64> {
        fn err() -> io::Error {
            io::Error::new(io::ErrorKind::InvalidInput,
                "invalid seek to a negative or overflowing position")
        }

        let (base_pos, offset) = match style {
            SeekFrom::Start(n) => (0, cast::i64(n).map_err(|_| err())?),
            SeekFrom::End(n) => (self.inner.as_ref().len() as u64, n),
            SeekFrom::Current(n) => (self.pos as u64, n),
        };
        let new_pos = if offset >= 0 {
            base_pos.checked_add(offset as u64)
        } else {
            base_pos.checked_sub((offset.wrapping_neg()) as u64)
        };
        let new_pos = new_pos.map(|p| cast::usize(p) );
        match new_pos {
            Some(n) => {
                self.pos = n;
                Ok(self.pos as u64)
            }
            None => Err(err())
        }
    }
}

impl<T: AsRef<[u8]>> Cursor<T> {
    pub fn available(&self) -> usize {
        self.inner.as_ref().len().saturating_sub(self.pos)
    }
}

impl<T: AsRef<[u8]>> io::BufRead for Cursor<T> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        let amt = cmp::min(self.pos, self.inner.as_ref().len());
        Ok(&self.inner.as_ref()[(amt as usize)..])
    }

    fn consume(&mut self, amt: usize) {
        self.pos += amt;
    }
}

impl<T> Read for Cursor<T> where T: AsRef<[u8]> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = Read::read(&mut self.fill_buf()?, buf)?;
        self.pos += n;
        Ok(n)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let n = buf.len();
        Read::read_exact(&mut self.fill_buf()?, buf)?;
        self.pos += n;
        Ok(())
    }
}

fn bytes_mut_write(bytes: &mut BytesMut, mut buf: &[u8], pos: &mut usize) -> io::Result<usize> {
    let mut p = *pos;
    if p < bytes.len() {
        let len = cmp::min(bytes.len() - p, buf.len());
        let dst = &mut bytes[p..p + len];
        dst.copy_from_slice(&buf[..len]);
        buf = &buf[len..];
        p += len;
    }
    if buf.len() > 0 {
        bytes.vec.extend_from_slice(buf);
        p += buf.len();
    }

    let written = p - *pos;
    *pos = p;

    Ok(written)
}

impl Write for Cursor<&mut BytesMut> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        bytes_mut_write(self.inner, buf, &mut self.pos)
    }

    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

impl Write for Cursor<BytesMut> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        bytes_mut_write(&mut self.inner, buf, &mut self.pos)
    }

    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(test)]
    mod cursor {
        use super::*;

        #[test]
        fn write() {
            let b: BytesMut = vec![1, 2, 3, 4, 5].into();
            let mut c = Cursor::new(b);
            c.set_position(2);
            assert_eq!(c.write(&[6, 7, 8, 9, 10]).unwrap(), 5);
            assert_eq!(c.get_ref().as_slice(), &[1, 2, 6, 7, 8, 9, 10]);
        }
    }
}