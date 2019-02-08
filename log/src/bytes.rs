pub use byteorder::{BigEndian, ByteOrder, LittleEndian};
use std::io::{self, SeekFrom};
use std::io::prelude::*;
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

macro_rules! impl_set {
    ($s:ident, $w:ident: $ty:ty) => {
        fn $s<B: ByteOrder>(&mut self, i: usize, v: $ty) {
            B::$w(&mut self.as_mut_slice()[i..], v);
        }
    };
}

pub trait BufMut: Buf + AsMut<[u8]> {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.as_mut()
    }

    fn set_u8(&mut self, i: usize, v: u8) {
        self.as_mut_slice()[i] = v;
    }

    fn set_i8(&mut self, i: usize, v: i8) {
        self.set_u8(i, v as u8);
    }

    impl_set!(set_u16, write_u16: u16);
    impl_set!(set_i16, write_i16: i16);
    impl_set!(set_u32, write_u32: u32);
    impl_set!(set_i32, write_i32: i32);
    impl_set!(set_u64, write_u64: u64);
    impl_set!(set_i64, write_i64: i64);
}

impl<T: Buf + AsMut<[u8]>> BufMut for T {}

pub trait GrowableBuf: BufMut {
    fn capacity(&self) -> usize;

    fn reserve(&mut self, extra_capacity: usize);

    fn ensure_capacity(&mut self, min_capacity: usize);

    fn is_empty(&self) -> bool;

    fn grow(&mut self, extra_len: usize);

    fn ensure_len_zeroed(&mut self, min_len: usize);

    fn set_len_zeroed(&mut self, len: usize);

    fn clear(&mut self);

    fn extend_from_slice(&mut self, other: &[u8]);
}

impl GrowableBuf for Vec<u8> {
    fn capacity(&self) -> usize {
        self.capacity()
    }

    fn reserve(&mut self, extra_capacity: usize) {
        self.reserve(extra_capacity);
    }

    fn ensure_capacity(&mut self, min_capacity: usize) {
        if min_capacity > self.capacity() {
            let extra = min_capacity - self.capacity();
            self.reserve(extra);
        }
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn grow(&mut self, extra_len: usize) {
        let new_len = self.len().checked_add(extra_len).unwrap();
        self.resize(new_len, 0);
    }

    fn ensure_len_zeroed(&mut self, min_len: usize) {
        if self.len() < min_len {
            self.resize(min_len, 0);
        }
    }

    fn set_len_zeroed(&mut self, len: usize) {
        self.resize(len, 0);
    }

    fn clear(&mut self) {
        self.clear();
    }

    fn extend_from_slice(&mut self, other: &[u8]) {
        self.extend_from_slice(other);
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

fn growable_buf_write(dst: &mut impl GrowableBuf, mut buf: &[u8], pos: &mut usize) -> io::Result<usize> {
    let mut p = *pos;
    if p < dst.len() {
        let len = cmp::min(dst.len() - p, buf.len());
        let dst = &mut dst.as_mut_slice()[p..p + len];
        dst.copy_from_slice(&buf[..len]);
        buf = &buf[len..];
        p += len;
    }
    if buf.len() > 0 {
        dst.extend_from_slice(buf);
        p += buf.len();
    }

    let written = p - *pos;
    *pos = p;

    Ok(written)
}

impl Write for Cursor<&mut Vec<u8>> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        growable_buf_write(self.inner, buf, &mut self.pos)
    }

    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

impl<T: GrowableBuf> Write for Cursor<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        growable_buf_write(&mut self.inner, buf, &mut self.pos)
    }

    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(test)]
    mod cursor {
        use super::*;
        use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

        #[test]
        fn write() {
            let b: Vec<u8> = vec![1, 2, 3, 4, 5].into();
            let mut c = Cursor::new(b);
            c.set_position(2);
            assert_eq!(c.write(&[6, 7, 8, 9, 10]).unwrap(), 5);
            assert_eq!(c.get_ref().as_slice(), &[1, 2, 6, 7, 8, 9, 10]);
        }

        #[test]
        fn mut_ref() {
            let mut buf = Vec::new();
            let mut c = Cursor::new(&mut buf);
            c.write_u32::<BigEndian>(12345).unwrap();
            c.set_position(0);
            assert_eq!(c.read_u32::<BigEndian>().unwrap(), 12345);
        }
    }
}