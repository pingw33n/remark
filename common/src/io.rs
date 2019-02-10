use num_traits::cast::ToPrimitive;
use std::cmp;
use std::io::{Error, ErrorKind, Result};
use std::io::prelude::*;

pub trait BoundedRead: Read {
    fn available(&self) -> Result<u64>;
    fn is_eof(&self) -> Result<bool> {
        Ok(self.available()? == 0)
    }
}

struct State {
    pos: Option<u64>,
    len: u64,
}

impl State {
    fn advance(&mut self, amt: usize) {
        let new_pos = self.pos().checked_add(amt as u64).unwrap();
        assert!(new_pos <= self.len, "attempted to consume beyond bound");
        self.pos = Some(new_pos)
    }

    fn pos(&self) -> u64 {
        self.pos.expect("BoundedReader is poisoned")
    }

    fn readable_len(&self, desired_len: usize) -> usize {
        (self.len - self.pos()).to_usize()
            .map(|l| cmp::min(l, desired_len))
            .unwrap_or(desired_len)
    }

    fn readable_buf<'a>(&self, buf: &'a [u8]) -> &'a [u8] {
        let len = self.readable_len(buf.len());
        &buf[..len]
    }

    fn readable_buf_mut<'a>(&self, buf: &'a mut [u8]) -> &'a mut [u8] {
        let len = self.readable_len(buf.len());
        &mut buf[..len]
    }
}

pub struct BoundedReader<R> {
    rd: R,
    state: State,
}

impl<R> BoundedReader<R> {
    pub fn new(rd: R, len: u64) -> Self {
        Self {
            rd,
            state: State {
                pos: Some(0),
                len,
            }
        }
    }

    pub fn position(&self) -> Option<u64> {
        self.state.pos
    }

    pub fn len(&self) -> u64 {
        self.state.len
    }

    pub fn set_len(&mut self, new_len: u64) {
        assert!(new_len >= self.state.pos());
        self.state.len = new_len;
    }
}

impl<R: Read> Read for BoundedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let buf = self.state.readable_buf_mut(buf);
        match self.rd.read(buf) {
            Ok(amt) => {
                self.state.advance(amt);
                Ok(amt)
            }
            Err(e) => {
                dbg!(&e);
                self.state.pos = None;
                Err(e)
            }
        }
    }
}

impl<R: BufRead> BufRead for BoundedReader<R> {
    fn fill_buf(&mut self) -> Result<&[u8]> {
        match self.rd.fill_buf() {
            Ok(v) => Ok(self.state.readable_buf(v)),
            Err(e) => {
                self.state.pos = None;
                Err(e)
            }
        }
    }

    fn consume(&mut self, amt: usize) {
        self.rd.consume(amt);
        self.state.advance(amt);
    }
}

impl<R: Read> BoundedRead for BoundedReader<R> {
    fn available(&self) -> Result<u64> {
        let pos = self.position().ok_or_else(|| Error::new(ErrorKind::InvalidInput,
            "BoundedReader is poisoned"))?;
        Ok(self.len() - pos)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    mod bounded_reader {
        use super::*;
        use std::io::Cursor;

        fn read_u8(rd: &mut impl Read) -> Result<u8> {
            let mut buf = [0];
            rd.read_exact(&mut buf).map(|_| buf[0])
        }

        #[test]
        fn read() {
            let mut c = Cursor::new(vec![1, 2, 3, 4]);
            c.set_position(1);

            let ref mut br = BoundedReader::new(c, 2);
            assert_eq!(br.len(), 2);
            assert_eq!(br.position(), Some(0));
            assert_eq!(br.available().unwrap(), 2);
            assert!(!br.is_eof().unwrap());

            assert_eq!(read_u8(br).unwrap(), 2);
            assert_eq!(br.position(), Some(1));
            assert_eq!(br.available().unwrap(), 1);
            assert!(!br.is_eof().unwrap());

            assert_eq!(read_u8(br).unwrap(), 3);
            assert_eq!(br.position(), Some(2));
            assert_eq!(br.available().unwrap(), 0);
            assert!(br.is_eof().unwrap());

            assert_eq!(read_u8(br).unwrap_err().kind(), ErrorKind::UnexpectedEof);
            assert_eq!(br.position(), Some(2));
            assert_eq!(br.available().unwrap(), 0);
            assert!(br.is_eof().unwrap());
        }

        #[test]
        fn eof() {
            let ref mut br = BoundedReader::new(Cursor::new(vec![1]), 2);

            assert_eq!(read_u8(br).unwrap(), 1);

            assert_eq!(read_u8(br).unwrap_err().kind(), ErrorKind::UnexpectedEof);
            assert_eq!(br.position(), Some(1));
            assert_eq!(br.available().unwrap(), 1);
            assert!(!br.is_eof().unwrap());
        }

        #[test]
        fn poisoned() {
            struct ReadErr;

            impl Read for ReadErr {
                fn read(&mut self, _buf: &mut [u8]) -> Result<usize> {
                    Err(Error::new(ErrorKind::Other, "test"))
                }
            }

            let ref mut br = BoundedReader::new(ReadErr, 100);
            assert_eq!(read_u8(br).unwrap_err().kind(), ErrorKind::Other);
            assert_eq!(br.position(), None);
            assert_eq!(br.available().unwrap_err().kind(), ErrorKind::InvalidInput);
            assert_eq!(br.is_eof().unwrap_err().kind(), ErrorKind::InvalidInput);
        }

        #[test]
        fn buf_read() {
            let mut c = Cursor::new(vec![1, 2, 3, 4]);
            c.set_position(1);

            let ref mut br = BoundedReader::new(c, 2);
            assert_eq!(br.fill_buf().unwrap(), &[2, 3]);
            assert_eq!(br.position(), Some(0));
            assert_eq!(br.available().unwrap(), 2);
            assert!(!br.is_eof().unwrap());

            br.consume(1);
            assert_eq!(br.position(), Some(1));
            assert_eq!(br.available().unwrap(), 1);
            assert!(!br.is_eof().unwrap());

            assert_eq!(br.fill_buf().unwrap(), &[3]);

            br.consume(1);
            assert_eq!(br.position(), Some(2));
            assert_eq!(br.available().unwrap(), 0);
            assert!(br.is_eof().unwrap());

            assert_eq!(br.fill_buf().unwrap(), &[]);

            assert_eq!(br.position(), Some(2));
            assert_eq!(br.available().unwrap(), 0);
            assert!(br.is_eof().unwrap());
        }

        #[should_panic]
        #[test]
        fn panic_if_consume_more_than_len() {
            let ref mut br = BoundedReader::new(Cursor::new(vec![1, 2]), 1);
            assert_eq!(br.fill_buf().unwrap(), &[1]);
            br.consume(2);
        }

        #[should_panic]
        #[test]
        fn panic_if_shrinks_before_pos() {
            let ref mut br = BoundedReader::new(Cursor::new(vec![1, 2]), 10000);
            read_u8(br).unwrap();
            assert_eq!(br.position(), Some(1));
            br.set_len(0);
        }
    }
}