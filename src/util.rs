pub mod atomic;
pub mod file_mutex;
pub mod varint;

use std::fmt;
use std::io;
use std::ops;
use std::time::Duration;

pub trait ResultExt<T, E> {
    fn expect_or_else<F, R>(self, f: F) -> T
        where R: fmt::Display,
              F: FnOnce(E) -> R;
}

impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn expect_or_else<F, R>(self, f: F) -> T
        where R: fmt::Display,
              F: FnOnce(E) -> R
    {
        match self {
            Ok(v) => v,
            Err(e) => panic!("{}", f(e)),
        }
    }
}

pub trait OptionResultExt<T, E> {
    fn transpose_(self) -> Result<Option<T>, E>;
}

impl<T, E> OptionResultExt<T, E> for Option<Result<T, E>> {
    fn transpose_(self) -> Result<Option<T>, E> {
        match self {
            Some(Ok(x)) => Ok(Some(x)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

pub trait ResultOptionExt<T, E> {
    fn transpose_(self) -> Option<Result<T, E>>;
}

impl<T, E> ResultOptionExt<T, E> for Result<Option<T>, E> {
    fn transpose_(self) -> Option<Result<T, E>> {
        match self {
            Ok(Some(x)) => Some(Ok(x)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}



pub trait WriteExt: io::Write {
    fn write_varint_u16_(&mut self, v: u16) -> io::Result<()> {
//        varint_enc!(v, buf, len => +);
//        buf[3] = 5;
//        self.write_all(&buf[..len])
        Ok(())
    }

    fn write_varint_u16(&mut self, v: u16) -> io::Result<()> {

//        self.write_all(&buf[..len])
        unimplemented!()
    }

    fn write_varint_u32(&mut self, v: u32) -> io::Result<()> {
        let mut len = 1;
        let mut buf = [v as u8, 0, 0, 0, 0];
        if buf[0] > 0x7f {
            buf[1] = (v >> 7) as u8;
            len += 1;
            if buf[1] > 0x7f {
                buf[2] = (v >> 14) as u8;
                len += 1;
                if buf[2] > 0x7f {
                    buf[3] = (v >> 21) as u8;
                    len += 1;
                    if buf[3] > 0x7f {
                        buf[4] = (v >> 28) as u8;
                        len += 1;
                    }
                }
            }
        }
        self.write_all(&buf[..len])
    }
}

impl<T: io::Write> WriteExt for T {}

pub trait DurationExt {
    fn as_millis_u64(&self) -> Option<u64>;
}

impl DurationExt for Duration {
    fn as_millis_u64(&self) -> Option<u64> {
        let secs = self.as_secs();
        let sub_millis = self.subsec_millis() as u64;
        secs.checked_mul(1000)?.checked_add(sub_millis)
    }
}