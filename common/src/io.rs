use std::io::Result;
use std::io::prelude::*;

pub trait BoundRead: Read {
    fn available(&self) -> Result<u64>;
    fn is_eof(&self) -> Result<bool> {
        Ok(self.available()? == 0)
    }
}