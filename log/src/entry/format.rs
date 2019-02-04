use byteorder::{BigEndian, ByteOrder, WriteBytesExt};
use std::io;
use std::io::prelude::*;
use std::ops::Range;
use std::marker::PhantomData;

use crate::bytes::Buf;
use crate::message::{Id, Timestamp};

macro_rules! fields {
    ($name:ident: $ty:tt = $pos:expr; $($rest:tt)*) => {
        pub const $name: Field<$ty> = Field {
            pos: $pos,
            len: $ty::LEN,
            next: ($pos) + $ty::LEN,
            _ty: PhantomData,
        };
        fields!(@impl $pos + $ty::LEN => $($rest)*);
    };
    (@impl $pos:expr => $name:ident: $ty:tt; $($rest:tt)*) => {
        pub const $name: Field<$ty> = Field {
            pos: $pos,
            len: $ty::LEN,
            next: ($pos) + $ty::LEN,
            _ty: PhantomData,
        };
        fields!(@impl ($pos) + $ty::LEN => $($rest)*);
    };
    (@impl $pos:expr =>) => {};
}

type OptionId = Option<Id>;

fields! {
    FRAME_LEN: u32 = 0;
    HEADER_CRC: u32;
    VERSION: u8;
    START_ID: OptionId;
    END_ID_DELTA: u32;
    FIRST_TIMESTAMP: Timestamp;
    MAX_TIMESTAMP: Timestamp;
    FLAGS: u16;
    TERM: u64;
    BODY_CRC: u32;
    MESSAGE_COUNT: u32;
}

// Frame prolog fields that are not versioned (i.e. never change across versions).
pub const FRAME_PROLOG_FIXED_LEN: usize = VERSION.next;

// Prolog fields for the current version.
pub const FRAME_PROLOG_LEN: usize = MESSAGE_COUNT.next;

pub const HEADER_CRC_RANGE: Range<usize> = HEADER_CRC.next..BODY_CRC.pos;
pub const BODY_CRC_START: usize = BODY_CRC.next;

pub const MIN_FRAME_LEN: usize = MESSAGE_COUNT.next;

pub const MESSAGES_START: usize = MESSAGE_COUNT.next;

pub const CURRENT_VERSION: u8 = 1;

pub struct Field<F: FieldType> {
    pub pos: usize,
    pub len: usize,
    pub next: usize,
    _ty: PhantomData<F>,
}

impl<F: FieldType> Field<F> {
    pub fn get(&self, buf: &impl Buf) -> F {
        F::get(buf, self.pos)
    }

    pub fn set(&self, buf: &mut [u8], v: F) {
        v.set(&mut buf[self.pos..])
    }

    pub fn write(&self, wr: &mut Write, v: F) -> io::Result<()> {
        v.write(wr)
    }
}

pub trait FieldType: Sized {
    const LEN: usize = std::mem::size_of::<Self>();

    fn get(buf: &impl Buf, i: usize) -> Self;
    fn set(&self, buf: &mut [u8]);
    fn write(&self, wr: &mut Write) -> io::Result<()>;
}

impl FieldType for u8 {
    fn get(buf: &impl Buf, i: usize) -> Self {
        buf.get_u8(i)
    }

    fn set(&self, buf: &mut [u8]) {
        buf[0] = *self;
    }

    fn write(&self, wr: &mut Write) -> io::Result<()> {
        wr.write_u8(*self)
    }
}

impl FieldType for u16 {
    fn get(buf: &impl Buf, i: usize) -> Self {
        buf.get_u16::<BigEndian>(i)
    }

    fn set(&self, buf: &mut [u8]) {
        BigEndian::write_u16(buf, *self);
    }

    fn write(&self, wr: &mut Write) -> io::Result<()> {
        wr.write_u16::<BigEndian>(*self)
    }
}

impl FieldType for u32 {
    fn get(buf: &impl Buf, i: usize) -> Self {
        buf.get_u32::<BigEndian>(i)
    }

    fn set(&self, buf: &mut [u8]) {
        BigEndian::write_u32(buf, *self);
    }

    fn write(&self, wr: &mut Write) -> io::Result<()> {
        wr.write_u32::<BigEndian>(*self)
    }
}

impl FieldType for u64 {
    fn get(buf: &impl Buf, i: usize) -> Self {
        buf.get_u64::<BigEndian>(i)
    }

    fn set(&self, buf: &mut [u8]) {
        BigEndian::write_u64(buf, *self);
    }

    fn write(&self, wr: &mut Write) -> io::Result<()> {
        wr.write_u64::<BigEndian>(*self)
    }
}

impl FieldType for i64 {
    fn get(buf: &impl Buf, i: usize) -> Self {
        buf.get_i64::<BigEndian>(i)
    }

    fn set(&self, buf: &mut [u8]) {
        BigEndian::write_i64(buf, *self);
    }

    fn write(&self, wr: &mut Write) -> io::Result<()> {
        wr.write_i64::<BigEndian>(*self)
    }
}

impl FieldType for Option<Id> {
    fn get(buf: &impl Buf, i: usize) -> Self {
        Id::new(u64::get(buf, i))
    }

    fn set(&self, buf: &mut [u8]) {
        self.map(|v| v.as_u64()).unwrap_or(0).set(buf);
    }

    fn write(&self, wr: &mut Write) -> io::Result<()> {
        self.map(|v| v.as_u64()).unwrap_or(0).write(wr)
    }
}

impl FieldType for Timestamp {
    fn get(buf: &impl Buf, i: usize) -> Self {
        i64::get(buf, i).into()
    }

    fn set(&self, buf: &mut [u8]) {
        self.millis().set(buf);
    }

    fn write(&self, wr: &mut Write) -> io::Result<()> {
        self.millis().write(wr)
    }
}

pub fn crc(buf: &[u8]) -> u32 {
    crc::crc32::checksum_castagnoli(buf)
}