use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use std::io;
use std::io::prelude::*;
use std::ops::{Range, RangeFrom};
use std::marker::PhantomData;

use crate::bytes::Buf;
use crate::Timestamp;

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

fields! {
    FRAME_LEN: u32 = 0;
    HEADER_CRC: u32;
    VERSION: u8;
    FIRST_ID: u64;
    LAST_ID_DELTA: u32;
    FIRST_TIMESTAMP: Timestamp;
    LAST_TIMESTAMP: Timestamp;
    FLAGS: u16;
    TERM: u64;
    BODY_CRC: u32;
    MESSAGE_COUNT: u32;
}
// Frame prolog fields that are not versioned (i.e. never change across versions).
pub const FRAME_PROLOG_FIXED_LEN: usize = VERSION.next;

// Same, but without the frame len.
pub const PROLOG_FIXED_LEN: usize = FRAME_PROLOG_FIXED_LEN - FRAME_LEN.len;

// Prolog fields for the current version.
pub const FRAME_PROLOG_LEN: usize = MESSAGE_COUNT.next;

// Same, but without the frame len.
//pub const PROLOG_LEN: usize = FRAME_PROLOG_LEN - FRAME_LEN.len;

pub const HEADER_CRC_RANGE: Range<usize> = HEADER_CRC.next..BODY_CRC.pos;
pub const BODY_CRC_RANGE: RangeFrom<usize> = BODY_CRC.next..;

pub const MIN_FRAME_LEN: usize = MESSAGE_COUNT.next;

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

    pub fn read(&self, rd: &mut Read) -> io::Result<F> {
        F::read(rd)
    }

    pub fn write(&self, wr: &mut Write, v: F) -> io::Result<()> {
        v.write(wr)
    }
}

pub trait FieldType: Sized {
    const LEN: usize = std::mem::size_of::<Self>();

    fn get(buf: &impl Buf, i: usize) -> Self;
    fn set(&self, buf: &mut [u8]);
    fn read(rd: &mut Read) -> io::Result<Self>;
    fn write(&self, wr: &mut Write) -> io::Result<()>;
}

impl FieldType for u8 {
    fn get(buf: &impl Buf, i: usize) -> Self {
        buf.get_u8(i)
    }

    fn set(&self, buf: &mut [u8]) {
        buf[0] = *self;
    }

    fn read(rd: &mut Read) -> io::Result<Self> {
        rd.read_u8()
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

    fn read(rd: &mut Read) -> io::Result<Self> {
        rd.read_u16::<BigEndian>()
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

    fn read(rd: &mut Read) -> io::Result<Self> {
        rd.read_u32::<BigEndian>()
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

    fn read(rd: &mut Read) -> io::Result<Self> {
        rd.read_u64::<BigEndian>()
    }

    fn write(&self, wr: &mut Write) -> io::Result<()> {
        wr.write_u64::<BigEndian>(*self)
    }
}

impl FieldType for Timestamp {
    fn get(buf: &impl Buf, i: usize) -> Self {
        u64::get(buf, i).into()
    }

    fn set(&self, buf: &mut [u8]) {
        self.millis().set(buf);
    }

    fn read(rd: &mut Read) -> io::Result<Self> {
        u64::read(rd).map(|v| v.into())
    }

    fn write(&self, wr: &mut Write) -> io::Result<()> {
        self.millis().write(wr)
    }
}