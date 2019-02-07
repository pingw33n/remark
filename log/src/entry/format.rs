use byteorder::{BigEndian, ByteOrder};
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

type OptionTimestamp = Option<Timestamp>;

fields! {
    FRAME_LEN: u32 = 0;
    HEADER_CHECKSUM: u32;
    VERSION: u8;
    START_ID: Id;
    END_ID_DELTA: u32;
    FIRST_TIMESTAMP: OptionTimestamp;
    MAX_TIMESTAMP: OptionTimestamp;
    FLAGS: u16;
    TERM: u64;
    MESSAGE_COUNT: u32;
    BODY_CHECKSUM: u32;
}

// Frame prolog fields that are not versioned (i.e. never change across versions).
pub const FRAME_PROLOG_FIXED_LEN: usize = VERSION.next;

pub const MESSAGES_START: usize = BODY_CHECKSUM.next;

// Prolog fields for the current version.
pub const FRAME_PROLOG_LEN: usize = MESSAGES_START;

pub const HEADER_CHECKSUM_RANGE: Range<usize> = HEADER_CHECKSUM.next..BODY_CHECKSUM.pos;

pub const BODY_CHECKSUM_START: usize = MESSAGES_START;

pub const MIN_FRAME_LEN: usize = MESSAGES_START;

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
}

pub trait FieldType: Sized {
    const LEN: usize = std::mem::size_of::<Self>();

    fn get(buf: &impl Buf, i: usize) -> Self;
    fn set(&self, buf: &mut [u8]);
}

impl FieldType for u8 {
    fn get(buf: &impl Buf, i: usize) -> Self {
        buf.get_u8(i)
    }

    fn set(&self, buf: &mut [u8]) {
        buf[0] = *self;
    }
}

impl FieldType for u16 {
    fn get(buf: &impl Buf, i: usize) -> Self {
        buf.get_u16::<BigEndian>(i)
    }

    fn set(&self, buf: &mut [u8]) {
        BigEndian::write_u16(buf, *self);
    }
}

impl FieldType for u32 {
    fn get(buf: &impl Buf, i: usize) -> Self {
        buf.get_u32::<BigEndian>(i)
    }

    fn set(&self, buf: &mut [u8]) {
        BigEndian::write_u32(buf, *self);
    }
}

impl FieldType for u64 {
    fn get(buf: &impl Buf, i: usize) -> Self {
        buf.get_u64::<BigEndian>(i)
    }

    fn set(&self, buf: &mut [u8]) {
        BigEndian::write_u64(buf, *self);
    }
}

impl FieldType for i64 {
    fn get(buf: &impl Buf, i: usize) -> Self {
        buf.get_i64::<BigEndian>(i)
    }

    fn set(&self, buf: &mut [u8]) {
        BigEndian::write_i64(buf, *self);
    }
}

impl FieldType for Id {
    fn get(buf: &impl Buf, i: usize) -> Self {
        Id::new(u64::get(buf, i))
    }

    fn set(&self, buf: &mut [u8]) {
        self.get().set(buf);
    }
}

impl FieldType for Option<Timestamp> {
    const LEN: usize = i64::LEN;

    fn get(buf: &impl Buf, i: usize) -> Self {
        Timestamp::from_millis(i64::get(buf, i))
    }

    fn set(&self, buf: &mut [u8]) {
        self.unwrap().millis().set(buf);
    }
}

pub fn checksum(buf: &[u8]) -> u32 {
    use std::hash::Hasher;

    let mut h = twox_hash::XxHash::with_seed(0);
    h.write(buf);
    h.finish() as u32
}