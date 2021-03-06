use std::io;

pub fn encode_zigzag_i16(v: i16) -> u16 {
    encode_zigzag_i64(v as i64) as u16
}

pub fn encode_zigzag_i32(v: i32) -> u32 {
    encode_zigzag_i64(v as i64) as u32
}

pub fn encode_zigzag_i64(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

pub fn decode_zigzag_i16(v: u16) -> i16 {
    decode_zigzag_i64(v as u64) as i16
}

pub fn decode_zigzag_i32(v: u32) -> i32 {
    decode_zigzag_i64(v as u64) as i32
}

pub fn decode_zigzag_i64(v: u64) -> i64 {
    ((v >> 1) as i64) ^ (-((v & 1) as i64))
}

macro_rules! impl_enc {
    ($v:ident, $buf:ident => +$($rest:tt)*) => {{
        let (len, v) = impl_enc!(@impl $v, $buf, 0 => $($rest)*);
        $buf[len] = v as u8;
        len + 1
    }};
    (@impl $v:ident, $buf:ident, $i:expr => +$($rest:tt)*) => {
        if $v & !0x7f != 0 {
            $buf[$i] = 0x80 | ($v as u8);
            let $v = $v >> 7;
            impl_enc!(@impl $v, $buf, $i + 1 => $($rest)*)
        } else {
            ($i, $v)
        }
    };
    (@impl $v:ident, $buf:ident, $i:expr => ) => {
        ($i, $v)
    };
}

pub fn encode_u16_into(v: u16, buf: &mut [u8]) -> usize {
    impl_enc!(v, buf => +++)
}

pub fn encode_i16_into(v: i16, buf: &mut [u8]) -> usize {
    encode_u16_into(encode_zigzag_i16(v), buf)
}

pub fn encode_u32_into(v: u32, buf: &mut [u8]) -> usize {
    impl_enc!(v, buf => +++++)
}

pub fn encode_i32_into(v: i32, buf: &mut [u8]) -> usize {
    encode_u32_into(encode_zigzag_i32(v), buf)
}

pub fn encode_u64_into(v: u64, buf: &mut [u8]) -> usize {
    impl_enc!(v, buf => ++++++++++)
}

pub fn encode_i64_into(v: i64, buf: &mut [u8]) -> usize {
    encode_u64_into(encode_zigzag_i64(v), buf)
}

pub fn encode_u16(v: u16) -> ([u8; 3], usize) {
    let mut buf = [0u8; 3];
    let len = encode_u16_into(v, &mut buf);
    (buf, len)
}

pub fn encode_i16(v: i16) -> ([u8; 3], usize) {
    encode_u16(encode_zigzag_i16(v))
}

pub fn encode_u32(v: u32) -> ([u8; 5], usize) {
    let mut buf = [0u8; 5];
    let len = encode_u32_into(v, &mut buf);
    (buf, len)
}

pub fn encode_i32(v: i32) -> ([u8; 5], usize) {
    encode_u32(encode_zigzag_i32(v))
}

pub fn encode_u64(v: u64) -> ([u8; 10], usize) {
    let mut buf = [0u8; 10];
    let len = encode_u64_into(v, &mut buf);
    (buf, len)
}

pub fn encode_i64(v: i64) -> ([u8; 10], usize) {
    encode_u64(encode_zigzag_i64(v))
}

macro_rules! impl_dec {
    ($buf:ident: $ty:ty => +$($rest:tt)*) => {{
        let len;
        let r = impl_dec!(@impl $buf: $ty, len, 0, 0 => $($rest)*);
        (r, len)
    }};
    (@impl $buf:ident: $ty:ty, $len:ident, $i:expr, $sh:expr => +$($rest:tt)*) => {{
        (if $buf[$i] & 0x80 != 0 {
            ($buf[$i] & 0x7f) as $ty | impl_dec!(@impl $buf: $ty, $len, $i + 1, 7 => $($rest)*)
        } else {
            $len = $i + 1;
            $buf[$i] as $ty
        }) << $sh
    }};
    (@impl $buf:ident: $ty:ty, $len:ident, $i:expr, $sh:expr => ) => {{
        $len = $i + 1;
        ($buf[$i] as $ty) << $sh
    }};
}

pub fn decode_u16(buf: &[u8]) -> (u16, usize) {
    impl_dec!(buf: u16 => +++)
}

pub fn decode_i16(buf: &[u8]) -> (i16, usize) {
    let (v, len) = decode_u16(buf);
    (decode_zigzag_i16(v), len)
}

pub fn decode_u32(buf: &[u8]) -> (u32, usize) {
    impl_dec!(buf: u32 => +++++)
}

pub fn decode_i32(buf: &[u8]) -> (i32, usize) {
    let (v, len) = decode_u32(buf);
    (decode_zigzag_i32(v), len)
}

pub fn decode_u64(buf: &[u8]) -> (u64, usize) {
    impl_dec!(buf: u64 => ++++++++++)
}

pub fn decode_i64(buf: &[u8]) -> (i64, usize) {
    let (v, len) = decode_u64(buf);
    (decode_zigzag_i64(v), len)
}

pub fn encoded_len(v: u64) -> u32 {
    if v < 0x80 { 1 }
    else if v < 0x4000 { 2 }
    else if v < 0x200000 { 3 }
    else if v < 0x10000000 { 4 }
    else if v < 0x800000000 { 5 }
    else if v < 0x40000000000 { 6 }
    else if v < 0x2000000000000 { 7 }
    else if v < 0x100000000000000 { 8 }
    else if v < 0x8000000000000000 { 9 }
    else { 10 }
}

pub fn encoded_len_i64(v: i64) -> u32 {
    encoded_len(encode_zigzag_i64(v))
}

fn read_varint<R: ?Sized + io::Read>(rd: &mut R, max_len: u32) -> io::Result<u64> {
    use byteorder::ReadBytesExt;

    let mut r = 0;
    for i in 0..max_len {
        let b = rd.read_u8()?;
        if b & 0x80 != 0 {
            r |= ((b & 0x7f) as u64) << (i * 7);
        } else {
            r |= (b as u64) << (i * 7);
            break;
        }
    }
    Ok(r)
}

pub trait ReadExt: io::Read {
    fn read_u16_varint(&mut self) -> io::Result<u16> {
        read_varint(self, 3).map(|v| v as u16)
    }

    fn read_i16_varint(&mut self) -> io::Result<i16> {
        self.read_u16_varint().map(decode_zigzag_i16)
    }

    fn read_u32_varint(&mut self) -> io::Result<u32> {
        read_varint(self, 5).map(|v| v as u32)
    }

    fn read_i32_varint(&mut self) -> io::Result<i32> {
        self.read_u32_varint().map(decode_zigzag_i32)
    }

    fn read_u64_varint(&mut self) -> io::Result<u64> {
        read_varint(self, 10).map(|v| v as u64)
    }

    fn read_i64_varint(&mut self) -> io::Result<i64> {
        self.read_u64_varint().map(decode_zigzag_i64)
    }
}

impl<T: io::Read> ReadExt for T {}

pub trait WriteExt: io::Write {
    fn write_u16_varint(&mut self, v: u16) -> io::Result<usize> {
        let (buf, len) = encode_u16(v);
        self.write_all(&buf[..len]).map(|_| len)
    }

    fn write_i16_varint(&mut self, v: i16) -> io::Result<usize> {
        self.write_u16_varint(encode_zigzag_i16(v))
    }

    fn write_u32_varint(&mut self, v: u32) -> io::Result<usize> {
        let (buf, len) = encode_u32(v);
        self.write_all(&buf[..len]).map(|_| len)
    }

    fn write_i32_varint(&mut self, v: i32) -> io::Result<usize> {
        self.write_u32_varint(encode_zigzag_i32(v))
    }

    fn write_u64_varint(&mut self, v: u64) -> io::Result<usize> {
        let (buf, len) = encode_u64(v);
        self.write_all(&buf[..len]).map(|_| len)
    }

    fn write_i64_varint(&mut self, v: i64) -> io::Result<usize> {
        self.write_u64_varint(encode_zigzag_i64(v))
    }
}

impl<T: io::Write> WriteExt for T {}

#[cfg(test)]
mod test {
    use super::*;

    macro_rules! zigzag_data {
        ($ity:tt => $uty:tt) => {
            &[
                (0, 0),
                (-1, 1),
                (1, 2),
                (-2, 3),
                ($ity::min_value(), $uty::max_value()),
                ($ity::max_value(), $uty::max_value() - 1),
            ]
        };
    }

    macro_rules! zigzag_test {
        ($n:ident, $enc:ident, $dec:ident, $ity:tt => $uty:tt) => {
            #[test]
            fn $n() {
                let d = zigzag_data!($ity => $uty);
                for &(v, enc) in d {
                    let actual_enc = $enc(v);
                    assert_eq!(actual_enc, enc);
                    assert_eq!($dec(enc), v);
                }
            }
        };
    }

    zigzag_test!(encode_decode_zigzag_i16, encode_zigzag_i16, decode_zigzag_i16, i16 => u16);
    zigzag_test!(encode_decode_zigzag_i32, encode_zigzag_i32, decode_zigzag_i32, i32 => u32);
    zigzag_test!(encode_decode_zigzag_i64, encode_zigzag_i64, decode_zigzag_i64, i64 => u64);

    fn read<T, F>(buf: &[u8], f: F) -> (T, usize)
        where F: FnOnce(&mut io::Read) -> io::Result<T>
    {
        let mut cur = io::Cursor::new(buf);
        let v = f(&mut cur).unwrap();
        (v, cur.position() as usize)
    }

    fn write(f: impl FnOnce(&mut io::Write) -> io::Result<usize>) -> Vec<u8> {
        let mut vec = Vec::new();
        f(&mut vec).unwrap();
        vec
    }


    #[test]
    fn encode_decode_u16() {
        let d = &[
            (0, [0u8; 3], 1),
            (0x7f, [0x7f, 0, 0], 1),
            (0x80, [0x80, 1, 0], 2),
            (0x3fff, [0xff, 0x7f, 0], 2),
            (0x4000, [0x80, 0x80, 1], 3),
            (0x7fff, [0xff, 0xff, 1], 3),
            (0x8000, [0x80, 0x80, 2], 3),
            (0xffff, [0xff, 0xff, 3], 3),
        ];
        for &(v, enc, len) in d {
            assert_eq!(encode_u16(v), (enc, len));
            assert_eq!(decode_u16(&enc[..]), (v, len), "{:?}", enc);

            assert_eq!(read(&enc[..len], |mut r| r.read_u16_varint()), (v, len));
            assert_eq!(write(|mut w| w.write_u16_varint(v)), enc[..len].to_vec());
        }
    }

    #[test]
    fn decode_u16_trailing_bits() {
        assert_eq!(decode_u16(&[0xff, 0xff, 0xff]), (0xffff, 3));
    }

    #[test]
    fn encode_decode_u32() {
        let d = &[
            (0, [0u8; 5], 1),
            (0x80, [0x80, 1, 0, 0, 0], 2),
            (0x4000, [0x80, 0x80, 1, 0, 0], 3),
            (0x20_0000, [0x80, 0x80, 0x80, 1, 0], 4),
            (0xffff_ffff, [0xff, 0xff, 0xff, 0xff, 0xf], 5),
        ];
        for &(v, enc, len) in d {
            assert_eq!(encode_u32(v), (enc, len));
            assert_eq!(decode_u32(&enc[..]), (v, len), "{:?}", enc);

            assert_eq!(read(&enc[..len], |mut r| r.read_u32_varint()), (v, len));
            assert_eq!(write(|mut w| w.write_u32_varint(v)), enc[..len].to_vec());
        }
    }

    #[test]
    fn decode_u32_trailing_bits() {
        assert_eq!(decode_u32(&[0xff, 0xff, 0xff, 0xff, 0xff]), (0xffff_ffff, 5));
    }

    #[test]
    fn encode_decode_u64() {
        let d = &[
            (0, [0u8; 10], 1),
            (0xffff_ffff_ffff_ffff,
                [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1], 10),
        ];
        for &(v, enc, len) in d {
            assert_eq!(encode_u64(v), (enc, len), "{:?}", enc);
            assert_eq!(decode_u64(&enc[..]), (v, len), "{:?}", enc);

            assert_eq!(read(&enc[..len], |mut r| r.read_u64_varint()), (v, len));
            assert_eq!(write(|mut w| w.write_u64_varint(v)), enc[..len].to_vec());
        }
    }
}