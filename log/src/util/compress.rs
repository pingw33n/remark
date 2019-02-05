use enum_primitive_derive::Primitive;
use std::io::prelude::*;
use std::io::Result;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Primitive)]
pub enum Codec {
    Uncompressed = 0,
    Lz4          = 1,
    Zstd         = 2,
}

pub struct Decoder<R: BufRead>(DecoderInner<R>);

impl<R: BufRead> Decoder<R> {
    pub fn new(rd: R, codec: Codec) -> Result<Self> {
        Ok(Self(match codec {
            Codec::Uncompressed => DecoderInner::Uncompressed(rd),
            Codec::Lz4 => DecoderInner::Lz4(lz4::Decoder::new(rd)?),
            Codec::Zstd => DecoderInner::Zstd(zstd::Decoder::with_buffer(rd)?),
        }))
    }
}

impl<R: BufRead> Read for Decoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.0.read(buf)
    }
}

enum DecoderInner<R: BufRead> {
    Uncompressed(R),
    Lz4(lz4::Decoder<R>),
    Zstd(zstd::Decoder<R>),
}

impl<R: BufRead> Read for DecoderInner<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            DecoderInner::Uncompressed(rd) => rd.read(buf),
            DecoderInner::Lz4(rd) => rd.read(buf),
            DecoderInner::Zstd(rd) => rd.read(buf),
        }
    }
}

pub struct Encoder<W: Write>(EncoderInner<W>);

impl<W: Write> Encoder<W> {
    pub fn new(wr: W, codec: Codec) -> Result<Self> {
        Ok(Self(match codec {
            Codec::Uncompressed => EncoderInner::Uncompressed(wr),
            Codec::Lz4 => {
                let enc = lz4::EncoderBuilder::new()
                    .checksum(lz4::ContentChecksum::NoChecksum)
                    .build(wr)?;
                EncoderInner::Lz4(enc)
            }
            Codec::Zstd => EncoderInner::Zstd(zstd::Encoder::new(wr, 0)?),
        }))
    }

    pub fn finish(self) -> Result<W> {
        self.0.finish()
    }

    pub fn get_ref(&self) -> &W {
        self.0.get_ref()
    }

    pub fn codec(&self) -> Codec {
        self.0.codec()
    }
}

impl<W: Write> Write for Encoder<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        self.0.flush()
    }
}

enum EncoderInner<W: Write> {
    Uncompressed(W),
    Lz4(lz4::Encoder<W>),
    Zstd(zstd::Encoder<W>),
}

impl<W: Write> EncoderInner<W> {
    pub fn finish(self) -> Result<W> {
        match self {
            EncoderInner::Uncompressed(wr) => Ok(wr),
            EncoderInner::Lz4(enc) => {
                let (wr, r) = enc.finish();
                r.map(|_| wr)
            }
            EncoderInner::Zstd(enc) => enc.finish(),
        }
    }

    pub fn get_ref(&self) -> &W {
        match self {
            EncoderInner::Uncompressed(wr) => wr,
            EncoderInner::Lz4(enc) => enc.writer(),
            EncoderInner::Zstd(enc) => enc.get_ref(),
        }
    }

    pub fn codec(&self) -> Codec {
        match self {
            EncoderInner::Uncompressed(_) => Codec::Uncompressed,
            EncoderInner::Lz4(_) => Codec::Lz4,
            EncoderInner::Zstd(_) => Codec::Zstd,
        }
    }
}

impl<W: Write> Write for EncoderInner<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        match self {
            EncoderInner::Uncompressed(wr) => wr.write(buf),
            EncoderInner::Lz4(wr) => wr.write(buf),
            EncoderInner::Zstd(wr) => wr.write(buf),
        }
    }

    fn flush(&mut self) -> Result<()> {
        match self {
            EncoderInner::Uncompressed(wr) => wr.flush(),
            EncoderInner::Lz4(wr) => wr.flush(),
            EncoderInner::Zstd(wr) => wr.flush(),
        }
    }
}