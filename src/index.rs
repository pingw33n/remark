use byteorder::{BigEndian, ByteOrder};
use if_chain::if_chain;
use log::error;
use matches::matches;
use memmap::{Mmap, MmapMut, MmapOptions};
use parking_lot::Mutex;
use std::borrow::{Borrow, Cow};
use std::cmp::{self, Ord, Ordering};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::ops;
use std::path::{Path, PathBuf};
use std::result::{Result as StdResult};
use std::slice;
use std::sync::Arc;
use std::time::SystemTime;

use crate::error::*;
use crate::Timestamp;

#[derive(Clone, Debug, Eq, Fail, PartialEq)]
pub enum Error {
    #[fail(display = "{}", _0)]
    Corrupted(Cow<'static, str>),

    #[fail(display = "attempted to push index entries in wrong order")]
    PushMisordered,

    #[fail(display = "index cant grow because it's already at max capacity or is opened in static mode")]
    CantGrow,

    #[fail(display = "IO error")]
    Io,
}

pub trait Field: Clone + Copy + fmt::Debug + Ord {
    const LEN: usize;

    fn encode(&self, buf: &mut [u8]);

    fn decode(buf: &[u8]) -> Self
        where Self: std::marker::Sized;
}

impl Field for u32 {
    const LEN: usize = mem::size_of::<u32>();

    fn encode(&self, buf: &mut [u8]) {
        BigEndian::write_u32(buf, *self);
    }

    fn decode(buf: &[u8]) -> Self {
        BigEndian::read_u32(buf)
    }
}

impl Field for u64 {
    const LEN: usize = mem::size_of::<u64>();

    fn encode(&self, buf: &mut [u8]) {
        BigEndian::write_u64(buf, *self);
    }

    fn decode(buf: &[u8]) -> Self {
        BigEndian::read_u64(buf)
    }
}

impl Field for Timestamp {
    const LEN: usize = u64::LEN;

    fn encode(&self, buf: &mut [u8]) {
        self.millis().encode(buf)
    }

    fn decode(buf: &[u8]) -> Self {
        u64::decode(buf).into()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Static,

    Growable {
        /// Number of entries index will grow by when capacity exceeded, up to `max_capacity`.
        preallocate: usize,

        /// Maximum capacity index can grow to.
        max_capacity: usize,
    }
}

impl Mode {
    pub fn is_static(&self) -> bool {
        matches!(self, Mode::Static)
    }

    pub fn is_growable(&self) -> bool {
        matches!(self, Mode::Growable { .. })
    }
}

struct Inner<K: Field, V: Field> {
    len_bytes: usize,
    capacity_bytes: usize,
    preallocate_bytes: usize,
    max_capacity_bytes: usize,
    _ty: PhantomData<(K, V)>,
}

impl<K: Field, V: Field> Inner<K, V> {
    unsafe fn free_buf<'a>(&mut self, mmap: &'a Option<MmapMut>) -> Option<&'a mut [u8]> {
        mmap.as_ref()
            .map(|m| &m[self.len_bytes..])
            .map(|sl| slice::from_raw_parts_mut(sl.as_ptr() as *mut _, sl.len()))
    }

    fn used_buf<'a>(&self, mmap: &'a Option<MmapMut>) -> Option<&'a [u8]> {
        mmap.as_ref()
            .map(|m| &m[..self.len_bytes])
    }

    fn is_full(&self) -> bool {
        self.len_bytes >= self.capacity_bytes
    }

    fn growable_bytes(&self) -> usize {
        self.max_capacity_bytes.checked_sub(self.capacity_bytes).unwrap_or(0)
    }

    fn ensure_capacity(&mut self, file: &File) -> Result<()> {
        if self.is_full() {
            let growable = self.growable_bytes();
            if growable == 0 {
                return Err(Error::CantGrow.into());
            }
            let amount = cmp::min(self.preallocate_bytes, growable);
            let new_capacity = self.capacity_bytes + amount;
            set_file_len(&file, new_capacity, false)?;
            self.capacity_bytes = new_capacity;
        }
        Ok(())
    }

    fn last_entry(&self, mmap: &Option<MmapMut>) -> Option<(K, V)> {
        let buf = if self.len_bytes > 0 {
            self.used_buf(mmap).map(|b| &b[self.len_bytes - Index::<K, V>::ENTRY_LEN..])
        } else {
            None
        };

        buf.map(Index::<K, V>::decode_entry)
    }
}

mod sealed { pub trait Sealed {} }

pub trait DupPolicy: sealed::Sealed {
    #[doc(hidden)]
    const __DUP_IGNORED: bool;
}

pub struct DupIgnored(());

impl DupPolicy for DupIgnored {
    #[doc(hidden)]
    const __DUP_IGNORED: bool = true;
}

impl sealed::Sealed for DupIgnored {}

pub struct DupNotAllowed(());

impl DupPolicy for DupNotAllowed {
    #[doc(hidden)]
    const __DUP_IGNORED: bool = false;
}

impl sealed::Sealed for DupNotAllowed {}

pub struct Index<K: Field, V: Field, KP: DupPolicy = DupNotAllowed> {
    path: PathBuf,
    file: File,
    mmap: Option<MmapMut>,
    inner: Mutex<Inner<K, V>>,
    _ty: PhantomData<KP>,
}

impl<K: Field, V: Field, KP: DupPolicy> Index<K, V, KP> {
    const ENTRY_LEN: usize = K::LEN + V::LEN;

    pub fn open_or_create(path: impl AsRef<Path>, mode: Mode) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let mut open_opts = OpenOptions::new();
        open_opts
            .write(true)
            .read(true);
        if mode.is_growable() {
            open_opts
                .create(true);
        }
        let file = open_opts.open(&path)
            .context(Error::Io)?;

        let file_len = file.metadata().context(Error::Io)?.len();
        if file_len >= usize::max_value() as u64 {
            return Err(Error::Corrupted("index file is too big to be mmaped".into()).into());
        }

        let mut file_len = file_len as usize;
        if file_len % Self::ENTRY_LEN != 0 {
            return Err(Error::Corrupted(
                format!("index file len ({}) is not a multiply of entry len ({})",
                    file_len, Self::ENTRY_LEN).into()).into());
        }
        let len_bytes = file_len;

        let mut capacity_bytes = file_len;
        let (preallocate_bytes, max_capacity_bytes) = if let Mode::Growable { preallocate, max_capacity } = mode {
            assert!(preallocate > 0);
            let preallocate_bytes = preallocate.checked_mul(Self::ENTRY_LEN).unwrap();
            if capacity_bytes < preallocate_bytes {
                // File len is a marker of index consistency, so we keep it invalid until the
                // file is flushed and closed during drop.
                assert!(Self::ENTRY_LEN > 1);
                set_file_len(&file, preallocate_bytes, false)?;
                capacity_bytes = preallocate_bytes;
            }
            (preallocate_bytes, max_capacity.checked_mul(Self::ENTRY_LEN).unwrap())
        } else {
            (0, capacity_bytes)
        };

        let mmap = Self::mmap(&file, max_capacity_bytes).context(Error::Io)?;

        Ok(Self {
            path,
            file,
            mmap,
            inner: Mutex::new(Inner {
                len_bytes,
                capacity_bytes,
                preallocate_bytes,
                max_capacity_bytes,
                _ty: PhantomData,
            }),
            _ty: PhantomData,
        })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().len_bytes == 0
    }

    pub fn capacity(&self) -> usize {
        self.inner.lock().capacity_bytes / Self::ENTRY_LEN
    }

    pub fn max_capacity(&self) -> usize {
        self.inner.lock().max_capacity_bytes / Self::ENTRY_LEN
    }

    pub fn len(&self) -> usize {
        self.inner.lock().len_bytes / Self::ENTRY_LEN
    }

    pub fn last_entry(&self) -> Option<(K, V)> {
        self.inner.lock().last_entry(&self.mmap)
    }

    pub fn last_value(&self) -> Option<V> {
        self.last_entry().map(|(_, v)| v)
    }

    fn used_buf(&self) -> &[u8] {
        // TODO This can be made fully non-blocking by using atomic len.
        self.inner.lock().used_buf(&self.mmap).unwrap_or(&[])
    }

    pub fn entry_by_key(&self, key: K) -> Option<(K, V)> {
        let buf = self.used_buf();
        let i = match Self::binary_search_by_key(buf, key) {
            Ok(i) => i,
            Err(i) => if i == 0 {
                return None;
            } else {
                i - Self::ENTRY_LEN
            }
        };
        Some(Self::decode_entry(&buf[i..]))
    }

    pub fn value_by_key(&self, key: K) -> Option<V> {
        self.entry_by_key(key).map(|(_, v)| v)
    }

    pub fn entry_by_value(&self, value: V) -> Option<(K, V)> {
        let i = match Self::binary_search_by_value(self.used_buf(), value) {
            Ok(i) => i,
            Err(i) => if i == 0 {
                return None;
            } else {
                i - Self::ENTRY_LEN
            }
        };
        Some(Self::decode_entry(&self.used_buf()[i..]))
    }

    pub fn key_by_value(&self, value: V) -> Option<K> {
        self.entry_by_value(value).map(|(k, _)| k)
    }

    pub fn push(&self, key: K, value: V) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.ensure_capacity(&self.file)?;
        if let Some((last_key, last_value)) = inner.last_entry(&self.mmap) {
            Self::check_dup::<_, KP>(key, last_key)?;
            Self::check_dup::<_, DupNotAllowed>(value, last_value)?;
        }

        let buf = unsafe { inner.free_buf(&self.mmap) }.unwrap();
        Self::encode_entry(buf, key, value);

        inner.len_bytes += Self::ENTRY_LEN;

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.mmap.as_ref().unwrap().flush().context(Error::Io).map_err(|e| e.into())
    }

    pub fn shrink_to_fit(&self) -> Result<()> {
        if let Some(mmap) = self.mmap.as_ref() {
            mmap.flush().context(Error::Io)?;
        }
        let mut inner = self.inner.lock();
        set_file_len(&self.file, inner.len_bytes, true)?;
        inner.capacity_bytes = inner.len_bytes;
        Ok(())
    }

    fn mmap(file: &File, len: usize) -> Result<Option<MmapMut>> {
        Ok(if len == 0 {
            // Can't mmap empty files.
            None
        } else {
            Some(unsafe {
                MmapOptions::new()
                    .len(len)
                    .map_mut(&file)
            }.context(Error::Io)?)
        })
    }

    fn binary_search(buf: &[u8], f: impl Fn(&[u8]) -> Ordering) -> StdResult<usize, usize> {
        debug_assert!(buf.len() % Self::ENTRY_LEN == 0);
        let mut len = buf.len();
        if len == 0 {
            return Err(0);
        }
        let mut base = 0;
        while len > Self::ENTRY_LEN {
            let half = len / 2 / Self::ENTRY_LEN * Self::ENTRY_LEN;
            let mid = base + half;
            let cmp = f(&buf[mid..mid + Self::ENTRY_LEN]);
            if cmp != Ordering::Greater {
                base = mid;
            }
            len -= half;
        }
        match f(&buf[base..base + Self::ENTRY_LEN]) {
            Ordering::Equal => Ok(base),
            Ordering::Greater => Err(base),
            Ordering::Less => Err(base + Self::ENTRY_LEN),
        }
    }

    fn binary_search_by_key(buf: &[u8], key: K) -> StdResult<usize, usize> {
        Self::binary_search(buf, |e| Self::decode_key(e).cmp(&key))
    }

    fn binary_search_by_value(buf: &[u8], value: V) -> StdResult<usize, usize> {
        Self::binary_search(buf, |e| Self::decode_value(e).cmp(&value))
    }

    fn decode_key(entry: &[u8]) -> K {
        K::decode(&entry[..K::LEN])
    }

    fn decode_value(entry: &[u8]) -> V {
        V::decode(&entry[K::LEN..])
    }

    fn decode_entry(entry: &[u8]) -> (K, V) {
        (Self::decode_key(entry), Self::decode_value(entry))
    }

    fn encode_entry(entry: &mut [u8], key: K, value: V) {
        key.encode(&mut entry[..K::LEN]);
        value.encode(&mut entry[K::LEN..]);
    }

    fn check_dup<T: Field, P: DupPolicy>(new: T, last: T) -> Result<()> {
        match new.cmp(&last) {
            Ordering::Less => Err(Error::PushMisordered.into()),
            Ordering::Equal => if KP::__DUP_IGNORED && new == last {
                Ok(())
            } else {
                Err(Error::PushMisordered.into())
            }
            Ordering::Greater => Ok(()),
        }
    }
}

impl<K: Field, V: Field, KP: DupPolicy> Drop for Index<K, V, KP> {
    fn drop(&mut self) {
        let _ = self.shrink_to_fit()
            .map_err(|e| error!("[{:?}] error compacting index: {:?}",
                self.path, e));
    }
}

fn prev_multiple_of<T>(v: T, mult: T) -> T
        where T: Copy + ops::Div<Output=T> + ops::Mul<Output=T> {
    v / mult * mult
}

fn set_file_len(file: &File, len: usize, committed: bool) -> Result<()> {
    // TODO this is not going to work on Windows: can't resize file while it's mmap'ed.
    file.set_len(len.checked_add(!committed as usize).unwrap() as u64)
        .context(Error::Io)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use super::Error;
    use assert_matches::assert_matches;
    use std::fs;

    #[test]
    fn concurrent() {
        env_logger::init();

        use rand::{Rng, thread_rng};
        use std::thread;

        const ENTRY_COUNT: usize = 3000;
        const THREAD_COUNT: usize = 32;
        const READ_COUNT: usize = (ENTRY_COUNT as f64 * 1.2) as usize;
        const PREALLOCATE: usize = ENTRY_COUNT / 10 + 3;
        const MAX_CAPACITY: usize = ENTRY_COUNT;

        let f = mktemp::Temp::new_file().unwrap();
        {
            let idx: Index<u64, u32> = Index::open_or_create(&f, Mode::Growable {
                preallocate: PREALLOCATE,
                max_capacity: MAX_CAPACITY,
            }).unwrap();

            let readers: Vec<_> = (0..THREAD_COUNT)
                .map(|_| thread::spawn(|| {
                    for i in 0..READ_COUNT {
                        let len = idx.len();
                        if len > 0 {
                            let n = thread_rng().gen_range(0, len);
                            let k = n as u64 * 100;
                            let v = n as u32 * 10;
                            assert_eq!(idx.entry_by_key(k), Some((k, v)));
                            assert_eq!(idx.entry_by_value(v), Some((k, v)));

                            if thread_rng().gen_range(0, 100) < 10 {
                                idx.shrink_to_fit();
                            }
                        }
                    }
                }))
                .collect();

            for k in 0..ENTRY_COUNT {
                idx.push(k as u64 * 100, k as u32 * 10).unwrap();
                if thread_rng().gen_range(0, 100) < 15 {
                    thread::sleep_ms(1);
                }
            }

            for h in readers {
                h.join().unwrap();
            }
        }

        assert_eq!(fs::metadata(&f).unwrap().len(),
            cmp::min((ENTRY_COUNT +  PREALLOCATE - 1) / PREALLOCATE * PREALLOCATE, MAX_CAPACITY) as u64 * 12 );
    }

    #[test]
    fn grow() {
        let f = mktemp::Temp::new_file().unwrap();

        let f_len = || fs::metadata(&f).unwrap().len();

        {
            let mut idx: Index<u32, u64> = Index::open_or_create(&f, Mode::Growable {
                preallocate: 2,
                max_capacity: 5,
            }).unwrap();
            assert_eq!(f_len(), 2 * 12 + 1);

            idx.push(1, 101).unwrap();
            assert_eq!(idx.len(), 1);
            assert_eq!(f_len(), 2 * 12 + 1);

            idx.push(2, 102).unwrap();
            assert_eq!(idx.len(), 2);
            assert_eq!(f_len(), 2 * 12 + 1);

            idx.push(3, 103).unwrap();
            assert_eq!(idx.len(), 3);
            assert_eq!(f_len(), 4 * 12 + 1);

            idx.push(4, 104).unwrap();
            idx.push(5, 105).unwrap();
            assert_eq!(idx.len(), 5);
            assert_eq!(f_len(), 5 * 12 + 1);

            for _ in 0..5 {
                assert_eq!(idx.push(6, 106).unwrap_err().kind(), &ErrorKind::Index(Error::CantGrow));
            }

            for k in 1..=5 {
                assert_eq!(idx.entry_by_key(k), Some((k, 100 + k as u64)));
            }
        }
        assert_eq!(f_len(), 5 * 12);
        {
            let mut idx: Index<u32, u64> = Index::open_or_create(&f, Mode::Static).unwrap();

            assert_eq!(idx.len(), 5);
            for k in 1..=5 {
                assert_eq!(idx.entry_by_key(k), Some((k, 100 + k as u64)));
            }

            for _ in 0..5 {
                assert_eq!(idx.push(6, 106).unwrap_err().kind(), &ErrorKind::Index(Error::CantGrow));
            }
        }
    }

    fn err_kind<T>(r: Result<T>) -> ErrorKind {
        r.err().unwrap().kind().clone()
    }

    #[test]
    fn push_misorder__key_no_dup() {
        let f = mktemp::Temp::new_file().unwrap();
        let mut idx: Index<u32, u64> = Index::open_or_create(&f, Mode::Growable {
            preallocate: 5,
            max_capacity: 5,
        }).unwrap();
        assert!(idx.push(100, 200).is_ok());
        assert!(idx.push(101, 201).is_ok());
        assert_matches!(err_kind(idx.push(101, 201)), ErrorKind::Index(Error::PushMisordered));
        assert_matches!(err_kind(idx.push(100, 200)), ErrorKind::Index(Error::PushMisordered));
        assert_matches!(err_kind(idx.push(101, 200)), ErrorKind::Index(Error::PushMisordered));
        assert_matches!(err_kind(idx.push(100, 201)), ErrorKind::Index(Error::PushMisordered));
        assert_matches!(err_kind(idx.push(101, 202)), ErrorKind::Index(Error::PushMisordered));
        assert_matches!(err_kind(idx.push(102, 201)), ErrorKind::Index(Error::PushMisordered));
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.entry_by_key(100), Some((100, 200)));
        assert_eq!(idx.entry_by_key(101), Some((101, 201)));
    }

    #[test]
    fn push_misorder__key_dup_ignored() {
        let f = mktemp::Temp::new_file().unwrap();
        let mut idx: Index<u64, u32, DupIgnored> = Index::open_or_create(&f, Mode::Growable {
            preallocate: 5,
            max_capacity: 5,
        }).unwrap();
        assert!(idx.push(100, 200).is_ok());
        assert!(idx.push(101, 201).is_ok());
        assert!(idx.push(101, 201).is_ok());
        assert!(idx.push(101, 202).is_ok());
        assert_matches!(err_kind(idx.push(101, 200)), ErrorKind::Index(Error::PushMisordered));
        assert_matches!(err_kind(idx.push(101, 199)), ErrorKind::Index(Error::PushMisordered));
        assert_matches!(err_kind(idx.push(100, 203)), ErrorKind::Index(Error::PushMisordered));
    }
}