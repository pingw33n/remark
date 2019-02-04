use byteorder::{BigEndian, ByteOrder};
use log::error;
use memmap::{MmapMut, MmapOptions};
use parking_lot::Mutex;
use std::cmp::{self, Ord, Ordering};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem;
use std::path::{Path, PathBuf};
use std::result::{Result as StdResult};
use std::slice;

use crate::error::*;
use crate::message::Timestamp;

#[derive(Clone, Copy, Debug, Eq, Fail, PartialEq)]
pub enum ErrorId {
    #[fail(display = "index corrupted")]
    Corrupted,

    #[fail(display = "attempted to push index entries in wrong order")]
    PushMisordered,

    #[fail(display = "index cant grow because it's already at max capacity or is opened in read-only mode")]
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

impl Field for i64 {
    const LEN: usize = mem::size_of::<i64>();

    fn encode(&self, buf: &mut [u8]) {
        BigEndian::write_i64(buf, *self);
    }

    fn decode(buf: &[u8]) -> Self {
        BigEndian::read_i64(buf)
    }
}

impl Field for Timestamp {
    const LEN: usize = i64::LEN;

    fn encode(&self, buf: &mut [u8]) {
        self.millis().encode(buf)
    }

    fn decode(buf: &[u8]) -> Self {
        // FIXME remove this unwrap()
        Self::from_millis(i64::decode(buf)).unwrap()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    ReadOnly,

    ReadWrite {
        /// Number of entries index will grow by when capacity exceeded, up to `max_capacity`.
        preallocate: usize,

        /// Maximum capacity index can grow to.
        max_capacity: usize,
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
        self.max_capacity_bytes.saturating_sub(self.capacity_bytes)
    }

    fn ensure_capacity(&mut self, file: &File) -> Result<()> {
        if self.is_full() {
            let growable = self.growable_bytes();
            if growable == 0 {
                return Err(Error::without_details(ErrorId::CantGrow));
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
    const __DUP_ALLOWED: bool;
}

pub struct DupAllowed(());

impl DupPolicy for DupAllowed {
    #[doc(hidden)]
    const __DUP_ALLOWED: bool = true;
}

impl sealed::Sealed for DupAllowed {}

pub struct DupNotAllowed(());

impl DupPolicy for DupNotAllowed {
    #[doc(hidden)]
    const __DUP_ALLOWED: bool = false;
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
    pub(in crate) const ENTRY_LEN: usize = K::LEN + V::LEN;

    pub fn open(path: impl AsRef<Path>, mode: Mode) -> Result<Self> {
        Self::new(path, mode, false)
    }

    pub fn create_new(path: impl AsRef<Path>, mode: Mode) -> Result<Self> {
        Self::new(path, mode, true)
    }

    fn new(path: impl AsRef<Path>, mode: Mode, create_new: bool) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let mut open_opts = OpenOptions::new();
        open_opts
            .write(true)
            .read(true);
        if create_new {
            open_opts
                .create_new(true);
        }
        let file = open_opts.open(&path)
            .wrap_err_id(ErrorId::Io)?;

        let file_len = file.metadata().wrap_err_id(ErrorId::Io)?.len();
        if file_len >= usize::max_value() as u64 {
            return Err(Error::new(ErrorId::Corrupted, "index file is too big to be mmaped"));
        }

        let file_len = file_len as usize;
        if file_len % Self::ENTRY_LEN != 0 {
            return Err(Error::new(ErrorId::Corrupted,format!(
                "index file len ({}) is not a multiply of entry len ({})",
                file_len, Self::ENTRY_LEN)));
        }
        let len_bytes = file_len;

        let mut capacity_bytes = file_len;
        let (preallocate_bytes, max_capacity_bytes) = if let Mode::ReadWrite { preallocate, max_capacity } = mode {
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

        let mmap = Self::mmap(&file, max_capacity_bytes).wrap_err_id(ErrorId::Io)?;

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

    pub fn last_key(&self) -> Option<K> {
        self.last_entry().map(|(k, _)| k)
    }

    pub fn last_value(&self) -> Option<V> {
        self.last_entry().map(|(_, v)| v)
    }

    pub fn entry_by_key(&self, key: K) -> Option<(K, V)> {
        self.entry_by(|buf| Self::binary_search_by_key(buf, key))
    }

    pub fn value_by_key(&self, key: K) -> Option<V> {
        self.entry_by_key(key).map(|(_, v)| v)
    }

    pub fn entry_by_value(&self, value: V) -> Option<(K, V)> {
        self.entry_by(|buf| Self::binary_search_by_value(buf, value))
    }

    pub fn key_by_value(&self, value: V) -> Option<K> {
        self.entry_by_value(value).map(|(k, _)| k)
    }

    pub fn push(&self, key: K, value: V) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.ensure_capacity(&self.file)?;
        if let Some((last_key, last_value)) = inner.last_entry(&self.mmap) {
            Self::check_field::<_, KP>(key, last_key)?;
            Self::check_field::<_, DupNotAllowed>(value, last_value)?;
        }

        let buf = unsafe { inner.free_buf(&self.mmap) }.unwrap();
        Self::encode_entry(buf, key, value);

        inner.len_bytes += Self::ENTRY_LEN;

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.mmap.as_ref().unwrap().flush().wrap_err_id(ErrorId::Io).map_err(|e| e.into())
    }

    /// Similar to `compact()` but also shrinks `max_capacity` effectively making this index
    /// read-only (non-growable).
    pub fn make_read_only(&self) -> Result<()> {
        self.compact0(true)
    }

    /// Sets capacity to the amount this index actually uses. Doesn't change `max_capacity` so
    /// it's still possible push entries afterwards unless `make_read_only()` was previously called.
    pub fn compact(&self) -> Result<()> {
        self.compact0(false)
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
            }.wrap_err_id(ErrorId::Io)?)
        })
    }

    fn used_buf(&self) -> &[u8] {
        // TODO This can be made fully non-blocking by using atomic len.
        self.inner.lock().used_buf(&self.mmap).unwrap_or(&[])
    }

    fn binary_search<DP, F>(buf: &[u8], f: F) -> StdResult<usize, usize>
            where DP: DupPolicy, F: Fn(&[u8]) -> Ordering {
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
            if !DP::__DUP_ALLOWED && cmp == Ordering::Equal {
                return Ok(mid);
            }
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
        Self::binary_search::<KP, _>(buf, |e| Self::decode_key(e).cmp(&key))
    }

    fn binary_search_by_value(buf: &[u8], value: V) -> StdResult<usize, usize> {
        Self::binary_search::<DupNotAllowed, _>(buf, |e| Self::decode_value(e).cmp(&value))
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

    fn check_field<T: Field, DP: DupPolicy>(new: T, last: T) -> Result<()> {
        match new.cmp(&last) {
            Ordering::Less => Err(Error::without_details(ErrorId::PushMisordered)),
            Ordering::Equal => if DP::__DUP_ALLOWED {
                Ok(())
            } else {
                Err(Error::without_details(ErrorId::PushMisordered))
            }
            Ordering::Greater => Ok(()),
        }
    }

    fn entry_by(&self, f: impl FnOnce(&[u8]) -> StdResult<usize, usize>) -> Option<(K, V)> {
        let buf = self.used_buf();
        let i = match f(buf) {
            Ok(i) => i,
            Err(i) => if i == 0 {
                return None;
            } else {
                i - Self::ENTRY_LEN
            }
        };
        Some(Self::decode_entry(&buf[i..]))
    }

    fn compact0(&self, committed: bool) -> Result<()> {
        if let Some(mmap) = self.mmap.as_ref() {
            mmap.flush().wrap_err_id(ErrorId::Io)?;
        }
        let mut inner = self.inner.lock();
        if !committed && inner.capacity_bytes <= inner.len_bytes {
            return Ok(());
        }
        set_file_len(&self.file, inner.len_bytes, committed)?;
        inner.capacity_bytes = inner.len_bytes;
        if committed {
            inner.max_capacity_bytes = inner.len_bytes;
        }
        Ok(())
    }
}

impl<K: Field, V: Field, KP: DupPolicy> Drop for Index<K, V, KP> {
    fn drop(&mut self) {
        let _ = self.make_read_only()
            .map_err(|e| error!("[{:?}] error compacting index: {:?}",
                self.path, e));
    }
}

fn set_file_len(file: &File, len: usize, committed: bool) -> Result<()> {
    // TODO this is not going to work on Windows: can't resize file while it's mmap'ed.
    file.set_len(len.checked_add(!committed as usize).unwrap() as u64)
        .wrap_err_id(ErrorId::Io)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use super::ErrorId;
    use std::fs;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn threaded() {
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
            let idx: Arc<Index<u64, u32>> = Arc::new(Index::open(&f, Mode::ReadWrite {
                preallocate: PREALLOCATE,
                max_capacity: MAX_CAPACITY,
            }).unwrap());

            let readers: Vec<_> = (0..THREAD_COUNT)
                .map(clone!(idx => move |_| thread::spawn(clone!(idx => move || {
                    for _ in 0..READ_COUNT {
                        let len = idx.len();
                        if len > 0 {
                            let n = thread_rng().gen_range(0, len);
                            let k = n as u64 * 100;
                            let v = n as u32 * 10;
                            assert_eq!(idx.entry_by_key(k), Some((k, v)));
                            assert_eq!(idx.entry_by_value(v), Some((k, v)));

                            if thread_rng().gen_range(0, 100) < 10 {
                                idx.compact().unwrap();
                            }
                        }
                    }
                }))))
                .collect();

            for k in 0..ENTRY_COUNT {
                idx.push(k as u64 * 100, k as u32 * 10).unwrap();
                if thread_rng().gen_range(0, 100) < 15 {
                    thread::sleep(Duration::from_millis(1));
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
            fs::remove_file(&f).unwrap();
            let idx: Index<u32, u64> = Index::create_new(&f, Mode::ReadWrite {
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
                assert_eq!(idx.push(6, 106).unwrap_err().id(), &ErrorId::CantGrow.into());
            }

            for k in 1..=5 {
                assert_eq!(idx.entry_by_key(k), Some((k, 100 + k as u64)));
            }
        }
        assert_eq!(f_len(), 5 * 12);
        {
            let idx: Index<u32, u64> = Index::open(&f, Mode::ReadOnly).unwrap();

            assert_eq!(idx.len(), 5);
            for k in 1..=5 {
                assert_eq!(idx.entry_by_key(k), Some((k, 100 + k as u64)));
            }

            for _ in 0..5 {
                assert_eq!(idx.push(6, 106).unwrap_err().id(), &ErrorId::CantGrow.into());
            }
        }
    }

    fn err_id<T>(r: Result<T>) -> crate::error::ErrorId {
        r.err().unwrap().id().clone()
    }

    #[test]
    fn push_misorder_key_no_dup() {
        let f = mktemp::Temp::new_file().unwrap();
        let idx: Index<u32, u64> = Index::open(&f, Mode::ReadWrite {
            preallocate: 5,
            max_capacity: 5,
        }).unwrap();
        assert!(idx.push(100, 200).is_ok());
        assert!(idx.push(101, 201).is_ok());
        assert_eq!(err_id(idx.push(101, 201)), ErrorId::PushMisordered.into());
        assert_eq!(err_id(idx.push(100, 200)), ErrorId::PushMisordered.into());
        assert_eq!(err_id(idx.push(101, 200)), ErrorId::PushMisordered.into());
        assert_eq!(err_id(idx.push(100, 201)), ErrorId::PushMisordered.into());
        assert_eq!(err_id(idx.push(101, 202)), ErrorId::PushMisordered.into());
        assert_eq!(err_id(idx.push(102, 201)), ErrorId::PushMisordered.into());
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.entry_by_key(100), Some((100, 200)));
        assert_eq!(idx.entry_by_key(101), Some((101, 201)));
    }

    #[test]
    fn push_misorder_key_dup_allowed() {
        let f = mktemp::Temp::new_file().unwrap();
        let idx: Index<u64, u32, DupAllowed> = Index::open(&f, Mode::ReadWrite {
            preallocate: 5,
            max_capacity: 5,
        }).unwrap();
        assert!(idx.push(100, 200).is_ok());
        assert!(idx.push(101, 201).is_ok());
        assert!(idx.push(101, 202).is_ok());
        assert_eq!(err_id(idx.push(101, 201)), ErrorId::PushMisordered.into());
        assert_eq!(err_id(idx.push(101, 200)), ErrorId::PushMisordered.into());
        assert_eq!(err_id(idx.push(101, 199)), ErrorId::PushMisordered.into());
        assert_eq!(err_id(idx.push(100, 203)), ErrorId::PushMisordered.into());
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn search_with_dup_allowed() {
        let f = mktemp::Temp::new_file().unwrap();
        let idx: Index<u64, u32, DupAllowed> = Index::open(&f, Mode::ReadWrite {
            preallocate: 5,
            max_capacity: 5,
        }).unwrap();
        idx.push(101, 201).unwrap();
        idx.push(101, 202).unwrap();
        idx.push(101, 203).unwrap();
        idx.push(102, 12345).unwrap();
        assert_eq!(idx.value_by_key(101), Some(203));
        assert_eq!(idx.value_by_key(100), None);
        assert_eq!(idx.value_by_key(102), Some(12345));
        assert_eq!(idx.entry_by_key(103), Some((102, 12345)));
    }
}