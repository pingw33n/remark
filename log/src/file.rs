use parking_lot::{Mutex, MutexGuard};
use std::borrow::Borrow;
use std::fs;
use std::io::prelude::*;
use std::io::{Error, Result};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::Ordering;

use crate::util::atomic::AtomicU64;
use rcommon::io::BoundedRead;
use rcommon::util::*;

pub struct OpenOptions {
    read_only: bool,
    create: bool,
    create_new: bool,
    truncate: bool,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self {
            read_only: false,
            create: false,
            create_new: false,
            truncate: false,
        }
    }

    pub fn read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }

    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.create_new = create_new;
        self
    }

    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.truncate = truncate;
        self
    }

    pub fn open(&self, path: impl AsRef<Path>) -> Result<File> {
        File::new(path, &self)
    }
}

pub struct File {
    file: fs::File,
    len: AtomicU64,
    write_lock: Mutex<()>,
}

impl File {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        OpenOptions::new().open(path)
    }

    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .open(path)
    }

    pub fn len(&self) -> u64 {
        self.len.load(Ordering::SeqCst)
    }

    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.file.read_at(buf, offset)
    }

    pub fn reader(&self) -> Reader<&File> {
        Reader {
            file: self,
            position: 0,
        }
    }

    pub fn writer(&self) -> Writer {
        Writer {
            file: self,
            _lock: self.write_lock.lock(),
        }
    }

    pub fn sync_all(&self) -> Result<()> {
        self.file.sync_all()
    }

    pub fn sync_data(&self) -> Result<()> {
        self.file.sync_data()
    }

    pub fn metadata(&self) -> Result<std::fs::Metadata> {
        self.file.metadata()
    }

    pub fn set_permissions(&self, perm: std::fs::Permissions) -> Result<()> {
        self.file.set_permissions(perm)
    }

    pub fn truncate(&self, new_len: u64) -> Result<()> {
        let old_len = self.len();
        assert!(new_len <= old_len);
        self.file.set_len(new_len)?;
        if self.len.compare_and_swap(old_len, new_len, Ordering::SeqCst) != old_len {
            panic!("concurrent truncation and write detected");
        }
        Ok(())
    }

    fn new(path: impl AsRef<Path>, options: &OpenOptions) -> Result<Self> {
        let path = path.as_ref();
        let file = fs::OpenOptions::new()
            .read(true)
            .append(!options.read_only)
            .create(options.create)
            .create_new(options.create_new)
            .open(&path)?;

        if options.truncate {
            file.set_len(0)?;
        }

        let len = file.metadata()?.len();

        Ok(Self {
            file,
            len: AtomicU64::new(len),
            write_lock: Mutex::new(()),
        })
    }
}

pub struct Reader<F> {
    file: F,
    position: u64,
}

impl<F: Borrow<File>> Reader<F> {
    pub fn inner(&self) -> &F {
        &self.file
    }
}

impl<F: Borrow<File>> Read for Reader<F> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let read = self.file.borrow().read_at(buf, self.position)?;
        self.position += read as u64;
        Ok(read)
    }
}

impl<F: Borrow<File>> Reader<F> {
    pub fn file(&self) -> &File {
        self.file.borrow()
    }

    pub fn position(&self) -> u64 {
        self.position
    }

    pub fn set_position(&mut self, position: u64) {
        self.position = position;
    }

    pub fn advance(&mut self, delta: u64) {
        self.position = self.position.checked_add(delta).unwrap();
    }

    pub fn available(&self) -> u64 {
        let len = self.file.borrow().len();
        if self.position < len {
            len - self.position
        } else {
            0
        }
    }
}

impl<F: Borrow<File>> BoundedRead for Reader<F> {
    fn available(&self) -> Result<u64> {
        Ok(self.available())
    }

    fn is_eof(&self) -> Result<bool> {
        unimplemented!()
    }
}

impl<F: Borrow<File>> From<F> for Reader<F> {
    fn from(file: F) -> Self {
        Self {
            file,
            position: 0,
        }
    }
}

pub struct Writer<'a> {
    file: &'a File,
    _lock: MutexGuard<'a, ()>,
}

impl Write for Writer<'_> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let written = unsafe {
            use std::os::unix::io::AsRawFd;
            libc::write(self.file.file.as_raw_fd(), buf.as_ptr() as *const _, buf.len())
        };
        let written = if written >= 0 {
            written as usize
        } else {
            let err = Error::last_os_error();
            // Attempt to sync the len after failed write.
            let len = self.file.metadata()
                .expect_or_else(|e| format!(
                    "couldn't get file metadata after failed write: {:?}", e))
                .len();
            self.file.len.store(len, Ordering::SeqCst);
            return Err(err);
        };
        self.file.len.fetch_add(written as u64, Ordering::SeqCst);
        Ok(written)
    }

    fn flush(&mut self) -> Result<()> { Ok(()) }
}