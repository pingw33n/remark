use fs2::FileExt;
use std::fs::File;
use std::io::Result;
use std::path::Path;

/// File-based inter-process mutex implemented using advisory file locking.
#[must_use]
pub struct FileMutex {
    file: File,
}

impl FileMutex {
    pub fn try_lock(path: impl AsRef<Path>) -> Result<Self> {
        Self::new(path, false)
    }

    pub fn lock(path: impl AsRef<Path>) -> Result<Self> {
        Self::new(path, true)
    }

    pub fn new(path: impl AsRef<Path>, block: bool) -> Result<Self> {
        let path = path.as_ref();
        let file = File::create(path)?;
        if block {
            file.lock_exclusive()?;
        } else {
            file.try_lock_exclusive()?;
        }
        Ok(Self {
            file,
        })
    }
}

impl Drop for FileMutex {
    fn drop(&mut self) {
        self.file.unlock().unwrap();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rcommon::clone;
    use std::io::ErrorKind;
    use std::mem;
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[test]
    fn delete_locked() {
        let f = mktemp::Temp::new_file().unwrap();
        let p = f.to_path_buf();

        let _l = FileMutex::try_lock(&f).unwrap();

        mem::drop(f);
        assert!(!p.exists());

        assert!(FileMutex::try_lock(&p).is_ok());
    }

    #[test]
    fn single_thread() {
        let f = mktemp::Temp::new_file().unwrap();
        let f = f.to_path_buf();

        let l = FileMutex::try_lock(&f).unwrap();
        assert_eq!(FileMutex::try_lock(&f).err().unwrap().kind(), ErrorKind::WouldBlock);

        mem::drop(l);
        assert!(f.exists());

        File::create(&f).unwrap();

        let l = FileMutex::try_lock(&f).unwrap();
        assert_eq!(FileMutex::try_lock(&f).err().unwrap().kind(), ErrorKind::WouldBlock);

        mem::drop(l);
        assert!(f.exists());
    }

    #[test]
    fn multi_thread() {
        const THREAD_COUNT: usize = 32;
        const GROUP_COUNT: usize = 10;
        const GROUP_LEN: usize = 50;

        let mut lock_path = mktemp::Temp::new_file().unwrap();
        lock_path.release();

        let out = Arc::new(Mutex::new(Vec::new()));

        let writers: Vec<_> = (0..THREAD_COUNT)
            .map(clone!(out, lock_path => move |_| thread::spawn(clone!(out, lock_path => move || {
                for _ in 0..GROUP_COUNT {
                    let _lock = FileMutex::lock(&lock_path).unwrap();
                    for i in 0..GROUP_LEN {
                        let mut out = out.lock().unwrap();
                        out.push(i);
                    }
                }
            }))))
            .collect();

        for h in writers {
            h.join().unwrap();
        }

        let out = out.lock().unwrap();
        for i in 0..(THREAD_COUNT * GROUP_COUNT) {
            for v in 0..GROUP_LEN {
                assert_eq!(out[i * GROUP_LEN + v], v, "{}", i * GROUP_LEN + v);
            }
        }

        assert!(lock_path.as_ref().exists());
    }

    mod multi_process {
        use super::*;
        use rand::Rng;
        use rusty_fork::*;
        use std::env;
        use std::fs::{self, OpenOptions};
        use std::io::prelude::*;
        use std::path::PathBuf;
        use std::time::Duration;
        use crate::util::test_common::thread_rng;

        const PROCESS_COUNT: usize = 4;
        const GROUP_COUNT: usize = 20;
        const GROUP_LEN: usize = 20;

        fn path(name: &str) -> PathBuf {
            env::temp_dir().join(name)
        }

        fn lock_path(name: &str) -> PathBuf {
            path(&format!("{}.lock", name))
        }

        fn write(name: &str, truncate: bool) {
            let mut out = OpenOptions::new()
                .append(true)
                .create(true)
                .open(path(name))
                .unwrap();

            let lock_path = lock_path(name);

            if truncate {
                out.set_len(0).unwrap();
            } else {
                let len = out.metadata().unwrap().len();
                while out.metadata().unwrap().len() == len {}
            }

            for _ in 0..GROUP_COUNT {
                let _lock = FileMutex::lock(&lock_path).unwrap();
                for i in 0..GROUP_LEN {
                    let b = [i as u8];
                    out.write_all(&b[..]).unwrap();
                }
                if thread_rng().gen_range(0, 100) < 20 {
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }

        fn wait_for_len(path: impl AsRef<Path>, expected_file_len: u64) {
            let file = File::open(path.as_ref()).unwrap();
            while file.metadata().unwrap().len() < expected_file_len {
                thread::sleep(Duration::from_millis(50));
            }
            assert_eq!(file.metadata().unwrap().len(), expected_file_len);
        }

        fn check(name: &str) {
            let path = path(name);

            const EXPECTED_FILE_LEN: u64 = (PROCESS_COUNT * GROUP_COUNT * GROUP_LEN) as u64;
            wait_for_len(&path, EXPECTED_FILE_LEN);

            let mut file = File::open(&path).unwrap();

            fs::remove_file(&path).unwrap();
            fs::remove_file(lock_path(name)).unwrap();

            let mut vec = Vec::new();
            file.read_to_end(&mut vec).unwrap();
            for i in 0..GROUP_COUNT * PROCESS_COUNT {
                for v in 0..GROUP_LEN {
                    assert_eq!(vec[i * GROUP_LEN + v], v as u8,
                        "{}", i * GROUP_LEN + v);
                }
            }
        }

        const FILE_NAME: &'static str = "39c1eeee-f4e5-4511-8c9b-06004906568b";
        
        rusty_fork_test! {
        #![rusty_fork(timeout_ms = 30_000)]

        #[test]
        fn process_1() {
            write(FILE_NAME, true);
            check(FILE_NAME);
        }

        #[test]
        fn process_2() {
            write(FILE_NAME, false);
        }

        #[test]
        fn process_3() {
            write(FILE_NAME, false);
        }

        #[test]
        fn process_4() {
            write(FILE_NAME, false);
        }

        }
    }
}