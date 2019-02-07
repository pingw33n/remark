use lazy_static::lazy_static;
use parking_lot::Mutex;
use rand::Error;
use rand::prelude::*;
use rand::distributions::Uniform;
use rand_xorshift::XorShiftRng;
use std::cell::UnsafeCell;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

lazy_static! {
    static ref RNG_SEED: u64 = {
        if let Some(seed) = std::env::var("TEST_RNG_SEED").ok()
                .and_then(|s| s.parse().ok()) {
            println!("[test_common] test RNG seed from TEST_RNG_SEED env var: {}", seed);
            seed
        } else {
            let seed = rand::random();
            println!("[test_common] test RNG seed from random(): {}", seed);
            seed
        }
    };
    static ref SEED_RNG: Arc<Mutex<XorShiftRng>> =
        Arc::new(Mutex::new(XorShiftRng::seed_from_u64(*RNG_SEED)));
}

thread_local! {
    static RNG: UnsafeCell<XorShiftRng> =
        UnsafeCell::new(XorShiftRng::seed_from_u64(SEED_RNG.lock().gen()));
}

pub fn thread_rng() -> ThreadRng {
    ThreadRng { rng: RNG.with(|t| t.get()) }
}

#[derive(Clone, Debug)]
pub struct ThreadRng {
    // use of raw pointer implies type is neither Send nor Sync
    rng: *mut XorShiftRng,
}

impl Default for ThreadRng {
    fn default() -> ThreadRng {
        thread_rng()
    }
}

impl RngCore for ThreadRng {
    #[inline(always)]
    fn next_u32(&mut self) -> u32 {
        unsafe { (*self.rng).next_u32() }
    }

    #[inline(always)]
    fn next_u64(&mut self) -> u64 {
        unsafe { (*self.rng).next_u64() }
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        unsafe { (*self.rng).fill_bytes(dest) }
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), Error> {
        unsafe { (*self.rng).try_fill_bytes(dest) }
    }
}

pub fn random_vec(len: impl RangeBounds<usize>) -> Vec<u8> {
    let start = match len.start_bound() {
        Bound::Included(v) => *v,
        Bound::Excluded(_) => unreachable!(),
        Bound::Unbounded => 0,
    };
    let end = match len.end_bound() {
        Bound::Included(v) => v.checked_add(1).unwrap(),
        Bound::Excluded(v) => *v,
        Bound::Unbounded => panic!("range end must be bounded"),
    };
    let len = thread_rng().gen_range(start, end);
    thread_rng().sample_iter(&Uniform::new_inclusive(0, 255))
        .take(len)
        .collect()
}