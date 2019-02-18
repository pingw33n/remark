use std::fmt;
use std::time::Duration;

pub trait ResultExt<T, E> {
    fn expect_or_else<F, R>(self, f: F) -> T
        where R: fmt::Display,
              F: FnOnce(E) -> R;
}

impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn expect_or_else<F, R>(self, f: F) -> T
        where R: fmt::Display,
              F: FnOnce(E) -> R
    {
        match self {
            Ok(v) => v,
            Err(e) => panic!("{}", f(e)),
        }
    }
}

pub trait OptionResultExt<T, E> {
    fn transpose_(self) -> Result<Option<T>, E>;
}

impl<T, E> OptionResultExt<T, E> for Option<Result<T, E>> {
    fn transpose_(self) -> Result<Option<T>, E> {
        match self {
            Some(Ok(x)) => Ok(Some(x)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

pub trait ResultOptionExt<T, E> {
    fn transpose_(self) -> Option<Result<T, E>>;
}

impl<T, E> ResultOptionExt<T, E> for Result<Option<T>, E> {
    fn transpose_(self) -> Option<Result<T, E>> {
        match self {
            Ok(Some(x)) => Some(Ok(x)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub trait DurationExt {
    fn as_millis_u64(&self) -> Option<u64>;
}

impl DurationExt for Duration {
    fn as_millis_u64(&self) -> Option<u64> {
        let secs = self.as_secs();
        let sub_millis = self.subsec_millis() as u64;
        secs.checked_mul(1000)?.checked_add(sub_millis)
    }
}