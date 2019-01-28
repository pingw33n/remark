use failure::{Backtrace, Context, Fail as TFail};
use std::borrow::Cow;
use std::fmt;
use std::io;

pub use failure::ResultExt;
pub use failure_derive::Fail;

pub type Result<T> = std::result::Result<T, Error>;

struct Msg {
    msg: Cow<'static, str>,
    next: Option<Box<Self>>,
}

pub struct Error {
    inner: Context<ErrorKind>,
    msg: Option<Msg>,
}

impl Error {
    pub fn kind(&self) -> &ErrorKind {
        self.inner.get_context()
    }

    pub fn more_context(mut self, msg: impl Into<Cow<'static, str>>) -> Self {
        let new_msg = Msg {
            msg: msg.into(),
            next: None,
        };
        if self.msg.is_some() {
            let mut msg = self.msg.as_mut();
            while let Some(m) = msg {
                if m.next.is_none() {
                    m.next = Some(Box::new(new_msg));
                    break;
                }
                msg = m.next.as_mut().map(|v| v.as_mut());
            }
        } else {
            self.msg = Some(new_msg);
        }
        self
    }
}

pub trait ResultExt2 {
    fn more_context(self, msg: impl Into<Cow<'static, str>>) -> Self where Self: Sized {
        self.with_more_context(|_| msg)
    }

    fn with_more_context<R, F>(self, f: F) -> Self
    where
        Self: Sized,
        F: FnOnce(&Error) -> R,
        R: Into<Cow<'static, str>>;
}

impl<T> ResultExt2 for Result<T> {
    fn with_more_context<R, F>(self, f: F) -> Self
    where
        F: FnOnce(&Error) -> R,
        R: Into<Cow<'static, str>>
    {
        self.map_err(|e| {
            let msg = f(&e);
            e.more_context(msg)
        })
    }
}

impl TFail for Error {
    fn cause(&self) -> Option<&TFail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)?;
        if let Some(bt) = self.backtrace() {
            writeln!(f);
            write!(f, "{}", bt)?;
        }
        Ok(())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut it = TFail::iter_chain(&self.inner);
        write!(f, "{}", it.next().unwrap())?;

        let mut msg = self.msg.as_ref();
        while let Some(m) = msg {
            writeln!(f);
            write!(f, "  ...while {}", m.msg)?;
            msg = m.next.as_ref().map(|v| v.as_ref());
        }

        for e in it {
            writeln!(f);
            write!(f, "  ...{}", e)?;
        }
        Ok(())
    }
}

#[derive(Debug, Eq, Fail, PartialEq)]
pub enum ErrorKind {
    #[fail(display = "{}", _0)]
    Entry(crate::entry::Error),

    #[fail(display = "{}", _0)]
    Index(crate::index::Error),

    #[fail(display = "{}", _0)]
    Log(crate::log::Error),

    #[fail(display = "{}", _0)]
    Segment(crate::segment::Error),
}

macro_rules! impl_from {
    ($($ty:ty) *) => {
        $(impl From<$ty> for Error {
            fn from(v: $ty) -> Self {
                Self {
                    inner: Context::new(v.into()),
                    msg: None,
                }
            }
        }

        impl From<Context<$ty>> for Error {
            fn from(v: Context<$ty>) -> Self {
                Self {
                    inner: v.map(|v| v.into()),
                    msg: None,
                }
            }
        })*
    };
}

macro_rules! impl_kind_from {
    ($($var:tt => $ty:ty,)*) => {
        $(impl From<$ty> for ErrorKind {
            fn from(v: $ty) -> Self {
                ErrorKind::$var(v)
            }
        })*
    };
}

impl_from!(
    ErrorKind
    crate::entry::Error
    crate::index::Error
    crate::log::Error
    crate::segment::Error
);

impl_kind_from!(
    Entry => crate::entry::Error,
    Index => crate::index::Error,
    Log => crate::log::Error,
    Segment => crate::segment::Error,
);