use backtrace::Backtrace;
use std::borrow::Cow;
use std::error::{Error as StdError};
use std::fmt;
use std::cell::RefCell;

/// Generic chained error type.
///
/// Consists of:
/// - Error ID - a lightweight type, likely a `Copy`able and `Eq`able enum, that can be used to
///   easily match the error kind.
/// - Optional details message. This message provides context to the error and is written next
///   to the id when error is displayed.
/// - Optional cause. This is a boxed `std::error::Error` which is assumed to be the logical cause
///   of this error. Written below the the error ID + details when error is displayed.
/// - Backtrace of the location where error was first created. Shown only when debug format is
///   requested.
/// - Arbitrary context stack. When error is propagated upwards different layers can push
///   context messages onto the context stack. This allows additional information to be stored
///   with the message for even better of understanding of the error reason and context.
pub struct Error<Id> {
    id: Id,
    details: Option<Cow<'static, str>>,
    cause: Option<Box<'static + StdError>>,
    backtrace: RefCell<Backtrace>,
    context: Option<Context>,
}

impl<T> Error<T> {
    pub fn new(id: impl Into<T>, details: impl Into<Cow<'static, str>>) -> Self {
        Self::new0(id, Some(details.into()), None)
    }

    pub fn without_details(id: impl Into<T>) -> Self {
        Self::new0(id, None, None)
    }

    pub fn with_cause(self, cause: impl 'static + StdError) -> Self {
        Self::new0(self.id, self.details, Some(Box::new(cause)))
    }

    pub fn with_context(mut self, message: impl Into<Cow<'static, str>>) -> Self {
        let new_ctx = Context {
            message: message.into(),
            next: None,
        };
        if self.context.is_some() {
            let mut ctx = self.context.as_mut();
            while let Some(m) = ctx {
                if m.next.is_none() {
                    m.next = Some(Box::new(new_ctx));
                    break;
                }
                ctx = m.next.as_mut().map(|v| v.as_mut());
            }
        } else {
            self.context = Some(new_ctx);
        }
        self
    }

    pub fn id(&self) -> &T {
        &self.id
    }

    pub fn details(&self) -> Option<&Cow<'static, str>> {
        self.details.as_ref()
    }

    fn new0(id: impl Into<T>, details: Option<Cow<'static, str>>,
            cause: Option<Box<'static + StdError>>) -> Self {
        Self {
            id: id.into(),
            details,
            cause,
            backtrace: RefCell::new(Backtrace::new_unresolved()),
            context: None,
        }
    }

    fn print_backtrace(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut bt = self.backtrace.borrow_mut();
        bt.resolve();
        write!(f, "{:?}", bt)
    }
}

impl<T: fmt::Debug + fmt::Display> StdError for Error<T> {
    fn source(&self) -> Option<&('static + StdError)> {
        self.cause.as_ref().map(|v| &**v)
    }
}

impl<T: fmt::Debug + fmt::Display> fmt::Debug for Error<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)?;
        writeln!(f)?;
        self.print_backtrace(f)
    }
}

impl<T: fmt::Debug + fmt::Display> fmt::Display for Error<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.id)?;
        if let Some(details) = self.details.as_ref() {
            write!(f, ": {}", details)?;
        }
        write!(f, " ({:?})", self.id)?;

        if let Some(cause) = self.source() {
            writeln!(f)?;
            write!(f, "   => caused by: {}", cause)?;
        }

        let mut ctx = self.context.as_ref();
        while let Some(c) = ctx {
            writeln!(f)?;
            write!(f, "   ...while {}", c.message)?;
            ctx = c.next.as_ref().map(|v| v.as_ref());
        }

        Ok(())
    }
}

struct Context {
    message: Cow<'static, str>,
    next: Option<Box<Self>>,
}

pub trait ResultExt<T, E> {
    fn wrap_err<IdIn, IdOut, D>(self, id: IdIn, details: D) -> Result<T, Error<IdOut>>
        where IdOut: fmt::Debug + fmt::Display,
              IdIn: Into<IdOut>,
              D: Into<Cow<'static, str>>,
              Self: Sized,
    {
        self.wrap_err_with(|_| (id, details))
    }

    fn wrap_err_with<IdIn, IdOut, M, F>(self, f: F) -> Result<T, Error<IdOut>>
            where IdOut: fmt::Debug + fmt::Display,
                  IdIn: Into<IdOut>,
                  M: Into<Cow<'static, str>>,
                  F: FnOnce(&E) -> (IdIn, M);

    fn wrap_err_id<IdIn, IdOut>(self, id: IdIn) -> Result<T, Error<IdOut>>
        where IdOut: fmt::Debug + fmt::Display,
              IdIn: Into<IdOut>;
}

impl<T, E: 'static + StdError> ResultExt<T, E> for Result<T, E> {
    fn wrap_err_with<IdIn, IdOut, D, F>(self, f: F) -> Result<T, Error<IdOut>>
            where IdOut: fmt::Debug + fmt::Display,
                  IdIn: Into<IdOut>,
                  D: Into<Cow<'static, str>>,
                  F: FnOnce(&E) -> (IdIn, D) {
        self.map_err(move |cause| {
            let (id, details) = f(&cause);
            Error::new(id, details).with_cause(cause)
        })
    }

    fn wrap_err_id<IdIn, IdOut>(self, id: IdIn) -> Result<T, Error<IdOut>>
        where IdOut: fmt::Debug + fmt::Display,
              IdIn: Into<IdOut>,
    {
        self.map_err(move |cause| Error::without_details(id).with_cause(cause))
    }
}

pub trait ResultErrorExt<T> {
    fn context(self, msg: impl Into<Cow<'static, str>>) -> Self
            where Self: Sized
    {
        self.context_with(|_| msg)
    }

    fn context_with<R, F>(self, f: F) -> Self
    where
        Self: Sized,
        F: FnOnce(&Error<T>) -> R,
        R: Into<Cow<'static, str>>;
}

impl<T, Id> ResultErrorExt<Id> for Result<T, Error<Id>> {
    fn context_with<R, F>(self, f: F) -> Self
    where
        F: FnOnce(&Error<Id>) -> R,
        R: Into<Cow<'static, str>>
    {
        self.map_err(|e| {
            let msg = f(&e);
            e.with_context(msg)
        })
    }
}
