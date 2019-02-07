pub use failure_derive::Fail;
pub use remark_common::error::{ResultExt, ResultErrorExt};

pub type Error = remark_common::error::Error<ErrorId>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, Eq, Fail, PartialEq)]
pub enum ErrorId {
    #[fail(display = "{}", _0)]
    Entry(crate::entry::ErrorId),

    #[fail(display = "{}", _0)]
    Index(crate::index::ErrorId),

    #[fail(display = "{}", _0)]
    Message(crate::message::ErrorId),

    #[fail(display = "{}", _0)]
    Log(crate::log::ErrorId),

    #[fail(display = "{}", _0)]
    Segment(crate::segment::ErrorId),
}

impl From<crate::entry::BadMessages> for ErrorId {
    fn from(v: crate::entry::BadMessages) -> Self {
        ErrorId::Entry(crate::entry::ErrorId::BadMessages(v))
    }
}

impl From<crate::entry::BadBody> for ErrorId {
    fn from(v: crate::entry::BadBody) -> Self {
        ErrorId::Entry(crate::entry::ErrorId::BadBody(v))
    }
}

//macro_rules! impl_from {
//    ($($ty:ty) *) => {
//        $(impl From<$ty> for Error {
//            fn from(v: $ty) -> Self {
//                Self {
//                    inner: Context::new(v.into()),
//                    msg: None,
//                }
//            }
//        }
//
//        impl From<Context<$ty>> for Error {
//            fn from(v: Context<$ty>) -> Self {
//                Self {
//                    inner: v.map(|v| v.into()),
//                    msg: None,
//                }
//            }
//        })*
//    };
//}

macro_rules! impl_id_from {
    ($($var:tt => $ty:ty,)*) => {
        $(impl From<$ty> for ErrorId {
            fn from(v: $ty) -> Self {
                ErrorId::$var(v)
            }
        })*
    };
}

//impl_from!(
//    ErrorId
//    crate::entry::Error
//    crate::entry::BadBody
//    crate::entry::BadMessages
//    crate::index::Error
//    crate::log::Error
//    crate::message::Error
//    crate::segment::Error
//);

impl_id_from!(
    Entry => crate::entry::ErrorId,
    Index => crate::index::ErrorId,
    Log => crate::log::ErrorId,
    Message => crate::message::ErrorId,
    Segment => crate::segment::ErrorId,
);