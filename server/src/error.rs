pub use rcommon::error::*;
pub use failure_derive::Fail;

pub type Error = rcommon::error::Error<ErrorId>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, Eq, Fail, PartialEq)]
pub enum ErrorId {
    #[fail(display = "IO error")]
    Io,

    #[fail(display = "TODO error")]
    Todo,
}