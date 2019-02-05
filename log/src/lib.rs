#![deny(non_snake_case)]
#![deny(unused_imports)]
#![deny(unused_must_use)]

#[macro_use]
mod macros;

pub mod bytes;
pub mod entry;
pub mod error;
pub mod file;
pub mod index;
pub mod log;
pub mod message;
pub mod segment;
mod util;
