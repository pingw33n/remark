#[macro_use] extern crate prost_derive;

mod root {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}
pub use root::*;

pub mod common {
    include!(concat!(env!("OUT_DIR"), "/common.rs"));
}
pub mod push {
    include!(concat!(env!("OUT_DIR"), "/push.rs"));

    impl Response {
        pub fn empty(status: super::common::Status) -> Self {
            Self {
                common: Some(super::common::Response { status: status.into() }),
                ..Default::default()
            }
        }
    }
}