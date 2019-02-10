#[macro_use] extern crate prost_derive;

mod root {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}
pub use root::*;

pub mod common {
    include!(concat!(env!("OUT_DIR"), "/common.rs"));
}

pub mod pull {
    include!(concat!(env!("OUT_DIR"), "/pull.rs"));
}

pub mod pull_more {
    include!(concat!(env!("OUT_DIR"), "/pull_more.rs"));
}

pub mod push {
    include!(concat!(env!("OUT_DIR"), "/push.rs"));

    impl Response {
        pub fn empty(error: super::common::Error) -> Self {
            Self {
                common: Some(super::common::Response { error: error.into() }),
                ..Default::default()
            }
        }
    }
}