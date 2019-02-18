#[macro_use] extern crate prost_derive;

pub trait MessageExt: prost::Message {
    fn encode_as_vec(&self) -> Vec<u8> where Self: Sized {
        let mut vec = Vec::with_capacity(self.encoded_len());
        self.encode(&mut vec).unwrap();
        vec
    }
}

impl<T: prost::Message> MessageExt for T {}

mod root {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));

    impl From<super::ask_vote::Response> for Response {
        fn from(v: super::ask_vote::Response) -> Self {
            Self {
                response: Some(response::Response::AskVote(v)),
            }
        }
    }
}
pub use root::*;

pub mod common {
    include!(concat!(env!("OUT_DIR"), "/common.rs"));
}

pub mod ask_vote {
    include!(concat!(env!("OUT_DIR"), "/ask_vote.rs"));
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
        pub fn empty(status: super::common::Status) -> Self {
            Self {
                common: Some(super::common::Response { status: status.into() }),
                ..Default::default()
            }
        }
    }
}

pub mod cluster {
    include!(concat!(env!("OUT_DIR"), "/cluster.rs"));
}