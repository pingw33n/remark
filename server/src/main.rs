use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, BufMut};
use if_chain::if_chain;
use prost::Message;
use std::collections::HashMap;
use std::net::TcpListener;
use std::io::prelude::*;

use remark_log::entry::{BufEntry, BufEntryBuilder};
use remark_log::error::{ErrorId, Result};
use remark_log::bytes::BytesMut;
use remark_log::log::Log;
use remark_log::message::MessageBuilder;
use remark_proto::*;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

struct Topic {
    shards: Vec<Shard>,
}

struct Shard {
    log: Log,
}

struct Server {
    topics: HashMap<String, Topic>,
}

impl Server {
    pub fn new() -> Result<Self> {
        let mut topics = HashMap::new();
        topics.insert("topic1".to_owned(), Topic {
            shards: vec![
                Shard {
                    log: Log::open_or_create("/tmp/remark/topic1/000000", Default::default())?,
                },
                Shard {
                    log: Log::open_or_create("/tmp/remark/topic1/000001", Default::default())?,
                },
            ]
        });
        Ok(Self {
            topics,
        })
    }

    pub fn push(&mut self, request: &push::Request,
            entries: &mut [(BufEntry, BytesMut)]) -> push::Response {
        use remark_proto::common::{self, Status};
        use remark_proto::push::*;

        if request.entries.len() != entries.len() {
            return Response::empty(Status::BadRequest);
        }

        let mut resp = Response::default();
        resp.common = Some(common::Response::default());
        resp.entries = Vec::with_capacity(entries.len());

        for (req_entry, (entry, buf)) in request.entries.iter().zip(entries.iter_mut()) {
            let status = if let Some(topic) = self.topics.get_mut(&req_entry.topic_name) {
                if let Some(shard) = topic.shards.get_mut(req_entry.shard_id as usize) {
                    measure_time::print_time!("push");
                    match shard.log.push(entry, buf) {
                        Ok(()) => Status::Ok,
                        Err(e) => {
                            dbg!(e);
                            Status::BadRequest
                        }
                    }
                } else {
                    Status::BadTopicShardId
                }
            } else {
                Status::BadTopicName
            };
            if status != Status::Ok {
                resp.common.as_mut().unwrap().status = Status::InnerErrors.into();
            }
            resp.entries.push(response::Entry { status: status.into() });
        }

        resp
    }
}

fn main() {
    let push = push::Request {
        entries: vec![
            push::request::Entry {
                topic_name: "topic1".into(),
                shard_id: 0,
            }
        ]
    };

    let mut b = BufEntryBuilder::dense();
    b.compression(remark_log::entry::Codec::Lz4);
    (0..100000)
        .for_each(|i| { b.message(MessageBuilder { value: Some(format!("test {}", i).into()), ..Default::default() }); });
    let (entry, buf) = b.build();
    std::fs::File::create("/tmp/rementry_big.bin").unwrap().write_all(&buf).unwrap();

    BufEntry::decode(&buf).unwrap().unwrap().validate_body(&buf,
        remark_log::entry::ValidBody { dense: true, without_timestamp: true } ).unwrap();

    let mut server = Server::new().unwrap();

    let l = TcpListener::bind("0.0.0.0:4820").unwrap();
    for stream in l.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("new connectoin: {}", stream.peer_addr().unwrap());

                loop {
                    measure_time::print_time!("");
                    let len = match stream.read_u32::<BigEndian>() {
                        Ok(v) => v,
                        Err(_) => break,
                    };
                    let mut buf = Vec::new();
                    buf.resize(len as usize, 0);
                    stream.read_exact(&mut buf).unwrap();

                    let req = push::Request::decode(&buf).unwrap();

                    let mut entries = Vec::new();
                    for _ in 0..req.entries.len() {
                        let len = stream.read_u32::<BigEndian>().unwrap();
                        let mut buf = BytesMut::new();
                        buf.ensure_len(len as usize);
                        BigEndian::write_u32(buf.as_mut_slice(), len);
                        stream.read_exact(&mut buf.as_mut_slice()[4..]).unwrap();
                        let entry = BufEntry::decode(&buf).unwrap().unwrap();
                        entries.push((entry, buf));
                    }

                    let resp = server.push(&req, &mut entries);
                    let mut buf = Vec::new();
                    buf.write_u32::<BigEndian>(resp.encoded_len() as u32).unwrap();
                    resp.encode(&mut buf).unwrap();
                    stream.write_all(&buf).unwrap();
                }
            }
            Err(e) => {
                dbg!(e);
            }
        }
    }

    dbg!(server.push(&push, &mut [(entry, buf)]));

    let mut buf = Vec::with_capacity(push.encoded_len());
    push.encode(&mut buf).unwrap();

    dbg!(buf.len());

    let push2 = push::Request::decode(&buf).unwrap();
    dbg!(push2);
}
