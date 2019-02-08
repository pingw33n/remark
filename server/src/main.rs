extern crate remark_common as rcommon;
extern crate remark_log as rlog;
extern crate remark_proto as rproto;

use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, BufMut};
use if_chain::if_chain;
use prost::{Message as PMessage};
use std::collections::HashMap;
use std::net::TcpListener;
use std::io;
use std::io::prelude::*;

use rcommon::bytes::*;
use rcommon::error::*;
use rcommon::varint::{self, ReadExt, WriteExt};
use rlog::entry::{BufEntry, BufEntryBuilder};
use rlog::error::{ErrorId, Result};
use rlog::log::Log;
use rlog::message::MessageBuilder;
use rproto::*;

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
            entries: &mut [(BufEntry, Vec<u8>)]) -> push::Response {
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

fn read_pb_frame<M: Default + PMessage, R: Read>(rd: &mut R) -> io::Result<M> {
    let len = rd.read_u32_varint()?;
    let mut buf = Vec::new();
    buf.ensure_len_zeroed(len as usize);
    rd.read_exact(&mut buf)?;

    M::decode(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData,
        format!("malformed protobuf data: {}", e)))
}

fn write_pb_frame(wr: &mut Write, msg: impl PMessage) -> io::Result<usize> {
    let len = msg.encoded_len();
    let mut buf = Vec::new();
    buf.reserve_exact(varint::encoded_len(len as u64) as usize + len);
    buf.write_u32_varint(len as u32)?;
    msg.encode(&mut buf).unwrap();
    wr.write_all(&buf)?;
    Ok(len)
}

fn read_entries(rd: &mut impl Read, count: usize) -> Result<Vec<(BufEntry, Vec<u8>)>> {
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let mut buf = Vec::new();
        let entry = BufEntry::read_full(rd, &mut buf)?.ok_or_else(||
            Error::new(rlog::entry::ErrorId::Io, "entries truncated"))?;
        entries.push((entry, buf));
    }
    Ok(entries)
}

fn main() {
    env_logger::init();
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
                let stream = &mut stream;
                println!("new connectoin: {}", stream.peer_addr().unwrap());

                loop {
                    let req = Request::default();
                    measure_time::print_time!("");
                    let req = match read_pb_frame::<Request, _>(stream) {
                        Ok(v) => v,
                        Err(e) => {
                            dbg!(e);
                            break;
                        }
                    };

                    match req.request.unwrap() {
                        request::Request::Push(req) => {
                            let mut entries = read_entries(stream, req.entries.len()).unwrap();
                            let resp = server.push(&req, &mut entries);
                            write_pb_frame(stream, Response { response: Some(response::Response::Push(resp)) }).unwrap();
                        }
                    }


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
