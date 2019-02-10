extern crate remark_common as rcommon;
extern crate remark_log as rlog;
extern crate remark_proto as rproto;

use prost::{Message as PMessage};
use std::collections::HashMap;
use std::net::TcpListener;
use std::io;
use std::io::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rcommon::clone;
use rcommon::bytes::*;
use rcommon::error::*;
use rcommon::varint::{self, ReadExt, WriteExt};
use rlog::entry::{BufEntry, BufEntryBuilder};
use rlog::error::{ErrorId, Result};
use rlog::log::{self, Log};
use rlog::message::{Id, MessageBuilder, Timestamp};
use rproto::*;
use parking_lot::Mutex;
use rproto::common::{Error as ApiError};
use std::result::{Result as StdResult};
use std::time::{Duration, Instant};
use num_traits::cast::ToPrimitive;

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
        use remark_proto::common;
        use remark_proto::push::*;

        if request.entries.len() != entries.len() {
            return Response::empty(ApiError::BadRequest);
        }

        let mut resp = Response::default();
        resp.common = Some(common::Response::default());
        resp.entries = Vec::with_capacity(entries.len());

        for (req_entry, (entry, buf)) in request.entries.iter().zip(entries.iter_mut()) {
            let error = if let Some(topic) = self.topics.get_mut(&req_entry.topic_name) {
                if let Some(shard) = topic.shards.get_mut(req_entry.shard_id as usize) {
                    measure_time::print_time!("push");

                    let timestamp = if request.flags & request::Flags::UseTimestamps as u32 != 0 {
                        Some(Timestamp::now())
                    } else {
                        None
                    };
                    let options = log::Push {
                        dense: true,
                        timestamp,
                    };
                    match shard.log.push(entry, buf, options) {
                        Ok(()) => ApiError::None,
                        Err(e) => {
                            dbg!(e);
                            ApiError::BadRequest
                        }
                    }
                } else {
                    ApiError::BadShardId
                }
            } else {
                ApiError::BadTopicName
            };
            if error != ApiError::None {
                resp.common.as_mut().unwrap().error = ApiError::InnerErrors.into();
            }
            resp.entries.push(response::Entry { error: error.into() });
        }

        resp
    }

    pub fn pull(&self, topic_name: &str, shard_id: u32, message_id: Id) -> StdResult<rlog::log::Iter, ApiError> {
        let topic = self.topics.get(topic_name).ok_or(ApiError::BadTopicName)?;
        let shard = topic.shards.get(shard_id as usize).ok_or(ApiError::BadShardId)?;
        Ok(shard.log.iter(message_id..))
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

fn write_pb_frame(wr: &mut Write, msg: &impl PMessage) -> io::Result<usize> {
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

    let mut b = BufEntryBuilder::dense();
//    b.compression(remark_log::entry::Codec::Lz4);
    while b.get_frame_len() < 4096 {
        b.message(MessageBuilder { value: Some(format!("test {}", b.get_frame_len()).into()), ..Default::default() });
    }
    dbg!(b.message_count());
    let (_, buf) = b.build();
    std::fs::File::create("/tmp/rementry.bin").unwrap().write_all(&buf).unwrap();

    BufEntry::decode(&buf).unwrap().unwrap().validate_body(&buf,
        remark_log::entry::ValidBody { dense: true, without_timestamp: true } ).unwrap();

    let terminated = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::SIGINT, terminated.clone()).unwrap();
    signal_hook::flag::register(signal_hook::SIGTERM, terminated.clone()).unwrap();
    signal_hook::flag::register(signal_hook::SIGQUIT, terminated.clone()).unwrap();

    let server = Arc::new(Mutex::new(Server::new().unwrap()));

    let l = TcpListener::bind("0.0.0.0:4820").unwrap();
    l.set_nonblocking(true).unwrap();

    let workers = threadpool::ThreadPool::new(16);


    for net_stream in l.incoming() {
        match net_stream {
            Ok(mut net_stream) => {
                workers.execute(clone!(terminated, server => move || {
                    net_stream.set_nonblocking(false).unwrap();
                    let net_stream = &mut net_stream;
                    println!("new connectoin: {}", net_stream.peer_addr().unwrap());
                    measure_time::print_time!("session");

                    loop {
                        if terminated.load(Ordering::Relaxed) {
                            println!("shutting down");
                            return;
                        }
                        measure_time::print_time!("req");
                        let req = match read_pb_frame::<Request, _>(net_stream) {
                            Ok(v) => v,
                            Err(e) => {
                                dbg!(e);
                                break;
                            }
                        };

                        dbg!(&req);

                        match req.request.unwrap() {
                            request::Request::Pull(req) => {
                                #[derive(Clone, Copy, Debug, Eq, PartialEq)]
                                enum State {
                                    Writing,
                                    End,
                                }
                                struct Stream {
                                    iter: rlog::log::Iter,
                                    state: State,
                                }
                                let mut streams = Vec::with_capacity(req.streams.len());
                                for req_stream in &req.streams {
                                    let stream = server.lock().pull(&req_stream.topic_name,
                                            req_stream.shard_id, Id::new(req_stream.message_id))
                                        .map(|iter| Stream { iter, state: State::Writing });
                                    streams.push(stream);
                                }

                                let mut written = write_pb_frame(net_stream, &Response {
                                    response: Some(response::Response::Pull(
                                    pull::Response {
                                        common: Some(common::Response { error: ApiError::None.into() }),
                                    }
                                )) }).unwrap();

                                let timeout = Duration::from_millis(req.timeout_millis as u64);
                                let writte_limit = req.per_stream_limit_bytes.to_usize().unwrap();

                                let mut last_activity = Instant::now();
                                let mut first_pass = true;
                                loop {
                                    let mut done = true;

                                    let now = Instant::now();
                                    for (stream_id, stream) in streams.iter_mut().enumerate() {
                                        if let Err(err) = stream {
                                            if first_pass {
                                                let err = err.clone();
                                                dbg!(err);
                                                written += write_pb_frame(net_stream, &pull::StreamFrame {
                                                    stream_id: stream_id as u32,
                                                    error: err.into(),
                                                }).unwrap();
                                            }
                                            continue;
                                        }
                                        let stream = stream.as_mut().unwrap();
                                        if stream.state == State::End {
                                            continue;
                                        }
                                        if now - last_activity >= timeout || written >= writte_limit {
                                            written += write_pb_frame(net_stream, &pull::StreamFrame {
                                                stream_id: stream_id as u32,
                                                error: ApiError::EndOfStream.into(),
                                            }).unwrap();
                                            stream.state = State::End;
                                            continue;
                                        }
                                        match stream.iter.next() {
                                            Some(Ok(_)) => {
                                                stream.iter.complete_read().unwrap();
                                                written += write_pb_frame(net_stream, &pull::StreamFrame {
                                                    stream_id: stream_id as u32,
                                                    error: ApiError::None.into(),
                                                }).unwrap();
                                                let buf = stream.iter.buf();
                                                net_stream.write_all(buf).unwrap();
                                                written += buf.len();
                                                done = false;
                                            },
                                            None => {
                                                written += write_pb_frame(net_stream, &pull::StreamFrame {
                                                    stream_id: stream_id as u32,
                                                    error: ApiError::EndOfStream.into(),
                                                }).unwrap();
                                                stream.state = State::End;
                                            }
                                            Some(Err(e)) => {
                                                dbg!(e);
                                                written += write_pb_frame(net_stream, &pull::StreamFrame {
                                                    stream_id: stream_id as u32,
                                                    error: ApiError::UnknownError.into(),
                                                }).unwrap();
                                                stream.state = State::End;
                                            }
                                        }
                                        last_activity = now;
                                    }

                                    if done {
                                        break;
                                    }

                                    first_pass = false;
                                }
                            }
                            request::Request::Push(req) => {
                                let mut entries = read_entries(net_stream, req.entries.len()).unwrap();
                                let resp = server.lock().push(&req, &mut entries);
                                write_pb_frame(net_stream, &Response {
                                    response: Some(response::Response::Push(resp))
                                }).unwrap();
                            }
                        }
                    }
                }));
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    if terminated.load(Ordering::Relaxed) {
                        println!("shutting down");
                        return;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                } else {
                    dbg!(e);
                }
            }
        }
    }

    terminated.store(true, Ordering::Relaxed);

    workers.join();
}
