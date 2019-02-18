#![allow(dead_code)]
#![deny(non_snake_case)]
//#![deny(unused_imports)]
#![deny(unused_must_use)]

extern crate remark_common as rcommon;
extern crate remark_log as rlog;
extern crate remark_proto as rproto;

mod error;
mod rpc;

use prost::{Message as PMessage};
use std::collections::HashMap;
use std::io::prelude::*;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use rcommon::clone;
use rlog::entry::{BufEntry, BufEntryBuilder};
use rlog::log::Log;
use rlog::message::{Id, MessageBuilder, Timestamp};
use rproto::*;
use parking_lot::Mutex;
use rproto::common::Status;
use std::result::{Result as StdResult};
use std::time::{Duration, Instant};
use num_traits::cast::ToPrimitive;
use std::path::PathBuf;
use std::fs;
use std::path::Path;
use uuid::Uuid;
use log::*;
use crate::error::*;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub use rproto::cluster::partition::{Kind as PartitionKind};
use crate::rpc::RequestSession;

#[derive(Default)]
struct TopicStore {
    partitions: HashMap<u32, PartitionStore>,
}

struct PartitionStore {
    log: Log,
}

#[derive(Debug)]
struct ClusterMetadata {
    uuid: Uuid,
    nodes: HashMap<u32, NodeMetadata>,
    topics: HashMap<String, TopicMetadata>,
}

impl Default for ClusterMetadata {
    fn default() -> Self {
        Self {
            uuid: Uuid::nil(),
            nodes: HashMap::new(),
            topics: HashMap::new(),
        }
    }
}

impl ClusterMetadata {
    pub fn apply(&mut self, metadata: cluster::Metadata) {
        if metadata.metadata.is_none() {
            return;
        }
        use cluster::metadata::Metadata::*;
        match metadata.metadata.unwrap() {
            Cluster(cluster) => {
                self.uuid = cluster.uuid.parse().unwrap();
            }
            Node(node) => {
                let node = NodeMetadata::from(node);
                self.nodes.insert(node.id, node);
            }
            Topic(topic) => {
                let id = topic.id.clone();
                let topic = TopicMetadata::from(topic);
                self.topics.insert(id, topic);
            }
        }
    }

    pub fn apply_log(&mut self, mut log: rlog::log::Iter) -> Result<()> {
        while let Some(entry) = log.next() {
            log.complete_read().wrap_err_id(ErrorId::Todo)?;
            let entry = entry.wrap_err_id(ErrorId::Todo)?;
            let mut msg_it = entry.iter(log.buf());
            while let Some(msg) = msg_it.next() {
                let msg = msg.wrap_err_id(ErrorId::Todo)?;
                let value = msg.value(msg_it.msg_buf());
                if value.is_none() {
                    continue;
                }
                let meta = rproto::cluster::Metadata::decode(value.unwrap())
                    .wrap_err_id(ErrorId::Io)?;
                self.apply(meta);
            }
        }
        Ok(())
    }

    pub fn to_protos(&self) -> Vec<cluster::Metadata> {
        let mut r = Vec::new();
        r.push(cluster::Metadata {
            metadata: Some(cluster::metadata::Metadata::Cluster(cluster::Cluster {
                uuid: self.uuid.to_string(),
            })),
        });
        for node in self.nodes.values() {
            r.push(cluster::Metadata {
                metadata: Some(cluster::metadata::Metadata::Node(node.to_proto())),
            });
        }
        for topic in self.topics.values() {
            r.push(cluster::Metadata {
                metadata: Some(cluster::metadata::Metadata::Topic(topic.to_proto())),
            });
        }
        r
    }
}

#[derive(Debug)]
struct NodeMetadata {
    uuid: Uuid,
    id: u32,
    host: String,
    port: u16,
    system_port: u16,
}

impl NodeMetadata {
    pub fn to_proto(&self) -> cluster::Node {
        cluster::Node {
            uuid: self.uuid.to_string(),
            id: self.id,
            host: self.host.clone(),
            port: self.port as u32,
            system_port: self.system_port as u32,
        }
    }
}

impl From<cluster::Node> for NodeMetadata {
    fn from(v: cluster::Node) -> Self {
        Self {
            uuid: v.uuid.parse().unwrap(),
            id: v.id,
            host: v.host,
            port: v.port as u16,
            system_port: v.system_port as u16,
        }
    }
}

#[derive(Debug, Default)]
struct TopicMetadata {
    id: String,
    partitions: HashMap<u32, PartitionMetadata>,
}

impl TopicMetadata {
    fn to_proto(&self) -> cluster::Topic {
        cluster::Topic {
            id: self.id.clone(),
            partitions: self.partitions.iter()
                .map(|(_, v)| v.to_proto())
                .collect()
        }
    }
}

impl From<cluster::Topic> for TopicMetadata {
    fn from(v: cluster::Topic) -> Self {
        Self {
            id: v.id,
            partitions: v.partitions.into_iter()
                .map(|v| (v.id, PartitionMetadata::from(v)))
                .collect(),
        }
    }
}

#[derive(Debug)]
struct PartitionMetadata {
    id: u32,
    kind: PartitionKind,
    nodes: Vec<u32>,
    leader: Option<u32>,
}

impl PartitionMetadata {
    pub fn to_proto(&self) -> cluster::Partition {
        cluster::Partition {
            id: self.id,
            kind: self.kind as i32,
            nodes: self.nodes.clone(),
            leader: self.leader.map(|v| v.to_i32().unwrap()).unwrap_or(-1),
        }
    }
}

impl From<cluster::Partition> for PartitionMetadata {
    fn from(v: cluster::Partition) -> Self {
        Self {
            id: v.id,
            kind: PartitionKind::from_i32(v.kind).unwrap(),
            nodes: v.nodes,
            leader: v.leader.to_u32(),
        }
    }
}

struct ClusterController {
    metadata: ClusterMetadata,
}

struct PartitionController {
}

struct Store {
    paths: Vec<PathBuf>,
    topics: HashMap<String, TopicStore>,
}

impl Store {
    pub fn open_or_create<P: AsRef<Path>>(paths: &[P]) -> Result<Self> {
        assert!(paths.len() > 0);
        let paths: Vec<_> = paths.iter().map(|v| v.as_ref().to_path_buf()).collect();
        let mut topics = HashMap::new();
        for path in &paths {
            if !path.exists() {
                continue;
            }
            for dir_entry in fs::read_dir(&path).wrap_err_id(ErrorId::Io)? {
                let dir_entry = dir_entry.wrap_err_id(ErrorId::Io)?;
                let topic_name = dir_entry.file_name().to_str()
                    .ok_or_else(|| Error::without_details(ErrorId::Io))?
                    .to_owned();
                if !dir_entry.metadata().wrap_err_id(ErrorId::Io)?.is_dir() {
                    return Err(Error::without_details(ErrorId::Todo));
                }
                let mut partitions = HashMap::new();
                for dir_entry in fs::read_dir(dir_entry.path()).wrap_err_id(ErrorId::Io)? {
                    let dir_entry = dir_entry.wrap_err_id(ErrorId::Io)?;
                    let partition_id: u32 = dir_entry.file_name().to_str()
                        .ok_or_else(|| Error::without_details(ErrorId::Todo))?
                        .parse()
                        .map_err(|_| Error::without_details(ErrorId::Todo))
                        .unwrap();
                    let log = Self::open_or_create_log(dir_entry.path())?;
                    partitions.insert(partition_id, PartitionStore { log });
                }
                topics.insert(topic_name, TopicStore { partitions });
            }
        }
        Ok(Self {
            paths,
            topics,
        })
    }

    pub fn is_partition_exists(&self, topic_id: &str, partition_id: u32) -> bool {
        self.topics.get(topic_id).map(|t| t.partitions.get(&partition_id).is_some()).unwrap_or(false)
    }

    pub fn create_partition(&mut self, topic_id: &str, partition_id: u32) -> Result<()> {
        assert!(!self.is_partition_exists(topic_id, partition_id));
        let path = self.select_path().join(format!("{}/{:05}", topic_id, partition_id));
        let log = Self::open_or_create_log(path).wrap_err_id(ErrorId::Todo)?;
        let ts = self.topics.entry(topic_id.into()).or_insert_with(|| TopicStore::default());
        ts.partitions.insert(partition_id, PartitionStore { log });
        Ok(())
    }

    fn open_or_create_log(path: impl AsRef<Path>) -> Result<Log> {
        Log::open_or_create(path, Default::default()).wrap_err_id(ErrorId::Todo)
    }

    fn select_path(&self) -> &PathBuf {
        // FIXME
        &self.paths[0]
    }
}

const CLUSTER_METADATA_TOPIC: &'static str = "__cluster";

type NodeHandle = Arc<Mutex<Node>>;

struct Node {
    store: Store,
    id: u32,
    cluster_ctrl: Option<ClusterController>,
}

impl Node {
    pub fn bootstrap_cluster<P: AsRef<Path>>(paths: &[P], node_meta: NodeMetadata) -> Result<Self> {
        let mut store = Store::open_or_create(paths)
            .context("opening store")?;

        if !store.is_partition_exists(CLUSTER_METADATA_TOPIC, 0) {
            info!("cluster metadata doesn't exist, will bootstrap new cluster");
            let mut cluster_topic_meta = TopicMetadata::default();
            cluster_topic_meta.id = CLUSTER_METADATA_TOPIC.into();
            cluster_topic_meta.partitions.insert(0, PartitionMetadata {
                id: 0,
                kind: rproto::cluster::partition::Kind::SingleNode,
                nodes: vec![0],
                leader: Some(0),
            });

            let mut cluster_meta = ClusterMetadata::default();
            cluster_meta.uuid = Uuid::new_v4();;
            assert_eq!(node_meta.id, 0);
            cluster_meta.nodes.insert(0, node_meta);
            cluster_meta.topics.insert(CLUSTER_METADATA_TOPIC.into(), cluster_topic_meta);


            store.create_partition(CLUSTER_METADATA_TOPIC, 0)?;

            let mut b = BufEntryBuilder::dense();
            for meta in cluster_meta.to_protos() {
                b.message(MessageBuilder {
                    value: Some(meta.encode_as_vec()),
                    ..Default::default()
                });
            }
            let (ref mut entry, ref mut buf) = b.build();
            store.topics[CLUSTER_METADATA_TOPIC].partitions[&0].log.push(entry, buf, rlog::log::Push {
                timestamp: Some(Timestamp::now()),
                ..Default::default()
            }).wrap_err_id(ErrorId::Todo)?;
        } else {
            info!("cluster metadata exists, will bootstrap existing cluster");
        }

        let log = store.topics[CLUSTER_METADATA_TOPIC].partitions[&0].log.iter(..);
        let mut cluster_meta = ClusterMetadata::default();
        cluster_meta.apply_log(log)?;

        info!("{:#?}", cluster_meta);

        let cluster_ctrl = Some(ClusterController {
            metadata: cluster_meta,
        });

        Ok(Self {
            store,
            id: 0,
            cluster_ctrl,
        })
    }

    pub fn push(&mut self, topic_id: &str, partition_id: u32, use_timestamps: bool,
            entry: &mut BufEntry, buf: &mut Vec<u8>) -> Status {
        if let Some(topic) = self.store.topics.get_mut(topic_id) {
            if let Some(partition) = topic.partitions.get_mut(&partition_id) {
                measure_time::print_time!("push");

                let timestamp = if use_timestamps {
                    Some(Timestamp::now())
                } else {
                    None
                };
                let options = rlog::log::Push {
                    dense: true,
                    timestamp,
                };
                match partition.log.push(entry, buf, options) {
                    Ok(()) => Status::Ok,
                    Err(e) => {
                        dbg!(e);
                        Status::BadRequest
                    }
                }
            } else {
                Status::BadPartitionId
            }
        } else {
            Status::BadTopicName
        }
    }

    pub fn pull(&self, topic_name: &str, partition_id: u32, message_id: Id) -> StdResult<rlog::log::Iter, Status> {
        let topic = self.store.topics.get(topic_name).ok_or(Status::BadTopicName)?;
        let partition = topic.partitions.get(&partition_id).ok_or(Status::BadPartitionId)?;
        Ok(partition.log.iter(message_id..))
    }
}

struct Stream {
    iter: rlog::log::Iter,
    sent_eos: bool,
}

fn send_entry_streams(
    wr: &mut rpc::StreamWriter,
    streams: &mut Vec<StdResult<Stream, Status>>,
    timeout: Duration,
    limit_bytes: usize)
    -> Result<()>
{
    for stream in streams.iter_mut() {
        if let Ok(stream) = stream {
            stream.sent_eos = false;
        }
    }

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
                    wr.write(stream_id as u32, err, None)?;
                }
                continue;
            }
            let stream = stream.as_mut().unwrap();
            if stream.sent_eos {
                continue;
            }
            if timeout > Duration::from_millis(0) && now - last_activity >= timeout
                || wr.written() >= limit_bytes
            {
                wr.write(stream_id as u32, Status::EndOfStream, None)?;
                stream.sent_eos = true;
                continue;
            }
            match stream.iter.next() {
                Some(Ok(_)) => {
                    dbg!(stream_id);
                    stream.iter.complete_read().unwrap();
                    wr.write(stream_id as u32, Status::Ok, Some(stream.iter.buf()))?;
                    done = false;
                },
                None => if timeout == Duration::from_millis(0) {
                    wr.write(stream_id as u32, Status::EndOfStream, None)?;
                    stream.sent_eos = true;
                }
                Some(Err(e)) => {
                    error!("{}", e);
                    wr.write(stream_id as u32, Status::UnknownError, None)?;
                    stream.sent_eos = true;
                }
            }
            last_activity = now;
        }

        if done {
            break;
        }

        first_pass = false;
    }

    Ok(())
}

fn handle_push_request(
    req: rproto::push::Request,
    mut session: RequestSession,
    node: &NodeHandle)
    -> Result<()>
{
    use rproto::push::*;
    let mut resp = Response::default();
    resp.common = Some(rproto::common::Response::default());
    resp.entries = Vec::with_capacity(req.entries.len());

    {
        let mut entries = session.read_entries(req.entries.len());
        for req_entry in &req.entries {
            let status = if let Some(entry) = entries.next() {
                let mut entry = entry?;
                dbg!(&entry);
                let status = node.lock().push(
                    &req_entry.topic_id,
                    req_entry.partition_id,
                    req.flags & remark_proto::push::request::Flags::UseTimestamps as u32 != 0,
                    &mut entry,
                    entries.buf_mut());
                resp.entries.push(response::Entry { status: status.into() });
                status
            } else {
                return session.respond(&Response::empty(Status::BadRequest));
            };
            if status != Status::Ok {
                resp.common.as_mut().unwrap().status = Status::InnerErrors.into();
            }
        }
    }
    session.respond(&rproto::Response {
        response: Some(rproto::response::Response::Push(resp)),
    })
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

    let shutdown = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::SIGINT, shutdown.clone()).unwrap();
    signal_hook::flag::register(signal_hook::SIGTERM, shutdown.clone()).unwrap();
    signal_hook::flag::register(signal_hook::SIGQUIT, shutdown.clone()).unwrap();

    let node = Arc::new(Mutex::new(Node::bootstrap_cluster(&[
        "/tmp/remark/dir1",
        "/tmp/remark/dir2",
    ], NodeMetadata {
        uuid: "fab5c1c7-8d64-4d7a-908e-16146e1af42f".parse().unwrap(),
        id: 0,
        host: "localhost".into(),
        port: 4820,
        system_port: 4821,
    }).unwrap()));

    if !node.lock().store.is_partition_exists("topic1", 0) {
        node.lock().store.create_partition("topic1", 0).unwrap();
    }
    if !node.lock().store.is_partition_exists("topic1", 1) {
        node.lock().store.create_partition("topic1", 1).unwrap();
    }

    #[derive(Default)]
    struct Context {
        pull_streams: Vec<StdResult<Stream, Status>>,
    }

    crate::rpc::serve("0.0.0.0:4820", shutdown, Default::default(),
    || Ok(Context::default()),
    clone!(node => move |ctx: &mut Context, req: Request, session: RequestSession| {
        measure_time::print_time!("session");

        if req.request.is_none() {
            return Err(Error::new(ErrorId::Todo, "invalid request"));
        }

        match req.request.unwrap() {
            request::Request::Pull(req) => {
                ctx.pull_streams.clear();
                for req_stream in &req.streams {
                    let stream = node.lock().pull(&req_stream.topic_name,
                            req_stream.partition_id, Id::new(req_stream.message_id))
                        .map(|iter| Stream { iter, sent_eos: false });
                    ctx.pull_streams.push(stream);
                }

                let timeout = Duration::from_millis(req.timeout_millis as u64);
                let limit_bytes = req.per_stream_limit_bytes.to_usize().unwrap();

                let ref mut stream_wr = session.respond_with_stream(&Response {
                    response: Some(response::Response::Pull(
                        pull::Response {
                            common: Some(common::Response { status: Status::Ok.into() }),
                        }))
                })?;
                send_entry_streams(stream_wr, &mut ctx.pull_streams, timeout, limit_bytes)?;
            }
            request::Request::PullMore(req) => {
                let timeout = Duration::from_millis(req.timeout_millis as u64);
                let limit_bytes = req.per_stream_limit_bytes.to_usize().unwrap();

                let ref mut stream_wr = session.respond_with_stream(&Response {
                    response: Some(response::Response::PullMore(
                        pull_more::Response {
                            common: Some(common::Response { status: Status::Ok.into() }),
                        }))
                })?;
                send_entry_streams(stream_wr, &mut ctx.pull_streams, timeout, limit_bytes)?;
            }
            request::Request::Push(req) => {
                handle_push_request(req, session, &node)?;
            }
        }

        Ok(())
    })).unwrap().join().unwrap();

//    let l = TcpListener::bind("0.0.0.0:4820").unwrap();
//    l.set_nonblocking(true).unwrap();
//
//    let workers = threadpool::ThreadPool::new(16);
//
//
//    for net_stream in l.incoming() {
//        match net_stream {
//            Ok(mut net_stream) => {
//                workers.execute(clone!(terminated, server => move || {
//                    net_stream.set_nonblocking(false).unwrap();
//                    let net_stream = &mut net_stream;
//                    println!("new connectoin: {}", net_stream.peer_addr().unwrap());
//                    measure_time::print_time!("session");
//
//                    let mut pull_streams: Vec<StdResult<Stream, Status>> = Vec::new();
//
//                    loop {
//                        if terminated.load(Ordering::Relaxed) {
//                            println!("shutting down");
//                            return;
//                        }
//                        measure_time::print_time!("req");
//                        let req = match read_pb_frame::<Request, _>(net_stream) {
//                            Ok(v) => v,
//                            Err(e) => {
//                                dbg!(e);
//                                break;
//                            }
//                        };
//
//                        dbg!(&req);
//
//                        match req.request.unwrap() {
//                            request::Request::Pull(req) => {
//                                pull_streams.clear();
//                                for req_stream in &req.streams {
//                                    let stream = server.lock().pull(&req_stream.topic_name,
//                                            req_stream.shard_id, Id::new(req_stream.message_id))
//                                        .map(|iter| Stream { iter, sent_eos: false });
//                                    pull_streams.push(stream);
//                                }
//
//                                write_pb_frame(net_stream, &Response {
//                                    response: Some(response::Response::Pull(
//                                    pull::Response {
//                                        common: Some(common::Response { status: Status::Ok.into() }),
//                                    }
//                                )) }).unwrap();
//
//                                let timeout = Duration::from_millis(req.timeout_millis as u64);
//                                let limit_bytes = req.per_stream_limit_bytes.to_usize().unwrap();
//
//                                send_pull_streams(&mut pull_streams, net_stream, timeout, limit_bytes);
//                            }
//                            request::Request::PullMore(req) => {
//                                dbg!(&req);
//                                let timeout = Duration::from_millis(req.timeout_millis as u64);
//                                let limit_bytes = req.per_stream_limit_bytes.to_usize().unwrap();
//
//                                write_pb_frame(net_stream, &Response {
//                                    response: Some(response::Response::PullMore(
//                                    pull_more::Response {
//                                        common: Some(common::Response { status: Status::Ok.into() }),
//                                    }
//                                )) }).unwrap();
//
//                                send_pull_streams(&mut pull_streams, net_stream, timeout, limit_bytes);
//                            }
//                            request::Request::Push(req) => {
//                                let mut entries = read_entries(net_stream, req.entries.len()).unwrap();
//                                let resp = server.lock().push(&req, &mut entries);
//                                write_pb_frame(net_stream, &Response {
//                                    response: Some(response::Response::Push(resp))
//                                }).unwrap();
//                            }
//                        }
//                    }
//                }));
//            }
//            Err(e) => {
//                if e.kind() == io::ErrorKind::WouldBlock {
//                    if terminated.load(Ordering::Relaxed) {
//                        println!("shutting down");
//                        return;
//                    }
//                    std::thread::sleep(std::time::Duration::from_millis(100));
//                } else {
//                    dbg!(e);
//                }
//            }
//        }
//    }
//
//    terminated.store(true, Ordering::Relaxed);
//
//    workers.join();
}
