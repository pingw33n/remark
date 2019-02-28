#![allow(dead_code)]
#![deny(non_snake_case)]
//#![deny(unused_imports)]
#![deny(unused_must_use)]

extern crate remark_common as rcommon;
extern crate remark_log as rlog;
extern crate remark_proto as rproto;

mod error;
mod raft;
mod rpc;

use prost::{Message as PMessage};
use std::collections::HashMap;
use std::io::prelude::*;
use std::sync::Arc;

use rcommon::clone;
use rlog::entry::{BufEntry, BufEntryBuilder};
use rlog::log::Log;
use rlog::message::{Id, MessageBuilder, Timestamp};
use rproto::*;
use parking_lot::Mutex;
use rproto::common::Status;
use std::result::{Result as StdResult};
use num_traits::cast::ToPrimitive;
use std::path::PathBuf;
use std::fs;
use std::path::Path;
use uuid::Uuid;
use log::*;
use crate::error::*;
pub use rproto::cluster::partition::{Kind as PartitionKind};
use rpc::Endpoint;
use rcommon::util::OptionResultExt;
use crate::rpc::client::EndpointClient;
use futures::Future;
use tokio::prelude::*;
use rproto::Request;
use crate::rpc::server::HandlerResult;
use rcommon::futures::FutureExt;
use crate::rpc::RequestStreamFrame;
use futures::stream::{Stream as FutureStream};
use signal_hook::iterator::Signals;
use parking_lot::RwLock;
use crate::rpc::ResponseStreamFrame;
use std::mem;
use futures::try_ready;
use tokio::timer::Delay;
use matches::matches;
use rcommon::futures::condvar::Condvar;
use std::time::Duration;
use rcommon::futures::StreamExt;
use rand::Rng;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

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
        debug!("applying cluster metadata log entry: {:#?}", metadata);
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

type NodeHandle = Arc<RwLock<Node>>;

struct Node {
    store: Store,
    id: u32,
    cluster_ctrl: Option<ClusterController>,
    raft_clusters: HashMap<(String, u32), raft::ClusterHandle>,
    push_notify: Condvar,
}

impl Node {
    pub fn bootstrap_cluster<P: AsRef<Path>>(paths: &[P], node_meta: NodeMetadata) -> Result<Self> {
        let mut store = Store::open_or_create(paths)
            .context("opening store")?;

        let my_uuid = node_meta.uuid;

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

        let my_id = cluster_meta.nodes.values()
            .filter(|n| n.uuid == my_uuid)
            .map(|n| n.id)
            .next()
            .unwrap();

        let mut raft_clusters = HashMap::new();
        for tm in cluster_meta.topics.values() {
            for pm in tm.partitions.values() {
                if pm.nodes.contains(&my_id) {
                    debug!("found myself ({}) among nodes for {}/{}: {:?}",
                        my_id, tm.id, pm.id, pm.nodes);
                    let last_entry = store.topics[&tm.id].partitions[&pm.id].log.iter(..)
                        .last()
                        .transpose_()
                        .expect("FIXME")
                        .map(|e| (e.start_id(), e.term()));
                    debug!("creating raft cluster with last_entry={:?}", last_entry);
                    let rc = raft::Cluster::new(my_id, last_entry, tm.id.clone(), pm.id,
                        Duration::from_millis(rand::thread_rng().gen_range(1500, 3000)));
                    for nid in &pm.nodes {
                        if let Some(node) = cluster_meta.nodes.get(&nid) {
                            rc.lock().add_node(node.id, EndpointClient::default(Endpoint {
                                host: node.host.clone(),
                                port: node.system_port,
                            }));
                        } else {
                            error!("unknown node id `{}`", nid);
                        }
                    }
                    raft_clusters.insert((tm.id.clone(), pm.id), rc);
                }
            }
        }

        let cluster_ctrl = Some(ClusterController {
            metadata: cluster_meta,
        });

        Ok(Self {
            store,
            id: 0,
            cluster_ctrl,
            raft_clusters,
            push_notify: Condvar::new(),
        })
    }

    pub fn push(&self, topic_id: &str, partition_id: u32, use_timestamps: bool,
            entry: &mut BufEntry, buf: &mut Vec<u8>) -> Status {
        if let Some(topic) = self.store.topics.get(topic_id) {
            if let Some(partition) = topic.partitions.get(&partition_id) {
//                measure_time::print_time!("push");

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
                    Ok(()) => {
                        self.push_notify.notify_all();
                        Status::Ok
                    }
                    Err(e) => {
                        dbg!(e);
                        Status::BadRequest
                    }
                }
            } else {
                Status::BadPartitionId
            }
        } else {
            Status::BadTopicId
        }
    }

    pub fn pull(&self, topic_id: &str, partition_id: u32, message_id: Id) -> StdResult<rlog::log::Iter, Status> {
        let topic = self.store.topics.get(topic_id).ok_or(Status::BadTopicId)?;
        let partition = topic.partitions.get(&partition_id).ok_or(Status::BadPartitionId)?;
        Ok(partition.log.iter(message_id..))
    }

    pub fn ask_vote(&mut self, req: rproto::ask_vote::Request) -> rproto::ask_vote::Response {
        if let Some(rc) = self.raft_clusters.get_mut(&(req.topic_id.clone(), req.partition_id)) {
            rc.lock().ask_vote(&req)
        } else {
            rproto::ask_vote::Response {
                common: Some(rproto::common::Response { status: Status::BadRaftClusterId.into() }),
                ..Default::default()
            }
        }
    }

    pub fn start_election(&mut self) {
        for cluster in self.raft_clusters.values() {
            raft::Cluster::start_election(cluster);
        }
    }
}

fn handle_push_request(
    req: rproto::push::Request,
    stream: BoxStream<RequestStreamFrame>,
    node: NodeHandle)
    -> impl Future<Item=HandlerResult, Error=Error>
{
    use rproto::push::*;

    let mut resp = Response::default();
    resp.common = Some(rproto::common::Response::default());
    resp.streams = req.streams.iter()
        .map(|_| response::Stream::default())
        .collect();

    stream
        .fold(resp, move |mut resp, RequestStreamFrame { stream_id, mut payload }| {
            let stream_id = stream_id as usize;
            if stream_id >= req.streams.len() {
                warn!("got stream with stream_id={} which is higher than the max defined stream_id={:?}",
                    stream_id, req.streams.len().checked_sub(1));
                resp.common.as_mut().unwrap().status = Status::BadRequest.into();
                resp.streams.clear();
                return future::ok(resp).into_box();
            }

            if resp.streams[stream_id].status != Status::Ok.into() {
                trace!("[{}] skipping entry for a failed", stream_id);
                return future::ok(resp).into_box();
            }

            let mut entry = match BufEntry::decode(&payload) {
                Ok(Some(v)) => v,
                Ok(None) => {
                    warn!("[{}] entry is truncated", stream_id);
                    resp.common.as_mut().unwrap().status = Status::InnerErrors.into();
                    resp.streams[stream_id].status = Status::BadEntry.into();
                    return future::ok(resp).into_box();
                }
                Err(e) => {
                    warn!("[{}] entry is malformed: {:?}", stream_id, e);
                    resp.common.as_mut().unwrap().status = Status::InnerErrors.into();
                    resp.streams[stream_id].status = Status::BadEntry.into();
                    return future::ok(resp).into_box();
                }
            };

            let req_stream = req.streams[stream_id].clone();
            let req_flags = req.flags;
            future::poll_fn(clone!(node => move || {
                Ok(tokio_threadpool::blocking(|| {
                    node.read().push(
                        &req_stream.topic_id,
                        req_stream.partition_id,
                        req_flags & remark_proto::push::request::Flags::UseTimestamps as u32 != 0,
                        &mut entry,
                        &mut payload)
                }).unwrap())
            })).and_then(move |status| {
                trace!("[{}] status={:?}", stream_id, status);
                resp.streams[stream_id].status = status.into();
                if status == Status::Ok {
                    resp.streams[stream_id].successful_entry_count += 1;
                } else {
                    resp.common.as_mut().unwrap().status = Status::InnerErrors.into();
                }
                Ok(resp)
            }).into_box()
        })
        .map(|resp| HandlerResult::new(resp))
}

fn handle_pull_request(
    req: rproto::pull::Request,
    cursors: &mut Vec<Cursor>,
    node: NodeHandle,
) -> impl Future<Item=HandlerResult, Error=Error>
{
    use rproto::pull::*;

    cursors.clear();
    for req_stream in &req.streams {
        let iter = node.read().pull(&req_stream.topic_id,
            req_stream.partition_id, Id::new(req_stream.message_id));
        let cursor = Cursor {
            push_notify: node.read().push_notify.clone(),
            iter: Arc::new(Mutex::new(iter))
        };
        cursors.push(cursor);
    }

    let timeout = Duration::from_millis(req.timeout_millis as u64);
    let limit_bytes = req.per_stream_limit_bytes;

    let resp_stream = EntryResponseStream::new(cursors, timeout, limit_bytes);
    future::ok(HandlerResult {
        response: Response {
            common: Some(rproto::common::Response {
                status: Status::Ok.into(),
            }),
        }.into(),
        stream: Some(resp_stream.map_err(|_| unreachable!()).into_box()),
    })
}

fn handle_pull_more_request(
    req: rproto::pull_more::Request,
    cursors: &Vec<Cursor>,
) -> impl Future<Item=HandlerResult, Error=Error>
{
    use rproto::pull::*;

    let timeout = Duration::from_millis(req.timeout_millis as u64);
    let limit_bytes = req.per_stream_limit_bytes;

    let resp_stream = EntryResponseStream::new(cursors, timeout, limit_bytes);
    future::ok(HandlerResult {
        response: Response {
            common: Some(rproto::common::Response {
                status: Status::Ok.into(),
            }),
        }.into(),
        stream: Some(resp_stream.map_err(|_| unreachable!()).into_box()),
    })
}

#[derive(Clone)]
struct Cursor {
    push_notify: Condvar,
    iter: Arc<Mutex<StdResult<rlog::log::Iter, Status>>>,
}

impl Stream for Cursor {
    type Item = (BufEntry, Vec<u8>);
    type Error = Status;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if let Err(e) = self.iter.lock().as_mut() {
            return Err(*e);
        }
        loop {
            let iter = self.iter.clone();
            let entry = Ok(tokio_threadpool::blocking(move || {
                let mut iter = iter.lock();
                let iter = iter.as_mut().unwrap();
                match iter.next() {
                    Some(Ok(entry)) => Some(iter.complete_read().map(|_| entry)),
                    v => v,
                }
            }).unwrap());

            let entry = match try_ready!(entry) {
                Some(Ok(v)) => v,
                None => {
                    match try_ready!(Ok(self.push_notify.poll().unwrap())) {
                        Some(()) => continue,
                        None => break Ok(Async::Ready(None)),
                    }
                },
                Some(Err(e)) => {
                    error!("cursor failed: {:?}", e);
                    *self.iter.lock() = Err(Status::UnknownError);
                    break Err(Status::UnknownError); // FIXME
                }
            };
            let buf = mem::replace(self.iter.lock().as_mut().unwrap().buf_mut(), Vec::new());
            break Ok(Async::Ready(Some((entry, buf))));
        }
    }
}

enum CursorState {
    Polling(Cursor),
    Done,
}

struct EntryResponseStream {
    cursors: Vec<CursorState>,
    stream_id: u32,
    timeout: Option<Delay>,
    sent_bytes: u32,
    limit_bytes: u32,
}

impl EntryResponseStream {
    pub fn new(cursors: &Vec<Cursor>, timeout: Duration, limit_bytes: u32) -> Self {
        assert!(!cursors.is_empty());
        let stream_id = cursors.len() as u32 - 1;
        let cursors: Vec<_> = cursors.clone().into_iter()
            .map(CursorState::Polling)
            .collect();
        Self {
            cursors,
            stream_id,
            timeout: Some(Delay::new(tokio::clock::now() + timeout)),
            sent_bytes: 0,
            limit_bytes,
        }
    }
}

impl Stream for EntryResponseStream {
    type Item = ResponseStreamFrame;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, ()> {
        let mut all_done = true;
        for _ in 0..self.cursors.len() {
            self.stream_id = (self.stream_id + 1) % self.cursors.len() as u32;

            let timed_out = if let Some(timeout) = &mut self.timeout {
                timeout.poll().unwrap() == Async::Ready(())
            } else {
                true
            };
            if timed_out {
                trace!("timed out");
            }

            let limit_bytes_reached = self.sent_bytes > 0 && self.sent_bytes >= self.limit_bytes;
            if limit_bytes_reached {
                trace!("limit_bytes reached: {}", limit_bytes_reached);
            }

            if timed_out || limit_bytes_reached {
                self.timeout = None;
                if matches!(self.cursors[self.stream_id as usize], CursorState::Polling(_)) {
                    self.cursors[self.stream_id as usize] = CursorState::Done;
                    return Ok(Async::Ready(Some(ResponseStreamFrame {
                        stream_id: self.stream_id,
                        payload: Err(Status::EndOfStream),
                    })));
                } else {
                    continue;
                }
            };

            let become_done = match &mut self.cursors[self.stream_id as usize] {
                CursorState::Polling(stream) => {
                    all_done = false;
                    match stream.poll() {
                        Ok(Async::Ready(Some((_, buf)))) => {
                            self.sent_bytes += buf.len() as u32;
                            return Ok(Async::Ready(Some(ResponseStreamFrame {
                                stream_id: self.stream_id,
                                payload: Ok(buf),
                            })));
                        }
                        Ok(Async::Ready(None)) => {
                            Some(ResponseStreamFrame {
                                stream_id: self.stream_id,
                                payload: Err(Status::EndOfStream),
                            })
                        }
                        Ok(Async::NotReady) => {
                            None
                        }
                        Err(e) => {
                            error!("cursor {} failed: {:?}", self.stream_id, e);
                            Some(ResponseStreamFrame {
                                stream_id: self.stream_id,
                                payload: Err(Status::UnknownError),
                            })
                        }
                    }
                }
                CursorState::Done => None,
            };
            if let Some(r) = become_done {
                self.cursors[self.stream_id as usize] = CursorState::Done;
                return Ok(Async::Ready(Some(r)));
            }
        }

        Ok(if all_done { Async::Ready(None) } else { Async::NotReady })
    }
}

fn main() {
    env_logger::Builder::from_default_env()
        .format(|buf, record| {
            let thread = std::thread::current();
            if let Some(thread_name) = thread.name() {
                writeln!(buf, "[{} {:5} {}][{}] {}",
                    buf.precise_timestamp(), record.level(),
                    record.module_path().unwrap_or(""),
                    thread_name,
                    record.args())
            } else {
                writeln!(buf, "[{} {:5} {}][{:?}] {}",
                    buf.precise_timestamp(), record.level(),
                    record.module_path().unwrap_or(""),
                    thread.id(),
                    record.args())
            }
        })
        .init();

    let mut b = BufEntryBuilder::dense();
//    b.compression(remark_log::entry::Codec::Lz4);
    while b.get_frame_len() < 100 {
        b.message(MessageBuilder { value: Some(format!("test {}", b.get_frame_len()).into()), ..Default::default() });
    }
    dbg!(b.message_count());
    let (_, buf) = b.build();
    std::fs::File::create("/tmp/rementry_small.bin").unwrap().write_all(&buf).unwrap();

    BufEntry::decode(&buf).unwrap().unwrap().validate_body(&buf,
        remark_log::entry::ValidBody { dense: true, without_timestamp: true } ).unwrap();

    let node = Arc::new(RwLock::new(Node::bootstrap_cluster(&[
        "/tmp/remark/dir1",
        "/tmp/remark/dir2",
    ], NodeMetadata {
        uuid: "fab5c1c7-8d64-4d7a-908e-16146e1af42f".parse().unwrap(),
        id: 0,
        host: "localhost".into(),
        port: 4820,
        system_port: 4821,
    }).unwrap()));

    for i in 0..8 {
        if !node.read().store.is_partition_exists("topic1", i) {
            node.write().store.create_partition("topic1", i).unwrap();
        }
    }

    #[derive(Default)]
    struct Context {
        cursors: Vec<Cursor>,
    }

    let sig_shutdown = Signals::new(&[
            signal_hook::SIGINT,
            signal_hook::SIGTERM])
        .unwrap()
        .into_async()
        .unwrap()
        .into_future()
        .map(|(sig, _)| info!("{} received",
            match sig {
                Some(signal_hook::SIGINT) => "SIGINT",
                Some(signal_hook::SIGTERM) => "SIGTERM",
                _ => "<unexpected signal>"
            }
        ))
        .map_err(|e| error!("error in signal handler: {:?}", e))
        .shared();

    let (_shutdown, shutdown_rx) = rcommon::futures::signal();

    let shutdown_rx = shutdown_rx
        .select(sig_shutdown.clone().map_all_unit())
        .map_all_unit()
        .shared();

    let mut rt = tokio::runtime::Runtime::new().unwrap();

    rt.spawn(rpc::server::serve("0.0.0.0:4820".parse().unwrap(),
        Default::default(),
        shutdown_rx.clone().map_all_unit(),
        || Ok(Context { cursors: Vec::new() }),
        clone!(node => move |ctx: &mut Context, req: Request, stream| {
            use rproto::request::Request::*;
            match req.request.unwrap() {
                Push(req) => {
                    handle_push_request(req, stream, node.clone()).into_box()
                }
                Pull(req) => {
                    handle_pull_request(req, &mut ctx.cursors, node.clone()).into_box()
                }
                PullMore(req) => {
                    handle_pull_more_request(req, &ctx.cursors).into_box()
                }
                AskVote(req) => {
                    future::ok(HandlerResult::new(node.write().ask_vote(req))).into_box()
                }
            }

        }))
        .map_all_unit()
    );

    rt.spawn(future::lazy(clone!(node => move || { node.write().start_election(); Ok(()) })));

    rt.shutdown_on_idle().wait().unwrap();

    info!("app shut down");
}
