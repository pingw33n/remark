use enum_kinds::EnumKind;
use futures::stream::FuturesUnordered;
use log::*;
use std::sync::Arc;
use std::time::Instant;
use parking_lot::Mutex;
use tokio::prelude::*;

use rcommon::clone;
use rlog::message::Id;
use rproto::common::Status;

use crate::rpc::client::EndpointClient;
use std::time::Duration;

struct Candidate {
    timestamp: Instant,
    votes_from: Vec<u32>,
}

#[derive(EnumKind)]
#[enum_kind(StateKind)]
enum State {
    Idle,
    Candidate(Candidate),
    Follower,
    Leader,
}

impl State {
    pub fn kind(&self) -> StateKind {
        self.into()
    }
}

impl State {
    pub fn as_candidate(&self) -> Option<&Candidate> {
        if let State::Candidate(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn as_candidate_mut(&mut self) -> Option<&mut Candidate> {
        if let State::Candidate(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

pub type ClusterHandle = Arc<Mutex<Cluster>>;

struct Node {
    id: u32,
    client: EndpointClient,
    last_activity: Instant,
}

pub struct Cluster {
    election_timeout: Duration,
    state: State,
    node_id: u32,

    // FIXME persistent
    term: u64,
    voted_for: Option<u32>,

    last_entry: Option<(Id, u64)>,
    horizon: Id,
    nodes: Vec<Node>,

    topic_id: String,
    partition_id: u32,
}

impl Cluster {
    pub fn new(node_id: u32, last_entry: Option<(Id, u64)>,
        topic_id: String, partition_id: u32, election_timeout: Duration) -> ClusterHandle
    {
        Arc::new(Mutex::new(Self {
            state: State::Idle,
            node_id,
            term: 0,
            voted_for: None,
            last_entry,
            horizon: Id::new(0),
            nodes: Vec::new(),
            topic_id,
            partition_id,
            election_timeout,
        }))
    }

    pub fn add_node(&mut self, id: u32, client: EndpointClient) {
        self.nodes.push(Node { id, client, last_activity: Instant::now() });
    }

    pub fn start_election(this: &ClusterHandle) {
        use rproto::ask_vote::*;
        let (futs, election_timeout) = {
            let mut this = this.lock();

            assert!(this.nodes.len() >= 1);
            debug_assert_eq!(this.nodes.iter().filter(|n| n.id == this.node_id).count(), 1);

            this.term += 1;

            if this.nodes.len() == 1 {
                info!("[{}] becoming leader for term {} without election \
                    because it's the only node in the cluster",
                    this.node_id, this.term);
                this.state = State::Leader;
                return;
            }

            this.state = State::Candidate(Candidate {
                timestamp: Instant::now(),
                votes_from: vec![this.node_id],
            });

            let mut futs = FuturesUnordered::new();
            for node in &this.nodes {
                let node_id = node.id;
                let this_node_id = this.node_id;
                if node_id == this_node_id {
                    continue;
                }
                let f = node.client.ask(
                    Request {
                        term: this.term,
                        node_id: this_node_id,
                        last_entry: this.last_entry.map(|(id, term)| request::Entry {
                            id: id.get(),
                            term,
                        }),
                        topic_id: this.topic_id.clone(),
                        partition_id: this.partition_id,
                    })
                    .map_err(move |e| warn!("[{}] AskVote RPC to {} failed: {:?}", this_node_id, node_id, e))
                    .map(move |resp| (node_id, resp));
                futs.push(f);
            }
            (futs, this.election_timeout)
        };

        fn maybe_retry(this: &ClusterHandle) {
            let retry = {
                let this = this.lock();
                if this.state.kind() == StateKind::Candidate {
                    debug!("[{}] election term {} timed out, starting a new one",
                        this.node_id, this.term);
                    true
                } else {
                    false
                }
            };
            if retry {
                Cluster::start_election(&this);
            }
        }

        let fut = futs
            .take_while(clone!(this => move |_| Ok(this.lock().state.kind() == StateKind::Candidate)))
            .for_each(clone!(this => move |(node_id, resp)| {
                let mut this = this.lock();
                let resp = if let rproto::response::Response::AskVote(v) = resp.response.unwrap() {
                    v
                } else {
                    warn!("[{}] invalid AskVote response from {}", this.node_id, node_id);
                    return Ok(());
                };
                if this.state.kind() != StateKind::Candidate {
                    debug!("[{}] ignoring AskVote response from {} since this node is no longer in Candidate state",
                        this.node_id, node_id);
                }
                let status = resp.common.unwrap().status;
                if status != Status::Ok.into() {
                    warn!("[{}] got non-Ok AskVote response from {}: {} ({:?})",
                        this.node_id, node_id, status, Status::from_i32(status));
                }
                if resp.term > this.term {
                    debug!("[{}] saw a higher term, becoming a follower: {} -> {}",
                        this.node_id, this.term, resp.term);
                    this.term = resp.term;
                    this.state = State::Follower;
                    return Ok(());
                } else if resp.term < this.term {
                    debug!("[{}] ignoring AskVote response from previous term {} < {}",
                        this.node_id, resp.term, this.term);
                    return Ok(());
                }
                if resp.vote_granted {
                    let vote_count = {
                        let votes_from = &mut this.state.as_candidate_mut().unwrap().votes_from;
                        votes_from.push(node_id);
                        votes_from.len()
                    };
                    debug!("[{}] vote received from {}, now have votes from {:?}",
                        this.node_id, node_id, this.state.as_candidate().unwrap().votes_from);
                    let majority_count = this.nodes.len() / 2 + 1;
                    if vote_count >= majority_count {
                        info!("[{}] becoming leader for term {} due to votes majority {} from {:?}",
                            this.node_id, this.term, vote_count,
                            this.state.as_candidate().unwrap().votes_from);
                        this.state = State::Leader;
                    }
                } else {
                    debug!("[{}] {} didn't grant the vote", this.node_id, node_id);
                }
                Ok(())
            }))
            .timeout(election_timeout)
            .map_err(clone!(this => move |_| maybe_retry(&this)));
        tokio::spawn(fut);
    }

    pub fn ask_vote(&mut self, request: &rproto::ask_vote::Request) -> rproto::ask_vote::Response {
        let vote_granted = request.term >= self.term &&
            (self.voted_for.is_none() || self.voted_for == Some(request.node_id)) &&
            (self.last_entry.is_none() == request.last_entry.is_none() ||
                request.last_entry.as_ref().and_then(|e| self.last_entry.map(|(my_id, my_term)|
                    e.id >= my_id.get() && e.term >= my_term)) == Some(true));
        // TODO prevent deposing of live leader

        if vote_granted {
            self.voted_for = Some(request.node_id);
        }

        rproto::ask_vote::Response {
            common: Some(rproto::common::Response { status: rproto::common::Status::Ok.into() }),
            term: self.term,
            vote_granted,
        }
    }
}