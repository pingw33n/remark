use log::*;
use std::time::Instant;

use rlog::message::Id;

use crate::error::*;
use crate::rpc::EndpointClient;

struct Candidate {
    timestamp: Instant,
    vote_count: usize,
}

enum State {
    Idle,
    Candidate(Candidate),
    Follower,
    Leader,
}

impl State {
    pub fn as_candidate_mut(&mut self) -> Option<&mut Candidate> {
        if let State::Candidate(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

struct Node {
    id: u32,
    client: EndpointClient,
    last_activity: Instant,
}

pub struct Cluster {
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
        topic_id: String, partition_id: u32) -> Self
    {
        Self {
            state: State::Idle,
            node_id,
            term: 0,
            voted_for: None,
            last_entry,
            horizon: Id::new(0),
            nodes: Vec::new(),
            topic_id,
            partition_id,
        }
    }

    pub fn add_node(&mut self, id: u32, client: EndpointClient) {
        self.nodes.push(Node { id, client, last_activity: Instant::now() });
    }

    pub fn start_election(&mut self) {
        self.term += 1;
        self.state = State::Candidate(Candidate {
            timestamp: Instant::now(),
            vote_count: 1,
        });

        use rproto::ask_vote::*;

        let majority_count = self.nodes.len() / 2 + 1;

        for node in &self.nodes {
            if node.id != self.node_id {
                let resp: Result<rproto::Response> = node.client.ask(&rproto::Request {
                    request: Some(rproto::request::Request::AskVote(Request {
                        term: self.term,
                        node_id: self.node_id,
                        last_entry: self.last_entry.map(|(id, term)| request::Entry {
                            id: id.get(),
                            term,
                        }),
                        topic_id: self.topic_id.clone(),
                        partition_id: self.partition_id,
                    })),
                });
                match resp {
                    Ok(resp) => {
                        if let rproto::response::Response::AskVote(resp) = resp.response.unwrap() {
                            if resp.common.unwrap().status == rproto::common::Status::Ok.into() {
                                if resp.term > self.term {
                                    self.term = resp.term;
                                    self.state = State::Follower;
                                    debug!("saw a higher term, becoming a follower: {} -> {}",
                                        self.term, resp.term);
                                    break;
                                }
                                if resp.vote_granted {
                                    let vote_count = &mut self.state.as_candidate_mut().unwrap().vote_count;
                                    *vote_count += 1;
                                    debug!("vote granted, have {} vote(s) now", *vote_count);
                                    if *vote_count >= majority_count {
                                        break;
                                    }
                                } else {
                                    debug!("vote not granted");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        dbg!(e);
                    }
                }
            }
        }

        let vote_count = self.state.as_candidate_mut().unwrap().vote_count;
        if vote_count >= majority_count {
            debug!("got majority of votes {} of {}, becoming leader for term {}",
                vote_count, self.nodes.len(), self.term);
            self.state = State::Leader;
        }
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