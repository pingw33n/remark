use log::*;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::prelude::{FutureExt as TokioFutureExt};

use rcommon::clone;
use rcommon::futures::FutureExt;
use rproto::{Request, Response};

use crate::error::*;
use crate::error::BoxFuture;

use super::*;

struct Conns {
    vec: Vec<TcpStream>,
    pending: usize,
}

impl Conns {
    pub fn new() -> Self {
        Self {
            vec: Vec::new(),
            pending: 0,
        }
    }
}

type ConnPoolHandle = Arc<Mutex<ConnPool>>;

struct ConnPool {
    connect_timeout: Option<Duration>,
    conns: HashMap<Endpoint, Conns>,
}

impl ConnPool {
    pub fn new(connect_timeout: Option<Duration>) -> ConnPoolHandle {
        Arc::new(Mutex::new(Self {
            connect_timeout,
            conns: HashMap::new(),
        }))
    }

    pub fn acquire(this: ConnPoolHandle, endpoint: Endpoint)
        -> impl Future<Item=ConnLease, Error=Error>
    {
        future::lazy(move || {
            let mut locked = this.lock();
            loop {
                if let Some(conns) = locked.conns.get_mut(&endpoint) {
                    conns.pending += 1;
                    break if conns.vec.is_empty() {
                        Self::new_connection(&endpoint, locked.connect_timeout)
                            .inspect_err(clone!(endpoint, this => move |e| {
                                warn!("error connecting to {:?}: {}", endpoint, e);
                                this.lock().conns.get_mut(&endpoint).unwrap().pending -= 1;
                            }))
                            .map(clone!(endpoint, this => move |conn|
                                ConnLease::new(this, endpoint, conn)))
                            .into_box()
                    } else {
                        let idx = conns.vec.len() - 1;
                        let conn = conns.vec.remove(idx);
                        future::ok(ConnLease::new(this.clone(), endpoint, conn))
                            .into_box()
                    };
                }
                locked.conns.insert(endpoint.clone(), Conns::new());
            }
        })
    }

    fn new_connection(endpoint: &Endpoint, timeout: Option<Duration>) -> BoxFuture<TcpStream> {
        let addr: IpAddr = match endpoint.host.parse() {
            Ok(v) => v,
            Err(e) => return future::err(e.wrap_id(ErrorId::Todo)).into_box(),
        };
        let sock_addr = match addr {
            IpAddr::V4(a) => SocketAddr::V4(SocketAddrV4::new(a, endpoint.port)),
            IpAddr::V6(a) => SocketAddr::V6(SocketAddrV6::new(a, endpoint.port, 0, 0)),
        };
        let f = future::lazy(move || {
            if let Some(timeout) = timeout {
                debug!("connecting to {} with timeout {}",
                    sock_addr, humantime::format_duration(timeout));
            } else {
                debug!("connecting to {}", sock_addr);
            }
            TcpStream::connect(&sock_addr)
                .map_err(|e| e.wrap_id(ErrorId::Io))
                .inspect(move |c| {
                    debug!("connected to {} (#{:?})", sock_addr, c.as_raw_fd());
                })
        });
        if let Some(timeout) = timeout {
            f.timeout(timeout)
                .map_err(|e| e.wrap_id(ErrorId::Io))
                .into_box()
        } else {
            f.into_box()
        }
    }
}

struct ConnLease {
    pool: ConnPoolHandle,
    endpoint: Endpoint,
    conn: Option<TcpStream>,
    recycle: bool,
}

impl ConnLease {
    pub fn new(pool: ConnPoolHandle, endpoint: Endpoint, conn: TcpStream) -> Self {
        Self {
            pool,
            endpoint,
            conn: Some(conn),
            recycle: false,
        }
    }

    pub fn get(&self) -> &TcpStream {
        self.conn.as_ref().unwrap()
    }

    pub fn get_mut(&mut self) -> &mut TcpStream {
        self.conn.as_mut().unwrap()
    }

    pub fn recycle(&mut self) {
        self.recycle = true;
    }
}

impl io::Read for ConnLease {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.get_mut().read(buf)
    }
}

impl AsyncRead for ConnLease {}

impl io::Write for ConnLease {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.get_mut().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.get_mut().flush()
    }
}

impl AsyncWrite for ConnLease {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        self.get_mut().shutdown()
    }
}

impl Drop for ConnLease {
    fn drop(&mut self) {
        let mut pool = self.pool.lock();
        let conns = pool.conns.get_mut(&self.endpoint).unwrap();
        conns.pending -= 1;
        if let Some(conn) = self.conn.take() {
            if self.recycle {
                trace!("recycling connection to {:?} (#{:?})", self.endpoint, conn.as_raw_fd());
                conns.vec.push(conn);
            } else {
                trace!("not recycling connection to {:?} (#{:?})", self.endpoint, conn.as_raw_fd());
            }
        }
    }
}

#[derive(Clone)]
pub struct Client {
    conn_pool: ConnPoolHandle,
}

impl Client {
    pub fn ask(&self, endpoint: Endpoint, req: Request)
        -> impl Future<Item=Response, Error=Error>
    {
        let conn = ConnPool::acquire(self.conn_pool.clone(), endpoint.clone());
        future::lazy(move || {
            trace!("asking {:?} with {:#?}", endpoint, req);
            conn.and_then(move |c| write_pb_frame(c, &req)
                    .map_err(|e| e.wrap_id(ErrorId::Io).with_context("sending request"))
                    .map(|(c, _)| c))
                .and_then(|c| read_pb_frame::<rproto::Response, _>(c)
                    .map_err(|e| e.wrap_id(ErrorId::Io).with_context("receiving response")))
                .map(|(mut c, r)| {
                    trace!("got response: {:#?}", r);
                    c.recycle();
                    r
                })
        })
    }
}

#[derive(Clone)]
pub struct EndpointClient {
    client: Client,
    endpoint: Endpoint,
}

impl EndpointClient {
    pub fn ask(&self, req: Request) -> impl Future<Item=Response, Error=Error> {
        self.client.ask(self.endpoint.clone(), req)
    }
}