use crossbeam_utils::thread;
use log::*;
use parking_lot::Mutex;
use prost::Message;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::io::prelude::*;
use std::net::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{sleep, JoinHandle};
use std::time::Duration;

use rcommon::{clone, contain};
use rcommon::bytes::*;
use rcommon::error::ResultExt;
use rcommon::util::*;
use rcommon::varint::{self, ReadExt, WriteExt};
use rlog::entry::BufEntry;
use rproto::common::{Status, ResponseStreamFrameHeader};
use rproto::{Request, Response};

use crate::error::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IpVersion {
    V4,
    V6,
}

impl IpVersion {
    pub fn of_addr(addr: &IpAddr) -> Self {
        match addr {
            IpAddr::V4(_) => IpVersion::V4,
            IpAddr::V6(_) => IpVersion::V6,
        }
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

struct ClientInner {
    options: ClientOptions,
    host_cache: HashMap<String, IpAddr>,
    connections: HashMap<Endpoint, Vec<TcpStream>>,
}

impl ClientInner {
    pub fn new_wrapped(options: ClientOptions) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            options,
            host_cache: HashMap::new(),
            connections: HashMap::new(),
        }))
    }

    pub fn ask(&mut self, endpoint: Endpoint, req: &Request) -> Result<Response>
    {
        trace!("asking {:?} with {:#?}", endpoint, req);
        let mut conn = self.acquire_conn(endpoint)?;
        let r = (|| {
            write_pb_frame(&mut conn.conn, req).wrap_err_id(ErrorId::Io)?;
            read_pb_frame(&mut conn.conn).wrap_err_id(ErrorId::Io)
        })();
        trace!("got response: {:#?}", r);
        self.release_conn(conn);
        r
    }

    fn acquire_conn(&mut self, endpoint: Endpoint) -> Result<LeasedConnection> {
        loop {
            if let Some(conns) = self.connections.get_mut(&endpoint) {
                let conn = if conns.is_empty() {
                    Self::new_connection(&mut self.host_cache, &endpoint, &self.options)?
                } else {
                    let idx = conns.len() - 1;
                    conns.remove(idx)
                };
                break Ok(LeasedConnection {
                    endpoint,
                    conn,
                });
            }
            self.connections.insert(endpoint.clone(), Vec::new());
        }
    }

    fn release_conn(&mut self, conn: LeasedConnection) {
        loop {
            if let Some(conns) = self.connections.get_mut(&conn.endpoint) {
                conns.push(conn.conn);
                break;
            }
            self.connections.insert(conn.endpoint.clone(), Vec::new());
        }
    }

    fn new_connection(host_cache: &mut HashMap<String, IpAddr>, endpoint: &Endpoint,
        options: &ClientOptions)
        -> Result<TcpStream>
    {
        let addr = Self::resolve_hostname(host_cache, &endpoint.host, options.prefer_ipv)?;
        let sock_addr = match addr {
            IpAddr::V4(a) => SocketAddr::V4(SocketAddrV4::new(a, endpoint.port)),
            IpAddr::V6(a) => SocketAddr::V6(SocketAddrV6::new(a, endpoint.port, 0, 0)),
        };
        let conn = if let Some(timeout) = options.connect_timeout {
            TcpStream::connect_timeout(&sock_addr, timeout)
        } else {
            TcpStream::connect(&sock_addr)
        }.wrap_err_id(ErrorId::Io)?;
        conn.set_read_timeout(options.read_timeout).wrap_err_id(ErrorId::Io)?;
        conn.set_write_timeout(options.write_timeout).wrap_err_id(ErrorId::Io)?;
        Ok(conn)
    }

    fn resolve_hostname(host_cache: &mut HashMap<String, IpAddr>, hostname: &str,
        prefer_ipv: Option<IpVersion>)
        -> Result<IpAddr>
    {
        if let Some(addr) = host_cache.get_mut(hostname) {
            return Ok(*addr);
        }
        let addr = (hostname, 0).to_socket_addrs().wrap_err_id(ErrorId::Todo)?
            .map(|a| a.ip())
            .filter(|a| prefer_ipv.is_none() || prefer_ipv.unwrap() == IpVersion::of_addr(a))
            .next()
            .ok_or_else(|| Error::new(ErrorId::Todo, format!(
                "couldn't resolve hostname `{}`", hostname)))?;
        host_cache.insert(hostname.to_owned(), addr);

        Ok(addr)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientOptions {
    pub prefer_ipv: Option<IpVersion>,
    pub connect_timeout: Option<Duration>,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            prefer_ipv: None,
            connect_timeout: Some(Duration::from_secs(15)),
            read_timeout: Some(Duration::from_secs(15)),
            write_timeout: Some(Duration::from_secs(15)),
        }
    }
}

pub struct Rpc {
    client: Arc<Mutex<ClientInner>>,
}

impl Rpc {
    pub fn new(options: ClientOptions) -> Self {
        Self {
            client: ClientInner::new_wrapped(options),
        }
    }

    pub fn client(&self) -> Client {
        Client {
            inner: self.client.clone(),
        }
    }

    pub fn endpoint_client(&self, endpoint: Endpoint) -> EndpointClient {
        EndpointClient {
            client: self.client(),
            endpoint,
        }
    }
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<Mutex<ClientInner>>,
}

impl Client {
    pub fn ask(&self, endpoint: Endpoint, req: &Request) -> Result<Response>
    {
        self.inner.lock().ask(endpoint, req)
    }
}

#[derive(Clone)]
pub struct EndpointClient {
    client: Client,
    endpoint: Endpoint,
}

impl EndpointClient {
    pub fn ask(&self, req: &Request) -> Result<Response>
    {
        self.client.ask(self.endpoint.clone(), req)
    }
}

struct LeasedConnection {
    endpoint: Endpoint,
    conn: TcpStream,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerOptions {
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            read_timeout: Some(Duration::from_secs(15)),
            write_timeout: Some(Duration::from_secs(15)),
        }
    }
}

pub fn serve<Req, Addr, Ctx, OnConnect, Handler>(
    addr: Addr,
    shutdown: Arc<AtomicBool>,
    options: ServerOptions,
    on_connect: OnConnect,
    handler: Handler)
    -> Result<JoinHandle<()>>
    where
        Req: Default + Message,
        Addr: 'static + ToSocketAddrs + fmt::Display + Send + Clone,
        OnConnect: 'static + Send + Sync + Fn() -> Result<Ctx>,
        Handler: 'static + Send + Sync + Fn(&mut Ctx, Req, RequestSession) -> Result<()>,
{
    let listener = TcpListener::bind(addr.clone()).wrap_err_id(ErrorId::Io)?;
    listener.set_nonblocking(true).wrap_err_id(ErrorId::Io)?;

    let handle = std::thread::spawn(clone!(shutdown => move || {
        thread::scope(|s| {
            info!("started server at {}", addr);
            for conn in listener.incoming() {
                match conn {
                    Ok(mut conn) => {
                        let options = &options;
                        let on_connect = &on_connect;
                        let handler = &handler;
                        s.spawn(move |_| {
                            let peer_addr = conn.peer_addr().unwrap();
                            if let Err(e) = contain! { Result<()>;
                                debug!("[{}] new connection", peer_addr);

                                conn.set_nonblocking(false).wrap_err_id(ErrorId::Io)?;
                                conn.set_read_timeout(options.read_timeout).wrap_err_id(ErrorId::Io)?;
                                conn.set_write_timeout(options.write_timeout).wrap_err_id(ErrorId::Io)?;

                                let mut ctx = on_connect()?;

                                loop {
                                    let message = read_pb_frame(&mut conn).wrap_err_id(ErrorId::Todo)?;
                                    debug!("[{}] {:#?}", peer_addr, message);
                                    let session = RequestSession { conn: &mut conn };
                                    handler(&mut ctx, message, session)?;
                                }
                            } {
                                error!("[{}] error serving RPC request: {}", peer_addr, e);
                            }
                        });
                    }
                    Err(e) => {
                        if e.kind() == io::ErrorKind::WouldBlock {
                            if shutdown.load(Ordering::Relaxed) {
                                info!("shutting down RPC server at {}", addr);
                                return;
                            }
                        } else {
                            error!("error while serving on {}: {}", addr, e);
                        }
                        sleep(Duration::from_millis(100));
                    }
                }
            }
        }).unwrap();
    }));

    Ok(handle)
}

pub struct RequestSession<'a> {
    conn: &'a mut TcpStream,
}

impl<'a> RequestSession<'a> {
    pub fn read_entries(&mut self, count: usize) -> EntryReader {
        EntryReader {
            conn: self.conn,
            left: count,
            buf: Vec::new(),
        }
    }

    pub fn respond(mut self, response: &Response) -> Result<()> {
        self.write_response(response)
    }

    pub fn respond_with_stream(mut self, response: &Response) -> Result<StreamWriter<'a>> {
        self.write_response(response)?;
        Ok(StreamWriter {
            session: self,
            written: 0,
        })
    }

    fn write_response(&mut self, response: &Response) -> Result<()> {
        debug!("[{}] {:#?}", self.conn.peer_addr().unwrap(), response);
        write_pb_frame(self.conn, response)
            .wrap_err_id(ErrorId::Io)
            .map(|_| {})
    }
}

pub struct EntryReader<'a> {
    conn: &'a mut TcpStream,
    buf: Vec<u8>,
    left: usize,
}

impl EntryReader<'_> {
    pub fn buf(&self) -> &[u8] {
        &self.buf
    }

    pub fn buf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }
}

impl Iterator for EntryReader<'_> {
    type Item = Result<BufEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.buf.clear();
        BufEntry::read_full(self.conn, &mut self.buf)
            .wrap_err_id(ErrorId::Todo)
            .transpose_()
    }
}

pub struct StreamWriter<'a> {
    session: RequestSession<'a>,
    written: usize,
}

impl StreamWriter<'_> {
    pub fn written(&self) -> usize {
        self.written
    }

    pub fn write(&mut self, stream_id: u32, status: rproto::common::Status, data: Option<&[u8]>)
        -> Result<()>
    {
        self.write_header(stream_id, status)?;

        if let Some(data) = data {
            self.session.conn.write_all(data).wrap_err_id(ErrorId::Io)?;
            self.written = self.written.checked_add(data.len()).unwrap();
        }

        Ok(())
    }

    fn write_header(&mut self, stream_id: u32, status: Status) -> Result<()> {
        let written = write_pb_frame(&mut self.session.conn, &ResponseStreamFrameHeader {
            stream_id,
            status: status.into(),
        }).wrap_err_id(ErrorId::Io)?;
        self.written = self.written.checked_add(written).unwrap();
        Ok(())
    }
}

fn read_pb_frame<M: Default + Message, R: Read>(rd: &mut R) -> io::Result<M> {
    let len = rd.read_u32_varint()?;
    let mut buf = Vec::new();
    buf.ensure_len_zeroed(len as usize);
    rd.read_exact(&mut buf)?;

    M::decode(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData,
        format!("malformed protobuf data: {}", e)))
}

fn write_pb_frame(wr: &mut Write, msg: &impl Message) -> io::Result<usize> {
    let len = msg.encoded_len();
    let mut buf = Vec::new();
    buf.reserve_exact(varint::encoded_len(len as u64) as usize + len);
    buf.write_u32_varint(len as u32)?;
    msg.encode(&mut buf).unwrap();
    wr.write_all(&buf)?;
    Ok(len)
}