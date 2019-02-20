pub mod client;
pub mod server;

use atomic_refcell::AtomicRefCell;
use prost::Message;
use std::fmt;
use std::io;
use std::io::prelude::*;
use std::net::{IpAddr, SocketAddr};
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::prelude::*;

use rcommon::bytes::*;
use rcommon::futures::FutureExt;
use rcommon::varint::{self, WriteExt};

const MAX_PB_MESSAGE_LEN: u32 = 65535;

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

struct ShexAsyncRead<T>(Arc<AtomicRefCell<T>>);

impl<T: AsyncRead> ShexAsyncRead<T> {
    pub fn new(inner: T) -> Self {
        Self(Arc::new(AtomicRefCell::new(inner)))
    }
}

impl<T: Read> Read for ShexAsyncRead<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.borrow_mut().read(buf)
    }
}

impl<T: AsyncRead> AsyncRead for ShexAsyncRead<T> {}

impl<T> Clone for ShexAsyncRead<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

struct ShexAsyncWrite<T>(Arc<AtomicRefCell<T>>);

impl<T: AsyncWrite> ShexAsyncWrite<T> {
    pub fn new(inner: T) -> Self {
        Self(Arc::new(AtomicRefCell::new(inner)))
    }
}

impl<T: Write> Write for ShexAsyncWrite<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.borrow_mut().flush()
    }
}

impl<T: AsyncWrite> AsyncWrite for ShexAsyncWrite<T> {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        self.0.borrow_mut().shutdown()
    }
}

impl<R> Clone for ShexAsyncWrite<R> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[derive(Clone, Copy)]
struct SocketIdDebug {
    peer_addr: Option<SocketAddr>,
    raw_fd: RawFd,
}

impl fmt::Debug for SocketIdDebug {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(peer_addr) = self.peer_addr {
            write!(f, "{}#{:?}", peer_addr, self.raw_fd)
        } else {
            write!(f, "?#{:?}", self.raw_fd)
        }
    }
}

fn socket_id_debug(socket: &TcpStream) -> SocketIdDebug {
    SocketIdDebug {
        peer_addr: socket.peer_addr().ok(),
        raw_fd: socket.as_raw_fd(),
    }
}

struct ReadVarint<R> {
    rd: Option<R>,
    v: u64,
    len: usize,
}

impl<R: AsyncRead> Future for ReadVarint<R> {
    type Item = (R, u64, usize);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut b = [0];
        loop {
            let read = futures::try_ready!(self.rd.as_mut().unwrap().poll_read(&mut b));
            if read == 0 {
                return if self.len > 0 {
                    Err(io::Error::new(io::ErrorKind::UnexpectedEof,
                        "unexpected eof while reading varint"))
                } else {
                    Ok(Async::Ready((self.rd.take().unwrap(), 0, 0)))
                };
            }
            if b[0] & 0x80 != 0 {
                self.v |= ((b[0] & 0x7f) as u64) << (self.len * 7);
                self.len += 1;
            } else {
                self.v |= (b[0] as u64) << (self.len * 7);
                self.len += 1;
                break;
            }
            if self.len == 8 {
                return Err(io::Error::new(io::ErrorKind::InvalidData,
                    "decoded varint value is to big to fit in u64"));
            }
        }
        Ok(Async::Ready((self.rd.take().unwrap(), self.v, self.len)))
    }
}

fn read_varint<R: AsyncRead>(rd: R) -> ReadVarint<R> {
    ReadVarint {
        rd: Some(rd),
        v: 0,
        len: 0,
    }
}

fn try_read_pb_frame<M, R>(rd: R) -> impl Future<Item=(R, Option<M>), Error=io::Error>
    where M: 'static + Default + Message + Send,
          R: 'static + AsyncRead + Send,
{
    read_varint(rd)
        .and_then(|(rd, len, read_count)| if len > MAX_PB_MESSAGE_LEN as u64 {
            Err(io::Error::new(io::ErrorKind::InvalidData,
                format!("protobuf message is too big: {}", len)))
        } else {
            Ok((rd, len, read_count))
        })
        .and_then(|(rd, len, read_count)| {
            if read_count > 0 {
                let mut buf = Vec::new();
                buf.ensure_len_zeroed(len as usize);
                tokio::io::read_exact(rd, buf)
                    .and_then(|(rd, buf)| {
                        M::decode(&buf)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData,
                                format!("malformed protobuf message: {}", e)))
                            .map(|m| (rd, Some(m)))
                    })
                    .into_box()
            } else {
                future::ok::<_, io::Error>((rd, None)).into_box()
            }
        })
}

fn read_pb_frame<M, R>(rd: R) -> impl Future<Item=(R, M), Error=io::Error>
    where M: 'static + Default + Message + Send,
          R: 'static + AsyncRead + Send,
{
    try_read_pb_frame(rd)
        .and_then(|(rd, m)| Ok((rd, m.ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof,
            "unexpected eof while reading protobuf frame"))?)))
}

fn read_fixed_frame<R>(rd: R) -> impl Future<Item=(R, Vec<u8>), Error=io::Error>
    where R: 'static + AsyncRead + Send,
{
    tokio::io::read_exact(rd, [0; 4])
        .and_then(|(rd, len_bytes)| {
            let len = BigEndian::read_u32(&len_bytes) as usize;
            if len >= 4 {
                let mut buf = Vec::new();
                buf.ensure_capacity(len);
                buf.ensure_len_zeroed(len - 4);
                tokio::io::read_exact(rd, buf)
                    // FIXME
                    .map(move |(rd, mut buf)| {
                        buf.splice(0..0, len_bytes.iter().cloned());
                        (rd, buf)
                    })
                    .into_box()
            } else {
                future::err(io::Error::new(io::ErrorKind::InvalidData, "invalid fixed frame len"))
                    .into_box()
            }
        })
}

fn write_pb_frame<W: AsyncWrite>(wr: W, msg: &impl Message)
    -> impl Future<Item=(W, usize), Error=io::Error>
{
    let msg_len = msg.encoded_len();
    let mut buf = Vec::new();
    buf.reserve_exact(varint::encoded_len(msg_len as u64) as usize + msg_len);
    let frame_len = buf.capacity();
    buf.write_u32_varint(msg_len as u32).unwrap();
    msg.encode(&mut buf).unwrap();
    tokio::io::write_all(wr, buf)
        .map(move |(wr, _)| (wr, frame_len))
}

pub fn foo() {
//    struct Ctx {
//        test: String,
//    }
//
//    let x = "asd".to_owned();
//
//    use signal_hook::iterator::Signals;
//
//
//
//    let user_shutdown = Signals::new(&[
//            signal_hook::SIGINT,
//            signal_hook::SIGTERM])
//        .unwrap()
//        .into_async()
//        .unwrap()
//        .into_future()
//        .map(|(sig, _)| info!("{} received",
//            match sig {
//                Some(signal_hook::SIGINT) => "SIGINT",
//                Some(signal_hook::SIGTERM) => "SIGTERM",
//                _ => "<unexpected signal>"
//            }
//        ))
//        .map_err(|e| error!("error in signal handler: {:?}", e))
//        .shared();
//
//    let (shutdown, shutdown_rx) = rcommon::futures::signal();
//
//    let shutdown_rx = shutdown_rx
//        .select(user_shutdown.clone().map_all_unit())
//        .map_all_unit()
//        .shared();
//
//    tokio::run(server::serve("0.0.0.0:4820".parse().unwrap(), Default::default(),
//        || Ok(Arc::new(AtomicRefCell::new(Ctx { test: "xxx".into() }))),
//        |ctx, req, stream| {
////            dbg!(&req);
//            match req.request.as_ref().unwrap() {
//                rproto::request::Request::Push(req) => {
//                    stream.for_each(clone!(ctx => move |server::RequestStreamFrame { stream_id, payload }| {
//                            ctx.borrow_mut().test = "xxx".into();
//            //                dbg!((stream_id, item.len()));
//
//            //                    dbg!(&req.streams[stream_id as usize].topic_id);
//                            Ok(())
//                        }))
//                        .and_then(clone!(ctx => move |_| {
//                            ctx.borrow_mut().test = "yyy".into();
//                            Ok(server::HandlerResponse {
//                                response: Response {
//                                    response: Some(rproto::response::Response::Push(
//                                        rproto::push::Response {
//                                            common: Some(rproto::common::Response { status: Status::InnerErrors.into() }),
//                                            streams: Vec::new(),
//                                        })),
//                                },
//                                stream: None,
//                            })
//                        }))
//                        .into_box()
//                }
//                rproto::request::Request::Pull(req) => {
//                    future::ok(server::HandlerResponse {
//                        response: Response {
//                            response: Some(rproto::response::Response::Pull(
//                                rproto::pull::Response {
//                                    common: Some(rproto::common::Response { status: Status::Ok.into() }),
//                                })),
//                        },
//                        stream: Some(stream::iter_ok(vec![
//                            server::ResponseStreamFrame { stream_id: 0, payload: Ok(vec![3, 2, 3, 4]) },
////                            (0, Ok(vec![3, 2, 3, 4])),
////                            (1, Ok(vec![2, 5, 6])),
////                            (0, Ok(vec![0])),
////                            (1, Ok(vec![2, 7, 8])),
//                            server::ResponseStreamFrame { stream_id: 0, payload: Err(Status::EndOfStream) },
////                            (1, Err(Status::EndOfStream)),
//                        ]).into_box()),
//                    }).into_box()
//                }
//                _ => unimplemented!()
//            }
//
//        })
//        .select(shutdown_rx.clone().map_all_unit().map(|_| info!("shutting down RPC server 0.0.0.0:4820")))
//        .map_all_unit()
//    );
}