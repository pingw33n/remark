use log::*;
use std::net::SocketAddr;
use std::result::{Result as StdResult};
use std::time::Duration;
use tk_listen::ListenExt;
use tokio::prelude::*;
use tokio::prelude::{FutureExt as TokioFutureExt};

use rcommon::clone;
use rcommon::futures::{FutureExt, StreamExt};
use rproto::common::{Status, ResponseStreamFrameHeader};
use rproto::{Request, Response};

use crate::error::*;
use tokio::net::TcpListener;

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerOptions {
    pub sleep_on_error: Duration,
    pub max_connections: usize,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            sleep_on_error: Duration::from_millis(500),
            max_connections: 10000,
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
        }
    }
}

pub struct RequestStreamFrame {
    pub stream_id: u32,
    pub payload: Vec<u8>,
}

pub struct ResponseStreamFrame {
    pub stream_id: u32,
    pub payload: StdResult<Vec<u8>, Status>,
}

pub struct HandlerResponse {
    pub response: Response,
    pub stream: Option<BoxStream<ResponseStreamFrame>>,
}

pub fn serve<Ctx, OnConnect, OnConnectResult, Handler, HandlerResult>(
    addr: SocketAddr,
    options: ServerOptions,
    on_connect: OnConnect,
    handler: Handler,
    ) -> impl Future<Item=(), Error=()>
    where
        Ctx: Send + 'static,
        OnConnect: Fn() -> OnConnectResult + Send + 'static,
        OnConnectResult: IntoFuture<Item=Ctx, Error=Error>,
        OnConnectResult::Future: Send + 'static,
        Handler: Fn(&mut Ctx, Request, BoxStream<RequestStreamFrame>)
            -> HandlerResult + Send + Clone + 'static,
        HandlerResult: IntoFuture<Item=HandlerResponse, Error=Error>,
        HandlerResult::Future: Send + 'static,
{
    let read_timeout = options.read_timeout;
    let write_timeout = options.write_timeout;
    future::lazy(move || {
        info!("starting RPC server at {}", addr);
        let listener = match TcpListener::bind(&addr) {
            Ok(v) => v,
            Err(e) => {
                error!("couldn't bind listener at {}: {}", addr, e);
                return future::err(()).into_box();
            }
        };
        listener.incoming()
            .sleep_on_error(options.sleep_on_error)
            .map(move |socket| {
                let sock_id = socket_id_debug(&socket);
                debug!("[{:?}] new connection", sock_id);
                let handler = handler.clone();
                on_connect()
                    .into_future()
                    .map_err(move |e| error!("[{:?}] error in on_connect(): {:?}", sock_id, e))
                    .and_then(move |mut ctx| {
                        let (rd, wr) = socket.split();
                        let wr = ShexAsyncWrite::new(wr);
                        read_requests(rd, read_timeout)
                            .map_err(move |e| error!("[{:?}] error reading request: {:?}", sock_id, e))
                            .for_each(move |(req, stream)| {
                                trace!("[{:?}] request {}: {:#?}", sock_id,
                                    if req.has_stream() { "with stream " } else { "" },
                                    req);
                                handler(&mut ctx, req, stream)
                                    .into_future()
                                    .map_err(move |e| error!("[{:?}] error handling request: {:?}",
                                        sock_id, e))
                                    .and_then(clone!(wr => move |r| {
                                        trace!("[{:?}] response {}: {:#?}", sock_id,
                                            if r.stream.is_some() { "with stream " } else { "" },
                                            r.response);
                                        write_pb_frame(wr, &r.response)
                                            .timeout(write_timeout)
                                            .map_err(move |e| error!("[{:?}] error writing response: {:?}", sock_id, e))
                                            .map(move |(wr, _)| (wr, r))
                                    }))
                                    .and_then(move |(wr, r)| {
                                        if let Some(stream) = r.stream {
                                            write_stream(stream, sock_id, wr, write_timeout).into_box()
                                        } else {
                                            future::ok(()).into_box()
                                        }
                                    })
                            })
                    })
            })
            .listen(options.max_connections)
            .into_box()
    })
}

fn read_requests<R>(rd: R, timeout: Duration)
    -> impl Stream<Item=(Request, BoxStream<RequestStreamFrame>), Error=Error>
    where R: 'static + AsyncRead + Send + Sync
{
    let rd = ShexAsyncRead::new(rd);
    stream::unfold(Some(rd), move |rd| {
            if let Some(rd) = rd {
                Some(try_read_pb_frame(rd)
                    .timeout(timeout)
                    .map_err(|e| e.wrap_id(ErrorId::Io))
                    .and_then(move |(rd, req): (_, Option<Request>)| {
                        Ok(if let Some(req) = req {
                            let stream = if req.has_stream() {
                                read_sub_stream(rd.clone(), timeout).into_box()
                            } else {
                                stream::empty().into_box()
                            };
                            (Some((req, stream)), Some(rd))
                        } else {
                            (None, None)
                        })
                    }))
            } else {
                None
            }
        })
        .filter_map(|r| r)
}

fn read_sub_stream<R>(rd: R, timeout: Duration)
    -> impl Stream<Item=RequestStreamFrame, Error=Error>
    where R: 'static + AsyncRead + Send
{
    stream::unfold(Some(rd), move |rd| {
        if let Some(rd) = rd {
            Some(read_pb_frame::<rproto::common::RequestStreamFrameHeader, _>(rd)
                .timeout(timeout)
                .map_err(|e| e.wrap_id(ErrorId::Io))
                .and_then(move |(rd, sfh)| {
                    if sfh.end_of_stream {
                        future::ok((None, None)).into_box()
                    } else {
                        let stream_id = sfh.stream_id;
                        read_fixed_frame(rd)
                            .timeout(timeout)
                            .map_err(|e| e.wrap_id(ErrorId::Io))
                            .map(move |(rd, payload)| (Some(RequestStreamFrame { stream_id, payload }), Some(rd)))
                            .into_box()
                    }
                }))
        } else {
            None
        }
    })
        .filter_map(|r| r)
}

fn write_stream(
    stream: impl Stream<Item=ResponseStreamFrame, Error=Error>,
    sock_id: SocketIdDebug,
    wr: impl AsyncWrite + Clone + Send + 'static,
    write_timeout: Duration,
) -> impl Future<Item=(), Error=()>
{
    stream.for_each(move |ResponseStreamFrame { stream_id, payload }| {
        assert!(payload != Err(Status::Ok));
        trace!("[{:?}] writing response stream frame: stream_id={}, {}",
            sock_id, stream_id,
            match &payload {
                Ok(buf) => format!("Buf({})", buf.len()),
                Err(st) => format!("Status({:?})", st),
            });
        let hdr = ResponseStreamFrameHeader {
            stream_id: stream_id,
            status: *payload.as_ref().err().unwrap_or(&Status::Ok) as i32,
        };
        write_pb_frame(wr.clone(), &hdr)
            .timeout(write_timeout)
            .map_err(|e| e.wrap_id(ErrorId::Io).with_context("writing response stream frame header"))
            .and_then(move |(wr, _)| {
                if let Ok(buf) = payload {
                    tokio::io::write_all(wr, buf)
                        .map_err(|e| e.wrap_id(ErrorId::Io).with_context("writing response stream frame payload"))
                        .map(|_| {})
                        .into_box()
                } else {
                    future::ok(()).into_box()
                }
            })
    })
    .map_err(move |e| error!("[{:?}] error writing response stream: {:?}", sock_id, e))
}