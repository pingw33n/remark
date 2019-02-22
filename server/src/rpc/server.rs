use log::*;
use std::net::SocketAddr;
use std::time::Duration;
use stream_cancel::{StreamExt as ScStreamExt};
use tk_listen::ListenExt;
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::prelude::{FutureExt as TokioFutureExt};

use rcommon::clone;
use rcommon::futures::{FutureExt, StreamExt};
use rproto::common::{Status, ResponseStreamFrameHeader};
use rproto::{Request, Response};

use crate::error::*;

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Options {
    pub sleep_on_error: Duration,
    pub max_connections: usize,
    pub socket_timeout: Duration,
    pub max_frame_count: u32,
    pub max_single_payload_len: u32,
    pub max_total_payload_len: u32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            sleep_on_error: Duration::from_millis(500),
            max_connections: 10000,
            socket_timeout: Duration::from_secs(30000),
            max_frame_count: 1000,
            max_single_payload_len: 1024 * 1024,
            max_total_payload_len: u32::max_value(),
        }
    }
}

pub struct HandlerResult {
    pub response: Response,
    pub stream: Option<ResponseStream>,
}

impl HandlerResult {
    pub fn new(response: impl Into<Response>) -> Self {
        Self {
            response: response.into(),
            stream: None,
        }
    }

    pub fn with_stream(response: impl Into<Response>, stream: ResponseStream) -> Self {
        Self {
            response: response.into(),
            stream: Some(stream),
        }
    }
}

pub fn serve<Ctx, Shutdown, OnConnect, OnConnectRes, Handler, HandlerRes>(
    addr: SocketAddr,
    options: Options,
    shutdown: Shutdown,
    on_connect: OnConnect,
    handler: Handler,
    ) -> impl Future<Item=(), Error=()>
    where
        Ctx: Send + 'static,
        OnConnect: Fn() -> OnConnectRes + Send + 'static,
        OnConnectRes: IntoFuture<Item=Ctx, Error=Error>,
        OnConnectRes::Future: Send + 'static,
        Handler: Fn(&mut Ctx, Request, RequestStream)
            -> HandlerRes + Send + Clone + 'static,
        HandlerRes: IntoFuture<Item=HandlerResult, Error=Error>,
        HandlerRes::Future: Send + 'static,
        Shutdown: Future<Item = (), Error = ()> + Clone + Send + 'static,
{
    let socket_timeout = options.socket_timeout;
    let limits = StreamLimits {
        max_frame_count: options.max_frame_count,
        max_single_payload_len: options.max_single_payload_len,
        max_total_payload_len: options.max_total_payload_len,
    };
    future::lazy(move || {
        info!("starting RPC server at {}", addr);
        let listener = match TcpListener::bind(&addr) {
            Ok(v) => v,
            Err(e) => {
                error!("couldn't bind listener at {}: {}", addr, e);
                return future::err(()).into_box();
            }
        };
        listener
            .incoming()
            .sleep_on_error(options.sleep_on_error)
            .for_each(clone!(shutdown => move |socket| {
                let sock_id = socket_id_debug(&socket);
                debug!("[{:?}] new connection", sock_id);
                let handler = handler.clone();
                tokio::spawn(on_connect()
                    .into_future()
                    .map_err(move |e| error!("[{:?}] error in on_connect(): {:?}", sock_id, e))
                    .and_then(clone!(shutdown => move |mut ctx| {
                        let (rd, wr) = socket.split();
                        let wr = ShexAsyncWrite::new(wr);
                        read_requests(rd, socket_timeout, limits)
                            .map_err(move |e| error!("[{:?}] error reading request: {:?}", sock_id, e))
                            .take_until(shutdown.map(move |_| debug!("[{:?}] shutting down connection", sock_id)))
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
                                            .timeout(socket_timeout)
                                            .map_err(move |e| error!("[{:?}] error writing response: {:?}", sock_id, e))
                                            .map(move |(wr, _)| (wr, r))
                                    }))
                                    .and_then(move |(wr, r)| {
                                        if let Some(stream) = r.stream {
                                            write_response_stream_frames(stream, sock_id, wr, socket_timeout).into_box()
                                        } else {
                                            future::ok(()).into_box()
                                        }
                                    })
                            })
                    })))
            }))
            .select(shutdown.map(move |_| info!("shutting down RPC server at {}", addr)))
            .map_all_unit()
            .into_box()
    })
}

#[derive(Clone, Copy)]
struct StreamLimits {
    max_frame_count: u32,
    max_single_payload_len: u32,
    max_total_payload_len: u32,
}

fn read_requests<R>(rd: R, timeout: Duration, limits: StreamLimits)
    -> impl Stream<Item=(Request, RequestStream), Error=Error>
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
                                read_request_stream_frames(rd.clone(), timeout, limits).into_box()
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

fn read_request_stream_frames<R>(rd: R, timeout: Duration, limits: StreamLimits)
    -> impl Stream<Item=RequestStreamFrame, Error=Error>
    where R: 'static + AsyncRead + Send
{
    #[derive(Clone, Copy, Default)]
    struct State {
        frame_count: u32,
        total_payload_len: u32,
    }

    stream::unfold(Some((rd, State::default())), move |state| {
        if let Some((rd, state)) = state {
            Some(read_pb_frame::<rproto::common::RequestStreamFrameHeader, _>(rd)
                .timeout(timeout)
                .map_err(|e| e.wrap_id(ErrorId::Io))
                .and_then(move |(rd, sfh)| {
                    if sfh.end_of_stream {
                        future::ok((None, None)).into_box()
                    } else {
                        if state.frame_count == limits.max_frame_count {
                            return future::err(Error::new(ErrorId::Todo,
                                "request stream has too many frames"))
                                .into_box();
                        }

                        let max_len = (limits.max_total_payload_len - state.total_payload_len)
                            .min(limits.max_single_payload_len);

                        let stream_id = sfh.stream_id;
                        read_stream_frame_payload(rd, max_len as usize)
                            .timeout(timeout)
                            .map_err(|e| e.wrap_id(ErrorId::Io))
                            .map(move |(rd, payload)| {
                                let state = State {
                                    frame_count: state.frame_count + 1,
                                    total_payload_len: state.total_payload_len + payload.len() as u32,
                                };
                                (Some(RequestStreamFrame { stream_id, payload }),
                                    Some((rd, state)))
                            })
                            .into_box()
                    }
                }))
        } else {
            None
        }
    })
        .filter_map(|r| r)
}

fn write_response_stream_frames(
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
            stream_id,
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