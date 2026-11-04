// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Shard-node side of the internode RDMA data plane.
//!
//! The server authenticates every request with the shared RPC secret before
//! touching a disk, then hands the request to an [`RdmaRequestHandler`]
//! supplied by the binary, which owns disk lookup, background admission and
//! the authenticated put_file commit. A rejected request is answered with a
//! typed error message and the connection stays open; a transport failure
//! closes the connection.

use super::RdmaSettings;
use super::buffer::RegisteredPool;
use super::conn::{Event, RdmaConn};
use super::error::RdmaError;
use super::ffi::{Connection, Listener, RawFdHandle, async_fd};
use super::proto::{
    Auth, ErrorCode, Message, RDMA_PROTOCOL_VERSION, RDMA_PUT_FILE_STREAM_PATH, ReadRequest, StreamOp, StreamRequest,
    WindowDescriptor, WriteRequest,
};
use super::stream::{ReaderSource, push_stream, receive_stream, window_descriptor};
use crate::cluster::rpc::http_auth::{check_and_record_signed_rpc_nonce, tonic_rpc_auth_failure_reason, verify_rpc_signature};
use crate::diagnostics::get::{
    GET_OBJECT_PATH_RDMA_TRANSPORT, GET_STAGE_RDMA_SERVE_OPEN, get_stage_timer, record_get_stage_duration_if_enabled,
};
use crate::disk::FileReader;
use crate::disk::error::DiskError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use http::HeaderMap;
use rustfs_io_metrics::internode_metrics::{
    INTERNODE_OPERATION_PUT_FILE_STREAM, INTERNODE_OPERATION_READ_FILE_STREAM, INTERNODE_TRANSPORT_BACKEND_RDMA,
    global_internode_metrics,
};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const LOG_COMPONENT_INTERNODE_RPC: &str = "internode_rpc";
const LOG_SUBSYSTEM_RDMA_SERVER: &str = "rdma_server";
const EVENT_RDMA_LISTENER_STATE: &str = "rdma_listener_state";
const EVENT_RPC_REQUEST_REJECTED: &str = "rpc_request_rejected";
const EVENT_RDMA_CONNECTION_CLOSED: &str = "rdma_connection_closed";
const LISTEN_BACKLOG: i32 = 128;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Body of an incoming write stream, delivered as owned chunks ending with the
/// client's authentication trailer exactly like the HTTP route's body.
pub type RdmaBodyStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>;

/// An authenticated read request.
#[derive(Debug, Clone)]
pub struct RdmaReadRequest {
    pub disk: String,
    pub volume: String,
    pub path: String,
    pub offset: u64,
    pub length: u64,
    pub background: bool,
    /// Canonical request target the signature covered.
    pub target: String,
}

/// An authenticated write request; replay protection for `nonce` already ran.
#[derive(Debug, Clone)]
pub struct RdmaWriteRequest {
    pub disk: String,
    pub volume: String,
    pub path: String,
    pub append: bool,
    pub size: i64,
    pub nonce: Uuid,
    pub server_epoch: Option<Uuid>,
    pub background: bool,
    pub target: String,
}

/// A typed rejection sent back to the client.
#[derive(Debug)]
pub struct RdmaReject {
    pub code: ErrorCode,
    pub message: String,
}

impl RdmaReject {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn from_disk_error(error: &DiskError, context: &str) -> Self {
        Self::new(ErrorCode::from_disk_error(error), format!("{context} {error}"))
    }
}

/// Disk-side behavior supplied by the binary.
#[async_trait]
pub trait RdmaRequestHandler: Send + Sync + 'static {
    async fn read(&self, request: RdmaReadRequest) -> std::result::Result<FileReader, RdmaReject>;
    async fn write(&self, request: RdmaWriteRequest, body: RdmaBodyStream) -> std::result::Result<u64, RdmaReject>;
}

struct ServerContext {
    settings: Arc<RdmaSettings>,
    pool: Arc<RegisteredPool>,
    handler: Arc<dyn RdmaRequestHandler>,
}

struct BoundListener {
    // Deregisters from the reactor before the listener closes its descriptor.
    fd: AsyncFd<RawFdHandle>,
    listener: Listener,
}

/// Accepts RDMA connections on one or more local addresses.
pub struct RdmaServer {
    cancel: CancellationToken,
    tracker: TaskTracker,
    local_addrs: Vec<SocketAddr>,
}

impl std::fmt::Debug for RdmaServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaServer").field("local_addrs", &self.local_addrs).finish()
    }
}

impl RdmaServer {
    /// Binds every address and starts accepting. Fails closed if any bind fails.
    pub async fn bind(
        listen: &[SocketAddr],
        settings: Arc<RdmaSettings>,
        pool: Arc<RegisteredPool>,
        handler: Arc<dyn RdmaRequestHandler>,
    ) -> std::result::Result<Self, RdmaError> {
        if listen.is_empty() {
            return Err(RdmaError::Protocol("rdma server needs at least one listen address".to_owned()));
        }
        let ctx = Arc::new(ServerContext { settings, pool, handler });
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();
        let mut bound = Vec::with_capacity(listen.len());
        for addr in listen {
            let listener = Listener::bind(*addr, LISTEN_BACKLOG)?;
            if let Some(device) = listener.device() {
                let pool = Arc::clone(&ctx.pool);
                tokio::task::spawn_blocking(move || pool.register_device(device))
                    .await
                    .map_err(|err| RdmaError::Protocol(format!("rdma pool registration task failed: {err}")))??;
            }
            let fd = async_fd("register listener fd", listener.fd())?;
            bound.push(BoundListener { fd, listener });
        }
        let local_addrs: Vec<SocketAddr> = bound.iter().map(|bound| bound.listener.local_addr()).collect();
        for bound in bound {
            tracker.spawn(accept_loop(bound, Arc::clone(&ctx), cancel.clone(), tracker.clone()));
        }
        info!(
            event = EVENT_RDMA_LISTENER_STATE,
            component = LOG_COMPONENT_INTERNODE_RPC,
            subsystem = LOG_SUBSYSTEM_RDMA_SERVER,
            state = "listening",
            listener_count = local_addrs.len(),
            "rdma internode listeners started"
        );
        Ok(Self {
            cancel,
            tracker,
            local_addrs,
        })
    }

    pub fn local_addrs(&self) -> &[SocketAddr] {
        &self.local_addrs
    }

    /// Stops accepting, aborts active connections, and waits briefly for tasks to finish.
    pub async fn shutdown(self) {
        self.cancel.cancel();
        self.tracker.close();
        if tokio::time::timeout(SHUTDOWN_GRACE, self.tracker.wait()).await.is_err() {
            warn!(
                event = EVENT_RDMA_LISTENER_STATE,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem = LOG_SUBSYSTEM_RDMA_SERVER,
                state = "shutdown_timeout",
                "rdma connection tasks did not finish within the shutdown grace period"
            );
        }
    }
}

async fn accept_loop(bound: BoundListener, ctx: Arc<ServerContext>, cancel: CancellationToken, tracker: TaskTracker) {
    let BoundListener { fd, listener } = &bound;
    loop {
        let mut guard = tokio::select! {
            _ = cancel.cancelled() => return,
            ready = fd.readable() => match ready {
                Ok(guard) => guard,
                Err(err) => {
                    error!(
                        event = EVENT_RDMA_LISTENER_STATE,
                        component = LOG_COMPONENT_INTERNODE_RPC,
                        subsystem = LOG_SUBSYSTEM_RDMA_SERVER,
                        state = "failed",
                        error = %err,
                        "rdma listener wait failed"
                    );
                    return;
                }
            },
        };
        loop {
            match listener.try_accept(&ctx.settings.qp_limits()) {
                Ok(Some(raw)) => {
                    tracker.spawn(serve_connection(raw, Arc::clone(&ctx), cancel.clone()));
                }
                Ok(None) => {
                    guard.clear_ready();
                    break;
                }
                Err(err) => {
                    warn!(
                        event = EVENT_RDMA_LISTENER_STATE,
                        component = LOG_COMPONENT_INTERNODE_RPC,
                        subsystem = LOG_SUBSYSTEM_RDMA_SERVER,
                        state = "accept_failed",
                        error = %err,
                        "rdma accept failed"
                    );
                    guard.clear_ready();
                    break;
                }
            }
        }
    }
}

async fn serve_connection(raw: Connection, ctx: Arc<ServerContext>, cancel: CancellationToken) {
    let mut conn = match RdmaConn::accept(raw, ctx.settings.connect_timeout).await {
        Ok(conn) => conn,
        Err(err) => {
            debug!(
                event = EVENT_RDMA_CONNECTION_CLOSED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem = LOG_SUBSYSTEM_RDMA_SERVER,
                result = "accept_failed",
                error = %err,
                "rdma connection not established"
            );
            return;
        }
    };
    loop {
        let event = tokio::select! {
            _ = cancel.cancelled() => {
                conn.abort();
                return;
            }
            event = conn.next_event() => event,
        };
        match event {
            Ok(Event::Message(Message::Request(request))) => {
                let StreamRequest {
                    version,
                    stream,
                    op,
                    auth,
                    window,
                } = *request;
                if let Err(err) = handle_request(&mut conn, &ctx, version, stream, op, auth, window).await {
                    debug!(
                        event = EVENT_RDMA_CONNECTION_CLOSED,
                        component = LOG_COMPONENT_INTERNODE_RPC,
                        subsystem = LOG_SUBSYSTEM_RDMA_SERVER,
                        result = "transport_failed",
                        error = %err,
                        "rdma connection closed after transport failure"
                    );
                    conn.abort();
                    return;
                }
            }
            Ok(Event::Message(other)) => {
                debug!(stream = other.stream(), "ignoring stale rdma control message");
            }
            Ok(Event::WriteDone) => {}
            Err(err) => {
                // Peer disconnects surface as flushed receives; that is the normal end of a connection.
                debug!(
                    event = EVENT_RDMA_CONNECTION_CLOSED,
                    component = LOG_COMPONENT_INTERNODE_RPC,
                    subsystem = LOG_SUBSYSTEM_RDMA_SERVER,
                    result = "closed",
                    error = %err,
                    "rdma connection closed"
                );
                return;
            }
        }
    }
}

enum Served {
    Done(u64),
    Read,
}

/// Outcome of one request: a typed rejection for the client or a transport failure.
enum ServeError {
    Reject(RdmaReject),
    Transport(RdmaError),
}

impl From<RdmaError> for ServeError {
    fn from(error: RdmaError) -> Self {
        Self::Transport(error)
    }
}

impl From<RdmaReject> for ServeError {
    fn from(reject: RdmaReject) -> Self {
        Self::Reject(reject)
    }
}

#[allow(clippy::too_many_arguments, reason = "request fields arrive unpacked from the message")]
async fn handle_request(
    conn: &mut RdmaConn,
    ctx: &Arc<ServerContext>,
    version: u16,
    stream: u64,
    op: StreamOp,
    auth: Auth,
    window: Option<WindowDescriptor>,
) -> std::result::Result<(), RdmaError> {
    let started = Instant::now();
    let operation = match &op {
        StreamOp::Read(_) => INTERNODE_OPERATION_READ_FILE_STREAM,
        StreamOp::Write(_) => INTERNODE_OPERATION_PUT_FILE_STREAM,
    };
    let metrics = global_internode_metrics();
    if version != RDMA_PROTOCOL_VERSION {
        return reply_error(
            conn,
            stream,
            ErrorCode::Unsupported,
            format!("rdma protocol version {version} is unsupported; this node speaks {RDMA_PROTOCOL_VERSION}"),
        )
        .await;
    }
    let target = op.canonical_target();
    let headers = match auth.to_headers() {
        Ok(headers) => headers,
        Err(err) => return reply_error(conn, stream, ErrorCode::BadRequest, format!("invalid auth: {err}")).await,
    };
    if let Err(err) = verify_rpc_signature(&target, &op.method(), &headers) {
        metrics.record_rpc_auth_failure_for_operation_and_backend(
            operation,
            INTERNODE_TRANSPORT_BACKEND_RDMA,
            tonic_rpc_auth_failure_reason(&err),
        );
        metrics.record_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA);
        warn!(
            event = EVENT_RPC_REQUEST_REJECTED,
            component = LOG_COMPONENT_INTERNODE_RPC,
            subsystem = LOG_SUBSYSTEM_RDMA_SERVER,
            operation,
            result = "rejected",
            rpc_path = op.path(),
            reason = "signature_verification_failed",
            error = %err,
            "internode rpc request rejected"
        );
        return reply_error(conn, stream, ErrorCode::Unauthorized, "rpc signature verification failed").await;
    }
    metrics.record_incoming_request_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA);

    let served = match op {
        StreamOp::Read(request) => match window {
            Some(window) => serve_read(conn, ctx, stream, request, auth.background, target, window).await,
            None => Err(ServeError::Reject(RdmaReject::new(
                ErrorCode::BadRequest,
                "read request carries no receive window",
            ))),
        },
        StreamOp::Write(request) => serve_write(conn, ctx, stream, request, auth.background, target, &headers).await,
    };
    metrics.record_duration_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA, started.elapsed());
    match served {
        Ok(Served::Read) => Ok(()),
        Ok(Served::Done(bytes)) => conn.send_message(&Message::Done { stream, bytes }).await,
        Err(ServeError::Reject(reject)) => {
            metrics.record_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA);
            warn!(
                event = EVENT_RPC_REQUEST_REJECTED,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem = LOG_SUBSYSTEM_RDMA_SERVER,
                operation,
                result = "rejected",
                rpc_path = match operation {
                    INTERNODE_OPERATION_PUT_FILE_STREAM => RDMA_PUT_FILE_STREAM_PATH,
                    _ => super::proto::RDMA_READ_FILE_STREAM_PATH,
                },
                reason = ?reject.code,
                error = %reject.message,
                "internode rpc request rejected"
            );
            reply_error(conn, stream, reject.code, reject.message).await
        }
        Err(ServeError::Transport(error)) => {
            metrics.record_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA);
            Err(error)
        }
    }
}

async fn reply_error(
    conn: &mut RdmaConn,
    stream: u64,
    code: ErrorCode,
    message: impl Into<String>,
) -> std::result::Result<(), RdmaError> {
    conn.send_message(&Message::Error {
        stream,
        code,
        message: message.into(),
    })
    .await
}

async fn serve_read(
    conn: &mut RdmaConn,
    ctx: &Arc<ServerContext>,
    stream: u64,
    request: ReadRequest,
    background: bool,
    target: String,
    window: WindowDescriptor,
) -> std::result::Result<Served, ServeError> {
    let stage = get_stage_timer();
    let reader = ctx
        .handler
        .read(RdmaReadRequest {
            disk: request.disk,
            volume: request.volume,
            path: request.path,
            offset: request.offset,
            length: request.length,
            background,
            target,
        })
        .await?;
    record_get_stage_duration_if_enabled(GET_OBJECT_PATH_RDMA_TRANSPORT, GET_STAGE_RDMA_SERVE_OPEN, stage);
    let limit = if request.length == 0 { u64::MAX } else { request.length };
    let source = ReaderSource::new(reader.take(limit));
    let metrics = global_internode_metrics();
    push_stream(conn, &ctx.pool, stream, &window, source, |bytes| {
        metrics.record_sent_bytes_for_operation_and_backend(
            INTERNODE_OPERATION_READ_FILE_STREAM,
            INTERNODE_TRANSPORT_BACKEND_RDMA,
            bytes,
        );
    })
    .await?;
    Ok(Served::Read)
}

async fn serve_write(
    conn: &mut RdmaConn,
    ctx: &Arc<ServerContext>,
    stream: u64,
    request: WriteRequest,
    background: bool,
    target: String,
    headers: &HeaderMap,
) -> std::result::Result<Served, ServeError> {
    check_and_record_signed_rpc_nonce(
        headers,
        request.nonce,
        RDMA_PUT_FILE_STREAM_PATH,
        INTERNODE_OPERATION_PUT_FILE_STREAM,
        INTERNODE_TRANSPORT_BACKEND_RDMA,
    )
    .map_err(|err| RdmaReject::new(ErrorCode::Unauthorized, format!("invalid put_file auth: {err}")))?;
    let slots = ctx
        .pool
        .try_reserve(ctx.settings.stream_slots)
        .map_err(|_| RdmaReject::new(ErrorCode::Busy, "rdma registered buffer pool exhausted"))?;
    let (_, rkey) = ctx.pool.keys_for(conn.raw())?;
    conn.send_message(&Message::Window {
        stream,
        window: window_descriptor(&slots, rkey)?,
    })
    .await?;

    let (tx, rx) = mpsc::channel::<io::Result<Bytes>>(ctx.settings.stream_slots.max(2));
    let body: RdmaBodyStream = Box::pin(ReceiverStream::new(rx));
    let handler = ctx.handler.write(
        RdmaWriteRequest {
            disk: request.disk,
            volume: request.volume,
            path: request.path,
            append: request.append,
            size: request.size,
            nonce: request.nonce,
            server_epoch: request.server_epoch,
            background,
            target,
        },
        body,
    );
    let metrics = global_internode_metrics();
    let stall = Some(ctx.settings.stall_timeout);
    let receive = async {
        let tx = tx;
        receive_stream(conn, stream, slots, &tx, stall, None, |bytes| {
            metrics.record_recv_bytes_for_operation_and_backend(
                INTERNODE_OPERATION_PUT_FILE_STREAM,
                INTERNODE_TRANSPORT_BACKEND_RDMA,
                bytes,
            );
        })
        .await
        // `tx` drops here so the handler observes end of body.
    };
    let (received, handled) = tokio::join!(receive, handler);
    match (received, handled) {
        (Ok(_), Ok(bytes)) => Ok(Served::Done(bytes)),
        (Err(RdmaError::ConsumerGone), Err(reject)) | (Ok(_), Err(reject)) => Err(ServeError::Reject(reject)),
        (Err(RdmaError::ConsumerGone), Ok(bytes)) => Ok(Served::Done(bytes)),
        (Err(error), _) => Err(ServeError::Transport(error)),
    }
}
