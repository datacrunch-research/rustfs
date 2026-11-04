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

//! Coordinator side of the internode RDMA data plane.
//!
//! Shard reads and writes travel over RDMA; directory walks, scanner streams,
//! and capability probes stay on the TCP backend. A setup failure that
//! happens before any payload byte moved falls back to `tcp-http` for that
//! transfer when `RUSTFS_RDMA_FALLBACK_TCP` allows it, is counted under the
//! `rdma` backend label, and puts the peer into a short backoff so a peer
//! without RDMA does not pay a connect timeout on every request.

use super::RdmaSettings;
use super::buffer::RegisteredPool;
use super::conn::{Event, RdmaConn};
use super::error::RdmaError;
use super::proto::{Auth, Message, RDMA_PROTOCOL_VERSION, ReadRequest, StreamOp, StreamRequest, WriteRequest};
use super::stream::{StreamSource, next_event_with_stall, push_stream, receive_stream, window_descriptor};
use crate::cluster::rpc::internode_data_transport::{
    InternodeDataTransport, InternodeDataTransportCapabilities, NsScannerCapabilityRequest, NsScannerStreamRequest,
    PutFileAuthWriter, ReadStreamRequest, TcpHttpInternodeDataTransport, WalkDirStreamRequest, WriteStreamRequest,
    reject_put_file_server_epoch,
};
use crate::diagnostics::get::{
    GET_OBJECT_PATH_RDMA_TRANSPORT, GET_STAGE_RDMA_CHECKOUT_IDLE, GET_STAGE_RDMA_OPEN_FIRST_EVENT, GET_STAGE_RDMA_OPEN_RESERVE,
    get_stage_timer, record_get_stage_duration_if_enabled,
};
use crate::disk::error::{DiskError, Result};
use crate::disk::{FileReader, FileWriter};
use crate::storage_api_contracts::internode::NsScannerCapabilityResponse;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use rustfs_config::INTERNODE_DATA_TRANSPORT_RDMA;
use rustfs_io_metrics::internode_metrics::{
    INTERNODE_OPERATION_PUT_FILE_STREAM, INTERNODE_OPERATION_READ_FILE_STREAM, INTERNODE_TRANSPORT_BACKEND_RDMA,
    global_internode_metrics,
};
use rustfs_rio::{ChunkReader, ChunkReaderBox};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::PollSender;
use tracing::{debug, warn};
use uuid::Uuid;

const LOG_COMPONENT_INTERNODE_RPC: &str = "internode_rpc";
const LOG_SUBSYSTEM_RDMA_TRANSPORT: &str = "rdma_transport";
const EVENT_RDMA_TRANSPORT_FALLBACK: &str = "rdma_transport_fallback";
const EVENT_RDMA_STREAM_FAILED: &str = "rdma_stream_failed";
const EVENT_RDMA_FRESH_CONNECT: &str = "rdma_fresh_connect";
const EVENT_RDMA_REFILL_FAILED: &str = "rdma_refill_failed";
const FALLBACK_CLASSIFICATION: &str = "tcp_fallback";

/// How long a peer stays on the TCP path after an RDMA setup failure.
const PEER_BACKOFF: Duration = Duration::from_secs(10);
const RESOLVE_TTL: Duration = Duration::from_secs(60);
const IDLE_TTL: Duration = Duration::from_secs(60);

struct IdlePool {
    conns: parking_lot::Mutex<HashMap<SocketAddr, Vec<(RdmaConn, Instant)>>>,
    /// Idle connections kept per peer; see `RUSTFS_RDMA_IDLE_CONNS_PER_PEER`.
    max_idle_per_peer: usize,
    /// Idle connections refilled in the background; see `RUSTFS_RDMA_MIN_IDLE_CONNS_PER_PEER`.
    min_idle_per_peer: usize,
    /// Refill dials in flight per peer, counted toward the minimum so a burst
    /// of takes does not fan out into a burst of dials.
    refilling: parking_lot::Mutex<HashMap<SocketAddr, usize>>,
    /// Connections dialed because no idle one was available (diagnostics).
    fresh_connects: std::sync::atomic::AtomicU64,
}

/// Refill dials in flight per peer at most; a dial storm makes every
/// handshake slow (tens of parallel dials took seconds each on a busy node).
const REFILL_DIAL_CONCURRENCY: usize = 8;

impl IdlePool {
    fn new(max_idle_per_peer: usize, min_idle_per_peer: usize) -> Self {
        let max_idle_per_peer = max_idle_per_peer.max(1);
        Self {
            conns: parking_lot::Mutex::new(HashMap::new()),
            max_idle_per_peer,
            min_idle_per_peer: min_idle_per_peer.min(max_idle_per_peer),
            refilling: parking_lot::Mutex::new(HashMap::new()),
            fresh_connects: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn idle_len(&self, addr: SocketAddr) -> usize {
        self.conns.lock().get(&addr).map_or(0, Vec::len)
    }

    /// Idle connections missing below the minimum once in-flight refills land.
    fn deficit(&self, addr: SocketAddr, refilling: usize) -> usize {
        self.min_idle_per_peer
            .saturating_sub(self.idle_len(addr).saturating_add(refilling))
    }

    /// Reserves up to `REFILL_DIAL_CONCURRENCY` refill dials for `addr` and
    /// returns how many the caller must start; each one ends with
    /// `continue_refill` or `release_refill`.
    fn claim_refills(&self, addr: SocketAddr) -> usize {
        let mut refilling = self.refilling.lock();
        let in_flight = refilling.get(&addr).copied().unwrap_or(0);
        let claimed = self
            .deficit(addr, in_flight)
            .min(REFILL_DIAL_CONCURRENCY.saturating_sub(in_flight));
        if claimed > 0 {
            refilling.insert(addr, in_flight + claimed);
        }
        claimed
    }

    /// Whether a refill dialer that just landed a connection should dial again;
    /// releases its reservation when the minimum is covered.
    fn continue_refill(&self, addr: SocketAddr) -> bool {
        let mut refilling = self.refilling.lock();
        let in_flight = refilling.get(&addr).copied().unwrap_or(0);
        let others = in_flight.saturating_sub(1);
        if self.deficit(addr, others) > 0 {
            return true;
        }
        if others == 0 {
            refilling.remove(&addr);
        } else {
            refilling.insert(addr, others);
        }
        false
    }

    fn release_refill(&self, addr: SocketAddr) {
        let mut refilling = self.refilling.lock();
        match refilling.get(&addr).copied().unwrap_or(0) {
            0 | 1 => {
                refilling.remove(&addr);
            }
            in_flight => {
                refilling.insert(addr, in_flight - 1);
            }
        }
    }

    fn take(&self, addr: SocketAddr) -> Option<RdmaConn> {
        let mut stale = Vec::new();
        let taken = {
            let mut conns = self.conns.lock();
            let list = conns.get_mut(&addr)?;
            let mut taken = None;
            while let Some((conn, since)) = list.pop() {
                if since.elapsed() < IDLE_TTL && conn.is_healthy() {
                    taken = Some(conn);
                    break;
                }
                stale.push(conn);
            }
            taken
        };
        // Closing a connection makes syscalls; keep that outside the lock.
        drop(stale);
        taken
    }

    fn put(&self, addr: SocketAddr, conn: RdmaConn) {
        let mut evicted = Vec::new();
        {
            let mut conns = self.conns.lock();
            let list = conns.entry(addr).or_default();
            let now = Instant::now();
            let (keep, expired): (Vec<_>, Vec<_>) = list.drain(..).partition(|(_, since)| now.duration_since(*since) < IDLE_TTL);
            evicted.extend(expired.into_iter().map(|(conn, _)| conn));
            *list = keep;
            if list.len() < self.max_idle_per_peer {
                list.push((conn, now));
            } else {
                evicted.push(conn);
            }
        }
        drop(evicted);
    }
}

/// Internode data transport that moves shard bytes over RDMA.
pub struct RdmaInternodeDataTransport {
    tcp: TcpHttpInternodeDataTransport,
    settings: Arc<RdmaSettings>,
    pool: Arc<RegisteredPool>,
    idle: Arc<IdlePool>,
    resolved: parking_lot::Mutex<HashMap<String, (SocketAddr, Instant)>>,
    backoff: parking_lot::Mutex<HashMap<String, Instant>>,
}

impl std::fmt::Debug for RdmaInternodeDataTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaInternodeDataTransport")
            .field("settings", &self.settings)
            .field("pool", &self.pool)
            .finish()
    }
}

impl RdmaInternodeDataTransport {
    pub fn new(settings: Arc<RdmaSettings>, pool: Arc<RegisteredPool>) -> Self {
        let idle = IdlePool::new(settings.idle_conns_per_peer, settings.min_idle_conns_per_peer);
        Self {
            tcp: TcpHttpInternodeDataTransport,
            settings,
            pool,
            idle: Arc::new(idle),
            resolved: parking_lot::Mutex::new(HashMap::new()),
            backoff: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// Maps an internode HTTP endpoint to the peer's RDMA listener address.
    async fn resolve(&self, endpoint: &str) -> std::result::Result<SocketAddr, RdmaError> {
        if let Some((addr, at)) = self.resolved.lock().get(endpoint).copied()
            && at.elapsed() < RESOLVE_TTL
        {
            return Ok(addr);
        }
        let url = url::Url::parse(endpoint).map_err(|_| RdmaError::Endpoint(endpoint.to_owned()))?;
        let host = url
            .host_str()
            .ok_or_else(|| RdmaError::Endpoint(endpoint.to_owned()))?
            .trim_matches(|c| c == '[' || c == ']')
            .to_owned();
        let port = url
            .port_or_known_default()
            .and_then(|port| port.checked_add(self.settings.port_offset))
            .ok_or_else(|| RdmaError::Endpoint(endpoint.to_owned()))?;
        let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(|source| RdmaError::Resolve {
                endpoint: endpoint.to_owned(),
                source,
            })?
            .collect();
        let addr = addrs
            .iter()
            .find(|addr| addr.is_ipv4())
            .or_else(|| addrs.first())
            .copied()
            .ok_or_else(|| RdmaError::Resolve {
                endpoint: endpoint.to_owned(),
                source: io::Error::new(io::ErrorKind::AddrNotAvailable, "host resolved to no address"),
            })?;
        self.resolved.lock().insert(endpoint.to_owned(), (addr, Instant::now()));
        Ok(addr)
    }

    async fn checkout(&self, endpoint: &str, fresh: bool) -> std::result::Result<(SocketAddr, RdmaConn), RdmaError> {
        let addr = self.resolve(endpoint).await?;
        let idle = if fresh { None } else { self.idle.take(addr) };
        self.refill_idle(addr);
        if let Some(conn) = idle {
            record_get_stage_duration_if_enabled(GET_OBJECT_PATH_RDMA_TRANSPORT, GET_STAGE_RDMA_CHECKOUT_IDLE, get_stage_timer());
            return Ok((addr, conn));
        }
        let conn = RdmaConn::connect(addr, self.settings.connect_timeout, self.settings.qp_limits()).await?;
        let fresh_connects = self.idle.fresh_connects.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        if fresh_connects.is_power_of_two() {
            debug!(
                event = EVENT_RDMA_FRESH_CONNECT,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem = LOG_SUBSYSTEM_RDMA_TRANSPORT,
                peer = %addr,
                fresh_connects,
                idle_conns_per_peer = self.idle.max_idle_per_peer,
                "rdma transport dialed a fresh connection (no idle connection to the peer)"
            );
        }
        Ok((addr, conn))
    }

    /// Starts background dials until `addr` holds the configured minimum of
    /// idle connections, bounded by `REFILL_DIAL_CONCURRENCY` in flight.
    fn refill_idle(&self, addr: SocketAddr) {
        let claimed = self.idle.claim_refills(addr);
        for _ in 0..claimed {
            let idle = Arc::clone(&self.idle);
            let timeout = self.settings.connect_timeout;
            let limits = self.settings.qp_limits();
            tokio::spawn(async move {
                loop {
                    match RdmaConn::connect(addr, timeout, limits).await {
                        Ok(conn) => idle.put(addr, conn),
                        Err(error) => {
                            idle.release_refill(addr);
                            debug!(
                                event = EVENT_RDMA_REFILL_FAILED,
                                component = LOG_COMPONENT_INTERNODE_RPC,
                                subsystem = LOG_SUBSYSTEM_RDMA_TRANSPORT,
                                peer = %addr,
                                result = "failed",
                                error = %error,
                                "rdma idle pool refill dial failed"
                            );
                            return;
                        }
                    }
                    if !idle.continue_refill(addr) {
                        return;
                    }
                }
            });
        }
    }

    pub(super) fn peer_backing_off(&self, endpoint: &str) -> bool {
        self.backoff.lock().get(endpoint).is_some_and(|until| Instant::now() < *until)
    }

    /// Records `error` and decides whether the request may be reissued over TCP.
    fn fall_back(&self, endpoint: &str, operation: &'static str, error: &RdmaError) -> bool {
        let metrics = global_internode_metrics();
        metrics.record_classified_error_for_operation_and_backend(
            operation,
            INTERNODE_TRANSPORT_BACKEND_RDMA,
            error.classification(),
        );
        if !(self.settings.fallback_tcp && error.is_setup_failure()) {
            metrics.record_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA);
            return false;
        }
        metrics.record_classified_error_for_operation_and_backend(
            operation,
            INTERNODE_TRANSPORT_BACKEND_RDMA,
            FALLBACK_CLASSIFICATION,
        );
        let newly_backing_off = {
            let mut backoff = self.backoff.lock();
            let now = Instant::now();
            let newly = backoff.get(endpoint).is_none_or(|until| *until <= now);
            backoff.insert(endpoint.to_owned(), now + PEER_BACKOFF);
            newly
        };
        if newly_backing_off {
            warn!(
                event = EVENT_RDMA_TRANSPORT_FALLBACK,
                component = LOG_COMPONENT_INTERNODE_RPC,
                subsystem = LOG_SUBSYSTEM_RDMA_TRANSPORT,
                operation,
                result = "fallback",
                backoff_secs = PEER_BACKOFF.as_secs(),
                error = %error,
                "rdma transport unavailable for peer, using tcp-http"
            );
        }
        true
    }

    async fn rdma_read(&self, request: &ReadStreamRequest, fresh: bool) -> std::result::Result<ChunkReaderBox, RdmaError> {
        let operation = INTERNODE_OPERATION_READ_FILE_STREAM;
        let (addr, mut conn) = self.checkout(&request.endpoint, fresh).await?;
        let stage = get_stage_timer();
        let slots = match self.pool.try_reserve(self.settings.stream_slots) {
            Ok(slots) => slots,
            Err(error) => {
                self.idle.put(addr, conn);
                return Err(error);
            }
        };
        let (_, rkey) = self.pool.keys_for(conn.raw())?;
        let window = window_descriptor(&slots, rkey)?;
        record_get_stage_duration_if_enabled(GET_OBJECT_PATH_RDMA_TRANSPORT, GET_STAGE_RDMA_OPEN_RESERVE, stage);
        let op = StreamOp::Read(ReadRequest {
            disk: request.disk.clone(),
            volume: request.volume.clone(),
            path: request.path.clone(),
            offset: u64::try_from(request.offset).map_err(|_| RdmaError::Protocol("read offset overflow".to_owned()))?,
            length: u64::try_from(request.length).map_err(|_| RdmaError::Protocol("read length overflow".to_owned()))?,
        });
        let auth = Auth::sign(&op.canonical_target(), &op.method())?;
        let stream = conn.next_stream_id();
        let metrics = global_internode_metrics();
        metrics.record_outgoing_request_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA);
        let started = Instant::now();
        let stall = request.stall_timeout.or(Some(self.settings.stall_timeout));
        let stage = get_stage_timer();
        let opened = async {
            conn.send_message(&Message::Request(Box::new(StreamRequest {
                version: RDMA_PROTOCOL_VERSION,
                stream,
                op,
                auth,
                window: Some(window),
            })))
            .await?;
            next_event_with_stall(&mut conn, stall).await
        }
        .await;
        record_get_stage_duration_if_enabled(GET_OBJECT_PATH_RDMA_TRANSPORT, GET_STAGE_RDMA_OPEN_FIRST_EVENT, stage);
        let first = match opened {
            Ok(first) => first,
            Err(error) => {
                conn.abort();
                metrics.record_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA);
                return Err(error);
            }
        };
        if let Event::Message(Message::Error {
            stream: failed,
            code,
            message,
        }) = &first
            && *failed == stream
        {
            // An open-time rejection leaves the connection usable; nothing was written into the window.
            let error = RdmaError::Remote {
                code: *code,
                message: message.clone(),
            };
            metrics.record_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA);
            self.idle.put(addr, conn);
            return Err(error);
        }

        let (tx, rx) = mpsc::channel(self.settings.stream_slots.max(2));
        let idle = Arc::clone(&self.idle);
        tokio::spawn(async move {
            let mut conn = conn;
            let result = receive_stream(&mut conn, stream, slots, &tx, stall, Some(first), |bytes| {
                metrics.record_recv_bytes_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA, bytes);
            })
            .await;
            metrics.record_duration_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA, started.elapsed());
            match result {
                Ok(_) => idle.put(addr, conn),
                Err(RdmaError::ConsumerGone) => idle.put(addr, conn),
                Err(error) => {
                    if matches!(error, RdmaError::Stalled(_)) {
                        metrics.record_stall_timeout_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA);
                    }
                    metrics.record_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA);
                    debug!(
                        event = EVENT_RDMA_STREAM_FAILED,
                        component = LOG_COMPONENT_INTERNODE_RPC,
                        subsystem = LOG_SUBSYSTEM_RDMA_TRANSPORT,
                        operation,
                        result = "failed",
                        error = %error,
                        "rdma read stream failed"
                    );
                    let _ = tx.send(Err(error.into_io_error())).await;
                    drop(conn);
                }
            }
        });
        Ok(Box::new(RdmaChunkReader::new(rx)))
    }

    async fn read_with_fallback(&self, request: ReadStreamRequest, fresh: bool) -> Result<Option<ChunkReaderBox>> {
        if self.peer_backing_off(&request.endpoint) {
            return self.tcp_read_chunks(request, fresh).await;
        }
        match self.rdma_read(&request, fresh).await {
            Ok(reader) => Ok(Some(reader)),
            Err(error) if self.fall_back(&request.endpoint, INTERNODE_OPERATION_READ_FILE_STREAM, &error) => {
                self.tcp_read_chunks(request, fresh).await
            }
            Err(error) => Err(error.into_disk_error()),
        }
    }

    async fn tcp_read_chunks(&self, request: ReadStreamRequest, fresh: bool) -> Result<Option<ChunkReaderBox>> {
        if fresh {
            self.tcp.open_read_chunks_fresh(request).await
        } else {
            self.tcp.open_read_chunks(request).await
        }
    }

    async fn rdma_write(&self, request: &WriteStreamRequest) -> std::result::Result<FileWriter, RdmaError> {
        let operation = INTERNODE_OPERATION_PUT_FILE_STREAM;
        // The authenticated put_file contract (nonce, server epoch, body digest trailer) is
        // required; a peer still on the legacy route keeps using TCP.
        let server_epoch = self
            .tcp
            .put_file_auth_capability(&request.endpoint)
            .await
            .map_err(|error| RdmaError::Unsupported(format!("put_file capability probe failed: {error}")))?
            .ok_or_else(|| RdmaError::Unsupported("peer does not offer authenticated put_file".to_owned()))?;
        let (addr, mut conn) = self.checkout(&request.endpoint, false).await?;
        let nonce = Uuid::new_v4();
        let op = StreamOp::Write(WriteRequest {
            disk: request.disk.clone(),
            volume: request.volume.clone(),
            path: request.path.clone(),
            append: request.append,
            size: request.size,
            nonce,
            server_epoch: Some(server_epoch),
        });
        let target = op.canonical_target();
        let auth = Auth::sign(&target, &op.method())?;
        let stream = conn.next_stream_id();
        let metrics = global_internode_metrics();
        metrics.record_outgoing_request_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA);
        let started = Instant::now();
        let setup_timeout = Some(self.settings.connect_timeout);
        let opened = async {
            conn.send_message(&Message::Request(Box::new(StreamRequest {
                version: RDMA_PROTOCOL_VERSION,
                stream,
                op,
                auth,
                window: None,
            })))
            .await?;
            loop {
                match next_event_with_stall(&mut conn, setup_timeout).await? {
                    Event::Message(Message::Window { stream: opened, window }) if opened == stream => return Ok(Ok(window)),
                    Event::Message(Message::Error {
                        stream: failed,
                        code,
                        message,
                    }) if failed == stream => return Ok(Err(RdmaError::Remote { code, message })),
                    _ => {}
                }
            }
        }
        .await;
        let window = match opened {
            Ok(Ok(window)) => window,
            Ok(Err(rejected)) => {
                metrics.record_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA);
                self.idle.put(addr, conn);
                return Err(rejected);
            }
            Err(error) => {
                conn.abort();
                metrics.record_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA);
                return Err(error);
            }
        };

        let (tx, rx) = mpsc::channel::<Bytes>(self.settings.stream_slots.max(2));
        let (done_tx, done_rx) = oneshot::channel::<io::Result<u64>>();
        let pool = Arc::clone(&self.pool);
        let idle = Arc::clone(&self.idle);
        let endpoint = request.endpoint.clone();
        let stall = Some(self.settings.stall_timeout);
        tokio::spawn(async move {
            let mut conn = conn;
            let source = StreamSource::new(ReceiverStream::new(rx).map(Ok::<Bytes, io::Error>));
            let outcome = async {
                push_stream(&mut conn, &pool, stream, &window, source, |bytes| {
                    metrics.record_sent_bytes_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA, bytes);
                })
                .await?;
                loop {
                    match next_event_with_stall(&mut conn, stall).await? {
                        Event::Message(Message::Done { stream: done, bytes }) if done == stream => return Ok(bytes),
                        Event::Message(Message::Error {
                            stream: failed,
                            code,
                            message,
                        }) if failed == stream => return Err(RdmaError::Remote { code, message }),
                        _ => {}
                    }
                }
            }
            .await;
            metrics.record_duration_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA, started.elapsed());
            match outcome {
                Ok(bytes) => {
                    idle.put(addr, conn);
                    let _ = done_tx.send(Ok(bytes));
                }
                Err(error) => {
                    if let RdmaError::Remote {
                        code: super::proto::ErrorCode::EpochConflict,
                        ..
                    } = &error
                    {
                        reject_put_file_server_epoch(&endpoint, server_epoch);
                    }
                    if matches!(error, RdmaError::Stalled(_)) {
                        metrics.record_stall_timeout_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA);
                    }
                    metrics.record_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_RDMA);
                    debug!(
                        event = EVENT_RDMA_STREAM_FAILED,
                        component = LOG_COMPONENT_INTERNODE_RPC,
                        subsystem = LOG_SUBSYSTEM_RDMA_TRANSPORT,
                        operation,
                        result = "failed",
                        error = %error,
                        "rdma write stream failed"
                    );
                    conn.abort();
                    drop(conn);
                    let _ = done_tx.send(Err(error.into_io_error()));
                }
            }
        });
        let writer = RdmaFileWriter::new(tx, done_rx);
        Ok(Box::new(PutFileAuthWriter::new(
            writer,
            target,
            nonce,
            request.endpoint.clone(),
            server_epoch,
        )))
    }
}

#[async_trait]
impl InternodeDataTransport for RdmaInternodeDataTransport {
    async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader> {
        self.open_read_chunks(request)
            .await?
            .map(|reader| Box::new(reader) as FileReader)
            .ok_or_else(|| DiskError::other("rdma transport returned no reader"))
    }

    async fn open_read_fresh(&self, request: ReadStreamRequest) -> Result<FileReader> {
        self.open_read_chunks_fresh(request)
            .await?
            .map(|reader| Box::new(reader) as FileReader)
            .ok_or_else(|| DiskError::other("rdma transport returned no reader"))
    }

    async fn open_read_chunks(&self, request: ReadStreamRequest) -> Result<Option<ChunkReaderBox>> {
        self.read_with_fallback(request, false).await
    }

    async fn open_read_chunks_fresh(&self, request: ReadStreamRequest) -> Result<Option<ChunkReaderBox>> {
        self.read_with_fallback(request, true).await
    }

    async fn warm_peer(&self, endpoint: &str) {
        if self.peer_backing_off(endpoint) {
            return;
        }
        if let Ok(addr) = self.resolve(endpoint).await {
            self.refill_idle(addr);
        }
    }

    async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter> {
        if self.peer_backing_off(&request.endpoint) {
            return self.tcp.open_write(request).await;
        }
        match self.rdma_write(&request).await {
            Ok(writer) => Ok(writer),
            Err(error) if self.fall_back(&request.endpoint, INTERNODE_OPERATION_PUT_FILE_STREAM, &error) => {
                self.tcp.open_write(request).await
            }
            Err(error) => Err(error.into_disk_error()),
        }
    }

    async fn open_walk_dir(&self, request: WalkDirStreamRequest) -> Result<FileReader> {
        self.tcp.open_walk_dir(request).await
    }

    async fn open_ns_scanner(&self, request: NsScannerStreamRequest) -> Result<FileReader> {
        self.tcp.open_ns_scanner(request).await
    }

    async fn probe_ns_scanner(&self, request: NsScannerCapabilityRequest) -> Result<Uuid> {
        self.tcp.probe_ns_scanner(request).await
    }

    async fn probe_ns_scanner_capability(&self, request: NsScannerCapabilityRequest) -> Result<NsScannerCapabilityResponse> {
        self.tcp.probe_ns_scanner_capability(request).await
    }

    fn name(&self) -> &'static str {
        INTERNODE_DATA_TRANSPORT_RDMA
    }

    fn capabilities(&self) -> InternodeDataTransportCapabilities {
        InternodeDataTransportCapabilities {
            streaming_read: true,
            streaming_write: true,
            streaming_walk_dir: false,
            ordered_delivery: true,
            max_transfer_size: None,
            fallback_supported: true,
        }
    }
}

/// Owned-chunk reader fed by the receive task.
pub struct RdmaChunkReader {
    rx: mpsc::Receiver<io::Result<Bytes>>,
    current: Option<Bytes>,
    done: bool,
}

impl RdmaChunkReader {
    fn new(rx: mpsc::Receiver<io::Result<Bytes>>) -> Self {
        Self {
            rx,
            current: None,
            done: false,
        }
    }
}

impl ChunkReader for RdmaChunkReader {
    fn poll_read_chunk(mut self: Pin<&mut Self>, cx: &mut Context<'_>, max: usize) -> Poll<io::Result<Option<Bytes>>> {
        if max == 0 {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidInput, "chunk read limit must be non-zero")));
        }
        loop {
            if let Some(mut current) = self.current.take() {
                let take = current.len().min(max);
                let chunk = current.split_to(take);
                if !current.is_empty() {
                    self.current = Some(current);
                }
                return Poll::Ready(Ok(Some(chunk)));
            }
            if self.done {
                return Poll::Ready(Ok(None));
            }
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    if !bytes.is_empty() {
                        self.current = Some(bytes);
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    self.done = true;
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(None) => {
                    self.done = true;
                    return Poll::Ready(Ok(None));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl AsyncRead for RdmaChunkReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        match self.poll_read_chunk(cx, buf.remaining()) {
            Poll::Ready(Ok(Some(chunk))) => {
                buf.put_slice(&chunk);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Ok(None)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// `AsyncWrite` facade over the push task of one write stream.
///
/// Bytes are copied into owned chunks and queued; `poll_shutdown` closes the
/// queue and reports the server's commit result.
struct RdmaFileWriter {
    tx: PollSender<Bytes>,
    done: oneshot::Receiver<io::Result<u64>>,
    outcome: Option<std::result::Result<u64, (io::ErrorKind, String)>>,
}

impl RdmaFileWriter {
    fn new(tx: mpsc::Sender<Bytes>, done: oneshot::Receiver<io::Result<u64>>) -> Self {
        Self {
            tx: PollSender::new(tx),
            done,
            outcome: None,
        }
    }

    fn poll_outcome(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        if let Some(outcome) = &self.outcome {
            return Poll::Ready(outcome.clone().map_err(|(kind, message)| io::Error::new(kind, message)));
        }
        match Pin::new(&mut self.done).poll(cx) {
            Poll::Ready(Ok(Ok(bytes))) => {
                self.outcome = Some(Ok(bytes));
                Poll::Ready(Ok(bytes))
            }
            Poll::Ready(Ok(Err(error))) => {
                self.outcome = Some(Err((error.kind(), error.to_string())));
                Poll::Ready(Err(error))
            }
            Poll::Ready(Err(_)) => {
                let message = "rdma write task ended before reporting a result";
                self.outcome = Some(Err((io::ErrorKind::BrokenPipe, message.to_owned())));
                Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, message)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn closed(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        match self.poll_outcome(cx) {
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(_)) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "rdma write stream completed before all bytes were queued",
            ))),
            Poll::Pending => Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "rdma write stream closed"))),
        }
    }
}

impl AsyncWrite for RdmaFileWriter {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match self.tx.poll_reserve(cx) {
            Poll::Ready(Ok(())) => match self.tx.send_item(Bytes::copy_from_slice(buf)) {
                Ok(()) => Poll::Ready(Ok(buf.len())),
                Err(_) => self.closed(cx),
            },
            Poll::Ready(Err(_)) => self.closed(cx),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.tx.close();
        match self.poll_outcome(cx) {
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr() -> SocketAddr {
        "10.0.0.1:10000".parse().expect("socket address")
    }

    #[test]
    fn refill_claims_are_bounded_by_the_minimum_and_the_dial_concurrency() {
        let pool = IdlePool::new(64, 20);
        // The first claim starts the concurrency cap, not the whole deficit.
        assert_eq!(pool.claim_refills(addr()), REFILL_DIAL_CONCURRENCY);
        // In-flight dials count toward the minimum, so a second claim adds nothing.
        assert_eq!(pool.claim_refills(addr()), 0);
        // A failed dial frees one reservation and the next claim restarts exactly one.
        pool.release_refill(addr());
        assert_eq!(pool.claim_refills(addr()), 1);
    }

    #[test]
    fn refill_dialer_stops_once_the_minimum_is_covered() {
        let pool = IdlePool::new(64, 2);
        assert_eq!(pool.claim_refills(addr()), 2);
        // Two dialers in flight and nothing idle: each keeps dialing while the
        // other alone cannot cover the minimum.
        assert!(pool.continue_refill(addr()));
        // With the pool still empty and one reservation left after this call,
        // the deficit is one, so the dialer continues again.
        assert!(pool.continue_refill(addr()));
        let pool = IdlePool::new(64, 0);
        assert_eq!(pool.claim_refills(addr()), 0);
        assert!(!pool.continue_refill(addr()));
        assert!(pool.refilling.lock().is_empty());
    }

    #[test]
    fn minimum_never_exceeds_the_pool_size() {
        let pool = IdlePool::new(4, 16);
        assert_eq!(pool.min_idle_per_peer, 4);
        assert_eq!(pool.claim_refills(addr()), 4);
    }
}
