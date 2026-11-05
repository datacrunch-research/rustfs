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

//! Internode shard reads whose bytes move over NIXL/UCX.
//!
//! The reading node keeps a pool of receive buffers registered with its NIXL
//! agent. For every chunk of a remote shard stream it sends a signed request
//! over the existing internode HTTP control plane naming the shard range, its
//! agent, one registered buffer and a digest of its agent metadata; the serving
//! node reads the range into its own registered staging memory and writes it
//! into that buffer with a NIXL WRITE, then answers with the byte count. The
//! buffer reaches the decoder as a zero-copy `Bytes`. Several chunk requests
//! run per stream so request latency overlaps the transfers.
//!
//! UCX addresses ports by LID or GID, so this works on InfiniBand rails that
//! have no IP addresses. Writes, walks and any stream whose first chunk cannot
//! be served this way use the TCP/HTTP transport.
//!
//! Buffer safety: the serving node may still write into a buffer whose request
//! was abandoned (a dropped reader, chunks past the end of the file). Such
//! buffers are held for [`ABANDONED_BUFFER_HOLD`] before they return to the
//! pool, which covers the serving node's transfer timeout.

use super::internode_data_transport::{
    InternodeDataTransport, InternodeDataTransportCapabilities, NsScannerCapabilityRequest, NsScannerStreamRequest,
    ReadStreamRequest, TcpHttpInternodeDataTransport, WalkDirStreamRequest, WriteStreamRequest, build_read_file_stream_url,
    json_headers,
};
use crate::cluster::rpc::build_auth_headers;
use crate::disk::error::Result;
use crate::disk::{FileReader, FileWriter};
use crate::storage_api_contracts::internode::NsScannerCapabilityResponse;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{FuturesOrdered, Stream};
use http::{HeaderValue, Method};
use rustfs_config::{DEFAULT_RUSTFS_NIXL_INTERNODE_DEPTH, ENV_RUSTFS_NIXL_INTERNODE_DEPTH};
use rustfs_rio::{ChunkReader, ChunkReaderBox};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, ReadBuf};
use tracing::debug;
use uuid::Uuid;

/// Internode route of a NIXL chunk read.
pub const READ_FILE_NIXL_PATH: &str = "/rustfs/rpc/read_file_nixl";
/// The route the URL builder emits, replaced by [`READ_FILE_NIXL_PATH`].
const READ_FILE_STREAM_PATH: &str = "/rustfs/rpc/read_file_stream";
/// Query parameters of a NIXL chunk read (signed with the URL).
pub const NIXL_AGENT_QUERY: &str = "nixl_agent";
pub const NIXL_BUFFER_QUERY: &str = "nixl_buffer";
pub const NIXL_METADATA_SHA256_QUERY: &str = "nixl_md";
/// The reader's agent metadata, base64; must hash to the signed digest.
pub const NIXL_METADATA_HEADER: &str = "x-rustfs-nixl-metadata";
/// Bytes the serving node wrote into the buffer; fewer than asked means end of file.
pub const NIXL_BYTES_HEADER: &str = "x-rustfs-nixl-bytes";

/// How long a buffer whose request was abandoned stays out of the pool.
pub const ABANDONED_BUFFER_HOLD: Duration = Duration::from_secs(120);
/// Request timeout when the caller sets no stall timeout.
const DEFAULT_CHUNK_TIMEOUT: Duration = Duration::from_secs(60);
const EVENT_NIXL_INTERNODE_FALLBACK: &str = "nixl_internode_fallback";
const LOG_COMPONENT_INTERNODE: &str = "internode";
const LOG_SUBSYSTEM_NIXL: &str = "nixl";

/// Receive buffers registered with this node's NIXL agent.
pub trait NixlRecvPool: Send + Sync + 'static {
    /// This node's agent name.
    fn agent_name(&self) -> &str;
    /// This node's agent metadata, base64.
    fn metadata_base64(&self) -> &str;
    /// Lowercase hex SHA-256 of the raw agent metadata.
    fn metadata_sha256_hex(&self) -> &str;
    /// Size of one receive buffer; one chunk request moves at most this much.
    fn chunk_bytes(&self) -> usize;
    /// One free buffer, waiting for one when the pool is busy.
    fn reserve(&self) -> BoxFuture<'static, io::Result<Box<dyn NixlRecvBuffer>>>;
}

/// One registered receive buffer, back in the pool when dropped.
pub trait NixlRecvBuffer: Send + Sync + 'static {
    /// `<dram|vram>:<device>:<address>:<length>` as the serving node parses it.
    fn descriptor(&self) -> String;
    /// The first `len` bytes as an owned, zero-copy `Bytes`.
    fn into_bytes(self: Box<Self>, len: usize) -> io::Result<Bytes>;
}

static RECV_POOL: OnceLock<Arc<dyn NixlRecvPool>> = OnceLock::new();

/// Installs this node's receive pool; only the first installation wins.
pub fn install_nixl_recv_pool(pool: Arc<dyn NixlRecvPool>) -> bool {
    RECV_POOL.set(pool).is_ok()
}

fn recv_pool() -> Option<Arc<dyn NixlRecvPool>> {
    RECV_POOL.get().cloned()
}

/// Internode transport that reads shard streams over NIXL and delegates
/// everything else to TCP/HTTP.
#[derive(Debug)]
pub struct NixlInternodeDataTransport {
    tcp: TcpHttpInternodeDataTransport,
    depth: usize,
}

impl NixlInternodeDataTransport {
    pub fn from_env() -> Self {
        Self {
            tcp: TcpHttpInternodeDataTransport,
            depth: rustfs_utils::get_env_usize(ENV_RUSTFS_NIXL_INTERNODE_DEPTH, DEFAULT_RUSTFS_NIXL_INTERNODE_DEPTH).clamp(1, 64),
        }
    }

    /// A NIXL chunk reader for `request` with its first chunk already landed,
    /// or `None` when this read should use TCP/HTTP.
    async fn open_nixl(&self, request: &ReadStreamRequest) -> Option<NixlChunkReader> {
        let pool = recv_pool()?;
        if request.length == 0 {
            // Unbounded reads need the file size; TCP/HTTP streams them.
            return None;
        }
        let mut reader = NixlChunkReader::new(pool, request.clone(), self.depth);
        match futures::future::poll_fn(|cx| reader.state_mut().poll_chunk(cx, usize::MAX)).await {
            Ok(Some(first)) => {
                reader.state_mut().current = first;
                Some(reader)
            }
            Ok(None) => Some(reader),
            Err(error) => {
                debug!(
                    event = EVENT_NIXL_INTERNODE_FALLBACK,
                    component = LOG_COMPONENT_INTERNODE,
                    subsystem = LOG_SUBSYSTEM_NIXL,
                    result = "fallback",
                    endpoint = %request.endpoint,
                    error = %error,
                    "nixl internode read fell back to tcp"
                );
                None
            }
        }
    }
}

#[async_trait]
impl InternodeDataTransport for NixlInternodeDataTransport {
    async fn open_read(&self, request: ReadStreamRequest) -> Result<FileReader> {
        match self.open_nixl(&request).await {
            Some(reader) => Ok(Box::new(reader)),
            None => self.tcp.open_read(request).await,
        }
    }

    async fn open_read_fresh(&self, request: ReadStreamRequest) -> Result<FileReader> {
        match self.open_nixl(&request).await {
            Some(reader) => Ok(Box::new(reader)),
            None => self.tcp.open_read_fresh(request).await,
        }
    }

    async fn open_read_chunks(&self, request: ReadStreamRequest) -> Result<Option<ChunkReaderBox>> {
        match self.open_nixl(&request).await {
            Some(reader) => Ok(Some(Box::new(reader))),
            None => self.tcp.open_read_chunks(request).await,
        }
    }

    async fn open_read_chunks_fresh(&self, request: ReadStreamRequest) -> Result<Option<ChunkReaderBox>> {
        match self.open_nixl(&request).await {
            Some(reader) => Ok(Some(Box::new(reader))),
            None => self.tcp.open_read_chunks_fresh(request).await,
        }
    }

    async fn open_write(&self, request: WriteStreamRequest) -> Result<FileWriter> {
        self.tcp.open_write(request).await
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
        "nixl"
    }

    fn capabilities(&self) -> InternodeDataTransportCapabilities {
        InternodeDataTransportCapabilities::tcp_http()
    }
}

/// A receive buffer lent to one chunk request. Unless the request completed,
/// dropping it holds the buffer out of the pool for [`ABANDONED_BUFFER_HOLD`]
/// because the serving node may still be writing into it.
struct LentBuffer {
    buffer: Option<Box<dyn NixlRecvBuffer>>,
}

impl LentBuffer {
    fn complete(mut self, len: usize) -> io::Result<Bytes> {
        let buffer = self.buffer.take().expect("buffer present until completion");
        buffer.into_bytes(len)
    }
}

impl Drop for LentBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn(async move {
                        tokio::time::sleep(ABANDONED_BUFFER_HOLD).await;
                        drop(buffer);
                    });
                }
                // No runtime to wait on: never hand the memory back.
                Err(_) => std::mem::forget(buffer),
            }
        }
    }
}

/// Fetches one chunk: the requested length and the bytes that landed.
async fn fetch_chunk(pool: Arc<dyn NixlRecvPool>, request: ReadStreamRequest, timeout: Duration) -> io::Result<(usize, Bytes)> {
    let buffer = LentBuffer {
        buffer: Some(pool.reserve().await?),
    };
    let descriptor = buffer.buffer.as_ref().map(|buffer| buffer.descriptor()).unwrap_or_default();
    let mut url = build_read_file_stream_url(&request).replacen(READ_FILE_STREAM_PATH, READ_FILE_NIXL_PATH, 1);
    url.push_str(&format!(
        "&{NIXL_AGENT_QUERY}={}&{NIXL_BUFFER_QUERY}={}&{NIXL_METADATA_SHA256_QUERY}={}",
        urlencoding::encode(pool.agent_name()),
        urlencoding::encode(&descriptor),
        pool.metadata_sha256_hex()
    ));
    let mut headers = json_headers();
    headers.insert(
        NIXL_METADATA_HEADER,
        HeaderValue::from_str(pool.metadata_base64())
            .map_err(|_| io::Error::other("nixl agent metadata is not a header value"))?,
    );
    build_auth_headers(&url, &Method::GET, &mut headers)?;
    let (status, response_headers, _) = rustfs_rio::internode_http_request(&url, Method::GET, headers, Some(timeout)).await?;
    if !status.is_success() {
        return Err(io::Error::other(format!("nixl internode read returned {status}")));
    }
    let landed = response_headers
        .get(NIXL_BYTES_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| io::Error::other("nixl internode read reply lacks the byte count"))?;
    if landed > request.length {
        return Err(io::Error::other("nixl internode read reports more bytes than requested"));
    }
    Ok((request.length, buffer.complete(landed)?))
}

type ChunkFuture = BoxFuture<'static, io::Result<(usize, Bytes)>>;

struct ReaderState {
    pool: Arc<dyn NixlRecvPool>,
    base: ReadStreamRequest,
    next: usize,
    end: usize,
    chunk: usize,
    depth: usize,
    timeout: Duration,
    inflight: FuturesOrdered<ChunkFuture>,
    current: Bytes,
    done: bool,
}

impl ReaderState {
    fn refill(&mut self) {
        while !self.done && self.inflight.len() < self.depth && self.next < self.end {
            let len = self.chunk.min(self.end - self.next);
            let mut request = self.base.clone();
            request.offset = self.next;
            request.length = len;
            self.next += len;
            self.inflight
                .push_back(Box::pin(fetch_chunk(Arc::clone(&self.pool), request, self.timeout)));
        }
    }

    fn poll_chunk(&mut self, cx: &mut Context<'_>, max: usize) -> Poll<io::Result<Option<Bytes>>> {
        loop {
            if !self.current.is_empty() {
                let take = self.current.len().min(max.max(1));
                return Poll::Ready(Ok(Some(self.current.split_to(take))));
            }
            if self.done {
                return Poll::Ready(Ok(None));
            }
            self.refill();
            match Pin::new(&mut self.inflight).poll_next(cx) {
                Poll::Ready(Some(Ok((asked, bytes)))) => {
                    if bytes.len() < asked {
                        // End of file: later chunks hold nothing; their buffers
                        // are held back as they are dropped.
                        self.done = true;
                        self.inflight = FuturesOrdered::new();
                    }
                    if bytes.is_empty() {
                        self.done = true;
                        return Poll::Ready(Ok(None));
                    }
                    self.current = bytes;
                }
                Poll::Ready(Some(Err(error))) => {
                    self.done = true;
                    self.inflight = FuturesOrdered::new();
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

/// A remote shard stream read over NIXL in pipelined chunks.
pub struct NixlChunkReader {
    // The mutex only makes the reader `Sync`; every access goes through `&mut self`.
    state: std::sync::Mutex<ReaderState>,
}

impl NixlChunkReader {
    fn new(pool: Arc<dyn NixlRecvPool>, request: ReadStreamRequest, depth: usize) -> Self {
        let chunk = pool.chunk_bytes().max(1);
        let timeout = request.stall_timeout.unwrap_or(DEFAULT_CHUNK_TIMEOUT);
        Self {
            state: std::sync::Mutex::new(ReaderState {
                next: request.offset,
                end: request.offset.saturating_add(request.length),
                pool,
                base: request,
                chunk,
                depth,
                timeout,
                inflight: FuturesOrdered::new(),
                current: Bytes::new(),
                done: false,
            }),
        }
    }

    fn state_mut(&mut self) -> &mut ReaderState {
        self.state.get_mut().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl ChunkReader for NixlChunkReader {
    fn poll_read_chunk(self: Pin<&mut Self>, cx: &mut Context<'_>, max: usize) -> Poll<io::Result<Option<Bytes>>> {
        self.get_mut().state_mut().poll_chunk(cx, max)
    }
}

impl AsyncRead for NixlChunkReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        match self.get_mut().state_mut().poll_chunk(cx, buf.remaining()) {
            Poll::Ready(Ok(Some(bytes))) => {
                buf.put_slice(&bytes);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Ok(None)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingBuffer {
        released: Arc<AtomicUsize>,
    }

    impl NixlRecvBuffer for CountingBuffer {
        fn descriptor(&self) -> String {
            "dram:0:0x1000:16".to_owned()
        }

        fn into_bytes(self: Box<Self>, len: usize) -> io::Result<Bytes> {
            Ok(Bytes::from(vec![7u8; len]))
        }
    }

    impl Drop for CountingBuffer {
        fn drop(&mut self) {
            self.released.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn abandoned_buffers_return_only_after_the_hold() {
        let released = Arc::new(AtomicUsize::new(0));
        let lent = LentBuffer {
            buffer: Some(Box::new(CountingBuffer {
                released: Arc::clone(&released),
            })),
        };
        drop(lent);
        tokio::task::yield_now().await;
        assert_eq!(released.load(Ordering::SeqCst), 0, "an abandoned buffer must not return at once");
        tokio::time::sleep(ABANDONED_BUFFER_HOLD + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn completed_buffers_return_with_their_bytes() {
        let released = Arc::new(AtomicUsize::new(0));
        let lent = LentBuffer {
            buffer: Some(Box::new(CountingBuffer {
                released: Arc::clone(&released),
            })),
        };
        let bytes = lent.complete(5).expect("bytes");
        assert_eq!(bytes.len(), 5);
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }
}
