//! Server side of RustFS S3-over-RDMA on NIXL with the UCX backend.
//!
//! A GET carrying the NIXL headers is served by the ordinary read path, and
//! the bytes are moved into the client's registered buffer with NIXL WRITE
//! transfers instead of an HTTP body; a PUT carrying them is read out of the
//! client's buffer with READ transfers. UCX addresses every device by its
//! GID, LID and queue-pair numbers exchanged in the agent metadata, so the
//! GPU-side InfiniBand rails work without IPoIB or a subnet administrator
//! and the CPU-side RoCE ports work without rdma_cm.
//!
//! The client sends three headers with the request:
//!
//! * [`NIXL_AGENT_HEADER`]: its agent name;
//! * [`NIXL_METADATA_HEADER`]: its agent metadata, base64 (may be omitted on
//!   later requests once this server has loaded that agent);
//! * [`NIXL_BUFFER_HEADER`]: `<dram|vram>:<device>:<address>:<length>`, the
//!   registered buffer the object (or range) lands in from offset 0.
//!
//! The reply carries [`RDMA_REPLY_HEADER`] and
//! [`RDMA_BYTES_TRANSFERRED_HEADER`] exactly like the cuObject transport, so a
//! client treats both the same.

use bytes::Bytes;
use nixl_sys::{Agent, AgentConfig, Backend, MemType, OptArgs, Params, ThreadSync, XferDescList, XferOp, XferStatus};

mod mem;

use mem::HostBuffer;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Semaphore;

/// The client's NIXL agent name.
pub const NIXL_AGENT_HEADER: &str = "x-rustfs-nixl-agent";
/// The client's NIXL agent metadata, base64.
pub const NIXL_METADATA_HEADER: &str = "x-rustfs-nixl-metadata";
/// The client's registered buffer: `<dram|vram>:<device>:<address>:<length>`.
pub const NIXL_BUFFER_HEADER: &str = "x-rustfs-nixl-buffer";
/// Reply status of the transfer; the same header the cuObject transport uses.
pub const RDMA_REPLY_HEADER: &str = "x-amz-rdma-reply";
/// Bytes moved into (or out of) the client's buffer.
pub const RDMA_BYTES_TRANSFERRED_HEADER: &str = "x-amz-rdma-bytes-transferred";
/// Reply status when the server cannot serve the transfer.
pub const RDMA_REPLY_NOT_IMPLEMENTED: u16 = 501;
/// Largest single staging chunk; one transfer moves at most this much per descriptor.
pub const MAX_RDMA_OP_BYTES: usize = 1024 * 1024 * 1024;
/// Ranges a scatter-gather write may carry; NIXL takes one descriptor per range.
pub const MAX_SG_RANGES: usize = 64;
/// The NIXL release this crate is pinned to (`nixl-sys` tag).
pub const NIXL_LIBRARY_VERSION: &str = "1.5.2";

#[derive(Debug, Error)]
pub enum NixlError {
    #[error("nixl {op} failed: {source}")]
    Nixl {
        op: &'static str,
        #[source]
        source: nixl_sys::NixlError,
    },
    #[error("nixl {op} failed: {source}")]
    Os {
        op: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("nixl rdma token is malformed: {0}")]
    Token(String),
    #[error("nixl rdma token buffer ({buffer} bytes) is smaller than the response ({needed} bytes)")]
    TokenTooSmall { buffer: u64, needed: u64 },
    #[error("nixl transfer did not complete within {0:?}")]
    TransferTimeout(Duration),
    #[error("nixl transfer moved {moved} of {expected} bytes")]
    ShortTransfer { moved: u64, expected: u64 },
    #[error("nixl staging pool exhausted")]
    PoolExhausted,
    #[error("nixl read source failed: {0}")]
    Source(#[source] std::io::Error),
}

fn nixl_err(op: &'static str) -> impl FnOnce(nixl_sys::NixlError) -> NixlError {
    move |source| NixlError::Nixl { op, source }
}

/// Where the client's buffer lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientMemory {
    Dram,
    Vram,
}

impl ClientMemory {
    fn mem_type(self) -> MemType {
        match self {
            Self::Dram => MemType::Dram,
            Self::Vram => MemType::Vram,
        }
    }
}

/// The client's transfer target, parsed from the request headers.
#[derive(Debug, Clone)]
pub struct RdmaToken {
    raw: String,
    /// The client's agent name.
    pub agent: String,
    /// The client's agent metadata when the request carried it.
    pub metadata: Option<Vec<u8>>,
    pub memory: ClientMemory,
    pub device: u64,
    pub remote_addr: u64,
    pub buffer_len: u64,
}

fn parse_number(field: &str, what: &str) -> Result<u64, NixlError> {
    let field = field.trim();
    let parsed = match field.strip_prefix("0x").or_else(|| field.strip_prefix("0X")) {
        Some(hex) => u64::from_str_radix(hex, 16),
        None => field.parse::<u64>(),
    };
    parsed.map_err(|_| NixlError::Token(format!("{what} is not a number")))
}

impl RdmaToken {
    /// Builds a token from the three request headers.
    pub fn from_headers(agent: &str, metadata_base64: Option<&str>, buffer: &str) -> Result<Self, NixlError> {
        let agent = agent.trim();
        if agent.is_empty() || agent.len() > 128 || !agent.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(NixlError::Token("agent name is empty, too long or not printable ASCII".to_owned()));
        }
        let metadata = match metadata_base64.map(str::trim).filter(|value| !value.is_empty()) {
            Some(encoded) => Some(
                base64_simd::STANDARD
                    .decode_to_vec(encoded)
                    .map_err(|_| NixlError::Token("agent metadata is not base64".to_owned()))?,
            ),
            None => None,
        };
        let mut fields = buffer.trim().split(':');
        let memory = match fields.next().map(|value| value.trim().to_ascii_lowercase()).as_deref() {
            Some("dram") | Some("host") => ClientMemory::Dram,
            Some("vram") | Some("gpu") => ClientMemory::Vram,
            _ => return Err(NixlError::Token("buffer memory type must be dram or vram".to_owned())),
        };
        let device = parse_number(
            fields
                .next()
                .ok_or_else(|| NixlError::Token("missing buffer device".to_owned()))?,
            "buffer device",
        )?;
        let remote_addr = parse_number(
            fields
                .next()
                .ok_or_else(|| NixlError::Token("missing buffer address".to_owned()))?,
            "buffer address",
        )?;
        let buffer_len = parse_number(
            fields
                .next()
                .ok_or_else(|| NixlError::Token("missing buffer length".to_owned()))?,
            "buffer length",
        )?;
        if buffer_len == 0 {
            return Err(NixlError::Token("zero-length buffer".to_owned()));
        }
        if remote_addr.checked_add(buffer_len).is_none() {
            return Err(NixlError::Token("buffer address overflow".to_owned()));
        }
        Ok(Self {
            raw: format!("{agent}:{}", buffer.trim()),
            agent: agent.to_owned(),
            metadata,
            memory,
            device,
            remote_addr,
            buffer_len,
        })
    }

    /// Agent name and buffer, for logs.
    pub fn as_str(&self) -> &str {
        &self.raw
    }
}

#[derive(Debug, Clone)]
pub struct NixlConfig {
    /// This server's agent name; must be unique in the cluster.
    pub agent_name: String,
    /// UCX device names (`mlx5_9`, ...; NIXL appends the port); empty lets UCX use every device it finds.
    pub ucx_devices: Vec<String>,
    /// UCX progress workers.
    pub num_workers: u32,
    /// Size of one staging buffer; one transfer descriptor moves at most this much.
    pub chunk_bytes: usize,
    /// Total registered staging memory.
    pub pool_bytes: usize,
    /// Transfers in flight per server at most.
    pub max_inflight: usize,
    /// Pause between completion polls once spinning stops.
    pub poll_interval: Duration,
    /// A transfer that has not completed after this long fails the request.
    pub transfer_timeout: Duration,
}

impl Default for NixlConfig {
    fn default() -> Self {
        Self {
            agent_name: "rustfs".to_owned(),
            ucx_devices: Vec::new(),
            num_workers: 1,
            chunk_bytes: 64 * 1024 * 1024,
            pool_bytes: 1024 * 1024 * 1024,
            max_inflight: 256,
            poll_interval: Duration::from_micros(50),
            transfer_timeout: Duration::from_secs(60),
        }
    }
}

/// Registered memory the erasure decode path can emit from in place
/// (the shard arena); scatter-gather writes name ranges of it.
pub struct RegisteredArena {
    buffer: HostBuffer,
    _registration: nixl_sys::RegistrationHandle,
    server: Arc<NixlServer>,
    len: usize,
    id: u64,
}

impl RegisteredArena {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn server(&self) -> &Arc<NixlServer> {
        &self.server
    }

    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.buffer.as_mut_ptr()
    }
}

impl std::fmt::Debug for RegisteredArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredArena")
            .field("id", &self.id)
            .field("len", &self.len)
            .finish()
    }
}

/// Ranges of one arena, written to consecutive client offsets in one transfer.
pub struct ArenaRanges {
    pub arena: Arc<RegisteredArena>,
    pub ranges: Vec<(usize, usize)>,
}

impl ArenaRanges {
    pub fn total_len(&self) -> usize {
        self.ranges.iter().map(|(_, len)| *len).sum()
    }
}

pub struct ArenaRange {
    pub arena: Arc<RegisteredArena>,
    pub local_offset: usize,
    pub len: usize,
}

struct Staging {
    buffer: HostBuffer,
    _registration: nixl_sys::RegistrationHandle,
    free: parking_lot::Mutex<VecDeque<usize>>,
    permits: Semaphore,
    chunk_bytes: usize,
}

/// One registered staging chunk, exclusively owned until dropped.
pub struct StagingChunk {
    server: Arc<NixlServer>,
    index: usize,
    len: usize,
}

impl StagingChunk {
    pub fn capacity(&self) -> usize {
        self.server.staging.chunk_bytes
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn append(&mut self, data: &[u8]) -> Result<(), NixlError> {
        if self.len + data.len() > self.server.staging.chunk_bytes {
            return Err(NixlError::Token("staging append exceeds chunk".to_owned()));
        }
        let offset = self.index * self.server.staging.chunk_bytes + self.len;
        self.server.staging.buffer.write(offset, data)?;
        self.len += data.len();
        Ok(())
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    pub fn is_full(&self) -> bool {
        self.len >= self.server.staging.chunk_bytes
    }

    pub fn staged(&self) -> Result<&[u8], NixlError> {
        let offset = self.index * self.server.staging.chunk_bytes;
        self.server.staging.buffer.slice(offset, self.len)
    }

    pub fn spare_mut(&mut self) -> Result<&mut [u8], NixlError> {
        let offset = self.index * self.server.staging.chunk_bytes + self.len;
        let len = self.server.staging.chunk_bytes - self.len;
        self.server.staging.buffer.slice_mut(offset, len)
    }

    pub fn advance(&mut self, n: usize) -> Result<(), NixlError> {
        if self.len + n > self.server.staging.chunk_bytes {
            return Err(NixlError::Token("staging advance exceeds chunk".to_owned()));
        }
        self.len += n;
        Ok(())
    }

    fn local_addr(&self) -> usize {
        self.server.staging.buffer.addr() + self.index * self.server.staging.chunk_bytes
    }
}

impl Drop for StagingChunk {
    fn drop(&mut self) {
        self.server.staging.free.lock().push_back(self.index);
        self.server.staging.permits.add_permits(1);
    }
}

/// Copies `data` into the chunk.
pub fn fill_chunk(chunk: &mut StagingChunk, data: &Bytes) -> Result<(), NixlError> {
    chunk.append(data)
}

/// The NIXL release the server is built against, as (major, minor).
pub fn library_version() -> (u32, u32) {
    let mut parts = NIXL_LIBRARY_VERSION.split('.').map(|part| part.parse::<u32>().unwrap_or(0));
    (parts.next().unwrap_or(0), parts.next().unwrap_or(0))
}

/// One NIXL agent with the UCX backend, a registered staging pool and the
/// remote agents it has loaded.
pub struct NixlServer {
    agent: Agent,
    backend: Backend,
    staging: Staging,
    /// Remote agents loaded so far, by name, with a hash of the metadata that
    /// was loaded (a client that restarts sends new metadata).
    remotes: parking_lot::Mutex<HashMap<String, u64>>,
    /// Serializes loading a remote agent, so the concurrent segments of a new
    /// client's first request load its metadata once.
    load_lock: tokio::sync::Mutex<()>,
    inflight: Arc<Semaphore>,
    poll_interval: Duration,
    transfer_timeout: Duration,
    name: String,
    devices: Vec<String>,
}

impl std::fmt::Debug for NixlServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NixlServer")
            .field("name", &self.name)
            .field("devices", &self.devices)
            .field("chunk_bytes", &self.staging.chunk_bytes)
            .finish()
    }
}

fn metadata_hash(metadata: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    metadata.hash(&mut hasher);
    hasher.finish()
}

impl NixlServer {
    /// Creates the agent, the UCX backend and the staging pool.
    pub fn new(config: &NixlConfig) -> Result<Arc<Self>, NixlError> {
        if config.chunk_bytes == 0 || config.chunk_bytes > MAX_RDMA_OP_BYTES || config.pool_bytes < config.chunk_bytes {
            return Err(NixlError::Token("invalid staging configuration".to_owned()));
        }
        // Request tasks post and poll transfers from every runtime thread. The
        // binding serializes its calls with a lock of its own, but the agent is
        // configured for concurrent use as well (reader-writer locking, UCX
        // workers in multi-thread mode) so correctness does not rest on that
        // detail; a UCX without thread support gets the strict mode instead.
        let agent = match Self::create_agent(config, ThreadSync::Rw) {
            Ok(agent) => agent,
            Err(err) => {
                tracing::warn!(error = %err, "NIXL agent with rw synchronization failed; using strict");
                Self::create_agent(config, ThreadSync::Strict)?
            }
        };
        let mut params = Params::from(std::iter::empty::<(&str, &str)>()).map_err(nixl_err("params"))?;
        if !config.ucx_devices.is_empty() {
            params
                .set("device_list", &config.ucx_devices.join(","))
                .map_err(nixl_err("device_list"))?;
        }
        let backend = agent.create_backend("UCX", &params).map_err(nixl_err("ucx backend"))?;
        let chunks = config.pool_bytes / config.chunk_bytes;
        let buffer = HostBuffer::new(chunks * config.chunk_bytes)?;
        let mut opt_args = OptArgs::new().map_err(nixl_err("opt args"))?;
        opt_args.add_backend(&backend).map_err(nixl_err("opt args backend"))?;
        let registration = agent
            .register_memory(&buffer, Some(&opt_args))
            .map_err(nixl_err("register staging"))?;
        Ok(Arc::new(Self {
            agent,
            backend,
            staging: Staging {
                buffer,
                _registration: registration,
                free: parking_lot::Mutex::new((0..chunks).collect()),
                permits: Semaphore::new(chunks),
                chunk_bytes: config.chunk_bytes,
            },
            remotes: parking_lot::Mutex::new(HashMap::new()),
            load_lock: tokio::sync::Mutex::new(()),
            inflight: Arc::new(Semaphore::new(config.max_inflight.max(1))),
            poll_interval: config.poll_interval,
            transfer_timeout: config.transfer_timeout,
            name: config.agent_name.clone(),
            devices: config.ucx_devices.clone(),
        }))
    }

    fn create_agent(config: &NixlConfig, thread_sync: ThreadSync) -> Result<Agent, NixlError> {
        let agent_config = AgentConfig {
            num_workers: config.num_workers.max(1),
            thread_sync,
            ..AgentConfig::default()
        };
        Agent::new_configured(&config.agent_name, &agent_config).map_err(nixl_err("agent"))
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// This agent's metadata, for a client that wants to address the server.
    pub fn local_metadata(&self) -> Result<Vec<u8>, NixlError> {
        self.agent.get_local_md().map_err(nixl_err("local metadata"))
    }

    pub fn chunk_bytes(&self) -> usize {
        self.staging.chunk_bytes
    }

    pub async fn reserve(self: &Arc<Self>, timeout: Duration) -> Result<StagingChunk, NixlError> {
        let permit = tokio::time::timeout(timeout, self.staging.permits.acquire())
            .await
            .map_err(|_| NixlError::PoolExhausted)?
            .map_err(|_| NixlError::PoolExhausted)?;
        permit.forget();
        let index = self.staging.free.lock().pop_front().ok_or(NixlError::PoolExhausted)?;
        Ok(StagingChunk {
            server: Arc::clone(self),
            index,
            len: 0,
        })
    }

    pub fn try_reserve(self: &Arc<Self>) -> Option<StagingChunk> {
        let permit = self.staging.permits.try_acquire().ok()?;
        permit.forget();
        let index = self.staging.free.lock().pop_front()?;
        Some(StagingChunk {
            server: Arc::clone(self),
            index,
            len: 0,
        })
    }

    pub fn available_chunks(&self) -> usize {
        self.staging.permits.available_permits()
    }

    /// Registers `len` bytes of host memory the decode path can emit from.
    pub fn register_arena(self: &Arc<Self>, len: usize) -> Result<Arc<RegisteredArena>, NixlError> {
        let buffer = HostBuffer::new(len)?;
        let mut opt_args = OptArgs::new().map_err(nixl_err("opt args"))?;
        opt_args.add_backend(&self.backend).map_err(nixl_err("opt args backend"))?;
        let registration = self
            .agent
            .register_memory(&buffer, Some(&opt_args))
            .map_err(nixl_err("register arena"))?;
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Ok(Arc::new(RegisteredArena {
            buffer,
            _registration: registration,
            server: Arc::clone(self),
            len,
            id: NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }))
    }

    /// Loads the client's agent metadata unless this exact metadata is already loaded.
    async fn ensure_remote(&self, token: &RdmaToken) -> Result<(), NixlError> {
        let Some(metadata) = &token.metadata else {
            if self.remotes.lock().contains_key(&token.agent) {
                return Ok(());
            }
            return Err(NixlError::Token(format!(
                "agent {:?} is unknown to this server; send {NIXL_METADATA_HEADER}",
                token.agent
            )));
        };
        let hash = metadata_hash(metadata);
        if self.remotes.lock().get(&token.agent) == Some(&hash) {
            return Ok(());
        }
        let _loading = self.load_lock.lock().await;
        if self.remotes.lock().get(&token.agent) == Some(&hash) {
            return Ok(());
        }
        let loaded = self
            .agent
            .load_remote_md(metadata)
            .map_err(nixl_err("load remote metadata"))?;
        if loaded != token.agent {
            return Err(NixlError::Token(format!(
                "metadata belongs to agent {loaded:?}, request names {:?}",
                token.agent
            )));
        }
        self.remotes.lock().insert(token.agent.clone(), hash);
        Ok(())
    }

    fn client_window(token: &RdmaToken, remote_offset: u64, len: u64) -> Result<u64, NixlError> {
        let end = remote_offset
            .checked_add(len)
            .ok_or_else(|| NixlError::Token("remote offset overflow".to_owned()))?;
        if end > token.buffer_len {
            return Err(NixlError::TokenTooSmall {
                buffer: token.buffer_len,
                needed: end,
            });
        }
        token
            .remote_addr
            .checked_add(remote_offset)
            .ok_or_else(|| NixlError::Token("remote address overflow".to_owned()))
    }

    /// Runs one transfer to completion. `build` produces the local and remote
    /// descriptor lists (pairing up by index) once a transfer slot is held,
    /// and every NIXL object except the request itself is dropped before the
    /// first await, so the future stays `Send` for the spawned pipelines.
    async fn transfer<B>(&self, op: XferOp, token: &RdmaToken, build: B) -> Result<(), NixlError>
    where
        B: FnOnce() -> Result<(XferDescList<'static>, XferDescList<'static>), NixlError> + Send,
    {
        self.ensure_remote(token).await?;
        self.transfer_loaded(op, token, build).await
    }

    /// `transfer` for an agent whose metadata is loaded.
    async fn transfer_loaded<B>(&self, op: XferOp, token: &RdmaToken, build: B) -> Result<(), NixlError>
    where
        B: FnOnce() -> Result<(XferDescList<'static>, XferDescList<'static>), NixlError> + Send,
    {
        let _inflight = Arc::clone(&self.inflight)
            .acquire_owned()
            .await
            .map_err(|_| NixlError::Token("server closed".to_owned()))?;
        let (request, in_progress) = {
            let (local, remote) = build()?;
            let mut opt_args = OptArgs::new().map_err(nixl_err("opt args"))?;
            opt_args.add_backend(&self.backend).map_err(nixl_err("opt args backend"))?;
            let request = self
                .agent
                .create_xfer_req(op, &local, &remote, &token.agent, Some(&opt_args))
                .map_err(nixl_err("create transfer"))?;
            // The binding answers `true` while the transfer is still in progress
            // and `false` when it completed before returning.
            let in_progress = self
                .agent
                .post_xfer_req(&request, Some(&opt_args))
                .map_err(nixl_err("post transfer"))?;
            (request, in_progress)
        };
        if !in_progress {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + self.transfer_timeout;
        let mut polls = 0u32;
        loop {
            match self.agent.get_xfer_status(&request).map_err(nixl_err("transfer status"))? {
                XferStatus::Success => return Ok(()),
                XferStatus::InProgress => {
                    if tokio::time::Instant::now() >= deadline {
                        // Dropping the request releases it on the library side; the
                        // client sees a failed GET instead of a hung one.
                        return Err(NixlError::TransferTimeout(self.transfer_timeout));
                    }
                    polls = polls.saturating_add(1);
                    if polls < 64 {
                        tokio::task::yield_now().await;
                    } else {
                        tokio::time::sleep(self.poll_interval).await;
                    }
                }
            }
        }
    }

    fn remote_list_for(memory: ClientMemory, device: u64, addr: u64, len: usize) -> Result<XferDescList<'static>, NixlError> {
        let mut list = XferDescList::new(memory.mem_type()).map_err(nixl_err("remote descriptors"))?;
        let addr = usize::try_from(addr).map_err(|_| NixlError::Token("remote address exceeds the platform".to_owned()))?;
        list.add_desc(addr, len, device);
        Ok(list)
    }

    fn local_list(addr: usize, len: usize) -> Result<XferDescList<'static>, NixlError> {
        let mut list = XferDescList::new(MemType::Dram).map_err(nixl_err("local descriptors"))?;
        list.add_desc(addr, len, 0);
        Ok(list)
    }

    /// Writes the chunk's staged bytes at `offset` of the client's buffer and
    /// hands the chunk back.
    pub async fn write_to_client_owned(
        self: &Arc<Self>,
        _key: &str,
        token: &RdmaToken,
        offset: u64,
        chunk: StagingChunk,
    ) -> Result<(u64, StagingChunk), NixlError> {
        let len = chunk.len();
        if len == 0 {
            return Ok((0, chunk));
        }
        let remote_addr = Self::client_window(token, offset, len as u64)?;
        let (local_addr, memory, device) = (chunk.local_addr(), token.memory, token.device);
        self.transfer(XferOp::Write, token, move || {
            Ok((
                Self::local_list(local_addr, len)?,
                Self::remote_list_for(memory, device, remote_addr, len)?,
            ))
        })
        .await?;
        Ok((len as u64, chunk))
    }

    /// Writes one arena range at `remote_offset`; `keep` stays alive for the transfer.
    pub async fn write_to_client_from_arena_owned<T: Send + 'static>(
        self: &Arc<Self>,
        _key: &str,
        token: &RdmaToken,
        remote_offset: u64,
        source: ArenaRange,
        keep: T,
    ) -> Result<(u64, T), NixlError> {
        if !Arc::ptr_eq(&source.arena.server, self) {
            return Err(NixlError::Token("arena is registered with another server".to_owned()));
        }
        if source.len == 0 {
            return Ok((0, keep));
        }
        if source
            .local_offset
            .checked_add(source.len)
            .is_none_or(|end| end > source.arena.len)
        {
            return Err(NixlError::Token("arena range outside the arena".to_owned()));
        }
        let remote_addr = Self::client_window(token, remote_offset, source.len as u64)?;
        let (local_addr, len, memory, device) =
            (source.arena.buffer.addr() + source.local_offset, source.len, token.memory, token.device);
        self.transfer(XferOp::Write, token, move || {
            Ok((
                Self::local_list(local_addr, len)?,
                Self::remote_list_for(memory, device, remote_addr, len)?,
            ))
        })
        .await?;
        Ok((source.len as u64, keep))
    }

    /// Writes several arena ranges to consecutive client offsets starting at
    /// `remote_offset` in one transfer; `keep` stays alive for the transfer.
    pub async fn write_to_client_sg_owned<T: Send + 'static>(
        self: &Arc<Self>,
        _key: &str,
        token: &RdmaToken,
        remote_offset: u64,
        source: ArenaRanges,
        keep: T,
    ) -> Result<(u64, T), NixlError> {
        if !Arc::ptr_eq(&source.arena.server, self) {
            return Err(NixlError::Token("arena is registered with another server".to_owned()));
        }
        if source.ranges.len() > MAX_SG_RANGES {
            return Err(NixlError::Token("too many scatter-gather ranges".to_owned()));
        }
        let total = source.total_len();
        if total == 0 {
            return Ok((0, keep));
        }
        let remote_base = Self::client_window(token, remote_offset, total as u64)?;
        let remote_base =
            usize::try_from(remote_base).map_err(|_| NixlError::Token("remote address exceeds the platform".to_owned()))?;
        let arena_base = source.arena.buffer.addr();
        for (local_offset, len) in &source.ranges {
            if local_offset.checked_add(*len).is_none_or(|end| end > source.arena.len) {
                return Err(NixlError::Token("arena range outside the arena".to_owned()));
            }
        }
        let (ranges, memory, device) = (source.ranges.clone(), token.memory, token.device);
        self.transfer(XferOp::Write, token, move || {
            let mut local = XferDescList::new(MemType::Dram).map_err(nixl_err("local descriptors"))?;
            let mut remote = XferDescList::new(memory.mem_type()).map_err(nixl_err("remote descriptors"))?;
            let mut cursor = 0usize;
            for (local_offset, len) in &ranges {
                local.add_desc(arena_base + local_offset, *len, 0);
                remote.add_desc(remote_base + cursor, *len, device);
                cursor += len;
            }
            Ok((local, remote))
        })
        .await?;
        Ok((total as u64, keep))
    }

    /// Reads up to the chunk's capacity from `offset` of the client's buffer
    /// into the chunk (RDMA PUT) and hands the chunk back filled.
    pub async fn read_from_client_owned(
        self: &Arc<Self>,
        _key: &str,
        token: &RdmaToken,
        offset: u64,
        len: usize,
        mut chunk: StagingChunk,
    ) -> Result<StagingChunk, NixlError> {
        if len == 0 {
            chunk.clear();
            return Ok(chunk);
        }
        if len > chunk.capacity() {
            return Err(NixlError::Token("read exceeds the staging chunk".to_owned()));
        }
        let remote_addr = Self::client_window(token, offset, len as u64)?;
        chunk.clear();
        let (local_addr, memory, device) = (chunk.local_addr(), token.memory, token.device);
        self.transfer(XferOp::Read, token, move || {
            Ok((
                Self::local_list(local_addr, len)?,
                Self::remote_list_for(memory, device, remote_addr, len)?,
            ))
        })
        .await?;
        chunk.advance(len)?;
        Ok(chunk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_parses_memory_device_address_and_length() {
        let token = RdmaToken::from_headers("loader-7", None, "vram:4:0x7f0000000000:1073741824").expect("token");
        assert_eq!(token.agent, "loader-7");
        assert_eq!(token.memory, ClientMemory::Vram);
        assert_eq!(token.device, 4);
        assert_eq!(token.remote_addr, 0x7f00_0000_0000);
        assert_eq!(token.buffer_len, 1 << 30);
        assert!(token.metadata.is_none());
        let host = RdmaToken::from_headers("a", Some("AQID"), "dram:0:4096:0x100").expect("token");
        assert_eq!(host.memory, ClientMemory::Dram);
        assert_eq!(host.metadata.as_deref(), Some(&[1u8, 2, 3][..]));
    }

    #[test]
    fn token_rejects_malformed_headers() {
        assert!(RdmaToken::from_headers("", None, "vram:0:1:1").is_err());
        assert!(RdmaToken::from_headers("a", None, "disk:0:1:1").is_err());
        assert!(RdmaToken::from_headers("a", None, "vram:0:1:0").is_err());
        assert!(RdmaToken::from_headers("a", None, "vram:0:0xffffffffffffffff:2").is_err());
        assert!(RdmaToken::from_headers("a", Some("not base64!"), "vram:0:1:1").is_err());
        assert!(RdmaToken::from_headers("a", None, "vram:0:1").is_err());
    }

    #[test]
    fn client_window_checks_the_buffer_bounds() {
        let token = RdmaToken::from_headers("a", None, "dram:0:1000:100").expect("token");
        assert_eq!(NixlServer::client_window(&token, 40, 60).expect("fits"), 1040);
        assert!(matches!(
            NixlServer::client_window(&token, 50, 60),
            Err(NixlError::TokenTooSmall {
                buffer: 100,
                needed: 110
            })
        ));
    }
}
