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

//! S3-over-RDMA GET through NVIDIA cuObject (`--features cuobject`).
//!
//! A GET carrying `x-amz-rdma-token` is served by the ordinary read path up to
//! the final byte stream; those bytes are staged in registered host memory and
//! RDMA-written into the client's buffer instead of streamed over HTTP. One
//! cuObject server exists per configured RDMA address; requests round-robin
//! over them. Everything before the body (authentication, range resolution,
//! decryption, decompression, erasure decode, bitrot verification) is shared
//! with the HTTP path, so the data is identical; only the last hop differs.

use crate::app::storage_api::object_usecase::io::DynReader;
use crate::app::storage_api::object_usecase::runtime::{get_global_endpoints_opt, global_rustfs_port};
use bytes::Bytes;
use http::HeaderMap;
use rustfs_config::{
    DEFAULT_RUSTFS_CUOBJECT_CHUNK_BYTES, DEFAULT_RUSTFS_CUOBJECT_ENABLE, DEFAULT_RUSTFS_CUOBJECT_NUM_DCIS,
    DEFAULT_RUSTFS_CUOBJECT_POOL_BYTES, DEFAULT_RUSTFS_CUOBJECT_PORT, ENV_RUSTFS_CUOBJECT_ADDRS, ENV_RUSTFS_CUOBJECT_CHUNK_BYTES,
    ENV_RUSTFS_CUOBJECT_ENABLE, ENV_RUSTFS_CUOBJECT_NUM_DCIS, ENV_RUSTFS_CUOBJECT_POOL_BYTES, ENV_RUSTFS_CUOBJECT_PORT,
};
use rustfs_cuobject::{CuObjectConfig, CuObjectError, CuObjectServer, RDMA_TOKEN_HEADER, RdmaToken};
use rustfs_utils::{get_env_bool, get_env_opt_str, get_env_u16, get_env_usize};
use s3s::{S3Error, S3ErrorCode, S3Result};
use std::collections::BTreeSet;
use std::io;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tracing::{info, warn};

const LOG_COMPONENT_OBJECT: &str = "object";
const LOG_SUBSYSTEM_CUOBJECT: &str = "cuobject";
const EVENT_CUOBJECT_SERVICE_STATE: &str = "cuobject_service_state";
const EVENT_CUOBJECT_GET_REJECTED: &str = "cuobject_get_rejected";
const STAGING_RESERVE_TIMEOUT: Duration = Duration::from_secs(30);

static SERVICE: OnceLock<Arc<CuObjectService>> = OnceLock::new();

pub(crate) struct CuObjectService {
    servers: Vec<Arc<CuObjectServer>>,
    next: AtomicUsize,
}

impl CuObjectService {
    fn pick(&self) -> &Arc<CuObjectServer> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.servers.len();
        &self.servers[index]
    }
}

fn local_endpoint_hosts() -> Vec<String> {
    get_global_endpoints_opt()
        .map(|pools| {
            pools
                .as_ref()
                .iter()
                .flat_map(|pool| pool.endpoints.as_ref().iter())
                .filter(|endpoint| endpoint.is_local)
                .filter_map(|endpoint| endpoint.url.host_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

async fn resolve_addrs() -> io::Result<Vec<IpAddr>> {
    let mut ips = BTreeSet::new();
    match get_env_opt_str(ENV_RUSTFS_CUOBJECT_ADDRS) {
        Some(raw) => {
            for entry in raw.split(',').map(str::trim).filter(|entry| !entry.is_empty()) {
                let ip: IpAddr = entry.parse().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("{ENV_RUSTFS_CUOBJECT_ADDRS} entry {entry:?} is not an IP address"),
                    )
                })?;
                ips.insert(ip);
            }
        }
        None => {
            for host in local_endpoint_hosts() {
                let host = host.trim_matches(|c| c == '[' || c == ']').to_owned();
                if let Ok(ip) = host.parse::<IpAddr>() {
                    if !ip.is_unspecified() && !ip.is_loopback() {
                        ips.insert(ip);
                    }
                    continue;
                }
                for addr in tokio::net::lookup_host((host.as_str(), global_rustfs_port())).await? {
                    if !addr.ip().is_loopback() {
                        ips.insert(addr.ip());
                    }
                }
            }
        }
    }
    Ok(ips.into_iter().collect())
}

/// Starts one cuObject server per RDMA address when `RUSTFS_CUOBJECT_ENABLE` is set.
///
/// Invalid settings and server construction failures fail startup; a node
/// without RDMA addresses logs and serves GETs over HTTP only.
pub(crate) async fn init_cuobject_service() -> io::Result<()> {
    if !get_env_bool(ENV_RUSTFS_CUOBJECT_ENABLE, DEFAULT_RUSTFS_CUOBJECT_ENABLE) {
        return Ok(());
    }
    let config = CuObjectConfig {
        port: get_env_u16(ENV_RUSTFS_CUOBJECT_PORT, DEFAULT_RUSTFS_CUOBJECT_PORT),
        num_dcis: i32::try_from(get_env_usize(ENV_RUSTFS_CUOBJECT_NUM_DCIS, DEFAULT_RUSTFS_CUOBJECT_NUM_DCIS as usize))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, format!("{ENV_RUSTFS_CUOBJECT_NUM_DCIS} is too large")))?,
        chunk_bytes: get_env_usize(ENV_RUSTFS_CUOBJECT_CHUNK_BYTES, DEFAULT_RUSTFS_CUOBJECT_CHUNK_BYTES),
        pool_bytes: get_env_usize(ENV_RUSTFS_CUOBJECT_POOL_BYTES, DEFAULT_RUSTFS_CUOBJECT_POOL_BYTES),
        ..CuObjectConfig::default()
    };
    if config.chunk_bytes == 0
        || config.chunk_bytes > rustfs_cuobject::MAX_RDMA_OP_BYTES
        || config.pool_bytes < config.chunk_bytes
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{ENV_RUSTFS_CUOBJECT_CHUNK_BYTES}/{ENV_RUSTFS_CUOBJECT_POOL_BYTES} are inconsistent"),
        ));
    }
    let addrs = resolve_addrs().await?;
    if addrs.is_empty() {
        info!(
            event = EVENT_CUOBJECT_SERVICE_STATE,
            component = LOG_COMPONENT_OBJECT,
            subsystem = LOG_SUBSYSTEM_CUOBJECT,
            state = "skipped",
            reason = "no_rdma_address",
            "cuobject service not started: no local endpoint host or RUSTFS_CUOBJECT_ADDRS"
        );
        return Ok(());
    }
    let mut servers = Vec::with_capacity(addrs.len());
    for addr in addrs {
        let config = config.clone();
        // Server construction opens the RDMA device and pins the staging pool.
        let server = tokio::task::spawn_blocking(move || CuObjectServer::new(addr, &config))
            .await
            .map_err(io::Error::other)?
            .map_err(|error| io::Error::other(format!("cuobject server on {addr}: {error}")))?;
        servers.push(server);
    }
    let (major, minor) = rustfs_cuobject::library_version();
    info!(
        event = EVENT_CUOBJECT_SERVICE_STATE,
        component = LOG_COMPONENT_OBJECT,
        subsystem = LOG_SUBSYSTEM_CUOBJECT,
        state = "started",
        server_count = servers.len(),
        port = config.port,
        chunk_bytes = config.chunk_bytes,
        pool_bytes = config.pool_bytes,
        library = format_args!("{major}.{minor}"),
        "cuobject s3-over-rdma service started"
    );
    let _ = SERVICE.set(Arc::new(CuObjectService {
        servers,
        next: AtomicUsize::new(0),
    }));
    Ok(())
}

/// The parsed RDMA token of a GET that must be served over RDMA.
///
/// Returns `None` when the header is absent, the service is not running, or
/// the token is malformed; in the last two cases the response carries no
/// `x-amz-rdma-reply`, which the client reads as "declined".
pub(crate) fn rdma_token_for_request(headers: &HeaderMap) -> Option<RdmaToken> {
    let raw = headers.get(RDMA_TOKEN_HEADER)?.to_str().ok()?;
    SERVICE.get()?;
    match RdmaToken::parse(raw) {
        Ok(token) => Some(token),
        Err(error) => {
            warn!(
                event = EVENT_CUOBJECT_GET_REJECTED,
                component = LOG_COMPONENT_OBJECT,
                subsystem = LOG_SUBSYSTEM_CUOBJECT,
                result = "declined",
                reason = "token_malformed",
                error = %error,
                "cuobject get declined"
            );
            None
        }
    }
}

fn transfer_error(context: &str, error: CuObjectError) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, format!("rdma get {context}: {error}"))
}

/// Writes the staged bytes at the client's current offset and empties the chunk.
async fn flush_chunk(
    server: &Arc<CuObjectServer>,
    object: &str,
    token: &RdmaToken,
    chunk: &mut rustfs_cuobject::StagingChunk,
    moved: &mut u64,
) -> S3Result<()> {
    let written = server
        .write_to_client(object, token, *moved, chunk)
        .await
        .map_err(|error| transfer_error("write", error))?;
    *moved += written;
    chunk.clear();
    Ok(())
}

/// Moves the response bytes into the client's buffer; returns the payload byte count.
pub(crate) async fn serve_get_over_rdma(
    stream: DynReader,
    buffered_body: Option<Bytes>,
    response_content_length: i64,
    bucket: &str,
    key: &str,
    token: &RdmaToken,
) -> S3Result<u64> {
    let service = SERVICE
        .get()
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::NotImplemented, "rdma get is not enabled"))?;
    let expected = u64::try_from(response_content_length).unwrap_or(0);
    if expected > token.buffer_len {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRange,
            format!(
                "rdma token buffer ({} bytes) is smaller than the response ({expected} bytes)",
                token.buffer_len
            ),
        ));
    }
    let server = Arc::clone(service.pick());
    let started = Instant::now();
    let object = format!("{bucket}/{key}");
    let mut chunk = server
        .reserve(STAGING_RESERVE_TIMEOUT)
        .await
        .map_err(|error| transfer_error("staging", error))?;
    let mut moved: u64 = 0;
    match buffered_body {
        Some(bytes) => {
            let mut rest = &bytes[..];
            while !rest.is_empty() {
                let take = rest.len().min(chunk.capacity() - chunk.len());
                chunk
                    .append(&rest[..take])
                    .map_err(|error| transfer_error("staging", error))?;
                rest = &rest[take..];
                if chunk.is_full() {
                    flush_chunk(&server, &object, token, &mut chunk, &mut moved).await?;
                }
            }
        }
        None => {
            let mut stream = stream;
            // Stop at the committed response length: readers may be unbounded.
            let mut remaining = expected;
            while remaining > 0 {
                let spare = chunk.spare_mut().map_err(|error| transfer_error("staging", error))?;
                let want = spare.len().min(usize::try_from(remaining).unwrap_or(usize::MAX));
                let read = stream
                    .read(&mut spare[..want])
                    .await
                    .map_err(|error| transfer_error("read", CuObjectError::Source(error)))?;
                if read == 0 {
                    break;
                }
                chunk.advance(read).map_err(|error| transfer_error("staging", error))?;
                remaining -= read as u64;
                if chunk.is_full() {
                    flush_chunk(&server, &object, token, &mut chunk, &mut moved).await?;
                }
            }
        }
    }
    if !chunk.is_empty() {
        flush_chunk(&server, &object, token, &mut chunk, &mut moved).await?;
    }
    if moved != expected {
        return Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("rdma get moved {moved} of {expected} bytes"),
        ));
    }
    rustfs_io_metrics::record_get_object_stage_duration(
        super::GET_OBJECT_STAGE_PATH_S3_HANDLER,
        "rdma_transfer",
        started.elapsed().as_secs_f64(),
    );
    Ok(moved)
}
