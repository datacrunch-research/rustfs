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

//! Internode RDMA data transport (`RUSTFS_INTERNODE_DATA_TRANSPORT=rdma`).
//!
//! Linux-only and compiled behind the `rdma` Cargo feature. Shard reads and
//! writes between nodes move through reliable-connected queue pairs over
//! libibverbs/librdmacm; the gRPC control plane, directory walks and scanner
//! streams stay on TCP. See `docs/operations/internode-rdma-transport.md`.

pub mod client;
pub mod error;
pub mod proto;
pub mod server;

mod buffer;
mod conn;
mod ffi;
mod stream;
#[cfg(test)]
mod tests;

pub use buffer::RegisteredPool;
pub use client::RdmaInternodeDataTransport;
pub use server::{RdmaBodyStream, RdmaReadRequest, RdmaReject, RdmaRequestHandler, RdmaServer, RdmaWriteRequest};

use crate::cluster::rpc::internode_data_transport::InternodeDataTransport;
use rustfs_config::{
    DEFAULT_RUSTFS_RDMA_CHUNK_SIZE, DEFAULT_RUSTFS_RDMA_CONNECT_TIMEOUT_MS, DEFAULT_RUSTFS_RDMA_FALLBACK_TCP,
    DEFAULT_RUSTFS_RDMA_IDLE_CONNS_PER_PEER, DEFAULT_RUSTFS_RDMA_POOL_BYTES, DEFAULT_RUSTFS_RDMA_PORT_OFFSET,
    DEFAULT_RUSTFS_RDMA_STALL_TIMEOUT_SECS, DEFAULT_RUSTFS_RDMA_STREAM_SLOTS, ENV_RUSTFS_INTERNODE_DATA_TRANSPORT,
    ENV_RUSTFS_RDMA_CHUNK_SIZE, ENV_RUSTFS_RDMA_CONNECT_TIMEOUT_MS, ENV_RUSTFS_RDMA_FALLBACK_TCP,
    ENV_RUSTFS_RDMA_IDLE_CONNS_PER_PEER, ENV_RUSTFS_RDMA_LISTEN_ADDRS, ENV_RUSTFS_RDMA_POOL_BYTES, ENV_RUSTFS_RDMA_PORT_OFFSET,
    ENV_RUSTFS_RDMA_SERVER_ENABLE, ENV_RUSTFS_RDMA_STALL_TIMEOUT_SECS, ENV_RUSTFS_RDMA_STREAM_SLOTS,
    INTERNODE_DATA_TRANSPORT_RDMA,
};
use rustfs_utils::{get_env_bool, get_env_opt_str, get_env_u16, get_env_u64, get_env_usize};
use std::collections::BTreeSet;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

const MIN_CHUNK_SIZE: usize = 4 * 1024;
const MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024;
const MAX_STREAM_SLOTS: usize = 64;
const MAX_IDLE_CONNS_PER_PEER: usize = 4096;

/// Runtime configuration of the RDMA transport, read once from the environment.
#[derive(Debug, Clone)]
pub struct RdmaSettings {
    /// Added to a peer's HTTP port to reach its RDMA listener.
    pub port_offset: u16,
    /// Bytes per registered slot and per RDMA write.
    pub chunk_size: usize,
    /// Slots per stream window on each side.
    pub stream_slots: usize,
    /// Total registered memory of the shared pool.
    pub pool_bytes: usize,
    /// Idle connections kept per peer (`RUSTFS_RDMA_IDLE_CONNS_PER_PEER`).
    pub idle_conns_per_peer: usize,
    /// Reissue a request over TCP when RDMA setup fails before any payload moved.
    pub fallback_tcp: bool,
    pub connect_timeout: Duration,
    /// Longest pause without stream progress before the stream fails.
    pub stall_timeout: Duration,
    /// Explicit listener addresses; empty means derive from the local endpoints.
    pub listen_addrs: Vec<IpAddr>,
}

impl RdmaSettings {
    pub fn from_env() -> io::Result<Self> {
        let chunk_size = get_env_usize(ENV_RUSTFS_RDMA_CHUNK_SIZE, DEFAULT_RUSTFS_RDMA_CHUNK_SIZE);
        if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{ENV_RUSTFS_RDMA_CHUNK_SIZE}={chunk_size} must be between {MIN_CHUNK_SIZE} and {MAX_CHUNK_SIZE} bytes"),
            ));
        }
        let stream_slots = get_env_usize(ENV_RUSTFS_RDMA_STREAM_SLOTS, DEFAULT_RUSTFS_RDMA_STREAM_SLOTS);
        let idle_conns_per_peer = get_env_usize(ENV_RUSTFS_RDMA_IDLE_CONNS_PER_PEER, DEFAULT_RUSTFS_RDMA_IDLE_CONNS_PER_PEER);
        if !(1..=MAX_IDLE_CONNS_PER_PEER).contains(&idle_conns_per_peer) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{ENV_RUSTFS_RDMA_IDLE_CONNS_PER_PEER}={idle_conns_per_peer} must be between 1 and {MAX_IDLE_CONNS_PER_PEER}"
                ),
            ));
        }
        if !(1..=MAX_STREAM_SLOTS).contains(&stream_slots) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{ENV_RUSTFS_RDMA_STREAM_SLOTS}={stream_slots} must be between 1 and {MAX_STREAM_SLOTS}"),
            ));
        }
        let pool_bytes = get_env_usize(ENV_RUSTFS_RDMA_POOL_BYTES, DEFAULT_RUSTFS_RDMA_POOL_BYTES);
        let minimum_pool = chunk_size
            .checked_mul(stream_slots)
            .and_then(|window| window.checked_mul(2))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "rdma window size overflow"))?;
        if pool_bytes < minimum_pool {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{ENV_RUSTFS_RDMA_POOL_BYTES}={pool_bytes} must hold at least two stream windows ({minimum_pool} bytes)"),
            ));
        }
        let connect_timeout_ms = get_env_u64(ENV_RUSTFS_RDMA_CONNECT_TIMEOUT_MS, DEFAULT_RUSTFS_RDMA_CONNECT_TIMEOUT_MS);
        if connect_timeout_ms == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{ENV_RUSTFS_RDMA_CONNECT_TIMEOUT_MS} must be positive"),
            ));
        }
        let stall_timeout_secs = get_env_u64(ENV_RUSTFS_RDMA_STALL_TIMEOUT_SECS, DEFAULT_RUSTFS_RDMA_STALL_TIMEOUT_SECS);
        if stall_timeout_secs == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{ENV_RUSTFS_RDMA_STALL_TIMEOUT_SECS} must be positive"),
            ));
        }
        let listen_addrs = match get_env_opt_str(ENV_RUSTFS_RDMA_LISTEN_ADDRS) {
            Some(raw) => raw
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(|entry| {
                    entry.parse::<IpAddr>().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("{ENV_RUSTFS_RDMA_LISTEN_ADDRS} entry {entry:?} is not an IP address"),
                        )
                    })
                })
                .collect::<io::Result<Vec<_>>>()?,
            None => Vec::new(),
        };
        Ok(Self {
            port_offset: get_env_u16(ENV_RUSTFS_RDMA_PORT_OFFSET, DEFAULT_RUSTFS_RDMA_PORT_OFFSET),
            chunk_size,
            stream_slots,
            pool_bytes,
            idle_conns_per_peer,
            fallback_tcp: get_env_bool(ENV_RUSTFS_RDMA_FALLBACK_TCP, DEFAULT_RUSTFS_RDMA_FALLBACK_TCP),
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            stall_timeout: Duration::from_secs(stall_timeout_secs),
            listen_addrs,
        })
    }

    pub(crate) fn qp_limits(&self) -> ffi::QpLimits {
        conn::qp_limits(self.stream_slots)
    }
}

static SETTINGS: OnceLock<Result<Arc<RdmaSettings>, String>> = OnceLock::new();
static POOL: OnceLock<Result<Arc<RegisteredPool>, String>> = OnceLock::new();

/// Process-wide RDMA settings; the first read fixes them for the process lifetime.
pub fn settings() -> io::Result<Arc<RdmaSettings>> {
    SETTINGS
        .get_or_init(|| RdmaSettings::from_env().map(Arc::new).map_err(|err| err.to_string()))
        .clone()
        .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))
}

/// The registered buffer pool shared by the client transport and the server.
pub fn shared_pool() -> io::Result<Arc<RegisteredPool>> {
    let settings = settings()?;
    POOL.get_or_init(|| RegisteredPool::new(settings.pool_bytes, settings.chunk_size).map_err(|err| err.to_string()))
        .clone()
        .map_err(io::Error::other)
}

/// Whether `RUSTFS_INTERNODE_DATA_TRANSPORT` selects the RDMA backend.
pub fn transport_selected() -> bool {
    std::env::var(ENV_RUSTFS_INTERNODE_DATA_TRANSPORT)
        .ok()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case(INTERNODE_DATA_TRANSPORT_RDMA))
}

/// Whether this node should serve shard streams over RDMA.
pub fn server_enabled() -> bool {
    get_env_bool(ENV_RUSTFS_RDMA_SERVER_ENABLE, transport_selected())
}

pub(super) fn build_transport() -> Result<Arc<dyn InternodeDataTransport>, String> {
    let settings = settings().map_err(|err| err.to_string())?;
    let pool = shared_pool().map_err(|err| err.to_string())?;
    Ok(Arc::new(RdmaInternodeDataTransport::new(settings, pool)))
}

/// Listener addresses for a node whose internode HTTP server listens on `server_port`.
///
/// Explicit `RUSTFS_RDMA_LISTEN_ADDRS` entries win; otherwise every distinct
/// address the local endpoint hosts resolve to is used.
pub async fn default_listen_addrs(local_hosts: &[String], server_port: u16) -> io::Result<Vec<SocketAddr>> {
    let settings = settings()?;
    let port = server_port.checked_add(settings.port_offset).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{ENV_RUSTFS_RDMA_PORT_OFFSET}={} overflows the server port {server_port}",
                settings.port_offset
            ),
        )
    })?;
    if !settings.listen_addrs.is_empty() {
        return Ok(settings.listen_addrs.iter().map(|ip| SocketAddr::new(*ip, port)).collect());
    }
    let mut ips = BTreeSet::new();
    for host in local_hosts {
        let host = host.trim_matches(|c| c == '[' || c == ']');
        if let Ok(ip) = host.parse::<IpAddr>() {
            if !ip.is_unspecified() && !ip.is_loopback() {
                ips.insert(ip);
            }
            continue;
        }
        for addr in tokio::net::lookup_host((host, port)).await? {
            if !addr.ip().is_loopback() {
                ips.insert(addr.ip());
            }
        }
    }
    Ok(ips.into_iter().map(|ip| SocketAddr::new(ip, port)).collect())
}
