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

//! Server side of NVIDIA cuObject S3-over-RDMA.
//!
//! A client that supports the protocol (NVIDIA cuObject, NIXL's object plugin,
//! …) sends an ordinary S3 GET with an `x-amz-rdma-token` header describing a
//! registered client buffer (host or GPU memory). Instead of streaming the
//! body over HTTP, the server RDMA-writes the object bytes into that buffer
//! through NVIDIA's `libcuobjserver` and answers with an empty body plus
//! `x-amz-rdma-reply` and `x-amz-rdma-bytes-transferred`.
//!
//! This crate wraps the C++ library behind a C shim and owns a pool of
//! registered, page-aligned host staging buffers: the regular read path fills
//! a staging buffer and [`CuObjectServer::write_to_client`] pushes it.

mod ffi;

pub use ffi::library_version;

use bytes::Bytes;
use std::collections::VecDeque;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Semaphore;

/// Header carrying the client's RDMA descriptor on a request.
pub const RDMA_TOKEN_HEADER: &str = "x-amz-rdma-token";
/// Header carrying the transfer status on the response (HTTP-style code).
pub const RDMA_REPLY_HEADER: &str = "x-amz-rdma-reply";
/// Header carrying the payload byte count moved over RDMA.
pub const RDMA_BYTES_TRANSFERRED_HEADER: &str = "x-amz-rdma-bytes-transferred";
/// Reply code for a declined transfer (the client then errors or falls back).
pub const RDMA_REPLY_NOT_IMPLEMENTED: u16 = 501;
/// Largest single `handleGetObject` the library accepts.
pub const MAX_RDMA_OP_BYTES: usize = 1024 * 1024 * 1024;
/// Default RDMA control port of the cuObject server.
pub const DEFAULT_PORT: u16 = 18515;

#[derive(Debug, Error)]
pub enum CuObjectError {
    #[error("cuobject {op} failed: {source}")]
    Os {
        op: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("cuobject rdma token is malformed: {0}")]
    Token(String),
    #[error("cuobject rdma token buffer ({buffer} bytes) is smaller than the response ({needed} bytes)")]
    TokenTooSmall { buffer: u64, needed: u64 },
    #[error("cuobject transfer failed with verbs status {verbs_status}: {source}")]
    Transfer {
        verbs_status: i32,
        #[source]
        source: std::io::Error,
    },
    #[error("cuobject transfer moved {moved} of {expected} bytes")]
    ShortTransfer { moved: u64, expected: u64 },
    #[error("cuobject staging pool exhausted")]
    PoolExhausted,
    #[error("cuobject read source failed: {0}")]
    Source(#[source] std::io::Error),
}

/// Parsed `x-amz-rdma-token`.
///
/// Wire format (cuObject RDMA descriptor protocol): colon-separated hex
/// fields `addr:size:rkey:lid:dctn:gid_present:gid`; only the leading
/// address and size are interpreted here, the library consumes the rest.
#[derive(Debug, Clone)]
pub struct RdmaToken {
    raw: String,
    pub remote_addr: u64,
    pub buffer_len: u64,
}

impl RdmaToken {
    pub fn parse(raw: &str) -> Result<Self, CuObjectError> {
        let raw = raw.trim();
        let mut fields = raw.split(':');
        let addr = fields
            .next()
            .ok_or_else(|| CuObjectError::Token("missing address field".to_owned()))?;
        let size = fields
            .next()
            .ok_or_else(|| CuObjectError::Token("missing size field".to_owned()))?;
        if fields.clone().count() < 4 {
            return Err(CuObjectError::Token("fewer than six fields".to_owned()));
        }
        let remote_addr = u64::from_str_radix(addr, 16).map_err(|_| CuObjectError::Token("address is not hex".to_owned()))?;
        let buffer_len = u64::from_str_radix(size, 16).map_err(|_| CuObjectError::Token("size is not hex".to_owned()))?;
        if buffer_len == 0 {
            return Err(CuObjectError::Token("zero-length buffer".to_owned()));
        }
        Ok(Self {
            raw: raw.to_owned(),
            remote_addr,
            buffer_len,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }
}

/// Tuning for one server instance.
#[derive(Debug, Clone)]
pub struct CuObjectConfig {
    pub port: u16,
    pub num_dcis: i32,
    pub cq_depth: u32,
    /// Size of one staging buffer; one RDMA write moves at most this much.
    pub chunk_bytes: usize,
    /// Total registered staging memory per server instance.
    pub pool_bytes: usize,
}

impl Default for CuObjectConfig {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            num_dcis: 128,
            cq_depth: 640,
            chunk_bytes: 64 * 1024 * 1024,
            pool_bytes: 1024 * 1024 * 1024,
        }
    }
}

struct Staging {
    buffer: ffi::RegisteredBuffer,
    free: parking_lot::Mutex<VecDeque<usize>>,
    permits: Semaphore,
    chunk_bytes: usize,
}

/// One cuObject server bound to the RDMA device that owns an IP address.
pub struct CuObjectServer {
    raw: ffi::Server,
    staging: Staging,
    addr: IpAddr,
    port: u16,
}

impl std::fmt::Debug for CuObjectServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CuObjectServer")
            .field("addr", &self.addr)
            .field("port", &self.port)
            .field("chunk_bytes", &self.staging.chunk_bytes)
            .finish()
    }
}

/// A reserved staging chunk; returned to the pool on drop.
pub struct StagingChunk {
    server: Arc<CuObjectServer>,
    index: usize,
    len: usize,
}

impl StagingChunk {
    pub fn capacity(&self) -> usize {
        self.server.staging.chunk_bytes
    }

    /// Bytes currently staged.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Copies `data` at `self.len()` and advances the staged length.
    pub fn append(&mut self, data: &[u8]) -> Result<(), CuObjectError> {
        let offset = self.index * self.server.staging.chunk_bytes + self.len;
        if self.len + data.len() > self.server.staging.chunk_bytes {
            return Err(CuObjectError::Token("staging append exceeds chunk".to_owned()));
        }
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

    /// The unfilled remainder of the chunk, for a reader to fill directly.
    pub fn spare_mut(&mut self) -> Result<&mut [u8], CuObjectError> {
        let offset = self.index * self.server.staging.chunk_bytes + self.len;
        let len = self.server.staging.chunk_bytes - self.len;
        self.server.staging.buffer.slice_mut(offset, len)
    }

    /// Marks `n` more bytes of the spare area as staged.
    pub fn advance(&mut self, n: usize) -> Result<(), CuObjectError> {
        if self.len + n > self.server.staging.chunk_bytes {
            return Err(CuObjectError::Token("staging advance exceeds chunk".to_owned()));
        }
        self.len += n;
        Ok(())
    }

    fn local_offset(&self) -> u64 {
        (self.index * self.server.staging.chunk_bytes) as u64
    }
}

impl Drop for StagingChunk {
    fn drop(&mut self) {
        self.server.staging.free.lock().push_back(self.index);
        self.server.staging.permits.add_permits(1);
    }
}

impl CuObjectServer {
    /// Creates the server and its registered staging pool.
    pub fn new(addr: IpAddr, config: &CuObjectConfig) -> Result<Arc<Self>, CuObjectError> {
        if config.chunk_bytes == 0 || config.chunk_bytes > MAX_RDMA_OP_BYTES || config.pool_bytes < config.chunk_bytes {
            return Err(CuObjectError::Token("invalid staging configuration".to_owned()));
        }
        let raw = ffi::Server::create(addr, config.port, config.num_dcis, config.cq_depth)?;
        let chunks = config.pool_bytes / config.chunk_bytes;
        let buffer = raw.register_host_buffer(chunks * config.chunk_bytes)?;
        Ok(Arc::new(Self {
            raw,
            staging: Staging {
                buffer,
                free: parking_lot::Mutex::new((0..chunks).collect()),
                permits: Semaphore::new(chunks),
                chunk_bytes: config.chunk_bytes,
            },
            addr,
            port: config.port,
        }))
    }

    pub fn addr(&self) -> IpAddr {
        self.addr
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn chunk_bytes(&self) -> usize {
        self.staging.chunk_bytes
    }

    /// Reserves one staging chunk, waiting up to `timeout` for a free one.
    pub async fn reserve(self: &Arc<Self>, timeout: Duration) -> Result<StagingChunk, CuObjectError> {
        let permit = tokio::time::timeout(timeout, self.staging.permits.acquire())
            .await
            .map_err(|_| CuObjectError::PoolExhausted)?
            .map_err(|_| CuObjectError::PoolExhausted)?;
        permit.forget();
        let index = self.staging.free.lock().pop_front().ok_or(CuObjectError::PoolExhausted)?;
        Ok(StagingChunk {
            server: Arc::clone(self),
            index,
            len: 0,
        })
    }

    /// RDMA-writes the staged bytes of `chunk` to `token.remote_addr + remote_offset`.
    ///
    /// Blocks a worker thread for the duration of the write; the client's
    /// buffer bounds from the token are enforced before any DMA.
    pub async fn write_to_client(
        self: &Arc<Self>,
        key: &str,
        token: &RdmaToken,
        remote_offset: u64,
        chunk: &StagingChunk,
    ) -> Result<u64, CuObjectError> {
        let len = chunk.len() as u64;
        if len == 0 {
            return Ok(0);
        }
        let end = remote_offset
            .checked_add(len)
            .ok_or_else(|| CuObjectError::Token("remote offset overflow".to_owned()))?;
        if end > token.buffer_len {
            return Err(CuObjectError::TokenTooSmall {
                buffer: token.buffer_len,
                needed: end,
            });
        }
        let remote_addr = token
            .remote_addr
            .checked_add(remote_offset)
            .ok_or_else(|| CuObjectError::Token("remote address overflow".to_owned()))?;
        let server = Arc::clone(self);
        let key = key.to_owned();
        let descriptor = token.raw.clone();
        let local_offset = chunk.local_offset();
        let len_usize = chunk.len();
        tokio::task::spawn_blocking(move || {
            let channel = server.raw.alloc_channel()?;
            let result = server
                .raw
                .get(&key, &server.staging.buffer, local_offset, remote_addr, len_usize, &descriptor, channel);
            server.raw.free_channel(channel);
            let moved = result?;
            if moved != len {
                return Err(CuObjectError::ShortTransfer { moved, expected: len });
            }
            Ok(moved)
        })
        .await
        .map_err(|error| CuObjectError::Os {
            op: "write task",
            source: std::io::Error::other(error),
        })?
    }
}

impl CuObjectServer {
    /// Like [`Self::write_to_client`], but owns the chunk for the whole DMA: the
    /// blocking task holds it until the library call returns, so cancelling the
    /// awaiting future can never release the staging slot under an in-flight
    /// write. Returns the chunk with the byte count so the caller can reuse it.
    pub async fn write_to_client_owned(
        self: &Arc<Self>,
        key: &str,
        token: &RdmaToken,
        remote_offset: u64,
        chunk: StagingChunk,
    ) -> Result<(u64, StagingChunk), CuObjectError> {
        let len = chunk.len() as u64;
        if len == 0 {
            return Ok((0, chunk));
        }
        let end = remote_offset
            .checked_add(len)
            .ok_or_else(|| CuObjectError::Token("remote offset overflow".to_owned()))?;
        if end > token.buffer_len {
            return Err(CuObjectError::TokenTooSmall {
                buffer: token.buffer_len,
                needed: end,
            });
        }
        let remote_addr = token
            .remote_addr
            .checked_add(remote_offset)
            .ok_or_else(|| CuObjectError::Token("remote address overflow".to_owned()))?;
        let server = Arc::clone(self);
        let key = key.to_owned();
        let descriptor = token.raw.clone();
        tokio::task::spawn_blocking(move || {
            let local_offset = chunk.local_offset();
            let len_usize = chunk.len();
            let channel = server.raw.alloc_channel()?;
            let result = server
                .raw
                .get(&key, &server.staging.buffer, local_offset, remote_addr, len_usize, &descriptor, channel);
            server.raw.free_channel(channel);
            let moved = result?;
            if moved != len {
                return Err(CuObjectError::ShortTransfer { moved, expected: len });
            }
            // `chunk` is dropped (or returned) only here, after the DMA finished.
            Ok((moved, chunk))
        })
        .await
        .map_err(|error| CuObjectError::Os {
            op: "write task",
            source: std::io::Error::other(error),
        })?
    }
}

/// Copies owned bytes into staging; convenience for callers holding `Bytes`.
pub fn fill_chunk(chunk: &mut StagingChunk, data: &Bytes) -> Result<(), CuObjectError> {
    chunk.append(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_parses_leading_fields_and_rejects_garbage() {
        let token =
            RdmaToken::parse("0000f52eddc00000:00100000:001824d2:0000:002873:1:00000000000000000000ffff0a0d5410").expect("token");
        assert_eq!(token.remote_addr, 0x0000_f52e_ddc0_0000);
        assert_eq!(token.buffer_len, 0x0010_0000);
        assert!(RdmaToken::parse("nothex:00100000:1:2:3:4:5").is_err());
        assert!(RdmaToken::parse("0000f52eddc00000:00000000:1:2:3:4:5").is_err());
        assert!(RdmaToken::parse("0000f52eddc00000:00100000").is_err());
    }
}
