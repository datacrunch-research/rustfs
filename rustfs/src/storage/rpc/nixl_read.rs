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

//! Serving side of internode NIXL chunk reads.
//!
//! A peer asks for one range of a shard file on a local disk and names a
//! buffer registered with its NIXL agent (signed query); this node reads the
//! range into its own registered staging memory and writes it into that buffer
//! with a NIXL WRITE before answering with the byte count. The request's
//! signature is checked by the internode dispatcher before it reaches here.

use crate::app::object::cuobject::nixl_server;
use crate::storage::storage_api::rpc_consumer::nixl_read::{
    Disk, NIXL_BYTES_HEADER, NIXL_METADATA_HEADER, find_local_disk_by_ref_for_rpc,
};
use http::{HeaderValue, Request, Response, StatusCode};
use hyper::body::Incoming;
use rustfs_nixl::RdmaToken;
use s3s::Body;
use sha2::{Digest, Sha256};
use std::time::Duration;
use tracing::warn;

const EVENT_NIXL_INTERNODE_READ: &str = "nixl_internode_read";
const LOG_COMPONENT_INTERNODE_RPC: &str = "internode_rpc";
const LOG_SUBSYSTEM_NIXL: &str = "nixl";
/// How long a request waits for a free staging chunk before it is refused.
const STAGING_WAIT: Duration = Duration::from_secs(30);

#[derive(Debug, serde::Deserialize)]
struct NixlReadQuery {
    disk: String,
    volume: String,
    path: String,
    offset: u64,
    length: usize,
    nixl_agent: String,
    nixl_buffer: String,
    nixl_md: String,
}

fn reply(status: StatusCode, message: impl Into<String>) -> Response<Body> {
    let mut response = Response::new(Body::from(message.into()));
    *response.status_mut() = status;
    response
}

fn refuse(status: StatusCode, reason: &'static str, detail: impl std::fmt::Display) -> Response<Body> {
    warn!(
        event = EVENT_NIXL_INTERNODE_READ,
        component = LOG_COMPONENT_INTERNODE_RPC,
        subsystem = LOG_SUBSYSTEM_NIXL,
        result = "refused",
        status_code = status.as_u16(),
        reason,
        detail = %detail,
        "nixl internode read refused"
    );
    reply(status, reason)
}

/// Lowercase hex SHA-256 of `bytes`.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(Sha256::digest(bytes), hex_simd::AsciiCase::Lower)
}

pub(crate) async fn handle_read_file_nixl(req: Request<Incoming>) -> Response<Body> {
    let query: NixlReadQuery = match serde_urlencoded::from_str(req.uri().query().unwrap_or_default()) {
        Ok(query) => query,
        Err(error) => return refuse(StatusCode::BAD_REQUEST, "bad_query", error),
    };
    let Some(server) = nixl_server() else {
        return reply(StatusCode::SERVICE_UNAVAILABLE, "nixl transport is not running on this node");
    };

    // The metadata travels unsigned in a header; it must match the signed digest,
    // so a replayed request cannot point the transfer at another endpoint.
    let metadata = match req.headers().get(NIXL_METADATA_HEADER).map(HeaderValue::to_str) {
        Some(Ok(value)) => Some(value),
        Some(Err(_)) => return refuse(StatusCode::BAD_REQUEST, "bad_metadata_header", "not ascii"),
        None => None,
    };
    if let Some(encoded) = metadata {
        let raw = match base64_simd::STANDARD.decode_to_vec(encoded) {
            Ok(raw) => raw,
            Err(error) => return refuse(StatusCode::BAD_REQUEST, "bad_metadata_encoding", error),
        };
        if sha256_hex(&raw) != query.nixl_md.to_ascii_lowercase() {
            return refuse(StatusCode::FORBIDDEN, "metadata_digest_mismatch", &query.nixl_agent);
        }
    }
    let token = match RdmaToken::from_headers(&query.nixl_agent, metadata, &query.nixl_buffer) {
        Ok(token) => token,
        Err(error) => return refuse(StatusCode::BAD_REQUEST, "bad_nixl_token", error),
    };
    if query.length == 0 || query.length as u64 > token.buffer_len || query.length > server.chunk_bytes() {
        return refuse(StatusCode::BAD_REQUEST, "bad_length", query.length);
    }

    let disk = match find_local_disk_by_ref_for_rpc(&query.disk).await {
        Ok(Some(disk)) => disk,
        Ok(None) => return refuse(StatusCode::BAD_REQUEST, "disk_not_found", &query.disk),
        Err(_) => return reply(StatusCode::SERVICE_UNAVAILABLE, "background storage admission deferred"),
    };
    let Disk::Local(local) = disk.as_ref() else {
        return refuse(StatusCode::BAD_REQUEST, "disk_not_local", &query.disk);
    };
    let path = match local.get_object_path_if_local(&query.volume, &query.path) {
        Ok(path) => path,
        Err(error) => return refuse(StatusCode::BAD_REQUEST, "bad_path", error),
    };

    let chunk = match server.reserve(STAGING_WAIT).await {
        Ok(chunk) => chunk,
        Err(error) => return refuse(StatusCode::SERVICE_UNAVAILABLE, "staging_exhausted", error),
    };
    let (offset, length, key) = (query.offset, query.length, query.path.clone());
    let read = tokio::task::spawn_blocking(move || -> std::io::Result<rustfs_nixl::StagingChunk> {
        use std::os::unix::fs::FileExt;
        let mut chunk = chunk;
        let file = super::open_shard_contained(&path, &key)?;
        let spare = chunk.spare_mut().map_err(std::io::Error::other)?;
        let dst = &mut spare[..length];
        let mut filled = 0;
        while filled < dst.len() {
            let n = file.read_at(&mut dst[filled..], offset + filled as u64)?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        chunk.advance(filled).map_err(std::io::Error::other)?;
        Ok(chunk)
    })
    .await;
    let chunk = match read {
        Ok(Ok(chunk)) => chunk,
        Ok(Err(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return reply(StatusCode::NOT_FOUND, "file not found");
        }
        Ok(Err(error)) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            return refuse(StatusCode::FORBIDDEN, "path_outside_volume", error);
        }
        Ok(Err(error)) => return refuse(StatusCode::INTERNAL_SERVER_ERROR, "read_failed", error),
        Err(error) => return refuse(StatusCode::INTERNAL_SERVER_ERROR, "read_task_failed", error),
    };

    let landed = chunk.len();
    if landed > 0
        && let Err(error) = server.write_to_client_owned(&query.path, &token, 0, chunk).await
    {
        return refuse(StatusCode::BAD_GATEWAY, "nixl_write_failed", error);
    }
    let mut response = reply(StatusCode::OK, "");
    response
        .headers_mut()
        .insert(NIXL_BYTES_HEADER, HeaderValue::from(landed as u64));
    response
}
