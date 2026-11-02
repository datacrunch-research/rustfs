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

//! Loopback tests against a real RDMA device.
//!
//! They run only when `RUSTFS_RDMA_TEST_ADDR` names a local RDMA-capable IP
//! address (for RoCE, the address of the RoCE netdev); otherwise each test
//! returns early so hosts and CI runners without RDMA stay green.

use super::RdmaSettings;
use super::buffer::RegisteredPool;
use super::client::RdmaInternodeDataTransport;
use super::proto::ErrorCode;
use super::server::{RdmaBodyStream, RdmaReadRequest, RdmaReject, RdmaRequestHandler, RdmaServer, RdmaWriteRequest};
use crate::cluster::rpc::http_auth::verify_put_file_auth_trailer;
use crate::cluster::rpc::internode_data_transport::{
    InternodeDataTransport, PUT_FILE_V1_CAPABILITY_TTL, PutFileCapabilityState, ReadStreamRequest, WriteStreamRequest,
    put_file_capability_cache_entry,
};
use crate::disk::FileReader;
use crate::disk::error::DiskError;
use crate::storage_api_contracts::internode::PUT_FILE_AUTH_TRAILER_LEN;
use async_trait::async_trait;
use futures::TryStreamExt;
use http::Method;
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

const PORT_OFFSET: u16 = 1000;

fn test_addr() -> Option<IpAddr> {
    let raw = std::env::var("RUSTFS_RDMA_TEST_ADDR").ok()?;
    match raw.trim().parse() {
        Ok(addr) => Some(addr),
        Err(_) => {
            eprintln!("RUSTFS_RDMA_TEST_ADDR={raw:?} is not an IP address; skipping rdma loopback test");
            None
        }
    }
}

fn settings() -> Arc<RdmaSettings> {
    Arc::new(RdmaSettings {
        port_offset: PORT_OFFSET,
        chunk_size: 64 * 1024,
        stream_slots: 3,
        pool_bytes: 64 * 64 * 1024,
        fallback_tcp: true,
        connect_timeout: Duration::from_secs(2),
        stall_timeout: Duration::from_secs(5),
        listen_addrs: Vec::new(),
    })
}

#[derive(Default)]
struct MemoryHandler {
    files: parking_lot::Mutex<HashMap<String, Vec<u8>>>,
    writes: parking_lot::Mutex<HashMap<String, (RdmaWriteRequest, Vec<u8>)>>,
}

#[async_trait]
impl RdmaRequestHandler for MemoryHandler {
    async fn read(&self, request: RdmaReadRequest) -> Result<FileReader, RdmaReject> {
        let data = self
            .files
            .lock()
            .get(&request.path)
            .cloned()
            .ok_or_else(|| RdmaReject::from_disk_error(&DiskError::FileNotFound, "read file err"))?;
        let start = usize::try_from(request.offset).unwrap_or(usize::MAX).min(data.len());
        let end = if request.length == 0 {
            data.len()
        } else {
            start
                .saturating_add(usize::try_from(request.length).unwrap_or(usize::MAX))
                .min(data.len())
        };
        Ok(Box::new(io::Cursor::new(data[start..end].to_vec())))
    }

    async fn write(&self, request: RdmaWriteRequest, mut body: RdmaBodyStream) -> Result<u64, RdmaReject> {
        if request.path.ends_with("rejected") {
            // Drain nothing: the server must still answer with a typed error.
            return Err(RdmaReject::new(ErrorCode::Internal, "rejected by handler"));
        }
        let mut received = Vec::new();
        while let Some(chunk) = body
            .try_next()
            .await
            .map_err(|error| RdmaReject::new(ErrorCode::Internal, error.to_string()))?
        {
            received.extend_from_slice(&chunk);
        }
        let payload = received.len().saturating_sub(PUT_FILE_AUTH_TRAILER_LEN) as u64;
        self.writes.lock().insert(request.path.clone(), (request, received));
        Ok(payload)
    }
}

struct Fixture {
    endpoint: String,
    _server: RdmaServer,
    handler: Arc<MemoryHandler>,
    transport: RdmaInternodeDataTransport,
}

async fn fixture(ip: IpAddr) -> Fixture {
    crate::runtime::sources::ensure_test_rpc_secret();
    let settings = settings();
    let pool = RegisteredPool::new(settings.pool_bytes, settings.chunk_size).expect("registered pool");
    let handler = Arc::new(MemoryHandler::default());
    let mut attempt = 0u16;
    let (server, http_port) = loop {
        let http_port = 20_000 + (Uuid::new_v4().as_u128() % 20_000) as u16;
        let listen = SocketAddr::new(ip, http_port + PORT_OFFSET);
        match RdmaServer::bind(&[listen], Arc::clone(&settings), Arc::clone(&pool), handler.clone()).await {
            Ok(server) => break (server, http_port),
            Err(error) if attempt < 5 => {
                attempt += 1;
                eprintln!("rdma listener bind on {listen} failed ({error}); retrying");
            }
            Err(error) => panic!("rdma listener bind failed: {error}"),
        }
    };
    let endpoint = format!("http://{ip}:{http_port}");
    put_file_capability_cache_entry(&endpoint).write().cached = Some(PutFileCapabilityState::V1 {
        server_epoch: Uuid::new_v4(),
        revalidate_after: Instant::now() + PUT_FILE_V1_CAPABILITY_TTL,
    });
    Fixture {
        endpoint,
        _server: server,
        handler,
        transport: RdmaInternodeDataTransport::new(settings, pool),
    }
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8 ^ (index / 7919) as u8).collect()
}

fn read_request(fixture: &Fixture, path: &str, offset: usize, length: usize) -> ReadStreamRequest {
    ReadStreamRequest {
        endpoint: fixture.endpoint.clone(),
        disk: "disk-a".to_owned(),
        volume: "bucket".to_owned(),
        path: path.to_owned(),
        offset,
        length,
        stall_timeout: Some(Duration::from_secs(5)),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rdma_read_round_trips_whole_and_ranged_streams() {
    let Some(ip) = test_addr() else { return };
    let fixture = fixture(ip).await;
    let data = pattern(3 * 1024 * 1024 + 4321);
    fixture.handler.files.lock().insert("object/part.1".to_owned(), data.clone());

    let mut reader = fixture
        .transport
        .open_read_chunks(read_request(&fixture, "object/part.1", 0, 0))
        .await
        .expect("open rdma read")
        .expect("rdma transport returns an owned-chunk reader");
    let mut body = Vec::new();
    reader.read_to_end(&mut body).await.expect("read whole stream");
    assert_eq!(body.len(), data.len());
    assert!(body == data, "whole stream bytes must match");

    let mut ranged = fixture
        .transport
        .open_read(read_request(&fixture, "object/part.1", 100_000, 70_001))
        .await
        .expect("open ranged rdma read");
    let mut body = Vec::new();
    ranged.read_to_end(&mut body).await.expect("read ranged stream");
    assert!(body == data[100_000..170_001], "ranged bytes must match");

    // The idle connection is reused for a second stream on the same peer.
    let mut again = fixture
        .transport
        .open_read_chunks(read_request(&fixture, "object/part.1", 0, 1))
        .await
        .expect("open second rdma read")
        .expect("reader");
    let mut body = Vec::new();
    again.read_to_end(&mut body).await.expect("read one byte");
    assert_eq!(body, data[..1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rdma_read_reports_typed_remote_disk_errors_at_open() {
    let Some(ip) = test_addr() else { return };
    let fixture = fixture(ip).await;
    let Err(error) = fixture
        .transport
        .open_read_chunks(read_request(&fixture, "missing/part.1", 0, 0))
        .await
    else {
        panic!("missing file must fail at open");
    };
    assert!(matches!(error, DiskError::FileNotFound), "got {error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rdma_write_delivers_payload_and_signed_trailer() {
    let Some(ip) = test_addr() else { return };
    let fixture = fixture(ip).await;
    let payload = pattern(700 * 1024 + 17);
    let request = WriteStreamRequest {
        endpoint: fixture.endpoint.clone(),
        disk: "disk-a".to_owned(),
        volume: "bucket".to_owned(),
        path: "object/part.2".to_owned(),
        append: false,
        size: i64::try_from(payload.len()).expect("size fits"),
    };
    let mut writer = fixture.transport.open_write(request).await.expect("open rdma write");
    for chunk in payload.chunks(100_000) {
        writer.write_all(chunk).await.expect("queue chunk");
    }
    writer.shutdown().await.expect("commit over rdma");

    let (request, received) = fixture
        .handler
        .writes
        .lock()
        .remove("object/part.2")
        .expect("handler received the write");
    assert_eq!(received.len(), payload.len() + PUT_FILE_AUTH_TRAILER_LEN);
    assert!(received[..payload.len()] == payload[..], "payload bytes must match");
    let digest = hex_simd::encode_to_string(<sha2::Sha256 as sha2::Digest>::digest(&payload[..]), hex_simd::AsciiCase::Lower);
    let signed = verify_put_file_auth_trailer(&request.target, &Method::PUT, request.nonce, &received[payload.len()..])
        .expect("trailer verifies against the canonical target");
    assert_eq!(signed, digest);
    assert!(request.server_epoch.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rdma_write_surfaces_handler_rejection_on_shutdown() {
    let Some(ip) = test_addr() else { return };
    let fixture = fixture(ip).await;
    let request = WriteStreamRequest {
        endpoint: fixture.endpoint.clone(),
        disk: "disk-a".to_owned(),
        volume: "bucket".to_owned(),
        path: "object/rejected".to_owned(),
        append: false,
        size: 5,
    };
    let mut writer = fixture.transport.open_write(request).await.expect("open rdma write");
    let _ = writer.write_all(b"hello").await;
    let error = writer.shutdown().await.expect_err("rejected commit must fail");
    assert!(error.to_string().contains("rejected by handler"), "got {error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rdma_setup_failure_falls_back_to_tcp_and_backs_off_the_peer() {
    let Some(ip) = test_addr() else { return };
    crate::runtime::sources::ensure_test_rpc_secret();
    let settings = settings();
    let pool = RegisteredPool::new(settings.pool_bytes, settings.chunk_size).expect("registered pool");
    let transport = RdmaInternodeDataTransport::new(settings, pool);
    // Nothing listens on this port over RDMA or HTTP.
    let endpoint = format!("http://{ip}:1");
    let request = ReadStreamRequest {
        endpoint: endpoint.clone(),
        disk: "disk-a".to_owned(),
        volume: "bucket".to_owned(),
        path: "object/part.1".to_owned(),
        offset: 0,
        length: 0,
        stall_timeout: Some(Duration::from_secs(5)),
    };
    let started = Instant::now();
    let Err(error) = transport.open_read_chunks(request.clone()).await else {
        panic!("no peer listens anywhere");
    };
    // The fallback reissued the request over HTTP, whose failure is what the caller sees.
    assert!(error.internode_http_error_kind().is_some(), "got {error:?}");
    assert!(
        transport.peer_backing_off(&endpoint),
        "peer must be in backoff after an rdma setup failure"
    );
    let Err(error) = transport.open_read_chunks(request).await else {
        panic!("backed-off peer goes straight to tcp");
    };
    assert!(error.internode_http_error_kind().is_some(), "got {error:?}");
    assert!(started.elapsed() < Duration::from_secs(10), "fallback must not pay a second rdma timeout");
}
