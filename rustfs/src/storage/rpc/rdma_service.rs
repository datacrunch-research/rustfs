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

//! Disk-side handler and listener sidecar for the internode RDMA data plane.
//!
//! The RDMA server in ECStore authenticates each request before calling into
//! this handler. The handler mirrors the HTTP stream routes: the same disk
//! lookup, background admission, legacy metadata-bucket fallback, and the
//! authenticated put_file commit with its staging file and digest trailer.

use super::http_service::{
    PUT_FILE_AUTH_STRICT, PutFileQuery, put_file_server_epoch_accepted, read_file_stream_with_legacy_meta_fallback,
    write_authenticated_put_file,
};
use crate::server::ShutdownHandle;
use crate::storage::storage_api::rpc_consumer::http_service::{
    PUT_FILE_AUTH_V1, RemoteBackgroundIo, find_local_disk_by_ref_for_rpc, with_remote_background_io,
};
use crate::storage::storage_api::rpc_consumer::rdma_service::{
    Disk, DiskStore, FileReader, PositionalRead, RdmaBodyStream, RdmaErrorCode, RdmaReadRequest, RdmaReject, RdmaRequestHandler,
    RdmaServer, RdmaWriteRequest, default_listen_addrs, get_global_endpoints_opt, global_rustfs_port, server_enabled, settings,
    shared_pool,
};
use async_trait::async_trait;
use std::io;
use std::sync::Arc;
use tracing::info;

const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const EVENT_RDMA_SERVICE_STATE: &str = "rdma_internode_service_state";

/// Serves shard streams from this node's local disks.
#[derive(Default)]
pub(crate) struct RdmaDiskHandler {
    /// Disks resolved without the global registry; tests only.
    #[cfg(test)]
    test_disks: std::collections::HashMap<String, DiskStore>,
}

#[async_trait]
impl RdmaRequestHandler for RdmaDiskHandler {
    async fn read(&self, request: RdmaReadRequest) -> Result<FileReader, RdmaReject> {
        let admission = background_admission(request.background)?;
        let disk = self.lookup_disk(&request.disk).await;
        with_remote_background_io(admission, async move {
            let outcome = open_read(disk, &request).await;
            if outcome.is_err() {
                RemoteBackgroundIo::discard_current_progress();
            }
            outcome
        })
        .await
    }

    async fn read_positional(&self, request: &RdmaReadRequest) -> Result<Option<PositionalRead>, RdmaReject> {
        let admission = background_admission(request.background)?;
        let disk = self.lookup_disk(&request.disk).await;
        with_remote_background_io(admission, async move {
            let outcome = open_positional(disk, request).await;
            if outcome.is_err() {
                RemoteBackgroundIo::discard_current_progress();
            }
            outcome
        })
        .await
    }

    async fn write(&self, request: RdmaWriteRequest, body: RdmaBodyStream) -> Result<u64, RdmaReject> {
        let admission = background_admission(request.background)?;
        let disk = self.lookup_disk(&request.disk).await;
        with_remote_background_io(admission, async move {
            let outcome = commit_write(disk, request, body).await;
            if outcome.is_err() {
                RemoteBackgroundIo::discard_current_progress();
            }
            outcome
        })
        .await
    }
}

/// Background requests need the node's background admission, exactly like the HTTP routes.
fn background_admission(background: bool) -> Result<Option<RemoteBackgroundIo>, RdmaReject> {
    if !background {
        return Ok(None);
    }
    crate::startup_background::background_node_admission()
        .map(|admission| Some(RemoteBackgroundIo::new(admission)))
        .ok_or_else(|| RdmaReject::new(RdmaErrorCode::Busy, "background resource admission is not enabled"))
}

impl RdmaDiskHandler {
    async fn lookup_disk(&self, disk_ref: &str) -> Result<DiskStore, RdmaReject> {
        #[cfg(test)]
        if let Some(disk) = self.test_disks.get(disk_ref) {
            return Ok(disk.clone());
        }
        match find_local_disk_by_ref_for_rpc(disk_ref).await {
            Ok(Some(disk)) => Ok(disk),
            Ok(None) => Err(RdmaReject::new(RdmaErrorCode::DiskNotFound, "disk not found")),
            Err(_) => Err(RdmaReject::new(RdmaErrorCode::Busy, "background storage admission deferred")),
        }
    }
}

async fn open_read(disk: Result<DiskStore, RdmaReject>, request: &RdmaReadRequest) -> Result<FileReader, RdmaReject> {
    let disk = disk?;
    let offset = usize::try_from(request.offset)
        .map_err(|_| RdmaReject::new(RdmaErrorCode::BadRequest, "read offset exceeds the platform limit"))?;
    let length = usize::try_from(request.length)
        .map_err(|_| RdmaReject::new(RdmaErrorCode::BadRequest, "read length exceeds the platform limit"))?;
    read_file_stream_with_legacy_meta_fallback(&disk, &request.volume, &request.path, offset, length)
        .await
        .map_err(|error| RdmaReject::from_disk_error(&error, "read file err"))
}

/// Opens the shard file of a local disk for positional reads. Remote or
/// unusual disks, and files that fail to open, fall back to the streaming
/// read (`None`) so its error handling and legacy-name retry stay in charge.
async fn open_positional(
    disk: Result<DiskStore, RdmaReject>,
    request: &RdmaReadRequest,
) -> Result<Option<PositionalRead>, RdmaReject> {
    let disk = disk?;
    let Disk::Local(local) = disk.as_ref() else {
        return Ok(None);
    };
    let Ok(path) = local.get_object_path_if_local(&request.volume, &request.path) else {
        return Ok(None);
    };
    let offset = request.offset;
    let length = request.length;
    let opened = tokio::task::spawn_blocking(move || -> io::Result<Option<PositionalRead>> {
        let file = std::fs::File::open(&path)?;
        let size = file.metadata()?.len();
        if offset > size {
            return Ok(None);
        }
        let available = size - offset;
        let length = if length == 0 { available } else { length.min(available) };
        Ok(Some(PositionalRead {
            file: Arc::new(file),
            offset,
            length,
        }))
    })
    .await;
    match opened {
        Ok(Ok(read)) => Ok(read),
        Ok(Err(_)) | Err(_) => Ok(None),
    }
}

async fn commit_write(
    disk: Result<DiskStore, RdmaReject>,
    request: RdmaWriteRequest,
    body: RdmaBodyStream,
) -> Result<u64, RdmaReject> {
    let query = PutFileQuery {
        disk: request.disk,
        volume: request.volume,
        path: request.path,
        append: request.append,
        size: request.size,
        put_file_auth: Some(PUT_FILE_AUTH_V1.to_owned()),
        put_file_nonce: Some(request.nonce),
        put_file_server_epoch: request.server_epoch,
    };
    if !put_file_server_epoch_accepted(&query, *PUT_FILE_AUTH_STRICT) {
        return Err(RdmaReject::new(RdmaErrorCode::EpochConflict, "put_file capability server epoch changed"));
    }
    let disk = disk?;
    write_authenticated_put_file(&disk, body, &query, request.nonce, &request.target)
        .await
        .map_err(|(stage, error)| RdmaReject::new(RdmaErrorCode::Internal, format!("{stage} file err {error}")))
}

/// Hosts of this node's own endpoints, which the RDMA listener binds by default.
fn local_endpoint_hosts() -> Vec<String> {
    get_global_endpoints_opt()
        .map(|pools| {
            pools
                .0
                .iter()
                .flat_map(|pool| pool.endpoints.as_ref().iter())
                .filter(|endpoint| endpoint.is_local)
                .filter_map(|endpoint| endpoint.url.host_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// Starts the RDMA listener when `RUSTFS_RDMA_SERVER_ENABLE` (or the selected
/// transport) asks for it. Invalid settings and bind failures fail startup.
pub(crate) async fn init_rdma_internode_service() -> io::Result<Option<ShutdownHandle>> {
    if !server_enabled() {
        return Ok(None);
    }
    let settings = settings()?;
    let pool = shared_pool()?;
    let listen = default_listen_addrs(&local_endpoint_hosts(), global_rustfs_port()).await?;
    if listen.is_empty() {
        info!(
            event = EVENT_RDMA_SERVICE_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "skipped",
            reason = "no_listen_address",
            "rdma internode service not started: no local endpoint host or RUSTFS_RDMA_LISTEN_ADDRS"
        );
        return Ok(None);
    }
    let server = RdmaServer::bind(&listen, settings, pool, Arc::new(RdmaDiskHandler::default()))
        .await
        .map_err(io::Error::other)?;
    info!(
        event = EVENT_RDMA_SERVICE_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "started",
        listener_count = server.local_addrs().len(),
        "rdma internode service started"
    );
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let task = tokio::spawn(async move {
        let _ = shutdown_rx.recv().await;
        server.shutdown().await;
    });
    Ok(Some(ShutdownHandle::new(shutdown_tx, task)))
}

#[cfg(test)]
mod tests {
    //! Loopback tests against a real RDMA device and a temporary local disk.
    //! They run only when `RUSTFS_RDMA_TEST_ADDR` names a local RDMA-capable IP.

    use super::RdmaDiskHandler;
    use crate::storage::storage_api::ecstore_rpc::build_put_file_auth_trailer;
    use crate::storage::storage_api::rpc_consumer::http_service::{
        DiskAPI as _, DiskOption, DiskStore, Endpoint, PUT_FILE_AUTH_TRAILER_LEN, new_disk,
    };
    use crate::storage::storage_api::rpc_consumer::rdma_service::{
        InternodeDataTransport, RdmaBodyStream, RdmaInternodeDataTransport, RdmaReject, RdmaRequestHandler, RdmaServer,
        RdmaSettings, RdmaStreamOp, RdmaWriteRequest, RdmaWriteRequestWire, ReadStreamRequest, RegisteredPool,
    };
    use bytes::Bytes;
    use futures::stream;
    use std::collections::HashMap;
    use std::io;
    use std::net::{IpAddr, SocketAddr};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    const PORT_OFFSET: u16 = 1000;
    const DISK_REF: &str = "rdma-test-disk";

    fn test_addr() -> Option<IpAddr> {
        std::env::var("RUSTFS_RDMA_TEST_ADDR").ok()?.trim().parse().ok()
    }

    fn settings() -> Arc<RdmaSettings> {
        Arc::new(RdmaSettings {
            port_offset: PORT_OFFSET,
            chunk_size: 64 * 1024,
            stream_slots: 2,
            pool_bytes: 32 * 64 * 1024,
            fallback_tcp: false,
            connect_timeout: Duration::from_secs(2),
            stall_timeout: Duration::from_secs(5),
            listen_addrs: Vec::new(),
        })
    }

    async fn test_disk() -> (DiskStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("temp directory should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("temp path should be utf8")).expect("disk endpoint should parse");
        let disk = new_disk(&endpoint, &DiskOption::default())
            .await
            .expect("local disk should be created");
        disk.make_volume("bucket").await.expect("test volume should be created");
        (disk, dir)
    }

    fn handler_for(disk: &DiskStore) -> RdmaDiskHandler {
        RdmaDiskHandler {
            test_disks: HashMap::from([(DISK_REF.to_owned(), disk.clone())]),
        }
    }

    fn write_request(path: &str, size: i64) -> RdmaWriteRequest {
        let nonce = uuid::Uuid::new_v4();
        let wire = RdmaWriteRequestWire {
            disk: DISK_REF.to_owned(),
            volume: "bucket".to_owned(),
            path: path.to_owned(),
            append: false,
            size,
            nonce,
            server_epoch: Some(*super::super::http_service::PUT_FILE_CAPABILITY_SERVER_EPOCH),
        };
        let target = RdmaStreamOp::Write(wire.clone()).canonical_target();
        RdmaWriteRequest {
            disk: wire.disk,
            volume: wire.volume,
            path: wire.path,
            append: wire.append,
            size: wire.size,
            nonce,
            server_epoch: wire.server_epoch,
            background: false,
            target,
        }
    }

    fn body_with_trailer(request: &RdmaWriteRequest, payload: &[u8]) -> RdmaBodyStream {
        let digest = hex_simd::encode_to_string(<sha2::Sha256 as sha2::Digest>::digest(payload), hex_simd::AsciiCase::Lower);
        let trailer =
            build_put_file_auth_trailer(&request.target, &http::Method::PUT, request.nonce, &digest).expect("trailer builds");
        assert_eq!(trailer.len(), PUT_FILE_AUTH_TRAILER_LEN);
        let chunks: Vec<io::Result<Bytes>> = payload
            .chunks(50_000)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .chain(std::iter::once(Ok(Bytes::from(trailer))))
            .collect();
        Box::pin(stream::iter(chunks))
    }

    #[tokio::test]
    async fn rdma_disk_handler_commits_authenticated_writes_to_the_local_disk() {
        let _ = rustfs_credentials::set_global_rpc_secret("rdma-service-test-secret".to_owned());
        let (disk, _dir) = test_disk().await;
        let handler = handler_for(&disk);
        let payload: Vec<u8> = (0..300_000u32).map(|index| (index % 253) as u8).collect();
        let request = write_request("object/part.1", i64::try_from(payload.len()).expect("size fits"));
        let committed = handler
            .write(request.clone(), body_with_trailer(&request, &payload))
            .await
            .expect("authenticated write commits");
        assert_eq!(committed, payload.len() as u64);
        let stored = disk
            .read_all("bucket", "object/part.1")
            .await
            .expect("committed file is readable");
        assert!(stored[..] == payload[..], "stored bytes must match the payload");

        // A tampered body never publishes the target.
        let request = write_request("object/part.2", 5);
        let mut tampered = b"hellx".to_vec();
        let trailer_source = body_with_trailer(&request, b"hello");
        let mut collected = Vec::new();
        let mut trailer_source = trailer_source;
        while let Some(chunk) = futures::StreamExt::next(&mut trailer_source).await {
            collected.extend_from_slice(&chunk.expect("fixture chunk"));
        }
        tampered.extend_from_slice(&collected[5..]);
        let body: RdmaBodyStream = Box::pin(stream::iter(vec![Ok(Bytes::from(tampered))]));
        let reject: RdmaReject = handler.write(request, body).await.expect_err("tampered body is rejected");
        assert!(reject.message.contains("write_body"), "{}", reject.message);
        assert!(disk.read_all("bucket", "object/part.2").await.is_err(), "tampered target must not exist");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rdma_disk_handler_serves_reads_over_a_loopback_listener() {
        let Some(ip) = test_addr() else { return };
        let _ = rustfs_credentials::set_global_rpc_secret("rdma-service-test-secret".to_owned());
        let (disk, _dir) = test_disk().await;
        let payload: Vec<u8> = (0..(2 * 1024 * 1024 + 99) as u32).map(|index| (index % 241) as u8).collect();
        disk.write_all("bucket", "object/part.1", Bytes::from(payload.clone()))
            .await
            .expect("fixture file is written");
        let settings = settings();
        let pool = RegisteredPool::new(settings.pool_bytes, settings.chunk_size).expect("registered pool");
        let http_port = 40_000 + (uuid::Uuid::new_v4().as_u128() % 20_000) as u16;
        let listen = SocketAddr::new(ip, http_port + PORT_OFFSET);
        let server = RdmaServer::bind(&[listen], Arc::clone(&settings), Arc::clone(&pool), Arc::new(handler_for(&disk)))
            .await
            .expect("rdma listener binds");
        let transport = RdmaInternodeDataTransport::new(settings, pool);
        let mut reader = transport
            .open_read_chunks(ReadStreamRequest {
                endpoint: format!("http://{ip}:{http_port}"),
                disk: DISK_REF.to_owned(),
                volume: "bucket".to_owned(),
                path: "object/part.1".to_owned(),
                offset: 4096,
                length: 0,
                stall_timeout: Some(Duration::from_secs(5)),
            })
            .await
            .expect("open rdma read against the local disk")
            .expect("owned-chunk reader");
        let mut body = Vec::new();
        reader.read_to_end(&mut body).await.expect("stream the shard");
        assert!(body[..] == payload[4096..], "shard bytes must match from the requested offset");

        let Err(missing) = transport
            .open_read_chunks(ReadStreamRequest {
                endpoint: format!("http://{ip}:{http_port}"),
                disk: DISK_REF.to_owned(),
                volume: "bucket".to_owned(),
                path: "object/missing".to_owned(),
                offset: 0,
                length: 0,
                stall_timeout: Some(Duration::from_secs(5)),
            })
            .await
        else {
            panic!("missing shard is a typed error");
        };
        assert!(
            matches!(missing, super::super::super::storage_api::DiskError::FileNotFound),
            "got {missing:?}"
        );
        server.shutdown().await;
    }
}
