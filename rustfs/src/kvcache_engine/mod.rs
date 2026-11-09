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

//! The `kv-cache` bucket engine: whole entries on one drive of one owner
//! node, chosen by rendezvous hashing over the placement view, stored by
//! `rustfs-kvcache`. Any node accepts any request; a node that does not own
//! a key hands it to the owner through the framework's internode engine
//! call. See `docs/architecture/kv-cache-buckets.md`.

mod batch;
mod config;
mod wire;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use rustfs_bucket_engine::{
    BucketContext, BucketEngine, BucketEngineBinding, BucketFeature, ByteRange, CustomOperationSpec, CustomRequest,
    CustomResponse, EngineCapabilities, EngineDescriptor, EngineError, EngineName, EngineObject, EngineObjectInfo,
    EngineObjectOperation, EngineResult, ListPage, NodeEnvironment, ObjectWriteMeta, OperationAction, OperationName,
    RdmaTransfer,
};
use rustfs_kvcache::{EntryInfo, KvError, KvStore, PutOutcome, StatvfsFs};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::bucket_engine::peer::{PeerCall, call_peer};
use batch::{
    BATCH_READ, BATCH_WRITE, ContainsReply, KeysRequest, MAX_KEYS_REQUEST_BYTES, MAX_READ_RESPONSE_BYTES, MAX_VIEW_REQUEST_BYTES,
    MAX_WRITE_REQUEST_BYTES, PEER_CONTAINS, PEER_GET_BATCH, PEER_PUT_BATCH, PLACEMENT_HASH, PLACEMENT_VIEW, PREFIX_QUERY,
    PeerKeys, PeerWrite, PlacementReply, PrefixQueryReply, ValuesHeader, WriteHeader, WriteItem, WriteReply, WriteResult,
};
use config::{CONFIG_VERSION, KvCacheConfig};
use wire::{EntryHeader, KeyRequest, ListReply, ListRequest, PEER_DELETE, PEER_GET, PEER_HEAD, PEER_LIST, PEER_PUT};

pub const KV_CACHE_ENGINE: &str = "kv-cache";

/// Deadline for a forwarded single-key operation (a 28 MB value on a loaded
/// link stays well inside it).
const PEER_TIMEOUT: Duration = Duration::from_secs(30);

const EVENT_KV_CACHE_MAINTENANCE: &str = "kv_cache_maintenance";
const LOG_COMPONENT_APP: &str = "app";
const LOG_SUBSYSTEM_KV_CACHE: &str = "kv_cache";

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

fn store_error(err: KvError) -> EngineError {
    match err {
        KvError::CacheFull { .. } => EngineError::CacheFull,
        KvError::KeyTooLong { .. } | KvError::ValueTooLarge { .. } => EngineError::InvalidRequest(err.to_string()),
        KvError::Io(err) => EngineError::Io(err),
        other => EngineError::Internal(other.to_string()),
    }
}

fn join_error(err: tokio::task::JoinError) -> EngineError {
    EngineError::Internal(format!("store task failed: {err}"))
}

fn object_info(key: &str, info: &EntryInfo) -> EngineObjectInfo {
    EngineObjectInfo {
        key: key.to_owned(),
        size: info.value_len,
        etag: format!("{:08x}", info.value_crc),
        last_modified_unix_secs: i64::try_from(info.write_time_ms / 1000).unwrap_or(i64::MAX),
    }
}

fn header_of(info: &EngineObjectInfo) -> EntryHeader {
    EntryHeader {
        key: info.key.clone(),
        size: info.size,
        etag: info.etag.clone(),
        last_modified_unix_secs: info.last_modified_unix_secs,
    }
}

fn info_of(header: EntryHeader) -> EngineObjectInfo {
    EngineObjectInfo {
        key: header.key,
        size: header.size,
        etag: header.etag,
        last_modified_unix_secs: header.last_modified_unix_secs,
    }
}

fn operation_spec(name: &str, action: OperationAction, max_request_bytes: u64) -> CustomOperationSpec {
    CustomOperationSpec {
        name: OperationName::new(name).expect("static operation name is valid"),
        action,
        max_request_bytes,
    }
}

fn json_response<T: serde::Serialize>(value: &T) -> EngineResult<CustomResponse> {
    Ok(CustomResponse {
        body: Bytes::from(serde_json::to_vec(value).map_err(|err| EngineError::Internal(err.to_string()))?),
        headers: Vec::new(),
    })
}

fn write_result(result: Result<PutOutcome, KvError>) -> WriteResult {
    match result {
        Ok(PutOutcome::Written) => WriteResult::Written,
        Ok(PutOutcome::Exists) => WriteResult::Exists,
        Err(KvError::CacheFull { .. }) => WriteResult::CacheFull,
        Err(err) => WriteResult::Error(err.to_string()),
    }
}

/// Keys of one batch split by owner: (index, drive) for local keys, and the
/// indices owned by each other node.
struct Grouped {
    local: Vec<(usize, u16)>,
    remote: HashMap<String, Vec<usize>>,
}

/// A bound bucket incarnation on this node.
struct BucketStore {
    store: Arc<KvStore>,
    maintenance: CancellationToken,
}

impl Drop for BucketStore {
    fn drop(&mut self) {
        self.maintenance.cancel();
    }
}

/// Where a key's request is served.
enum Owner {
    Local(u16),
    Remote(String),
}

pub struct KvCacheEngine {
    descriptor: EngineDescriptor,
    environment: Arc<NodeEnvironment>,
    buckets: RwLock<HashMap<u128, Arc<BucketStore>>>,
}

impl KvCacheEngine {
    pub fn new(environment: Arc<NodeEnvironment>) -> Self {
        Self {
            descriptor: EngineDescriptor {
                name: EngineName::new(KV_CACHE_ENGINE).expect("static engine name is valid"),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                config_version: CONFIG_VERSION,
                capabilities: EngineCapabilities {
                    object_operations: [
                        EngineObjectOperation::GetObject,
                        EngineObjectOperation::HeadObject,
                        EngineObjectOperation::PutObject,
                        EngineObjectOperation::DeleteObject,
                        EngineObjectOperation::ListObjects,
                    ]
                    .into_iter()
                    .collect(),
                    custom_operations: vec![
                        operation_spec(PREFIX_QUERY, OperationAction::GetObject, MAX_KEYS_REQUEST_BYTES),
                        operation_spec(BATCH_READ, OperationAction::GetObject, MAX_KEYS_REQUEST_BYTES),
                        operation_spec(BATCH_WRITE, OperationAction::PutObject, MAX_WRITE_REQUEST_BYTES),
                        operation_spec(PLACEMENT_VIEW, OperationAction::GetObject, MAX_VIEW_REQUEST_BYTES),
                    ],
                    consistent_listing: false,
                    refused_features: BucketFeature::ALL.into_iter().collect(),
                },
            },
            environment,
            buckets: RwLock::new(HashMap::new()),
        }
    }

    fn owner(&self, key: &str) -> EngineResult<Owner> {
        let placement = self.environment.placement();
        let target = placement
            .owner(key.as_bytes())
            .ok_or_else(|| EngineError::Unavailable("the placement view has no targets".to_owned()))?;
        if target.node == self.environment.node() {
            Ok(Owner::Local(target.drive))
        } else {
            Ok(Owner::Remote(target.node))
        }
    }

    fn bucket(&self, bucket: &BucketContext) -> EngineResult<Arc<BucketStore>> {
        self.buckets
            .read()
            .get(&bucket.incarnation)
            .cloned()
            .ok_or_else(|| EngineError::Unavailable("kv-cache bucket is not attached on this node".to_owned()))
    }

    async fn forward(&self, bucket: &BucketContext, node: String, operation: &str, body: Bytes) -> EngineResult<Bytes> {
        let call = PeerCall {
            bucket: bucket.bucket.clone(),
            incarnation: bucket.incarnation,
            engine: KV_CACHE_ENGINE.to_owned(),
            operation: operation.to_owned(),
            body,
        };
        call_peer(&node, call, PEER_TIMEOUT).await
    }

    fn key_request(&self, key: &str) -> KeyRequest {
        KeyRequest {
            key: key.to_owned(),
            view: self.environment.placement().version,
        }
    }

    /// A forwarded request is served only by the key's owner under the same
    /// placement view, so a stale sender cannot store keys where no reader
    /// will look for them.
    fn local_drive_for(&self, request: &KeyRequest) -> EngineResult<u16> {
        let current = self.environment.placement().version;
        if request.view != current {
            return Err(EngineError::StalePlacement {
                client: request.view,
                current,
            });
        }
        match self.owner(&request.key)? {
            Owner::Local(drive) => Ok(drive),
            Owner::Remote(_) => Err(EngineError::StalePlacement {
                client: request.view,
                current,
            }),
        }
    }

    async fn local_get(&self, bucket: &BucketContext, key: String) -> EngineResult<(EngineObjectInfo, Bytes)> {
        let state = self.bucket(bucket)?;
        tokio::task::spawn_blocking(move || {
            let info = state.store.head(&key).ok_or(EngineError::NotFound)?;
            let value = state.store.get(&key).map_err(store_error)?.ok_or(EngineError::NotFound)?;
            Ok((object_info(&key, &info), Bytes::copy_from_slice(value.as_slice())))
        })
        .await
        .map_err(join_error)?
    }

    async fn local_put(&self, bucket: &BucketContext, drive: u16, key: String, body: Bytes) -> EngineResult<EngineObjectInfo> {
        let state = self.bucket(bucket)?;
        tokio::task::spawn_blocking(move || {
            match state.store.put(drive, &key, &body, now_ms()).map_err(store_error)? {
                PutOutcome::Written | PutOutcome::Exists => {}
            }
            let info = state.store.head(&key).ok_or(EngineError::NotFound)?;
            Ok(object_info(&key, &info))
        })
        .await
        .map_err(join_error)?
    }

    async fn local_delete(&self, bucket: &BucketContext, key: String) -> EngineResult<()> {
        let state = self.bucket(bucket)?;
        tokio::task::spawn_blocking(move || state.store.delete(&key, now_ms()).map(|_| ()).map_err(store_error))
            .await
            .map_err(join_error)?
    }

    fn local_list(&self, bucket: &BucketContext, request: &ListRequest) -> EngineResult<ListReply> {
        let state = self.bucket(bucket)?;
        let mut entries = state
            .store
            .list(&request.prefix, request.start_after.as_deref(), request.max_keys.saturating_add(1));
        let truncated = entries.len() > request.max_keys;
        entries.truncate(request.max_keys);
        Ok(ListReply {
            entries: entries.iter().map(|(key, info)| header_of(&object_info(key, info))).collect(),
            truncated,
        })
    }

    fn group(&self, keys: &[&str]) -> EngineResult<Grouped> {
        let mut grouped = Grouped {
            local: Vec::new(),
            remote: HashMap::new(),
        };
        for (index, key) in keys.iter().enumerate() {
            match self.owner(key)? {
                Owner::Local(drive) => grouped.local.push((index, drive)),
                Owner::Remote(node) => grouped.remote.entry(node).or_default().push(index),
            }
        }
        Ok(grouped)
    }

    /// Whether each key is stored, asking every owner once in parallel. An
    /// owner that cannot answer reports its keys as missing.
    async fn contains_many(&self, bucket: &BucketContext, keys: &[String]) -> EngineResult<Vec<bool>> {
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let grouped = self.group(&refs)?;
        let mut present = vec![false; keys.len()];
        if !grouped.local.is_empty() {
            let store = self.bucket(bucket)?;
            for (index, _) in &grouped.local {
                present[*index] = store.store.contains(&keys[*index]);
            }
        }
        let view = self.environment.placement().version;
        let replies = futures::future::join_all(grouped.remote.iter().map(|(node, indices)| async move {
            let request = PeerKeys {
                keys: indices.iter().map(|index| keys[*index].clone()).collect(),
                view,
            };
            let reply = self
                .forward(bucket, node.clone(), PEER_CONTAINS, wire::encode(&request, &[])?)
                .await?;
            Ok::<_, EngineError>((indices, wire::decode::<ContainsReply>(reply)?.0))
        }))
        .await;
        for reply in replies {
            // An unreachable or stale owner is a miss for its keys.
            if let Ok((indices, reply)) = reply
                && reply.present.len() == indices.len()
            {
                for (index, found) in indices.iter().zip(reply.present) {
                    present[*index] = found;
                }
            }
        }
        Ok(present)
    }

    /// Reads every key from its owner in parallel; a miss, an unreachable
    /// owner or a value past the response budget is `None`.
    async fn read_many(&self, bucket: &BucketContext, keys: &[String]) -> EngineResult<Vec<Option<Bytes>>> {
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let grouped = self.group(&refs)?;
        let mut values: Vec<Option<Bytes>> = vec![None; keys.len()];
        let local_keys: Vec<(usize, String)> = grouped
            .local
            .iter()
            .map(|(index, _)| (*index, keys[*index].clone()))
            .collect();
        let local = async {
            if local_keys.is_empty() {
                return Ok(Vec::new());
            }
            let state = self.bucket(bucket)?;
            tokio::task::spawn_blocking(move || {
                local_keys
                    .into_iter()
                    .map(|(index, key)| {
                        let value = state.store.get(&key).ok().flatten();
                        (index, value.map(|value| Bytes::copy_from_slice(value.as_slice())))
                    })
                    .collect::<Vec<_>>()
            })
            .await
            .map_err(join_error)
        };
        let view = self.environment.placement().version;
        let remote = futures::future::join_all(grouped.remote.iter().map(|(node, indices)| async move {
            let request = PeerKeys {
                keys: indices.iter().map(|index| keys[*index].clone()).collect(),
                view,
            };
            let reply = self
                .forward(bucket, node.clone(), PEER_GET_BATCH, wire::encode(&request, &[])?)
                .await?;
            let (header, payload): (ValuesHeader, Bytes) = wire::decode(reply)?;
            Ok::<_, EngineError>((indices, batch::unjoin_values(&header, &payload)?))
        }));
        let (local, remote) = tokio::join!(local, remote);
        for (index, value) in local? {
            values[index] = value;
        }
        for reply in remote.into_iter().flatten() {
            let (indices, read) = reply;
            if read.len() == indices.len() {
                for (index, value) in indices.iter().zip(read) {
                    values[*index] = value;
                }
            }
        }
        // Keep the response bounded: values past the budget become misses
        // the client fetches in another request.
        let mut budget = MAX_READ_RESPONSE_BYTES;
        for value in &mut values {
            if let Some(bytes) = value {
                if bytes.len() as u64 > budget {
                    *value = None;
                } else {
                    budget -= bytes.len() as u64;
                }
            }
        }
        Ok(values)
    }

    /// Writes every value to its owner in parallel; each item gets its own
    /// result, and a failed owner fails only its items.
    async fn write_many(
        &self,
        bucket: &BucketContext,
        items: Vec<WriteItem>,
        values: Vec<Bytes>,
    ) -> EngineResult<Vec<WriteResult>> {
        let refs: Vec<&str> = items.iter().map(|item| item.key.as_str()).collect();
        let grouped = self.group(&refs)?;
        let mut results = vec![WriteResult::Error("not attempted".to_owned()); items.len()];
        let local_items: Vec<(usize, u16, String, Bytes)> = grouped
            .local
            .iter()
            .map(|(index, drive)| (*index, *drive, items[*index].key.clone(), values[*index].clone()))
            .collect();
        let local = async {
            if local_items.is_empty() {
                return Ok(Vec::new());
            }
            let state = self.bucket(bucket)?;
            tokio::task::spawn_blocking(move || {
                local_items
                    .into_iter()
                    .map(|(index, drive, key, value)| (index, write_result(state.store.put(drive, &key, &value, now_ms()))))
                    .collect::<Vec<_>>()
            })
            .await
            .map_err(join_error)
        };
        let view = self.environment.placement().version;
        let (items_ref, values_ref) = (&items, &values);
        let remote = futures::future::join_all(grouped.remote.iter().map(|(node, indices)| async move {
            let request = PeerWrite {
                items: indices.iter().map(|index| items_ref[*index].clone()).collect(),
                view,
            };
            let mut payload = bytes::BytesMut::new();
            for index in indices {
                payload.extend_from_slice(&values_ref[*index]);
            }
            let outcome = async {
                let reply = self
                    .forward(bucket, node.clone(), PEER_PUT_BATCH, wire::encode(&request, &payload)?)
                    .await?;
                Ok::<_, EngineError>(wire::decode::<WriteReply>(reply)?.0.results)
            }
            .await;
            (indices, node, outcome)
        }));
        let (local, remote) = tokio::join!(local, remote);
        for (index, result) in local? {
            results[index] = result;
        }
        for (indices, node, outcome) in remote {
            match outcome {
                Ok(owner_results) if owner_results.len() == indices.len() => {
                    for (index, result) in indices.iter().zip(owner_results) {
                        results[*index] = result;
                    }
                }
                Ok(_) => {
                    for index in indices {
                        results[*index] = WriteResult::Error(format!("owner {node} answered a different batch"));
                    }
                }
                Err(err) => {
                    for index in indices {
                        results[*index] = WriteResult::Error(format!("owner {node}: {err}"));
                    }
                }
            }
        }
        Ok(results)
    }

    /// Refuses a client request routed with an older placement view, so the
    /// client refreshes the view and routes again.
    fn check_client_view(&self, view: Option<u64>) -> EngineResult<()> {
        let current = self.environment.placement().version;
        match view {
            Some(view) if view != current => Err(EngineError::StalePlacement { client: view, current }),
            _ => Ok(()),
        }
    }

    /// Checks a forwarded batch's view and returns, for each key, the local
    /// drive when this node owns it.
    fn owned_drives(&self, keys: &[&str], view: u64) -> EngineResult<Vec<Option<u16>>> {
        let current = self.environment.placement().version;
        if view != current {
            return Err(EngineError::StalePlacement { client: view, current });
        }
        keys.iter()
            .map(|key| {
                self.owner(key).map(|owner| match owner {
                    Owner::Local(drive) => Some(drive),
                    Owner::Remote(_) => None,
                })
            })
            .collect()
    }

    /// Starts the capacity task of a bucket store: evict and replenish spare
    /// segments every interval, until detach or purge cancels it.
    fn spawn_maintenance(&self, bucket: &BucketContext, store: Arc<KvStore>, config: &KvCacheConfig) -> CancellationToken {
        let token = CancellationToken::new();
        let cancelled = token.clone();
        let policy = config.policy();
        let spares = config.spare_segments;
        let interval = Duration::from_secs(config.maintenance_interval_secs);
        let bucket_name = bucket.bucket.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = cancelled.cancelled() => break,
                    _ = ticker.tick() => {}
                }
                let store = Arc::clone(&store);
                let pass = tokio::task::spawn_blocking(move || {
                    let report = store.evict(&StatvfsFs, &policy, now_ms())?;
                    store.replenish_spares(spares)?;
                    Ok::<_, KvError>(report)
                })
                .await;
                match pass {
                    Ok(Ok(report)) if report.aged_segments + report.capacity_segments > 0 || !report.full_drives.is_empty() => {
                        debug!(
                            event = EVENT_KV_CACHE_MAINTENANCE,
                            component = LOG_COMPONENT_APP,
                            subsystem = LOG_SUBSYSTEM_KV_CACHE,
                            result = "evicted",
                            bucket = %bucket_name,
                            aged_segments = report.aged_segments,
                            capacity_segments = report.capacity_segments,
                            keys_removed = report.keys_removed,
                            full_drives = report.full_drives.len(),
                            "KV cache capacity pass"
                        );
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => warn!(
                        event = EVENT_KV_CACHE_MAINTENANCE,
                        component = LOG_COMPONENT_APP,
                        subsystem = LOG_SUBSYSTEM_KV_CACHE,
                        result = "failed",
                        bucket = %bucket_name,
                        error = %err,
                        "KV cache capacity pass failed"
                    ),
                    Err(err) => warn!(
                        event = EVENT_KV_CACHE_MAINTENANCE,
                        component = LOG_COMPONENT_APP,
                        subsystem = LOG_SUBSYSTEM_KV_CACHE,
                        result = "panicked",
                        bucket = %bucket_name,
                        error = %err,
                        "KV cache capacity pass panicked"
                    ),
                }
            }
        });
        token
    }
}

#[async_trait]
impl BucketEngine for KvCacheEngine {
    fn descriptor(&self) -> &EngineDescriptor {
        &self.descriptor
    }

    fn validate_config(&self, binding: &BucketEngineBinding) -> EngineResult<()> {
        KvCacheConfig::from_binding(binding).map(|_| ())
    }

    async fn bind(&self, _bucket: &BucketContext) -> EngineResult<()> {
        Ok(())
    }

    async fn attach(&self, bucket: &BucketContext) -> EngineResult<()> {
        let config = KvCacheConfig::from_binding(&bucket.binding)?;
        if self.buckets.read().contains_key(&bucket.incarnation) {
            return Ok(());
        }
        let dirs = self.environment.engine_dirs(&bucket.binding.engine, bucket.incarnation);
        let options = config.store_options();
        let (store, _) = tokio::task::spawn_blocking(move || KvStore::open(options, dirs, now_ms()))
            .await
            .map_err(join_error)?
            .map_err(store_error)?;
        let store = Arc::new(store);
        let maintenance = self.spawn_maintenance(bucket, Arc::clone(&store), &config);
        self.buckets
            .write()
            .insert(bucket.incarnation, Arc::new(BucketStore { store, maintenance }));
        Ok(())
    }

    async fn reconfigure(&self, bucket: &BucketContext, _previous: &BucketEngineBinding) -> EngineResult<()> {
        // Capacity settings take effect on the next attach; segment layout
        // changes apply to new segments only.
        KvCacheConfig::from_binding(&bucket.binding).map(|_| ())
    }

    async fn detach(&self, bucket: &BucketContext) -> EngineResult<()> {
        let Some(state) = self.buckets.write().remove(&bucket.incarnation) else {
            return Ok(());
        };
        state.maintenance.cancel();
        tokio::task::spawn_blocking(move || state.store.seal_open_segments(now_ms()))
            .await
            .map_err(join_error)?
            .map_err(store_error)
    }

    async fn holds_data(&self, bucket: &BucketContext) -> EngineResult<bool> {
        Ok(self
            .buckets
            .read()
            .get(&bucket.incarnation)
            .is_some_and(|state| !state.store.is_empty()))
    }

    async fn purge(&self, bucket: &BucketContext) -> EngineResult<()> {
        // Dropping the store closes its files; the framework removes the
        // engine directories afterwards.
        self.buckets.write().remove(&bucket.incarnation);
        Ok(())
    }

    async fn status(&self, bucket: &BucketContext) -> EngineResult<serde_json::Value> {
        let state = self.bucket(bucket)?;
        let store = &state.store;
        let drives: Vec<serde_json::Value> = (0..store.drive_count() as u16)
            .map(|drive| {
                let segments = store.segments(drive);
                serde_json::json!({
                    "drive": drive,
                    "segments": segments.len(),
                    "live_bytes": segments.iter().map(|segment| segment.live_bytes).sum::<u64>(),
                    "full": store.is_full(drive),
                })
            })
            .collect();
        Ok(serde_json::json!({
            "node": self.environment.node(),
            "placement_version": self.environment.placement().version,
            "entries": store.len(),
            "drives": drives,
        }))
    }

    async fn get_object(
        &self,
        bucket: &BucketContext,
        key: &str,
        _range: Option<ByteRange>,
        _rdma: Option<RdmaTransfer>,
    ) -> EngineResult<EngineObject> {
        let (info, body) = match self.owner(key)? {
            Owner::Local(_) => self.local_get(bucket, key.to_owned()).await?,
            Owner::Remote(node) => {
                let reply = self
                    .forward(bucket, node, PEER_GET, wire::encode(&self.key_request(key), &[])?)
                    .await?;
                let (header, body): (EntryHeader, Bytes) = wire::decode(reply)?;
                (info_of(header), body)
            }
        };
        Ok(EngineObject {
            info,
            body,
            rdma_bytes: None,
        })
    }

    async fn head_object(&self, bucket: &BucketContext, key: &str) -> EngineResult<EngineObjectInfo> {
        match self.owner(key)? {
            Owner::Local(_) => {
                let state = self.bucket(bucket)?;
                state
                    .store
                    .head(key)
                    .map(|info| object_info(key, &info))
                    .ok_or(EngineError::NotFound)
            }
            Owner::Remote(node) => {
                let reply = self
                    .forward(bucket, node, PEER_HEAD, wire::encode(&self.key_request(key), &[])?)
                    .await?;
                Ok(info_of(wire::decode::<EntryHeader>(reply)?.0))
            }
        }
    }

    async fn put_object(
        &self,
        bucket: &BucketContext,
        key: &str,
        body: Bytes,
        _meta: ObjectWriteMeta,
    ) -> EngineResult<EngineObjectInfo> {
        match self.owner(key)? {
            Owner::Local(drive) => self.local_put(bucket, drive, key.to_owned(), body).await,
            Owner::Remote(node) => {
                let reply = self
                    .forward(bucket, node, PEER_PUT, wire::encode(&self.key_request(key), &body)?)
                    .await?;
                Ok(info_of(wire::decode::<EntryHeader>(reply)?.0))
            }
        }
    }

    async fn delete_object(&self, bucket: &BucketContext, key: &str) -> EngineResult<()> {
        match self.owner(key)? {
            Owner::Local(_) => self.local_delete(bucket, key.to_owned()).await,
            Owner::Remote(node) => self
                .forward(bucket, node, PEER_DELETE, wire::encode(&self.key_request(key), &[])?)
                .await
                .map(|_| ()),
        }
    }

    /// Best effort: every node lists its own keys and the pages are merged.
    async fn list_objects(
        &self,
        bucket: &BucketContext,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> EngineResult<ListPage> {
        let request = ListRequest {
            prefix: prefix.to_owned(),
            start_after: start_after.map(str::to_owned),
            max_keys,
        };
        let local = self.local_list(bucket, &request)?;
        let mut entries = local.entries;
        let mut truncated = local.truncated;
        let peers = self.environment.placement();
        let message = wire::encode(&request, &[])?;
        let others: Vec<String> = peers
            .nodes
            .iter()
            .map(|node| node.node.clone())
            .filter(|node| node != self.environment.node())
            .collect();
        let replies = futures::future::join_all(
            others
                .into_iter()
                .map(|node| self.forward(bucket, node, PEER_LIST, message.clone())),
        )
        .await;
        for reply in replies {
            let (reply, _): (ListReply, Bytes) = wire::decode(reply?)?;
            truncated |= reply.truncated;
            entries.extend(reply.entries);
        }
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        entries.dedup_by(|left, right| left.key == right.key);
        if entries.len() > max_keys {
            truncated = true;
            entries.truncate(max_keys);
        }
        Ok(ListPage {
            next_start_after: if truncated {
                entries.last().map(|entry| entry.key.clone())
            } else {
                None
            },
            objects: entries.into_iter().map(info_of).collect(),
        })
    }

    async fn custom_operation(&self, bucket: &BucketContext, request: CustomRequest) -> EngineResult<CustomResponse> {
        match request.operation.as_str() {
            PREFIX_QUERY => {
                let keys: KeysRequest =
                    serde_json::from_slice(&request.body).map_err(|err| EngineError::InvalidRequest(err.to_string()))?;
                keys.validate()?;
                self.check_client_view(keys.view)?;
                let present = self.contains_many(bucket, &keys.keys).await?;
                let hits = batch::longest_prefix(&present);
                let mut response = json_response(&PrefixQueryReply { hits })?;
                response.headers.push(("x-rustfs-engine-hits".to_owned(), hits.to_string()));
                Ok(response)
            }
            BATCH_READ => {
                let keys: KeysRequest =
                    serde_json::from_slice(&request.body).map_err(|err| EngineError::InvalidRequest(err.to_string()))?;
                keys.validate()?;
                self.check_client_view(keys.view)?;
                let values = self.read_many(bucket, &keys.keys).await?;
                let (header, payload) = batch::join_values(&values);
                Ok(CustomResponse {
                    body: wire::encode(&header, &payload)?,
                    headers: Vec::new(),
                })
            }
            BATCH_WRITE => {
                let (header, payload): (WriteHeader, Bytes) = wire::decode(request.body)?;
                self.check_client_view(header.view)?;
                let values = batch::split_values(&header.items, &payload)?;
                let results = self.write_many(bucket, header.items, values).await?;
                json_response(&WriteReply { results })
            }
            PLACEMENT_VIEW => {
                let placement = self.environment.placement();
                json_response(&PlacementReply {
                    version: placement.version,
                    hash: PLACEMENT_HASH.to_owned(),
                    replicas: 1,
                    nodes: placement.nodes.clone(),
                })
            }
            _ => Err(EngineError::UnknownOperation {
                engine: self.descriptor.name.clone(),
                operation: request.operation,
            }),
        }
    }

    async fn handle_peer_call(&self, bucket: &BucketContext, operation: &OperationName, body: Bytes) -> EngineResult<Bytes> {
        match operation.as_str() {
            PEER_CONTAINS => {
                let (request, _): (PeerKeys, Bytes) = wire::decode(body)?;
                let keys: Vec<&str> = request.keys.iter().map(String::as_str).collect();
                let drives = self.owned_drives(&keys, request.view)?;
                let state = self.bucket(bucket)?;
                let present = keys
                    .iter()
                    .zip(drives)
                    .map(|(key, drive)| drive.is_some() && state.store.contains(key))
                    .collect();
                wire::encode(&ContainsReply { present }, &[])
            }
            PEER_GET_BATCH => {
                let (request, _): (PeerKeys, Bytes) = wire::decode(body)?;
                let keys: Vec<&str> = request.keys.iter().map(String::as_str).collect();
                let drives = self.owned_drives(&keys, request.view)?;
                let state = self.bucket(bucket)?;
                let owned: Vec<Option<String>> = request
                    .keys
                    .into_iter()
                    .zip(drives)
                    .map(|(key, drive)| drive.map(|_| key))
                    .collect();
                let values = tokio::task::spawn_blocking(move || {
                    owned
                        .into_iter()
                        .map(|key| {
                            key.and_then(|key| state.store.get(&key).ok().flatten())
                                .map(|value| Bytes::copy_from_slice(value.as_slice()))
                        })
                        .collect::<Vec<_>>()
                })
                .await
                .map_err(join_error)?;
                let (header, payload) = batch::join_values(&values);
                wire::encode(&header, &payload)
            }
            PEER_PUT_BATCH => {
                let (request, payload): (PeerWrite, Bytes) = wire::decode(body)?;
                let values = batch::split_values(&request.items, &payload)?;
                let keys: Vec<&str> = request.items.iter().map(|item| item.key.as_str()).collect();
                let drives = self.owned_drives(&keys, request.view)?;
                let state = self.bucket(bucket)?;
                let items = request.items;
                let results = tokio::task::spawn_blocking(move || {
                    items
                        .iter()
                        .zip(values)
                        .zip(drives)
                        .map(|((item, value), drive)| match drive {
                            Some(drive) => write_result(state.store.put(drive, &item.key, &value, now_ms())),
                            None => WriteResult::Error("this node does not own the key".to_owned()),
                        })
                        .collect::<Vec<_>>()
                })
                .await
                .map_err(join_error)?;
                wire::encode(&WriteReply { results }, &[])
            }
            PEER_LIST => {
                let (request, _): (ListRequest, Bytes) = wire::decode(body)?;
                wire::encode(&self.local_list(bucket, &request)?, &[])
            }
            PEER_GET => {
                let (request, _): (KeyRequest, Bytes) = wire::decode(body)?;
                self.local_drive_for(&request)?;
                let (info, value) = self.local_get(bucket, request.key).await?;
                wire::encode(&header_of(&info), &value)
            }
            PEER_HEAD => {
                let (request, _): (KeyRequest, Bytes) = wire::decode(body)?;
                self.local_drive_for(&request)?;
                let info = self.bucket(bucket)?.store.head(&request.key).ok_or(EngineError::NotFound)?;
                wire::encode(&header_of(&object_info(&request.key, &info)), &[])
            }
            PEER_PUT => {
                let (request, value): (KeyRequest, Bytes) = wire::decode(body)?;
                let drive = self.local_drive_for(&request)?;
                let info = self.local_put(bucket, drive, request.key, value).await?;
                wire::encode(&header_of(&info), &[])
            }
            PEER_DELETE => {
                let (request, _): (KeyRequest, Bytes) = wire::decode(body)?;
                self.local_drive_for(&request)?;
                self.local_delete(bucket, request.key).await?;
                Ok(Bytes::new())
            }
            _ => Err(EngineError::UnknownOperation {
                engine: self.descriptor.name.clone(),
                operation: operation.clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_bucket_engine::{NodeTargets, PlacementView};
    use std::path::Path;

    fn environment(root: &Path, nodes: &[&str]) -> Arc<NodeEnvironment> {
        let drives = (0..2).map(|i| root.join(format!("d{i}"))).collect();
        let view = PlacementView::from_nodes(
            nodes
                .iter()
                .map(|node| NodeTargets {
                    node: (*node).to_owned(),
                    drives: 2,
                    online: true,
                })
                .collect(),
        );
        Arc::new(NodeEnvironment::new(nodes[0].to_owned(), drives, view))
    }

    fn bucket(config: serde_json::Value) -> BucketContext {
        BucketContext {
            bucket: "kv".to_owned(),
            incarnation: 7,
            binding: BucketEngineBinding {
                engine: EngineName::new(KV_CACHE_ENGINE).expect("valid engine name"),
                config_version: CONFIG_VERSION,
                config,
            },
        }
    }

    fn small_segments() -> serde_json::Value {
        // Buffered I/O: test temp directories may not support O_DIRECT.
        serde_json::json!({ "segment_mib": 16, "direct_io": false, "spare_segments": 0 })
    }

    fn key_owned_by(engine: &KvCacheEngine, local: bool) -> String {
        (0..10_000)
            .map(|i| format!("hicache/{i:08x}"))
            .find(|key| matches!(engine.owner(key), Ok(Owner::Local(_))) == local)
            .expect("some key routes each way")
    }

    #[tokio::test]
    async fn single_node_bucket_serves_object_operations_and_survives_reattach() {
        let root = tempfile::tempdir().expect("tempdir");
        let engine = KvCacheEngine::new(environment(root.path(), &["a:9000"]));
        let bucket = bucket(small_segments());
        engine.validate_config(&bucket.binding).expect("valid config");
        engine.attach(&bucket).await.expect("attach");
        assert!(!engine.holds_data(&bucket).await.expect("holds data"));

        let value = Bytes::from(vec![7u8; 900_000]);
        let put = engine
            .put_object(&bucket, "page-1", value.clone(), ObjectWriteMeta::default())
            .await
            .expect("put");
        assert_eq!(put.size, 900_000);
        let again = engine
            .put_object(&bucket, "page-1", Bytes::from_static(b"other"), ObjectWriteMeta::default())
            .await
            .expect("idempotent put");
        assert_eq!(again.etag, put.etag, "an existing key keeps its first value");
        engine
            .put_object(&bucket, "page-2", Bytes::from_static(b"x"), ObjectWriteMeta::default())
            .await
            .expect("put");

        let got = engine.get_object(&bucket, "page-1", None, None).await.expect("get");
        assert_eq!(got.body, value);
        assert_eq!(engine.head_object(&bucket, "page-2").await.expect("head").size, 1);
        let listed = engine.list_objects(&bucket, "page-", None, 1).await.expect("list");
        assert_eq!(listed.objects.len(), 1);
        assert_eq!(listed.next_start_after.as_deref(), Some("page-1"));
        assert!(engine.holds_data(&bucket).await.expect("holds data"));

        engine.delete_object(&bucket, "page-2").await.expect("delete");
        assert!(matches!(engine.head_object(&bucket, "page-2").await, Err(EngineError::NotFound)));

        // Detach seals; a new attach rebuilds the index from the segments.
        engine.detach(&bucket).await.expect("detach");
        assert!(matches!(
            engine.get_object(&bucket, "page-1", None, None).await,
            Err(EngineError::Unavailable(_))
        ));
        engine.attach(&bucket).await.expect("reattach");
        assert_eq!(engine.get_object(&bucket, "page-1", None, None).await.expect("get").body, value);
        let status = engine.status(&bucket).await.expect("status");
        assert_eq!(status["entries"], 1);

        engine.purge(&bucket).await.expect("purge");
        assert!(!engine.holds_data(&bucket).await.expect("holds data"));
    }

    #[tokio::test]
    async fn keys_owned_elsewhere_are_forwarded_never_stored_locally() {
        let root = tempfile::tempdir().expect("tempdir");
        let engine = KvCacheEngine::new(environment(root.path(), &["a:9000", "b:9000"]));
        let bucket = bucket(small_segments());
        engine.attach(&bucket).await.expect("attach");

        let remote = key_owned_by(&engine, false);
        // No peers in a unit test: the forward fails and nothing is stored here.
        let result = engine
            .put_object(&bucket, &remote, Bytes::from_static(b"v"), ObjectWriteMeta::default())
            .await;
        assert!(matches!(result, Err(EngineError::Unavailable(_))), "{result:?}");
        assert!(!engine.holds_data(&bucket).await.expect("holds data"));

        let local = key_owned_by(&engine, true);
        engine
            .put_object(&bucket, &local, Bytes::from_static(b"v"), ObjectWriteMeta::default())
            .await
            .expect("local put");
        assert!(engine.holds_data(&bucket).await.expect("holds data"));
    }

    fn custom(name: &str, body: Bytes) -> CustomRequest {
        CustomRequest {
            operation: OperationName::new(name).expect("valid operation name"),
            body,
            rdma: None,
        }
    }

    #[tokio::test]
    async fn batch_operations_query_read_and_write_in_prefix_order() {
        let root = tempfile::tempdir().expect("tempdir");
        let engine = KvCacheEngine::new(environment(root.path(), &["a:9000"]));
        let bucket = bucket(small_segments());
        engine.attach(&bucket).await.expect("attach");
        assert_eq!(engine.descriptor().capabilities.custom_operations.len(), 4);

        let keys: Vec<String> = (0..5).map(|i| format!("prompt/page-{i}")).collect();
        let items: Vec<WriteItem> = keys[..3]
            .iter()
            .enumerate()
            .map(|(i, key)| WriteItem {
                key: key.clone(),
                size: 10 * (i as u64 + 1),
            })
            .collect();
        let payload: Vec<u8> = items
            .iter()
            .flat_map(|item| vec![item.size as u8; item.size as usize])
            .collect();
        let write = engine
            .custom_operation(
                &bucket,
                custom(
                    BATCH_WRITE,
                    wire::encode(
                        &WriteHeader {
                            items: items.clone(),
                            view: None,
                        },
                        &payload,
                    )
                    .expect("encode"),
                ),
            )
            .await
            .expect("batch write");
        let reply: WriteReply = serde_json::from_slice(&write.body).expect("json");
        assert_eq!(reply.results, vec![WriteResult::Written; 3]);
        let again = engine
            .custom_operation(
                &bucket,
                custom(BATCH_WRITE, wire::encode(&WriteHeader { items, view: None }, &payload).expect("encode")),
            )
            .await
            .expect("batch write");
        let reply: WriteReply = serde_json::from_slice(&again.body).expect("json");
        assert_eq!(reply.results, vec![WriteResult::Exists; 3]);

        let query = serde_json::to_vec(&KeysRequest {
            keys: keys.clone(),
            view: None,
        })
        .expect("json");
        let response = engine
            .custom_operation(&bucket, custom(PREFIX_QUERY, Bytes::from(query.clone())))
            .await
            .expect("prefix query");
        let reply: PrefixQueryReply = serde_json::from_slice(&response.body).expect("json");
        assert_eq!(reply.hits, 3);
        assert_eq!(response.headers, vec![("x-rustfs-engine-hits".to_owned(), "3".to_owned())]);

        let response = engine
            .custom_operation(&bucket, custom(BATCH_READ, Bytes::from(query)))
            .await
            .expect("batch read");
        let (header, payload): (ValuesHeader, Bytes) = wire::decode(response.body).expect("decode");
        assert_eq!(header.sizes, [Some(10), Some(20), Some(30), None, None]);
        let values = batch::unjoin_values(&header, &payload).expect("values");
        assert_eq!(values[1].as_deref(), Some(&[20u8; 20][..]));

        let short = wire::encode(
            &WriteHeader {
                items: vec![WriteItem {
                    key: "k".to_owned(),
                    size: 99,
                }],
                view: None,
            },
            b"x",
        )
        .expect("encode");
        assert!(engine.custom_operation(&bucket, custom(BATCH_WRITE, short)).await.is_err());
    }

    #[tokio::test]
    async fn owners_answer_forwarded_batches_and_skip_keys_they_do_not_own() {
        let root = tempfile::tempdir().expect("tempdir");
        let engine = KvCacheEngine::new(environment(root.path(), &["a:9000", "b:9000"]));
        let bucket = bucket(small_segments());
        engine.attach(&bucket).await.expect("attach");
        let view = engine.environment.placement().version;
        let op = |name: &str| OperationName::new(name).expect("valid operation name");
        let ours = key_owned_by(&engine, true);
        let theirs = key_owned_by(&engine, false);

        let write = PeerWrite {
            items: vec![
                WriteItem {
                    key: ours.clone(),
                    size: 2,
                },
                WriteItem {
                    key: theirs.clone(),
                    size: 1,
                },
            ],
            view,
        };
        let reply = engine
            .handle_peer_call(&bucket, &op(PEER_PUT_BATCH), wire::encode(&write, b"abc").expect("encode"))
            .await
            .expect("peer put batch");
        let (reply, _): (WriteReply, Bytes) = wire::decode(reply).expect("decode");
        assert_eq!(reply.results[0], WriteResult::Written);
        assert!(matches!(reply.results[1], WriteResult::Error(_)));

        let keys = PeerKeys {
            keys: vec![ours.clone(), theirs.clone()],
            view,
        };
        let reply = engine
            .handle_peer_call(&bucket, &op(PEER_CONTAINS), wire::encode(&keys, &[]).expect("encode"))
            .await
            .expect("peer contains");
        let (contains, _): (ContainsReply, Bytes) = wire::decode(reply).expect("decode");
        assert_eq!(contains.present, [true, false]);

        let reply = engine
            .handle_peer_call(&bucket, &op(PEER_GET_BATCH), wire::encode(&keys, &[]).expect("encode"))
            .await
            .expect("peer get batch");
        let (header, payload): (ValuesHeader, Bytes) = wire::decode(reply).expect("decode");
        assert_eq!(header.sizes, [Some(2), None]);
        assert_eq!(&payload[..], b"ab");

        let stale = PeerKeys {
            keys: vec![ours],
            view: view + 1,
        };
        assert!(matches!(
            engine
                .handle_peer_call(&bucket, &op(PEER_CONTAINS), wire::encode(&stale, &[]).expect("encode"))
                .await,
            Err(EngineError::StalePlacement { .. })
        ));

        // From the client side, keys of an unreachable owner are misses.
        let query = serde_json::to_vec(&KeysRequest {
            keys: vec![theirs],
            view: None,
        })
        .expect("json");
        let response = engine
            .custom_operation(&bucket, custom(PREFIX_QUERY, Bytes::from(query)))
            .await
            .expect("prefix query");
        let reply: PrefixQueryReply = serde_json::from_slice(&response.body).expect("json");
        assert_eq!(reply.hits, 0);
    }

    #[tokio::test]
    async fn clients_get_the_placement_view_and_stale_views_are_refused() {
        let root = tempfile::tempdir().expect("tempdir");
        let engine = KvCacheEngine::new(environment(root.path(), &["a:9000", "b:9000"]));
        let bucket = bucket(small_segments());
        engine.attach(&bucket).await.expect("attach");

        let response = engine
            .custom_operation(&bucket, custom(PLACEMENT_VIEW, Bytes::new()))
            .await
            .expect("placement view");
        let view: PlacementReply = serde_json::from_slice(&response.body).expect("json");
        assert_eq!(view.version, engine.environment.placement().version);
        assert_eq!(view.hash, PLACEMENT_HASH);
        assert_eq!(view.nodes.len(), 2);
        // A client computing owners from the reply agrees with the engine.
        let routed = PlacementView {
            version: view.version,
            nodes: view.nodes.clone(),
        };
        let key = key_owned_by(&engine, true);
        assert_eq!(routed.owner(key.as_bytes()).expect("owner").node, "a:9000");

        let stale = serde_json::to_vec(&KeysRequest {
            keys: vec![key.clone()],
            view: Some(view.version + 1),
        })
        .expect("json");
        assert!(matches!(
            engine
                .custom_operation(&bucket, custom(PREFIX_QUERY, Bytes::from(stale)))
                .await,
            Err(EngineError::StalePlacement { .. })
        ));
        let current = serde_json::to_vec(&KeysRequest {
            keys: vec![key],
            view: Some(view.version),
        })
        .expect("json");
        engine
            .custom_operation(&bucket, custom(PREFIX_QUERY, Bytes::from(current)))
            .await
            .expect("current view");
    }

    #[tokio::test]
    async fn forwarded_requests_are_served_only_by_the_owner_under_the_same_view() {
        let root = tempfile::tempdir().expect("tempdir");
        let engine = KvCacheEngine::new(environment(root.path(), &["a:9000", "b:9000"]));
        let bucket = bucket(small_segments());
        engine.attach(&bucket).await.expect("attach");
        let view = engine.environment.placement().version;
        let op = |name: &str| OperationName::new(name).expect("valid operation name");

        let local = key_owned_by(&engine, true);
        let request = KeyRequest {
            key: local.clone(),
            view,
        };
        engine
            .handle_peer_call(&bucket, &op(PEER_PUT), wire::encode(&request, b"payload").expect("encode"))
            .await
            .expect("owner accepts the write");
        let reply = engine
            .handle_peer_call(&bucket, &op(PEER_GET), wire::encode(&request, &[]).expect("encode"))
            .await
            .expect("owner serves the read");
        let (header, body): (EntryHeader, Bytes) = wire::decode(reply).expect("decode");
        assert_eq!((header.key.as_str(), &body[..]), (local.as_str(), &b"payload"[..]));

        let stale = KeyRequest {
            key: local.clone(),
            view: view + 1,
        };
        let err = engine
            .handle_peer_call(&bucket, &op(PEER_GET), wire::encode(&stale, &[]).expect("encode"))
            .await
            .expect_err("stale view");
        assert!(matches!(err, EngineError::StalePlacement { .. }));

        let not_ours = KeyRequest {
            key: key_owned_by(&engine, false),
            view,
        };
        let err = engine
            .handle_peer_call(&bucket, &op(PEER_PUT), wire::encode(&not_ours, b"x").expect("encode"))
            .await
            .expect_err("not the owner");
        assert!(matches!(err, EngineError::StalePlacement { .. }));

        let err = engine
            .handle_peer_call(&bucket, &op("peer-reboot"), Bytes::new())
            .await
            .expect_err("unknown operation");
        assert!(matches!(err, EngineError::UnknownOperation { .. }));
    }
}
