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

//! `memory` engine: a node-local, non-durable engine that keeps objects in
//! process memory. It exercises the framework end to end (binding, dispatch,
//! listing, holds-data and purge) and is the reference engine for the
//! dispatch-overhead benchmark. A multi-node deployment sees a different
//! namespace on every node; use it on a single node only.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use rustfs_bucket_engine::{
    BucketContext, BucketEngine, BucketEngineBinding, BucketFeature, ByteRange, EngineCapabilities, EngineDescriptor,
    EngineError, EngineName, EngineObject, EngineObjectInfo, EngineObjectOperation, EngineResult, ListPage, ObjectWriteMeta,
    RdmaTransfer,
};
use time::OffsetDateTime;

pub const MEMORY_ENGINE: &str = "memory";

struct StoredObject {
    body: Bytes,
    info: EngineObjectInfo,
}

pub struct MemoryEngine {
    descriptor: EngineDescriptor,
    /// Objects by bucket incarnation, then key.
    buckets: RwLock<HashMap<u128, BTreeMap<String, StoredObject>>>,
}

impl Default for MemoryEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryEngine {
    pub fn new() -> Self {
        Self {
            descriptor: EngineDescriptor {
                name: EngineName::new(MEMORY_ENGINE).expect("static engine name is valid"),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                config_version: 1,
                capabilities: EngineCapabilities {
                    object_operations: BTreeSet::from([
                        EngineObjectOperation::GetObject,
                        EngineObjectOperation::HeadObject,
                        EngineObjectOperation::PutObject,
                        EngineObjectOperation::DeleteObject,
                        EngineObjectOperation::ListObjects,
                    ]),
                    custom_operations: Vec::new(),
                    consistent_listing: true,
                    refused_features: BucketFeature::ALL
                        .into_iter()
                        .filter(|f| *f != BucketFeature::Notifications)
                        .collect(),
                },
            },
            buckets: RwLock::default(),
        }
    }
}

#[async_trait]
impl BucketEngine for MemoryEngine {
    fn descriptor(&self) -> &EngineDescriptor {
        &self.descriptor
    }

    fn validate_config(&self, binding: &BucketEngineBinding) -> EngineResult<()> {
        match binding.config.as_object() {
            Some(config) if !config.is_empty() => Err(EngineError::InvalidConfig("the memory engine takes no config".to_owned())),
            _ => Ok(()),
        }
    }

    async fn bind(&self, _bucket: &BucketContext) -> EngineResult<()> {
        Ok(())
    }

    async fn attach(&self, bucket: &BucketContext) -> EngineResult<()> {
        self.buckets.write().entry(bucket.incarnation).or_default();
        Ok(())
    }

    async fn reconfigure(&self, _bucket: &BucketContext, _previous: &BucketEngineBinding) -> EngineResult<()> {
        Ok(())
    }

    async fn holds_data(&self, bucket: &BucketContext) -> EngineResult<bool> {
        Ok(self
            .buckets
            .read()
            .get(&bucket.incarnation)
            .is_some_and(|objects| !objects.is_empty()))
    }

    async fn purge(&self, bucket: &BucketContext) -> EngineResult<()> {
        self.buckets.write().remove(&bucket.incarnation);
        Ok(())
    }

    async fn get_object(
        &self,
        bucket: &BucketContext,
        key: &str,
        range: Option<ByteRange>,
        _rdma: Option<RdmaTransfer>,
    ) -> EngineResult<EngineObject> {
        let buckets = self.buckets.read();
        let object = buckets
            .get(&bucket.incarnation)
            .and_then(|objects| objects.get(key))
            .ok_or(EngineError::NotFound)?;
        let body = match range {
            None => object.body.clone(),
            Some(range) => {
                let start = usize::try_from(range.start).unwrap_or(usize::MAX).min(object.body.len());
                let end = start
                    .saturating_add(usize::try_from(range.length).unwrap_or(usize::MAX))
                    .min(object.body.len());
                object.body.slice(start..end)
            }
        };
        Ok(EngineObject {
            info: object.info.clone(),
            body,
            rdma_bytes: None,
        })
    }

    async fn head_object(&self, bucket: &BucketContext, key: &str) -> EngineResult<EngineObjectInfo> {
        self.buckets
            .read()
            .get(&bucket.incarnation)
            .and_then(|objects| objects.get(key))
            .map(|object| object.info.clone())
            .ok_or(EngineError::NotFound)
    }

    async fn put_object(
        &self,
        bucket: &BucketContext,
        key: &str,
        body: Bytes,
        _meta: ObjectWriteMeta,
    ) -> EngineResult<EngineObjectInfo> {
        let info = EngineObjectInfo {
            key: key.to_owned(),
            size: body.len() as u64,
            etag: format!("{:032x}", xxhash_rust::xxh3::xxh3_128(&body)),
            last_modified_unix_secs: OffsetDateTime::now_utc().unix_timestamp(),
        };
        let mut buckets = self.buckets.write();
        let objects = buckets
            .get_mut(&bucket.incarnation)
            .ok_or_else(|| EngineError::Unavailable("bucket is not attached".to_owned()))?;
        objects.insert(
            key.to_owned(),
            StoredObject {
                body,
                info: info.clone(),
            },
        );
        Ok(info)
    }

    async fn delete_object(&self, bucket: &BucketContext, key: &str) -> EngineResult<()> {
        self.buckets
            .write()
            .get_mut(&bucket.incarnation)
            .and_then(|objects| objects.remove(key))
            .map(|_| ())
            .ok_or(EngineError::NotFound)
    }

    async fn list_objects(
        &self,
        bucket: &BucketContext,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> EngineResult<ListPage> {
        let buckets = self.buckets.read();
        let Some(objects) = buckets.get(&bucket.incarnation) else {
            return Ok(ListPage::default());
        };
        let lower = match start_after {
            Some(after) if after >= prefix => std::ops::Bound::Excluded(after.to_owned()),
            _ => std::ops::Bound::Included(prefix.to_owned()),
        };
        let mut matching = objects
            .range((lower, std::ops::Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(_, object)| object.info.clone());
        let objects: Vec<_> = matching.by_ref().take(max_keys).collect();
        let next_start_after = if matching.next().is_some() {
            objects.last().map(|info| info.key.clone())
        } else {
            None
        };
        Ok(ListPage {
            objects,
            next_start_after,
        })
    }
}
