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

//! Bucket engines: the engines this node implements and the engine each
//! bucket is bound to.
//!
//! A bucket's binding is the `engine.json` bucket metadata entry, delivered
//! through the bucket config subscriber registry. Buckets without a binding,
//! or bound to the `object` engine, are served by the object layer. A bucket
//! bound to an engine this node cannot serve (unknown engine, invalid binding,
//! failed attach) is refused instead of falling back to the object layer,
//! whose namespace does not hold that bucket's data. See
//! `docs/architecture/bucket-engines.md`.

mod registry;
mod storage_api;

pub use registry::{BoundBucket, EngineRegistry, EngineRoute};

/// Registers the built-in engines and subscribes the registry to bucket
/// engine bindings. Must run before bucket metadata is loaded so the initial
/// load reaches the registry. Idempotent across embedded and server startups.
pub(crate) fn init_bucket_engine_runtime() -> bool {
    let registry = EngineRegistry::global();
    storage_api::register_bucket_config_subscriber(
        "bucket-engine",
        storage_api::BUCKET_ENGINE_CONFIG,
        Box::new(move |bucket, _config_file, stored| {
            let stored = stored.map(|(bytes, _updated_at, incarnation)| (bytes, incarnation.as_u128()));
            let _ = registry.publish(bucket, stored);
        }),
    )
}
