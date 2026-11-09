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

use crate::name::{EngineName, OperationName};
use thiserror::Error;

pub type EngineResult<T> = Result<T, EngineError>;

/// Errors an engine or the binding contract reports. The dispatch layer maps
/// them to S3 errors; `Unavailable` and `CacheFull` are retryable or skippable
/// by clients, the rest are not.
#[derive(Debug, Error)]
pub enum EngineError {
    #[error("engine {engine} does not serve {operation}")]
    Unsupported { engine: EngineName, operation: String },
    #[error("engine {engine} has no custom operation {operation}")]
    UnknownOperation { engine: EngineName, operation: OperationName },
    #[error("invalid bucket engine binding: {0}")]
    InvalidBinding(String),
    #[error("invalid engine config: {0}")]
    InvalidConfig(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("not found")]
    NotFound,
    #[error("bucket still holds data")]
    BucketNotEmpty,
    #[error("cache full")]
    CacheFull,
    #[error("stale placement view {client}, current is {current}")]
    StalePlacement { client: u64, current: u64 },
    #[error("temporarily unavailable: {0}")]
    Unavailable(String),
    #[error("engine I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("internal engine error: {0}")]
    Internal(String),
}

impl EngineError {
    /// Whether a client may retry the same request.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Unavailable(_) | Self::StalePlacement { .. })
    }
}
