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

use super::proto::ErrorCode;
use crate::disk::error::DiskError;
use std::io;
use std::time::Duration;
use thiserror::Error;

/// Failure of the internode RDMA transport.
///
/// Setup failures (see [`RdmaError::is_setup_failure`]) happen before any
/// payload byte moved and are the only failures the transport may retry over
/// `tcp-http`. Every other variant maps to the same disk error class the HTTP
/// stream backend produces, so quorum and disk-health handling are unchanged.
#[derive(Debug, Error)]
pub enum RdmaError {
    #[error("rdma {op} failed: {source}")]
    Os {
        op: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("rdma connect to {peer} failed: {source}")]
    Connect {
        peer: String,
        #[source]
        source: io::Error,
    },
    #[error("rdma connect to {peer} timed out after {timeout:?}")]
    ConnectTimeout { peer: String, timeout: Duration },
    #[error("rdma peer {peer} rejected the connection")]
    Rejected { peer: String },
    #[error("rdma connection to {peer} was lost")]
    Disconnected { peer: String },
    #[error("rdma work request failed: {status} (kind {kind}, vendor error {vendor_err})")]
    WorkCompletion { status: String, kind: u8, vendor_err: u32 },
    #[error("rdma registered buffer pool exhausted")]
    PoolExhausted,
    #[error("rdma protocol violation: {0}")]
    Protocol(String),
    #[error("rdma message encoding failed: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("rdma message decoding failed: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("rdma peer reported {code:?}: {message}")]
    Remote { code: ErrorCode, message: String },
    #[error("rdma stream source failed: {0}")]
    Source(#[source] io::Error),
    #[error("rdma stream stalled for {0:?}")]
    Stalled(Duration),
    #[error("rdma stream consumer went away")]
    ConsumerGone,
    #[error("rdma endpoint {0:?} is not usable")]
    Endpoint(String),
    #[error("rdma address for {endpoint} did not resolve: {source}")]
    Resolve {
        endpoint: String,
        #[source]
        source: io::Error,
    },
    #[error("rdma internode authentication failed: {0}")]
    Auth(#[source] io::Error),
    #[error("rdma transport unsupported by peer: {0}")]
    Unsupported(String),
}

impl RdmaError {
    /// True when the failure happened before any payload byte moved, so the
    /// same request can be issued again over the TCP backend.
    pub fn is_setup_failure(&self) -> bool {
        match self {
            Self::Connect { .. }
            | Self::ConnectTimeout { .. }
            | Self::Rejected { .. }
            | Self::PoolExhausted
            | Self::Resolve { .. }
            | Self::Endpoint(_)
            | Self::Unsupported(_) => true,
            Self::Remote { code, .. } => matches!(code, ErrorCode::Unsupported | ErrorCode::Busy),
            _ => false,
        }
    }

    /// Low-cardinality label for metrics.
    pub fn classification(&self) -> &'static str {
        match self {
            Self::Os { .. } => "os",
            Self::Connect { .. } => "connect_failed",
            Self::ConnectTimeout { .. } => "connect_timeout",
            Self::Rejected { .. } => "connection_refused",
            Self::Disconnected { .. } => "connection_reset",
            Self::WorkCompletion { .. } => "work_completion",
            Self::PoolExhausted => "pool_exhausted",
            Self::Protocol(_) | Self::Encode(_) | Self::Decode(_) => "protocol",
            Self::Remote { .. } => "remote_error",
            Self::Source(_) => "source",
            Self::Stalled(_) => "stall_timeout",
            Self::ConsumerGone => "consumer_gone",
            Self::Endpoint(_) | Self::Resolve { .. } => "resolve",
            Self::Auth(_) => "auth",
            Self::Unsupported(_) => "unsupported",
        }
    }

    /// Converts into the disk error the HTTP stream backend would have produced.
    pub fn into_disk_error(self) -> DiskError {
        match self {
            Self::Remote { code, message } => match code.to_disk_error() {
                Some(disk_error) => disk_error,
                None => DiskError::Io(io::Error::other(Self::Remote { code, message })),
            },
            Self::Stalled(timeout) => DiskError::Io(io::Error::new(io::ErrorKind::TimedOut, rustfs_rio::BodyStalled { timeout })),
            Self::Auth(source) => DiskError::Io(io::Error::new(io::ErrorKind::PermissionDenied, source)),
            other => DiskError::Io(other.into_io_error()),
        }
    }

    /// Converts into an I/O error that `DiskError::from` classifies like the
    /// HTTP backend's errors: typed remote disk errors survive the round trip
    /// and connection losses stay retryable.
    pub fn into_io_error(self) -> io::Error {
        match self {
            Self::Remote { code, message } => match code.to_disk_error() {
                Some(disk_error) => io::Error::new(io::ErrorKind::NotFound, disk_error),
                None => io::Error::other(Self::Remote { code, message }),
            },
            Self::Stalled(timeout) => io::Error::new(io::ErrorKind::TimedOut, rustfs_rio::BodyStalled { timeout }),
            Self::Disconnected { .. } | Self::WorkCompletion { .. } => io::Error::new(io::ErrorKind::ConnectionReset, self),
            Self::ConsumerGone => io::Error::new(io::ErrorKind::BrokenPipe, self),
            Self::ConnectTimeout { .. } => io::Error::new(io::ErrorKind::TimedOut, self),
            Self::Rejected { .. } => io::Error::new(io::ErrorKind::ConnectionRefused, self),
            Self::Auth(_) => io::Error::new(io::ErrorKind::PermissionDenied, self),
            other => io::Error::other(other),
        }
    }
}
