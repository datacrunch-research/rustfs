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

//! Wire protocol of the internode RDMA data plane.
//!
//! Control messages are msgpack-encoded [`Message`] values carried in RDMA
//! `SEND` work requests of at most [`MESSAGE_BYTES`]. Payload bytes never
//! travel in messages: the data owner RDMA-writes them into slots of a window
//! the receiver advertised, then sends a [`Message::Data`] notice. Because a
//! reliable connection delivers the notice after the write it follows, the
//! receiver may read the slot as soon as the notice arrives. The receiver
//! returns the slot with [`Message::Credit`] once it copied the bytes out.
//!
//! Requests are authenticated with the same HMAC scheme as the HTTP stream
//! routes over a canonical request target that is distinct from the HTTP
//! paths, so a signature captured on one transport is not valid on the other.

use super::error::RdmaError;
use crate::cluster::rpc::http_auth::{
    RPC_BACKGROUND_ADMISSION_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER, gen_signature_headers,
};
use crate::disk::error::DiskError;
use http::{HeaderMap, HeaderValue, Method};
use serde::{Deserialize, Serialize};
use std::io;
use uuid::Uuid;

pub const RDMA_PROTOCOL_VERSION: u16 = 1;
/// Maximum encoded size of one control message.
pub const MESSAGE_BYTES: usize = 4096;
pub const RDMA_READ_FILE_STREAM_PATH: &str = "/rustfs/rpc/rdma/read_file_stream";
pub const RDMA_PUT_FILE_STREAM_PATH: &str = "/rustfs/rpc/rdma/put_file_stream";

/// Signature material for one request, carried in the request message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Auth {
    pub signature: String,
    pub timestamp: i64,
    pub background: bool,
}

impl Auth {
    pub fn sign(target: &str, method: &Method) -> Result<Self, RdmaError> {
        let headers = gen_signature_headers(target, method).map_err(RdmaError::Auth)?;
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
                .ok_or_else(|| RdmaError::Protocol(format!("rpc signature header {name} missing")))
        };
        Ok(Self {
            signature: header(SIGNATURE_HEADER)?,
            timestamp: header(TIMESTAMP_HEADER)?
                .parse()
                .map_err(|_| RdmaError::Protocol("invalid rpc timestamp".to_owned()))?,
            background: headers.contains_key(RPC_BACKGROUND_ADMISSION_HEADER),
        })
    }

    /// Rebuilds the header map the HTTP verification helpers expect.
    pub fn to_headers(&self) -> io::Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(
            SIGNATURE_HEADER,
            HeaderValue::from_str(&self.signature).map_err(|_| io::Error::other("invalid rpc signature"))?,
        );
        headers.insert(
            TIMESTAMP_HEADER,
            HeaderValue::from_str(&self.timestamp.to_string()).map_err(|_| io::Error::other("invalid rpc timestamp"))?,
        );
        if self.background {
            headers.insert(RPC_BACKGROUND_ADMISSION_HEADER, HeaderValue::from_static("v1"));
        }
        Ok(headers)
    }
}

/// Registered slots a peer may RDMA-write into for one stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowDescriptor {
    pub rkey: u32,
    pub slot_size: u32,
    pub addrs: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadRequest {
    pub disk: String,
    pub volume: String,
    pub path: String,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRequest {
    pub disk: String,
    pub volume: String,
    pub path: String,
    pub append: bool,
    pub size: i64,
    pub nonce: Uuid,
    pub server_epoch: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamOp {
    Read(ReadRequest),
    Write(WriteRequest),
}

impl StreamOp {
    pub fn method(&self) -> Method {
        match self {
            Self::Read(_) => Method::GET,
            Self::Write(_) => Method::PUT,
        }
    }

    pub fn path(&self) -> &'static str {
        match self {
            Self::Read(_) => RDMA_READ_FILE_STREAM_PATH,
            Self::Write(_) => RDMA_PUT_FILE_STREAM_PATH,
        }
    }

    /// Canonical request target both peers sign and verify.
    pub fn canonical_target(&self) -> String {
        match self {
            Self::Read(request) => format!(
                "{}?disk={}&volume={}&path={}&offset={}&length={}",
                RDMA_READ_FILE_STREAM_PATH,
                urlencoding::encode(&request.disk),
                urlencoding::encode(&request.volume),
                urlencoding::encode(&request.path),
                request.offset,
                request.length
            ),
            Self::Write(request) => {
                let mut target = format!(
                    "{}?disk={}&volume={}&path={}&append={}&size={}&put_file_auth=digest-trailer-v1&put_file_nonce={}",
                    RDMA_PUT_FILE_STREAM_PATH,
                    urlencoding::encode(&request.disk),
                    urlencoding::encode(&request.volume),
                    urlencoding::encode(&request.path),
                    request.append,
                    request.size,
                    request.nonce
                );
                if let Some(server_epoch) = request.server_epoch {
                    target.push_str(&format!("&put_file_server_epoch={server_epoch}"));
                }
                target
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    Unauthorized,
    Unsupported,
    BadRequest,
    Busy,
    DiskNotFound,
    VolumeNotFound,
    FileNotFound,
    FileCorrupt,
    EpochConflict,
    Internal,
}

impl ErrorCode {
    pub fn from_disk_error(error: &DiskError) -> Self {
        match error {
            DiskError::FileNotFound => Self::FileNotFound,
            DiskError::VolumeNotFound => Self::VolumeNotFound,
            DiskError::FileCorrupt => Self::FileCorrupt,
            DiskError::DiskNotFound => Self::DiskNotFound,
            _ => Self::Internal,
        }
    }

    pub fn to_disk_error(self) -> Option<DiskError> {
        match self {
            Self::FileNotFound => Some(DiskError::FileNotFound),
            Self::VolumeNotFound => Some(DiskError::VolumeNotFound),
            Self::FileCorrupt => Some(DiskError::FileCorrupt),
            Self::DiskNotFound => Some(DiskError::DiskNotFound),
            _ => None,
        }
    }
}

/// Opens a stream; boxed inside [`Message`] because it is far larger than the other variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRequest {
    pub version: u16,
    pub stream: u64,
    pub op: StreamOp,
    pub auth: Auth,
    pub window: Option<WindowDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// Client to server: opens a stream. Reads carry the client's receive window.
    Request(Box<StreamRequest>),
    /// Server to client: the server's receive window for a write stream.
    Window {
        stream: u64,
        window: WindowDescriptor,
    },
    /// Data owner to receiver: `len` bytes landed in `slot`.
    Data {
        stream: u64,
        slot: u16,
        len: u32,
    },
    /// Receiver to data owner: `slot` may be written again.
    Credit {
        stream: u64,
        slot: u16,
    },
    /// Data owner to receiver: no more data; `total` bytes were written.
    Eof {
        stream: u64,
        total: u64,
    },
    /// Server to client: a write stream was committed with `bytes` payload bytes.
    Done {
        stream: u64,
        bytes: u64,
    },
    Error {
        stream: u64,
        code: ErrorCode,
        message: String,
    },
}

impl Message {
    pub fn stream(&self) -> u64 {
        match self {
            Self::Request(request) => request.stream,
            Self::Window { stream, .. }
            | Self::Data { stream, .. }
            | Self::Credit { stream, .. }
            | Self::Eof { stream, .. }
            | Self::Done { stream, .. }
            | Self::Error { stream, .. } => *stream,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, RdmaError> {
        let encoded = rmp_serde::to_vec(self)?;
        if encoded.len() > MESSAGE_BYTES {
            return Err(RdmaError::Protocol(format!(
                "control message of {} bytes exceeds {MESSAGE_BYTES}",
                encoded.len()
            )));
        }
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RdmaError> {
        Ok(rmp_serde::from_slice(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_round_trip_through_msgpack() {
        let request = Message::Request(Box::new(StreamRequest {
            version: RDMA_PROTOCOL_VERSION,
            stream: 7,
            op: StreamOp::Read(ReadRequest {
                disk: "http://node1:9000/data/rustfs0".to_owned(),
                volume: "bucket".to_owned(),
                path: "object/part.1".to_owned(),
                offset: 4096,
                length: 1 << 20,
            }),
            auth: Auth {
                signature: "sig".to_owned(),
                timestamp: 1,
                background: true,
            },
            window: Some(WindowDescriptor {
                rkey: 9,
                slot_size: 1 << 20,
                addrs: vec![1, 2, 3, 4],
            }),
        }));
        let decoded = Message::decode(&request.encode().expect("encode request")).expect("decode request");
        assert_eq!(decoded.stream(), 7);
        let Message::Request(decoded) = decoded else {
            panic!("decoded message must stay a request");
        };
        let window = decoded.window.expect("decoded request keeps its window");
        assert_eq!(window.addrs, vec![1, 2, 3, 4]);
    }

    #[test]
    fn canonical_targets_differ_from_http_paths_and_bind_every_field() {
        let read = StreamOp::Read(ReadRequest {
            disk: "disk a".to_owned(),
            volume: "bucket".to_owned(),
            path: "object/part.1".to_owned(),
            offset: 1,
            length: 2,
        });
        assert_eq!(
            read.canonical_target(),
            "/rustfs/rpc/rdma/read_file_stream?disk=disk%20a&volume=bucket&path=object%2Fpart.1&offset=1&length=2"
        );
        let nonce = Uuid::parse_str("11111111-2222-4333-8444-555555555555").expect("nonce");
        let write = StreamOp::Write(WriteRequest {
            disk: "disk".to_owned(),
            volume: "bucket".to_owned(),
            path: "p".to_owned(),
            append: true,
            size: -1,
            nonce,
            server_epoch: None,
        });
        assert_eq!(
            write.canonical_target(),
            "/rustfs/rpc/rdma/put_file_stream?disk=disk&volume=bucket&path=p&append=true&size=-1&put_file_auth=digest-trailer-v1&put_file_nonce=11111111-2222-4333-8444-555555555555"
        );
        assert!(!read.canonical_target().starts_with("/rustfs/rpc/read_file_stream"));
    }

    #[test]
    fn oversized_messages_are_rejected() {
        let message = Message::Error {
            stream: 1,
            code: ErrorCode::Internal,
            message: "x".repeat(MESSAGE_BYTES),
        };
        assert!(message.encode().is_err());
    }

    #[test]
    fn error_codes_map_to_typed_disk_errors() {
        assert!(matches!(ErrorCode::FileNotFound.to_disk_error(), Some(DiskError::FileNotFound)));
        assert!(matches!(
            ErrorCode::from_disk_error(&DiskError::VolumeNotFound),
            ErrorCode::VolumeNotFound
        ));
        assert!(ErrorCode::Internal.to_disk_error().is_none());
    }
}
