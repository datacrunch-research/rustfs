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

use super::buffer::ControlRegion;
use super::error::RdmaError;
use super::ffi::{CmStatus, Connection, QpLimits, RawFdHandle, WC_SUCCESS, WorkCompletion, async_fd, wc_status_name};
use super::proto::{MESSAGE_BYTES, Message};
use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::unix::AsyncFd;

/// Posted receive buffers per connection; bounds how many control messages a
/// peer can send before the receiver drains them.
pub const MESSAGE_RECV_DEPTH: usize = 32;
/// Send buffers per connection.
pub const MESSAGE_SEND_DEPTH: usize = 32;

const WR_KIND_SHIFT: u32 = 56;
const WR_KIND_RECV: u8 = 1;
const WR_KIND_SEND: u8 = 2;
const WR_KIND_WRITE: u8 = 3;

/// Queue limits sized for the control ring plus one stream window.
pub fn qp_limits(stream_slots: usize) -> QpLimits {
    let slots = u32::try_from(stream_slots).unwrap_or(u32::MAX).min(1024);
    let send_depth = u32::try_from(MESSAGE_SEND_DEPTH).unwrap_or(u32::MAX) + slots * 2 + 8;
    let recv_depth = u32::try_from(MESSAGE_RECV_DEPTH).unwrap_or(u32::MAX);
    QpLimits {
        send_depth,
        recv_depth,
        cq_depth: send_depth + recv_depth + 16,
    }
}

fn wr_id(kind: u8, index: u64) -> u64 {
    (u64::from(kind) << WR_KIND_SHIFT) | (index & ((1 << WR_KIND_SHIFT) - 1))
}

fn wr_kind(id: u64) -> (u8, u64) {
    ((id >> WR_KIND_SHIFT) as u8, id & ((1 << WR_KIND_SHIFT) - 1))
}

/// Something the connection delivered: a decoded control message or the local
/// completion of an RDMA write.
#[derive(Debug)]
pub enum Event {
    Message(Message),
    WriteDone,
}

/// An established reliable connection driven from one task at a time.
///
/// Field order matters for `Drop`: the async descriptors deregister from the
/// reactor before the underlying connection closes them, and closing the
/// connection moves its queue pair to the error state, so a caller that
/// declares its pool slots before the connection releases them only after
/// no DMA can reference them.
pub struct RdmaConn {
    comp: AsyncFd<RawFdHandle>,
    cm: AsyncFd<RawFdHandle>,
    raw: Connection,
    control: ControlRegion,
    send_free: Vec<u16>,
    pending: VecDeque<Event>,
    next_stream: u64,
}

impl std::fmt::Debug for RdmaConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaConn").field("peer", &self.raw.peer()).finish()
    }
}

impl RdmaConn {
    /// Connects to `peer`, bounded by `timeout` for resolution and establishment.
    pub async fn connect(peer: SocketAddr, timeout: Duration, limits: QpLimits) -> Result<Self, RdmaError> {
        let raw = tokio::task::spawn_blocking(move || Connection::connect_client(peer, timeout, &limits))
            .await
            .map_err(|err| RdmaError::Protocol(format!("rdma connect task failed: {err}")))??;
        let conn = Self::new(raw)?;
        conn.raw.start_connect()?;
        tokio::time::timeout(timeout, conn.wait_established())
            .await
            .map_err(|_| RdmaError::ConnectTimeout {
                peer: peer.to_string(),
                timeout,
            })??;
        Ok(conn)
    }

    /// Completes a connection accepted by a listener.
    pub async fn accept(raw: Connection, timeout: Duration) -> Result<Self, RdmaError> {
        let peer = raw.peer().to_owned();
        let conn = Self::new(raw)?;
        conn.raw.start_accept()?;
        tokio::time::timeout(timeout, conn.wait_established())
            .await
            .map_err(|_| RdmaError::ConnectTimeout { peer, timeout })??;
        Ok(conn)
    }

    fn new(raw: Connection) -> Result<Self, RdmaError> {
        let control = ControlRegion::new(&raw, (MESSAGE_RECV_DEPTH + MESSAGE_SEND_DEPTH) * MESSAGE_BYTES)?;
        let comp = async_fd("register completion fd", raw.comp_fd())?;
        let cm = async_fd("register cm fd", raw.cm_fd())?;
        // Receives are posted before the connection is established so the
        // peer's first message can never arrive without a buffer.
        for index in 0..MESSAGE_RECV_DEPTH {
            raw.post_recv(
                control.lkey(),
                control.addr(index * MESSAGE_BYTES),
                u32::try_from(MESSAGE_BYTES).unwrap_or(u32::MAX),
                wr_id(WR_KIND_RECV, index as u64),
            )?;
        }
        Ok(Self {
            comp,
            cm,
            raw,
            control,
            send_free: (0..MESSAGE_SEND_DEPTH).map(|index| index as u16).collect(),
            pending: VecDeque::new(),
            next_stream: 1,
        })
    }

    async fn wait_established(&self) -> Result<(), RdmaError> {
        loop {
            let mut guard = self
                .cm
                .readable()
                .await
                .map_err(|source| RdmaError::Os { op: "cm wait", source })?;
            match self.raw.poll_cm()? {
                CmStatus::None => guard.clear_ready(),
                CmStatus::Established => return Ok(()),
                CmStatus::Rejected => {
                    return Err(RdmaError::Rejected {
                        peer: self.raw.peer().to_owned(),
                    });
                }
                CmStatus::Disconnected => {
                    return Err(RdmaError::Disconnected {
                        peer: self.raw.peer().to_owned(),
                    });
                }
                CmStatus::Unreachable | CmStatus::Error => {
                    return Err(RdmaError::Connect {
                        peer: self.raw.peer().to_owned(),
                        source: io::Error::new(io::ErrorKind::HostUnreachable, "rdma connection establishment failed"),
                    });
                }
            }
        }
    }

    /// Whether the peer is still connected, judged from pending connection events only.
    pub fn is_healthy(&self) -> bool {
        matches!(self.raw.poll_cm(), Ok(CmStatus::None | CmStatus::Established))
    }

    pub fn raw(&self) -> &Connection {
        &self.raw
    }

    pub fn next_stream_id(&mut self) -> u64 {
        let id = self.next_stream;
        self.next_stream = self.next_stream.wrapping_add(1);
        id
    }

    /// Stops all DMA on this connection; the queue pair cannot be used afterwards.
    pub fn abort(&self) {
        self.raw.abort();
    }

    pub fn post_write(&self, lkey: u32, addr: u64, len: usize, remote_addr: u64, rkey: u32, slot: u64) -> Result<(), RdmaError> {
        let len = u32::try_from(len).map_err(|_| RdmaError::Protocol("rdma write exceeds 4 GiB".to_owned()))?;
        self.raw
            .post_write(lkey, addr, len, remote_addr, rkey, wr_id(WR_KIND_WRITE, slot))
    }

    pub async fn send_message(&mut self, message: &Message) -> Result<(), RdmaError> {
        let encoded = message.encode()?;
        let slot = loop {
            if let Some(slot) = self.send_free.pop() {
                break slot;
            }
            self.pump().await?;
        };
        let offset = (MESSAGE_RECV_DEPTH + slot as usize) * MESSAGE_BYTES;
        self.control.write(offset, &encoded)?;
        self.raw.post_send(
            self.control.lkey(),
            self.control.addr(offset),
            u32::try_from(encoded.len()).unwrap_or(u32::MAX),
            wr_id(WR_KIND_SEND, u64::from(slot)),
        )
    }

    /// Waits for the next message or write completion.
    pub async fn next_event(&mut self) -> Result<Event, RdmaError> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(event);
            }
            self.pump().await?;
        }
    }

    /// Processes at least one completion, waiting on the completion channel when none is ready.
    async fn pump(&mut self) -> Result<(), RdmaError> {
        let mut completions = [WorkCompletion::default(); 16];
        loop {
            let count = self.raw.poll_completions(&mut completions)?;
            if count > 0 {
                for completion in &completions[..count] {
                    self.handle_completion(*completion)?;
                }
                return Ok(());
            }
            let mut guard = self.comp.readable().await.map_err(|source| RdmaError::Os {
                op: "completion wait",
                source,
            })?;
            guard.clear_ready();
        }
    }

    fn handle_completion(&mut self, completion: WorkCompletion) -> Result<(), RdmaError> {
        let (kind, index) = wr_kind(completion.wr_id);
        if completion.status != WC_SUCCESS {
            return Err(RdmaError::WorkCompletion {
                status: wc_status_name(completion.status),
                kind,
                vendor_err: completion.vendor_err,
            });
        }
        match kind {
            WR_KIND_SEND => {
                self.send_free
                    .push(u16::try_from(index).map_err(|_| RdmaError::Protocol("send slot index overflow".to_owned()))?);
            }
            WR_KIND_WRITE => self.pending.push_back(Event::WriteDone),
            WR_KIND_RECV => {
                let offset = usize::try_from(index)
                    .ok()
                    .filter(|index| *index < MESSAGE_RECV_DEPTH)
                    .map(|index| index * MESSAGE_BYTES)
                    .ok_or_else(|| RdmaError::Protocol("receive slot index out of range".to_owned()))?;
                let bytes = self.control.read(offset, completion.byte_len as usize)?;
                self.raw.post_recv(
                    self.control.lkey(),
                    self.control.addr(offset),
                    u32::try_from(MESSAGE_BYTES).unwrap_or(u32::MAX),
                    wr_id(WR_KIND_RECV, index),
                )?;
                self.pending.push_back(Event::Message(Message::decode(&bytes)?));
            }
            other => return Err(RdmaError::Protocol(format!("unknown work request kind {other}"))),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_request_ids_round_trip_kind_and_index() {
        for kind in [WR_KIND_RECV, WR_KIND_SEND, WR_KIND_WRITE] {
            for index in [0, 1, 31, (1 << 40) + 5] {
                assert_eq!(wr_kind(wr_id(kind, index)), (kind, index));
            }
        }
    }

    #[test]
    fn queue_limits_cover_control_ring_and_window() {
        let limits = qp_limits(4);
        assert!(limits.send_depth as usize >= MESSAGE_SEND_DEPTH + 8);
        assert_eq!(limits.recv_depth as usize, MESSAGE_RECV_DEPTH);
        assert!(limits.cq_depth >= limits.send_depth + limits.recv_depth);
    }
}
