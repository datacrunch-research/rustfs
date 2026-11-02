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

//! Direction-neutral transfer engine: a data owner pushes bytes into the
//! peer's window, the receiver copies them out and returns credits.
//!
//! Both entry points abort the connection before returning an error, so a
//! caller may release slots in any order afterwards: an aborted queue pair
//! performs no further DMA.

use super::buffer::{RegisteredPool, Slot};
use super::conn::{Event, RdmaConn};
use super::error::RdmaError;
use super::proto::{Message, WindowDescriptor};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::debug;

/// Describes `slots` as a window the peer may write into.
pub fn window_descriptor(slots: &[Slot], rkey: u32) -> Result<WindowDescriptor, RdmaError> {
    let slot_size = slots
        .first()
        .map(Slot::capacity)
        .ok_or_else(|| RdmaError::Protocol("window needs at least one slot".to_owned()))?;
    Ok(WindowDescriptor {
        rkey,
        slot_size: u32::try_from(slot_size).map_err(|_| RdmaError::Protocol("slot size exceeds u32".to_owned()))?,
        addrs: slots.iter().map(Slot::addr).collect(),
    })
}

/// Pushes every byte of `source` into the peer's `window` and finishes with `Eof`.
///
/// Returns the number of payload bytes written. `on_progress` reports the
/// bytes handed to each RDMA write. On error the connection has been aborted.
pub async fn push_stream<S>(
    conn: &mut RdmaConn,
    pool: &Arc<RegisteredPool>,
    stream: u64,
    window: &WindowDescriptor,
    source: S,
    on_progress: impl FnMut(usize),
) -> Result<u64, RdmaError>
where
    S: Stream<Item = io::Result<Bytes>> + Unpin,
{
    if window.addrs.is_empty() || window.addrs.len() > usize::from(u16::MAX) {
        conn.abort();
        return Err(RdmaError::Protocol("peer window has an invalid slot count".to_owned()));
    }
    let slot_size = usize::try_from(window.slot_size).unwrap_or(usize::MAX);
    if slot_size == 0 || slot_size > pool.chunk_size() {
        conn.abort();
        return Err(RdmaError::Protocol("peer slot size is incompatible with the local pool".to_owned()));
    }
    let staging = match pool.reserve(window.addrs.len()).await {
        Ok(staging) => staging,
        Err(error) => {
            conn.abort();
            return Err(error);
        }
    };
    let result = push_into_window(conn, pool, stream, window, slot_size, &staging, source, on_progress).await;
    if result.is_err() {
        // No DMA may reference the staging slots once they return to the pool.
        conn.abort();
    }
    drop(staging);
    result
}

#[allow(clippy::too_many_arguments, reason = "internal engine step with one call site")]
async fn push_into_window<S>(
    conn: &mut RdmaConn,
    pool: &Arc<RegisteredPool>,
    stream: u64,
    window: &WindowDescriptor,
    slot_size: usize,
    staging: &[Slot],
    mut source: S,
    mut on_progress: impl FnMut(usize),
) -> Result<u64, RdmaError>
where
    S: Stream<Item = io::Result<Bytes>> + Unpin,
{
    let (lkey, _) = pool.keys_for(conn.raw())?;
    let mut free: Vec<usize> = (0..staging.len()).rev().collect();
    let mut inflight = 0usize;
    let mut leftover: Option<Bytes> = None;
    let mut exhausted = false;
    let mut total = 0u64;

    loop {
        while !exhausted {
            let Some(index) = free.pop() else { break };
            let mut filled = 0usize;
            while filled < slot_size {
                let chunk = match leftover.take() {
                    Some(chunk) => chunk,
                    None => match source.next().await {
                        Some(Ok(chunk)) => chunk,
                        Some(Err(error)) => return Err(RdmaError::Source(error)),
                        None => {
                            exhausted = true;
                            break;
                        }
                    },
                };
                if chunk.is_empty() {
                    continue;
                }
                let take = chunk.len().min(slot_size - filled);
                staging[index].write(filled, &chunk[..take])?;
                filled += take;
                if take < chunk.len() {
                    leftover = Some(chunk.slice(take..));
                }
            }
            if filled == 0 {
                free.push(index);
                break;
            }
            conn.post_write(lkey, staging[index].addr(), filled, window.addrs[index], window.rkey, index as u64)?;
            conn.send_message(&Message::Data {
                stream,
                slot: u16::try_from(index).map_err(|_| RdmaError::Protocol("slot index overflow".to_owned()))?,
                len: u32::try_from(filled).map_err(|_| RdmaError::Protocol("chunk length overflow".to_owned()))?,
            })
            .await?;
            inflight += 1;
            total += filled as u64;
            on_progress(filled);
        }

        if exhausted && inflight == 0 {
            // Every write was credited back, so no staging slot is referenced by the device.
            conn.send_message(&Message::Eof { stream, total }).await?;
            return Ok(total);
        }

        match conn.next_event().await? {
            Event::Message(Message::Credit { stream: credited, slot }) if credited == stream => {
                let slot = usize::from(slot);
                if slot >= staging.len() || free.contains(&slot) {
                    return Err(RdmaError::Protocol("peer credited an unknown or idle slot".to_owned()));
                }
                free.push(slot);
                inflight = inflight.saturating_sub(1);
            }
            Event::Message(Message::Error {
                stream: failed,
                code,
                message,
            }) if failed == stream => {
                return Err(RdmaError::Remote { code, message });
            }
            Event::Message(other) => debug!(stream, got = other.stream(), "ignoring stale rdma control message"),
            Event::WriteDone => {}
        }
    }
}

/// Receiver-side bookkeeping for one stream.
pub struct ReceiveState {
    stream: u64,
    slot_count: usize,
    total: u64,
}

/// What a received event means for the stream.
pub enum Step {
    /// A chunk arrived; its slot was already credited back.
    Chunk(Bytes),
    /// The data owner finished with this many payload bytes.
    Eof(u64),
    /// Nothing for this stream.
    Continue,
}

impl ReceiveState {
    pub fn new(stream: u64, slots: &[Slot]) -> Self {
        Self {
            stream,
            slot_count: slots.len(),
            total: 0,
        }
    }

    /// Applies one event, copying a landed chunk out of its slot and crediting the slot back.
    pub async fn step(&mut self, conn: &mut RdmaConn, slots: &[Slot], event: Event) -> Result<Step, RdmaError> {
        match event {
            Event::Message(Message::Data { stream, slot, len }) if stream == self.stream => {
                let index = usize::from(slot);
                if index >= self.slot_count {
                    return Err(RdmaError::Protocol("peer wrote to an unknown slot".to_owned()));
                }
                let bytes = slots[index].read(len as usize)?;
                conn.send_message(&Message::Credit { stream, slot }).await?;
                self.total += u64::from(len);
                Ok(Step::Chunk(bytes))
            }
            Event::Message(Message::Eof { stream, total }) if stream == self.stream => {
                if total != self.total {
                    return Err(RdmaError::Protocol(format!(
                        "peer reported {total} bytes but {} were delivered",
                        self.total
                    )));
                }
                Ok(Step::Eof(total))
            }
            Event::Message(Message::Error { stream, code, message }) if stream == self.stream => {
                Err(RdmaError::Remote { code, message })
            }
            Event::Message(other) => {
                debug!(stream = self.stream, got = other.stream(), "ignoring stale rdma control message");
                Ok(Step::Continue)
            }
            Event::WriteDone => Ok(Step::Continue),
        }
    }
}

/// Waits for the next event, failing with `Stalled` after `stall` without progress.
pub async fn next_event_with_stall(conn: &mut RdmaConn, stall: Option<Duration>) -> Result<Event, RdmaError> {
    match stall {
        Some(stall) => tokio::time::timeout(stall, conn.next_event())
            .await
            .map_err(|_| RdmaError::Stalled(stall))?,
        None => conn.next_event().await,
    }
}

/// Receives a whole stream into `sink`, returning the payload byte count.
///
/// `first` is an event the caller already took from the connection. When the
/// sink goes away the remaining chunks are drained and credited so the peer
/// can finish, then `ConsumerGone` is returned with the connection intact.
/// Every other error aborts the connection.
pub async fn receive_stream(
    conn: &mut RdmaConn,
    state: &mut ReceiveState,
    slots: &[Slot],
    sink: &mpsc::Sender<io::Result<Bytes>>,
    stall: Option<Duration>,
    first: Option<Event>,
    on_progress: impl FnMut(usize),
) -> Result<u64, RdmaError> {
    let result = receive_into_sink(conn, state, slots, sink, stall, first, on_progress).await;
    if !matches!(result, Ok(_) | Err(RdmaError::ConsumerGone)) {
        conn.abort();
    }
    result
}

async fn receive_into_sink(
    conn: &mut RdmaConn,
    state: &mut ReceiveState,
    slots: &[Slot],
    sink: &mpsc::Sender<io::Result<Bytes>>,
    stall: Option<Duration>,
    first: Option<Event>,
    mut on_progress: impl FnMut(usize),
) -> Result<u64, RdmaError> {
    let mut pending = first;
    let mut consumer_gone = false;
    loop {
        let event = match pending.take() {
            Some(event) => event,
            None => next_event_with_stall(conn, stall).await?,
        };
        match state.step(conn, slots, event).await? {
            Step::Chunk(bytes) => {
                on_progress(bytes.len());
                if !consumer_gone && sink.send(Ok(bytes)).await.is_err() {
                    consumer_gone = true;
                }
            }
            Step::Eof(total) => {
                return if consumer_gone {
                    Err(RdmaError::ConsumerGone)
                } else {
                    Ok(total)
                };
            }
            Step::Continue => {}
        }
    }
}
