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
//! peer's window, the receiver hands the landed slots to its consumer and
//! returns a credit once the consumer has dropped them.
//!
//! No payload byte is copied on the receiving side: each delivered chunk is a
//! `Bytes` backed directly by the registered slot the peer wrote into, and the
//! slot is credited back (and may be rewritten) only after every reference to
//! that chunk is gone. On the sending side a file is read straight into the
//! registered staging slot; only caller-owned byte streams are copied once.
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
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
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

/// Produces payload bytes directly into registered staging memory.
pub trait ChunkSource: Send {
    /// Fills `dst` completely unless the data ends first; returns the byte
    /// count, which is below `dst.len()` only at end of data and `0` once
    /// nothing is left.
    fn fill(&mut self, dst: &mut [u8]) -> impl Future<Output = io::Result<usize>> + Send;
}

/// A file or any async reader, read straight into the staging slot.
pub struct ReaderSource<R>(R);

impl<R: AsyncRead + Unpin + Send> ReaderSource<R> {
    pub fn new(reader: R) -> Self {
        Self(reader)
    }
}

impl<R: AsyncRead + Unpin + Send> ChunkSource for ReaderSource<R> {
    async fn fill(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        let mut filled = 0;
        while filled < dst.len() {
            let read = self.0.read(&mut dst[filled..]).await?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        Ok(filled)
    }
}

/// Caller-owned chunks (for example a writer's queue), copied once into the slot.
pub struct StreamSource<S> {
    stream: S,
    leftover: Option<Bytes>,
}

impl<S: Stream<Item = io::Result<Bytes>> + Unpin + Send> StreamSource<S> {
    pub fn new(stream: S) -> Self {
        Self { stream, leftover: None }
    }
}

impl<S: Stream<Item = io::Result<Bytes>> + Unpin + Send> ChunkSource for StreamSource<S> {
    async fn fill(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        let mut filled = 0;
        while filled < dst.len() {
            let chunk = match self.leftover.take() {
                Some(chunk) => chunk,
                None => match self.stream.next().await {
                    Some(Ok(chunk)) => chunk,
                    Some(Err(error)) => return Err(error),
                    None => break,
                },
            };
            if chunk.is_empty() {
                continue;
            }
            let take = chunk.len().min(dst.len() - filled);
            dst[filled..filled + take].copy_from_slice(&chunk[..take]);
            filled += take;
            if take < chunk.len() {
                self.leftover = Some(chunk.slice(take..));
            }
        }
        Ok(filled)
    }
}

/// Pushes every byte of `source` into the peer's `window` and finishes with `Eof`.
///
/// Returns the number of payload bytes written. `on_progress` reports the
/// bytes handed to each RDMA write. On error the connection has been aborted.
pub async fn push_stream<S: ChunkSource>(
    conn: &mut RdmaConn,
    pool: &Arc<RegisteredPool>,
    stream: u64,
    window: &WindowDescriptor,
    source: S,
    on_progress: impl FnMut(usize),
) -> Result<u64, RdmaError> {
    if window.addrs.is_empty() || window.addrs.len() > usize::from(u16::MAX) {
        conn.abort();
        return Err(RdmaError::Protocol("peer window has an invalid slot count".to_owned()));
    }
    let slot_size = usize::try_from(window.slot_size).unwrap_or(usize::MAX);
    if slot_size == 0 || slot_size > pool.chunk_size() {
        conn.abort();
        return Err(RdmaError::Protocol("peer slot size is incompatible with the local pool".to_owned()));
    }
    let mut staging = match pool.reserve(window.addrs.len()).await {
        Ok(staging) => staging,
        Err(error) => {
            conn.abort();
            return Err(error);
        }
    };
    let result = push_into_window(conn, pool, stream, window, slot_size, &mut staging, source, on_progress).await;
    if result.is_err() {
        // No DMA may reference the staging slots once they return to the pool.
        conn.abort();
    }
    drop(staging);
    result
}

#[allow(clippy::too_many_arguments, reason = "internal engine step with one call site")]
async fn push_into_window<S: ChunkSource>(
    conn: &mut RdmaConn,
    pool: &Arc<RegisteredPool>,
    stream: u64,
    window: &WindowDescriptor,
    slot_size: usize,
    staging: &mut [Slot],
    mut source: S,
    mut on_progress: impl FnMut(usize),
) -> Result<u64, RdmaError> {
    let (lkey, _) = pool.keys_for(conn.raw())?;
    let mut free: Vec<usize> = (0..staging.len()).rev().collect();
    let mut inflight = 0usize;
    let mut exhausted = false;
    let mut total = 0u64;

    loop {
        while !exhausted {
            let Some(index) = free.pop() else { break };
            let filled = source
                .fill(staging[index].as_mut_slice(slot_size)?)
                .await
                .map_err(RdmaError::Source)?;
            if filled < slot_size {
                exhausted = true;
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

/// A delivered chunk: a view of the registered slot the peer wrote into.
///
/// Dropping the last reference returns the slot's credit to the peer through
/// the stream task; the slot itself stays reserved until then, so a chunk may
/// outlive the stream and the connection safely.
struct SlotChunk {
    slot: Arc<Slot>,
    index: u16,
    len: usize,
    credits: mpsc::UnboundedSender<u16>,
}

impl AsRef<[u8]> for SlotChunk {
    fn as_ref(&self) -> &[u8] {
        // `len` was validated against the slot capacity when the chunk was built.
        self.slot.as_slice(self.len).unwrap_or(&[])
    }
}

impl Drop for SlotChunk {
    fn drop(&mut self) {
        // A closed channel means the stream already finished; the slot just returns to the pool.
        let _ = self.credits.send(self.index);
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
/// Every other error aborts the connection. Chunks still held by the consumer
/// keep their slots reserved after this returns.
pub async fn receive_stream(
    conn: &mut RdmaConn,
    stream: u64,
    slots: Vec<Slot>,
    sink: &mpsc::Sender<io::Result<Bytes>>,
    stall: Option<Duration>,
    first: Option<Event>,
    on_progress: impl FnMut(usize),
) -> Result<u64, RdmaError> {
    let slots: Vec<Arc<Slot>> = slots.into_iter().map(Arc::new).collect();
    let result = receive_into_sink(conn, stream, &slots, sink, stall, first, on_progress).await;
    if !matches!(result, Ok(_) | Err(RdmaError::ConsumerGone)) {
        conn.abort();
    }
    result
}

async fn receive_into_sink(
    conn: &mut RdmaConn,
    stream: u64,
    slots: &[Arc<Slot>],
    sink: &mpsc::Sender<io::Result<Bytes>>,
    stall: Option<Duration>,
    first: Option<Event>,
    mut on_progress: impl FnMut(usize),
) -> Result<u64, RdmaError> {
    let (credit_tx, mut credit_rx) = mpsc::unbounded_channel::<u16>();
    let mut pending = first;
    let mut consumer_gone = false;
    let mut total = 0u64;
    // Slots the peer may write into right now. While the consumer holds every
    // slot the peer cannot make progress, so silence then is backpressure, not
    // a stall; the stall timeout only runs while the peer owes data.
    let mut credited = slots.len();
    loop {
        let event = match pending.take() {
            Some(event) => event,
            None => {
                let stall_now = if credited > 0 { stall } else { None };
                tokio::select! {
                    credit = credit_rx.recv() => {
                        if let Some(slot) = credit {
                            conn.send_message(&Message::Credit { stream, slot }).await?;
                            credited += 1;
                        }
                        continue;
                    }
                    event = next_event_with_stall(conn, stall_now) => event?,
                }
            }
        };
        match event {
            Event::Message(Message::Data {
                stream: landed,
                slot,
                len,
            }) if landed == stream => {
                let index = usize::from(slot);
                let owner = slots
                    .get(index)
                    .ok_or_else(|| RdmaError::Protocol("peer wrote to an unknown slot".to_owned()))?;
                let len = usize::try_from(len).map_err(|_| RdmaError::Protocol("chunk length overflow".to_owned()))?;
                if len > owner.capacity() {
                    return Err(RdmaError::Protocol("peer reported a chunk larger than its slot".to_owned()));
                }
                let chunk = Bytes::from_owner(SlotChunk {
                    slot: Arc::clone(owner),
                    index: slot,
                    len,
                    credits: credit_tx.clone(),
                });
                credited = credited.saturating_sub(1);
                total += len as u64;
                on_progress(len);
                if consumer_gone {
                    drop(chunk);
                } else if sink.send(Ok(chunk)).await.is_err() {
                    consumer_gone = true;
                }
            }
            Event::Message(Message::Eof {
                stream: ended,
                total: sent,
            }) if ended == stream => {
                if sent != total {
                    return Err(RdmaError::Protocol(format!("peer reported {sent} bytes but {total} were delivered")));
                }
                return if consumer_gone {
                    Err(RdmaError::ConsumerGone)
                } else {
                    Ok(total)
                };
            }
            Event::Message(Message::Error {
                stream: failed,
                code,
                message,
            }) if failed == stream => {
                return Err(RdmaError::Remote { code, message });
            }
            Event::Message(other) => {
                debug!(stream, got = other.stream(), "ignoring stale rdma control message");
            }
            Event::WriteDone => {}
        }
    }
}
