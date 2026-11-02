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

use super::error::RdmaError;
use super::ffi::{Connection, DeviceId, MemoryRegion, copy_from_region, copy_into_region};
use bytes::Bytes;
use memmap2::{MmapMut, MmapOptions};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// A pinned, page-aligned mapping carved into fixed-size slots and registered
/// lazily with every RDMA device that uses it.
///
/// Slots are handed out through [`Slot`] guards. A guard must only be dropped
/// after no in-flight work request can still reference its memory: transfers
/// release slots after the peer acknowledged them, and failure paths abort the
/// queue pair first (see `RdmaConn::abort`).
pub struct RegisteredPool {
    // Keeps the mapping alive; the slots access it through `base`.
    _map: MmapMut,
    base: usize,
    len: usize,
    chunk_size: usize,
    slot_count: u32,
    free: parking_lot::Mutex<Vec<u32>>,
    permits: Semaphore,
    regions: parking_lot::Mutex<HashMap<DeviceId, Arc<MemoryRegion>>>,
}

impl std::fmt::Debug for RegisteredPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredPool")
            .field("chunk_size", &self.chunk_size)
            .field("slot_count", &self.slot_count)
            .field("free", &self.free.lock().len())
            .finish()
    }
}

impl RegisteredPool {
    pub fn new(total_bytes: usize, chunk_size: usize) -> Result<Arc<Self>, RdmaError> {
        if chunk_size == 0 || total_bytes < chunk_size {
            return Err(RdmaError::Protocol("rdma pool must hold at least one chunk".to_owned()));
        }
        let slot_count =
            u32::try_from(total_bytes / chunk_size).map_err(|_| RdmaError::Protocol("too many pool slots".to_owned()))?;
        let len = chunk_size
            .checked_mul(slot_count as usize)
            .ok_or_else(|| RdmaError::Protocol("pool size overflow".to_owned()))?;
        let mut map = MmapOptions::new()
            .len(len)
            .populate()
            .map_anon()
            .map_err(|source| RdmaError::Os { op: "mmap pool", source })?;
        let base = map.as_mut_ptr() as usize;
        Ok(Arc::new(Self {
            _map: map,
            base,
            len,
            chunk_size,
            slot_count,
            free: parking_lot::Mutex::new((0..slot_count).rev().collect()),
            permits: Semaphore::new(slot_count as usize),
            regions: parking_lot::Mutex::new(HashMap::new()),
        }))
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    pub fn slot_count(&self) -> u32 {
        self.slot_count
    }

    pub fn available(&self) -> usize {
        self.permits.available_permits()
    }

    /// Reserves `count` slots without waiting.
    pub fn try_reserve(self: &Arc<Self>, count: usize) -> Result<Vec<Slot>, RdmaError> {
        let needed = u32::try_from(count).map_err(|_| RdmaError::PoolExhausted)?;
        let permit = self.permits.try_acquire_many(needed).map_err(|_| RdmaError::PoolExhausted)?;
        permit.forget();
        Ok(self.pop_slots(count))
    }

    /// Reserves `count` slots, waiting for other streams to release theirs.
    pub async fn reserve(self: &Arc<Self>, count: usize) -> Result<Vec<Slot>, RdmaError> {
        let needed = u32::try_from(count).map_err(|_| RdmaError::PoolExhausted)?;
        if needed > self.slot_count {
            return Err(RdmaError::PoolExhausted);
        }
        let permit = self
            .permits
            .acquire_many(needed)
            .await
            .map_err(|_| RdmaError::PoolExhausted)?;
        permit.forget();
        Ok(self.pop_slots(count))
    }

    fn pop_slots(self: &Arc<Self>, count: usize) -> Vec<Slot> {
        let mut free = self.free.lock();
        // The permit count equals the free-list length, so the pops cannot fail.
        (0..count)
            .filter_map(|_| free.pop())
            .map(|index| Slot {
                pool: Arc::clone(self),
                index,
            })
            .collect()
    }

    /// Local and remote keys for this pool on the connection's device,
    /// registering the mapping on first use.
    pub fn keys_for(&self, conn: &Connection) -> Result<(u32, u32), RdmaError> {
        let device = conn.device();
        // Registration runs under the lock: a losing racer must never hand out keys of a
        // region that is deregistered when the winner's entry is kept.
        let mut regions = self.regions.lock();
        if let Some(region) = regions.get(&device) {
            return Ok((region.lkey(), region.rkey()));
        }
        let region = Arc::new(MemoryRegion::register(device, self.base as *mut u8, self.len)?);
        let keys = (region.lkey(), region.rkey());
        regions.insert(device, region);
        Ok(keys)
    }
}

/// Exclusive ownership of one pool slot.
pub struct Slot {
    pool: Arc<RegisteredPool>,
    index: u32,
}

impl std::fmt::Debug for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Slot").field("index", &self.index).finish()
    }
}

impl Slot {
    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn capacity(&self) -> usize {
        self.pool.chunk_size
    }

    fn offset(&self) -> usize {
        self.index as usize * self.pool.chunk_size
    }

    /// Address the peer or the local device writes to.
    pub fn addr(&self) -> u64 {
        (self.pool.base + self.offset()) as u64
    }

    pub fn write(&self, offset: usize, data: &[u8]) -> Result<(), RdmaError> {
        if offset.saturating_add(data.len()) > self.pool.chunk_size {
            return Err(RdmaError::Protocol("slot write exceeds chunk size".to_owned()));
        }
        copy_into_region(self.pool.base, self.pool.len, self.offset() + offset, data)
    }

    pub fn read(&self, len: usize) -> Result<Bytes, RdmaError> {
        if len > self.pool.chunk_size {
            return Err(RdmaError::Protocol("slot read exceeds chunk size".to_owned()));
        }
        copy_from_region(self.pool.base, self.pool.len, self.offset(), len)
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.pool.free.lock().push(self.index);
        self.pool.permits.add_permits(1);
    }
}

/// A small per-connection mapping for control messages.
pub struct ControlRegion {
    _map: MmapMut,
    base: usize,
    len: usize,
    region: MemoryRegion,
}

impl ControlRegion {
    pub fn new(conn: &Connection, len: usize) -> Result<Self, RdmaError> {
        let mut map = MmapOptions::new()
            .len(len)
            .populate()
            .map_anon()
            .map_err(|source| RdmaError::Os {
                op: "mmap control",
                source,
            })?;
        let base = map.as_mut_ptr() as usize;
        let region = conn.register(map.as_mut_ptr(), len)?;
        Ok(Self {
            _map: map,
            base,
            len,
            region,
        })
    }

    pub fn lkey(&self) -> u32 {
        self.region.lkey()
    }

    pub fn addr(&self, offset: usize) -> u64 {
        (self.base + offset) as u64
    }

    pub fn write(&self, offset: usize, data: &[u8]) -> Result<(), RdmaError> {
        copy_into_region(self.base, self.len, offset, data)
    }

    pub fn read(&self, offset: usize, count: usize) -> Result<Bytes, RdmaError> {
        copy_from_region(self.base, self.len, offset, count)
    }
}
