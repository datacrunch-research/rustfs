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

// This module is the only caller of the C shim in `crates/ecstore/csrc/rdma_shim.c`.
// Every raw pointer handed to the shim comes from a live owner (`Listener`, `Connection`,
// `MemoryRegion`) that releases it exactly once in `Drop`, and every registered memory range
// stays mapped for as long as the region that references it.
// SAFETY: per-call invariants are documented on each `unsafe` block below.
#![allow(unsafe_code)]

use super::error::RdmaError;
use bytes::Bytes;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::ptr::{self, NonNull};
use std::time::Duration;
use tokio::io::unix::AsyncFd;

/// Queue sizes for one connection's queue pair and completion queue.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct QpLimits {
    pub send_depth: u32,
    pub recv_depth: u32,
    pub cq_depth: u32,
}

/// One completed work request as reported by the shim.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct WorkCompletion {
    pub wr_id: u64,
    pub status: u32,
    pub opcode: u32,
    pub byte_len: u32,
    pub vendor_err: u32,
}

pub const WC_SUCCESS: u32 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmStatus {
    None,
    Established,
    Disconnected,
    Rejected,
    Unreachable,
    Error,
}

#[repr(C)]
pub struct RawListener {
    _private: [u8; 0],
}

#[repr(C)]
pub struct RawConn {
    _private: [u8; 0],
}

#[repr(C)]
pub struct RawMr {
    _private: [u8; 0],
}

#[repr(C)]
pub struct RawDevice {
    _private: [u8; 0],
}

unsafe extern "C" {
    fn rfs_rdma_listener_create(ip: *const c_char, port: u16, backlog: c_int, out: *mut *mut RawListener) -> c_int;
    fn rfs_rdma_listener_fd(listener: *const RawListener) -> c_int;
    fn rfs_rdma_listener_accept(listener: *mut RawListener, limits: *const QpLimits, out: *mut *mut RawConn) -> c_int;
    fn rfs_rdma_listener_destroy(listener: *mut RawListener);
    fn rfs_rdma_listener_device(listener: *const RawListener) -> *mut RawDevice;
    fn rfs_rdma_client_create(
        ip: *const c_char,
        port: u16,
        timeout_ms: c_int,
        limits: *const QpLimits,
        out: *mut *mut RawConn,
    ) -> c_int;
    fn rfs_rdma_conn_connect(conn: *mut RawConn) -> c_int;
    fn rfs_rdma_conn_accept(conn: *mut RawConn) -> c_int;
    fn rfs_rdma_conn_cm_fd(conn: *const RawConn) -> c_int;
    fn rfs_rdma_conn_poll_cm(conn: *mut RawConn) -> c_int;
    fn rfs_rdma_conn_comp_fd(conn: *const RawConn) -> c_int;
    fn rfs_rdma_conn_poll_completions(conn: *mut RawConn, out: *mut WorkCompletion, max: c_int) -> c_int;
    fn rfs_rdma_conn_device(conn: *const RawConn) -> *mut RawDevice;
    fn rfs_rdma_conn_post_recv(conn: *mut RawConn, lkey: u32, addr: u64, len: u32, wr_id: u64) -> c_int;
    fn rfs_rdma_conn_post_send(conn: *mut RawConn, lkey: u32, addr: u64, len: u32, wr_id: u64) -> c_int;
    fn rfs_rdma_conn_post_write(
        conn: *mut RawConn,
        lkey: u32,
        addr: u64,
        len: u32,
        remote_addr: u64,
        rkey: u32,
        wr_id: u64,
    ) -> c_int;
    fn rfs_rdma_conn_peer_addr(conn: *const RawConn, buf: *mut c_char, buf_len: usize) -> c_int;
    fn rfs_rdma_conn_abort(conn: *mut RawConn);
    fn rfs_rdma_conn_close(conn: *mut RawConn);
    fn rfs_rdma_device_reg_mr(device: *mut RawDevice, addr: *mut c_void, len: usize, out: *mut *mut RawMr) -> c_int;
    fn rfs_rdma_mr_lkey(mr: *const RawMr) -> u32;
    fn rfs_rdma_mr_rkey(mr: *const RawMr) -> u32;
    fn rfs_rdma_mr_dereg(mr: *mut RawMr);
    fn rfs_rdma_wc_status_str(status: u32) -> *const c_char;
}

fn os_error(op: &'static str, code: c_int) -> RdmaError {
    RdmaError::Os {
        op,
        source: io::Error::from_raw_os_error(-code),
    }
}

fn ip_cstring(addr: &SocketAddr) -> Result<CString, RdmaError> {
    CString::new(addr.ip().to_string()).map_err(|_| RdmaError::Endpoint(addr.to_string()))
}

/// Human-readable name of a work completion status.
pub fn wc_status_name(status: u32) -> String {
    // SAFETY: the shim returns a pointer to a static, NUL-terminated string for every status value.
    let name = unsafe { CStr::from_ptr(rfs_rdma_wc_status_str(status)) };
    name.to_string_lossy().into_owned()
}

/// Identity of an RDMA device context; memory regions are registered per device.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DeviceId(usize);

/// A bound and listening RDMA connection-manager identifier.
pub struct Listener {
    raw: NonNull<RawListener>,
    local: SocketAddr,
}

// SAFETY: the shim serializes device-table access with a mutex and librdmacm objects may be
// used from any thread; the listener is only driven from one task at a time.
unsafe impl Send for Listener {}
unsafe impl Sync for Listener {}

impl Listener {
    pub fn bind(local: SocketAddr, backlog: i32) -> Result<Self, RdmaError> {
        let ip = ip_cstring(&local)?;
        let mut out: *mut RawListener = ptr::null_mut();
        // SAFETY: `ip` is a valid NUL-terminated string and `out` is a valid out-pointer.
        let rc = unsafe { rfs_rdma_listener_create(ip.as_ptr(), local.port(), backlog, &mut out) };
        if rc < 0 {
            return Err(os_error("listen", rc));
        }
        let raw = NonNull::new(out).ok_or_else(|| RdmaError::Protocol("listener handle missing".to_owned()))?;
        Ok(Self { raw, local })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local
    }

    pub fn fd(&self) -> RawFd {
        // SAFETY: `raw` is live until `Drop`.
        unsafe { rfs_rdma_listener_fd(self.raw.as_ptr()) }
    }

    /// The device behind a listener bound to a specific address; `None` for a wildcard bind.
    pub fn device(&self) -> Option<DeviceId> {
        // SAFETY: `raw` is live until `Drop`; the device pointer outlives the process.
        let device = unsafe { rfs_rdma_listener_device(self.raw.as_ptr()) };
        (!device.is_null()).then_some(DeviceId(device as usize))
    }

    /// Accepts one pending connection request without blocking.
    pub fn try_accept(&self, limits: &QpLimits) -> Result<Accept, RdmaError> {
        let mut out: *mut RawConn = ptr::null_mut();
        // SAFETY: `raw` is live, `limits` outlives the call, and `out` is a valid out-pointer.
        let rc = unsafe { rfs_rdma_listener_accept(self.raw.as_ptr(), limits, &mut out) };
        if rc < 0 {
            return Err(os_error("accept", rc));
        }
        if rc == 0 {
            return Ok(Accept::WouldBlock);
        }
        if rc == 2 {
            return Ok(Accept::Skipped);
        }
        let raw = NonNull::new(out).ok_or_else(|| RdmaError::Protocol("accepted connection handle missing".to_owned()))?;
        Ok(Accept::Connection(Connection::from_raw(raw)))
    }
}

/// One step of draining a listener's event channel.
pub enum Accept {
    /// A connection request was accepted into a new connection.
    Connection(Connection),
    /// No event is pending; the caller may wait for readiness again.
    WouldBlock,
    /// An event that was not a connection request was consumed; keep draining
    /// before waiting, since readiness is edge-triggered.
    Skipped,
}

impl Drop for Listener {
    fn drop(&mut self) {
        // SAFETY: the handle was created by the shim and is released exactly once here.
        unsafe { rfs_rdma_listener_destroy(self.raw.as_ptr()) };
    }
}

/// One reliable-connected queue pair plus its completion machinery.
pub struct Connection {
    raw: NonNull<RawConn>,
    peer: String,
}

// SAFETY: see `Listener`; a connection is driven by one task at a time and the verbs
// library itself is thread-safe for posting and polling.
unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

impl Connection {
    fn from_raw(raw: NonNull<RawConn>) -> Self {
        let mut buf = [0 as c_char; 64];
        // SAFETY: `raw` is live and `buf` is a writable buffer of the given length.
        let rc = unsafe { rfs_rdma_conn_peer_addr(raw.as_ptr(), buf.as_mut_ptr(), buf.len()) };
        let peer = if rc == 0 {
            // SAFETY: the shim NUL-terminates the buffer on success.
            unsafe { CStr::from_ptr(buf.as_ptr()) }.to_string_lossy().into_owned()
        } else {
            "unknown".to_owned()
        };
        Self { raw, peer }
    }

    /// Resolves the peer address and route synchronously and creates the queue pair.
    /// Blocks for up to `timeout` per resolution step; call it from a blocking thread.
    pub fn connect_client(peer: SocketAddr, timeout: Duration, limits: &QpLimits) -> Result<Self, RdmaError> {
        let ip = ip_cstring(&peer)?;
        let timeout_ms = c_int::try_from(timeout.as_millis()).unwrap_or(c_int::MAX);
        let mut out: *mut RawConn = ptr::null_mut();
        // SAFETY: `ip` is NUL-terminated, `limits` outlives the call, `out` is a valid out-pointer.
        let rc = unsafe { rfs_rdma_client_create(ip.as_ptr(), peer.port(), timeout_ms, limits, &mut out) };
        if rc < 0 {
            let source = io::Error::from_raw_os_error(-rc);
            return Err(match source.kind() {
                io::ErrorKind::TimedOut => RdmaError::ConnectTimeout {
                    peer: peer.to_string(),
                    timeout,
                },
                io::ErrorKind::ConnectionRefused => RdmaError::Rejected { peer: peer.to_string() },
                _ => RdmaError::Connect {
                    peer: peer.to_string(),
                    source,
                },
            });
        }
        let raw = NonNull::new(out).ok_or_else(|| RdmaError::Protocol("client connection handle missing".to_owned()))?;
        Ok(Self {
            raw,
            peer: peer.to_string(),
        })
    }

    pub fn peer(&self) -> &str {
        &self.peer
    }

    pub fn start_connect(&self) -> Result<(), RdmaError> {
        // SAFETY: `raw` is live until `Drop`.
        let rc = unsafe { rfs_rdma_conn_connect(self.raw.as_ptr()) };
        if rc < 0 { Err(os_error("connect", rc)) } else { Ok(()) }
    }

    pub fn start_accept(&self) -> Result<(), RdmaError> {
        // SAFETY: `raw` is live until `Drop`.
        let rc = unsafe { rfs_rdma_conn_accept(self.raw.as_ptr()) };
        if rc < 0 { Err(os_error("accept", rc)) } else { Ok(()) }
    }

    pub fn cm_fd(&self) -> RawFd {
        // SAFETY: `raw` is live until `Drop`.
        unsafe { rfs_rdma_conn_cm_fd(self.raw.as_ptr()) }
    }

    pub fn comp_fd(&self) -> RawFd {
        // SAFETY: `raw` is live until `Drop`.
        unsafe { rfs_rdma_conn_comp_fd(self.raw.as_ptr()) }
    }

    /// Consumes one connection-manager event if one is pending.
    pub fn poll_cm(&self) -> Result<CmStatus, RdmaError> {
        // SAFETY: `raw` is live until `Drop`.
        let rc = unsafe { rfs_rdma_conn_poll_cm(self.raw.as_ptr()) };
        match rc {
            0 => Ok(CmStatus::None),
            1 => Ok(CmStatus::Established),
            2 => Ok(CmStatus::Disconnected),
            3 => Ok(CmStatus::Rejected),
            4 => Ok(CmStatus::Unreachable),
            5 => Ok(CmStatus::Error),
            code if code < 0 => Err(os_error("cm event", code)),
            code => Err(RdmaError::Protocol(format!("unknown cm status {code}"))),
        }
    }

    /// Drains completion events and fills `out` with completed work requests.
    pub fn poll_completions(&self, out: &mut [WorkCompletion]) -> Result<usize, RdmaError> {
        let max = c_int::try_from(out.len()).unwrap_or(c_int::MAX);
        if max == 0 {
            return Ok(0);
        }
        // SAFETY: `raw` is live and `out` has room for `max` entries.
        let rc = unsafe { rfs_rdma_conn_poll_completions(self.raw.as_ptr(), out.as_mut_ptr(), max) };
        if rc < 0 {
            return Err(os_error("poll completions", rc));
        }
        usize::try_from(rc).map_err(|_| RdmaError::Protocol("negative completion count".to_owned()))
    }

    pub fn device(&self) -> DeviceId {
        // SAFETY: `raw` is live until `Drop`; the device pointer outlives the process.
        DeviceId(unsafe { rfs_rdma_conn_device(self.raw.as_ptr()) } as usize)
    }

    pub fn post_recv(&self, lkey: u32, addr: u64, len: u32, wr_id: u64) -> Result<(), RdmaError> {
        // SAFETY: `raw` is live; the caller guarantees `addr..addr+len` lies inside a region
        // registered with `lkey` that outlives the work request.
        let rc = unsafe { rfs_rdma_conn_post_recv(self.raw.as_ptr(), lkey, addr, len, wr_id) };
        if rc < 0 { Err(os_error("post recv", rc)) } else { Ok(()) }
    }

    pub fn post_send(&self, lkey: u32, addr: u64, len: u32, wr_id: u64) -> Result<(), RdmaError> {
        // SAFETY: as for `post_recv`.
        let rc = unsafe { rfs_rdma_conn_post_send(self.raw.as_ptr(), lkey, addr, len, wr_id) };
        if rc < 0 { Err(os_error("post send", rc)) } else { Ok(()) }
    }

    pub fn post_write(&self, lkey: u32, addr: u64, len: u32, remote_addr: u64, rkey: u32, wr_id: u64) -> Result<(), RdmaError> {
        // SAFETY: as for `post_recv`; the remote window was handed out by the peer for this stream.
        let rc = unsafe { rfs_rdma_conn_post_write(self.raw.as_ptr(), lkey, addr, len, remote_addr, rkey, wr_id) };
        if rc < 0 { Err(os_error("post write", rc)) } else { Ok(()) }
    }

    /// Moves the queue pair to the error state so no further DMA can touch any
    /// buffer this connection referenced. Safe to call more than once.
    pub fn abort(&self) {
        // SAFETY: `raw` is live until `Drop`; the shim tolerates repeated aborts.
        unsafe { rfs_rdma_conn_abort(self.raw.as_ptr()) };
    }

    /// Registers `len` bytes at `addr` with this connection's device.
    ///
    /// The caller keeps the mapping alive for the whole lifetime of the returned region.
    pub fn register(&self, addr: *mut u8, len: usize) -> Result<MemoryRegion, RdmaError> {
        MemoryRegion::register(self.device(), addr, len)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // SAFETY: the handle was created by the shim and is released exactly once here, after
        // every `AsyncFd` that borrowed its descriptors has been dropped (see `RdmaConn`).
        unsafe { rfs_rdma_conn_close(self.raw.as_ptr()) };
    }
}

/// A memory region registered with one device's protection domain.
pub struct MemoryRegion {
    raw: NonNull<RawMr>,
    lkey: u32,
    rkey: u32,
}

// SAFETY: a memory region is an immutable handle after registration.
unsafe impl Send for MemoryRegion {}
unsafe impl Sync for MemoryRegion {}

impl MemoryRegion {
    pub fn register(device: DeviceId, addr: *mut u8, len: usize) -> Result<Self, RdmaError> {
        let mut out: *mut RawMr = ptr::null_mut();
        // SAFETY: `device` was produced by `Connection::device` for a live device context and the
        // caller guarantees `addr..addr+len` is a mapped range that outlives the region.
        let rc = unsafe { rfs_rdma_device_reg_mr(device.0 as *mut RawDevice, addr.cast::<c_void>(), len, &mut out) };
        if rc < 0 {
            return Err(os_error("register memory", rc));
        }
        let raw = NonNull::new(out).ok_or_else(|| RdmaError::Protocol("memory region handle missing".to_owned()))?;
        // SAFETY: `raw` is a live region handle.
        let (lkey, rkey) = unsafe { (rfs_rdma_mr_lkey(raw.as_ptr()), rfs_rdma_mr_rkey(raw.as_ptr())) };
        Ok(Self { raw, lkey, rkey })
    }

    pub fn lkey(&self) -> u32 {
        self.lkey
    }

    pub fn rkey(&self) -> u32 {
        self.rkey
    }
}

impl Drop for MemoryRegion {
    fn drop(&mut self) {
        // SAFETY: the handle was created by the shim and is released exactly once here.
        unsafe { rfs_rdma_mr_dereg(self.raw.as_ptr()) };
    }
}

/// Copies `data` into a mapped region at `offset`, bounds-checked against `len`.
pub fn copy_into_region(base: usize, len: usize, offset: usize, data: &[u8]) -> Result<(), RdmaError> {
    let end = offset
        .checked_add(data.len())
        .filter(|end| *end <= len)
        .ok_or_else(|| RdmaError::Protocol("buffer write exceeds region".to_owned()))?;
    debug_assert!(end <= len);
    // SAFETY: `base..base+len` is a live anonymous mapping owned by the caller, the destination
    // range was bounds-checked above, and `data` cannot alias the mapping because the mapping is
    // never exposed as a slice outside this module.
    unsafe { ptr::copy_nonoverlapping(data.as_ptr(), (base + offset) as *mut u8, data.len()) };
    Ok(())
}

/// Copies `count` bytes out of a mapped region at `offset` into owned bytes.
pub fn copy_from_region(base: usize, len: usize, offset: usize, count: usize) -> Result<Bytes, RdmaError> {
    offset
        .checked_add(count)
        .filter(|end| *end <= len)
        .ok_or_else(|| RdmaError::Protocol("buffer read exceeds region".to_owned()))?;
    // SAFETY: the range was bounds-checked against a live mapping, and the protocol guarantees
    // the device finished writing this range before the owner reads it (the peer's data message
    // is delivered after its RDMA write on the same reliable connection).
    let slice = unsafe { std::slice::from_raw_parts((base + offset) as *const u8, count) };
    Ok(Bytes::copy_from_slice(slice))
}

/// A descriptor owned by a shim object (completion channel, event channel).
///
/// The owner keeps the descriptor open for as long as the handle is registered
/// with the reactor; `RdmaConn` and the server's bound listener enforce that by
/// dropping the `AsyncFd` before the owning shim object.
pub struct RawFdHandle(pub(super) RawFd);

impl AsRawFd for RawFdHandle {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

impl AsFd for RawFdHandle {
    fn as_fd(&self) -> BorrowedFd<'_> {
        // SAFETY: the descriptor stays open for the lifetime of the handle (see the type docs).
        unsafe { BorrowedFd::borrow_raw(self.0) }
    }
}

/// Registers a shim-owned descriptor with the Tokio reactor for readiness notifications.
pub fn async_fd(op: &'static str, fd: RawFd) -> Result<AsyncFd<RawFdHandle>, RdmaError> {
    // SAFETY: the descriptor is valid and stays open until the returned `AsyncFd` is dropped,
    // which its owners guarantee by field order (see `RawFdHandle`).
    unsafe { AsyncFd::register(RawFdHandle(fd)) }.map_err(|error| RdmaError::Os {
        op,
        source: error.into_parts().1,
    })
}

/// Borrows `count` bytes of a mapped region at `offset`.
///
/// The caller guarantees the device does not write the range while the borrow
/// lives: a receive slot is uncredited, a staging slot is locally owned.
pub fn region_slice<'a>(base: usize, len: usize, offset: usize, count: usize) -> Result<&'a [u8], RdmaError> {
    offset
        .checked_add(count)
        .filter(|end| *end <= len)
        .ok_or_else(|| RdmaError::Protocol("buffer borrow exceeds region".to_owned()))?;
    // SAFETY: the range was bounds-checked against a live mapping that outlives every borrow
    // (slots hold the pool), and the protocol keeps the device away from it (see above).
    Ok(unsafe { std::slice::from_raw_parts((base + offset) as *const u8, count) })
}

/// Mutably borrows `count` bytes of a mapped region at `offset`; same contract as [`region_slice`].
pub fn region_slice_mut<'a>(base: usize, len: usize, offset: usize, count: usize) -> Result<&'a mut [u8], RdmaError> {
    offset
        .checked_add(count)
        .filter(|end| *end <= len)
        .ok_or_else(|| RdmaError::Protocol("buffer borrow exceeds region".to_owned()))?;
    // SAFETY: as for `region_slice`; the caller holds the only `Slot` for this range, so no
    // other Rust reference to it exists while the mutable borrow lives.
    Ok(unsafe { std::slice::from_raw_parts_mut((base + offset) as *mut u8, count) })
}
