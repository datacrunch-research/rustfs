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

// This module is the only caller of the C shim in `csrc/cuobj_shim.cpp`. Every pointer
// handed to the shim comes from a live owner (`Server`, `RegisteredBuffer`) released once
// in `Drop`; the registered host buffer is allocated by the library and never freed while
// a registration references it.
// SAFETY: per-call invariants are documented on each `unsafe` block below.
#![allow(unsafe_code)]

use super::CuObjectError;
use std::ffi::{CString, c_char, c_int, c_void};
use std::net::IpAddr;
use std::ptr::{self, NonNull};

#[repr(C)]
pub struct RawServer {
    _private: [u8; 0],
}

#[repr(C)]
pub struct RawBuffer {
    _private: [u8; 0],
}

unsafe extern "C" {
    fn rfs_cuobj_server_create(
        ip: *const c_char,
        port: u16,
        num_dcis: c_int,
        cq_depth: u32,
        max_sge: c_int,
        out: *mut *mut RawServer,
    ) -> c_int;
    fn rfs_cuobj_server_destroy(server: *mut RawServer);
    fn rfs_cuobj_alloc_host(server: *mut RawServer, size: usize) -> *mut c_void;
    fn rfs_cuobj_register(server: *mut RawServer, ptr: *mut c_void, size: usize, out: *mut *mut RawBuffer) -> c_int;
    fn rfs_cuobj_deregister(server: *mut RawServer, buffer: *mut RawBuffer);
    fn rfs_cuobj_sg_buffer(
        server: *mut RawServer,
        base: *mut RawBuffer,
        entries: *const SgEntry,
        count: usize,
        out: *mut *mut RawBuffer,
    ) -> c_int;
    fn rfs_cuobj_sg_buffer_put(server: *mut RawServer, sg: *mut RawBuffer);
    fn rfs_cuobj_channel_alloc(server: *mut RawServer, out: *mut u16) -> c_int;
    fn rfs_cuobj_channel_free(server: *mut RawServer, channel: u16);
    fn rfs_cuobj_get(
        server: *mut RawServer,
        key: *const c_char,
        buffer: *mut RawBuffer,
        local_offset: u64,
        remote_addr: u64,
        size: usize,
        descriptor: *const c_char,
        channel: u16,
        verbs_status: *mut c_int,
    ) -> isize;
    fn rfs_cuobj_put(
        server: *mut RawServer,
        key: *const c_char,
        buffer: *mut RawBuffer,
        local_offset: u64,
        remote_addr: u64,
        size: usize,
        descriptor: *const c_char,
        channel: u16,
        verbs_status: *mut c_int,
    ) -> isize;
    fn rfs_cuobj_version() -> c_int;
}

fn os_error(op: &'static str, code: c_int) -> CuObjectError {
    CuObjectError::Os {
        op,
        source: std::io::Error::from_raw_os_error(-code),
    }
}

/// Version of the linked cuObject server library as (major, minor).
pub fn library_version() -> (u32, u32) {
    // SAFETY: a pure function of compile-time constants.
    let packed = unsafe { rfs_cuobj_version() };
    let packed = u32::try_from(packed).unwrap_or(0);
    (packed / 1000, packed % 1000)
}

pub struct Server {
    raw: NonNull<RawServer>,
}

// SAFETY: the library documents the server object as thread-safe when callers use distinct
// channels; `Server` hands out channels per operation.
unsafe impl Send for Server {}
unsafe impl Sync for Server {}

impl Server {
    pub fn create(addr: IpAddr, port: u16, num_dcis: i32, cq_depth: u32) -> Result<Self, CuObjectError> {
        let ip = CString::new(addr.to_string()).map_err(|_| CuObjectError::Token("address contains NUL".to_owned()))?;
        let mut out: *mut RawServer = ptr::null_mut();
        // SAFETY: `ip` is NUL-terminated and `out` is a valid out-pointer.
        let rc = unsafe { rfs_cuobj_server_create(ip.as_ptr(), port, num_dcis, cq_depth, 0, &mut out) };
        if rc < 0 {
            return Err(os_error("server create", rc));
        }
        NonNull::new(out)
            .map(|raw| Self { raw })
            .ok_or_else(|| CuObjectError::Token("server handle missing".to_owned()))
    }

    /// Allocates and registers a page-aligned host buffer owned by the library.
    pub fn register_host_buffer(&self, len: usize) -> Result<RegisteredBuffer, CuObjectError> {
        // SAFETY: `raw` is live until `Drop`.
        let ptr = unsafe { rfs_cuobj_alloc_host(self.raw.as_ptr(), len) };
        if ptr.is_null() {
            return Err(os_error("alloc host buffer", -libc_enomem()));
        }
        let mut out: *mut RawBuffer = ptr::null_mut();
        // SAFETY: `ptr` is a live allocation of `len` bytes from the library and `out` is valid.
        let rc = unsafe { rfs_cuobj_register(self.raw.as_ptr(), ptr, len, &mut out) };
        if rc < 0 {
            return Err(os_error("register buffer", rc));
        }
        let raw = NonNull::new(out).ok_or_else(|| CuObjectError::Token("buffer handle missing".to_owned()))?;
        Ok(RegisteredBuffer {
            raw,
            base: ptr as usize,
            len,
            server: self.raw,
        })
    }

    /// Builds a scatter-gather transfer handle over `entries` of `base`.
    pub fn sg_buffer(&self, base: &RegisteredBuffer, entries: &[SgEntry]) -> Result<SgBuffer, CuObjectError> {
        if entries.is_empty() {
            return Err(CuObjectError::Token("empty scatter-gather list".to_owned()));
        }
        let mut total = 0usize;
        for entry in entries {
            if entry
                .offset
                .checked_add(entry.size as u64)
                .is_none_or(|end| end > base.len as u64)
            {
                return Err(CuObjectError::Token("scatter-gather entry exceeds the registered buffer".to_owned()));
            }
            total = total
                .checked_add(entry.size)
                .ok_or_else(|| CuObjectError::Token("scatter-gather length overflow".to_owned()))?;
        }
        let mut out: *mut RawBuffer = ptr::null_mut();
        // SAFETY: `base` is a live registration of this server and `entries` is a valid slice.
        let rc = unsafe { rfs_cuobj_sg_buffer(self.raw.as_ptr(), base.raw.as_ptr(), entries.as_ptr(), entries.len(), &mut out) };
        if rc < 0 {
            return Err(os_error("scatter-gather buffer", rc));
        }
        let raw = NonNull::new(out).ok_or_else(|| CuObjectError::Token("scatter-gather handle missing".to_owned()))?;
        Ok(SgBuffer {
            raw,
            len: total,
            server: self.raw,
        })
    }

    /// Blocking RDMA write of all ranges of `sg`, in order, to `remote_addr`.
    pub fn get_sg(
        &self,
        key: &str,
        sg: &SgBuffer,
        remote_addr: u64,
        descriptor: &str,
        channel: u16,
    ) -> Result<u64, CuObjectError> {
        let key = CString::new(key).map_err(|_| CuObjectError::Token("key contains NUL".to_owned()))?;
        let descriptor = CString::new(descriptor).map_err(|_| CuObjectError::Token("token contains NUL".to_owned()))?;
        let mut verbs_status: c_int = 0;
        // SAFETY: all handles are live; the list's ranges were bounds-checked when it was built,
        // and the library ignores `local_offset` for a scatter-gather handle.
        let rc = unsafe {
            rfs_cuobj_get(
                self.raw.as_ptr(),
                key.as_ptr(),
                sg.raw.as_ptr(),
                0,
                remote_addr,
                sg.len,
                descriptor.as_ptr(),
                channel,
                &mut verbs_status,
            )
        };
        if rc < 0 {
            return Err(CuObjectError::Transfer {
                verbs_status,
                source: std::io::Error::from_raw_os_error(i32::try_from(-rc).unwrap_or(libc_eio())),
            });
        }
        Ok(rc as u64)
    }

    pub fn alloc_channel(&self) -> Result<u16, CuObjectError> {
        let mut channel = 0u16;
        // SAFETY: `raw` is live; `channel` is a valid out-pointer.
        let rc = unsafe { rfs_cuobj_channel_alloc(self.raw.as_ptr(), &mut channel) };
        if rc < 0 {
            return Err(os_error("channel alloc", rc));
        }
        Ok(channel)
    }

    pub fn free_channel(&self, channel: u16) {
        // SAFETY: `raw` is live; the channel came from `alloc_channel`.
        unsafe { rfs_cuobj_channel_free(self.raw.as_ptr(), channel) };
    }

    /// Blocking RDMA write of `size` bytes from `buffer + local_offset` into client memory.
    #[allow(clippy::too_many_arguments, reason = "mirrors the library call")]
    pub fn get(
        &self,
        key: &str,
        buffer: &RegisteredBuffer,
        local_offset: u64,
        remote_addr: u64,
        size: usize,
        descriptor: &str,
        channel: u16,
    ) -> Result<u64, CuObjectError> {
        let key = CString::new(key).map_err(|_| CuObjectError::Token("key contains NUL".to_owned()))?;
        let descriptor = CString::new(descriptor).map_err(|_| CuObjectError::Token("token contains NUL".to_owned()))?;
        let mut verbs_status: c_int = 0;
        // SAFETY: all handles are live; the staged range lies inside the registered buffer
        // because `local_offset + size <= buffer.len` is checked here; the remote range was
        // bounds-checked against the token by the caller.
        if local_offset
            .checked_add(size as u64)
            .is_none_or(|end| end > buffer.len as u64)
        {
            return Err(CuObjectError::Token("staged range exceeds the registered buffer".to_owned()));
        }
        let rc = unsafe {
            rfs_cuobj_get(
                self.raw.as_ptr(),
                key.as_ptr(),
                buffer.raw.as_ptr(),
                local_offset,
                remote_addr,
                size,
                descriptor.as_ptr(),
                channel,
                &mut verbs_status,
            )
        };
        if rc < 0 {
            return Err(CuObjectError::Transfer {
                verbs_status,
                source: std::io::Error::from_raw_os_error(i32::try_from(-rc).unwrap_or(libc_eio())),
            });
        }
        Ok(rc as u64)
    }

    /// Blocking RDMA read of `size` bytes from client memory into `buffer + local_offset`.
    #[allow(clippy::too_many_arguments, reason = "mirrors the library call")]
    pub fn put(
        &self,
        key: &str,
        buffer: &RegisteredBuffer,
        local_offset: u64,
        remote_addr: u64,
        size: usize,
        descriptor: &str,
        channel: u16,
    ) -> Result<u64, CuObjectError> {
        let key = CString::new(key).map_err(|_| CuObjectError::Token("key contains NUL".to_owned()))?;
        let descriptor = CString::new(descriptor).map_err(|_| CuObjectError::Token("token contains NUL".to_owned()))?;
        let mut verbs_status: c_int = 0;
        if local_offset
            .checked_add(size as u64)
            .is_none_or(|end| end > buffer.len as u64)
        {
            return Err(CuObjectError::Token("staged range exceeds the registered buffer".to_owned()));
        }
        // SAFETY: all handles are live; the destination range lies inside the registered
        // buffer (checked above) and the caller holds the only staging chunk covering it;
        // the remote range was bounds-checked against the token by the caller.
        let rc = unsafe {
            rfs_cuobj_put(
                self.raw.as_ptr(),
                key.as_ptr(),
                buffer.raw.as_ptr(),
                local_offset,
                remote_addr,
                size,
                descriptor.as_ptr(),
                channel,
                &mut verbs_status,
            )
        };
        if rc < 0 {
            return Err(CuObjectError::Transfer {
                verbs_status,
                source: std::io::Error::from_raw_os_error(i32::try_from(-rc).unwrap_or(libc_eio())),
            });
        }
        Ok(rc as u64)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // SAFETY: the handle was created by the shim and is released once, after every
        // `RegisteredBuffer` (which borrows the server pointer) has been deregistered.
        unsafe { rfs_cuobj_server_destroy(self.raw.as_ptr()) };
    }
}

/// A registered host buffer owned by the library.
/// One (offset, size) range of a registered buffer; layout matches the shim's `rfs_cuobj_sg_entry`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SgEntry {
    pub offset: u64,
    pub size: usize,
}

/// A transfer handle over several ranges of one registered buffer; released on drop.
pub struct SgBuffer {
    raw: NonNull<RawBuffer>,
    len: usize,
    server: NonNull<RawServer>,
}

unsafe impl Send for SgBuffer {}
unsafe impl Sync for SgBuffer {}

impl Drop for SgBuffer {
    fn drop(&mut self) {
        // SAFETY: both handles are live; the base registration outlives the list by construction.
        unsafe { rfs_cuobj_sg_buffer_put(self.server.as_ptr(), self.raw.as_ptr()) };
    }
}

pub struct RegisteredBuffer {
    raw: NonNull<RawBuffer>,
    base: usize,
    len: usize,
    server: NonNull<RawServer>,
}

// SAFETY: the registration is immutable; writes are bounds-checked and the caller serializes
// use of each staging chunk.
unsafe impl Send for RegisteredBuffer {}
unsafe impl Sync for RegisteredBuffer {}

impl RegisteredBuffer {
    /// Copies `data` into the buffer at `offset`, bounds-checked.
    pub fn write(&self, offset: usize, data: &[u8]) -> Result<(), CuObjectError> {
        if offset.checked_add(data.len()).is_none_or(|end| end > self.len) {
            return Err(CuObjectError::Token("staging write exceeds the registered buffer".to_owned()));
        }
        // SAFETY: the destination lies inside the live library-owned allocation and cannot alias
        // `data`, which is caller memory outside the registered buffer.
        unsafe { ptr::copy_nonoverlapping(data.as_ptr(), (self.base + offset) as *mut u8, data.len()) };
        Ok(())
    }
}

impl RegisteredBuffer {
    /// Borrows `len` staged bytes at `offset`.
    pub fn slice(&self, offset: usize, len: usize) -> Result<&[u8], CuObjectError> {
        if offset.checked_add(len).is_none_or(|end| end > self.len) {
            return Err(CuObjectError::Token("staging borrow exceeds the registered buffer".to_owned()));
        }
        // SAFETY: the range lies inside the live library-owned allocation; the caller holds
        // the only staging chunk covering it and no DMA targets it while the borrow lives.
        Ok(unsafe { std::slice::from_raw_parts((self.base + offset) as *const u8, len) })
    }

    /// Mutably borrows `len` bytes at `offset` for filling by a reader.
    ///
    /// The caller holds the only staging chunk covering this range, and no RDMA
    /// operation references it until the chunk is handed to `get`.
    // Exclusivity comes from `StagingChunk` ownership, not from the borrow of `self`.
    #[allow(clippy::mut_from_ref)]
    pub fn slice_mut(&self, offset: usize, len: usize) -> Result<&mut [u8], CuObjectError> {
        if offset.checked_add(len).is_none_or(|end| end > self.len) {
            return Err(CuObjectError::Token("staging borrow exceeds the registered buffer".to_owned()));
        }
        // SAFETY: the range lies inside the live library-owned allocation; exclusivity is
        // guaranteed by the chunk ownership described above.
        Ok(unsafe { std::slice::from_raw_parts_mut((self.base + offset) as *mut u8, len) })
    }
}

impl Drop for RegisteredBuffer {
    fn drop(&mut self) {
        // SAFETY: both handles are live; `Server` outlives its buffers by construction
        // (the server struct holds them and drops them first via field order).
        unsafe { rfs_cuobj_deregister(self.server.as_ptr(), self.raw.as_ptr()) };
    }
}

const fn libc_enomem() -> c_int {
    12
}

const fn libc_eio() -> c_int {
    5
}
