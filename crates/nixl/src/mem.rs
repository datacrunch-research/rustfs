//! Page-aligned host memory handed to NIXL for registration. The unsafe
//! surface of the crate lives here: raw allocation, disjoint range views and
//! the memory-region traits the binding needs.

#![allow(unsafe_code)]

use crate::NixlError;
use nixl_sys::{MemType, MemoryRegion, NixlDescriptor};
use std::alloc::Layout;
use std::ptr::NonNull;

/// Page-aligned host memory registered with the agent.
pub(crate) struct HostBuffer {
    ptr: NonNull<u8>,
    layout: Layout,
}

// SAFETY: the buffer is plain memory; callers hand out disjoint ranges.
unsafe impl Send for HostBuffer {}
unsafe impl Sync for HostBuffer {}

impl std::fmt::Debug for HostBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostBuffer").field("len", &self.layout.size()).finish()
    }
}

impl HostBuffer {
    pub(crate) fn new(len: usize) -> Result<Self, NixlError> {
        let layout = Layout::from_size_align(len.max(1), 4096).map_err(|_| NixlError::Os {
            op: "staging layout",
            source: std::io::Error::new(std::io::ErrorKind::InvalidInput, "buffer length"),
        })?;
        // SAFETY: the layout has a non-zero size.
        let raw = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).ok_or_else(|| NixlError::Os {
            op: "staging allocation",
            source: std::io::Error::from(std::io::ErrorKind::OutOfMemory),
        })?;
        Ok(Self { ptr, layout })
    }

    pub(crate) fn len(&self) -> usize {
        self.layout.size()
    }

    pub(crate) fn addr(&self) -> usize {
        self.ptr.as_ptr() as usize
    }

    pub(crate) fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub(crate) fn check(&self, offset: usize, len: usize) -> Result<(), NixlError> {
        if offset.checked_add(len).is_none_or(|end| end > self.len()) {
            return Err(NixlError::Token("staging range outside the buffer".to_owned()));
        }
        Ok(())
    }

    pub(crate) fn slice(&self, offset: usize, len: usize) -> Result<&[u8], NixlError> {
        self.check(offset, len)?;
        // SAFETY: the range is inside the allocation; callers own disjoint ranges.
        Ok(unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().add(offset), len) })
    }

    #[allow(clippy::mut_from_ref, reason = "chunk ranges are disjoint and owned by one chunk at a time")]
    pub(crate) fn slice_mut(&self, offset: usize, len: usize) -> Result<&mut [u8], NixlError> {
        self.check(offset, len)?;
        // SAFETY: as above; the caller holds the only handle to this range.
        Ok(unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr().add(offset), len) })
    }

    pub(crate) fn write(&self, offset: usize, data: &[u8]) -> Result<(), NixlError> {
        self.slice_mut(offset, data.len())?.copy_from_slice(data);
        Ok(())
    }
}

impl Drop for HostBuffer {
    fn drop(&mut self) {
        // SAFETY: allocated with this layout in `new`.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

impl MemoryRegion for HostBuffer {
    unsafe fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    fn size(&self) -> usize {
        self.len()
    }
}

impl NixlDescriptor for HostBuffer {
    fn mem_type(&self) -> MemType {
        MemType::Dram
    }

    fn device_id(&self) -> u64 {
        0
    }
}
