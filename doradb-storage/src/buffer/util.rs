use crate::buffer::frame::BufferFrame;
use crate::buffer::page::{Page, PageID};
use crate::error::{Error, Result};
use libc::{
    MADV_DONTFORK, MADV_DONTNEED, MADV_HUGEPAGE, MADV_REMOVE, MAP_ANONYMOUS, MAP_FAILED,
    MAP_PRIVATE, PROT_READ, PROT_WRITE, c_void, madvise, mmap, munmap,
};
use std::mem;

pub(super) const SHARED_SAFETY_PAGES: usize = 10;

/// Calculates total mmap bytes needed for frame headers with safety padding.
#[inline]
pub(super) fn frame_total_bytes(capacity: usize) -> usize {
    mem::size_of::<BufferFrame>() * (capacity + SHARED_SAFETY_PAGES)
}

#[inline]
fn page_total_bytes(capacity: usize) -> usize {
    mem::size_of::<Page>() * (capacity + SHARED_SAFETY_PAGES)
}

/// Allocates contiguous mmap regions for frame and page arrays, then initializes
/// each `BufferFrame` with its stable frame id and page pointer.
///
/// If page allocation fails, the already allocated frame region is cleaned up.
#[inline]
pub(super) unsafe fn initialize_frame_and_page_arrays(
    capacity: usize,
) -> Result<(*mut BufferFrame, *mut Page)> {
    let frame_total_bytes = frame_total_bytes(capacity);
    let page_total_bytes = page_total_bytes(capacity);
    unsafe {
        let frames = mmap_allocate(frame_total_bytes)? as *mut BufferFrame;
        let pages = match mmap_allocate(page_total_bytes) {
            Ok(ptr) => ptr as *mut Page,
            Err(err) => {
                mmap_deallocate(frames as *mut u8, frame_total_bytes);
                return Err(err);
            }
        };
        for i in 0..capacity {
            let frame_ptr = frames.add(i);
            std::ptr::write(frame_ptr, BufferFrame::default());
            (*frame_ptr).page_id = i as PageID;
            (*frame_ptr).page = pages.add(i);
        }
        Ok((frames, pages))
    }
}

/// Deallocates contiguous mmap regions previously allocated for frames/pages.
#[inline]
pub(super) unsafe fn deallocate_frame_and_page_arrays(
    frames: *mut BufferFrame,
    pages: *mut Page,
    capacity: usize,
) {
    let frame_total_bytes = frame_total_bytes(capacity);
    let page_total_bytes = page_total_bytes(capacity);
    unsafe {
        mmap_deallocate(frames as *mut u8, frame_total_bytes);
        mmap_deallocate(pages as *mut u8, page_total_bytes);
    }
}

#[inline]
pub(super) unsafe fn mmap_allocate(total_bytes: usize) -> Result<*mut u8> {
    unsafe {
        let memory_chunk = mmap(
            std::ptr::null_mut(),
            total_bytes,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS,
            -1,
            0,
        );
        if memory_chunk == MAP_FAILED {
            return Err(Error::InsufficientMemory(total_bytes));
        }
        madvise(memory_chunk, total_bytes, MADV_HUGEPAGE);
        madvise(memory_chunk, total_bytes, MADV_DONTFORK);
        Ok(memory_chunk as *mut u8)
    }
}

#[inline]
pub(super) unsafe fn mmap_deallocate(ptr: *mut u8, total_bytes: usize) {
    unsafe {
        munmap(ptr as *mut c_void, total_bytes);
    }
}

#[inline]
pub(super) unsafe fn madvise_dontneed(ptr: *mut u8, len: usize) -> bool {
    unsafe { madvise(ptr as *mut c_void, len, MADV_DONTNEED) == 0 }
}

#[allow(dead_code)]
#[inline]
pub(super) unsafe fn madvise_remove(ptr: *mut u8, len: usize) -> bool {
    unsafe { madvise(ptr as *mut c_void, len, MADV_REMOVE) == 0 }
}
