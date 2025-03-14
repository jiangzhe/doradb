use crate::buffer::frame::BufferFrame;
use crate::buffer::guard::{PageExclusiveGuard, PageGuard};
use crate::buffer::page::BufferPage;
use crate::error::{Error, Result};
use crate::ptr::UnsafePtr;
use libc::{
    c_void, madvise, mmap, munmap, MADV_DONTFORK, MADV_DONTNEED, MADV_HUGEPAGE, MADV_REMOVE,
    MAP_ANONYMOUS, MAP_FAILED, MAP_PRIVATE, PROT_READ, PROT_WRITE,
};

#[inline]
pub(super) fn init_bf_exclusive_guard<T: BufferPage>(
    bf: UnsafePtr<BufferFrame>,
) -> PageExclusiveGuard<T> {
    unsafe {
        let g = (*bf.0).latch.exclusive_blocking();
        PageGuard::new(bf, g).exclusive_blocking()
    }
}

#[inline]
pub(super) unsafe fn mmap_allocate(total_bytes: usize) -> Result<*mut u8> {
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

#[inline]
pub(super) unsafe fn mmap_deallocate(ptr: *mut u8, total_bytes: usize) {
    munmap(ptr as *mut c_void, total_bytes);
}

#[inline]
pub(super) unsafe fn madvise_dontneed(ptr: *mut u8, len: usize) -> bool {
    madvise(ptr as *mut c_void, len, MADV_DONTNEED) == 0
}

#[allow(dead_code)]
#[inline]
pub(super) unsafe fn madvise_remove(ptr: *mut u8, len: usize) -> bool {
    madvise(ptr as *mut c_void, len, MADV_REMOVE) == 0
}
