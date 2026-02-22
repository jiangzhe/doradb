use crate::buffer::frame::{BufferFrame, BufferFrames, FrameKind};
use crate::buffer::guard::PageExclusiveGuard;
use crate::buffer::page::{Page, PageID};
use crate::error::{Error, Result};
use libc::{
    MADV_DONTFORK, MADV_DONTNEED, MADV_HUGEPAGE, MADV_REMOVE, MAP_ANONYMOUS, MAP_FAILED,
    MAP_PRIVATE, PROT_READ, PROT_WRITE, c_void, madvise, mmap, munmap,
};
use std::collections::BTreeSet;
use std::mem;
use std::ops::{Range, RangeFrom, RangeTo};

pub(super) const SHARED_SAFETY_PAGES: usize = 10;

/// Shared clock hand state for clock-sweep eviction.
#[derive(Debug, Clone)]
pub(super) enum ClockHand {
    From(RangeFrom<PageID>),
    FromTo(RangeFrom<PageID>, RangeTo<PageID>),
    To(RangeTo<PageID>),
    Between(Range<PageID>),
}

impl Default for ClockHand {
    #[inline]
    fn default() -> Self {
        ClockHand::From(0..)
    }
}

impl ClockHand {
    /// Resets the hand and preserves sweep locality.
    #[inline]
    pub(super) fn reset(&mut self) {
        let start = self.start();
        if start == 0 {
            *self = ClockHand::default()
        } else {
            *self = ClockHand::FromTo(start.., ..start)
        }
    }

    #[inline]
    fn start(&self) -> PageID {
        match self {
            ClockHand::Between(between) => between.start,
            ClockHand::From(from) => from.start,
            ClockHand::To(_) => 0,
            ClockHand::FromTo(from, _) => from.start,
        }
    }
}

/// Iterates the ordered resident-set according to clock-hand position.
///
/// The callback returns true to stop scanning and preserve next hand position.
#[inline]
pub(super) fn clock_collect<F>(
    set: &BTreeSet<PageID>,
    clock_hand: ClockHand,
    mut callback: F,
) -> Option<ClockHand>
where
    F: FnMut(PageID) -> bool,
{
    match clock_hand {
        ClockHand::From(from) => {
            let mut range = set.range(from);
            while let Some(page_id) = range.next() {
                if callback(*page_id) {
                    if let Some(page_id) = range.next() {
                        return Some(ClockHand::From(*page_id..));
                    }
                    return None;
                }
            }
            None
        }
        ClockHand::FromTo(from, to) => {
            let mut range = set.range(from);
            while let Some(page_id) = range.next() {
                if callback(*page_id) {
                    if let Some(page_id) = range.next() {
                        return Some(ClockHand::FromTo(*page_id.., to));
                    }
                    return Some(ClockHand::To(to));
                }
            }
            range = set.range(to);
            while let Some(page_id) = range.next() {
                if callback(*page_id) {
                    if let Some(page_id) = range.next() {
                        return Some(ClockHand::Between(*page_id..to.end));
                    }
                    return None;
                }
            }
            None
        }
        ClockHand::To(to) => {
            let mut range = set.range(to);
            while let Some(page_id) = range.next() {
                if callback(*page_id) {
                    if let Some(page_id) = range.next() {
                        return Some(ClockHand::Between(*page_id..to.end));
                    }
                    return None;
                }
            }
            None
        }
        ClockHand::Between(between) => {
            let mut range = set.range(between.clone());
            while let Some(page_id) = range.next() {
                if callback(*page_id) {
                    if let Some(page_id) = range.next() {
                        return Some(ClockHand::Between(*page_id..between.end));
                    }
                    return None;
                }
            }
            None
        }
    }
}

/// Applies one clock-sweep step and returns a page guard when a frame
/// is selected for eviction.
#[inline]
pub(super) fn clock_sweep_candidate(
    frames: &BufferFrames,
    page_id: PageID,
) -> Option<PageExclusiveGuard<Page>> {
    match frames.frame_kind(page_id) {
        FrameKind::Uninitialized | FrameKind::Fixed | FrameKind::Evicting | FrameKind::Evicted => {
            None
        }
        FrameKind::Cool => {
            if let Some(page_guard) = frames.try_lock_page_exclusive(page_id) {
                if page_guard
                    .bf()
                    .compare_exchange_kind(FrameKind::Cool, FrameKind::Evicting)
                    != FrameKind::Cool
                {
                    return None;
                }
                return Some(page_guard);
            }
            None
        }
        FrameKind::Hot => {
            let _ = frames.compare_exchange_frame_kind(page_id, FrameKind::Hot, FrameKind::Cool);
            None
        }
    }
}

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
