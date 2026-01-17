use crate::bitmap::AllocMap;
use crate::buffer::BufferPool;
use crate::buffer::frame::{BufferFrame, FrameKind};
use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::{BufferPage, INVALID_PAGE_ID, IOKind, PAGE_SIZE, Page, PageID, PageIO};
use crate::buffer::util::{
    init_bf_exclusive_guard, madvise_dontneed, mmap_allocate, mmap_deallocate,
};
use crate::error::Validation::Valid;
use crate::error::{Error, Result, Validation};
use crate::file::SparseFile;
use crate::io::{
    AIOClient, AIOContext, AIOEventListener, AIOEventLoop, AIOKey, AIOKind, AIOStats, IOQueue,
    UnsafeAIO,
};
use crate::latch::{GuardState, LatchFallbackMode};
use crate::lifetime::StaticLifetime;
use crate::ptr::UnsafePtr;
use crate::thread;
use byte_unit::Byte;
use event_listener::{Event, Listener, listener};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::mem;
use std::ops::{Range, RangeFrom, RangeTo};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

pub const SAFETY_PAGES: usize = 10;
const EVICT_CHECK_INTERVAL: Duration = Duration::from_secs(1);
// min buffer pool size is 64KB * 128 = 8MB.
const MIN_IN_MEM_PAGES: usize = 128;

/// EvictableBufferPool is a buffer pool which can evict
/// pages to disk.
pub struct EvictableBufferPool {
    // Continuous memory area of frames.
    frames: BufferFrames,
    // Continuous memory area of pages.
    pages: *mut Page,
    // Takes care of page allocation and deallocation.
    alloc_map: AllocMap,
    // Event to notify allocating new page is available.
    alloc_ev: Event,
    // IO client to send IO requests.
    io_client: AIOClient<PoolRequest>,
    // IO thread handle.
    io_thread: Mutex<Option<JoinHandle<()>>>,
    // shutdown flag.
    shutdown_flag: Arc<AtomicBool>,
    // In-memory page set.
    in_mem: Arc<InMemPageSet>,
    // Inflight IO map.
    inflight_io: Arc<InflightIO>,
    // statistics of IO submit/wait.
    stats: Arc<EvictableBufferPoolStats>,
}

impl EvictableBufferPool {
    /// Create a new listener to handle IO events on given file.
    #[inline]
    pub fn new_listener(&self, file_io: SingleFileIO) -> EvictableBufferPoolListener {
        EvictableBufferPoolListener {
            inflight_io: Arc::clone(&self.inflight_io),
            in_mem: Arc::clone(&self.in_mem),
            stats: Arc::clone(&self.stats),
            file_io,
            prev_queuing: 0,
            prev_running: 0,
        }
    }

    #[inline]
    pub fn new_evictor(&self) -> BufferPoolEvictor {
        BufferPoolEvictor {
            frames: BufferFrames(self.frames.0),
            in_mem: Arc::clone(&self.in_mem),
            io_client: self.io_client.clone(),
            inflight_io: Arc::clone(&self.inflight_io),
            shutdown_flag: Arc::clone(&self.shutdown_flag),
        }
    }

    /// Returns AIO statistics.
    #[inline]
    pub fn stats(&self) -> &EvictableBufferPoolStats {
        &self.stats
    }

    #[inline]
    async fn try_wait_for_io_write(&self, page_id: PageID) {
        self.inflight_io
            .wait_for_write(page_id, unsafe { self.frames.frame(page_id) })
            .await
    }

    /// Try to dispatch read IO on given page.
    /// This method may not succeed, and client should retry.
    #[allow(clippy::await_holding_lock)]
    #[inline]
    async fn try_dispatch_io_read(&self, page_id: PageID) {
        // Use sync lock because the critical section is small.
        // make sure do not await when holding the guard.
        let mut g = self.inflight_io.map.lock();
        match g.entry(page_id) {
            Entry::Vacant(vac) => {
                // First thread to initialize IO read.
                // Try lock page for IO.
                match self.frames.try_lock_page_exclusive(page_id) {
                    // If page lock can not be acquired, we just retry.
                    None => (),
                    Some(page_guard) => {
                        // Page is locked, so we dispatch read request to IO thread.
                        if !self.in_mem.try_inc() {
                            // we do not have memory budget to load the page.
                            listener!(self.in_mem.load_ev => listener);
                            // re-check
                            if !self.in_mem.try_inc() {
                                // still no budget.
                                // we can only wait for signal and retry.
                                // before waiting, notify evict thread to work.
                                // this event may be ignored if evict thread is busy.
                                self.in_mem.evict_ev.notify(1);
                                // explicitly drop guards before await
                                drop(page_guard);
                                drop(g);
                                listener.await;
                                self.in_mem.load_ev.notify(1); // notify next reader to retry.
                                return; // now retry.
                            }
                        }
                        // we have memory budget now.
                        let event = Event::new();
                        let listener = event.listen();
                        vac.insert(IOStatus {
                            kind: IOKind::Read,
                            event: Some(event),
                            page_guard: None, // will update later
                        });
                        self.inflight_io.reads.fetch_add(1, Ordering::AcqRel);
                        let _ = self
                            .io_client
                            .send_async(PoolRequest::Read(page_guard))
                            .await;
                        drop(g); // explicitly drop guard before await
                        listener.await;
                    }
                }
            }
            Entry::Occupied(mut occ) => {
                let status = occ.get_mut();
                match status.kind {
                    IOKind::Read | IOKind::ReadWaitForWrite => {
                        // Wait for existing signal.
                        let listener = status.event.as_ref().unwrap().listen();
                        drop(g); // explicitly drop guard before await
                        listener.await;
                    }
                    IOKind::Write => {
                        // Write IO in progress
                        let event = Event::new();
                        let listener = event.listen();
                        status.event = Some(event);
                        drop(g); // explicitly drop guard before await
                        listener.await;
                    }
                }
            }
        }
    }

    #[inline]
    fn start_io_thread(&self, event_loop: AIOEventLoop<PoolRequest>, file_io: SingleFileIO) {
        let listener = self.new_listener(file_io);
        let io_thread = event_loop.start_thread(listener);
        let mut g = self.io_thread.lock();
        *g = Some(io_thread);
    }

    #[inline]
    fn start_evict_thread(&self) {
        let handle = self.new_evictor().start_thread();
        let mut g = self.in_mem.evict_thread.lock();
        *g = Some(handle);
    }

    /// Reserve a page in memory.
    /// If this function returns true, in-mem page counter is incremented.
    /// Caller should handle the failure of add a page into memory and decrease this number.
    #[inline]
    async fn reserve_page(&self) {
        if self.in_mem.try_inc() {
            return;
        }
        // Slow path. Wait for other to release memory.
        loop {
            listener!(self.in_mem.load_ev => listener);
            if self.in_mem.try_inc() {
                self.in_mem.load_ev.notify(1);
                return;
            }
            // Notify evictor thread to work.
            self.in_mem.evict_ev.notify(1);
            listener.await;
        }
    }
}

impl BufferPool for EvictableBufferPool {
    #[inline]
    fn capacity(&self) -> usize {
        self.alloc_map.len()
    }

    #[inline]
    fn allocated(&self) -> usize {
        self.alloc_map.allocated()
    }

    #[inline]
    async fn allocate_page<T: BufferPage>(&'static self) -> PageExclusiveGuard<T> {
        loop {
            self.reserve_page().await;

            // Now we have memory budget to allocate new page.
            match self.alloc_map.try_allocate() {
                Some(page_id) => {
                    self.in_mem.pin(page_id as PageID);
                    return unsafe { self.frames.init_page(page_id as PageID) };
                }
                None => {
                    listener!(self.alloc_ev => listener);
                    // re-check
                    if let Some(page_id) = self.alloc_map.try_allocate() {
                        self.in_mem.pin(page_id as PageID);
                        return unsafe { self.frames.init_page(page_id as PageID) };
                    }

                    // Here we cannot find a free page to load, we should cancel reservation of a page
                    // and retry.
                    self.in_mem.dec();
                    listener.await;
                }
            }
        }
    }

    #[inline]
    async fn allocate_page_at<T: BufferPage>(
        &'static self,
        page_id: PageID,
    ) -> Result<PageExclusiveGuard<T>> {
        self.reserve_page().await;

        if self.alloc_map.allocate_at(page_id as usize) {
            self.in_mem.pin(page_id);
            Ok(unsafe { self.frames.init_page(page_id) })
        } else {
            self.in_mem.dec();
            Err(Error::BufferPageAlreadyAllocated)
        }
    }

    #[inline]
    async fn get_page<T: BufferPage>(
        &'static self,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> FacadePageGuard<T> {
        loop {
            unsafe {
                let bf = self.frames.frame_ptr(page_id);
                let frame = &mut *bf.0;
                match frame.kind() {
                    FrameKind::Uninitialized => {
                        panic!("get an uninitialized page");
                    }
                    FrameKind::Fixed | FrameKind::Hot => {
                        let g = frame.latch.optimistic_fallback(mode).await;
                        return FacadePageGuard::new(bf, g);
                    }
                    FrameKind::Cool => {
                        // Try to mark this page as HOT.
                        if frame.compare_exchange_kind(FrameKind::Cool, FrameKind::Hot)
                            != FrameKind::Cool
                        {
                            // This page is going to be evicted. we have to retry and probably wait.
                            continue;
                        }
                        let g = frame.latch.optimistic_fallback(mode).await;
                        return FacadePageGuard::new(bf, g);
                    }
                    FrameKind::Evicting => {
                        // The page is marked evicting in order to be evicted to disk in near future.
                        // Here we do not break the write operation,
                        // Instead, we wait for it to complete.
                        // And then, reload it in memory.
                        self.try_wait_for_io_write(page_id).await;
                    }
                    FrameKind::Evicted => {
                        // The page is already on disk.
                        // This means we should let one thread to exclusively lock this page
                        // and initiate a IO read.
                        // Other threads can wait for the initiator to finish.
                        self.try_dispatch_io_read(page_id).await;
                    }
                }
            }
        }
    }

    #[inline]
    fn deallocate_page<T: BufferPage>(&'static self, mut g: PageExclusiveGuard<T>) {
        let page_id = g.page_id();
        g.page_mut().zero(); // zero the page
        T::deinit_frame(g.bf_mut());
        self.in_mem.unpin(page_id);
        self.in_mem.mark_page_dontneed(g);
        self.alloc_map.deallocate(page_id as usize);
        self.alloc_ev.notify(usize::MAX);
    }

    #[inline]
    async fn get_child_page<T>(
        &'static self,
        p_guard: &FacadePageGuard<T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Validation<FacadePageGuard<T>> {
        loop {
            unsafe {
                let bf = self.frames.frame_ptr(page_id);
                let frame = &mut *bf.0;
                match frame.kind() {
                    FrameKind::Uninitialized => {
                        panic!("get an uninitialized page");
                    }
                    FrameKind::Fixed | FrameKind::Hot => {
                        let g = frame.latch.optimistic_fallback(mode).await;
                        // apply lock coupling.
                        // the validation make sure parent page does not change until child
                        // page is acquired.
                        if p_guard.validate_bool() {
                            return Valid(FacadePageGuard::new(bf, g));
                        }
                        if g.state == GuardState::Exclusive {
                            g.rollback_exclusive_bit();
                        }
                        return Validation::Invalid;
                    }
                    FrameKind::Cool => {
                        let g = frame.latch.optimistic_fallback(mode).await;
                        // Try to mark this page as HOT.
                        if frame.compare_exchange_kind(FrameKind::Cool, FrameKind::Hot)
                            != FrameKind::Cool
                        {
                            // This page is going to be evicted.
                            continue;
                        }
                        // apply lock coupling.
                        // the validation make sure parent page does not change until child
                        // page is acquired.
                        return p_guard
                            .validate()
                            .and_then(|_| Valid(FacadePageGuard::new(bf, g)));
                    }
                    FrameKind::Evicting => {
                        // The page is being evicted to disk.
                        self.try_wait_for_io_write(page_id).await;
                    }
                    FrameKind::Evicted => {
                        // The page is already on disk.
                        // This means we should let one thread to exclusively lock this page
                        // and initiate a IO read.
                        // Other threads can wait for the initiator to finish.
                        self.try_dispatch_io_read(page_id).await;
                    }
                }
            }
        }
    }
}

impl Drop for EvictableBufferPool {
    #[inline]
    fn drop(&mut self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);

        // Close in-mem page set and stop evictor thread.
        self.in_mem.close();

        // Stop IO thread.
        self.io_client.shutdown();
        {
            let mut g = self.io_thread.lock();
            let handle = g.take().unwrap();
            drop(g);
            handle.join().unwrap();
        }

        unsafe {
            // Drop all frames.
            for page_id in 0..self.capacity() {
                let frame_ptr = self.frames.0.add(page_id);
                std::ptr::drop_in_place(frame_ptr);
            }

            // Deallocate memory of frames.
            let frame_total_bytes =
                mem::size_of::<BufferFrame>() * (self.capacity() + SAFETY_PAGES);
            mmap_deallocate(self.frames.0 as *mut u8, frame_total_bytes);
            // Deallocate memory of pages.
            let page_total_bytes = mem::size_of::<Page>() * (self.capacity() + SAFETY_PAGES);
            mmap_deallocate(self.pages as *mut u8, page_total_bytes);
        }
    }
}

unsafe impl Send for EvictableBufferPool {}

unsafe impl Sync for EvictableBufferPool {}

unsafe impl StaticLifetime for EvictableBufferPool {}

struct BufferFrames(*mut BufferFrame);

impl BufferFrames {
    #[inline]
    unsafe fn frame_ptr(&self, page_id: PageID) -> UnsafePtr<BufferFrame> {
        unsafe {
            let bf_ptr = self.0.offset(page_id as isize);
            UnsafePtr(bf_ptr)
        }
    }

    #[inline]
    unsafe fn frame(&self, page_id: PageID) -> &BufferFrame {
        unsafe {
            let bf_ptr = self.frame_ptr(page_id);
            &*bf_ptr.0
        }
    }

    #[inline]
    fn frame_kind(&self, page_id: PageID) -> FrameKind {
        unsafe {
            let bf_ptr = self.0.offset(page_id as isize);
            (*bf_ptr).kind()
        }
    }

    #[inline]
    unsafe fn init_page<T: BufferPage>(&'static self, page_id: PageID) -> PageExclusiveGuard<T> {
        unsafe {
            let bf = self.frame_ptr(page_id);
            let frame = &mut *bf.0;
            T::init_frame(frame);
            frame.next_free = INVALID_PAGE_ID;
            let mut guard = init_bf_exclusive_guard::<T>(bf);
            guard.page_mut().zero();
            guard
        }
    }

    #[inline]
    fn compare_exchange_frame_kind(
        &self,
        page_id: PageID,
        old_kind: FrameKind,
        new_kind: FrameKind,
    ) -> FrameKind {
        unsafe {
            let bf_ptr = self.0.offset(page_id as isize);
            (*bf_ptr).compare_exchange_kind(old_kind, new_kind)
        }
    }

    #[inline]
    fn try_lock_page_exclusive(&self, page_id: PageID) -> Option<PageExclusiveGuard<Page>> {
        unsafe {
            let bf = self.frame_ptr(page_id);
            let frame = &mut *bf.0;
            frame
                .latch
                .try_exclusive()
                .map(|g| FacadePageGuard::new(bf, g).must_exclusive())
        }
    }
}

pub struct EvictableBufferPoolListener {
    inflight_io: Arc<InflightIO>,
    in_mem: Arc<InMemPageSet>,
    stats: Arc<EvictableBufferPoolStats>,
    file_io: SingleFileIO,
    prev_queuing: usize,
    prev_running: usize,
}

impl AIOEventListener for EvictableBufferPoolListener {
    type Request = PoolRequest;
    type Submission = PageIO;

    #[inline]
    fn on_request(&mut self, req: PoolRequest, queue: &mut IOQueue<PageIO>) {
        match req {
            PoolRequest::Read(mut page_guard) => {
                let page_id = page_guard.page_id();
                debug_assert!(self.inflight_io.contains(page_id));
                debug_assert!(page_guard.bf().kind() == FrameKind::Evicted);
                let uio = self.file_io.prepare_read(page_id, page_guard.page_mut());
                let iocb = uio.iocb().load(Ordering::Relaxed);
                let sub = PageIO {
                    page_guard,
                    kind: IOKind::Read,
                };
                queue.push(iocb, sub);
            }
            PoolRequest::BatchWrite(page_guards) => {
                for mut page_guard in page_guards {
                    let page_id = page_guard.page_id();
                    debug_assert!(self.inflight_io.contains(page_id));
                    debug_assert!(page_guard.bf().kind() == FrameKind::Evicting);
                    let uio = self.file_io.prepare_write(page_id, page_guard.page_mut());
                    let iocb = uio.iocb().load(Ordering::Relaxed);
                    let req = PageIO {
                        page_guard,
                        kind: IOKind::Write,
                    };
                    queue.push(iocb, req);
                }
            }
        }
    }

    #[inline]
    fn on_submit(&mut self, sub: PageIO) {
        // attach page guard to inflight map so
        // other thread can not acquire exlusive lock when
        // IO is being performed.
        let res = self.inflight_io.attach_page_guard(sub.page_guard);
        debug_assert!(res);
    }

    #[inline]
    fn on_complete(&mut self, key: AIOKey, res: std::io::Result<usize>) -> AIOKind {
        match res {
            Ok(len) => {
                let page_id = key;
                // Page IO always succeeds with exact page size.
                debug_assert!(len == PAGE_SIZE);
                let mut g = self.inflight_io.map.lock();
                let mut status = g.remove(&page_id).unwrap();
                let page_guard = status.page_guard.take().unwrap();
                let bf = page_guard.bf();
                match status.kind {
                    IOKind::Read => {
                        debug_assert!(bf.kind() == FrameKind::Evicted);
                        bf.set_dirty(false);
                        bf.compare_exchange_kind(FrameKind::Evicted, FrameKind::Hot);
                        let event = status.event.take();
                        self.inflight_io.reads.fetch_sub(1, Ordering::Relaxed);
                        self.in_mem.pin(page_id);
                        drop(page_guard);
                        if let Some(event) = event {
                            event.notify(usize::MAX);
                        }
                        drop(g);
                        AIOKind::Read
                    }
                    IOKind::Write => {
                        debug_assert!(bf.kind() == FrameKind::Evicting);
                        let event = status.event.take();
                        self.inflight_io.writes.fetch_sub(1, Ordering::Relaxed);
                        self.in_mem.evict_page(page_guard);
                        if let Some(event) = event {
                            event.notify(usize::MAX);
                        }
                        drop(g);
                        AIOKind::Write
                    }
                    IOKind::ReadWaitForWrite => {
                        // Write request will overwrite it to IOKind::Write.
                        unreachable!()
                    }
                }
            }
            Err(err) => {
                unimplemented!("AIO error: page_id={}, {}", key, err)
            }
        }
    }

    #[inline]
    fn on_batch_complete(&mut self, _read_count: usize, write_count: usize) {
        if write_count > 0 {
            // notify readers which wait for any inflight write.
            self.inflight_io.writes_finish_ev.notify(1);
        }
    }

    #[inline]
    fn on_stats(&mut self, stats: &AIOStats) {
        if stats.queuing != self.prev_queuing {
            self.stats.queuing.store(stats.queuing, Ordering::Release);
            self.prev_queuing = stats.queuing;
        }
        if stats.running != self.prev_running {
            self.stats.running.store(stats.running, Ordering::Release);
            self.prev_running = stats.running;
        }
        if stats.finished_reads != 0 {
            self.stats
                .finished_reads
                .fetch_add(stats.finished_reads, Ordering::Relaxed);
        }
        if stats.finished_writes != 0 {
            self.stats
                .finished_writes
                .fetch_add(stats.finished_writes, Ordering::Relaxed);
        }
        if stats.io_submit_count != 0 {
            self.stats
                .io_submit_calls
                .fetch_add(stats.io_submit_count, Ordering::Relaxed);
            self.stats
                .io_submit_nanos
                .fetch_add(stats.io_submit_nanos, Ordering::Relaxed);
        }
        if stats.io_wait_count != 0 {
            self.stats
                .io_wait_calls
                .fetch_add(stats.io_wait_count, Ordering::Relaxed);
            self.stats
                .io_wait_nanos
                .fetch_add(stats.io_wait_nanos, Ordering::Relaxed);
        }
    }

    #[inline]
    fn end_loop(self, _ctx: &AIOContext) {
        // do nothing
    }
}

pub struct BufferPoolEvictor {
    frames: BufferFrames,
    in_mem: Arc<InMemPageSet>,
    inflight_io: Arc<InflightIO>,
    io_client: AIOClient<PoolRequest>,
    shutdown_flag: Arc<AtomicBool>,
}

impl BufferPoolEvictor {
    #[inline]
    pub fn start_thread(self) -> JoinHandle<()> {
        thread::spawn_named("BufferPoolEvictor", move || self.run())
    }

    #[inline]
    fn run(self) {
        const EVICT_BATCH: usize = 64;
        const SMALL_BATCH: usize = 8;
        // Used to store temporary page ids to avoid lock in-mem page set too long.
        let mut tmp_page_ids = vec![];
        let mut evict_candidates = vec![];
        let mut clock_hand = ClockHand::default();
        loop {
            if self.shutdown_flag.load(Ordering::Acquire) {
                return;
            }

            let mut batch_size = match self.in_mem.pages_to_evict() {
                Some(n) => n.min(EVICT_BATCH),
                None => {
                    listener!(self.in_mem.evict_ev => listener);
                    // re-check
                    match self.in_mem.pages_to_evict() {
                        Some(n) => n.min(EVICT_BATCH),
                        None => {
                            // Because we don't specify wait timeout,
                            // we need to check shutdown flag in case
                            // the event is missed.
                            if self.shutdown_flag.load(Ordering::Acquire) {
                                return;
                            }
                            // Before waiting, check if there are any free space
                            // for page loading. And let readers to continue.
                            let in_mem_count = self.in_mem.count.load(Ordering::Acquire);
                            if in_mem_count < self.in_mem.max_count {
                                self.in_mem.load_ev.notify(1);
                            }
                            listener.wait();
                            continue;
                        }
                    }
                }
            };

            // There might be IO writes in-progress and queued,
            // we should ignore these numbers.
            batch_size = batch_size.saturating_sub(self.inflight_io.writes.load(Ordering::Relaxed));
            if batch_size == 0 {
                // Unnecessary to evict more pages because many page evictions are in progress.
                listener!(self.in_mem.evict_ev => listener);
                // Always wait a moment and re-check if eviction is required.
                listener.wait_timeout(EVICT_CHECK_INTERVAL);
                continue;
            }

            // Start eviction.
            let mut next_ch = None;
            // Iterate over all pages twice.
            'SWEEP: for _ in 0..2 {
                next_ch = Some(clock_hand.clone());
                while let Some(ch) = next_ch.take() {
                    if batch_size <= SMALL_BATCH {
                        next_ch = self.in_mem.clock_collect(ch, |page_id| {
                            if let Some(page_guard) = self.clock_sweep(page_id) {
                                evict_candidates.push(page_guard);
                                batch_size -= 1;
                                batch_size == 0
                            } else {
                                false
                            }
                        });
                    } else {
                        tmp_page_ids.clear();
                        next_ch = self.in_mem.clock_collect(ch, |page_id| {
                            tmp_page_ids.push(page_id);
                            tmp_page_ids.len() == batch_size
                        });
                        for page_id in tmp_page_ids.drain(..) {
                            if let Some(page_guard) = self.clock_sweep(page_id) {
                                evict_candidates.push(page_guard);
                                batch_size -= 1;
                            }
                        }
                    }
                    if batch_size == 0 {
                        break 'SWEEP;
                    }
                }
            }

            // Send all evict candidates to IO thread
            if evict_candidates.is_empty() {
                // Because we don't find any evict candidate, we can wait sometime
                // to avoid busy loop
                listener!(self.in_mem.evict_ev => listener);
                listener.wait_timeout(EVICT_CHECK_INTERVAL);
                continue;
            }
            // If the page is not dirty, we can directly drop it.
            // This will largely reduce IO pressure.
            let mut page_guards = vec![];
            for page_guard in mem::take(&mut evict_candidates) {
                if page_guard.is_dirty() {
                    page_guards.push(page_guard);
                } else {
                    // Page is not dirty means it's a read-only copy of the data on disk.
                    // So we can directly remove it from memory without IO write.
                    // But concurrent readers may rely on the status updated in inflight
                    // IO map. So we lock the map when updating frame kind.
                    let g = self.inflight_io.map.lock();
                    debug_assert!(page_guard.bf().kind() == FrameKind::Evicting);
                    self.in_mem.evict_page(page_guard);
                    drop(g);
                }
            }

            if page_guards.is_empty() {
                continue;
            }

            listener!(self.inflight_io.writes_finish_ev => writes_finish);
            self.dispatch_io_writes(page_guards);

            // To void busy we always wait for at least one write finish.
            // Because we don't specify wait timeout,
            // we need to check shutdown flag in case
            // the event is missed.
            if self.shutdown_flag.load(Ordering::Acquire) {
                return;
            }
            writes_finish.wait();

            // update clock hand.
            if let Some(mut ch) = next_ch {
                ch.reset();
                clock_hand = ch;
            } else {
                clock_hand.reset();
            }
        }
    }

    #[inline]
    fn dispatch_io_writes(&self, page_guards: Vec<PageExclusiveGuard<Page>>) {
        self.inflight_io.batch_writes(&page_guards);
        let _ = self.io_client.send(PoolRequest::BatchWrite(page_guards));
    }

    #[inline]
    fn clock_sweep(&self, page_id: PageID) -> Option<PageExclusiveGuard<Page>> {
        match self.frames.frame_kind(page_id) {
            FrameKind::Uninitialized
            | FrameKind::Fixed
            | FrameKind::Evicting
            | FrameKind::Evicted => (),
            FrameKind::Cool => {
                // Acquire exclusive lock first, in order to block other thread to access
                // this page at the same time. If fails, we just skip.
                if let Some(page_guard) = self.frames.try_lock_page_exclusive(page_id) {
                    if page_guard
                        .bf()
                        .compare_exchange_kind(FrameKind::Cool, FrameKind::Evicting)
                        != FrameKind::Cool
                    {
                        // Some other thread access this page and mark it as hot, so we skip it
                        return None;
                    }
                    return Some(page_guard);
                }
            }
            FrameKind::Hot => {
                // follow clock-sweep strategy, mark it as cool.
                let _ = self.frames.compare_exchange_frame_kind(
                    page_id,
                    FrameKind::Hot,
                    FrameKind::Cool,
                );
            }
        }
        None
    }
}

unsafe impl Send for BufferPoolEvictor {}

struct InMemPageSet {
    // Current page number held in memory.
    count: AtomicUsize,
    // Maximum page number held in memory.
    max_count: usize,
    // Evict threshold.
    evict_threshold: usize,
    // Ordered page id set, used for evict thread.
    set: Mutex<BTreeSet<PageID>>,
    // Event to notify thread that loading a page is available.
    load_ev: Event,
    // Event to notify evictor thread to choose page candidates,
    // write to disk and release memory.
    evict_ev: Event,
    evict_thread: Mutex<Option<JoinHandle<()>>>,
}

impl InMemPageSet {
    #[inline]
    fn new(max_count: usize) -> Self {
        const EVICT_THRESHOLD: f64 = 0.9;
        let evict_threshold = (max_count as f64 * EVICT_THRESHOLD) as usize;
        InMemPageSet {
            count: AtomicUsize::new(0),
            max_count,
            evict_threshold,
            set: Mutex::new(BTreeSet::new()),
            load_ev: Event::new(),
            evict_ev: Event::new(),
            evict_thread: Mutex::new(None),
        }
    }

    // Try to increment in-mem page count.
    // fail if exceeds limit.
    #[inline]
    fn try_inc(&self) -> bool {
        let mut curr_count = self.count.load(Ordering::Acquire);
        loop {
            if curr_count >= self.max_count {
                return false;
            }
            match self.count.compare_exchange(
                curr_count,
                curr_count + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(n) => curr_count = n,
            }
        }
    }

    /// Decrement in-mem page count.
    #[inline]
    fn dec(&self) {
        let count = self.count.fetch_sub(1, Ordering::AcqRel);
        if count == self.max_count {
            self.load_ev.notify(1);
        }
    }

    /// Returns number of pages which would be evicted.
    #[inline]
    fn pages_to_evict(&self) -> Option<usize> {
        let curr_count = self.count.load(Ordering::Acquire);
        if curr_count < MIN_IN_MEM_PAGES / 2 {
            // do not evict if page count is too small.
            return None;
        } else if curr_count > self.evict_threshold {
            return Some(curr_count - self.evict_threshold);
        }
        None
    }

    /// Pin given page in memory.
    /// Use mlock() to prevent page to swap out to disk.
    #[inline]
    fn pin(&self, page_id: PageID) {
        // assert!(mlock(ptr as *mut u8, PAGE_SIZE));
        let mut g = self.set.lock();
        g.insert(page_id);
    }

    #[inline]
    fn unpin(&self, page_id: PageID) {
        // assert!(munlock(ptr as *mut u8, PAGE_SIZE));
        let mut g = self.set.lock();
        g.remove(&page_id);
    }

    #[inline]
    fn evict_page(&self, mut g: PageExclusiveGuard<Page>) {
        let page_id = g.page_id();
        g.page_mut().zero(); // zero the page
        let old_kind = g
            .bf_mut()
            .compare_exchange_kind(FrameKind::Evicting, FrameKind::Evicted);
        debug_assert!(old_kind == FrameKind::Evicting);
        self.unpin(page_id);
        self.mark_page_dontneed(g);
    }

    /// Reclaim memory for given page.
    /// This method is invoked after page eviction.
    #[inline]
    fn mark_page_dontneed<T: BufferPage>(&self, mut page_guard: PageExclusiveGuard<T>) {
        unsafe {
            assert!(madvise_dontneed(
                page_guard.page_mut() as *mut T as *mut u8,
                PAGE_SIZE
            ));
        }
        drop(page_guard);
        self.dec();
    }

    #[inline]
    fn clock_collect<F: FnMut(PageID) -> bool>(
        &self,
        clock_hand: ClockHand,
        mut callback: F,
    ) -> Option<ClockHand> {
        let g = self.set.lock();
        match clock_hand {
            ClockHand::From(from) => {
                let mut range = g.range(from);
                while let Some(page_id) = range.next() {
                    // page_ids.push(*page_id);
                    let stop = callback(*page_id);
                    if stop {
                        // check if we have next page id
                        if let Some(page_id) = range.next() {
                            return Some(ClockHand::From(*page_id..));
                        }
                        // all pages exhausted
                        return None;
                    }
                }
                // whole buffer pool exhausted but still not enough pages.
                None
            }
            ClockHand::FromTo(from, to) => {
                let mut range = g.range(from);
                while let Some(page_id) = range.next() {
                    let stop = callback(*page_id);
                    if stop {
                        // check if we have next page id
                        if let Some(page_id) = range.next() {
                            return Some(ClockHand::FromTo(*page_id.., to));
                        }
                        return Some(ClockHand::To(to));
                    }
                }
                range = g.range(to);
                while let Some(page_id) = range.next() {
                    let stop = callback(*page_id);
                    if stop {
                        // check if we have next page id
                        if let Some(page_id) = range.next() {
                            return Some(ClockHand::Between(*page_id..to.end));
                        }
                        return None;
                    }
                }
                None
            }
            ClockHand::To(to) => {
                let mut range = g.range(to);
                while let Some(page_id) = range.next() {
                    let stop = callback(*page_id);
                    if stop {
                        // check if we have next page id
                        if let Some(page_id) = range.next() {
                            return Some(ClockHand::Between(*page_id..to.end));
                        }
                        return None;
                    }
                }
                None
            }
            ClockHand::Between(between) => {
                let mut range = g.range(between.clone());
                while let Some(page_id) = range.next() {
                    let stop = callback(*page_id);
                    if stop {
                        // check if we have next page id
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

    #[inline]
    fn close(&self) {
        // notify evict thread to quit.
        self.evict_ev.notify(1);
        {
            let mut g = self.evict_thread.lock();
            if let Some(handle) = g.take() {
                handle.join().unwrap();
            }
        }
    }
}

pub struct SingleFileIO {
    file: SparseFile,
}

impl SingleFileIO {
    #[inline]
    fn new(file: SparseFile) -> Self {
        SingleFileIO { file }
    }

    #[inline]
    fn prepare_read(&self, page_id: PageID, ptr: *mut Page) -> UnsafeAIO {
        unsafe {
            self.file.pread_unchecked(
                page_id,
                page_id as usize * PAGE_SIZE,
                ptr as *mut u8,
                PAGE_SIZE,
            )
        }
    }

    #[inline]
    fn prepare_write(&self, page_id: PageID, ptr: *mut Page) -> UnsafeAIO {
        unsafe {
            self.file.pwrite_unchecked(
                page_id,
                page_id as usize * PAGE_SIZE,
                ptr as *mut u8,
                PAGE_SIZE,
            )
        }
    }
}

const DEFAULT_FILE_PATH: &str = "databuffer.bin";
const DEFAULT_MAX_FILE_SIZE: Byte = Byte::from_u64(2 * 1024 * 1024 * 1024); // by default 2GB
const DEFAULT_MAX_MEM_SIZE: Byte = Byte::from_u64(1024 * 1024 * 1024); // by default 1GB
const DEFAULT_MAX_IO_DEPTH: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictableBufferPoolConfig {
    file_path: String,
    max_file_size: Byte,
    max_mem_size: Byte,
    max_io_depth: usize,
}

impl Default for EvictableBufferPoolConfig {
    #[inline]
    fn default() -> Self {
        EvictableBufferPoolConfig {
            file_path: String::from(DEFAULT_FILE_PATH),
            max_file_size: DEFAULT_MAX_FILE_SIZE,
            max_mem_size: DEFAULT_MAX_MEM_SIZE,
            max_io_depth: DEFAULT_MAX_IO_DEPTH,
        }
    }
}

impl EvictableBufferPoolConfig {
    #[inline]
    pub fn with_main_dir(mut self, main_dir: impl AsRef<Path>) -> Self {
        let path = main_dir.as_ref().join(&self.file_path);
        self.file_path = path.to_string_lossy().to_string();
        self
    }

    #[inline]
    pub fn file_path(mut self, file_path: impl Into<String>) -> Self {
        self.file_path = file_path.into();
        self
    }

    #[inline]
    pub fn max_file_size<T>(mut self, max_file_size: T) -> Self
    where
        Byte: From<T>,
    {
        self.max_file_size = Byte::from(max_file_size);
        self
    }

    #[inline]
    pub fn max_mem_size<T>(mut self, max_mem_size: T) -> Self
    where
        Byte: From<T>,
    {
        self.max_mem_size = Byte::from(max_mem_size);
        self
    }

    #[inline]
    pub fn max_io_depth(mut self, max_io_depth: usize) -> Self {
        self.max_io_depth = max_io_depth;
        self
    }

    #[inline]
    pub fn build(self) -> Result<EvictableBufferPool> {
        // 1. Calculate memory usage.
        let max_file_size = self.max_file_size.as_u64() as usize;
        let max_mem_size = self.max_mem_size.as_u64() as usize;
        let max_nbr = max_file_size / mem::size_of::<Page>();
        // We need to hold all frames in-memory, even if many pages
        // can not be loaded into memory at the same time.
        let frame_total_bytes = mem::size_of::<BufferFrame>() * (max_nbr + SAFETY_PAGES);
        assert!(
            max_mem_size <= max_file_size,
            "max mem size of buffer pool should be no more than max file size"
        );
        assert!(
            max_mem_size > frame_total_bytes,
            "max mem size of buffer pool can not hold all buffer frames"
        );
        let max_nbr_in_mem = (max_mem_size - frame_total_bytes) / mem::size_of::<Page>();
        if max_nbr_in_mem < MIN_IN_MEM_PAGES {
            return Err(Error::BufferPoolSizeTooSmall);
        }

        // We allocate a much larger memory area to support 1:1 file page mapping.
        // And buffer pool will control memory usage within limit.
        let page_total_bytes = mem::size_of::<Page>() * (max_nbr + SAFETY_PAGES);

        // 2. Initialize memory of frames and pages.
        let frames = unsafe { mmap_allocate(frame_total_bytes)? } as *mut BufferFrame;
        let pages = unsafe {
            match mmap_allocate(page_total_bytes) {
                Ok(ptr) => ptr,
                Err(e) => {
                    // cleanup previous allocated memory
                    mmap_deallocate(frames as *mut u8, frame_total_bytes);
                    return Err(e);
                }
            }
        } as *mut Page;

        // 3. Create file and initialize AIO manager.
        let io_ctx = AIOContext::new(self.max_io_depth)?;
        let (event_loop, io_client) = io_ctx.event_loop();

        let file = SparseFile::create_or_trunc(&self.file_path, max_file_size)?;
        let file_io = SingleFileIO::new(file);

        // 4. Initialize frames.
        // NOTE: we need to initialize all frames, not only maximum number that can be held in memory.
        unsafe {
            for i in 0..max_nbr {
                let f_ptr = frames.add(i);
                std::ptr::write(f_ptr, BufferFrame::default());
                let frame = &mut *f_ptr;
                frame.page_id = i as PageID;
                frame.page = pages.add(i);
                frame.next_free = i as PageID + 1;
            }
            // Update last frame's next_free
            (*frames.add(max_nbr - 1)).next_free = INVALID_PAGE_ID;
        }

        let pool = EvictableBufferPool {
            frames: BufferFrames(frames),
            pages,
            alloc_map: AllocMap::new(max_nbr),
            alloc_ev: Event::new(),
            io_client,
            io_thread: Mutex::new(None),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            in_mem: Arc::new(InMemPageSet::new(max_nbr_in_mem)),
            inflight_io: Arc::new(InflightIO::default()),
            stats: Arc::new(EvictableBufferPoolStats::default()),
        };

        // Start background IO thread.
        pool.start_io_thread(event_loop, file_io);
        // Start background evictor thread.
        pool.start_evict_thread();
        Ok(pool)
    }

    /// Create a new evictable buffer pool with given capacity.
    #[inline]
    pub fn build_static(self) -> Result<&'static EvictableBufferPool> {
        let pool = self.build()?;
        let pool = StaticLifetime::new_static(pool);
        Ok(pool)
    }
}

struct IOStatus {
    kind: IOKind,
    event: Option<Event>,
    page_guard: Option<PageExclusiveGuard<Page>>,
}

struct InflightIO {
    map: Mutex<HashMap<PageID, IOStatus>>,
    // Signal to notify map change on IO writes.
    // writes_submit_signal: Signal,
    writes_finish_ev: Event,
    reads: AtomicUsize,
    writes: AtomicUsize,
}

impl Default for InflightIO {
    #[inline]
    fn default() -> Self {
        InflightIO {
            map: Mutex::new(HashMap::new()),
            // writes_submit_signal: Signal::default(),
            writes_finish_ev: Event::new(),
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
        }
    }
}

impl InflightIO {
    #[allow(clippy::await_holding_lock)]
    #[inline]
    async fn wait_for_write(&self, page_id: PageID, frame: &BufferFrame) {
        let mut g = self.map.lock();
        match g.entry(page_id) {
            Entry::Vacant(vac) => {
                // Check whether the page is done.
                match frame.kind() {
                    FrameKind::Evicting => {
                        // Evict thread marked it as evicting, but for some reason, does
                        // not process it immediately.
                        // Here we insert a ReadWaitForWrite entry.
                        // And wait for the event
                        let event = Event::new();
                        let listener = event.listen();
                        vac.insert(IOStatus {
                            kind: IOKind::ReadWaitForWrite,
                            event: Some(event),
                            page_guard: None, // will update later.
                        });
                        drop(g); // explicit drop guard before await.
                        listener.await;
                    }
                    // In any other kind, we let caller retry.
                    FrameKind::Cool
                    | FrameKind::Hot
                    | FrameKind::Fixed
                    | FrameKind::Uninitialized
                    | FrameKind::Evicted => (),
                }
            }
            Entry::Occupied(mut occ) => {
                // There is a write in progress, or a preceding read, waiting for write.
                // In both cases, we can just wait for the event.
                debug_assert!({
                    let kind = occ.get().kind;
                    kind == IOKind::ReadWaitForWrite || kind == IOKind::Write
                });
                let event = occ.get_mut().event.get_or_insert_default();
                let listener = event.listen();
                drop(g); // explicitly drop guard before await.
                listener.await;
            }
        }
    }

    #[inline]
    fn batch_writes(&self, page_guards: &[PageExclusiveGuard<Page>]) {
        let mut g = self.map.lock();
        let count = page_guards.len();
        for page_guard in page_guards {
            let page_id = page_guard.page_id();
            match g.entry(page_id) {
                Entry::Vacant(vac) => {
                    vac.insert(IOStatus {
                        kind: IOKind::Write,
                        event: None,
                        page_guard: None, // will update later.
                    });
                }
                Entry::Occupied(mut occ) => {
                    // There could be concurrent read requests.
                    let status = occ.get_mut();
                    debug_assert!(status.kind == IOKind::ReadWaitForWrite);
                    status.kind = IOKind::Write;
                }
            }
        }
        self.writes.fetch_add(count, Ordering::AcqRel);
    }

    #[inline]
    fn attach_page_guard(&self, page_guard: PageExclusiveGuard<Page>) -> bool {
        let page_id = page_guard.page_id();
        let mut g = self.map.lock();
        if let Some(status) = g.get_mut(&page_id) {
            let res = status.page_guard.replace(page_guard);
            debug_assert!(res.is_none());
            return true;
        }
        false
    }

    #[inline]
    fn contains(&self, page_id: PageID) -> bool {
        let g = self.map.lock();
        g.contains_key(&page_id)
    }
}

#[derive(Default)]
pub struct EvictableBufferPoolStats {
    running: AtomicUsize,
    queuing: AtomicUsize,
    finished_reads: AtomicUsize,
    finished_writes: AtomicUsize,
    io_submit_calls: AtomicUsize,
    io_submit_nanos: AtomicUsize,
    io_wait_calls: AtomicUsize,
    io_wait_nanos: AtomicUsize,
}

#[derive(Debug, Clone)]
enum ClockHand {
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
    /// Returns start pointer of clock hand.
    #[inline]
    fn start(&self) -> PageID {
        match self {
            ClockHand::Between(between) => between.start,
            ClockHand::From(from) => from.start,
            ClockHand::To(_) => 0,
            ClockHand::FromTo(from, _) => from.start,
        }
    }

    #[inline]
    fn reset(&mut self) {
        let start = self.start();
        if start == 0 {
            *self = ClockHand::default()
        } else {
            *self = ClockHand::FromTo(start.., ..start)
        }
    }
}

pub enum PoolRequest {
    Read(PageExclusiveGuard<Page>),
    BatchWrite(Vec<PageExclusiveGuard<Page>>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::RowPage;
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn test_evict_buffer_pool_simple() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let pool = EvictableBufferPoolConfig::default()
                .with_main_dir(temp_dir.path())
                .max_mem_size(1024u64 * 1024 * 128)
                .max_file_size(1024u64 * 1024 * 256)
                .build_static()
                .unwrap();
            {
                let g = pool.allocate_page::<RowPage>().await;
                assert_eq!(g.page_id(), 0);
            }
            {
                let g = pool.allocate_page::<RowPage>().await;
                assert_eq!(g.page_id(), 1);
                pool.deallocate_page(g);
                let g = pool.allocate_page::<RowPage>().await;
                assert_eq!(g.page_id(), 1);
                drop(g);
            }
            {
                let g = pool
                    .get_page::<RowPage>(0, LatchFallbackMode::Spin)
                    .await
                    .downgrade();
                assert_eq!(g.page_id(), 0);
                let p = g.facade();
                // test coupling.
                let c = pool
                    .get_child_page::<RowPage>(&p, 1, LatchFallbackMode::Exclusive)
                    .await;
                let c = c.unwrap();
                drop(c);
            }
            {
                let g = pool
                    .get_page::<RowPage>(0, LatchFallbackMode::Spin)
                    .await
                    .downgrade();
                assert_eq!(g.page_id(), 0);
                let p = g.facade();

                // modify page 0.
                let g = pool
                    .get_page::<RowPage>(0, LatchFallbackMode::Exclusive)
                    .await
                    .verify_exclusive_async::<false>()
                    .await;
                drop(g.unwrap());

                // test coupling fail.
                let c = pool
                    .get_child_page::<RowPage>(&p, 1, LatchFallbackMode::Exclusive)
                    .await;
                assert!(c.is_invalid());
            }
            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }

    #[test]
    fn test_evict_buffer_pool_full() {
        // 100 in-mem pages and 200 total pages.
        let temp_dir = TempDir::new().unwrap();
        let pool: &EvictableBufferPool = EvictableBufferPoolConfig::default()
            .with_main_dir(temp_dir.path())
            .max_mem_size(64u64 * 1024 * 130)
            .max_file_size(128u64 * 1024 * 130)
            .build_static()
            .unwrap();

        let (tx, rx) = flume::unbounded();
        let handle1 = {
            thread::spawn(move || {
                smol::block_on(async move {
                    // allocate more pages than memory limit.
                    for i in 0..160 {
                        let g = pool.allocate_page::<RowPage>().await;
                        let _ = tx.send(g.page_id());
                        println!("allocated page {}", i);
                    }
                    drop(tx);
                })
            })
        };

        thread::sleep(Duration::from_millis(50));
        println!("wait sometime");
        smol::block_on(async move {
            while let Ok(page_id) = rx.recv() {
                let g = pool
                    .get_page::<RowPage>(page_id, LatchFallbackMode::Exclusive)
                    .await
                    .exclusive_async()
                    .await;
                pool.deallocate_page(g);
                println!("deallocated page {}", page_id);
            }
        });

        handle1.join().unwrap();

        unsafe {
            StaticLifetime::drop_static(pool);
        }
    }

    #[test]
    fn test_evict_buffer_pool_alloc() {
        // max pages 16k, max in-mem 1k
        let temp_dir = TempDir::new().unwrap();
        let pool = EvictableBufferPoolConfig::default()
            .with_main_dir(temp_dir.path())
            .max_mem_size(1024u64 * 1024 * 64)
            .max_file_size(1024u64 * 1024 * 128)
            .build_static()
            .unwrap();

        println!(
            "max_nbr={}, max_nbr_in_mem={}",
            pool.capacity(),
            pool.in_mem.max_count
        );
        smol::block_on(async {
            let mut pages = vec![];
            for _ in 0..2048 {
                let g = pool.allocate_page::<RowPage>().await;
                pages.push(g.page_id());
            }
            debug_assert!(pages.len() == 2048);
        });
    }

    #[test]
    fn test_evict_buffer_pool_multi_threads() {
        use rand::{Rng, prelude::IndexedRandom};
        // max pages 2k, max in-mem 1k
        let temp_dir = TempDir::new().unwrap();
        let pool = EvictableBufferPoolConfig::default()
            .with_main_dir(temp_dir.path())
            .max_mem_size(64u64 * 1024 * 1024)
            .max_file_size(64u64 * 1024 * 2048)
            .build_static()
            .unwrap();

        println!(
            "max_nbr={}, max_nbr_in_mem={}",
            pool.capacity(),
            pool.in_mem.max_count
        );
        let mut handles = vec![];
        for thread_id in 0..10 {
            let handle = thread::spawn(move || {
                smol::block_on(async {
                    let mut rng = rand::rng();

                    let mut pages = vec![];
                    for _ in 0..200 {
                        // allocate a new page.
                        println!("thread {} alloc page start", thread_id);
                        let g = pool.allocate_page::<RowPage>().await;
                        pages.push(g.page_id());
                        println!(
                            "thread {} alloc page end page_id {}, allocated {}, in-mem {}, threshold {}, reads {}, writes {}",
                            thread_id,
                            g.page_id(),
                            pool.allocated(),
                            pool.in_mem.count.load(Ordering::Relaxed),
                            pool.in_mem.evict_threshold,
                            pool.inflight_io.reads.load(Ordering::Relaxed),
                            pool.inflight_io.writes.load(Ordering::Relaxed),
                        );
                        // unlock the page.
                        drop(g);
                        if rng.random_bool(0.3) {
                            // choose one page and try to lock it.
                            let page_id = pages.choose(&mut rng).cloned().unwrap();
                            println!(
                                "thread {} read page {}, allocated {}, in-mem {}, threshold {}, reads {}, writes {}",
                                thread_id,
                                page_id,
                                pool.allocated(),
                                pool.in_mem.count.load(Ordering::Relaxed),
                                pool.in_mem.evict_threshold,
                                pool.inflight_io.reads.load(Ordering::Relaxed),
                                pool.inflight_io.writes.load(Ordering::Relaxed),
                            );
                            let g = pool
                                .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                                .await;
                            println!("thread {} read page {} end", thread_id, page_id);
                            drop(g);
                        }
                    }
                    println!("thread {} done", thread_id);
                })
            });
            handles.push(handle);
        }
        for h in handles {
            h.join().unwrap();
        }
        unsafe {
            StaticLifetime::drop_static(pool);
        }
    }
}
