use crate::buffer::frame::{BufferFrame, FrameKind};
use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::{BufferPage, IOKind, Page, PageID, PageIO, INVALID_PAGE_ID, PAGE_SIZE};
use crate::buffer::util::{
    init_bf_exclusive_guard, madvise_dontneed, mmap_allocate, mmap_deallocate, AllocMap,
};
use crate::buffer::{BufferPool, BufferRequest};
use crate::error::Validation::Valid;
use crate::error::{Error, Result, Validation};
use crate::io::{AIOManager, AIOManagerConfig, IocbRawPtr, SparseFile, UnsafeAIO};
use crate::latch::{GuardState, LatchFallbackMode};
use crate::lifetime::StaticLifetime;
use crate::ptr::UnsafePtr;
use crate::thread;
use byte_unit::Byte;
use crossbeam_utils::CachePadded;
use event_listener::{listener, Event, Listener};
use flume::{Receiver, Sender, TryRecvError};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::mem;
use std::ops::{Range, RangeFrom, RangeTo};
use std::os::fd::AsRawFd;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub const SAFETY_PAGES: usize = 10;
const EVICT_CHECK_INTERVAL: Duration = Duration::from_secs(1);
// min buffer pool size is 64KB * 512 = 32MB.
const MIN_IN_MEM_PAGES: usize = 512;

/// EvictableBufferPool is a buffer pool which can evict
/// pages to disk.
pub struct EvictableBufferPool {
    // Continuous memory area of frames.
    frames: *mut BufferFrame,
    // Continuous memory area of pages.
    pages: *mut Page,
    // In-memory page set.
    in_mem: InMemPageSet,
    // Takes care of page allocation and deallocation.
    alloc_map: AllocMap,
    // Event to notify allocating new page is available.
    alloc_ev: Event,
    // Page IO control read pages from disk and write pages
    // to disk.
    file_io: SingleFileIO,
    shutdown_flag: AtomicBool,
    inflight_io: CachePadded<InflightIO>,
    stats: CachePadded<EvictableBufferPoolStats>,
}

impl EvictableBufferPool {
    #[inline]
    pub fn start(&'static self, start_ctx: EvictableBufferPoolStartContext) {
        self.start_io_read_thread(start_ctx.io_read_rx);
        self.start_io_write_thread(start_ctx.io_write_rx);
        self.start_evict_thread();
    }

    #[inline]
    pub fn stats(&self) -> &EvictableBufferPoolStats {
        &self.stats
    }

    #[inline]
    unsafe fn frame_ptr(&self, page_id: PageID) -> UnsafePtr<BufferFrame> {
        let bf_ptr = self.frames.offset(page_id as isize);
        UnsafePtr(bf_ptr)
    }

    #[inline]
    unsafe fn frame(&self, page_id: PageID) -> &BufferFrame {
        let bf_ptr = self.frame_ptr(page_id);
        &*bf_ptr.0
    }

    #[inline]
    fn pin_page(&self, page_id: PageID) {
        unsafe { self.in_mem.pin(page_id, self.pages.add(page_id as usize)) }
    }

    #[inline]
    fn unpin_page(&self, page_id: PageID) {
        unsafe { self.in_mem.unpin(page_id, self.pages.add(page_id as usize)) }
    }

    #[inline]
    fn frame_kind(&self, page_id: PageID) -> FrameKind {
        unsafe {
            let bf_ptr = self.frames.offset(page_id as isize);
            (*bf_ptr).kind()
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
            let bf_ptr = self.frames.offset(page_id as isize);
            (*bf_ptr).compare_exchange_kind(old_kind, new_kind)
        }
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
        self.in_mem.dec();
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

    #[inline]
    async fn try_wait_for_io_write(&self, page_id: PageID) {
        self.inflight_io
            .wait_for_write(page_id, unsafe { self.frame(page_id) })
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
                match self.try_lock_page_exclusive(page_id) {
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
                        });
                        self.inflight_io.reads.fetch_add(1, Ordering::AcqRel);
                        self.file_io.request_read(page_guard);
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
    fn dispatch_io_writes(&self, page_guards: Vec<PageExclusiveGuard<Page>>) {
        self.inflight_io.batch_writes(&page_guards);
        self.file_io.request_batch_write(page_guards);
    }

    #[inline]
    fn start_io_read_thread(&'static self, io_rx: Receiver<BufferRequest>) {
        let handle = thread::spawn_named("BufferPageReader", move || self.io_loop(io_rx, false));
        let mut g = self.file_io.read_thread.lock();
        *g = Some(handle);
    }

    #[inline]
    fn start_io_write_thread(&'static self, io_rx: Receiver<BufferRequest>) {
        let handle = thread::spawn_named("BufferPageWriter", move || self.io_loop(io_rx, true));
        let mut g = self.file_io.write_thread.lock();
        *g = Some(handle);
    }

    #[inline]
    fn start_evict_thread(&'static self) {
        let handle = thread::spawn_named("BufferPoolEvictor", || self.evict_loop());
        let mut g = self.in_mem.evict_thread.lock();
        *g = Some(handle);
    }

    #[inline]
    fn prepare_io_read(&self, page_guard: &mut PageExclusiveGuard<Page>) -> UnsafeAIO {
        let page_id = page_guard.page_id();
        debug_assert!(self.inflight_io.contains(page_id));
        debug_assert!(self.frame_kind(page_id) == FrameKind::Evicted);
        self.file_io.prepare_read(page_id, page_guard.page_mut())
    }

    #[inline]
    fn prepare_io_write(&self, page_id: PageID) -> UnsafeAIO {
        // Before this IO request, the dispatcher thread should have already acquired
        // exclusive lock on this page and mark it as evicted to disable other read/write.
        debug_assert!(self.inflight_io.contains(page_id));
        debug_assert!(self.frame_kind(page_id) == FrameKind::Evicting);
        self.file_io
            .prepare_write(page_id, unsafe { self.pages.add(page_id as usize) })
    }

    #[inline]
    unsafe fn init_page<T: BufferPage>(&'static self, page_id: PageID) -> PageExclusiveGuard<T> {
        let bf = self.frame_ptr(page_id);
        let frame = &mut *bf.0;
        T::init_frame(frame);
        frame.next_free = INVALID_PAGE_ID;
        let mut guard = init_bf_exclusive_guard::<T>(bf);
        guard.page_mut().zero();
        guard
    }

    #[inline]
    fn try_fetch_reqs(&'static self, io_rx: &Receiver<BufferRequest>, queue: &mut IOQueue) -> bool {
        loop {
            match io_rx.try_recv() {
                Ok(req) => match req {
                    BufferRequest::Shutdown => {
                        return true;
                    }
                    BufferRequest::Read(mut page_guard) => {
                        let uio = self.prepare_io_read(&mut page_guard);
                        queue.iocbs.push(uio.iocb().load(Ordering::Relaxed));
                        queue.reqs.push_back(PageIO {
                            page_guard,
                            kind: IOKind::Read,
                        });
                    }
                    BufferRequest::BatchWrite(page_guards) => {
                        for page_guard in page_guards {
                            let uio = self.prepare_io_write(page_guard.page_id());
                            queue.iocbs.push(uio.iocb().load(Ordering::Relaxed));
                            queue.reqs.push_back(PageIO {
                                page_guard,
                                kind: IOKind::Write,
                            });
                        }
                    }
                },
                Err(TryRecvError::Empty) => {
                    return false;
                }
                Err(TryRecvError::Disconnected) => unreachable!(),
            }
        }
    }

    #[inline]
    fn fetch_reqs(&'static self, io_rx: &Receiver<BufferRequest>, queue: &mut IOQueue) -> bool {
        let mut req = io_rx.recv().expect("recv error");
        loop {
            loop {
                match req {
                    BufferRequest::Shutdown => {
                        return true;
                    }
                    BufferRequest::Read(mut page_guard) => {
                        let uio = self.prepare_io_read(&mut page_guard);
                        queue.iocbs.push(uio.iocb().load(Ordering::Relaxed));
                        queue.reqs.push_back(PageIO {
                            page_guard,
                            kind: IOKind::Read,
                        });
                    }
                    BufferRequest::BatchWrite(page_guards) => {
                        for page_guard in page_guards {
                            let uio = self.prepare_io_write(page_guard.page_id());
                            queue.iocbs.push(uio.iocb().load(Ordering::Relaxed));
                            queue.reqs.push_back(PageIO {
                                page_guard,
                                kind: IOKind::Write,
                            });
                        }
                    }
                }
                req = match io_rx.try_recv() {
                    Ok(r) => r,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => unreachable!(),
                }
            }
            if !queue.is_empty() {
                return false;
            }
            req = io_rx.recv().expect("recv error");
        }
    }

    #[inline]
    fn io_loop(&'static self, io_rx: Receiver<BufferRequest>, write: bool) {
        let aio_mgr = if write {
            &self.file_io.aio_writer
        } else {
            &self.file_io.aio_reader
        };
        let io_depth = aio_mgr.max_events();
        let counter = if write {
            &self.file_io.write_counter
        } else {
            &self.file_io.read_counter
        };
        // Queued IO requests.
        let mut queue = IOQueue::with_capacity(io_depth * 2);
        // IO results.
        let mut results = aio_mgr.events();
        // Current IO tasks submitted.
        let mut submitted = 0usize;
        // Store page locks locally.
        let mut page_guards = HashMap::new();
        let mut shutdown = false;
        loop {
            debug_assert!(
                queue.consistent(),
                "pending IO number equals to pending request number"
            );
            // We only accept request if shutdown flag is false.
            if !shutdown {
                if queue.len() + submitted == 0 {
                    // there is no processing AIO, so we can block on waiting for next request.
                    shutdown |= self.fetch_reqs(&io_rx, &mut queue);
                    counter.set_queued(queue.len());
                } else if queue.len() < io_depth {
                    // only try non-blocking way to fetch incoming requests, because we also
                    // need to finish previous IO.
                    shutdown |= self.try_fetch_reqs(&io_rx, &mut queue);
                    counter.set_queued(queue.len());
                }
            }
            // Event if shutdown flag is set to true, we still process queued requests.
            let (io_submit_count, io_submit_nanos) = if !queue.is_empty() {
                let start = Instant::now();
                // Try to submit as many IO requests as possible
                let limit = io_depth - submitted;
                let submit_count = aio_mgr.submit_limit(&mut queue.iocbs, limit);
                // Add requests to inflight tree.
                for req in queue.reqs.drain(..submit_count) {
                    let res = page_guards.insert(req.page_guard.page_id(), req.page_guard);
                    debug_assert!(res.is_none());
                }
                debug_assert!(queue.consistent());
                submitted += submit_count;
                counter.set_submitted(submitted);
                counter.set_queued(queue.len());
                debug_assert!(counter.submitted() <= io_depth);
                (1, start.elapsed().as_nanos() as usize)
            } else {
                (0, 0)
            };

            // wait for any request to be done.
            // Note: even if we received shutdown message, we should wait all submitted IO finish before quiting.
            // This will prevent kernel from accessing a freed memory via async IO processing.
            let (io_wait_count, io_wait_nanos) = if submitted != 0 {
                let start = Instant::now();
                let finish_count =
                    aio_mgr.wait_at_least(&mut results, 1, |page_id, res| match res {
                        Ok(len) => {
                            // Page IO always succeeds with exact page size.
                            debug_assert!(len == PAGE_SIZE);
                            let mut page_guard = page_guards.remove(&page_id).unwrap();
                            let bf = page_guard.bf_mut();
                            let mut g = self.inflight_io.map.lock();
                            let mut status = g.remove(&page_id).unwrap();
                            match status.kind {
                                IOKind::Read => {
                                    debug_assert!(bf.kind() == FrameKind::Evicted);
                                    bf.set_dirty(false);
                                    bf.compare_exchange_kind(FrameKind::Evicted, FrameKind::Hot);
                                    let event = status.event.take();
                                    self.inflight_io.reads.fetch_sub(1, Ordering::Relaxed);
                                    self.pin_page(page_id);
                                    drop(page_guard);
                                    if let Some(event) = event {
                                        event.notify(usize::MAX);
                                    }
                                    drop(g);
                                }
                                IOKind::Write => {
                                    debug_assert!(bf.kind() == FrameKind::Evicting);
                                    let event = status.event.take();
                                    self.inflight_io.writes.fetch_sub(1, Ordering::Relaxed);
                                    self.evict_page(page_guard);
                                    if let Some(event) = event {
                                        event.notify(usize::MAX);
                                    }
                                    drop(g);
                                }
                                IOKind::ReadWaitForWrite => {
                                    // Write request will overwrite it to IOKind::Write.
                                    unreachable!()
                                }
                            }
                        }
                        Err(err) => {
                            unimplemented!("AIO error: page_id={}, {}", page_id, err)
                        }
                    });
                if write {
                    // IO writes finish, should notify evict thread.
                    self.inflight_io.writes_finish_ev.notify(1);
                    self.file_io.write_counter.add_finished(finish_count);
                } else {
                    self.file_io.read_counter.add_finished(finish_count);
                }
                submitted -= finish_count;
                counter.set_submitted(submitted);
                (1, start.elapsed().as_nanos() as usize)
            } else {
                (0, 0)
            };
            if io_submit_count != 0 {
                self.stats
                    .io_submit_count
                    .fetch_add(io_submit_count, Ordering::Relaxed);
                self.stats
                    .io_submit_nanos
                    .fetch_add(io_submit_nanos, Ordering::Relaxed);
            }
            if io_wait_count != 0 {
                self.stats
                    .io_wait_count
                    .fetch_add(io_wait_count, Ordering::Relaxed);
                self.stats
                    .io_wait_nanos
                    .fetch_add(io_wait_nanos, Ordering::Relaxed);
            }

            if shutdown
                && (write && self.inflight_io.no_writes()
                    || (!write && self.inflight_io.no_reads()))
            {
                return;
            }
        }
    }

    #[inline]
    fn evict_loop(&'static self) {
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
            batch_size = batch_size
                .saturating_sub(self.file_io.write_counter.submitted())
                .saturating_sub(self.file_io.write_counter.queued());
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
                    // Page is not dirty means it's a read-only only of the data on disk.
                    // So we can directly remove it from memory without IO write.
                    // But concurrent readers may rely on the status updated in inflight
                    // IO map. So we lock the map when updating frame kind.
                    let g = self.inflight_io.map.lock();
                    debug_assert!(page_guard.bf().kind() == FrameKind::Evicting);
                    self.evict_page(page_guard);
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
    fn clock_sweep(&'static self, page_id: PageID) -> Option<PageExclusiveGuard<Page>> {
        match self.frame_kind(page_id) {
            FrameKind::Uninitialized
            | FrameKind::Fixed
            | FrameKind::Evicting
            | FrameKind::Evicted => (),
            FrameKind::Cool => {
                // Acquire exclusive lock first, in order to block other thread to access
                // this page at the same time. If fails, we just skip.
                if let Some(page_guard) = self.try_lock_page_exclusive(page_id) {
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
                let _ = self.compare_exchange_frame_kind(page_id, FrameKind::Hot, FrameKind::Cool);
            }
        }
        None
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
                    self.pin_page(page_id as PageID);
                    return unsafe { self.init_page(page_id as PageID) };
                }
                None => {
                    listener!(self.alloc_ev => listener);
                    // re-check
                    if let Some(page_id) = self.alloc_map.try_allocate() {
                        self.pin_page(page_id as PageID);
                        return unsafe { self.init_page(page_id as PageID) };
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
            self.pin_page(page_id as PageID);
            Ok(unsafe { self.init_page(page_id as PageID) })
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
                let bf = self.frame_ptr(page_id);
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
        self.unpin_page(page_id);
        self.mark_page_dontneed(g);
        self.alloc_map.deallocate(page_id as usize);
        self.alloc_ev.notify(usize::MAX);
    }

    #[inline]
    fn evict_page<T: BufferPage>(&'static self, mut g: PageExclusiveGuard<T>) {
        let page_id = g.page_id();
        g.page_mut().zero(); // zero the page
        let old_kind = g
            .bf_mut()
            .compare_exchange_kind(FrameKind::Evicting, FrameKind::Evicted);
        debug_assert!(old_kind == FrameKind::Evicting);
        self.unpin_page(page_id);
        self.mark_page_dontneed(g);
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
                let bf = self.frame_ptr(page_id);
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

impl RefUnwindSafe for EvictableBufferPool {}

impl Drop for EvictableBufferPool {
    #[inline]
    fn drop(&mut self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);

        // Close in-mem page set.
        self.in_mem.close();

        // Close file IO.
        self.file_io.close();

        unsafe {
            // Drop all frames.
            for page_id in 0..self.capacity() {
                let frame_ptr = self.frames.add(page_id);
                std::ptr::drop_in_place(frame_ptr);
            }

            // Deallocate memory of frames.
            let frame_total_bytes =
                mem::size_of::<BufferFrame>() * (self.capacity() + SAFETY_PAGES);
            mmap_deallocate(self.frames as *mut u8, frame_total_bytes);
            // Deallocate memory of pages.
            let page_total_bytes = mem::size_of::<Page>() * (self.capacity() + SAFETY_PAGES);
            mmap_deallocate(self.pages as *mut u8, page_total_bytes);
        }
    }
}

unsafe impl Send for EvictableBufferPool {}

unsafe impl Sync for EvictableBufferPool {}

unsafe impl StaticLifetime for EvictableBufferPool {}

impl UnwindSafe for EvictableBufferPool {}

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
    unsafe fn pin(&self, page_id: PageID, _ptr: *mut Page) {
        // assert!(mlock(ptr as *mut u8, PAGE_SIZE));
        let mut g = self.set.lock();
        g.insert(page_id);
    }

    #[inline]
    unsafe fn unpin(&self, page_id: PageID, _ptr: *mut Page) {
        // assert!(munlock(ptr as *mut u8, PAGE_SIZE));
        let mut g = self.set.lock();
        g.remove(&page_id);
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

#[derive(Default)]
struct IOCounter {
    submitted: AtomicUsize,
    queued: AtomicUsize,
    finished: AtomicUsize,
}

impl IOCounter {
    #[inline]
    fn submitted(&self) -> usize {
        self.submitted.load(Ordering::Relaxed)
    }

    #[inline]
    fn set_submitted(&self, submitted: usize) {
        self.submitted.store(submitted, Ordering::Relaxed);
    }

    #[inline]
    fn queued(&self) -> usize {
        self.queued.load(Ordering::Relaxed)
    }

    #[inline]
    fn set_queued(&self, queued: usize) {
        self.queued.store(queued, Ordering::Relaxed);
    }

    #[inline]
    fn finished(&self) -> usize {
        self.finished.load(Ordering::Relaxed)
    }

    #[inline]
    fn add_finished(&self, count: usize) {
        self.finished.fetch_add(count, Ordering::Relaxed);
    }
}

struct SingleFileIO {
    aio_reader: AIOManager,
    aio_writer: AIOManager,
    file: SparseFile,
    read_tx: Sender<BufferRequest>,
    read_thread: Mutex<Option<JoinHandle<()>>>,
    write_tx: Sender<BufferRequest>,
    write_thread: Mutex<Option<JoinHandle<()>>>,
    read_counter: IOCounter,
    write_counter: IOCounter,
}

impl SingleFileIO {
    #[inline]
    fn new(
        aio_reader: AIOManager,
        aio_writer: AIOManager,
        file: SparseFile,
        read_tx: Sender<BufferRequest>,
        write_tx: Sender<BufferRequest>,
    ) -> Self {
        SingleFileIO {
            aio_reader,
            aio_writer,
            file,
            read_tx,
            read_thread: Mutex::new(None),
            write_tx,
            write_thread: Mutex::new(None),
            read_counter: IOCounter::default(),
            write_counter: IOCounter::default(),
        }
    }

    #[inline]
    fn request_read(&self, page_guard: PageExclusiveGuard<Page>) {
        let _ = self.read_tx.send(BufferRequest::Read(page_guard));
    }

    #[inline]
    fn request_batch_write(&self, page_guards: Vec<PageExclusiveGuard<Page>>) {
        let _ = self.write_tx.send(BufferRequest::BatchWrite(page_guards));
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

    #[inline]
    fn close(&self) {
        // deregister sparse file
        self.aio_writer.deregister_fd(self.file.as_raw_fd());

        // notify IO thread and wait.
        let _ = self.read_tx.send(BufferRequest::Shutdown);
        let _ = self.write_tx.send(BufferRequest::Shutdown);
        {
            let mut g = self.read_thread.lock();
            if let Some(handle) = g.take() {
                handle.join().unwrap();
            }
        }
        {
            let mut g = self.write_thread.lock();
            if let Some(handle) = g.take() {
                handle.join().unwrap();
            }
        }
    }
}

const DEFAULT_FILE_PATH: &str = "databuffer.bin";
const DEFAULT_MAX_FILE_SIZE: Byte = Byte::from_u64(2 * 1024 * 1024 * 1024); // by default 2GB
const DEFAULT_MAX_MEM_SIZE: Byte = Byte::from_u64(1024 * 1024 * 1024); // by default 1GB
const DEFAULT_MAX_IO_READS: usize = 64;
const DEFAULT_MAX_IO_WRITES: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictableBufferPoolConfig {
    file_path: String,
    max_file_size: Byte,
    max_mem_size: Byte,
    max_io_reads: usize,
    max_io_writes: usize,
}

impl Default for EvictableBufferPoolConfig {
    #[inline]
    fn default() -> Self {
        EvictableBufferPoolConfig {
            file_path: String::from(DEFAULT_FILE_PATH),
            max_file_size: DEFAULT_MAX_FILE_SIZE,
            max_mem_size: DEFAULT_MAX_MEM_SIZE,
            max_io_reads: DEFAULT_MAX_IO_READS,
            max_io_writes: DEFAULT_MAX_IO_WRITES,
        }
    }
}

impl EvictableBufferPoolConfig {
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
    pub fn max_io_reads(mut self, max_io_reads: usize) -> Self {
        self.max_io_reads = max_io_reads;
        self
    }

    #[inline]
    pub fn max_io_writes(mut self, max_io_writes: usize) -> Self {
        self.max_io_writes = max_io_writes;
        self
    }

    #[inline]
    pub fn build(self) -> Result<(EvictableBufferPool, EvictableBufferPoolStartContext)> {
        let (io_read_tx, io_read_rx) = flume::unbounded();
        let (io_write_tx, io_write_rx) = flume::unbounded();

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
        let aio_writer = AIOManagerConfig::default()
            .max_events(self.max_io_writes)
            .build()?;
        let aio_reader = AIOManagerConfig::default()
            .max_events(self.max_io_reads)
            .build()?;
        let file = aio_writer.create_sparse_file(&self.file_path, max_file_size)?;

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
            frames,
            pages,
            in_mem: InMemPageSet::new(max_nbr_in_mem),
            alloc_map: AllocMap::new(max_nbr),
            alloc_ev: Event::new(),
            file_io: SingleFileIO::new(aio_reader, aio_writer, file, io_read_tx, io_write_tx),
            shutdown_flag: AtomicBool::new(false),
            inflight_io: CachePadded::new(InflightIO::default()),
            stats: CachePadded::new(EvictableBufferPoolStats::default()),
        };
        Ok((
            pool,
            EvictableBufferPoolStartContext {
                io_read_rx,
                io_write_rx,
            },
        ))
    }

    /// Create a new evictable buffer pool with given capacity.
    #[inline]
    pub fn build_static(self) -> Result<&'static EvictableBufferPool> {
        let (pool, start_ctx) = self.build()?;
        let pool = StaticLifetime::new_static(pool);
        pool.start(start_ctx);
        Ok(pool)
    }
}

struct IOStatus {
    kind: IOKind,
    event: Option<Event>,
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
        // self.writes_submit_signal.notify(usize::MAX);
    }

    #[inline]
    fn contains(&self, page_id: PageID) -> bool {
        let g = self.map.lock();
        g.contains_key(&page_id)
    }

    #[inline]
    fn no_reads(&self) -> bool {
        self.reads.load(Ordering::Relaxed) == 0
    }

    #[inline]
    fn no_writes(&self) -> bool {
        self.writes.load(Ordering::Relaxed) == 0
    }
}

#[derive(Default)]
pub struct EvictableBufferPoolStats {
    io_submit_count: AtomicUsize,
    io_submit_nanos: AtomicUsize,
    io_wait_count: AtomicUsize,
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

pub struct EvictableBufferPoolStartContext {
    // Receiver side of IO read requests, used by IO read thread.
    pub io_read_rx: Receiver<BufferRequest>,
    // Receiver side of IO write requests, used by IO write thread.
    pub io_write_rx: Receiver<BufferRequest>,
}

struct IOQueue {
    // aligned with AIO interface.
    iocbs: Vec<IocbRawPtr>,
    reqs: VecDeque<PageIO>,
}

impl IOQueue {
    #[inline]
    fn with_capacity(capacity: usize) -> Self {
        IOQueue {
            iocbs: Vec::with_capacity(capacity),
            reqs: VecDeque::with_capacity(capacity),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.iocbs.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    fn consistent(&self) -> bool {
        self.reqs.len() == self.iocbs.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::RowPage;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_evict_buffer_pool_simple() {
        smol::block_on(async {
            let pool = EvictableBufferPoolConfig::default()
                .max_mem_size(1024u64 * 1024 * 128)
                .max_file_size(1024u64 * 1024 * 256)
                .file_path("data1.bin")
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
            let _ = std::fs::remove_file("data1.bin");
        })
    }

    #[test]
    fn test_evict_buffer_pool_full() {
        // 1024 in-mem pages and 2048 total pages.
        let pool: &EvictableBufferPool = EvictableBufferPoolConfig::default()
            .max_mem_size(64u64 * 1024 * 1024)
            .max_file_size(128u64 * 1024 * 1024)
            .file_path("data2.bin")
            .build_static()
            .unwrap();

        let (tx, rx) = flume::unbounded();
        let handle1 = {
            thread::spawn(move || {
                smol::block_on(async move {
                    // allocate more pages than memory limit.
                    for i in 0..1500 {
                        let g = pool.allocate_page::<RowPage>().await;
                        let _ = tx.send(g.page_id());
                        println!("allocated page {}", i);
                    }
                    drop(tx);
                })
            })
        };

        thread::sleep(Duration::from_secs(1));
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
        let _ = std::fs::remove_file("data2.bin");
    }

    #[test]
    fn test_evict_buffer_pool_alloc() {
        // max pages 16k, max in-mem 1k
        let pool = EvictableBufferPoolConfig::default()
            .max_mem_size(1024u64 * 1024 * 64)
            .max_file_size(1024u64 * 1024 * 128)
            .file_path("data3.bin")
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
        let _ = std::fs::remove_file("data3.bin");
    }

    #[test]
    fn test_evict_buffer_pool_multi_threads() {
        use rand::{prelude::IndexedRandom, Rng};
        // max pages 2k, max in-mem 1k
        let pool = EvictableBufferPoolConfig::default()
            .max_mem_size(64u64 * 1024 * 1024)
            .max_file_size(64u64 * 1024 * 2048)
            .file_path("data4.bin")
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
        let _ = std::fs::remove_file("data4.bin");
    }
}
