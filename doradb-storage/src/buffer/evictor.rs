use crate::buffer::arena::ArenaGuard;
use crate::buffer::frame::FrameKind;
use crate::buffer::guard::PageExclusiveGuard;
use crate::buffer::page::{Page, PageID};
use crate::component::{Component, ComponentRegistry, ShelfScope};
use crate::error::Result;
use crate::quiescent::{QuiescentBox, SyncQuiescentGuard};
use crate::thread;
use crate::{DiskPool, IndexPool, MemPool};
use event_listener::{Event, EventListener, Listener};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::ops::{Range, RangeFrom, RangeTo};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
const DEFAULT_TARGET_FREE_RATIO: f64 = 0.10;
const DEFAULT_HYSTERESIS_RATIO: f64 = 0.30;

/// Snapshot of shared-evictor wake and domain-execution activity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct SharedPoolEvictorStats {
    /// Number of wakeups observed after the evictor blocked for work.
    pub wake_count: usize,
    /// Number of times the evictor blocked waiting for work.
    pub wait_count: usize,
    /// Number of readonly-domain runs completed by the shared evictor.
    pub readonly_runs: usize,
    /// Number of mem-pool-domain runs completed by the shared evictor.
    pub mem_runs: usize,
    /// Number of index-pool-domain runs completed by the shared evictor.
    pub index_runs: usize,
}

impl SharedPoolEvictorStats {
    /// Returns the saturating delta from one earlier snapshot.
    #[inline]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn delta_since(self, earlier: SharedPoolEvictorStats) -> SharedPoolEvictorStats {
        SharedPoolEvictorStats {
            wake_count: self.wake_count.saturating_sub(earlier.wake_count),
            wait_count: self.wait_count.saturating_sub(earlier.wait_count),
            readonly_runs: self.readonly_runs.saturating_sub(earlier.readonly_runs),
            mem_runs: self.mem_runs.saturating_sub(earlier.mem_runs),
            index_runs: self.index_runs.saturating_sub(earlier.index_runs),
        }
    }
}

#[derive(Default)]
struct SharedPoolEvictorStatsCounters {
    wake_count: AtomicUsize,
    wait_count: AtomicUsize,
    readonly_runs: AtomicUsize,
    mem_runs: AtomicUsize,
    index_runs: AtomicUsize,
}

#[derive(Clone, Default)]
pub(crate) struct SharedPoolEvictorStatsHandle(Arc<SharedPoolEvictorStatsCounters>);

impl SharedPoolEvictorStatsHandle {
    #[inline]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn snapshot(&self) -> SharedPoolEvictorStats {
        SharedPoolEvictorStats {
            wake_count: self.0.wake_count.load(Ordering::Relaxed),
            wait_count: self.0.wait_count.load(Ordering::Relaxed),
            readonly_runs: self.0.readonly_runs.load(Ordering::Relaxed),
            mem_runs: self.0.mem_runs.load(Ordering::Relaxed),
            index_runs: self.0.index_runs.load(Ordering::Relaxed),
        }
    }

    #[inline]
    fn record_wait(&self) {
        self.0.wait_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn record_wake(&self) {
        self.0.wake_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn record_domain_run(&self, id: SharedEvictionDomainId) {
        match id {
            SharedEvictionDomainId::Readonly => {
                self.0.readonly_runs.fetch_add(1, Ordering::Relaxed);
            }
            SharedEvictionDomainId::Mem => {
                self.0.mem_runs.fetch_add(1, Ordering::Relaxed);
            }
            SharedEvictionDomainId::Index => {
                self.0.index_runs.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

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
        ClockHand::From(PageID::new(0)..)
    }
}

impl ClockHand {
    /// Resets the hand and preserves sweep locality.
    #[inline]
    pub(super) fn reset(&mut self) {
        let start = self.start();
        if start == PageID::new(0) {
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
            ClockHand::To(_) => PageID::new(0),
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

/// Collects up to `limit` page ids from the resident-set according to
/// clock-hand position and returns the next hand state.
#[inline]
pub(super) fn clock_collect_batch(
    set: &BTreeSet<PageID>,
    clock_hand: ClockHand,
    limit: usize,
    out: &mut Vec<PageID>,
) -> Option<ClockHand> {
    if limit == 0 {
        return Some(clock_hand);
    }
    clock_collect(set, clock_hand, |page_id| {
        out.push(page_id);
        out.len() >= limit
    })
}

/// Applies one clock-sweep step and returns a page guard when a frame
/// is selected for eviction.
#[inline]
pub(super) fn clock_sweep_candidate(
    arena: &ArenaGuard,
    page_id: PageID,
) -> Option<PageExclusiveGuard<Page>> {
    match arena.frame_kind(page_id) {
        FrameKind::Uninitialized | FrameKind::Fixed | FrameKind::Evicting | FrameKind::Evicted => {
            None
        }
        FrameKind::Cool => {
            if let Some(page_guard) = arena.try_lock_page_exclusive(page_id) {
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
            let _ = arena.compare_exchange_frame_kind(page_id, FrameKind::Hot, FrameKind::Cool);
            None
        }
    }
}

/// Eviction arbiter shared by evictable and readonly buffer pools.
///
/// Semantics:
/// - trigger eviction when `free_frames < target_free` OR failure-rate reaches threshold;
/// - stop when `free_frames >= target_free + hysteresis` AND failure-rate recovers;
/// - choose dynamic batch size within `[min_batch, max_batch]`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct EvictionArbiter {
    target_free: usize,
    hysteresis: usize,
    failure_rate_threshold: f64,
    failure_window: usize,
    min_batch: usize,
    max_batch: usize,
}

impl EvictionArbiter {
    #[inline]
    pub fn builder() -> EvictionArbiterBuilder {
        EvictionArbiterBuilder::default()
    }

    #[inline]
    pub fn failure_window(&self) -> usize {
        self.failure_window
    }

    #[inline]
    pub fn target_free(&self) -> usize {
        self.target_free
    }

    #[inline]
    pub fn hysteresis(&self) -> usize {
        self.hysteresis
    }

    /// Shared pressure-delta + hysteresis eviction decision.
    #[inline]
    pub fn decide(
        &self,
        resident: usize,
        capacity: usize,
        inflight_evicts: usize,
        failure_rate: f64,
        min_resident: usize,
    ) -> Option<EvictionDecision> {
        // Keep a minimum resident floor so eviction cannot drain the pool below safety bounds.
        if resident <= min_resident {
            return None;
        }

        // Compute free-frame pressure from the current residency snapshot.
        let capacity = capacity.max(1);
        let free_frames = capacity.saturating_sub(resident);

        // Trigger eviction on either low free frames or sustained allocation failures.
        let trigger_pressure = free_frames < self.target_free;
        let trigger_failures = failure_rate >= self.failure_rate_threshold;
        if !trigger_pressure && !trigger_failures {
            return None;
        }

        // Stop once free frames recover past hysteresis and failure pressure subsides.
        let stop_free = self.target_free.saturating_add(self.hysteresis);
        let recovered = free_frames >= stop_free && failure_rate < self.failure_rate_threshold;
        if recovered {
            return None;
        }

        // Base batch is the distance to the stop target, with a configured minimum.
        let delta = stop_free.saturating_sub(free_frames);
        let mut batch = delta.max(self.min_batch);
        if trigger_failures && self.failure_rate_threshold < 1.0 {
            // Under high failure pressure, scale batch size toward max_batch.
            let ratio = ((failure_rate - self.failure_rate_threshold)
                / (1.0 - self.failure_rate_threshold))
                .clamp(0.0, 1.0);
            let span = self.max_batch.saturating_sub(self.min_batch);
            let boosted = self.min_batch + ((span as f64) * ratio) as usize;
            batch = batch.max(boosted);
        }

        // Enforce bounds and discount already in-flight evictions.
        batch = batch.min(self.max_batch);
        batch = batch.saturating_sub(inflight_evicts);
        if batch == 0 {
            return None;
        }

        // Emit a concrete batch decision for the evictor loop.
        Some(EvictionDecision { batch_size: batch })
    }
}

/// Builder for [`EvictionArbiter`].
///
/// If `target_free`/`hysteresis` are not explicitly set, they are derived from
/// ratio fields and the runtime capacity passed to `build(capacity)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictionArbiterBuilder {
    target_free: Option<usize>,
    target_free_ratio: f64,
    hysteresis: Option<usize>,
    hysteresis_ratio: f64,
    failure_rate_threshold: f64,
    failure_window: usize,
    min_batch: usize,
    max_batch: usize,
}

impl Default for EvictionArbiterBuilder {
    #[inline]
    fn default() -> Self {
        EvictionArbiterBuilder {
            target_free: None,
            target_free_ratio: DEFAULT_TARGET_FREE_RATIO,
            hysteresis: None,
            hysteresis_ratio: DEFAULT_HYSTERESIS_RATIO,
            failure_rate_threshold: 0.25,
            failure_window: 256,
            min_batch: 8,
            max_batch: 64,
        }
    }
}

impl EvictionArbiterBuilder {
    /// Creates a builder with default eviction settings.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets an absolute free-frame target that triggers pressure eviction.
    ///
    /// If not set, `build()` derives it from `target_free_ratio * capacity`.
    #[inline]
    pub fn target_free(mut self, target_free: usize) -> Self {
        self.target_free = Some(target_free);
        self
    }

    /// Sets the free-frame target ratio used when `target_free` is unset.
    ///
    /// Values are normalized to `[0.0, 1.0]` during `build()`.
    #[inline]
    pub fn target_free_ratio(mut self, ratio: f64) -> Self {
        self.target_free_ratio = ratio;
        self
    }

    /// Sets an absolute hysteresis margin added to `target_free` for stop checks.
    ///
    /// If not set, `build()` derives it from `hysteresis_ratio * target_free`.
    #[inline]
    pub fn hysteresis(mut self, hysteresis: usize) -> Self {
        self.hysteresis = Some(hysteresis);
        self
    }

    /// Sets the hysteresis ratio used when `hysteresis` is unset.
    ///
    /// Values are normalized to `[0.0, 1.0]` during `build()`.
    #[inline]
    pub fn hysteresis_ratio(mut self, ratio: f64) -> Self {
        self.hysteresis_ratio = ratio;
        self
    }

    /// Sets allocation-failure threshold used to trigger/stop eviction.
    ///
    /// Values are normalized to `[0.0, 1.0]` during `build()`.
    #[inline]
    pub fn failure_rate_threshold(mut self, threshold: f64) -> Self {
        self.failure_rate_threshold = threshold;
        self
    }

    /// Sets sliding-window sample size for failure-rate tracking.
    #[inline]
    pub fn failure_window(mut self, window: usize) -> Self {
        self.failure_window = window;
        self
    }

    /// Sets minimum and maximum eviction batch size bounds.
    #[inline]
    pub fn dynamic_batch_bounds(mut self, min_batch: usize, max_batch: usize) -> Self {
        self.min_batch = min_batch;
        self.max_batch = max_batch;
        self
    }

    /// Builds a normalized [`EvictionArbiter`] for a specific runtime capacity.
    ///
    /// This method clamps invalid values and fills unset fields from ratio-based defaults.
    #[inline]
    pub fn build(self, capacity: usize) -> EvictionArbiter {
        let capacity = capacity.max(1);
        let target_free_ratio = if self.target_free_ratio.is_finite() {
            self.target_free_ratio.clamp(0.0, 1.0)
        } else {
            DEFAULT_TARGET_FREE_RATIO
        };
        let mut target_free = self
            .target_free
            .unwrap_or(((capacity as f64) * target_free_ratio) as usize);
        target_free = target_free.max(1).min(capacity);

        let hysteresis_ratio = if self.hysteresis_ratio.is_finite() {
            self.hysteresis_ratio.clamp(0.0, 1.0)
        } else {
            DEFAULT_HYSTERESIS_RATIO
        };
        let mut hysteresis = self
            .hysteresis
            .unwrap_or(((target_free as f64) * hysteresis_ratio) as usize);
        hysteresis = hysteresis.max(1);

        let failure_window = self.failure_window.max(1);
        let failure_rate_threshold = if self.failure_rate_threshold.is_finite() {
            self.failure_rate_threshold.clamp(0.0, 1.0)
        } else {
            1.0
        };

        let min_batch = self.min_batch.max(1);
        let mut max_batch = self.max_batch.max(1);
        if max_batch < min_batch {
            max_batch = min_batch;
        }

        EvictionArbiter {
            target_free,
            hysteresis,
            failure_rate_threshold,
            failure_window,
            min_batch,
            max_batch,
        }
    }
}

/// Output of [`EvictionArbiter::decide`], consumed by the evictor loop.
///
/// `batch_size` is the number of resident pages the runtime should try to
/// evict in the current pass.
#[derive(Debug, Clone, Copy)]
pub struct EvictionDecision {
    pub batch_size: usize,
}

struct FailureWindowState {
    samples: Vec<bool>,
    cursor: usize,
    len: usize,
    failures: usize,
}

impl FailureWindowState {
    #[inline]
    fn with_window(window: usize) -> Self {
        FailureWindowState {
            samples: vec![false; window.max(1)],
            cursor: 0,
            len: 0,
            failures: 0,
        }
    }

    #[inline]
    fn record(&mut self, failed: bool) {
        if self.len < self.samples.len() {
            self.samples[self.cursor] = failed;
            self.len += 1;
            if failed {
                self.failures += 1;
            }
            self.cursor = (self.cursor + 1) % self.samples.len();
            return;
        }

        let old = self.samples[self.cursor];
        if old {
            self.failures = self.failures.saturating_sub(1);
        }
        self.samples[self.cursor] = failed;
        if failed {
            self.failures += 1;
        }
        self.cursor = (self.cursor + 1) % self.samples.len();
    }

    #[inline]
    fn failure_rate(&self) -> f64 {
        if self.len == 0 {
            return 0.0;
        }
        self.failures as f64 / self.len as f64
    }
}

/// Sliding-window allocation failure-rate tracker shared by buffer pools.
pub struct FailureRateTracker {
    state: Mutex<FailureWindowState>,
}

impl FailureRateTracker {
    #[inline]
    pub fn new(window: usize) -> Self {
        FailureRateTracker {
            state: Mutex::new(FailureWindowState::with_window(window)),
        }
    }

    #[inline]
    pub fn record_success(&self) {
        let mut g = self.state.lock();
        g.record(false);
    }

    #[inline]
    pub fn record_failure(&self) {
        let mut g = self.state.lock();
        g.record(true);
    }

    #[inline]
    pub fn failure_rate(&self) -> f64 {
        let g = self.state.lock();
        g.failure_rate()
    }
}

/// Unified pool-specific eviction runtime.
///
/// Implemented by concrete buffer-pool runtimes to expose pressure state,
/// candidate iteration/marking, and execution semantics.
pub(super) trait EvictionRuntime {
    /// Returns the number of pages currently considered resident by the pool.
    fn resident_count(&self) -> usize;

    /// Returns the runtime page capacity used by the eviction policy.
    fn capacity(&self) -> usize;

    /// Returns the number of pages already in-flight for eviction/writeback.
    fn inflight_evicts(&self) -> usize;

    /// Returns recent allocation-failure ratio used as eviction pressure signal.
    fn allocation_failure_rate(&self) -> f64;

    /// Emits post-eviction progress notifications to blocked alloc/load waiters.
    fn notify_progress(&self);

    /// Collects up to `limit` resident page ids according to `hand` ordering.
    ///
    /// Implementations should only hold internal residency locks for the
    /// duration of id collection and must not perform frame probing here.
    fn collect_batch_ids(
        &self,
        hand: ClockHand,
        limit: usize,
        out: &mut Vec<PageID>,
    ) -> Option<ClockHand>;

    /// Tries to transition a candidate page to evicting state and returns its guard.
    fn try_mark_evicting(&self, page_id: PageID) -> Option<PageExclusiveGuard<Page>>;

    /// Executes eviction for selected pages and returns an optional completion waiter.
    ///
    /// `None` means no asynchronous completion phase is required.
    fn execute(&self, pages: Vec<PageExclusiveGuard<Page>>) -> Option<EventListener>;
}

/// Concrete pressure-delta + clock-sweep policy used by all pools.
pub(super) struct PressureDeltaClockPolicy {
    arbiter: EvictionArbiter,
    min_resident: usize,
    clock_hand: ClockHand,
    tmp_page_ids: Vec<PageID>,
}

impl PressureDeltaClockPolicy {
    #[inline]
    pub(super) fn new(arbiter: EvictionArbiter, min_resident: usize) -> Self {
        PressureDeltaClockPolicy {
            arbiter,
            min_resident,
            clock_hand: ClockHand::default(),
            tmp_page_ids: vec![],
        }
    }

    #[inline]
    fn decide_batch(&self, runtime: &dyn EvictionRuntime) -> Option<usize> {
        self.arbiter
            .decide(
                runtime.resident_count(),
                runtime.capacity(),
                runtime.inflight_evicts(),
                runtime.allocation_failure_rate(),
                self.min_resident,
            )
            .map(|decision| decision.batch_size)
    }

    #[inline]
    fn select_candidates(
        &mut self,
        runtime: &dyn EvictionRuntime,
        target: usize,
    ) -> Vec<PageExclusiveGuard<Page>> {
        let mut batch_size = target;
        let mut candidates = Vec::with_capacity(target);
        let mut next_ch = None;
        'SWEEP: for _ in 0..2 {
            next_ch = Some(self.clock_hand.clone());
            while let Some(ch) = next_ch.take() {
                // Step 1: collect candidate ids while holding residency lock only.
                self.tmp_page_ids.clear();
                next_ch = runtime.collect_batch_ids(ch, batch_size, &mut self.tmp_page_ids);

                // Step 2: probe frame state and mark evicting after lock release.
                for page_id in self.tmp_page_ids.drain(..) {
                    if let Some(page_guard) = runtime.try_mark_evicting(page_id) {
                        candidates.push(page_guard);
                        batch_size -= 1;
                        if batch_size == 0 {
                            break 'SWEEP;
                        }
                    }
                }
            }
        }

        if !candidates.is_empty() {
            if let Some(mut ch) = next_ch {
                ch.reset();
                self.clock_hand = ch;
            } else {
                self.clock_hand.reset();
            }
        }
        candidates
    }
}

pub(super) struct SharedEvictionDomain {
    id: SharedEvictionDomainId,
    runtime: Box<dyn EvictionRuntime + Send>,
    policy: PressureDeltaClockPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SharedEvictionDomainId {
    Readonly,
    Mem,
    Index,
}

impl SharedEvictionDomain {
    #[inline]
    pub(super) fn new<T>(
        id: SharedEvictionDomainId,
        runtime: T,
        policy: PressureDeltaClockPolicy,
    ) -> Self
    where
        T: EvictionRuntime + Send + 'static,
    {
        SharedEvictionDomain {
            id,
            runtime: Box::new(runtime),
            policy,
        }
    }

    #[inline]
    fn id(&self) -> SharedEvictionDomainId {
        self.id
    }

    #[inline]
    fn is_ready(&self) -> bool {
        self.policy.decide_batch(self.runtime.as_ref()).is_some()
    }

    #[inline]
    fn try_run_once(&mut self) -> bool {
        let target = match self.policy.decide_batch(self.runtime.as_ref()) {
            Some(target) if target > 0 => target,
            _ => return false,
        };

        let candidates = self.policy.select_candidates(self.runtime.as_ref(), target);
        if candidates.is_empty() {
            return false;
        }

        let waiter = self.runtime.execute(candidates);
        if let Some(waiter) = waiter {
            waiter.wait();
        }
        self.runtime.notify_progress();
        true
    }
}

struct SharedEvictor {
    domains: Vec<SharedEvictionDomain>,
    shutdown_flag: Arc<AtomicBool>,
    wake_event: Arc<Event>,
    next_domain: usize,
    stats: SharedPoolEvictorStatsHandle,
}

impl SharedEvictor {
    #[inline]
    fn new(
        domains: Vec<SharedEvictionDomain>,
        shutdown_flag: Arc<AtomicBool>,
        wake_event: Arc<Event>,
        stats: SharedPoolEvictorStatsHandle,
    ) -> Self {
        SharedEvictor {
            domains,
            shutdown_flag,
            wake_event,
            next_domain: 0,
            stats,
        }
    }

    #[inline]
    fn start_thread(self) -> JoinHandle<()> {
        thread::spawn_named("SharedPoolEvictor", move || self.run())
    }

    #[inline]
    fn any_domain_ready(&self) -> bool {
        self.domains.iter().any(SharedEvictionDomain::is_ready)
    }

    #[inline]
    fn next_ready_domain_index(&mut self) -> Option<usize> {
        let len = self.domains.len();
        for _ in 0..len {
            let idx = self.next_domain;
            self.next_domain = (self.next_domain + 1) % len;
            if self.domains[idx].is_ready() {
                return Some(idx);
            }
        }
        None
    }

    #[inline]
    fn run_one_ready_domain(&mut self) -> bool {
        let len = self.domains.len();
        for _ in 0..len {
            let Some(idx) = self.next_ready_domain_index() else {
                return false;
            };
            if self.domains[idx].try_run_once() {
                self.stats.record_domain_run(self.domains[idx].id());
                return true;
            }
        }
        false
    }

    #[inline]
    fn wait_for_work(&self) -> bool {
        let listener = self.wake_event.listen();
        if self.shutdown_flag.load(Ordering::Acquire) {
            return false;
        }
        if self.any_domain_ready() {
            return true;
        }
        self.stats.record_wait();
        listener.wait();
        if self.shutdown_flag.load(Ordering::Acquire) {
            return false;
        }
        self.stats.record_wake();
        true
    }

    #[inline]
    fn run(mut self) {
        loop {
            if self.shutdown_flag.load(Ordering::Acquire) {
                return;
            }
            if self.run_one_ready_domain() {
                continue;
            }
            if !self.wait_for_work() {
                return;
            }
        }
    }
}

pub(crate) struct SharedPoolEvictorWorkers;

pub(crate) struct SharedPoolEvictorWorkersOwned {
    disk_pool: SyncQuiescentGuard<crate::buffer::GlobalReadonlyBufferPool>,
    index_pool: SyncQuiescentGuard<crate::buffer::EvictableBufferPool>,
    mem_pool: SyncQuiescentGuard<crate::buffer::EvictableBufferPool>,
    shutdown_flag: Arc<AtomicBool>,
    wake_event: Arc<Event>,
    stats: SharedPoolEvictorStatsHandle,
    evict_thread: Mutex<Option<JoinHandle<()>>>,
}

impl Component for SharedPoolEvictorWorkers {
    type Config = ();
    type Owned = SharedPoolEvictorWorkersOwned;
    type Access = SharedPoolEvictorStatsHandle;

    const NAME: &'static str = "shared_pool_evictor_workers";

    #[inline]
    async fn build(
        _config: Self::Config,
        registry: &mut ComponentRegistry,
        _shelf: ShelfScope<'_, Self>,
    ) -> Result<()> {
        let disk_pool = registry.dependency::<DiskPool>()?;
        let index_pool = registry.dependency::<IndexPool>()?;
        let mem_pool = registry.dependency::<MemPool>()?;

        let disk_pool = disk_pool.clone_inner().into_sync();
        let index_pool = index_pool.clone_inner().into_sync();
        let mem_pool = mem_pool.clone_inner().into_sync();
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let wake_event = Arc::new(Event::new());
        let stats = SharedPoolEvictorStatsHandle::default();

        disk_pool.install_shared_evictor_wake(Arc::clone(&wake_event));
        index_pool.install_shared_evictor_wake(Arc::clone(&wake_event));
        mem_pool.install_shared_evictor_wake(Arc::clone(&wake_event));

        let handle = SharedEvictor::new(
            vec![
                crate::buffer::GlobalReadonlyBufferPool::shared_evictor_domain(disk_pool.clone()),
                crate::buffer::EvictableBufferPool::shared_evictor_domain(mem_pool.clone()),
                crate::buffer::EvictableBufferPool::shared_evictor_domain(index_pool.clone()),
            ],
            Arc::clone(&shutdown_flag),
            Arc::clone(&wake_event),
            stats.clone(),
        )
        .start_thread();

        registry.register::<Self>(SharedPoolEvictorWorkersOwned {
            disk_pool,
            index_pool,
            mem_pool,
            shutdown_flag,
            wake_event,
            stats,
            evict_thread: Mutex::new(Some(handle)),
        })
    }

    #[inline]
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
        owner.stats.clone()
    }

    #[inline]
    fn shutdown(component: &Self::Owned) {
        component.shutdown_flag.store(true, Ordering::SeqCst);
        component.disk_pool.signal_shutdown();
        component.index_pool.signal_shutdown();
        component.mem_pool.signal_shutdown();
        component.wake_event.notify(usize::MAX);
        if let Some(handle) = component.evict_thread.lock().take() {
            handle.join().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPool;
    use crate::buffer::frame::BufferFrame;
    use crate::buffer::page::Page;
    use crate::buffer::{PoolRole, ReadonlyBufferPool};
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableMetadata,
    };
    use crate::component::{ComponentRegistry, DiskPoolConfig, IndexPoolConfig, RegistryBuilder};
    use crate::conf::{EvictableBufferPoolConfig, FileSystemConfig};
    use crate::file::BlockID;
    use crate::file::cow_file::COW_FILE_PAGE_SIZE;
    use crate::file::fs::{FileSystem, FileSystemWorkers};
    use crate::file::table_file::TableFile;
    use crate::io::{DirectBuf, IOBuf};
    use crate::{DiskPool, IndexPool, MemPool};
    use event_listener::Event;
    use std::mem;
    use std::path::Path;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    #[test]
    fn test_failure_rate_tracker_window() {
        let tracker = FailureRateTracker::new(4);
        tracker.record_success();
        tracker.record_failure();
        tracker.record_failure();
        assert!((tracker.failure_rate() - 2.0 / 3.0).abs() < 1e-9);

        tracker.record_success();
        tracker.record_success();
        // Window keeps latest 4 samples: failure, failure, success, success.
        assert!((tracker.failure_rate() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn test_arbiter_decide_pressure_delta_and_hysteresis() {
        let arbiter = EvictionArbiterBuilder::new()
            .target_free(10)
            .hysteresis(4)
            .failure_rate_threshold(0.3)
            .failure_window(128)
            .dynamic_batch_bounds(4, 32)
            .build(100);

        // Triggered by low free frames.
        let decision = arbiter.decide(95, 100, 0, 0.0, 1).unwrap();
        assert!(decision.batch_size >= 9);

        // Stop condition satisfied.
        let no_evict = arbiter.decide(84, 100, 0, 0.0, 1);
        assert!(no_evict.is_none());
    }

    #[test]
    fn test_arbiter_decide_failure_rate_trigger_and_dynamic_batch() {
        let arbiter = EvictionArbiterBuilder::new()
            .target_free(10)
            .hysteresis(4)
            .failure_rate_threshold(0.2)
            .failure_window(128)
            .dynamic_batch_bounds(4, 40)
            .build(100);

        // Plenty of free frames, but failure rate is high so eviction still triggers.
        let decision = arbiter.decide(50, 100, 0, 0.9, 1).unwrap();
        assert!(decision.batch_size >= 20);

        // Inflight evictions reduce effective batch.
        let decision = arbiter.decide(50, 100, 8, 0.9, 1).unwrap();
        assert!(decision.batch_size < 40);
    }

    #[test]
    fn test_arbiter_builder_normalizes_invalid_inputs() {
        let arbiter = EvictionArbiterBuilder::new()
            .target_free_ratio(f64::NAN)
            .hysteresis_ratio(f64::INFINITY)
            .failure_rate_threshold(2.0)
            .failure_window(0)
            .dynamic_batch_bounds(0, 0)
            .build(10);

        // target_free_ratio falls back to default (0.10), then min-clamped to 1.
        assert_eq!(arbiter.target_free, 1);
        // hysteresis_ratio is clamped to 1.0, so hysteresis becomes target_free.
        assert_eq!(arbiter.hysteresis, 1);
        assert_eq!(arbiter.failure_rate_threshold, 1.0);
        assert_eq!(arbiter.failure_window, 1);
        assert_eq!(arbiter.min_batch, 1);
        assert_eq!(arbiter.max_batch, 1);
    }

    #[test]
    fn test_arbiter_builder_respects_explicit_values() {
        let arbiter = EvictionArbiterBuilder::new()
            .target_free(7)
            .target_free_ratio(0.95)
            .hysteresis(5)
            .hysteresis_ratio(0.8)
            .failure_rate_threshold(0.42)
            .failure_window(17)
            .dynamic_batch_bounds(9, 4)
            .build(100);

        // Absolute fields override ratio-derived defaults.
        assert_eq!(arbiter.target_free, 7);
        assert_eq!(arbiter.hysteresis, 5);
        assert_eq!(arbiter.failure_rate_threshold, 0.42);
        assert_eq!(arbiter.failure_window, 17);
        // max_batch is normalized to be at least min_batch.
        assert_eq!(arbiter.min_batch, 9);
        assert_eq!(arbiter.max_batch, 9);
    }

    struct MockRuntime {
        resident: usize,
        capacity: usize,
        inflight: usize,
        failure_rate: f64,
    }

    impl MockRuntime {
        fn new(resident: usize, capacity: usize) -> Self {
            Self {
                resident,
                capacity,
                inflight: 0,
                failure_rate: 0.0,
            }
        }
    }

    impl EvictionRuntime for MockRuntime {
        fn resident_count(&self) -> usize {
            self.resident
        }

        fn capacity(&self) -> usize {
            self.capacity
        }

        fn inflight_evicts(&self) -> usize {
            self.inflight
        }

        fn allocation_failure_rate(&self) -> f64 {
            self.failure_rate
        }

        fn notify_progress(&self) {}

        fn collect_batch_ids(
            &self,
            _hand: ClockHand,
            _limit: usize,
            _out: &mut Vec<PageID>,
        ) -> Option<ClockHand> {
            panic!("selection is not exercised by this scheduler test")
        }

        fn try_mark_evicting(&self, _page_id: PageID) -> Option<PageExclusiveGuard<Page>> {
            panic!("selection is not exercised by this scheduler test")
        }

        fn execute(&self, _pages: Vec<PageExclusiveGuard<Page>>) -> Option<EventListener> {
            panic!("selection is not exercised by this scheduler test")
        }
    }

    fn test_policy() -> PressureDeltaClockPolicy {
        PressureDeltaClockPolicy::new(
            EvictionArbiterBuilder::new()
                .target_free(2)
                .hysteresis(1)
                .dynamic_batch_bounds(1, 4)
                .build(8),
            1,
        )
    }

    const TEST_POOL_BYTES: usize = 64 * 1024 * 130;
    const TEST_POOL_MAX_FILE_BYTES: usize = 128 * 1024 * 260;
    const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
    const TEST_WAIT_INTERVAL: Duration = Duration::from_millis(10);

    fn frame_page_bytes(capacity: usize) -> usize {
        capacity * (mem::size_of::<BufferFrame>() + mem::size_of::<Page>())
    }

    fn wait_for(mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + TEST_WAIT_TIMEOUT;
        loop {
            if predicate() {
                return;
            }
            if Instant::now() >= deadline {
                break;
            }
            thread::sleep(TEST_WAIT_INTERVAL);
        }
        panic!("condition was not satisfied before timeout");
    }

    fn make_metadata() -> Arc<TableMetadata> {
        Arc::new(TableMetadata::new(
            vec![ColumnSpec::new(
                "c0",
                crate::value::ValKind::U32,
                ColumnAttributes::empty(),
            )],
            vec![IndexSpec::new(
                "idx_pk",
                vec![IndexKey::new(0)],
                IndexAttributes::PK,
            )],
        ))
    }

    async fn write_payload(table_file: &Arc<TableFile>, block_id: BlockID, payload: &[u8]) {
        let mut buf = DirectBuf::zeroed(COW_FILE_PAGE_SIZE);
        buf.as_bytes_mut()[..payload.len()].copy_from_slice(payload);
        table_file.write_block(block_id, buf).await.unwrap();
    }

    #[derive(Clone)]
    struct ReadonlyPressureFixture {
        pool: ReadonlyBufferPool,
        base_block_id: BlockID,
        block_count: usize,
    }

    struct StartedSharedEvictorRuntime {
        fs: crate::quiescent::QuiescentGuard<FileSystem>,
        disk_pool: DiskPool,
        mem_pool: MemPool,
        index_pool: IndexPool,
        stats: SharedPoolEvictorStatsHandle,
        registry: ComponentRegistry,
    }

    impl StartedSharedEvictorRuntime {
        fn new(root: &Path) -> Self {
            smol::block_on(async {
                let mut builder = RegistryBuilder::new();
                let file = FileSystemConfig::default()
                    .data_dir(root)
                    .readonly_buffer_size(frame_page_bytes(256));
                builder
                    .build::<DiskPool>(DiskPoolConfig::new(file.readonly_buffer_size))
                    .await
                    .unwrap();
                builder.build::<FileSystem>(file).await.unwrap();
                builder
                    .build::<IndexPool>(IndexPoolConfig::new(
                        TEST_POOL_BYTES,
                        root.join("index.swp"),
                        TEST_POOL_MAX_FILE_BYTES,
                    ))
                    .await
                    .unwrap();
                builder
                    .build::<MemPool>(
                        EvictableBufferPoolConfig::default()
                            .role(PoolRole::Mem)
                            .max_mem_size(TEST_POOL_BYTES)
                            .max_file_size(TEST_POOL_MAX_FILE_BYTES)
                            .data_swap_file(root.join("data.swp")),
                    )
                    .await
                    .unwrap();
                builder.build::<FileSystemWorkers>(()).await.unwrap();
                builder.build::<SharedPoolEvictorWorkers>(()).await.unwrap();
                let registry = builder.finish().unwrap();
                let fs = registry.dependency::<FileSystem>().unwrap();
                let disk_pool = registry.dependency::<DiskPool>().unwrap();
                let mem_pool = registry.dependency::<MemPool>().unwrap();
                let index_pool = registry.dependency::<IndexPool>().unwrap();
                let stats = registry.dependency::<SharedPoolEvictorWorkers>().unwrap();
                Self {
                    fs,
                    disk_pool,
                    mem_pool,
                    index_pool,
                    stats,
                    registry,
                }
            })
        }

        fn stats(&self) -> SharedPoolEvictorStats {
            self.stats.snapshot()
        }

        fn wait_until_idle(&self) -> SharedPoolEvictorStats {
            // Startup synchronization only needs evidence that the shared
            // evictor has parked at least once. Requiring a fresh
            // post-snapshot wait is racy because the worker may already be
            // idle before the test calls this helper.
            wait_for(|| self.stats().wait_count > 0);
            self.stats()
        }
    }

    impl Drop for StartedSharedEvictorRuntime {
        fn drop(&mut self) {
            self.registry.shutdown_all();
        }
    }

    async fn allocate_with_pressure(pool: &crate::buffer::EvictableBufferPool, total_pages: usize) {
        let pool_guard = pool.pool_guard();
        for _ in 0..total_pages {
            let page = pool.allocate_page::<Page>(&pool_guard).await;
            drop(page);
        }
    }

    async fn prepare_read_pressure(
        fs: &FileSystem,
        disk_pool: &DiskPool,
        table_id: u64,
    ) -> ReadonlyPressureFixture {
        let table_file = fs
            .create_table_file(table_id, make_metadata(), false)
            .unwrap();
        let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
        drop(old_root);

        let capacity = disk_pool.capacity();
        let base = 32u64;
        for i in 0..=capacity {
            let block_id = BlockID::from(base + i as u64);
            let payload = format!("page-{i}");
            write_payload(&table_file, block_id, payload.as_bytes()).await;
        }

        let (_opened, pool) = fs
            .open_table_file(table_id, disk_pool.clone_inner())
            .await
            .unwrap();
        ReadonlyPressureFixture {
            pool,
            base_block_id: BlockID::from(base),
            block_count: capacity + 1,
        }
    }

    async fn drive_read_pressure(fixture: &ReadonlyPressureFixture) {
        let pool = &fixture.pool;
        let pool_guard = pool.pool_guard();
        for i in 0..fixture.block_count {
            let block_id = fixture.base_block_id + i as u64;
            let g = pool.read_block(&pool_guard, block_id).await.unwrap();
            drop(g);
        }
        drop(pool_guard);
    }

    #[test]
    fn test_shared_evictor_skips_idle_domains() {
        let mut evictor = SharedEvictor::new(
            vec![
                SharedEvictionDomain::new(
                    SharedEvictionDomainId::Readonly,
                    MockRuntime::new(4, 8),
                    test_policy(),
                ),
                SharedEvictionDomain::new(
                    SharedEvictionDomainId::Mem,
                    MockRuntime::new(7, 8),
                    test_policy(),
                ),
                SharedEvictionDomain::new(
                    SharedEvictionDomainId::Index,
                    MockRuntime::new(3, 8),
                    test_policy(),
                ),
            ],
            Arc::new(AtomicBool::new(false)),
            Arc::new(Event::new()),
            SharedPoolEvictorStatsHandle::default(),
        );

        assert_eq!(evictor.next_ready_domain_index(), Some(1));
        assert_eq!(evictor.next_domain, 2);
    }

    #[test]
    fn test_shared_evictor_rotates_across_ready_domains() {
        let mut evictor = SharedEvictor::new(
            vec![
                SharedEvictionDomain::new(
                    SharedEvictionDomainId::Readonly,
                    MockRuntime::new(7, 8),
                    test_policy(),
                ),
                SharedEvictionDomain::new(
                    SharedEvictionDomainId::Mem,
                    MockRuntime::new(7, 8),
                    test_policy(),
                ),
                SharedEvictionDomain::new(
                    SharedEvictionDomainId::Index,
                    MockRuntime::new(4, 8),
                    test_policy(),
                ),
            ],
            Arc::new(AtomicBool::new(false)),
            Arc::new(Event::new()),
            SharedPoolEvictorStatsHandle::default(),
        );

        assert_eq!(evictor.next_ready_domain_index(), Some(0));
        assert_eq!(evictor.next_ready_domain_index(), Some(1));
        assert_eq!(evictor.next_ready_domain_index(), Some(0));
    }

    #[test]
    fn test_shared_evictor_stats_track_isolated_domain_pressure() {
        smol::block_on(async {
            {
                let root = TempDir::new().unwrap();
                let runtime = StartedSharedEvictorRuntime::new(root.path());
                let parked = runtime.wait_until_idle();
                assert!(parked.wait_count > 0);

                let readonly_fixture =
                    prepare_read_pressure(&runtime.fs, &runtime.disk_pool, 201).await;

                let start = runtime.stats();
                drive_read_pressure(&readonly_fixture).await;
                wait_for(|| runtime.stats().delta_since(start).readonly_runs > 0);
                let delta = runtime.stats().delta_since(start);
                assert!(delta.readonly_runs > 0);
                assert_eq!(delta.mem_runs, 0);
                assert_eq!(delta.index_runs, 0);
                drop(readonly_fixture);
            }

            {
                let root = TempDir::new().unwrap();
                let runtime = StartedSharedEvictorRuntime::new(root.path());
                runtime.wait_until_idle();

                let start = runtime.stats();
                allocate_with_pressure(&runtime.mem_pool, 192).await;
                wait_for(|| runtime.stats().delta_since(start).mem_runs > 0);
                let delta = runtime.stats().delta_since(start);
                assert_eq!(delta.readonly_runs, 0);
                assert!(delta.mem_runs > 0);
                assert_eq!(delta.index_runs, 0);
            }

            {
                let root = TempDir::new().unwrap();
                let runtime = StartedSharedEvictorRuntime::new(root.path());
                runtime.wait_until_idle();

                let start = runtime.stats();
                allocate_with_pressure(&runtime.index_pool, 192).await;
                wait_for(|| runtime.stats().delta_since(start).index_runs > 0);
                let delta = runtime.stats().delta_since(start);
                assert_eq!(delta.readonly_runs, 0);
                assert_eq!(delta.mem_runs, 0);
                assert!(delta.index_runs > 0);
            }
        });
    }

    #[test]
    fn test_shared_evictor_makes_progress_across_concurrent_domains() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let runtime = StartedSharedEvictorRuntime::new(root.path());
            runtime.wait_until_idle();

            let table_id = 202u64;
            let readonly_fixture =
                prepare_read_pressure(&runtime.fs, &runtime.disk_pool, table_id).await;

            let start = runtime.stats();
            let readonly_task = {
                let readonly_fixture = readonly_fixture.clone();
                smol::spawn(async move {
                    drive_read_pressure(&readonly_fixture).await;
                })
            };
            let mem_task = {
                let mem_pool = runtime.mem_pool.clone();
                smol::spawn(async move {
                    allocate_with_pressure(&mem_pool, 192).await;
                })
            };
            let index_task = {
                let index_pool = runtime.index_pool.clone();
                smol::spawn(async move {
                    allocate_with_pressure(&index_pool, 192).await;
                })
            };

            wait_for(|| {
                let delta = runtime.stats().delta_since(start);
                delta.readonly_runs > 0 && delta.mem_runs > 0 && delta.index_runs > 0
            });
            let delta = runtime.stats().delta_since(start);
            assert!(delta.readonly_runs > 0);
            assert!(delta.mem_runs > 0);
            assert!(delta.index_runs > 0);

            readonly_task.await;
            mem_task.await;
            index_task.await;
        });
    }
}
