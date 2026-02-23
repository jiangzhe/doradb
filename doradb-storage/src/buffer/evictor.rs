use crate::buffer::frame::{BufferFrames, FrameKind};
use crate::buffer::guard::PageExclusiveGuard;
use crate::buffer::page::{Page, PageID};
use crate::thread;
use event_listener::{EventListener, Listener};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::ops::{Range, RangeFrom, RangeTo};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
const DEFAULT_TARGET_FREE_RATIO: f64 = 0.10;
const DEFAULT_HYSTERESIS_RATIO: f64 = 0.30;

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
        if resident < min_resident {
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

    /// Returns a listener notified when eviction work may become available.
    fn work_listener(&self) -> EventListener;

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
    fn decide_batch<T: EvictionRuntime>(&self, runtime: &T) -> Option<usize> {
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
    fn select_candidates<T: EvictionRuntime>(
        &mut self,
        runtime: &T,
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

/// Generic eviction runner shared by mutable and readonly pools.
pub(super) struct Evictor<T> {
    runtime: T,
    policy: PressureDeltaClockPolicy,
    shutdown_flag: Arc<AtomicBool>,
}

impl<T> Evictor<T>
where
    T: EvictionRuntime + Send + 'static,
{
    #[inline]
    pub(super) fn new(
        runtime: T,
        policy: PressureDeltaClockPolicy,
        shutdown_flag: Arc<AtomicBool>,
    ) -> Self {
        Evictor {
            runtime,
            policy,
            shutdown_flag,
        }
    }

    #[inline]
    pub(super) fn start_thread(self, thread_name: &'static str) -> JoinHandle<()> {
        thread::spawn_named(thread_name, move || self.run())
    }

    #[inline]
    fn wait_for_work(&self) -> bool {
        // Register listener before re-checking pressure to avoid missed wakeups.
        let listener = self.runtime.work_listener();
        if self.shutdown_flag.load(Ordering::Acquire) {
            return false;
        }
        if self.policy.decide_batch(&self.runtime).is_some() {
            return true;
        }
        listener.wait();
        !self.shutdown_flag.load(Ordering::Acquire)
    }

    #[inline]
    pub(super) fn run(mut self) {
        loop {
            if self.shutdown_flag.load(Ordering::Acquire) {
                return;
            }

            let target = match self.policy.decide_batch(&self.runtime) {
                Some(target) if target > 0 => target,
                _ => {
                    if !self.wait_for_work() {
                        return;
                    }
                    continue;
                }
            };

            let candidates = self.policy.select_candidates(&self.runtime, target);
            if candidates.is_empty() {
                if !self.wait_for_work() {
                    return;
                }
                continue;
            }

            let waiter = self.runtime.execute(candidates);
            if self.shutdown_flag.load(Ordering::Acquire) {
                return;
            }
            if let Some(waiter) = waiter {
                waiter.wait();
            }
            self.runtime.notify_progress();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
