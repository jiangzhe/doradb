use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

/// Shared eviction tuning for evictable and readonly buffer pools.
///
/// Semantics:
/// - trigger eviction when `free_frames < target_free` OR failure-rate reaches threshold;
/// - stop when `free_frames >= target_free + hysteresis` AND failure-rate recovers;
/// - choose dynamic batch size within `[min_batch, max_batch]`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct EvictionTuning {
    /// Target number of free frames to keep available.
    ///
    /// `0` means "derive from capacity" (10% of capacity, at least 1).
    pub target_free: usize,
    /// Extra free-frame margin required to stop active eviction.
    ///
    /// `0` means "derive from target_free" (`target_free / 2`, at least 1).
    pub hysteresis: usize,
    /// Allocation failure-rate threshold in range `[0.0, 1.0]`.
    pub failure_rate_threshold: f64,
    /// Sliding window size used by failure-rate tracking.
    pub failure_window: usize,
    /// Minimum eviction batch size when eviction is triggered.
    pub min_batch: usize,
    /// Maximum eviction batch size.
    pub max_batch: usize,
}

impl Default for EvictionTuning {
    #[inline]
    fn default() -> Self {
        EvictionTuning {
            target_free: 0,
            hysteresis: 0,
            failure_rate_threshold: 0.25,
            failure_window: 1024,
            min_batch: 8,
            max_batch: 64,
        }
    }
}

impl EvictionTuning {
    #[inline]
    fn normalized(self, capacity: usize) -> Self {
        let mut tuning = self;
        let capacity = capacity.max(1);
        if tuning.target_free == 0 {
            tuning.target_free = (capacity / 10).max(1);
        }
        if tuning.hysteresis == 0 {
            tuning.hysteresis = (tuning.target_free / 2).max(1);
        }
        if tuning.failure_window == 0 {
            tuning.failure_window = 1;
        }
        if !tuning.failure_rate_threshold.is_finite() {
            tuning.failure_rate_threshold = 1.0;
        }
        tuning.failure_rate_threshold = tuning.failure_rate_threshold.clamp(0.0, 1.0);
        if tuning.min_batch == 0 {
            tuning.min_batch = 1;
        }
        if tuning.max_batch == 0 {
            tuning.max_batch = tuning.min_batch;
        }
        if tuning.max_batch < tuning.min_batch {
            tuning.max_batch = tuning.min_batch;
        }
        tuning.target_free = tuning.target_free.min(capacity);
        tuning
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EvictionDecision {
    pub batch_size: usize,
}

/// Shared pressure-delta + hysteresis eviction decision.
#[inline]
pub fn decide_eviction(
    resident: usize,
    capacity: usize,
    inflight_evicts: usize,
    failure_rate: f64,
    tuning: EvictionTuning,
    min_resident: usize,
) -> Option<EvictionDecision> {
    if resident < min_resident {
        return None;
    }
    let capacity = capacity.max(1);
    let tuning = tuning.normalized(capacity);
    let free_frames = capacity.saturating_sub(resident);
    let trigger_pressure = free_frames < tuning.target_free;
    let trigger_failures = failure_rate >= tuning.failure_rate_threshold;
    if !trigger_pressure && !trigger_failures {
        return None;
    }

    let stop_free = tuning.target_free.saturating_add(tuning.hysteresis);
    let recovered = free_frames >= stop_free && failure_rate < tuning.failure_rate_threshold;
    if recovered {
        return None;
    }

    let delta = stop_free.saturating_sub(free_frames);
    let mut batch = delta.max(tuning.min_batch);
    if trigger_failures && tuning.failure_rate_threshold < 1.0 {
        let ratio = ((failure_rate - tuning.failure_rate_threshold)
            / (1.0 - tuning.failure_rate_threshold))
            .clamp(0.0, 1.0);
        let span = tuning.max_batch.saturating_sub(tuning.min_batch);
        let boosted = tuning.min_batch + ((span as f64) * ratio) as usize;
        batch = batch.max(boosted);
    }
    batch = batch.min(tuning.max_batch);
    batch = batch.saturating_sub(inflight_evicts);
    if batch == 0 {
        return None;
    }
    Some(EvictionDecision { batch_size: batch })
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
    fn test_decide_eviction_pressure_delta_and_hysteresis() {
        let tuning = EvictionTuning {
            target_free: 10,
            hysteresis: 4,
            failure_rate_threshold: 0.3,
            failure_window: 128,
            min_batch: 4,
            max_batch: 32,
        };

        // Triggered by low free frames.
        let decision = decide_eviction(95, 100, 0, 0.0, tuning, 1).unwrap();
        assert!(decision.batch_size >= 9);

        // Stop condition satisfied.
        let no_evict = decide_eviction(84, 100, 0, 0.0, tuning, 1);
        assert!(no_evict.is_none());
    }

    #[test]
    fn test_decide_eviction_failure_rate_trigger_and_dynamic_batch() {
        let tuning = EvictionTuning {
            target_free: 10,
            hysteresis: 4,
            failure_rate_threshold: 0.2,
            failure_window: 128,
            min_batch: 4,
            max_batch: 40,
        };

        // Plenty of free frames, but failure rate is high so eviction still triggers.
        let decision = decide_eviction(50, 100, 0, 0.9, tuning, 1).unwrap();
        assert!(decision.batch_size >= 20);

        // Inflight evictions reduce effective batch.
        let decision = decide_eviction(50, 100, 8, 0.9, tuning, 1).unwrap();
        assert!(decision.batch_size < 40);
    }
}
