use crate::error::{ResourceError, ResourceResult};
use error_stack::Report;
use std::alloc::Layout;
use std::cmp::Ordering as CmpOrdering;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
pub(super) use tests::BudgetFailure;
#[cfg(test)]
pub(crate) use tests::{fail_after, fail_at, payload_accounting};

/// Preferred payload refill for checkpoint extraction and assembly.
pub(crate) const PAYLOAD_CREDIT_BYTES: usize = 64 * 1024;

struct BudgetState {
    limit: usize,
    used: AtomicUsize,
    #[cfg(test)]
    test: BudgetFailure,
    #[cfg(any(test, feature = "profiling"))]
    peak: AtomicUsize,
}

/// Concurrent allocation admission for one resident index-build invocation.
/// Each caller defines its charged allocations and separately reports fixed
/// runtime and pool-owned costs; reservations can follow accepted storage work.
#[derive(Clone)]
pub(crate) struct MemoryBudget {
    state: Arc<BudgetState>,
    #[cfg(any(test, feature = "profiling"))]
    usage: Option<Arc<MemoryUsage>>,
}

impl MemoryBudget {
    /// Create a budget with no bulk allocations admitted yet.
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            state: Arc::new(BudgetState {
                limit,
                used: AtomicUsize::new(0),
                #[cfg(test)]
                test: BudgetFailure::default(),
                #[cfg(any(test, feature = "profiling"))]
                peak: AtomicUsize::new(0),
            }),
            #[cfg(any(test, feature = "profiling"))]
            usage: None,
        }
    }

    /// Observe this independent budget without imposing an aggregate admission limit.
    #[cfg(any(test, feature = "profiling"))]
    pub(crate) fn tracked(limit: usize, usage: &Arc<MemoryUsage>) -> Self {
        let mut budget = Self::new(limit);
        budget.usage = Some(usage.clone());
        budget
    }

    /// Share this ceiling for logical occupancy without counting payload allocations twice.
    pub(crate) fn quota(&self) -> Self {
        Self {
            state: self.state.clone(),
            #[cfg(any(test, feature = "profiling"))]
            usage: None,
        }
    }

    /// Reserve bytes before their allocation; release follows allocation ownership.
    #[inline]
    pub(crate) fn reserve(
        &self,
        bytes: usize,
        purpose: &'static str,
    ) -> ResourceResult<MemoryReservation> {
        self.admit(bytes, purpose)?;
        Ok(MemoryReservation {
            budget: self.clone(),
            bytes,
        })
    }

    #[inline]
    fn admit(&self, bytes: usize, purpose: &'static str) -> ResourceResult<()> {
        self.admit_preferred(bytes, bytes, purpose).map(|_| ())
    }

    #[inline]
    fn admit_preferred(
        &self,
        required: usize,
        preferred: usize,
        purpose: &'static str,
    ) -> ResourceResult<usize> {
        assert!(
            required <= preferred,
            "scratch admission preference is below requirement: required={required}, preferred={preferred}"
        );
        #[cfg(test)]
        if self.state.test.rejects(purpose) {
            return Err(memory_error(
                required,
                self.used(),
                self.state.limit,
                purpose,
            ));
        }
        if required == 0 {
            return Ok(0);
        }
        let mut used = self.state.used.load(Ordering::Relaxed);
        loop {
            let minimum = used
                .checked_add(required)
                .filter(|&next| next <= self.state.limit)
                .ok_or_else(|| memory_error(required, used, self.state.limit, purpose))?;
            let next = used
                .checked_add(preferred)
                .filter(|&next| next <= self.state.limit)
                .unwrap_or(minimum);
            match self.state.used.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Peak is monotonic; avoid another shared write when
                    // admitted buffers only revisit an earlier peak.
                    #[cfg(any(test, feature = "profiling"))]
                    if next > self.state.peak.load(Ordering::Relaxed) {
                        self.state.peak.fetch_max(next, Ordering::Relaxed);
                    }
                    #[cfg(any(test, feature = "profiling"))]
                    if let Some(usage) = &self.usage {
                        usage.admit(next - used);
                    }
                    return Ok(next - used);
                }
                Err(actual) => used = actual,
            }
        }
    }

    /// Return admitted bulk allocation bytes.
    #[inline]
    pub(crate) fn used(&self) -> usize {
        self.state.used.load(Ordering::Acquire)
    }

    /// Assert quiescence and reset the next build's high-water to retained descriptors.
    pub(crate) fn reset_peak(&self, retained: usize) {
        assert_eq!(
            self.used(),
            retained,
            "hot-build scratch remains at index boundary"
        );
        #[cfg(any(test, feature = "profiling"))]
        self.state.peak.store(retained, Ordering::Release);
    }

    /// Return the highest simultaneous bulk allocation capacity admitted.
    #[cfg(any(test, feature = "profiling"))]
    #[inline]
    pub(crate) fn peak(&self) -> usize {
        self.state.peak.load(Ordering::Acquire)
    }
}

/// Simultaneous allocation high-water across independently limited components.
#[cfg(any(test, feature = "profiling"))]
#[derive(Default)]
pub(crate) struct MemoryUsage {
    used: AtomicUsize,
    peak: AtomicUsize,
}

#[cfg(any(test, feature = "profiling"))]
impl MemoryUsage {
    #[inline]
    fn admit(&self, bytes: usize) {
        let used = self.used.fetch_add(bytes, Ordering::AcqRel) + bytes;
        self.peak.fetch_max(used, Ordering::Relaxed);
    }

    /// Read the largest simultaneous admitted allocation total.
    #[inline]
    pub(crate) fn peak(&self) -> usize {
        self.peak.load(Ordering::Acquire)
    }
}

/// Allocation-lifetime admission; moving a reservation never changes accounting.
pub(crate) struct MemoryReservation {
    budget: MemoryBudget,
    bytes: usize,
}

impl MemoryReservation {
    /// Transfer earmarked admission without releasing or charging it again.
    #[inline]
    pub(crate) fn split(&mut self, bytes: usize) -> Self {
        assert!(
            bytes <= self.bytes,
            "scratch subdivision exceeds reservation: bytes={bytes}, owned={}",
            self.bytes
        );
        self.bytes -= bytes;
        Self {
            budget: self.budget.clone(),
            bytes,
        }
    }

    /// Consume earmarked bytes, admitting only a missing remainder.
    #[inline]
    pub(crate) fn take(&mut self, bytes: usize, purpose: &'static str) -> ResourceResult<Self> {
        if bytes > self.bytes {
            self.grow(bytes - self.bytes, purpose)?;
        }
        Ok(self.split(bytes))
    }

    /// Return this owner's admitted allocation capacity.
    #[inline]
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Construct an empty reservation without admitting any bytes.
    #[inline]
    pub(crate) fn new(budget: &MemoryBudget) -> Self {
        Self {
            budget: budget.clone(),
            bytes: 0,
        }
    }

    /// Add admission before extending an allocation owned by this reservation.
    #[inline]
    pub(crate) fn grow(&mut self, bytes: usize, purpose: &'static str) -> ResourceResult<()> {
        if bytes == 0 {
            return Ok(());
        }
        self.budget.admit(bytes, purpose)?;
        // The checked aggregate admission includes our existing bytes, so this
        // addition cannot overflow. Grow in place without cloning an Arc token
        // for every outlined key.
        self.bytes += bytes;
        Ok(())
    }

    /// Admit preferred growth when it fits, otherwise only the required minimum.
    /// Both choices use the same atomic ceiling check; zero required growth is inert.
    #[inline]
    pub(crate) fn grow_preferred(
        &mut self,
        required: usize,
        preferred: usize,
        purpose: &'static str,
    ) -> ResourceResult<()> {
        if required == 0 {
            return Ok(());
        }
        let bytes = self.budget.admit_preferred(required, preferred, purpose)?;
        // Aggregate admission includes this owner's existing reservation.
        self.bytes += bytes;
        Ok(())
    }

    /// Release a freed payload while retaining the reservation's budget owner.
    #[inline]
    pub(crate) fn release(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        assert!(
            bytes <= self.bytes,
            "hot-build scratch release exceeds owned reservation"
        );
        self.bytes -= bytes;
        #[cfg(any(test, feature = "profiling"))]
        if let Some(usage) = &self.budget.usage {
            usage.used.fetch_sub(bytes, Ordering::AcqRel);
        }
        // Publish released usage before another reservation can reuse its capacity.
        self.budget.state.used.fetch_sub(bytes, Ordering::AcqRel);
    }

    /// Release all admission after the associated allocations have been freed.
    #[inline]
    pub(crate) fn release_all(&mut self) {
        self.release(self.bytes);
    }
}

impl Drop for MemoryReservation {
    #[inline]
    fn drop(&mut self) {
        self.release_all();
    }
}

/// Vector whose memory reservation outlives its backing allocation.
/// Allocator-provided excess capacity is outside the budget.
pub(crate) struct BudgetedVec<T> {
    values: Vec<T>,
    reservation: MemoryReservation,
}

impl<T> BudgetedVec<T> {
    /// Admit the maximum temporary element allocation used by a stable sort.
    fn reserve_sort_workspace(&self, purpose: &'static str) -> ResourceResult<MemoryReservation> {
        self.reservation
            .budget
            .reserve(self.len() * size_of::<T>(), purpose)
    }

    /// Move allocation and admission together into a downstream lifetime owner.
    #[inline]
    pub(super) fn into_parts(self) -> (Vec<T>, MemoryReservation) {
        (self.values, self.reservation)
    }

    /// Allocate exact element capacity from an existing earmark.
    pub(crate) fn from_reservation(mut reservation: MemoryReservation, capacity: usize) -> Self {
        let bytes = Layout::array::<T>(capacity)
            .unwrap_or_else(|_| unreachable!("admitted vector layout"))
            .size();
        let reservation = reservation.split(bytes);
        Self {
            values: Vec::with_capacity(capacity),
            reservation,
        }
    }

    /// Remove the final element while retaining backing allocation admission.
    #[inline]
    pub(crate) fn pop(&mut self) -> Option<T> {
        self.values.pop()
    }

    /// Admit the next insertion before performing work that cannot fail or await.
    #[inline]
    pub(crate) fn reserve_one(&mut self, purpose: &'static str) -> ResourceResult<()> {
        if self.values.len() == self.values.capacity() {
            self.ensure_capacity(self.values.capacity().max(4).saturating_mul(2), purpose)?;
        }
        Ok(())
    }

    /// Append to already admitted storage without allocation or failure.
    #[inline]
    pub(crate) fn push_reserved(&mut self, value: T) {
        assert!(
            self.values.len() < self.values.capacity(),
            "hot-build reserved insertion has no capacity"
        );
        self.values.push(value);
    }

    /// Drop elements while retaining both the allocation and its admission.
    #[inline]
    pub(crate) fn clear(&mut self) {
        self.values.clear();
    }

    /// Return allocated element capacity, independent of the current length.
    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.values.capacity()
    }

    /// Construct an empty charged vector without allocating element storage.
    #[inline]
    pub(crate) fn new(budget: &MemoryBudget) -> Self {
        Self {
            values: Vec::new(),
            reservation: MemoryReservation::new(budget),
        }
    }

    /// Ensure total capacity, accounting for both old and replacement buffers.
    pub(crate) fn ensure_capacity(
        &mut self,
        capacity: usize,
        purpose: &'static str,
    ) -> ResourceResult<()> {
        if capacity <= self.values.capacity() {
            return Ok(());
        }
        let layout = Layout::array::<T>(capacity).map_err(|_| {
            memory_error(
                usize::MAX,
                self.reservation.budget.used(),
                self.reservation.budget.state.limit,
                purpose,
            )
        })?;
        let new_reservation = self.reservation.budget.reserve(layout.size(), purpose)?;
        // Vec requests storage for exactly this capacity; allocator-provided
        // excess capacity is excluded alongside other allocator overhead.
        let mut replacement = Vec::with_capacity(capacity);
        replacement.append(&mut self.values);
        // Free the old buffer before its admission can be reused by another job.
        self.values = replacement;
        self.reservation = new_reservation;
        Ok(())
    }

    /// Append an element after admitting geometric capacity growth.
    #[inline]
    pub(crate) fn push(&mut self, value: T, purpose: &'static str) -> ResourceResult<()> {
        if self.values.len() == self.values.capacity() {
            let capacity = self
                .values
                .capacity()
                .max(4)
                .checked_mul(2)
                .ok_or_else(|| {
                    memory_error(
                        usize::MAX,
                        self.reservation.budget.used(),
                        self.reservation.budget.state.limit,
                        purpose,
                    )
                })?;
            self.ensure_capacity(capacity, purpose)?;
        }
        self.values.push(value);
        Ok(())
    }
}

impl<T> Deref for BudgetedVec<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        &self.values
    }
}

impl<T> DerefMut for BudgetedVec<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.values
    }
}

/// Heap storage owned by an entry, excluding its in-vector representation.
pub(crate) trait PayloadSize {
    /// Return bytes released when this entry is destroyed.
    fn payload_bytes(&self) -> usize;
}

/// Entry storage and its aggregate payload admission travel together.
/// Entries drop before the payload reservation; mutations cannot detach keys
/// from their accounting owner.
pub(crate) struct BudgetedEntries<T> {
    entries: BudgetedVec<T>,
    payload: MemoryReservation,
    used: usize,
    credit: usize,
}

impl<T: PayloadSize> BudgetedEntries<T> {
    /// Start collection with exact payload admission.
    #[inline]
    pub(crate) fn new(budget: &MemoryBudget) -> Self {
        Self::with_payload_credit(budget, 0)
    }

    /// Start collection with bounded preferred payload refills.
    #[inline]
    pub(crate) fn with_payload_credit(budget: &MemoryBudget, credit: usize) -> Self {
        Self {
            entries: BudgetedVec::new(budget),
            payload: MemoryReservation::new(budget),
            used: 0,
            credit,
        }
    }

    /// Admit replacement element storage after returning unused payload credit.
    #[inline]
    pub(crate) fn ensure_capacity(
        &mut self,
        capacity: usize,
        purpose: &'static str,
    ) -> ResourceResult<()> {
        if capacity > self.entries.capacity() {
            self.trim_payload();
            self.entries.ensure_capacity(capacity, purpose)?;
        }
        Ok(())
    }

    /// Admit storage before constructing an entry with the supplied owned size.
    /// Construction must neither fail nor await, and must own exactly `bytes`.
    #[inline]
    pub(crate) fn push_with(
        &mut self,
        bytes: usize,
        entry_purpose: &'static str,
        payload_purpose: &'static str,
        make: impl FnOnce() -> T,
    ) -> ResourceResult<()> {
        if self.entries.len() == self.entries.capacity() {
            self.trim_payload();
            self.entries.reserve_one(entry_purpose)?;
        }
        // Inline and borrowed entries leave payload accounting untouched.
        let used = if bytes == 0 {
            None
        } else {
            let used = self.used.checked_add(bytes).ok_or_else(|| {
                memory_error(
                    bytes,
                    self.payload.budget.used(),
                    self.payload.budget.state.limit,
                    payload_purpose,
                )
            })?;
            if used > self.payload.bytes() {
                let required = used - self.payload.bytes();
                self.payload.grow_preferred(
                    required,
                    if bytes > self.credit {
                        required
                    } else {
                        self.credit
                    },
                    payload_purpose,
                )?;
            }
            Some(used)
        };
        let entry = make();
        assert_eq!(
            entry.payload_bytes(),
            bytes,
            "budgeted entry payload differs from admitted construction: allocation={payload_purpose}"
        );
        self.entries.push_reserved(entry);
        if let Some(used) = used {
            self.used = used;
        }
        Ok(())
    }

    /// Return spare payload admission without disturbing retained entries.
    #[inline]
    pub(crate) fn trim_payload(&mut self) {
        self.payload.release(self.payload.bytes() - self.used);
    }

    /// Destroy a prefix, then return its payload and unused refill credit.
    pub(crate) fn consume_prefix(&mut self, count: usize) {
        let released: usize = self.entries[..count].iter().map(T::payload_bytes).sum();
        self.entries.rotate_left(count);
        for _ in 0..count {
            self.entries.pop();
        }
        self.used -= released;
        self.trim_payload();
    }

    /// Destroy all entries while retaining element capacity for reuse.
    #[inline]
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.used = 0;
        self.payload.release_all();
    }

    /// Reorder entries without changing their payload ownership.
    pub(crate) fn sort_unstable_by(&mut self, compare: impl FnMut(&T, &T) -> CmpOrdering) {
        self.trim_payload();
        self.entries.sort_unstable_by(compare);
    }

    /// Admit stable-sort workspace while preserving entry and payload ownership.
    pub(crate) fn sort_by(
        &mut self,
        compare: impl FnMut(&T, &T) -> CmpOrdering,
        purpose: &'static str,
    ) -> ResourceResult<()> {
        self.trim_payload();
        let _workspace = self.entries.reserve_sort_workspace(purpose)?;
        self.entries.sort_by(compare);
        Ok(())
    }

    /// Accounted element capacity and payload retained by this container.
    #[cfg(feature = "profiling")]
    #[inline]
    pub(crate) fn retained_bytes(&self) -> u64 {
        (self.entries.capacity() * size_of::<T>() + self.payload.bytes()) as u64
    }
}

impl<T> Deref for BudgetedEntries<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        &self.entries
    }
}

/// Conservative bounded-channel storage, including overlapping deque growth.
#[inline]
pub(super) fn handoff_bytes<T>(capacity: usize) -> Option<usize> {
    capacity
        .checked_next_power_of_two()?
        .checked_mul(2)?
        .checked_mul(size_of::<T>())
}

fn memory_error(
    requested: usize,
    used: usize,
    limit: usize,
    purpose: &'static str,
) -> Report<ResourceError> {
    Report::new(ResourceError::InsufficientMemory).attach(format!(
        "requested={requested}, used={used}, limit={limit}, allocation={purpose}"
    ))
}

#[cfg(test)]
mod tests {
    use super::{BudgetedEntries, MemoryBudget, MemoryReservation, MemoryUsage, PayloadSize};
    use parking_lot::Mutex;
    use std::cell::Cell;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    /// Named admission failure control with no mutex traffic while disabled.
    #[derive(Default)]
    pub(crate) struct BudgetFailure {
        enabled: AtomicBool,
        purpose: Mutex<Option<&'static str>>,
        remaining: AtomicUsize,
    }

    impl BudgetFailure {
        /// Check a configured failure only after the cheap enabled predicate.
        pub(super) fn rejects(&self, purpose: &'static str) -> bool {
            self.enabled.load(Ordering::Acquire)
                && *self.purpose.lock() == Some(purpose)
                && self
                    .remaining
                    .try_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                        left.checked_sub(1)
                    })
                    .is_err()
        }
    }

    struct Entry {
        payload: Box<[u8]>,
        order: u8,
        budget: MemoryBudget,
        dropped: Arc<Mutex<Vec<(u8, usize)>>>,
    }

    impl PayloadSize for Entry {
        fn payload_bytes(&self) -> usize {
            self.payload.len()
        }
    }

    impl Drop for Entry {
        fn drop(&mut self) {
            self.dropped.lock().push((self.order, self.budget.used()));
        }
    }

    /// Inspect live payload and admitted capacity without exposing mutable accounting.
    pub(crate) fn payload_accounting<T>(entries: &BudgetedEntries<T>) -> (usize, usize) {
        (entries.used, entries.payload.bytes())
    }

    /// Fail a named admission boundary to exercise production error/settlement paths.
    pub(crate) fn fail_at(budget: &MemoryBudget, purpose: &'static str) {
        fail_after(budget, purpose, 0);
    }

    /// Reject a named request after a deterministic number of successful admissions.
    pub(crate) fn fail_after(budget: &MemoryBudget, purpose: &'static str, successful: usize) {
        *budget.state.test.purpose.lock() = Some(purpose);
        budget
            .state
            .test
            .remaining
            .store(successful, Ordering::Release);
        budget.state.test.enabled.store(true, Ordering::Release);
    }

    fn append(
        entries: &mut BudgetedEntries<Entry>,
        budget: &MemoryBudget,
        dropped: &Arc<Mutex<Vec<(u8, usize)>>>,
        bytes: usize,
        order: u8,
    ) {
        entries
            .push_with(bytes, "test entries", "test payload", || Entry {
                payload: vec![order; bytes].into_boxed_slice(),
                order,
                budget: budget.clone(),
                dropped: dropped.clone(),
            })
            .unwrap();
    }

    /// Purpose: Measure simultaneous allocations across independent budgets without counting occupancy twice.
    /// Expected: Logical quotas enforce their ceiling, aggregate peaks exclude quotas and nonoverlapping allocations, and all usage is released.
    #[test]
    fn independent_budget_usage_excludes_occupancy_quotas() {
        let usage = Arc::new(MemoryUsage::default());
        let first = MemoryBudget::tracked(64, &usage);
        let second = MemoryBudget::tracked(128, &usage);
        let payload = first.reserve(48, "first payload").unwrap();
        let quota = first.quota().reserve(16, "pending payload quota").unwrap();
        assert_eq!(first.used(), 64);
        assert!(first.reserve(1, "over quota").is_err());
        let mut other = second.reserve(80, "second payload").unwrap();
        assert_eq!(usage.peak(), 128);
        drop(payload);
        other.grow(48, "second growth").unwrap();
        assert_eq!(usage.peak(), 128);
        drop(quota);
        assert_eq!(first.used(), 0);
        assert_eq!(usage.used.load(Ordering::Acquire), 128);
        drop(other);
        assert_eq!(second.used(), 0);
        assert_eq!(usage.used.load(Ordering::Acquire), 0);
    }

    /// Purpose: Preserve payload ownership through sorting, prefix consumption, reuse and destruction.
    /// Expected: Stable order is retained, each removed entry drops before its charge, and surviving entries remain fully admitted.
    #[test]
    fn entries_keep_payload_until_destruction() {
        for credit in [0, 128] {
            let budget = MemoryBudget::new(4096);
            let dropped = Arc::new(Mutex::new(Vec::new()));
            let mut entries = BudgetedEntries::with_payload_credit(&budget, credit);
            entries.ensure_capacity(4, "test entries").unwrap();
            let metadata = budget.used();
            for (bytes, order) in [(0, 2), (80, 1), (64, 1), (0, 0)] {
                append(&mut entries, &budget, &dropped, bytes, order);
            }
            assert_eq!(entries.used, 144);
            entries
                .sort_by(|a, b| a.order.cmp(&b.order), "test stable sort")
                .unwrap();
            assert_eq!(
                entries
                    .iter()
                    .map(|entry| (entry.order, entry.payload.len()))
                    .collect::<Vec<_>>(),
                [(0, 0), (1, 80), (1, 64), (2, 0)]
            );
            assert_eq!(budget.used(), metadata + 144);
            entries.consume_prefix(2);
            let mut consumed = dropped.lock().clone();
            consumed.sort_unstable();
            assert_eq!(consumed, [(0, metadata + 144), (1, metadata + 144)]);
            assert_eq!(budget.used(), metadata + 64);
            assert_eq!(entries.len(), 2);
            entries.clear();
            let mut cleared = dropped.lock()[2..].to_vec();
            cleared.sort_unstable();
            assert_eq!(cleared, [(1, metadata + 64), (2, metadata + 64)]);
            assert_eq!(budget.used(), metadata);
            append(&mut entries, &budget, &dropped, 96, 3);
            let retained = budget.used();
            drop(entries);
            assert_eq!(dropped.lock().last(), Some(&(3, retained)));
            assert_eq!(budget.used(), 0);
        }
    }

    /// Purpose: Admit construction only after both element and payload storage fit.
    /// Expected: Exact-fit and oversized payloads succeed, failure never invokes construction, and inline entries bypass payload admission.
    #[test]
    fn entries_admit_before_construction() {
        for credit in [0, 128] {
            for bytes in [0usize, 31, 257] {
                for insufficient in [false, true] {
                    let available = bytes.saturating_sub(usize::from(insufficient));
                    let budget = MemoryBudget::new(size_of::<Entry>() + available);
                    let mut entries = BudgetedEntries::with_payload_credit(&budget, credit);
                    entries.ensure_capacity(1, "test entries").unwrap();
                    if bytes == 0 {
                        fail_at(&budget, "test payload");
                    }
                    let built = Cell::new(false);
                    let result = entries.push_with(bytes, "test entries", "test payload", || {
                        built.set(true);
                        Entry {
                            payload: vec![0; bytes].into_boxed_slice(),
                            order: 0,
                            budget: budget.clone(),
                            dropped: Arc::default(),
                        }
                    });
                    let fits = !insufficient || bytes == 0;
                    assert_eq!(result.is_ok(), fits);
                    assert_eq!(built.get(), fits);
                    assert_eq!(entries.len(), usize::from(fits));
                    assert_eq!(entries.used, if fits { bytes } else { 0 });
                    assert_eq!(entries.payload.bytes(), entries.used);
                    drop(entries);
                    assert_eq!(budget.used(), 0);
                }
            }
        }
        let budget = MemoryBudget::new(4096);
        let mut entries = BudgetedEntries::<Entry>::new(&budget);
        fail_at(&budget, "test entries");
        assert!(
            entries
                .push_with(1, "test entries", "test payload", || {
                    panic!("construction must follow metadata admission")
                })
                .is_err()
        );
        assert_eq!(budget.used(), 0);
    }

    /// Purpose: Reuse bounded payload credit while admissions are disabled, then release it for metadata growth.
    /// Expected: Existing credit serves inserts without readmission, refill failure preserves contents, and spare credit cannot prevent fitting storage growth.
    #[test]
    fn entries_reuse_and_trim_credit() {
        let budget = MemoryBudget::new(4096);
        let dropped = Arc::new(Mutex::new(Vec::new()));
        let mut entries = BudgetedEntries::with_payload_credit(&budget, 128);
        entries.ensure_capacity(5, "test entries").unwrap();
        append(&mut entries, &budget, &dropped, 32, 0);
        fail_at(&budget, "test payload");
        for order in 1..4 {
            append(&mut entries, &budget, &dropped, 32, order);
        }
        assert_eq!(entries.used, 128);
        assert!(
            entries
                .push_with(1, "test entries", "test payload", || {
                    panic!("refill must precede construction")
                })
                .is_err()
        );
        assert_eq!(entries.len(), 4);
        entries.consume_prefix(4);
        assert_eq!(entries.payload.bytes(), 0);
        drop(entries);
        assert_eq!(budget.used(), 0);

        let metadata = size_of::<Entry>();
        let credit = 2 * metadata;
        let budget = MemoryBudget::new(3 * metadata + 10);
        let mut entries = BudgetedEntries::with_payload_credit(&budget, credit);
        entries.ensure_capacity(1, "test entries").unwrap();
        append(&mut entries, &budget, &dropped, 10, 5);
        assert_eq!(entries.payload.bytes(), credit);
        entries.ensure_capacity(2, "test entries").unwrap();
        assert_eq!(entries.payload.bytes(), 10);
        assert_eq!(budget.used(), 2 * metadata + 10);
        drop(entries);
        assert_eq!(budget.used(), 0);
    }

    /// Purpose: Admit optional reservation headroom without rejecting a fitting minimum.
    /// Expected: Preferred, minimum, zero and overflowing requests preserve the ceiling and exact ownership on success or failure.
    #[test]
    fn preferred_growth_respects_available_capacity() {
        for (limit, occupied, required, preferred, expected) in [
            (100, 0, 10, 32, Some(32)),
            (100, 80, 10, 32, Some(10)),
            (100, 95, 10, 32, None),
            (100, 100, 0, 32, Some(0)),
            (usize::MAX, usize::MAX - 10, 4, usize::MAX, Some(4)),
            (usize::MAX, usize::MAX - 10, 11, usize::MAX, None),
        ] {
            let budget = MemoryBudget::new(limit);
            let other = budget.reserve(occupied, "other owner").unwrap();
            let mut reservation = MemoryReservation::new(&budget);
            let result = reservation.grow_preferred(required, preferred, "preferred test");
            assert_eq!(result.is_ok(), expected.is_some());
            assert_eq!(reservation.bytes(), expected.unwrap_or(0));
            assert_eq!(budget.used(), occupied + expected.unwrap_or(0));
            drop(reservation);
            assert_eq!(budget.used(), occupied);
            drop(other);
            assert_eq!(budget.used(), 0);
        }
        let budget = MemoryBudget::new(100);
        let mut reservation = MemoryReservation::new(&budget);
        fail_at(&budget, "injected refill");
        assert!(
            reservation
                .grow_preferred(1, 32, "injected refill")
                .is_err()
        );
        assert_eq!(budget.used(), 0);
        assert_eq!(reservation.bytes(), 0);
    }

    /// Purpose: Exercise preferred and minimum admissions competing for one shared ceiling.
    /// Expected: All fitting minima remain admitted, extra growth fails while owners are live, and settlement releases every byte.
    #[test]
    fn concurrent_preferred_growth_keeps_the_ceiling() {
        let budget = MemoryBudget::new(80);
        let barrier = Barrier::new(5);
        thread::scope(|scope| {
            let jobs: Vec<_> = (0..4)
                .map(|_| {
                    let budget = &budget;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        let mut reservation = MemoryReservation::new(budget);
                        let result = reservation.grow_preferred(8, 32, "concurrent refill");
                        barrier.wait();
                        barrier.wait();
                        result.unwrap();
                        reservation.bytes()
                    })
                })
                .collect();
            barrier.wait();
            let used = budget.used();
            let exhausted = budget.reserve(1, "beyond ceiling").is_err();
            barrier.wait();
            let mut admitted: Vec<_> = jobs.into_iter().map(|job| job.join().unwrap()).collect();
            admitted.sort_unstable();
            assert_eq!(admitted, [8, 8, 32, 32]);
            assert_eq!(used, 80);
            assert!(exhausted);
        });
        assert_eq!(budget.used(), 0);
    }
}
