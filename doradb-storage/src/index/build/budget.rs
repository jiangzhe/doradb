use crate::error::{ResourceError, ResourceResult};
use error_stack::Report;
use std::alloc::Layout;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
pub(super) use tests::BudgetFailure;
#[cfg(test)]
pub(crate) use tests::{fail_after, fail_at};

struct BudgetState {
    limit: usize,
    used: AtomicUsize,
    #[cfg(test)]
    test: BudgetFailure,
    #[cfg(feature = "profiling")]
    peak: AtomicUsize,
}

/// Concurrent allocation admission for one resident index-build invocation.
/// Each caller defines its charged allocations and separately reports fixed
/// runtime and pool-owned costs; reservations can follow accepted storage work.
#[derive(Clone)]
pub(crate) struct MemoryBudget(Arc<BudgetState>);

impl MemoryBudget {
    /// Create a budget with no bulk allocations admitted yet.
    pub(crate) fn new(limit: usize) -> Self {
        Self(Arc::new(BudgetState {
            limit,
            used: AtomicUsize::new(0),
            #[cfg(test)]
            test: BudgetFailure::default(),
            #[cfg(feature = "profiling")]
            peak: AtomicUsize::new(0),
        }))
    }

    /// Reserve bytes before their allocation; release follows allocation ownership.
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

    fn admit(&self, bytes: usize, purpose: &'static str) -> ResourceResult<()> {
        #[cfg(test)]
        if self.0.test.rejects(purpose) {
            return Err(memory_error(bytes, self.used(), self.0.limit, purpose));
        }
        if bytes == 0 {
            return Ok(());
        }
        let mut used = self.0.used.load(Ordering::Relaxed);
        loop {
            let next = used
                .checked_add(bytes)
                .filter(|&next| next <= self.0.limit)
                .ok_or_else(|| memory_error(bytes, used, self.0.limit, purpose))?;
            match self
                .0
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => {
                    // Peak is monotonic; avoid another shared write when
                    // admitted buffers only revisit an earlier peak.
                    #[cfg(feature = "profiling")]
                    if next > self.0.peak.load(Ordering::Relaxed) {
                        self.0.peak.fetch_max(next, Ordering::Relaxed);
                    }
                    return Ok(());
                }
                Err(actual) => used = actual,
            }
        }
    }

    /// Return admitted bulk allocation bytes.
    pub(crate) fn used(&self) -> usize {
        self.0.used.load(Ordering::Acquire)
    }

    /// Assert quiescence and reset the next build's high-water to retained descriptors.
    pub(crate) fn reset_peak(&self, retained: usize) {
        assert_eq!(
            self.used(),
            retained,
            "hot-build scratch remains at index boundary"
        );
        #[cfg(feature = "profiling")]
        self.0.peak.store(retained, Ordering::Release);
    }

    /// Return the highest simultaneous bulk allocation capacity admitted.
    #[cfg(feature = "profiling")]
    pub(crate) fn peak(&self) -> usize {
        self.0.peak.load(Ordering::Acquire)
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
    pub(crate) fn new(budget: &MemoryBudget) -> Self {
        Self {
            budget: budget.clone(),
            bytes: 0,
        }
    }

    /// Add admission before extending an allocation owned by this reservation.
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

    /// Release a freed payload while retaining the reservation's budget owner.
    pub(crate) fn release(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        assert!(
            bytes <= self.bytes,
            "hot-build scratch release exceeds owned reservation"
        );
        self.bytes -= bytes;
        self.budget.0.used.fetch_sub(bytes, Ordering::AcqRel);
    }

    /// Release all admission after the associated allocations have been freed.
    pub(crate) fn release_all(&mut self) {
        self.release(self.bytes);
    }
}

impl Drop for MemoryReservation {
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

    /// Remove an unordered element while retaining its backing capacity.
    #[inline]
    pub(crate) fn swap_remove(&mut self, index: usize) -> T {
        self.values.swap_remove(index)
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
    #[cfg(any(test, feature = "profiling"))]
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
                self.reservation.budget.0.limit,
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
                        self.reservation.budget.0.limit,
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
    use super::MemoryBudget;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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

    /// Fail a named admission boundary to exercise production error/settlement paths.
    pub(crate) fn fail_at(budget: &MemoryBudget, purpose: &'static str) {
        fail_after(budget, purpose, 0);
    }

    /// Reject a named request after a deterministic number of successful admissions.
    pub(crate) fn fail_after(budget: &MemoryBudget, purpose: &'static str, successful: usize) {
        *budget.0.test.purpose.lock() = Some(purpose);
        budget.0.test.remaining.store(successful, Ordering::Release);
        budget.0.test.enabled.store(true, Ordering::Release);
    }
}
