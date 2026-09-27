use crate::error::{ResourceError, ResourceResult};
use error_stack::Report;
use std::alloc::Layout;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
pub(super) use tests::{BudgetFailure, fail_at};

struct BudgetState {
    limit: usize,
    used: AtomicUsize,
    #[cfg(feature = "profiling")]
    peak: AtomicUsize,
    #[cfg(test)]
    test: BudgetFailure,
}

/// Concurrent admission for bulk scratch buffers shared by one index build.
/// Bookkeeping and bounded worker temporaries are outside this accounting.
#[derive(Clone)]
pub(crate) struct MemoryBudget(Arc<BudgetState>);

impl MemoryBudget {
    /// Create a budget with no bulk allocations admitted yet.
    pub(crate) fn new(limit: usize) -> Self {
        Self(Arc::new(BudgetState {
            limit,
            used: AtomicUsize::new(0),
            #[cfg(feature = "profiling")]
            peak: AtomicUsize::new(0),
            #[cfg(test)]
            test: BudgetFailure::default(),
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
    /// Construct an empty charged vector without allocating element storage.
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

    fn deref(&self) -> &[T] {
        &self.values
    }
}

impl<T> DerefMut for BudgetedVec<T> {
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
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Named admission failure control with no mutex traffic while disabled.
    #[derive(Default)]
    pub(crate) struct BudgetFailure {
        enabled: AtomicBool,
        purpose: Mutex<Option<&'static str>>,
    }

    impl BudgetFailure {
        /// Check a configured failure only after the cheap enabled predicate.
        pub(super) fn rejects(&self, purpose: &'static str) -> bool {
            self.enabled.load(Ordering::Acquire) && *self.purpose.lock() == Some(purpose)
        }
    }

    /// Fail a named admission boundary to exercise production error/settlement paths.
    pub(crate) fn fail_at(budget: &MemoryBudget, purpose: &'static str) {
        *budget.0.test.purpose.lock() = Some(purpose);
        budget.0.test.enabled.store(true, Ordering::Release);
    }
}
