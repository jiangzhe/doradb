//! Shared reserve/publish primitives for buffer-pool page loads.
//!
//! This module intentionally stays small. It does not own any request or
//! submission state machine. Readonly and evictable pools each build their own
//! worker submissions on top of the shared reserve/publish contract here.

use crate::buffer::page::{Page, PageID};
use crate::error::Result;

/// Pool-specific lifecycle for one reserved page-load destination.
///
/// A reservation owns exclusive access to the destination page until it is
/// either published successfully or rolled back. `publish(self)` consumes the
/// reservation so the success path cannot accidentally trigger rollback later.
pub(crate) trait PageReservation {
    /// Returns the reserved page bytes for read-only inspection before publish.
    fn page(&self) -> &Page;
    /// Returns the reserved page bytes for DMA or decode before publish.
    fn page_mut(&mut self) -> &mut Page;
    /// Publishes the reserved page into the owning pool and returns its page id.
    fn publish(self) -> Result<PageID>;
    /// Reverts the reservation and releases any pool-specific resources.
    fn rollback(self);
}

/// Arms one page reservation and guarantees rollback if the owner drops it.
///
/// The guard exists to make the common lifecycle explicit:
/// reserve page memory, fill it, then either publish once or let drop roll it
/// back automatically.
pub(crate) struct PageReservationGuard<R: PageReservation> {
    reservation: Option<R>,
}

impl<R: PageReservation> PageReservationGuard<R> {
    #[inline]
    /// Wraps one live reservation so rollback happens automatically on drop.
    pub(crate) fn new(reservation: R) -> Self {
        PageReservationGuard {
            reservation: Some(reservation),
        }
    }

    #[inline]
    /// Returns the reserved page bytes for validation before publish.
    pub(crate) fn page(&self) -> &Page {
        self.reservation
            .as_ref()
            .expect("page reservation must hold backend state before publish")
            .page()
    }

    #[inline]
    /// Returns the reserved page bytes for mutation before publish.
    pub(crate) fn page_mut(&mut self) -> &mut Page {
        self.reservation
            .as_mut()
            .expect("page reservation must hold backend state before publish")
            .page_mut()
    }

    #[inline]
    /// Consumes the guard and publishes the reserved page into its owning pool.
    pub(crate) fn publish(mut self) -> Result<PageID> {
        self.reservation
            .take()
            .expect("page reservation must hold backend state before publish")
            .publish()
    }
}

impl<R: PageReservation> Drop for PageReservationGuard<R> {
    #[inline]
    fn drop(&mut self) {
        if let Some(reservation) = self.reservation.take() {
            reservation.rollback();
        }
    }
}
