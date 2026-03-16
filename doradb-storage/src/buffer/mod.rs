mod arena;
mod evict;
mod evictor;
mod fixed;
pub mod frame;
pub mod guard;
pub mod page;
mod readonly;
mod util;

pub use evict::{EvictableBufferPool, EvictableBufferPoolConfig};
pub use evictor::{EvictionArbiter, EvictionArbiterBuilder};
pub use fixed::FixedBufferPool;
pub use readonly::{
    GlobalReadonlyBufferPool, ReadonlyBufferPool, ReadonlyCacheKey, ReadonlyPageSource,
};

/// Physical file identity used by the shared readonly cache.
pub type ReadonlyFileID = u64;

use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::{BufferPage, PageID, VersionedPageID};
use crate::error::Result;
use crate::error::Validation;
use crate::latch::LatchFallbackMode;
use crate::lifetime::StaticLifetime;
use crate::quiescent::SyncQuiescentGuard;
use std::future::Future;

pub type PoolGuard = SyncQuiescentGuard<()>;

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PoolGuardSlot {
    Meta,
    Index,
    Mem,
    Disk,
}

#[derive(Clone, Default)]
pub struct PoolGuards {
    meta: Option<PoolGuard>,
    index: Option<PoolGuard>,
    mem: Option<PoolGuard>,
    disk: Option<PoolGuard>,
}

#[derive(Default)]
pub struct PoolGuardsBuilder {
    guards: PoolGuards,
}

impl PoolGuards {
    #[inline]
    pub fn builder() -> PoolGuardsBuilder {
        PoolGuardsBuilder::default()
    }

    #[inline]
    pub(crate) fn try_guard(&self, slot: PoolGuardSlot) -> Option<&PoolGuard> {
        match slot {
            PoolGuardSlot::Meta => self.meta.as_ref(),
            PoolGuardSlot::Index => self.index.as_ref(),
            PoolGuardSlot::Mem => self.mem.as_ref(),
            PoolGuardSlot::Disk => self.disk.as_ref(),
        }
    }
}

impl PoolGuardsBuilder {
    #[inline]
    pub fn meta(mut self, guard: PoolGuard) -> Self {
        set_guard_slot(&mut self.guards.meta, Some(guard), "meta");
        self
    }

    #[inline]
    pub fn index(mut self, guard: PoolGuard) -> Self {
        set_guard_slot(&mut self.guards.index, Some(guard), "index");
        self
    }

    #[inline]
    pub fn mem(mut self, guard: PoolGuard) -> Self {
        set_guard_slot(&mut self.guards.mem, Some(guard), "mem");
        self
    }

    #[inline]
    pub fn disk(mut self, guard: PoolGuard) -> Self {
        set_guard_slot(&mut self.guards.disk, Some(guard), "disk");
        self
    }

    #[inline]
    pub fn build(self) -> PoolGuards {
        self.guards
    }
}

#[inline]
fn set_guard_slot(slot: &mut Option<PoolGuard>, incoming: Option<PoolGuard>, name: &'static str) {
    if slot.is_some() && incoming.is_some() {
        duplicate_guard_slot(name);
    }
    if slot.is_none() {
        *slot = incoming;
    }
}

#[cold]
fn duplicate_guard_slot(name: &'static str) -> ! {
    panic!("duplicate pool guard slot: {name}");
}

/// Abstraction of buffer pool.
/// The implementation should be a static pointer providing
/// pooling functionality.
pub trait BufferPool: Send + Sync + StaticLifetime + 'static {
    /// Returns the maximum number of pages that can be allocated.
    fn capacity(&self) -> usize;

    /// Returns the number of allocated pages.
    fn allocated(&self) -> usize;

    /// Returns a cloneable keepalive guard for this pool.
    fn guard(&self) -> PoolGuard;

    /// Allocate a new page.
    ///
    /// Caller must pass the guard from the same pool instance the method is
    /// invoked on. This task does not add a runtime pool-identity check.
    fn allocate_page<T: BufferPage>(
        &'static self,
        guard: &PoolGuard,
    ) -> impl Future<Output = PageExclusiveGuard<T>> + Send;

    /// Allocate a new page at given id(offset);
    fn allocate_page_at<T: BufferPage>(
        &'static self,
        guard: &PoolGuard,
        page_id: PageID,
    ) -> impl Future<Output = Result<PageExclusiveGuard<T>>> + Send;

    /// Get page.
    fn get_page<T: BufferPage>(
        &'static self,
        guard: &PoolGuard,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = FacadePageGuard<T>> + Send;

    /// Get page by versioned page identity.
    /// Returns None if page is unavailable or version mismatches.
    fn try_get_page_versioned<T: BufferPage>(
        &'static self,
        guard: &PoolGuard,
        id: VersionedPageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = Option<FacadePageGuard<T>>> + Send;

    /// Deallocate page.
    fn deallocate_page<T: BufferPage>(&'static self, g: PageExclusiveGuard<T>);

    /// Get child page.
    /// This method is used for tree-like data structure with lock coupling support.
    /// The implementation has to validate the parent page when child page is returned,
    /// to ensure no change happens in-between.
    fn get_child_page<T: BufferPage>(
        &'static self,
        guard: &PoolGuard,
        p_guard: &FacadePageGuard<T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> impl Future<Output = Validation<FacadePageGuard<T>>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quiescent::QuiescentBox;

    #[test]
    fn test_pool_guards_builder_empty_builds_partial() {
        let guards = PoolGuards::builder().build();
        assert!(guards.try_guard(PoolGuardSlot::Meta).is_none());
        assert!(guards.try_guard(PoolGuardSlot::Index).is_none());
        assert!(guards.try_guard(PoolGuardSlot::Mem).is_none());
        assert!(guards.try_guard(PoolGuardSlot::Disk).is_none());
    }

    #[test]
    fn test_pool_guards_builder_duplicate_slot_panics() {
        let owner = QuiescentBox::new(());
        let guard = owner.guard().into_sync();
        let result = std::panic::catch_unwind(|| {
            let _ = PoolGuards::builder()
                .meta(guard.clone())
                .meta(guard)
                .build();
        });
        assert!(result.is_err());
        drop(owner);
    }

    #[test]
    fn test_pool_guards_builder_builds_explicit_slot() {
        let owner = QuiescentBox::new(());
        let guard = owner.guard().into_sync();
        let guards = PoolGuards::builder().meta(guard).build();
        assert!(guards.try_guard(PoolGuardSlot::Meta).is_some());
        assert!(guards.try_guard(PoolGuardSlot::Index).is_none());
        assert!(guards.try_guard(PoolGuardSlot::Mem).is_none());
        assert!(guards.try_guard(PoolGuardSlot::Disk).is_none());
        drop(guards);
        drop(owner);
    }

    #[test]
    fn test_pool_guards_field_is_none_when_slot_missing() {
        let guards = PoolGuards::builder().build();
        assert!(guards.try_guard(PoolGuardSlot::Meta).is_none());
    }
}
