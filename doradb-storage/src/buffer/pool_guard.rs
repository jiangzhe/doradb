use crate::buffer::identity::{PoolIdentity, RowPoolIdentity};
use crate::quiescent::SyncQuiescentGuard;

/// Cloneable keepalive guard branded with one buffer-pool identity.
#[derive(Clone)]
pub struct PoolGuard {
    identity: PoolIdentity,
    _keepalive: SyncQuiescentGuard<()>,
}

impl PoolGuard {
    #[inline]
    pub(crate) fn new(identity: PoolIdentity, keepalive: SyncQuiescentGuard<()>) -> Self {
        Self {
            identity,
            _keepalive: keepalive,
        }
    }

    #[inline]
    pub(crate) fn identity(&self) -> PoolIdentity {
        self.identity
    }

    #[inline]
    pub(crate) fn assert_matches(&self, expected: PoolIdentity, context: &'static str) {
        if self.identity != expected {
            pool_guard_identity_mismatch(context, expected, self.identity);
        }
    }
}

/// Bundle of pool guards used by storage operations that touch multiple pools.
#[derive(Clone, Default)]
pub struct PoolGuards {
    guards: Box<[PoolGuard]>,
}

/// Builder for assembling a [`PoolGuards`] bundle slot by slot.
#[derive(Default)]
pub struct PoolGuardsBuilder {
    guards: Vec<PoolGuard>,
}

impl PoolGuards {
    /// Create a builder for assembling a pool-guard bundle.
    #[inline]
    pub fn builder() -> PoolGuardsBuilder {
        PoolGuardsBuilder::default()
    }

    /// Returns the guard for the metadata pool.
    #[inline]
    pub fn meta_guard(&self) -> &PoolGuard {
        require_guard_slot(self.try_guard(PoolIdentity::Meta), "meta")
    }

    /// Returns the guard for the secondary-index pool.
    #[inline]
    pub fn index_guard(&self) -> &PoolGuard {
        require_guard_slot(self.try_guard(PoolIdentity::Index), "index")
    }

    /// Returns the guard for the in-memory row-page pool.
    #[inline]
    pub fn mem_guard(&self) -> &PoolGuard {
        require_guard_slot(self.try_guard(PoolIdentity::Mem), "mem")
    }

    /// Returns the guard for the persisted read-only page pool.
    #[inline]
    pub fn disk_guard(&self) -> &PoolGuard {
        require_guard_slot(self.try_guard(PoolIdentity::Disk), "disk")
    }

    #[inline]
    pub(crate) fn try_guard(&self, identity: PoolIdentity) -> Option<&PoolGuard> {
        self.guards
            .iter()
            .find(|guard| guard.identity() == identity)
    }

    #[inline]
    pub(crate) fn try_row_guard(&self, identity: RowPoolIdentity) -> Option<&PoolGuard> {
        self.try_guard(identity.into())
    }
}

impl PoolGuardsBuilder {
    /// Add one pool guard to the bundle.
    #[inline]
    pub fn push(mut self, guard: PoolGuard) -> Self {
        push_guard_slot(&self.guards, guard.identity());
        self.guards.push(guard);
        self
    }

    /// Finalize the builder into a guard bundle.
    #[inline]
    pub fn build(self) -> PoolGuards {
        PoolGuards {
            guards: self.guards.into_boxed_slice(),
        }
    }
}

#[inline]
fn push_guard_slot(slots: &[PoolGuard], identity: PoolIdentity) {
    if slots.iter().any(|existing| existing.identity() == identity) {
        duplicate_guard_slot(identity);
    }
}

#[inline]
fn require_guard_slot<'a>(slot: Option<&'a PoolGuard>, name: &'static str) -> &'a PoolGuard {
    slot.unwrap_or_else(|| missing_guard_slot(name))
}

#[cold]
fn missing_guard_slot(name: &'static str) -> ! {
    panic!("missing {name} pool guard");
}

#[cold]
fn duplicate_guard_slot(identity: PoolIdentity) -> ! {
    panic!("duplicate pool guard identity: {identity:?}");
}

#[cold]
fn pool_guard_identity_mismatch(
    context: &'static str,
    expected: PoolIdentity,
    actual: PoolIdentity,
) -> ! {
    panic!("pool guard identity mismatch in {context}: expected {expected:?}, got {actual:?}");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_guard(identity: PoolIdentity) -> PoolGuard {
        let owner = Box::leak(Box::new(crate::quiescent::QuiescentBox::new(())));
        PoolGuard::new(identity, owner.guard().into_sync())
    }

    #[test]
    fn test_pool_guards_builder_empty_builds_partial() {
        let guards = PoolGuards::builder().build();
        assert!(guards.try_guard(PoolIdentity::Meta).is_none());
    }

    #[test]
    fn test_pool_guards_builder_duplicate_slot_panics() {
        let guard = test_guard(PoolIdentity::Meta);
        let result = std::panic::catch_unwind(|| {
            let _ = PoolGuards::builder()
                .push(guard.clone())
                .push(guard)
                .build();
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_pool_guards_builder_builds_explicit_slot() {
        let guard = test_guard(PoolIdentity::Meta);
        let guard_id = guard.identity();
        let guards = PoolGuards::builder().push(guard).build();
        assert!(guards.try_guard(guard_id).is_some());
    }

    #[test]
    fn test_pool_guards_try_guard_matches_identity() {
        let meta_guard = test_guard(PoolIdentity::Meta);
        let index_guard = test_guard(PoolIdentity::Index);
        let mem_guard = test_guard(PoolIdentity::Mem);
        let disk_guard = test_guard(PoolIdentity::Disk);
        let guards = PoolGuards::builder()
            .push(meta_guard.clone())
            .push(index_guard.clone())
            .push(mem_guard.clone())
            .push(disk_guard.clone())
            .build();
        assert_eq!(
            guards
                .try_guard(meta_guard.identity())
                .map(PoolGuard::identity),
            Some(meta_guard.identity())
        );
        assert_eq!(
            guards
                .try_guard(index_guard.identity())
                .map(PoolGuard::identity),
            Some(index_guard.identity())
        );
        assert_eq!(
            guards
                .try_guard(mem_guard.identity())
                .map(PoolGuard::identity),
            Some(mem_guard.identity())
        );
        assert_eq!(
            guards
                .try_guard(disk_guard.identity())
                .map(PoolGuard::identity),
            Some(disk_guard.identity())
        );
    }

    #[test]
    fn test_pool_guards_named_getters_return_configured_slots() {
        let guards = PoolGuards::builder()
            .push(test_guard(PoolIdentity::Meta))
            .push(test_guard(PoolIdentity::Index))
            .push(test_guard(PoolIdentity::Mem))
            .push(test_guard(PoolIdentity::Disk))
            .build();
        let _ = guards.meta_guard();
        let _ = guards.index_guard();
        let _ = guards.mem_guard();
        let _ = guards.disk_guard();
    }

    #[test]
    #[should_panic(expected = "missing meta pool guard")]
    fn test_pool_guards_meta_guard_panics_when_slot_missing() {
        let guards = PoolGuards::builder().build();
        let _ = guards.meta_guard();
    }

    #[test]
    #[should_panic(expected = "missing index pool guard")]
    fn test_pool_guards_index_guard_panics_when_slot_missing() {
        let guards = PoolGuards::builder().build();
        let _ = guards.index_guard();
    }

    #[test]
    #[should_panic(expected = "missing mem pool guard")]
    fn test_pool_guards_mem_guard_panics_when_slot_missing() {
        let guards = PoolGuards::builder().build();
        let _ = guards.mem_guard();
    }

    #[test]
    #[should_panic(expected = "missing disk pool guard")]
    fn test_pool_guards_disk_guard_panics_when_slot_missing() {
        let guards = PoolGuards::builder().build();
        let _ = guards.disk_guard();
    }

    #[test]
    #[should_panic(expected = "pool guard identity mismatch")]
    fn test_pool_guard_assert_matches_panics_on_identity_mismatch() {
        let guard = test_guard(PoolIdentity::Meta);
        guard.assert_matches(PoolIdentity::Index, "test");
    }
}
