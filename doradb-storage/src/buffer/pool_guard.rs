use crate::buffer::identity::{PoolIdentity, PoolRole, RowPoolRole};
use crate::quiescent::SyncQuiescentGuard;

/// Cloneable keepalive guard branded with one exact buffer-pool identity.
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
        let actual = self.identity();
        if actual != expected {
            pool_guard_identity_mismatch(context, expected, actual);
        }
    }
}

/// Bundle of pool guards used by storage operations that touch multiple pools.
#[derive(Clone, Default)]
pub struct PoolGuards {
    meta: Option<PoolGuard>,
    index: Option<PoolGuard>,
    mem: Option<PoolGuard>,
    disk: Option<PoolGuard>,
}

/// Builder for assembling a [`PoolGuards`] bundle by named pool role.
#[derive(Default)]
pub struct PoolGuardsBuilder {
    meta: Option<PoolGuard>,
    index: Option<PoolGuard>,
    mem: Option<PoolGuard>,
    disk: Option<PoolGuard>,
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
        require_guard_slot(self.meta.as_ref(), "meta")
    }

    /// Returns the guard for the secondary-index pool.
    #[inline]
    pub fn index_guard(&self) -> &PoolGuard {
        require_guard_slot(self.index.as_ref(), "index")
    }

    /// Returns the guard for the in-memory row-page pool.
    #[inline]
    pub fn mem_guard(&self) -> &PoolGuard {
        require_guard_slot(self.mem.as_ref(), "mem")
    }

    /// Returns the guard for the persisted read-only page pool.
    #[inline]
    pub fn disk_guard(&self) -> &PoolGuard {
        require_guard_slot(self.disk.as_ref(), "disk")
    }

    #[inline]
    pub(crate) fn try_guard(&self, role: PoolRole) -> Option<&PoolGuard> {
        match role {
            PoolRole::Invalid => None,
            PoolRole::Meta => self.meta.as_ref(),
            PoolRole::Index => self.index.as_ref(),
            PoolRole::Mem => self.mem.as_ref(),
            PoolRole::Disk => self.disk.as_ref(),
        }
    }

    #[inline]
    pub(crate) fn try_row_guard(&self, role: RowPoolRole) -> Option<&PoolGuard> {
        self.try_guard(role.into())
    }
}

impl PoolGuardsBuilder {
    /// Add one pool guard to the bundle.
    #[inline]
    pub fn push(mut self, role: PoolRole, guard: PoolGuard) -> Self {
        push_guard_slot(self.slot_mut(role), role);
        *self.slot_mut(role) = Some(guard);
        self
    }

    /// Finalize the builder into a guard bundle.
    #[inline]
    pub fn build(self) -> PoolGuards {
        PoolGuards {
            meta: self.meta,
            index: self.index,
            mem: self.mem,
            disk: self.disk,
        }
    }

    #[inline]
    fn slot_mut(&mut self, role: PoolRole) -> &mut Option<PoolGuard> {
        match role {
            PoolRole::Invalid => invalid_guard_slot(),
            PoolRole::Meta => &mut self.meta,
            PoolRole::Index => &mut self.index,
            PoolRole::Mem => &mut self.mem,
            PoolRole::Disk => &mut self.disk,
        }
    }
}

#[inline]
fn push_guard_slot(slot: &mut Option<PoolGuard>, role: PoolRole) {
    if slot.is_some() {
        duplicate_guard_slot(role);
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
fn invalid_guard_slot() -> ! {
    panic!("invalid pool role in pool guards builder");
}

#[cold]
fn duplicate_guard_slot(role: PoolRole) -> ! {
    panic!("duplicate pool guard role: {role:?}");
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

    fn test_guard() -> PoolGuard {
        let owner = Box::leak(Box::new(crate::quiescent::QuiescentBox::new(())));
        PoolGuard::new(owner.owner_identity(), owner.guard().into_sync())
    }

    #[test]
    fn test_pool_guards_builder_empty_builds_partial() {
        let guards = PoolGuards::builder().build();
        assert!(guards.try_guard(PoolRole::Meta).is_none());
    }

    #[test]
    fn test_pool_guards_builder_rejects_duplicate_role() {
        let result = std::panic::catch_unwind(|| {
            let _ = PoolGuards::builder()
                .push(PoolRole::Meta, test_guard())
                .push(PoolRole::Meta, test_guard())
                .build();
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_pool_guards_try_guard_matches_named_role() {
        let meta_guard = test_guard();
        let index_guard = test_guard();
        let mem_guard = test_guard();
        let disk_guard = test_guard();
        let guards = PoolGuards::builder()
            .push(PoolRole::Meta, meta_guard.clone())
            .push(PoolRole::Index, index_guard.clone())
            .push(PoolRole::Mem, mem_guard.clone())
            .push(PoolRole::Disk, disk_guard.clone())
            .build();
        assert_eq!(
            guards.try_guard(PoolRole::Meta).map(PoolGuard::identity),
            Some(meta_guard.identity())
        );
        assert_eq!(
            guards.try_guard(PoolRole::Index).map(PoolGuard::identity),
            Some(index_guard.identity())
        );
        assert_eq!(
            guards.try_guard(PoolRole::Mem).map(PoolGuard::identity),
            Some(mem_guard.identity())
        );
        assert_eq!(
            guards.try_guard(PoolRole::Disk).map(PoolGuard::identity),
            Some(disk_guard.identity())
        );
    }

    #[test]
    fn test_pool_guards_named_getters_return_configured_slots() {
        let guards = PoolGuards::builder()
            .push(PoolRole::Meta, test_guard())
            .push(PoolRole::Index, test_guard())
            .push(PoolRole::Mem, test_guard())
            .push(PoolRole::Disk, test_guard())
            .build();
        let _ = guards.meta_guard();
        let _ = guards.index_guard();
        let _ = guards.mem_guard();
        let _ = guards.disk_guard();
    }

    #[test]
    fn test_pool_guards_try_row_guard_uses_named_row_role() {
        let meta_guard = test_guard();
        let mem_guard = test_guard();
        let guards = PoolGuards::builder()
            .push(PoolRole::Meta, meta_guard.clone())
            .push(PoolRole::Mem, mem_guard.clone())
            .build();
        assert_eq!(
            guards
                .try_row_guard(RowPoolRole::Meta)
                .map(PoolGuard::identity),
            Some(meta_guard.identity())
        );
        assert_eq!(
            guards
                .try_row_guard(RowPoolRole::Mem)
                .map(PoolGuard::identity),
            Some(mem_guard.identity())
        );
    }

    #[test]
    #[should_panic(expected = "invalid pool role in pool guards builder")]
    fn test_pool_guards_builder_rejects_invalid_role() {
        let _ = PoolGuards::builder()
            .push(PoolRole::Invalid, test_guard())
            .build();
    }

    #[test]
    #[should_panic(expected = "missing meta pool guard")]
    fn test_pool_guards_meta_guard_panics_when_slot_missing() {
        let guards = PoolGuards::builder().build();
        let _ = guards.meta_guard();
    }

    #[test]
    #[should_panic(expected = "pool guard identity mismatch")]
    fn test_pool_guard_assert_matches_panics_on_identity_mismatch() {
        let guard = test_guard();
        let foreign_guard = test_guard();
        guard.assert_matches(foreign_guard.identity(), "test");
    }
}
