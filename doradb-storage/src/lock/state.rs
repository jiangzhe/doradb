use crate::error::Result;
use crate::lock::{LockGrant, LockManager, LockMode, LockOwner, LockOwnerGroup, LockResource};
use std::collections::HashMap;

/// Owner-local logical lock cache.
///
/// The cache records only successfully granted resources for one logical owner.
/// It lets owner cleanup release exactly those resources instead of scanning the
/// whole lock table.
pub(crate) struct OwnerLockState {
    owner: LockOwner,
    owner_group: Option<LockOwnerGroup>,
    held: HashMap<LockResource, LockMode>,
}

impl OwnerLockState {
    /// Create an empty cache for one logical lock owner.
    #[inline]
    pub(crate) fn new(owner: LockOwner) -> Self {
        OwnerLockState {
            owner,
            owner_group: None,
            held: HashMap::new(),
        }
    }

    /// Create an empty cache for one logical owner inside an owner group.
    #[inline]
    pub(crate) fn new_grouped(owner: LockOwner, owner_group: LockOwnerGroup) -> Self {
        OwnerLockState {
            owner,
            owner_group: Some(owner_group),
            held: HashMap::new(),
        }
    }

    /// Returns the logical lock owner represented by this state.
    #[inline]
    pub(crate) fn owner(&self) -> LockOwner {
        self.owner
    }

    /// Returns the owner group represented by this state, if any.
    #[inline]
    pub(crate) fn owner_group(&self) -> Option<LockOwnerGroup> {
        self.owner_group
    }

    /// Returns whether the cached lock mode covers the requested mode.
    #[inline]
    pub(crate) fn cached_covers(&self, resource: LockResource, mode: LockMode) -> Result<bool> {
        match self.held.get(&resource).copied() {
            Some(held) => held.covers(resource, mode),
            None => {
                mode.validate_for(resource)?;
                Ok(false)
            }
        }
    }

    /// Acquires an owner-scoped lock, waiting for fresh conflicts.
    ///
    /// Blocking conversion is still delegated to the lock manager and remains
    /// unsupported; the cache updates only after a successful grant.
    #[inline]
    pub(crate) async fn acquire(
        &mut self,
        lock_manager: &LockManager,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<()> {
        if self.cached_covers(resource, mode)? {
            return Ok(());
        }
        match self.owner_group {
            Some(owner_group) => {
                lock_manager
                    .acquire_grouped(resource, mode, self.owner, owner_group)
                    .await?;
            }
            None => {
                lock_manager.acquire(resource, mode, self.owner).await?;
            }
        }
        self.cache_granted(resource, mode);
        Ok(())
    }

    /// Acquires through the lock manager without updating the local cache.
    #[inline]
    pub(crate) async fn acquire_uncached(
        &self,
        lock_manager: &LockManager,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<LockGrant> {
        if self.cached_covers(resource, mode)? {
            return Ok(LockGrant::Existing);
        }
        match self.owner_group {
            Some(owner_group) => {
                lock_manager
                    .acquire_grouped_with_grant(resource, mode, self.owner, owner_group)
                    .await
            }
            None => {
                lock_manager
                    .acquire_with_grant(resource, mode, self.owner)
                    .await
            }
        }
    }

    #[inline]
    pub(crate) fn cache_granted(&mut self, resource: LockResource, mode: LockMode) {
        if let Some(held) = self.held.get(&resource).copied() {
            debug_assert!(
                mode.covers(resource, held).unwrap_or(false),
                "newly granted lock mode must cover the previous cached mode"
            );
        }
        self.held.insert(resource, mode);
    }

    /// Releases every cached lock and clears the cache.
    #[inline]
    pub(crate) fn release_all(&mut self, lock_manager: &LockManager) -> usize {
        let mut resources: Vec<_> = self.held.keys().copied().collect();
        resources.sort_unstable();
        let mut removed = 0;
        for resource in resources {
            removed += lock_manager.release(resource, self.owner);
        }
        self.held.clear();
        removed
    }

    /// Asserts that every cached lock has been cleared.
    #[inline]
    pub(crate) fn assert_cleared(&self) {
        assert!(self.held.is_empty(), "logical locks should be cleared");
    }
}
