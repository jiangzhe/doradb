use crate::error::OperationResult;
use crate::lock::{LockGrant, LockManager, LockMode, LockOwner, LockOwnerGroup, LockResource};
use crate::map::FastHashMap;

/// Owner-local logical lock cache.
///
/// The cache records only successfully granted resources for one logical owner.
/// It lets owner cleanup release exactly those resources instead of scanning the
/// whole lock table.
pub(crate) struct OwnerLockState {
    owner: LockOwner,
    owner_group: Option<LockOwnerGroup>,
    held: FastHashMap<LockResource, LockMode>,
}

impl OwnerLockState {
    /// Create an empty cache for one logical lock owner.
    #[inline]
    pub(crate) fn new(owner: LockOwner) -> Self {
        OwnerLockState {
            owner,
            owner_group: None,
            held: FastHashMap::default(),
        }
    }

    /// Create an empty cache for one logical owner inside an owner group.
    #[inline]
    pub(crate) fn new_grouped(owner: LockOwner, owner_group: LockOwnerGroup) -> Self {
        OwnerLockState {
            owner,
            owner_group: Some(owner_group),
            held: FastHashMap::default(),
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
    pub(crate) fn cached_covers(
        &self,
        resource: LockResource,
        mode: LockMode,
    ) -> OperationResult<bool> {
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
    ) -> OperationResult<()> {
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
    ) -> OperationResult<LockGrant> {
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

    /// Records a lock grant in the owner-local cache.
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
        let mut removed = 0;
        for (resource, _mode) in self.held.drain() {
            removed += lock_manager.release(resource, self.owner);
        }
        removed
    }

    /// Asserts that every cached lock has been cleared.
    #[inline]
    pub(crate) fn assert_cleared(&self) {
        assert!(self.held.is_empty(), "logical locks should be cleared");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::OperationError;
    use crate::id::{SessionID, TableID, TrxID};
    use crate::lock::tests::{LockDebugEntryState, debug_snapshot};

    fn table_data(id: u64) -> LockResource {
        LockResource::TableData(TableID::new(id))
    }

    fn table_metadata(id: u64) -> LockResource {
        LockResource::TableMetadata(TableID::new(id))
    }

    fn trx(id: u64) -> LockOwner {
        LockOwner::Transaction(TrxID::new(id))
    }

    fn group(id: u64) -> LockOwnerGroup {
        LockOwnerGroup::Session(SessionID::new(id))
    }

    fn has_entry(
        manager: &LockManager,
        resource: LockResource,
        mode: LockMode,
        owner: LockOwner,
        owner_group: Option<LockOwnerGroup>,
    ) -> bool {
        debug_snapshot(manager).entries.iter().any(|entry| {
            entry.resource == resource
                && entry.mode == mode
                && entry.owner == owner
                && entry.owner_group == owner_group
                && entry.state == LockDebugEntryState::Granted
        })
    }

    fn owner_entry_count(manager: &LockManager, owner: LockOwner) -> usize {
        debug_snapshot(manager)
            .entries
            .iter()
            .filter(|entry| entry.owner == owner)
            .count()
    }

    fn assert_operation_err<T>(res: OperationResult<T>, expected: OperationError) {
        let err = res.err().unwrap();
        assert_eq!(*err.current_context(), expected);
    }

    #[test]
    fn owner_lock_state_acquire_uses_cache_for_covered_requests() {
        smol::block_on(async {
            let manager = LockManager::new();
            let owner = trx(1);
            let resource = table_data(10);
            let mut state = OwnerLockState::new(owner);

            state
                .acquire(&manager, resource, LockMode::Exclusive)
                .await
                .unwrap();
            assert!(state.cached_covers(resource, LockMode::Shared).unwrap());
            state
                .acquire(&manager, resource, LockMode::Shared)
                .await
                .unwrap();

            assert_eq!(owner_entry_count(&manager, owner), 1);
            assert!(has_entry(
                &manager,
                resource,
                LockMode::Exclusive,
                owner,
                None
            ));

            assert_eq!(state.release_all(&manager), 1);
            state.assert_cleared();
            assert_eq!(owner_entry_count(&manager, owner), 0);
        });
    }

    #[test]
    fn owner_lock_state_acquire_uncached_does_not_update_cache() {
        smol::block_on(async {
            let manager = LockManager::new();
            let owner = trx(2);
            let resource = table_metadata(20);
            let mut state = OwnerLockState::new(owner);

            assert_eq!(
                state
                    .acquire_uncached(&manager, resource, LockMode::Shared)
                    .await
                    .unwrap(),
                LockGrant::Fresh
            );

            assert!(!state.cached_covers(resource, LockMode::Shared).unwrap());
            assert!(has_entry(&manager, resource, LockMode::Shared, owner, None));
            assert_eq!(state.release_all(&manager), 0);
            assert!(has_entry(&manager, resource, LockMode::Shared, owner, None));
            assert_eq!(manager.release(resource, owner), 1);
        });
    }

    #[test]
    fn grouped_owner_lock_state_records_grouped_grants() {
        smol::block_on(async {
            let manager = LockManager::new();
            let owner = trx(3);
            let owner_group = group(30);
            let resource = table_metadata(30);
            let mut state = OwnerLockState::new_grouped(owner, owner_group);

            state
                .acquire(&manager, resource, LockMode::Shared)
                .await
                .unwrap();

            assert_eq!(state.owner_group(), Some(owner_group));
            assert!(state.cached_covers(resource, LockMode::Shared).unwrap());
            assert!(has_entry(
                &manager,
                resource,
                LockMode::Shared,
                owner,
                Some(owner_group)
            ));

            assert_eq!(state.release_all(&manager), 1);
            state.assert_cleared();
        });
    }

    #[test]
    fn failed_conversion_preserves_cached_mode() {
        smol::block_on(async {
            let manager = LockManager::new();
            let owner = trx(4);
            let resource = table_data(40);
            let mut state = OwnerLockState::new(owner);

            state
                .acquire(&manager, resource, LockMode::IntentExclusive)
                .await
                .unwrap();
            assert_operation_err(
                state.acquire(&manager, resource, LockMode::Shared).await,
                OperationError::LockConversionNotSupported,
            );

            assert!(
                state
                    .cached_covers(resource, LockMode::IntentExclusive)
                    .unwrap()
            );
            assert!(!state.cached_covers(resource, LockMode::Shared).unwrap());
            assert_eq!(owner_entry_count(&manager, owner), 1);
            assert!(has_entry(
                &manager,
                resource,
                LockMode::IntentExclusive,
                owner,
                None
            ));

            assert_eq!(state.release_all(&manager), 1);
        });
    }

    #[test]
    fn cached_covers_validates_requested_mode() {
        let resource = table_metadata(50);
        let mut state = OwnerLockState::new(trx(5));

        assert_operation_err(
            state.cached_covers(resource, LockMode::IntentShared),
            OperationError::InvalidLockMode,
        );

        state.cache_granted(resource, LockMode::Shared);
        assert_operation_err(
            state.cached_covers(resource, LockMode::IntentExclusive),
            OperationError::InvalidLockMode,
        );
    }

    #[test]
    fn release_all_releases_only_cached_locks() {
        smol::block_on(async {
            let manager = LockManager::new();
            let owner = trx(6);
            let cached_metadata = table_metadata(60);
            let cached_data = table_data(60);
            let uncached_data = table_data(61);
            let mut state = OwnerLockState::new(owner);

            state
                .acquire(&manager, cached_metadata, LockMode::Shared)
                .await
                .unwrap();
            state
                .acquire(&manager, cached_data, LockMode::IntentExclusive)
                .await
                .unwrap();
            assert_eq!(
                state
                    .acquire_uncached(&manager, uncached_data, LockMode::IntentShared)
                    .await
                    .unwrap(),
                LockGrant::Fresh
            );

            assert_eq!(owner_entry_count(&manager, owner), 3);
            assert_eq!(state.release_all(&manager), 2);
            state.assert_cleared();
            assert_eq!(owner_entry_count(&manager, owner), 1);
            assert!(has_entry(
                &manager,
                uncached_data,
                LockMode::IntentShared,
                owner,
                None
            ));
            assert_eq!(manager.release(uncached_data, owner), 1);
        });
    }
}
