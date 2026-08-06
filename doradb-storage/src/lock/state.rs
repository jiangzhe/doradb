use super::claim::{ClaimToken, LocalFamilyResourceState, ModeMask, ScopeClaim};
use super::{
    LockFamily, LockGrant, LockManager, LockMode, LockOwner, LockResource, LockScope,
    lock_family_conflict_err,
};
use crate::error::OperationResult;
use crate::id::{ClaimNo, SessionID, TableID, TrxID};
use crate::map::FastHashMap;

/// Authoritative cleanup index for one exact logical lock scope.
pub(crate) struct LockScopeState {
    owner: LockOwner,
    claims: FastHashMap<LockResource, ScopeClaim>,
}

impl LockScopeState {
    /// Creates an empty exact-scope cleanup index.
    #[inline]
    pub(crate) fn new(owner: LockOwner) -> Self {
        Self {
            owner,
            claims: FastHashMap::default(),
        }
    }

    /// Returns this state's exact logical owner.
    #[inline]
    pub(crate) const fn owner(&self) -> LockOwner {
        self.owner
    }

    /// Returns whether this exact scope has a claim covering `mode`.
    #[inline]
    pub(crate) fn covers(&self, resource: LockResource, mode: LockMode) -> bool {
        match self.claims.get(&resource) {
            Some(claim) => claim.mode.covers(resource, mode),
            None => {
                mode.assert_valid_for(resource);
                false
            }
        }
    }

    /// Asserts that this scope no longer owns accepted claims.
    #[inline]
    pub(crate) fn assert_cleared(&self) {
        assert!(
            self.claims.is_empty(),
            "logical lock scope should be cleared: owner={}, remaining_claims={}",
            self.owner,
            self.claims.len()
        );
    }

    #[inline]
    fn claim_token(&self, resource: LockResource) -> Option<ClaimToken> {
        self.claims.get(&resource).map(|claim| ClaimToken {
            resource,
            owner: self.owner,
            claim_no: claim.claim_no,
        })
    }
}

/// Local Phase 1 family-lock path counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct FamilyLockStats {
    /// Acquisitions covered by the same exact logical claim.
    pub(crate) repeated_exact_covered: u64,
    /// Fresh exact manager mirrors accepted under family-level coverage.
    pub(crate) family_covered_manager_mirrors: u64,
    /// Calls that reached the physical lock manager acquire path.
    pub(crate) manager_acquires: u64,
    /// Calls that released an exact physical manager mirror.
    pub(crate) manager_releases: u64,
    /// Family/resource entries promoted from inline to expanded storage.
    pub(crate) expansions: u64,
    /// Fresh accepted logical claim identities.
    pub(crate) accepted_fresh_claims: u64,
    /// Exact logical claims converted to a covering mode.
    pub(crate) conversions: u64,
    /// Exact logical scopes closed through their cleanup indexes.
    pub(crate) scopes_closed: u64,
    /// Claims visited while closing exact logical scopes.
    pub(crate) close_claims_visited: u64,
    /// Releases that left the family/resource physical mode unchanged.
    pub(crate) physical_mode_preserving_releases: u64,
}

/// Authoritative family/resource index for one session family.
pub(crate) struct FamilyLockState {
    family: LockFamily,
    next_claim_no: u64,
    resources: FastHashMap<LockResource, LocalFamilyResourceState>,
    stats: FamilyLockStats,
}

impl FamilyLockState {
    #[inline]
    fn new(family: LockFamily) -> Self {
        Self {
            family,
            next_claim_no: 1,
            resources: FastHashMap::default(),
            stats: FamilyLockStats::default(),
        }
    }

    /// Acquires or immediately converts one exact logical claim.
    #[inline]
    pub(crate) async fn acquire(
        &mut self,
        curr_scope: &mut LockScopeState,
        lock_manager: &LockManager,
        resource: LockResource,
        mode: LockMode,
    ) -> OperationResult<LockGrant> {
        // 1. Validate the request before consulting either owner-side index.
        self.assert_scope_family(curr_scope);
        mode.assert_valid_for(resource);

        // 2. Resolve an existing exact-scope claim first. Each owner-side
        // mutation preserves the dual-index invariant, so this path does not
        // traverse the family index merely to revalidate it.
        if let Some(existing) = curr_scope.claims.get(&resource).copied() {
            // A directionally covered request is entirely owner-local.
            if existing.mode.covers(resource, mode) {
                self.stats.repeated_exact_covered += 1;
                return Ok(LockGrant::Existing);
            }

            // A conversion is allowed only when every other exact scope in the
            // family already covers the requested mode. The manager then
            // performs the existing exact grant's immediate-only conversion;
            // an error leaves both owner-side indexes unchanged.
            self.validate_family_coverage(resource, mode, curr_scope.owner)?;
            self.stats.manager_acquires += 1;
            let grant = lock_manager
                .acquire_with_grant(resource, mode, curr_scope.owner)
                .await?;
            assert!(
                grant == LockGrant::Existing,
                "indexed exact conversion must match an existing manager grant: \
                 resource={resource}, owner={}, mode={mode}, grant={grant:?}",
                curr_scope.owner
            );

            // Exclusive family authority preserves the indexed claim across
            // the await. Update the family aggregate first, then its
            // exact-scope mirror.
            let family_resource = self.resources.get_mut(&resource).unwrap_or_else(|| {
                panic!("scope claim requires family/resource state: resource={resource}")
            });
            family_resource.update(resource, curr_scope.owner.scope(), existing.claim_no, mode);
            curr_scope.claims.get_mut(&resource).unwrap().mode = mode;
            self.stats.conversions += 1;
            return Ok(LockGrant::Existing);
        }

        // 3. A fresh exact claim reserves its identity before any policy error,
        // wait, or cancellation can occur; an unsuccessful attempt burns it.
        let claim_no = self.reserve_claim_no();
        let family_covered = self.validate_family_coverage(resource, mode, curr_scope.owner)?;

        // 4. Phase 1 still mirrors every fresh logical claim into the manager,
        // including requests covered by another exact scope in this family.
        self.stats.manager_acquires += 1;
        let grant = lock_manager
            .acquire_with_grant(resource, mode, curr_scope.owner)
            .await?;
        assert!(
            grant == LockGrant::Fresh,
            "fresh local claim must create one exact Phase 1 manager grant: \
             resource={resource}, owner={}, mode={mode}, claim_no={claim_no:?}, grant={grant:?}",
            curr_scope.owner
        );

        // 5. Publish the fresh claim synchronously into both authoritative
        // owner-side indexes. This invariant operation has no suspension point
        // or recoverable failure after the manager reports a fresh grant.
        self.insert_claim(curr_scope, resource, claim_no, mode);
        self.stats.accepted_fresh_claims += 1;
        if family_covered {
            self.stats.family_covered_manager_mirrors += 1;
        }
        Ok(LockGrant::Fresh)
    }

    /// Releases one accepted claim from this exact scope.
    #[inline]
    pub(crate) fn release(
        &mut self,
        curr_scope: &mut LockScopeState,
        lock_manager: &LockManager,
        resource: LockResource,
    ) -> bool {
        let Some(token) = curr_scope.claim_token(resource) else {
            return false;
        };
        self.release_token(curr_scope, lock_manager, token);
        true
    }

    /// Closes exactly the claims indexed by `curr_scope`.
    #[inline]
    pub(crate) fn close_scope(
        &mut self,
        curr_scope: &mut LockScopeState,
        lock_manager: &LockManager,
    ) -> usize {
        self.assert_scope_family(curr_scope);
        let mut released = 0;
        while let Some(resource) = curr_scope.claims.keys().next().copied() {
            let token = curr_scope
                .claim_token(resource)
                .expect("scope key must retain its claim");
            self.release_token(curr_scope, lock_manager, token);
            released += 1;
            self.stats.close_claims_visited += 1;
        }
        self.stats.scopes_closed += 1;
        curr_scope.assert_cleared();
        released
    }

    /// Returns whether the session-explicit family slot claims `resource`.
    #[inline]
    pub(crate) fn session_explicit_claim(&self, resource: LockResource) -> Option<LockMode> {
        self.resources
            .get(&resource)
            .and_then(|state| state.get(LockScope::SessionExplicit))
            .map(|claim| claim.mode)
    }

    /// Rejects table DDL under an explicit session claim in this authority.
    #[inline]
    pub(crate) fn reject_table_ddl_explicit_session_lock(
        &self,
        table_id: TableID,
        ddl_owner: LockOwner,
    ) -> OperationResult<()> {
        self.assert_owner_family(ddl_owner);
        let explicit_owner = LockOwner::session_explicit(self.family.session_id());
        for resource in [
            LockResource::TableMetadata(table_id),
            LockResource::TableData(table_id),
        ] {
            if let Some(held) = self.session_explicit_claim(resource) {
                return Err(lock_family_conflict_err(
                    resource,
                    held,
                    LockMode::Exclusive,
                    ddl_owner,
                    explicit_owner,
                )
                .attach(format!(
                    "table_id={table_id}, policy=reject_ddl_under_explicit_session_lock"
                )));
            }
        }
        Ok(())
    }

    /// Asserts that no accepted claims remain in this family.
    #[inline]
    pub(crate) fn assert_empty(&self) {
        assert!(
            self.resources.is_empty(),
            "family lock authority should be empty: family={}, remaining_resources={}",
            self.family,
            self.resources.len()
        );
    }

    /// Asserts that only session-explicit slots may remain while idle.
    #[inline]
    pub(crate) fn assert_idle(&self) {
        for (resource, state) in &self.resources {
            state.for_each(|scope, _claim| {
                assert!(
                    scope == LockScope::SessionExplicit,
                    "idle family authority retains shorter-lived claim: \
                     family={}, resource={resource}, scope={scope:?}",
                    self.family
                );
            });
        }
    }

    #[inline]
    fn assert_no_transaction_claims(&self) {
        for (resource, state) in &self.resources {
            state.for_each(|scope, _claim| {
                assert!(
                    !matches!(scope, LockScope::Transaction(_)),
                    "terminal transaction left a family claim: \
                     family={}, resource={resource}, scope={scope:?}",
                    self.family
                );
            });
        }
    }

    #[inline]
    fn reserve_claim_no(&mut self) -> ClaimNo {
        let claim_no = ClaimNo::new(self.next_claim_no);
        self.next_claim_no = self.next_claim_no.checked_add(1).unwrap_or_else(|| {
            panic!(
                "family lock claim number exhausted: family={}, last_claim_no={:?}",
                self.family, claim_no
            )
        });
        claim_no
    }

    #[inline]
    fn validate_family_coverage(
        &self,
        resource: LockResource,
        requested: LockMode,
        owner: LockOwner,
    ) -> OperationResult<bool> {
        let Some(state) = self.resources.get(&resource) else {
            return Ok(false);
        };
        let mut covered = false;
        let mut conflict = None;
        state.for_each(|scope, claim| {
            if scope == owner.scope() {
                return;
            }
            if !claim.mode.covers(resource, requested) && conflict.is_none() {
                conflict = Some((scope, claim.mode));
            }
            covered = true;
        });
        if let Some((held_scope, held)) = conflict {
            return Err(lock_family_conflict_err(
                resource,
                held,
                requested,
                owner,
                LockOwner::from_parts(self.family, held_scope),
            ));
        }
        Ok(covered)
    }

    #[inline]
    fn insert_claim(
        &mut self,
        curr_scope: &mut LockScopeState,
        resource: LockResource,
        claim_no: ClaimNo,
        mode: LockMode,
    ) {
        assert!(
            !curr_scope.claims.contains_key(&resource),
            "fresh family claim requires an exact-scope miss: resource={resource}, owner={}",
            curr_scope.owner
        );
        match self.resources.get_mut(&resource) {
            Some(state) => {
                let expanded = state.insert(resource, curr_scope.owner.scope(), claim_no, mode);
                if expanded {
                    self.stats.expansions += 1;
                }
            }
            None => {
                self.resources.insert(
                    resource,
                    LocalFamilyResourceState::new(curr_scope.owner.scope(), claim_no, mode),
                );
            }
        }
        let previous = curr_scope
            .claims
            .insert(resource, ScopeClaim { claim_no, mode });
        assert!(
            previous.is_none(),
            "fresh exact-scope insertion replaced a claim: resource={resource}, owner={}",
            curr_scope.owner
        );
    }

    #[inline]
    fn release_token(
        &mut self,
        curr_scope: &mut LockScopeState,
        lock_manager: &LockManager,
        token: ClaimToken,
    ) {
        self.assert_scope_family(curr_scope);
        assert!(
            token.owner == curr_scope.owner,
            "claim token exact-scope mismatch: token_owner={}, curr_scope_owner={}",
            token.owner,
            curr_scope.owner
        );
        let scope_claim = curr_scope
            .claims
            .get(&token.resource)
            .copied()
            .unwrap_or_else(|| {
                panic!(
                    "claim token resource missing from exact-scope index: \
                     resource={}, owner={}, claim_no={:?}",
                    token.resource, token.owner, token.claim_no
                )
            });
        assert!(
            scope_claim.claim_no == token.claim_no,
            "stale family claim token: resource={}, owner={}, token_claim_no={:?}, \
             current_claim_no={:?}",
            token.resource,
            token.owner,
            token.claim_no,
            scope_claim.claim_no
        );

        let family_resource = self.resources.get_mut(&token.resource).unwrap();
        let old_covering_mode = family_resource.covering_mode();
        let mut remaining_mask = ModeMask::default();
        let mut new_covering_mode = None;
        family_resource.for_each(|scope, claim| {
            if scope == token.owner.scope() {
                return;
            }
            remaining_mask.insert(claim.mode);
            new_covering_mode = match new_covering_mode {
                None => Some(claim.mode),
                Some(current) if current.covers(token.resource, claim.mode) => Some(current),
                Some(current) if claim.mode.covers(token.resource, current) => Some(claim.mode),
                Some(current) => panic!(
                    "remaining family/resource claims have no occupied covering mode: \
                     resource={}, left={current}, right={}",
                    token.resource, claim.mode
                ),
            };
        });

        let removed_manager = lock_manager.release(token.resource, token.owner);
        assert!(
            removed_manager == 1,
            "accepted logical claim must have one exact Phase 1 manager mirror: \
             resource={}, owner={}, claim_no={:?}, removed={removed_manager}",
            token.resource,
            token.owner,
            token.claim_no
        );
        self.stats.manager_releases += 1;
        if new_covering_mode == Some(old_covering_mode) {
            self.stats.physical_mode_preserving_releases += 1;
        }
        let remove_resource = new_covering_mode.is_none();
        family_resource.remove(
            token.owner.scope(),
            token.claim_no,
            remaining_mask,
            new_covering_mode,
        );
        assert!(
            curr_scope.claims.remove(&token.resource).is_some(),
            "exact-scope claim disappeared during release: resource={}, owner={}",
            token.resource,
            token.owner
        );
        if remove_resource {
            assert!(
                self.resources.remove(&token.resource).is_some(),
                "empty family/resource state disappeared during release: resource={}",
                token.resource
            );
        }
    }

    #[inline]
    fn assert_scope_family(&self, curr_scope: &LockScopeState) {
        assert!(
            curr_scope.owner.family() == self.family,
            "family/scope authority mismatch: family={}, owner={}",
            self.family,
            curr_scope.owner
        );
    }

    #[inline]
    fn assert_owner_family(&self, owner: LockOwner) {
        assert!(
            owner.family() == self.family,
            "family/owner authority mismatch: family={}, owner={owner}",
            self.family
        );
    }
}

/// Sole boxed owner of one session family's lock authority.
pub(crate) struct FamilyLockAuthority {
    family: FamilyLockState,
    /// Always the session-explicit scope for this family.
    session_scope: LockScopeState,
}

impl FamilyLockAuthority {
    /// Allocates one empty authority for an engine-local session.
    #[inline]
    pub(crate) fn new(session_id: SessionID) -> Box<Self> {
        let family = LockFamily::new(session_id);
        Box::new(Self {
            family: FamilyLockState::new(family),
            session_scope: LockScopeState::new(LockOwner::session_explicit(session_id)),
        })
    }

    /// Returns the represented session family.
    #[inline]
    pub(crate) const fn lock_family(&self) -> LockFamily {
        self.family.family
    }

    /// Mutably splits the family/resource index from the session-explicit scope index.
    #[inline]
    pub(crate) fn parts(&mut self) -> (&mut FamilyLockState, &mut LockScopeState) {
        (&mut self.family, &mut self.session_scope)
    }

    /// Returns immutable family state.
    #[inline]
    pub(crate) const fn family(&self) -> &FamilyLockState {
        &self.family
    }

    /// Returns mutable family/resource index state without borrowing the session scope.
    #[inline]
    pub(crate) const fn family_mut(&mut self) -> &mut FamilyLockState {
        &mut self.family
    }

    /// Closes the session-explicit scope and requires the whole family to drain.
    #[inline]
    pub(crate) fn close_session(&mut self, lock_manager: &LockManager) -> usize {
        let released = self
            .family
            .close_scope(&mut self.session_scope, lock_manager);
        self.family.assert_empty();
        released
    }

    /// Asserts that only session-explicit claims remain.
    #[inline]
    pub(crate) fn assert_idle(&self) {
        self.family.assert_idle();
    }
}

/// Family authority paired with one transaction's exact scope.
pub(crate) struct TransactionLockState {
    authority: Box<FamilyLockAuthority>,
    curr_scope: LockScopeState,
}

impl TransactionLockState {
    /// Creates a transaction scope around the exact supplied family authority.
    #[inline]
    pub(crate) fn new(authority: Box<FamilyLockAuthority>, trx_id: TrxID) -> Self {
        let owner = LockOwner::transaction(authority.lock_family().session_id(), trx_id);
        Self {
            authority,
            curr_scope: LockScopeState::new(owner),
        }
    }

    /// Returns the exact transaction owner.
    #[inline]
    pub(crate) const fn owner(&self) -> LockOwner {
        self.curr_scope.owner()
    }

    /// Returns whether the transaction claim covers this request.
    #[inline]
    pub(crate) fn covers(&self, resource: LockResource, mode: LockMode) -> bool {
        self.curr_scope.covers(resource, mode)
    }

    /// Returns mutable family/resource index state without borrowing the transaction scope.
    #[inline]
    pub(crate) const fn family_mut(&mut self) -> &mut FamilyLockState {
        &mut self.authority.family
    }

    /// Splits family authority from the transaction scope.
    #[inline]
    pub(crate) fn parts(&mut self) -> (&mut FamilyLockState, &mut LockScopeState) {
        (&mut self.authority.family, &mut self.curr_scope)
    }

    /// Acquires one transaction claim.
    #[inline]
    pub(crate) async fn acquire(
        &mut self,
        lock_manager: &LockManager,
        resource: LockResource,
        mode: LockMode,
    ) -> OperationResult<LockGrant> {
        self.authority
            .family
            .acquire(&mut self.curr_scope, lock_manager, resource, mode)
            .await
    }

    /// Closes the transaction scope and returns the exact boxed family root.
    #[inline]
    pub(crate) fn close(
        mut self,
        lock_manager: &LockManager,
        expected_trx_id: TrxID,
    ) -> Box<FamilyLockAuthority> {
        assert!(
            self.curr_scope.owner.scope() == LockScope::Transaction(expected_trx_id),
            "transaction lock-state identity mismatch: expected_trx_id={expected_trx_id}, owner={}",
            self.curr_scope.owner
        );
        self.authority
            .family
            .close_scope(&mut self.curr_scope, lock_manager);
        self.authority.family.assert_no_transaction_claims();
        self.authority
    }

    /// Asserts that this carrier no longer has transaction claims.
    #[inline]
    pub(crate) fn assert_cleared(&self) {
        self.curr_scope.assert_cleared();
    }
}

/// Fixed-capacity rollback guard for newly accepted owner-side claims.
pub(crate) struct FreshClaimsGuard<'a, const N: usize> {
    family: &'a mut FamilyLockState,
    curr_scope: &'a mut LockScopeState,
    lock_manager: &'a LockManager,
    fresh: [Option<ClaimToken>; N],
    len: usize,
    armed: bool,
}

impl<'a, const N: usize> FreshClaimsGuard<'a, N> {
    /// Creates an armed group around one exact scope.
    #[inline]
    pub(crate) fn new(
        family: &'a mut FamilyLockState,
        curr_scope: &'a mut LockScopeState,
        lock_manager: &'a LockManager,
    ) -> Self {
        Self {
            family,
            curr_scope,
            lock_manager,
            fresh: [None; N],
            len: 0,
            armed: true,
        }
    }

    /// Acquires one claim and records only a fresh accepted identity.
    #[inline]
    pub(crate) async fn acquire(
        &mut self,
        resource: LockResource,
        mode: LockMode,
    ) -> OperationResult<LockGrant> {
        let grant = self
            .family
            .acquire(self.curr_scope, self.lock_manager, resource, mode)
            .await?;
        if grant == LockGrant::Fresh {
            let token = self.curr_scope.claim_token(resource).unwrap_or_else(|| {
                panic!(
                    "fresh accepted claim must expose its exact token: \
                     resource={resource}, owner={}",
                    self.curr_scope.owner
                )
            });
            if self.len >= N {
                self.family
                    .release_token(self.curr_scope, self.lock_manager, token);
                panic!(
                    "fresh-claim rollback guard capacity exceeded: capacity={N}, owner={}",
                    self.curr_scope.owner
                );
            }
            assert!(
                self.fresh[self.len].replace(token).is_none(),
                "fresh-claim rollback slot must be empty: index={}, owner={}",
                self.len,
                self.curr_scope.owner
            );
            self.len += 1;
        }
        Ok(grant)
    }

    /// Leaves every accepted claim owned by its enclosing lifecycle carrier.
    #[inline]
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<const N: usize> Drop for FreshClaimsGuard<'_, N> {
    #[inline]
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        while self.len > 0 {
            self.len -= 1;
            let token = self.fresh[self.len]
                .take()
                .expect("fresh-claim guard length must identify an occupied token");
            self.family
                .release_token(self.curr_scope, self.lock_manager, token);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::OperationError;
    use crate::id::{OperationID, SessionOperationKey};
    use crate::lock::tests::{LockDebugEntryState, debug_snapshot};
    use futures::{FutureExt, poll};
    use std::collections::BTreeMap;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::ptr::from_ref;
    use std::task::Poll;

    fn table_data(id: u64) -> LockResource {
        LockResource::TableData(TableID::new(id))
    }

    fn owner_count(manager: &LockManager, owner: LockOwner) -> usize {
        debug_snapshot(manager)
            .entries
            .iter()
            .filter(|entry| entry.owner == owner && entry.state == LockDebugEntryState::Granted)
            .count()
    }

    fn family_snapshot(
        family: &FamilyLockState,
    ) -> Vec<(
        LockResource,
        LockOwner,
        ClaimNo,
        LockMode,
        ModeMask,
        LockMode,
    )> {
        let mut snapshot = Vec::new();
        for (&resource, state) in &family.resources {
            let mut expected_mask = ModeMask::default();
            let mut expected_covering = None;
            state.for_each(|scope, claim| {
                expected_mask.insert(claim.mode);
                expected_covering = match expected_covering {
                    None => Some(claim.mode),
                    Some(current) if current.covers(resource, claim.mode) => Some(current),
                    Some(current) if claim.mode.covers(resource, current) => Some(claim.mode),
                    Some(current) => panic!(
                        "debug snapshot found incomparable occupied modes: \
                         resource={resource}, left={current}, right={}",
                        claim.mode
                    ),
                };
                snapshot.push((
                    resource,
                    LockOwner::from_parts(family.family, scope),
                    claim.claim_no,
                    claim.mode,
                    state.claim_mask(),
                    state.covering_mode(),
                ));
            });
            assert_eq!(
                state.claim_mask(),
                expected_mask,
                "debug snapshot found claim-mask mismatch: resource={resource}"
            );
            assert_eq!(
                Some(state.covering_mode()),
                expected_covering,
                "debug snapshot found covering-mode mismatch: resource={resource}"
            );
        }
        snapshot.sort_unstable_by_key(|entry| (entry.0, entry.1));
        snapshot
    }

    fn assert_manager_agreement(family: &FamilyLockState, lock_manager: &LockManager) {
        let local = family_snapshot(family)
            .into_iter()
            .map(|entry| (entry.0, entry.1, entry.3))
            .collect::<Vec<_>>();
        let mut manager = debug_snapshot(lock_manager)
            .entries
            .into_iter()
            .filter(|entry| {
                entry.state == LockDebugEntryState::Granted && entry.owner.family() == family.family
            })
            .map(|entry| (entry.resource, entry.owner, entry.mode))
            .collect::<Vec<_>>();
        manager.sort_unstable_by_key(|entry| (entry.0, entry.1));
        assert_eq!(
            local, manager,
            "owner-side family index disagrees with exact manager mirrors: family={}",
            family.family
        );
    }

    #[test]
    fn new_authority_starts_with_one_empty_session_root() {
        let session_id = SessionID::new(9);
        let authority = FamilyLockAuthority::new(session_id);
        assert_eq!(authority.lock_family(), LockFamily::new(session_id));
        assert_eq!(
            authority.session_scope.owner(),
            LockOwner::session_explicit(session_id)
        );
        authority.session_scope.assert_cleared();
        assert_eq!(authority.family.next_claim_no, 1);
        assert!(authority.family.resources.is_empty());
        assert_eq!(authority.family.stats, FamilyLockStats::default());
    }

    #[test]
    fn second_scope_expands_once_and_closing_it_preserves_session_claim() {
        smol::block_on(async {
            let manager = LockManager::new();
            let session_id = SessionID::new(1);
            let mut authority = FamilyLockAuthority::new(session_id);
            let resource = table_data(10);
            let trx_id = TrxID::new(20);

            {
                let (family, session_scope) = authority.parts();
                family
                    .acquire(session_scope, &manager, resource, LockMode::Exclusive)
                    .await
                    .unwrap();
            }

            let mut trx = LockScopeState::new(LockOwner::transaction(session_id, trx_id));
            authority
                .family
                .acquire(&mut trx, &manager, resource, LockMode::Shared)
                .await
                .unwrap();
            assert_eq!(authority.family.stats.expansions, 1);
            authority.family.close_scope(&mut trx, &manager);
            assert_eq!(
                authority.family.session_explicit_claim(resource),
                Some(LockMode::Exclusive)
            );
            authority.close_session(&manager);
        });
    }

    #[test]
    fn repeated_covered_acquire_is_local_and_reacquire_burns_identity() {
        smol::block_on(async {
            let manager = LockManager::new();
            let mut authority = FamilyLockAuthority::new(SessionID::new(2));
            let resource = table_data(20);
            let (family, session_scope) = authority.parts();
            family
                .acquire(session_scope, &manager, resource, LockMode::Exclusive)
                .await
                .unwrap();
            let first = session_scope.claim_token(resource).unwrap();
            assert_eq!(
                family
                    .acquire(session_scope, &manager, resource, LockMode::Shared)
                    .await
                    .unwrap(),
                LockGrant::Existing
            );
            assert_eq!(owner_count(&manager, session_scope.owner()), 1);
            assert!(family.release(session_scope, &manager, resource));
            family
                .acquire(session_scope, &manager, resource, LockMode::Shared)
                .await
                .unwrap();
            assert_ne!(
                first.claim_no,
                session_scope.claim_token(resource).unwrap().claim_no
            );
            assert_eq!(family.stats.repeated_exact_covered, 1);
            family.close_scope(session_scope, &manager);
        });
    }

    #[test]
    fn transaction_carrier_returns_the_same_family_allocation() {
        let manager = LockManager::new();
        let session_id = SessionID::new(21);
        let authority = FamilyLockAuthority::new(session_id);
        let ptr = from_ref(authority.as_ref());
        let lock_state = TransactionLockState::new(authority, TrxID::new(210));
        let authority = lock_state.close(&manager, TrxID::new(210));
        assert_eq!(from_ref(authority.as_ref()), ptr);
    }

    #[test]
    fn same_family_noncovering_request_is_rejected_locally() {
        smol::block_on(async {
            let manager = LockManager::new();
            let session_id = SessionID::new(3);
            let mut authority = FamilyLockAuthority::new(session_id);
            let resource = table_data(30);
            let (family, session_scope) = authority.parts();
            family
                .acquire(session_scope, &manager, resource, LockMode::IntentExclusive)
                .await
                .unwrap();
            let mut trx = LockScopeState::new(LockOwner::transaction(session_id, TrxID::new(31)));
            let err = family
                .acquire(&mut trx, &manager, resource, LockMode::Shared)
                .await
                .unwrap_err();
            assert_eq!(*err.current_context(), OperationError::LockFamilyConflict);
            assert_eq!(owner_count(&manager, trx.owner()), 0);
            family.close_scope(session_scope, &manager);
        });
    }

    #[test]
    fn four_scope_acquisitions_update_stats_and_manager_mirrors() {
        smol::block_on(async {
            let manager = LockManager::new();
            let session_id = SessionID::new(4);
            let trx_id = TrxID::new(41);
            let resource = table_data(40);
            let mut authority = FamilyLockAuthority::new(session_id);
            let mut operation = LockScopeState::new(LockOwner::operation(
                SessionOperationKey::new(session_id, OperationID::new(42)),
            ));
            let mut transaction = LockScopeState::new(LockOwner::transaction(session_id, trx_id));
            let mut statement =
                LockScopeState::new(LockOwner::transaction(session_id, trx_id).statement(43));

            let (family, session_scope) = authority.parts();
            family
                .acquire(session_scope, &manager, resource, LockMode::Exclusive)
                .await
                .unwrap();
            family
                .acquire(&mut operation, &manager, resource, LockMode::Shared)
                .await
                .unwrap();
            family
                .acquire(&mut transaction, &manager, resource, LockMode::IntentShared)
                .await
                .unwrap();
            family
                .acquire(&mut statement, &manager, resource, LockMode::IntentShared)
                .await
                .unwrap();

            assert_eq!(family.stats.expansions, 1);
            assert_eq!(family.stats.family_covered_manager_mirrors, 3);
            assert_eq!(family.stats.manager_acquires, 4);
            assert_eq!(family.stats.accepted_fresh_claims, 4);
            assert_eq!(family_snapshot(family).len(), 4);
            assert_manager_agreement(family, &manager);

            family.close_scope(&mut statement, &manager);
            family.close_scope(&mut transaction, &manager);
            family.close_scope(&mut operation, &manager);
            family.close_scope(session_scope, &manager);
            family.assert_empty();
            assert_manager_agreement(family, &manager);
            assert_eq!(family.stats.manager_releases, 4);
            assert_eq!(family.stats.scopes_closed, 4);
            assert_eq!(family.stats.close_claims_visited, 4);
            assert_eq!(family.stats.physical_mode_preserving_releases, 3);
        });
    }

    #[test]
    fn conversion_retains_claim_identity_and_rejection_preserves_mode() {
        smol::block_on(async {
            let manager = LockManager::new();
            let mut authority = FamilyLockAuthority::new(SessionID::new(5));
            let resource = table_data(50);
            let (family, session_scope) = authority.parts();
            family
                .acquire(session_scope, &manager, resource, LockMode::IntentShared)
                .await
                .unwrap();
            let claim_no = session_scope.claim_token(resource).unwrap().claim_no;
            family
                .acquire(session_scope, &manager, resource, LockMode::IntentExclusive)
                .await
                .unwrap();
            assert_eq!(
                session_scope.claim_token(resource).unwrap().claim_no,
                claim_no
            );
            assert!(session_scope.covers(resource, LockMode::IntentExclusive));

            let err = family
                .acquire(session_scope, &manager, resource, LockMode::Shared)
                .await
                .unwrap_err();
            assert_eq!(
                *err.current_context(),
                OperationError::LockConversionNotSupported
            );
            assert_eq!(
                session_scope.claim_token(resource).unwrap().claim_no,
                claim_no
            );
            assert!(session_scope.covers(resource, LockMode::IntentExclusive));
            assert!(!session_scope.covers(resource, LockMode::Shared));
            assert_eq!(family.stats.conversions, 1);
            family.close_scope(session_scope, &manager);
        });
    }

    #[test]
    fn cancelled_wait_burns_claim_number_without_accepted_indexes() {
        smol::block_on(async {
            let manager = LockManager::new();
            let resource = table_data(60);
            let blocker = LockOwner::session_explicit(SessionID::new(60));
            manager
                .acquire(resource, LockMode::Exclusive, blocker)
                .await
                .unwrap();

            let mut authority = FamilyLockAuthority::new(SessionID::new(61));
            let (family, session_scope) = authority.parts();
            let mut acquire =
                Box::pin(family.acquire(session_scope, &manager, resource, LockMode::Shared));
            assert!(matches!(poll!(acquire.as_mut()), Poll::Pending));
            drop(acquire);

            assert_eq!(family.next_claim_no, 2);
            assert!(family.resources.is_empty());
            session_scope.assert_cleared();
            assert_eq!(owner_count(&manager, session_scope.owner()), 0);
            assert_eq!(manager.release(resource, blocker), 1);
        });
    }

    #[test]
    fn stale_claim_token_panics_before_touching_reacquired_claim() {
        smol::block_on(async {
            let manager = LockManager::new();
            let mut authority = FamilyLockAuthority::new(SessionID::new(7));
            let resource = table_data(70);
            let (family, session_scope) = authority.parts();
            family
                .acquire(session_scope, &manager, resource, LockMode::Shared)
                .await
                .unwrap();
            let stale = session_scope.claim_token(resource).unwrap();
            family.release_token(session_scope, &manager, stale);
            family
                .acquire(session_scope, &manager, resource, LockMode::Shared)
                .await
                .unwrap();
            let current = session_scope.claim_token(resource).unwrap();

            let panic = catch_unwind(AssertUnwindSafe(|| {
                family.release_token(session_scope, &manager, stale);
            }));
            assert!(panic.is_err());
            assert_eq!(session_scope.claim_token(resource), Some(current));
            assert_eq!(owner_count(&manager, session_scope.owner()), 1);
            family.close_scope(session_scope, &manager);
        });
    }

    #[test]
    fn missing_manager_mirror_panics_before_mutating_owner_indexes() {
        smol::block_on(async {
            let manager = LockManager::new();
            let mut authority = FamilyLockAuthority::new(SessionID::new(71));
            let resource = table_data(710);
            let (family, session_scope) = authority.parts();
            family
                .acquire(session_scope, &manager, resource, LockMode::Shared)
                .await
                .unwrap();
            assert_manager_agreement(family, &manager);
            let before = family_snapshot(family);
            let token = session_scope.claim_token(resource).unwrap();
            assert_eq!(manager.release(resource, session_scope.owner()), 1);

            let panic = catch_unwind(AssertUnwindSafe(|| {
                family.release_token(session_scope, &manager, token);
            }));
            assert!(panic.is_err());
            assert_eq!(family_snapshot(family), before);
            assert_eq!(session_scope.claim_token(resource), Some(token));

            manager
                .acquire(resource, LockMode::Shared, session_scope.owner())
                .await
                .unwrap();
            family.release_token(session_scope, &manager, token);
            family.assert_empty();
            assert_manager_agreement(family, &manager);
        });
    }

    #[test]
    fn fresh_group_rollback_preserves_preexisting_exact_claims() {
        smol::block_on(async {
            let manager = LockManager::new();
            let mut authority = FamilyLockAuthority::new(SessionID::new(8));
            let existing = table_data(80);
            let fresh_resource = table_data(81);
            let (family, session_scope) = authority.parts();
            family
                .acquire(session_scope, &manager, existing, LockMode::Exclusive)
                .await
                .unwrap();
            {
                let mut fresh = FreshClaimsGuard::<2>::new(family, session_scope, &manager);
                assert_eq!(
                    fresh.acquire(existing, LockMode::Shared).await.unwrap(),
                    LockGrant::Existing
                );
                assert_eq!(
                    fresh
                        .acquire(fresh_resource, LockMode::Shared)
                        .await
                        .unwrap(),
                    LockGrant::Fresh
                );
            }
            assert!(session_scope.covers(existing, LockMode::Exclusive));
            assert!(!session_scope.covers(fresh_resource, LockMode::Shared));
            assert_eq!(owner_count(&manager, session_scope.owner()), 1);
            family.close_scope(session_scope, &manager);
        });
    }

    #[test]
    fn deterministic_reference_model_matches_dual_indexes_and_manager() {
        let manager = LockManager::new();
        let session_id = SessionID::new(90);
        let family_id = LockFamily::new(session_id);
        let trx_id = TrxID::new(901);
        let mut family = FamilyLockState::new(family_id);
        let mut scopes = [
            LockScopeState::new(LockOwner::session_explicit(session_id)),
            LockScopeState::new(LockOwner::operation(SessionOperationKey::new(
                session_id,
                OperationID::new(902),
            ))),
            LockScopeState::new(LockOwner::transaction(session_id, trx_id)),
            LockScopeState::new(LockOwner::transaction(session_id, trx_id).statement(903)),
        ];
        let resources = [table_data(900), table_data(901), table_data(902)];
        let modes = [
            LockMode::IntentShared,
            LockMode::IntentExclusive,
            LockMode::Shared,
            LockMode::Exclusive,
        ];
        let mut model = BTreeMap::<(usize, LockResource), (ClaimNo, LockMode)>::new();
        let mut seed = 0x258d_0a27_4c6f_91e3_u64;
        let mut last_claim_no = 0;

        for _step in 0..512 {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let scope_index = (seed as usize) % scopes.len();
            let resource = resources[((seed >> 8) as usize) % resources.len()];
            match (seed >> 16) % 8 {
                0 => {
                    let expected_resources = model
                        .keys()
                        .filter(|(index, _resource)| *index == scope_index)
                        .map(|(_index, resource)| *resource)
                        .collect::<Vec<_>>();
                    family.close_scope(&mut scopes[scope_index], &manager);
                    for resource in expected_resources {
                        model.remove(&(scope_index, resource));
                    }
                }
                1 | 2 => {
                    let expected = model.remove(&(scope_index, resource));
                    assert_eq!(
                        family.release(&mut scopes[scope_index], &manager, resource),
                        expected.is_some()
                    );
                }
                _ => {
                    let requested = modes[((seed >> 24) as usize) % modes.len()];
                    let exact = model.get(&(scope_index, resource)).copied();
                    let family_covers =
                        model.iter().all(|(&(index, held_resource), &(_no, held))| {
                            held_resource != resource
                                || index == scope_index
                                || held.covers(resource, requested)
                        });
                    let expected_success = match exact {
                        Some((_claim_no, held)) if held.covers(resource, requested) => true,
                        Some((_claim_no, held)) => {
                            family_covers && requested.covers(resource, held)
                        }
                        None => family_covers,
                    };
                    let next_claim_no = family.next_claim_no;
                    let result = family
                        .acquire(&mut scopes[scope_index], &manager, resource, requested)
                        .now_or_never()
                        .expect("reference-model acquisition unexpectedly waited");
                    if expected_success {
                        result.unwrap();
                        let actual = scopes[scope_index].claims[&resource];
                        if let Some((claim_no, _held)) = exact {
                            assert_eq!(actual.claim_no, claim_no);
                        } else {
                            assert_eq!(actual.claim_no.as_u64(), next_claim_no);
                            assert!(actual.claim_no.as_u64() > last_claim_no);
                            last_claim_no = actual.claim_no.as_u64();
                        }
                        model.insert((scope_index, resource), (actual.claim_no, actual.mode));
                    } else {
                        assert!(result.is_err());
                        assert_eq!(
                            scopes[scope_index].claims.get(&resource).copied(),
                            exact.map(|(claim_no, mode)| ScopeClaim { claim_no, mode })
                        );
                    }
                    assert_eq!(
                        family.next_claim_no,
                        next_claim_no + u64::from(exact.is_none())
                    );
                }
            }

            for (index, scope) in scopes.iter().enumerate() {
                assert_eq!(
                    scope.claims.len(),
                    model
                        .keys()
                        .filter(|(scope_index, _)| *scope_index == index)
                        .count()
                );
                for (&resource, &claim) in &scope.claims {
                    assert_eq!(
                        model.get(&(index, resource)).copied(),
                        Some((claim.claim_no, claim.mode))
                    );
                }
            }
            let expected_claims = model
                .iter()
                .map(|(&(index, resource), &(claim_no, mode))| {
                    (resource, scopes[index].owner(), claim_no, mode)
                })
                .collect::<Vec<_>>();
            let actual_claims = family_snapshot(&family)
                .into_iter()
                .map(|entry| (entry.0, entry.1, entry.2, entry.3))
                .collect::<Vec<_>>();
            let mut expected_claims = expected_claims;
            expected_claims.sort_unstable_by_key(|entry| (entry.0, entry.1));
            assert_eq!(actual_claims, expected_claims);
            assert_manager_agreement(&family, &manager);
        }

        for scope in &mut scopes {
            family.close_scope(scope, &manager);
        }
        family.assert_empty();
        assert_manager_agreement(&family, &manager);
    }
}
