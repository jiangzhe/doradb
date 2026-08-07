use super::{LockMode, LockOwner, LockResource, LockScope};
use crate::id::{ClaimNo, OperationID, TrxID};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ScopeClaim {
    pub(super) claim_no: ClaimNo,
    pub(super) mode: LockMode,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct ClaimToken {
    pub(super) resource: LockResource,
    pub(super) owner: LockOwner,
    pub(super) claim_no: ClaimNo,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct PendingClaimToken {
    pub(super) resource: LockResource,
    pub(super) owner: LockOwner,
    pub(super) claim_no: ClaimNo,
}

impl PendingClaimToken {
    #[inline]
    pub(super) fn accept(self) -> ClaimToken {
        ClaimToken {
            resource: self.resource,
            owner: self.owner,
            claim_no: self.claim_no,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FamilyClaim<I> {
    id: I,
    claim_no: ClaimNo,
    mode: LockMode,
}

/// Fixed exact-scope claims for one `(family, resource)` pair.
///
/// This is the resource-oriented half of the owner-side dual index. Each
/// occupied slot has a matching `LockScopeState` entry with the same
/// `ClaimNo` and mode. The fixed layout also makes aggregate recomputation
/// bounded by the number of scope classes rather than by session history.
#[derive(Debug, Default, PartialEq, Eq)]
struct FamilyClaimSlots {
    session_explicit: Option<FamilyClaim<()>>,
    operation: Option<FamilyClaim<OperationID>>,
    transaction: Option<FamilyClaim<TrxID>>,
}

impl FamilyClaimSlots {
    #[inline]
    fn get(&self, scope: LockScope) -> Option<ScopeClaim> {
        match scope {
            LockScope::SessionExplicit => self.session_explicit.as_ref().map(scope_claim),
            LockScope::Operation(id) => self
                .operation
                .as_ref()
                .filter(|claim| claim.id == id)
                .map(scope_claim),
            LockScope::Transaction(id) => self
                .transaction
                .as_ref()
                .filter(|claim| claim.id == id)
                .map(scope_claim),
        }
    }

    #[inline]
    fn insert(&mut self, scope: LockScope, claim_no: ClaimNo, mode: LockMode) {
        match scope {
            LockScope::SessionExplicit => {
                assert!(
                    self.session_explicit.is_none(),
                    "duplicate session-explicit family claim slot"
                );
                self.session_explicit = Some(FamilyClaim {
                    id: (),
                    claim_no,
                    mode,
                });
            }
            LockScope::Operation(id) => {
                assert!(
                    self.operation.is_none(),
                    "duplicate operation family claim slot: operation_id={id}"
                );
                self.operation = Some(FamilyClaim { id, claim_no, mode });
            }
            LockScope::Transaction(id) => {
                assert!(
                    self.transaction.is_none(),
                    "duplicate transaction family claim slot: trx_id={id}"
                );
                self.transaction = Some(FamilyClaim { id, claim_no, mode });
            }
        }
    }

    #[inline]
    fn update(&mut self, scope: LockScope, claim_no: ClaimNo, mode: LockMode) {
        match scope {
            LockScope::SessionExplicit => {
                update_claim(self.session_explicit.as_mut(), (), claim_no, mode, scope)
            }
            LockScope::Operation(id) => {
                update_claim(self.operation.as_mut(), id, claim_no, mode, scope)
            }
            LockScope::Transaction(id) => {
                update_claim(self.transaction.as_mut(), id, claim_no, mode, scope)
            }
        }
    }

    #[inline]
    fn remove(&mut self, scope: LockScope, claim_no: ClaimNo) -> ScopeClaim {
        let claim = match scope {
            LockScope::SessionExplicit => take_matching(&mut self.session_explicit, ()),
            LockScope::Operation(id) => take_matching(&mut self.operation, id),
            LockScope::Transaction(id) => take_matching(&mut self.transaction, id),
        }
        .unwrap_or_else(|| {
            panic!("missing or wrong-id expanded family claim on removal: scope={scope:?}")
        });
        assert!(
            claim.claim_no == claim_no,
            "expanded family claim-number mismatch on removal: scope={scope:?}, \
             expected={claim_no:?}, actual={:?}",
            claim.claim_no
        );
        claim
    }

    #[inline]
    fn for_each(&self, mut visit: impl FnMut(LockScope, ScopeClaim)) {
        if let Some(claim) = self.session_explicit {
            visit(LockScope::SessionExplicit, scope_claim(&claim));
        }
        if let Some(claim) = self.operation {
            visit(LockScope::Operation(claim.id), scope_claim(&claim));
        }
        if let Some(claim) = self.transaction {
            visit(LockScope::Transaction(claim.id), scope_claim(&claim));
        }
    }
}

/// Compact set of distinct modes represented by a family or resource.
///
/// This mask records presence, not multiplicity, and it is not itself a lock
/// mode. `ResourceState` pairs its mask with per-mode counts. The owner-side
/// family aggregate instead recomputes this mask from its bounded claim slots,
/// which is necessary when two scopes hold the same mode and one is removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ModeMask(u8);

impl ModeMask {
    pub(super) const EMPTY: Self = Self(0);

    #[inline]
    pub(super) fn insert(&mut self, mode: LockMode) {
        self.0 |= mode_bit(mode);
    }

    #[inline]
    pub(super) fn remove(&mut self, mode: LockMode) {
        self.0 &= !mode_bit(mode);
    }

    #[inline]
    pub(super) const fn is_empty(self) -> bool {
        self.0 == 0
    }
}

/// Owner-side aggregate for one `(LockFamily, LockResource)` pair.
///
/// `claims` preserves the exact logical owners and lifetimes. `covering_mode`
/// is the single mode published for this family in the physical
/// `LockManager`; it must be one of the occupied claim modes and must cover
/// every other occupied mode. `claim_mask` records all distinct logical modes
/// for diagnostics and invariant checks.
///
/// Directional same-family admission keeps the occupied modes comparable, so
/// aggregation only selects an existing strongest claim. It never synthesizes
/// a join such as `SIX` and never silently promotes `S + IX` to `X`.
///
/// Mutations first calculate a candidate aggregate from the fixed slots.
/// Callers can then perform any required physical transition before committing
/// the matching claim-slot and exact-scope changes.
pub(super) struct LocalFamilyResourceState {
    claims: FamilyClaimSlots,
    claim_mask: ModeMask,
    covering_mode: LockMode,
}

impl LocalFamilyResourceState {
    #[inline]
    pub(super) fn new(scope: LockScope, claim_no: ClaimNo, mode: LockMode) -> Self {
        let mut claim_mask = ModeMask::default();
        claim_mask.insert(mode);
        let mut claims = FamilyClaimSlots::default();
        claims.insert(scope, claim_no, mode);
        Self {
            claims,
            claim_mask,
            covering_mode: mode,
        }
    }

    #[inline]
    pub(super) fn get(&self, scope: LockScope) -> Option<ScopeClaim> {
        self.claims.get(scope)
    }

    #[inline]
    pub(super) fn insert(
        &mut self,
        resource: LockResource,
        scope: LockScope,
        claim_no: ClaimNo,
        mode: LockMode,
    ) {
        self.claims.insert(scope, claim_no, mode);
        self.recompute(resource);
    }

    #[inline]
    pub(super) fn update(
        &mut self,
        resource: LockResource,
        scope: LockScope,
        claim_no: ClaimNo,
        mode: LockMode,
    ) {
        self.claims.update(scope, claim_no, mode);
        self.recompute(resource);
    }

    #[inline]
    pub(super) fn remove(
        &mut self,
        scope: LockScope,
        claim_no: ClaimNo,
        remaining_mask: ModeMask,
        remaining_covering_mode: Option<LockMode>,
    ) -> ScopeClaim {
        let removed = self.claims.remove(scope, claim_no);
        if let Some(covering_mode) = remaining_covering_mode {
            self.claim_mask = remaining_mask;
            self.covering_mode = covering_mode;
        }
        removed
    }

    #[inline]
    pub(super) fn for_each(&self, visit: impl FnMut(LockScope, ScopeClaim)) {
        self.claims.for_each(visit);
    }

    #[inline]
    pub(super) fn aggregates_after_update(
        &self,
        resource: LockResource,
        scope: LockScope,
        claim_no: ClaimNo,
        mode: LockMode,
    ) -> (ModeMask, LockMode) {
        let current = self.claims.get(scope).unwrap_or_else(|| {
            panic!(
                "family claim update plan requires an occupied slot: \
                 resource={resource}, scope={scope:?}, claim_no={claim_no:?}"
            )
        });
        assert!(
            current.claim_no == claim_no,
            "family claim update plan has a stale claim number: \
             resource={resource}, scope={scope:?}, expected={claim_no:?}, actual={:?}",
            current.claim_no
        );
        // Plan the replacement without changing either owner-side index. A
        // physical conversion may still fail, in which case the old aggregate
        // and exact claim must remain authoritative.
        self.aggregates_excluding(resource, scope, Some(mode))
            .unwrap_or_else(|| {
                panic!(
                    "family claim update plan unexpectedly produced no claims: \
                     resource={resource}, scope={scope:?}"
                )
            })
    }

    #[inline]
    pub(super) fn aggregates_after_remove(
        &self,
        resource: LockResource,
        scope: LockScope,
        claim_no: ClaimNo,
    ) -> Option<(ModeMask, LockMode)> {
        let current = self.claims.get(scope).unwrap_or_else(|| {
            panic!(
                "family claim removal plan requires an occupied slot: \
                 resource={resource}, scope={scope:?}, claim_no={claim_no:?}"
            )
        });
        assert!(
            current.claim_no == claim_no,
            "family claim removal plan has a stale claim number: \
             resource={resource}, scope={scope:?}, expected={claim_no:?}, actual={:?}",
            current.claim_no
        );
        // As with conversion, compute the post-removal physical mode before
        // deleting the logical claim. The caller publishes the physical
        // transition first and commits both owner-side removals afterward.
        self.aggregates_excluding(resource, scope, None)
    }

    #[cfg(test)]
    #[inline]
    pub(super) const fn claim_mask(&self) -> ModeMask {
        self.claim_mask
    }

    #[inline]
    pub(super) const fn covering_mode(&self) -> LockMode {
        self.covering_mode
    }

    #[inline]
    fn recompute(&mut self, resource: LockResource) {
        let mut mask = ModeMask::default();
        let mut covering = None;
        self.claims.for_each(|_scope, claim| {
            mask.insert(claim.mode);
            // Same-family policy admits only a directional chain. Therefore
            // one occupied claim must cover the other at each merge; reaching
            // an incomparable pair means admission violated the aggregate
            // representation rather than that a synthetic mode is needed.
            covering = match covering {
                None => Some(claim.mode),
                Some(current) if current.covers(resource, claim.mode) => Some(current),
                Some(current) if claim.mode.covers(resource, current) => Some(claim.mode),
                Some(current) => panic!(
                    "family/resource claims have no occupied covering mode: \
                     resource={resource}, left={current}, right={}",
                    claim.mode
                ),
            };
        });
        self.claim_mask = mask;
        self.covering_mode = covering.unwrap_or_else(|| {
            panic!("live family/resource state must retain at least one claim: resource={resource}")
        });
    }

    #[inline]
    fn aggregates_excluding(
        &self,
        resource: LockResource,
        excluded_scope: LockScope,
        replacement: Option<LockMode>,
    ) -> Option<(ModeMask, LockMode)> {
        // Rebuild instead of clearing one bit from `claim_mask`: multiple
        // scopes may hold the same mode, so removing one claim does not imply
        // that the mode disappears from the aggregate.
        let mut mask = ModeMask::default();
        let mut covering = None;
        self.claims.for_each(|scope, claim| {
            let mode = if scope == excluded_scope {
                let Some(mode) = replacement else {
                    return;
                };
                mode
            } else {
                claim.mode
            };
            mask.insert(mode);
            covering = merge_covering(resource, covering, mode);
        });
        covering.map(|mode| (mask, mode))
    }
}

#[inline]
const fn mode_bit(mode: LockMode) -> u8 {
    match mode {
        LockMode::IntentShared => 1,
        LockMode::IntentExclusive => 1 << 1,
        LockMode::Shared => 1 << 2,
        LockMode::Exclusive => 1 << 3,
    }
}

#[inline]
fn merge_covering(
    resource: LockResource,
    current: Option<LockMode>,
    mode: LockMode,
) -> Option<LockMode> {
    match current {
        None => Some(mode),
        Some(current) if current.covers(resource, mode) => Some(current),
        Some(current) if mode.covers(resource, current) => Some(mode),
        Some(current) => panic!(
            "family/resource claims have no occupied covering mode: \
             resource={resource}, left={current}, right={mode}"
        ),
    }
}

#[inline]
fn scope_claim<I>(claim: &FamilyClaim<I>) -> ScopeClaim {
    ScopeClaim {
        claim_no: claim.claim_no,
        mode: claim.mode,
    }
}

#[inline]
fn update_claim<I: Copy + PartialEq>(
    claim: Option<&mut FamilyClaim<I>>,
    id: I,
    claim_no: ClaimNo,
    mode: LockMode,
    scope: LockScope,
) {
    let claim = claim
        .filter(|claim| claim.id == id)
        .unwrap_or_else(|| panic!("missing or wrong-id expanded claim on update: scope={scope:?}"));
    assert!(
        claim.claim_no == claim_no,
        "expanded family claim-number mismatch on update: scope={scope:?}, \
         expected={claim_no:?}, actual={:?}",
        claim.claim_no
    );
    claim.mode = mode;
}

#[inline]
fn take_matching<I: Copy + PartialEq>(
    slot: &mut Option<FamilyClaim<I>>,
    id: I,
) -> Option<ScopeClaim> {
    if slot.as_ref().is_some_and(|claim| claim.id == id) {
        slot.take().map(|claim| ScopeClaim {
            claim_no: claim.claim_no,
            mode: claim.mode,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::TableID;
    use std::mem::size_of;

    #[test]
    fn claim_layout_is_compact() {
        assert!(size_of::<FamilyClaim<()>>() <= 16);
        assert!(size_of::<FamilyClaim<OperationID>>() <= 24);
        assert!(size_of::<FamilyClaim<TrxID>>() <= 24);
        assert_eq!(size_of::<FamilyClaimSlots>(), 64);
    }

    #[test]
    fn fixed_claim_slots_reuse_scope_classes() {
        let operation_id = OperationID::new(2);
        let transaction_id = TrxID::new(3);
        let mut claims = FamilyClaimSlots::default();
        claims.insert(
            LockScope::SessionExplicit,
            ClaimNo::new(1),
            LockMode::Exclusive,
        );
        claims.insert(
            LockScope::Operation(operation_id),
            ClaimNo::new(2),
            LockMode::Shared,
        );

        claims.remove(LockScope::Operation(operation_id), ClaimNo::new(2));
        claims.insert(
            LockScope::Transaction(transaction_id),
            ClaimNo::new(3),
            LockMode::IntentShared,
        );
        assert_eq!(
            claims.get(LockScope::Transaction(transaction_id)),
            Some(ScopeClaim {
                claim_no: ClaimNo::new(3),
                mode: LockMode::IntentShared,
            })
        );
    }

    #[test]
    fn expanded_claims_retain_typed_identities_and_aggregates() {
        let resource = LockResource::TableData(TableID::new(10));
        let operation_id = OperationID::new(11);
        let transaction_id = TrxID::new(12);
        let mut state = LocalFamilyResourceState::new(
            LockScope::SessionExplicit,
            ClaimNo::new(1),
            LockMode::Exclusive,
        );
        state.insert(
            resource,
            LockScope::Operation(operation_id),
            ClaimNo::new(2),
            LockMode::Shared,
        );
        state.insert(
            resource,
            LockScope::Transaction(transaction_id),
            ClaimNo::new(3),
            LockMode::IntentShared,
        );
        let slots = &state.claims;
        assert!(slots.session_explicit.is_some());
        assert_eq!(slots.operation.unwrap().id, operation_id);
        assert_eq!(slots.transaction.unwrap().id, transaction_id);
        assert_eq!(state.claim_mask(), ModeMask(0b1101));
        assert_eq!(state.covering_mode(), LockMode::Exclusive);
        assert_eq!(
            state.get(LockScope::SessionExplicit),
            Some(ScopeClaim {
                claim_no: ClaimNo::new(1),
                mode: LockMode::Exclusive,
            })
        );
        assert_eq!(
            state.get(LockScope::Operation(operation_id)),
            Some(ScopeClaim {
                claim_no: ClaimNo::new(2),
                mode: LockMode::Shared,
            })
        );
    }
}
