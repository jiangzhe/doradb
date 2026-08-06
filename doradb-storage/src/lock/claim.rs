use super::{LockMode, LockOwner, LockResource, LockScope, StmtNo};
use crate::id::{ClaimNo, OperationID, TrxID};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ScopeClaim {
    pub(super) claim_no: ClaimNo,
    pub(super) mode: LockMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ClaimToken {
    pub(super) resource: LockResource,
    pub(super) owner: LockOwner,
    pub(super) claim_no: ClaimNo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InlineFamilyClaim {
    scope: LockScope,
    claim_no: ClaimNo,
    mode: LockMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FamilyClaim<I> {
    id: I,
    claim_no: ClaimNo,
    mode: LockMode,
}

#[derive(Debug, PartialEq, Eq)]
enum FamilyClaims {
    Inline(InlineFamilyClaim),
    Expanded(Box<FamilyClaimSlots>),
}

impl FamilyClaims {
    #[inline]
    fn new(scope: LockScope, claim_no: ClaimNo, mode: LockMode) -> Self {
        Self::Inline(InlineFamilyClaim {
            scope,
            claim_no,
            mode,
        })
    }

    #[inline]
    fn get(&self, scope: LockScope) -> Option<ScopeClaim> {
        match self {
            Self::Inline(claim) if claim.scope == scope => Some(ScopeClaim {
                claim_no: claim.claim_no,
                mode: claim.mode,
            }),
            Self::Inline(_) => None,
            Self::Expanded(slots) => slots.get(scope),
        }
    }

    /// Inserts a distinct exact scope and returns whether inline storage expanded.
    #[inline]
    fn insert(&mut self, scope: LockScope, claim_no: ClaimNo, mode: LockMode) -> bool {
        assert!(
            self.get(scope).is_none(),
            "duplicate family lock scope slot: scope={scope:?}, claim_no={claim_no:?}"
        );
        match self {
            Self::Inline(inline) => {
                assert!(
                    scope_class(inline.scope) != scope_class(scope),
                    "family lock topology permits at most one live scope per class: \
                     existing_scope={:?}, new_scope={scope:?}",
                    inline.scope
                );
                let previous = *inline;
                let mut slots = Box::<FamilyClaimSlots>::default();
                slots.insert(previous.scope, previous.claim_no, previous.mode);
                slots.insert(scope, claim_no, mode);
                *self = Self::Expanded(slots);
                true
            }
            Self::Expanded(slots) => {
                slots.insert(scope, claim_no, mode);
                false
            }
        }
    }

    #[inline]
    fn update(&mut self, scope: LockScope, claim_no: ClaimNo, mode: LockMode) {
        match self {
            Self::Inline(claim) if claim.scope == scope && claim.claim_no == claim_no => {
                claim.mode = mode;
            }
            Self::Inline(claim) => panic!(
                "family inline claim update mismatch: expected_scope={scope:?}, \
                 expected_claim_no={claim_no:?}, actual_claim={claim:?}"
            ),
            Self::Expanded(slots) => slots.update(scope, claim_no, mode),
        }
    }

    #[inline]
    fn remove(&mut self, scope: LockScope, claim_no: ClaimNo) -> ScopeClaim {
        match self {
            Self::Inline(claim) if claim.scope == scope && claim.claim_no == claim_no => {
                ScopeClaim {
                    claim_no: claim.claim_no,
                    mode: claim.mode,
                }
            }
            Self::Inline(claim) => panic!(
                "family inline claim removal mismatch: expected_scope={scope:?}, \
                 expected_claim_no={claim_no:?}, actual_claim={claim:?}"
            ),
            Self::Expanded(slots) => slots.remove(scope, claim_no),
        }
    }

    #[inline]
    fn for_each(&self, mut visit: impl FnMut(LockScope, ScopeClaim)) {
        match self {
            Self::Inline(claim) => visit(
                claim.scope,
                ScopeClaim {
                    claim_no: claim.claim_no,
                    mode: claim.mode,
                },
            ),
            Self::Expanded(slots) => slots.for_each(visit),
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct FamilyClaimSlots {
    session_explicit: Option<FamilyClaim<()>>,
    operation: Option<FamilyClaim<OperationID>>,
    transaction: Option<FamilyClaim<TrxID>>,
    statement: Option<FamilyClaim<(TrxID, StmtNo)>>,
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
            LockScope::Statement(trx_id, stmt_no) => self
                .statement
                .as_ref()
                .filter(|claim| claim.id == (trx_id, stmt_no))
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
            LockScope::Statement(trx_id, stmt_no) => {
                assert!(
                    self.statement.is_none(),
                    "duplicate statement family claim slot: trx_id={trx_id}, stmt_no={stmt_no}"
                );
                self.statement = Some(FamilyClaim {
                    id: (trx_id, stmt_no),
                    claim_no,
                    mode,
                });
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
            LockScope::Statement(trx_id, stmt_no) => update_claim(
                self.statement.as_mut(),
                (trx_id, stmt_no),
                claim_no,
                mode,
                scope,
            ),
        }
    }

    #[inline]
    fn remove(&mut self, scope: LockScope, claim_no: ClaimNo) -> ScopeClaim {
        let claim = match scope {
            LockScope::SessionExplicit => take_matching(&mut self.session_explicit, ()),
            LockScope::Operation(id) => take_matching(&mut self.operation, id),
            LockScope::Transaction(id) => take_matching(&mut self.transaction, id),
            LockScope::Statement(trx_id, stmt_no) => {
                take_matching(&mut self.statement, (trx_id, stmt_no))
            }
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
        if let Some(claim) = self.statement {
            visit(
                LockScope::Statement(claim.id.0, claim.id.1),
                scope_claim(&claim),
            );
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ModeMask(u8);

impl ModeMask {
    #[inline]
    pub(super) fn insert(&mut self, mode: LockMode) {
        self.0 |= match mode {
            LockMode::IntentShared => 1,
            LockMode::IntentExclusive => 1 << 1,
            LockMode::Shared => 1 << 2,
            LockMode::Exclusive => 1 << 3,
        };
    }
}

pub(super) struct LocalFamilyResourceState {
    claims: FamilyClaims,
    claim_mask: ModeMask,
    covering_mode: LockMode,
}

impl LocalFamilyResourceState {
    #[inline]
    pub(super) fn new(scope: LockScope, claim_no: ClaimNo, mode: LockMode) -> Self {
        let mut claim_mask = ModeMask::default();
        claim_mask.insert(mode);
        Self {
            claims: FamilyClaims::new(scope, claim_no, mode),
            claim_mask,
            covering_mode: mode,
        }
    }

    #[inline]
    pub(super) fn get(&self, scope: LockScope) -> Option<ScopeClaim> {
        self.claims.get(scope)
    }

    /// Inserts a distinct exact claim and returns whether inline storage expanded.
    #[inline]
    pub(super) fn insert(
        &mut self,
        resource: LockResource,
        scope: LockScope,
        claim_no: ClaimNo,
        mode: LockMode,
    ) -> bool {
        let expanded = self.claims.insert(scope, claim_no, mode);
        self.recompute(resource);
        expanded
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
}

#[inline]
const fn scope_class(scope: LockScope) -> u8 {
    match scope {
        LockScope::SessionExplicit => 0,
        LockScope::Operation(_) => 1,
        LockScope::Transaction(_) => 2,
        LockScope::Statement(_, _) => 3,
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
        assert!(size_of::<FamilyClaim<(TrxID, StmtNo)>>() <= 32);
        assert_eq!(size_of::<FamilyClaimSlots>(), 96);
    }

    #[test]
    fn inline_claims_expand_once_reuse_slots_and_never_shrink() {
        let operation_id = OperationID::new(2);
        let transaction_id = TrxID::new(3);
        let mut claims = FamilyClaims::new(
            LockScope::SessionExplicit,
            ClaimNo::new(1),
            LockMode::Exclusive,
        );
        assert!(matches!(claims, FamilyClaims::Inline(_)));
        assert!(claims.insert(
            LockScope::Operation(operation_id),
            ClaimNo::new(2),
            LockMode::Shared
        ));
        assert!(matches!(claims, FamilyClaims::Expanded(_)));

        claims.remove(LockScope::Operation(operation_id), ClaimNo::new(2));
        assert!(matches!(claims, FamilyClaims::Expanded(_)));
        assert!(!claims.insert(
            LockScope::Transaction(transaction_id),
            ClaimNo::new(3),
            LockMode::IntentShared
        ));
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
        let statement_no = 13;
        let mut state = LocalFamilyResourceState::new(
            LockScope::SessionExplicit,
            ClaimNo::new(1),
            LockMode::Exclusive,
        );
        assert!(state.insert(
            resource,
            LockScope::Operation(operation_id),
            ClaimNo::new(2),
            LockMode::Shared,
        ));
        assert!(!state.insert(
            resource,
            LockScope::Transaction(transaction_id),
            ClaimNo::new(3),
            LockMode::IntentShared,
        ));
        assert!(!state.insert(
            resource,
            LockScope::Statement(transaction_id, statement_no),
            ClaimNo::new(4),
            LockMode::IntentShared,
        ));

        let FamilyClaims::Expanded(slots) = &state.claims else {
            panic!("four claims must use expanded typed slots")
        };
        assert!(slots.session_explicit.is_some());
        assert_eq!(slots.operation.unwrap().id, operation_id);
        assert_eq!(slots.transaction.unwrap().id, transaction_id);
        assert_eq!(slots.statement.unwrap().id, (transaction_id, statement_no));
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
