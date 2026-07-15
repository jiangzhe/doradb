use super::checkpoint_workflow::{
    FreezeAttempt, FrozenPage, FrozenPageBatch, FrozenPageValidationState, PreparedTransitionPage,
};
use super::{DeleteMarker, Table};
use crate::bitmap::Bitmap;
use crate::buffer::PoolGuards;
use crate::buffer::frame::FrameContext;
use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::error::{InternalError, Result};
use crate::id::{PageID, RowID, TableID, TrxID};
use crate::row::RowPage;
use crate::trx::undo::{RowUndoKind, UndoStatus};
use crate::trx::ver_map::RowPageState;
use crate::trx::{MAX_SNAPSHOT_TS, SharedTrxStatus, trx_is_committed};
use error_stack::Report;
use std::mem::take;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FrozenPageValidationDelay {
    pub(super) page_id: PageID,
    pub(super) stable_page_count: usize,
    pub(super) required_cutoff_ts: Option<TrxID>,
    pub(super) unresolved_status: bool,
}

struct FrozenPageReadinessAnalysis {
    validation: FrozenPageValidationState,
    blockers: Vec<Arc<SharedTrxStatus>>,
    plan: Option<PreparedTransitionPage>,
}

struct FrozenPageScan {
    page_info: FrozenPage,
    required_cutoff_ts: Option<TrxID>,
    blocked: bool,
    blockers: Vec<Arc<SharedTrxStatus>>,
    del_bitmap: Vec<u64>,
    overlay_markers: Vec<(RowID, DeleteMarker)>,
}

impl FrozenPageScan {
    #[inline]
    fn validation(&self) -> FrozenPageValidationState {
        if self.blocked {
            FrozenPageValidationState::Blocked {
                required_cutoff_ts: self.required_cutoff_ts,
            }
        } else {
            FrozenPageValidationState::Stable {
                required_cutoff_ts: self.required_cutoff_ts,
            }
        }
    }

    fn into_readiness_analysis(
        mut self,
        cutoff_ts: TrxID,
        observed_version: u64,
    ) -> FrozenPageReadinessAnalysis {
        let validation = self.validation();
        let blockers = take(&mut self.blockers);
        let plan = self.into_plan(cutoff_ts, observed_version);
        FrozenPageReadinessAnalysis {
            validation,
            blockers,
            plan,
        }
    }

    fn into_plan(self, cutoff_ts: TrxID, observed_version: u64) -> Option<PreparedTransitionPage> {
        if self.blocked
            || self
                .required_cutoff_ts
                .is_some_and(|required| required > cutoff_ts)
        {
            return None;
        }
        Some(PreparedTransitionPage {
            page_id: self.page_info.page_id,
            start_row_id: self.page_info.start_row_id,
            end_row_id: self.page_info.end_row_id,
            cutoff_ts,
            observed_version,
            required_cutoff_ts: self.required_cutoff_ts,
            del_bitmap: self.del_bitmap,
            overlay_markers: self.overlay_markers,
        })
    }
}

#[derive(Clone, Copy)]
enum FrozenPageScanMode {
    EstablishReadiness { frozen_ts: TrxID },
    RefreshStablePlan,
}

impl FrozenPageScanMode {
    #[inline]
    fn unresolved_ownership_blocks(self, ts: TrxID) -> bool {
        match self {
            FrozenPageScanMode::EstablishReadiness { frozen_ts } => {
                active_writer_sts(ts) < frozen_ts
            }
            FrozenPageScanMode::RefreshStablePlan => false,
        }
    }
}

impl Table {
    pub(super) async fn load_frozen_pages_for_transition(
        &self,
        guards: &PoolGuards,
        frozen_pages: &[FrozenPage],
    ) -> Result<Vec<PageSharedGuard<RowPage>>> {
        let mut page_guards = Vec::with_capacity(frozen_pages.len());
        for page_info in frozen_pages {
            page_guards.push(
                self.mem
                    .must_get_row_page_shared(guards, page_info.page_id)
                    .await?,
            );
        }
        Ok(page_guards)
    }

    pub(super) fn validate_and_publish_loaded_pages_frozen(
        &self,
        page_guards: &[PageSharedGuard<RowPage>],
        pages: &[FrozenPage],
        attempt: &mut FreezeAttempt<'_>,
    ) -> bool {
        #[cfg(test)]
        use super::test_hooks;

        assert_eq!(
            page_guards.len(),
            pages.len(),
            "freeze page guard count mismatch: table_id={}",
            self.table_id()
        );

        // Preflight is fully reversible. Workflow admission excludes another
        // maintenance state transition, while foreground writers do not change
        // page state or row-page identity.
        for (page_guard, page_info) in page_guards.iter().zip(pages) {
            let (ctx, page) = page_guard.ctx_and_page();
            assert_eq!(
                page_guard.page_id(),
                page_info.page_id,
                "freeze page id mismatch: table_id={}",
                self.table_id()
            );
            assert_eq!(
                page.header.start_row_id,
                page_info.start_row_id,
                "freeze page start row id mismatch: table_id={}, page_id={}",
                self.table_id(),
                page_info.page_id
            );
            let state = ctx.expect_vmap().inspect_state();
            assert_eq!(
                state,
                RowPageState::Active,
                "admitted freeze requires ACTIVE page: table_id={}, page_id={}",
                self.table_id(),
                page_info.page_id
            );
        }

        // No fallible operation or await is allowed after workflow publication
        // admission. Each state critical section is independent so unrelated
        // page writers are not held behind a batch-wide set of state locks.
        if !attempt.begin_page_publication() {
            return false;
        }
        for (page_guard, page_info) in page_guards.iter().zip(pages) {
            let (ctx, _) = page_guard.ctx_and_page();
            let mut state = ctx.expect_vmap().write_state();
            assert_eq!(
                *state,
                RowPageState::Active,
                "freeze page state changed after preflight: table_id={}, page_id={}",
                self.table_id(),
                page_info.page_id
            );
            #[cfg(test)]
            test_hooks::run_test_freeze_page_state_locked_hook(page_info.page_id);
            *state = RowPageState::Frozen;
        }
        true
    }

    pub(super) fn prepare_page_transition(
        &self,
        page_guards: &[PageSharedGuard<RowPage>],
        batch: &mut FrozenPageBatch,
        cutoff_ts: TrxID,
    ) -> Option<FrozenPageValidationDelay> {
        #[cfg(test)]
        use super::test_hooks;

        self.assert_frozen_batch_shape(page_guards, batch);
        assert!(
            !page_guards.is_empty(),
            "transition preparation requires a non-empty frozen batch: table_id={}",
            batch.table_id
        );
        // Phase 1 advances the cached ready prefix only. A delayed attempt
        // returns before doing work on pages beyond its first blocker.
        if let Some(delay) = validate_frozen_pages_incrementally(page_guards, batch, cutoff_ts) {
            return Some(delay);
        }
        #[cfg(test)]
        test_hooks::run_test_frozen_pages_ready_hook();

        // Phase 2 refreshes cutoff- and version-specific plans for the entire
        // ready batch. Readiness is now monotonic: later frozen-page ownership
        // changes may invalidate plans but cannot reintroduce a cutoff delay.
        refresh_frozen_page_plans_optimistically(page_guards, batch, cutoff_ts);
        #[cfg(test)]
        test_hooks::run_test_stable_page_plans_refreshed_hook();
        None
    }

    /// Refresh one canonical frozen page's complete readiness description.
    pub(super) fn refresh_frozen_page_readiness(
        &self,
        page_guard: &PageSharedGuard<RowPage>,
        batch: &mut FrozenPageBatch,
        page_idx: usize,
        cutoff_ts: TrxID,
    ) -> FrozenPageValidationState {
        let page_info = batch.pages[page_idx];
        match batch.validation[page_idx] {
            state @ FrozenPageValidationState::Stable { .. } => {
                let (ctx, _) = page_guard.ctx_and_page();
                assert_frozen_page_state(ctx, batch.table_id, page_info.page_id);
                debug_assert!(batch.blockers_for(page_info.page_id).is_none());
                state
            }
            FrozenPageValidationState::Unchecked | FrozenPageValidationState::Blocked { .. } => {
                analyze_frozen_page_readiness(page_guard, batch, page_idx, cutoff_ts)
            }
        }
    }

    pub(super) fn apply_page_transition(
        &self,
        page_guards: &[PageSharedGuard<RowPage>],
        batch: &mut FrozenPageBatch,
        cutoff_ts: TrxID,
    ) -> Result<()> {
        #[cfg(test)]
        use super::test_hooks;

        self.assert_frozen_batch_shape(page_guards, batch);
        assert!(
            !page_guards.is_empty(),
            "transition application requires a non-empty frozen batch: table_id={}",
            batch.table_id
        );
        // Preparation has completed every retryable decision. The caller now
        // owns workflow admission, so this phase performs only authoritative
        // page-local revalidation and fail-closed Transition publication.
        for (page_idx, page_guard) in page_guards.iter().enumerate() {
            let page_info = batch.pages[page_idx];
            let (ctx, page) = page_guard.ctx_and_page();
            let map = ctx.expect_vmap();
            assert!(
                matches!(
                    batch.validation[page_idx],
                    FrozenPageValidationState::Stable { required_cutoff_ts }
                        if required_cutoff_ts.is_none_or(|required| required <= cutoff_ts)
                ),
                "transition revalidation requires stable page readiness: table_id={}, page_id={}, cutoff_ts={cutoff_ts}",
                batch.table_id,
                page_info.page_id
            );
            // The page-state write lock drains all in-flight frozen writers
            // and prevents another writer from starting until this page has
            // transitioned with a plan regenerated from a stable image proof.
            let mut state = map.write_state();
            assert_eq!(
                *state,
                RowPageState::Frozen,
                "frozen page batch state mismatch: table_id={}, page_id={}",
                batch.table_id,
                page_info.page_id
            );
            // This load closes the race left by optimistic refresh. Reuse is
            // safe only when the prepared plan still names this exact page,
            // cutoff, and full mutation version; otherwise rebuild while the
            // page remains exclusively state-locked.
            let version = map.frozen_mutation_version();
            let plan = batch.prepared[page_idx]
                .take()
                .filter(|plan| plan.matches(page_info, cutoff_ts, version))
                .or_else(|| {
                    scan_frozen_page(
                        page,
                        ctx,
                        page_info,
                        FrozenPageScanMode::RefreshStablePlan,
                        cutoff_ts,
                    )
                    .into_plan(cutoff_ts, version)
                })
                .expect("stable frozen page must yield a transition plan under its state lock");
            *state = RowPageState::Transition;
            self.install_transition_markers(&plan)?;
            batch.prepared[page_idx] = Some(plan);
            // Release each page independently so writers on the still-frozen
            // suffix are not stalled behind the whole batch transition.
            drop(state);
            #[cfg(test)]
            test_hooks::run_test_transition_page_published_hook(page_info.page_id);
        }
        Ok(())
    }

    fn assert_frozen_batch_shape(
        &self,
        page_guards: &[PageSharedGuard<RowPage>],
        batch: &FrozenPageBatch,
    ) {
        assert_eq!(page_guards.len(), batch.pages.len());
        assert_eq!(batch.validation.len(), batch.pages.len());
        assert_eq!(batch.prepared.len(), batch.pages.len());
        for (page_guard, page_info) in page_guards.iter().zip(&batch.pages) {
            assert_eq!(page_guard.page_id(), page_info.page_id);
            assert_eq!(
                page_guard.page().header.start_row_id,
                page_info.start_row_id
            );
        }
    }

    fn install_transition_markers(&self, plan: &PreparedTransitionPage) -> Result<()> {
        for (row_id, marker) in &plan.overlay_markers {
            let result = match marker {
                DeleteMarker::Ref(status) => {
                    self.deletion_buffer()
                        .put_ref(*row_id, Arc::clone(status), MAX_SNAPSHOT_TS)
                }
                DeleteMarker::Committed(cts) => self.deletion_buffer().put_committed(*row_id, *cts),
            };
            if let Err(reason) = result {
                return Err(Report::new(InternalError::Generic)
                    .attach(format!(
                        "transition marker install conflict: table_id={}, page_id={}, row_id={row_id}, reason={reason:?}",
                        self.table_id(), plan.page_id
                    ))
                    .into());
            }
        }
        Ok(())
    }
}

fn validate_frozen_pages_incrementally(
    page_guards: &[PageSharedGuard<RowPage>],
    batch: &mut FrozenPageBatch,
    cutoff_ts: TrxID,
) -> Option<FrozenPageValidationDelay> {
    // Stop at the first delay. Stable pages in the preceding prefix retain
    // their readiness proof, while the unchecked suffix is deferred until a
    // retry can advance past the blocker.
    let mut first_delay = None;
    let mut stable_page_count = 0usize;
    for (page_idx, page_guard) in page_guards.iter().enumerate() {
        let page_info = batch.pages[page_idx];
        let validation = match batch.validation[page_idx] {
            FrozenPageValidationState::Stable { .. } => {
                // Stable caches only the image-readiness proof and required
                // cutoff. Plan freshness and current overlays are deliberately
                // deferred to the full optimistic refresh below.
                let (ctx, _) = page_guard.ctx_and_page();
                assert_frozen_page_state(ctx, batch.table_id, page_info.page_id);
                batch.validation[page_idx]
            }
            FrozenPageValidationState::Unchecked | FrozenPageValidationState::Blocked { .. } => {
                // Unchecked pages need their first proof; blocked pages must be
                // revisited because transaction resolution may have made the
                // image checkpointable since the previous attempt.
                analyze_frozen_page_readiness(page_guard, batch, page_idx, cutoff_ts)
            }
        };
        record_frozen_page_readiness(
            &mut first_delay,
            &mut stable_page_count,
            page_info.page_id,
            validation,
            cutoff_ts,
        );
        if first_delay.is_some() {
            // Do not speculate on the suffix. Its Unchecked state makes the
            // next retry resume at the blocker following the cached prefix.
            return first_delay;
        }
    }
    None
}

fn refresh_frozen_page_plans_optimistically(
    page_guards: &[PageSharedGuard<RowPage>],
    batch: &mut FrozenPageBatch,
    cutoff_ts: TrxID,
) {
    // Once cached readiness says the whole batch can proceed, refresh every
    // cutoff-specific plan before acquiring the first page state write lock.
    // Later mutations observed the Frozen state, so they can change bitmap or
    // overlay output but cannot invalidate the established image proof.
    for (page_idx, page_guard) in page_guards.iter().enumerate() {
        refresh_stable_page_plan(page_guard, batch, page_idx, cutoff_ts);
    }
}

fn analyze_frozen_page_readiness(
    page_guard: &PageSharedGuard<RowPage>,
    batch: &mut FrozenPageBatch,
    page_idx: usize,
    cutoff_ts: TrxID,
) -> FrozenPageValidationState {
    let page_info = batch.pages[page_idx];
    let (ctx, page) = page_guard.ctx_and_page();
    let map = ctx.expect_vmap();
    assert_frozen_page_state(ctx, batch.table_id, page_info.page_id);
    // This version is an equality token, not a seqlock parity value. Any
    // mutation between the samples invalidates the optimistic plan.
    let version_before = map.frozen_mutation_version();
    let analysis = scan_frozen_page(
        page,
        ctx,
        page_info,
        FrozenPageScanMode::EstablishReadiness {
            frozen_ts: batch.frozen_ts,
        },
        cutoff_ts,
    )
    .into_readiness_analysis(cutoff_ts, version_before);
    // A mutation that overlaps analysis can start and finish before the final
    // transition lock is taken. Comparing the complete counter values here
    // prevents carrying that already-stale plan into locked revalidation.
    let version_after = map.frozen_mutation_version();
    batch.validation[page_idx] = analysis.validation;
    match analysis.validation {
        FrozenPageValidationState::Blocked { .. } => {
            batch.set_blocked_page(page_info.page_id, analysis.blockers);
        }
        FrozenPageValidationState::Stable { .. } => {
            assert!(
                analysis.blockers.is_empty(),
                "stable frozen page cannot retain transaction blockers"
            );
            batch.clear_blocked_page(page_info.page_id);
        }
        FrozenPageValidationState::Unchecked => {
            unreachable!("readiness analysis cannot publish unchecked blockers")
        }
    }
    // The readiness proof remains monotonic, but a plan from a changing page
    // must be rebuilt under its final state write lock.
    retain_optimistic_page_plan(
        batch,
        page_idx,
        version_before,
        version_after,
        analysis.plan,
    );
    analysis.validation
}

fn refresh_stable_page_plan(
    page_guard: &PageSharedGuard<RowPage>,
    batch: &mut FrozenPageBatch,
    page_idx: usize,
    cutoff_ts: TrxID,
) {
    let page_info = batch.pages[page_idx];
    let (ctx, page) = page_guard.ctx_and_page();
    let map = ctx.expect_vmap();
    assert_frozen_page_state(ctx, batch.table_id, page_info.page_id);
    assert!(
        matches!(
            batch.validation[page_idx],
            FrozenPageValidationState::Stable { required_cutoff_ts }
                if required_cutoff_ts.is_none_or(|required| required <= cutoff_ts)
        ),
        "stable-plan refresh requires ready page: table_id={}, page_id={}, cutoff_ts={cutoff_ts}",
        batch.table_id,
        page_info.page_id
    );
    let version_before = map.frozen_mutation_version();
    // Exact matches avoid another undo walk. A cutoff or version change makes
    // the representation stale even though the cached readiness proof survives.
    let plan = batch.prepared[page_idx]
        .take()
        .filter(|plan| plan.matches(page_info, cutoff_ts, version_before))
        .or_else(|| {
            scan_frozen_page(
                page,
                ctx,
                page_info,
                FrozenPageScanMode::RefreshStablePlan,
                cutoff_ts,
            )
            .into_plan(cutoff_ts, version_before)
        });
    let version_after = map.frozen_mutation_version();
    retain_optimistic_page_plan(batch, page_idx, version_before, version_after, plan);
}

fn retain_optimistic_page_plan(
    batch: &mut FrozenPageBatch,
    page_idx: usize,
    version_before: u64,
    version_after: u64,
    plan: Option<PreparedTransitionPage>,
) {
    batch.prepared[page_idx] = (version_before == version_after).then_some(plan).flatten();
    #[cfg(test)]
    {
        use super::test_hooks::run_test_optimistic_page_plan_comparison_hook;

        run_test_optimistic_page_plan_comparison_hook(
            batch.pages[page_idx].page_id,
            version_before,
            version_after,
            batch.prepared[page_idx].is_some(),
        );
    }
}

#[inline]
fn assert_frozen_page_state(ctx: &FrameContext, table_id: TableID, page_id: PageID) {
    assert_eq!(
        ctx.expect_vmap().inspect_state(),
        RowPageState::Frozen,
        "optimistic preparation requires frozen page: table_id={table_id}, page_id={page_id}"
    );
}

#[inline]
fn record_frozen_page_readiness(
    first_delay: &mut Option<FrozenPageValidationDelay>,
    stable_page_count: &mut usize,
    page_id: PageID,
    validation: FrozenPageValidationState,
    cutoff_ts: TrxID,
) {
    // Stable means the source image is proven, but it is usable only after the
    // exclusive cutoff passes every committed image CTS found by analysis.
    let ready = match validation {
        FrozenPageValidationState::Unchecked => unreachable!("page was not analyzed"),
        FrozenPageValidationState::Blocked { .. } => false,
        FrozenPageValidationState::Stable { required_cutoff_ts } => {
            required_cutoff_ts.is_none_or(|required| required <= cutoff_ts)
        }
    };
    // Count the ready prefix preceding the first canonical unsafe page so the
    // incremental caller can return the existing structured delay.
    if first_delay.is_none() {
        if ready {
            *stable_page_count += 1;
        } else {
            *first_delay = Some(frozen_page_delay(page_id, *stable_page_count, validation));
        }
    }
}

fn scan_frozen_page(
    page: &RowPage,
    ctx: &FrameContext,
    page_info: FrozenPage,
    mode: FrozenPageScanMode,
    cutoff_ts: TrxID,
) -> FrozenPageScan {
    #[cfg(test)]
    use super::test_hooks;

    #[cfg(test)]
    test_hooks::run_test_frozen_page_scan_hook(page_info.page_id);
    let row_count = page.header.row_count();
    // Start from the current physical image. Undo inspection below only
    // adjusts rows whose cutoff visibility differs from that latest image.
    let mut del_bitmap = page.del_bitmap(row_count);
    let map = ctx.expect_vmap();
    let mut required_cutoff_ts = None;
    let mut blocked = false;
    let mut blockers = Vec::new();
    let mut overlay_markers = Vec::new();
    for row_idx in 0..row_count {
        let undo_guard = map.read_latch(row_idx);
        let Some(head) = undo_guard.as_ref() else {
            drop(undo_guard);
            #[cfg(test)]
            test_hooks::run_test_frozen_page_row_scan_hook(page_info.page_id, row_idx);
            continue;
        };
        // Walk the main branch from newest to oldest. Leading locks and deletes
        // describe ownership/visibility, but the first Insert or Update is the
        // image boundary that proves whether this row can be checkpointed.
        let mut status = &head.next.main.status;
        let mut entry = head.next.main.entry.as_ref();
        let mut marker = None;
        let mut delete_seen = false;
        loop {
            let ts = status.ts();
            let committed = trx_is_committed(ts);
            match entry.kind {
                RowUndoKind::Lock => {
                    // Initial readiness blocks ownership that may predate the
                    // freeze observation. After readiness is established, any
                    // newly seen lock acquired Frozen state and is representable
                    // as the first cold ownership marker candidate.
                    if !committed && mode.unresolved_ownership_blocks(ts) {
                        blocked = true;
                        push_blocker(&mut blockers, status);
                    }
                    if marker.is_none()
                        && !committed
                        && let UndoStatus::Ref(shared) = status
                    {
                        marker = Some(DeleteMarker::Ref(Arc::clone(shared)));
                    }
                }
                RowUndoKind::Delete => {
                    // Like Lock, pre-fence ownership blocks only initial
                    // readiness. Stable-plan refresh represents later deletes.
                    // The newest Delete alone determines cutoff visibility:
                    // committed-before-cutoff stays deleted, while a future or
                    // unresolved delete revives the row in the prepared view.
                    if !committed && mode.unresolved_ownership_blocks(ts) {
                        blocked = true;
                        push_blocker(&mut blockers, status);
                    }
                    if !delete_seen {
                        if committed && ts < cutoff_ts {
                            del_bitmap.bitmap_set(row_idx);
                        } else {
                            del_bitmap.bitmap_unset(row_idx);
                        }
                        delete_seen = true;
                    }
                    // Preserve only the first marker needed after hot routing is
                    // removed. Older entries are still visited to prove the
                    // source image, not to install competing overlays.
                    if marker.is_none() {
                        marker = if committed && ts >= cutoff_ts {
                            Some(DeleteMarker::Committed(ts))
                        } else if !committed {
                            match status {
                                UndoStatus::Ref(shared) => {
                                    Some(DeleteMarker::Ref(Arc::clone(shared)))
                                }
                                UndoStatus::Committed(_) => None,
                            }
                        } else {
                            None
                        };
                    }
                }
                RowUndoKind::Insert | RowUndoKind::Update(_) => {
                    // Insert/Update supplies the physical row image. Its commit
                    // must be strictly below the exclusive checkpoint cutoff;
                    // an unresolved image cannot produce a transition plan.
                    if committed {
                        let required = ts.saturating_add(1);
                        required_cutoff_ts = Some(
                            required_cutoff_ts
                                .map_or(required, |current: TrxID| current.max(required)),
                        );
                    } else {
                        blocked = true;
                        push_blocker(&mut blockers, status);
                    }
                    if !delete_seen && committed && ts < cutoff_ts {
                        del_bitmap.bitmap_unset(row_idx);
                    }
                    break;
                }
            }
            // Locks and deletes do not prove the image, so continue until the
            // first image-producing entry or the end of retained history.
            let Some(next) = entry.next.as_ref() else {
                break;
            };
            status = &next.main.status;
            entry = next.main.entry.as_ref();
        }
        if let Some(marker) = marker {
            overlay_markers.push((page.row_id(row_idx), marker));
        }
        drop(undo_guard);
        #[cfg(test)]
        test_hooks::run_test_frozen_page_row_scan_hook(page_info.page_id, row_idx);
    }
    FrozenPageScan {
        page_info,
        required_cutoff_ts,
        blocked,
        blockers,
        del_bitmap,
        overlay_markers,
    }
}

#[inline]
fn push_blocker(blockers: &mut Vec<Arc<SharedTrxStatus>>, status: &UndoStatus) {
    let UndoStatus::Ref(shared) = status else {
        return;
    };
    if blockers.iter().any(|blocker| Arc::ptr_eq(blocker, shared)) {
        return;
    }
    blockers.push(Arc::clone(shared));
}

#[inline]
fn frozen_page_delay(
    page_id: PageID,
    stable_page_count: usize,
    validation: FrozenPageValidationState,
) -> FrozenPageValidationDelay {
    match validation {
        FrozenPageValidationState::Unchecked => unreachable!("page was not analyzed"),
        FrozenPageValidationState::Blocked { required_cutoff_ts } => FrozenPageValidationDelay {
            page_id,
            stable_page_count,
            required_cutoff_ts,
            unresolved_status: true,
        },
        FrozenPageValidationState::Stable { required_cutoff_ts } => FrozenPageValidationDelay {
            page_id,
            stable_page_count,
            required_cutoff_ts,
            unresolved_status: false,
        },
    }
}

#[inline]
fn active_writer_sts(trx_id: TrxID) -> TrxID {
    debug_assert!(!trx_is_committed(trx_id));
    TrxID::new(trx_id.as_u64() & (MAX_SNAPSHOT_TS.as_u64() - 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmap::Bitmap;
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
    use crate::id::RowID;
    use crate::table::test_hooks;
    use crate::trx::row::RowWriteAccess;
    use crate::trx::undo::{
        MainBranch, NextRowUndo, OwnedRowUndo, RowUndoHead, RowUndoKind, UndoStatus,
    };
    use crate::trx::ver_map::{RowPageState, RowVersionMap};
    use crate::trx::{MIN_ACTIVE_TRX_ID, MIN_SNAPSHOT_TS, SharedTrxStatus};
    use crate::value::{Val, ValKind};
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::thread;

    struct FrozenAnalyzerFixture {
        page: RowPage,
        ctx: FrameContext,
        page_info: FrozenPage,
        _undo_owners: Vec<OwnedRowUndo>,
    }

    fn frozen_analyzer_fixture(
        nodes: Vec<(RowUndoKind, UndoStatus)>,
        latest_deleted: bool,
    ) -> FrozenAnalyzerFixture {
        assert!(!nodes.is_empty());
        let metadata = TableMetadata::try_new(
            vec![ColumnSpec::new(
                "id",
                ValKind::I32,
                ColumnAttributes::empty(),
            )],
            vec![],
        )
        .unwrap();
        let mut page = RowPage::new_test_page();
        page.init(RowID::new(100), 4, metadata.col.as_ref());
        assert!(
            page.insert(metadata.col.as_ref(), &[Val::from(1i32)])
                .is_ok()
        );
        if latest_deleted {
            assert!(page.set_deleted(0, true));
        }

        let mut nodes = nodes
            .into_iter()
            .map(|(kind, status)| {
                (
                    OwnedRowUndo::new(TableID::new(1), None, RowID::new(100), kind),
                    Some(status),
                )
            })
            .collect::<Vec<_>>();
        for idx in 0..nodes.len() - 1 {
            let next_entry = nodes[idx + 1].0.leak();
            let next_status = nodes[idx + 1].1.take().unwrap();
            nodes[idx].0.next = Some(NextRowUndo::new(MainBranch {
                entry: next_entry,
                status: next_status,
            }));
        }
        let head_status = nodes[0].1.take().unwrap();
        let head_entry = nodes[0].0.leak();
        let row_ver = RowVersionMap::new(Arc::clone(&metadata.col), 4);
        *row_ver.write_state() = RowPageState::Frozen;
        *row_ver.write_latch(0) = Some(Box::new(RowUndoHead {
            next: NextRowUndo::new(MainBranch {
                entry: head_entry,
                status: head_status,
            }),
            purge_ts: MIN_SNAPSHOT_TS,
        }));
        let page_info = FrozenPage {
            page_id: PageID::new(7),
            start_row_id: RowID::new(100),
            end_row_id: RowID::new(101),
        };
        FrozenAnalyzerFixture {
            page,
            ctx: FrameContext::RowVerMap(row_ver),
            page_info,
            _undo_owners: nodes.into_iter().map(|(undo, _)| undo).collect(),
        }
    }

    fn run_frozen_analyzer(
        fixture: &FrozenAnalyzerFixture,
        frozen_ts: TrxID,
        cutoff_ts: TrxID,
    ) -> FrozenPageReadinessAnalysis {
        let observed_version = fixture.ctx.expect_vmap().frozen_mutation_version();
        scan_frozen_page(
            &fixture.page,
            &fixture.ctx,
            fixture.page_info,
            FrozenPageScanMode::EstablishReadiness { frozen_ts },
            cutoff_ts,
        )
        .into_readiness_analysis(cutoff_ts, observed_version)
    }

    fn run_stable_frozen_analyzer(
        fixture: &FrozenAnalyzerFixture,
        cutoff_ts: TrxID,
    ) -> Option<PreparedTransitionPage> {
        let observed_version = fixture.ctx.expect_vmap().frozen_mutation_version();
        scan_frozen_page(
            &fixture.page,
            &fixture.ctx,
            fixture.page_info,
            FrozenPageScanMode::RefreshStablePlan,
            cutoff_ts,
        )
        .into_plan(cutoff_ts, observed_version)
    }

    #[test]
    fn test_frozen_mutation_finishing_during_analysis_discards_optimistic_plan() {
        let metadata = TableMetadata::try_new(
            vec![ColumnSpec::new(
                "id",
                ValKind::I32,
                ColumnAttributes::empty(),
            )],
            vec![],
        )
        .unwrap();
        let mut page = RowPage::new_test_page();
        page.init(RowID::new(100), 4, metadata.col.as_ref());
        assert!(
            page.insert(metadata.col.as_ref(), &[Val::from(1i32)])
                .is_ok()
        );
        assert!(
            page.insert(metadata.col.as_ref(), &[Val::from(2i32)])
                .is_ok()
        );
        let row_ver = RowVersionMap::new(Arc::clone(&metadata.col), 4);
        *row_ver.write_state() = RowPageState::Frozen;
        let ctx = FrameContext::RowVerMap(row_ver);
        let page_info = FrozenPage {
            page_id: PageID::new(8),
            start_row_id: RowID::new(100),
            end_row_id: RowID::new(102),
        };
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);

        thread::scope(|scope| {
            let writer = scope.spawn(|| {
                let dirty = AtomicBool::new(false);
                let mut access = RowWriteAccess::new(&page, &ctx, &dirty, 1);
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                access.delete_row();
            });
            entered_rx.recv().unwrap();
            let version_before = ctx.expect_vmap().frozen_mutation_version();
            assert_eq!(version_before, 1);
            test_hooks::set_test_frozen_page_scan_hook(move |_| {
                release_tx.send(()).unwrap();
            });
            let analysis = scan_frozen_page(
                &page,
                &ctx,
                page_info,
                FrozenPageScanMode::EstablishReadiness {
                    frozen_ts: TrxID::new(20),
                },
                TrxID::new(20),
            )
            .into_readiness_analysis(TrxID::new(20), version_before);
            writer.join().unwrap();
            let version_after = ctx.expect_vmap().frozen_mutation_version();
            assert_eq!(version_after, 2);
            assert!(analysis.plan.is_some());
            let retained_plan = (version_before == version_after).then_some(analysis.plan);
            assert!(retained_plan.is_none());
        });
    }

    #[test]
    fn test_frozen_analyzer_blocks_unresolved_image_and_pre_fence_ownership() {
        let frozen_ts = TrxID::new(20);
        let cutoff_ts = TrxID::new(50);
        let cases = [
            (RowUndoKind::Lock, MIN_ACTIVE_TRX_ID + 10, false),
            (RowUndoKind::Delete, MIN_ACTIVE_TRX_ID + 10, true),
            (RowUndoKind::Insert, MIN_ACTIVE_TRX_ID + 30, false),
            (RowUndoKind::Update(vec![]), MIN_ACTIVE_TRX_ID + 30, false),
        ];

        for (kind, active_ts, deleted) in cases {
            let fixture = frozen_analyzer_fixture(
                vec![(
                    kind,
                    UndoStatus::Ref(Arc::new(SharedTrxStatus::new(active_ts))),
                )],
                deleted,
            );
            let analysis = run_frozen_analyzer(&fixture, frozen_ts, cutoff_ts);
            assert!(matches!(
                analysis.validation,
                FrozenPageValidationState::Blocked { .. }
            ));
            assert!(analysis.plan.is_none());
        }
    }

    #[test]
    fn test_stable_frozen_analyzer_represents_pre_fence_ownership() {
        let cutoff_ts = TrxID::new(50);
        for (kind, latest_deleted) in [(RowUndoKind::Lock, false), (RowUndoKind::Delete, true)] {
            let status = Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 10));
            let fixture = frozen_analyzer_fixture(
                vec![
                    (kind, UndoStatus::Ref(Arc::clone(&status))),
                    (RowUndoKind::Insert, UndoStatus::Committed(TrxID::new(5))),
                ],
                latest_deleted,
            );
            let plan = run_stable_frozen_analyzer(&fixture, cutoff_ts).unwrap();
            assert!(!plan.del_bitmap.bitmap_get(0));
            let [(_, DeleteMarker::Ref(marker_status))] = plan.overlay_markers.as_slice() else {
                panic!("stable ownership must produce one shared-status marker");
            };
            assert!(Arc::ptr_eq(marker_status, &status));
        }
    }

    #[test]
    fn test_stable_frozen_analyzer_still_rejects_unresolved_image() {
        for kind in [RowUndoKind::Insert, RowUndoKind::Update(vec![])] {
            let fixture = frozen_analyzer_fixture(
                vec![(
                    kind,
                    UndoStatus::Ref(Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 10))),
                )],
                false,
            );
            assert!(run_stable_frozen_analyzer(&fixture, TrxID::new(50)).is_none());
        }
    }

    #[test]
    fn test_frozen_analyzer_retains_status_ref_across_commit_after_preparation() {
        let status = Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 30));
        let fixture = frozen_analyzer_fixture(
            vec![
                (RowUndoKind::Delete, UndoStatus::Ref(Arc::clone(&status))),
                (RowUndoKind::Insert, UndoStatus::Committed(TrxID::new(5))),
            ],
            true,
        );
        let plan = run_frozen_analyzer(&fixture, TrxID::new(20), TrxID::new(20))
            .plan
            .unwrap();
        assert!(!plan.del_bitmap.bitmap_get(0));
        let [(row_id, DeleteMarker::Ref(marker_status))] = plan.overlay_markers.as_slice() else {
            panic!("expected one unresolved delete marker");
        };
        assert_eq!(*row_id, RowID::new(100));
        assert!(Arc::ptr_eq(marker_status, &status));

        status.commit_for_test(TrxID::new(25));
        assert_eq!(marker_status.ts(), TrxID::new(25));
    }

    #[test]
    fn test_frozen_analyzer_leading_lock_delete_chain_selects_first_marker() {
        let lock_status = Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 30));
        let fixture = frozen_analyzer_fixture(
            vec![
                (RowUndoKind::Lock, UndoStatus::Ref(Arc::clone(&lock_status))),
                (RowUndoKind::Delete, UndoStatus::Committed(TrxID::new(25))),
                (RowUndoKind::Insert, UndoStatus::Committed(TrxID::new(5))),
            ],
            true,
        );
        let plan = run_frozen_analyzer(&fixture, TrxID::new(20), TrxID::new(20))
            .plan
            .unwrap();
        assert!(!plan.del_bitmap.bitmap_get(0));
        let [(_, DeleteMarker::Ref(marker_status))] = plan.overlay_markers.as_slice() else {
            panic!("leading unresolved lock should supply the first marker");
        };
        assert!(Arc::ptr_eq(marker_status, &lock_status));

        let delete_status = Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 31));
        let fixture = frozen_analyzer_fixture(
            vec![
                (RowUndoKind::Lock, UndoStatus::Committed(TrxID::new(30))),
                (
                    RowUndoKind::Delete,
                    UndoStatus::Ref(Arc::clone(&delete_status)),
                ),
                (RowUndoKind::Insert, UndoStatus::Committed(TrxID::new(5))),
            ],
            true,
        );
        let plan = run_frozen_analyzer(&fixture, TrxID::new(20), TrxID::new(20))
            .plan
            .unwrap();
        assert!(!plan.del_bitmap.bitmap_get(0));
        let [(_, DeleteMarker::Ref(marker_status))] = plan.overlay_markers.as_slice() else {
            panic!("unresolved delete should supply the first marker");
        };
        assert!(Arc::ptr_eq(marker_status, &delete_status));
    }
}
