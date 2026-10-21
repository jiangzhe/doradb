//! Immutable checkpoint-selected row sources and admitted per-index extraction.
use super::checkpoint_workflow::PreparedTransitionPage;
use super::{Table, TableRuntimeLayout};
use crate::bitmap::Bitmap;
use crate::buffer::PoolGuards;
use crate::buffer::guard::PageGuard;
use crate::catalog::IndexRef;
use crate::catalog::table::TableIndexMetadata;
use crate::error::{
    DataIntegrityError, MultiDomainResultExt, RuntimeError, RuntimeOrFatalError,
    RuntimeOrFatalResult,
};
use crate::id::{PageID, RowID, TrxID};
use crate::index::build::checkpoint::CheckpointAdmission;
use crate::index::build::disk_builder::ColdBuildPolicy;
use crate::index::build::local_sort::LocalSortJobs;
use crate::index::build::merge::execution_error;
use crate::index::build::{
    BudgetedVec, BuildEntries, BuildOutcome, DuplicateCheck, IndexBuildEntry, MemoryBudget,
    PAYLOAD_CREDIT_BYTES, RunKind, SortedRun, SortedRuns,
};
use crate::index::{BTreeKeyEncoder, ColumnLeafEntry};
use crate::memcmp::MEM_CMP_KEY_INLINE;
use crate::quiescent::QuiescentGuard;
use crate::runtime::{thread_pool::ThreadPool, yield_now};
use crate::value::{Val, ValKind};
use error_stack::{Report, ResultExt};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(feature = "profiling")]
use crate::profiling::clock::Instant;

struct PutPage {
    page: PageID,
    start: RowID,
    end: RowID,
    deleted: BudgetedVec<u64>,
}

struct DeleteBlock {
    entry: ColumnLeafEntry,
    rows: BudgetedVec<(u16, RowID)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceGroup {
    kind: RunKind,
    range: Range<usize>,
}

struct ExtractedGroup {
    source: Arc<CheckpointIndexSources>,
    index: IndexRef,
    group: SourceGroup,
    run: Arc<SortedRun>,
}

type ExtractionResult = RuntimeOrFatalResult<BuildOutcome<ExtractedGroup>>;

/// One checkpoint's fixed selection, shared by sequential index consumers.
pub(super) struct CheckpointIndexSources {
    puts: BudgetedVec<PutPage>,
    deletes: BudgetedVec<DeleteBlock>,
    /// Shared scratch follows sources, keys, planning, and output ownership.
    pub(super) budget: MemoryBudget,
    // Held before source growth so resident input cannot consume output progress.
    pub(super) admission: Arc<CheckpointAdmission>,
    cutoff: TrxID,
}

impl CheckpointIndexSources {
    /// Protect downstream progress before capturing any index source.
    pub(super) fn new(policy: ColdBuildPolicy, cutoff: TrxID) -> RuntimeOrFatalResult<Self> {
        let budget = MemoryBudget::new(policy.max_scratch_bytes);
        let admission = CheckpointAdmission::new(&budget, policy)?;
        Ok(Self {
            puts: BudgetedVec::new(&budget),
            deletes: BudgetedVec::new(&budget),
            budget,
            admission,
            cutoff,
        })
    }

    /// Copy only immutable page identity and the authoritative prepared bitmap.
    pub(super) fn capture_puts(
        &mut self,
        pages: &[Option<PreparedTransitionPage>],
    ) -> RuntimeOrFatalResult<()> {
        self.puts
            .ensure_capacity(pages.len(), "checkpoint put descriptors")
            .change_context(RuntimeError::CheckpointExecution)?;
        for page in pages {
            let page = page
                .as_ref()
                .ok_or_else(|| execution_error("checkpoint put source lacks a prepared page"))?;
            if page.cutoff_ts != self.cutoff
                || self
                    .puts
                    .last()
                    .is_some_and(|previous| previous.end > page.start_row_id)
            {
                return Err(execution_error(
                    "checkpoint put source has foreign cutoff or overlapping pages",
                ));
            }
            let mut deleted = BudgetedVec::new(&self.budget);
            deleted
                .ensure_capacity(page.del_bitmap.len(), "checkpoint put bitmap")
                .change_context(RuntimeError::CheckpointExecution)?;
            for &word in &page.del_bitmap {
                deleted.push_reserved(word);
            }
            self.puts.push_reserved(PutPage {
                page: page.page_id,
                start: page.start_row_id,
                end: page.end_row_id,
                deleted,
            });
        }
        Ok(())
    }

    /// Retain resolved identities; extraction validates the LWC binding before decoding keys.
    pub(super) fn capture_deletes(
        &mut self,
        entry: ColumnLeafEntry,
        selected: &[(u16, RowID)],
    ) -> RuntimeOrFatalResult<()> {
        self.deletes
            .reserve_one("checkpoint delete descriptors")
            .change_context(RuntimeError::CheckpointExecution)?;
        let mut rows = BudgetedVec::new(&self.budget);
        rows.ensure_capacity(selected.len(), "checkpoint delete ordinals")
            .change_context(RuntimeError::CheckpointExecution)?;
        // The coordinator establishes exact membership once. Check descriptor
        // bounds and order here without repeating rank/select on the identity.
        for &(ordinal, row_id) in selected {
            if ordinal >= entry.row_count()
                || row_id < entry.start_row_id
                || row_id >= entry.end_row_id()
                || rows.last().is_some_and(|&(previous, previous_id)| {
                    previous >= ordinal || previous_id >= row_id
                })
            {
                return Err(execution_error(
                    "checkpoint delete identities exceed bounds or are not strictly ordered",
                ));
            }
            rows.push_reserved((ordinal, row_id));
        }
        self.deletes.push_reserved(DeleteBlock { entry, rows });
        Ok(())
    }

    /// Whether the fixed checkpoint selection has no row sources.
    pub(super) fn is_empty(&self) -> bool {
        self.puts.is_empty() && self.deletes.is_empty()
    }

    /// Extract one index's resident input, draining every accepted worker on failure.
    /// Pool admission accepts each finite job. Its supervised completion is the
    /// authoritative result during poison and shutdown. The accepted checkpoint
    /// owns this future and cleanup; observer detachment cannot cancel it.
    pub(super) async fn extract(
        self: &Arc<Self>,
        table: Arc<Table>,
        layout: Arc<TableRuntimeLayout>,
        guards: PoolGuards,
        index: IndexRef,
        pool: &QuiescentGuard<ThreadPool>,
        workers: usize,
    ) -> RuntimeOrFatalResult<SortedRuns> {
        #[cfg(feature = "profiling")]
        let started = Instant::now();
        let encoder = layout
            .expect_secondary_index(index)
            .disk_runtime()
            .key_encoder_arc();
        let source = Arc::new(IndexSource {
            source: self.clone(),
            table,
            layout,
            guards,
            index,
            encoder,
        });
        let mut groups = BudgetedVec::new(&self.budget);
        for (kind, len) in [
            (RunKind::Put, self.puts.len()),
            (RunKind::Delete, self.deletes.len()),
        ] {
            let count = len.min(workers * 4);
            for group in 0..count {
                groups
                    .push(
                        SourceGroup {
                            kind,
                            range: group * len / count..(group + 1) * len / count,
                        },
                        "checkpoint source groups",
                    )
                    .change_context(RuntimeError::CheckpointExecution)?;
            }
        }
        let mut jobs = LocalSortJobs::new(
            &self.budget,
            groups.len(),
            workers,
            "checkpoint extraction ledger",
        )
        .change_context(RuntimeError::CheckpointExecution)?;
        let mut runs = BudgetedVec::new(&self.budget);
        runs.ensure_capacity(groups.len(), "checkpoint run owners")
            .change_context(RuntimeError::CheckpointExecution)?;
        let stop = Arc::new(AtomicBool::new(false));
        let mut failure: Option<RuntimeOrFatalError> = None;
        while jobs.collected() < groups.len() {
            while failure.is_none() && jobs.can_submit() && !stop.load(Ordering::Acquire) {
                let source = source.clone();
                let group = groups[jobs.submitted()].clone();
                let stop = stop.clone();
                let id = jobs.submitted();
                jobs.submit(pool, async move {
                    let result = source.extract_group(id, group, &stop).await;
                    if result.is_err() {
                        stop.store(true, Ordering::Release);
                    }
                    result
                });
            }
            if !jobs.pending() {
                break;
            }
            let collected = jobs.collected();
            let result = jobs.collect().await
                .map_err(|bridge| bridge.into_runtime_or_fatal(RuntimeError::CheckpointExecution))
                .and_then(|result| result)
                .attach_with(|| format!("operation=checkpoint_index_extract, index={index}, group={collected}, cutoff={}", self.cutoff));
            let result = result.and_then(|result| match result {
                BuildOutcome::Complete(result) => {
                    if !Arc::ptr_eq(self, &result.source)
                        || result.index != index
                        || result.group != groups[collected]
                        || result.run.group_id != collected
                        || result.run.kind != result.group.kind
                    {
                        return Err(execution_error(
                            "checkpoint extraction completion has foreign provenance",
                        ));
                    }
                    if !result.run.entries().is_empty() {
                        runs.push_reserved(result.run);
                    }
                    Ok(())
                }
                BuildOutcome::Stopped if stop.load(Ordering::Acquire) => Ok(()),
                BuildOutcome::Stopped => Err(execution_error(
                    "checkpoint extraction stopped without failure",
                )),
            });
            if let Err(error) = result {
                stop.store(true, Ordering::Release);
                failure = Some(match failure.take() {
                    Some(primary) => primary.merge_cleanup(error),
                    None => error,
                });
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        if jobs.collected() != groups.len() {
            return Err(execution_error("checkpoint source coverage is incomplete"));
        }
        #[cfg(feature = "profiling")]
        self.admission
            .measure(|m| m.extraction_nanos += started.elapsed().as_nanos() as u64);
        Ok(SortedRuns::from_admitted(
            runs,
            self.budget.clone(),
            DuplicateCheck::Skip,
        ))
    }
}

struct IndexSource {
    source: Arc<CheckpointIndexSources>,
    table: Arc<Table>,
    layout: Arc<TableRuntimeLayout>,
    guards: PoolGuards,
    index: IndexRef,
    encoder: Arc<BTreeKeyEncoder>,
}

impl IndexSource {
    async fn extract_group(
        self: &Arc<Self>,
        id: usize,
        group: SourceGroup,
        stop: &AtomicBool,
    ) -> ExtractionResult {
        let spec = self.layout.metadata().idx.expect_index_spec(self.index);
        if spec.unique() {
            self.extract_mutations::<true>(spec, id, group, stop).await
        } else {
            self.extract_mutations::<false>(spec, id, group, stop).await
        }
    }

    async fn extract_mutations<const UNIQUE: bool>(
        self: &Arc<Self>,
        spec: &TableIndexMetadata,
        id: usize,
        group: SourceGroup,
        stop: &AtomicBool,
    ) -> ExtractionResult {
        let budget = &self.source.budget;
        let columns = &self.layout.metadata().col;
        // Row-page variable payload lengths are u16. LWC keeps those original
        // values. Charge their maximal simultaneous decoded ownership before
        // decoding; pool-owned backing pages are accounted separately.
        let payload_bytes = spec
            .keys
            .iter()
            .filter(|key| columns.val_kind(key.column_ordinal.as_usize()) == ValKind::VarByte)
            .count()
            .checked_mul(usize::from(u16::MAX))
            .ok_or_else(|| execution_error("checkpoint projection size overflow"))?;
        let decode = budget
            .reserve(payload_bytes, "checkpoint decode payloads")
            .change_context(RuntimeError::CheckpointExecution)?;
        // Declare the projection after decode admission so its owned values
        // are destroyed first on every return, including partial-row failures.
        let mut projection = BudgetedVec::new(budget);
        projection
            .ensure_capacity(
                spec.keys.len() + usize::from(!UNIQUE),
                "checkpoint projection",
            )
            .change_context(RuntimeError::CheckpointExecution)?;
        let mut rows = BuildEntries::with_payload_credit(budget, PAYLOAD_CREDIT_BYTES);
        for offset in group.range.clone() {
            if stop.load(Ordering::Acquire) {
                return Ok(BuildOutcome::Stopped);
            }
            match group.kind {
                RunKind::Put => {
                    let source = &self.source.puts[offset];
                    let guard = self
                        .table
                        .row_store
                        .must_get_row_page_shared(&self.guards, source.page)
                        .await?;
                    let page = guard.page();
                    if page.header.start_row_id != source.start
                        || page.header.row_count() > source.deleted.len() * 64
                    {
                        return Err(execution_error("checkpoint put page identity changed"));
                    }
                    for row in 0..page.header.row_count() {
                        if source.deleted.bitmap_get(row) {
                            continue;
                        }
                        for key in &spec.keys {
                            projection.push_reserved(page.val(
                                columns,
                                row,
                                key.column_ordinal.as_usize(),
                            ));
                        }
                        Self::encode::<UNIQUE>(
                            &self.encoder,
                            &mut projection,
                            page.row_id(row),
                            &mut rows,
                        )?;
                    }
                }
                RunKind::Delete => {
                    let source = &self.source.deletes[offset];
                    let persisted = self
                        .table
                        .storage
                        .load_lwc_block(self.guards.disk_guard(), source.entry.block_id())
                        .await?;
                    let block = persisted.block();
                    if block.row_count() != usize::from(source.entry.row_count())
                        || block.block_binding_value() != source.entry.block_binding_value()
                    {
                        return Err(Report::new(DataIntegrityError::InvalidPayload)
                            .attach(format!(
                                "LWC block metadata mismatch: block_id={}, block_row_count={}, expected_row_count={}, block_binding_value={}, expected_binding_value={}",
                                source.entry.block_id(), block.row_count(), source.entry.row_count(),
                                block.block_binding_value(), source.entry.block_binding_value()
                            ))
                            .change_context(RuntimeError::CheckpointExecution)
                            .attach(format!("operation=extract_checkpoint_deletes, table_id={}", self.table.table_id()))
                            .into());
                    }
                    for &(ordinal, row_id) in &*source.rows {
                        for key in &spec.keys {
                            projection.push_reserved(
                                block
                                    .decode_value(
                                        columns,
                                        usize::from(ordinal),
                                        key.column_ordinal.as_usize(),
                                    )
                                    .change_context(RuntimeError::CheckpointExecution)?,
                            );
                        }
                        Self::encode::<UNIQUE>(&self.encoder, &mut projection, row_id, &mut rows)?;
                    }
                }
                RunKind::Create => unreachable!("checkpoint source group has a mutation kind"),
            }
            yield_now().await;
        }
        drop(projection);
        drop(decode);
        rows.trim_payload();
        match SortedRun::finish_mutations(rows, id, group.kind, UNIQUE, stop)? {
            BuildOutcome::Stopped => Ok(BuildOutcome::Stopped),
            BuildOutcome::Complete(run) => Ok(BuildOutcome::Complete(ExtractedGroup {
                source: self.source.clone(),
                index: self.index,
                group,
                run: Arc::new(run),
            })),
        }
    }

    #[inline]
    fn encode<const UNIQUE: bool>(
        encoder: &BTreeKeyEncoder,
        values: &mut BudgetedVec<Val>,
        row_id: RowID,
        rows: &mut BuildEntries,
    ) -> RuntimeOrFatalResult<()> {
        if !UNIQUE {
            values.push_reserved(Val::from(row_id));
        }
        let len = encoder
            .encoded_len(values)
            .change_context(RuntimeError::CheckpointExecution)?;
        rows.push_with(
            if len > MEM_CMP_KEY_INLINE { len } else { 0 },
            "checkpoint run entries",
            "checkpoint outlined key",
            || IndexBuildEntry {
                key: encoder.encode_with_len(values, len),
                row_id,
            },
        )
        .change_context(RuntimeError::CheckpointExecution)?;
        values.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf::ColdIndexBuildConfig;
    use crate::error::ResourceError;
    use crate::index::build::{fail_build_budget, payload_accounting};
    use crate::session::tests::SessionTestExt;
    use crate::value::ValType;

    struct Encoding {
        encoder: BTreeKeyEncoder,
        projection: BudgetedVec<Val>,
        rows: BuildEntries,
    }

    impl Encoding {
        fn new(budget: &MemoryBudget, types: &[ValType]) -> Self {
            let mut projection = BudgetedVec::new(budget);
            projection
                .ensure_capacity(types.len(), "test projection")
                .unwrap();
            Self {
                encoder: BTreeKeyEncoder::new(types.iter().copied()),
                projection,
                rows: BuildEntries::with_payload_credit(budget, PAYLOAD_CREDIT_BYTES),
            }
        }

        fn push<const UNIQUE: bool>(
            &mut self,
            values: &[Val],
            row: u64,
        ) -> RuntimeOrFatalResult<()> {
            for value in values {
                self.projection.push_reserved(value.clone());
            }
            IndexSource::encode::<UNIQUE>(
                &self.encoder,
                &mut self.projection,
                RowID::new(row),
                &mut self.rows,
            )
        }
    }

    fn sources() -> CheckpointIndexSources {
        CheckpointIndexSources::new(
            ColdBuildPolicy::new(ColdIndexBuildConfig::default(), 2).unwrap(),
            TrxID::new(10),
        )
        .unwrap()
    }

    fn prepared() -> PreparedTransitionPage {
        PreparedTransitionPage {
            page_id: PageID::new(1),
            start_row_id: RowID::new(0),
            end_row_id: RowID::new(64),
            cutoff_ts: TrxID::new(10),
            observed_version: 1,
            required_cutoff_ts: None,
            del_bitmap: vec![5],
            overlay_markers: Vec::new(),
        }
    }

    fn check_encoding_mode<const UNIQUE: bool>(types: &[ValType], values: &[Vec<Val>]) {
        let budget = MemoryBudget::new(1024 * 1024);
        let mut types = types.to_vec();
        if !UNIQUE {
            types.push(ValType::new(ValKind::U64, false));
        }
        let mut encoding = Encoding::new(&budget, &types);
        let mut expected = Vec::new();
        for (position, values) in values.iter().enumerate() {
            let row = RowID::new(position as u64 + 10);
            let mut physical = values.clone();
            if !UNIQUE {
                physical.push(Val::from(row));
            }
            expected.push((encoding.encoder.encode(&physical), row));
            encoding
                .push::<UNIQUE>(values, position as u64 + 10)
                .unwrap();
            assert!(encoding.projection.is_empty());
        }
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        let payload: usize = expected
            .iter()
            .map(|(key, _)| key.as_bytes().len())
            .filter(|&len| len > MEM_CMP_KEY_INLINE)
            .sum();
        assert_eq!(payload_accounting(&encoding.rows).0, payload);
        if payload == 0 {
            assert_eq!(payload_accounting(&encoding.rows).1, 0);
        }
        let Encoding {
            rows, projection, ..
        } = encoding;
        let BuildOutcome::Complete(run) =
            SortedRun::finish_mutations(rows, 0, RunKind::Put, UNIQUE, &AtomicBool::new(false))
                .unwrap()
        else {
            panic!("encoding fixture stopped");
        };
        let actual: Vec<_> = run
            .entries()
            .iter()
            .map(|entry| (&entry.key, entry.row_id))
            .collect();
        assert_eq!(
            actual,
            expected
                .iter()
                .map(|(key, row)| (key, *row))
                .collect::<Vec<_>>()
        );
        drop(run);
        drop(projection);
        assert_eq!(budget.used(), 0);
    }

    /// Purpose: Preserve physical key shape and stable duplicate-put order through specialized encoding.
    /// Expected: Both index modes match independently projected keys for inline, outlined, nullable and composite inputs.
    #[test]
    fn specialized_encoding_preserves_keys_and_order() {
        for (types, values) in [
            (
                vec![ValType::new(ValKind::U64, false)],
                vec![
                    vec![Val::from(7u64)],
                    vec![Val::from(7u64)],
                    vec![Val::from(3u64)],
                ],
            ),
            (
                vec![ValType::new(ValKind::VarByte, true)],
                vec![
                    vec![Val::from(vec![7u8; 128])],
                    vec![Val::from(vec![7u8; 128])],
                    vec![Val::Null],
                ],
            ),
            (
                vec![
                    ValType::new(ValKind::VarByte, false),
                    ValType::new(ValKind::U64, false),
                ],
                vec![
                    vec![Val::from(vec![7u8; 128]), Val::from(9u64)],
                    vec![Val::from(vec![7u8; 128]), Val::from(9u64)],
                    vec![Val::from(vec![3u8; 128]), Val::from(8u64)],
                ],
            ),
        ] {
            check_encoding_mode::<true>(&types, &values);
            check_encoding_mode::<false>(&types, &values);
        }
    }

    /// Purpose: Consume already admitted payload credit after further shared admission is disabled.
    /// Expected: Keys within the credit remain encodable, the first refill fails without appending a key, and cleanup releases all charges.
    #[test]
    fn encoding_reuses_credit_until_refill() {
        let budget = MemoryBudget::new(1024 * 1024);
        let mut encoding = Encoding::new(&budget, &[ValType::new(ValKind::VarByte, false)]);
        encoding.rows.ensure_capacity(128, "test entries").unwrap();
        let values = [Val::from(vec![7u8; 1024])];
        encoding.push::<true>(&values, 0).unwrap();
        let bytes = encoding.rows[0].key.as_bytes().len();
        assert_eq!(payload_accounting(&encoding.rows).1, PAYLOAD_CREDIT_BYTES);
        fail_build_budget(&budget, "checkpoint outlined key");
        let count = PAYLOAD_CREDIT_BYTES / bytes;
        for row in 1..count {
            encoding.push::<true>(&values, row as u64).unwrap();
        }
        assert_eq!(payload_accounting(&encoding.rows).0, count * bytes);
        let error = encoding.push::<true>(&values, count as u64).unwrap_err();
        let RuntimeOrFatalError::Runtime(error) = error else {
            panic!("expected resource error");
        };
        assert!(error.contains::<ResourceError>());
        assert_eq!(encoding.rows.len(), count);
        assert_eq!(payload_accounting(&encoding.rows).0, count * bytes);
        drop(encoding);
        assert_eq!(budget.used(), 0);
    }

    /// Purpose: Refill credits across a growing run and then admit an unusually large key.
    /// Expected: Spare credit stays bounded, existing keys retain their charges, and a large key admits only its shortfall.
    #[test]
    fn encoding_refills_follow_retained_payload() {
        let budget = MemoryBudget::new(1024 * 1024);
        let mut encoding = Encoding::new(&budget, &[ValType::new(ValKind::VarByte, false); 2]);
        encoding.rows.ensure_capacity(64, "test entries").unwrap();
        let values = [Val::from(vec![7u8; 1024]), Val::from(vec![9u8; 1024])];
        for row in 0..35 {
            encoding.push::<true>(&values, row).unwrap();
            assert!(payload_accounting(&encoding.rows).1 >= payload_accounting(&encoding.rows).0);
            assert!(
                payload_accounting(&encoding.rows).1 - payload_accounting(&encoding.rows).0
                    < PAYLOAD_CREDIT_BYTES
            );
        }
        assert!(payload_accounting(&encoding.rows).1 > PAYLOAD_CREDIT_BYTES);
        assert!(payload_accounting(&encoding.rows).1 > payload_accounting(&encoding.rows).0);
        let large = [Val::from(vec![7u8; 40_000]), Val::from(vec![9u8; 40_000])];
        let bytes = encoding.encoder.encoded_len(&large).unwrap();
        assert!(bytes > PAYLOAD_CREDIT_BYTES);
        assert!(
            payload_accounting(&encoding.rows).0 + bytes - payload_accounting(&encoding.rows).1
                < PAYLOAD_CREDIT_BYTES
        );
        let retained = payload_accounting(&encoding.rows).0;
        encoding.push::<true>(&large, 35).unwrap();
        assert_eq!(payload_accounting(&encoding.rows).0, retained + bytes);
        assert_eq!(
            payload_accounting(&encoding.rows).1,
            payload_accounting(&encoding.rows).0
        );
        assert_eq!(encoding.rows.len(), 36);
        drop(encoding);
        assert_eq!(budget.used(), 0);
    }

    /// Purpose: Admit exact fitting payloads near the ceiling, including composite keys larger than a refill.
    /// Expected: Inline keys need no payload charge, outlined keys fit their actual bytes, and insufficient capacity leaves retained entries unchanged.
    #[test]
    fn encoding_payload_boundaries() {
        for lengths in [
            vec![MEM_CMP_KEY_INLINE],
            vec![MEM_CMP_KEY_INLINE + 1],
            vec![40_000, 40_000],
        ] {
            for insufficient in [false, true] {
                let budget = MemoryBudget::new(1024 * 1024);
                let types = vec![ValType::new(ValKind::VarByte, false); lengths.len()];
                let mut encoding = Encoding::new(&budget, &types);
                encoding.rows.ensure_capacity(1, "test entry").unwrap();
                let values: Vec<_> = lengths
                    .iter()
                    .map(|&len| Val::from(vec![7u8; len]))
                    .collect();
                let expected = encoding.encoder.encode(&values);
                let len = expected.as_bytes().len();
                let payload = if len > MEM_CMP_KEY_INLINE { len } else { 0 };
                let available = payload.saturating_sub(usize::from(insufficient));
                let other = budget
                    .reserve(1024 * 1024 - budget.used() - available, "other owner")
                    .unwrap();
                let result = encoding.push::<true>(&values, 1);
                if insufficient && payload != 0 {
                    assert!(result.is_err());
                    assert!(encoding.rows.is_empty());
                    assert_eq!(payload_accounting(&encoding.rows).1, 0);
                    assert_eq!(payload_accounting(&encoding.rows).0, 0);
                } else {
                    result.unwrap();
                    assert_eq!(encoding.rows[0].key, expected);
                    assert_eq!(payload_accounting(&encoding.rows).1, payload);
                    assert_eq!(payload_accounting(&encoding.rows).0, payload);
                }
                drop(encoding);
                drop(other);
                assert_eq!(budget.used(), 0);
            }
        }
    }

    /// Purpose: Release extraction-only ownership before admitting stable-sort workspace.
    /// Expected: Sorting sees only source and retained-key charges, and failed or stopped extraction leaves no per-job reservations.
    #[test]
    fn extraction_releases_workspace_before_sorting() {
        use crate::catalog::tests::table3;
        use crate::table::tests::{lightweight_test_engine, table_for_internal_assertion};
        use tempfile::TempDir;
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp, "checkpoint_sort_admission").await;
            let table_id = table3(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let layout = table.layout_snapshot();
            let index = layout.active_secondary_indexes().next().unwrap().0;
            let session = engine.new_session().unwrap();
            let guards = session.pool_guards();
            let guard = table
                .row_store
                .try_get_insert_page(&guards, 8)
                .await
                .unwrap();
            let page = guard.page();
            let values = [Val::from(vec![7u8; 128])];
            for _ in 0..8 {
                assert!(page.insert(&layout.metadata().col, &values).is_ok());
            }
            let mut sources = sources();
            sources
                .capture_puts(&[Some(PreparedTransitionPage {
                    page_id: guard.page_id(),
                    start_row_id: page.header.start_row_id,
                    end_row_id: page.header.start_row_id + u64::from(page.header.max_row_count),
                    cutoff_ts: sources.cutoff,
                    observed_version: 0,
                    required_cutoff_ts: None,
                    del_bitmap: page.del_bitmap(page.header.row_count()),
                    overlay_markers: Vec::new(),
                })])
                .unwrap();
            drop(guard);
            let budget = sources.budget.clone();
            let retained = budget.used();
            let encoder = layout
                .expect_secondary_index(index)
                .disk_runtime()
                .key_encoder_arc();
            let key = encoder.encode(&values);
            let expected = retained + 8 * (size_of::<IndexBuildEntry>() + key.as_bytes().len());
            let source = Arc::new(IndexSource {
                source: Arc::new(sources),
                table,
                layout,
                guards,
                index,
                encoder,
            });
            let group = SourceGroup {
                kind: RunKind::Put,
                range: 0..1,
            };
            let stop = AtomicBool::new(false);
            let BuildOutcome::Complete(result) =
                source.extract_group(0, group.clone(), &stop).await.unwrap()
            else {
                panic!("live extraction stopped");
            };
            assert_eq!(result.run.entries().len(), 8);
            assert!(result.run.entries().iter().all(|entry| entry.key == key));
            assert_eq!(budget.used(), expected);
            drop(result);
            assert_eq!(budget.used(), retained);
            fail_build_budget(&budget, "checkpoint stable sort");
            let error = match source.extract_group(0, group.clone(), &stop).await {
                Err(error) => error,
                Ok(_) => panic!("expected stable-sort admission failure"),
            };
            // Admission snapshots used bytes before cleanup destroys the run.
            assert!(
                format!("{error:?}").contains(&format!("used={expected},")),
                "{error:?}"
            );
            assert_eq!(budget.used(), retained);
            stop.store(true, Ordering::Release);
            assert!(matches!(
                source.extract_group(0, group, &stop).await.unwrap(),
                BuildOutcome::Stopped
            ));
            assert_eq!(budget.used(), retained);
            drop(source);
            assert_eq!(budget.used(), 0);
        });
    }

    /// Purpose: Capture the authoritative put bitmap independently of later source changes.
    /// Expected: Captured page identity and selection stay fixed, and all source reservations release with their owner.
    #[test]
    fn put_capture_owns_selection_and_admission() {
        let mut sources = sources();
        let budget = sources.budget.clone();
        let before = budget.used();
        let mut pages = vec![Some(prepared())];
        sources.capture_puts(&pages).unwrap();
        assert!(budget.used() > before);
        pages[0].as_mut().unwrap().del_bitmap[0] = u64::MAX;
        assert_eq!(&*sources.puts[0].deleted, &[5]);
        assert_eq!(sources.puts[0].page, PageID::new(1));
        assert_eq!(
            (sources.puts[0].start, sources.puts[0].end),
            (RowID::new(0), RowID::new(64))
        );
        assert!(!sources.is_empty());
        drop(sources);
        assert_eq!(budget.used(), 0);
    }

    /// Purpose: Reject incomplete, foreign-cutoff, and overlapping checkpoint source selections.
    /// Expected: Invalid source authority never becomes a complete accepted capture.
    #[test]
    fn put_capture_rejects_invalid_provenance() {
        let mut foreign = prepared();
        foreign.cutoff_ts = TrxID::new(11);
        for pages in [
            vec![None],
            vec![Some(foreign)],
            vec![Some(prepared()), Some(prepared())],
        ] {
            let mut sources = sources();
            let budget = sources.budget.clone();
            assert!(sources.capture_puts(&pages).is_err());
            drop(sources);
            assert_eq!(budget.used(), 0);
        }
    }

    /// Purpose: Enforce source admission at both descriptor and bitmap allocation boundaries.
    /// Expected: Allocation failure preserves its resource cause, publishes no captured page, and releases retained capacity on cleanup.
    #[test]
    fn put_capture_admission_failures() {
        for purpose in ["checkpoint put descriptors", "checkpoint put bitmap"] {
            let mut sources = sources();
            let budget = sources.budget.clone();
            fail_build_budget(&budget, purpose);
            let error = sources.capture_puts(&[Some(prepared())]).unwrap_err();
            let RuntimeOrFatalError::Runtime(error) = error else {
                panic!("expected resource failure")
            };
            assert!(error.contains::<ResourceError>(), "{purpose}: {error:?}");
            assert!(sources.puts.is_empty());
            drop(sources);
            assert_eq!(budget.used(), 0);
        }
    }

    /// Purpose: Retain resolved sparse delete identities under exact descriptor admission.
    /// Expected: Captured pairs preserve caller RowIDs; malformed selections and both admission failures retain no descriptor and release their charges.
    #[test]
    fn delete_capture_preserves_sparse_pairs_and_cleans_failures() {
        use crate::session::tests::{assert_checkpoint_published, wait_for_checkpoint_purge};
        use crate::table::tests::{
            assert_freeze_created, column_block_index_snapshot, create_table2_for_test,
            expect_delete_committed, insert_one_row, lightweight_test_engine, single_key,
        };
        use tempfile::TempDir;
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp, "delete_capture").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut ids = Vec::new();
            for key in 0..8i32 {
                ids.push(
                    insert_one_row(
                        table_id,
                        &mut session,
                        vec![Val::from(key), Val::from("name")],
                    )
                    .await,
                );
            }
            for key in [2i32, 4] {
                expect_delete_committed(table_id, &mut session, &single_key(key)).await;
            }
            // The fixture requires physical holes, so both deletes must be
            // purged before freeze captures the materialization cutoff.
            wait_for_checkpoint_purge(&session, session.last_cts()).await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            let guards = session.pool_guards();
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let context = snapshot
                .index(guards.disk_guard())
                .locate_deletion_context(ids[3])
                .await
                .unwrap()
                .unwrap();
            assert_eq!(context.ordinal, 2);
            assert_eq!(context.entry.row_count(), 6);
            let pairs = [(0, ids[0]), (2, ids[3]), (4, ids[6])];
            let mut captured = sources();
            captured.capture_deletes(context.entry, &pairs).unwrap();
            assert_eq!(&*captured.deletes[0].rows, &pairs);
            let budget = captured.budget.clone();
            drop(captured);
            assert_eq!(budget.used(), 0);

            for invalid in [
                vec![(6, ids[7])],
                vec![(0, context.entry.end_row_id())],
                vec![(2, ids[3]), (0, ids[0])],
                vec![(0, ids[0]), (0, ids[3])],
            ] {
                let mut captured = sources();
                let budget = captured.budget.clone();
                assert!(captured.capture_deletes(context.entry, &invalid).is_err());
                assert!(captured.deletes.is_empty());
                drop(captured);
                assert_eq!(budget.used(), 0);
            }
            for purpose in [
                "checkpoint delete descriptors",
                "checkpoint delete ordinals",
            ] {
                let mut captured = sources();
                let budget = captured.budget.clone();
                fail_build_budget(&budget, purpose);
                let Err(RuntimeOrFatalError::Runtime(error)) =
                    captured.capture_deletes(context.entry, &pairs)
                else {
                    panic!("expected capture resource failure: {purpose}");
                };
                assert!(error.contains::<ResourceError>());
                assert!(captured.deletes.is_empty());
                drop(captured);
                assert_eq!(budget.used(), 0);
            }
        });
    }

    /// Purpose: Settle checkpoint extraction workers after projection admission fails.
    /// Expected: The original resource failure reaches the coordinator and all per-index job storage is released before returning.
    #[test]
    fn extraction_failure_drains_accepted_jobs() {
        use crate::catalog::tests::table2;
        use crate::table::tests::{lightweight_test_engine, table_for_internal_assertion};
        use tempfile::TempDir;
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp, "checkpoint_extract_failure").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let layout = table.layout_snapshot();
            let index = layout.active_secondary_indexes().next().unwrap().0;
            let session = engine.new_session().unwrap();
            for purpose in [
                "checkpoint projection",
                "checkpoint decode payloads",
                "checkpoint extraction ledger",
            ] {
                let mut sources = sources();
                // Admission is deliberately rejected before these descriptors
                // can cause page reads; no invented page is ever dereferenced.
                let pages = (0..8)
                    .map(|page| {
                        let mut prepared = prepared();
                        prepared.start_row_id = RowID::new(page * 64);
                        prepared.end_row_id = RowID::new((page + 1) * 64);
                        Some(prepared)
                    })
                    .collect::<Vec<_>>();
                sources.capture_puts(&pages).unwrap();
                let sources = Arc::new(sources);
                let budget = sources.budget.clone();
                let retained = budget.used();
                fail_build_budget(&budget, purpose);
                let result = sources
                    .extract(
                        table.clone(),
                        layout.clone(),
                        session.pool_guards(),
                        index,
                        &engine.inner().core.thread_pool,
                        2,
                    )
                    .await;
                let Err(RuntimeOrFatalError::Runtime(error)) = result else {
                    panic!("{purpose}: expected extraction resource failure")
                };
                assert!(error.contains::<ResourceError>(), "{purpose}: {error:?}");
                assert_eq!(
                    budget.used(),
                    retained,
                    "{purpose}: accepted child still owns input"
                );
                assert_eq!(
                    Arc::strong_count(&sources),
                    1,
                    "{purpose}: accepted child retains source"
                );
                drop(sources);
                assert_eq!(budget.used(), 0, "{purpose}");
            }
        });
    }
}
