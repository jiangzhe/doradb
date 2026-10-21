//! Captured CREATE authority and physical cold-row decoding.
#[cfg(test)]
use super::cold_extract::tests::Hooks;
use super::{
    BudgetedVec, BuildEntries, BuildOutcome, DuplicateCheck, IndexBuildEntry, MemoryBudget,
};
use crate::buffer::PoolGuards;
use crate::catalog::TableIndexMetadata;
use crate::catalog::index::IndexDdlGateScope;
use crate::error::{DataIntegrityError, OperationError, QuadResult, RuntimeError, RuntimeResult};
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::id::{BlockID, RowID, TrxID};
#[cfg(feature = "profiling")]
use crate::index::column_block_index::ColdReadProfile;
use crate::index::column_block_index::{ColumnBlockRangeCursor, ColumnBuildWorkspace};
use crate::index::{BTreeKeyEncoder, ColumnBlockIndex, secondary_index_encoder};
use crate::lwc::LwcBlock;
use crate::memcmp::MEM_CMP_KEY_INLINE;
#[cfg(feature = "profiling")]
use crate::profiling::ColdRunMeasurements;
use crate::runtime::yield_now;
use crate::table::{CreateIndexPlan, DeleteMarker, Table, TableRuntimeLayout};
use crate::trx::trx_is_committed;
use crate::value::Val;
use error_stack::{Report, ResultExt};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Reusable extraction storage, retained between partitions at the worker allowance.
pub(super) struct ColdWorkspace {
    /// Reusable traversal storage, cleared before transfer to the next partition.
    pub(super) scan: Option<ColumnBuildWorkspace>,
    /// Per-partition counters retained with the reusable workspace.
    #[cfg(feature = "profiling")]
    pub(super) sample: ColdRunMeasurements,
    // Retain admitted vector capacity; decoded payloads live only through row encoding.
    values: BudgetedVec<Val>,
}

/// Source resources and exact key shape retained by every accepted cold worker.
pub(crate) struct ColdBuildSource {
    /// Stable table and file resources.
    pub(super) table: Arc<Table>,
    layout: Arc<TableRuntimeLayout>,
    guards: PoolGuards,
    spec: TableIndexMetadata,
    encoder: BTreeKeyEncoder,
    root: BlockID,
    pivot: RowID,
    /// Invocation scratch shared with durable construction.
    pub(super) budget: MemoryBudget,
    /// Timestamp captured by the accepted CREATE owner.
    pub(super) build_ts: TrxID,
    _ddl: Arc<IndexDdlGateScope>,
    /// Component scheduling controls for accepted-worker tests.
    #[cfg(test)]
    pub(super) test: Hooks,
    /// Source-scoped read and concurrency observations, never published on failure.
    #[cfg(feature = "profiling")]
    pub(super) profile: Arc<ColdReadProfile>,
}

impl ColdBuildSource {
    /// Capture immutable routing and DDL exclusion before any child submission.
    pub(crate) fn capture(
        plan: &CreateIndexPlan,
        guards: PoolGuards,
        ddl: Arc<IndexDdlGateScope>,
        build_ts: TrxID,
        budget: MemoryBudget,
    ) -> Self {
        let root = plan.active_root();
        assert_eq!(
            (root.pivot_row_id, root.column_block_index_root),
            plan.table().row_store.blk_idx().column_route_snapshot(),
            "cold CREATE capture requires matching runtime routing"
        );
        assert!(
            root.column_block_index_root == SUPER_BLOCK_ID || root.pivot_row_id != RowID::new(0),
            "cold CREATE nonempty root requires nonzero pivot"
        );
        let spec = plan.new_index_spec();
        Self {
            table: plan.table().clone(),
            layout: plan.old_layout().clone(),
            guards,
            spec: spec.clone(),
            encoder: secondary_index_encoder(plan.old_layout().metadata(), spec, !spec.unique()),
            root: root.column_block_index_root,
            pivot: root.pivot_row_id,
            budget,
            build_ts,
            _ddl: ddl,
            #[cfg(test)]
            test: Hooks::default(),
            #[cfg(feature = "profiling")]
            profile: Arc::new(ColdReadProfile::default()),
        }
    }

    /// Open the exact captured column tree without acquiring another root snapshot.
    #[inline]
    pub(super) fn column_index(&self) -> ColumnBlockIndex<'_> {
        let index = ColumnBlockIndex::new(
            self.root,
            self.pivot,
            self.table.file().file_kind(),
            self.table.file().sparse_file(),
            self.table.disk_pool(),
            self.guards.disk_guard(),
        );
        #[cfg(feature = "profiling")]
        let index = index.with_source_profile(self.profile.clone());
        index
    }

    /// Preserve duplicate policy even when every source partition is empty.
    #[inline]
    pub(super) fn duplicates(&self) -> DuplicateCheck {
        if self.spec.unique() {
            DuplicateCheck::Collect
        } else {
            DuplicateCheck::Skip
        }
    }

    /// Whether the captured source has no persisted root.
    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.root == SUPER_BLOCK_ID
    }

    /// Admit one minimum extraction workspace before fixing the source plan.
    pub(super) fn workspace(&self) -> QuadResult<ColdWorkspace> {
        let scan = ColumnBuildWorkspace::new(&self.budget)?;
        let mut values = BudgetedVec::new(&self.budget);
        values
            .ensure_capacity(
                self.spec.keys.len() + usize::from(!self.spec.unique()),
                "cold projection",
            )
            .change_context(RuntimeError::IndexAccess)?;
        Ok(ColdWorkspace {
            scan: Some(scan),
            values,
            #[cfg(feature = "profiling")]
            sample: ColdRunMeasurements::default(),
        })
    }

    /// Extract indexed values only, preserving physical membership and deletion errors.
    /// Finish each descriptor batch before yielding and observing cooperative stop.
    pub(super) async fn extract(
        &self,
        cursor: &mut ColumnBlockRangeCursor,
        projection: &mut ColdWorkspace,
        stop: &AtomicBool,
    ) -> QuadResult<BuildOutcome<BuildEntries>> {
        let budget = &self.budget;
        let table = &self.table;
        let disk_guard = self.guards.disk_guard();
        let column_index = self.column_index();
        let mut rows = BuildEntries::new(budget);
        let file_kind = table.file().file_kind();
        while !cursor.exhausted() {
            let batch = cursor.next_batch(&column_index).await?;
            #[cfg(feature = "profiling")]
            self.profile
                .descriptor_peak
                .fetch_max(batch.entries.len(), Ordering::Relaxed);
            for entry in &*batch.entries {
                let identity = &entry.identity;
                let durable_deleted = &entry.deletes;
                let block_id = entry.block_id;
                let persisted = table.storage.load_lwc_block(disk_guard, block_id).await?;
                #[cfg(feature = "profiling")]
                {
                    self.profile.lwc_reads.fetch_add(1, Ordering::Relaxed);
                    projection.sample.descriptors += 1;
                    projection.sample.physical_rows += identity.row_count() as u64;
                }
                let block = persisted.block();
                if usize::from(entry.row_count) != identity.row_count()
                    || block.row_count() != identity.row_count()
                {
                    return Err(Report::new(DataIntegrityError::InvalidPayload)
                        .attach(format!("file={file_kind}, block=lwc_block, block_id={block_id}, create index LWC row count mismatch: entry_rows={}, block_rows={}, row_ids={}", entry.row_count, block.row_count(), identity.row_count()))
                        .change_context(RuntimeError::CatalogAccess).into());
                }
                if block.block_binding_value() != entry.block_binding_value {
                    return Err(Report::new(DataIntegrityError::InvalidPayload)
                        .attach(format!("file={file_kind}, block=lwc_block, block_id={block_id}, create index LWC block binding value mismatch"))
                        .change_context(RuntimeError::CatalogAccess).into());
                }
                for (row_idx, row_id) in identity
                    .as_ref()
                    .iter_deltas()
                    .map(|delta| entry.start_row_id + u64::from(delta))
                    .enumerate()
                {
                    if durable_deleted.contains(row_idx as u16) {
                        continue;
                    }
                    match table.deletion_buffer().get(row_id) {
                        Some(DeleteMarker::Committed(_)) => continue,
                        Some(DeleteMarker::Ref(status)) if trx_is_committed(status.ts()) => continue,
                        Some(DeleteMarker::Ref(_)) => return Err(Report::new(OperationError::WriteConflict)
                            .attach(format!("create index found uncommitted cold-row delete marker: table_id={}, row_id={row_id}", table.table_id())).into()),
                        None => (),
                    }
                    self.encode_row(block, row_idx, row_id, projection, &mut rows).attach_with(|| format!("operation=create_index, phase=encode_cold_row, file={file_kind}, block_id={block_id}, row_id={row_id}"))?;
                }
                drop(persisted);
            }
            yield_now().await;
            if stop.load(Ordering::Acquire) {
                return Ok(BuildOutcome::Stopped);
            }
        }
        Ok(BuildOutcome::Complete(rows))
    }

    // As in hot extraction, decoded row payloads are temporary and outside scratch
    // admission. Only the encoded keys retained by the run need payload admission.
    #[inline]
    fn encode_row(
        &self,
        block: &LwcBlock,
        row_idx: usize,
        row_id: RowID,
        projection: &mut ColdWorkspace,
        rows: &mut BuildEntries,
    ) -> RuntimeResult<()> {
        let metadata = self.layout.metadata();
        let index_spec = &self.spec;
        for index_key in &index_spec.keys {
            let column = index_key.column_ordinal.as_usize();
            let value = block
                .decode_value(&metadata.col, row_idx, column)
                .change_context(RuntimeError::CatalogAccess)?;
            projection.values.push_reserved(value);
        }
        if !index_spec.unique() {
            projection.values.push_reserved(Val::from(row_id));
        }
        let len = self
            .encoder
            .encoded_len(&projection.values)
            .change_context(RuntimeError::CatalogAccess)?;
        rows.push_with(
            if len > MEM_CMP_KEY_INLINE { len } else { 0 },
            "cold run entries",
            "cold outlined key",
            || IndexBuildEntry {
                key: self.encoder.encode_with_len(&projection.values, len),
                row_id,
            },
        )
        .change_context(RuntimeError::CatalogAccess)?;
        #[cfg(feature = "profiling")]
        {
            projection.sample.live_rows += 1;
            projection.sample.encoded_bytes += len as u64;
        }
        projection.values.clear();
        Ok(())
    }
}
