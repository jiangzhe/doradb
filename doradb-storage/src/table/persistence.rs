use super::checkpoint_workflow::{
    CheckpointAttempt, FrozenPageValidationState, PreparedTransitionPage,
};
use super::lifecycle::{CheckpointPublishLease, TableCheckpointRootMutationLease, TableTerminal};
use crate::buffer::PoolGuards;
use crate::buffer::guard::PageGuard;
use crate::catalog::{IndexSpec, SilentWatermarkObject, TableColumnLayout, TableMetadata};
use crate::error::{
    DataIntegrityError, DataIntegrityResult, FatalError, InternalError, LifecycleError,
    MultiDomainResultExt, RuntimeError, RuntimeOrFatalError, RuntimeOrFatalResult,
    RuntimeOrFatalResultExt, RuntimeResult,
};
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::file::table_file::{ActiveRoot, LwcBlockPersist, MutableTableFile};
use crate::id::{BlockID, PageID, RowID, TableID, TrxID};
use crate::index::BTreeKeyEncoder;
use crate::index::disk_tree::{
    NonUniqueDiskTreeEncodedExact, UniqueDiskTreeEncodedDelete, UniqueDiskTreeEncodedPut,
};
use crate::index::{
    ColumnBlockEntryShape, ColumnBlockIndex, ColumnDeleteDeltaPatch, ColumnLeafEntry,
};
use crate::lwc::LwcBuilder;
use crate::obs;
use crate::row::RowPage;
use crate::session::SessionPin;
use crate::table::{
    CheckpointCancelReason, FreezeOutcome, FrozenPage, FrozenPageBatch, Table,
    TableRedoReplayFloor, TableRuntimeLayout,
};
use crate::trx::RetiredRowPageBatch;
use crate::value::{Val, ValKind, ValType};
use error_stack::{Report, ResultExt};
use futures::future::select_all;
use std::collections::BTreeSet;
use std::result::Result as StdResult;

#[cfg(test)]
pub(crate) use tests::test_hooks;

/// User-table checkpoint execution result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointOutcome {
    /// A checkpoint publication was accepted into ordered system commit.
    Published {
        /// Non-active timestamp used to construct and publish checkpoint state.
        checkpoint_ts: TrxID,
        /// System CTS accepted into ordered group commit.
        ///
        /// Acceptance does not by itself acknowledge redo durability.
        redo_cts: TrxID,
        /// Whether publication used only a catalog silent replay watermark.
        ///
        /// `true` means no user-table root was published. `false` means the
        /// checkpoint published a user-table root.
        silent: bool,
    },
    /// No checkpoint work was published because the active root is still live.
    Delayed {
        /// Diagnostic details explaining why checkpoint waited.
        reason: CheckpointDelayReason,
    },
    /// No checkpoint work was published because checkpoint publication was cancelled.
    Cancelled {
        /// Diagnostic details explaining why checkpoint publication was cancelled.
        reason: CheckpointCancelReason,
    },
}

/// Diagnostic payload for a normal, retryable checkpoint scheduling delay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointDelayReason {
    /// The active root is still visible to an active transaction.
    ActiveRoot {
        /// Table whose active root was observed.
        table_id: TableID,
        /// Runtime post-publish root-observation boundary used by reclamation.
        effective_ts: TrxID,
        /// Current global minimum active snapshot timestamp used by GC.
        min_active_sts: TrxID,
    },
    /// A frozen row page cannot be encoded at the selected cutoff yet.
    FrozenPageCutoff {
        /// Table whose frozen batch was validated.
        table_id: TableID,
        /// First selected page that is not ready for this attempt.
        page_id: PageID,
        /// Number of leading pages ready for this cutoff.
        stable_page_count: usize,
        /// Purge-published exclusive checkpoint cutoff.
        cutoff_ts: TrxID,
        /// Minimum exclusive cutoff required by committed row image state.
        required_cutoff_ts: Option<TrxID>,
        /// Whether an unresolved transaction status blocks the page.
        unresolved_status: bool,
    },
}

/// Owns one table checkpoint attempt and its reversible-to-fatal boundary.
struct TableCheckpointer<'table, 'session> {
    table: &'table Table,
    session: &'session SessionPin,
    // Declaration order preserves publication -> root mutation -> attempt
    // release ordering when this owner is dropped.
    publish_lease: Option<CheckpointPublishLease<'table>>,
    root_mutation_lease: Option<TableCheckpointRootMutationLease<'table>>,
    attempt: CheckpointAttempt<'table>,
    irreversible: Option<FatalError>,
}

impl<'table, 'session> TableCheckpointer<'table, 'session> {
    #[inline]
    fn new(
        table: &'table Table,
        session: &'session SessionPin,
        attempt: CheckpointAttempt<'table>,
    ) -> Self {
        Self {
            table,
            session,
            publish_lease: None,
            root_mutation_lease: None,
            attempt,
            irreversible: None,
        }
    }

    async fn run(&mut self) -> RuntimeOrFatalResult<CheckpointOutcome> {
        let table = self.table;
        let session = self.session;
        let table_id = table.table_id();
        let table_file = table.file();
        let disk_pool = table.disk_pool();
        let trx_sys = session.engine.trx_sys.clone();
        let table_writes = session.engine.table_fs.background_writes().clone();
        let pool_guards = session.pool_guards();
        match table.try_begin_checkpoint_root_mutation() {
            Ok(lease) => self.root_mutation_lease = Some(lease),
            Err(reason) => return Ok(CheckpointOutcome::Cancelled { reason }),
        }
        if let Some(reason) = table.active_root_checkpoint_delay(session) {
            return Ok(CheckpointOutcome::Delayed { reason });
        }
        let layout = table.layout_snapshot();
        let metadata = layout.metadata();

        // Step 1: claim one mutable root snapshot and initialize checkpoint
        // boundaries. This is checkpoint-internal current-root access after the
        // post-lease liveness check above.
        let mut mutable_file = MutableTableFile::fork(table_file, &table_writes, disk_pool.clone());
        let pivot_row_id = mutable_file.root().pivot_row_id;
        let mut secondary_sidecar = SecondaryCheckpointSidecar::new(metadata);

        // Step 2: derive replay and data cutoffs from the explicit batch. The
        // purge-published horizon is the exclusive cutoff for both frozen row
        // images and cold deletes.
        let (pages, frozen_heap_redo_start_ts) = self
            .attempt
            .batch()
            .map(|batch| (batch.pages.clone(), batch.heap_redo_start_ts))
            .unwrap_or_default();
        let cutoff_ts = trx_sys.published_gc_horizon();

        // Step 3: allocate a construction/publication timestamp without
        // registering an active transaction STS. Ordered system redo receives a
        // separate CTS only after publication reaches group-commit enqueue.
        let checkpoint_ts = trx_sys.allocate_checkpoint_ts();
        let mut sys_trx = trx_sys.begin_sys_trx();
        #[cfg(test)]
        test_hooks::run_test_checkpoint_after_trx_start_hook().await;
        // If freeze did not observe a successor page, resolve the current hot
        // boundary only after allocating checkpoint STS. A page appended after
        // an empty boundary scan must then have create_cts above the fallback.
        let heap_redo_start_row_id = pages
            .last()
            .map(|page| page.end_row_id)
            .unwrap_or(pivot_row_id);
        let next_heap_redo_start_ts = match frozen_heap_redo_start_ts {
            Some(heap_redo_start_ts) => Some(heap_redo_start_ts),
            None => {
                let heap_redo_start_ts = table
                    .heap_redo_start_from(&pool_guards, heap_redo_start_row_id)
                    .await
                    .change_context(RuntimeError::CheckpointExecution)
                    .attach_with(|| {
                        format!(
                            "operation=checkpoint_table, phase=resolve_heap_redo_start, table_id={table_id}, start_row_id={heap_redo_start_row_id}"
                        )
                    })?;
                if let Some(batch) = self.attempt.batch_mut() {
                    batch.heap_redo_start_ts = heap_redo_start_ts;
                }
                heap_redo_start_ts
            }
        };
        if !pages.is_empty() {
            let transition_pages = table
                .load_frozen_pages_for_transition(&pool_guards, &pages)
                .await
                .change_context(RuntimeError::CheckpointExecution)
                .attach_with(|| {
                    format!(
                        "operation=checkpoint_table, phase=load_frozen_pages, table_id={table_id}"
                    )
                })?;
            let delay = {
                let Some(batch) = self.attempt.batch_mut() else {
                    panic!("non-empty checkpoint page list requires frozen source")
                };
                table.prepare_page_transition(&transition_pages, batch, cutoff_ts)
            };
            if let Some(delay) = delay {
                return Ok(CheckpointOutcome::Delayed {
                    reason: CheckpointDelayReason::FrozenPageCutoff {
                        table_id,
                        page_id: delay.page_id,
                        stable_page_count: delay.stable_page_count,
                        cutoff_ts,
                        required_cutoff_ts: delay.required_cutoff_ts,
                        unresolved_status: delay.unresolved_status,
                    },
                });
            }
            // Preparation has exhausted every reversible delay. Acquire the
            // workflow lease and fatal boundary before applying any page state.
            match self.begin_transition() {
                Ok(()) => {}
                Err(reason) => return Ok(CheckpointOutcome::Cancelled { reason }),
            }
            let Some(batch) = self.attempt.batch_mut() else {
                panic!("non-empty checkpoint page list requires frozen source")
            };
            table.apply_page_transition(&transition_pages, batch, cutoff_ts);
        }
        #[cfg(test)]
        if !pages.is_empty() {
            test_hooks::run_test_checkpoint_after_publish_admission_hook().await;
        }

        // Step 4: build LWC blocks from transition pages using the cutoff
        // snapshot. The sidecar callback observes the same committed-visible
        // rows accepted by the LWC builder, independent of later block splits.
        let new_pivot_row_id = pages
            .last()
            .map(|page| page.end_row_id)
            .unwrap_or(pivot_row_id);
        let collect_visible_row = if secondary_sidecar.indexes.is_empty() {
            None
        } else {
            let col_layout = metadata.col.as_ref();
            Some(|page: &RowPage, row_idx: usize, row_id: RowID| {
                secondary_sidecar.add_data_row(col_layout, page, row_idx, row_id)
            })
        };
        let mut lwc_blocks = table
            .build_lwc_blocks(
                metadata,
                &pool_guards,
                self.attempt
                    .batch()
                    .map(|batch| batch.prepared.as_slice())
                    .unwrap_or_default(),
                collect_visible_row,
            )
            .await
            .change_context(RuntimeError::CheckpointExecution)
            .attach_with(|| {
                format!("operation=checkpoint_table, phase=build_lwc_blocks, table_id={table_id}")
            })?;
        // A heartbeat checkpoint still advances the heap replay floor: its
        // transaction STS comes from the global timestamp sequence.
        let heap_redo_start_ts = next_heap_redo_start_ts.unwrap_or(checkpoint_ts);

        if let Some(last) = lwc_blocks.last_mut()
            && last.shape.end_row_id() < new_pivot_row_id
        {
            last.shape.set_end_row_id(new_pivot_row_id);
        }

        // Step 5: apply checkpoint changes to the already-checked mutable root.
        if !lwc_blocks.is_empty() {
            mutable_file
                .apply_lwc_blocks(lwc_blocks, heap_redo_start_ts, checkpoint_ts, disk_pool)
                .await
                .change_runtime_context(RuntimeError::CheckpointExecution)
                .attach_with(|| {
                    format!(
                        "operation=checkpoint_table, phase=apply_lwc_blocks, table_id={table_id}"
                    )
                })?;
        } else {
            mutable_file.apply_checkpoint_metadata(new_pivot_row_id, heap_redo_start_ts);
        }

        // Step 7: merge committed cold-row deletions into column index payloads,
        // collect matching secondary-index delete sidecar work, and publish the
        // durable cold-delete replay watermark.
        table
            .apply_deletion_checkpoint(
                &mut mutable_file,
                metadata,
                &mut secondary_sidecar,
                cutoff_ts,
                checkpoint_ts,
            )
            .await
            .change_runtime_context(RuntimeError::CheckpointExecution)
            .attach_with(|| {
                format!(
                    "operation=checkpoint_table, phase=apply_deletion_checkpoint, table_id={table_id}"
                )
            })?;

        // Step 8: apply accumulated secondary-index sidecar work to DiskTree
        // roots on the same mutable file fork. Root publication remains atomic
        // with the table checkpoint commit below.
        table
            .apply_secondary_checkpoint_sidecar(
                &mut mutable_file,
                &layout,
                &mut secondary_sidecar,
                checkpoint_ts,
            )
            .await
            .change_runtime_context(RuntimeError::CheckpointExecution)
            .attach_with(|| {
                format!(
                    "operation=checkpoint_table, phase=apply_secondary_sidecar, table_id={table_id}"
                )
            })?;

        // Step 9: after all checkpoint CoW writes are represented in the
        // mutable root, rebuild its allocation map from the current active root
        // and the mutable root that will be published.
        table
            .rebuild_reachable_alloc_map(&mut mutable_file, &layout)
            .await
            .change_context(RuntimeError::CheckpointExecution)
            .attach_with(|| {
                format!("operation=checkpoint_table, phase=rebuild_alloc_map, table_id={table_id}")
            })?;

        if let Some(requested_floor) =
            silent_watermark_floor(table_file.active_root_unchecked(), mutable_file.root())
        {
            drop(mutable_file);
            match self.begin_publishing() {
                Ok(true) => {
                    #[cfg(test)]
                    test_hooks::run_test_checkpoint_after_publish_admission_hook().await;
                }
                Ok(false) => {}
                Err(reason) => return Ok(CheckpointOutcome::Cancelled { reason }),
            }
            let watermark = SilentWatermarkObject {
                table_id: table.table_id(),
                heap_redo_start_ts: requested_floor.heap_redo_start_ts,
                deletion_cutoff_ts: requested_floor.deletion_cutoff_ts,
            };
            self.set_irreversible(FatalError::CatalogWrite);
            #[cfg(test)]
            test_hooks::run_test_silent_watermark_mutation_hook()
                .await
                .change_context(RuntimeError::CheckpointExecution)
                .attach_with(|| {
                    format!(
                        "operation=checkpoint_table, phase=test_silent_watermark_hook, table_id={table_id}"
                    )
                })?;
            sys_trx
                .upsert_silent_watermark(session.engine.catalog(), &pool_guards, watermark)
                .await
                .change_context(RuntimeError::CheckpointExecution)
                .attach_with(|| {
                    format!(
                        "operation=checkpoint_table, phase=upsert_silent_watermark, table_id={table_id}"
                    )
                })?;
            self.set_irreversible(FatalError::CheckpointWrite);
            #[cfg(test)]
            test_hooks::maybe_force_checkpoint_commit_error()?;
            let redo_cts = trx_sys
                .commit_sys(sys_trx)
                .change_runtime_context(RuntimeError::CheckpointExecution)
                .attach_with(|| {
                    format!(
                        "operation=checkpoint_table, phase=commit_silent_redo, table_id={table_id}"
                    )
                })?;
            return Ok(CheckpointOutcome::Published {
                checkpoint_ts,
                redo_cts,
                silent: true,
            });
        }

        if let (Some(first), Some(last)) = (pages.first(), pages.last()) {
            let page_ids = pages
                .iter()
                .map(|page| page.page_id)
                .collect::<Vec<_>>()
                .into_boxed_slice();
            sys_trx.retire_row_pages(RetiredRowPageBatch::new(
                table.table_id(),
                first.start_row_id,
                last.end_row_id,
                page_ids,
            ));
        }
        sys_trx.record_data_checkpoint(table.table_id(), new_pivot_row_id, checkpoint_ts);

        // Step 10: enter the no-cancel publication section, publish a new
        // table-file root, and then enqueue the checkpoint system transaction. This
        // happens only when table-file state beyond replay-bound fields
        // changed. Replay-bound-only checkpoints are published as catalog
        // silent watermark rows above.
        match self.begin_publishing() {
            Ok(true) => {
                #[cfg(test)]
                test_hooks::run_test_checkpoint_after_publish_admission_hook().await;
            }
            Ok(false) => {}
            Err(reason) => return Ok(CheckpointOutcome::Cancelled { reason }),
        }
        self.set_irreversible(FatalError::CheckpointWrite);
        let published_root = mutable_file.root();
        let published_pivot_row_id = published_root.pivot_row_id;
        let published_column_root = published_root.column_block_index_root;
        trx_sys
            .publish_table_file_root(mutable_file, checkpoint_ts, false)
            .await
            .change_context(RuntimeError::CheckpointExecution)
            .attach_with(|| {
                format!("operation=checkpoint_table, phase=publish_table_root, table_id={table_id}")
            })?;
        table
            .mem
            .blk_idx()
            .update_column_root(published_pivot_row_id, published_column_root)
            .await;
        #[cfg(test)]
        test_hooks::maybe_force_post_publish_checkpoint_error()?;

        #[cfg(test)]
        test_hooks::maybe_force_checkpoint_commit_error()?;
        let redo_cts = trx_sys
            .commit_sys(sys_trx)
            .change_runtime_context(RuntimeError::CheckpointExecution)
            .attach_with(|| {
                format!(
                    "operation=checkpoint_table, phase=commit_checkpoint_redo, table_id={table_id}"
                )
            })?;
        Ok(CheckpointOutcome::Published {
            checkpoint_ts,
            redo_cts,
            silent: false,
        })
    }

    fn begin_transition(&mut self) -> StdResult<(), CheckpointCancelReason> {
        assert!(
            self.publish_lease.is_none() && self.irreversible.is_none(),
            "page transition requires an unadmitted table checkpointer"
        );
        let lease = self
            .table
            .checkpoint_workflow
            .try_begin_transition(&self.table.lifecycle)?;
        self.publish_lease = Some(lease);
        self.irreversible = Some(FatalError::CheckpointWrite);
        Ok(())
    }

    /// Ensures final publication admission and reports whether this call
    /// acquired it. A transitioned checkpoint already owns the admission.
    fn begin_publishing(&mut self) -> StdResult<bool, CheckpointCancelReason> {
        if self.publish_lease.is_some() {
            assert!(
                self.irreversible.is_some(),
                "only an irreversible transition may reuse publication admission"
            );
            return Ok(false);
        }
        let lease = self
            .table
            .checkpoint_workflow
            .try_begin_publishing(&self.table.lifecycle, self.attempt.source())?;
        self.publish_lease = Some(lease);
        Ok(true)
    }

    #[inline]
    fn set_irreversible(&mut self, reason: FatalError) {
        assert!(
            self.publish_lease.is_some(),
            "irreversible checkpoint work requires publication admission"
        );
        self.irreversible = Some(reason);
    }

    fn resolve(
        mut self,
        result: RuntimeOrFatalResult<CheckpointOutcome>,
    ) -> RuntimeOrFatalResult<CheckpointOutcome> {
        match result {
            Ok(outcome) => {
                match outcome {
                    CheckpointOutcome::Published { .. } => {
                        assert!(
                            self.publish_lease.is_some() && self.irreversible.is_some(),
                            "published checkpoint must cross an irreversible admission"
                        );
                        self.table.checkpoint_workflow.finish_publication();
                        self.irreversible = None;
                        drop(self.publish_lease.take());
                    }
                    CheckpointOutcome::Delayed { .. } | CheckpointOutcome::Cancelled { .. } => {
                        assert!(
                            self.publish_lease.is_none() && self.irreversible.is_none(),
                            "non-publication outcome cannot escape publication admission"
                        );
                    }
                }
                Ok(outcome)
            }
            Err(err) => {
                if let Some(reason) = self.irreversible.take() {
                    let report = err.into_fatal_report(reason);
                    let poison = self.session.engine.poisoner.poison(report);
                    drop(self.publish_lease.take());
                    Err(poison.into())
                } else {
                    if self.publish_lease.is_some() {
                        self.table.checkpoint_workflow.finish_publication();
                        drop(self.publish_lease.take());
                    }
                    Err(err)
                }
            }
        }
    }
}

impl Drop for TableCheckpointer<'_, '_> {
    fn drop(&mut self) {
        debug_assert!(self.irreversible.is_none() || self.publish_lease.is_some());
        let Some(_) = self.publish_lease else {
            return;
        };
        if let Some(reason) = self.irreversible.take() {
            let report = Report::new(reason)
                .attach("table checkpoint stopped after an irreversible transition");
            obs::error!(
                "event=engine_poison component=table action=poison result=error error={:?}",
                report
            );
            let _ = self.session.engine.poisoner.poison(report);
        } else {
            self.table.checkpoint_workflow.finish_publication();
        }
    }
}

#[derive(Clone, Copy)]
struct BlockPatchGroup {
    entry: ColumnLeafEntry,
    pending_start: usize,
    pending_end: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EncodedRowEntry {
    key: Vec<u8>,
    row_id: RowID,
}

// Checkpoint-local accumulator for one secondary index. Batches are appended
// while scanning checkpoint inputs, then sorted and coalesced once before the
// DiskTree writer runs.
enum SecondaryIndexSidecar {
    Unique {
        encoder: BTreeKeyEncoder,
        puts: Vec<EncodedRowEntry>,
        deletes: Vec<EncodedRowEntry>,
    },
    NonUnique {
        encoder: BTreeKeyEncoder,
        inserts: Vec<Vec<u8>>,
        deletes: Vec<Vec<u8>>,
    },
}

impl SecondaryIndexSidecar {
    #[inline]
    fn new(metadata: &TableMetadata, index_spec: &IndexSpec) -> Self {
        if index_spec.unique() {
            Self::Unique {
                encoder: secondary_disk_tree_encoder(metadata, index_spec, false),
                puts: Vec::new(),
                deletes: Vec::new(),
            }
        } else {
            Self::NonUnique {
                encoder: secondary_disk_tree_encoder(metadata, index_spec, true),
                inserts: Vec::new(),
                deletes: Vec::new(),
            }
        }
    }

    #[inline]
    fn has_work(&self) -> bool {
        match self {
            Self::Unique { puts, deletes, .. } => !puts.is_empty() || !deletes.is_empty(),
            Self::NonUnique {
                inserts, deletes, ..
            } => !inserts.is_empty() || !deletes.is_empty(),
        }
    }

    fn add_data(&mut self, key: Vec<Val>, row_id: RowID) {
        match self {
            Self::Unique { encoder, puts, .. } => {
                let encoded = encoder.encode(&key).as_bytes().to_vec();
                puts.push(EncodedRowEntry {
                    key: encoded,
                    row_id,
                });
                // Same-run delete suppression is applied once, after all data
                // and deletion sidecar inputs are sorted.
            }
            Self::NonUnique {
                encoder, inserts, ..
            } => {
                // Non-unique DiskTree stores exact (logical_key, row_id)
                // membership; there is no durable delete-mask value.
                let encoded = encoder
                    .encode_pair(&key, Val::from(row_id))
                    .as_bytes()
                    .to_vec();
                inserts.push(encoded);
            }
        }
    }

    fn add_delete(&mut self, key: Vec<Val>, row_id: RowID) {
        match self {
            Self::Unique {
                encoder, deletes, ..
            } => {
                let encoded = encoder.encode(&key).as_bytes().to_vec();
                // Unique deletes are conditional on the old owner. They are
                // suppressed during normalization if this checkpoint also
                // publishes a new owner for the same key.
                deletes.push(EncodedRowEntry {
                    key: encoded,
                    row_id,
                });
            }
            Self::NonUnique {
                encoder, deletes, ..
            } => {
                let encoded = encoder
                    .encode_pair(&key, Val::from(row_id))
                    .as_bytes()
                    .to_vec();
                deletes.push(encoded);
            }
        }
    }

    fn normalize(&mut self) {
        match self {
            Self::Unique { puts, deletes, .. } => {
                normalize_unique_puts(puts);
                normalize_unique_deletes(deletes, puts);
            }
            Self::NonUnique {
                inserts, deletes, ..
            } => {
                normalize_encoded_keys(inserts);
                normalize_encoded_keys(deletes);
                inserts.retain(|key| deletes.binary_search(key).is_err());
            }
        }
    }
}

struct ActiveSecondaryIndexSidecar {
    index_no: usize,
    key_cols: Box<[usize]>,
    sidecar: SecondaryIndexSidecar,
}

struct SecondaryCheckpointSidecar {
    indexes: Vec<ActiveSecondaryIndexSidecar>,
}

impl SecondaryCheckpointSidecar {
    fn new(metadata: &TableMetadata) -> Self {
        // Active-index iteration and every sidecar shape come from the same
        // validated immutable metadata snapshot, so construction cannot fail.
        let indexes = metadata
            .idx
            .active_indexes()
            .map(|(index_no, index_spec)| ActiveSecondaryIndexSidecar {
                index_no,
                key_cols: index_spec
                    .cols
                    .iter()
                    .map(|index_key| index_key.col_no as usize)
                    .collect(),
                sidecar: SecondaryIndexSidecar::new(metadata, index_spec),
            })
            .collect();
        Self { indexes }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.indexes.iter().all(|index| !index.sidecar.has_work())
    }

    fn add_data_row(
        &mut self,
        col_layout: &TableColumnLayout,
        page: &RowPage,
        row_idx: usize,
        row_id: RowID,
    ) {
        // The page and column layout were validated together before the LWC
        // callback runs, and active key columns come from that same metadata.
        for active in &mut self.indexes {
            let key = active
                .key_cols
                .iter()
                .map(|col_idx| page.val(col_layout, row_idx, *col_idx))
                .collect();
            active.sidecar.add_data(key, row_id);
        }
    }

    fn add_deleted_key_at(
        &mut self,
        sidecar_pos: usize,
        index_no: usize,
        row_id: RowID,
        key: Vec<Val>,
    ) {
        // `sidecar_pos` is produced by iterating this same active-index vector,
        // so it must still address the corresponding entry.
        let active = self
            .indexes
            .get_mut(sidecar_pos)
            .unwrap_or_else(|| {
                panic!(
                    "secondary checkpoint sidecar position missing: sidecar_pos={sidecar_pos}, index_no={index_no}, row_id={row_id}"
                )
            });
        assert_eq!(
            active.index_no, index_no,
            "secondary checkpoint sidecar identity changed: sidecar_pos={sidecar_pos}, row_id={row_id}"
        );
        active.sidecar.add_delete(key, row_id);
    }
}

/// Builds the durable secondary DiskTree key encoder for one index spec.
pub(crate) fn secondary_disk_tree_encoder(
    metadata: &TableMetadata,
    index_spec: &IndexSpec,
    append_row_id: bool,
) -> BTreeKeyEncoder {
    assert!(
        !index_spec.cols.is_empty(),
        "secondary-index encoder invariant violated: index has no key columns"
    );
    let mut types = Vec::with_capacity(index_spec.cols.len() + usize::from(append_row_id));
    for key in &index_spec.cols {
        let col_no = key.col_no as usize;
        let ty = metadata
            .col
            .col_types()
            .get(col_no)
            .copied()
            .unwrap_or_else(|| {
                panic!(
                    "secondary-index encoder invariant violated: column_no={col_no}, column_count={}",
                    metadata.col.col_count()
                )
            });
        types.push(ty);
    }
    if append_row_id {
        types.push(ValType::new(ValKind::U64, false));
    }
    BTreeKeyEncoder::new(types)
}

#[inline]
fn normalize_unique_puts(puts: &mut Vec<EncodedRowEntry>) {
    puts.sort_by(|left, right| left.key.cmp(&right.key));
    let mut normalized: Vec<EncodedRowEntry> = Vec::with_capacity(puts.len());
    for entry in puts.drain(..) {
        if let Some(last) = normalized.last_mut()
            && last.key == entry.key
        {
            last.row_id = entry.row_id;
            continue;
        }
        normalized.push(entry);
    }
    *puts = normalized;
}

fn normalize_unique_deletes(deletes: &mut Vec<EncodedRowEntry>, puts: &[EncodedRowEntry]) {
    deletes.sort_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then(left.row_id.cmp(&right.row_id))
    });
    deletes.dedup();
    deletes.retain(|delete| {
        puts.binary_search_by(|put| put.key.as_slice().cmp(delete.key.as_slice()))
            .is_err()
    });
}

fn normalize_encoded_keys(keys: &mut Vec<Vec<u8>>) {
    keys.sort();
    keys.dedup();
}

fn validate_reachable_block(root: &ActiveRoot, block_id: BlockID) -> DataIntegrityResult<()> {
    let idx = usize::from(block_id);
    if idx >= root.alloc_map.len() {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
            "invalid table-root reachable block: root_ts={}, block_id={block_id}, alloc_map_len={}",
            root.root_ts,
            root.alloc_map.len()
        )));
    }
    if !root.alloc_map.is_allocated(idx) {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
            "invalid table-root reachable block: root_ts={}, block_id={block_id}, allocation bit is not set",
            root.root_ts
        )));
    }
    Ok(())
}

impl Table {
    async fn collect_root_reachable_blocks(
        &self,
        root: &ActiveRoot,
        layout: &TableRuntimeLayout,
        reachable: &mut BTreeSet<BlockID>,
    ) -> RuntimeResult<()> {
        if root.secondary_index_roots.len() != layout.index_slot_count() {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "secondary root count mismatch: root_count={}, index_slot_count={}",
                    root.secondary_index_roots.len(),
                    layout.index_slot_count()
                ))
                .change_context(RuntimeError::CheckpointExecution)
                .attach(format!(
                    "operation=collect_root_reachable_blocks, table_id={}, root_ts={}",
                    self.table_id(),
                    root.root_ts
                )));
        }

        let mut root_reachable = BTreeSet::new();
        root_reachable.insert(SUPER_BLOCK_ID);
        root_reachable.insert(root.meta_block_id);

        if root.column_block_index_root != SUPER_BLOCK_ID {
            let disk_pool = self.disk_pool();
            let disk_pool_guard = disk_pool.pool_guard();
            let column_index = ColumnBlockIndex::new(
                root.column_block_index_root,
                root.pivot_row_id,
                self.file().file_kind(),
                self.file().sparse_file(),
                disk_pool,
                &disk_pool_guard,
            );
            column_index
                .collect_reachable_blocks(&mut root_reachable)
                .await
                .change_context(RuntimeError::CheckpointExecution)
                .attach_with(|| {
                    format!(
                        "operation=collect_root_reachable_blocks, phase=walk_column_index, table_id={}, root_ts={}",
                        self.table_id(), root.root_ts
                    )
                })?;
        }

        for (index_no, index_slot) in layout.secondary_indexes().iter().enumerate() {
            let root_block_id = root.secondary_index_roots[index_no];
            let Some(index) = index_slot.as_ref() else {
                if root_block_id != SUPER_BLOCK_ID {
                    return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                        .attach(format!(
                            "inactive secondary index slot has root: index_no={index_no}, root={root_block_id}"
                        ))
                        .change_context(RuntimeError::CheckpointExecution)
                        .attach(format!(
                            "operation=collect_root_reachable_blocks, table_id={}, root_ts={}",
                            self.table_id(), root.root_ts
                        )));
                }
                continue;
            };
            if root_block_id == SUPER_BLOCK_ID {
                continue;
            }
            let runtime = index.disk_runtime();
            let disk_pool_guard = runtime.disk_pool_guard();
            runtime
                .collect_reachable_blocks(root_block_id, &disk_pool_guard, &mut root_reachable)
                .await
                .change_context(RuntimeError::CheckpointExecution)
                .attach_with(|| {
                    format!(
                        "operation=collect_root_reachable_blocks, phase=walk_secondary_index, table_id={}, index_no={index_no}, root_block_id={root_block_id}",
                        self.table_id()
                    )
                })?;
        }

        for block_id in root_reachable {
            validate_reachable_block(root, block_id)
                .change_context(RuntimeError::CheckpointExecution)
                .attach_with(|| {
                    format!(
                        "operation=collect_root_reachable_blocks, phase=validate_block, table_id={}, root_ts={}, block_id={block_id}",
                        self.table_id(), root.root_ts
                    )
                })?;
            reachable.insert(block_id);
        }
        Ok(())
    }

    async fn rebuild_reachable_alloc_map(
        &self,
        mutable_file: &mut MutableTableFile,
        layout: &TableRuntimeLayout,
    ) -> RuntimeResult<usize> {
        let mut reachable = BTreeSet::new();
        self.collect_root_reachable_blocks(
            self.file().active_root_unchecked(),
            layout,
            &mut reachable,
        )
        .await?;
        self.collect_root_reachable_blocks(mutable_file.root(), layout, &mut reachable)
            .await?;
        Ok(mutable_file.rebuild_alloc_map_from_reachable(&reachable))
    }

    async fn apply_deletion_checkpoint(
        &self,
        mutable_file: &mut MutableTableFile,
        metadata: &TableMetadata,
        secondary_sidecar: &mut SecondaryCheckpointSidecar,
        cutoff_ts: TrxID,
        checkpoint_ts: TrxID,
    ) -> RuntimeOrFatalResult<()> {
        let disk_pool = self.disk_pool();
        let disk_pool_guard = disk_pool.pool_guard();
        let (column_block_index_root, pivot_row_id, deletion_cutoff_ts) = {
            let root = mutable_file.root();
            (
                root.column_block_index_root,
                root.pivot_row_id,
                root.deletion_cutoff_ts,
            )
        };
        let cutoff_advanced = cutoff_ts > deletion_cutoff_ts;

        // Step 1: pick committed deletion markers in this checkpoint's durable
        // replay range: [previous_cutoff, current_cutoff). Markers below
        // deletion_cutoff_ts were already covered by an earlier checkpoint,
        // even if the in-memory deletion buffer has not been GC'ed yet. Markers
        // at or above cutoff_ts are intentionally left for a later checkpoint
        // because they may have just been moved from transition row pages.
        // This is currently a simple scan + in-place filter + sort/dedup path.
        // For very large deletion buffers, we can optimize this step later by
        // introducing parallel marker selection and parallel sort/merge while
        // preserving deterministic ordering before patch application.
        let mut selected_row_ids = self
            .deletion_buffer()
            .collect_committed_in_range(deletion_cutoff_ts, cutoff_ts);
        selected_row_ids.retain(|row_id| *row_id < pivot_row_id);
        if selected_row_ids.is_empty() {
            // No eligible cold delete marker remains for this checkpoint range,
            // so advancing only metadata is safe even when there is no persisted
            // column_block_index_root yet, e.g. all frozen rows were already
            // deleted and no LWC block was produced.
            if cutoff_advanced {
                mutable_file.advance_deletion_cutoff_ts(cutoff_ts);
                return Ok(());
            }
            return Ok(());
        }
        selected_row_ids.sort_unstable();
        selected_row_ids.dedup();

        // Step 2: fail closed when eligible cold delete markers exist but the
        // persisted column index needed to resolve them is absent. Advancing
        // deletion_cutoff_ts here would make recovery skip delete redo that was
        // never reflected in column payloads.
        if column_block_index_root == SUPER_BLOCK_ID || pivot_row_id == RowID::new(0) {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "eligible delete markers require column index: column_block_index_root={column_block_index_root}, pivot_row_id={pivot_row_id}"
                ))
                .change_context(RuntimeError::CheckpointExecution)
                .attach(format!(
                    "operation=apply_deletion_checkpoint, phase=validate_column_index, table_id={}",
                    self.table_id()
                ))
                .into());
        }

        // Step 3: resolve each row-id to its persisted block payload.
        // Future improvement:
        // 1) resolve disjoint row-id ranges in parallel for higher throughput;
        // 2) when roaring bitmap encoding is introduced, also fetch the target LWC block
        //    row-id array to map each input row-id to the correct offset-based bit index.
        let column_index = ColumnBlockIndex::new(
            column_block_index_root,
            pivot_row_id,
            self.file().file_kind(),
            self.file().sparse_file(),
            disk_pool,
            &disk_pool_guard,
        );

        let mut groups: Vec<BlockPatchGroup> = Vec::new();
        let mut pending_deltas = Vec::new();
        let mut cached_entry: Option<ColumnLeafEntry> = None;
        for row_id in selected_row_ids {
            let entry = if let Some(entry) = cached_entry
                && row_id >= entry.start_row_id
                && row_id < entry.end_row_id()
            {
                entry
            } else {
                let Some(entry) = column_index.locate_block(row_id).await? else {
                    // The marker is in [previous_cutoff, current_cutoff), so it is
                    // eligible now. A missing locate_block result means we cannot
                    // prove the delete is already durable; do not advance the cutoff.
                    return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                        .attach(format!(
                            "eligible delete marker cannot be located: row_id={row_id}"
                        ))
                        .change_context(RuntimeError::CheckpointExecution)
                        .attach(format!(
                            "operation=apply_deletion_checkpoint, phase=locate_delete_marker, table_id={}, row_id={row_id}",
                            self.table_id()
                        ))
                        .into());
                };
                cached_entry = Some(entry);
                entry
            };
            let delta_u64 = row_id
                .checked_sub(entry.start_row_id)
                .ok_or_else(|| Report::new(DataIntegrityError::InvalidRootInvariant))
                .attach_with(|| {
                    format!(
                        "delete marker precedes block start: row_id={row_id}, start_row_id={}",
                        entry.start_row_id
                    )
                })
                .change_context(RuntimeError::CheckpointExecution)
                .attach_with(|| {
                    format!(
                        "operation=apply_deletion_checkpoint, phase=calculate_delete_delta, table_id={}, row_id={row_id}",
                        self.table_id()
                    )
                })?;
            if delta_u64 > u32::MAX as u64 {
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                    .attach(format!(
                        "delete marker delta exceeds u32: delta={delta_u64}, row_id={row_id}, start_row_id={}",
                        entry.start_row_id
                    ))
                    .change_context(RuntimeError::CheckpointExecution)
                    .attach(format!(
                        "operation=apply_deletion_checkpoint, phase=validate_delete_delta, table_id={}, row_id={row_id}",
                        self.table_id()
                    ))
                    .into());
            }
            let delta = delta_u64 as u32;
            if let Some(group) = groups
                .last_mut()
                .filter(|group| group.entry.start_row_id == entry.start_row_id)
            {
                pending_deltas.push(delta);
                group.pending_end = pending_deltas.len();
            } else {
                let pending_start = pending_deltas.len();
                pending_deltas.push(delta);
                groups.push(BlockPatchGroup {
                    entry,
                    pending_start,
                    pending_end: pending_deltas.len(),
                });
            }
        }
        // A selected marker either failed validation above or contributed one
        // delta to a patch group, so a successful non-empty selection cannot
        // produce an empty rewrite batch.
        assert!(
            !groups.is_empty(),
            "delete marker selection produced no column-index patch groups: table_id={}",
            self.table_id()
        );

        // Step 4: load authoritative persisted deltas and merge pending row-id deltas.
        let mut patch_storage: Vec<(RowID, Vec<u32>)> = Vec::new();
        for group in groups {
            let pending = &pending_deltas[group.pending_start..group.pending_end];
            let (base_deltas, row_ids) = column_index
                .load_delete_deltas_and_row_ids(&group.entry)
                .await?;
            let mut base = base_deltas.into_iter().collect::<BTreeSet<_>>();
            let new_deltas = pending
                .iter()
                .copied()
                .filter(|delta| !base.contains(delta))
                .collect::<Vec<_>>();
            if new_deltas.is_empty() {
                continue;
            }

            self.collect_deleted_secondary_sidecar(
                &group.entry,
                &row_ids,
                &new_deltas,
                metadata,
                secondary_sidecar,
            )
            .await?;

            base.extend(new_deltas);
            patch_storage.push((group.entry.start_row_id, base.into_iter().collect()));
        }
        if patch_storage.is_empty() {
            if cutoff_advanced {
                mutable_file.advance_deletion_cutoff_ts(cutoff_ts);
                return Ok(());
            }
            return Ok(());
        }

        // Step 5: apply typed delete rewrites and advance the index root in the mutable file.
        let patches: Vec<ColumnDeleteDeltaPatch<'_>> = patch_storage
            .iter()
            .map(|(start_row_id, delete_deltas)| ColumnDeleteDeltaPatch {
                start_row_id: *start_row_id,
                delete_deltas,
            })
            .collect();
        let new_root = column_index
            .batch_replace_delete_deltas(mutable_file, &patches, checkpoint_ts)
            .await?;
        mutable_file.set_column_block_index_root(new_root);
        mutable_file.advance_deletion_cutoff_ts(cutoff_ts);
        Ok(())
    }

    async fn apply_secondary_checkpoint_sidecar(
        &self,
        mutable_file: &mut MutableTableFile,
        layout: &TableRuntimeLayout,
        sidecar: &mut SecondaryCheckpointSidecar,
        checkpoint_ts: TrxID,
    ) -> RuntimeOrFatalResult<()> {
        if sidecar.is_empty() {
            return Ok(());
        }
        // Apply secondary-index checkpoint work against the same mutable table
        // file fork as LWC and delete metadata. A later error abandons the
        // fork, so no secondary root can be published on its own.
        #[cfg(test)]
        test_hooks::maybe_force_secondary_sidecar_error()?;

        let metadata = layout.metadata();
        if mutable_file.secondary_index_roots().len() != metadata.idx.index_slot_count() {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "secondary root count mismatch: root_count={}, index_slot_count={}",
                    mutable_file.secondary_index_roots().len(),
                    metadata.idx.index_slot_count()
                ))
                .change_context(RuntimeError::CheckpointExecution)
                .attach(format!(
                    "operation=apply_secondary_checkpoint_sidecar, table_id={}",
                    self.table_id()
                ))
                .into());
        }

        let disk_pool = self.disk_pool();
        let disk_pool_guard = disk_pool.pool_guard();
        for active in &mut sidecar.indexes {
            let index_no = active.index_no;
            // The sidecar is built directly from this immutable metadata
            // snapshot, so each recorded active index must still exist.
            assert!(
                metadata.idx.index_spec(index_no).is_some(),
                "secondary checkpoint sidecar index missing from metadata: table_id={}, index_no={index_no}",
                self.table_id()
            );
            let index_sidecar = &mut active.sidecar;
            index_sidecar.normalize();
            if !index_sidecar.has_work() {
                continue;
            }
            let old_root = mutable_file
                .secondary_index_root(index_no)
                .change_context(RuntimeError::CheckpointExecution)
                .attach_with(|| {
                    format!(
                        "operation=apply_secondary_checkpoint_sidecar, phase=load_index_root, table_id={}, index_no={index_no}",
                        self.table_id()
                    )
                })?;
            let runtime = layout.secondary_index(index_no)?.disk_runtime();
            let new_root = match index_sidecar {
                SecondaryIndexSidecar::Unique { puts, deletes, .. } => {
                    // Use one writer per affected index so same-run puts and
                    // conditional deletes produce a single new DiskTree root.
                    let tree = runtime.open_unique_at(old_root, &disk_pool_guard)?;
                    let mut writer = tree.batch_writer(mutable_file, checkpoint_ts);
                    let put_entries = puts
                        .iter()
                        .map(|entry| UniqueDiskTreeEncodedPut {
                            key: &entry.key,
                            row_id: entry.row_id,
                        })
                        .collect::<Vec<_>>();
                    writer.batch_put_encoded(&put_entries);
                    // Deletes for keys with same-run puts were filtered during
                    // accumulation; remaining deletes must still match the old
                    // checkpointed owner before removing the key.
                    let delete_entries = deletes
                        .iter()
                        .map(|entry| UniqueDiskTreeEncodedDelete {
                            key: &entry.key,
                            expected_old_row_id: entry.row_id,
                        })
                        .collect::<Vec<_>>();
                    writer.batch_conditional_delete_encoded(&delete_entries);
                    writer.finish().await?
                }
                SecondaryIndexSidecar::NonUnique {
                    inserts, deletes, ..
                } => {
                    // Non-unique roots are exact-entry sets. Inserts and
                    // deletes are independent facts keyed by (key, row_id).
                    let tree = runtime.open_non_unique_at(old_root, &disk_pool_guard)?;
                    let mut writer = tree.batch_writer(mutable_file, checkpoint_ts);
                    let insert_entries = inserts
                        .iter()
                        .map(|key| NonUniqueDiskTreeEncodedExact { key })
                        .collect::<Vec<_>>();
                    let delete_entries = deletes
                        .iter()
                        .map(|key| NonUniqueDiskTreeEncodedExact { key })
                        .collect::<Vec<_>>();
                    writer.batch_insert_encoded(&insert_entries)?;
                    writer.batch_exact_delete_encoded(&delete_entries)?;
                    writer.finish().await?
                }
            };
            if new_root != old_root {
                // The root update stays private to mutable_file until the
                // final table-file commit publishes every checkpoint change.
                mutable_file
                    .set_secondary_index_root(index_no, new_root)
                    .change_context(RuntimeError::CheckpointExecution)
                    .attach_with(|| {
                        format!(
                            "operation=apply_secondary_checkpoint_sidecar, phase=set_index_root, table_id={}, index_no={index_no}",
                            self.table_id()
                        )
                    })?;
            }
        }
        Ok(())
    }

    async fn collect_deleted_secondary_sidecar(
        &self,
        entry: &ColumnLeafEntry,
        row_ids: &[RowID],
        delete_deltas: &[u32],
        metadata: &TableMetadata,
        secondary_sidecar: &mut SecondaryCheckpointSidecar,
    ) -> RuntimeResult<()> {
        if secondary_sidecar.indexes.is_empty() || delete_deltas.is_empty() {
            return Ok(());
        }

        // ColumnBlockIndex supplies the authoritative row-id ordering for this
        // persisted block, which maps deletion deltas back to row indexes.
        if row_ids.len() != entry.row_count() as usize
            || row_ids.windows(2).any(|window| window[0] >= window[1])
        {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "invalid persisted row id set: block_id={}, row_count={}, row_id_count={}",
                    entry.block_id(),
                    entry.row_count(),
                    row_ids.len()
                ))
                .change_context(RuntimeError::CheckpointExecution)
                .attach(format!(
                    "operation=collect_deleted_secondary_sidecar, table_id={}, block_id={}",
                    self.table_id(),
                    entry.block_id()
                )));
        }
        let dense_row_ids = row_ids.len() == entry.row_id_span() as usize
            && row_ids
                .iter()
                .enumerate()
                .all(|(idx, row_id)| *row_id == entry.start_row_id + idx as u64);

        let disk_pool = self.disk_pool();
        let disk_pool_guard = disk_pool.pool_guard();
        // Decode the persisted LWC block once for this block group, then derive
        // all secondary delete keys from the selected row indexes.
        let file_kind = self.file().file_kind();
        let block_id = entry.block_id();
        let persisted = self
            .storage
            .load_lwc_block(&disk_pool_guard, block_id)
            .await?;
        let block = persisted.block();
        if block.row_count() != row_ids.len()
            || block.row_shape_fingerprint() != entry.row_shape_fingerprint()
        {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "LWC block metadata mismatch: block_id={}, block_row_count={}, expected_row_count={}, block_fingerprint={}, expected_fingerprint={}",
                    entry.block_id(),
                    block.row_count(),
                    row_ids.len(),
                    block.row_shape_fingerprint(),
                    entry.row_shape_fingerprint()
                ))
                .change_context(RuntimeError::CheckpointExecution)
                .attach(format!(
                    "operation=collect_deleted_secondary_sidecar, table_id={}, block_id={block_id}",
                    self.table_id()
                )));
        }

        let mut sparse_row_idx = 0usize;
        for delta in delete_deltas {
            let row_id = entry
                .start_row_id
                .checked_add(u64::from(*delta))
                .ok_or_else(|| Report::new(DataIntegrityError::InvalidPayload))
                .attach_with(|| {
                    format!(
                        "delete delta overflows row id: start_row_id={}, delta={delta}",
                        entry.start_row_id
                    )
                })
                .change_context(RuntimeError::CheckpointExecution)
                .attach_with(|| {
                    format!(
                        "operation=collect_deleted_secondary_sidecar, table_id={}, block_id={block_id}",
                        self.table_id()
                    )
                })?;
            let row_idx = if dense_row_ids {
                usize::try_from(*delta)
                    .change_context(DataIntegrityError::InvalidPayload)
                    .attach_with(|| format!("delete delta does not fit usize: delta={delta}"))
                    .change_context(RuntimeError::CheckpointExecution)
                    .attach_with(|| {
                        format!(
                            "operation=collect_deleted_secondary_sidecar, table_id={}, block_id={block_id}",
                            self.table_id()
                        )
                    })?
            } else {
                while row_ids
                    .get(sparse_row_idx)
                    .is_some_and(|current| *current < row_id)
                {
                    sparse_row_idx += 1;
                }
                if row_ids.get(sparse_row_idx) != Some(&row_id) {
                    return Err(Report::new(DataIntegrityError::InvalidPayload)
                        .attach(format!(
                            "delete delta does not map to row id: row_id={row_id}, delta={delta}"
                        ))
                        .change_context(RuntimeError::CheckpointExecution)
                        .attach(format!(
                            "operation=collect_deleted_secondary_sidecar, table_id={}, block_id={block_id}",
                            self.table_id()
                        )));
                }
                sparse_row_idx
            };
            if row_idx >= row_ids.len() {
                return Err(Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!(
                        "delete delta row index out of bounds: row_idx={row_idx}, row_count={}",
                        row_ids.len()
                    ))
                    .change_context(RuntimeError::CheckpointExecution)
                    .attach(format!(
                        "operation=collect_deleted_secondary_sidecar, table_id={}, block_id={block_id}",
                        self.table_id()
                    )));
            }
            for sidecar_pos in 0..secondary_sidecar.indexes.len() {
                let (index_no, key) = {
                    let active = &secondary_sidecar.indexes[sidecar_pos];
                    let key = block
                        .decode_row_values(metadata.col.as_ref(), row_idx, active.key_cols.as_ref())
                        .change_context(RuntimeError::CheckpointExecution)
                        .attach_with(|| {
                            format!(
                                "operation=collect_deleted_secondary_sidecar, table_id={}, file={file_kind}, block=lwc_block, block_id={block_id}, row_idx={row_idx}",
                                self.table_id()
                            )
                        })?;
                    (active.index_no, key)
                };
                secondary_sidecar.add_deleted_key_at(sidecar_pos, index_no, row_id, key);
            }
        }
        Ok(())
    }

    #[inline]
    fn active_root_checkpoint_delay(&self, session: &SessionPin) -> Option<CheckpointDelayReason> {
        let active_root = self.file().active_root_unchecked();
        let min_active_sts = session.engine.trx_sys.calc_min_active_sts_for_gc();
        let effective_ts = active_root.effective_ts();
        (effective_ts >= min_active_sts).then_some(CheckpointDelayReason::ActiveRoot {
            table_id: self.table_id(),
            effective_ts,
            min_active_sts,
        })
    }

    /// Wait until the predicate represented by a checkpoint delay may be retried.
    pub(crate) async fn wait_for_checkpoint_retry(
        &self,
        session: &SessionPin,
        reason: CheckpointDelayReason,
    ) -> RuntimeOrFatalResult<()> {
        match reason {
            CheckpointDelayReason::ActiveRoot {
                table_id,
                effective_ts,
                ..
            } => {
                debug_assert_eq!(table_id, self.table_id());
                self.wait_for_active_root_retry(session, effective_ts).await
            }
            CheckpointDelayReason::FrozenPageCutoff {
                table_id, page_id, ..
            } => {
                debug_assert_eq!(table_id, self.table_id());
                self.wait_for_frozen_page_retry(session, page_id).await
            }
        }
    }

    async fn wait_for_active_root_retry(
        &self,
        session: &SessionPin,
        effective_ts: TrxID,
    ) -> RuntimeOrFatalResult<()> {
        let trx_sys = &session.engine.trx_sys;
        loop {
            ensure_maintenance_wait_running(session, "wait for active-root checkpoint retry")?;
            if self.lifecycle.inspect_terminal() != TableTerminal::Live
                || self.file().active_root_unchecked().effective_ts() != effective_ts
                || trx_sys.published_gc_horizon() > effective_ts
            {
                return Ok(());
            }

            trx_sys.request_purge_observation();
            let horizon_listener = trx_sys.gc_horizon_listener();
            let lifecycle_listener = self.lifecycle.listener();
            let poison_listener = session.engine.poisoner.listener();
            let shutdown_listener = session.engine.shutdown_listener();

            ensure_maintenance_wait_running(session, "wait for active-root checkpoint retry")?;
            if self.lifecycle.inspect_terminal() != TableTerminal::Live
                || self.file().active_root_unchecked().effective_ts() != effective_ts
                || trx_sys.published_gc_horizon() > effective_ts
            {
                return Ok(());
            }
            select_all(vec![
                horizon_listener,
                lifecycle_listener,
                poison_listener,
                shutdown_listener,
            ])
            .await;
        }
    }

    async fn wait_for_frozen_page_retry(
        &self,
        session: &SessionPin,
        page_id: PageID,
    ) -> RuntimeOrFatalResult<()> {
        ensure_maintenance_wait_running(session, "wait for frozen-page checkpoint retry")?;
        let mut attempt = match self.checkpoint_workflow.begin_checkpoint(&self.lifecycle) {
            Ok(attempt) => attempt,
            Err(_) => return Ok(()),
        };
        let Some(batch) = attempt.batch_mut() else {
            return Ok(());
        };
        if batch.table_id != self.table_id() {
            return Ok(());
        }
        let Some(page_idx) = batch.pages.iter().position(|page| page.page_id == page_id) else {
            return Ok(());
        };
        let page_info = batch.pages[page_idx];
        let guards = session.pool_guards();
        let mut page_guards = self
            .load_frozen_pages_for_transition(&guards, &[page_info])
            .await?;
        let page_guard = page_guards
            .pop()
            .expect("one-page frozen readiness load must return one page");
        let trx_sys = &session.engine.trx_sys;
        let mut reanalyze = matches!(
            batch.validation[page_idx],
            FrozenPageValidationState::Unchecked
        );

        loop {
            ensure_maintenance_wait_running(session, "wait for frozen-page checkpoint retry")?;
            if self.lifecycle.inspect_terminal() != TableTerminal::Live {
                return Ok(());
            }
            if reanalyze {
                let cutoff_ts = trx_sys.published_gc_horizon();
                self.refresh_frozen_page_readiness(&page_guard, batch, page_idx, cutoff_ts);
                reanalyze = false;
            }

            match batch.validation[page_idx] {
                FrozenPageValidationState::Unchecked => {
                    reanalyze = true;
                }
                FrozenPageValidationState::Blocked { .. } => {
                    let blockers = batch.blockers_for(page_id).expect(
                        "blocked frozen page must retain its complete transaction blocker set",
                    );
                    let mut listeners = Vec::with_capacity(blockers.len() + 3);
                    for blocker in blockers {
                        if let Some(listener) = blocker.terminal_listener() {
                            listeners.push(listener);
                        }
                    }
                    if listeners.is_empty() {
                        reanalyze = true;
                        continue;
                    }
                    listeners.push(self.lifecycle.listener());
                    listeners.push(session.engine.poisoner.listener());
                    listeners.push(session.engine.shutdown_listener());

                    ensure_maintenance_wait_running(
                        session,
                        "wait for frozen-page checkpoint retry",
                    )?;
                    if self.lifecycle.inspect_terminal() != TableTerminal::Live {
                        return Ok(());
                    }
                    if blockers.iter().all(|blocker| blocker.terminal()) {
                        reanalyze = true;
                        continue;
                    }
                    select_all(listeners).await;
                }
                FrozenPageValidationState::Stable { required_cutoff_ts } => {
                    let cutoff_ts = trx_sys.published_gc_horizon();
                    if required_cutoff_ts.is_none_or(|required| required <= cutoff_ts) {
                        return Ok(());
                    }

                    trx_sys.request_purge_observation();
                    let listeners = vec![
                        trx_sys.gc_horizon_listener(),
                        self.lifecycle.listener(),
                        session.engine.poisoner.listener(),
                        session.engine.shutdown_listener(),
                    ];
                    ensure_maintenance_wait_running(
                        session,
                        "wait for frozen-page checkpoint retry",
                    )?;
                    if self.lifecycle.inspect_terminal() != TableTerminal::Live {
                        return Ok(());
                    }
                    if required_cutoff_ts
                        .is_some_and(|required| trx_sys.published_gc_horizon() >= required)
                    {
                        continue;
                    }
                    select_all(listeners).await;
                }
            }
        }
    }

    /// Claim and freeze a contiguous hot-page prefix up to the requested row budget.
    pub(crate) async fn freeze(
        &self,
        session: &SessionPin,
        max_rows: usize,
    ) -> RuntimeResult<FreezeOutcome> {
        let mut attempt = match self.checkpoint_workflow.begin_freeze(&self.lifecycle) {
            Ok(attempt) => attempt,
            Err(outcome) => return Ok(outcome),
        };
        let _root_mutation_lease = match self.try_begin_checkpoint_root_mutation() {
            Ok(lease) => lease,
            Err(reason) => return Ok(FreezeOutcome::Cancelled { reason }),
        };
        let guards = session.pool_guards();
        let mut rows = 0usize;
        let mut pages = Vec::new();
        let mut reached_row_budget = false;
        let mut heap_redo_start_ts = None;
        self.mem_scan(&guards, |page_guard| {
            if reached_row_budget {
                heap_redo_start_ts = Some(page_guard.unwrap_vmap().create_cts());
                return false;
            }
            let page = page_guard.page();
            rows += page.header.approx_non_deleted();
            pages.push(FrozenPage {
                page_id: page_guard.page_id(),
                start_row_id: page.header.start_row_id,
                end_row_id: page.header.start_row_id + page.header.max_row_count as u64,
            });
            reached_row_budget = rows >= max_rows;
            true
        })
        .await?;
        let page_guards = self
            .load_frozen_pages_for_transition(&guards, &pages)
            .await?;
        #[cfg(test)]
        test_hooks::run_test_freeze_after_loading_hook().await;
        let publish =
            self.validate_and_publish_loaded_pages_frozen(&page_guards, &pages, &mut attempt);
        if !publish {
            return Ok(attempt.cancelled(&self.lifecycle));
        }
        // The fence is allocated only after every selected page has published
        // FROZEN under its state lock.
        let frozen_ts = session.engine.trx_sys.allocate_snapshot_fence();
        let batch =
            FrozenPageBatch::new(self.table_id(), frozen_ts, heap_redo_start_ts, rows, pages);
        Ok(attempt.finish(batch, &self.lifecycle))
    }

    async fn heap_redo_start_from(
        &self,
        guards: &PoolGuards,
        start_row_id: RowID,
    ) -> RuntimeResult<Option<TrxID>> {
        let mut heap_redo_start_ts = None;
        self.mem
            .scan_from(guards, start_row_id, |page_guard| {
                heap_redo_start_ts = Some(page_guard.unwrap_vmap().create_cts());
                false
            })
            .await?;
        Ok(heap_redo_start_ts)
    }

    async fn build_lwc_blocks<C>(
        &self,
        metadata: &TableMetadata,
        guards: &PoolGuards,
        prepared_pages: &[Option<PreparedTransitionPage>],
        mut collect_visible_row: Option<C>,
    ) -> RuntimeResult<Vec<LwcBlockPersist>>
    where
        C: FnMut(&RowPage, usize, RowID),
    {
        #[cfg(test)]
        use super::test_hooks as table_test_hooks;

        #[cfg(test)]
        table_test_hooks::maybe_force_lwc_build_error()?;
        let mut lwc_blocks = Vec::new();
        if !prepared_pages.is_empty() {
            let mut builder = LwcBuilder::new(metadata.col.as_ref());
            let mut current_start = RowID::new(0);
            let mut current_end = RowID::new(0);
            for prepared in prepared_pages {
                let Some(prepared) = prepared.as_ref() else {
                    return Err(Report::new(InternalError::LwcBuilderMisuse)
                        .attach("transitioned page has no prepared visibility plan")
                        .change_context(RuntimeError::CheckpointExecution)
                        .attach(format!(
                            "operation=build_lwc_blocks, table_id={}",
                            self.table_id()
                        )));
                };
                let page_guard = self
                    .mem
                    .must_get_row_page_shared(guards, prepared.page_id)
                    .await?;
                let page = page_guard.page();
                assert_eq!(page.header.start_row_id, prepared.start_row_id);
                let view = page
                    .vector_view_with_del_bitmap(metadata.col.as_ref(), prepared.del_bitmap.clone())
                    .change_context(RuntimeError::CheckpointExecution)
                    .attach_with(|| {
                        format!(
                            "operation=build_lwc_blocks, phase=build_vector_view, table_id={}, page_id={}",
                            self.table_id(), prepared.page_id
                        )
                    })?;
                if view.rows_non_deleted() == 0 {
                    continue;
                }
                if let Some(collect_visible_row) = collect_visible_row.as_mut() {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        for row_idx in start_idx..end_idx {
                            collect_visible_row(page, row_idx, page.row_id(row_idx));
                        }
                    }
                }
                if builder.is_empty() {
                    current_start = prepared.start_row_id;
                    current_end = prepared.end_row_id;
                }
                if !builder.append_view(view, prepared.start_row_id) {
                    if builder.is_empty() {
                        return Err(Report::new(InternalError::LwcBuilderMisuse)
                            .attach(format!(
                                "single row page does not fit in LWC block: page_id={}",
                                prepared.page_id
                            ))
                            .change_context(RuntimeError::CheckpointExecution)
                            .attach(format!(
                                "operation=build_lwc_blocks, phase=append_page, table_id={}",
                                self.table_id()
                            )));
                    }
                    let shape = ColumnBlockEntryShape::new(
                        current_start,
                        current_end,
                        builder.row_ids().to_vec(),
                        Vec::new(),
                    );
                    let buf = builder
                        .build(shape.row_shape_fingerprint())
                        .change_context(RuntimeError::CheckpointExecution)
                        .attach_with(|| {
                            format!(
                                "operation=build_lwc_blocks, phase=encode_block, table_id={}, start_row_id={current_start}, end_row_id={current_end}",
                                self.table_id()
                            )
                        })?;
                    lwc_blocks.push(LwcBlockPersist { shape, buf });
                    builder = LwcBuilder::new(metadata.col.as_ref());
                    current_start = prepared.start_row_id;
                    current_end = prepared.end_row_id;
                    let view = page
                        .vector_view_with_del_bitmap(
                            metadata.col.as_ref(),
                            prepared.del_bitmap.clone(),
                        )
                        .change_context(RuntimeError::CheckpointExecution)
                        .attach_with(|| {
                            format!(
                                "operation=build_lwc_blocks, phase=rebuild_vector_view, table_id={}, page_id={}",
                                self.table_id(), prepared.page_id
                            )
                        })?;
                    if !builder.append_view(view, prepared.start_row_id) {
                        return Err(Report::new(InternalError::LwcBuilderMisuse)
                            .attach(format!(
                                "single row page does not fit in LWC block: page_id={}",
                                prepared.page_id
                            ))
                            .change_context(RuntimeError::CheckpointExecution)
                            .attach(format!(
                                "operation=build_lwc_blocks, phase=append_page, table_id={}",
                                self.table_id()
                            )));
                    }
                } else {
                    current_end = prepared.end_row_id;
                }
            }
            if !builder.is_empty() {
                let shape = ColumnBlockEntryShape::new(
                    current_start,
                    current_end,
                    builder.row_ids().to_vec(),
                    Vec::new(),
                );
                let buf = builder
                    .build(shape.row_shape_fingerprint())
                    .change_context(RuntimeError::CheckpointExecution)
                    .attach_with(|| {
                        format!(
                            "operation=build_lwc_blocks, phase=encode_final_block, table_id={}, start_row_id={current_start}, end_row_id={current_end}",
                            self.table_id()
                        )
                    })?;
                lwc_blocks.push(LwcBlockPersist { shape, buf });
            }
        }
        Ok(lwc_blocks)
    }

    /// Execute one user-table checkpoint attempt against table-owned workflow state.
    pub(crate) async fn checkpoint(
        &self,
        session: &SessionPin,
    ) -> RuntimeOrFatalResult<CheckpointOutcome> {
        let table_id = self.table_id();
        let result = match self.checkpoint_workflow.begin_checkpoint(&self.lifecycle) {
            Ok(attempt) => {
                let mut checkpointer = TableCheckpointer::new(self, session, attempt);
                let result = checkpointer.run().await;
                checkpointer.resolve(result)
            }
            Err(reason) => Ok(CheckpointOutcome::Cancelled { reason }),
        };
        result
            .inspect(|outcome| match outcome {
                CheckpointOutcome::Published {
                    checkpoint_ts,
                    redo_cts,
                    silent,
                } => obs::info!(
                    "event=checkpoint_publish component=table table_id={} action=publish result=ok checkpoint_ts={} redo_cts={} silent={}",
                    table_id,
                    checkpoint_ts,
                    redo_cts,
                    silent
                ),
                CheckpointOutcome::Delayed { reason } => obs::warn!(
                    "event=checkpoint_publish component=table table_id={} action=delay result=delayed reason={:?}",
                    table_id,
                    reason
                ),
                CheckpointOutcome::Cancelled { reason } => obs::warn!(
                    "event=checkpoint_publish component=table table_id={} action=cancel result=cancelled reason={:?}",
                    table_id,
                    reason
                ),
            })
            .inspect_err(|err| {
                obs::error!(
                    "event=checkpoint_publish component=table table_id={} action=error result=error error={:?}",
                    table_id,
                    err
                );
            })
    }
}

#[inline]
fn silent_watermark_floor(
    active_root: &ActiveRoot,
    mutable_root: &ActiveRoot,
) -> Option<TableRedoReplayFloor> {
    // Any checkpointed table-file data/state change must update one of these
    // logical root fields before this decision:
    // - pivot_row_id for persisted row coverage,
    // - column_block_index_root for LWC inserts or cold-delete rewrites,
    // - secondary_index_roots for secondary DiskTree checkpoint work,
    // - metadata for table/index shape changes,
    // - alloc_map for CoW reachability/allocation changes.
    // Replay bounds are intentionally excluded because those are the catalog
    // watermark payload. `meta_block_id` is also excluded because the normal
    // table checkpoint path reserves the publish meta block only after this
    // silent-vs-root-publication decision.
    let table_file_work = active_root.pivot_row_id != mutable_root.pivot_row_id
        || active_root.column_block_index_root != mutable_root.column_block_index_root
        || active_root.secondary_index_roots != mutable_root.secondary_index_roots
        || active_root.metadata != mutable_root.metadata
        || active_root.alloc_map != mutable_root.alloc_map;
    if table_file_work {
        return None;
    }
    // Root fields can be unchanged while checkpoint STS or delete cutoff
    // progress advances replay bounds; publish that through a catalog watermark.
    Some(TableRedoReplayFloor {
        heap_redo_start_ts: mutable_root.heap_redo_start_ts,
        deletion_cutoff_ts: mutable_root.deletion_cutoff_ts,
    })
}

#[inline]
fn ensure_maintenance_wait_running(
    session: &SessionPin,
    operation: &'static str,
) -> RuntimeOrFatalResult<()> {
    session.engine.poisoner.ensure_healthy()?;
    if session.engine.shutdown_started() {
        return Err(RuntimeOrFatalError::from(
            Report::new(LifecycleError::Shutdown)
                .change_context(RuntimeError::CheckpointExecution)
                .attach(format!("{operation}: engine shutdown started")),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    pub(crate) mod test_hooks {
        use crate::error::{FatalError, FatalResult, RuntimeError, RuntimeResult};
        use error_stack::Report;
        use std::cell::{Cell, RefCell};
        use std::future::Future;
        use std::pin::Pin;

        type TableHook = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + 'static>> + 'static>;
        type FallibleTableHook = Box<
            dyn FnOnce() -> Pin<Box<dyn Future<Output = RuntimeResult<()>> + 'static>> + 'static,
        >;

        thread_local! {
            static TEST_FORCE_SECONDARY_SIDECAR_ERROR: Cell<bool> = const { Cell::new(false) };
            static TEST_FORCE_POST_PUBLISH_CHECKPOINT_ERROR: Cell<bool> = const { Cell::new(false) };
            static TEST_FORCE_CHECKPOINT_COMMIT_ERROR: Cell<bool> = const { Cell::new(false) };
            static TEST_FREEZE_AFTER_LOADING_HOOK: RefCell<Option<TableHook>> =
                const { RefCell::new(None) };
            static TEST_CHECKPOINT_AFTER_TRX_START_HOOK: RefCell<Option<TableHook>> =
                const { RefCell::new(None) };
            static TEST_CHECKPOINT_AFTER_PUBLISH_ADMISSION_HOOK: RefCell<Option<TableHook>> =
                const { RefCell::new(None) };
            static TEST_SILENT_WATERMARK_MUTATION_HOOK: RefCell<Option<FallibleTableHook>> =
                const { RefCell::new(None) };
        }

        pub(crate) fn set_test_force_secondary_sidecar_error(enabled: bool) {
            TEST_FORCE_SECONDARY_SIDECAR_ERROR.with(|flag| flag.set(enabled));
        }

        pub(crate) fn maybe_force_secondary_sidecar_error() -> RuntimeResult<()> {
            if TEST_FORCE_SECONDARY_SIDECAR_ERROR.with(|flag| flag.get()) {
                return Err(Report::new(RuntimeError::CheckpointExecution)
                    .attach("injected secondary-index sidecar failure"));
            }
            Ok(())
        }

        pub(crate) fn set_test_force_post_publish_checkpoint_error(enabled: bool) {
            TEST_FORCE_POST_PUBLISH_CHECKPOINT_ERROR.with(|flag| flag.set(enabled));
        }

        pub(crate) struct ForcePostPublishCheckpointErrorGuard;

        impl ForcePostPublishCheckpointErrorGuard {
            pub(crate) fn new() -> Self {
                set_test_force_post_publish_checkpoint_error(true);
                Self
            }
        }

        impl Drop for ForcePostPublishCheckpointErrorGuard {
            fn drop(&mut self) {
                set_test_force_post_publish_checkpoint_error(false);
            }
        }

        pub(crate) fn maybe_force_post_publish_checkpoint_error() -> FatalResult<()> {
            if TEST_FORCE_POST_PUBLISH_CHECKPOINT_ERROR.with(Cell::get) {
                return Err(Report::new(FatalError::CheckpointWrite)
                    .attach("forced post-publication table checkpoint failure"));
            }
            Ok(())
        }

        pub(crate) struct ForceCheckpointCommitErrorGuard;

        impl ForceCheckpointCommitErrorGuard {
            pub(crate) fn new() -> Self {
                TEST_FORCE_CHECKPOINT_COMMIT_ERROR.with(|flag| flag.set(true));
                Self
            }
        }

        impl Drop for ForceCheckpointCommitErrorGuard {
            fn drop(&mut self) {
                TEST_FORCE_CHECKPOINT_COMMIT_ERROR.with(|flag| flag.set(false));
            }
        }

        pub(crate) fn maybe_force_checkpoint_commit_error() -> FatalResult<()> {
            if TEST_FORCE_CHECKPOINT_COMMIT_ERROR.with(Cell::get) {
                return Err(Report::new(FatalError::CheckpointWrite)
                    .attach("forced table checkpoint commit failure"));
            }
            Ok(())
        }

        pub(crate) fn set_test_freeze_after_loading_hook<F, Fut>(hook: F)
        where
            F: FnOnce() -> Fut + 'static,
            Fut: Future<Output = ()> + 'static,
        {
            TEST_FREEZE_AFTER_LOADING_HOOK.with(|slot| {
                let old = slot
                    .borrow_mut()
                    .replace(Box::new(move || Box::pin(hook())));
                assert!(old.is_none(), "freeze loading hook already installed");
            });
        }

        pub(crate) fn set_test_checkpoint_after_trx_start_hook<F, Fut>(hook: F)
        where
            F: FnOnce() -> Fut + 'static,
            Fut: Future<Output = ()> + 'static,
        {
            TEST_CHECKPOINT_AFTER_TRX_START_HOOK.with(|slot| {
                let old = slot
                    .borrow_mut()
                    .replace(Box::new(move || Box::pin(hook())));
                assert!(
                    old.is_none(),
                    "checkpoint transaction-start hook already installed"
                );
            });
        }

        pub(crate) fn set_test_checkpoint_after_publish_admission_hook<F, Fut>(hook: F)
        where
            F: FnOnce() -> Fut + 'static,
            Fut: Future<Output = ()> + 'static,
        {
            TEST_CHECKPOINT_AFTER_PUBLISH_ADMISSION_HOOK.with(|slot| {
                let old = slot
                    .borrow_mut()
                    .replace(Box::new(move || Box::pin(hook())));
                assert!(
                    old.is_none(),
                    "checkpoint publish-admission hook already installed"
                );
            });
        }

        pub(crate) fn set_test_silent_watermark_mutation_hook<F, Fut>(hook: F)
        where
            F: FnOnce() -> Fut + 'static,
            Fut: Future<Output = RuntimeResult<()>> + 'static,
        {
            TEST_SILENT_WATERMARK_MUTATION_HOOK.with(|slot| {
                let old = slot
                    .borrow_mut()
                    .replace(Box::new(move || Box::pin(hook())));
                assert!(
                    old.is_none(),
                    "silent watermark mutation hook already installed"
                );
            });
        }

        pub(crate) async fn run_test_checkpoint_after_trx_start_hook() {
            let hook = TEST_CHECKPOINT_AFTER_TRX_START_HOOK.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook().await;
            }
        }

        pub(crate) async fn run_test_checkpoint_after_publish_admission_hook() {
            let hook =
                TEST_CHECKPOINT_AFTER_PUBLISH_ADMISSION_HOOK.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook().await;
            }
        }

        pub(crate) async fn run_test_silent_watermark_mutation_hook() -> RuntimeResult<()> {
            let hook = TEST_SILENT_WATERMARK_MUTATION_HOOK.with(|slot| slot.borrow_mut().take());
            match hook {
                Some(hook) => hook().await,
                None => Ok(()),
            }
        }

        pub(crate) async fn run_test_freeze_after_loading_hook() {
            let hook = TEST_FREEZE_AFTER_LOADING_HOOK.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook().await;
            }
        }
    }

    use super::*;
    use crate::buffer::BufferPool;
    use crate::buffer::guard::PageSharedGuard;
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableSpec};
    use crate::conf::TrxSysConfig;
    use crate::engine::Engine;
    use crate::error::{
        Error, FatalError, LifecycleError, OperationError, RuntimeError, RuntimeOrFatalError,
    };
    use crate::file::cow_file::tests::old_root_drop_count;
    use crate::index::{RowLocation, UniqueIndex};
    use crate::io::install_storage_backend_test_hook;
    use crate::row::ops::{SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
    use crate::session::{
        Session,
        tests::{
            SessionTestExt, assert_checkpoint_published, wait_for_checkpoint_purge,
            wait_for_checkpoint_root_ready,
        },
    };
    use crate::table::checkpoint_workflow::FrozenPageValidationState;
    use crate::table::persistence::test_hooks::{
        ForceCheckpointCommitErrorGuard, ForcePostPublishCheckpointErrorGuard,
        set_test_checkpoint_after_publish_admission_hook, set_test_checkpoint_after_trx_start_hook,
        set_test_force_secondary_sidecar_error, set_test_freeze_after_loading_hook,
        set_test_silent_watermark_mutation_hook,
    };
    use crate::table::test_hooks::{
        ForceLwcBuildErrorGuard, set_test_freeze_page_state_locked_hook,
        set_test_frozen_page_row_scan_hook, set_test_frozen_page_scan_hook,
        set_test_frozen_pages_ready_hook, set_test_hot_row_write_before_state_lock_hook,
        set_test_locked_page_plan_rebuild_hook, set_test_optimistic_page_plan_comparison_hook,
        set_test_stable_page_plans_refreshed_hook, set_test_transition_page_published_hook,
    };
    use crate::table::tests::*;
    use crate::table::{DeleteMarker, TableTerminal};
    use crate::trx::Transaction;
    use crate::trx::stmt::tests as stmt_tests;
    use crate::trx::tests::discard_transaction_after_fatal_rollback;
    use crate::trx::undo::{OwnedRowUndo, RowUndoHead, RowUndoKind};
    use crate::trx::ver_map::RowPageState;
    use crate::trx::{MIN_ACTIVE_TRX_ID, SharedTrxStatus};
    use futures::FutureExt;
    use futures::future::pending;
    use std::cell::{Cell, RefCell};
    use std::cmp::Ordering;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::task::Poll;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn test_maintenance_wait_stacks_shutdown_under_checkpoint_runtime() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "maintenance-wait-shutdown").await;
            let session = engine.new_session().unwrap();
            let pin = session.pin().unwrap();

            thread::scope(|scope| {
                let shutdown = scope.spawn(|| engine.shutdown());
                while !pin.engine.shutdown_started() {
                    thread::yield_now();
                }

                let err =
                    ensure_maintenance_wait_running(&pin, "test maintenance wait").unwrap_err();
                let RuntimeOrFatalError::Runtime(err) = err else {
                    panic!("shutdown must remain a recoverable checkpoint Runtime failure");
                };
                assert_eq!(*err.current_context(), RuntimeError::CheckpointExecution);
                assert_eq!(
                    err.downcast_ref::<LifecycleError>().copied(),
                    Some(LifecycleError::Shutdown)
                );

                drop(pin);
                shutdown.join().unwrap().unwrap();
            });
        });
    }

    #[test]
    fn test_maintenance_wait_preserves_existing_fatal_reason() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "maintenance-wait-poison").await;
            let session = engine.new_session().unwrap();
            let pin = session.pin().unwrap();
            engine
                .inner()
                .poisoner
                .poison(Report::new(FatalError::RedoWrite).attach("test redo poison"));

            let err = ensure_maintenance_wait_running(&pin, "test maintenance wait").unwrap_err();
            let RuntimeOrFatalError::Fatal(err) = err else {
                panic!("existing poison must remain Fatal");
            };
            assert_eq!(*err.current_context(), FatalError::RedoWrite);

            drop(pin);
            drop(session);
            engine.shutdown().unwrap();
        });
    }

    async fn row_page_states(table: &Table, guards: &PoolGuards) -> Vec<RowPageState> {
        let mut states = Vec::new();
        table
            .mem_scan(guards, |page_guard| {
                states.push(page_guard.unwrap_vmap().inspect_state());
                true
            })
            .await
            .unwrap();
        states
    }

    async fn hot_page_create_timestamps(table: &Table, guards: &PoolGuards) -> Vec<TrxID> {
        let mut timestamps = Vec::new();
        table
            .mem_scan(guards, |page_guard| {
                timestamps.push(page_guard.unwrap_vmap().create_cts());
                true
            })
            .await
            .unwrap();
        timestamps
    }

    async fn hot_page_ids(table: &Table, guards: &PoolGuards) -> Vec<PageID> {
        let mut page_ids = Vec::new();
        table
            .mem_scan(guards, |page_guard| {
                page_ids.push(page_guard.page_id());
                true
            })
            .await
            .unwrap();
        page_ids
    }

    async fn hot_page_id_for_key(table: &Table, session: &Session, key: &SelectKey) -> PageID {
        let guards = session.pool_guards();
        let index = bound_unique_index(table, &guards, key.index_no);
        let (row_id, _) = index
            .lookup(&key.vals, session.last_cts())
            .await
            .unwrap()
            .expect("test key should resolve to a hot row");
        match table.find_row(&guards, row_id).await.unwrap() {
            RowLocation::RowPage(page_id) => page_id,
            RowLocation::NotFound => panic!("test row should exist"),
            RowLocation::LwcBlock { .. } => panic!("test row should still be hot"),
        }
    }

    async fn hot_page_state(table: &Table, guards: &PoolGuards, page_id: PageID) -> RowPageState {
        let page_guard = table
            .mem
            .must_get_row_page_shared(guards, page_id)
            .await
            .unwrap();
        page_guard.unwrap_vmap().inspect_state()
    }

    fn delete_last_frozen_row_image(page_guard: PageSharedGuard<RowPage>) {
        let page = page_guard.page();
        let map = page_guard.unwrap_vmap();
        assert_eq!(map.inspect_state(), RowPageState::Frozen);
        let row_idx = page.header.row_count() - 1;
        assert!(
            map.read_latch(row_idx).is_none(),
            "raw frozen-page image delete requires purged row undo"
        );
        let mut access = page_guard.write_row(row_idx);
        access.delete_row();
    }

    async fn wait_session_idle(engine: &Engine, session: &Session) {
        loop {
            let observed_epoch = engine.inner().session_registry.trx_change_epoch();
            if !session.in_trx().unwrap() {
                return;
            }
            engine
                .inner()
                .session_registry
                .wait_for_trx_change_since_async(observed_epoch)
                .await;
        }
    }

    async fn assert_drop_waits_for_no_page_publication(
        engine: &Engine,
        table_id: TableID,
        checkpoint_session: &mut Session,
        expected_silent: bool,
    ) {
        let table = table_for_internal_assertion(engine, table_id);
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        set_test_checkpoint_after_publish_admission_hook(move || async move {
            entered_tx.send_async(()).await.unwrap();
            release_rx.recv_async().await.unwrap();
        });

        let checkpoint_guards = checkpoint_session.pool_guards();
        let checkpoint = checkpoint_session.checkpoint_table(table_id).fuse();
        futures::pin_mut!(checkpoint);
        let entered = entered_rx.recv_async().fuse();
        futures::pin_mut!(entered);
        futures::select! {
            result = checkpoint => {
                panic!("checkpoint completed before publish-admission hook: {result:?}");
            }
            result = entered => result.unwrap(),
        }
        assert_eq!(table.checkpoint_workflow.state_name(), "Publishing");
        let states = row_page_states(&table, &checkpoint_guards).await;
        assert!(states.iter().all(|state| *state == RowPageState::Active));

        let mut competing_session = engine.new_session().unwrap();
        assert_eq!(
            competing_session
                .freeze_table(table_id, usize::MAX)
                .await
                .unwrap(),
            FreezeOutcome::Cancelled {
                reason: CheckpointCancelReason::CheckpointInProgress,
            }
        );
        assert_eq!(
            competing_session.checkpoint_table(table_id).await.unwrap(),
            CheckpointOutcome::Cancelled {
                reason: CheckpointCancelReason::CheckpointInProgress,
            }
        );

        let mut drop_session = engine.new_session().unwrap();
        let checkpoint_redo_cts = {
            let drop_table = drop_session.drop_table(table_id).fuse();
            futures::pin_mut!(drop_table);
            assert!(matches!(
                futures::poll!(drop_table.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Live);
            assert_eq!(table.checkpoint_workflow.state_name(), "Publishing");

            release_tx.send_async(()).await.unwrap();
            let outcome = checkpoint.await.unwrap();
            let CheckpointOutcome::Published {
                redo_cts, silent, ..
            } = outcome
            else {
                panic!("admitted checkpoint should publish: {outcome:?}");
            };
            assert_eq!(silent, expected_silent);
            drop_table.await.unwrap();
            redo_cts
        };
        assert!(checkpoint_redo_cts < drop_session.last_cts());
        assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Dropped);
    }

    #[test]
    fn test_unique_sidecar_normalize_keeps_latest_put_and_suppresses_delete() {
        let mut puts = vec![
            EncodedRowEntry {
                key: b"b".to_vec(),
                row_id: RowID::new(20),
            },
            EncodedRowEntry {
                key: b"a".to_vec(),
                row_id: RowID::new(10),
            },
            EncodedRowEntry {
                key: b"a".to_vec(),
                row_id: RowID::new(11),
            },
        ];
        let mut deletes = vec![
            EncodedRowEntry {
                key: b"a".to_vec(),
                row_id: RowID::new(9),
            },
            EncodedRowEntry {
                key: b"c".to_vec(),
                row_id: RowID::new(30),
            },
            EncodedRowEntry {
                key: b"c".to_vec(),
                row_id: RowID::new(30),
            },
        ];

        normalize_unique_puts(&mut puts);
        normalize_unique_deletes(&mut deletes, &puts);

        assert_eq!(
            puts,
            vec![
                EncodedRowEntry {
                    key: b"a".to_vec(),
                    row_id: RowID::new(11),
                },
                EncodedRowEntry {
                    key: b"b".to_vec(),
                    row_id: RowID::new(20),
                },
            ]
        );
        assert_eq!(
            deletes,
            vec![EncodedRowEntry {
                key: b"c".to_vec(),
                row_id: RowID::new(30),
            }]
        );
    }

    #[test]
    fn test_non_unique_sidecar_normalize_delete_wins_exact_key() {
        let mut sidecar = SecondaryIndexSidecar::NonUnique {
            encoder: BTreeKeyEncoder::new(vec![ValType::new(ValKind::U8, false)]),
            inserts: vec![b"b".to_vec(), b"a".to_vec(), b"b".to_vec()],
            deletes: vec![b"c".to_vec(), b"b".to_vec(), b"b".to_vec()],
        };

        sidecar.normalize();

        let SecondaryIndexSidecar::NonUnique {
            inserts, deletes, ..
        } = sidecar
        else {
            panic!("expected non-unique sidecar");
        };
        assert_eq!(inserts, vec![b"a".to_vec()]);
        assert_eq!(deletes, vec![b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn test_checkpoint_persists_committed_cold_delete_markers() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(6i32);
            let reader = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id =
                assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
            reader.commit().await.unwrap();

            expect_delete_committed(table_id, &mut session, &key).await;
            let marker = table.deletion_buffer().get(row_id).unwrap();
            let marker_ts = delete_marker_ts(marker);
            session.wait_for_gc_horizon_after(marker_ts).await.unwrap();
            let pool_guards = session.pool_guards();
            let snapshot_before = column_block_index_snapshot(&engine, table_id);
            let index_before = snapshot_before.index(pool_guards.disk_guard());
            let entry_before = index_before
                .locate_block(row_id)
                .await
                .unwrap()
                .expect("persisted entry should exist before delete checkpoint");

            assert_checkpoint_published(&mut session, table_id).await;

            let pool_guards = session.pool_guards();
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let active_root = &snapshot.active_root;
            let index = snapshot.index(pool_guards.disk_guard());
            let entry = index
                .locate_block(row_id)
                .await
                .unwrap()
                .expect("persisted entry should exist");
            assert!(active_root.deletion_cutoff_ts > marker_ts);
            assert_eq!(entry.block_id(), entry_before.block_id());
            assert_eq!(entry.end_row_id(), entry_before.end_row_id());
            assert_eq!(entry.row_id_span(), entry_before.row_id_span());
            assert_eq!(entry.row_count(), entry_before.row_count());
            let deltas = index.load_delete_deltas(&entry).await.unwrap();
            let expected_delta = (row_id - entry.start_row_id) as u32;
            assert!(deltas.contains(&expected_delta));
        });
    }

    #[test]
    fn test_checkpoint_publishes_unique_secondary_disk_tree_root() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 3, "name").await;

            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let active_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert_ne!(active_root.secondary_index_roots[0], SUPER_BLOCK_ID);
            let reader = session.begin_trx().unwrap();
            for key_value in 0..3 {
                let key = single_key(key_value);
                let row_id = assert_row_in_lwc(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &key,
                    reader.sts(),
                )
                .await;
                assert_eq!(
                    unique_disk_tree_lookup(
                        &table_for_internal_assertion(&engine, table_id),
                        &session.pool_guards(),
                        &key
                    )
                    .await,
                    Some(row_id)
                );
            }
            reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_trx_read_proof_root_snapshot_captures_active_root() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            let mut trx = session.begin_trx().unwrap();
            for id in 0..8 {
                trx = expect_trx_insert(
                    table_id,
                    trx,
                    vec![Val::from(id), Val::from(format!("v{id}").as_str())],
                )
                .await;
            }
            trx.commit().await.unwrap();
            assert_checkpoint_published(&mut session, table_id).await;

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                let proof = rt.read_proof();
                let snapshot =
                    table_for_internal_assertion(&engine, table_id).root_snapshot(&proof);
                let _effects_addr = effects as *mut _;
                table_for_internal_assertion(&engine, table_id).with_active_root(
                    &proof,
                    |active_root| {
                        assert_eq!(snapshot.root_ts(), active_root.root_ts);
                        assert_eq!(snapshot.pivot_row_id(), active_root.pivot_row_id);
                        assert_eq!(
                            snapshot.column_block_index_root(),
                            active_root.column_block_index_root
                        );
                        assert_eq!(
                            snapshot.deletion_cutoff_ts(),
                            active_root.deletion_cutoff_ts
                        );
                        assert_eq!(
                            snapshot.secondary_index_root(0).unwrap(),
                            active_root.secondary_index_roots[0]
                        );
                        assert_eq!(
                            snapshot.root_is_visible_to(rt.sts()),
                            active_root.effective_ts() < rt.sts()
                        );
                    },
                );
                Ok(())
            })
            .await
            .unwrap();
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_build_lwc_blocks_rejects_oversized_first_page() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "oversized-first-lwc-page").await;
            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    TableSpec::new(vec![ColumnSpec::new(
                        "payload",
                        ValKind::VarByte,
                        ColumnAttributes::empty(),
                    )]),
                    vec![],
                )
                .await
                .unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let metadata = table.metadata();
            let guards = session.pool_guards();
            let page_guard = table.mem.try_get_insert_page(&guards, 1).await.unwrap();
            let page = page_guard.page();
            let payload_len =
                page.header.var_field_offset() - usize::from(page.header.fix_field_end);
            let values = [Val::from(vec![0xab; payload_len])];
            assert!(page.insert(metadata.col.as_ref(), &values).is_ok());
            let prepared = PreparedTransitionPage {
                page_id: page_guard.page_id(),
                start_row_id: page.header.start_row_id,
                end_row_id: page.header.start_row_id + u64::from(page.header.max_row_count),
                cutoff_ts: TrxID::new(1),
                observed_version: 0,
                required_cutoff_ts: None,
                del_bitmap: page.del_bitmap(page.header.row_count()),
                overlay_markers: Vec::new(),
            };
            let page_id = prepared.page_id;
            drop(page_guard);

            let err = match table
                .build_lwc_blocks(
                    &metadata,
                    &guards,
                    &[Some(prepared)],
                    None::<fn(&RowPage, usize, RowID)>,
                )
                .await
            {
                Ok(_) => panic!("oversized first row page should fail LWC block build"),
                Err(err) => err,
            };
            assert_eq!(
                err.downcast_ref::<InternalError>().copied(),
                Some(InternalError::LwcBuilderMisuse)
            );
            let report = format!("{err:?}");
            assert!(
                report.contains("single row page does not fit in LWC block"),
                "{report}"
            );
            assert!(report.contains(&format!("page_id={page_id}")), "{report}");
        });
    }

    #[test]
    fn test_checkpoint_publishes_non_unique_secondary_disk_tree_entries_across_lwc_splits() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique")
                    .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "split-name-".repeat(120);
            let row_count = 80;
            insert_rows(table_id, &mut session, 0, row_count, &name).await;

            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            session
                .wait_for_gc_horizon_after(session.last_cts())
                .await
                .unwrap();
            wait_for_checkpoint_root_ready(&session, table_id).await;
            let frozen_page_count = table_for_internal_assertion(&engine, table_id)
                .checkpoint_workflow
                .frozen_page_ids()
                .unwrap()
                .len();
            let analysis_count = Rc::new(Cell::new(0usize));
            let hook_analysis_count = Rc::clone(&analysis_count);
            set_test_frozen_page_scan_hook(move |_| {
                hook_analysis_count.set(hook_analysis_count.get() + 1);
            });
            let analysis_count_at_build = Rc::new(Cell::new(usize::MAX));
            let hook_analysis_count = Rc::clone(&analysis_count);
            let hook_analysis_count_at_build = Rc::clone(&analysis_count_at_build);
            set_test_checkpoint_after_publish_admission_hook(move || async move {
                hook_analysis_count_at_build.set(hook_analysis_count.get());
            });
            let outcome = session.checkpoint_table(table_id).await.unwrap();
            assert!(matches!(outcome, CheckpointOutcome::Published { .. }));
            assert!(analysis_count.get() >= frozen_page_count);
            assert_eq!(analysis_count.get(), analysis_count_at_build.get());

            let name_key = name_key(&name);
            let table = table_for_internal_assertion(&engine, table_id);
            let row_ids =
                non_unique_disk_tree_prefix_scan(&table, &session.pool_guards(), &name_key).await;
            assert_eq!(row_ids.len(), row_count as usize);

            let first_key = single_key(0i32);
            let last_key = single_key(row_count - 1);
            let first_row_id = unique_disk_tree_lookup(&table, &session.pool_guards(), &first_key)
                .await
                .unwrap();
            let last_row_id = unique_disk_tree_lookup(&table, &session.pool_guards(), &last_key)
                .await
                .unwrap();
            let pool_guards = session.pool_guards();
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let column_index = snapshot.index(pool_guards.disk_guard());
            let first_entry = column_index
                .locate_block(first_row_id)
                .await
                .unwrap()
                .unwrap();
            let last_entry = column_index
                .locate_block(last_row_id)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(first_entry.block_id(), last_entry.block_id());
        });
    }

    #[test]
    fn test_checkpoint_prepared_bitmap_without_secondary_indexes() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "checkpoint-no-index").await;
            let mut ddl_session = engine.new_session().unwrap();
            let table_id = ddl_session
                .create_table(
                    TableSpec::new(vec![
                        ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                        ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                    ]),
                    vec![],
                )
                .await
                .unwrap();
            drop(ddl_session);

            let mut session = engine.new_session().unwrap();
            let mut row_ids = Vec::new();
            for id in 0..12 {
                row_ids.push(
                    insert_one_row(
                        table_id,
                        &mut session,
                        vec![Val::from(id), Val::from(format!("v{id}").as_str())],
                    )
                    .await,
                );
            }
            let redo_cts = session.last_cts();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            wait_for_checkpoint_purge(&session, redo_cts).await;
            wait_for_checkpoint_root_ready(&session, table_id).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let page_ids = table.checkpoint_workflow.frozen_page_ids().unwrap();
            assert_eq!(page_ids.len(), 1);
            let page_guard = table
                .mem
                .must_get_row_page_shared(&session.pool_guards(), page_ids[0])
                .await
                .unwrap();
            let deleted_row_id = page_guard
                .page()
                .row_id(page_guard.page().header.row_count() - 1);
            let hook_page = Rc::new(RefCell::new(Some(page_guard)));
            let refreshed_hook_page = Rc::clone(&hook_page);
            let refreshed_hook_ran = Rc::new(Cell::new(false));
            let hook_refreshed_hook_ran = Rc::clone(&refreshed_hook_ran);
            set_test_stable_page_plans_refreshed_hook(move || {
                hook_refreshed_hook_ran.set(true);
                let page_guard = refreshed_hook_page.borrow_mut().take().unwrap();
                delete_last_frozen_row_image(page_guard);
            });

            let outcome = session.checkpoint_table(table_id).await.unwrap();
            assert!(matches!(outcome, CheckpointOutcome::Published { .. }));
            assert!(refreshed_hook_ran.get());
            assert!(
                table
                    .file()
                    .active_root_unchecked()
                    .secondary_index_roots
                    .is_empty()
            );
            for row_id in row_ids
                .iter()
                .copied()
                .filter(|row_id| *row_id != deleted_row_id)
            {
                assert!(matches!(
                    table
                        .find_row(&session.pool_guards(), row_id)
                        .await
                        .unwrap(),
                    RowLocation::LwcBlock { .. }
                ));
            }
            assert!(matches!(
                table
                    .find_row(&session.pool_guards(), deleted_row_id)
                    .await
                    .unwrap(),
                RowLocation::NotFound
            ));
            let mut reader = session.begin_trx().unwrap();
            let rows = scan_table_pairs(&mut reader, table_id).await;
            reader.commit().await.unwrap();
            assert_eq!(rows.len(), row_ids.len() - 1);
            assert!(!rows.iter().any(|(id, _)| *id == 11));
        });
    }

    #[test]
    fn test_deletion_checkpoint_updates_secondary_disk_tree_roots() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique")
                    .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 2, "same-name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let delete_key = single_key(0i32);
            let keep_key = single_key(1i32);
            let deleted_row_id = unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &delete_key,
            )
            .await
            .unwrap();
            let kept_row_id = unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &keep_key,
            )
            .await
            .unwrap();

            expect_delete_committed(table_id, &mut session, &delete_key).await;
            let marker_ts = delete_marker_ts(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(deleted_row_id)
                    .unwrap(),
            );
            session.wait_for_gc_horizon_after(marker_ts).await.unwrap();
            assert_checkpoint_published(&mut session, table_id).await;

            assert_eq!(
                unique_disk_tree_lookup(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &delete_key
                )
                .await,
                None
            );
            assert_eq!(
                unique_disk_tree_lookup(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &keep_key
                )
                .await,
                Some(kept_row_id)
            );
            let exact_rows = non_unique_disk_tree_prefix_scan(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &name_key("same-name"),
            )
            .await;
            assert_eq!(exact_rows, vec![kept_row_id]);
        });
    }

    #[test]
    fn test_unique_checkpoint_overlap_keeps_new_disk_tree_owner() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let old_row_id = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1i32), Val::from("old")],
            )
            .await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(1i32);
            expect_delete_committed(table_id, &mut session, &key).await;
            let delete_ts = delete_marker_ts(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(old_row_id)
                    .unwrap(),
            );
            let new_row_id = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1i32), Val::from("new")],
            )
            .await;

            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let readiness_ts = delete_ts.max(
                table_for_internal_assertion(&engine, table_id)
                    .file()
                    .active_root_unchecked()
                    .root_ts,
            );
            session
                .wait_for_gc_horizon_after(readiness_ts)
                .await
                .unwrap();
            assert_checkpoint_published(&mut session, table_id).await;

            assert_eq!(
                unique_disk_tree_lookup(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &key
                )
                .await,
                Some(new_row_id)
            );
        });
    }

    #[test]
    fn test_secondary_sidecar_failure_keeps_checkpoint_root_atomic() {
        struct ResetSidecarHook;

        impl Drop for ResetSidecarHook {
            fn drop(&mut self) {
                set_test_force_secondary_sidecar_error(false);
            }
        }

        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 2, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(0i32);
            let row_id = unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &key,
            )
            .await
            .unwrap();
            expect_delete_committed(table_id, &mut session, &key).await;
            let marker_ts = delete_marker_ts(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(row_id)
                    .unwrap(),
            );
            session.wait_for_gc_horizon_after(marker_ts).await.unwrap();
            let root_before = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            wait_for_checkpoint_root_ready(&session, table_id).await;

            set_test_force_secondary_sidecar_error(true);
            let _reset = ResetSidecarHook;
            session.checkpoint_table(table_id).await.unwrap_err();

            let root_after = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert_eq!(
                root_after.deletion_cutoff_ts,
                root_before.deletion_cutoff_ts
            );
            assert_eq!(
                root_after.column_block_index_root,
                root_before.column_block_index_root
            );
            assert_eq!(
                root_after.secondary_index_roots,
                root_before.secondary_index_roots
            );
        });
    }

    #[test]
    fn test_checkpoint_all_deleted_row_page_advances_without_column_index() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            delete_key_range_and_wait_gc_cutoff(table_id, &mut session, 0, 10).await;

            let root_before = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let root_after = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert!(root_after.pivot_row_id > root_before.pivot_row_id);
            assert_eq!(root_after.column_block_index_root, SUPER_BLOCK_ID);
            assert!(root_after.deletion_cutoff_ts > root_before.deletion_cutoff_ts);
            for i in 0..10 {
                expect_select_not_found_committed(table_id, &mut session, &single_key(i)).await;
            }
        });
    }

    #[test]
    fn test_checkpoint_transition_delete_marker_waits_for_next_cutoff_range() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;

            let key = single_key(0i32);
            let reader = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let pool_guards = session.pool_guards();
            let index = bound_unique_index(&table, &pool_guards, key.index_no);
            let (row_id, _) = index
                .lookup(&key.vals, reader.sts())
                .await
                .unwrap()
                .expect("row should exist before delete");
            assert!(matches!(
                table
                    .find_row(&session.pool_guards(), row_id)
                    .await
                    .unwrap(),
                RowLocation::RowPage(_)
            ));
            reader.commit().await.unwrap();

            let mut hold_session = engine.new_session().unwrap();
            let hold_trx = hold_session.begin_trx().unwrap();
            let hold_sts = hold_trx.sts();

            let mut writer_session = engine.new_session().unwrap();
            expect_delete_committed(table_id, &mut writer_session, &key).await;
            assert!(table.deletion_buffer().get(row_id).is_none());

            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let mut checkpoint_session = engine.new_session().unwrap();
            assert_checkpoint_published(&mut checkpoint_session, table_id).await;

            let marker = table.deletion_buffer().get(row_id).unwrap();
            let delete_cts = delete_marker_ts(marker);
            assert!(delete_cts >= hold_sts);

            let snapshot_after_first = column_block_index_snapshot(&engine, table_id);
            let root_after_first = &snapshot_after_first.active_root;
            let pool_guards = session.pool_guards();
            let index_after_first = snapshot_after_first.index(pool_guards.disk_guard());
            let entry_after_first = index_after_first
                .locate_block(row_id)
                .await
                .unwrap()
                .expect("transition snapshot should persist the row into LWC");
            assert!(root_after_first.deletion_cutoff_ts <= delete_cts);
            assert!(
                index_after_first
                    .load_delete_deltas(&entry_after_first)
                    .await
                    .unwrap()
                    .is_empty()
            );

            hold_trx.rollback().await.unwrap();
            checkpoint_session
                .wait_for_gc_horizon_after(delete_cts)
                .await
                .unwrap();
            assert_checkpoint_published(&mut checkpoint_session, table_id).await;

            let snapshot_after_second = column_block_index_snapshot(&engine, table_id);
            let root_after_second = &snapshot_after_second.active_root;
            let pool_guards = session.pool_guards();
            let index_after_second = snapshot_after_second.index(pool_guards.disk_guard());
            let entry_after_second = index_after_second
                .locate_block(row_id)
                .await
                .unwrap()
                .expect("persisted entry should still exist");
            let deltas = index_after_second
                .load_delete_deltas(&entry_after_second)
                .await
                .unwrap();
            let expected_delta = (row_id - entry_after_second.start_row_id) as u32;
            assert!(root_after_second.deletion_cutoff_ts > delete_cts);
            assert!(deltas.contains(&expected_delta));
        });
    }

    #[test]
    fn test_checkpoint_fails_when_eligible_delete_marker_has_no_column_index() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "name").await;
            delete_key_range_and_wait_gc_cutoff(table_id, &mut session, 0, 4).await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let root_before = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert!(root_before.pivot_row_id > RowID::new(0));
            assert_eq!(root_before.column_block_index_root, SUPER_BLOCK_ID);
            let marker_ts = root_before.deletion_cutoff_ts;
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .put_committed(RowID::new(0), marker_ts)
                .unwrap();
            session.wait_for_gc_horizon_after(marker_ts).await.unwrap();
            wait_for_checkpoint_root_ready(&session, table_id).await;

            let err = session.checkpoint_table(table_id).await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidRootInvariant)
            );
            let root_after = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert_eq!(
                root_after.deletion_cutoff_ts,
                root_before.deletion_cutoff_ts
            );
            assert_eq!(
                root_after.column_block_index_root,
                root_before.column_block_index_root
            );
        });
    }

    #[test]
    fn test_checkpoint_fails_when_eligible_delete_marker_cannot_be_located() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(0i32);
            let reader = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id =
                assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
            reader.commit().await.unwrap();

            let snapshot_before = column_block_index_snapshot(&engine, table_id);
            let root_before = &snapshot_before.active_root;
            assert_ne!(root_before.column_block_index_root, SUPER_BLOCK_ID);
            let missing_row_id = row_id + 1;
            assert!(missing_row_id < root_before.pivot_row_id);
            let pool_guards = session.pool_guards();
            let index = snapshot_before.index(pool_guards.disk_guard());
            assert!(index.locate_block(missing_row_id).await.unwrap().is_none());

            let marker_ts = root_before.deletion_cutoff_ts;
            table
                .deletion_buffer()
                .put_committed(missing_row_id, marker_ts)
                .unwrap();
            session.wait_for_gc_horizon_after(marker_ts).await.unwrap();
            wait_for_checkpoint_root_ready(&session, table_id).await;

            let err = session.checkpoint_table(table_id).await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidRootInvariant)
            );
            assert_eq!(
                table.file().active_root_unchecked().deletion_cutoff_ts,
                root_before.deletion_cutoff_ts
            );
        });
    }

    #[test]
    fn test_checkpoint_ignores_missing_old_delete_marker_below_previous_cutoff() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(0i32);
            let reader = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id =
                assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
            reader.commit().await.unwrap();

            let root_before = table.file().active_root_unchecked().clone();
            assert!(root_before.deletion_cutoff_ts > TrxID::new(0));
            let missing_row_id = row_id + 1;
            assert!(missing_row_id < root_before.pivot_row_id);
            let old_marker_ts = root_before.deletion_cutoff_ts.saturating_sub(1);
            table
                .deletion_buffer()
                .put_committed(missing_row_id, old_marker_ts)
                .unwrap();
            session
                .wait_for_gc_horizon_after(root_before.deletion_cutoff_ts)
                .await
                .unwrap();

            assert_checkpoint_published(&mut session, table_id).await;
            let root_after = table.file().active_root_unchecked().clone();
            if root_after.deletion_cutoff_ts <= root_before.deletion_cutoff_ts {
                let watermark = engine
                    .catalog()
                    .storage
                    .table_replay_silent_watermarks()
                    .find_uncommitted_by_table_id(&session.pool_guards(), table_id)
                    .await
                    .unwrap()
                    .expect("silent checkpoint should write a watermark");
                assert!(watermark.deletion_cutoff_ts > root_before.deletion_cutoff_ts);
                insert_rows(table_id, &mut session, 1, 2, "durability-anchor").await;
                session.checkpoint_catalog().await.unwrap();
            }
            let effective = engine.catalog().effective_user_table_redo_replay_floor(
                table_id,
                table.redo_replay_floor_snapshot(),
            );
            assert!(
                effective.deletion_cutoff_ts > root_before.deletion_cutoff_ts,
                "{effective:?}"
            );
        });
    }

    #[test]
    fn test_checkpoint_skips_cold_delete_markers_at_or_after_cutoff() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(7i32);
            let reader = session.begin_trx().unwrap();
            let row_id = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &key,
                reader.sts(),
            )
            .await;
            reader.commit().await.unwrap();

            let mut hold_session = engine.new_session().unwrap();
            let hold_trx = hold_session.begin_trx().unwrap();
            let hold_sts = hold_trx.sts();

            let mut writer_session = engine.new_session().unwrap();
            expect_delete_committed(table_id, &mut writer_session, &key).await;

            let table = table_for_internal_assertion(&engine, table_id);
            let marker = table.deletion_buffer().get(row_id).unwrap();
            let delete_cts = match marker {
                DeleteMarker::Committed(ts) => ts,
                DeleteMarker::Ref(status) => status.ts(),
            };
            assert!(delete_cts >= hold_sts);

            let mut checkpoint_session = engine.new_session().unwrap();
            assert_checkpoint_published(&mut checkpoint_session, table_id).await;

            let snapshot = column_block_index_snapshot(&engine, table_id);
            let active_root = &snapshot.active_root;
            let pool_guards = session.pool_guards();
            let index = snapshot.index(pool_guards.disk_guard());
            let entry = index
                .locate_block(row_id)
                .await
                .unwrap()
                .expect("persisted entry should exist");
            assert!(active_root.deletion_cutoff_ts <= delete_cts);
            assert!(index.load_delete_deltas(&entry).await.unwrap().is_empty());

            hold_trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_checkpoint_fails_on_invalid_v2_delete_metadata() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key1 = single_key(6i32);
            let reader = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id1 =
                assert_row_in_lwc(&table, &session.pool_guards(), &key1, reader.sts()).await;
            reader.commit().await.unwrap();

            expect_delete_committed(table_id, &mut session, &key1).await;
            let marker1 = table.deletion_buffer().get(row_id1).unwrap();
            let marker1_ts = delete_marker_ts(marker1);
            session.wait_for_gc_horizon_after(marker1_ts).await.unwrap();
            assert_checkpoint_published(&mut session, table_id).await;

            let pool_guards = session.pool_guards();
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let index = snapshot.index(pool_guards.disk_guard());
            let entry = index
                .locate_block(row_id1)
                .await
                .unwrap()
                .expect("persisted entry should exist");

            let key2 = single_key(7i32);
            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            let row_id2 =
                assert_row_in_lwc(&table, &reader_session.pool_guards(), &key2, reader.sts()).await;
            reader.commit().await.unwrap();
            let entry2 = index
                .locate_block(row_id2)
                .await
                .unwrap()
                .expect("second persisted entry should exist");
            assert_eq!(entry2.leaf_block_id, entry.leaf_block_id);
            drop(reader_session);

            expect_delete_committed(table_id, &mut session, &key2).await;
            let marker2 = table.deletion_buffer().get(row_id2).unwrap();
            let marker2_ts = delete_marker_ts(marker2);
            session.wait_for_gc_horizon_after(marker2_ts).await.unwrap();
            wait_for_checkpoint_root_ready(&session, table_id).await;

            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            corrupt_leaf_delete_codec(table_file_path, entry.leaf_block_id, 0);
            let _ = table
                .disk_pool()
                .invalidate_block(table.file().sparse_file().file_id(), entry.leaf_block_id);

            let err = session.checkpoint_table(table_id).await.unwrap_err();
            assert_table_data_integrity(
                err,
                "column_block_index",
                entry.leaf_block_id,
                DataIntegrityError::InvalidPayload,
            );
        });
    }

    #[test]
    fn test_checkpoint_fails_on_short_v2_delete_section_header() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key1 = single_key(6i32);
            let reader = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id1 =
                assert_row_in_lwc(&table, &session.pool_guards(), &key1, reader.sts()).await;
            reader.commit().await.unwrap();

            expect_delete_committed(table_id, &mut session, &key1).await;
            let marker1 = table.deletion_buffer().get(row_id1).unwrap();
            let marker1_ts = delete_marker_ts(marker1);
            session.wait_for_gc_horizon_after(marker1_ts).await.unwrap();
            assert_checkpoint_published(&mut session, table_id).await;

            let pool_guards = session.pool_guards();
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let index = snapshot.index(pool_guards.disk_guard());
            let entry = index
                .locate_block(row_id1)
                .await
                .unwrap()
                .expect("persisted entry should exist");

            let key2 = single_key(7i32);
            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            let row_id2 =
                assert_row_in_lwc(&table, &reader_session.pool_guards(), &key2, reader.sts()).await;
            reader.commit().await.unwrap();
            let entry2 = index
                .locate_block(row_id2)
                .await
                .unwrap()
                .expect("second persisted entry should exist");
            assert_eq!(entry2.leaf_block_id, entry.leaf_block_id);
            drop(reader_session);

            expect_delete_committed(table_id, &mut session, &key2).await;
            let marker2 = table.deletion_buffer().get(row_id2).unwrap();
            let marker2_ts = delete_marker_ts(marker2);
            session.wait_for_gc_horizon_after(marker2_ts).await.unwrap();
            wait_for_checkpoint_root_ready(&session, table_id).await;

            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            corrupt_leaf_short_delete_section_header(table_file_path, entry.leaf_block_id, 0);
            let _ = table
                .disk_pool()
                .invalidate_block(table.file().sparse_file().file_id(), entry.leaf_block_id);

            let err = session.checkpoint_table(table_id).await.unwrap_err();
            assert_table_data_integrity(
                err,
                "column_block_index",
                entry.leaf_block_id,
                DataIntegrityError::InvalidPayload,
            );
        });
    }

    #[test]
    fn test_checkpoint_basic_flow() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "x".repeat(1024);
            insert_rows(table_id, &mut session, 0, 200, &name).await;

            let table = table_for_internal_assertion(&engine, table_id);
            let old_root = table.file().active_root_unchecked().clone();
            let batch =
                assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert!(!batch.is_empty());
            session
                .wait_for_gc_horizon_after(session.last_cts())
                .await
                .unwrap();

            let outcome = session.checkpoint_table(table_id).await.unwrap();
            assert!(
                matches!(outcome, CheckpointOutcome::Published { silent: false, .. }),
                "{outcome:?}"
            );
            assert!(engine.inner().poisoner.poison_error().is_none());

            let new_root = table.file().active_root_unchecked().clone();
            assert!(new_root.pivot_row_id > old_root.pivot_row_id);
            assert_ne!(new_root.column_block_index_root, SUPER_BLOCK_ID);
            assert!(new_root.deletion_cutoff_ts > old_root.deletion_cutoff_ts);
        });
    }

    #[test]
    fn test_partial_freeze_uses_successor_page_heap_redo_start() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "p".repeat(1024);
            insert_rows(table_id, &mut session, 0, 200, &name).await;

            let table = table_for_internal_assertion(&engine, table_id);
            let create_timestamps =
                hot_page_create_timestamps(&table, &session.pool_guards()).await;
            assert!(create_timestamps.len() > 1);
            let batch = assert_freeze_created(session.freeze_table(table_id, 1).await.unwrap());
            assert_eq!(batch.page_count(), 1);

            session
                .wait_for_gc_horizon_after(session.last_cts())
                .await
                .unwrap();
            assert_checkpoint_published(&mut session, table_id).await;
            assert_eq!(
                table.file().active_root_unchecked().heap_redo_start_ts,
                create_timestamps[1]
            );
        });
    }

    #[test]
    fn test_freeze_all_resolves_later_successor_heap_redo_start() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 40, "before-freeze").await;

            let table = table_for_internal_assertion(&engine, table_id);
            let batch =
                assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            insert_rows(table_id, &mut session, 10_000, 1, "after-freeze").await;
            let create_timestamps =
                hot_page_create_timestamps(&table, &session.pool_guards()).await;
            assert_eq!(create_timestamps.len(), batch.page_count() + 1);
            let successor_create_ts = *create_timestamps.last().unwrap();
            assert!(successor_create_ts > batch.frozen_ts());

            session
                .wait_for_gc_horizon_after(session.last_cts())
                .await
                .unwrap();
            assert_checkpoint_published(&mut session, table_id).await;
            assert_eq!(
                table.file().active_root_unchecked().heap_redo_start_ts,
                successor_create_ts
            );
        });
    }

    #[test]
    fn test_freeze_all_without_successor_uses_checkpoint_ts() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 40, "freeze-all").await;

            let table = table_for_internal_assertion(&engine, table_id);
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            session
                .wait_for_gc_horizon_after(session.last_cts())
                .await
                .unwrap();
            let checkpoint_ts = assert_checkpoint_published(&mut session, table_id).await;
            assert_eq!(
                table.file().active_root_unchecked().heap_redo_start_ts,
                checkpoint_ts
            );
        });
    }

    #[test]
    fn test_empty_freeze_resolves_first_later_page_heap_redo_start() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let batch =
                assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert!(batch.is_empty());

            insert_rows(table_id, &mut session, 0, 1, "after-empty-freeze").await;
            let table = table_for_internal_assertion(&engine, table_id);
            let create_timestamps =
                hot_page_create_timestamps(&table, &session.pool_guards()).await;
            assert_eq!(create_timestamps.len(), 1);
            session
                .wait_for_gc_horizon_after(session.last_cts())
                .await
                .unwrap();

            let checkpoint_ts = assert_checkpoint_published(&mut session, table_id).await;
            let watermark = engine
                .catalog()
                .storage
                .table_replay_silent_watermarks()
                .find_uncommitted_by_table_id(&session.pool_guards(), table_id)
                .await
                .unwrap()
                .expect("empty-batch checkpoint should publish a silent watermark");
            assert_eq!(watermark.heap_redo_start_ts, create_timestamps[0]);
            assert!(watermark.heap_redo_start_ts < checkpoint_ts);
        });
    }

    #[test]
    fn test_repeated_freeze_returns_original_table_owned_batch() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 200, "repeat-freeze").await;

            let first = session.freeze_table(table_id, 1).await.unwrap();
            let FreezeOutcome::Frozen { batch: first } = first else {
                panic!("first freeze should install a canonical batch: {first:?}");
            };
            let repeated = session.freeze_table(table_id, usize::MAX).await.unwrap();
            let FreezeOutcome::AlreadyFrozen { batch: repeated } = repeated else {
                panic!("repeated freeze should return the original batch: {repeated:?}");
            };
            assert_eq!(repeated, first);

            let table = table_for_internal_assertion(&engine, table_id);
            let page_ids = table.checkpoint_workflow.frozen_page_ids().unwrap();
            assert_eq!(page_ids.len(), first.page_count());
            assert_eq!(table.checkpoint_workflow.state_name(), "Frozen");
        });
    }

    #[test]
    fn test_cancelled_freeze_loading_restores_idle_and_active_pages() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut first_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut first_session, 0, 200, "cancel-freeze").await;

            let (entered_tx, entered_rx) = flume::bounded(1);
            let (_release_tx, release_rx) = flume::bounded::<()>(1);
            set_test_freeze_after_loading_hook(move || async move {
                entered_tx.send_async(()).await.unwrap();
                release_rx.recv_async().await.unwrap();
            });

            {
                let freeze = first_session.freeze_table(table_id, usize::MAX).fuse();
                futures::pin_mut!(freeze);
                let entered = entered_rx.recv_async().fuse();
                futures::pin_mut!(entered);
                futures::select! {
                    result = freeze => {
                        panic!("freeze completed before the loading hook: {result:?}");
                    }
                    result = entered => result.unwrap(),
                }

                let mut competing_session = engine.new_session().unwrap();
                assert_eq!(
                    competing_session
                        .freeze_table(table_id, usize::MAX)
                        .await
                        .unwrap(),
                    FreezeOutcome::Cancelled {
                        reason: CheckpointCancelReason::FreezeInProgress,
                    }
                );
                assert_eq!(
                    competing_session.checkpoint_table(table_id).await.unwrap(),
                    CheckpointOutcome::Cancelled {
                        reason: CheckpointCancelReason::FreezeInProgress,
                    }
                );
            }

            let table = table_for_internal_assertion(&engine, table_id);
            assert_eq!(table.checkpoint_workflow.state_name(), "Idle");
            assert!(table.checkpoint_workflow.frozen_page_ids().is_none());
            let page_states = row_page_states(&table, &first_session.pool_guards()).await;
            assert!(!page_states.is_empty());
            assert!(
                page_states
                    .iter()
                    .all(|state| *state == RowPageState::Active)
            );
            assert!(matches!(
                first_session
                    .freeze_table(table_id, usize::MAX)
                    .await
                    .unwrap(),
                FreezeOutcome::Frozen { .. }
            ));
        });
    }

    #[test]
    fn test_new_and_recovered_table_workflow_is_idle_and_volatile() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir.clone(),
                "workflow-recovery",
            ))
            .await
            .unwrap();
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            assert_eq!(table.checkpoint_workflow.state_name(), "Idle");

            insert_rows(table_id, &mut session, 0, 8, "volatile-freeze").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_eq!(table.checkpoint_workflow.state_name(), "Frozen");

            drop(table);
            drop(session);
            drop(engine);

            let recovered = Engine::bootstrap(lightweight_test_engine_config(
                main_dir,
                "workflow-recovery",
            ))
            .await
            .unwrap();
            let session = recovered.new_session().unwrap();
            let table = table_for_internal_assertion(&recovered, table_id);
            assert_eq!(table.checkpoint_workflow.state_name(), "Idle");
            let states = row_page_states(&table, &session.pool_guards()).await;
            assert!(!states.is_empty());
            assert!(states.iter().all(|state| *state == RowPageState::Active));
        });
    }

    #[test]
    fn test_freeze_panics_on_non_active_selected_page() {
        for unexpected in [RowPageState::Frozen, RowPageState::Transition] {
            let panic = catch_unwind(AssertUnwindSafe(|| {
                smol::block_on(async {
                    let temp_dir = TempDir::new().unwrap();
                    let engine = lightweight_test_engine(&temp_dir, "freeze-invariant").await;
                    let table_id = create_table2_for_test(&engine).await;
                    let mut session = engine.new_session().unwrap();
                    insert_rows(table_id, &mut session, 0, 1, "invariant").await;
                    let table = table_for_internal_assertion(&engine, table_id);
                    let page_id = hot_page_ids(&table, &session.pool_guards()).await[0];
                    let page_guard = table
                        .mem
                        .must_get_row_page_shared(&session.pool_guards(), page_id)
                        .await
                        .unwrap();
                    *page_guard.unwrap_vmap().write_state() = unexpected;
                    drop(page_guard);

                    // The selected-page invariant must panic before an outcome is returned.
                    let _ = session.freeze_table(table_id, usize::MAX).await;
                });
            }))
            .expect_err("freeze must reject an unexpected selected-page state");
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            assert!(
                message.contains("admitted freeze requires ACTIVE page"),
                "unexpected panic for {unexpected:?}: {message}"
            );
        }
    }

    #[test]
    fn test_cancelled_checkpoint_before_publish_restores_source_state() {
        smol::block_on(async {
            for frozen_source in [false, true] {
                let temp_dir = TempDir::new().unwrap();
                let engine = lightweight_test_engine(&temp_dir, "cancel-checkpoint").await;
                let table_id = create_table2_for_test(&engine).await;
                let mut session = engine.new_session().unwrap();
                insert_rows(table_id, &mut session, 0, 8, "cancel").await;
                let table = table_for_internal_assertion(&engine, table_id);
                let frozen_page_ids = if frozen_source {
                    assert_freeze_created(
                        session.freeze_table(table_id, usize::MAX).await.unwrap(),
                    );
                    table.checkpoint_workflow.frozen_page_ids().unwrap()
                } else {
                    Vec::new()
                };
                wait_for_checkpoint_root_ready(&session, table_id).await;

                let (entered_tx, entered_rx) = flume::bounded(1);
                let (_release_tx, release_rx) = flume::bounded::<()>(1);
                set_test_checkpoint_after_trx_start_hook(move || async move {
                    entered_tx.send_async(()).await.unwrap();
                    release_rx.recv_async().await.unwrap();
                });
                {
                    let checkpoint = session.checkpoint_table(table_id).fuse();
                    futures::pin_mut!(checkpoint);
                    let entered = entered_rx.recv_async().fuse();
                    futures::pin_mut!(entered);
                    futures::select! {
                        result = checkpoint => {
                            panic!("checkpoint completed before cancellation: {result:?}");
                        }
                        result = entered => result.unwrap(),
                    }
                }

                if frozen_source {
                    assert_eq!(table.checkpoint_workflow.state_name(), "Frozen");
                    assert_eq!(
                        table.checkpoint_workflow.frozen_page_ids().unwrap(),
                        frozen_page_ids
                    );
                    let states = row_page_states(&table, &session.pool_guards()).await;
                    assert!(states.iter().all(|state| *state == RowPageState::Frozen));
                } else {
                    assert_eq!(table.checkpoint_workflow.state_name(), "Idle");
                    let states = row_page_states(&table, &session.pool_guards()).await;
                    assert!(states.iter().all(|state| *state == RowPageState::Active));
                }
                wait_session_idle(&engine, &session).await;
                drop(table);
                drop(session);
                engine.shutdown().unwrap();
            }
        });
    }

    #[test]
    fn test_page_by_page_freeze_isolates_writers_and_validates_later_page() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "freeze-page-race").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut setup = engine.new_session().unwrap();
            let value = "x".repeat(1024);
            insert_rows(table_id, &mut setup, 0, 200, &value).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let page_ids = hot_page_ids(&table, &setup.pool_guards()).await;
            assert!(page_ids.len() > 1);

            let first_page_id = page_ids[0];
            let later_page_id = page_ids[1];
            let mut first_key = None;
            let mut later_key = None;
            for key_value in 0..200 {
                let key = single_key(key_value);
                let page_id = hot_page_id_for_key(&table, &setup, &key).await;
                if page_id == first_page_id && first_key.is_none() {
                    first_key = Some(key);
                } else if page_id == later_page_id && later_key.is_none() {
                    later_key = Some(key);
                }
                if first_key.is_some() && later_key.is_some() {
                    break;
                }
            }
            let first_key = first_key.expect("first frozen page should contain a test key");
            let later_key = later_key.expect("later frozen page should contain a test key");

            let mut horizon_session = engine.new_session().unwrap();
            let horizon_trx = horizon_session.begin_trx().unwrap();
            let (locked_tx, locked_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let mut freeze_session = engine.new_session().unwrap();
            let freeze_handle = thread::spawn(move || {
                set_test_freeze_page_state_locked_hook(move |page_id| {
                    locked_tx.send(page_id).unwrap();
                    release_rx.recv().unwrap();
                });
                smol::block_on(freeze_session.freeze_table(table_id, usize::MAX)).unwrap()
            });

            assert_eq!(locked_rx.recv().unwrap(), first_page_id);
            let (writer_entered_tx, writer_entered_rx) = flume::bounded(1);
            let (writer_done_tx, writer_done_rx) = flume::bounded(1);
            let blocked_value = value.clone();
            let mut blocked_session = engine.new_session().unwrap();
            let mut blocked_trx = blocked_session.begin_trx().unwrap();
            let blocked_handle = thread::spawn(move || {
                set_test_hot_row_write_before_state_lock_hook(move || {
                    writer_entered_tx.send(()).unwrap();
                });
                smol::block_on(async {
                    let update = trx_update_row_by_id(
                        &mut blocked_trx,
                        table_id,
                        &first_key,
                        vec![UpdateCol {
                            idx: 1,
                            val: Val::from(blocked_value.as_str()),
                        }],
                    )
                    .await
                    .unwrap();
                    blocked_trx.rollback().await.unwrap();
                    writer_done_tx.send(()).unwrap();
                    update
                })
            });
            writer_entered_rx.recv().unwrap();
            assert!(matches!(
                writer_done_rx.try_recv(),
                Err(flume::TryRecvError::Empty)
            ));

            let mut later_session = engine.new_session().unwrap();
            let mut later_trx = later_session.begin_trx().unwrap();
            let later_sts = later_trx.sts();
            let later_value = "y".repeat(1024);
            let later_update = trx_update_row_by_id(
                &mut later_trx,
                table_id,
                &later_key,
                vec![UpdateCol {
                    idx: 1,
                    val: Val::from(later_value.as_str()),
                }],
            )
            .await
            .unwrap();
            assert!(matches!(later_update, UpdateMvcc::Updated(_)));
            let later_cts = later_trx.commit().await.unwrap();

            release_tx.send(()).unwrap();
            let blocked_result = blocked_handle.join().unwrap();
            assert!(matches!(blocked_result, UpdateMvcc::Updated(_)));
            writer_done_rx.recv().unwrap();
            let outcome = freeze_handle.join().unwrap();
            let batch = assert_freeze_created(outcome);
            assert!(later_sts < batch.frozen_ts());

            let mut checkpoint_session = engine.new_session().unwrap();
            let outcome = checkpoint_session.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Delayed { reason } = outcome else {
                panic!("later-page update should delay checkpoint: {outcome:?}");
            };
            let CheckpointDelayReason::FrozenPageCutoff {
                page_id,
                unresolved_status,
                ..
            } = reason
            else {
                panic!("expected frozen-page cutoff delay, got {reason:?}");
            };
            assert_eq!(page_id, later_page_id);
            assert!(!unresolved_status);
            assert_eq!(batch.page_count(), page_ids.len());

            horizon_trx.rollback().await.unwrap();
            checkpoint_session
                .wait_for_gc_horizon_after(later_cts)
                .await
                .unwrap();
            assert_checkpoint_published(&mut checkpoint_session, table_id).await;
        });
    }

    #[test]
    fn test_checkpoint_publish_write_failure_poisons_storage() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "publish-write-fail").await;
            delete_key_range_and_wait_gc_cutoff(table_id, &mut session, 0, 4).await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let table = table_for_internal_assertion(&engine, table_id);
            let root_before = table.file().active_root_unchecked().clone();
            wait_for_checkpoint_root_ready(&session, table_id).await;
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            let hook = Arc::new(FailingFirstWriteHook::new(table_file_path));
            let _install = install_storage_backend_test_hook(hook.clone());

            let err = session.checkpoint_table(table_id).await.unwrap_err();
            assert_checkpoint_write_poisoned(&err, &engine);
            assert!(hook.call_count() > 0);
            assert_root_metadata_unchanged(
                &root_before,
                &table_for_internal_assertion(&engine, table_id),
            );
            assert_eq!(table.checkpoint_workflow.state_name(), "Transition");
            assert!(table.checkpoint_workflow.frozen_page_ids().is_none());
            assert!(!session.in_trx().unwrap());
        });
    }

    #[test]
    fn test_checkpoint_post_publication_failure_poisons_storage() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "post-publish-fail").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let table = table_for_internal_assertion(&engine, table_id);
            let root_before = table.file().active_root_unchecked().clone();
            session
                .wait_for_gc_horizon_after(session.last_cts())
                .await
                .unwrap();
            wait_for_checkpoint_root_ready(&session, table_id).await;

            let res = {
                let _guard = ForcePostPublishCheckpointErrorGuard::new();
                session.checkpoint_table(table_id).await
            };

            let err = res.unwrap_err();
            assert_checkpoint_write_poisoned(&err, &engine);
            assert!(table.file().active_root_unchecked().root_ts > root_before.root_ts);
            assert_eq!(table.checkpoint_workflow.state_name(), "Transition");
            assert!(table.checkpoint_workflow.frozen_page_ids().is_none());
            assert!(!session.in_trx().unwrap());
        });
    }

    #[test]
    fn test_checkpoint_commit_failure_after_route_is_fail_closed() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "checkpoint-commit-fail").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 8, "commit-fail").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            session
                .wait_for_gc_horizon_after(session.last_cts())
                .await
                .unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let root_before = table.file().active_root_unchecked().clone();

            let result = {
                let _failure = ForceCheckpointCommitErrorGuard::new();
                session.checkpoint_table(table_id).await
            };
            let err = result.unwrap_err();
            assert_checkpoint_write_poisoned(&err, &engine);
            assert!(table.file().active_root_unchecked().root_ts > root_before.root_ts);
            assert_eq!(table.checkpoint_workflow.state_name(), "Transition");
            assert!(table.checkpoint_workflow.frozen_page_ids().is_none());
            assert!(!session.in_trx().unwrap());

            drop(table);
            drop(session);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_active_root_readiness_ready_when_effective_ts_crossed_gc_horizon() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let session = engine.new_session().unwrap();
            let root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            let effective_ts = root.effective_ts();
            session
                .wait_for_gc_horizon_after(effective_ts)
                .await
                .unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let pin = session.pin().unwrap();
            assert!(table.active_root_checkpoint_delay(&pin).is_none());
        });
    }

    #[test]
    fn test_checkpoint_readiness_delayed_reports_effective_ts_and_horizon() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 120, "readiness-delay").await;

            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            assert_checkpoint_published(&mut session, table_id).await;

            let active_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            let active_root_effective_ts = active_root.effective_ts();
            let outcome = session.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Delayed { reason } = outcome else {
                panic!("expected delayed checkpoint, got {outcome:?}");
            };
            let CheckpointDelayReason::ActiveRoot {
                table_id: delayed_table_id,
                effective_ts,
                min_active_sts,
            } = reason
            else {
                panic!("expected active-root delay, got {reason:?}");
            };
            assert_eq!(delayed_table_id, table_id);
            assert_eq!(effective_ts, active_root_effective_ts);
            assert_eq!(min_active_sts, reader.sts());
            assert!(effective_ts >= min_active_sts);

            reader.commit().await.unwrap();
            session
                .wait_for_gc_horizon_after(active_root_effective_ts)
                .await
                .unwrap();
            let pin = session.pin().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            assert!(table.active_root_checkpoint_delay(&pin).is_none());
        });
    }

    #[test]
    fn test_checkpoint_readiness_uses_root_effective_ts_not_checkpoint_start_ts() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 120, "effective-delay").await;

            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            session
                .wait_for_gc_horizon_after(session.last_cts())
                .await
                .unwrap();
            let reader_holder: Rc<RefCell<Option<(Session, Transaction)>>> =
                Rc::new(RefCell::new(None));
            let reader_sts = Rc::new(Cell::new(TrxID::new(0)));
            let hook_reader_holder = Rc::clone(&reader_holder);
            let hook_reader_sts = Rc::clone(&reader_sts);
            let hook_engine = engine.new_ref().unwrap();
            set_test_checkpoint_after_trx_start_hook(move || async move {
                let mut reader_session = hook_engine.new_session().unwrap();
                let reader = reader_session.begin_trx().unwrap();
                hook_reader_sts.set(reader.sts());
                *hook_reader_holder.borrow_mut() = Some((reader_session, reader));
            });

            let outcome = session.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Published { checkpoint_ts, .. } = outcome else {
                panic!("checkpoint should publish after the hook: {outcome:?}");
            };
            let active_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            let effective_ts = active_root.effective_ts();
            assert!(checkpoint_ts < reader_sts.get());
            assert!(effective_ts > reader_sts.get());

            let delayed = session.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Delayed { reason } = delayed else {
                panic!("expected effective timestamp delay, got {delayed:?}");
            };
            let CheckpointDelayReason::ActiveRoot {
                table_id: delayed_table_id,
                effective_ts: delayed_effective_ts,
                min_active_sts,
            } = reason
            else {
                panic!("expected active-root delay, got {reason:?}");
            };
            assert_eq!(delayed_table_id, table_id);
            assert_eq!(delayed_effective_ts, effective_ts);
            assert!(min_active_sts <= reader_sts.get());
            assert!(delayed_effective_ts >= min_active_sts);

            let (_, mut reader) = reader_holder
                .borrow_mut()
                .take()
                .expect("reader hook should install an active transaction");
            reader
                .exec(async |stmt| {
                    let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                    let proof = rt.read_proof();
                    let snapshot =
                        table_for_internal_assertion(&engine, table_id).root_snapshot(&proof);
                    let _effects_addr = effects as *mut _;
                    assert!(snapshot.root_ts() < rt.sts());
                    assert_eq!(snapshot.effective_ts(), effective_ts);
                    assert!(!snapshot.root_is_visible_to(rt.sts()));
                    Ok(())
                })
                .await
                .unwrap();
            reader.commit().await.unwrap();
            session
                .wait_for_gc_horizon_after(effective_ts)
                .await
                .unwrap();
            let pin = session.pin().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            assert!(table.active_root_checkpoint_delay(&pin).is_none());
        });
    }

    #[test]
    fn test_checkpoint_requires_idle_session_before_delayed_outcome() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 120, "idle-before-delay").await;

            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            let first_checkpoint_ts = assert_checkpoint_published(&mut session, table_id).await;
            assert_eq!(
                table_for_internal_assertion(&engine, table_id)
                    .file()
                    .active_root_unchecked()
                    .root_ts,
                first_checkpoint_ts
            );

            let checkpoint_trx = session.begin_trx().unwrap();
            assert!(session.in_trx().unwrap());
            let err = session.checkpoint_table(table_id).await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::NotSupported)
            );
            let report = format!("{err:?}");
            assert!(
                report.contains("maintenance requires idle session"),
                "{report}"
            );
            assert!(report.contains("operation=checkpoint_table"), "{report}");
            assert!(session.in_trx().unwrap());

            checkpoint_trx.rollback().await.unwrap();
            assert!(!session.in_trx().unwrap());
            reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_checkpoint_delayed_preserves_root_and_frozen_pages_until_ready() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 120, "delayed-root").await;

            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            assert_checkpoint_published(&mut session, table_id).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let root_after_first = table.file().active_root_unchecked().clone();
            let effective_ts_protected_by_reader = root_after_first.effective_ts();

            insert_rows(table_id, &mut session, 1_000, 80, "delayed-frozen").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let frozen_page_ids = table.checkpoint_workflow.frozen_page_ids().unwrap();
            assert!(!frozen_page_ids.is_empty());
            let first_frozen_page = frozen_page_ids[0];
            let root_before_delay = table.file().active_root_unchecked().clone();

            let outcome = session.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Delayed { reason } = outcome else {
                panic!("expected delayed checkpoint, got {outcome:?}");
            };
            assert_eq!(
                reason,
                CheckpointDelayReason::ActiveRoot {
                    table_id,
                    effective_ts: effective_ts_protected_by_reader,
                    min_active_sts: reader.sts(),
                }
            );
            assert_root_metadata_unchanged(&root_before_delay, &table);

            let page_guard = table
                .mem
                .must_get_row_page_shared(&session.pool_guards(), first_frozen_page)
                .await
                .unwrap();
            assert_eq!(
                page_guard.unwrap_vmap().inspect_state(),
                RowPageState::Frozen
            );
            drop(page_guard);
            assert_eq!(
                table.checkpoint_workflow.frozen_page_ids().unwrap(),
                frozen_page_ids
            );

            reader.commit().await.unwrap();
            session
                .wait_for_gc_horizon_after(effective_ts_protected_by_reader)
                .await
                .unwrap();
            let checkpoint_ts = assert_checkpoint_published(&mut session, table_id).await;
            let root_after_publish = table.file().active_root_unchecked().clone();
            assert_eq!(root_after_publish.root_ts, checkpoint_ts);
            assert!(root_after_publish.pivot_row_id > root_before_delay.pivot_row_id);
        });
    }

    #[test]
    fn test_second_checkpoint_waits_for_previous_root_horizon() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 120, "second-delay").await;

            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            let first_checkpoint_ts = assert_checkpoint_published(&mut session, table_id).await;
            let first_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            let first_effective_ts = first_root.effective_ts();
            assert_eq!(first_root.root_ts, first_checkpoint_ts);

            let root_before_second = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            let outcome = session.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Delayed { reason } = outcome else {
                panic!("expected second checkpoint to wait, got {outcome:?}");
            };
            assert_eq!(
                reason,
                CheckpointDelayReason::ActiveRoot {
                    table_id,
                    effective_ts: first_effective_ts,
                    min_active_sts: reader.sts(),
                }
            );
            assert_root_metadata_unchanged(
                &root_before_second,
                &table_for_internal_assertion(&engine, table_id),
            );

            reader.commit().await.unwrap();
            session
                .wait_for_gc_horizon_after(first_effective_ts)
                .await
                .unwrap();
            let second_checkpoint_ts = assert_checkpoint_published(&mut session, table_id).await;
            assert!(second_checkpoint_ts > first_checkpoint_ts);
        });
    }

    #[test]
    fn test_checkpoint_reachability_reclaims_obsolete_column_index_root() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 120, "reachability-first").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            let first_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            let first_column_root = first_root.column_block_index_root;
            assert_ne!(first_column_root, SUPER_BLOCK_ID);

            insert_rows(table_id, &mut session, 1_000, 120, "reachability-second").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            let second_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert_ne!(second_root.column_block_index_root, first_column_root);
            assert!(
                second_root
                    .alloc_map
                    .is_allocated(usize::from(first_column_root)),
                "the old column-index root stays allocated while the displaced root is protected"
            );

            session
                .wait_for_gc_horizon_after(second_root.effective_ts())
                .await
                .unwrap();
            assert_checkpoint_published(&mut session, table_id).await;
            let reclaimed_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert_ne!(reclaimed_root.column_block_index_root, first_column_root);
            if reclaimed_root
                .alloc_map
                .is_allocated(usize::from(first_column_root))
            {
                assert_eq!(
                    reclaimed_root.meta_block_id, first_column_root,
                    "the freed obsolete column-index root may be immediately reused for the new meta block"
                );
            }
            assert!(
                reclaimed_root
                    .alloc_map
                    .is_allocated(usize::from(second_root.column_block_index_root)),
                "current column-index root must remain allocated"
            );
        });
    }

    #[test]
    fn test_concurrent_checkpoint_table_returns_in_progress_cancellation() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut first_session = engine.new_session().unwrap();
            wait_for_checkpoint_root_ready(&first_session, table_id).await;

            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            set_test_checkpoint_after_trx_start_hook(move || async move {
                entered_tx.send_async(()).await.unwrap();
                release_rx.recv_async().await.unwrap();
            });

            let first_outcome = {
                let first_checkpoint = first_session.checkpoint_table(table_id).fuse();
                futures::pin_mut!(first_checkpoint);
                let checkpoint_entered = entered_rx.recv_async().fuse();
                futures::pin_mut!(checkpoint_entered);
                futures::select! {
                    res = first_checkpoint => {
                        panic!("first checkpoint completed before the concurrency hook: {res:?}");
                    }
                    res = checkpoint_entered => {
                        res.unwrap();
                    }
                }

                let mut second_session = engine.new_session().unwrap();
                let outcome = second_session.checkpoint_table(table_id).await.unwrap();
                assert_eq!(
                    outcome,
                    CheckpointOutcome::Cancelled {
                        reason: CheckpointCancelReason::CheckpointInProgress,
                    }
                );
                assert!(!second_session.in_trx().unwrap());

                release_tx.send_async(()).await.unwrap();
                first_checkpoint.await.unwrap()
            };
            assert!(
                matches!(first_outcome, CheckpointOutcome::Published { .. }),
                "first checkpoint should publish after the competing checkpoint cancels: {first_outcome:?}"
            );
            assert!(!first_session.in_trx().unwrap());
        });
    }

    #[test]
    fn test_drop_waits_for_reversible_checkpoint() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut checkpoint_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut checkpoint_session, 0, 4, "drop-reversible").await;
            assert_freeze_created(
                checkpoint_session
                    .freeze_table(table_id, usize::MAX)
                    .await
                    .unwrap(),
            );
            checkpoint_session
                .wait_for_gc_horizon_after(checkpoint_session.last_cts())
                .await
                .unwrap();
            wait_for_checkpoint_root_ready(&checkpoint_session, table_id).await;

            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            set_test_checkpoint_after_trx_start_hook(move || async move {
                entered_tx.send_async(()).await.unwrap();
                release_rx.recv_async().await.unwrap();
            });

            let checkpoint = checkpoint_session.checkpoint_table(table_id).fuse();
            futures::pin_mut!(checkpoint);
            let entered = entered_rx.recv_async().fuse();
            futures::pin_mut!(entered);
            futures::select! {
                result = checkpoint => {
                    panic!("checkpoint completed before reversible hook: {result:?}");
                }
                result = entered => result.unwrap(),
            }

            let mut drop_session = engine.new_session().unwrap();
            let drop_table = drop_session.drop_table(table_id).fuse();
            futures::pin_mut!(drop_table);
            assert!(matches!(
                futures::poll!(drop_table.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(
                table_for_internal_assertion(&engine, table_id)
                    .lifecycle
                    .inspect_terminal(),
                TableTerminal::Live
            );

            release_tx.send_async(()).await.unwrap();
            assert!(matches!(
                checkpoint.await.unwrap(),
                CheckpointOutcome::Published { .. }
            ));
            drop_table.await.unwrap();
        });
    }

    #[test]
    fn test_drop_wins_validated_transition_admission_without_page_transition() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "drop-before-admission").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 8, "drop-before-admission").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            session
                .wait_for_gc_horizon_after(session.last_cts())
                .await
                .unwrap();

            let table = table_for_internal_assertion(&engine, table_id);
            let mut attempt = table
                .checkpoint_workflow
                .begin_checkpoint(&table.lifecycle)
                .unwrap();
            let root_lease = table.try_begin_checkpoint_root_mutation().unwrap();
            let frozen_pages = attempt.batch().unwrap().pages.clone();
            let transition_pages = table
                .load_frozen_pages_for_transition(&session.pool_guards(), &frozen_pages)
                .await
                .unwrap();
            let cutoff_ts = engine.inner().trx_sys.published_gc_horizon();
            let drop_table = Arc::clone(&table);
            set_test_stable_page_plans_refreshed_hook(move || {
                let _drain = drop_table.start_drop_lifecycle().unwrap();
            });
            let delay = table.prepare_page_transition(
                &transition_pages,
                attempt.batch_mut().unwrap(),
                cutoff_ts,
            );
            assert!(delay.is_none());
            let result = table
                .checkpoint_workflow
                .try_begin_transition(&table.lifecycle);
            assert!(matches!(result, Err(CheckpointCancelReason::TableDropping)));
            assert!(transition_pages.iter().all(|page_guard| {
                page_guard.unwrap_vmap().inspect_state() == RowPageState::Frozen
            }));
            drop(transition_pages);
            drop(attempt);
            assert_checkpoint_workflow_closed(&table);
            table.mark_dropped_lifecycle();
            drop(root_lease);
        });
    }

    #[test]
    fn test_drop_waits_for_active_freeze() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "drop-active-freeze").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut freeze_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut freeze_session, 0, 8, "drop-freeze").await;
            let table = table_for_internal_assertion(&engine, table_id);

            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            set_test_freeze_after_loading_hook(move || async move {
                entered_tx.send_async(()).await.unwrap();
                release_rx.recv_async().await.unwrap();
            });
            let freeze = freeze_session.freeze_table(table_id, usize::MAX).fuse();
            futures::pin_mut!(freeze);
            let entered = entered_rx.recv_async().fuse();
            futures::pin_mut!(entered);
            futures::select! {
                result = freeze => panic!("freeze completed before test hook: {result:?}"),
                result = entered => result.unwrap(),
            }

            let mut drop_session = engine.new_session().unwrap();
            let drop_table = drop_session.drop_table(table_id).fuse();
            futures::pin_mut!(drop_table);
            assert!(matches!(
                futures::poll!(drop_table.as_mut()),
                std::task::Poll::Pending
            ));
            assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Live);
            assert_eq!(table.checkpoint_workflow.state_name(), "Freezing");

            release_tx.send_async(()).await.unwrap();
            assert_freeze_created(freeze.await.unwrap());
            drop_table.await.unwrap();
            assert_checkpoint_workflow_closed(&table);
        });
    }

    #[test]
    fn test_drop_discards_delayed_frozen_batch_before_runtime_destroy() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir,
                "drop-delayed-batch",
            ))
            .await
            .unwrap();
            let table_id = create_table2_for_test(&engine).await;
            let mut setup = engine.new_session().unwrap();
            insert_rows(table_id, &mut setup, 1, 1, "before").await;
            let active_root_effective_ts = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .effective_ts();
            setup
                .wait_for_gc_horizon_after(active_root_effective_ts)
                .await
                .unwrap();

            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            let update = trx_update_row_by_id(
                &mut writer,
                table_id,
                &single_key(1),
                vec![UpdateCol {
                    idx: 1,
                    val: Val::from("after"),
                }],
            )
            .await
            .unwrap();
            assert!(matches!(update, UpdateMvcc::Updated(_)));

            let mut checkpoint_session = engine.new_session().unwrap();
            assert_freeze_created(
                checkpoint_session
                    .freeze_table(table_id, usize::MAX)
                    .await
                    .unwrap(),
            );
            let mut horizon_session = engine.new_session().unwrap();
            let horizon = horizon_session.begin_trx().unwrap();
            writer.commit().await.unwrap();
            let outcome = checkpoint_session.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Delayed { reason } = outcome else {
                panic!("checkpoint should wait for frozen-page cutoff: {outcome:?}");
            };
            assert!(matches!(
                reason,
                CheckpointDelayReason::FrozenPageCutoff { .. }
            ));
            let table = table_for_internal_assertion(&engine, table_id);
            assert_eq!(table.checkpoint_workflow.state_name(), "Frozen");
            let mut wait = Box::pin(checkpoint_session.wait_for_checkpoint_retry(reason));
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert_eq!(table.checkpoint_workflow.state_name(), "Checkpointing");

            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            let mut drop_session = engine.new_session().unwrap();
            drop_session.drop_table(table_id).await.unwrap();
            wait.await.unwrap();
            assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Dropped);
            assert_checkpoint_workflow_closed(&table);
            horizon.rollback().await.unwrap();
            drop(table);

            engine.inner().trx_sys.request_dropped_table_purge();
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();
            wait_path_exists(&table_file_path, false).await;
        });
    }

    #[test]
    fn test_checkpoint_carrier_preserves_fatal_reason() {
        let err = RuntimeOrFatalError::Fatal(
            Report::new(FatalError::CatalogWrite).attach("fatal checkpoint source"),
        );

        let report = err.into_fatal_report(FatalError::CheckpointWrite);

        assert_eq!(*report.current_context(), FatalError::CatalogWrite);
        assert!(format!("{report:?}").contains("fatal checkpoint source"));
    }

    #[test]
    fn test_checkpoint_runtime_carrier_uses_fallback_reason() {
        let err = RuntimeOrFatalError::Runtime(
            Report::new(InternalError::SecondaryIndexOutOfBounds)
                .attach("typed checkpoint invariant")
                .change_context(RuntimeError::CheckpointExecution),
        );

        let report = err.into_fatal_report(FatalError::CheckpointWrite);

        assert_eq!(*report.current_context(), FatalError::CheckpointWrite);
        assert_eq!(
            report.downcast_ref::<RuntimeError>().copied(),
            Some(RuntimeError::CheckpointExecution)
        );
        assert_eq!(
            report.downcast_ref::<InternalError>().copied(),
            Some(InternalError::SecondaryIndexOutOfBounds)
        );
        assert!(format!("{report:?}").contains("typed checkpoint invariant"));
    }

    #[test]
    fn test_drop_waits_for_checkpoint_that_won_publish_admission() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut checkpoint_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut checkpoint_session, 0, 4, "drop-publisher").await;
            assert_freeze_created(
                checkpoint_session
                    .freeze_table(table_id, usize::MAX)
                    .await
                    .unwrap(),
            );
            checkpoint_session
                .wait_for_gc_horizon_after(checkpoint_session.last_cts())
                .await
                .unwrap();
            wait_for_checkpoint_root_ready(&checkpoint_session, table_id).await;

            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            set_test_checkpoint_after_publish_admission_hook(move || async move {
                entered_tx.send_async(()).await.unwrap();
                release_rx.recv_async().await.unwrap();
            });

            let checkpoint = checkpoint_session.checkpoint_table(table_id).fuse();
            futures::pin_mut!(checkpoint);
            let entered = entered_rx.recv_async().fuse();
            futures::pin_mut!(entered);
            futures::select! {
                result = checkpoint => {
                    panic!("checkpoint completed before transition hook: {result:?}");
                }
                result = entered => result.unwrap(),
            }

            let mut drop_session = engine.new_session().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let checkpoint_redo_cts = {
                let drop_table = drop_session.drop_table(table_id).fuse();
                futures::pin_mut!(drop_table);
                assert!(matches!(
                    futures::poll!(drop_table.as_mut()),
                    std::task::Poll::Pending
                ));
                assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Live);
                assert_eq!(table.checkpoint_workflow.state_name(), "Transition");

                release_tx.send_async(()).await.unwrap();
                let outcome = checkpoint.await.unwrap();
                let CheckpointOutcome::Published { redo_cts, .. } = outcome else {
                    panic!("admitted checkpoint should publish: {outcome:?}");
                };
                drop_table.as_mut().await.unwrap();
                redo_cts
            };
            assert!(checkpoint_redo_cts < drop_session.last_cts());
            assert_eq!(table.lifecycle.inspect_terminal(), TableTerminal::Dropped);
        });
    }

    #[test]
    fn test_transition_allows_reads_and_suffix_inserts_and_wakes_writers() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "transition-foreground").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut checkpoint_session = engine.new_session().unwrap();
            let frozen_value = "f".repeat(1024);
            insert_rows(table_id, &mut checkpoint_session, 1, 200, &frozen_value).await;
            assert_freeze_created(
                checkpoint_session
                    .freeze_table(table_id, usize::MAX)
                    .await
                    .unwrap(),
            );
            checkpoint_session
                .wait_for_gc_horizon_after(checkpoint_session.last_cts())
                .await
                .unwrap();

            let mut frozen_foreground = engine.new_session().unwrap();
            expect_select_committed(table_id, &mut frozen_foreground, &single_key(1), |row| {
                assert_eq!(row[0].as_i32(), Some(1))
            })
            .await;
            expect_insert_committed(
                table_id,
                &mut frozen_foreground,
                vec![Val::from(1000i32), Val::from("active-suffix")],
            )
            .await;
            let table = table_for_internal_assertion(&engine, table_id);
            let frozen_page_ids = table.checkpoint_workflow.frozen_page_ids().unwrap();
            assert!(frozen_page_ids.len() > 1);
            let frozen_page_count = frozen_page_ids.len();
            let first_frozen_page_id = frozen_page_ids[0];
            let first_frozen_page = table
                .mem
                .must_get_row_page_shared(&frozen_foreground.pool_guards(), first_frozen_page_id)
                .await
                .unwrap();
            let second_frozen_page = table
                .mem
                .must_get_row_page_shared(&frozen_foreground.pool_guards(), frozen_page_ids[1])
                .await
                .unwrap();
            let analysis_count = Rc::new(Cell::new(0usize));
            let first_page_analysis_count = Rc::new(Cell::new(0usize));
            let mutation_started = Rc::new(Cell::new(false));
            let hook_analysis_count = Rc::clone(&analysis_count);
            let hook_first_page_analysis_count = Rc::clone(&first_page_analysis_count);
            set_test_frozen_page_scan_hook(move |page_id| {
                hook_analysis_count.set(hook_analysis_count.get() + 1);
                if page_id != first_frozen_page_id {
                    return;
                }
                hook_first_page_analysis_count.set(hook_first_page_analysis_count.get() + 1);
            });
            let first_frozen_page = Rc::new(RefCell::new(Some(first_frozen_page)));
            let row_hook_page = Rc::clone(&first_frozen_page);
            let row_hook_mutation_started = Rc::clone(&mutation_started);
            set_test_frozen_page_row_scan_hook(move |page_id, row_idx| {
                if page_id == first_frozen_page_id
                    && row_idx == 0
                    && !row_hook_mutation_started.replace(true)
                {
                    let page_guard = row_hook_page.borrow();
                    page_guard
                        .as_ref()
                        .unwrap()
                        .unwrap_vmap()
                        .begin_frozen_mutation();
                }
            });
            let comparison_hook_page = Rc::clone(&first_frozen_page);
            set_test_optimistic_page_plan_comparison_hook(
                move |page_id, version_before, version_after, retained| {
                    if page_id != first_frozen_page_id || version_before == version_after {
                        return;
                    }
                    assert_eq!(version_after, version_before + 1);
                    assert!(!retained);
                    let page_guard = comparison_hook_page.borrow();
                    let guard_ref = page_guard.as_ref().unwrap();
                    let page = guard_ref.page();
                    let row_idx = page.header.row_count() - 1;
                    assert!(page.set_deleted(row_idx, true));
                    page.inc_approx_deleted();
                    guard_ref.unwrap_vmap().finish_frozen_mutation();
                    drop(page_guard);
                    comparison_hook_page.borrow_mut().take();
                },
            );
            let suffix_key = {
                let mut suffix_key = None;
                for key in 3..200 {
                    if hot_page_id_for_key(&table, &frozen_foreground, &single_key(key)).await
                        == frozen_page_ids[1]
                    {
                        suffix_key = Some(key);
                        break;
                    }
                }
                suffix_key.expect("second frozen page must contain a test key")
            };

            let (prefix_update_start_tx, prefix_update_start_rx) = flume::bounded(1);
            let (prefix_update_done_tx, prefix_update_done_rx) = flume::bounded(1);
            let mut prefix_update_session = engine.new_session().unwrap();
            let mut prefix_update_trx = prefix_update_session.begin_trx().unwrap();
            let prefix_update_thread = thread::spawn(move || {
                prefix_update_start_rx.recv().unwrap();
                smol::block_on(async {
                    let res = trx_update_row_by_id(
                        &mut prefix_update_trx,
                        table_id,
                        &single_key(1),
                        vec![UpdateCol {
                            idx: 1,
                            val: Val::from("updated-after-route"),
                        }],
                    )
                    .await
                    .unwrap();
                    assert!(matches!(res, UpdateMvcc::Updated(_)));
                    prefix_update_trx.commit().await.unwrap();
                });
                drop(prefix_update_session);
                prefix_update_done_tx.send(()).unwrap();
            });

            let (prefix_delete_start_tx, prefix_delete_start_rx) = flume::bounded(1);
            let (prefix_delete_done_tx, prefix_delete_done_rx) = flume::bounded(1);
            let mut prefix_delete_session = engine.new_session().unwrap();
            let mut prefix_delete_trx = prefix_delete_session.begin_trx().unwrap();
            let prefix_delete_thread = thread::spawn(move || {
                prefix_delete_start_rx.recv().unwrap();
                smol::block_on(async {
                    let res =
                        trx_delete_row_by_id(&mut prefix_delete_trx, table_id, &single_key(2))
                            .await
                            .unwrap();
                    assert_eq!(res, crate::row::ops::DeleteMvcc::Deleted);
                    prefix_delete_trx.commit().await.unwrap();
                });
                drop(prefix_delete_session);
                prefix_delete_done_tx.send(()).unwrap();
            });

            let (suffix_delete_start_tx, suffix_delete_start_rx) = flume::bounded(1);
            let (suffix_delete_done_tx, suffix_delete_done_rx) = flume::bounded(1);
            let mut suffix_delete_session = engine.new_session().unwrap();
            let mut suffix_delete_trx = suffix_delete_session.begin_trx().unwrap();
            let suffix_delete_thread = thread::spawn(move || {
                suffix_delete_start_rx.recv().unwrap();
                smol::block_on(async {
                    let res = trx_delete_row_by_id(
                        &mut suffix_delete_trx,
                        table_id,
                        &single_key(suffix_key),
                    )
                    .await
                    .unwrap();
                    assert_eq!(res, crate::row::ops::DeleteMvcc::Deleted);
                    suffix_delete_trx.commit().await.unwrap();
                });
                drop(suffix_delete_session);
                suffix_delete_done_tx.send(()).unwrap();
            });
            let hook_prefix_update_done_rx = prefix_update_done_rx.clone();
            let hook_prefix_delete_done_rx = prefix_delete_done_rx.clone();
            set_test_transition_page_published_hook(move |page_id| {
                assert_eq!(page_id, first_frozen_page_id);
                assert_eq!(
                    second_frozen_page.unwrap_vmap().inspect_state(),
                    RowPageState::Frozen
                );
                prefix_update_start_tx.send(()).unwrap();
                prefix_delete_start_tx.send(()).unwrap();
                suffix_delete_start_tx.send(()).unwrap();
                suffix_delete_done_rx.recv().unwrap();
                assert!(hook_prefix_update_done_rx.try_recv().is_err());
                assert!(hook_prefix_delete_done_rx.try_recv().is_err());
            });
            let suffix_page_id =
                hot_page_id_for_key(&table, &frozen_foreground, &single_key(1000)).await;
            assert_eq!(
                hot_page_state(&table, &frozen_foreground.pool_guards(), suffix_page_id).await,
                RowPageState::Active
            );

            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            set_test_checkpoint_after_publish_admission_hook(move || async move {
                entered_tx.send_async(()).await.unwrap();
                release_rx.recv_async().await.unwrap();
            });

            let outcome = {
                let checkpoint = checkpoint_session.checkpoint_table(table_id).fuse();
                futures::pin_mut!(checkpoint);
                let entered = entered_rx.recv_async().fuse();
                futures::pin_mut!(entered);
                futures::select! {
                    result = checkpoint => {
                        panic!("checkpoint completed before transition hook: {result:?}");
                    }
                    result = entered => result.unwrap(),
                }
                assert_eq!(table.checkpoint_workflow.state_name(), "Transition");
                let states = row_page_states(&table, &frozen_foreground.pool_guards()).await;
                assert!(states.contains(&RowPageState::Transition));
                assert!(states.contains(&RowPageState::Active));

                let mut transition_foreground = engine.new_session().unwrap();
                expect_select_committed(
                    table_id,
                    &mut transition_foreground,
                    &single_key(1),
                    |row| assert_eq!(row[0].as_i32(), Some(1)),
                )
                .await;
                expect_insert_committed(
                    table_id,
                    &mut transition_foreground,
                    vec![Val::from(1001i32), Val::from("transition-suffix")],
                )
                .await;

                assert!(prefix_update_done_rx.try_recv().is_err());
                assert!(prefix_delete_done_rx.try_recv().is_err());

                release_tx.send_async(()).await.unwrap();
                checkpoint.await.unwrap()
            };
            assert!(matches!(outcome, CheckpointOutcome::Published { .. }));
            assert_eq!(analysis_count.get(), frozen_page_count + 2);
            assert_eq!(first_page_analysis_count.get(), 2);
            prefix_update_thread.join().unwrap();
            prefix_delete_thread.join().unwrap();
            suffix_delete_thread.join().unwrap();
            prefix_update_done_rx.recv().unwrap();
            prefix_delete_done_rx.recv().unwrap();

            assert_eq!(table.checkpoint_workflow.state_name(), "Idle");
            assert_eq!(
                hot_page_state(&table, &checkpoint_session.pool_guards(), suffix_page_id).await,
                RowPageState::Active
            );
            expect_select_committed(table_id, &mut checkpoint_session, &single_key(1), |row| {
                assert_eq!(row[1].as_str(), Some("updated-after-route"))
            })
            .await;
            expect_select_not_found_committed(table_id, &mut checkpoint_session, &single_key(2))
                .await;
            expect_select_not_found_committed(
                table_id,
                &mut checkpoint_session,
                &single_key(suffix_key),
            )
            .await;
            expect_select_committed(table_id, &mut checkpoint_session, &single_key(1001), |_| {})
                .await;
        });
    }

    #[test]
    fn test_final_page_lock_rebuild_is_page_local() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "page-local-final-rebuild").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 1, 200, &"r".repeat(1024)).await;
            let redo_cts = session.last_cts();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            wait_for_checkpoint_purge(&session, redo_cts).await;
            wait_for_checkpoint_root_ready(&session, table_id).await;

            let table = table_for_internal_assertion(&engine, table_id);
            let page_ids = table.checkpoint_workflow.frozen_page_ids().unwrap();
            assert!(page_ids.len() > 1);
            let first_page_id = page_ids[0];
            let second_page_id = page_ids[1];
            let first_page = table
                .mem
                .must_get_row_page_shared(&session.pool_guards(), first_page_id)
                .await
                .unwrap();
            let second_page = table
                .mem
                .must_get_row_page_shared(&session.pool_guards(), second_page_id)
                .await
                .unwrap();
            let first_page = Rc::new(RefCell::new(Some(first_page)));
            let second_page = Rc::new(RefCell::new(Some(second_page)));
            let total_analysis_count = Rc::new(Cell::new(0usize));
            let first_analysis_count = Rc::new(Cell::new(0usize));
            let second_analysis_count = Rc::new(Cell::new(0usize));
            let cross_page_delete_completed = Rc::new(Cell::new(false));

            let refreshed_first_page = Rc::clone(&first_page);
            set_test_stable_page_plans_refreshed_hook(move || {
                let page_guard = refreshed_first_page.borrow_mut().take().unwrap();
                delete_last_frozen_row_image(page_guard);
            });

            let hook_total_analysis_count = Rc::clone(&total_analysis_count);
            let hook_first_analysis_count = Rc::clone(&first_analysis_count);
            let hook_second_analysis_count = Rc::clone(&second_analysis_count);
            let hook_second_page = Rc::clone(&second_page);
            let hook_cross_page_delete_completed = Rc::clone(&cross_page_delete_completed);
            set_test_frozen_page_scan_hook(move |page_id| {
                hook_total_analysis_count.set(hook_total_analysis_count.get() + 1);
                if page_id == first_page_id {
                    let count = hook_first_analysis_count.get() + 1;
                    hook_first_analysis_count.set(count);
                    if count == 2 {
                        let page_guard = hook_second_page.borrow_mut().take().unwrap();
                        assert_eq!(
                            page_guard.unwrap_vmap().inspect_state(),
                            RowPageState::Frozen
                        );
                        delete_last_frozen_row_image(page_guard);
                        hook_cross_page_delete_completed.set(true);
                    }
                } else if page_id == second_page_id {
                    hook_second_analysis_count.set(hook_second_analysis_count.get() + 1);
                }
            });

            let outcome = session.checkpoint_table(table_id).await.unwrap();
            assert!(matches!(outcome, CheckpointOutcome::Published { .. }));
            assert!(cross_page_delete_completed.get());
            assert!(first_analysis_count.get() >= 2);
            assert!(second_analysis_count.get() >= 2);
            assert!(total_analysis_count.get() >= page_ids.len() + 2);
        });
    }

    #[test]
    fn test_continuous_frozen_deletes_rebuild_each_page_without_global_quiet_window() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "continuous-frozen-delete").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 1, 200, &"d".repeat(1024)).await;
            let redo_cts = session.last_cts();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            wait_for_checkpoint_purge(&session, redo_cts).await;
            wait_for_checkpoint_root_ready(&session, table_id).await;

            let table = table_for_internal_assertion(&engine, table_id);
            let page_ids = table.checkpoint_workflow.frozen_page_ids().unwrap();
            assert!(page_ids.len() > 1);
            let mut page_guards = Vec::with_capacity(page_ids.len());
            for page_id in &page_ids {
                page_guards.push(Some(
                    table
                        .mem
                        .must_get_row_page_shared(&session.pool_guards(), *page_id)
                        .await
                        .unwrap(),
                ));
            }
            let page_guards = Rc::new(RefCell::new(page_guards));
            let analysis_counts = Rc::new(RefCell::new(vec![0usize; page_ids.len()]));

            let refreshed_page_guards = Rc::clone(&page_guards);
            set_test_stable_page_plans_refreshed_hook(move || {
                for page_guard in refreshed_page_guards.borrow_mut().iter_mut() {
                    let page_guard = page_guard.take().unwrap();
                    delete_last_frozen_row_image(page_guard);
                }
            });
            let hook_page_ids = page_ids.clone();
            let hook_analysis_counts = Rc::clone(&analysis_counts);
            set_test_frozen_page_scan_hook(move |page_id| {
                let page_idx = hook_page_ids
                    .iter()
                    .position(|candidate| *candidate == page_id)
                    .unwrap();
                hook_analysis_counts.borrow_mut()[page_idx] += 1;
            });

            let outcome = session.checkpoint_table(table_id).await.unwrap();
            assert!(matches!(outcome, CheckpointOutcome::Published { .. }));
            assert!(analysis_counts.borrow().iter().all(|count| *count >= 2));
        });
    }

    #[test]
    fn test_full_plan_refresh_represents_frozen_observed_pre_fence_ownership() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "stable-full-refresh").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 1, 20, "blocked").await;
            let batch =
                assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            session
                .wait_for_gc_horizon_after(session.last_cts())
                .await
                .unwrap();
            wait_for_checkpoint_root_ready(&session, table_id).await;

            let table = table_for_internal_assertion(&engine, table_id);
            let page_ids = table.checkpoint_workflow.frozen_page_ids().unwrap();
            let first_page_id = page_ids[0];
            let first_page = table
                .mem
                .must_get_row_page_shared(&session.pool_guards(), first_page_id)
                .await
                .unwrap();
            let row_id = first_page.page().row_id(0);
            let first_page = Rc::new(RefCell::new(Some(first_page)));
            let undo_owner = Rc::new(RefCell::new(None));
            let hook_undo_owner = Rc::clone(&undo_owner);
            let hook_first_page = Rc::clone(&first_page);
            let pre_fence_sts = batch.frozen_ts().saturating_sub(2);
            let ownership_status = Arc::new(SharedTrxStatus::new(
                MIN_ACTIVE_TRX_ID + pre_fence_sts.as_u64(),
            ));
            let hook_ownership_status = Arc::clone(&ownership_status);
            set_test_frozen_pages_ready_hook(move || {
                let page_guard = hook_first_page.borrow_mut().take().unwrap();
                let page = page_guard.page();
                let map = page_guard.unwrap_vmap();
                let undo = OwnedRowUndo::new(table_id, None, page.row_id(0), RowUndoKind::Lock);
                map.begin_frozen_mutation();
                *map.write_latch(0) = Some(Box::new(RowUndoHead::new(
                    hook_ownership_status,
                    undo.leak(),
                )));
                map.finish_frozen_mutation();
                hook_undo_owner.borrow_mut().replace(undo);
                drop(page_guard);
            });
            let publish_admitted = Rc::new(Cell::new(false));
            let hook_publish_admitted = Rc::clone(&publish_admitted);
            set_test_checkpoint_after_publish_admission_hook(move || async move {
                hook_publish_admitted.set(true);
            });

            let outcome = session.checkpoint_table(table_id).await.unwrap();
            assert!(matches!(outcome, CheckpointOutcome::Published { .. }));
            assert!(publish_admitted.get());
            assert_eq!(table.checkpoint_workflow.state_name(), "Idle");
            let Some(DeleteMarker::Ref(marker_status)) = table.deletion_buffer().get(row_id) else {
                panic!("stable-plan refresh must install the ownership marker");
            };
            assert!(Arc::ptr_eq(&marker_status, &ownership_status));
            undo_owner.borrow_mut().take();
        });
    }

    #[test]
    fn test_first_page_locked_rebuild_represents_frozen_observed_pre_fence_ownership() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "stable-locked-refresh").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 1, 20, "blocked").await;
            let batch =
                assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            session
                .wait_for_gc_horizon_after(session.last_cts())
                .await
                .unwrap();
            wait_for_checkpoint_root_ready(&session, table_id).await;

            let table = table_for_internal_assertion(&engine, table_id);
            let page_ids = table.checkpoint_workflow.frozen_page_ids().unwrap();
            let first_page_id = page_ids[0];
            let first_page = table
                .mem
                .must_get_row_page_shared(&session.pool_guards(), first_page_id)
                .await
                .unwrap();
            let row_id = first_page.page().row_id(0);
            let first_page = Rc::new(RefCell::new(Some(first_page)));
            let undo_owner = Rc::new(RefCell::new(None));
            let hook_undo_owner = Rc::clone(&undo_owner);
            let hook_first_page = Rc::clone(&first_page);
            let pre_fence_sts = batch.frozen_ts().saturating_sub(2);
            let ownership_status = Arc::new(SharedTrxStatus::new(
                MIN_ACTIVE_TRX_ID + pre_fence_sts.as_u64(),
            ));
            let hook_ownership_status = Arc::clone(&ownership_status);
            set_test_stable_page_plans_refreshed_hook(move || {
                let page_guard = hook_first_page.borrow_mut().take().unwrap();
                let page = page_guard.page();
                let map = page_guard.unwrap_vmap();
                let undo = OwnedRowUndo::new(table_id, None, page.row_id(0), RowUndoKind::Lock);
                map.begin_frozen_mutation();
                *map.write_latch(0) = Some(Box::new(RowUndoHead::new(
                    hook_ownership_status,
                    undo.leak(),
                )));
                map.finish_frozen_mutation();
                hook_undo_owner.borrow_mut().replace(undo);
                drop(page_guard);
            });
            let locked_rebuild_observed = Rc::new(Cell::new(false));
            let hook_locked_rebuild_observed = Rc::clone(&locked_rebuild_observed);
            let hook_table = Arc::downgrade(&table);
            set_test_locked_page_plan_rebuild_hook(move |page_id| {
                assert_eq!(page_id, first_page_id);
                assert_eq!(
                    hook_table
                        .upgrade()
                        .unwrap()
                        .checkpoint_workflow
                        .state_name(),
                    "Transition"
                );
                hook_locked_rebuild_observed.set(true);
            });
            let publish_admitted = Rc::new(Cell::new(false));
            let hook_publish_admitted = Rc::clone(&publish_admitted);
            set_test_checkpoint_after_publish_admission_hook(move || async move {
                hook_publish_admitted.set(true);
            });

            let outcome = session.checkpoint_table(table_id).await.unwrap();
            assert!(matches!(outcome, CheckpointOutcome::Published { .. }));
            assert!(publish_admitted.get());
            assert!(locked_rebuild_observed.get());
            assert_eq!(table.checkpoint_workflow.state_name(), "Idle");
            let Some(DeleteMarker::Ref(marker_status)) = table.deletion_buffer().get(row_id) else {
                panic!("locked rebuild must install the ownership marker");
            };
            assert!(Arc::ptr_eq(&marker_status, &ownership_status));
            undo_owner.borrow_mut().take();
        });
    }

    #[test]
    fn test_transition_writers_wake_through_checkpoint_poison() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "transition-poison").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut checkpoint_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut checkpoint_session, 1, 2, "poison").await;
            assert_freeze_created(
                checkpoint_session
                    .freeze_table(table_id, usize::MAX)
                    .await
                    .unwrap(),
            );
            checkpoint_session
                .wait_for_gc_horizon_after(checkpoint_session.last_cts())
                .await
                .unwrap();
            let table = table_for_internal_assertion(&engine, table_id);

            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            set_test_checkpoint_after_publish_admission_hook(move || async move {
                entered_tx.send_async(()).await.unwrap();
                release_rx.recv_async().await.unwrap();
            });
            let mut update_session = engine.new_session().unwrap();
            let mut update_trx = update_session.begin_trx().unwrap();
            let mut delete_session = engine.new_session().unwrap();
            let mut delete_trx = delete_session.begin_trx().unwrap();
            let (update_err, delete_err) = {
                let _failure = ForceLwcBuildErrorGuard::new();
                let checkpoint = checkpoint_session.checkpoint_table(table_id).fuse();
                futures::pin_mut!(checkpoint);
                let entered = entered_rx.recv_async().fuse();
                futures::pin_mut!(entered);
                futures::select! {
                    result = checkpoint => {
                        panic!("checkpoint completed before transition hook: {result:?}");
                    }
                    result = entered => result.unwrap(),
                }

                let update_key = single_key(1);
                let delete_key = single_key(2);
                let update = trx_update_row_by_id(
                    &mut update_trx,
                    table_id,
                    &update_key,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("must-fail"),
                    }],
                );
                futures::pin_mut!(update);
                assert!(matches!(
                    futures::poll!(update.as_mut()),
                    std::task::Poll::Pending
                ));
                let delete = trx_delete_row_by_id(&mut delete_trx, table_id, &delete_key);
                futures::pin_mut!(delete);
                assert!(matches!(
                    futures::poll!(delete.as_mut()),
                    std::task::Poll::Pending
                ));

                release_tx.send_async(()).await.unwrap();
                checkpoint.await.unwrap_err();
                let update_err = update.await.unwrap_err();
                let delete_err = delete.await.unwrap_err();
                (update_err, delete_err)
            };
            assert!(
                engine
                    .inner()
                    .poisoner
                    .poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::CheckpointWrite)
            );
            assert_checkpoint_write_poisoned(&update_err, &engine);
            assert_checkpoint_write_poisoned(&delete_err, &engine);
            assert_eq!(table.checkpoint_workflow.state_name(), "Transition");
            assert!(table.checkpoint_workflow.frozen_page_ids().is_none());

            discard_transaction_after_fatal_rollback(&mut update_trx);
            discard_transaction_after_fatal_rollback(&mut delete_trx);
            drop(update_trx);
            drop(delete_trx);
            drop(update_session);
            drop(delete_session);
            drop(table);
            drop(checkpoint_session);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_checkpoint_snapshot_consistency() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "y".repeat(256);
            insert_rows(table_id, &mut session, 0, 120, &name).await;

            assert_freeze_created(session.freeze_table(table_id, 1).await.unwrap());

            let mut read_trx = session.begin_trx().unwrap();
            {
                let key = SelectKey::new(0, vec![Val::from(1)]);
                let res = trx_select_row_mvcc_by_id(&mut read_trx, table_id, &key, &[0, 1]).await;
                assert!(matches!(res, Ok(SelectMvcc::Found(_))));
            }

            let mut write_session = engine.new_session().unwrap();
            let mut write_trx = write_session.begin_trx().unwrap();
            {
                let insert = vec![Val::from(10_000i32), Val::from("new")];
                let res = trx_insert_row_by_id(&mut write_trx, table_id, insert).await;
                assert!(res.is_ok());
            }

            let mut checkpoint_session = engine.new_session().unwrap();
            assert_checkpoint_published(&mut checkpoint_session, table_id).await;

            {
                let key = SelectKey::new(0, vec![Val::from(10_000i32)]);
                let res = trx_select_row_mvcc_by_id(&mut read_trx, table_id, &key, &[0, 1]).await;
                assert!(matches!(res, Ok(SelectMvcc::NotFound)));
            }

            write_trx.rollback().await.unwrap();
            read_trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_checkpoint_old_root_released_after_active_reader_purged() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "retained-root".repeat(64);
            insert_rows(table_id, &mut session, 0, 120, &name).await;

            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let retained_root_ptr = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked() as *const _ as usize;
            let drop_count_before = old_root_drop_count(retained_root_ptr);

            let mut read_session = engine.new_session().unwrap();
            let read_trx = read_session.begin_trx().unwrap();

            let mut checkpoint_session = engine.new_session().unwrap();
            assert_checkpoint_published(&mut checkpoint_session, table_id).await;
            let retained_root_fence = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .effective_ts();

            assert_eq!(
                old_root_drop_count(retained_root_ptr),
                drop_count_before,
                "old root must stay retained while a pre-checkpoint transaction is active"
            );

            read_trx.commit().await.unwrap();
            expect_insert_committed(
                table_id,
                &mut session,
                vec![Val::from(50_000i32), Val::from("after-retention-reader")],
            )
            .await;

            session
                .wait_for_purge_completion_after(retained_root_fence)
                .await
                .unwrap();
            assert!(
                old_root_drop_count(retained_root_ptr) > drop_count_before,
                "old root should be released after transaction GC crosses the checkpoint"
            );
        });
    }

    #[test]
    fn test_checkpoint_persistence_recovery() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let table = engine
                .catalog()
                .get_table_now(table_id)
                .expect("test table should exist");
            let mut session = engine.new_session().unwrap();
            let name = "z".repeat(512);
            insert_rows_direct(table_id, &mut session, 0, 150, &name).await;

            assert_freeze_created(
                session
                    .freeze_table(table.table_id(), usize::MAX)
                    .await
                    .unwrap(),
            );
            assert_checkpoint_published(&mut session, table.table_id()).await;

            let root_before = table.file().active_root_unchecked().clone();
            drop(table);

            let table_file = engine
                .inner()
                .table_fs
                .open_table_file(table_id, engine.inner().disk_pool.clone_inner())
                .await
                .unwrap();
            let root_after = table_file.active_root_unchecked();
            assert_eq!(root_after.pivot_row_id, root_before.pivot_row_id);
            assert_eq!(
                root_after.heap_redo_start_ts,
                root_before.heap_redo_start_ts
            );
            assert_eq!(
                root_after.deletion_cutoff_ts,
                root_before.deletion_cutoff_ts
            );
        });
    }

    async fn prepare_silent_checkpoint_failure(
        engine: &Engine,
        session: &mut Session,
    ) -> (TableID, ActiveRoot) {
        let table_id = create_table2_for_test(engine).await;
        insert_rows(table_id, session, 0, 8, "silent-failure").await;
        wait_for_checkpoint_root_ready(session, table_id).await;
        let root = table_for_internal_assertion(engine, table_id)
            .file()
            .active_root_unchecked()
            .clone();
        (table_id, root)
    }

    fn assert_catalog_write_poisoned(err: &Error, engine: &Engine) {
        assert_eq!(
            err.report().downcast_ref::<FatalError>().copied(),
            Some(FatalError::CatalogWrite)
        );
        assert!(
            engine
                .inner()
                .poisoner
                .poison_error()
                .as_ref()
                .is_some_and(|err| *err.current_context() == FatalError::CatalogWrite)
        );
    }

    async fn assert_silent_watermark_absent(
        engine: &Engine,
        guards: &PoolGuards,
        table_id: TableID,
    ) {
        let watermark = engine
            .catalog()
            .storage
            .table_replay_silent_watermarks()
            .find_uncommitted_by_table_id(guards, table_id)
            .await
            .unwrap();
        assert!(watermark.is_none());
    }

    #[test]
    fn test_publication_admission_cancellation_remains_reversible() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "publish-admission-cancel").await;
            let mut session = engine.new_session().unwrap();
            let (table_id, _) = prepare_silent_checkpoint_failure(&engine, &mut session).await;
            let (entered_tx, entered_rx) = flume::bounded(1);
            set_test_checkpoint_after_publish_admission_hook(move || async move {
                entered_tx.send_async(()).await.unwrap();
                pending().await
            });
            let mut checkpoint = Box::pin(session.checkpoint_table(table_id).fuse());
            let mut entered = Box::pin(entered_rx.recv_async().fuse());
            futures::select! {
                result = checkpoint.as_mut() => {
                    panic!("checkpoint completed before admission cancellation: {result:?}");
                }
                result = entered.as_mut() => result.unwrap(),
            }

            drop(checkpoint);

            assert!(engine.inner().poisoner.poison_error().is_none());
            assert_eq!(
                table_for_internal_assertion(&engine, table_id)
                    .checkpoint_workflow
                    .state_name(),
                "Idle"
            );
            assert!(matches!(
                session.checkpoint_table(table_id).await.unwrap(),
                CheckpointOutcome::Published { silent: true, .. }
            ));
        });
    }

    #[test]
    fn test_silent_watermark_mutation_failure_poisons_catalog_write() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "silent-mutation-failure").await;
            let mut session = engine.new_session().unwrap();
            let (table_id, root_before) =
                prepare_silent_checkpoint_failure(&engine, &mut session).await;
            let guards = session.pool_guards();
            set_test_silent_watermark_mutation_hook(|| async {
                Err(Report::new(InternalError::SecondaryIndexBindingMismatch)
                    .attach("test silent-watermark mutation failure")
                    .change_context(RuntimeError::CatalogAccess))
            });

            let err = session.checkpoint_table(table_id).await.unwrap_err();

            assert_catalog_write_poisoned(&err, &engine);
            assert_silent_watermark_absent(&engine, &guards, table_id).await;
            assert_root_metadata_unchanged(
                &root_before,
                &table_for_internal_assertion(&engine, table_id),
            );
            assert_eq!(
                table_for_internal_assertion(&engine, table_id)
                    .checkpoint_workflow
                    .state_name(),
                "Publishing"
            );
            drop(session);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_silent_watermark_mutation_cancellation_poisons_catalog_write() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "silent-mutation-cancel").await;
            let mut session = engine.new_session().unwrap();
            let (table_id, root_before) =
                prepare_silent_checkpoint_failure(&engine, &mut session).await;
            let guards = session.pool_guards();
            let (entered_tx, entered_rx) = flume::bounded(1);
            set_test_silent_watermark_mutation_hook(move || async move {
                entered_tx.send_async(()).await.unwrap();
                pending().await
            });
            let mut checkpoint = Box::pin(session.checkpoint_table(table_id).fuse());
            let mut entered = Box::pin(entered_rx.recv_async().fuse());
            futures::select! {
                result = checkpoint.as_mut() => {
                    panic!("silent checkpoint completed before mutation cancellation: {result:?}");
                }
                result = entered.as_mut() => result.unwrap(),
            }

            drop(checkpoint);

            let poison = engine
                .inner()
                .poisoner
                .poison_error()
                .expect("cancelled silent mutation should poison storage");
            assert_eq!(*poison.current_context(), FatalError::CatalogWrite);
            assert_silent_watermark_absent(&engine, &guards, table_id).await;
            assert_root_metadata_unchanged(
                &root_before,
                &table_for_internal_assertion(&engine, table_id),
            );
            assert_eq!(
                table_for_internal_assertion(&engine, table_id)
                    .checkpoint_workflow
                    .state_name(),
                "Publishing"
            );
            drop(session);
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn test_checkpoint_heartbeat() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "h".repeat(128);
            insert_rows(table_id, &mut session, 0, 40, &name).await;

            let table = table_for_internal_assertion(&engine, table_id);
            let create_timestamps =
                hot_page_create_timestamps(&table, &session.pool_guards()).await;
            assert!(!create_timestamps.is_empty());

            let root_before = table.file().active_root_unchecked().clone();
            let outcome = session.checkpoint_table_with_wait(table_id).await.unwrap();
            assert!(
                matches!(outcome, CheckpointOutcome::Published { silent: true, .. }),
                "{outcome:?}"
            );
            let root_after = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();

            assert_eq!(root_after.pivot_row_id, root_before.pivot_row_id);
            assert_eq!(
                root_after.heap_redo_start_ts,
                root_before.heap_redo_start_ts
            );
            assert_eq!(
                root_after.deletion_cutoff_ts,
                root_before.deletion_cutoff_ts
            );
            assert_eq!(
                root_after.column_block_index_root,
                root_before.column_block_index_root
            );

            let guards = session.pool_guards();
            let watermark = engine
                .catalog()
                .storage
                .table_replay_silent_watermarks()
                .find_uncommitted_by_table_id(&guards, table_id)
                .await
                .unwrap()
                .expect("silent checkpoint should write a catalog row");
            assert!(watermark.heap_redo_start_ts > root_before.heap_redo_start_ts);
            assert_eq!(watermark.heap_redo_start_ts, create_timestamps[0]);
            assert!(watermark.deletion_cutoff_ts > root_before.deletion_cutoff_ts);
            assert!(
                engine
                    .catalog()
                    .storage
                    .checkpointed_silent_watermarks()
                    .get(&table_id)
                    .is_none(),
                "uncheckpointed watermark rows must not update durable cache"
            );

            let snapshot = engine.catalog().storage.checkpoint_snapshot();
            let (live_before_catalog_checkpoint, _) = engine
                .catalog()
                .snapshot_user_table_redo_floors(snapshot.catalog_replay_start_ts);
            assert_eq!(live_before_catalog_checkpoint.len(), 1);
            assert_eq!(
                live_before_catalog_checkpoint[0].floor,
                table_for_internal_assertion(&engine, table_id).redo_replay_floor_snapshot()
            );

            insert_rows(table_id, &mut session, 40, 41, &name).await;
            session.checkpoint_catalog().await.unwrap();
            let checkpointed = engine.catalog().storage.checkpointed_silent_watermarks();
            let checkpointed_floor = checkpointed
                .get(&table_id)
                .copied()
                .expect("catalog checkpoint should rebuild durable watermark cache");
            assert_eq!(
                checkpointed_floor.heap_redo_start_ts,
                watermark.heap_redo_start_ts
            );
            assert_eq!(
                checkpointed_floor.deletion_cutoff_ts,
                watermark.deletion_cutoff_ts
            );
            let snapshot = engine.catalog().storage.checkpoint_snapshot();
            let (live_after_catalog_checkpoint, _) = engine
                .catalog()
                .snapshot_user_table_redo_floors(snapshot.catalog_replay_start_ts);
            assert_eq!(live_after_catalog_checkpoint.len(), 1);
            assert_eq!(live_after_catalog_checkpoint[0].floor, checkpointed_floor);
        });
    }

    #[test]
    fn test_drop_waits_for_silent_watermark_checkpoint_commit() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "drop-silent-checkpoint").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut checkpoint_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut checkpoint_session, 0, 8, "silent").await;
            wait_for_checkpoint_root_ready(&checkpoint_session, table_id).await;

            assert_drop_waits_for_no_page_publication(
                &engine,
                table_id,
                &mut checkpoint_session,
                true,
            )
            .await;
        });
    }

    #[test]
    fn test_drop_waits_for_deletion_only_root_checkpoint_commit() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "drop-deletion-checkpoint").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut checkpoint_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut checkpoint_session, 0, 8, "cold").await;
            assert_freeze_created(
                checkpoint_session
                    .freeze_table(table_id, usize::MAX)
                    .await
                    .unwrap(),
            );
            assert_checkpoint_published(&mut checkpoint_session, table_id).await;
            wait_for_checkpoint_root_ready(&checkpoint_session, table_id).await;

            expect_delete_committed(table_id, &mut checkpoint_session, &single_key(0)).await;
            checkpoint_session
                .wait_for_gc_horizon_after(checkpoint_session.last_cts())
                .await
                .unwrap();
            assert_drop_waits_for_no_page_publication(
                &engine,
                table_id,
                &mut checkpoint_session,
                false,
            )
            .await;
        });
    }

    #[test]
    fn test_frozen_batch_allows_metadata_change_and_metadata_blocks_maintenance() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "frozen-metadata-change").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 8, "metadata").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let table = table_for_internal_assertion(&engine, table_id);
            let frozen_page_ids = table.checkpoint_workflow.frozen_page_ids().unwrap();

            let metadata_change = table.begin_metadata_change();
            futures::pin_mut!(metadata_change);
            let metadata_lease = match futures::poll!(metadata_change.as_mut()) {
                Poll::Ready(result) => result.unwrap(),
                Poll::Pending => {
                    panic!("a merely frozen batch must not block metadata change")
                }
            };
            assert_eq!(table.checkpoint_workflow.state_name(), "Frozen");
            assert_eq!(
                table.checkpoint_workflow.frozen_page_ids().unwrap(),
                frozen_page_ids
            );
            drop(metadata_lease);

            session
                .wait_for_gc_horizon_after(session.last_cts())
                .await
                .unwrap();
            assert_checkpoint_published(&mut session, table_id).await;
            insert_rows(table_id, &mut session, 100, 1, "active").await;
            let metadata_lease = table.begin_metadata_change().await.unwrap();
            assert_eq!(
                session.freeze_table(table_id, usize::MAX).await.unwrap(),
                FreezeOutcome::Cancelled {
                    reason: CheckpointCancelReason::TableMetadataChanging,
                }
            );
            assert_eq!(
                session.checkpoint_table(table_id).await.unwrap(),
                CheckpointOutcome::Cancelled {
                    reason: CheckpointCancelReason::TableMetadataChanging,
                }
            );
            assert_eq!(table.checkpoint_workflow.state_name(), "Idle");
            let states = row_page_states(&table, &session.pool_guards()).await;
            assert!(!states.is_empty());
            assert!(states.iter().all(|state| *state == RowPageState::Active));
            drop(metadata_lease);
        });
    }

    #[test]
    fn test_checkpoint_gc_verification() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "g".repeat(1024);
            insert_rows(table_id, &mut session, 0, 200, &name).await;

            let allocated_before = engine.inner().mem_pool.allocated();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let outcome = session.checkpoint_table_with_wait(table_id).await.unwrap();
            let CheckpointOutcome::Published { redo_cts, .. } = outcome else {
                panic!("checkpoint should publish, got {outcome:?}");
            };
            let allocated_after = engine.inner().mem_pool.allocated();
            wait_for_checkpoint_purge(&session, redo_cts).await;
            let reclaimed = allocated_after < allocated_before
                || engine.inner().mem_pool.allocated() < allocated_before;
            assert!(reclaimed, "row pages should be reclaimed after purge");
        });
    }

    async fn assert_checkpoint_retirement_waits_for_reader(
        purge_threads: usize,
        log_file_stem: &str,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let engine = Engine::bootstrap(
            lightweight_test_engine_config(temp_dir.path(), log_file_stem).trx(
                TrxSysConfig::default()
                    .log_write_io_depth(1)
                    .recovery_io_depth(1)
                    .catalog_checkpoint_scan_io_depth(1)
                    .log_file_stem(log_file_stem)
                    .purge_threads(purge_threads),
            ),
        )
        .await
        .unwrap();
        let table_id = create_table2_for_test(&engine).await;
        let mut checkpoint_session = engine.new_session().unwrap();
        insert_rows(
            table_id,
            &mut checkpoint_session,
            0,
            200,
            &"retirement".repeat(128),
        )
        .await;
        let insert_cts = checkpoint_session.last_cts();
        wait_for_checkpoint_purge(&checkpoint_session, insert_cts).await;
        wait_for_checkpoint_root_ready(&checkpoint_session, table_id).await;

        let table = table_for_internal_assertion(&engine, table_id);
        let mut reader_session = engine.new_session().unwrap();
        let reader = reader_session.begin_trx().unwrap();
        assert_freeze_created(
            checkpoint_session
                .freeze_table(table_id, usize::MAX)
                .await
                .unwrap(),
        );
        let retired_page_ids = table.checkpoint_workflow.frozen_page_ids().unwrap();
        assert!(!retired_page_ids.is_empty());
        let allocated_before_checkpoint = engine.inner().mem_pool.allocated();
        let outcome = checkpoint_session
            .checkpoint_table_with_wait(table_id)
            .await
            .unwrap();
        let CheckpointOutcome::Published { redo_cts, .. } = outcome else {
            panic!("checkpoint should publish, got {outcome:?}");
        };

        checkpoint_session
            .wait_for_purge_handoff_for_test(redo_cts)
            .await
            .unwrap();
        for page_id in &retired_page_ids {
            let page = table
                .mem
                .must_get_row_page_shared(&checkpoint_session.pool_guards(), *page_id)
                .await
                .unwrap();
            drop(page);
        }
        assert_eq!(
            engine.inner().mem_pool.allocated(),
            allocated_before_checkpoint,
            "checkpoint-retired pages must remain allocated while the reader pins system CTS eligibility"
        );

        reader.commit().await.unwrap();
        checkpoint_session
            .wait_for_purge_completion_after(redo_cts)
            .await
            .unwrap();
        assert!(
            engine.inner().mem_pool.allocated() < allocated_before_checkpoint,
            "checkpoint-retired pages must be deallocated after the reader releases the horizon"
        );
    }

    #[test]
    fn test_checkpoint_retirement_waits_for_reader_in_single_and_dispatcher_modes() {
        smol::block_on(async {
            assert_checkpoint_retirement_waits_for_reader(1, "retire-reader-single").await;
            assert_checkpoint_retirement_waits_for_reader(2, "retire-reader-dispatcher").await;
        });
    }

    #[test]
    fn test_checkpoint_batch_delays_for_pre_fence_uncommitted_insert() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut writer = engine.new_session().unwrap();
            wait_for_checkpoint_root_ready(&writer, table_id).await;
            let mut trx = writer.begin_trx().unwrap();
            trx_insert_row_by_id(
                &mut trx,
                table_id,
                vec![Val::from(1i32), Val::from("blocked")],
            )
            .await
            .unwrap();

            let mut checkpoint_session = engine.new_session().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let batch = assert_freeze_created(
                checkpoint_session
                    .freeze_table(table_id, usize::MAX)
                    .await
                    .unwrap(),
            );
            assert_eq!(batch.page_count(), 1);
            let page_ids = table.checkpoint_workflow.frozen_page_ids().unwrap();
            let frozen_page_id = page_ids[0];
            let root_before = table.file().active_root_unchecked().clone();
            let outcome = checkpoint_session.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Delayed { reason } = outcome else {
                panic!("uncommitted insert should delay checkpoint: {outcome:?}");
            };
            let CheckpointDelayReason::FrozenPageCutoff {
                table_id: delayed_table_id,
                page_id,
                stable_page_count,
                cutoff_ts,
                required_cutoff_ts,
                unresolved_status,
            } = reason
            else {
                panic!("expected frozen-page cutoff delay, got {reason:?}");
            };
            assert_eq!(delayed_table_id, table_id);
            assert_eq!(page_id, frozen_page_id);
            assert_eq!(stable_page_count, 0);
            assert!(cutoff_ts <= trx.sts());
            assert_eq!(required_cutoff_ts, None);
            assert!(unresolved_status);
            assert_root_metadata_unchanged(&root_before, &table);
            let page_guard = table
                .mem
                .must_get_row_page_shared(&checkpoint_session.pool_guards(), frozen_page_id)
                .await
                .unwrap();
            assert_eq!(
                page_guard.unwrap_vmap().inspect_state(),
                RowPageState::Frozen
            );
            drop(page_guard);
            let (wait_result, rollback_result) = futures::join!(
                checkpoint_session.wait_for_checkpoint_retry(reason),
                trx.rollback()
            );
            rollback_result.unwrap();
            wait_result.unwrap();
            assert!(matches!(
                checkpoint_session
                    .checkpoint_table_with_wait(table_id)
                    .await
                    .unwrap(),
                CheckpointOutcome::Published { .. }
            ));
        });
    }

    #[test]
    fn test_frozen_page_wait_requires_all_image_blockers() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut setup = engine.new_session().unwrap();
            insert_rows(table_id, &mut setup, 1, 2, "before").await;
            setup
                .wait_for_gc_horizon_after(setup.last_cts())
                .await
                .unwrap();
            wait_for_checkpoint_root_ready(&setup, table_id).await;

            let mut writer1_session = engine.new_session().unwrap();
            let mut writer1 = writer1_session.begin_trx().unwrap();
            let mut writer2_session = engine.new_session().unwrap();
            let mut writer2 = writer2_session.begin_trx().unwrap();
            for (writer, key, value) in [
                (&mut writer1, 1i32, "writer-1"),
                (&mut writer2, 2i32, "writer-2"),
            ] {
                let update = trx_update_row_by_id(
                    writer,
                    table_id,
                    &single_key(key),
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from(value),
                    }],
                )
                .await
                .unwrap();
                assert!(matches!(update, UpdateMvcc::Updated(_)));
            }

            let mut checkpoint_session = engine.new_session().unwrap();
            assert_freeze_created(
                checkpoint_session
                    .freeze_table(table_id, usize::MAX)
                    .await
                    .unwrap(),
            );
            let analysis_count = Rc::new(Cell::new(0usize));
            let hook_analysis_count = Rc::clone(&analysis_count);
            set_test_frozen_page_scan_hook(move |_| {
                hook_analysis_count.set(hook_analysis_count.get() + 1);
            });
            let outcome = checkpoint_session.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Delayed { reason } = outcome else {
                panic!("two unresolved images should delay checkpoint: {outcome:?}");
            };
            assert_eq!(analysis_count.get(), 1);
            assert!(matches!(
                reason,
                CheckpointDelayReason::FrozenPageCutoff {
                    unresolved_status: true,
                    ..
                }
            ));

            let table = table_for_internal_assertion(&engine, table_id);
            let frozen_page_ids = table.checkpoint_workflow.frozen_page_ids().unwrap();
            let mut cancelled_wait = Box::pin(checkpoint_session.wait_for_checkpoint_retry(reason));
            assert!(futures::poll!(cancelled_wait.as_mut()).is_pending());
            assert_eq!(analysis_count.get(), 1);
            assert_eq!(table.checkpoint_workflow.state_name(), "Checkpointing");
            drop(cancelled_wait);
            assert_eq!(table.checkpoint_workflow.state_name(), "Frozen");
            assert_eq!(
                table.checkpoint_workflow.frozen_page_ids().unwrap(),
                frozen_page_ids
            );

            let mut wait = Box::pin(checkpoint_session.wait_for_checkpoint_retry(reason));
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert_eq!(analysis_count.get(), 1);
            writer1.rollback().await.unwrap();
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert_eq!(analysis_count.get(), 1);

            let mut unrelated_session = engine.new_session().unwrap();
            let unrelated = unrelated_session.begin_trx().unwrap();
            unrelated.rollback().await.unwrap();
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert_eq!(analysis_count.get(), 1);

            writer2.rollback().await.unwrap();
            wait.await.unwrap();
            assert_eq!(analysis_count.get(), 2);
            assert!(matches!(
                checkpoint_session
                    .checkpoint_table_with_wait(table_id)
                    .await
                    .unwrap(),
                CheckpointOutcome::Published { .. }
            ));
        });
    }

    #[test]
    fn test_checkpoint_batch_retries_stale_committed_update_cutoff() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut setup = engine.new_session().unwrap();
            insert_rows(table_id, &mut setup, 1, 1, "before").await;

            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            let key = single_key(1i32);
            let update = trx_update_row_by_id(
                &mut writer,
                table_id,
                &key,
                vec![UpdateCol {
                    idx: 1,
                    val: Val::from("after"),
                }],
            )
            .await
            .unwrap();
            assert!(matches!(update, UpdateMvcc::Updated(_)));

            let mut checkpoint_session = engine.new_session().unwrap();
            assert_freeze_created(
                checkpoint_session
                    .freeze_table(table_id, usize::MAX)
                    .await
                    .unwrap(),
            );
            let table = table_for_internal_assertion(&engine, table_id);
            let page_ids = table.checkpoint_workflow.frozen_page_ids().unwrap();
            let frozen_page_id = page_ids[0];
            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            let reader_sts = reader.sts();
            let writer_cts = writer.commit().await.unwrap();
            assert!(writer_cts >= reader_sts);

            let trx_sys = &engine.inner().trx_sys;
            checkpoint_session
                .wait_for_gc_horizon_after(reader_sts.saturating_sub(1))
                .await
                .unwrap();
            let cutoff_ts = trx_sys.published_gc_horizon();
            assert!(cutoff_ts <= writer_cts);
            let analysis_count = Rc::new(Cell::new(0usize));
            let hook_analysis_count = Rc::clone(&analysis_count);
            set_test_frozen_page_scan_hook(move |page_id| {
                assert_eq!(page_id, frozen_page_id);
                hook_analysis_count.set(hook_analysis_count.get() + 1);
            });

            let outcome = checkpoint_session.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Delayed { reason } = outcome else {
                panic!("future committed update should delay checkpoint: {outcome:?}");
            };
            assert_eq!(
                reason,
                CheckpointDelayReason::FrozenPageCutoff {
                    table_id,
                    page_id: frozen_page_id,
                    stable_page_count: 0,
                    cutoff_ts,
                    required_cutoff_ts: Some(writer_cts + 1),
                    unresolved_status: false,
                }
            );
            let repeated = checkpoint_session
                .freeze_table(table_id, usize::MAX)
                .await
                .unwrap();
            let FreezeOutcome::AlreadyFrozen { batch } = repeated else {
                panic!("delayed checkpoint should retain its frozen batch: {repeated:?}");
            };
            assert_eq!(batch.stable_page_count(), 1);
            assert_eq!(analysis_count.get(), 1);

            let same_cutoff_retry = checkpoint_session.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Delayed { reason } = same_cutoff_retry else {
                panic!("same cutoff must remain delayed: {same_cutoff_retry:?}");
            };
            assert!(matches!(
                reason,
                CheckpointDelayReason::FrozenPageCutoff {
                    cutoff_ts: retry_cutoff_ts,
                    unresolved_status: false,
                    ..
                } if retry_cutoff_ts == cutoff_ts
            ));
            assert_eq!(analysis_count.get(), 1);

            reader.commit().await.unwrap();
            checkpoint_session
                .wait_for_checkpoint_retry(reason)
                .await
                .unwrap();
            let retry = checkpoint_session.checkpoint_table(table_id).await.unwrap();
            assert!(matches!(
                retry,
                CheckpointOutcome::Published { silent: false, .. }
            ));
            assert!(analysis_count.get() >= 2);
        });
    }

    #[test]
    fn test_blocked_page_retry_reuses_stable_prefix() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "later-page-plan-reuse").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut setup = engine.new_session().unwrap();
            insert_rows(table_id, &mut setup, 1, 200, &"p".repeat(1024)).await;
            setup
                .wait_for_gc_horizon_after(setup.last_cts())
                .await
                .unwrap();

            let mut hold_session = engine.new_session().unwrap();
            let hold = hold_session.begin_trx().unwrap();
            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            assert_freeze_created(setup.freeze_table(table_id, usize::MAX).await.unwrap());
            let table = table_for_internal_assertion(&engine, table_id);
            let page_ids = table.checkpoint_workflow.frozen_page_ids().unwrap();
            assert!(page_ids.len() > 2);
            let delayed_page_idx = page_ids.len() / 2;
            let delayed_page_id = page_ids[delayed_page_idx];
            let mut delayed_key = None;
            for key in (1..201).rev() {
                if hot_page_id_for_key(&table, &setup, &single_key(key)).await == delayed_page_id {
                    delayed_key = Some(key);
                    break;
                }
            }
            let delayed_key = delayed_key.unwrap();
            let delete = trx_delete_row_by_id(&mut writer, table_id, &single_key(delayed_key))
                .await
                .unwrap();
            assert_eq!(delete, crate::row::ops::DeleteMvcc::Deleted);
            setup
                .wait_for_gc_horizon_after(hold.sts().saturating_sub(1))
                .await
                .unwrap();
            wait_for_checkpoint_root_ready(&setup, table_id).await;

            let analysis_counts = Rc::new(RefCell::new(vec![0usize; page_ids.len()]));
            let hook_page_ids = page_ids.clone();
            let hook_analysis_counts = Rc::clone(&analysis_counts);
            set_test_frozen_page_scan_hook(move |page_id| {
                let page_idx = hook_page_ids
                    .iter()
                    .position(|candidate| *candidate == page_id)
                    .unwrap();
                hook_analysis_counts.borrow_mut()[page_idx] += 1;
            });
            let ready_hook_ran = Rc::new(Cell::new(false));
            let hook_ready_hook_ran = Rc::clone(&ready_hook_ran);
            let counts_at_first_delay = Rc::new(RefCell::new(Vec::new()));
            let hook_counts_at_first_delay = Rc::clone(&counts_at_first_delay);
            let ready_hook_analysis_counts = Rc::clone(&analysis_counts);
            set_test_frozen_pages_ready_hook(move || {
                hook_ready_hook_ran.set(true);
                let before_retry = hook_counts_at_first_delay.borrow();
                let after_readiness = ready_hook_analysis_counts.borrow();
                assert_eq!(before_retry.len(), after_readiness.len());
                for (page_idx, (before, after)) in
                    before_retry.iter().zip(after_readiness.iter()).enumerate()
                {
                    let expected = before + usize::from(page_idx >= delayed_page_idx);
                    assert_eq!(
                        *after, expected,
                        "incremental readiness unexpectedly rescanned page {page_idx}"
                    );
                }
            });

            let first = setup.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Delayed { reason } = first else {
                panic!("pre-fence delete should delay the later page: {first:?}");
            };
            let CheckpointDelayReason::FrozenPageCutoff {
                page_id,
                stable_page_count,
                cutoff_ts,
                unresolved_status,
                ..
            } = reason
            else {
                panic!("expected frozen-page delay: {reason:?}");
            };
            assert_eq!(page_id, delayed_page_id);
            assert_eq!(stable_page_count, delayed_page_idx);
            assert!(unresolved_status);
            assert!(!ready_hook_ran.get());
            assert!(
                analysis_counts.borrow()[..=delayed_page_idx]
                    .iter()
                    .all(|count| *count == 1)
            );
            assert!(
                analysis_counts.borrow()[delayed_page_idx + 1..]
                    .iter()
                    .all(|count| *count == 0)
            );
            *counts_at_first_delay.borrow_mut() = analysis_counts.borrow().clone();

            let validation = table.checkpoint_workflow.frozen_page_validation().unwrap();
            assert_eq!(validation.len(), page_ids.len());
            for (page_idx, (page_id, state)) in validation.into_iter().enumerate() {
                assert_eq!(page_id, page_ids[page_idx]);
                match page_idx.cmp(&delayed_page_idx) {
                    Ordering::Less => {
                        assert!(matches!(state, FrozenPageValidationState::Stable { .. }));
                    }
                    Ordering::Equal => {
                        assert!(matches!(state, FrozenPageValidationState::Blocked { .. }));
                    }
                    Ordering::Greater => {
                        assert_eq!(state, FrozenPageValidationState::Unchecked);
                    }
                }
            }
            let prepared_page_ids = table.checkpoint_workflow.prepared_page_ids().unwrap();
            assert!(!prepared_page_ids.is_empty());
            assert!(
                prepared_page_ids
                    .iter()
                    .all(|page_id| page_ids[..delayed_page_idx].contains(page_id))
            );

            writer.rollback().await.unwrap();
            assert_eq!(engine.inner().trx_sys.published_gc_horizon(), cutoff_ts);
            setup.wait_for_checkpoint_retry(reason).await.unwrap();
            let retry = setup.checkpoint_table(table_id).await.unwrap();
            assert!(matches!(retry, CheckpointOutcome::Published { .. }));
            assert!(ready_hook_ran.get());
            assert!(analysis_counts.borrow()[delayed_page_idx] >= 2);
            assert!(
                analysis_counts.borrow()[delayed_page_idx + 1..]
                    .iter()
                    .all(|count| *count >= 1)
            );
            hold.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_checkpoint_batch_detects_future_update_behind_delete() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut setup = engine.new_session().unwrap();
            insert_rows(table_id, &mut setup, 1, 1, "before").await;

            let mut reader_session = engine.new_session().unwrap();
            let reader = reader_session.begin_trx().unwrap();
            let reader_sts = reader.sts();
            let key = single_key(1i32);
            let mut writer = engine.new_session().unwrap();
            expect_update_committed(
                table_id,
                &mut writer,
                &key,
                vec![UpdateCol {
                    idx: 1,
                    val: Val::from("after"),
                }],
            )
            .await;
            let update_cts = writer.last_cts();
            expect_delete_committed(table_id, &mut writer, &key).await;
            let delete_cts = writer.last_cts();
            assert!(delete_cts > update_cts);

            let mut checkpoint_session = engine.new_session().unwrap();
            assert_freeze_created(
                checkpoint_session
                    .freeze_table(table_id, usize::MAX)
                    .await
                    .unwrap(),
            );
            let table = table_for_internal_assertion(&engine, table_id);
            let page_ids = table.checkpoint_workflow.frozen_page_ids().unwrap();
            let frozen_page_id = page_ids[0];
            let trx_sys = &engine.inner().trx_sys;
            checkpoint_session
                .wait_for_gc_horizon_after(reader_sts.saturating_sub(1))
                .await
                .unwrap();
            let cutoff_ts = trx_sys.published_gc_horizon();
            assert!(cutoff_ts <= update_cts);

            let outcome = checkpoint_session.checkpoint_table(table_id).await.unwrap();
            let CheckpointOutcome::Delayed { reason } = outcome else {
                panic!("update hidden behind delete should delay checkpoint: {outcome:?}");
            };
            assert_eq!(
                reason,
                CheckpointDelayReason::FrozenPageCutoff {
                    table_id,
                    page_id: frozen_page_id,
                    stable_page_count: 0,
                    cutoff_ts,
                    required_cutoff_ts: Some(update_cts + 1),
                    unresolved_status: false,
                }
            );

            reader.commit().await.unwrap();
            checkpoint_session
                .wait_for_checkpoint_retry(reason)
                .await
                .unwrap();
            let retry = checkpoint_session.checkpoint_table(table_id).await.unwrap();
            assert!(matches!(
                retry,
                CheckpointOutcome::Published { silent: false, .. }
            ));
            assert!(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(RowID::new(0))
                    .is_none()
            );
        });
    }

    #[test]
    fn test_checkpoint_error_rollback() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "e".repeat(256);
            insert_rows(table_id, &mut session, 0, 80, &name).await;

            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let root_before = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            session
                .wait_for_gc_horizon_after(session.last_cts())
                .await
                .unwrap();
            wait_for_checkpoint_root_ready(&session, table_id).await;

            let res = {
                let _guard = ForceLwcBuildErrorGuard::new();
                session.checkpoint_table(table_id).await
            };
            assert!(res.is_err());
            let poison = engine
                .inner()
                .poisoner
                .poison_error()
                .expect("irreversible table checkpointer should poison storage");
            assert_eq!(*poison.current_context(), FatalError::CheckpointWrite);

            let root_after = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert_eq!(root_after.pivot_row_id, root_before.pivot_row_id);
            assert_eq!(
                root_after.heap_redo_start_ts,
                root_before.heap_redo_start_ts
            );
            assert_eq!(
                root_after.deletion_cutoff_ts,
                root_before.deletion_cutoff_ts
            );
            assert_eq!(
                root_after.column_block_index_root,
                root_before.column_block_index_root
            );
            let table = table_for_internal_assertion(&engine, table_id);
            assert_eq!(table.checkpoint_workflow.state_name(), "Transition");
            assert!(table.checkpoint_workflow.frozen_page_ids().is_none());
        });
    }

    #[test]
    fn test_checkpoint_cancelled_while_table_metadata_change_active() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut checkpoint_session = engine.new_session().unwrap();

            wait_for_checkpoint_root_ready(&checkpoint_session, table_id).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let _metadata_lease = table.begin_metadata_change().await.unwrap();

            let outcome = checkpoint_session
                .checkpoint_table(table_id)
                .await
                .expect("checkpoint should return a normal cancellation");

            assert_eq!(
                outcome,
                CheckpointOutcome::Cancelled {
                    reason: CheckpointCancelReason::TableMetadataChanging,
                }
            );
        })
    }

    #[test]
    fn test_checkpoint_reachability_reclaims_dropped_secondary_disk_tree_root() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 64, "drop-index-reclaim").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            let indexed_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            let dropped_disk_root = indexed_root.secondary_index_roots[0];
            assert_ne!(dropped_disk_root, SUPER_BLOCK_ID);
            assert!(
                indexed_root
                    .alloc_map
                    .is_allocated(usize::from(dropped_disk_root))
            );

            session.drop_index(table_id, 0).await.unwrap();
            let after_drop_root = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert_eq!(after_drop_root.secondary_index_roots[0], SUPER_BLOCK_ID);
            assert!(
                after_drop_root
                    .alloc_map
                    .is_allocated(usize::from(dropped_disk_root)),
                "DROP INDEX detaches the root but leaves page reclamation to checkpoint reachability"
            );

            session
                .wait_for_gc_horizon_after(after_drop_root.effective_ts())
                .await
                .unwrap();
            assert_checkpoint_published(&mut session, table_id).await;
            let after_reclaim = table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .clone();
            assert!(
                !after_reclaim
                    .alloc_map
                    .is_allocated(usize::from(dropped_disk_root)),
                "checkpoint reachability should reclaim detached DiskTree pages"
            );
        })
    }
}
