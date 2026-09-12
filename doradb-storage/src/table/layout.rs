use crate::buffer::{BufferPool, EvictableBufferPool};
use crate::catalog::{
    IndexID, IndexRef, IndexSlot, ResolvedIndexKey, TableColumnLayout, TableIndexMetadata,
    TableMetadata, user_key_from_index_ref,
};
use crate::error::{InternalError, RuntimeError, RuntimeResult};
use crate::index::{InMemorySecondaryIndex, SecondaryIndex};
use crate::map::FastHashMap;
use crate::value::Val;
use error_stack::Report;
use std::sync::Arc;

/// One exact active index identity and its runtime owner.
#[derive(Clone)]
pub(crate) struct RuntimeIndexEntry<R = Arc<SecondaryIndex<EvictableBufferPool>>> {
    index: IndexRef,
    runtime: R,
}

impl<R> RuntimeIndexEntry<R> {
    /// Creates one exact runtime entry.
    #[inline]
    pub(crate) fn new(index: IndexRef, runtime: R) -> Self {
        Self { index, runtime }
    }

    /// Returns this entry's exact table-local identity.
    #[inline]
    pub(crate) const fn index_ref(&self) -> IndexRef {
        self.index
    }

    /// Borrows the owned index runtime.
    #[inline]
    pub(crate) fn runtime(&self) -> &R {
        &self.runtime
    }

    /// Consumes this entry and returns its runtime owner.
    #[inline]
    pub(crate) fn into_runtime(self) -> R {
        self.runtime
    }
}

impl<R> RuntimeIndexEntry<Arc<R>> {
    /// Returns a shared owner of the index runtime.
    #[inline]
    pub(crate) fn runtime_arc(&self) -> &Arc<R> {
        &self.runtime
    }
}

/// Selector admitted by exact runtime lookup.
#[cfg(test)]
pub(crate) trait LayoutIndexSelector {
    /// Resolves this selector to an exact active user-index generation.
    fn resolve(self, layout: &TableRuntimeLayout) -> RuntimeResult<IndexRef>;
}

#[cfg(test)]
impl LayoutIndexSelector for IndexRef {
    #[inline]
    fn resolve(self, layout: &TableRuntimeLayout) -> RuntimeResult<IndexRef> {
        layout.index_entry(self).map(|_| self)
    }
}

#[cfg(test)]
impl LayoutIndexSelector for IndexSlot {
    #[inline]
    fn resolve(self, layout: &TableRuntimeLayout) -> RuntimeResult<IndexRef> {
        layout
            .index_entry_at_slot(self)
            .map(RuntimeIndexEntry::index_ref)
    }
}

/// Fixed layout directly owning a memory table's index runtimes.
pub(crate) type MemTableLayout<I> = TableRuntimeLayout<InMemorySecondaryIndex<I>>;

/// Immutable binding between table metadata and exact active index runtimes.
pub(crate) struct TableRuntimeLayout<R = Arc<SecondaryIndex<EvictableBufferPool>>> {
    generation: u64,
    metadata: Arc<TableMetadata>,
    secondary_indexes: Box<[Option<RuntimeIndexEntry<R>>]>,
    slot_by_id: FastHashMap<IndexID, IndexSlot>,
    indexed_columns: Box<[usize]>,
}

impl TableRuntimeLayout {
    /// Resets test-only resolve-once counters.
    #[cfg(test)]
    pub(crate) fn reset_index_access_counters() {
        tests::reset_index_access_counters();
    }

    /// Returns test-only `(map resolutions, direct validations, active iterations)`.
    #[cfg(test)]
    pub(crate) fn index_access_counters() -> (usize, usize, usize) {
        tests::index_access_counters()
    }

    /// Creates a validated user-table layout from newly bound runtimes.
    #[inline]
    pub(crate) fn new(
        generation: u64,
        metadata: Arc<TableMetadata>,
        secondary_indexes: Box<[Option<Arc<SecondaryIndex<EvictableBufferPool>>>]>,
    ) -> Self {
        let layout = Self::from_runtimes(generation, metadata, secondary_indexes);
        layout.assert_user_runtimes();
        layout
    }

    /// Creates a validated user layout from exact retained runtime entries.
    #[inline]
    pub(crate) fn from_entries(
        generation: u64,
        metadata: Arc<TableMetadata>,
        secondary_indexes: Box<[Option<RuntimeIndexEntry>]>,
    ) -> Self {
        let layout = Self::assemble(generation, metadata, secondary_indexes);
        layout.assert_user_runtimes();
        layout
    }

    /// Checks user runtime slots and kinds against the immutable binding.
    #[inline]
    pub(crate) fn assert_valid(&self) {
        self.assert_shape();
        self.assert_user_runtimes();
    }

    fn assert_user_runtimes(&self) {
        for entry in self.secondary_indexes.iter().flatten() {
            let spec = self.metadata.idx.expect_index_spec(entry.index_ref());
            assert_eq!(
                entry.runtime().index_slot(),
                spec.index.slot(),
                "table runtime layout invariant violated: runtime index slot mismatch, index={}",
                spec.index
            );
            assert_eq!(
                entry.runtime().is_unique(),
                spec.unique(),
                "table runtime layout invariant violated: runtime index kind mismatch, index={}",
                spec.index
            );
        }
    }

    /// Returns the runtime for an exact reference resolved against this retained layout.
    #[inline]
    pub(crate) fn expect_secondary_index(
        &self,
        index: IndexRef,
    ) -> &SecondaryIndex<EvictableBufferPool> {
        self.expect_index_entry(index).runtime()
    }

    /// Returns one active secondary-index runtime by exact reference.
    #[cfg(test)]
    #[inline]
    pub(crate) fn secondary_index<I: LayoutIndexSelector>(
        &self,
        index: I,
    ) -> RuntimeResult<&SecondaryIndex<EvictableBufferPool>> {
        let index = index.resolve(self)?;
        self.index_entry(index)
            .map(|entry| entry.runtime().as_ref())
    }

    /// Iterates exact active references paired with their user runtimes.
    #[inline]
    pub(crate) fn active_secondary_indexes(
        &self,
    ) -> impl Iterator<Item = (IndexRef, &SecondaryIndex<EvictableBufferPool>)> + '_ {
        #[cfg(test)]
        tests::record_active_iteration();
        self.secondary_indexes
            .iter()
            .flatten()
            .map(|entry| (entry.index_ref(), entry.runtime().as_ref()))
    }
}

impl<I: BufferPool> TableRuntimeLayout<InMemorySecondaryIndex<I>> {
    /// Binds memory indexes built from this table's fixed metadata.
    #[inline]
    pub(crate) fn new_memory(
        metadata: Arc<TableMetadata>,
        secondary_indexes: Box<[Option<InMemorySecondaryIndex<I>>]>,
    ) -> Self {
        let layout = Self::from_runtimes(0, metadata, secondary_indexes);
        for entry in layout.secondary_indexes.iter().flatten() {
            let spec = layout.metadata.idx.expect_index_spec(entry.index_ref());
            assert_eq!(
                entry.runtime().is_unique(),
                spec.unique(),
                "memory table layout invariant violated: runtime index kind mismatch, index={}",
                spec.index
            );
        }
        layout
    }
}

impl<R> TableRuntimeLayout<R> {
    fn from_runtimes(
        generation: u64,
        metadata: Arc<TableMetadata>,
        secondary_indexes: Box<[Option<R>]>,
    ) -> Self {
        let entries = secondary_indexes.into_vec().into_iter().enumerate()
            .map(|(slot, runtime)| runtime.map(|runtime| {
                let slot = IndexSlot::try_from(slot).unwrap_or_else(|_| {
                    panic!("table runtime layout slot exceeds u16: slot={slot}")
                });
                let spec = metadata.idx.index_spec(slot).unwrap_or_else(|| {
                    panic!("table runtime layout has runtime for inactive metadata slot: slot={slot}")
                });
                RuntimeIndexEntry::new(spec.index, runtime)
            }))
            .collect::<Vec<_>>().into_boxed_slice();
        Self::assemble(generation, metadata, entries)
    }

    fn assemble(
        generation: u64,
        metadata: Arc<TableMetadata>,
        secondary_indexes: Box<[Option<RuntimeIndexEntry<R>>]>,
    ) -> Self {
        let mut slot_by_id = FastHashMap::default();
        for entry in secondary_indexes.iter().flatten() {
            let previous = slot_by_id.insert(entry.index_ref().id(), entry.index_ref().slot());
            assert!(
                previous.is_none(),
                "table runtime layout invariant violated: duplicate active index id, index={}",
                entry.index_ref()
            );
        }
        let mut indexed_columns = metadata
            .idx
            .index_columns()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        indexed_columns.sort_unstable();
        let layout = Self {
            generation,
            metadata,
            secondary_indexes,
            slot_by_id,
            indexed_columns: indexed_columns.into_boxed_slice(),
        };
        layout.assert_shape();
        layout
    }

    /// Checks the immutable metadata, sparse entries, and exact identity map.
    #[inline]
    fn assert_shape(&self) {
        assert_eq!(
            self.secondary_indexes.len(),
            self.metadata.idx.index_slot_count(),
            "table runtime layout invariant violated: runtime_slots={}, metadata_slots={}",
            self.secondary_indexes.len(),
            self.metadata.idx.index_slot_count()
        );

        for (index_slot, index_spec) in self.metadata.idx.active_indexes() {
            let entry = self
                .secondary_indexes
                .get(index_slot.as_usize())
                .and_then(Option::as_ref);
            assert!(
                entry.is_some(),
                "table runtime layout invariant violated: active metadata index missing runtime slot, index_slot={index_slot}"
            );
            let entry = entry.expect("active metadata entry was asserted present");
            assert_eq!(
                entry.index_ref(),
                index_spec.index,
                "table runtime layout invariant violated: metadata/runtime identity mismatch, generation={}, metadata_index={}, runtime_index={}",
                self.generation,
                index_spec.index,
                entry.index_ref()
            );
            assert_eq!(
                entry.index_ref().slot(),
                index_slot,
                "table runtime layout invariant violated: entry reference targets another slot, expected_slot={index_slot}, index={}",
                entry.index_ref()
            );
            assert_eq!(
                self.slot_by_id.get(&entry.index_ref().id()).copied(),
                Some(entry.index_ref().slot()),
                "table runtime layout invariant violated: active entry is missing or disagrees with id map, index={}",
                entry.index_ref()
            );
        }

        for (index_slot, entry) in self.secondary_indexes.iter().enumerate() {
            let Some(entry) = entry else {
                continue;
            };
            let index_slot = IndexSlot::try_from(index_slot).unwrap_or_else(|_| {
                panic!("validated runtime index slot exceeds u16: index_slot={index_slot}")
            });
            assert!(
                self.metadata.idx.index_spec(index_slot).is_some(),
                "table runtime layout invariant violated: runtime slot has no active metadata spec, index={}",
                entry.index_ref()
            );
        }

        assert_eq!(
            self.slot_by_id.len(),
            self.metadata.idx.active_index_count(),
            "table runtime layout invariant violated: id-map cardinality disagrees with active metadata"
        );
        for (id, slot) in &self.slot_by_id {
            let entry = self
                .secondary_indexes
                .get(slot.as_usize())
                .and_then(Option::as_ref);
            assert!(
                entry.is_some_and(|entry| entry.index_ref() == IndexRef::new(*id, *slot)),
                "table runtime layout invariant violated: id map targets inactive or different entry, index_id={id}, slot={}",
                slot.get()
            );
        }
    }

    /// Returns the monotonic runtime layout generation.
    #[inline]
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns this layout's table metadata.
    #[inline]
    pub(crate) fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Returns this layout's table metadata as a shared owner.
    #[inline]
    pub(crate) fn metadata_arc(&self) -> &Arc<TableMetadata> {
        &self.metadata
    }

    /// Returns the sparse secondary-index slot count.
    #[inline]
    pub(crate) fn index_slot_count(&self) -> usize {
        self.secondary_indexes.len()
    }

    /// Returns the sparse secondary-index runtime slots.
    #[inline]
    pub(crate) fn secondary_indexes(&self) -> &[Option<RuntimeIndexEntry<R>>] {
        &self.secondary_indexes
    }

    /// Consumes the layout and returns its secondary-index runtime slots.
    #[inline]
    pub(crate) fn into_secondary_indexes(self) -> Box<[Option<RuntimeIndexEntry<R>>]> {
        self.secondary_indexes
    }

    /// Resolves one stable identity through the layout's active ID map.
    #[inline]
    pub(crate) fn resolve_index_id(&self, index_id: IndexID) -> Option<IndexRef> {
        #[cfg(test)]
        tests::record_map_resolution();
        self.slot_by_id
            .get(&index_id)
            .copied()
            .map(|slot| IndexRef::new(index_id, slot))
    }

    /// Validates one exact reference using only direct slot/generation access.
    #[inline]
    pub(crate) fn validate_index_ref(&self, index: IndexRef) -> bool {
        #[cfg(test)]
        tests::record_direct_validation();
        self.secondary_indexes
            .get(index.slot().as_usize())
            .and_then(Option::as_ref)
            .is_some_and(|entry| entry.index_ref() == index)
    }

    /// Returns one exact active secondary-index entry.
    #[inline]
    pub(crate) fn index_entry(&self, index: IndexRef) -> RuntimeResult<&RuntimeIndexEntry<R>> {
        self.secondary_indexes
            .get(index.slot().as_usize())
            .and_then(Option::as_ref)
            .filter(|entry| entry.index_ref() == index)
            .ok_or_else(|| self.index_access_error(index))
    }

    /// Returns an entry already resolved against this retained layout.
    /// Layout ownership keeps the exact generation active throughout execution.
    #[inline]
    pub(crate) fn expect_index_entry(&self, index: IndexRef) -> &RuntimeIndexEntry<R> {
        let entry = self
            .secondary_indexes
            .get(index.slot().as_usize())
            .and_then(Option::as_ref)
            .unwrap_or_else(|| {
                panic!(
                    "retained index runtime invariant violated: missing entry, generation={}, index={index}, index_slot_count={}",
                    self.generation,
                    self.index_slot_count()
                )
            });
        assert_eq!(
            entry.index_ref(),
            index,
            "retained index runtime invariant violated: generation mismatch, generation={}, requested_index={index}, actual_index={}",
            self.generation,
            entry.index_ref()
        );
        entry
    }

    /// Returns one active secondary-index entry by an already trusted slot.
    #[inline]
    pub(crate) fn index_entry_at_slot(
        &self,
        slot: IndexSlot,
    ) -> RuntimeResult<&RuntimeIndexEntry<R>> {
        self.secondary_indexes
            .get(slot.as_usize())
            .and_then(Option::as_ref)
            .ok_or_else(|| {
                Report::new(InternalError::SecondaryIndexOutOfBounds)
                    .attach(format!(
                        "index_slot={slot}, index_slot_count={}",
                        self.index_slot_count()
                    ))
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=resolve_secondary_index_runtime")
            })
    }

    #[inline]
    fn index_access_error(&self, index: IndexRef) -> error_stack::Report<RuntimeError> {
        Report::new(InternalError::SecondaryIndexOutOfBounds)
            .attach(format!(
                "index={index}, index_slot_count={}",
                self.index_slot_count()
            ))
            .change_context(RuntimeError::IndexAccess)
            .attach("operation=resolve_secondary_index_runtime")
    }

    /// Qualifies a validated active positional key for retained user state.
    #[inline]
    pub(crate) fn resolve_active_user_key(
        &self,
        index: IndexRef,
        vals: Vec<Val>,
    ) -> ResolvedIndexKey {
        self.expect_index_entry(index);
        user_key_from_index_ref(index, vals)
    }

    /// Iterates active metadata and its exact runtime entry in physical-slot order.
    #[inline]
    pub(crate) fn active_indexes(
        &self,
    ) -> impl Iterator<Item = (&TableIndexMetadata, &RuntimeIndexEntry<R>)> + '_ {
        #[cfg(test)]
        tests::record_active_iteration();
        self.secondary_indexes.iter().flatten().map(|entry| {
            (
                self.metadata.idx.expect_index_spec(entry.index_ref()),
                entry,
            )
        })
    }

    /// Borrows the active indexed columns precomputed in decoding order.
    #[inline]
    pub(crate) fn indexed_column_read_set(&self) -> &[usize] {
        &self.indexed_columns
    }

    /// Checks row-byte compatibility with an owning table's physical storage.
    /// Table ownership is established separately by construction and admission.
    #[inline]
    pub(crate) fn assert_column_layout(&self, columns: &Arc<TableColumnLayout>) {
        assert!(
            Arc::ptr_eq(&self.metadata.col, columns),
            "table runtime layout invariant violated: column allocation mismatch, generation={}",
            self.generation
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::FixedBufferPool;
    use crate::catalog::table::tests::metadata_with_replacement_index;
    use crate::catalog::{
        ActiveIndexSpec, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexKey,
        StorageIndexSpec,
    };
    use crate::table::IndexPlacement;
    use crate::table::tests::*;
    use crate::trx::purge::PurgeTestEvent;
    use crate::value::ValKind;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    static MAP_RESOLUTIONS: AtomicUsize = AtomicUsize::new(0);
    static DIRECT_VALIDATIONS: AtomicUsize = AtomicUsize::new(0);
    static ACTIVE_ITERATIONS: AtomicUsize = AtomicUsize::new(0);

    pub(super) fn record_map_resolution() {
        MAP_RESOLUTIONS.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_direct_validation() {
        DIRECT_VALIDATIONS.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_active_iteration() {
        ACTIVE_ITERATIONS.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn reset_index_access_counters() {
        MAP_RESOLUTIONS.store(0, Ordering::Relaxed);
        DIRECT_VALIDATIONS.store(0, Ordering::Relaxed);
        ACTIVE_ITERATIONS.store(0, Ordering::Relaxed);
    }

    pub(super) fn index_access_counters() -> (usize, usize, usize) {
        (
            MAP_RESOLUTIONS.load(Ordering::Relaxed),
            DIRECT_VALIDATIONS.load(Ordering::Relaxed),
            ACTIVE_ITERATIONS.load(Ordering::Relaxed),
        )
    }

    fn table2_columns() -> Vec<StorageColumnSpec> {
        vec![
            StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
            StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
        ]
    }

    fn metadata_without_indexes() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new(
                vec![StorageColumnSpec::new(
                    ValKind::I32,
                    StorageColumnFlags::empty(),
                )],
                vec![],
            )
            .expect("valid table metadata"),
        )
    }

    fn sparse_binding_metadata() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new_with_index_slot_count(
                table2_columns(),
                vec![
                    ActiveIndexSpec::new(
                        IndexRef::new(IndexID::new(73), IndexSlot::new(0)),
                        StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK),
                    ),
                    ActiveIndexSpec::new(
                        IndexRef::new(IndexID::new(29), IndexSlot::new(2)),
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty(),
                        ),
                    ),
                ],
                IndexSlot::new(3),
            )
            .unwrap(),
        )
    }

    #[test]
    fn generic_layout_rejects_inconsistent_entries_and_identity_maps() {
        let metadata = sparse_binding_metadata();
        let first = metadata.idx.index_spec(IndexSlot::new(0)).unwrap().index;
        let last = metadata.idx.index_spec(IndexSlot::new(2)).unwrap().index;
        let entry = |index| Some(RuntimeIndexEntry::new(index, ()));
        for (case, entries) in [
            ("wrong count", vec![entry(first)]),
            ("missing runtime", vec![entry(first), None, None]),
            (
                "inactive runtime",
                vec![
                    entry(first),
                    entry(IndexRef::new(IndexID::new(81), IndexSlot::new(1))),
                    entry(last),
                ],
            ),
            (
                "wrong id",
                vec![
                    entry(IndexRef::new(IndexID::new(81), first.slot())),
                    None,
                    entry(last),
                ],
            ),
            ("wrong slot", vec![entry(last), None, entry(first)]),
            (
                "duplicate id",
                vec![
                    entry(first),
                    None,
                    entry(IndexRef::new(first.id(), last.slot())),
                ],
            ),
        ] {
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    TableRuntimeLayout::assemble(
                        0,
                        Arc::clone(&metadata),
                        entries.into_boxed_slice(),
                    )
                }))
                .is_err(),
                "layout accepted {case}"
            );
        }

        for case in ["missing id", "extra id", "wrong slot"] {
            let mut layout = TableRuntimeLayout::from_runtimes(
                0,
                Arc::clone(&metadata),
                vec![Some(()), None, Some(())].into_boxed_slice(),
            );
            match case {
                "missing id" => {
                    layout.slot_by_id.remove(&first.id());
                }
                "extra id" => {
                    layout.slot_by_id.insert(IndexID::new(81), first.slot());
                }
                "wrong slot" => {
                    layout.slot_by_id.insert(first.id(), last.slot());
                }
                _ => unreachable!(),
            }
            assert!(
                catch_unwind(AssertUnwindSafe(|| layout.assert_shape())).is_err(),
                "layout accepted {case}"
            );
        }
    }

    #[test]
    fn generic_layout_binds_paired_entries_and_column_allocation() {
        let metadata = sparse_binding_metadata();
        let layout = TableRuntimeLayout::from_runtimes(
            9,
            Arc::clone(&metadata),
            vec![Some(73), None, Some(29)].into_boxed_slice(),
        );
        let pairs = layout
            .active_indexes()
            .map(|(spec, entry)| {
                assert_eq!(spec.index, entry.index_ref());
                assert!(ptr::eq(
                    spec,
                    metadata.idx.expect_index_spec(entry.index_ref())
                ));
                assert!(ptr::eq(
                    entry,
                    layout.index_entry(entry.index_ref()).unwrap()
                ));
                (entry.index_ref().slot(), *entry.runtime())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            pairs,
            vec![(IndexSlot::new(0), 73), (IndexSlot::new(2), 29)]
        );
        assert_eq!(layout.indexed_column_read_set(), &[0, 1]);
        assert_eq!(
            layout.resolve_index_id(IndexID::new(73)),
            Some(metadata.idx.index_spec(IndexSlot::new(0)).unwrap().index)
        );
        assert!(layout.index_entry_at_slot(IndexSlot::new(1)).is_err());
        layout.assert_column_layout(&metadata.col);
        let equal_but_distinct_columns = Arc::new(metadata.col.as_ref().clone());
        assert!(
            catch_unwind(AssertUnwindSafe(
                || layout.assert_column_layout(&equal_but_distinct_columns)
            ))
            .is_err()
        );
    }

    #[test]
    fn runtime_layout_accepts_matching_empty_index_shape() {
        let metadata = metadata_without_indexes();
        let layout = TableRuntimeLayout::new(
            7,
            Arc::clone(&metadata),
            Vec::<Option<Arc<SecondaryIndex<EvictableBufferPool>>>>::new().into_boxed_slice(),
        );

        let memory =
            MemTableLayout::<FixedBufferPool>::new_memory(Arc::clone(&metadata), Box::new([]));
        assert_eq!(memory.generation(), 0);
        assert_eq!(memory.active_indexes().count(), 0);
        assert!(memory.indexed_column_read_set().is_empty());

        assert_eq!(layout.generation(), 7);
        assert_eq!(layout.metadata().idx.index_slot_count(), 0);
        assert_eq!(layout.index_slot_count(), 0);
        assert!(layout.indexed_column_read_set().is_empty());
    }

    #[test]
    fn runtime_layout_rejects_structural_mismatches() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "runtime_layout_validation").await;
            let table_id = create_table2_for_test(&engine).await;
            let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
            let runtime = Arc::clone(
                layout.secondary_indexes()[0]
                    .as_ref()
                    .unwrap()
                    .runtime_arc(),
            );

            let mismatched_ref = IndexRef::new(IndexID::new(100), IndexSlot::new(0));
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    TableRuntimeLayout::from_entries(
                        layout.generation() + 1,
                        Arc::clone(layout.metadata_arc()),
                        vec![Some(RuntimeIndexEntry::new(
                            mismatched_ref,
                            Arc::clone(&runtime),
                        ))]
                        .into_boxed_slice(),
                    )
                }))
                .is_err()
            );

            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    TableRuntimeLayout::new(
                        layout.generation() + 1,
                        Arc::clone(layout.metadata_arc()),
                        vec![None].into_boxed_slice(),
                    )
                }))
                .is_err()
            );

            let inactive_metadata = Arc::new(
                TableMetadata::try_new_with_index_slot_count(
                    table2_columns(),
                    vec![],
                    IndexSlot::new(1),
                )
                .unwrap(),
            );
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    TableRuntimeLayout::new(
                        layout.generation() + 1,
                        inactive_metadata,
                        vec![Some(Arc::clone(&runtime))].into_boxed_slice(),
                    )
                }))
                .is_err()
            );

            let shifted_metadata = Arc::new(
                TableMetadata::try_new_with_index_slot_count(
                    table2_columns(),
                    vec![ActiveIndexSpec::new(
                        IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                        StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK),
                    )],
                    IndexSlot::new(2),
                )
                .unwrap(),
            );
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    TableRuntimeLayout::new(
                        layout.generation() + 1,
                        shifted_metadata,
                        vec![None, Some(Arc::clone(&runtime))].into_boxed_slice(),
                    )
                }))
                .is_err()
            );

            let non_unique_metadata = Arc::new(
                TableMetadata::try_new(
                    table2_columns(),
                    vec![StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::empty(),
                    )],
                )
                .unwrap(),
            );
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    TableRuntimeLayout::new(
                        layout.generation() + 1,
                        non_unique_metadata,
                        vec![Some(runtime)].into_boxed_slice(),
                    )
                }))
                .is_err()
            );
        });
    }

    #[test]
    fn runtime_layout_validates_exact_generation_without_id_map_lookup() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "runtime_layout_generation").await;
            let table_id = create_table2_for_test(&engine).await;
            let current = table_for_internal_assertion(&engine, table_id).layout_snapshot();
            let runtime = Arc::clone(
                current.secondary_indexes()[0]
                    .as_ref()
                    .unwrap()
                    .runtime_arc(),
            );
            let slot = IndexSlot::new(0);
            let old_ref = current
                .index_entry_at_slot(slot)
                .expect("fixture index must be active")
                .index_ref();
            let replacement_ref = IndexRef::new(IndexID::new(100), slot);
            let replacement = TableRuntimeLayout::from_entries(
                current.generation() + 1,
                metadata_with_replacement_index(current.metadata(), replacement_ref),
                vec![Some(RuntimeIndexEntry::new(replacement_ref, runtime))].into_boxed_slice(),
            );

            TableRuntimeLayout::reset_index_access_counters();
            assert_eq!(
                replacement.resolve_index_id(IndexID::new(100)),
                Some(replacement_ref)
            );
            assert_eq!(replacement.resolve_index_id(IndexID::new(0)), None);
            assert!(!replacement.validate_index_ref(old_ref));
            assert!(replacement.validate_index_ref(replacement_ref));
            assert_eq!(TableRuntimeLayout::index_access_counters(), (2, 2, 0));
        });
    }

    #[test]
    fn retained_index_access_requires_exact_active_generation() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "retained_index_invariants").await;
            let table_id = create_table2_for_test(&engine).await;
            let current = table_for_internal_assertion(&engine, table_id).layout_snapshot();
            let index = current.resolve_index_id(IndexID::new(0)).unwrap();
            let runtime = Arc::clone(current.expect_index_entry(index).runtime_arc());
            let metadata = Arc::new(
                TableMetadata::try_new_with_index_slot_count(
                    table2_columns(),
                    vec![ActiveIndexSpec::new(
                        index,
                        StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK),
                    )],
                    IndexSlot::new(2),
                )
                .unwrap(),
            );
            let layout = TableRuntimeLayout::new(
                current.generation() + 1,
                metadata,
                vec![Some(Arc::clone(&runtime)), None].into_boxed_slice(),
            );

            TableRuntimeLayout::reset_index_access_counters();
            assert_eq!(layout.metadata().idx.expect_index_spec(index).index, index);
            assert!(ptr::eq(
                layout.expect_secondary_index(index),
                runtime.as_ref()
            ));
            assert_eq!(TableRuntimeLayout::index_access_counters(), (0, 0, 0));

            for (case, invalid) in [
                (
                    "replaced generation",
                    IndexRef::new(IndexID::new(100), index.slot()),
                ),
                (
                    "inactive slot",
                    IndexRef::new(IndexID::new(1), IndexSlot::new(1)),
                ),
                (
                    "absent slot",
                    IndexRef::new(IndexID::new(2), IndexSlot::new(2)),
                ),
            ] {
                assert!(
                    catch_unwind(AssertUnwindSafe(|| layout.expect_secondary_index(invalid)))
                        .is_err(),
                    "runtime must reject {case}: index={invalid}"
                );
                assert!(
                    catch_unwind(AssertUnwindSafe(|| layout
                        .metadata()
                        .idx
                        .expect_index_spec(invalid)))
                    .is_err(),
                    "metadata must reject {case}: index={invalid}"
                );
            }
        });
    }

    #[test]
    fn test_runtime_layout_install_retains_removed_index_while_layout_is_pinned() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let (purge_event_tx, purge_event_rx) = flume::unbounded();
            engine
                .inner()
                .trx_sys
                .set_purge_test_observer(purge_event_tx);
            let table_id = create_table2_for_test(&engine).await;
            engine.inner().trx_sys.request_purge_observation();
            let mut create_commit_recorded = false;
            loop {
                match purge_event_rx.recv_async().await.unwrap() {
                    PurgeTestEvent::CommittedRecorded { .. } => {
                        create_commit_recorded = true;
                    }
                    PurgeTestEvent::CycleCompleted if create_commit_recorded => break,
                    _ => {}
                }
            }
            let table = table_for_internal_assertion(&engine, table_id);
            let old_layout = table.layout_snapshot();
            assert_eq!(old_layout.metadata().idx.active_index_count(), 1);
            let retired_index = old_layout.secondary_indexes()[0]
                .as_ref()
                .unwrap()
                .index_ref();
            let mut session = engine.new_session().unwrap();
            session
                .drop_index(table_id, retired_index.id())
                .await
                .unwrap();
            let installed = table.layout_snapshot();
            assert_eq!(old_layout.metadata().idx.active_index_count(), 1);
            assert_eq!(installed.metadata().idx.active_index_count(), 0);
            assert_eq!(
                installed.metadata().idx.index_slot_count_u32(),
                old_layout.metadata().idx.index_slot_count_u32()
            );
            assert_eq!(
                installed.metadata().idx.index_slot_count(),
                old_layout.metadata().idx.index_slot_count()
            );
            assert_eq!(installed.index_slot_count(), old_layout.index_slot_count());
            assert!(installed.secondary_indexes()[0].is_none());
            assert_eq!(
                table_for_internal_assertion(&engine, table_id)
                    .metadata()
                    .idx
                    .active_index_count(),
                0
            );
            assert!(
                table_for_internal_assertion(&engine, table_id).has_retired_secondary_indexes()
            );

            let (placement, _) = table_for_internal_assertion(&engine, table_id)
                .select_index_create_placement(installed.metadata().idx.index_slot_count_u32())
                .unwrap();
            assert_eq!(
                placement,
                IndexPlacement::Append(IndexSlot::new(1)),
                "a physical slot cannot be reused while its exact retired owner is registered"
            );

            assert_eq!(
                table
                    .cleanup_retired_secondary_indexes(engine.inner().core.pools.pool_guards())
                    .await
                    .unwrap(),
                0
            );
            drop(old_layout);
        })
    }
}
