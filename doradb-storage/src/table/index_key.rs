use super::layout::TableRuntimeLayout;
use crate::buffer::guard::PageSharedGuard;
use crate::catalog::{IndexRef, IndexSlot, TableMetadata};
use crate::id::RowID;
use crate::map::FastHashMap;
use crate::row::ops::SelectKey;
use crate::row::{RowPage, RowRead};
use crate::value::Val;
use std::marker::PhantomData;

/// One exact index key derived within a borrowed runtime layout.
///
/// The lifetime retains the layout borrow without depending on its runtime
/// owner type. Table admission and row ownership remain the caller's contract.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct WriteIndexKey<'layout> {
    index: IndexRef,
    vals: Vec<Val>,
    _layout: PhantomData<&'layout TableMetadata>,
}

impl<'layout> WriteIndexKey<'layout> {
    /// Resolves a metadata-proven active slot against the borrowed layout.
    #[inline]
    pub(super) fn new<R>(layout: &'layout TableRuntimeLayout<R>, key: SelectKey) -> Self {
        let SelectKey { index_slot, vals } = key;
        let index = layout
            .index_entry_at_slot(index_slot)
            .unwrap_or_else(|err| {
                panic!(
                    "write index key must reference an active layout entry: layout_generation={}, index_slot={index_slot}, error={err:?}",
                    layout.generation()
                )
            })
            .index_ref();
        Self {
            index,
            vals,
            _layout: PhantomData,
        }
    }

    /// Returns the physical slot carried by the exact index identity.
    #[inline]
    pub(super) fn index_slot(&self) -> IndexSlot {
        self.index.slot()
    }

    /// Returns the exact index identity.
    #[inline]
    pub(super) fn index_ref(&self) -> IndexRef {
        self.index
    }

    /// Borrows the owned key values in index-column order.
    #[inline]
    pub(super) fn vals(&self) -> &[Val] {
        &self.vals
    }

    /// Replaces key values while preserving identity and layout lifetime.
    #[inline]
    pub(super) fn with_vals(&self, vals: Vec<Val>) -> Self {
        Self {
            index: self.index,
            vals,
            _layout: PhantomData,
        }
    }

    /// Consumes the key into its exact identity and owned values.
    #[inline]
    pub(super) fn into_parts(self) -> (IndexRef, Vec<Val>) {
        (self.index, self.vals)
    }
}

/// Complete mutation keys derived from one admitted metadata/runtime layout.
///
/// Private fields and controlled construction preserve active index-slot order.
/// The layout lifetime remains attached through either new-side claims
/// or old-row ownership proof construction.
pub(super) struct WriteIndexKeySet<'layout> {
    keys: Vec<WriteIndexKey<'layout>>,
}

impl<'layout> WriteIndexKeySet<'layout> {
    /// Copies every active index key from a validated complete row.
    #[inline]
    pub(super) fn from_full_row<R>(layout: &'layout TableRuntimeLayout<R>, row: &[Val]) -> Self {
        let keys = layout
            .active_indexes()
            .map(|(spec, entry)| {
                let index = entry.index_ref();
                let vals = spec
                    .keys
                    .iter()
                    .map(|key| row[key.column_ordinal.as_usize()].clone())
                    .collect();
                WriteIndexKey {
                    index,
                    vals,
                    _layout: PhantomData,
                }
            })
            .collect();
        Self { keys }
    }

    /// Copies every active key under one physical row read guard.
    ///
    /// The caller retains logical write ownership over this row image. Reading
    /// bypasses MVCC and delete-bit filtering, including after a hot deletion.
    /// Returned values own their bytes and do not retain the page guard.
    #[inline]
    pub(super) fn from_physical_row<R>(
        layout: &'layout TableRuntimeLayout<R>,
        page_guard: &PageSharedGuard<RowPage>,
        row_id: RowID,
    ) -> Self {
        let access = page_guard.read_row_by_id(row_id);
        let row = access.row();
        let columns = layout.metadata().col.as_ref();
        let keys = layout
            .active_indexes()
            .map(|(spec, entry)| {
                let vals = spec
                    .keys
                    .iter()
                    .map(|key| row.val(columns, key.column_ordinal.as_usize()))
                    .collect();
                WriteIndexKey {
                    index: entry.index_ref(),
                    vals,
                    _layout: PhantomData,
                }
            })
            .collect();
        Self { keys }
    }

    /// Binds a complete key set from the metadata-derived indexed-column read.
    #[inline]
    pub(super) fn from_indexed_values<R>(
        layout: &'layout TableRuntimeLayout<R>,
        read_set: &[usize],
        vals: Vec<Val>,
    ) -> Self {
        let generation = layout.generation();
        assert_eq!(
            read_set.len(),
            vals.len(),
            "indexed-column read must return one value per requested column: layout_generation={generation}, read_set_len={}, value_count={}",
            read_set.len(),
            vals.len()
        );
        let indexed_vals = read_set
            .iter()
            .copied()
            .zip(vals)
            .collect::<FastHashMap<_, _>>();
        let keys = layout
            .active_indexes()
            .map(|(index_spec, entry)| {
                let index_ref = entry.index_ref();
                let vals = index_spec
                    .keys
                    .iter()
                    .map(|key| {
                        indexed_vals
                            .get(&(key.column_ordinal.as_usize()))
                            .cloned()
                            .unwrap_or_else(|| {
                                panic!(
                                    "active index column must be present in the metadata-derived read set: layout_generation={generation}, index={index_ref}, column_no={}",
                                    key.column_ordinal
                                )
                            })
                    })
                    .collect();
                WriteIndexKey {
                    index: index_ref,
                    vals,
                    _layout: PhantomData,
                }
            })
            .collect();
        Self { keys }
    }

    /// Borrows all keys in active index-slot order.
    #[inline]
    pub(super) fn as_slice(&self) -> &[WriteIndexKey<'layout>] {
        &self.keys
    }

    /// Consumes the complete set in active index-slot order.
    #[inline]
    pub(super) fn into_keys(self) -> impl Iterator<Item = WriteIndexKey<'layout>> {
        self.keys.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::WriteIndexKeySet;
    use crate::buffer::guard::PageGuard;
    use crate::buffer::{BufferPool, FixedBufferPool, PoolGuards, PoolRole};
    use crate::catalog::{
        ActiveIndexSpec, IndexID, IndexRef, IndexSlot, StorageColumnFlags, StorageColumnSpec,
        StorageIndexFlags, StorageIndexKey, StorageIndexSpec, TableMetadata,
    };
    use crate::id::RowID;
    use crate::index::{BlockIndex, RowLocation};
    use crate::quiescent::QuiescentBox;
    use crate::table::{
        MemTable, MemTableLayout, RowStore, TableRuntimeLayout, build_in_memory_secondary_indexes,
        test_user_table_id,
    };
    use crate::trx::MIN_SNAPSHOT_TS;
    use crate::value::{Val, ValKind};
    use std::sync::Arc;

    fn key_metadata(sparse: bool) -> Arc<TableMetadata> {
        let second_slot = IndexSlot::new(if sparse { 2 } else { 1 });
        Arc::new(
            TableMetadata::try_new_with_index_slot_count(
                vec![
                    StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                    StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                    StorageColumnSpec::new(ValKind::I64, StorageColumnFlags::empty()),
                ],
                vec![
                    ActiveIndexSpec::new(
                        IndexRef::new(IndexID::new(73), IndexSlot::new(0)),
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1), StorageIndexKey::new(0)],
                            StorageIndexFlags::UK,
                        ),
                    ),
                    ActiveIndexSpec::new(
                        IndexRef::new(IndexID::new(29), second_slot),
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1)],
                            StorageIndexFlags::empty(),
                        ),
                    ),
                ],
                IndexSlot::new(second_slot.get() + 1),
            )
            .unwrap(),
        )
    }

    fn assert_empty_keys<R>(layout: &TableRuntimeLayout<R>) {
        let full = WriteIndexKeySet::from_full_row(layout, &[Val::from(7i32)]);
        let indexed = WriteIndexKeySet::from_indexed_values(layout, &[], vec![]);
        assert!(full.as_slice().is_empty());
        assert!(indexed.into_keys().next().is_none());
    }

    #[test]
    fn test_empty_user_and_memory_key_sets() {
        let metadata = Arc::new(
            TableMetadata::try_new(
                vec![StorageColumnSpec::new(
                    ValKind::I32,
                    StorageColumnFlags::empty(),
                )],
                vec![],
            )
            .unwrap(),
        );
        let user = TableRuntimeLayout::new(0, Arc::clone(&metadata), Box::new([]));
        let memory = MemTableLayout::<FixedBufferPool>::new_memory(metadata, Box::new([]));
        assert_empty_keys(&user);
        assert_empty_keys(&memory);
    }

    #[test]
    fn test_memory_key_derivation_owns_live_and_deleted_physical_values() {
        smol::block_on(async {
            for sparse in [false, true] {
                let pool = QuiescentBox::new(
                    FixedBufferPool::with_capacity(PoolRole::Meta, 1024 * 1024).unwrap(),
                );
                let guards = PoolGuards::builder()
                    .push(PoolRole::Meta, pool.create_base_guard())
                    .build();
                let metadata = key_metadata(sparse);
                let indexes = build_in_memory_secondary_indexes(
                    pool.guard(),
                    guards.meta_guard(),
                    &metadata,
                    MIN_SNAPSHOT_TS,
                )
                .await
                .unwrap();
                let block_index = BlockIndex::new_catalog(pool.guard(), guards.meta_guard())
                    .await
                    .unwrap();
                let row_store = RowStore::new(
                    test_user_table_id(301),
                    Arc::clone(&metadata.col),
                    pool.guard(),
                    pool.row_pool_role(),
                    block_index,
                );
                let layout = MemTableLayout::new_memory(metadata, indexes);
                let table = MemTable::new(row_store, layout, PoolRole::Meta);
                let text = Val::from("an indexed value longer than the inline representation");
                let row = vec![Val::from(7i32), text.clone(), Val::from(999i64)];
                table.insert_no_trx(&guards, &row, false).await.unwrap();
                let row_id = RowID::new(0);
                let RowLocation::RowPage(page_id) =
                    table.row_store.find_row(&guards, row_id).await.unwrap()
                else {
                    panic!("inserted row must remain hot")
                };
                {
                    let layout = &table.layout;
                    let full = WriteIndexKeySet::from_full_row(layout, &row);
                    let columns = layout.indexed_column_read_set();
                    assert_eq!(columns, &[0, 1]);
                    let indexed = WriteIndexKeySet::from_indexed_values(
                        layout,
                        columns,
                        vec![row[0].clone(), text.clone()],
                    );
                    assert_eq!(full.as_slice(), indexed.as_slice());
                    let expected = vec![
                        (
                            IndexRef::new(IndexID::new(73), IndexSlot::new(0)),
                            vec![text.clone(), Val::from(7i32)],
                        ),
                        (
                            IndexRef::new(
                                IndexID::new(29),
                                IndexSlot::new(if sparse { 2 } else { 1 }),
                            ),
                            vec![text.clone()],
                        ),
                    ];
                    for deleted in [false, true] {
                        // This isolated table has no concurrent readers or writers.
                        // Set the physical delete bit under exclusive page access.
                        let mut page = table
                            .row_store
                            .must_get_row_page_exclusive(&guards, page_id)
                            .await
                            .unwrap();
                        page.page_mut().set_deleted_exclusive(0, deleted);
                        drop(page);
                        let page = table
                            .row_store
                            .must_get_row_page_shared(&guards, page_id)
                            .await
                            .unwrap();
                        assert_eq!(page.page().is_deleted(0), deleted);
                        let physical = WriteIndexKeySet::from_physical_row(layout, &page, row_id);
                        drop(page);
                        assert_eq!(full.as_slice(), physical.as_slice());
                        let actual = physical
                            .into_keys()
                            .map(|key| key.into_parts())
                            .collect::<Vec<_>>();
                        assert_eq!(actual, expected);
                    }
                }
                table.destroy(&guards).await.unwrap();
                assert_eq!(pool.allocated(), 0);
            }
        });
    }
}
