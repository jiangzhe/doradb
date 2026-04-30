use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::buffer::page::INVALID_PAGE_ID;
use crate::buffer::{BufferPool, EvictableBufferPool, FixedBufferPool, PageID, PoolGuards};
use crate::catalog::{CatalogTable, TableMetadata};
use crate::error::{DataIntegrityError, Error, FileKind, InternalError, OperationError, Result};
use crate::file::BlockID;
use crate::index::util::{Maskable, RowPageCreateRedoCtx};
use crate::index::{
    InMemorySecondaryIndex, IndexCompareExchange, IndexInsert, NonUniqueIndex,
    NonUniqueSecondaryIndex, RowLocation, SecondaryIndex, UniqueIndex, UniqueSecondaryIndex,
};
use crate::lwc::PersistedLwcBlock;
use crate::row::ops::{
    DeleteMvcc, InsertIndex, LinkForUniqueIndex, ReadRow, ScanMvcc, SelectKey, SelectMvcc, UndoCol,
    UpdateCol, UpdateIndex, UpdateMvcc, UpdateRow,
};
use crate::row::{Row, RowID, RowPage, RowRead, estimate_max_row_count, var_len_for_insert};
use crate::table::{
    ColumnDeletionBuffer, ColumnStorage, DeleteInternal, DeleteMarker, DeletionError,
    GenericMemTable, InsertRowIntoPage, Table, TableRootSnapshot, UpdateRowInplace,
    index_key_is_changed, index_key_replace, read_latest_index_key, row_len,
    validate_page_row_range,
};
use crate::trx::redo::{RowRedo, RowRedoKind};
use crate::trx::row::{
    FindOldVersion, LockRowForWrite, LockUndo, ReadAllRows, RowReadAccess, RowWriteAccess,
};
use crate::trx::stmt::StmtEffects;
use crate::trx::undo::{IndexBranch, IndexBranchTarget, OwnedRowUndo, RowUndoKind};
use crate::trx::ver_map::RowPageState;
use crate::trx::{MIN_SNAPSHOT_TS, TrxContext, TrxID, trx_is_committed};
use crate::value::Val;
use error_stack::Report;
use std::collections::HashMap;
use std::future::Future;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;

#[inline]
fn invalid_lwc_payload(
    file_kind: FileKind,
    block_id: BlockID,
    message: impl Into<String>,
) -> Error {
    let message = message.into();
    Report::new(DataIntegrityError::InvalidPayload)
        .attach(format!(
            "file={file_kind}, block=lwc-block, block_id={block_id}, {message}"
        ))
        .into()
}

#[inline]
fn missing_user_secondary_index(index_no: usize) -> Error {
    Report::new(InternalError::UserSecondaryIndexMissing)
        .attach(format!("index_no={index_no}"))
        .into()
}

#[inline]
fn secondary_index_view_mismatch(operation: &'static str) -> Error {
    Report::new(InternalError::SecondaryIndexViewMismatch)
        .attach(format!("operation={operation}"))
        .into()
}

#[inline]
fn missing_secondary_index(index_no: usize, index_count: usize) -> Error {
    Report::new(InternalError::SecondaryIndexOutOfBounds)
        .attach(format!("index_no={index_no}, index_count={index_count}"))
        .into()
}

#[inline]
fn secondary_index_kind_mismatch(operation: &'static str, expected: &'static str) -> Error {
    Report::new(InternalError::SecondaryIndexKindMismatch)
        .attach(format!("operation={operation}, expected={expected}"))
        .into()
}

#[derive(Clone, Copy)]
enum SecondaryIndexView {
    Catalog { ts: TrxID },
    User { ts: TrxID, root: BlockID },
}

impl SecondaryIndexView {
    #[inline]
    const fn catalog(ts: TrxID) -> Self {
        Self::Catalog { ts }
    }

    #[inline]
    const fn user(ts: TrxID, root: BlockID) -> Self {
        Self::User { ts, root }
    }

    #[inline]
    const fn ts(self) -> TrxID {
        match self {
            Self::Catalog { ts } | Self::User { ts, .. } => ts,
        }
    }
}

#[derive(Clone, Copy)]
struct RowIdMove {
    old: RowID,
    new: RowID,
}

impl RowIdMove {
    #[inline]
    const fn new(old: RowID, new: RowID) -> Self {
        Self { old, new }
    }
}

pub trait TableAccess {
    /// Table scan including uncommitted versions.
    ///
    /// This method iterates raw latest row versions and includes rows marked
    /// as deleted. Callers should explicitly filter `row.is_deleted()` if they
    /// only need live rows.
    ///
    /// Note: this scans only in-memory row-store pages and does not include
    /// persisted column-store rows on disk.
    fn table_scan_uncommitted<F>(
        &self,
        guards: &PoolGuards,
        row_action: F,
    ) -> impl Future<Output = ()>
    where
        F: for<'m, 'p> FnMut(&'m TableMetadata, Row<'p>) -> bool;

    /// Table scan with MVCC.
    ///
    /// Note: this scans only in-memory row-store pages and does not include
    /// persisted column-store rows on disk.
    fn table_scan_mvcc<F>(
        &self,
        ctx: &TrxContext,
        read_set: &[usize],
        row_action: F,
    ) -> impl Future<Output = ()>
    where
        F: FnMut(Vec<Val>) -> bool;

    /// Index lookup unique row with MVCC.
    /// Result should be no more than one row.
    fn index_lookup_unique_mvcc(
        &self,
        ctx: &TrxContext,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> impl Future<Output = Result<SelectMvcc>>;

    /// Index lookup unique row including uncommitted version.
    fn index_lookup_unique_uncommitted<R, F>(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_action: F,
    ) -> impl Future<Output = Result<Option<R>>>
    where
        for<'m, 'p> F: FnOnce(&'m TableMetadata, Row<'p>) -> R;

    /// Index scan with MVCC of given key.
    fn index_scan_mvcc(
        &self,
        ctx: &TrxContext,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> impl Future<Output = Result<ScanMvcc>>;

    /// Insert row in transaction.
    fn insert_mvcc(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        cols: Vec<Val>,
    ) -> impl Future<Output = Result<RowID>>;

    /// Update row in transaction.
    /// This method is for update based on unique index lookup.
    /// It also takes care of index change.
    fn update_unique_mvcc(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> impl Future<Output = Result<UpdateMvcc>>;

    /// Delete row in transaction.
    /// This method is for delete based on unique index lookup.
    ///
    /// If the parameter log_by_key is set to true, the delete operation
    /// is logged with (unique) key instead of row id.
    /// Such type of log is used for catalog tables, which will have
    /// inconsistent page_id/row_id among multiple restarts(recoveries)
    /// of database.
    fn delete_unique_mvcc(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        key: &SelectKey,
        log_by_key: bool,
    ) -> impl Future<Output = Result<DeleteMvcc>>;

    /// Delete index by purge threads.
    /// This method will be only called by internal threads and don't maintain
    /// transaction properties.
    ///
    /// It checks whether the index entry still points to valid row, and if not,
    /// remove the entry.
    ///
    /// The validation is based on MVCC with minimum active STS. If the input
    /// key is not found on the path of undo chain, it means the index entry can be
    /// removed.
    ///
    /// todo: The look-back mechanism for each index entry is not performant. A
    /// potential optimization is similar to covering index, page ts can be checked
    /// to see if all delete-masked values in it can be remove directly.
    fn delete_index(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        unique: bool,
        min_active_sts: TrxID,
    ) -> impl Future<Output = Result<bool>>;
}

/// Thin operation wrapper that exposes `TableAccess` over a table reference.
///
/// `D` is the row-page pool type and `I` is the secondary-index pool type.
/// Runtime aliases bind user tables to evictable row/index pools and catalog
/// tables to the fixed metadata pool for both roles.
pub struct TableAccessor<'a, D: 'static, I: 'static> {
    mem: &'a GenericMemTable<D, I>,
    storage: Option<&'a ColumnStorage>,
    user_sec_idx: Option<&'a [SecondaryIndex<EvictableBufferPool>]>,
}

enum IndexPurgeDecision {
    Delete,
    Keep,
    RowPage(PageID),
}

enum ColdRowUpdateRead {
    Ok(Vec<Val>),
    NotFound,
    WriteConflict,
}

#[inline]
fn ctx_pool_guards(ctx: &TrxContext) -> &PoolGuards {
    ctx.pool_guards()
        .expect("table access requires an attached session for pool guards")
}

impl<'a> From<&'a Table> for TableAccessor<'a, EvictableBufferPool, EvictableBufferPool> {
    #[inline]
    fn from(table: &'a Table) -> Self {
        TableAccessor {
            mem: table,
            storage: Some(&table.storage),
            user_sec_idx: Some(table.sec_idx()),
        }
    }
}

impl<'a> From<&'a CatalogTable> for TableAccessor<'a, FixedBufferPool, FixedBufferPool> {
    #[inline]
    fn from(table: &'a CatalogTable) -> Self {
        TableAccessor {
            mem: table,
            storage: None,
            user_sec_idx: None,
        }
    }
}

impl<'a, D: BufferPool, I: BufferPool> TableAccessor<'a, D, I> {
    #[inline]
    fn generic_sec_idx(&self) -> &[InMemorySecondaryIndex<I>] {
        self.mem.sec_idx()
    }

    #[inline]
    fn sec_idx_len(&self) -> usize {
        self.user_sec_idx
            .map_or_else(|| self.generic_sec_idx().len(), <[_]>::len)
    }

    #[inline]
    fn sec_idx_is_unique(&self, index_no: usize) -> bool {
        match self.user_sec_idx {
            Some(indexes) => indexes[index_no].is_unique(),
            None => self.generic_sec_idx()[index_no].is_unique(),
        }
    }

    #[inline]
    fn bound_secondary_view(
        &self,
        ctx: &TrxContext,
        index_no: usize,
    ) -> Result<SecondaryIndexView> {
        let Some(storage) = self.storage else {
            return Ok(SecondaryIndexView::catalog(ctx.sts()));
        };
        let proof = ctx.read_proof();
        let root = storage.with_active_root(&proof, |root| {
            root.secondary_index_roots
                .get(index_no)
                .copied()
                .ok_or_else(|| missing_secondary_index(index_no, root.secondary_index_roots.len()))
        })?;
        Ok(SecondaryIndexView::user(ctx.sts(), root))
    }

    #[inline]
    fn unchecked_secondary_view(&self, ts: TrxID, index_no: usize) -> Result<SecondaryIndexView> {
        let Some(storage) = self.storage else {
            return Ok(SecondaryIndexView::catalog(ts));
        };
        let root = storage
            .file()
            .active_root_unchecked()
            .secondary_index_roots
            .get(index_no)
            .copied()
            .ok_or_else(|| {
                missing_secondary_index(
                    index_no,
                    storage
                        .file()
                        .active_root_unchecked()
                        .secondary_index_roots
                        .len(),
                )
            })?;
        Ok(SecondaryIndexView::user(ts, root))
    }

    #[inline]
    fn root_snapshot<'ctx>(
        &self,
        ctx: &'ctx TrxContext,
    ) -> Result<Option<TableRootSnapshot<'ctx>>> {
        let Some(storage) = self.storage else {
            return Ok(None);
        };
        let proof = ctx.read_proof();
        Ok(Some(storage.with_active_root(&proof, |root| {
            TableRootSnapshot::from_active_root(root, &proof)
        })))
    }

    #[inline]
    fn snapshot_secondary_view(
        &self,
        ts: TrxID,
        snapshot: Option<&TableRootSnapshot<'_>>,
        index_no: usize,
    ) -> Result<SecondaryIndexView> {
        match (self.user_sec_idx, snapshot) {
            (Some(_), Some(snapshot)) => Ok(SecondaryIndexView::user(
                ts,
                snapshot.secondary_index_root(index_no)?,
            )),
            (Some(_), None) => Err(missing_user_secondary_index(index_no)),
            (None, _) => Ok(SecondaryIndexView::catalog(ts)),
        }
    }

    #[inline]
    fn bound_unique_user_index(
        &self,
        index_no: usize,
        root: BlockID,
    ) -> Result<UniqueSecondaryIndex<'a, EvictableBufferPool>> {
        let indexes = self
            .user_sec_idx
            .ok_or_else(|| missing_user_secondary_index(index_no))?;
        indexes[index_no].bind_unique(root)
    }

    #[inline]
    fn bound_non_unique_user_index(
        &self,
        index_no: usize,
        root: BlockID,
    ) -> Result<NonUniqueSecondaryIndex<'a, EvictableBufferPool>> {
        let indexes = self
            .user_sec_idx
            .ok_or_else(|| missing_user_secondary_index(index_no))?;
        indexes[index_no].bind_non_unique(root)
    }

    #[inline]
    async fn unique_lookup(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key: &[Val],
        view: SecondaryIndexView,
    ) -> Result<Option<(RowID, bool)>> {
        let index_pool_guard = self.index_pool_guard(guards);
        match (self.user_sec_idx, view) {
            (Some(_), SecondaryIndexView::User { ts, root }) => {
                self.bound_unique_user_index(index_no, root)?
                    .lookup(index_pool_guard, key, ts)
                    .await
            }
            (Some(_), SecondaryIndexView::Catalog { .. }) => {
                Err(secondary_index_view_mismatch("unique lookup"))
            }
            (None, view) => {
                self.generic_sec_idx()[index_no]
                    .unique()
                    .ok_or_else(|| secondary_index_kind_mismatch("unique lookup", "unique"))?
                    .lookup(index_pool_guard, key, view.ts())
                    .await
            }
        }
    }

    #[inline]
    async fn unique_insert_if_not_exists(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        view: SecondaryIndexView,
    ) -> Result<IndexInsert> {
        let index_pool_guard = self.index_pool_guard(guards);
        match (self.user_sec_idx, view) {
            (Some(_), SecondaryIndexView::User { ts, root }) => {
                self.bound_unique_user_index(index_no, root)?
                    .insert_if_not_exists(index_pool_guard, key, row_id, merge_if_match_deleted, ts)
                    .await
            }
            (Some(_), SecondaryIndexView::Catalog { .. }) => {
                Err(secondary_index_view_mismatch("unique insert"))
            }
            (None, view) => {
                self.generic_sec_idx()[index_no]
                    .unique()
                    .ok_or_else(|| secondary_index_kind_mismatch("unique insert", "unique"))?
                    .insert_if_not_exists(
                        index_pool_guard,
                        key,
                        row_id,
                        merge_if_match_deleted,
                        view.ts(),
                    )
                    .await
            }
        }
    }

    #[inline]
    async fn unique_compare_delete(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key: &[Val],
        old_row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool> {
        let index_pool_guard = self.index_pool_guard(guards);
        match self.user_sec_idx {
            Some(indexes) => {
                indexes[index_no]
                    .unique_mem()?
                    .compare_delete(index_pool_guard, key, old_row_id, ignore_del_mask, ts)
                    .await
            }
            None => {
                self.generic_sec_idx()[index_no]
                    .unique()
                    .ok_or_else(|| {
                        secondary_index_kind_mismatch("unique compare delete", "unique")
                    })?
                    .compare_delete(index_pool_guard, key, old_row_id, ignore_del_mask, ts)
                    .await
            }
        }
    }

    #[inline]
    async fn unique_mask_as_deleted(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key: &[Val],
        row_id: RowID,
        view: SecondaryIndexView,
    ) -> Result<bool> {
        let index_pool_guard = self.index_pool_guard(guards);
        match (self.user_sec_idx, view) {
            (Some(_), SecondaryIndexView::User { ts, root }) => {
                self.bound_unique_user_index(index_no, root)?
                    .mask_as_deleted(index_pool_guard, key, row_id, ts)
                    .await
            }
            (Some(_), SecondaryIndexView::Catalog { .. }) => {
                Err(secondary_index_view_mismatch("unique mask deleted"))
            }
            (None, view) => {
                self.generic_sec_idx()[index_no]
                    .unique()
                    .ok_or_else(|| secondary_index_kind_mismatch("unique mask deleted", "unique"))?
                    .mask_as_deleted(index_pool_guard, key, row_id, view.ts())
                    .await
            }
        }
    }

    #[inline]
    async fn unique_compare_exchange(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
        view: SecondaryIndexView,
    ) -> Result<IndexCompareExchange> {
        let index_pool_guard = self.index_pool_guard(guards);
        match (self.user_sec_idx, view) {
            (Some(_), SecondaryIndexView::User { ts, root }) => {
                self.bound_unique_user_index(index_no, root)?
                    .compare_exchange(index_pool_guard, key, old_row_id, new_row_id, ts)
                    .await
            }
            (Some(_), SecondaryIndexView::Catalog { .. }) => {
                Err(secondary_index_view_mismatch("unique compare exchange"))
            }
            (None, view) => {
                self.generic_sec_idx()[index_no]
                    .unique()
                    .ok_or_else(|| {
                        secondary_index_kind_mismatch("unique compare exchange", "unique")
                    })?
                    .compare_exchange(index_pool_guard, key, old_row_id, new_row_id, view.ts())
                    .await
            }
        }
    }

    #[inline]
    async fn non_unique_lookup(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key: &[Val],
        res: &mut Vec<RowID>,
        view: SecondaryIndexView,
    ) -> Result<()> {
        let index_pool_guard = self.index_pool_guard(guards);
        match (self.user_sec_idx, view) {
            (Some(_), SecondaryIndexView::User { ts, root }) => {
                self.bound_non_unique_user_index(index_no, root)?
                    .lookup(index_pool_guard, key, res, ts)
                    .await
            }
            (Some(_), SecondaryIndexView::Catalog { .. }) => {
                Err(secondary_index_view_mismatch("non-unique lookup"))
            }
            (None, view) => {
                self.generic_sec_idx()[index_no]
                    .non_unique()
                    .ok_or_else(|| {
                        secondary_index_kind_mismatch("non-unique lookup", "non-unique")
                    })?
                    .lookup(index_pool_guard, key, res, view.ts())
                    .await
            }
        }
    }

    #[inline]
    async fn non_unique_lookup_unique(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key: &[Val],
        row_id: RowID,
        view: SecondaryIndexView,
    ) -> Result<Option<bool>> {
        let index_pool_guard = self.index_pool_guard(guards);
        match (self.user_sec_idx, view) {
            (Some(_), SecondaryIndexView::User { ts, root }) => {
                self.bound_non_unique_user_index(index_no, root)?
                    .lookup_unique(index_pool_guard, key, row_id, ts)
                    .await
            }
            (Some(_), SecondaryIndexView::Catalog { .. }) => {
                Err(secondary_index_view_mismatch("non-unique lookup unique"))
            }
            (None, view) => {
                self.generic_sec_idx()[index_no]
                    .non_unique()
                    .ok_or_else(|| {
                        secondary_index_kind_mismatch("non-unique lookup unique", "non-unique")
                    })?
                    .lookup_unique(index_pool_guard, key, row_id, view.ts())
                    .await
            }
        }
    }

    #[inline]
    async fn non_unique_insert_if_not_exists(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        view: SecondaryIndexView,
    ) -> Result<IndexInsert> {
        let index_pool_guard = self.index_pool_guard(guards);
        match (self.user_sec_idx, view) {
            (Some(_), SecondaryIndexView::User { ts, root }) => {
                self.bound_non_unique_user_index(index_no, root)?
                    .insert_if_not_exists(index_pool_guard, key, row_id, merge_if_match_deleted, ts)
                    .await
            }
            (Some(_), SecondaryIndexView::Catalog { .. }) => {
                Err(secondary_index_view_mismatch("non-unique insert"))
            }
            (None, view) => {
                self.generic_sec_idx()[index_no]
                    .non_unique()
                    .ok_or_else(|| {
                        secondary_index_kind_mismatch("non-unique insert", "non-unique")
                    })?
                    .insert_if_not_exists(
                        index_pool_guard,
                        key,
                        row_id,
                        merge_if_match_deleted,
                        view.ts(),
                    )
                    .await
            }
        }
    }

    #[inline]
    async fn non_unique_mask_as_deleted(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key: &[Val],
        row_id: RowID,
        view: SecondaryIndexView,
    ) -> Result<bool> {
        let index_pool_guard = self.index_pool_guard(guards);
        match (self.user_sec_idx, view) {
            (Some(_), SecondaryIndexView::User { ts, root }) => {
                self.bound_non_unique_user_index(index_no, root)?
                    .mask_as_deleted(index_pool_guard, key, row_id, ts)
                    .await
            }
            (Some(_), SecondaryIndexView::Catalog { .. }) => {
                Err(secondary_index_view_mismatch("non-unique mask deleted"))
            }
            (None, view) => {
                self.generic_sec_idx()[index_no]
                    .non_unique()
                    .ok_or_else(|| {
                        secondary_index_kind_mismatch("non-unique mask deleted", "non-unique")
                    })?
                    .mask_as_deleted(index_pool_guard, key, row_id, view.ts())
                    .await
            }
        }
    }

    #[inline]
    async fn non_unique_compare_delete(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key: &[Val],
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool> {
        let index_pool_guard = self.index_pool_guard(guards);
        match self.user_sec_idx {
            Some(indexes) => {
                indexes[index_no]
                    .non_unique_mem()?
                    .compare_delete(index_pool_guard, key, row_id, ignore_del_mask, ts)
                    .await
            }
            None => {
                self.generic_sec_idx()[index_no]
                    .non_unique()
                    .ok_or_else(|| {
                        secondary_index_kind_mismatch("non-unique compare delete", "non-unique")
                    })?
                    .compare_delete(index_pool_guard, key, row_id, ignore_del_mask, ts)
                    .await
            }
        }
    }

    #[inline]
    fn deletion_buffer(&self) -> Option<&ColumnDeletionBuffer> {
        self.storage.map(ColumnStorage::deletion_buffer)
    }

    /// Returns the deletion buffer for paths that already resolved a row as LWC.
    /// Catalog accessors have no column storage, but they also must not resolve
    /// row ids to persisted LWC blocks.
    #[inline]
    fn lwc_deletion_buffer(&self) -> Result<&ColumnDeletionBuffer> {
        self.deletion_buffer().ok_or_else(|| {
            Report::new(InternalError::DeletionBufferMissing)
                .attach("LWC row access requires column storage")
                .into()
        })
    }

    #[inline]
    async fn index_lookup_unique_row_mvcc(
        &self,
        ctx: &TrxContext,
        key: &SelectKey,
        user_read_set: &[usize],
        row_id: RowID,
    ) -> Result<SelectMvcc> {
        loop {
            let location = self
                .try_find_row(ctx_pool_guards(ctx), row_id, self.storage)
                .await?;
            match location {
                RowLocation::NotFound => return Ok(SelectMvcc::NotFound),
                RowLocation::LwcBlock {
                    block_id,
                    row_idx,
                    row_shape_fingerprint,
                } => {
                    let deletion_buffer = self.lwc_deletion_buffer()?;
                    if let Some(marker) = deletion_buffer.get(row_id) {
                        match marker {
                            DeleteMarker::Committed(ts) => {
                                if ts <= ctx.sts() {
                                    return Ok(SelectMvcc::NotFound);
                                }
                            }
                            DeleteMarker::Ref(status) => {
                                let ts = status.ts();
                                if trx_is_committed(ts) {
                                    if ts <= ctx.sts() {
                                        return Ok(SelectMvcc::NotFound);
                                    }
                                } else if Arc::ptr_eq(&status, &ctx.status()) {
                                    return Ok(SelectMvcc::NotFound);
                                }
                            }
                        }
                    }
                    let vals = self
                        .read_lwc_row(
                            ctx_pool_guards(ctx),
                            block_id,
                            row_idx,
                            row_shape_fingerprint,
                            user_read_set,
                        )
                        .await?;
                    return Ok(SelectMvcc::Found(vals));
                }
                RowLocation::RowPage(page_id) => {
                    let page_guard = match self
                        .get_row_page_shared(ctx_pool_guards(ctx), page_id)
                        .await
                    {
                        Ok(Some(page_guard)) => page_guard,
                        Ok(None) => continue,
                        Err(err) => return Err(err),
                    };
                    let page = page_guard.page();
                    if !page.row_id_in_valid_range(row_id) {
                        continue;
                    }
                    let (page_ctx, page) = page_guard.ctx_and_page();
                    let access = RowReadAccess::new(page, page_ctx, page.row_idx(row_id));
                    return match access.read_row_mvcc(
                        ctx,
                        self.metadata(),
                        user_read_set,
                        Some(key),
                    ) {
                        ReadRow::Ok(vals) => Ok(SelectMvcc::Found(vals)),
                        ReadRow::InvalidIndex | ReadRow::NotFound => Ok(SelectMvcc::NotFound),
                    };
                }
            }
        }
    }

    #[inline]
    async fn read_lwc_row(
        &self,
        guards: &PoolGuards,
        block_id: BlockID,
        row_idx: usize,
        row_shape_fingerprint: u128,
        read_set: &[usize],
    ) -> Result<Vec<Val>> {
        let Some(storage) = self.storage else {
            return Err(Report::new(InternalError::TableStorageMissing)
                .attach(format!("block_id={block_id}, row_idx={row_idx}"))
                .into());
        };
        let block = PersistedLwcBlock::load(
            storage.file().file_kind(),
            storage.file().sparse_file(),
            storage.disk_pool(),
            guards.disk_guard(),
            block_id,
        )
        .await?;
        if block.row_shape_fingerprint() != row_shape_fingerprint {
            return Err(invalid_lwc_payload(
                FileKind::TableFile,
                block_id,
                "row shape fingerprint mismatch",
            ));
        }
        block.decode_row_values(self.metadata(), row_idx, read_set)
    }

    #[inline]
    async fn read_lwc_full_row(
        &self,
        guards: &PoolGuards,
        block_id: BlockID,
        row_idx: usize,
        row_shape_fingerprint: u128,
    ) -> Result<Vec<Val>> {
        let Some(storage) = self.storage else {
            return Err(Report::new(InternalError::TableStorageMissing)
                .attach(format!("block_id={block_id}, row_idx={row_idx}"))
                .into());
        };
        let block = PersistedLwcBlock::load(
            storage.file().file_kind(),
            storage.file().sparse_file(),
            storage.disk_pool(),
            guards.disk_guard(),
            block_id,
        )
        .await?;
        if block.row_shape_fingerprint() != row_shape_fingerprint {
            return Err(invalid_lwc_payload(
                FileKind::TableFile,
                block_id,
                "row shape fingerprint mismatch",
            ));
        }
        block.decode_full_row_values(self.metadata(), row_idx)
    }

    #[inline]
    fn index_keys_from_indexed_values(
        &self,
        read_set: &[usize],
        vals: Vec<Val>,
    ) -> Result<Vec<SelectKey>> {
        if read_set.len() != vals.len() {
            return Err(Report::new(InternalError::IndexedValueMissing)
                .attach(format!(
                    "read_set_len={}, value_count={}",
                    read_set.len(),
                    vals.len()
                ))
                .into());
        }
        let indexed_vals = read_set
            .iter()
            .copied()
            .zip(vals)
            .collect::<HashMap<_, _>>();
        self.metadata()
            .index_specs
            .iter()
            .enumerate()
            .map(|(index_no, index)| {
                let vals = index
                    .index_cols
                    .iter()
                    .map(|key| {
                        indexed_vals
                            .get(&(key.col_no as usize))
                            .cloned()
                            .ok_or_else(|| {
                                Error::from(Report::new(InternalError::IndexedValueMissing).attach(
                                    format!("index_no={index_no}, column_no={}", key.col_no),
                                ))
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(SelectKey::new(index_no, vals))
            })
            .collect()
    }

    #[inline]
    async fn read_lwc_index_keys(
        &self,
        guards: &PoolGuards,
        block_id: BlockID,
        row_idx: usize,
        row_shape_fingerprint: u128,
    ) -> Result<Vec<SelectKey>> {
        let mut read_set = self
            .metadata()
            .index_cols
            .iter()
            .copied()
            .collect::<Vec<_>>();
        read_set.sort_unstable();
        let vals = self
            .read_lwc_row(guards, block_id, row_idx, row_shape_fingerprint, &read_set)
            .await?;
        self.index_keys_from_indexed_values(&read_set, vals)
    }

    #[inline]
    fn index_key_matches(keys: &[SelectKey], key: &SelectKey) -> Result<bool> {
        let old_key = keys.get(key.index_no).ok_or_else(|| {
            Error::from(Report::new(InternalError::IndexKeyMissing).attach(format!(
                "index_no={}, key_count={}",
                key.index_no,
                keys.len()
            )))
        })?;
        Ok(old_key.vals == key.vals)
    }

    #[inline]
    async fn read_lwc_row_for_update(
        &self,
        ctx: &TrxContext,
        key: &SelectKey,
        row_id: RowID,
        block_id: BlockID,
        row_idx: usize,
        row_shape_fingerprint: u128,
    ) -> Result<ColdRowUpdateRead> {
        let deletion_buffer = self.lwc_deletion_buffer()?;
        // Cold rows are immutable, so their write visibility is tracked by the
        // column deletion buffer rather than by a row-page undo chain. A marker
        // committed at or before this writer's snapshot means the row is gone
        // for this statement. An uncommitted marker owned by this transaction
        // means this statement already consumed the cold row. A marker owned by
        // another active transaction is a write conflict.
        if let Some(marker) = deletion_buffer.get(row_id) {
            match marker {
                DeleteMarker::Committed(ts) => {
                    if ts <= ctx.sts() {
                        return Ok(ColdRowUpdateRead::NotFound);
                    }
                }
                DeleteMarker::Ref(status) => {
                    let ts = status.ts();
                    if trx_is_committed(ts) {
                        if ts <= ctx.sts() {
                            return Ok(ColdRowUpdateRead::NotFound);
                        }
                    } else if Arc::ptr_eq(&status, &ctx.status()) {
                        // This transaction already consumed the cold row.
                        return Ok(ColdRowUpdateRead::NotFound);
                    } else {
                        return Ok(ColdRowUpdateRead::WriteConflict);
                    }
                }
            }
        }
        // Decode after the deletion-buffer visibility check, then revalidate
        // the unique key. The index candidate can be stale while delete/index
        // cleanup catches up with a cold-row delete.
        let vals = self
            .read_lwc_full_row(
                ctx_pool_guards(ctx),
                block_id,
                row_idx,
                row_shape_fingerprint,
            )
            .await?;
        if !self.metadata().match_key(key, &vals) {
            return Ok(ColdRowUpdateRead::NotFound);
        }
        Ok(ColdRowUpdateRead::Ok(vals))
    }

    #[inline]
    async fn persisted_lwc_key_differs(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        block_id: BlockID,
        row_idx: usize,
        row_shape_fingerprint: u128,
    ) -> Result<bool> {
        let read_set = self.metadata().index_specs[key.index_no]
            .index_cols
            .iter()
            .map(|key| key.col_no as usize)
            .collect::<Vec<_>>();
        let vals = self
            .read_lwc_row(guards, block_id, row_idx, row_shape_fingerprint, &read_set)
            .await?;
        Ok(vals.as_slice() != key.vals.as_slice())
    }

    #[inline]
    async fn index_purge_decision(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        min_active_sts: TrxID,
    ) -> Result<IndexPurgeDecision> {
        // This path is physical GC cleanup for a previously delete-marked
        // secondary-index entry. A cold delete marker proves that every key for
        // the row is unreachable only after its transaction is committed and
        // older than the current purge horizon.
        if self.deletion_buffer().is_some_and(|deletion_buffer| {
            deletion_buffer.delete_marker_is_globally_purgeable(row_id, min_active_sts)
        }) {
            return Ok(IndexPurgeDecision::Delete);
        }

        match self.try_find_row(guards, row_id, self.storage).await? {
            RowLocation::NotFound => Ok(IndexPurgeDecision::Delete),
            RowLocation::LwcBlock {
                block_id,
                row_idx,
                row_shape_fingerprint,
            } => {
                // LWC rows are immutable persisted images. If no globally
                // purgeable marker proves the whole row invisible, decode only
                // the indexed columns and delete the purge key only when it no
                // longer matches the persisted current key.
                if self
                    .persisted_lwc_key_differs(
                        guards,
                        key,
                        block_id,
                        row_idx,
                        row_shape_fingerprint,
                    )
                    .await?
                {
                    Ok(IndexPurgeDecision::Delete)
                } else {
                    Ok(IndexPurgeDecision::Keep)
                }
            }
            RowLocation::RowPage(page_id) => Ok(IndexPurgeDecision::RowPage(page_id)),
        }
    }

    #[inline]
    async fn insert_index_no_trx(
        &self,
        guards: &PoolGuards,
        key: SelectKey,
        row_id: RowID,
    ) -> Result<()> {
        if self.metadata().index_specs[key.index_no].unique() {
            let res = self
                .unique_insert_if_not_exists(
                    guards,
                    key.index_no,
                    &key.vals,
                    row_id,
                    false,
                    SecondaryIndexView::catalog(MIN_SNAPSHOT_TS),
                )
                .await?;
            assert!(res.is_ok());
        } else {
            let res = self
                .non_unique_insert_if_not_exists(
                    guards,
                    key.index_no,
                    &key.vals,
                    row_id,
                    false,
                    SecondaryIndexView::catalog(MIN_SNAPSHOT_TS),
                )
                .await?;
            assert!(res.is_ok());
        }
        Ok(())
    }

    #[inline]
    async fn insert_index(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<InsertIndex> {
        if self.metadata().index_specs[key.index_no].unique() {
            self.insert_unique_index(ctx, effects, key, row_id, page_guard, root_snapshot)
                .await
        } else {
            self.insert_non_unique_index(ctx, effects, key, row_id, root_snapshot)
                .await
        }
    }

    #[inline]
    async fn insert_row_internal(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        mut insert: Vec<Val>,
        mut undo_kind: RowUndoKind,
        mut index_branches: Vec<IndexBranch>,
    ) -> Result<(RowID, PageSharedGuard<RowPage>)> {
        let metadata = self.metadata();
        let row_len = row_len(metadata, &insert);
        let row_count = estimate_max_row_count(row_len, metadata.col_count());
        loop {
            let page_guard = self.get_insert_page(ctx, row_count).await?;
            match self.insert_row_to_page(
                ctx,
                effects,
                page_guard,
                insert,
                undo_kind,
                index_branches,
            ) {
                InsertRowIntoPage::Ok(row_id, page_guard) => {
                    ctx.save_active_insert_page(
                        self.table_id(),
                        page_guard.versioned_page_id(),
                        row_id,
                    );
                    return Ok((row_id, page_guard));
                }
                // this page cannot be inserted any more, just leave it and retry another page.
                InsertRowIntoPage::NoSpaceOrFrozen(ins, uk, ib) => {
                    insert = ins;
                    undo_kind = uk;
                    index_branches = ib;
                }
            }
        }
    }

    #[inline]
    async fn insert_no_trx_inner(&self, guards: &PoolGuards, cols: &[Val]) -> Result<()> {
        debug_assert!(cols.len() == self.metadata().col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| self.metadata().col_type_match(idx, val))
        });
        let metadata = self.metadata();
        // prepare index keys.
        let keys = metadata.keys_for_insert(cols);
        // calculate row length.
        let row_len = row_len(metadata, cols);
        // estimate max row count for insert page.
        let row_count = estimate_max_row_count(row_len, metadata.col_count());
        loop {
            // acquire insert page from block index.
            let mut page_guard =
                GenericMemTable::get_insert_page_exclusive(self, guards, row_count, None).await?;
            let page = page_guard.page_mut();
            debug_assert!(metadata.col_count() == page.header.col_count as usize);
            debug_assert!(cols.len() == page.header.col_count as usize);
            let var_len = var_len_for_insert(metadata, cols);
            let (row_idx, var_offset) =
                if let Some((row_idx, var_offset)) = page.request_row_idx_and_free_space(var_len) {
                    (row_idx, var_offset)
                } else {
                    // we just ignore this page and retry.
                    continue;
                };
            let row_id = page.header.start_row_id + row_idx as RowID;
            let mut row = page.row_mut_exclusive(row_idx, var_offset, var_offset + var_len);
            debug_assert!(row.is_deleted());
            for (col_idx, user_col) in cols.iter().enumerate() {
                row.update_col(metadata, col_idx, user_col, false);
            }
            // update index
            for key in keys {
                self.insert_index_no_trx(guards, key, row_id).await?;
            }
            row.finish_insert();
            // Cache insert page.
            GenericMemTable::cache_exclusive_insert_page(self, page_guard);
            return Ok(());
        }
    }

    #[inline]
    async fn delete_unique_no_trx_inner(&self, guards: &PoolGuards, key: &SelectKey) -> Result<()> {
        debug_assert!(key.index_no < self.sec_idx_len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        let view = self.unchecked_secondary_view(MIN_SNAPSHOT_TS, key.index_no)?;
        let (mut page_guard, row_id) = match self
            .unique_lookup(guards, key.index_no, &key.vals, view)
            .await?
        {
            None => unreachable!(),
            Some((row_id, _)) => match self.find_row(guards, row_id, self.storage).await {
                RowLocation::NotFound => unreachable!(),
                RowLocation::LwcBlock { .. } => todo!("lwc block"),
                RowLocation::RowPage(page_id) => {
                    let page_guard = self
                        .get_row_page_exclusive(guards, page_id)
                        .await
                        .expect("delete_unique_no_trx_inner should not ignore page-I/O failures")
                        .expect("failed to lock exclusive row page");
                    (page_guard, row_id)
                }
            },
        };
        let page = page_guard.page_mut();
        let row_idx = page.row_idx(row_id);
        debug_assert!(!page.is_deleted(row_idx));
        let row = page.row(row_idx);
        let keys = self.metadata().keys_for_delete(row);
        // delete index immediately.
        for key in keys {
            let res = self.delete_index_directly(guards, &key, row_id).await?;
            assert!(res);
        }
        page.set_deleted_exclusive(row_idx, true);
        Ok(())
    }

    /// Insert row into given page.
    /// There might be move+update call this method, in such case, undo_kind will be
    /// set to UndoKind::Update.
    /// If row page is frozen, the insert will fail.
    #[inline]
    pub(super) fn insert_row_to_page(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        page_guard: PageSharedGuard<RowPage>,
        cols: Vec<Val>,
        undo_kind: RowUndoKind,
        index_branches: Vec<IndexBranch>,
    ) -> InsertRowIntoPage {
        debug_assert!(matches!(undo_kind, RowUndoKind::Insert));
        let metadata = self.metadata();
        let page_id = page_guard.page_id();
        let versioned_page_id = page_guard.versioned_page_id();
        let (page_ctx, page) = page_guard.ctx_and_page();
        let ver_map = page_ctx.row_ver().unwrap();
        let state_guard = ver_map.read_state();
        if *state_guard != RowPageState::Active {
            return InsertRowIntoPage::NoSpaceOrFrozen(cols, undo_kind, index_branches);
        }
        debug_assert!(metadata.col_count() == page.header.col_count as usize);
        debug_assert!(cols.len() == page.header.col_count as usize);

        let var_len = var_len_for_insert(metadata, &cols);
        let (row_idx, var_offset) =
            if let Some((row_idx, var_offset)) = page.request_row_idx_and_free_space(var_len) {
                (row_idx, var_offset)
            } else {
                return InsertRowIntoPage::NoSpaceOrFrozen(cols, undo_kind, index_branches);
            };
        // Before real insert, we need to lock the row.
        let row_id = page.header.start_row_id + row_idx as u64;
        let mut access = RowWriteAccess::new_with_state_guard(
            page,
            page_ctx,
            row_idx,
            Some(ctx.sts()),
            true,
            state_guard,
        );
        let res = access.lock_undo(
            ctx,
            effects,
            metadata,
            self.table_id(),
            versioned_page_id,
            row_id,
            None,
        );
        debug_assert!(res.is_ok());
        // Apply insert
        let mut new_row = page.new_row(row_idx, var_offset);
        for v in &cols {
            new_row.add_col(metadata, v);
        }
        let new_row_id = new_row.finish();
        debug_assert!(new_row_id == row_id);
        effects.update_last_row_undo(undo_kind);
        for branch in index_branches {
            match branch.target {
                IndexBranchTarget::Hot { cts, entry } => {
                    access.link_for_unique_index(branch.key, cts, entry, branch.undo_vals);
                }
                IndexBranchTarget::ColdTerminal { delete_cts } => {
                    access.link_for_unique_index_cold_terminal(
                        branch.key,
                        delete_cts,
                        branch.undo_vals,
                    );
                }
            }
        }
        drop(access);

        // Here we do not unlock the page because we need to verify validity of unique index update
        // according to this insert.
        // There might be scenario that a deleted row or old version of updated row shares the same
        // key with this insert.
        // Then we have to link insert's undo head to that version via *INDEX* branch.
        // Hold the page guard in order to re-lock the undo head fast.
        //
        // create redo log.
        let redo_entry = RowRedo {
            page_id,
            row_id,
            kind: RowRedoKind::Insert(cols),
        };
        // Store redo in the statement buffer until the statement succeeds.
        effects.insert_row_redo(self.table_id(), redo_entry);
        InsertRowIntoPage::Ok(row_id, page_guard)
    }

    #[inline]
    pub(super) async fn update_row_inplace(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        page_guard: PageSharedGuard<RowPage>,
        key: &SelectKey,
        row_id: RowID,
        mut update: Vec<UpdateCol>,
    ) -> UpdateRowInplace {
        let page_id = page_guard.page_id();
        let (_, page) = page_guard.ctx_and_page();
        // column indexes must be in range
        debug_assert!(
            {
                update
                    .iter()
                    .all(|uc| uc.idx < page_guard.page().header.col_count as usize)
            },
            "update column indexes must be in range"
        );
        // column indexes should be in order.
        debug_assert!(
            {
                update.is_empty()
                    || update
                        .iter()
                        .zip(update.iter().skip(1))
                        .all(|(l, r)| l.idx < r.idx)
            },
            "update columns should be in order"
        );
        if row_id < page.header.start_row_id
            || row_id >= page.header.start_row_id + page.header.max_row_count as u64
        {
            return UpdateRowInplace::RowNotFound;
        }
        // The row-page image must not change until the undo-head lock is
        // installed. The lock path also rejects stale index candidates whose
        // latest hot-row key no longer matches the lookup key.
        let mut lock_row = self
            .lock_row_for_write(ctx, effects, &page_guard, row_id, Some(key))
            .await;
        let metadata = self.metadata();
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => UpdateRowInplace::RowNotFound,
            LockRowForWrite::WriteConflict => UpdateRowInplace::WriteConflict,
            LockRowForWrite::RetryInTransition => UpdateRowInplace::RetryInTransition,
            LockRowForWrite::Ok(access) => {
                let mut access = access.take().unwrap();
                let frozen = access.page_state() == RowPageState::Frozen;
                if access.row().is_deleted() {
                    return UpdateRowInplace::RowDeleted;
                }
                match access.update_row(metadata, &update, frozen) {
                    UpdateRow::NoFreeSpaceOrFrozen(old_row) => {
                        // The hot row cannot be updated in place because the
                        // page has no reusable space or has been frozen for
                        // tuple movement. Convert this statement into a move
                        // update: delete the old RowID with undo, insert the
                        // replacement as a new hot RowID, and connect unique
                        // owners with runtime branches when older snapshots
                        // may still need the old version.
                        //
                        // Mark page data as deleted.
                        access.delete_row();
                        // Update LOCK entry to DELETE entry.
                        effects.update_last_row_undo(RowUndoKind::Delete);
                        drop(access); // unlock row
                        drop(lock_row);
                        // Here we do not unlock page because we need to perform out-of-place
                        // update and link undo entries of two rows via index branches.
                        // The re-lock of current undo is required.
                        let redo_entry = RowRedo {
                            page_id,
                            row_id,
                            // use DELETE for redo is ok, no version chain should be maintained if recovering from redo.
                            kind: RowRedoKind::Delete,
                        };
                        effects.insert_row_redo(self.table_id(), redo_entry);
                        UpdateRowInplace::NoFreeSpace(row_id, old_row, update, page_guard)
                    }
                    UpdateRow::Ok(mut row) => {
                        // In-place update keeps the RowID stable. Only changed
                        // columns are copied into undo/redo; indexed old values
                        // are retained so MemIndex can shadow or remap keys
                        // after the row latch is released.
                        let mut index_change_cols = HashMap::new();
                        // perform in-place update.
                        let (mut undo_cols, mut redo_cols) = (vec![], vec![]);
                        for uc in &mut update {
                            if let Some((old_val, var_offset)) =
                                row.different(metadata, uc.idx, &uc.val)
                            {
                                let new_val = mem::take(&mut uc.val);
                                // we also check whether the value change is related to any index,
                                // so we can update index later.
                                if metadata.index_cols.contains(&uc.idx) {
                                    index_change_cols.insert(uc.idx, old_val.clone());
                                }
                                // actual update
                                row.update_col(metadata, uc.idx, &new_val);
                                // record undo and redo
                                undo_cols.push(UndoCol {
                                    idx: uc.idx,
                                    val: old_val,
                                    var_offset,
                                });
                                redo_cols.push(UpdateCol {
                                    idx: uc.idx,
                                    // new value no longer needed, so safe to take it here.
                                    val: new_val,
                                });
                            }
                        }
                        // The provisional row lock now becomes the operation
                        // kind that MVCC reads and rollback will interpret.
                        effects.update_last_row_undo(RowUndoKind::Update(undo_cols));
                        // Mark this access as update, so page-level max_ins_sts will be updated.
                        access.enable_ins_or_update();
                        drop(access); // unlock the row.
                        drop(lock_row);
                        // we may still need this page if we'd like to update index.
                        if !redo_cols.is_empty() {
                            // A no-op update still used a row lock, but only a
                            // real value change needs redo.
                            let redo_entry = RowRedo {
                                page_id,
                                row_id,
                                kind: RowRedoKind::Update(redo_cols),
                            };
                            effects.insert_row_redo(self.table_id(), redo_entry);
                        }
                        UpdateRowInplace::Ok(row_id, index_change_cols, page_guard)
                    }
                }
            }
        }
    }

    #[inline]
    async fn move_update_for_space(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        old_row: Vec<(Val, Option<u16>)>,
        update: Vec<UpdateCol>,
        old_id: RowID,
        old_guard: PageSharedGuard<RowPage>,
    ) -> Result<(RowID, HashMap<usize, Val>, PageSharedGuard<RowPage>)> {
        // Build the replacement hot row and remember indexed old values. The
        // caller already turned the old hot row's first undo entry into
        // `Delete`, so this path is logically delete-old plus insert-new.
        let (new_row, old_vals, index_change_cols) = {
            let mut index_change_cols = HashMap::new();
            let mut row = Vec::with_capacity(old_row.len());
            let mut old_vals = Vec::with_capacity(old_row.len());
            for (v, _) in old_row {
                old_vals.push(v.clone());
                row.push(v);
            }
            let metadata = self.metadata();
            for mut uc in update {
                let old_val = &mut row[uc.idx];
                if old_val != &uc.val {
                    if metadata.index_cols.contains(&uc.idx) {
                        index_change_cols.insert(uc.idx, old_val.clone());
                    }
                    // swap old value and new value
                    mem::swap(&mut uc.val, old_val);
                }
            }
            (row, old_vals, index_change_cols)
        };
        let metadata = self.metadata();
        let undo_vals: Vec<UpdateCol> = new_row
            .iter()
            .enumerate()
            .filter_map(|(idx, val)| {
                if val != &old_vals[idx] {
                    Some(UpdateCol {
                        idx,
                        val: old_vals[idx].clone(),
                    })
                } else {
                    None
                }
            })
            .collect();
        let index_branches = {
            // Unique indexes keep only the latest owner in MemIndex. When a
            // move update changes RowID, the new hot row's insert undo carries
            // runtime branches back to the deleted old hot row so older
            // snapshots can still resolve the previous unique-key owner.
            let (page_ctx, page) = old_guard.ctx_and_page();
            let old_access = RowReadAccess::new(page, page_ctx, page.row_idx(old_id));
            let undo_head = old_access.undo_head().expect("undo head");
            debug_assert!(ctx.is_same_trx(undo_head));
            let old_entry = old_access.first_undo_entry().expect("old undo entry");
            debug_assert!(matches!(old_entry.as_ref().kind, RowUndoKind::Delete));
            metadata
                .index_specs
                .iter()
                .enumerate()
                .filter(|(_, index)| index.unique())
                .map(|(index_no, index)| {
                    let vals = index
                        .index_cols
                        .iter()
                        .map(|key| new_row[key.col_no as usize].clone())
                        .collect();
                    IndexBranch {
                        key: SelectKey::new(index_no, vals),
                        target: IndexBranchTarget::Hot {
                            cts: undo_head.ts(),
                            entry: old_entry.clone(),
                        },
                        undo_vals: undo_vals.clone(),
                    }
                })
                .collect::<Vec<_>>()
        };
        old_guard.set_dirty(); // mark as dirty page.
        let (new_row_id, new_guard) = self
            .insert_row_internal(ctx, effects, new_row, RowUndoKind::Insert, index_branches)
            .await?;
        // do not unlock the page because we may need to update index
        Ok((new_row_id, index_change_cols, new_guard))
    }

    #[inline]
    fn build_cold_update_row(&self, mut vals: Vec<Val>, update: Vec<UpdateCol>) -> Vec<Val> {
        for uc in update {
            let val = &mut vals[uc.idx];
            if val != &uc.val {
                *val = uc.val;
            }
        }
        vals
    }

    #[inline]
    async fn update_indexes_only_key_change(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        index_change_cols: &HashMap<usize, Val>,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<UpdateIndex> {
        let metadata = self.metadata();
        for (index_no, index_schema) in metadata.index_specs.iter().enumerate() {
            debug_assert_eq!(self.sec_idx_is_unique(index_no), index_schema.unique());
            if index_key_is_changed(index_schema, index_change_cols) {
                let new_key = read_latest_index_key(metadata, index_no, page_guard, row_id);
                let old_key = index_key_replace(index_schema, &new_key, index_change_cols);
                // First we need to insert new entry to index due to key change.
                // There might be conflict we will try to fix (if old one is already deleted).
                // Once the insert is done, we also need to defer deletion of original key.
                if index_schema.unique() {
                    match self
                        .update_unique_index_only_key_change(
                            ctx,
                            effects,
                            old_key,
                            new_key,
                            row_id,
                            page_guard,
                            root_snapshot,
                        )
                        .await?
                    {
                        UpdateIndex::Updated => (),
                        UpdateIndex::WriteConflict => return Ok(UpdateIndex::WriteConflict),
                        UpdateIndex::DuplicateKey => return Ok(UpdateIndex::DuplicateKey),
                    }
                } else {
                    let res = self
                        .update_non_unique_index_only_key_change(
                            ctx,
                            effects,
                            old_key,
                            new_key,
                            row_id,
                            root_snapshot,
                        )
                        .await?;
                    debug_assert!(res.is_updated());
                }
            } // otherwise, in-place update do not change row id, so we do nothing
        }
        Ok(UpdateIndex::Updated)
    }

    #[inline]
    async fn update_indexes_only_row_id_change(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        old_row_id: RowID,
        new_row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<UpdateIndex> {
        debug_assert!(old_row_id != new_row_id);
        let metadata = self.metadata();
        for (index_no, index_schema) in metadata.index_specs.iter().enumerate() {
            debug_assert_eq!(self.sec_idx_is_unique(index_no), index_schema.unique());
            let key = read_latest_index_key(metadata, index_no, page_guard, new_row_id);
            if index_schema.unique() {
                let res = self
                    .update_unique_index_only_row_id_change(
                        ctx,
                        effects,
                        key,
                        old_row_id,
                        new_row_id,
                        root_snapshot,
                    )
                    .await?;
                debug_assert!(res.is_updated());
            } else {
                let res = self
                    .update_non_unique_index_only_row_id_change(
                        ctx,
                        effects,
                        key,
                        old_row_id,
                        new_row_id,
                        root_snapshot,
                    )
                    .await?;
                debug_assert!(res.is_updated());
            }
        }
        Ok(UpdateIndex::Updated)
    }

    #[inline]
    async fn update_indexes_may_both_change(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        row_id_move: RowIdMove,
        index_change_cols: &HashMap<usize, Val>,
        page_guard: &PageSharedGuard<RowPage>,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<UpdateIndex> {
        debug_assert!(row_id_move.old != row_id_move.new);
        let metadata = self.metadata();
        for (index_no, index_schema) in metadata.index_specs.iter().enumerate() {
            debug_assert_eq!(self.sec_idx_is_unique(index_no), index_schema.unique());
            let key = read_latest_index_key(metadata, index_no, page_guard, row_id_move.new);
            if index_key_is_changed(index_schema, index_change_cols) {
                let old_key = index_key_replace(index_schema, &key, index_change_cols);
                // key change and row id change.
                if index_schema.unique() {
                    match self
                        .update_unique_index_key_and_row_id_change(
                            ctx,
                            effects,
                            old_key,
                            key,
                            row_id_move.old,
                            row_id_move.new,
                            page_guard,
                            root_snapshot,
                        )
                        .await?
                    {
                        UpdateIndex::DuplicateKey => return Ok(UpdateIndex::DuplicateKey),
                        UpdateIndex::WriteConflict => return Ok(UpdateIndex::WriteConflict),
                        UpdateIndex::Updated => (),
                    }
                } else {
                    let res = self
                        .update_non_unique_index_key_and_row_id_change(
                            ctx,
                            effects,
                            old_key,
                            key,
                            row_id_move.old,
                            row_id_move.new,
                            root_snapshot,
                        )
                        .await?;
                    debug_assert!(res.is_updated());
                }
            } else {
                // only row id change.
                if index_schema.unique() {
                    match self
                        .update_unique_index_only_row_id_change(
                            ctx,
                            effects,
                            key,
                            row_id_move.old,
                            row_id_move.new,
                            root_snapshot,
                        )
                        .await?
                    {
                        UpdateIndex::DuplicateKey => return Ok(UpdateIndex::DuplicateKey),
                        UpdateIndex::WriteConflict => return Ok(UpdateIndex::WriteConflict),
                        UpdateIndex::Updated => (),
                    }
                } else {
                    let res = self
                        .update_non_unique_index_only_row_id_change(
                            ctx,
                            effects,
                            key,
                            row_id_move.old,
                            row_id_move.new,
                            root_snapshot,
                        )
                        .await?;
                    debug_assert!(res.is_updated());
                }
            }
        }
        Ok(UpdateIndex::Updated)
    }

    #[inline]
    pub(super) async fn delete_row_internal(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        page_guard: PageSharedGuard<RowPage>,
        row_id: RowID,
        key: &SelectKey,
        log_by_key: bool,
    ) -> DeleteInternal {
        let page_id = page_guard.page_id();
        let (_, page) = page_guard.ctx_and_page();
        if !page.row_id_in_valid_range(row_id) {
            return DeleteInternal::NotFound;
        }
        // The undo-head lock is the conflict check for hot delete. Once the
        // lock is owned, the row-page delete bit becomes the latest image and
        // the same undo entry is rewritten to `Delete`.
        let mut lock_row = self
            .lock_row_for_write(ctx, effects, &page_guard, row_id, Some(key))
            .await;
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => DeleteInternal::NotFound,
            LockRowForWrite::WriteConflict => DeleteInternal::WriteConflict,
            LockRowForWrite::RetryInTransition => DeleteInternal::RetryInTransition,
            LockRowForWrite::Ok(access) => {
                let mut access = access.take().unwrap();
                if access.row().is_deleted() {
                    return DeleteInternal::NotFound;
                }
                access.delete_row();
                // update LOCK entry to DELETE entry.
                effects.update_last_row_undo(RowUndoKind::Delete);
                drop(access); // unlock row.
                drop(lock_row);
                // hold page lock in order to update index later.
                // create redo log.
                let redo_entry = RowRedo {
                    page_id,
                    row_id,
                    kind: if log_by_key {
                        RowRedoKind::DeleteByUniqueKey(key.clone())
                    } else {
                        RowRedoKind::Delete
                    },
                };
                effects.insert_row_redo(self.table_id(), redo_entry);
                DeleteInternal::Ok(page_guard)
            }
        }
    }

    #[inline]
    async fn defer_delete_indexes(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<()> {
        let metadata = self.metadata();
        let keys = (0..self.sec_idx_len())
            .map(|index_no| read_latest_index_key(metadata, index_no, page_guard, row_id))
            .collect();
        self.defer_delete_index_keys(ctx, effects, row_id, keys, root_snapshot)
            .await
    }

    #[inline]
    async fn defer_delete_index_keys(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        row_id: RowID,
        keys: Vec<SelectKey>,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<()> {
        debug_assert_eq!(keys.len(), self.sec_idx_len());
        for key in keys {
            let index_schema = &self.metadata().index_specs[key.index_no];
            debug_assert_eq!(self.sec_idx_is_unique(key.index_no), index_schema.unique());
            if index_schema.unique() {
                self.defer_delete_unique_index(ctx, effects, row_id, key, root_snapshot)
                    .await?;
            } else {
                self.defer_delete_non_unique_index(ctx, effects, row_id, key, root_snapshot)
                    .await?;
            }
        }
        Ok(())
    }

    #[inline]
    async fn delete_index_directly(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
    ) -> Result<bool> {
        let index_schema = &self.metadata().index_specs[key.index_no];
        if index_schema.unique() {
            self.unique_compare_delete(
                guards,
                key.index_no,
                &key.vals,
                row_id,
                true,
                MIN_SNAPSHOT_TS,
            )
            .await
        } else {
            self.non_unique_compare_delete(
                guards,
                key.index_no,
                &key.vals,
                row_id,
                true,
                MIN_SNAPSHOT_TS,
            )
            .await
        }
    }

    #[inline]
    async fn delete_unique_index(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        min_active_sts: TrxID,
    ) -> Result<bool> {
        let (page_guard, row_id) = loop {
            let view = self.unchecked_secondary_view(MIN_SNAPSHOT_TS, key.index_no)?;
            match self
                .unique_lookup(guards, key.index_no, &key.vals, view)
                .await?
            {
                None => return Ok(false), // Another thread deleted this entry.
                Some((index_row_id, deleted)) => {
                    if !deleted || index_row_id != row_id {
                        // 1. Delete flag is unset by other transaction,
                        // so we skip to delete it.
                        // 2. Row id changed, means another transaction inserted
                        // new row with same key and reused this index entry.
                        // So we skip to delete it.
                        return Ok(false);
                    }
                    match self
                        .index_purge_decision(guards, key, row_id, min_active_sts)
                        .await?
                    {
                        IndexPurgeDecision::Delete => {
                            return self
                                .unique_compare_delete(
                                    guards,
                                    key.index_no,
                                    &key.vals,
                                    row_id,
                                    false,
                                    MIN_SNAPSHOT_TS,
                                )
                                .await;
                        }
                        IndexPurgeDecision::Keep => return Ok(false),
                        IndexPurgeDecision::RowPage(page_id) => {
                            let Some(page_guard) = self
                                .try_get_validated_row_page_shared_result(guards, page_id, row_id)
                                .await?
                            else {
                                continue;
                            };
                            break (page_guard, row_id);
                        }
                    }
                }
            }
        };
        let (ctx, page) = page_guard.ctx_and_page();
        let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
        // To safely delete an index entry, we need to make sure
        // no version with matched keys can be found in either page
        // data or version chain. Hot row pages still have undo chains, unlike
        // LWC rows whose persisted image is the only current key material.
        if !access.any_version_matches_key(self.metadata(), key) {
            return self
                .unique_compare_delete(
                    guards,
                    key.index_no,
                    &key.vals,
                    row_id,
                    false,
                    MIN_SNAPSHOT_TS,
                )
                .await;
        }
        Ok(false)
    }

    #[inline]
    async fn delete_non_unique_index(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        min_active_sts: TrxID,
    ) -> Result<bool> {
        let (page_guard, row_id) = loop {
            let view = self.unchecked_secondary_view(MIN_SNAPSHOT_TS, key.index_no)?;
            match self
                .non_unique_lookup_unique(guards, key.index_no, &key.vals, row_id, view)
                .await?
            {
                None => return Ok(false), // Another thread deleted this entry.
                Some(active) => {
                    if active {
                        // 1. Delete flag is unset by other transaction,
                        // so we skip to delete it.
                        // 2. Row id changed, means another transaction inserted
                        // new row with same key and reused this index entry.
                        // So we skip to delete it.
                        return Ok(false);
                    }
                    match self
                        .index_purge_decision(guards, key, row_id, min_active_sts)
                        .await?
                    {
                        IndexPurgeDecision::Delete => {
                            return self
                                .non_unique_compare_delete(
                                    guards,
                                    key.index_no,
                                    &key.vals,
                                    row_id,
                                    false,
                                    MIN_SNAPSHOT_TS,
                                )
                                .await;
                        }
                        IndexPurgeDecision::Keep => return Ok(false),
                        IndexPurgeDecision::RowPage(page_id) => {
                            let Some(page_guard) = self
                                .try_get_validated_row_page_shared_result(guards, page_id, row_id)
                                .await?
                            else {
                                continue;
                            };
                            break (page_guard, row_id);
                        }
                    }
                }
            }
        };
        let (ctx, page) = page_guard.ctx_and_page();
        let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
        // To safely delete an index entry, we need to make sure
        // no version with matched keys can be found in either page
        // data or version chain. Hot row pages still have undo chains, unlike
        // LWC rows whose persisted image is the only current key material.
        if !access.any_version_matches_key(self.metadata(), key) {
            return self
                .non_unique_compare_delete(
                    guards,
                    key.index_no,
                    &key.vals,
                    row_id,
                    false,
                    MIN_SNAPSHOT_TS,
                )
                .await;
        }
        Ok(false)
    }

    #[inline]
    pub(super) async fn try_get_validated_row_page_shared_result(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
        row_id: RowID,
    ) -> Result<Option<PageSharedGuard<RowPage>>> {
        let Some(page_guard) = self.get_row_page_shared(guards, page_id).await? else {
            return Ok(None);
        };
        if validate_page_row_range(&page_guard, page_id, row_id) {
            Ok(Some(page_guard))
        } else {
            Ok(None)
        }
    }

    #[inline]
    async fn get_insert_page(
        &self,
        ctx: &TrxContext,
        row_count: usize,
    ) -> Result<PageSharedGuard<RowPage>> {
        if let Some((page_id, row_id)) = ctx.load_active_insert_page(self.table_id()) {
            let page_guard = self
                .get_row_page_versioned_shared(ctx_pool_guards(ctx), page_id)
                .await?;
            if let Some(page_guard) = page_guard {
                // because we save last insert page in session and meanwhile other thread may access this page
                // and do some modification, even worse, buffer pool may evict it and reload other data into
                // this page. so here, we do not require that no change should happen, but if something change,
                // we validate that page id and row id range is still valid.
                if validate_page_row_range(&page_guard, page_id.page_id, row_id) {
                    return Ok(page_guard);
                }
            }
        }
        let redo_ctx = self.row_page_create_redo_ctx(ctx);
        GenericMemTable::try_get_insert_page(self, ctx_pool_guards(ctx), row_count, redo_ctx).await
    }

    #[inline]
    fn row_page_create_redo_ctx<'b>(
        &self,
        ctx: &'b TrxContext,
    ) -> Option<RowPageCreateRedoCtx<'b>> {
        self.storage?;
        let engine = ctx
            .engine()
            .expect("user-table insert requires an attached engine");
        Some(RowPageCreateRedoCtx::new(&engine.trx_sys, self.table_id()))
    }

    // Hot-row writes acquire ownership by installing a `Lock` undo entry at
    // the row's undo head. That entry is the row-level write lock and the
    // rollback anchor that is later rewritten to Insert/Update/Delete.
    // clippy cannot find the guard is actually dropped before await point.
    #[allow(clippy::await_holding_lock)]
    #[inline]
    pub(super) async fn lock_row_for_write<'b>(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        page_guard: &'b PageSharedGuard<RowPage>,
        row_id: RowID,
        key: Option<&SelectKey>,
    ) -> LockRowForWrite<'b> {
        let (page_ctx, page) = page_guard.ctx_and_page();
        let ver_map = page_ctx.row_ver().unwrap();
        loop {
            let state_guard = ver_map.read_state();
            if *state_guard == RowPageState::Transition {
                return LockRowForWrite::RetryInTransition;
            }
            let mut access = RowWriteAccess::new_with_state_guard(
                page,
                page_ctx,
                page.row_idx(row_id),
                Some(ctx.sts()),
                false,
                state_guard,
            );
            let lock_undo = access.lock_undo(
                ctx,
                effects,
                self.metadata(),
                self.table_id(),
                page_guard.versioned_page_id(),
                row_id,
                key,
            );
            match lock_undo {
                LockUndo::Ok => {
                    return LockRowForWrite::Ok(Some(access));
                }
                LockUndo::InvalidIndex => {
                    return LockRowForWrite::InvalidIndex;
                }
                LockUndo::WriteConflict => {
                    return LockRowForWrite::WriteConflict;
                }
                LockUndo::Preparing(listener) => {
                    if let Some(listener) = listener {
                        drop(access);

                        // Here we do not unlock the page, because the preparation time of commit is supposed
                        // to be short.
                        // And as active transaction is using this page, we don't want page evictor swap it onto
                        // disk.
                        // Other transactions can still access this page and modify other rows.

                        listener.await; // wait for that transaction to be committed.

                        // now we get back on current page.
                        // maybe another thread modify our row before the lock acquisition,
                        // so we need to recheck.
                    } // there might be progress on preparation, so recheck.
                }
            }
        }
    }

    #[inline]
    async fn unmasked_duplicate_has_lwc_delete_marker(
        &self,
        ctx: &TrxContext,
        row_id: RowID,
    ) -> Result<bool> {
        // The normal LWC delete/update path first writes the CDB marker and
        // then masks index entries. Another transaction can observe the small
        // window before masking completes. Any LWC marker therefore forces the
        // duplicate path through link_for_unique_index_lwc(), where snapshot
        // visibility decides between duplicate, link, and write conflict.
        match self
            .try_find_row(ctx_pool_guards(ctx), row_id, self.storage)
            .await?
        {
            RowLocation::LwcBlock { .. } => {
                let deletion_buffer = self.lwc_deletion_buffer()?;
                Ok(deletion_buffer.get(row_id).is_some())
            }
            RowLocation::RowPage(_) | RowLocation::NotFound => Ok(false),
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    async fn link_for_unique_index_lwc(
        &self,
        ctx: &TrxContext,
        old_id: RowID,
        key: &SelectKey,
        new_id: RowID,
        new_guard: &PageSharedGuard<RowPage>,
        block_id: BlockID,
        row_idx: usize,
        row_shape_fingerprint: u128,
    ) -> Result<LinkForUniqueIndex> {
        let deletion_buffer = self.lwc_deletion_buffer()?;
        // Convert the CDB marker into the delete timestamp carried by a cold
        // terminal unique branch. `None` below means the old cold owner is
        // still visible to this statement, either because there is no marker or
        // because the committed delete is newer than the statement snapshot. If
        // the persisted row still matches the key, that remains a duplicate.
        // `Some(None)` means this transaction itself installed the cold delete
        // marker, so the old cold image must be reachable only through the new
        // hot row's runtime unique branch. `Some(Some(ts))` means an earlier
        // committed delete is visible to this statement, and older snapshots
        // before `ts` may still need the old cold image.
        let delete_cts = match deletion_buffer.get(old_id) {
            None => None,
            Some(DeleteMarker::Committed(ts)) => {
                if ts <= ctx.sts() {
                    Some(Some(ts))
                } else {
                    None
                }
            }
            Some(DeleteMarker::Ref(status)) => {
                let ts = status.ts();
                if trx_is_committed(ts) {
                    if ts <= ctx.sts() {
                        Some(Some(ts))
                    } else {
                        None
                    }
                } else if Arc::ptr_eq(&status, &ctx.status()) {
                    Some(None)
                } else {
                    return Ok(LinkForUniqueIndex::WriteConflict);
                }
            }
        };
        let old_row = self
            .read_lwc_full_row(
                ctx_pool_guards(ctx),
                block_id,
                row_idx,
                row_shape_fingerprint,
            )
            .await?;
        // The unique index entry may be stale while purge is catching up, so
        // verify the persisted row still owns the key before linking it.
        if !self.metadata().match_key(key, &old_row) {
            return Ok(LinkForUniqueIndex::NotNeeded);
        }
        let Some(delete_cts) = delete_cts else {
            return Ok(LinkForUniqueIndex::DuplicateKey);
        };
        let metadata = self.metadata();
        let (page_ctx, page) = new_guard.ctx_and_page();
        let mut new_access =
            RowWriteAccess::new(page, page_ctx, page.row_idx(new_id), Some(ctx.sts()), false);
        let undo_vals = new_access.row().calc_delta(metadata, &old_row);
        // The new hot row owns the key now. The terminal branch preserves the
        // old cold image for snapshots that still need to see it. The branch is
        // runtime-only; recovery restores only the latest committed mapping.
        new_access.link_for_unique_index_cold_terminal(key.clone(), delete_cts, undo_vals);
        Ok(LinkForUniqueIndex::Linked)
    }

    /// Link old version for index.
    /// This is a special operation for unique index maintenance.
    /// It's triggered by duplicate key finding when updating index.
    ///
    /// There are scenarios as below:
    /// 1. The old row not found. Just skip it.
    /// 2. The old row is being modified. Just throw write conflict.
    /// 3. Then we search from row page through version chain,
    ///    try to find one version that is not deleted and matches
    ///    the index key.
    ///    a) we find it, then link it.
    ///    b) no version found, we skip this row.
    #[inline]
    async fn link_for_unique_index(
        &self,
        ctx: &TrxContext,
        old_id: RowID,
        key: &SelectKey,
        new_id: RowID,
        new_guard: &PageSharedGuard<RowPage>,
    ) -> Result<LinkForUniqueIndex> {
        debug_assert!(old_id != new_id);
        let (old_guard, old_id) = loop {
            match self
                .try_find_row(ctx_pool_guards(ctx), old_id, self.storage)
                .await
            {
                Ok(RowLocation::NotFound) => return Ok(LinkForUniqueIndex::NotNeeded),
                Ok(RowLocation::LwcBlock {
                    block_id,
                    row_idx,
                    row_shape_fingerprint,
                }) => {
                    return self
                        .link_for_unique_index_lwc(
                            ctx,
                            old_id,
                            key,
                            new_id,
                            new_guard,
                            block_id,
                            row_idx,
                            row_shape_fingerprint,
                        )
                        .await;
                }
                Ok(RowLocation::RowPage(page_id)) => {
                    // A hot duplicate candidate must be inspected through its
                    // row-page undo chain. It may be a stale latest mapping, a
                    // deleted owner that older snapshots still need, or a true
                    // duplicate visible to this transaction.
                    let Some(old_guard) = self
                        .try_get_validated_row_page_shared_result(
                            ctx_pool_guards(ctx),
                            page_id,
                            old_id,
                        )
                        .await?
                    else {
                        continue;
                    };
                    break (old_guard, old_id);
                }
                Err(err) => return Err(err),
            }
        };
        // Find a non-deleted old hot version that matches the unique key. If
        // this transaction cannot see that version, a runtime branch from the
        // new owner to the old owner's undo chain preserves it for older
        // snapshots. If this transaction can see it, the new claim is a real
        // duplicate.
        let metadata = self.metadata();
        let (page_ctx, page) = old_guard.ctx_and_page();
        let old_access = RowReadAccess::new(page, page_ctx, page.row_idx(old_id));
        match old_access.find_old_version_for_unique_key(metadata, key, ctx) {
            FindOldVersion::None => Ok(LinkForUniqueIndex::NotNeeded),
            FindOldVersion::DuplicateKey => Ok(LinkForUniqueIndex::DuplicateKey),
            FindOldVersion::WriteConflict => Ok(LinkForUniqueIndex::WriteConflict),
            FindOldVersion::Ok(old_row, cts, old_entry) => {
                // row latch is enough, because row lock is already acquired.
                let (page_ctx, page) = new_guard.ctx_and_page();
                let mut new_access = RowWriteAccess::new(
                    page,
                    page_ctx,
                    page.row_idx(new_id),
                    Some(ctx.sts()),
                    false,
                );
                let undo_vals = new_access.row().calc_delta(metadata, &old_row);
                new_access.link_for_unique_index(key.clone(), cts, old_entry, undo_vals);
                Ok(LinkForUniqueIndex::Linked)
            }
        }
    }

    #[inline]
    async fn insert_unique_index(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<InsertIndex> {
        let view = self.snapshot_secondary_view(ctx.sts(), root_snapshot, key.index_no)?;
        loop {
            match self
                .unique_insert_if_not_exists(
                    ctx_pool_guards(ctx),
                    key.index_no,
                    &key.vals,
                    row_id,
                    false,
                    view,
                )
                .await?
            {
                IndexInsert::Ok(merged) => {
                    // insert index success.
                    effects.push_insert_unique_index_undo(self.table_id(), row_id, key, merged);
                    return Ok(InsertIndex::Inserted);
                }
                IndexInsert::DuplicateKey(old_row_id, deleted) => {
                    // A unique key already has a latest mapping. A live
                    // non-deleted owner is a duplicate. A delete-masked or
                    // cold-marked owner may instead be a stale/old owner that
                    // should be linked for snapshots before this new claim.
                    debug_assert!(old_row_id != row_id);
                    if !deleted
                        && !self
                            .unmasked_duplicate_has_lwc_delete_marker(ctx, old_row_id)
                            .await?
                    {
                        return Ok(InsertIndex::DuplicateKey);
                    }
                    match self
                        .link_for_unique_index(ctx, old_row_id, &key, row_id, page_guard)
                        .await?
                    {
                        LinkForUniqueIndex::DuplicateKey => return Ok(InsertIndex::DuplicateKey),
                        LinkForUniqueIndex::WriteConflict => {
                            return Ok(InsertIndex::WriteConflict);
                        }
                        LinkForUniqueIndex::NotNeeded => {
                            // No old version matched the key. The index entry
                            // was stale or has already been purged, so claim
                            // the latest mapping if it still points to the
                            // same old row id.
                            let index_old_row_id = if deleted {
                                old_row_id.deleted()
                            } else {
                                old_row_id
                            };
                            match self
                                .unique_compare_exchange(
                                    ctx_pool_guards(ctx),
                                    key.index_no,
                                    &key.vals,
                                    index_old_row_id,
                                    row_id,
                                    view,
                                )
                                .await?
                            {
                                IndexCompareExchange::Ok => {
                                    // If we rollback this transaction, we need to undo the index update.
                                    effects.push_update_unique_index_undo(
                                        self.table_id(),
                                        old_row_id,
                                        row_id,
                                        key,
                                        deleted,
                                    );
                                    return Ok(InsertIndex::Inserted);
                                }
                                IndexCompareExchange::NotExists => {
                                    // There is race condition when GC thread delete the index entry concurrently.
                                    // So try to insert index entry again.
                                    continue;
                                }
                                IndexCompareExchange::Mismatch => {
                                    return Ok(InsertIndex::WriteConflict);
                                }
                            }
                        }
                        LinkForUniqueIndex::Linked => {
                            // Linking preserved the older visible owner but
                            // does not lock the logical key globally. Competing
                            // transactions can race here; compare_exchange is
                            // the serialization point for the new latest owner.
                            let index_old_row_id = if deleted {
                                old_row_id.deleted()
                            } else {
                                old_row_id
                            };
                            match self
                                .unique_compare_exchange(
                                    ctx_pool_guards(ctx),
                                    key.index_no,
                                    &key.vals,
                                    index_old_row_id,
                                    row_id,
                                    view,
                                )
                                .await?
                            {
                                IndexCompareExchange::Ok => {
                                    effects.push_update_unique_index_undo(
                                        self.table_id(),
                                        old_row_id,
                                        row_id,
                                        key,
                                        deleted,
                                    );
                                    return Ok(InsertIndex::Inserted);
                                }
                                IndexCompareExchange::NotExists => {
                                    // The purge thread may concurrently delete the index entry.
                                    // In this case, we need to retry the insertion of index.
                                    continue;
                                }
                                IndexCompareExchange::Mismatch => {
                                    return Ok(InsertIndex::WriteConflict);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[inline]
    async fn insert_non_unique_index(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        key: SelectKey,
        row_id: RowID,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<InsertIndex> {
        let view = self.snapshot_secondary_view(ctx.sts(), root_snapshot, key.index_no)?;
        // For non-unique index, it's guaranteed to be success.
        match self
            .non_unique_insert_if_not_exists(
                ctx_pool_guards(ctx),
                key.index_no,
                &key.vals,
                row_id,
                false,
                view,
            )
            .await?
        {
            IndexInsert::Ok(merged) => {
                // insert index success.
                effects.push_insert_non_unique_index_undo(self.table_id(), row_id, key, merged);
                Ok(InsertIndex::Inserted)
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }

    #[inline]
    async fn defer_delete_unique_index(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        row_id: RowID,
        key: SelectKey,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<()> {
        let view = self.snapshot_secondary_view(ctx.sts(), root_snapshot, key.index_no)?;
        // Foreground hot delete/update masks the latest unique mapping instead
        // of physically removing it. The row id is retained so older snapshots
        // and rollback can still recover the previous owner.
        let res = self
            .unique_mask_as_deleted(ctx_pool_guards(ctx), key.index_no, &key.vals, row_id, view)
            .await?;
        debug_assert!(res); // should always succeed.
        effects.push_delete_index_undo(self.table_id(), row_id, key, true);
        Ok(())
    }

    #[inline]
    async fn defer_delete_non_unique_index(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        row_id: RowID,
        key: SelectKey,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<()> {
        let view = self.snapshot_secondary_view(ctx.sts(), root_snapshot, key.index_no)?;
        // Non-unique entries are exact `(key, row_id)` claims, so masking this
        // pair shadows the old hot version while preserving rollback state.
        let res = self
            .non_unique_mask_as_deleted(ctx_pool_guards(ctx), key.index_no, &key.vals, row_id, view)
            .await?;
        debug_assert!(res);
        effects.push_delete_index_undo(self.table_id(), row_id, key, false);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    async fn update_unique_index_key_and_row_id_change(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        old_key: SelectKey,
        new_key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        new_guard: &PageSharedGuard<RowPage>,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<UpdateIndex> {
        debug_assert!(old_row_id != new_row_id);
        let view = self.snapshot_secondary_view(ctx.sts(), root_snapshot, new_key.index_no)?;
        loop {
            // Move update with a unique-key change. The new RowID cannot
            // already be in the index; duplicate handling below decides
            // whether an existing logical-key owner is visible, stale, or
            // should be linked for older snapshots.
            match self
                .unique_insert_if_not_exists(
                    ctx_pool_guards(ctx),
                    new_key.index_no,
                    &new_key.vals,
                    new_row_id,
                    false,
                    view,
                )
                .await?
            {
                IndexInsert::Ok(merged) => {
                    debug_assert!(!merged);
                    // New key insert succeed.
                    effects.push_insert_unique_index_undo(
                        self.table_id(),
                        new_row_id,
                        new_key,
                        false,
                    );
                    // mark index of old row as deleted and defer delete.
                    self.defer_delete_unique_index(
                        ctx,
                        effects,
                        old_row_id,
                        old_key,
                        root_snapshot,
                    )
                    .await?;
                    return Ok(UpdateIndex::Updated);
                }
                IndexInsert::DuplicateKey(index_row_id, deleted) => {
                    // The new row id is the insert id, so a duplicate points
                    // to another latest or delete-masked owner.
                    debug_assert!(index_row_id != new_row_id);
                    if !deleted
                        && !self
                            .unmasked_duplicate_has_lwc_delete_marker(ctx, index_row_id)
                            .await?
                    {
                        return Ok(UpdateIndex::DuplicateKey);
                    }
                    // todo: change the logic.
                    // If we treat move-update just as delete and insert,
                    // with an extra linking step. then, we don't need to
                    // care about if index_row_id equal to old_row_id.
                    if deleted && index_row_id == old_row_id {
                        // This is possible.
                        // For example, transaction update row(RowID=100) key=1 to key=2.
                        //
                        // Then index has following entries:
                        // key=1 -> RowID=100 (old version)
                        // key=2 -> RowID=100 (latest version)
                        //
                        // Then we update key=2 to key=1 again.
                        // And page does not have enough space, so move+update with RowID=200.
                        // Now we should have:
                        // key=1 -> RowID=200 (latest version)
                        // key=2 -> RowID=100 (old version)
                        //
                        // In this case, we can just update index to point to new version.
                        //
                        // There can be an optimization to combine the update into insert.
                        // e.g. add a new method BTree::insert_if_not_exists_or_merge_match_value().
                        // But I think the case is rare so keep as is.
                        match self
                            .unique_compare_exchange(
                                ctx_pool_guards(ctx),
                                new_key.index_no,
                                &new_key.vals,
                                old_row_id.deleted(),
                                new_row_id,
                                view,
                            )
                            .await?
                        {
                            IndexCompareExchange::Ok => {
                                // New key update succeed.
                                effects.push_update_unique_index_undo(
                                    self.table_id(),
                                    old_row_id,
                                    new_row_id,
                                    new_key,
                                    deleted,
                                );
                                // mark index of old row as deleted and defer delete.
                                self.defer_delete_unique_index(
                                    ctx,
                                    effects,
                                    old_row_id,
                                    old_key,
                                    root_snapshot,
                                )
                                .await?;
                                return Ok(UpdateIndex::Updated);
                            }
                            IndexCompareExchange::Mismatch => {
                                unreachable!();
                            }
                            IndexCompareExchange::NotExists => {
                                // re-insert index entry.
                                continue;
                            }
                        }
                    }
                    // A conflicting key points to another row. Inspect that
                    // hot/cold owner before deciding whether this is a true
                    // duplicate, a write conflict, or a linkable old owner.
                    match self
                        .link_for_unique_index(ctx, index_row_id, &new_key, new_row_id, new_guard)
                        .await?
                    {
                        LinkForUniqueIndex::DuplicateKey => return Ok(UpdateIndex::DuplicateKey),
                        LinkForUniqueIndex::WriteConflict => {
                            return Ok(UpdateIndex::WriteConflict);
                        }
                        LinkForUniqueIndex::NotNeeded => {
                            // No visible old version matched the key, so the
                            // existing index entry is stale and can be claimed
                            // if it has not changed concurrently.
                            let index_old_row_id = if deleted {
                                index_row_id.deleted()
                            } else {
                                index_row_id
                            };
                            match self
                                .unique_compare_exchange(
                                    ctx_pool_guards(ctx),
                                    new_key.index_no,
                                    &new_key.vals,
                                    index_old_row_id,
                                    new_row_id,
                                    view,
                                )
                                .await?
                            {
                                IndexCompareExchange::Ok => {
                                    // New key update succeed.
                                    effects.push_update_unique_index_undo(
                                        self.table_id(),
                                        index_row_id,
                                        new_row_id,
                                        new_key,
                                        deleted,
                                    );
                                    self.defer_delete_unique_index(
                                        ctx,
                                        effects,
                                        old_row_id,
                                        old_key,
                                        root_snapshot,
                                    )
                                    .await?;
                                    return Ok(UpdateIndex::Updated);
                                }
                                IndexCompareExchange::Mismatch => {
                                    // This may happen when another transaction insert/update with same key.
                                    return Ok(UpdateIndex::WriteConflict);
                                }
                                IndexCompareExchange::NotExists => {
                                    // Purge thread may delete the index entry before we update,
                                    // we should re-insert.
                                    continue;
                                }
                            }
                        }
                        LinkForUniqueIndex::Linked => {
                            // The older owner was preserved through a runtime
                            // branch. The compare_exchange publishes the new
                            // latest owner while ensuring the entry still
                            // points at the owner we inspected.
                            let index_old_row_id = if deleted {
                                index_row_id.deleted()
                            } else {
                                index_row_id
                            };
                            match self
                                .unique_compare_exchange(
                                    ctx_pool_guards(ctx),
                                    new_key.index_no,
                                    &new_key.vals,
                                    index_old_row_id,
                                    new_row_id,
                                    view,
                                )
                                .await?
                            {
                                IndexCompareExchange::Ok => {
                                    // New key update succeeds.
                                    effects.push_update_unique_index_undo(
                                        self.table_id(),
                                        index_row_id,
                                        new_row_id,
                                        new_key,
                                        deleted,
                                    );
                                    self.defer_delete_unique_index(
                                        ctx,
                                        effects,
                                        old_row_id,
                                        old_key,
                                        root_snapshot,
                                    )
                                    .await?;
                                    return Ok(UpdateIndex::Updated);
                                }
                                IndexCompareExchange::Mismatch
                                | IndexCompareExchange::NotExists => {
                                    unreachable!()
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    async fn update_non_unique_index_key_and_row_id_change(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        old_key: SelectKey,
        new_key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<UpdateIndex> {
        debug_assert!(old_row_id != new_row_id);
        let view = self.snapshot_secondary_view(ctx.sts(), root_snapshot, new_key.index_no)?;
        // Non-unique indexes store exact `(key, row_id)` entries, so a move
        // update inserts the new exact entry and masks the old one.
        match self
            .non_unique_insert_if_not_exists(
                ctx_pool_guards(ctx),
                new_key.index_no,
                &new_key.vals,
                new_row_id,
                false,
                view,
            )
            .await?
        {
            IndexInsert::Ok(merged) => {
                debug_assert!(!merged);
                // New key insert succeed.
                effects.push_insert_non_unique_index_undo(
                    self.table_id(),
                    new_row_id,
                    new_key,
                    false,
                );
                // mark index of old row as deleted and defer delete.
                self.defer_delete_non_unique_index(
                    ctx,
                    effects,
                    old_row_id,
                    old_key,
                    root_snapshot,
                )
                .await?;
                Ok(UpdateIndex::Updated)
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }

    #[inline]
    async fn update_unique_index_only_row_id_change(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<UpdateIndex> {
        debug_assert!(old_row_id != new_row_id);
        let view = self.snapshot_secondary_view(ctx.sts(), root_snapshot, key.index_no)?;
        // Move update where the unique key is unchanged. The logical key keeps
        // one latest mapping, so atomically replace the old RowID with the new
        // hot RowID and record undo to restore it on rollback.
        match self
            .unique_compare_exchange(
                ctx_pool_guards(ctx),
                key.index_no,
                &key.vals,
                old_row_id,
                new_row_id,
                view,
            )
            .await?
        {
            IndexCompareExchange::Ok => {
                effects.push_update_unique_index_undo(
                    self.table_id(),
                    old_row_id,
                    new_row_id,
                    key,
                    false,
                );
                Ok(UpdateIndex::Updated)
            }
            IndexCompareExchange::Mismatch | IndexCompareExchange::NotExists => {
                unreachable!()
            }
        }
    }

    #[inline]
    async fn update_non_unique_index_only_row_id_change(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<UpdateIndex> {
        debug_assert!(old_row_id != new_row_id);
        let view = self.snapshot_secondary_view(ctx.sts(), root_snapshot, key.index_no)?;
        // Non-unique key unchanged but RowID changed: publish the replacement
        // exact entry, then mask the old exact entry for rollback/GC.
        let res = self
            .non_unique_insert_if_not_exists(
                ctx_pool_guards(ctx),
                key.index_no,
                &key.vals,
                new_row_id,
                false,
                view,
            )
            .await?;
        debug_assert!(res.is_ok());
        effects.push_insert_non_unique_index_undo(self.table_id(), new_row_id, key.clone(), false);
        // defer delete old entry.
        self.defer_delete_non_unique_index(ctx, effects, old_row_id, key, root_snapshot)
            .await?;
        Ok(UpdateIndex::Updated)
    }

    /// Update unique index due to key change.
    /// In this scenario, we only need to insert pair of new key and row id
    /// into index. Keep old index entry as is.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    async fn update_unique_index_only_key_change(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        old_key: SelectKey,
        new_key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<UpdateIndex> {
        let view = self.snapshot_secondary_view(ctx.sts(), root_snapshot, new_key.index_no)?;
        loop {
            // In-place unique-key change keeps the same RowID. Repeated key
            // changes in the same transaction can encounter this RowID already
            // delete-masked under the new key, so insert may merge by flipping
            // the delete flag back to active.
            //
            // This is case for one transaction or multiple transactions to update
            // key of the same row back and forth.
            // e.g. update k=1 to k=2, then update k=2 to k=1, ...
            //
            // Each update will mask old index entry as deleted, and try to insert a new
            // entry, with same row id(Because it's the same row).
            // And all old versions are also linked from the same row.
            // That mean we can just merge the new index entry into the deleted entry(flip
            // the delete flag) if key and row id all match.
            // So we set merge_if_match_deleted to true.
            match self
                .unique_insert_if_not_exists(
                    ctx_pool_guards(ctx),
                    new_key.index_no,
                    &new_key.vals,
                    row_id,
                    true,
                    view,
                )
                .await?
            {
                IndexInsert::Ok(merged) => {
                    // Insert new key success.
                    effects.push_insert_unique_index_undo(self.table_id(), row_id, new_key, merged);
                    // Defer delete old key.
                    self.defer_delete_unique_index(ctx, effects, row_id, old_key, root_snapshot)
                        .await?;
                    return Ok(UpdateIndex::Updated);
                }
                IndexInsert::DuplicateKey(index_row_id, deleted) => {
                    // Another owner is mapped to the new key. Inspect it
                    // before deciding whether this is a duplicate, a stale
                    // mapping, or an old owner to preserve through a runtime
                    // unique branch.
                    if !deleted
                        && !self
                            .unmasked_duplicate_has_lwc_delete_marker(ctx, index_row_id)
                            .await?
                    {
                        return Ok(UpdateIndex::DuplicateKey);
                    }
                    match self
                        .link_for_unique_index(ctx, index_row_id, &new_key, row_id, page_guard)
                        .await?
                    {
                        LinkForUniqueIndex::DuplicateKey => return Ok(UpdateIndex::DuplicateKey),
                        LinkForUniqueIndex::WriteConflict => {
                            return Ok(UpdateIndex::WriteConflict);
                        }
                        LinkForUniqueIndex::NotNeeded => {
                            // no old row found.
                            let index_old_row_id = if deleted {
                                index_row_id.deleted()
                            } else {
                                index_row_id
                            };
                            match self
                                .unique_compare_exchange(
                                    ctx_pool_guards(ctx),
                                    new_key.index_no,
                                    &new_key.vals,
                                    index_old_row_id,
                                    row_id,
                                    view,
                                )
                                .await?
                            {
                                IndexCompareExchange::Ok => {
                                    // Update new key succeeds.
                                    effects.push_update_unique_index_undo(
                                        self.table_id(),
                                        index_row_id,
                                        row_id,
                                        new_key,
                                        deleted,
                                    );
                                    self.defer_delete_unique_index(
                                        ctx,
                                        effects,
                                        row_id,
                                        old_key,
                                        root_snapshot,
                                    )
                                    .await?;
                                    return Ok(UpdateIndex::Updated);
                                }
                                IndexCompareExchange::Mismatch => {
                                    return Ok(UpdateIndex::WriteConflict);
                                }
                                IndexCompareExchange::NotExists => {
                                    // re-insert
                                    continue;
                                }
                            }
                        }
                        LinkForUniqueIndex::Linked => {
                            // Both old row(index points to) and new row are locked.
                            // we must succeed on updating index.
                            let index_old_row_id = if deleted {
                                index_row_id.deleted()
                            } else {
                                index_row_id
                            };
                            match self
                                .unique_compare_exchange(
                                    ctx_pool_guards(ctx),
                                    new_key.index_no,
                                    &new_key.vals,
                                    index_old_row_id,
                                    row_id,
                                    view,
                                )
                                .await?
                            {
                                IndexCompareExchange::Ok => {
                                    // New key update succeeds.
                                    effects.push_update_unique_index_undo(
                                        self.table_id(),
                                        index_row_id,
                                        row_id,
                                        new_key,
                                        deleted,
                                    );
                                    self.defer_delete_unique_index(
                                        ctx,
                                        effects,
                                        row_id,
                                        old_key,
                                        root_snapshot,
                                    )
                                    .await?;
                                    return Ok(UpdateIndex::Updated);
                                }
                                IndexCompareExchange::Mismatch => {
                                    return Ok(UpdateIndex::WriteConflict);
                                }
                                IndexCompareExchange::NotExists => {
                                    // re-insert
                                    continue;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[inline]
    async fn update_non_unique_index_only_key_change(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        old_key: SelectKey,
        new_key: SelectKey,
        row_id: RowID,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> Result<UpdateIndex> {
        let view = self.snapshot_secondary_view(ctx.sts(), root_snapshot, new_key.index_no)?;
        // This is case for one transaction or multiple transactions to update
        // key of the same row back and forth.
        // e.g. update k=1 to k=2, then update k=2 to k=1, ...
        //
        // Each update will mask old index entry as deleted, and try to insert a new
        // entry, with same row id(Because it's the same row).
        // And all old versions are also linked from the same row.
        // That mean we can just merge the new index entry into the deleted entry(flip
        // the delete flag) if key and row id all match.
        // So we set merge_if_match_deleted to true.
        match self
            .non_unique_insert_if_not_exists(
                ctx_pool_guards(ctx),
                new_key.index_no,
                &new_key.vals,
                row_id,
                true,
                view,
            )
            .await?
        {
            IndexInsert::Ok(merged) => {
                effects.push_insert_non_unique_index_undo(self.table_id(), row_id, new_key, merged);
                // Defer delete old key.
                self.defer_delete_non_unique_index(ctx, effects, row_id, old_key, root_snapshot)
                    .await?;
                Ok(UpdateIndex::Updated)
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }
}

/// Runtime accessor type binding for user tables:
/// - `D = EvictableBufferPool` for row pages
/// - `I = EvictableBufferPool` for secondary indexes
pub type HybridTableAccessor<'a> = TableAccessor<'a, EvictableBufferPool, EvictableBufferPool>;

/// Runtime accessor type binding for in-memory catalog tables:
/// - `D = FixedBufferPool` for row pages
/// - `I = FixedBufferPool` for secondary indexes
pub type MemTableAccessor<'a> = TableAccessor<'a, FixedBufferPool, FixedBufferPool>;

impl TableAccessor<'_, FixedBufferPool, FixedBufferPool> {
    #[inline]
    pub(crate) async fn insert_catalog_no_trx(
        &self,
        guards: &PoolGuards,
        cols: &[Val],
    ) -> Result<()> {
        self.insert_no_trx_inner(guards, cols).await
    }

    #[inline]
    pub(crate) async fn delete_catalog_unique_no_trx(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
    ) -> Result<()> {
        self.delete_unique_no_trx_inner(guards, key).await
    }
}

impl<D: BufferPool, I: BufferPool> TableAccess for TableAccessor<'_, D, I> {
    async fn table_scan_uncommitted<F>(&self, guards: &PoolGuards, mut row_action: F)
    where
        F: for<'m, 'p> FnMut(&'m TableMetadata, Row<'p>) -> bool,
    {
        self.mem_scan(guards, |page_guard| {
            let (ctx, page) = page_guard.ctx_and_page();
            let metadata = &*ctx.row_ver().unwrap().metadata;
            for row_access in ReadAllRows::new(page, ctx) {
                if !row_action(metadata, row_access.row()) {
                    return false;
                }
            }
            true
        })
        .await;
    }

    async fn table_scan_mvcc<F>(&self, ctx: &TrxContext, read_set: &[usize], mut row_action: F)
    where
        F: FnMut(Vec<Val>) -> bool,
    {
        self.mem_scan(ctx_pool_guards(ctx), |page_guard| {
            let (page_ctx, page) = page_guard.ctx_and_page();
            let metadata = &*page_ctx.row_ver().unwrap().metadata;
            for row_access in ReadAllRows::new(page, page_ctx) {
                match row_access.read_row_mvcc(ctx, metadata, read_set, None) {
                    ReadRow::InvalidIndex => unreachable!(),
                    ReadRow::NotFound => (),
                    ReadRow::Ok(vals) => {
                        if !row_action(vals) {
                            return false;
                        }
                    }
                }
            }
            true
        })
        .await;
    }

    async fn index_lookup_unique_mvcc(
        &self,
        ctx: &TrxContext,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> Result<SelectMvcc> {
        debug_assert!(key.index_no < self.sec_idx_len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        debug_assert!({
            !user_read_set.is_empty()
                && user_read_set
                    .iter()
                    .zip(user_read_set.iter().skip(1))
                    .all(|(l, r)| l < r)
        });
        let view = self.bound_secondary_view(ctx, key.index_no)?;
        match self
            .unique_lookup(ctx_pool_guards(ctx), key.index_no, &key.vals, view)
            .await?
        {
            None => Ok(SelectMvcc::NotFound),
            Some((row_id, _)) => {
                self.index_lookup_unique_row_mvcc(ctx, key, user_read_set, row_id)
                    .await
            }
        }
    }

    async fn index_lookup_unique_uncommitted<R, F>(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_action: F,
    ) -> Result<Option<R>>
    where
        for<'m, 'p> F: FnOnce(&'m TableMetadata, Row<'p>) -> R,
    {
        debug_assert!(key.index_no < self.sec_idx_len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        let view = self.unchecked_secondary_view(MIN_SNAPSHOT_TS, key.index_no)?;
        let (page_guard, row_id) = match self
            .unique_lookup(guards, key.index_no, &key.vals, view)
            .await?
        {
            None => return Ok(None),
            Some((row_id, _)) => match self.find_row(guards, row_id, self.storage).await {
                RowLocation::NotFound => return Ok(None),
                RowLocation::LwcBlock { .. } => todo!("lwc block"),
                RowLocation::RowPage(page_id) => {
                    let page_guard = self.must_get_row_page_shared(guards, page_id).await?;
                    (page_guard, row_id)
                }
            },
        };
        let (ctx, page) = page_guard.ctx_and_page();
        if !page.row_id_in_valid_range(row_id) {
            return Ok(None);
        }
        let metadata = &*ctx.row_ver().unwrap().metadata;
        let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
        let row = access.row();
        // latest version in row page.
        if row.is_deleted() {
            return Ok(None);
        }
        if row.is_key_different(self.metadata(), key) {
            return Ok(None);
        }
        Ok(Some(row_action(metadata, row)))
    }

    async fn index_scan_mvcc(
        &self,
        ctx: &TrxContext,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> Result<ScanMvcc> {
        debug_assert!(key.index_no < self.sec_idx_len());
        // Index scan should be applied to non-unique index.
        // todo: support partial key scan on unique index.
        debug_assert!(!self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        debug_assert!({
            !user_read_set.is_empty()
                && user_read_set
                    .iter()
                    .zip(user_read_set.iter().skip(1))
                    .all(|(l, r)| l < r)
        });
        // todo: support batching, streaming and sorting.
        let mut row_ids = vec![];
        let view = self.bound_secondary_view(ctx, key.index_no)?;
        self.non_unique_lookup(
            ctx_pool_guards(ctx),
            key.index_no,
            &key.vals,
            &mut row_ids,
            view,
        )
        .await?;
        let mut res = vec![];
        for row_id in row_ids {
            match self
                .index_lookup_unique_row_mvcc(ctx, key, user_read_set, row_id)
                .await?
            {
                SelectMvcc::NotFound => (),
                SelectMvcc::Found(vals) => {
                    res.push(vals);
                }
            }
        }
        Ok(ScanMvcc::Rows(res))
    }

    async fn insert_mvcc(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        cols: Vec<Val>,
    ) -> Result<RowID> {
        let metadata = self.metadata();
        debug_assert!(cols.len() == metadata.col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| self.metadata().col_type_match(idx, val))
        });
        let keys = self.metadata().keys_for_insert(&cols);
        let root_snapshot = self.root_snapshot(ctx)?;
        // Insert always creates a hot RowStore row. The insert undo head makes
        // the new row invisible to older snapshots and is also the rollback
        // handle if any following index insert fails.
        let (row_id, page_guard) = self
            .insert_row_internal(ctx, effects, cols, RowUndoKind::Insert, Vec::new())
            .await?;
        // Secondary-index claims are made after the row exists so unique
        // duplicate handling can link this new hot row to older owners if
        // needed for MVCC visibility.
        for key in keys {
            match self
                .insert_index(
                    ctx,
                    effects,
                    key,
                    row_id,
                    &page_guard,
                    root_snapshot.as_ref(),
                )
                .await?
            {
                InsertIndex::Inserted => (),
                InsertIndex::DuplicateKey => {
                    return Err(Report::new(OperationError::DuplicateKey)
                        .attach("insert MVCC secondary index claim")
                        .into());
                }
                InsertIndex::WriteConflict => {
                    return Err(Report::new(OperationError::WriteConflict)
                        .attach("insert MVCC secondary index claim")
                        .into());
                }
            }
        }
        page_guard.set_dirty(); // mark as dirty page.
        Ok(row_id)
    }

    async fn update_unique_mvcc(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> Result<UpdateMvcc> {
        debug_assert!(key.index_no < self.sec_idx_len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        debug_assert!(update.iter().all(|uc| {
            uc.idx < self.metadata().col_count() && self.metadata().col_type_match(uc.idx, &uc.val)
        }));
        debug_assert!(
            update.is_empty()
                || update
                    .iter()
                    .zip(update.iter().skip(1))
                    .all(|(l, r)| l.idx < r.idx)
        );
        loop {
            let root_snapshot = self.root_snapshot(ctx)?;
            let lookup_view =
                self.snapshot_secondary_view(ctx.sts(), root_snapshot.as_ref(), key.index_no)?;
            let (page_guard, row_id) = match self
                .unique_lookup(ctx_pool_guards(ctx), key.index_no, &key.vals, lookup_view)
                .await?
            {
                None => return Ok(UpdateMvcc::NotFound),
                Some((row_id, _)) => match self
                    .try_find_row(ctx_pool_guards(ctx), row_id, self.storage)
                    .await
                {
                    Ok(RowLocation::NotFound) => return Ok(UpdateMvcc::NotFound),
                    Ok(RowLocation::LwcBlock {
                        block_id,
                        row_idx,
                        row_shape_fingerprint,
                    }) => {
                        // LWC rows are immutable. A cold update is represented
                        // as an owned CDB delete marker for the old row plus a
                        // new hot RowStore row containing the updated values.
                        // read_lwc_row_for_update() checks snapshot visibility
                        // before decoding and key revalidation.
                        let old_vals = match self
                            .read_lwc_row_for_update(
                                ctx,
                                key,
                                row_id,
                                block_id,
                                row_idx,
                                row_shape_fingerprint,
                            )
                            .await?
                        {
                            ColdRowUpdateRead::Ok(vals) => vals,
                            ColdRowUpdateRead::NotFound => return Ok(UpdateMvcc::NotFound),
                            ColdRowUpdateRead::WriteConflict => {
                                return Err(Report::new(OperationError::WriteConflict)
                                    .attach("update MVCC cold row read")
                                    .into());
                            }
                        };
                        let deletion_buffer = self.lwc_deletion_buffer()?;
                        // The read above is only validation. This put_ref() is
                        // the definitive ownership claim and rechecks the CDB
                        // state under the map entry to catch races with other
                        // cold delete/update transactions.
                        match deletion_buffer.put_ref(row_id, ctx.status(), ctx.sts()) {
                            Ok(()) => (),
                            Err(DeletionError::WriteConflict) => {
                                return Err(Report::new(OperationError::WriteConflict)
                                    .attach("update MVCC cold delete marker ownership")
                                    .into());
                            }
                            Err(DeletionError::AlreadyDeleted) => {
                                return Ok(UpdateMvcc::NotFound);
                            }
                        }
                        // Cold delete undo has no row page. Rollback routes
                        // page_id=None to CDB marker removal; redo uses
                        // INVALID_PAGE_ID so recovery replays the delete into
                        // the deletion buffer when it is newer than the
                        // deletion checkpoint cutoff.
                        effects.push_row_undo(OwnedRowUndo::new(
                            self.table_id(),
                            None,
                            row_id,
                            RowUndoKind::Delete,
                        ));
                        effects.insert_row_redo(
                            self.table_id(),
                            RowRedo {
                                page_id: INVALID_PAGE_ID,
                                row_id,
                                kind: RowRedoKind::Delete,
                            },
                        );

                        // Match row-page update/delete behavior: mask the old
                        // cold index entries now and let index undo restore
                        // them on rollback or GC remove them after commit.
                        // Unique indexes may also install runtime branches from
                        // the hot replacement to the old cold owner while the
                        // new row's index entries are inserted below.
                        let old_index_keys = self.metadata().keys_for_insert(&old_vals);
                        self.defer_delete_index_keys(
                            ctx,
                            effects,
                            row_id,
                            old_index_keys,
                            root_snapshot.as_ref(),
                        )
                        .await?;

                        let new_row = self.build_cold_update_row(old_vals, update.clone());
                        let new_index_keys = self.metadata().keys_for_insert(&new_row);
                        let (new_row_id, new_guard) = self
                            .insert_row_internal(
                                ctx,
                                effects,
                                new_row,
                                RowUndoKind::Insert,
                                Vec::new(),
                            )
                            .await?;
                        for key in new_index_keys {
                            match self
                                .insert_index(
                                    ctx,
                                    effects,
                                    key,
                                    new_row_id,
                                    &new_guard,
                                    root_snapshot.as_ref(),
                                )
                                .await?
                            {
                                InsertIndex::Inserted => (),
                                InsertIndex::DuplicateKey => {
                                    return Err(Report::new(OperationError::DuplicateKey)
                                        .attach("update MVCC cold replacement index claim")
                                        .into());
                                }
                                InsertIndex::WriteConflict => {
                                    return Err(Report::new(OperationError::WriteConflict)
                                        .attach("update MVCC cold replacement index claim")
                                        .into());
                                }
                            }
                        }
                        new_guard.set_dirty();
                        return Ok(UpdateMvcc::Updated(new_row_id));
                    }
                    Ok(RowLocation::RowPage(page_id)) => {
                        // Hot-row update proceeds through row-page locking and
                        // undo-chain visibility. A stale index location is
                        // retried or rejected before any row mutation.
                        let Some(page_guard) = self
                            .try_get_validated_row_page_shared_result(
                                ctx_pool_guards(ctx),
                                page_id,
                                row_id,
                            )
                            .await?
                        else {
                            continue;
                        };
                        (page_guard, row_id)
                    }
                    Err(err) => return Err(err),
                },
            };
            let res = self
                .update_row_inplace(ctx, effects, page_guard, key, row_id, update.clone())
                .await;
            match res {
                UpdateRowInplace::Ok(new_row_id, index_change_cols, page_guard) => {
                    debug_assert!(row_id == new_row_id);
                    if !index_change_cols.is_empty() {
                        // RowID is unchanged, but logical keys may have moved.
                        // Update MemIndex after the page mutation so rollback
                        // can restore both row data and index visibility.
                        let res = self
                            .update_indexes_only_key_change(
                                ctx,
                                effects,
                                row_id,
                                &page_guard,
                                &index_change_cols,
                                root_snapshot.as_ref(),
                            )
                            .await?;
                        page_guard.set_dirty(); // mark as dirty page.
                        return match res {
                            UpdateIndex::Updated => Ok(UpdateMvcc::Updated(new_row_id)),
                            UpdateIndex::DuplicateKey => {
                                Err(Report::new(OperationError::DuplicateKey)
                                    .attach("update MVCC key-change index update")
                                    .into())
                            }
                            UpdateIndex::WriteConflict => {
                                Err(Report::new(OperationError::WriteConflict)
                                    .attach("update MVCC key-change index update")
                                    .into())
                            }
                        };
                    } // otherwise, do nothing
                    page_guard.set_dirty(); // mark as dirty page.
                    return Ok(UpdateMvcc::Updated(row_id));
                }
                UpdateRowInplace::RowDeleted | UpdateRowInplace::RowNotFound => {
                    return Ok(UpdateMvcc::NotFound);
                }
                UpdateRowInplace::WriteConflict => {
                    return Err(Report::new(OperationError::WriteConflict)
                        .attach("update MVCC row-page write lock")
                        .into());
                }
                UpdateRowInplace::RetryInTransition => {
                    smol::Timer::after(std::time::Duration::from_millis(1)).await;
                    continue;
                }
                UpdateRowInplace::NoFreeSpace(old_row_id, old_row, update, old_guard) => {
                    // In-place update failed after the old row was locked and
                    // marked deleted. Finish the move update by inserting the
                    // replacement row and then update indexes for any RowID or
                    // key movement.
                    let (new_row_id, index_change_cols, new_guard) = self
                        .move_update_for_space(ctx, effects, old_row, update, old_row_id, old_guard)
                        .await?;
                    if !index_change_cols.is_empty() {
                        let res = self
                            .update_indexes_may_both_change(
                                ctx,
                                effects,
                                RowIdMove::new(old_row_id, new_row_id),
                                &index_change_cols,
                                &new_guard,
                                root_snapshot.as_ref(),
                            )
                            .await?;
                        // old guard is already marked inside.
                        new_guard.set_dirty(); // mark as dirty page.
                        return match res {
                            UpdateIndex::Updated => Ok(UpdateMvcc::Updated(new_row_id)),
                            UpdateIndex::DuplicateKey => {
                                Err(Report::new(OperationError::DuplicateKey)
                                    .attach("update MVCC moved-row index update")
                                    .into())
                            }
                            UpdateIndex::WriteConflict => {
                                Err(Report::new(OperationError::WriteConflict)
                                    .attach("update MVCC moved-row index update")
                                    .into())
                            }
                        };
                    } else {
                        let res = self
                            .update_indexes_only_row_id_change(
                                ctx,
                                effects,
                                old_row_id,
                                new_row_id,
                                &new_guard,
                                root_snapshot.as_ref(),
                            )
                            .await?;
                        new_guard.set_dirty(); // mark as dirty page.
                        return match res {
                            UpdateIndex::Updated => Ok(UpdateMvcc::Updated(new_row_id)),
                            UpdateIndex::DuplicateKey => {
                                Err(Report::new(OperationError::DuplicateKey)
                                    .attach("update MVCC moved-row index update")
                                    .into())
                            }
                            UpdateIndex::WriteConflict => {
                                Err(Report::new(OperationError::WriteConflict)
                                    .attach("update MVCC moved-row index update")
                                    .into())
                            }
                        };
                    }
                }
            }
        }
    }

    async fn delete_unique_mvcc(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        key: &SelectKey,
        log_by_key: bool,
    ) -> Result<DeleteMvcc> {
        debug_assert!(key.index_no < self.sec_idx_len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        loop {
            let root_snapshot = self.root_snapshot(ctx)?;
            let lookup_view =
                self.snapshot_secondary_view(ctx.sts(), root_snapshot.as_ref(), key.index_no)?;
            let (page_guard, row_id) = match self
                .unique_lookup(ctx_pool_guards(ctx), key.index_no, &key.vals, lookup_view)
                .await?
            {
                None => return Ok(DeleteMvcc::NotFound),
                Some((row_id, _)) => match self
                    .try_find_row(ctx_pool_guards(ctx), row_id, self.storage)
                    .await
                {
                    Ok(RowLocation::NotFound) => return Ok(DeleteMvcc::NotFound),
                    Ok(RowLocation::LwcBlock {
                        block_id,
                        row_idx,
                        row_shape_fingerprint,
                    }) => {
                        // Delete only needs old secondary-index keys, so read
                        // indexed columns instead of decoding the whole row.
                        // The key recheck prevents acting on stale DiskTree or
                        // MemIndex state after another path already moved the
                        // logical key away from this cold row.
                        let index_keys = self
                            .read_lwc_index_keys(
                                ctx_pool_guards(ctx),
                                block_id,
                                row_idx,
                                row_shape_fingerprint,
                            )
                            .await?;
                        if !Self::index_key_matches(&index_keys, key)? {
                            return Ok(DeleteMvcc::NotFound);
                        }
                        let deletion_buffer = self.lwc_deletion_buffer()?;
                        match deletion_buffer.put_ref(row_id, ctx.status(), ctx.sts()) {
                            Ok(()) => {
                                // The marker is statement-owned delete state
                                // until success. Row undo removes it on
                                // rollback; redo rebuilds it as a cold delete
                                // during recovery.
                                let undo = OwnedRowUndo::new(
                                    self.table_id(),
                                    None,
                                    row_id,
                                    RowUndoKind::Delete,
                                );
                                effects.push_row_undo(undo);
                                let redo_kind = if log_by_key {
                                    RowRedoKind::DeleteByUniqueKey(key.clone())
                                } else {
                                    RowRedoKind::Delete
                                };
                                let redo = RowRedo {
                                    page_id: INVALID_PAGE_ID,
                                    row_id,
                                    kind: redo_kind,
                                };
                                effects.insert_row_redo(self.table_id(), redo);
                                // Mask old index entries immediately; physical
                                // deletion remains deferred to index GC.
                                self.defer_delete_index_keys(
                                    ctx,
                                    effects,
                                    row_id,
                                    index_keys,
                                    root_snapshot.as_ref(),
                                )
                                .await?;
                                return Ok(DeleteMvcc::Deleted);
                            }
                            Err(DeletionError::WriteConflict) => {
                                return Ok(DeleteMvcc::WriteConflict);
                            }
                            Err(DeletionError::AlreadyDeleted) => {
                                return Ok(DeleteMvcc::NotFound);
                            }
                        }
                    }
                    Ok(RowLocation::RowPage(page_id)) => {
                        // Hot delete is an in-page delete bit guarded by row
                        // undo. Index entries are masked after the row mutation
                        // and restored by index undo on rollback.
                        let Some(page_guard) = self
                            .try_get_validated_row_page_shared_result(
                                ctx_pool_guards(ctx),
                                page_id,
                                row_id,
                            )
                            .await?
                        else {
                            continue;
                        };
                        (page_guard, row_id)
                    }
                    Err(err) => return Err(err),
                },
            };
            match self
                .delete_row_internal(ctx, effects, page_guard, row_id, key, log_by_key)
                .await
            {
                DeleteInternal::NotFound => return Ok(DeleteMvcc::NotFound),
                DeleteInternal::WriteConflict => return Ok(DeleteMvcc::WriteConflict),
                DeleteInternal::RetryInTransition => {
                    smol::Timer::after(std::time::Duration::from_millis(1)).await;
                    continue;
                }
                DeleteInternal::Ok(page_guard) => {
                    // Mask every secondary-index entry for this hot row. The
                    // physical index entry remains until rollback unmasks it
                    // or index GC removes it after it is no longer visible.
                    self.defer_delete_indexes(
                        ctx,
                        effects,
                        row_id,
                        &page_guard,
                        root_snapshot.as_ref(),
                    )
                    .await?;
                    page_guard.set_dirty(); // mark as dirty.
                    return Ok(DeleteMvcc::Deleted);
                }
            }
        }
    }

    async fn delete_index(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        unique: bool,
        min_active_sts: TrxID,
    ) -> Result<bool> {
        // todo: consider index drop.
        let index_schema = &self.metadata().index_specs[key.index_no];
        debug_assert_eq!(unique, index_schema.unique());
        if unique {
            self.delete_unique_index(guards, key, row_id, min_active_sts)
                .await
        } else {
            self.delete_non_unique_index(guards, key, row_id, min_active_sts)
                .await
        }
    }
}

impl<D: BufferPool, I: BufferPool> Deref for TableAccessor<'_, D, I> {
    type Target = GenericMemTable<D, I>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.mem
    }
}
