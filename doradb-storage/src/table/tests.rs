use super::{DeleteInternal, FrozenPage, InsertRowIntoPage, UpdateRowInplace};
use crate::buffer::BufferPool;
use crate::buffer::frame::FrameKind;
use crate::buffer::guard::PageGuard;
use crate::buffer::page::PAGE_SIZE;
use crate::buffer::{
    EvictableBufferPool, PoolGuard, PoolGuards, PoolRole, ReadonlyBufferPool, test_frame_kind,
};
use crate::catalog::table::test_hooks as catalog_table_tests;
use crate::catalog::table::test_hooks::CreateTableTestFailure;
use crate::catalog::tests::table4;
use crate::catalog::{
    CatalogCheckpointScanStopReason, ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey,
    IndexNo, IndexSpec, TableMetadata, TableSpec, USER_OBJ_ID_START,
};
use crate::conf::{EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig};
use crate::engine::Engine;
use crate::error::{
    CompletionErrorKind, ConfigError, DataIntegrityError, Error, ErrorKind, FatalError, FileKind,
    InternalError, OperationError, ResourceError, Result,
};
use crate::file::SparseFile;
use crate::file::block_integrity::{BLOCK_INTEGRITY_HEADER_SIZE, write_block_checksum};
use crate::file::cow_file::{COW_FILE_PAGE_SIZE, SUPER_BLOCK_ID, tests::old_root_drop_count};
use crate::file::table_file::ActiveRoot;
use crate::id::{BlockID, PageID, RowID, SessionID, TableID, TrxID};
use crate::index::{
    COLUMN_BLOCK_HEADER_SIZE, COLUMN_BLOCK_LEAF_HEADER_SIZE, ColumnBlockIndex, IndexInsert,
    NonUniqueIndex, RowLocation, SecondaryIndex, UniqueIndex,
};
use crate::io::{
    IOKind, StdIoResult, StorageBackendFileIdentity, StorageBackendOp, StorageBackendTestHook,
    install_storage_backend_test_hook,
};
use crate::latch::LatchFallbackMode;
use crate::lock::tests::{LockDebugEntryState, debug_snapshot, try_acquire};
use crate::lock::{LockMode, LockOwner, LockResource};
use crate::quiescent::QuiescentGuard;
use crate::row::RowPage;
use crate::row::ops::{DeleteMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
use crate::session::{Session, tests::SessionTestExt};
use crate::table::{
    CheckpointCancelReason, CheckpointOutcome, CheckpointReadiness, DeleteMarker, Table,
    TableLifecycleState, TableRuntimeLayout,
};
use crate::trx::redo::DDLRedo;
use crate::trx::row::LockRowForWrite;
use crate::trx::stmt::Statement;
use crate::trx::stmt::tests as stmt_tests;
use crate::trx::tests as trx_tests;
use crate::trx::undo::RowUndoKind;
use crate::trx::ver_map::RowPageState;
use crate::trx::{MAX_SNAPSHOT_TS, Transaction};
use crate::value::{Val, ValKind};
use error_stack::Report;
use futures::FutureExt;
use std::cell::{Cell, RefCell};
use std::fs::OpenOptions;
use std::future::Future;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tempfile::TempDir;

type TableHook = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + 'static>> + 'static>;

thread_local! {
    static TEST_FORCE_LWC_BUILD_ERROR: Cell<bool> = const { Cell::new(false) };
    static TEST_FORCE_SECONDARY_SIDECAR_ERROR: Cell<bool> = const { Cell::new(false) };
    static TEST_FORCE_POST_PUBLISH_CHECKPOINT_ERROR: Cell<bool> = const { Cell::new(false) };
    static TEST_CHECKPOINT_AFTER_READINESS_HOOK: RefCell<Option<TableHook>> =
        const { RefCell::new(None) };
    static TEST_CHECKPOINT_AFTER_TRX_START_HOOK: RefCell<Option<TableHook>> =
        const { RefCell::new(None) };
}

const LIGHTWEIGHT_TEST_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const LIGHTWEIGHT_TEST_MAX_FILE_BYTES: usize = 32 * 1024 * 1024;
const LIGHTWEIGHT_TEST_READONLY_BUFFER_BYTES: usize = 32 * 1024 * 1024;

#[inline]
pub(crate) fn test_user_table_id(offset: u64) -> TableID {
    USER_OBJ_ID_START
        .checked_add(offset)
        .expect("test user table id offset overflow")
}

pub(super) fn set_test_force_lwc_build_error(enabled: bool) {
    TEST_FORCE_LWC_BUILD_ERROR.with(|flag| flag.set(enabled));
}

pub(super) fn test_force_lwc_build_error_enabled() -> bool {
    TEST_FORCE_LWC_BUILD_ERROR.with(|flag| flag.get())
}

pub(super) fn set_test_force_secondary_sidecar_error(enabled: bool) {
    TEST_FORCE_SECONDARY_SIDECAR_ERROR.with(|flag| flag.set(enabled));
}

pub(super) fn test_force_secondary_sidecar_error_enabled() -> bool {
    TEST_FORCE_SECONDARY_SIDECAR_ERROR.with(|flag| flag.get())
}

pub(super) fn set_test_force_post_publish_checkpoint_error(enabled: bool) {
    TEST_FORCE_POST_PUBLISH_CHECKPOINT_ERROR.with(|flag| flag.set(enabled));
}

pub(super) fn test_force_post_publish_checkpoint_error_enabled() -> bool {
    TEST_FORCE_POST_PUBLISH_CHECKPOINT_ERROR.with(|flag| flag.get())
}

fn set_test_checkpoint_after_readiness_hook<F, Fut>(hook: F)
where
    F: FnOnce() -> Fut + 'static,
    Fut: Future<Output = ()> + 'static,
{
    TEST_CHECKPOINT_AFTER_READINESS_HOOK.with(|slot| {
        let old = slot
            .borrow_mut()
            .replace(Box::new(move || Box::pin(hook())));
        assert!(old.is_none(), "checkpoint readiness hook already installed");
    });
}

fn set_test_checkpoint_after_trx_start_hook<F, Fut>(hook: F)
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

pub(super) async fn run_test_checkpoint_after_readiness_hook() {
    let hook = TEST_CHECKPOINT_AFTER_READINESS_HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook().await;
    }
}

pub(super) async fn run_test_checkpoint_after_trx_start_hook() {
    let hook = TEST_CHECKPOINT_AFTER_TRX_START_HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook().await;
    }
}

fn assert_table_data_integrity(
    err: Error,
    block_kind: &str,
    block_id: BlockID,
    expected: DataIntegrityError,
) {
    let report = format!("{err:?}");
    if err.completion_error() == Some(CompletionErrorKind::DataIntegrity(expected)) {
        assert!(report.contains("propagate from other threads"), "{report}");
        assert!(report.contains("wait for"), "{report}");
        return;
    }
    assert_eq!(err.data_integrity_error(), Some(expected), "{report}");
    assert!(report.contains("table-file"), "{report}");
    assert!(report.contains(block_kind), "{report}");
    assert!(report.contains(&format!("block_id={block_id}")), "{report}");
}

fn assert_checkpoint_write_poisoned(err: &Error, engine: &Engine) {
    assert_eq!(
        err.report().downcast_ref::<FatalError>().copied(),
        Some(FatalError::CheckpointWrite)
    );
    assert!(
        engine
            .trx_sys
            .storage_poison_error()
            .as_ref()
            .is_some_and(|err| *err.current_context() == FatalError::CheckpointWrite)
    );
}

async fn stmt_insert_row_by_id(
    stmt: &mut Statement<'_>,
    table_id: TableID,
    cols: Vec<Val>,
) -> Result<RowID> {
    stmt.table_insert_mvcc(table_id, cols).await
}

async fn stmt_delete_row_by_id(
    stmt: &mut Statement<'_>,
    table_id: TableID,
    key: &SelectKey,
) -> Result<DeleteMvcc> {
    stmt.table_delete_unique_mvcc(table_id, key, false).await
}

async fn stmt_update_row_by_id(
    stmt: &mut Statement<'_>,
    table_id: TableID,
    key: &SelectKey,
    update: Vec<UpdateCol>,
) -> Result<UpdateMvcc> {
    stmt.table_update_unique_mvcc(table_id, key, update).await
}

async fn stmt_select_row_mvcc_by_id(
    stmt: &mut Statement<'_>,
    table_id: TableID,
    key: &SelectKey,
    user_read_set: &[usize],
) -> Result<SelectMvcc> {
    stmt.table_lookup_unique_mvcc(table_id, key, user_read_set)
        .await
}

async fn trx_insert_row_by_id(
    trx: &mut Transaction,
    table_id: TableID,
    cols: Vec<Val>,
) -> Result<RowID> {
    trx.exec(async |stmt| stmt_insert_row_by_id(stmt, table_id, cols).await)
        .await
}

async fn trx_delete_row_by_id(
    trx: &mut Transaction,
    table_id: TableID,
    key: &SelectKey,
) -> Result<DeleteMvcc> {
    trx.exec(async |stmt| stmt_delete_row_by_id(stmt, table_id, key).await)
        .await
}

async fn trx_update_row_by_id(
    trx: &mut Transaction,
    table_id: TableID,
    key: &SelectKey,
    update: Vec<UpdateCol>,
) -> Result<UpdateMvcc> {
    trx.exec(async |stmt| stmt_update_row_by_id(stmt, table_id, key, update).await)
        .await
}

async fn trx_select_row_mvcc_by_id(
    trx: &mut Transaction,
    table_id: TableID,
    key: &SelectKey,
    user_read_set: &[usize],
) -> Result<SelectMvcc> {
    trx.exec(async |stmt| stmt_select_row_mvcc_by_id(stmt, table_id, key, user_read_set).await)
        .await
}

struct FailingPageReadHook {
    file: StorageBackendFileIdentity,
    offset: usize,
    errno: i32,
    calls: AtomicUsize,
}

impl FailingPageReadHook {
    #[inline]
    fn for_page(file: StorageBackendFileIdentity, page_id: PageID, errno: i32) -> Self {
        Self {
            file,
            offset: usize::from(page_id) * PAGE_SIZE,
            errno,
            calls: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    #[inline]
    fn matches(&self, op: StorageBackendOp) -> bool {
        op.kind() == IOKind::Read
            && op.matches_file_identity(self.file)
            && op.offset() == self.offset
    }
}

impl StorageBackendTestHook for FailingPageReadHook {
    fn on_submit(&self, op: StorageBackendOp) {
        if self.matches(op) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn on_complete(&self, op: StorageBackendOp, res: &mut StdIoResult<usize>) {
        if self.matches(op) {
            *res = Err(io::Error::from_raw_os_error(self.errno));
        }
    }
}

struct FailingFirstWriteHook {
    file_path: PathBuf,
    calls: AtomicUsize,
}

impl FailingFirstWriteHook {
    #[inline]
    fn new(file_path: impl Into<PathBuf>) -> Self {
        Self {
            file_path: file_path.into(),
            calls: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    #[inline]
    fn matches(&self, op: StorageBackendOp) -> bool {
        if op.kind() != IOKind::Write {
            return false;
        }
        StorageBackendFileIdentity::from_path(&self.file_path)
            .is_ok_and(|expected| op.matches_file_identity(expected))
    }
}

impl StorageBackendTestHook for FailingFirstWriteHook {
    fn on_complete(&self, op: StorageBackendOp, res: &mut StdIoResult<usize>) {
        if self.matches(op) && self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            *res = Err(io::Error::from_raw_os_error(libc::EIO));
        }
    }
}

#[test]
fn test_mvcc_insert_normal() {
    smol::block_on(async {
        const SIZE: i32 = 10000;

        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;

        let mut session = engine.new_session().unwrap();
        {
            let mut trx = session.begin_trx().unwrap();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = expect_trx_insert(table_id, trx, insert).await;
            }
            trx.commit().await.unwrap();
        }
        {
            let mut trx = session.begin_trx().unwrap();
            for i in 16..SIZE {
                let key = SelectKey::new(0, vec![Val::from(i)]);
                trx = expect_trx_select(table_id, trx, &key, |vals| {
                    assert!(vals.len() == 2);
                    assert!(vals[0] == Val::from(i));
                    let s = format!("{}", i);
                    assert!(vals[1] == Val::from(&s[..]));
                })
                .await;
            }
            let _ = trx.commit().await.unwrap();
        }
    });
}

#[test]
fn test_mvcc_insert_dup_key() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        // dup key
        {
            // insert [1, "hello"]
            let insert = vec![Val::from(1i32), Val::from("hello")];
            let mut trx = session.begin_trx().unwrap();
            trx = expect_trx_insert(table_id, trx, insert).await;
            trx.commit().await.unwrap();

            // insert [1, "world"]
            let insert = vec![Val::from(1i32), Val::from("world")];
            let mut trx = session.begin_trx().unwrap();
            let res = trx_insert_row_by_id(&mut trx, table_id, insert).await;
            let err = res.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::DuplicateKey));
            trx.rollback().await.unwrap();
        }
        // write conflict
        {
            // insert [2, "hello"], but not commit
            let insert1 = vec![Val::from(2i32), Val::from("hello")];
            let mut trx1 = session.begin_trx().unwrap();
            let res = trx_insert_row_by_id(&mut trx1, table_id, insert1).await;
            assert!(res.is_ok());

            // begin concurrent transaction and insert [2, "world"]
            let mut session2 = engine.new_session().unwrap();
            let insert2 = vec![Val::from(2i32), Val::from("world")];
            let mut trx2 = session2.begin_trx().unwrap();
            let res = trx_insert_row_by_id(&mut trx2, table_id, insert2).await;
            // still dup key because circuit breaker on index search.
            let err = res.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::DuplicateKey));
            trx2.rollback().await.unwrap();
            drop(session2);

            trx1.commit().await.unwrap();
        }
    });
}

#[test]
fn test_mvcc_update_normal() {
    smol::block_on(async {
        const SIZE: i32 = 1000;

        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        {
            let mut session = engine.new_session().unwrap();
            // insert 1000 rows
            let mut trx = session.begin_trx().unwrap();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = expect_trx_insert(table_id, trx, insert).await;
            }
            trx.commit().await.unwrap();

            // update 1 row with short value
            let mut trx = session.begin_trx().unwrap();
            let k1 = single_key(1i32);
            let s1 = "hello";
            let update1 = vec![UpdateCol {
                idx: 1,
                val: Val::from(s1),
            }];
            trx = expect_trx_update(table_id, trx, &k1, update1).await;
            trx.commit().await.unwrap();

            // update 1 row with long value
            let mut trx = session.begin_trx().unwrap();
            let k2 = single_key(100i32);
            let s2: String = (0..50_000).map(|_| '1').collect();
            let update2 = vec![UpdateCol {
                idx: 1,
                val: Val::from(&s2[..]),
            }];
            trx = expect_trx_update(table_id, trx, &k2, update2).await;

            // lookup this updated value inside same transaction
            trx = expect_trx_select(table_id, trx, &k2, |row| {
                assert!(row.len() == 2);
                assert!(row[0] == k2.vals[0]);
                assert!(row[1] == Val::from(&s2[..]));
            })
            .await;

            trx.commit().await.unwrap();

            // lookup with a new transaction
            let mut trx = session.begin_trx().unwrap();
            trx = expect_trx_select(table_id, trx, &k2, |row| {
                assert!(row.len() == 2);
                assert!(row[0] == k2.vals[0]);
                assert!(row[1] == Val::from(&s2[..]));
            })
            .await;

            let _ = trx.commit().await.unwrap();
        }
    });
}

#[test]
fn test_mvcc_delete_normal() {
    smol::block_on(async {
        const SIZE: i32 = 1000;

        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        {
            let mut session = engine.new_session().unwrap();
            // insert 1000 rows
            // let mut trx = session.begin_trx(trx_sys);
            let mut trx = session.begin_trx().unwrap();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = expect_trx_insert(table_id, trx, insert).await;
            }
            trx.commit().await.unwrap();

            // delete 1 row
            let mut trx = session.begin_trx().unwrap();
            let k1 = single_key(1i32);
            trx = expect_trx_delete(table_id, trx, &k1).await;

            // lookup row in same transaction
            trx = expect_trx_select_not_found(table_id, trx, &k1).await;
            trx.commit().await.unwrap();

            // lookup row in new transaction
            let mut trx = session.begin_trx().unwrap();
            let k1 = single_key(1i32);
            trx = expect_trx_select_not_found(table_id, trx, &k1).await;
            let _ = trx.commit().await.unwrap();
        }
    });
}

#[test]
fn test_column_delete_basic() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(1i32);
        let mut reader_session = engine.new_session().unwrap();
        let trx = reader_session.begin_trx().unwrap();
        let _ = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &reader_session.pool_guards(),
            &key,
            trx.sts(),
        )
        .await;
        trx.commit().await.unwrap();

        let mut trx = session.begin_trx().unwrap();
        let res = trx_delete_row_by_id(&mut trx, table_id, &key).await;
        assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
        trx.commit().await.unwrap();

        let mut trx = session.begin_trx().unwrap();
        trx = expect_trx_select_not_found(table_id, trx, &key).await;
        trx.commit().await.unwrap();
    });
}

#[test]
fn test_lwc_read_uses_readonly_buffer_pool() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(1i32);
        let mut reader_session = engine.new_session().unwrap();
        let trx = reader_session.begin_trx().unwrap();
        let _ = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &reader_session.pool_guards(),
            &key,
            trx.sts(),
        )
        .await;
        trx.commit().await.unwrap();

        let allocated_after_route = engine.disk_pool.allocated();
        assert!(allocated_after_route >= 1);

        expect_select_committed(table_id, &mut session, &key, |vals| {
            assert_eq!(vals[0], Val::from(1i32));
            assert_eq!(vals[1], Val::from("name"));
        })
        .await;
        let allocated_after_first = engine.disk_pool.allocated();
        assert!(allocated_after_first >= allocated_after_route);

        expect_select_committed(table_id, &mut session, &key, |vals| {
            assert_eq!(vals[0], Val::from(1i32));
            assert_eq!(vals[1], Val::from("name"));
        })
        .await;
        assert_eq!(engine.disk_pool.allocated(), allocated_after_first);
    });
}

#[test]
fn test_find_row_returns_resolved_lwc_page_location() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(1i32);
        let trx = session.begin_trx().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let index = bound_unique_index_no(&table, key.index_no);
        let (row_id, _) = index
            .lookup(session.pool_guards().index_guard(), &key.vals, trx.sts())
            .await
            .unwrap()
            .unwrap();

        let pool_guards = session.pool_guards();
        let snapshot = column_block_index_snapshot(&engine, table_id);
        let column_index = snapshot.index(pool_guards.disk_guard());
        let resolved = column_index
            .locate_and_resolve_row(row_id)
            .await
            .unwrap()
            .unwrap();

        match table
            .find_row(&session.pool_guards(), row_id)
            .await
            .unwrap()
        {
            RowLocation::LwcBlock {
                block_id,
                row_idx,
                row_shape_fingerprint,
            } => {
                assert_eq!(block_id, resolved.block_id());
                assert_eq!(row_idx, resolved.row_idx());
                assert_eq!(row_shape_fingerprint, resolved.row_shape_fingerprint());
            }
            RowLocation::RowPage(..) => panic!("row should be in lwc"),
            RowLocation::NotFound => panic!("row should exist"),
        }
        trx.commit().await.unwrap();
    });
}

#[test]
fn test_lwc_select_surfaces_persisted_corruption() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(1i32);
        let trx = session.begin_trx().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let row_id = assert_row_in_lwc(&table, &session.pool_guards(), &key, trx.sts()).await;
        trx.commit().await.unwrap();

        let pool_guards = session.pool_guards();
        let snapshot = column_block_index_snapshot(&engine, table_id);
        let index = snapshot.index(pool_guards.disk_guard());
        let entry = index.locate_block(row_id).await.unwrap().unwrap();
        let block_id = entry.block_id();

        let table_file_path = engine.table_fs.user_table_file_path(table_id);
        corrupt_page_checksum(table_file_path, block_id);

        let mut trx = session.begin_trx().unwrap();
        let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0, 1]).await;
        let err = match res {
            Err(err) => err,
            other => panic!("expected persisted LWC corruption, got {other:?}"),
        };
        assert_table_data_integrity(
            err,
            "lwc-block",
            block_id,
            DataIntegrityError::ChecksumMismatch,
        );
        trx.rollback().await.unwrap();
    });
}

#[test]
fn test_lwc_select_surfaces_column_block_index_row_metadata_corruption() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 4, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(1i32);
        let trx = session.begin_trx().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let row_id = assert_row_in_lwc(&table, &session.pool_guards(), &key, trx.sts()).await;
        trx.commit().await.unwrap();

        let pool_guards = session.pool_guards();
        let snapshot = column_block_index_snapshot(&engine, table_id);
        let index = snapshot.index(pool_guards.disk_guard());
        let entry = index.locate_block(row_id).await.unwrap().unwrap();

        let table_file_path = engine.table_fs.user_table_file_path(table_id);
        corrupt_leaf_row_codec(table_file_path, entry.leaf_block_id, 0);
        let _ = table
            .disk_pool()
            .invalidate_block(table.file().sparse_file().file_id(), entry.leaf_block_id);

        let mut trx = session.begin_trx().unwrap();
        let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0, 1]).await;
        let err = match res {
            Err(err) => err,
            other => panic!("expected persisted column-block-index corruption, got {other:?}"),
        };
        assert_table_data_integrity(
            err,
            "column-block-index",
            entry.leaf_block_id,
            DataIntegrityError::InvalidPayload,
        );
        trx.rollback().await.unwrap();
    });
}

#[test]
fn test_lwc_select_surfaces_column_block_index_zero_block_id_corruption() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 4, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(1i32);
        let trx = session.begin_trx().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let row_id = assert_row_in_lwc(&table, &session.pool_guards(), &key, trx.sts()).await;
        trx.commit().await.unwrap();

        let pool_guards = session.pool_guards();
        let snapshot = column_block_index_snapshot(&engine, table_id);
        let index = snapshot.index(pool_guards.disk_guard());
        let entry = index.locate_block(row_id).await.unwrap().unwrap();

        let table_file_path = engine.table_fs.user_table_file_path(table_id);
        corrupt_leaf_block_id(table_file_path, entry.leaf_block_id, 0);
        let _ = table
            .disk_pool()
            .invalidate_block(table.file().sparse_file().file_id(), entry.leaf_block_id);

        let mut trx = session.begin_trx().unwrap();
        let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0, 1]).await;
        let err = match res {
            Err(err) => err,
            other => panic!("expected persisted column-block-index corruption, got {other:?}"),
        };
        assert_table_data_integrity(
            err,
            "column-block-index",
            entry.leaf_block_id,
            DataIntegrityError::InvalidPayload,
        );
        trx.rollback().await.unwrap();
    });
}

#[test]
fn test_lwc_select_surfaces_row_shape_fingerprint_mismatch_corruption() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 4, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(1i32);
        let trx = session.begin_trx().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let row_id = assert_row_in_lwc(&table, &session.pool_guards(), &key, trx.sts()).await;
        trx.commit().await.unwrap();

        let pool_guards = session.pool_guards();
        let snapshot = column_block_index_snapshot(&engine, table_id);
        let index = snapshot.index(pool_guards.disk_guard());
        let entry = index.locate_block(row_id).await.unwrap().unwrap();

        let table_file_path = engine.table_fs.user_table_file_path(table_id);
        corrupt_lwc_row_shape_fingerprint(table_file_path, entry.block_id());
        let _ = table
            .disk_pool()
            .invalidate_block(table.file().sparse_file().file_id(), entry.block_id());

        let mut trx = session.begin_trx().unwrap();
        let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0, 1]).await;
        let err = match res {
            Err(err) => err,
            other => panic!("expected persisted LWC invalid-payload corruption, got {other:?}"),
        };
        assert_table_data_integrity(
            err,
            "lwc-block",
            entry.block_id(),
            DataIntegrityError::InvalidPayload,
        );
        trx.rollback().await.unwrap();
    });
}

#[test]
fn test_column_delete_rollback() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(2i32);
        let mut reader_session = engine.new_session().unwrap();
        let trx = reader_session.begin_trx().unwrap();
        let old_row_id = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &reader_session.pool_guards(),
            &key,
            trx.sts(),
        )
        .await;
        trx.commit().await.unwrap();

        let mut trx = session.begin_trx().unwrap();
        trx.exec(async |stmt| {
            let res = stmt_delete_row_by_id(stmt, table_id, &key).await;
            assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
            assert_unique_index_entry(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &key,
                stmt.runtime().sts(),
                old_row_id,
                true,
            )
            .await;
            Ok(())
        })
        .await
        .unwrap();
        trx.rollback().await.unwrap();
        assert_unique_index_entry(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &key,
            MAX_SNAPSHOT_TS,
            old_row_id,
            false,
        )
        .await;

        let mut trx = session.begin_trx().unwrap();
        trx = expect_trx_select(table_id, trx, &key, |row| {
            assert_eq!(row[0], Val::from(2i32));
        })
        .await;
        trx.commit().await.unwrap();
    });
}

#[test]
fn test_column_delete_rollback_after_checkpoint() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;

        let key = single_key(3i32);
        let mut trx_delete = session.begin_trx().unwrap();
        let res = trx_delete_row_by_id(&mut trx_delete, table_id, &key).await;
        assert!(matches!(res, Ok(DeleteMvcc::Deleted)));

        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let mut checkpoint_session = engine.new_session().unwrap();
        checkpoint_published(table_id, &mut checkpoint_session).await;

        let mut reader_session = engine.new_session().unwrap();
        let trx = reader_session.begin_trx().unwrap();
        let _ = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &reader_session.pool_guards(),
            &key,
            trx.sts(),
        )
        .await;
        trx.commit().await.unwrap();

        let res = trx_select_row_mvcc_by_id(&mut trx_delete, table_id, &key, &[0, 1]).await;
        assert!(matches!(res, Ok(SelectMvcc::NotFound)));
        trx_delete.rollback().await.unwrap();

        let mut trx = session.begin_trx().unwrap();
        trx = expect_trx_select(table_id, trx, &key, |row| {
            assert_eq!(row[0], Val::from(3i32));
        })
        .await;
        trx.commit().await.unwrap();
    });
}

#[test]
fn test_column_delete_write_conflict() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(4i32);
        let trx = session.begin_trx().unwrap();
        let _ = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &key,
            trx.sts(),
        )
        .await;
        trx.commit().await.unwrap();

        let mut trx1 = session.begin_trx().unwrap();
        let res1 = trx_delete_row_by_id(&mut trx1, table_id, &key).await;
        assert!(matches!(res1, Ok(DeleteMvcc::Deleted)));

        let mut session2 = engine.new_session().unwrap();
        let mut trx2 = session2.begin_trx().unwrap();
        let res2 = trx_delete_row_by_id(&mut trx2, table_id, &key).await;
        assert!(matches!(res2, Ok(DeleteMvcc::WriteConflict)));
        trx2.rollback().await.unwrap();
        drop(session2);

        trx1.rollback().await.unwrap();
    });
}

#[test]
fn test_column_delete_mvcc_visibility() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(5i32);
        let trx = session.begin_trx().unwrap();
        let _ = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &key,
            trx.sts(),
        )
        .await;
        trx.commit().await.unwrap();

        let mut reader_session = engine.new_session().unwrap();
        let mut trx_reader = reader_session.begin_trx().unwrap();

        let mut delete_session = engine.new_session().unwrap();
        let mut trx_delete = delete_session.begin_trx().unwrap();
        let res = trx_delete_row_by_id(&mut trx_delete, table_id, &key).await;
        assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
        trx_delete.commit().await.unwrap();

        trx_reader = expect_trx_select(table_id, trx_reader, &key, |row| {
            assert_eq!(row[0], Val::from(5i32));
        })
        .await;
        trx_reader.commit().await.unwrap();

        let mut trx_new = session.begin_trx().unwrap();
        trx_new = expect_trx_select_not_found(table_id, trx_new, &key).await;
        trx_new.commit().await.unwrap();
    });
}

#[test]
fn test_lwc_delete_unique_conflicts_when_delete_committed_after_snapshot() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(5i32);
        let mut writer_session = engine.new_session().unwrap();
        let mut writer = writer_session.begin_trx().unwrap();
        let writer_sts = writer.sts();
        let row_id = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &writer_session.pool_guards(),
            &key,
            writer_sts,
        )
        .await;

        expect_delete_committed(table_id, &mut session, &key).await;
        let delete_cts = delete_marker_ts(
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .get(row_id)
                .unwrap(),
        );
        assert!(delete_cts > writer_sts);

        writer = expect_trx_select(table_id, writer, &key, |row| {
            assert_eq!(row, vec![Val::from(5i32), Val::from("name")]);
        })
        .await;

        let res = trx_delete_row_by_id(&mut writer, table_id, &key).await;
        assert!(matches!(res, Ok(DeleteMvcc::WriteConflict)));
        writer.rollback().await.unwrap();

        expect_select_not_found_committed(table_id, &mut session, &key).await;
    });
}

#[test]
fn test_lwc_update_unique_same_key_reinserts_hot_and_preserves_old_snapshot() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 4, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(1i32);
        let mut old_reader_session = engine.new_session().unwrap();
        let mut old_reader = old_reader_session.begin_trx().unwrap();
        let old_row_id = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &old_reader_session.pool_guards(),
            &key,
            old_reader.sts(),
        )
        .await;

        let mut writer = session.begin_trx().unwrap();
        writer
            .exec(async |stmt| {
                let res = stmt_update_row_by_id(
                    stmt,
                    table_id,
                    &key,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("updated"),
                    }],
                )
                .await;
                let new_row_id = match res {
                    Ok(UpdateMvcc::Updated(row_id)) => row_id,
                    other => panic!("expected update success, got {other:?}"),
                };
                assert_ne!(old_row_id, new_row_id);
                assert_unique_index_entry(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &key,
                    stmt.runtime().sts(),
                    new_row_id,
                    false,
                )
                .await;
                assert!(matches!(
                    table_for_internal_assertion(&engine, table_id)
                        .find_row(&session.pool_guards(), new_row_id)
                        .await
                        .unwrap(),
                    RowLocation::RowPage(_)
                ));
                match table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(old_row_id)
                    .unwrap()
                {
                    DeleteMarker::Ref(status) => {
                        assert!(Arc::ptr_eq(&status, &stmt.runtime().status()));
                    }
                    DeleteMarker::Committed(_) => {
                        panic!("update should hold an in-flight delete marker")
                    }
                }

                let res = stmt_select_row_mvcc_by_id(stmt, table_id, &key, &[0, 1]).await;
                assert!(matches!(
                    res,
                    Ok(SelectMvcc::Found(vals))
                        if vals == vec![Val::from(1i32), Val::from("updated")]
                ));
                Ok(())
            })
            .await
            .unwrap();
        old_reader = expect_trx_select(table_id, old_reader, &key, |vals| {
            assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
        })
        .await;

        writer.commit().await.unwrap();

        expect_select_committed(table_id, &mut session, &key, |vals| {
            assert_eq!(vals, vec![Val::from(1i32), Val::from("updated")]);
        })
        .await;
        old_reader = expect_trx_select(table_id, old_reader, &key, |vals| {
            assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
        })
        .await;
        old_reader.commit().await.unwrap();
    });
}

#[test]
fn test_lwc_update_unique_conflicts_when_delete_committed_after_snapshot() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(5i32);
        let mut writer_session = engine.new_session().unwrap();
        let mut writer = writer_session.begin_trx().unwrap();
        let writer_sts = writer.sts();
        let row_id = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &writer_session.pool_guards(),
            &key,
            writer_sts,
        )
        .await;

        expect_delete_committed(table_id, &mut session, &key).await;
        let delete_cts = delete_marker_ts(
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .get(row_id)
                .unwrap(),
        );
        assert!(delete_cts > writer_sts);

        writer = expect_trx_select(table_id, writer, &key, |row| {
            assert_eq!(row, vec![Val::from(5i32), Val::from("name")]);
        })
        .await;

        let res = trx_update_row_by_id(
            &mut writer,
            table_id,
            &key,
            vec![UpdateCol {
                idx: 1,
                val: Val::from("updated"),
            }],
        )
        .await;
        let err = res.unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::WriteConflict));
        writer.rollback().await.unwrap();

        expect_select_not_found_committed(table_id, &mut session, &key).await;
    });
}

#[test]
fn test_lwc_update_unique_key_change_preserves_old_and_new_key_visibility() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 4, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let old_key = single_key(2i32);
        let new_key = single_key(20i32);
        let mut old_reader_session = engine.new_session().unwrap();
        let mut old_reader = old_reader_session.begin_trx().unwrap();
        let old_row_id = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &old_reader_session.pool_guards(),
            &old_key,
            old_reader.sts(),
        )
        .await;

        let mut writer = session.begin_trx().unwrap();
        writer
            .exec(async |stmt| {
                let res = stmt_update_row_by_id(
                    stmt,
                    table_id,
                    &old_key,
                    vec![
                        UpdateCol {
                            idx: 0,
                            val: Val::from(20i32),
                        },
                        UpdateCol {
                            idx: 1,
                            val: Val::from("moved"),
                        },
                    ],
                )
                .await;
                let new_row_id = match res {
                    Ok(UpdateMvcc::Updated(row_id)) => row_id,
                    other => panic!("expected update success, got {other:?}"),
                };
                assert_unique_index_entry(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &old_key,
                    stmt.runtime().sts(),
                    old_row_id,
                    true,
                )
                .await;
                assert_unique_index_entry(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &new_key,
                    stmt.runtime().sts(),
                    new_row_id,
                    false,
                )
                .await;
                Ok(())
            })
            .await
            .unwrap();
        writer.commit().await.unwrap();

        expect_select_not_found_committed(table_id, &mut session, &old_key).await;
        expect_select_committed(table_id, &mut session, &new_key, |vals| {
            assert_eq!(vals, vec![Val::from(20i32), Val::from("moved")]);
        })
        .await;

        old_reader = expect_trx_select(table_id, old_reader, &old_key, |vals| {
            assert_eq!(vals, vec![Val::from(2i32), Val::from("name")]);
        })
        .await;
        old_reader = expect_trx_select_not_found(table_id, old_reader, &new_key).await;
        old_reader.commit().await.unwrap();
    });
}

#[test]
fn test_lwc_update_unique_duplicate_rolls_back_cold_marker_and_hot_insert() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 4, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(1i32);
        let duplicate_key = single_key(2i32);
        let trx = session.begin_trx().unwrap();
        let old_row_id = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &key,
            trx.sts(),
        )
        .await;
        trx.commit().await.unwrap();

        let mut trx = session.begin_trx().unwrap();
        let res = trx_update_row_by_id(
            &mut trx,
            table_id,
            &key,
            vec![UpdateCol {
                idx: 0,
                val: Val::from(2i32),
            }],
        )
        .await;
        let err = res.unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::DuplicateKey));
        trx.rollback().await.unwrap();

        assert!(
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .get(old_row_id)
                .is_none()
        );
        expect_select_committed(table_id, &mut session, &key, |vals| {
            assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
        })
        .await;
        expect_select_committed(table_id, &mut session, &duplicate_key, |vals| {
            assert_eq!(vals, vec![Val::from(2i32), Val::from("name")]);
        })
        .await;
    });
}

#[test]
fn test_lwc_update_unique_claims_committed_deleted_cold_owner_with_visibility_bridge() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 1, 2, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let old_key = single_key(1i32);
        let claimed_key = single_key(2i32);
        let mut old_reader_session = engine.new_session().unwrap();
        let mut old_reader = old_reader_session.begin_trx().unwrap();
        let _ = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &old_reader_session.pool_guards(),
            &claimed_key,
            old_reader.sts(),
        )
        .await;

        expect_delete_committed(table_id, &mut session, &claimed_key).await;

        let mut gap_reader_session = engine.new_session().unwrap();
        let mut gap_reader = gap_reader_session.begin_trx().unwrap();

        expect_update_committed(
            table_id,
            &mut session,
            &old_key,
            vec![
                UpdateCol {
                    idx: 0,
                    val: Val::from(2i32),
                },
                UpdateCol {
                    idx: 1,
                    val: Val::from("claimed"),
                },
            ],
        )
        .await;

        expect_select_committed(table_id, &mut session, &claimed_key, |vals| {
            assert_eq!(vals, vec![Val::from(2i32), Val::from("claimed")]);
        })
        .await;
        old_reader = expect_trx_select(table_id, old_reader, &claimed_key, |vals| {
            assert_eq!(vals, vec![Val::from(2i32), Val::from("name")]);
        })
        .await;
        gap_reader = expect_trx_select_not_found(table_id, gap_reader, &claimed_key).await;

        old_reader.commit().await.unwrap();
        gap_reader.commit().await.unwrap();
    });
}

#[test]
fn test_lwc_update_unique_rejects_cold_owner_deleted_after_snapshot() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 1, 2, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let old_key = single_key(1i32);
        let claimed_key = single_key(2i32);
        let mut writer_session = engine.new_session().unwrap();
        let mut writer = writer_session.begin_trx().unwrap();
        let writer_sts = writer.sts();
        let claimed_row_id = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &writer_session.pool_guards(),
            &claimed_key,
            writer_sts,
        )
        .await;

        expect_delete_committed(table_id, &mut session, &claimed_key).await;
        let delete_cts = delete_marker_ts(
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .get(claimed_row_id)
                .unwrap(),
        );
        assert!(delete_cts > writer_sts);
        assert_unique_index_entry(
            &table_for_internal_assertion(&engine, table_id),
            &writer_session.pool_guards(),
            &claimed_key,
            MAX_SNAPSHOT_TS,
            claimed_row_id,
            true,
        )
        .await;

        writer = expect_trx_select(table_id, writer, &claimed_key, |vals| {
            assert_eq!(vals, vec![Val::from(2i32), Val::from("name")]);
        })
        .await;

        let res = trx_update_row_by_id(
            &mut writer,
            table_id,
            &old_key,
            vec![
                UpdateCol {
                    idx: 0,
                    val: Val::from(2i32),
                },
                UpdateCol {
                    idx: 1,
                    val: Val::from("claimed"),
                },
            ],
        )
        .await;
        let err = res.unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::DuplicateKey));
        writer.rollback().await.unwrap();

        assert_unique_index_entry(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &claimed_key,
            MAX_SNAPSHOT_TS,
            claimed_row_id,
            true,
        )
        .await;
        expect_select_committed(table_id, &mut session, &old_key, |vals| {
            assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
        })
        .await;
        expect_select_not_found_committed(table_id, &mut session, &claimed_key).await;
    });
}

#[test]
fn test_lwc_update_unique_claim_rollback_restores_deleted_cold_owner() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 1, 2, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let old_key = single_key(1i32);
        let claimed_key = single_key(2i32);
        let mut old_reader_session = engine.new_session().unwrap();
        let mut old_reader = old_reader_session.begin_trx().unwrap();
        let claimed_row_id = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &old_reader_session.pool_guards(),
            &claimed_key,
            old_reader.sts(),
        )
        .await;

        expect_delete_committed(table_id, &mut session, &claimed_key).await;
        assert_unique_index_entry(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &claimed_key,
            MAX_SNAPSHOT_TS,
            claimed_row_id,
            true,
        )
        .await;

        let mut writer = session.begin_trx().unwrap();
        writer
            .exec(async |stmt| {
                let res = stmt_update_row_by_id(
                    stmt,
                    table_id,
                    &old_key,
                    vec![
                        UpdateCol {
                            idx: 0,
                            val: Val::from(2i32),
                        },
                        UpdateCol {
                            idx: 1,
                            val: Val::from("claimed"),
                        },
                    ],
                )
                .await;
                let new_row_id = match res {
                    Ok(UpdateMvcc::Updated(row_id)) => row_id,
                    other => panic!("expected update success, got {other:?}"),
                };
                assert_ne!(claimed_row_id, new_row_id);
                assert_unique_index_entry(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &claimed_key,
                    stmt.runtime().sts(),
                    new_row_id,
                    false,
                )
                .await;
                assert!(matches!(
                    table_for_internal_assertion(&engine, table_id)
                        .find_row(&session.pool_guards(), claimed_row_id)
                        .await
                        .unwrap(),
                    RowLocation::LwcBlock { .. }
                ));
                Ok(())
            })
            .await
            .unwrap();

        // Keep the statement changes in the transaction so transaction rollback
        // exercises index undo before row undo. That is the path where the
        // claimed deleted owner must still resolve as RowLocation::LwcBlock.
        writer.rollback().await.unwrap();

        assert_unique_index_entry(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &claimed_key,
            MAX_SNAPSHOT_TS,
            claimed_row_id,
            true,
        )
        .await;
        expect_select_committed(table_id, &mut session, &old_key, |vals| {
            assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
        })
        .await;
        expect_select_not_found_committed(table_id, &mut session, &claimed_key).await;
        old_reader = expect_trx_select(table_id, old_reader, &claimed_key, |vals| {
            assert_eq!(vals, vec![Val::from(2i32), Val::from("name")]);
        })
        .await;
        old_reader.commit().await.unwrap();
    });
}

#[test]
fn test_lwc_update_unique_claim_rollback_drops_purgeable_deleted_cold_owner() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 1, 2, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let old_key = single_key(1i32);
        let claimed_key = single_key(2i32);
        let reader = session.begin_trx().unwrap();
        let claimed_row_id = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &claimed_key,
            reader.sts(),
        )
        .await;
        reader.commit().await.unwrap();

        let index = bound_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            claimed_key.index_no,
        );
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &claimed_key.vals,
                    claimed_row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );
        let delete_cts = TrxID::new(1);
        table_for_internal_assertion(&engine, table_id)
            .deletion_buffer()
            .put_committed(claimed_row_id, delete_cts)
            .unwrap();
        expect_select_not_found_committed(table_id, &mut session, &claimed_key).await;

        let mut writer = session.begin_trx().unwrap();
        assert!(delete_cts < writer.sts());
        assert!(
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .delete_marker_is_globally_purgeable(claimed_row_id, writer.sts())
        );
        writer
            .exec(async |stmt| {
                let res = stmt_update_row_by_id(
                    stmt,
                    table_id,
                    &old_key,
                    vec![
                        UpdateCol {
                            idx: 0,
                            val: Val::from(2i32),
                        },
                        UpdateCol {
                            idx: 1,
                            val: Val::from("claimed"),
                        },
                    ],
                )
                .await;
                let new_row_id = match res {
                    Ok(UpdateMvcc::Updated(row_id)) => row_id,
                    other => panic!("expected update success, got {other:?}"),
                };
                assert_unique_index_entry(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &claimed_key,
                    stmt.runtime().sts(),
                    new_row_id,
                    false,
                )
                .await;
                assert!(matches!(
                    table_for_internal_assertion(&engine, table_id)
                        .find_row(&session.pool_guards(), claimed_row_id)
                        .await
                        .unwrap(),
                    RowLocation::LwcBlock { .. }
                ));
                Ok(())
            })
            .await
            .unwrap();

        // This is the stale GC attempt from the original delete. While the
        // replacement claim owns the unique key, GC observes a row-id mismatch
        // and skips the entry, so rollback must not recreate that skipped
        // delete-masked owner.
        let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
        let deleted = table_for_internal_assertion(&engine, table_id)
            .accessor_with_layout(&layout)
            .delete_index(
                &session.pool_guards(),
                &claimed_key,
                claimed_row_id,
                true,
                MAX_SNAPSHOT_TS,
            )
            .await
            .unwrap();
        assert!(!deleted);

        writer.rollback().await.unwrap();

        // The composite index may still fall through to the checkpointed cold
        // root; MVCC reads filter that stale cold owner through the committed
        // deletion marker.
        expect_select_committed(table_id, &mut session, &old_key, |vals| {
            assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
        })
        .await;
        expect_select_not_found_committed(table_id, &mut session, &claimed_key).await;
    });
}

#[test]
fn test_checkpoint_persists_committed_cold_delete_markers() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(6i32);
        let reader = session.begin_trx().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let row_id = assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
        reader.commit().await.unwrap();

        expect_delete_committed(table_id, &mut session, &key).await;
        let marker = table.deletion_buffer().get(row_id).unwrap();
        let marker_ts = delete_marker_ts(marker);
        wait_gc_cutoff_after(&session, marker_ts).await;
        let pool_guards = session.pool_guards();
        let snapshot_before = column_block_index_snapshot(&engine, table_id);
        let index_before = snapshot_before.index(pool_guards.disk_guard());
        let entry_before = index_before
            .locate_block(row_id)
            .await
            .unwrap()
            .expect("persisted entry should exist before delete checkpoint");

        checkpoint_published(table_id, &mut session).await;

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
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 3, "name").await;

        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

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
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
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
        checkpoint_published(table_id, &mut session).await;

        let mut trx = session.begin_trx().unwrap();
        trx.exec(async |stmt| {
            let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
            let proof = rt.read_proof();
            let snapshot = table_for_internal_assertion(&engine, table_id)
                .root_snapshot(&proof)
                .unwrap();
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
fn test_checkpoint_publishes_non_unique_secondary_disk_tree_entries_across_lwc_splits() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine =
            evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique").await;
        let table_id = create_non_unique_name_table_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let name = "split-name-".repeat(120);
        let row_count = 80;
        insert_rows(table_id, &mut session, 0, row_count, &name).await;

        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

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
fn test_secondary_mem_index_cleanup_removes_redundant_live_unique_entries() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let row_count = 4;
        insert_rows(table_id, &mut session, 0, row_count, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let index = bound_unique_index_no(&table_for_internal_assertion(&engine, table_id), 0);
        let stats = session
            .cleanup_secondary_mem_indexes(table_id, true)
            .await
            .unwrap();
        assert!(!session.in_trx().unwrap());
        assert_eq!(stats.indexes.len(), 1);
        assert_eq!(stats.indexes[0].index_no, 0);
        assert!(stats.indexes[0].unique);
        assert_eq!(stats.indexes[0].scanned, row_count as usize);
        assert_eq!(stats.indexes[0].removed, row_count as usize);
        assert_eq!(stats.indexes[0].retained, 0);
        assert_eq!(stats.indexes[0].skipped_live, 0);
        assert_eq!(stats.indexes[0].skipped_hot_deleted, 0);

        for key_value in 0..row_count {
            let key = single_key(key_value);
            let disk_row_id = unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &key,
            )
            .await
            .unwrap();
            assert_eq!(
                index
                    .lookup(
                        session.pool_guards().index_guard(),
                        &key.vals,
                        MAX_SNAPSHOT_TS,
                    )
                    .await
                    .unwrap(),
                Some((disk_row_id, false))
            );
        }
    });
}

#[test]
fn test_secondary_mem_index_cleanup_requires_idle_session() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let trx = session.begin_trx().unwrap();

        let err = session
            .cleanup_secondary_mem_indexes(table_id, true)
            .await
            .unwrap_err();
        let operation_error = err.operation_error();
        let was_in_trx = session.in_trx().unwrap();

        trx.rollback().await.unwrap();
        assert_eq!(operation_error, Some(OperationError::ExistingTransaction));
        assert!(was_in_trx);
        assert!(!session.in_trx().unwrap());
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_redundant_live_non_unique_entries() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine =
            evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique").await;
        let table_id = create_non_unique_name_table_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let row_count = 5;
        insert_rows(table_id, &mut session, 0, row_count, "same-name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let index = bound_non_unique_index_no(&table_for_internal_assertion(&engine, table_id), 1);
        let stats = session
            .cleanup_secondary_mem_indexes(table_id, true)
            .await
            .unwrap();
        assert_eq!(stats.indexes.len(), 2);
        assert_eq!(stats.indexes[1].index_no, 1);
        assert!(!stats.indexes[1].unique);
        assert_eq!(stats.indexes[1].scanned, row_count as usize);
        assert_eq!(stats.indexes[1].removed, row_count as usize);
        assert_eq!(stats.indexes[1].retained, 0);
        assert_eq!(stats.indexes[1].skipped_live, 0);
        assert_eq!(stats.indexes[1].skipped_hot_deleted, 0);

        let key = name_key("same-name");
        let disk_rows = non_unique_disk_tree_prefix_scan(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &key,
        )
        .await;
        assert_eq!(disk_rows.len(), row_count as usize);
        let mut lookup_rows = Vec::new();
        index
            .lookup(
                session.pool_guards().index_guard(),
                &key.vals,
                &mut lookup_rows,
                MAX_SNAPSHOT_TS,
            )
            .await
            .unwrap();
        assert_eq!(lookup_rows, disk_rows);
    });
}

#[test]
fn test_secondary_mem_index_cleanup_aggregates_bounded_batches() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine =
            evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique").await;
        let table_id = create_non_unique_name_table_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let name = "batch-name-".repeat(120);
        let row_count = 80;
        insert_rows(table_id, &mut session, 0, row_count, &name).await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let stats = session
            .cleanup_secondary_mem_indexes(table_id, true)
            .await
            .unwrap();
        assert_eq!(stats.indexes[1].scanned, row_count as usize);
        assert_eq!(stats.indexes[1].removed, row_count as usize);
        assert_eq!(stats.indexes[1].retained, 0);
        assert_eq!(stats.indexes[1].skipped_live, 0);
        assert_eq!(stats.indexes[1].skipped_hot_deleted, 0);
    });
}

#[test]
fn test_secondary_mem_index_cleanup_can_retain_live_cache_entries() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine =
            evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique").await;
        let table_id = create_non_unique_name_table_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let row_count = 4;
        insert_rows(table_id, &mut session, 0, row_count, "same-name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let unique_index =
            bound_unique_index_no(&table_for_internal_assertion(&engine, table_id), 0);
        let non_unique_index =
            bound_non_unique_index_no(&table_for_internal_assertion(&engine, table_id), 1);
        let stats = session
            .cleanup_secondary_mem_indexes(table_id, false)
            .await
            .unwrap();
        assert_eq!(stats.indexes.len(), 2);
        for index_stats in &stats.indexes {
            assert_eq!(index_stats.scanned, 0);
            assert_eq!(index_stats.removed, 0);
            assert_eq!(index_stats.retained, 0);
            assert_eq!(index_stats.skipped_live, row_count as usize);
            assert_eq!(index_stats.skipped_hot_deleted, 0);
        }

        let unique_key = single_key(0i32);
        let unique_row_id = unique_disk_tree_lookup(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &unique_key,
        )
        .await
        .unwrap();
        assert_eq!(
            unique_index
                .lookup(
                    session.pool_guards().index_guard(),
                    &unique_key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some((unique_row_id, false))
        );

        let name_key = name_key("same-name");
        let disk_rows = non_unique_disk_tree_prefix_scan(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &name_key,
        )
        .await;
        let mut lookup_rows = Vec::new();
        non_unique_index
            .lookup(
                session.pool_guards().index_guard(),
                &name_key.vals,
                &mut lookup_rows,
                MAX_SNAPSHOT_TS,
            )
            .await
            .unwrap();
        assert_eq!(lookup_rows, disk_rows);
    });
}

#[test]
fn test_secondary_mem_index_cleanup_retains_unique_delete_shadow_without_delete_proof() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "name").await;

        let current_key = single_key(0i32);
        let stale_key = single_key(-1i32);
        let index = bound_unique_index_no(&table_for_internal_assertion(&engine, table_id), 0);
        let row_id = index
            .lookup(
                session.pool_guards().index_guard(),
                &current_key.vals,
                MAX_SNAPSHOT_TS,
            )
            .await
            .unwrap()
            .unwrap()
            .0;
        assert!(
            index
                .insert_if_not_exists(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    false,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );

        let stats = session
            .cleanup_secondary_mem_indexes(table_id, true)
            .await
            .unwrap();
        assert_eq!(stats.indexes[0].scanned, 0);
        assert_eq!(stats.indexes[0].removed, 0);
        assert_eq!(stats.indexes[0].retained, 0);
        assert_eq!(stats.indexes[0].skipped_live, 1);
        assert_eq!(stats.indexes[0].skipped_hot_deleted, 1);
        assert_eq!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some((row_id, true))
        );
        assert_eq!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some((row_id, false))
        );
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_unique_delete_shadow_with_purgeable_marker() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let current_key = single_key(0i32);
        let stale_key = single_key(-1i32);
        let row_id = unique_disk_tree_lookup(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &current_key,
        )
        .await
        .unwrap();
        let index = bound_unique_index_no(&table_for_internal_assertion(&engine, table_id), 0);
        assert!(
            index
                .insert_if_not_exists(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    false,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );
        table_for_internal_assertion(&engine, table_id)
            .deletion_buffer()
            .put_committed(row_id, TrxID::new(1))
            .unwrap();

        let stats = session
            .cleanup_secondary_mem_indexes(table_id, true)
            .await
            .unwrap();
        assert_eq!(stats.indexes[0].scanned, 2);
        assert_eq!(stats.indexes[0].removed, 2);
        assert_eq!(stats.indexes[0].retained, 0);
        assert_eq!(stats.indexes[0].skipped_live, 0);
        assert_eq!(stats.indexes[0].skipped_hot_deleted, 0);
        assert_eq!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            None
        );
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_delete_shadow_when_live_cleanup_disabled() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let current_key = single_key(0i32);
        let stale_key = single_key(-1i32);
        let row_id = unique_disk_tree_lookup(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &current_key,
        )
        .await
        .unwrap();
        let index = bound_unique_index_no(&table_for_internal_assertion(&engine, table_id), 0);
        assert!(
            index
                .insert_if_not_exists(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    false,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );
        table_for_internal_assertion(&engine, table_id)
            .deletion_buffer()
            .put_committed(row_id, TrxID::new(1))
            .unwrap();

        let stats = session
            .cleanup_secondary_mem_indexes(table_id, false)
            .await
            .unwrap();
        assert_eq!(stats.indexes[0].scanned, 1);
        assert_eq!(stats.indexes[0].removed, 1);
        assert_eq!(stats.indexes[0].retained, 0);
        assert_eq!(stats.indexes[0].skipped_live, 1);
        assert_eq!(stats.indexes[0].skipped_hot_deleted, 0);
        assert_eq!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some((row_id, false))
        );
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_unique_delete_shadow_with_matching_cold_entry() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let current_key = single_key(0i32);
        let row_id = unique_disk_tree_lookup(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &current_key,
        )
        .await
        .unwrap();
        let index = bound_unique_index_no(&table_for_internal_assertion(&engine, table_id), 0);
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );

        let stats = session
            .cleanup_secondary_mem_indexes(table_id, true)
            .await
            .unwrap();
        assert_eq!(stats.indexes[0].scanned, 1);
        assert_eq!(stats.indexes[0].removed, 0);
        assert_eq!(stats.indexes[0].retained, 1);
        assert_eq!(stats.indexes[0].skipped_live, 0);
        assert_eq!(stats.indexes[0].skipped_hot_deleted, 0);
        assert_eq!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some((row_id, true))
        );

        table_for_internal_assertion(&engine, table_id)
            .deletion_buffer()
            .put_committed(row_id, TrxID::new(1))
            .unwrap();
        let stats = session
            .cleanup_secondary_mem_indexes(table_id, true)
            .await
            .unwrap();
        assert_eq!(stats.indexes[0].scanned, 1);
        assert_eq!(stats.indexes[0].removed, 1);
        assert_eq!(stats.indexes[0].retained, 0);
        assert_eq!(stats.indexes[0].skipped_live, 0);
        assert_eq!(stats.indexes[0].skipped_hot_deleted, 0);
        assert_eq!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some((row_id, false))
        );
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_unique_delete_shadow_when_cold_row_key_differs() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let checkpoint_ts = checkpoint_published(table_id, &mut session).await;
        wait_gc_cutoff_after(&session, checkpoint_ts).await;

        let current_key = single_key(0i32);
        let stale_key = single_key(-1i32);
        let row_id = unique_disk_tree_lookup(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &current_key,
        )
        .await
        .unwrap();
        assert_eq!(
            unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &stale_key
            )
            .await,
            None
        );
        let index = bound_unique_index_no(&table_for_internal_assertion(&engine, table_id), 0);
        assert!(
            index
                .insert_if_not_exists(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    false,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );
        assert_eq!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some((row_id, true))
        );
        assert_eq!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some((row_id, false))
        );

        let stats = session
            .cleanup_secondary_mem_indexes(table_id, true)
            .await
            .unwrap();
        assert_eq!(stats.indexes[0].scanned, 2);
        assert_eq!(stats.indexes[0].removed, 2);
        assert_eq!(stats.indexes[0].retained, 0);
        assert_eq!(stats.indexes[0].skipped_live, 0);
        assert_eq!(stats.indexes[0].skipped_hot_deleted, 0);
        assert_eq!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some((row_id, false))
        );
    });
}

#[test]
fn test_secondary_mem_index_cleanup_propagates_cold_delete_overlay_proof_error() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let checkpoint_ts = checkpoint_published(table_id, &mut session).await;
        wait_gc_cutoff_after(&session, checkpoint_ts).await;

        let current_key = single_key(0i32);
        let stale_key = single_key(-1i32);
        let table = table_for_internal_assertion(&engine, table_id);
        let row_id = unique_disk_tree_lookup(&table, &session.pool_guards(), &current_key)
            .await
            .unwrap();
        let block_id = {
            let pool_guards = session.pool_guards();
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let column_index = snapshot.index(pool_guards.disk_guard());
            column_index
                .locate_block(row_id)
                .await
                .unwrap()
                .unwrap()
                .block_id()
        };
        let index = bound_unique_index_no(&table, 0);
        assert!(
            index
                .insert_if_not_exists(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    false,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );

        let table_file_path = engine.table_fs.user_table_file_path(table_id);
        corrupt_lwc_row_shape_fingerprint(table_file_path, block_id);
        let _ = table
            .disk_pool()
            .invalidate_block(table.file().sparse_file().file_id(), block_id);

        let err = session
            .cleanup_secondary_mem_indexes(table_id, true)
            .await
            .unwrap_err();
        assert_table_data_integrity(
            err,
            "lwc-block",
            block_id,
            DataIntegrityError::InvalidPayload,
        );
        assert_eq!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some((row_id, true))
        );
    });
}

#[test]
fn test_secondary_mem_index_cleanup_retains_non_unique_delete_mark_without_delete_proof() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine =
            evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique").await;
        let table_id = create_non_unique_name_table_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "current").await;

        let pk = single_key(0i32);
        let row_id = bound_unique_index_no(&table_for_internal_assertion(&engine, table_id), 0)
            .lookup(
                session.pool_guards().index_guard(),
                &pk.vals,
                MAX_SNAPSHOT_TS,
            )
            .await
            .unwrap()
            .unwrap()
            .0;
        let stale_key = name_key("stale");
        let index = bound_non_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            stale_key.index_no,
        );
        assert!(
            index
                .insert_if_not_exists(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    false,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );

        let stats = session
            .cleanup_secondary_mem_indexes(table_id, true)
            .await
            .unwrap();
        assert_eq!(stats.indexes[1].scanned, 0);
        assert_eq!(stats.indexes[1].removed, 0);
        assert_eq!(stats.indexes[1].retained, 0);
        assert_eq!(stats.indexes[1].skipped_live, 1);
        assert_eq!(stats.indexes[1].skipped_hot_deleted, 1);
        assert_eq!(
            index
                .lookup_unique(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some(false)
        );
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_non_unique_delete_mark_with_purgeable_marker() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine =
            evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique").await;
        let table_id = create_non_unique_name_table_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "current").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let pk = single_key(0i32);
        let row_id = unique_disk_tree_lookup(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &pk,
        )
        .await
        .unwrap();
        let stale_key = name_key("stale");
        let index = bound_non_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            stale_key.index_no,
        );
        assert!(
            index
                .insert_if_not_exists(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    false,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );
        table_for_internal_assertion(&engine, table_id)
            .deletion_buffer()
            .put_committed(row_id, TrxID::new(1))
            .unwrap();

        let stats = session
            .cleanup_secondary_mem_indexes(table_id, true)
            .await
            .unwrap();
        assert_eq!(stats.indexes[1].scanned, 2);
        assert_eq!(stats.indexes[1].removed, 2);
        assert_eq!(stats.indexes[1].retained, 0);
        assert_eq!(stats.indexes[1].skipped_live, 0);
        assert_eq!(stats.indexes[1].skipped_hot_deleted, 0);
        assert_eq!(
            index
                .lookup_unique(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            None
        );
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_non_unique_delete_mark_with_matching_cold_entry() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine =
            evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique").await;
        let table_id = create_non_unique_name_table_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "current").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let pk = single_key(0i32);
        let row_id = unique_disk_tree_lookup(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &pk,
        )
        .await
        .unwrap();
        let current_key = name_key("current");
        let index = bound_non_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            current_key.index_no,
        );
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );

        let stats = session
            .cleanup_secondary_mem_indexes(table_id, true)
            .await
            .unwrap();
        assert_eq!(stats.indexes[1].scanned, 1);
        assert_eq!(stats.indexes[1].removed, 0);
        assert_eq!(stats.indexes[1].retained, 1);
        assert_eq!(stats.indexes[1].skipped_live, 0);
        assert_eq!(stats.indexes[1].skipped_hot_deleted, 0);
        assert_eq!(
            index
                .lookup_unique(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some(false)
        );

        table_for_internal_assertion(&engine, table_id)
            .deletion_buffer()
            .put_committed(row_id, TrxID::new(1))
            .unwrap();
        let stats = session
            .cleanup_secondary_mem_indexes(table_id, true)
            .await
            .unwrap();
        assert_eq!(stats.indexes[1].scanned, 1);
        assert_eq!(stats.indexes[1].removed, 1);
        assert_eq!(stats.indexes[1].retained, 0);
        assert_eq!(stats.indexes[1].skipped_live, 0);
        assert_eq!(stats.indexes[1].skipped_hot_deleted, 0);
        assert_eq!(
            index
                .lookup_unique(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some(true)
        );
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_non_unique_delete_mark_when_cold_row_key_differs() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine =
            evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique").await;
        let table_id = create_non_unique_name_table_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "current").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let checkpoint_ts = checkpoint_published(table_id, &mut session).await;
        wait_gc_cutoff_after(&session, checkpoint_ts).await;

        let pk = single_key(0i32);
        let current_key = name_key("current");
        let stale_key = name_key("stale");
        let row_id = unique_disk_tree_lookup(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &pk,
        )
        .await
        .unwrap();
        assert!(
            non_unique_disk_tree_prefix_scan(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &stale_key
            )
            .await
            .is_empty()
        );
        assert_eq!(
            non_unique_disk_tree_prefix_scan(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &current_key
            )
            .await,
            vec![row_id]
        );
        let index = bound_non_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            stale_key.index_no,
        );
        assert!(
            index
                .insert_if_not_exists(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    false,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );
        assert_eq!(
            index
                .lookup_unique(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some(false)
        );
        assert_eq!(
            index
                .lookup_unique(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some(true)
        );

        let stats = session
            .cleanup_secondary_mem_indexes(table_id, true)
            .await
            .unwrap();
        assert_eq!(stats.indexes[1].scanned, 2);
        assert_eq!(stats.indexes[1].removed, 2);
        assert_eq!(stats.indexes[1].retained, 0);
        assert_eq!(stats.indexes[1].skipped_live, 0);
        assert_eq!(stats.indexes[1].skipped_hot_deleted, 0);
        assert_eq!(
            index
                .lookup_unique(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            index
                .lookup_unique(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some(true)
        );
    });
}

#[test]
fn test_deletion_checkpoint_updates_secondary_disk_tree_roots() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine =
            evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique").await;
        let table_id = create_non_unique_name_table_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 2, "same-name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

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
        wait_gc_cutoff_after(&session, marker_ts).await;
        checkpoint_published(table_id, &mut session).await;

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
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

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

        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let readiness_ts = delete_ts.max(
            table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .root_ts,
        );
        wait_gc_cutoff_after(&session, readiness_ts).await;
        checkpoint_published(table_id, &mut session).await;

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
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 2, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

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
        wait_gc_cutoff_after(&session, marker_ts).await;
        let root_before = table_for_internal_assertion(&engine, table_id)
            .file()
            .active_root_unchecked()
            .clone();

        set_test_force_secondary_sidecar_error(true);
        let _reset = ResetSidecarHook;
        let err = session.checkpoint_table(table_id).await.unwrap_err();
        assert_eq!(
            err.report().downcast_ref::<InternalError>().copied(),
            Some(InternalError::InjectedTestFailure)
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
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;
        delete_key_range_and_wait_gc_cutoff(table_id, &mut session, 0, 10).await;

        let root_before = table_for_internal_assertion(&engine, table_id)
            .file()
            .active_root_unchecked()
            .clone();
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

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
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "name").await;

        let key = single_key(0i32);
        let reader = session.begin_trx().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let index = bound_unique_index_no(&table, key.index_no);
        let (row_id, _) = index
            .lookup(session.pool_guards().index_guard(), &key.vals, reader.sts())
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

        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let mut checkpoint_session = engine.new_session().unwrap();
        checkpoint_published(table_id, &mut checkpoint_session).await;

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
        wait_gc_cutoff_after(&checkpoint_session, delete_cts).await;
        checkpoint_published(table_id, &mut checkpoint_session).await;

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
fn test_lwc_unique_index_purge_uses_purgeable_delete_marker_fast_path() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(0i32);
        let reader = session.begin_trx().unwrap();
        let row_id = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &key,
            reader.sts(),
        )
        .await;
        reader.commit().await.unwrap();

        let index = bound_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            key.index_no,
        );
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );
        table_for_internal_assertion(&engine, table_id)
            .deletion_buffer()
            .put_committed(row_id, TrxID::new(10))
            .unwrap();

        let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();

        let deleted = table_for_internal_assertion(&engine, table_id)
            .accessor_with_layout(&layout)
            .delete_index(&session.pool_guards(), &key, row_id, true, TrxID::new(11))
            .await
            .unwrap();
        assert!(deleted);
        // A reinsertion attempt must not merge a stale MemIndex delete overlay;
        // after purge it falls through to the immutable cold root instead.
        assert_eq!(
            index
                .insert_if_not_exists(
                    session.pool_guards().index_guard(),
                    &key.vals,
                    row_id,
                    true,
                    TrxID::new(11),
                )
                .await
                .unwrap(),
            IndexInsert::DuplicateKey(row_id, false)
        );
    });
}

#[test]
fn test_lwc_unique_index_purge_compares_persisted_key_when_marker_is_not_purgeable() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let current_key = single_key(0i32);
        let stale_key = single_key(-1i32);
        let reader = session.begin_trx().unwrap();
        let row_id = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &current_key,
            reader.sts(),
        )
        .await;
        reader.commit().await.unwrap();

        let index = bound_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            current_key.index_no,
        );
        let _ = index
            .insert_if_not_exists(
                session.pool_guards().index_guard(),
                &stale_key.vals,
                row_id,
                false,
                MAX_SNAPSHOT_TS,
            )
            .await
            .unwrap();
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );
        let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
        let deleted = table_for_internal_assertion(&engine, table_id)
            .accessor_with_layout(&layout)
            .delete_index(
                &session.pool_guards(),
                &stale_key,
                row_id,
                true,
                MAX_SNAPSHOT_TS,
            )
            .await
            .unwrap();
        assert!(deleted);
        assert!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
                .is_none()
        );

        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );
        table_for_internal_assertion(&engine, table_id)
            .deletion_buffer()
            .put_committed(row_id, TrxID::new(100))
            .unwrap();
        let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
        let deleted = table_for_internal_assertion(&engine, table_id)
            .accessor_with_layout(&layout)
            .delete_index(
                &session.pool_guards(),
                &current_key,
                row_id,
                true,
                TrxID::new(100),
            )
            .await
            .unwrap();
        assert!(!deleted);
        assert!(matches!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some((actual_row_id, true)) if actual_row_id == row_id
        ));
    });
}

#[test]
fn test_lwc_non_unique_index_purge_compares_persisted_key_when_marker_is_not_purgeable() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine =
            evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique").await;
        let table_id = create_non_unique_name_table_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "current").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let pk = single_key(0i32);
        let current_key = name_key("current");
        let stale_key = name_key("stale");
        let reader = session.begin_trx().unwrap();
        let row_id = assert_row_in_lwc(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &pk,
            reader.sts(),
        )
        .await;
        reader.commit().await.unwrap();

        let index = bound_non_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            current_key.index_no,
        );
        let _ = index
            .insert_if_not_exists(
                session.pool_guards().index_guard(),
                &stale_key.vals,
                row_id,
                false,
                MAX_SNAPSHOT_TS,
            )
            .await
            .unwrap();
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );
        let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
        let deleted = table_for_internal_assertion(&engine, table_id)
            .accessor_with_layout(&layout)
            .delete_index(
                &session.pool_guards(),
                &stale_key,
                row_id,
                false,
                MAX_SNAPSHOT_TS,
            )
            .await
            .unwrap();
        assert!(deleted);
        assert!(
            index
                .lookup_unique(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
                .is_none()
        );

        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );
        table_for_internal_assertion(&engine, table_id)
            .deletion_buffer()
            .put_committed(row_id, TrxID::new(200))
            .unwrap();
        let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
        let deleted = table_for_internal_assertion(&engine, table_id)
            .accessor_with_layout(&layout)
            .delete_index(
                &session.pool_guards(),
                &current_key,
                row_id,
                false,
                TrxID::new(200),
            )
            .await
            .unwrap();
        assert!(!deleted);
        assert!(matches!(
            index
                .lookup_unique(
                    session.pool_guards().index_guard(),
                    &current_key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some(false)
        ));
    });
}

#[test]
fn test_index_purge_removes_delete_marked_unique_entry_when_row_is_not_found() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let session = engine.new_session().unwrap();
        let key = single_key(9999i32);
        let row_id = 9999;
        let index = bound_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            key.index_no,
        );
        let _ = index
            .insert_if_not_exists(
                session.pool_guards().index_guard(),
                &key.vals,
                RowID::new(row_id),
                false,
                MAX_SNAPSHOT_TS,
            )
            .await
            .unwrap();
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &key.vals,
                    RowID::new(row_id),
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );

        let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();

        let deleted = table_for_internal_assertion(&engine, table_id)
            .accessor_with_layout(&layout)
            .delete_index(
                &session.pool_guards(),
                &key,
                RowID::new(row_id),
                true,
                MAX_SNAPSHOT_TS,
            )
            .await
            .unwrap();
        assert!(deleted);
        assert!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn test_unique_insert_rollback_restores_deleted_owner_even_when_row_missing() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let key = single_key(10_001i32);
        let stale_row_id = 10_001;

        assert!(matches!(
            table_for_internal_assertion(&engine, table_id)
                .find_row(&session.pool_guards(), RowID::new(stale_row_id))
                .await
                .unwrap(),
            RowLocation::NotFound
        ));

        let index = bound_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            key.index_no,
        );
        assert!(
            index
                .insert_if_not_exists(
                    session.pool_guards().index_guard(),
                    &key.vals,
                    RowID::new(stale_row_id),
                    false,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &key.vals,
                    RowID::new(stale_row_id),
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );
        assert_unique_index_entry(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &key,
            MAX_SNAPSHOT_TS,
            RowID::new(stale_row_id),
            true,
        )
        .await;

        let mut trx = session.begin_trx().unwrap();
        let res: Result<()> = trx
            .exec(async |stmt| {
                let new_row_id = unwrap_insert_result(
                    stmt_insert_row_by_id(
                        stmt,
                        table_id,
                        vec![Val::from(10_001i32), Val::from("reborn")],
                    )
                    .await,
                );
                assert_ne!(new_row_id, RowID::new(stale_row_id));
                assert_unique_index_entry(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &key,
                    stmt.runtime().sts(),
                    new_row_id,
                    false,
                )
                .await;
                Err(Report::new(OperationError::NotSupported).into())
            })
            .await;
        assert_eq!(
            res.unwrap_err().operation_error(),
            Some(OperationError::NotSupported)
        );
        trx.rollback().await.unwrap();

        assert_unique_index_entry(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &key,
            MAX_SNAPSHOT_TS,
            RowID::new(stale_row_id),
            true,
        )
        .await;
        expect_select_not_found_committed(table_id, &mut session, &key).await;
        expect_insert_committed(
            table_id,
            &mut session,
            vec![Val::from(10_001i32), Val::from("reclaimed")],
        )
        .await;
        expect_select_committed(table_id, &mut session, &key, |vals| {
            assert_eq!(vals, vec![Val::from(10_001i32), Val::from("reclaimed")]);
        })
        .await;
    });
}

#[test]
fn test_unique_insert_rollback_restores_delete_marked_stale_hot_owner() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let live_key = single_key(1i32);
        let stale_key = single_key(2i32);
        expect_insert_committed(
            table_id,
            &mut session,
            vec![Val::from(1i32), Val::from("one")],
        )
        .await;

        let reader = session.begin_trx().unwrap();
        let old_row_id = bound_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            live_key.index_no,
        )
        .lookup(
            session.pool_guards().index_guard(),
            &live_key.vals,
            reader.sts(),
        )
        .await
        .unwrap()
        .unwrap()
        .0;
        reader.commit().await.unwrap();
        assert!(matches!(
            table_for_internal_assertion(&engine, table_id)
                .find_row(&session.pool_guards(), old_row_id)
                .await
                .unwrap(),
            RowLocation::RowPage(_)
        ));

        let index = bound_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            stale_key.index_no,
        );
        assert!(
            index
                .insert_if_not_exists(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    old_row_id,
                    false,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .mask_as_deleted(
                    session.pool_guards().index_guard(),
                    &stale_key.vals,
                    old_row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );

        let mut trx = session.begin_trx().unwrap();
        let res: Result<()> = trx
            .exec(async |stmt| {
                let new_row_id = unwrap_insert_result(
                    stmt_insert_row_by_id(stmt, table_id, vec![Val::from(2i32), Val::from("two")])
                        .await,
                );
                assert_ne!(new_row_id, old_row_id);
                assert_unique_index_entry(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &stale_key,
                    stmt.runtime().sts(),
                    new_row_id,
                    false,
                )
                .await;
                Err(Report::new(OperationError::NotSupported).into())
            })
            .await;
        assert_eq!(
            res.unwrap_err().operation_error(),
            Some(OperationError::NotSupported)
        );
        trx.rollback().await.unwrap();

        assert_unique_index_entry(
            &table_for_internal_assertion(&engine, table_id),
            &session.pool_guards(),
            &stale_key,
            MAX_SNAPSHOT_TS,
            old_row_id,
            true,
        )
        .await;
        expect_select_committed(table_id, &mut session, &live_key, |vals| {
            assert_eq!(vals, vec![Val::from(1i32), Val::from("one")]);
        })
        .await;
        expect_select_not_found_committed(table_id, &mut session, &stale_key).await;
        expect_insert_committed(
            table_id,
            &mut session,
            vec![Val::from(2i32), Val::from("two-final")],
        )
        .await;
        expect_select_committed(table_id, &mut session, &stale_key, |vals| {
            assert_eq!(vals, vec![Val::from(2i32), Val::from("two-final")]);
        })
        .await;
    });
}

#[test]
fn test_checkpoint_fails_when_eligible_delete_marker_has_no_column_index() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 4, "name").await;
        delete_key_range_and_wait_gc_cutoff(table_id, &mut session, 0, 4).await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

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
        wait_gc_cutoff_after(&session, marker_ts).await;

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
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(0i32);
        let reader = session.begin_trx().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let row_id = assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
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
        wait_gc_cutoff_after(&session, marker_ts).await;

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
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(0i32);
        let reader = session.begin_trx().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let row_id = assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
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
        wait_gc_cutoff_after(&session, root_before.deletion_cutoff_ts).await;

        checkpoint_published(table_id, &mut session).await;
        assert!(
            table.file().active_root_unchecked().deletion_cutoff_ts
                > root_before.deletion_cutoff_ts
        );
    });
}

#[test]
fn test_recover_cold_delete_rejects_already_deleted_with_different_cts() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(6i32);
        let reader = session.begin_trx().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let row_id = assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
        reader.commit().await.unwrap();

        let active_root = table.file().active_root_unchecked().clone();
        assert!(row_id < active_root.pivot_row_id);
        let cts = active_root.deletion_cutoff_ts;
        table
            .recover_row_delete(&session.pool_guards(), PageID::from(0u64), row_id, cts)
            .await
            .unwrap();
        table
            .recover_row_delete(&session.pool_guards(), PageID::from(0u64), row_id, cts)
            .await
            .unwrap();

        let err = table
            .recover_row_delete(&session.pool_guards(), PageID::from(0u64), row_id, cts + 1)
            .await
            .unwrap_err();
        assert_eq!(
            err.report().downcast_ref::<DataIntegrityError>().copied(),
            Some(DataIntegrityError::InvalidRootInvariant)
        );
    });
}

#[test]
fn test_recover_row_page_reports_invalid_replay_state() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let session = engine.new_session().unwrap();
        let metadata = table_for_internal_assertion(&engine, table_id).metadata();
        let mut page_guard = table_for_internal_assertion(&engine, table_id)
            .mem
            .get_insert_page_exclusive(&session.pool_guards(), 2, None)
            .await
            .unwrap();
        let row_id = page_guard.page().header.start_row_id;
        let assert_invalid_root = |err: Error, reason: &str| {
            let report = format!("{err:?}");
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::InvalidRootInvariant),
                "{report}"
            );
            assert!(report.contains(reason), "{report}");
            assert!(report.contains("recover row"), "{report}");
        };

        let err = table_for_internal_assertion(&engine, table_id)
            .recover_row_insert_to_page(
                metadata.as_ref(),
                &mut page_guard,
                row_id,
                &[Val::from(1i32), Val::from("name")],
                TrxID::new(10),
            )
            .unwrap_err();
        assert_invalid_root(err, "missing recover map");

        page_guard.bf_mut().init_recover_map(TrxID::new(10));
        let err = table_for_internal_assertion(&engine, table_id)
            .recover_row_insert_to_page(
                metadata.as_ref(),
                &mut page_guard,
                row_id,
                &[Val::from(1i32), Val::from(vec![b'x'; PAGE_SIZE - 1])],
                TrxID::new(11),
            )
            .unwrap_err();
        assert_invalid_root(err, "insufficient row page space");

        table_for_internal_assertion(&engine, table_id)
            .recover_row_insert_to_page(
                metadata.as_ref(),
                &mut page_guard,
                row_id,
                &[Val::from(1i32), Val::from("name")],
                TrxID::new(12),
            )
            .unwrap();
        assert_eq!(page_guard.page().header.approx_non_deleted(), 1);

        let err = table_for_internal_assertion(&engine, table_id)
            .recover_row_insert_to_page(
                metadata.as_ref(),
                &mut page_guard,
                row_id,
                &[Val::from(2i32), Val::from("other")],
                TrxID::new(13),
            )
            .unwrap_err();
        assert_invalid_root(err, "row slot is not vacant");

        let err = table_for_internal_assertion(&engine, table_id)
            .recover_row_update_to_page(
                metadata.as_ref(),
                &mut page_guard,
                row_id + 1,
                &[UpdateCol {
                    idx: 1,
                    val: Val::from("new"),
                }],
                TrxID::new(14),
            )
            .unwrap_err();
        assert_invalid_root(err, "row is deleted");

        table_for_internal_assertion(&engine, table_id)
            .recover_row_delete_to_page(&mut page_guard, row_id, TrxID::new(15))
            .unwrap();
        assert_eq!(page_guard.page().header.approx_non_deleted(), 0);

        let err = table_for_internal_assertion(&engine, table_id)
            .recover_row_delete_to_page(&mut page_guard, row_id, TrxID::new(16))
            .unwrap_err();
        assert_invalid_root(err, "row is already deleted");

        let err = table_for_internal_assertion(&engine, table_id)
            .recover_row_delete_to_page(&mut page_guard, row_id + 2, TrxID::new(17))
            .unwrap_err();
        assert_invalid_root(err, "row id outside page range");
    });
}

#[test]
fn test_checkpoint_skips_cold_delete_markers_at_or_after_cutoff() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

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
        checkpoint_published(table_id, &mut checkpoint_session).await;

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
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key1 = single_key(6i32);
        let reader = session.begin_trx().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let row_id1 = assert_row_in_lwc(&table, &session.pool_guards(), &key1, reader.sts()).await;
        reader.commit().await.unwrap();

        expect_delete_committed(table_id, &mut session, &key1).await;
        let marker1 = table.deletion_buffer().get(row_id1).unwrap();
        let marker1_ts = delete_marker_ts(marker1);
        wait_gc_cutoff_after(&session, marker1_ts).await;
        checkpoint_published(table_id, &mut session).await;

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
        wait_gc_cutoff_after(&session, marker2_ts).await;

        let table_file_path = engine.table_fs.user_table_file_path(table_id);
        corrupt_leaf_delete_codec(table_file_path, entry.leaf_block_id, 0);
        let _ = table
            .disk_pool()
            .invalidate_block(table.file().sparse_file().file_id(), entry.leaf_block_id);

        let err = session.checkpoint_table(table_id).await.unwrap_err();
        assert_table_data_integrity(
            err,
            "column-block-index",
            entry.leaf_block_id,
            DataIntegrityError::InvalidPayload,
        );
    });
}

#[test]
fn test_checkpoint_fails_on_short_v2_delete_section_header() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 10, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key1 = single_key(6i32);
        let reader = session.begin_trx().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let row_id1 = assert_row_in_lwc(&table, &session.pool_guards(), &key1, reader.sts()).await;
        reader.commit().await.unwrap();

        expect_delete_committed(table_id, &mut session, &key1).await;
        let marker1 = table.deletion_buffer().get(row_id1).unwrap();
        let marker1_ts = delete_marker_ts(marker1);
        wait_gc_cutoff_after(&session, marker1_ts).await;
        checkpoint_published(table_id, &mut session).await;

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
        wait_gc_cutoff_after(&session, marker2_ts).await;

        let table_file_path = engine.table_fs.user_table_file_path(table_id);
        corrupt_leaf_short_delete_section_header(table_file_path, entry.leaf_block_id, 0);
        let _ = table
            .disk_pool()
            .invalidate_block(table.file().sparse_file().file_id(), entry.leaf_block_id);

        let err = session.checkpoint_table(table_id).await.unwrap_err();
        assert_table_data_integrity(
            err,
            "column-block-index",
            entry.leaf_block_id,
            DataIntegrityError::InvalidPayload,
        );
    });
}

#[test]
fn test_row_page_transition_retries_update_delete() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        {
            let insert = vec![Val::from(1i32), Val::from("hello")];
            let mut trx = session.begin_trx().unwrap();
            trx = expect_trx_insert(table_id, trx, insert).await;
            trx.commit().await.unwrap();
        }
        let key = single_key(1i32);
        let mut trx = session.begin_trx().unwrap();
        let index = bound_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            key.index_no,
        );
        let (row_id, _) = index
            .lookup(session.pool_guards().index_guard(), &key.vals, trx.sts())
            .await
            .unwrap()
            .unwrap();
        let page_id = match table_for_internal_assertion(&engine, table_id)
            .find_row(&session.pool_guards(), row_id)
            .await
            .unwrap()
        {
            RowLocation::RowPage(page_id) => page_id,
            RowLocation::NotFound => panic!("row should exist"),
            RowLocation::LwcBlock { .. } => unreachable!("lwc block"),
        };
        let page_guard = engine
            .mem_pool
            .get_page::<RowPage>(
                session.pool_guards().mem_guard(),
                page_id,
                LatchFallbackMode::Shared,
            )
            .await
            .expect("buffer-pool read failed in test")
            .lock_shared_async()
            .await
            .unwrap();
        let (ctx, _) = page_guard.ctx_and_page();
        let row_ver = ctx.row_ver().unwrap();
        row_ver.set_frozen();
        row_ver.set_transition();

        let insert_page_guard = engine
            .mem_pool
            .get_page::<RowPage>(
                session.pool_guards().mem_guard(),
                page_id,
                LatchFallbackMode::Shared,
            )
            .await
            .expect("buffer-pool read failed in test")
            .lock_shared_async()
            .await
            .unwrap();
        let insert = vec![Val::from(2i32), Val::from("insert")];
        let res: Result<()> = trx
            .exec(async |stmt| {
                stmt.acquire_table_write_locks(table_id).await?;
                let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
                let insert_res = table_for_internal_assertion(&engine, table_id)
                    .accessor_with_layout(&layout)
                    .insert_row_to_page(
                        rt,
                        effects,
                        insert_page_guard,
                        insert,
                        RowUndoKind::Insert,
                        vec![],
                    );
                assert!(matches!(
                    insert_res,
                    InsertRowIntoPage::NoSpaceOrFrozen(_, _, _)
                ));

                let update = vec![UpdateCol {
                    idx: 1,
                    val: Val::from("world"),
                }];
                let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
                let res = table_for_internal_assertion(&engine, table_id)
                    .accessor_with_layout(&layout)
                    .update_row_inplace(rt, effects, page_guard, &key, row_id, update)
                    .await;
                assert!(matches!(res, UpdateRowInplace::RetryInTransition));
                Err(Report::new(OperationError::NotSupported).into())
            })
            .await;
        assert_eq!(
            res.unwrap_err().operation_error(),
            Some(OperationError::NotSupported)
        );
        trx.rollback().await.unwrap();

        let mut trx = session.begin_trx().unwrap();
        let page_guard = engine
            .mem_pool
            .get_page::<RowPage>(
                session.pool_guards().mem_guard(),
                page_id,
                LatchFallbackMode::Shared,
            )
            .await
            .expect("buffer-pool read failed in test")
            .lock_shared_async()
            .await
            .unwrap();
        let res: Result<()> = trx
            .exec(async |stmt| {
                stmt.acquire_table_write_locks(table_id).await?;
                let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
                let res = table_for_internal_assertion(&engine, table_id)
                    .accessor_with_layout(&layout)
                    .delete_row_internal(rt, effects, page_guard, row_id, &key, false)
                    .await;
                assert!(matches!(res, DeleteInternal::RetryInTransition));
                Err(Report::new(OperationError::NotSupported).into())
            })
            .await;
        assert_eq!(
            res.unwrap_err().operation_error(),
            Some(OperationError::NotSupported)
        );
        trx.rollback().await.unwrap();
    });
}

#[test]
fn test_mvcc_rollback_insert_normal() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        {
            let mut session = engine.new_session().unwrap();
            // insert 1 row
            let mut trx = session.begin_trx().unwrap();
            let insert = vec![Val::from(1i32), Val::from("hello")];
            trx = expect_trx_insert(table_id, trx, insert).await;
            // explicit rollback
            trx.rollback().await.unwrap();

            // select 1 row
            let key = single_key(1i32);
            _ = expect_select_not_found_committed(table_id, &mut session, &key).await;
        }
    });
}

#[test]
fn test_mvcc_insert_link_unique_index() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        {
            let mut session = engine.new_session().unwrap();
            // insert 1 row
            let insert = vec![Val::from(1i32), Val::from("hello")];
            expect_insert_committed(table_id, &mut session, insert).await;

            // we must hold a transaction before the deletion,
            // to prevent index GC.
            let trx_to_prevent_gc = engine.new_session().unwrap().begin_trx().unwrap();
            // delete it
            let key = single_key(1i32);
            expect_delete_committed(table_id, &mut session, &key).await;

            // insert again, trigger insert+link
            let insert = vec![Val::from(1i32), Val::from("world")];
            expect_insert_committed(table_id, &mut session, insert).await;

            trx_to_prevent_gc.rollback().await.unwrap();

            // select 1 row
            let key = single_key(1i32);
            _ = expect_select_committed(table_id, &mut session, &key, |vals| {
                assert!(vals[0] == Val::from(1i32));
                assert!(vals[1] == Val::from("world"));
            })
            .await;
        }
    });
}

#[test]
fn test_mvcc_rollback_insert_link_unique_index() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        {
            let mut session = engine.new_session().unwrap();
            // insert 1 row
            let insert = vec![Val::from(1i32), Val::from("hello")];
            expect_insert_committed(table_id, &mut session, insert).await;

            // delete it
            let key = single_key(1i32);
            expect_delete_committed(table_id, &mut session, &key).await;

            // insert again, trigger insert+link
            let insert = vec![Val::from(1i32), Val::from("world")];
            let mut trx = session.begin_trx().unwrap();
            trx = expect_trx_insert(table_id, trx, insert).await;
            // explicit rollback
            trx.rollback().await.unwrap();

            // select 1 row
            let key = single_key(1i32);
            _ = expect_select_not_found_committed(table_id, &mut session, &key).await;
        }
    });
}

#[test]
fn test_mvcc_insert_link_update() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        {
            let mut session = engine.new_session().unwrap();
            // insert 1 row: v1=1, v2=hello
            let insert = vec![Val::from(1i32), Val::from("hello")];
            expect_insert_committed(table_id, &mut session, insert).await;

            // open one session and trnasaction to see this row
            let mut sess1 = engine.new_session().unwrap();
            let mut trx1 = sess1.begin_trx().unwrap();

            // update it: v1=2, v2=world
            let key = single_key(1i32);
            let update = vec![
                UpdateCol {
                    idx: 0,
                    val: Val::from(2i32),
                },
                UpdateCol {
                    idx: 1,
                    val: Val::from("world"),
                },
            ];
            expect_update_committed(table_id, &mut session, &key, update).await;

            // open session and transaction to see row 2
            let mut sess2 = engine.new_session().unwrap();
            let mut trx2 = sess2.begin_trx().unwrap();

            // insert again, trigger insert+link
            let insert = vec![Val::from(1i32), Val::from("rust")];
            expect_insert_committed(table_id, &mut session, insert).await;

            // use transaction 1 to see version 1.
            let key = single_key(1i32);
            trx1 = expect_trx_select(table_id, trx1, &key, |vals| {
                assert!(vals[0] == Val::from(1i32));
                assert!(vals[1] == Val::from("hello"));
            })
            .await;
            _ = trx1.commit().await.unwrap();

            // use transaction 2 to see version 2.
            let key = single_key(2i32);
            trx2 = expect_trx_select(table_id, trx2, &key, |vals| {
                assert!(vals[0] == Val::from(2i32));
                assert!(vals[1] == Val::from("world"));
            })
            .await;
            _ = trx2.commit().await.unwrap();

            // use new transaction to see version 3.
            let key = single_key(1i32);
            _ = expect_select_committed(table_id, &mut session, &key, |vals| {
                assert!(vals[0] == Val::from(1i32));
                assert!(vals[1] == Val::from("rust"));
            })
            .await;
        }
    });
}

#[test]
fn test_mvcc_update_link_insert() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        {
            let mut session = engine.new_session().unwrap();
            // insert 1 row: v1=1, v2=hello
            let insert = vec![Val::from(1i32), Val::from("hello")];
            expect_insert_committed(table_id, &mut session, insert).await;
            println!("debug-only insert finish");

            // open one session and trnasaction to see this row
            let mut sess1 = engine.new_session().unwrap();
            let mut trx1 = sess1.begin_trx().unwrap();

            // update it: v1=2, v2=world
            let key = single_key(1i32);
            let update = vec![
                UpdateCol {
                    idx: 0,
                    val: Val::from(2i32),
                },
                UpdateCol {
                    idx: 1,
                    val: Val::from("world"),
                },
            ];
            expect_update_committed(table_id, &mut session, &key, update).await;
            println!("debug-only update finish");

            // open session and transaction to see row 2
            let mut sess2 = engine.new_session().unwrap();
            let mut trx2 = sess2.begin_trx().unwrap();

            // insert v1=5, v2=rust
            let insert = vec![Val::from(5i32), Val::from("rust")];
            expect_insert_committed(table_id, &mut session, insert).await;
            println!("debug-only insert2 finish");

            // update it: v1=1, v2=c++, trigger update+link
            let key = single_key(5i32);
            let update = vec![
                UpdateCol {
                    idx: 0,
                    val: Val::from(1i32),
                },
                UpdateCol {
                    idx: 1,
                    val: Val::from("c++"),
                },
            ];
            expect_update_committed(table_id, &mut session, &key, update).await;
            println!("debug-only update2 finish");

            // use transaction 1 to see version 1.
            let key = single_key(1i32);
            trx1 = expect_trx_select(table_id, trx1, &key, |vals| {
                assert!(vals[0] == Val::from(1i32));
                assert!(vals[1] == Val::from("hello"));
            })
            .await;
            _ = trx1.commit().await;

            // use transaction 2 to see version 2.
            let key = single_key(2i32);
            trx2 = expect_trx_select(table_id, trx2, &key, |vals| {
                assert!(vals[0] == Val::from(2i32));
                assert!(vals[1] == Val::from("world"));
            })
            .await;
            _ = trx2.commit().await;

            // use new transaction to see version 3.
            let key = single_key(1i32);
            _ = expect_select_committed(table_id, &mut session, &key, |vals| {
                assert!(vals[0] == Val::from(1i32));
                assert!(vals[1] == Val::from("c++"));
            })
        }
    });
}

#[test]
fn test_mvcc_multi_update() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        {
            let mut session = engine.new_session().unwrap();
            // insert: v1
            let insert = vec![Val::from(1i32), Val::from("hello")];
            expect_insert_committed(table_id, &mut session, insert).await;

            // transaction to see version 1
            let mut sess1 = engine.new_session().unwrap();
            let mut trx1 = sess1.begin_trx().unwrap();

            let mut trx = session.begin_trx().unwrap();
            // update 1: v2
            let key = single_key(1i32);
            let update = vec![UpdateCol {
                idx: 1,
                val: Val::from("rust"),
            }];
            trx = expect_trx_update(table_id, trx, &key, update).await;
            // update 2: v3
            let key = single_key(1i32);
            let update = vec![
                UpdateCol {
                    idx: 0,
                    val: Val::from(2i32),
                },
                UpdateCol {
                    idx: 1,
                    val: Val::from("world"),
                },
            ];
            trx = expect_trx_update(table_id, trx, &key, update).await;
            // within transaction, query row
            // v2 not found
            let key = single_key(1i32);
            trx = expect_trx_select_not_found(table_id, trx, &key).await;
            // v3 found
            let key = single_key(2i32);
            trx = expect_trx_select(table_id, trx, &key, |vals| {
                assert!(vals[0] == Val::from(2i32));
                assert!(vals[1] == Val::from("world"));
            })
            .await;
            trx.commit().await.unwrap();

            //v1 found
            let key = single_key(1i32);
            trx1 = expect_trx_select(table_id, trx1, &key, |vals| {
                assert!(vals[0] == Val::from(1i32));
                assert!(vals[1] == Val::from("hello"));
            })
            .await;
            trx1.commit().await.unwrap();
        }
    });
}

#[test]
fn test_string_non_index_updates() {
    smol::block_on(async {
        const COUNT: usize = 100;
        const SIZE: usize = 500;
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        {
            let mut session = engine.new_session().unwrap();
            let value = vec![1u8; SIZE];
            let insert = vec![Val::from(1i32), Val::from(&[])];
            expect_insert_committed(table_id, &mut session, insert).await;
            let key = SelectKey::new(0, vec![Val::from(1i32)]);
            for i in SIZE - COUNT..SIZE {
                expect_update_committed(
                    table_id,
                    &mut session,
                    &key,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from(&value[..i]),
                    }],
                )
                .await;
            }
        }
    });
}

#[test]
fn test_string_index_updates() {
    use crate::catalog::tests::table3;
    smol::block_on(async {
        const COUNT: usize = 100;
        const SIZE: usize = 500;
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let _ = create_table2_for_test(&engine).await;
        {
            let table_id = table3(&engine).await;
            let mut session = engine.new_session().unwrap();
            let s: String = std::iter::repeat_n('0', SIZE).collect();
            // insert single row.
            {
                let insert = vec![Val::from(&s[..0])];
                let mut trx = session.begin_trx().unwrap();
                let res = trx_insert_row_by_id(&mut trx, table_id, insert).await;
                assert!(res.is_ok());
                trx.commit().await.unwrap();
            }
            // perform updates.
            for i in 0..COUNT {
                let key = SelectKey::new(0, vec![Val::from(&s[..i])]);
                let update = vec![UpdateCol {
                    idx: 0,
                    val: Val::from(&s[..i + 1]),
                }];
                let mut trx = session.begin_trx().unwrap();
                let res = trx_update_row_by_id(&mut trx, table_id, &key, update).await;
                assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
                trx.commit().await.unwrap();
            }
        }
    });
}

#[test]
fn test_mvcc_out_of_place_update() {
    use crate::catalog::tests::table3;
    smol::block_on(async {
        const COUNT: usize = 60;
        const DELTA: usize = 5;
        const BASE: usize = 1000;
        const SIZE: usize = 2000;
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let _ = create_table2_for_test(&engine).await;
        {
            let table_id = table3(&engine).await;
            let mut session = engine.new_session().unwrap();
            let s: String = std::iter::repeat_n('0', SIZE).collect();
            // insert 60 rows
            for i in 0usize..COUNT {
                let insert = vec![Val::from(&s[..BASE + i])];
                let mut trx = session.begin_trx().unwrap();
                let res = trx_insert_row_by_id(&mut trx, table_id, insert).await;
                assert!(res.is_ok());
                trx.commit().await.unwrap();
            }
            // perform updates to trigger out-of-place update.
            // try to update k=s[..BASE+DELTA] to s[..BASE+COUNT+DELTA]
            for i in 0..DELTA {
                let key = SelectKey::new(0, vec![Val::from(&s[..BASE + i])]);
                let update = vec![UpdateCol {
                    idx: 0,
                    val: Val::from(&s[..BASE + COUNT + i]),
                }];
                let mut trx = session.begin_trx().unwrap();
                let res = trx_update_row_by_id(&mut trx, table_id, &key, update).await;
                assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
                trx.commit().await.unwrap();
            }
        }
    });
}

#[test]
fn test_evict_pool_insert_full() {
    smol::block_on(async {
        const SIZE: i32 = 800;

        // in-mem ~1000 pages, on-disk 2000 pages.
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        {
            let mut session = engine.new_session().unwrap();
            // insert 1000 rows
            let mut trx = session.begin_trx().unwrap();
            for i in 0..SIZE {
                // make string 1KB long, so a page can only hold about 60 rows.
                // if page is full, 17 pages are required.
                // if page is half full, 35 pages are required.
                let s: String = (0..1000).map(|_| 'a').collect();
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = expect_trx_insert(table_id, trx, insert).await;
            }
            let _ = trx.commit().await.unwrap();
        }
    });
}

#[test]
fn test_mem_scan_uncommitted() {
    smol::block_on(async {
        const SIZE: i32 = 10000;

        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;

        let mut session = engine.new_session().unwrap();
        {
            let mut trx = session.begin_trx().unwrap();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = expect_trx_insert(table_id, trx, insert).await;
            }
            _ = trx.commit().await.unwrap();
        }
        {
            let mut res_len = 0usize;
            let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
            table_for_internal_assertion(&engine, table_id)
                .accessor_with_layout(&layout)
                .mem_scan_uncommitted(&session.pool_guards(), |_metadata, _row| {
                    res_len += 1;
                    true
                })
                .await
                .unwrap();
            println!("res.len()={}", res_len);
            assert!(res_len == SIZE as usize);
        }
    });
}

#[test]
fn test_mem_scan_uncommitted_returns_error_without_meta_guard() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
        let empty_guards = PoolGuards::builder().build();

        let err = table_for_internal_assertion(&engine, table_id)
            .accessor_with_layout(&layout)
            .mem_scan_uncommitted(&empty_guards, |_metadata, _row| true)
            .await
            .unwrap_err();

        assert_eq!(
            err.downcast_ref::<InternalError>().copied(),
            Some(InternalError::Generic)
        );
    });
}

#[test]
fn test_statement_read_takes_metadata_lock_only() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 1, "name").await;

        let stmt_owner = Cell::new(None);
        let mut trx = session.begin_trx().unwrap();
        trx.exec(async |stmt| {
            let owner = stmt_tests::lock_owner(stmt);
            stmt_owner.set(Some(owner));
            let selected = stmt
                .table_lookup_unique_mvcc(table_id, &single_key(0i32), &[0, 1])
                .await?;
            assert!(selected.is_found());
            let repeated = stmt
                .table_lookup_unique_mvcc(table_id, &single_key(0i32), &[0, 1])
                .await?;
            assert!(repeated.is_found());
            assert_eq!(lock_entry_count(&engine, owner), 1);
            assert!(has_lock_entry(
                &engine,
                owner,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));
            assert!(!has_lock_resource(
                &engine,
                owner,
                LockResource::TableData(table_id),
            ));
            Ok(())
        })
        .await
        .unwrap();

        let owner = stmt_owner.get().unwrap();
        assert_eq!(lock_entry_count(&engine, owner), 0);
        trx.commit().await.unwrap();
    });
}

#[test]
fn test_statement_write_locks_are_transaction_owned_and_cached() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let mut trx = session.begin_trx().unwrap();
        let owner = trx_tests::lock_owner(&trx).unwrap();

        trx.exec(async |stmt| {
            stmt.table_insert_mvcc(table_id, vec![Val::from(10i32), Val::from("a")])
                .await?;
            Ok(())
        })
        .await
        .unwrap();
        assert!(has_lock_entry(
            &engine,
            owner,
            LockResource::TableMetadata(table_id),
            LockMode::Shared,
            LockDebugEntryState::Granted,
        ));
        assert!(has_lock_entry(
            &engine,
            owner,
            LockResource::TableData(table_id),
            LockMode::IntentExclusive,
            LockDebugEntryState::Granted,
        ));
        assert_eq!(lock_entry_count(&engine, owner), 2);

        trx.exec(async |stmt| {
            stmt.table_insert_mvcc(table_id, vec![Val::from(11i32), Val::from("b")])
                .await?;
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(lock_entry_count(&engine, owner), 2);

        trx.rollback().await.unwrap();
        assert_eq!(lock_entry_count(&engine, owner), 0);
    });
}

#[test]
fn test_create_table_waits_on_catalog_namespace_lock() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let _ = create_table2_for_test(&engine).await;
        let blocker = LockOwner::Session(SessionID::new(91_400));
        assert!(
            try_acquire(
                engine.lock_manager(),
                LockResource::CatalogNamespace,
                LockMode::Exclusive,
                blocker,
            )
            .unwrap()
        );

        let mut session = engine.new_session().unwrap();
        let waiting_owner = LockOwner::Session(session.id());
        let create_task = smol::spawn(async move {
            session
                .create_table(
                    TableSpec::new(vec![ColumnSpec::new(
                        "id",
                        crate::value::ValKind::I32,
                        ColumnAttributes::empty(),
                    )]),
                    vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
                )
                .await
        });

        wait_for_lock_entry(
            &engine,
            waiting_owner,
            LockResource::CatalogNamespace,
            LockMode::Exclusive,
            LockDebugEntryState::Waiting,
        )
        .await;

        assert_eq!(engine.lock_manager().release_owner(blocker), 1);
        let table_id = create_task.await.unwrap();
        assert!(engine.catalog().get_table(table_id).await.is_some());
        assert!(!has_lock_resource(
            &engine,
            waiting_owner,
            LockResource::CatalogNamespace,
        ));
    });
}

#[test]
fn test_create_table_rejects_invalid_metadata_before_file_creation() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = lightweight_test_engine_config(main_dir, "create_invalid_metadata")
            .build()
            .await
            .unwrap();
        let mut session = engine.new_session().unwrap();
        let table_id = engine.catalog().curr_next_table_id();
        let table_file_path = engine.table_fs.user_table_file_path(table_id);

        let err = session
            .create_table(
                TableSpec::new(vec![ColumnSpec::new(
                    "id",
                    ValKind::I32,
                    ColumnAttributes::empty(),
                )]),
                vec![IndexSpec::new(vec![], IndexAttributes::PK)],
            )
            .await
            .unwrap_err();

        assert_eq!(
            err.report().downcast_ref::<ConfigError>().copied(),
            Some(ConfigError::InvalidIndexSpec)
        );
        assert!(engine.catalog().get_table(table_id).await.is_none());
        assert!(!session.in_trx().unwrap());
        wait_path_exists(&table_file_path, false).await;
    });
}

#[test]
fn test_create_table_catalog_staging_failure_rolls_back_and_deletes_file() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = lightweight_test_engine_config(main_dir, "create_fail_catalog")
            .build()
            .await
            .unwrap();
        let mut session = engine.new_session().unwrap();
        let table_id = engine.catalog().curr_next_table_id();
        let table_file_path = engine.table_fs.user_table_file_path(table_id);
        let (table_spec, index_specs) = drop_table_test_spec();

        catalog_table_tests::set_create_table_failure(Some(
            CreateTableTestFailure::AfterCatalogStaged,
        ));
        let res = session.create_table(table_spec, index_specs).await;
        catalog_table_tests::set_create_table_failure(None);

        let err = res.unwrap_err();
        assert_eq!(
            err.report().downcast_ref::<InternalError>().copied(),
            Some(InternalError::InjectedTestFailure)
        );
        assert!(engine.catalog().get_table(table_id).await.is_none());
        assert!(!session.in_trx().unwrap());
        wait_path_exists(&table_file_path, false).await;
    });
}

#[test]
fn test_create_table_file_publish_failure_rolls_back_catalog_and_deletes_file() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = lightweight_test_engine_config(main_dir, "create_fail_publish")
            .build()
            .await
            .unwrap();
        let mut session = engine.new_session().unwrap();
        let table_id = engine.catalog().curr_next_table_id();
        let table_file_path = engine.table_fs.user_table_file_path(table_id);
        let hook = Arc::new(FailingFirstWriteHook::new(table_file_path.clone()));
        let _install = install_storage_backend_test_hook(hook.clone());
        let (table_spec, index_specs) = drop_table_test_spec();

        let err = session
            .create_table(table_spec, index_specs)
            .await
            .unwrap_err();

        assert!(err.is_kind(ErrorKind::Io), "{err:?}");
        assert!(hook.call_count() > 0);
        assert!(engine.catalog().get_table(table_id).await.is_none());
        assert!(!session.in_trx().unwrap());
        wait_path_exists(&table_file_path, false).await;
    });
}

#[test]
fn test_create_table_after_file_published_failure_rolls_back_catalog_and_deletes_file() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = lightweight_test_engine_config(main_dir, "create_fail_after_file")
            .build()
            .await
            .unwrap();
        let mut session = engine.new_session().unwrap();
        let table_id = engine.catalog().curr_next_table_id();
        let table_file_path = engine.table_fs.user_table_file_path(table_id);
        let (table_spec, index_specs) = drop_table_test_spec();

        catalog_table_tests::set_create_table_failure(Some(
            CreateTableTestFailure::AfterFilePublished,
        ));
        let res = session.create_table(table_spec, index_specs).await;
        catalog_table_tests::set_create_table_failure(None);

        let err = res.unwrap_err();
        assert_eq!(
            err.report().downcast_ref::<InternalError>().copied(),
            Some(InternalError::InjectedTestFailure)
        );
        assert!(engine.catalog().get_table(table_id).await.is_none());
        assert!(!session.in_trx().unwrap());
        wait_path_exists(&table_file_path, false).await;
    });
}

#[test]
fn test_create_table_runtime_failure_after_file_publish_rolls_back_and_deletes_file() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = lightweight_test_engine_config(main_dir, "create_fail_runtime")
            .build()
            .await
            .unwrap();
        let mut session = engine.new_session().unwrap();
        let table_id = engine.catalog().curr_next_table_id();
        let table_file_path = engine.table_fs.user_table_file_path(table_id);
        let (table_spec, index_specs) = drop_table_test_spec();

        catalog_table_tests::set_create_table_failure(Some(
            CreateTableTestFailure::AfterRuntimeBuilt,
        ));
        let res = session.create_table(table_spec, index_specs).await;
        catalog_table_tests::set_create_table_failure(None);

        let err = res.unwrap_err();
        assert_eq!(
            err.report().downcast_ref::<InternalError>().copied(),
            Some(InternalError::InjectedTestFailure)
        );
        assert!(engine.catalog().get_table(table_id).await.is_none());
        assert!(!session.in_trx().unwrap());
        wait_path_exists(&table_file_path, false).await;
    });
}

#[test]
fn test_create_table_catalog_commit_error_after_file_publish_poisons_and_keeps_file() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = lightweight_test_engine_config(main_dir, "create_fail_commit")
            .build()
            .await
            .unwrap();
        let mut session = engine.new_session().unwrap();
        let table_id = engine.catalog().curr_next_table_id();
        let table_file_path = engine.table_fs.user_table_file_path(table_id);
        let (table_spec, index_specs) = drop_table_test_spec();

        catalog_table_tests::set_create_table_failure(Some(
            CreateTableTestFailure::PoisonBeforeCatalogCommit,
        ));
        let res = session.create_table(table_spec, index_specs).await;
        catalog_table_tests::set_create_table_failure(None);

        let err = res.unwrap_err();
        assert_eq!(
            err.report().downcast_ref::<FatalError>().copied(),
            Some(FatalError::Poisoned)
        );
        assert!(
            engine
                .trx_sys
                .storage_poison_error()
                .as_ref()
                .is_some_and(|err| *err.current_context() == FatalError::Poisoned)
        );
        assert!(engine.catalog().get_table(table_id).await.is_none());
        assert!(!session.in_trx().unwrap());
        assert!(std::path::Path::new(&table_file_path).exists());
    });
}

#[test]
fn test_explicit_table_locks_reject_intent_modes() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();

        for mode in [LockMode::IntentShared, LockMode::IntentExclusive] {
            let err = session.lock_table(table_id, mode).await.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::InvalidLockMode));
        }

        let mut trx = session.begin_trx().unwrap();
        for mode in [LockMode::IntentShared, LockMode::IntentExclusive] {
            let err = trx.lock_table(table_id, mode).await.unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::InvalidLockMode));
        }
        trx.rollback().await.unwrap();
    });
}

#[test]
fn test_transaction_shared_table_lock_blocks_external_row_writer() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let mut trx = session.begin_trx().unwrap();
        let owner = trx_tests::lock_owner(&trx).unwrap();

        trx.lock_table(table_id, LockMode::Shared).await.unwrap();
        assert!(has_lock_entry(
            &engine,
            owner,
            LockResource::TableMetadata(table_id),
            LockMode::Shared,
            LockDebugEntryState::Granted,
        ));
        assert!(has_lock_entry(
            &engine,
            owner,
            LockResource::TableData(table_id),
            LockMode::Shared,
            LockDebugEntryState::Granted,
        ));

        let engine_ref = engine.new_ref().unwrap();
        let (owner_tx, owner_rx) = flume::bounded(1);
        let writer = smol::spawn(async move {
            let mut writer_session = engine_ref.new_session().unwrap();
            let mut writer_trx = writer_session.begin_trx().unwrap();
            owner_tx
                .send_async(trx_tests::lock_owner(&writer_trx).unwrap())
                .await
                .unwrap();
            trx_insert_row_by_id(
                &mut writer_trx,
                table_id,
                vec![Val::from(31_001i32), Val::from("blocked")],
            )
            .await?;
            writer_trx.commit().await?;
            Ok::<(), Error>(())
        });
        let writer_owner = owner_rx.recv_async().await.unwrap();
        wait_for_lock_entry(
            &engine,
            writer_owner,
            LockResource::TableData(table_id),
            LockMode::IntentExclusive,
            LockDebugEntryState::Waiting,
        )
        .await;

        trx.rollback().await.unwrap();
        writer.await.unwrap();
    });
}

#[test]
fn test_transaction_exclusive_table_lock_uses_cache_and_releases_on_commit() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let mut trx = session.begin_trx().unwrap();
        let owner = trx_tests::lock_owner(&trx).unwrap();

        trx.lock_table(table_id, LockMode::Exclusive).await.unwrap();
        trx.lock_table(table_id, LockMode::Shared).await.unwrap();
        trx.lock_table(table_id, LockMode::Exclusive).await.unwrap();

        assert_eq!(lock_entry_count(&engine, owner), 2);
        assert!(has_lock_entry(
            &engine,
            owner,
            LockResource::TableMetadata(table_id),
            LockMode::Shared,
            LockDebugEntryState::Granted,
        ));
        assert!(has_lock_entry(
            &engine,
            owner,
            LockResource::TableData(table_id),
            LockMode::Exclusive,
            LockDebugEntryState::Granted,
        ));

        assert_eq!(trx.commit().await.unwrap(), TrxID::new(0));
        assert_eq!(lock_entry_count(&engine, owner), 0);
    });
}

#[test]
fn test_session_shared_table_lock_allows_reads_but_rejects_same_session_writes() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut setup_session = engine.new_session().unwrap();
        insert_rows(table_id, &mut setup_session, 0, 1, "name").await;

        let mut session = engine.new_session().unwrap();
        session
            .lock_table(table_id, LockMode::Shared)
            .await
            .unwrap();

        let mut read_trx = session.begin_trx().unwrap();
        read_trx
            .exec(async |stmt| {
                let selected = stmt
                    .table_lookup_unique_mvcc(table_id, &single_key(0i32), &[0, 1])
                    .await?;
                assert!(selected.is_found());
                Ok(())
            })
            .await
            .unwrap();
        read_trx.commit().await.unwrap();

        let mut write_trx = session.begin_trx().unwrap();
        let err = trx_insert_row_by_id(
            &mut write_trx,
            table_id,
            vec![Val::from(31_101i32), Val::from("same-session-s")],
        )
        .await
        .unwrap_err();
        assert_eq!(
            err.operation_error(),
            Some(OperationError::LockOwnerGroupConflict)
        );
        assert!(!has_lock_entry(
            &engine,
            trx_tests::lock_owner(&write_trx).unwrap(),
            LockResource::TableData(table_id),
            LockMode::IntentExclusive,
            LockDebugEntryState::Waiting,
        ));
        write_trx.rollback().await.unwrap();

        session.unlock_table(table_id).unwrap();
    });
}

#[test]
fn test_session_table_lock_failure_releases_fresh_metadata() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let session_owner = LockOwner::Session(session.id());
        let mut trx = session.begin_trx().unwrap();

        trx_insert_row_by_id(
            &mut trx,
            table_id,
            vec![Val::from(31_301i32), Val::from("same-session-ix")],
        )
        .await
        .unwrap();

        let err = session
            .lock_table(table_id, LockMode::Shared)
            .await
            .unwrap_err();
        assert_eq!(
            err.operation_error(),
            Some(OperationError::LockOwnerGroupConflict)
        );
        assert!(!has_lock_resource(
            &engine,
            session_owner,
            LockResource::TableMetadata(table_id),
        ));
        assert!(!has_lock_resource(
            &engine,
            session_owner,
            LockResource::TableData(table_id),
        ));

        trx.rollback().await.unwrap();
    });
}

#[test]
fn test_session_table_lock_cancellation_releases_fresh_metadata() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let blocker = LockOwner::Transaction(TrxID::new(91_301));
        assert!(
            try_acquire(
                engine.lock_manager(),
                LockResource::TableData(table_id),
                LockMode::Exclusive,
                blocker,
            )
            .unwrap()
        );

        let session = engine.new_session().unwrap();
        let session_owner = LockOwner::Session(session.id());
        let mut lock_fut = Box::pin(session.lock_table(table_id, LockMode::Shared));
        assert!(matches!(
            futures::poll!(lock_fut.as_mut()),
            std::task::Poll::Pending
        ));
        assert!(has_lock_entry(
            &engine,
            session_owner,
            LockResource::TableMetadata(table_id),
            LockMode::Shared,
            LockDebugEntryState::Granted,
        ));
        assert!(has_lock_entry(
            &engine,
            session_owner,
            LockResource::TableData(table_id),
            LockMode::Shared,
            LockDebugEntryState::Waiting,
        ));

        drop(lock_fut);
        wait_for_no_lock_resource(
            &engine,
            session_owner,
            LockResource::TableMetadata(table_id),
        )
        .await;
        wait_for_no_lock_resource(&engine, session_owner, LockResource::TableData(table_id)).await;
        assert_eq!(
            engine
                .lock_manager()
                .release(LockResource::TableData(table_id), blocker),
            1
        );
    });
}

#[test]
fn test_transaction_table_lock_failure_releases_fresh_metadata() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let session_owner = LockOwner::Session(session.id());
        session
            .lock_table(table_id, LockMode::Shared)
            .await
            .unwrap();
        let mut trx = session.begin_trx().unwrap();
        let trx_owner = trx_tests::lock_owner(&trx).unwrap();

        let err = trx
            .lock_table(table_id, LockMode::Exclusive)
            .await
            .unwrap_err();
        assert_eq!(
            err.operation_error(),
            Some(OperationError::LockOwnerGroupConflict)
        );
        assert!(!has_lock_resource(
            &engine,
            trx_owner,
            LockResource::TableMetadata(table_id),
        ));
        assert!(
            !trx_tests::cached_transaction_lock_covers(
                &trx,
                LockResource::TableMetadata(table_id),
                LockMode::Shared
            )
            .unwrap()
        );
        assert!(has_lock_resource(
            &engine,
            session_owner,
            LockResource::TableMetadata(table_id),
        ));

        trx.rollback().await.unwrap();
        session.unlock_table(table_id).unwrap();
    });
}

#[test]
fn test_transaction_table_lock_cancellation_releases_fresh_metadata() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let blocker = LockOwner::Transaction(TrxID::new(91_302));
        assert!(
            try_acquire(
                engine.lock_manager(),
                LockResource::TableData(table_id),
                LockMode::Exclusive,
                blocker,
            )
            .unwrap()
        );

        let mut session = engine.new_session().unwrap();
        let mut trx = session.begin_trx().unwrap();
        let trx_owner = trx_tests::lock_owner(&trx).unwrap();
        let mut lock_fut = Box::pin(trx.lock_table(table_id, LockMode::Shared));
        assert!(matches!(
            futures::poll!(lock_fut.as_mut()),
            std::task::Poll::Pending
        ));
        assert!(has_lock_entry(
            &engine,
            trx_owner,
            LockResource::TableMetadata(table_id),
            LockMode::Shared,
            LockDebugEntryState::Granted,
        ));
        assert!(has_lock_entry(
            &engine,
            trx_owner,
            LockResource::TableData(table_id),
            LockMode::Shared,
            LockDebugEntryState::Waiting,
        ));

        drop(lock_fut);
        wait_for_no_lock_resource(&engine, trx_owner, LockResource::TableMetadata(table_id)).await;
        wait_for_no_lock_resource(&engine, trx_owner, LockResource::TableData(table_id)).await;
        assert!(
            !trx_tests::cached_transaction_lock_covers(
                &trx,
                LockResource::TableMetadata(table_id),
                LockMode::Shared
            )
            .unwrap()
        );
        assert_eq!(
            engine
                .lock_manager()
                .release(LockResource::TableData(table_id), blocker),
            1
        );
        trx.rollback().await.unwrap();
    });
}

#[test]
fn test_session_exclusive_table_lock_covers_same_session_writer() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let session_owner = LockOwner::Session(session.id());
        session
            .lock_table(table_id, LockMode::Exclusive)
            .await
            .unwrap();

        let engine_ref = engine.new_ref().unwrap();
        let (owner_tx, owner_rx) = flume::bounded(1);
        let external_writer = smol::spawn(async move {
            let mut writer_session = engine_ref.new_session().unwrap();
            let mut writer_trx = writer_session.begin_trx().unwrap();
            owner_tx
                .send_async(trx_tests::lock_owner(&writer_trx).unwrap())
                .await
                .unwrap();
            trx_insert_row_by_id(
                &mut writer_trx,
                table_id,
                vec![Val::from(31_201i32), Val::from("external")],
            )
            .await?;
            writer_trx.commit().await?;
            Ok::<(), Error>(())
        });
        let external_owner = owner_rx.recv_async().await.unwrap();
        wait_for_lock_entry(
            &engine,
            external_owner,
            LockResource::TableData(table_id),
            LockMode::IntentExclusive,
            LockDebugEntryState::Waiting,
        )
        .await;

        let mut same_session_trx = session.begin_trx().unwrap();
        let same_session_owner = trx_tests::lock_owner(&same_session_trx).unwrap();
        trx_insert_row_by_id(
            &mut same_session_trx,
            table_id,
            vec![Val::from(31_202i32), Val::from("covered")],
        )
        .await
        .unwrap();
        assert!(has_lock_entry(
            &engine,
            same_session_owner,
            LockResource::TableData(table_id),
            LockMode::IntentExclusive,
            LockDebugEntryState::Granted,
        ));

        let err = session.unlock_table(table_id).unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::NotSupported));

        same_session_trx.commit().await.unwrap();
        assert!(has_lock_entry(
            &engine,
            session_owner,
            LockResource::TableData(table_id),
            LockMode::Exclusive,
            LockDebugEntryState::Granted,
        ));
        assert!(has_lock_entry(
            &engine,
            external_owner,
            LockResource::TableData(table_id),
            LockMode::IntentExclusive,
            LockDebugEntryState::Waiting,
        ));

        session.unlock_table(table_id).unwrap();
        assert!(!has_lock_resource(
            &engine,
            session_owner,
            LockResource::TableData(table_id),
        ));
        external_writer.await.unwrap();
    });
}

#[test]
fn test_table_scan_mvcc() {
    smol::block_on(async {
        const SIZE: i32 = 100;

        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;

        // insert 100 rows and commit
        let mut session1 = engine.new_session().unwrap();
        {
            let mut trx = session1.begin_trx().unwrap();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = expect_trx_insert(table_id, trx, insert).await;
            }
            _ = trx.commit().await.unwrap();
        }
        // we should see 100 committed rows.
        let mut session2 = engine.new_session().unwrap();
        {
            let mut trx = session2.begin_trx().unwrap();
            let mut res_len = 0usize;
            trx.exec(async |stmt| {
                stmt.table_scan_mvcc(table_id, &[0], |_| {
                    res_len += 1;
                    true
                })
                .await?;
                Ok(())
            })
            .await
            .unwrap();
            println!("res.len()={}", res_len);
            assert!(res_len == SIZE as usize);
            trx.commit().await.unwrap();
        }
        // insert 100 rows but not commit.
        let pending_trx = {
            let mut trx = session1.begin_trx().unwrap();
            for i in SIZE..SIZE * 2 {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = expect_trx_insert(table_id, trx, insert).await;
            }
            trx
        };
        // we should see only 100 rows
        {
            let mut trx = session2.begin_trx().unwrap();
            let mut res_len = 0usize;
            trx.exec(async |stmt| {
                stmt.table_scan_mvcc(table_id, &[0], |_| {
                    res_len += 1;
                    true
                })
                .await?;
                Ok(())
            })
            .await
            .unwrap();
            println!("res.len()={}", res_len);
            assert!(res_len == SIZE as usize);
            trx.commit().await.unwrap();
        }
        // commit the pending transaction.
        pending_trx.commit().await.unwrap();
        // now we should see 200 rows.
        {
            let mut trx = session2.begin_trx().unwrap();
            let mut res_len = 0usize;
            trx.exec(async |stmt| {
                stmt.table_scan_mvcc(table_id, &[0], |_| {
                    res_len += 1;
                    true
                })
                .await?;
                Ok(())
            })
            .await
            .unwrap();
            println!("res.len()={}", res_len);
            assert!(res_len == (SIZE * 2) as usize);
            trx.commit().await.unwrap();
        }
    });
}

#[test]
fn test_table_scan_mvcc_includes_cold_and_hot_rows() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 5, "cold").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let mut trx = session.begin_trx().unwrap();
        assert_eq!(
            scan_table_i32s(&mut trx, table_id).await,
            vec![0, 1, 2, 3, 4]
        );
        trx.commit().await.unwrap();

        insert_rows(table_id, &mut session, 100, 3, "hot").await;

        let mut trx = session.begin_trx().unwrap();
        assert_eq!(
            scan_table_i32s(&mut trx, table_id).await,
            vec![0, 1, 2, 3, 4, 100, 101, 102]
        );
        trx.commit().await.unwrap();
    });
}

#[test]
fn test_table_scan_mvcc_cold_delete_buffer_visibility() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 5, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let mut old_reader_session = engine.new_session().unwrap();
        let mut old_reader = old_reader_session.begin_trx().unwrap();

        let key = single_key(2i32);
        expect_delete_committed(table_id, &mut session, &key).await;

        assert_eq!(
            scan_table_i32s(&mut old_reader, table_id).await,
            vec![0, 1, 2, 3, 4]
        );
        old_reader.commit().await.unwrap();

        let mut new_reader = session.begin_trx().unwrap();
        assert_eq!(
            scan_table_i32s(&mut new_reader, table_id).await,
            vec![0, 1, 3, 4]
        );
        new_reader.commit().await.unwrap();
    });
}

#[test]
fn test_table_scan_mvcc_cold_update_visibility() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 3, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let mut old_reader_session = engine.new_session().unwrap();
        let mut old_reader = old_reader_session.begin_trx().unwrap();

        expect_update_committed(
            table_id,
            &mut session,
            &single_key(1i32),
            vec![UpdateCol {
                idx: 1,
                val: Val::from("updated"),
            }],
        )
        .await;

        assert_eq!(
            scan_table_pairs(&mut old_reader, table_id).await,
            vec![
                (0, "name".to_string()),
                (1, "name".to_string()),
                (2, "name".to_string()),
            ]
        );
        old_reader.commit().await.unwrap();

        let mut new_reader = session.begin_trx().unwrap();
        assert_eq!(
            scan_table_pairs(&mut new_reader, table_id).await,
            vec![
                (0, "name".to_string()),
                (1, "updated".to_string()),
                (2, "name".to_string()),
            ]
        );
        new_reader.commit().await.unwrap();
    });
}

#[test]
fn test_table_scan_mvcc_uncommitted_cold_delete_visibility() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 4, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let mut writer = session.begin_trx().unwrap();
        let res = trx_delete_row_by_id(&mut writer, table_id, &single_key(1i32)).await;
        assert!(matches!(res, Ok(DeleteMvcc::Deleted)));

        assert_eq!(scan_table_i32s(&mut writer, table_id).await, vec![0, 2, 3]);

        let mut other_session = engine.new_session().unwrap();
        let mut other_reader = other_session.begin_trx().unwrap();
        assert_eq!(
            scan_table_i32s(&mut other_reader, table_id).await,
            vec![0, 1, 2, 3]
        );
        other_reader.commit().await.unwrap();
        writer.rollback().await.unwrap();
    });
}

#[test]
fn test_table_scan_mvcc_skips_persisted_delete_delta() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 6, "name").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;

        let key = single_key(4i32);
        let reader = session.begin_trx().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let row_id = assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
        reader.commit().await.unwrap();

        expect_delete_committed(table_id, &mut session, &key).await;
        let marker_ts = delete_marker_ts(table.deletion_buffer().get(row_id).unwrap());
        wait_gc_cutoff_after(&session, marker_ts).await;
        checkpoint_published(table_id, &mut session).await;

        let snapshot = column_block_index_snapshot(&engine, table_id);
        let pool_guards = session.pool_guards();
        let index = snapshot.index(pool_guards.disk_guard());
        let entry = index.locate_block(row_id).await.unwrap().unwrap();
        let deltas = index.load_delete_deltas(&entry).await.unwrap();
        assert!(deltas.contains(&((row_id - entry.start_row_id) as u32)));

        let mut trx = session.begin_trx().unwrap();
        assert_eq!(
            scan_table_i32s(&mut trx, table_id).await,
            vec![0, 1, 2, 3, 5]
        );
        trx.commit().await.unwrap();
    });
}

#[test]
fn test_table_scan_mvcc_early_stop_before_hot_phase() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 5, "cold").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;
        insert_rows(table_id, &mut session, 100, 2, "hot").await;

        let mut trx = session.begin_trx().unwrap();
        let mut rows = Vec::new();
        trx.exec(async |stmt| {
            stmt.table_scan_mvcc(table_id, &[0], |vals| {
                rows.push(vals[0].as_i32().unwrap());
                rows.len() < 3
            })
            .await?;
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(rows, vec![0, 1, 2]);
        trx.commit().await.unwrap();
    });
}

#[test]
fn test_mem_scan_from_requires_row_page_boundary() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 4, "first").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;
        let table = table_for_internal_assertion(&engine, table_id);
        let captured_pivot = table.file().active_root_unchecked().pivot_row_id;

        insert_rows(table_id, &mut session, 100, 4, "second").await;
        let mut later_pivot = captured_pivot;
        let mut explicit_count = 0usize;
        table
            .mem
            .mem_scan_from(&session.pool_guards(), captured_pivot, |page_guard| {
                let page = page_guard.page();
                explicit_count += page.header.approx_non_deleted();
                later_pivot = page.header.start_row_id + u64::from(page.header.max_row_count);
                true
            })
            .await
            .unwrap();
        assert_eq!(explicit_count, 4);
        assert!(later_pivot > captured_pivot);

        let interior_start = captured_pivot + 2;
        let err = table
            .mem
            .mem_scan_from(&session.pool_guards(), interior_start, |_| true)
            .await
            .unwrap_err();
        assert!(err.kind() == ErrorKind::Internal);

        // Keep the second batch's row page allocated while advancing only the
        // memory-scan pivot to that page's exclusive row-id boundary. A real
        // later checkpoint may reclaim pages once no transaction root protects
        // them, which is outside this helper's direct contract.
        table
            .mem
            .blk_idx()
            .update_column_root(later_pivot, SUPER_BLOCK_ID)
            .await;
        assert_eq!(table.mem.pivot_row_id(), later_pivot);

        let mut current_hot_pages = 0usize;
        table
            .mem
            .mem_scan(&session.pool_guards(), |_| {
                current_hot_pages += 1;
                true
            })
            .await
            .unwrap();
        assert_eq!(current_hot_pages, 0);
    });
}

#[test]
fn test_table_freeze() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;

        let mut session1 = engine.new_session().unwrap();
        {
            let trx = session1.begin_trx().unwrap();
            let insert = vec![Val::from(1), Val::from("1")];
            expect_trx_insert(table_id, trx, insert)
                .await
                .commit()
                .await
                .unwrap();
        }
        let row_pages = session1.total_row_pages(table_id).await.unwrap();
        assert!(row_pages == 1);
        session1.freeze_table(table_id, 10).await.unwrap();
        // after freezing, new row should be inserted into second page.
        {
            let trx = session1.begin_trx().unwrap();
            let insert = vec![Val::from(2), Val::from("2")];
            expect_trx_insert(table_id, trx, insert)
                .await
                .commit()
                .await
                .unwrap();
        }
        let row_pages = session1.total_row_pages(table_id).await.unwrap();
        assert!(row_pages == 2);
        session1.freeze_table(table_id, 10).await.unwrap();

        // update row 1 will cause new insert into new page.
        {
            let mut trx = session1.begin_trx().unwrap();
            let key = SelectKey::new(0, vec![Val::from(1)]);
            let res = trx_update_row_by_id(
                &mut trx,
                table_id,
                &key,
                vec![UpdateCol {
                    idx: 1,
                    val: Val::from("3"),
                }],
            )
            .await;
            assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
            trx.commit().await.unwrap();
        }
        let row_pages = session1.total_row_pages(table_id).await.unwrap();
        assert!(row_pages == 3);

        // update row 1 will just be in-place.
        {
            let mut trx = session1.begin_trx().unwrap();
            let key = SelectKey::new(0, vec![Val::from(1)]);
            let res = trx_update_row_by_id(
                &mut trx,
                table_id,
                &key,
                vec![UpdateCol {
                    idx: 1,
                    val: Val::from("4"),
                }],
            )
            .await;
            assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
            trx.commit().await.unwrap();
        }
        let row_pages = session1.total_row_pages(table_id).await.unwrap();
        assert!(row_pages == 3);
    });
}

#[test]
fn test_transition_captures_uncommitted_lock_into_deletion_buffer() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 1, 1, "lock").await;

        let key = single_key(1i32);
        let mut trx = session.begin_trx().unwrap();
        let index = bound_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            key.index_no,
        );
        let (row_id, _) = index
            .lookup(session.pool_guards().index_guard(), &key.vals, trx.sts())
            .await
            .unwrap()
            .unwrap();
        let page_id = match table_for_internal_assertion(&engine, table_id)
            .find_row(&session.pool_guards(), row_id)
            .await
            .unwrap()
        {
            RowLocation::RowPage(page_id) => page_id,
            RowLocation::NotFound => panic!("row should exist"),
            RowLocation::LwcBlock { .. } => unreachable!("row page expected"),
        };

        let res: Result<()> = trx
            .exec(async |stmt| {
                let page_guard = engine
                    .mem_pool
                    .get_page::<RowPage>(
                        session.pool_guards().mem_guard(),
                        page_id,
                        LatchFallbackMode::Shared,
                    )
                    .await
                    .expect("buffer-pool read failed in test")
                    .lock_shared_async()
                    .await
                    .unwrap();
                stmt.acquire_table_write_locks(table_id).await?;
                let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
                let mut lock_row = table_for_internal_assertion(&engine, table_id)
                    .accessor_with_layout(&layout)
                    .lock_row_for_write(rt, effects, &page_guard, row_id, Some(&key))
                    .await;
                match &mut lock_row {
                    LockRowForWrite::Ok(access) => {
                        drop(access.take());
                    }
                    _ => panic!("lock should succeed"),
                }

                let frozen_page = {
                    let (page_ctx, page) = page_guard.ctx_and_page();
                    let frozen_page = FrozenPage {
                        page_id,
                        start_row_id: page.header.start_row_id,
                        end_row_id: page.header.start_row_id + page.header.max_row_count as u64,
                    };
                    page_ctx.row_ver().unwrap().set_frozen();
                    frozen_page
                };
                table_for_internal_assertion(&engine, table_id)
                    .set_frozen_pages_to_transition(
                        &session.pool_guards(),
                        &[frozen_page],
                        stmt.runtime().sts(),
                    )
                    .await
                    .unwrap();

                let marker = table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(row_id)
                    .unwrap();
                match marker {
                    DeleteMarker::Ref(status) => {
                        assert!(std::sync::Arc::ptr_eq(&status, &stmt.runtime().status()));
                    }
                    DeleteMarker::Committed(_) => {
                        panic!("uncommitted lock should remain as marker ref")
                    }
                }
                drop(lock_row);
                drop(page_guard);
                Err(Report::new(OperationError::NotSupported).into())
            })
            .await;
        assert_eq!(
            res.unwrap_err().operation_error(),
            Some(OperationError::NotSupported)
        );
        trx.rollback().await.unwrap();
    });
}

#[test]
fn test_checkpoint_basic_flow() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let name = "x".repeat(1024);
        insert_rows(table_id, &mut session, 0, 200, &name).await;

        let table = table_for_internal_assertion(&engine, table_id);
        let old_root = table.file().active_root_unchecked().clone();
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let (frozen_pages, _) = table
            .collect_frozen_pages(&session.pool_guards())
            .await
            .unwrap();
        assert!(!frozen_pages.is_empty());

        checkpoint_published(table_id, &mut session).await;

        let new_root = table.file().active_root_unchecked().clone();
        assert!(new_root.pivot_row_id > old_root.pivot_row_id);
        assert_ne!(new_root.column_block_index_root, SUPER_BLOCK_ID);
        assert!(new_root.deletion_cutoff_ts > old_root.deletion_cutoff_ts);
    });
}

#[test]
fn test_foreground_lifecycle_rejects_dropping_and_dropped_handles() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_one_row(
            table_id,
            &mut session,
            vec![Val::from(1), Val::from("lifecycle")],
        )
        .await;

        let table = table_for_internal_assertion(&engine, table_id);
        table.begin_drop_lifecycle().await.unwrap();

        let mut read_trx = session.begin_trx().unwrap();
        let err = trx_select_row_mvcc_by_id(&mut read_trx, table_id, &single_key(1), &[0, 1])
            .await
            .unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::TableDropping));
        assert_eq!(read_trx.commit().await.unwrap(), TrxID::new(0));

        let mut write_trx = session.begin_trx().unwrap();
        let err = trx_insert_row_by_id(
            &mut write_trx,
            table_id,
            vec![Val::from(2), Val::from("blocked")],
        )
        .await
        .unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::TableDropping));
        assert_eq!(write_trx.commit().await.unwrap(), TrxID::new(0));

        table.mark_dropped_lifecycle().unwrap();

        let mut dropped_read = session.begin_trx().unwrap();
        let err = trx_select_row_mvcc_by_id(&mut dropped_read, table_id, &single_key(1), &[0, 1])
            .await
            .unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
        assert_eq!(dropped_read.commit().await.unwrap(), TrxID::new(0));

        let mut dropped_write = session.begin_trx().unwrap();
        let err = trx_insert_row_by_id(
            &mut dropped_write,
            table_id,
            vec![Val::from(3), Val::from("dropped")],
        )
        .await
        .unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
        assert_eq!(dropped_write.commit().await.unwrap(), TrxID::new(0));
    });
}

#[test]
fn test_table_drop_gate_waits_for_checkpoint_publish_lease() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let table = table_for_internal_assertion(&engine, table_id);
        let _root_lease = table.try_begin_checkpoint_root_mutation().unwrap();
        let publish_lease = table.try_begin_checkpoint_publish().unwrap();
        let mut drop_fut = Box::pin(table.begin_drop_lifecycle());

        assert!(matches!(
            futures::poll!(drop_fut.as_mut()),
            std::task::Poll::Pending
        ));
        assert_eq!(table.lifecycle.state(), TableLifecycleState::Dropping);
        match table.try_begin_checkpoint_publish() {
            Ok(_lease) => panic!("publish lease should be blocked by drop gate"),
            Err(reason) => assert_eq!(reason, CheckpointCancelReason::TableDropping),
        }

        drop(publish_lease);
        drop_fut.await.unwrap();
        assert_eq!(table.lifecycle.state(), TableLifecycleState::Dropping);
    });
}

#[test]
fn test_checkpoint_cancelled_when_table_dropping() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let root_before = table.file().active_root_unchecked().clone();

        wait_checkpoint_ready(table_id, &session).await;
        table_for_internal_assertion(&engine, table_id)
            .begin_drop_lifecycle()
            .await
            .unwrap();
        let outcome = session.checkpoint_table(table_id).await.unwrap();
        assert_eq!(
            outcome,
            CheckpointOutcome::Cancelled {
                reason: CheckpointCancelReason::TableDropping
            }
        );
        assert_root_metadata_unchanged(
            &root_before,
            &table_for_internal_assertion(&engine, table_id),
        );
        assert!(!session.in_trx().unwrap());
    });
}

#[test]
fn test_checkpoint_cancelled_when_table_dropping_before_delayed_readiness() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 120, "dropping-before-delay").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();

        let mut reader_session = engine.new_session().unwrap();
        let reader = reader_session.begin_trx().unwrap();
        checkpoint_published(table_id, &mut session).await;
        assert!(matches!(
            session.table_checkpoint_readiness(table_id).unwrap(),
            CheckpointReadiness::Delayed { .. }
        ));

        let table = table_for_internal_assertion(&engine, table_id);
        let root_before = table.file().active_root_unchecked().clone();
        table.begin_drop_lifecycle().await.unwrap();

        let outcome = session.checkpoint_table(table_id).await.unwrap();
        assert_eq!(
            outcome,
            CheckpointOutcome::Cancelled {
                reason: CheckpointCancelReason::TableDropping
            }
        );
        assert_root_metadata_unchanged(&root_before, &table);
        assert!(!session.in_trx().unwrap());

        reader.commit().await.unwrap();
    });
}

#[test]
fn test_checkpoint_cancelled_when_table_lifecycle_dropped() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let root_before = table.file().active_root_unchecked().clone();

        table.begin_drop_lifecycle().await.unwrap();
        table.mark_dropped_lifecycle().unwrap();

        let outcome = session.checkpoint_table(table_id).await.unwrap();
        assert_eq!(
            outcome,
            CheckpointOutcome::Cancelled {
                reason: CheckpointCancelReason::TableDropped
            }
        );
        assert_root_metadata_unchanged(&root_before, &table);
        assert!(!session.in_trx().unwrap());
    });
}

#[test]
fn test_drop_table_rejects_already_dropping_lifecycle_without_poison() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();

        table_for_internal_assertion(&engine, table_id)
            .begin_drop_lifecycle()
            .await
            .unwrap();

        let err = session.drop_table(table_id).await.unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::TableDropping));
        assert_eq!(
            table_for_internal_assertion(&engine, table_id)
                .lifecycle
                .state(),
            TableLifecycleState::Dropping
        );
        assert!(!session.in_trx().unwrap());
        assert!(engine.trx_sys.storage_poison_error().is_none());
    });
}

#[test]
fn test_drop_table_rejects_active_transaction() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let trx = session.begin_trx().unwrap();

        let err = session.drop_table(table_id).await.unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::NotSupported));

        trx.rollback().await.unwrap();
    });
}

#[test]
fn test_drop_table_returns_not_found_for_missing_table() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();

        let err = session.drop_table(TableID::new(0)).await.unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));

        let missing_user_table_id = table_id + 1000;
        let err = session.drop_table(missing_user_table_id).await.unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
    });
}

#[test]
fn test_drop_table_rejects_runtime_missing_catalog_row_before_gate() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut corrupt_session = engine.new_session().unwrap();
        let mut corrupt_trx = corrupt_session.begin_trx().unwrap();

        corrupt_trx
            .exec(async |stmt| {
                let deleted = engine
                    .catalog()
                    .storage
                    .tables()
                    .delete_by_id(stmt, table_id)
                    .await;
                assert!(deleted);
                let old = stmt
                    .effects_mut()
                    .set_ddl_redo(DDLRedo::DropTable(table_id));
                debug_assert!(old.is_none());
                Ok(())
            })
            .await
            .unwrap();
        corrupt_trx.commit().await.unwrap();

        let mut drop_session = engine.new_session().unwrap();
        let err = drop_session.drop_table(table_id).await.unwrap_err();

        assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
        assert_eq!(
            table_for_internal_assertion(&engine, table_id)
                .lifecycle
                .state(),
            TableLifecycleState::Live
        );
        assert!(!drop_session.in_trx().unwrap());
        assert!(engine.trx_sys.storage_poison_error().is_none());
        assert!(engine.catalog().get_table(table_id).await.is_some());
    });
}

#[test]
fn test_drop_table_rejects_same_session_explicit_table_lock() {
    smol::block_on(async {
        for mode in [LockMode::Shared, LockMode::Exclusive] {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let owner = LockOwner::Session(session.id());

            session.lock_table(table_id, mode).await.unwrap();
            let err = session.drop_table(table_id).await.unwrap_err();
            assert_eq!(
                err.operation_error(),
                Some(OperationError::LockOwnerGroupConflict)
            );

            assert_eq!(
                table_for_internal_assertion(&engine, table_id)
                    .lifecycle
                    .state(),
                TableLifecycleState::Live
            );
            assert!(engine.catalog().get_table(table_id).await.is_some());
            assert!(!has_lock_resource(
                &engine,
                owner,
                LockResource::CatalogNamespace,
            ));
            assert!(has_lock_entry(
                &engine,
                owner,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
                LockDebugEntryState::Granted,
            ));
            assert!(has_lock_entry(
                &engine,
                owner,
                LockResource::TableData(table_id),
                mode,
                LockDebugEntryState::Granted,
            ));

            session.unlock_table(table_id).unwrap();
            assert!(!has_lock_resource(
                &engine,
                owner,
                LockResource::TableMetadata(table_id),
            ));
            assert!(!has_lock_resource(
                &engine,
                owner,
                LockResource::TableData(table_id),
            ));
            session.drop_table(table_id).await.unwrap();
        }
    });
}

#[test]
fn test_drop_table_fails_waiting_session_table_lock() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let table = table_for_internal_assertion(&engine, table_id);
        let root_lease = table.try_begin_checkpoint_root_mutation().unwrap();
        let publish_lease = table.try_begin_checkpoint_publish().unwrap();
        let mut drop_session = engine.new_session().unwrap();
        let drop_owner = LockOwner::Session(drop_session.id());
        let mut drop_fut = Box::pin(drop_session.drop_table(table_id));
        assert!(matches!(
            futures::poll!(drop_fut.as_mut()),
            std::task::Poll::Pending
        ));
        assert!(has_lock_entry(
            &engine,
            drop_owner,
            LockResource::TableMetadata(table_id),
            LockMode::Exclusive,
            LockDebugEntryState::Granted,
        ));

        let lock_session = engine.new_session().unwrap();
        let lock_owner = LockOwner::Session(lock_session.id());
        let err = lock_session
            .lock_table(table_id, LockMode::Shared)
            .await
            .unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::TableDropping));
        assert!(!has_lock_resource(
            &engine,
            lock_owner,
            LockResource::TableMetadata(table_id),
        ));

        drop(publish_lease);
        drop_fut.await.unwrap();
        drop(root_lease);
        assert!(!has_lock_resource(
            &engine,
            lock_owner,
            LockResource::TableMetadata(table_id),
        ));
        assert!(!has_lock_resource(
            &engine,
            lock_owner,
            LockResource::TableData(table_id),
        ));
    });
}

#[test]
fn test_drop_table_fails_waiting_transaction_table_lock() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let table = table_for_internal_assertion(&engine, table_id);
        let root_lease = table.try_begin_checkpoint_root_mutation().unwrap();
        let publish_lease = table.try_begin_checkpoint_publish().unwrap();
        let mut drop_session = engine.new_session().unwrap();
        let drop_owner = LockOwner::Session(drop_session.id());
        let mut drop_fut = Box::pin(drop_session.drop_table(table_id));
        assert!(matches!(
            futures::poll!(drop_fut.as_mut()),
            std::task::Poll::Pending
        ));
        assert!(has_lock_entry(
            &engine,
            drop_owner,
            LockResource::TableMetadata(table_id),
            LockMode::Exclusive,
            LockDebugEntryState::Granted,
        ));

        let mut lock_session = engine.new_session().unwrap();
        let mut trx = lock_session.begin_trx().unwrap();
        let lock_owner = trx_tests::lock_owner(&trx).unwrap();
        let err = trx
            .lock_table(table_id, LockMode::Exclusive)
            .await
            .unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::TableDropping));
        assert!(!has_lock_resource(
            &engine,
            lock_owner,
            LockResource::TableMetadata(table_id),
        ));

        drop(publish_lease);
        drop_fut.await.unwrap();
        drop(root_lease);
        assert!(!has_lock_resource(
            &engine,
            lock_owner,
            LockResource::TableMetadata(table_id),
        ));
        assert!(!has_lock_resource(
            &engine,
            lock_owner,
            LockResource::TableData(table_id),
        ));

        trx.rollback().await.unwrap();
    });
}

#[test]
fn test_explicit_table_lock_after_drop_returns_not_found_without_locks() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut drop_session = engine.new_session().unwrap();
        drop_session.drop_table(table_id).await.unwrap();

        let lock_session = engine.new_session().unwrap();
        let session_owner = LockOwner::Session(lock_session.id());
        let err = lock_session
            .lock_table(table_id, LockMode::Shared)
            .await
            .unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
        assert!(!has_lock_resource(
            &engine,
            session_owner,
            LockResource::TableMetadata(table_id),
        ));
        assert!(!has_lock_resource(
            &engine,
            session_owner,
            LockResource::TableData(table_id),
        ));

        let mut trx_session = engine.new_session().unwrap();
        let mut trx = trx_session.begin_trx().unwrap();
        let trx_owner = trx_tests::lock_owner(&trx).unwrap();
        let err = trx
            .lock_table(table_id, LockMode::Exclusive)
            .await
            .unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
        assert!(!has_lock_resource(
            &engine,
            trx_owner,
            LockResource::TableMetadata(table_id),
        ));
        assert!(!has_lock_resource(
            &engine,
            trx_owner,
            LockResource::TableData(table_id),
        ));
        trx.rollback().await.unwrap();
    });
}

#[test]
fn test_drop_table_logical_cascade() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine =
            evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique").await;
        let table_id = create_non_unique_name_table_for_test(&engine).await;
        let table_file_path = engine.table_fs.user_table_file_path(table_id);
        let mut session = engine.new_session().unwrap();
        insert_one_row(
            table_id,
            &mut session,
            vec![Val::from(1), Val::from("drop-me")],
        )
        .await;
        let (other_spec, other_indexes) = drop_table_test_spec();
        let other_table_id = session
            .create_table(other_spec, other_indexes)
            .await
            .unwrap();
        let owner = LockOwner::Session(session.id());

        assert!(std::path::Path::new(&table_file_path).exists());
        session.drop_table(table_id).await.unwrap();

        assert!(!has_lock_resource(
            &engine,
            owner,
            LockResource::CatalogNamespace,
        ));
        assert!(!has_lock_resource(
            &engine,
            owner,
            LockResource::TableMetadata(table_id),
        ));
        assert!(!has_lock_resource(
            &engine,
            owner,
            LockResource::TableData(table_id),
        ));
        assert!(engine.catalog().get_table(table_id).await.is_none());
        assert!(
            engine
                .catalog()
                .storage
                .tables()
                .find_uncommitted_by_id(&session.pool_guards(), table_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            engine
                .catalog()
                .storage
                .columns()
                .list_uncommitted_by_table_id(&session.pool_guards(), table_id)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            engine
                .catalog()
                .storage
                .indexes()
                .list_uncommitted_by_table_id(&session.pool_guards(), table_id)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            engine
                .catalog()
                .storage
                .index_columns()
                .list_uncommitted_by_table_id(&session.pool_guards(), table_id)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            engine
                .catalog()
                .storage
                .tables()
                .find_uncommitted_by_id(&session.pool_guards(), other_table_id)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            !engine
                .catalog()
                .storage
                .columns()
                .list_uncommitted_by_table_id(&session.pool_guards(), other_table_id)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(std::path::Path::new(&table_file_path).exists());

        let err = session.drop_table(table_id).await.unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));

        let mut stale_read = session.begin_trx().unwrap();
        let err = trx_select_row_mvcc_by_id(&mut stale_read, table_id, &single_key(1), &[0, 1])
            .await
            .unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
        assert_eq!(stale_read.commit().await.unwrap(), TrxID::new(0));

        let mut stale_write = session.begin_trx().unwrap();
        let err = trx_insert_row_by_id(
            &mut stale_write,
            table_id,
            vec![Val::from(2), Val::from("blocked")],
        )
        .await
        .unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
        assert_eq!(stale_write.commit().await.unwrap(), TrxID::new(0));

        let (later_spec, later_indexes) = drop_table_test_spec();
        let later_table_id = session
            .create_table(later_spec, later_indexes)
            .await
            .unwrap();
        assert!(later_table_id > table_id);
        assert!(later_table_id > other_table_id);
    });
}

#[test]
fn test_drop_table_gc_deletes_file_after_catalog_checkpoint() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = lightweight_test_engine_config(main_dir, "drop_gc_destroy")
            .build()
            .await
            .unwrap();
        let mut session = engine.new_session().unwrap();
        let (table_spec, index_specs) = drop_table_test_spec();
        let table_id = session.create_table(table_spec, index_specs).await.unwrap();
        insert_one_row(
            table_id,
            &mut session,
            vec![Val::from(11), Val::from("gc-delete")],
        )
        .await;
        let table_file_path = engine.table_fs.user_table_file_path(table_id);

        session.drop_table(table_id).await.unwrap();
        engine.trx_sys.request_dropped_table_purge();
        engine
            .catalog()
            .checkpoint_now(&engine.trx_sys)
            .await
            .unwrap();
        wait_path_exists(&table_file_path, false).await;

        let mut trx = session.begin_trx().unwrap();
        let err = trx
            .exec(async |stmt| {
                stmt.table_lookup_unique_mvcc(table_id, &single_key(11), &[0, 1])
                    .await
            })
            .await
            .unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::TableNotFound));
        trx.rollback().await.unwrap();
    });
}

#[test]
fn test_drop_table_catalog_cascade_poison_preserves_source_error() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut corrupt_session = engine.new_session().unwrap();
        let mut corrupt_trx = corrupt_session.begin_trx().unwrap();

        corrupt_trx
            .exec(async |stmt| {
                let deleted = engine
                    .catalog()
                    .storage
                    .index_columns()
                    .delete_by_index(stmt, table_id, 0)
                    .await
                    .unwrap();
                assert_eq!(deleted, 1);
                let old = stmt.effects_mut().set_ddl_redo(DDLRedo::DropIndex {
                    table_id,
                    index_no: 0,
                });
                debug_assert!(old.is_none());
                Ok(())
            })
            .await
            .unwrap();
        corrupt_trx.commit().await.unwrap();

        let mut drop_session = engine.new_session().unwrap();
        let err = drop_session.drop_table(table_id).await.unwrap_err();
        let report = format!("{err:?}");

        assert_eq!(
            err.report().downcast_ref::<FatalError>().copied(),
            Some(FatalError::Poisoned),
            "{report}"
        );
        assert_eq!(
            err.report().downcast_ref::<InternalError>().copied(),
            Some(InternalError::Generic),
            "{report}"
        );
        assert!(
            report.contains("drop table failed after lifecycle gate: table_id="),
            "{report}"
        );
        assert!(report.contains("operation=catalog cascade"), "{report}");
        assert!(
            report.contains("drop table catalog cascade count mismatch"),
            "{report}"
        );
        assert!(
            engine
                .trx_sys
                .storage_poison_error()
                .as_ref()
                .is_some_and(|err| *err.current_context() == FatalError::Poisoned)
        );
        assert!(!drop_session.in_trx().unwrap());
    });
}

#[test]
fn test_drop_table_commit_poison_preserves_source_error() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let redo_file_path = temp_dir.path().join("redo_testsys_lightweight.0.00000000");
        let hook = Arc::new(FailingFirstWriteHook::new(redo_file_path));
        let _install = install_storage_backend_test_hook(hook.clone());
        let mut session = engine.new_session().unwrap();

        let err = session.drop_table(table_id).await.unwrap_err();
        let report = format!("{err:?}");

        assert!(hook.call_count() > 0);
        assert_eq!(
            err.report().downcast_ref::<FatalError>().copied(),
            Some(FatalError::RedoWrite),
            "{report}"
        );
        assert_eq!(
            err.completion_error(),
            Some(CompletionErrorKind::Fatal(FatalError::RedoWrite)),
            "{report}"
        );
        assert!(
            report.contains("drop table failed after lifecycle gate: table_id="),
            "{report}"
        );
        assert!(report.contains("operation=commit"), "{report}");
        assert!(report.contains("wait for redo group commit"), "{report}");
        assert!(report.contains("propagate from other threads"), "{report}");
        assert!(
            engine
                .trx_sys
                .storage_poison_error()
                .as_ref()
                .is_some_and(|err| *err.current_context() == FatalError::RedoWrite)
        );
        assert!(!session.in_trx().unwrap());
    });
}

#[test]
fn test_user_insert_commit_poison_rolls_back_session_before_return() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        expect_insert_committed(
            table_id,
            &mut session,
            vec![Val::from(1), Val::from("seed")],
        )
        .await;
        let redo_file_path = temp_dir.path().join("redo_testsys_lightweight.0.00000000");
        let hook = Arc::new(FailingFirstWriteHook::new(redo_file_path));
        let _install = install_storage_backend_test_hook(hook.clone());

        let mut trx = session.begin_trx().unwrap();
        trx = expect_trx_insert(table_id, trx, vec![Val::from(169), Val::from("redo-fail")]).await;
        let err = trx.commit().await.unwrap_err();
        let report = format!("{err:?}");

        assert!(hook.call_count() > 0);
        assert!(report.contains("redo write failed"), "{report}");
        assert!(
            engine
                .trx_sys
                .storage_poison_error()
                .as_ref()
                .is_some_and(|err| *err.current_context() == FatalError::RedoWrite)
        );
        assert_eq!(engine.session_registry.active_transaction_count(), 0);
    });
}

#[test]
fn test_drop_table_waits_for_active_metadata_reader() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut reader_session = engine.new_session().unwrap();
        let mut reader_trx = reader_session.begin_trx().unwrap();
        let (held_tx, held_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let mut reader_fut = Box::pin(reader_trx.exec(async |stmt| {
            stmt_tests::acquire_statement_lock(
                stmt,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
            )
            .await?;
            held_tx.send_async(()).await.unwrap();
            release_rx.recv_async().await.unwrap();
            Ok(())
        }));

        loop {
            if held_rx.try_recv().is_ok() {
                break;
            }
            assert!(matches!(
                futures::poll!(reader_fut.as_mut()),
                std::task::Poll::Pending
            ));
        }

        let mut drop_session = engine.new_session().unwrap();
        let mut drop_fut = Box::pin(drop_session.drop_table(table_id));
        assert!(matches!(
            futures::poll!(drop_fut.as_mut()),
            std::task::Poll::Pending
        ));

        release_tx.send_async(()).await.unwrap();
        reader_fut.await.unwrap();
        assert_eq!(reader_trx.commit().await.unwrap(), TrxID::new(0));
        drop_fut.await.unwrap();
    });
}

#[test]
fn test_drop_table_waits_for_active_table_writer() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut writer_session = engine.new_session().unwrap();
        let mut writer_trx = writer_session.begin_trx().unwrap();
        trx_insert_row_by_id(
            &mut writer_trx,
            table_id,
            vec![Val::from(91), Val::from("writer")],
        )
        .await
        .unwrap();

        let mut drop_session = engine.new_session().unwrap();
        let mut drop_fut = Box::pin(drop_session.drop_table(table_id));
        assert!(matches!(
            futures::poll!(drop_fut.as_mut()),
            std::task::Poll::Pending
        ));

        assert!(writer_trx.commit().await.unwrap() > TrxID::new(0));
        drop_fut.await.unwrap();
    });
}

#[test]
fn test_catalog_checkpoint_scan_allows_runtime_removed_drop_table() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();

        session.drop_table(table_id).await.unwrap();
        let batch = engine
            .catalog()
            .scan_checkpoint_batch(&engine.trx_sys)
            .unwrap();

        assert_eq!(
            batch.stop_reason,
            CatalogCheckpointScanStopReason::ReachedDurableUpper
        );
        assert_eq!(batch.catalog_ddl_txn_count, 2);
        assert!(batch.safe_cts >= batch.replay_start_ts);
    });
}

#[test]
fn test_drop_table_recovery_keeps_table_live_without_committed_drop() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = lightweight_test_engine_config(main_dir.clone(), "drop_recover_uncommitted")
            .build()
            .await
            .unwrap();
        let mut session = engine.new_session().unwrap();
        let (table_spec, index_specs) = drop_table_test_spec();
        let table_id = session.create_table(table_spec, index_specs).await.unwrap();
        let table_for_internal_lifecycle = engine
            .catalog()
            .get_table_now(table_id)
            .expect("created table should still be loaded");
        table_for_internal_lifecycle
            .begin_drop_lifecycle()
            .await
            .unwrap();

        drop(table_for_internal_lifecycle);
        drop(session);
        drop(engine);

        let engine = lightweight_test_engine_config(main_dir, "drop_recover_uncommitted")
            .build()
            .await
            .unwrap();
        assert!(engine.catalog().get_table(table_id).await.is_some());
    });
}

#[test]
fn test_drop_table_recovery_replays_committed_drop_before_catalog_checkpoint() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = lightweight_test_engine_config(main_dir.clone(), "drop_recover_replay")
            .build()
            .await
            .unwrap();
        let mut session = engine.new_session().unwrap();
        let (table_spec, index_specs) = drop_table_test_spec();
        let table_id = session.create_table(table_spec, index_specs).await.unwrap();
        let table_file_path = engine.table_fs.user_table_file_path(table_id);

        session.drop_table(table_id).await.unwrap();
        assert!(std::path::Path::new(&table_file_path).exists());

        drop(session);
        drop(engine);

        let engine = lightweight_test_engine_config(main_dir, "drop_recover_replay")
            .build()
            .await
            .unwrap();
        assert!(engine.catalog().get_table(table_id).await.is_none());
        assert!(std::path::Path::new(&table_file_path).exists());
        let mut session = engine.new_session().unwrap();
        let (table_spec, index_specs) = drop_table_test_spec();
        let _ = session.create_table(table_spec, index_specs).await.unwrap();
        engine
            .catalog()
            .checkpoint_now(&engine.trx_sys)
            .await
            .unwrap();
        wait_path_exists(&table_file_path, false).await;
    });
}

#[test]
fn test_drop_table_catalog_checkpoint_cleans_absent_leftover_file() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = lightweight_test_engine_config(main_dir.clone(), "drop_recover_absence")
            .build()
            .await
            .unwrap();
        let mut session = engine.new_session().unwrap();
        let (table_spec, index_specs) = drop_table_test_spec();
        let table_id = session.create_table(table_spec, index_specs).await.unwrap();
        let mut trx = session.begin_trx().unwrap();
        let insert = trx_insert_row_by_id(
            &mut trx,
            table_id,
            vec![Val::from(7), Val::from("checkpoint-covered")],
        )
        .await;
        let Ok(_) = insert else {
            panic!("insert should succeed: {insert:?}");
        };
        trx.commit().await.unwrap();
        let table_file_path = engine.table_fs.user_table_file_path(table_id);

        session.drop_table(table_id).await.unwrap();
        engine
            .catalog()
            .checkpoint_now(&engine.trx_sys)
            .await
            .unwrap();
        assert!(
            engine
                .catalog()
                .storage
                .checkpoint_snapshot()
                .unwrap()
                .catalog_replay_start_ts
                > TrxID::new(1)
        );
        wait_path_exists(&table_file_path, false).await;

        drop(session);
        drop(engine);

        let engine = lightweight_test_engine_config(main_dir, "drop_recover_absence")
            .build()
            .await
            .unwrap();
        assert!(engine.catalog().get_table(table_id).await.is_none());
        assert!(!std::path::Path::new(&table_file_path).exists());
    });
}

#[test]
fn test_recovery_cleans_post_replay_create_table_provisional_file() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = lightweight_test_engine_config(main_dir.clone(), "create_orphan_recover")
            .build()
            .await
            .unwrap();
        let table_id = USER_OBJ_ID_START + 99;
        let table_file_path = engine.table_fs.user_table_file_path(table_id);
        let (table_spec, index_specs) = drop_table_test_spec();
        let metadata = Arc::new(TableMetadata::try_new(table_spec.columns, index_specs).unwrap());
        let mutable = engine
            .table_fs
            .create_table_file(table_id, metadata, false)
            .unwrap();
        let (table_file, old_root) = mutable.commit(TrxID::new(1), false).await.unwrap();
        drop(old_root);
        drop(table_file);
        assert!(std::path::Path::new(&table_file_path).exists());

        drop(engine);

        let engine = lightweight_test_engine_config(main_dir, "create_orphan_recover")
            .build()
            .await
            .unwrap();
        assert!(engine.catalog().get_table(table_id).await.is_none());
        wait_path_exists(&table_file_path, false).await;
    });
}

#[test]
fn test_checkpoint_publish_write_failure_poisons_storage() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        let root_before = table.file().active_root_unchecked().clone();
        wait_checkpoint_ready(table_id, &session).await;
        let table_file_path = engine.table_fs.user_table_file_path(table_id);
        let hook = Arc::new(FailingFirstWriteHook::new(table_file_path));
        let _install = install_storage_backend_test_hook(hook.clone());

        let err = session.checkpoint_table(table_id).await.unwrap_err();
        assert_checkpoint_write_poisoned(&err, &engine);
        assert!(hook.call_count() > 0);
        assert_root_metadata_unchanged(
            &root_before,
            &table_for_internal_assertion(&engine, table_id),
        );
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
        let table = table_for_internal_assertion(&engine, table_id);
        let root_before = table.file().active_root_unchecked().clone();
        wait_checkpoint_ready(table_id, &session).await;

        set_test_force_post_publish_checkpoint_error(true);
        let res = session.checkpoint_table(table_id).await;
        set_test_force_post_publish_checkpoint_error(false);

        let err = res.unwrap_err();
        assert_checkpoint_write_poisoned(&err, &engine);
        assert!(table.file().active_root_unchecked().root_ts > root_before.root_ts);
        assert!(!session.in_trx().unwrap());
    });
}

#[test]
fn test_checkpoint_readiness_ready_when_effective_ts_crossed_gc_horizon() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let session = engine.new_session().unwrap();
        let root = table_for_internal_assertion(&engine, table_id)
            .file()
            .active_root_unchecked()
            .clone();
        let effective_ts = root.effective_ts();
        wait_gc_cutoff_after(&session, effective_ts).await;
        assert!(matches!(
            session.table_checkpoint_readiness(table_id).unwrap(),
            CheckpointReadiness::Ready
        ));
    });
}

#[test]
fn test_checkpoint_readiness_returns_error_after_session_close() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();

        session.close().await.unwrap();

        let err = session.table_checkpoint_readiness(table_id).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Operation);
        assert_eq!(err.operation_error(), Some(OperationError::NotSupported));
    });
}

#[test]
fn test_checkpoint_readiness_reports_missing_and_non_live_tables() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let session = engine.new_session().unwrap();

        assert_eq!(
            session.table_checkpoint_readiness(table_id + 1000).unwrap(),
            CheckpointReadiness::TableNotFound
        );

        let _ = session.table_checkpoint_readiness(table_id).unwrap();
        let table = table_for_internal_assertion(&engine, table_id);
        table.begin_drop_lifecycle().await.unwrap();

        assert_eq!(
            session.table_checkpoint_readiness(table_id).unwrap(),
            CheckpointReadiness::TableDropping
        );
        let uncached_session = engine.new_session().unwrap();
        assert_eq!(
            uncached_session
                .table_checkpoint_readiness(table_id)
                .unwrap(),
            CheckpointReadiness::TableDropping
        );

        table.mark_dropped_lifecycle().unwrap();

        assert_eq!(
            session.table_checkpoint_readiness(table_id).unwrap(),
            CheckpointReadiness::TableNotFound
        );
        let uncached_session = engine.new_session().unwrap();
        assert_eq!(
            uncached_session
                .table_checkpoint_readiness(table_id)
                .unwrap(),
            CheckpointReadiness::TableNotFound
        );
    });
}

#[test]
fn test_checkpoint_readiness_delayed_reports_effective_ts_and_horizon() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 120, "readiness-delay").await;

        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let mut reader_session = engine.new_session().unwrap();
        let reader = reader_session.begin_trx().unwrap();
        checkpoint_published(table_id, &mut session).await;

        let active_root = table_for_internal_assertion(&engine, table_id)
            .file()
            .active_root_unchecked()
            .clone();
        let active_root_effective_ts = active_root.effective_ts();
        let readiness = session.table_checkpoint_readiness(table_id).unwrap();
        let CheckpointReadiness::Delayed { reason } = readiness else {
            panic!("expected delayed checkpoint readiness, got {readiness:?}");
        };
        assert_eq!(reason.effective_ts, active_root_effective_ts);
        assert_eq!(reason.min_active_sts, reader.sts());
        assert!(reason.effective_ts >= reason.min_active_sts);

        reader.commit().await.unwrap();
        wait_gc_cutoff_after(&session, active_root_effective_ts).await;
        assert!(matches!(
            session.table_checkpoint_readiness(table_id).unwrap(),
            CheckpointReadiness::Ready
        ));
    });
}

#[test]
fn test_checkpoint_readiness_uses_root_effective_ts_not_checkpoint_start_ts() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 120, "effective-delay").await;

        session.freeze_table(table_id, usize::MAX).await.unwrap();
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

        let checkpoint_ts = checkpoint_published(table_id, &mut session).await;
        let active_root = table_for_internal_assertion(&engine, table_id)
            .file()
            .active_root_unchecked()
            .clone();
        let effective_ts = active_root.effective_ts();
        assert!(checkpoint_ts < reader_sts.get());
        assert!(effective_ts > reader_sts.get());

        let readiness = session.table_checkpoint_readiness(table_id).unwrap();
        let CheckpointReadiness::Delayed { reason } = readiness else {
            panic!("expected effective timestamp delay, got {readiness:?}");
        };
        assert_eq!(reason.effective_ts, effective_ts);
        assert!(reason.min_active_sts <= reader_sts.get());
        assert!(reason.effective_ts >= reason.min_active_sts);

        let (_, mut reader) = reader_holder
            .borrow_mut()
            .take()
            .expect("reader hook should install an active transaction");
        reader
            .exec(async |stmt| {
                let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                let proof = rt.read_proof();
                let snapshot = table_for_internal_assertion(&engine, table_id)
                    .root_snapshot(&proof)
                    .unwrap();
                let _effects_addr = effects as *mut _;
                assert!(snapshot.root_ts() < rt.sts());
                assert_eq!(snapshot.effective_ts(), effective_ts);
                assert!(!snapshot.root_is_visible_to(rt.sts()));
                Ok(())
            })
            .await
            .unwrap();
        reader.commit().await.unwrap();
        wait_gc_cutoff_after(&session, effective_ts).await;
        assert!(matches!(
            session.table_checkpoint_readiness(table_id).unwrap(),
            CheckpointReadiness::Ready
        ));
    });
}

#[test]
fn test_checkpoint_requires_idle_session_before_delayed_outcome() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 120, "idle-before-delay").await;

        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let mut reader_session = engine.new_session().unwrap();
        let reader = reader_session.begin_trx().unwrap();
        let first_checkpoint_ts = checkpoint_published(table_id, &mut session).await;
        assert_eq!(
            table_for_internal_assertion(&engine, table_id)
                .file()
                .active_root_unchecked()
                .root_ts,
            first_checkpoint_ts
        );

        let checkpoint_trx = session.begin_trx().unwrap();
        assert!(session.in_trx().unwrap());
        assert!(matches!(
            session.table_checkpoint_readiness(table_id).unwrap(),
            CheckpointReadiness::Delayed { .. }
        ));

        let err = session.checkpoint_table(table_id).await.unwrap_err();
        assert_eq!(err.operation_error(), Some(OperationError::NotSupported));
        assert!(format!("{err:?}").contains("checkpoint requires idle session"));
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
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 120, "delayed-root").await;

        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let mut reader_session = engine.new_session().unwrap();
        let reader = reader_session.begin_trx().unwrap();
        checkpoint_published(table_id, &mut session).await;
        let table = table_for_internal_assertion(&engine, table_id);
        let root_after_first = table.file().active_root_unchecked().clone();
        let effective_ts_protected_by_reader = root_after_first.effective_ts();

        insert_rows(table_id, &mut session, 1_000, 80, "delayed-frozen").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let (frozen_pages, _) = table
            .collect_frozen_pages(&session.pool_guards())
            .await
            .unwrap();
        assert!(!frozen_pages.is_empty());
        let first_frozen_page = frozen_pages[0].page_id;
        let root_before_delay = table.file().active_root_unchecked().clone();

        let outcome = session.checkpoint_table(table_id).await.unwrap();
        let CheckpointOutcome::Delayed { reason } = outcome else {
            panic!("expected delayed checkpoint, got {outcome:?}");
        };
        assert_eq!(reason.effective_ts, effective_ts_protected_by_reader);
        assert_eq!(reason.min_active_sts, reader.sts());
        assert_root_metadata_unchanged(&root_before_delay, &table);

        let page_guard = table
            .mem
            .must_get_row_page_shared(&session.pool_guards(), first_frozen_page)
            .await
            .unwrap();
        let (ctx, _) = page_guard.ctx_and_page();
        assert_eq!(ctx.row_ver().unwrap().state(), RowPageState::Frozen);
        drop(page_guard);
        let (still_frozen_pages, _) = table
            .collect_frozen_pages(&session.pool_guards())
            .await
            .unwrap();
        assert_eq!(still_frozen_pages[0].page_id, first_frozen_page);

        reader.commit().await.unwrap();
        wait_gc_cutoff_after(&session, effective_ts_protected_by_reader).await;
        let checkpoint_ts = checkpoint_published(table_id, &mut session).await;
        let root_after_publish = table.file().active_root_unchecked().clone();
        assert_eq!(root_after_publish.root_ts, checkpoint_ts);
        assert!(root_after_publish.pivot_row_id > root_before_delay.pivot_row_id);
    });
}

#[test]
fn test_second_checkpoint_waits_for_previous_root_horizon() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 120, "second-delay").await;

        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let mut reader_session = engine.new_session().unwrap();
        let reader = reader_session.begin_trx().unwrap();
        let first_checkpoint_ts = checkpoint_published(table_id, &mut session).await;
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
        assert_eq!(reason.effective_ts, first_effective_ts);
        assert_eq!(reason.min_active_sts, reader.sts());
        assert_root_metadata_unchanged(
            &root_before_second,
            &table_for_internal_assertion(&engine, table_id),
        );

        reader.commit().await.unwrap();
        wait_gc_cutoff_after(&session, first_effective_ts).await;
        let second_checkpoint_ts = checkpoint_published(table_id, &mut session).await;
        assert!(second_checkpoint_ts > first_checkpoint_ts);
    });
}

#[test]
fn test_checkpoint_reachability_reclaims_obsolete_column_index_root() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 0, 120, "reachability-first").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;
        let first_root = table_for_internal_assertion(&engine, table_id)
            .file()
            .active_root_unchecked()
            .clone();
        let first_column_root = first_root.column_block_index_root;
        assert_ne!(first_column_root, SUPER_BLOCK_ID);

        insert_rows(table_id, &mut session, 1_000, 120, "reachability-second").await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;
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

        wait_gc_cutoff_after(&session, second_root.effective_ts()).await;
        checkpoint_published(table_id, &mut session).await;
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
fn test_checkpoint_rechecks_readiness_after_root_mutation_lease() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut checkpoint_session = engine.new_session().unwrap();
        let root_before = table_for_internal_assertion(&engine, table_id)
            .file()
            .active_root_unchecked()
            .clone();
        wait_gc_cutoff_after(&checkpoint_session, root_before.effective_ts()).await;

        let mut reader_session = engine.new_session().unwrap();
        let reader = reader_session.begin_trx().unwrap();
        assert!(matches!(
            checkpoint_session
                .table_checkpoint_readiness(table_id)
                .unwrap(),
            CheckpointReadiness::Ready
        ));

        let hook_table = table_for_internal_assertion(&engine, table_id);
        let hook_engine = engine.new_ref().unwrap();
        let root_before_trx = root_before.root_ts;
        set_test_checkpoint_after_readiness_hook(move || async move {
            let mut competing_session = hook_engine.new_session().unwrap();
            let checkpoint_ts =
                checkpoint_published(hook_table.table_id(), &mut competing_session).await;
            assert!(checkpoint_ts > root_before_trx);
        });

        let outcome = checkpoint_session.checkpoint_table(table_id).await.unwrap();
        let CheckpointOutcome::Delayed { reason } = outcome else {
            panic!("expected post-lease readiness delay, got {outcome:?}");
        };
        let root_after = table_for_internal_assertion(&engine, table_id)
            .file()
            .active_root_unchecked()
            .clone();
        assert!(root_after.root_ts > root_before.root_ts);
        assert_eq!(reason.effective_ts, root_after.effective_ts());
        assert_eq!(reason.min_active_sts, reader.sts());

        reader.commit().await.unwrap();
    });
}

#[test]
fn test_concurrent_checkpoint_table_returns_in_progress_cancellation() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut first_session = engine.new_session().unwrap();
        wait_checkpoint_ready(table_id, &first_session).await;

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
fn test_checkpoint_snapshot_consistency() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let name = "y".repeat(256);
        insert_rows(table_id, &mut session, 0, 120, &name).await;

        session.freeze_table(table_id, 1).await.unwrap();

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
        checkpoint_published(table_id, &mut checkpoint_session).await;

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
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let name = "retained-root".repeat(64);
        insert_rows(table_id, &mut session, 0, 120, &name).await;

        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let retained_root_ptr = table_for_internal_assertion(&engine, table_id)
            .file()
            .active_root_unchecked() as *const _ as usize;
        let drop_count_before = old_root_drop_count(retained_root_ptr);

        let mut read_session = engine.new_session().unwrap();
        let read_trx = read_session.begin_trx().unwrap();

        let mut checkpoint_session = engine.new_session().unwrap();
        checkpoint_published(table_id, &mut checkpoint_session).await;

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

        for _ in 0..50 {
            if old_root_drop_count(retained_root_ptr) > drop_count_before {
                break;
            }
            smol::Timer::after(Duration::from_millis(20)).await;
        }
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
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let table = engine
            .catalog()
            .get_table_now(table_id)
            .expect("test table should exist");
        let mut session = engine.new_session().unwrap();
        let name = "z".repeat(512);
        insert_rows_direct(table_id, &mut session, 0, 150, &name).await;

        session
            .freeze_table(table.table_id(), usize::MAX)
            .await
            .unwrap();
        checkpoint_published(table.table_id(), &mut session).await;

        let root_before = table.file().active_root_unchecked().clone();
        drop(table);

        let table_file = engine
            .table_fs
            .open_table_file(table_id, engine.disk_pool.clone_inner())
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

#[test]
fn test_checkpoint_heartbeat() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let name = "h".repeat(128);
        insert_rows(table_id, &mut session, 0, 40, &name).await;

        let root_before = table_for_internal_assertion(&engine, table_id)
            .file()
            .active_root_unchecked()
            .clone();
        checkpoint_published(table_id, &mut session).await;
        let root_after = table_for_internal_assertion(&engine, table_id)
            .file()
            .active_root_unchecked()
            .clone();

        assert_eq!(root_after.pivot_row_id, root_before.pivot_row_id);
        assert!(root_after.heap_redo_start_ts > root_before.heap_redo_start_ts);
        assert!(root_after.deletion_cutoff_ts > root_before.deletion_cutoff_ts);
        assert_eq!(
            root_after.column_block_index_root,
            root_before.column_block_index_root
        );
    });
}

#[test]
fn test_checkpoint_gc_verification() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let name = "g".repeat(1024);
        insert_rows(table_id, &mut session, 0, 200, &name).await;

        let allocated_before = engine.mem_pool.allocated();
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;
        let allocated_after = engine.mem_pool.allocated();
        let mut reclaimed = allocated_after < allocated_before;
        for _ in 0..20 {
            smol::Timer::after(Duration::from_millis(200)).await;
            let allocated_now = engine.mem_pool.allocated();
            if allocated_now < allocated_before {
                reclaimed = true;
                break;
            }
        }
        assert!(reclaimed, "row pages should be reclaimed after purge");
    });
}

#[test]
fn test_session_cached_insert_page_reuses_live_versioned_page() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();

        let mut trx = session.begin_trx().unwrap();
        let row_id = unwrap_insert_result(
            trx_insert_row_by_id(
                &mut trx,
                table_id,
                vec![Val::from(1), Val::from("cached-row")],
            )
            .await,
        );
        trx.commit().await.unwrap();

        let (cached_page, cached_row_id) = session.load_active_insert_page(table_id).unwrap();
        assert_eq!(cached_row_id, row_id);
        assert!(
            table_for_internal_assertion(&engine, table_id)
                .mem
                .get_row_page_versioned_shared(&session.pool_guards(), cached_page)
                .await
                .unwrap()
                .is_some()
        );
        session.save_active_insert_page(table_id, cached_page, cached_row_id);

        let mut trx = session.begin_trx().unwrap();
        let next_row_id = unwrap_insert_result(
            trx_insert_row_by_id(
                &mut trx,
                table_id,
                vec![Val::from(2), Val::from("still-cached")],
            )
            .await,
        );
        trx.commit().await.unwrap();

        let next_page_id = match table_for_internal_assertion(&engine, table_id)
            .find_row(&session.pool_guards(), next_row_id)
            .await
            .unwrap()
        {
            RowLocation::RowPage(page_id) => page_id,
            RowLocation::LwcBlock { .. } | RowLocation::NotFound => {
                panic!("row should still be in the in-memory row store")
            }
        };
        assert_eq!(next_page_id, cached_page.page_id);

        let (next_cached_page, next_cached_row_id) =
            session.load_active_insert_page(table_id).unwrap();
        assert_eq!(next_cached_row_id, next_row_id);
        assert_eq!(next_cached_page, cached_page);
    });
}

#[test]
fn test_stale_session_cached_insert_page_falls_back_after_checkpoint_gc() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();

        let mut trx = session.begin_trx().unwrap();
        let row_id = unwrap_insert_result(
            trx_insert_row_by_id(
                &mut trx,
                table_id,
                vec![Val::from(1), Val::from("cached-row")],
            )
            .await,
        );
        trx.commit().await.unwrap();

        let (cached_page, cached_row_id) = session.load_active_insert_page(table_id).unwrap();
        assert_eq!(cached_row_id, row_id);
        session.save_active_insert_page(table_id, cached_page, cached_row_id);

        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let mut checkpoint_session = engine.new_session().unwrap();
        checkpoint_published(table_id, &mut checkpoint_session).await;

        let mut reclaimed = false;
        for _ in 0..20 {
            if table_for_internal_assertion(&engine, table_id)
                .mem
                .get_row_page_versioned_shared(&session.pool_guards(), cached_page)
                .await
                .unwrap()
                .is_none()
            {
                reclaimed = true;
                break;
            }
            smol::Timer::after(Duration::from_millis(200)).await;
        }
        assert!(
            reclaimed,
            "row page should be reclaimed before repro insert"
        );

        let mut trx = session.begin_trx().unwrap();
        let post_gc_row_id = unwrap_insert_result(
            trx_insert_row_by_id(
                &mut trx,
                table_id,
                vec![Val::from(2), Val::from("post-gc-row")],
            )
            .await,
        );
        trx.commit().await.unwrap();

        let key = single_key(2i32);
        expect_select_committed(table_id, &mut session, &key, |vals| {
            assert_eq!(vals, vec![Val::from(2), Val::from("post-gc-row")]);
        })
        .await;

        let (next_cached_page, next_cached_row_id) =
            session.load_active_insert_page(table_id).unwrap();
        assert_eq!(next_cached_row_id, post_gc_row_id);
        assert_ne!(next_cached_page, cached_page);
    });
}

#[test]
fn test_validated_row_page_shared_result_rejects_stale_reused_page_range() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();

        let mut trx = session.begin_trx().unwrap();
        let stale_row_id = unwrap_insert_result(
            trx_insert_row_by_id(
                &mut trx,
                table_id,
                vec![Val::from(1), Val::from("cached-row")],
            )
            .await,
        );
        trx.commit().await.unwrap();

        let (stale_page, cached_row_id) = session.load_active_insert_page(table_id).unwrap();
        assert_eq!(cached_row_id, stale_row_id);

        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let mut checkpoint_session = engine.new_session().unwrap();
        checkpoint_published(table_id, &mut checkpoint_session).await;

        let mut reclaimed = false;
        for _ in 0..20 {
            if table_for_internal_assertion(&engine, table_id)
                .mem
                .get_row_page_versioned_shared(&session.pool_guards(), stale_page)
                .await
                .unwrap()
                .is_none()
            {
                reclaimed = true;
                break;
            }
            smol::Timer::after(Duration::from_millis(200)).await;
        }
        assert!(
            reclaimed,
            "row page should be reclaimed before stale-range validation"
        );

        let large = "r".repeat(48 * 1024);
        let mut reused_row_id = None;
        for key in 2..258 {
            let mut trx = session.begin_trx().unwrap();
            let row_id = unwrap_insert_result(
                trx_insert_row_by_id(
                    &mut trx,
                    table_id,
                    vec![Val::from(key), Val::from(&large[..])],
                )
                .await,
            );
            trx.commit().await.unwrap();
            match table_for_internal_assertion(&engine, table_id)
                .find_row(&session.pool_guards(), row_id)
                .await
                .unwrap()
            {
                RowLocation::RowPage(page_id) if page_id == stale_page.page_id => {
                    reused_row_id = Some(row_id);
                    break;
                }
                RowLocation::RowPage(..) => (),
                RowLocation::LwcBlock { .. } | RowLocation::NotFound => {
                    panic!("newly inserted row should stay in a row page")
                }
            }
        }
        let reused_row_id = reused_row_id.expect("stale row-page slot should be reused");

        let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();

        let stale_guard = table_for_internal_assertion(&engine, table_id)
            .accessor_with_layout(&layout)
            .try_get_validated_row_page_shared_result(
                &session.pool_guards(),
                stale_page.page_id,
                stale_row_id,
            )
            .await
            .unwrap();
        assert!(
            stale_guard.is_none(),
            "stale row id should not validate against the reused page range"
        );

        let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();

        let reused_guard = table_for_internal_assertion(&engine, table_id)
            .accessor_with_layout(&layout)
            .try_get_validated_row_page_shared_result(
                &session.pool_guards(),
                stale_page.page_id,
                reused_row_id,
            )
            .await
            .unwrap();
        assert!(
            reused_guard.is_some(),
            "reused row should validate on the reused page"
        );
    });
}

#[test]
fn test_mvcc_insert_surfaces_cached_insert_page_reload_error() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 9u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();

        let large = "r".repeat(48 * 1024);
        let mut trx = session.begin_trx().unwrap();
        let row_id = unwrap_insert_result(
            trx_insert_row_by_id(
                &mut trx,
                table_id,
                vec![Val::from(1), Val::from(&large[..])],
            )
            .await,
        );
        trx.commit().await.unwrap();

        let (cached_page, cached_row_id) = session.load_active_insert_page(table_id).unwrap();
        assert_eq!(cached_row_id, row_id);
        session.save_active_insert_page(table_id, cached_page, cached_row_id);

        let mut writer = engine.new_session().unwrap();
        for i in 2..258 {
            expect_insert_committed(
                table_id,
                &mut writer,
                vec![Val::from(i), Val::from(&large[..])],
            )
            .await;
            if test_frame_kind(
                &table_for_internal_assertion(&engine, table_id).mem.mem_pool,
                cached_page.page_id,
            ) == FrameKind::Evicted
            {
                break;
            }
        }
        let mut evicted = false;
        for _ in 0..20 {
            if test_frame_kind(
                &table_for_internal_assertion(&engine, table_id).mem.mem_pool,
                cached_page.page_id,
            ) == FrameKind::Evicted
            {
                evicted = true;
                break;
            }
            smol::Timer::after(Duration::from_millis(50)).await;
        }
        assert!(
            evicted,
            "cached insert page should be evicted before repro insert"
        );

        let mem_pool_file =
            StorageBackendFileIdentity::from_path(temp_dir.path().join("data.swp")).unwrap();
        let read_hook = Arc::new(FailingPageReadHook::for_page(
            mem_pool_file,
            cached_page.page_id,
            libc::EIO,
        ));
        let _hook = install_storage_backend_test_hook(read_hook.clone());
        let expected_error_kind = io::Error::from_raw_os_error(libc::EIO).kind();

        let mut trx = session.begin_trx().unwrap();
        let res = trx_insert_row_by_id(
            &mut trx,
            table_id,
            vec![Val::from(100), Val::from("reload-fails")],
        )
        .await;
        trx.rollback().await.unwrap();
        assert!(
            res.as_ref()
                .is_err_and(|err| err.completion_error()
                    == Some(CompletionErrorKind::Io(expected_error_kind))),
            "expected insert-page reload failure, got {res:?}"
        );
        assert!(
            read_hook.call_count() > 0,
            "cached insert page should reload from disk"
        );
    });
}

#[test]
fn test_mvcc_rollback_poisons_runtime_on_row_page_reload_error() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 9u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();

        let large = "r".repeat(48 * 1024);
        let mut trx = session.begin_trx().unwrap();
        let row_id = match trx_insert_row_by_id(
            &mut trx,
            table_id,
            vec![Val::from(1), Val::from(&large[..])],
        )
        .await
        {
            Ok(row_id) => row_id,
            res => panic!("res={res:?}"),
        };

        let (cached_page, cached_row_id) = session.load_active_insert_page(table_id).unwrap();
        assert_eq!(cached_row_id, row_id);

        let mut writer = engine.new_session().unwrap();
        for i in 2..258 {
            expect_insert_committed(
                table_id,
                &mut writer,
                vec![Val::from(i), Val::from(&large[..])],
            )
            .await;
            if test_frame_kind(
                &table_for_internal_assertion(&engine, table_id).mem.mem_pool,
                cached_page.page_id,
            ) == FrameKind::Evicted
            {
                break;
            }
        }
        let mut evicted = false;
        for _ in 0..20 {
            if test_frame_kind(
                &table_for_internal_assertion(&engine, table_id).mem.mem_pool,
                cached_page.page_id,
            ) == FrameKind::Evicted
            {
                evicted = true;
                break;
            }
            smol::Timer::after(Duration::from_millis(50)).await;
        }
        assert!(evicted, "rollback row page should be evicted before repro");

        let mem_pool_file =
            StorageBackendFileIdentity::from_path(temp_dir.path().join("data.swp")).unwrap();
        let read_hook = Arc::new(FailingPageReadHook::for_page(
            mem_pool_file,
            cached_page.page_id,
            libc::EIO,
        ));
        let _hook = install_storage_backend_test_hook(read_hook);

        assert!(
            trx.rollback().await.as_ref().is_err_and(|err| err
                .report()
                .downcast_ref::<FatalError>()
                .copied()
                == Some(FatalError::RollbackAccess))
        );
        assert!(
            engine
                .trx_sys
                .storage_poison_error()
                .as_ref()
                .is_some_and(|err| *err.current_context() == FatalError::RollbackAccess)
        );
        assert_eq!(engine.trx_sys.fatal_rollback_retention_len(), 1);
        assert!(
            engine
                .trx_sys
                .ensure_runtime_healthy()
                .as_ref()
                .is_err_and(|err| *err.current_context() == FatalError::RollbackAccess)
        );
        assert!(!session.in_trx().unwrap());
    });
}

#[test]
fn test_statement_rollback_poisons_runtime_on_row_page_reload_error() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 9u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let large = "r".repeat(48 * 1024);
        let mut writer = engine.new_session().unwrap();
        let mut trx = session.begin_trx().unwrap();
        let mut hook_guard = None;
        let mut read_hook = None;

        let res: Result<()> = trx
            .exec(async |stmt| {
                let row_id = match stmt_insert_row_by_id(
                    stmt,
                    table_id,
                    vec![Val::from(1), Val::from(&large[..])],
                )
                .await
                {
                    Ok(row_id) => row_id,
                    res => panic!("res={res:?}"),
                };

                let (cached_page, cached_row_id) =
                    session.load_active_insert_page(table_id).unwrap();
                assert_eq!(cached_row_id, row_id);

                for i in 2..258 {
                    expect_insert_committed(
                        table_id,
                        &mut writer,
                        vec![Val::from(i), Val::from(&large[..])],
                    )
                    .await;
                    if test_frame_kind(
                        &table_for_internal_assertion(&engine, table_id).mem.mem_pool,
                        cached_page.page_id,
                    ) == FrameKind::Evicted
                    {
                        break;
                    }
                }
                let mut evicted = false;
                for _ in 0..20 {
                    if test_frame_kind(
                        &table_for_internal_assertion(&engine, table_id).mem.mem_pool,
                        cached_page.page_id,
                    ) == FrameKind::Evicted
                    {
                        evicted = true;
                        break;
                    }
                    smol::Timer::after(Duration::from_millis(50)).await;
                }
                assert!(
                    evicted,
                    "statement rollback page should be evicted before repro"
                );

                let mem_pool_file =
                    StorageBackendFileIdentity::from_path(temp_dir.path().join("data.swp"))
                        .unwrap();
                let hook = Arc::new(FailingPageReadHook::for_page(
                    mem_pool_file,
                    cached_page.page_id,
                    libc::EIO,
                ));
                hook_guard = Some(install_storage_backend_test_hook(hook.clone()));
                read_hook = Some(hook);

                Err(Report::new(OperationError::NotSupported).into())
            })
            .await;

        assert!(
            res.as_ref()
                .is_err_and(|err| err.report().downcast_ref::<FatalError>().copied()
                    == Some(FatalError::RollbackAccess))
        );
        assert!(
            read_hook
                .as_ref()
                .is_some_and(|hook: &Arc<FailingPageReadHook>| hook.call_count() > 0),
            "statement rollback should reload the evicted page"
        );
        assert!(
            engine
                .trx_sys
                .storage_poison_error()
                .as_ref()
                .is_some_and(|err| *err.current_context() == FatalError::RollbackAccess)
        );
        assert_eq!(engine.trx_sys.fatal_rollback_retention_len(), 1);
        assert!(!session.in_trx().unwrap());

        let err = trx.rollback().await.unwrap_err();
        assert_eq!(
            err.downcast_ref::<InternalError>().copied(),
            Some(InternalError::ActiveTransactionDiscarded)
        );
    });
}

#[test]
fn test_checkpoint_error_rollback() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let name = "e".repeat(256);
        insert_rows(table_id, &mut session, 0, 80, &name).await;

        session.freeze_table(table_id, usize::MAX).await.unwrap();
        let root_before = table_for_internal_assertion(&engine, table_id)
            .file()
            .active_root_unchecked()
            .clone();

        set_test_force_lwc_build_error(true);
        let res = session.checkpoint_table(table_id).await;
        set_test_force_lwc_build_error(false);
        assert!(res.is_err());

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
    });
}

#[test]
fn test_build_in_memory_secondary_indexes_reclaims_staged_indexes_on_error() {
    smol::block_on(async {
        use super::mem_table::build_in_memory_secondary_indexes;
        use crate::buffer::FixedBufferPool;
        use crate::catalog::{
            ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableMetadata,
        };
        use crate::quiescent::QuiescentBox;
        use crate::value::ValKind;

        let pool_bytes = std::mem::size_of::<crate::buffer::frame::BufferFrame>()
            + std::mem::size_of::<crate::buffer::page::Page>();
        let pool = QuiescentBox::new(
            FixedBufferPool::with_capacity(PoolRole::Index, pool_bytes)
                .expect("one-page fixed index pool should be constructible"),
        );
        let pool_guard = (*pool).pool_guard();
        let metadata = TableMetadata::try_new(
            vec![ColumnSpec::new(
                "id",
                ValKind::I32,
                ColumnAttributes::empty(),
            )],
            vec![
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::empty()),
            ],
        )
        .expect("valid table metadata");

        let err = match build_in_memory_secondary_indexes(
            pool.guard(),
            &pool_guard,
            &metadata,
            TrxID::new(100),
        )
        .await
        {
            Ok(_) => panic!("second secondary-index construction should fail in one-page pool"),
            Err(err) => err,
        };
        assert_eq!(err.resource_error(), Some(ResourceError::BufferPoolFull));
        assert_eq!(pool.allocated(), 0);
    });
}

#[test]
fn test_user_secondary_indexes_evict_and_continue_serving_lookups() {
    smol::block_on(async {
        use crate::catalog::{
            ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableSpec,
        };
        use crate::value::ValKind;

        let temp_dir = TempDir::new().unwrap();
        let engine = EngineConfig::default()
            .storage_root(temp_dir.path())
            .index_buffer(16u64 * 1024 * 1024)
            .index_max_file_size(32u64 * 1024 * 1024)
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .max_mem_size(64u64 * 1024 * 1024)
                    .max_file_size(128u64 * 1024 * 1024),
            )
            .trx(TrxSysConfig::default().log_file_stem("redo_index_evict"))
            .build()
            .await
            .unwrap();

        let mut ddl_session = engine.new_session().unwrap();
        let mut index_specs = vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)];
        for _ in 0..12 {
            index_specs.push(IndexSpec::new(
                vec![IndexKey::new(1)],
                IndexAttributes::empty(),
            ));
        }
        let table_id = ddl_session
            .create_table(
                TableSpec::new(vec![
                    ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                    ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                ]),
                index_specs,
            )
            .await
            .unwrap();
        drop(ddl_session);

        let mut session = engine.new_session().unwrap();
        let mut inserted = Vec::new();

        for batch in 0..96usize {
            let mut trx = session.begin_trx().unwrap();
            for i in 0..64usize {
                let row_id = (batch * 64 + i) as i32;
                let seed = format!("{:08x}", row_id);
                let key = seed.repeat(64);
                let res = trx_insert_row_by_id(
                    &mut trx,
                    table_id,
                    vec![Val::from(row_id), Val::from(&key[..])],
                )
                .await;
                assert!(res.is_ok(), "res={res:?}");
                inserted.push((row_id, key));
            }
            trx.commit().await.unwrap();
            let stats = engine.index_pool.stats();
            if stats.completed_writes > 0 && stats.write_errors == 0 {
                break;
            }
        }

        for _ in 0..20 {
            let stats = engine.index_pool.stats();
            if stats.completed_writes > 0 && stats.write_errors == 0 {
                break;
            }
            smol::Timer::after(Duration::from_millis(50)).await;
        }

        let stats = engine.index_pool.stats();
        assert!(
            stats.completed_writes > 0 && stats.write_errors == 0,
            "user secondary-index pool should evict with a small index buffer"
        );

        for key_idx in [0usize, inserted.len() / 2, inserted.len() - 1] {
            let key = SelectKey::new(0, vec![Val::from(inserted[key_idx].0)]);
            let mut trx = session.begin_trx().unwrap();
            let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0, 1]).await;
            match res {
                Ok(SelectMvcc::Found(vals)) => {
                    assert_eq!(
                        vals,
                        vec![
                            Val::from(inserted[key_idx].0),
                            Val::from(&inserted[key_idx].1[..]),
                        ]
                    );
                }
                other => panic!("unexpected lookup result: {other:?}"),
            }
            trx.commit().await.unwrap();
        }

        let mut trx = session.begin_trx().unwrap();
        let visible_rows = scan_table_i32s(&mut trx, table_id).await.len();
        trx.commit().await.unwrap();
        assert_eq!(visible_rows, inserted.len());
    });
}

struct ColumnBlockIndexSnapshot {
    active_root: ActiveRoot,
    sparse_file: Arc<SparseFile>,
    disk_pool: QuiescentGuard<ReadonlyBufferPool>,
}

impl ColumnBlockIndexSnapshot {
    #[inline]
    fn index<'a>(&'a self, disk_pool_guard: &'a PoolGuard) -> ColumnBlockIndex<'a> {
        ColumnBlockIndex::new(
            self.active_root.column_block_index_root,
            self.active_root.pivot_row_id,
            FileKind::TableFile,
            &self.sparse_file,
            &self.disk_pool,
            disk_pool_guard,
        )
    }
}

#[inline]
async fn evictable_test_engine(
    temp_dir: &TempDir,
    max_mem_size: u64,
    log_file_stem: &str,
) -> Engine {
    EngineConfig::default()
        .storage_root(temp_dir.path().to_path_buf())
        .data_buffer(
            EvictableBufferPoolConfig::default()
                .role(PoolRole::Mem)
                .max_mem_size(max_mem_size)
                .max_file_size(128u64 * 1024 * 1024),
        )
        .trx(TrxSysConfig::default().log_file_stem(log_file_stem))
        .file(
            FileSystemConfig::default()
                .io_depth(16)
                .readonly_buffer_size(128 * 1024 * 1024)
                .data_dir("."),
        )
        .build()
        .await
        .unwrap()
}

#[inline]
async fn lightweight_test_engine(temp_dir: &TempDir, log_file_stem: &str) -> Engine {
    lightweight_test_engine_config(temp_dir.path().to_path_buf(), log_file_stem)
        .build()
        .await
        .unwrap()
}

#[inline]
async fn create_table2_for_test(engine: &Engine) -> TableID {
    crate::catalog::tests::table2(engine).await
}

#[inline]
async fn create_non_unique_name_table_for_test(engine: &Engine) -> TableID {
    let mut ddl_session = engine.new_session().unwrap();
    let table_id = ddl_session
        .create_table(
            TableSpec::new(vec![
                ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
            ]),
            vec![
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
            ],
        )
        .await
        .unwrap();
    drop(ddl_session);
    table_id
}

fn lightweight_test_engine_config(
    main_dir: impl Into<std::path::PathBuf>,
    log_file_stem: &str,
) -> EngineConfig {
    EngineConfig::default()
        .storage_root(main_dir)
        .meta_buffer(LIGHTWEIGHT_TEST_BUFFER_BYTES)
        .index_buffer(LIGHTWEIGHT_TEST_BUFFER_BYTES)
        .index_max_file_size(LIGHTWEIGHT_TEST_MAX_FILE_BYTES)
        .data_buffer(
            EvictableBufferPoolConfig::default()
                .role(PoolRole::Mem)
                .max_mem_size(LIGHTWEIGHT_TEST_BUFFER_BYTES)
                .max_file_size(LIGHTWEIGHT_TEST_MAX_FILE_BYTES),
        )
        .trx(
            TrxSysConfig::default()
                .io_depth_per_log(1)
                .log_file_stem(log_file_stem)
                .purge_threads(1),
        )
        .file(
            FileSystemConfig::default()
                .io_depth(1)
                .readonly_buffer_size(LIGHTWEIGHT_TEST_READONLY_BUFFER_BYTES)
                .data_dir("."),
        )
}

#[inline]
fn table_for_internal_assertion(engine: &Engine, table_id: TableID) -> Arc<Table> {
    engine
        .catalog()
        .get_table_now(table_id)
        .expect("test table should exist")
}

#[inline]
fn column_block_index_snapshot(engine: &Engine, table_id: TableID) -> ColumnBlockIndexSnapshot {
    let table = table_for_internal_assertion(engine, table_id);
    ColumnBlockIndexSnapshot {
        active_root: table.file().active_root_unchecked().clone(),
        sparse_file: Arc::clone(table.file().sparse_file()),
        disk_pool: table.disk_pool().clone(),
    }
}

#[inline]
async fn expect_insert_committed(table_id: TableID, session: &mut Session, insert: Vec<Val>) {
    let mut trx = session.begin_trx().unwrap();
    trx = expect_trx_insert(table_id, trx, insert).await;
    trx.commit().await.unwrap();
}

#[inline]
async fn expect_trx_insert(
    table_id: TableID,
    mut trx: Transaction,
    insert: Vec<Val>,
) -> Transaction {
    let res = trx_insert_row_by_id(&mut trx, table_id, insert).await;
    if res.is_err() {
        panic!("res={:?}", res);
    }
    trx
}

#[inline]
async fn expect_delete_committed(table_id: TableID, session: &mut Session, key: &SelectKey) {
    let mut trx = session.begin_trx().unwrap();
    trx = expect_trx_delete(table_id, trx, key).await;
    trx.commit().await.unwrap();
}

#[inline]
async fn expect_trx_delete(
    table_id: TableID,
    mut trx: Transaction,
    key: &SelectKey,
) -> Transaction {
    let res = trx_delete_row_by_id(&mut trx, table_id, key).await;
    if !matches!(res, Ok(DeleteMvcc::Deleted)) {
        panic!("res={:?}", res);
    }
    trx
}

#[inline]
async fn expect_update_committed(
    table_id: TableID,
    session: &mut Session,
    key: &SelectKey,
    update: Vec<UpdateCol>,
) {
    let mut trx = session.begin_trx().unwrap();
    trx = expect_trx_update(table_id, trx, key, update).await;
    trx.commit().await.unwrap();
}

#[inline]
async fn expect_trx_update(
    table_id: TableID,
    mut trx: Transaction,
    key: &SelectKey,
    update: Vec<UpdateCol>,
) -> Transaction {
    let res = trx_update_row_by_id(&mut trx, table_id, key, update).await;
    if !matches!(res, Ok(UpdateMvcc::Updated(_))) {
        panic!("res={:?}", res);
    }
    trx
}

#[inline]
async fn expect_select_committed<F: FnOnce(Vec<Val>)>(
    table_id: TableID,
    session: &mut Session,
    key: &SelectKey,
    action: F,
) {
    let mut trx = session.begin_trx().unwrap();
    trx = expect_trx_select(table_id, trx, key, action).await;
    trx.commit().await.unwrap();
}

#[inline]
async fn expect_select_not_found_committed(
    table_id: TableID,
    session: &mut Session,
    key: &SelectKey,
) {
    let mut trx = session.begin_trx().unwrap();
    trx = expect_trx_select_not_found(table_id, trx, key).await;
    trx.commit().await.unwrap();
}

#[inline]
async fn expect_trx_select_not_found(
    table_id: TableID,
    mut trx: Transaction,
    key: &SelectKey,
) -> Transaction {
    let res = trx_select_row_mvcc_by_id(&mut trx, table_id, key, &[0, 1]).await;
    assert!(matches!(res, Ok(SelectMvcc::NotFound)));
    trx
}

#[inline]
async fn expect_trx_select<F: FnOnce(Vec<Val>)>(
    table_id: TableID,
    mut trx: Transaction,
    key: &SelectKey,
    action: F,
) -> Transaction {
    let res = trx_select_row_mvcc_by_id(&mut trx, table_id, key, &[0, 1]).await;
    if !matches!(res, Ok(SelectMvcc::Found(_))) {
        panic!("res={:?}", res);
    }
    action(res.unwrap().unwrap_found());
    trx
}

async fn insert_one_row(table_id: TableID, session: &mut Session, values: Vec<Val>) -> RowID {
    let mut trx = session.begin_trx().unwrap();
    let insert = trx_insert_row_by_id(&mut trx, table_id, values).await;
    let Ok(row_id) = insert else {
        panic!("insert should succeed: {insert:?}");
    };
    trx.commit().await.unwrap();
    row_id
}

fn single_key<V: Into<Val>>(value: V) -> SelectKey {
    SelectKey {
        index_no: 0,
        vals: vec![value.into()],
    }
}

async fn scan_table_i32s(trx: &mut Transaction, table_id: TableID) -> Vec<i32> {
    let mut rows = Vec::new();
    trx.exec(async |stmt| {
        stmt.table_scan_mvcc(table_id, &[0], |vals| {
            rows.push(vals[0].as_i32().unwrap());
            true
        })
        .await?;
        Ok(())
    })
    .await
    .unwrap();
    rows.sort_unstable();
    rows
}

async fn scan_table_pairs(trx: &mut Transaction, table_id: TableID) -> Vec<(i32, String)> {
    let mut rows = Vec::new();
    trx.exec(async |stmt| {
        stmt.table_scan_mvcc(table_id, &[0, 1], |vals| {
            rows.push((
                vals[0].as_i32().unwrap(),
                vals[1].as_str().unwrap().to_string(),
            ));
            true
        })
        .await?;
        Ok(())
    })
    .await
    .unwrap();
    rows.sort_unstable();
    rows
}

fn drop_table_test_spec() -> (TableSpec, Vec<IndexSpec>) {
    (
        TableSpec::new(vec![
            ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
            ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
        ]),
        vec![
            IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
            IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
        ],
    )
}

fn lock_entry_count(engine: &Engine, owner: LockOwner) -> usize {
    debug_snapshot(engine.lock_manager())
        .entries
        .iter()
        .filter(|entry| entry.owner == owner)
        .count()
}

fn has_lock_entry(
    engine: &Engine,
    owner: LockOwner,
    resource: LockResource,
    mode: LockMode,
    state: LockDebugEntryState,
) -> bool {
    debug_snapshot(engine.lock_manager())
        .entries
        .iter()
        .any(|entry| {
            entry.owner == owner
                && entry.resource == resource
                && entry.mode == mode
                && entry.state == state
        })
}

fn has_lock_resource(engine: &Engine, owner: LockOwner, resource: LockResource) -> bool {
    debug_snapshot(engine.lock_manager())
        .entries
        .iter()
        .any(|entry| entry.owner == owner && entry.resource == resource)
}

async fn wait_for_no_lock_resource(engine: &Engine, owner: LockOwner, resource: LockResource) {
    for _ in 0..100 {
        if !has_lock_resource(engine, owner, resource) {
            return;
        }
        smol::Timer::after(Duration::from_millis(1)).await;
    }
    panic!("lock resource still present: owner={owner:?}, resource={resource:?}");
}

async fn wait_for_lock_entry(
    engine: &Engine,
    owner: LockOwner,
    resource: LockResource,
    mode: LockMode,
    state: LockDebugEntryState,
) {
    for _ in 0..100 {
        if has_lock_entry(engine, owner, resource, mode, state) {
            return;
        }
        smol::Timer::after(Duration::from_millis(1)).await;
    }
    panic!(
        "lock entry not observed: owner={owner:?}, resource={resource:?}, mode={mode:?}, state={state:?}"
    );
}

fn name_key(value: &str) -> SelectKey {
    SelectKey {
        index_no: 1,
        vals: vec![Val::from(value)],
    }
}

fn unwrap_insert_result(res: Result<RowID>) -> RowID {
    match res {
        Ok(row_id) => row_id,
        res => panic!("unexpected insert result: {res:?}"),
    }
}

fn active_secondary_root(table: &Table, index_no: usize) -> BlockID {
    table.file().active_root_unchecked().secondary_index_roots[index_no]
}

struct BoundUniqueIndexNo {
    layout: Arc<TableRuntimeLayout>,
    index_no: usize,
    root: BlockID,
}

impl UniqueIndex for BoundUniqueIndexNo {
    #[inline]
    async fn lookup(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        ts: TrxID,
    ) -> Result<Option<(RowID, bool)>> {
        let index = self.layout.secondary_index(self.index_no)?;
        index
            .bind_unique(self.root)?
            .lookup(pool_guard, key, ts)
            .await
    }

    #[inline]
    async fn insert_if_not_exists(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> Result<IndexInsert> {
        let index = self.layout.secondary_index(self.index_no)?;
        index
            .bind_unique(self.root)?
            .insert_if_not_exists(pool_guard, key, row_id, merge_if_match_deleted, ts)
            .await
    }

    #[inline]
    async fn compare_delete(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        old_row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool> {
        let index = self.layout.secondary_index(self.index_no)?;
        index
            .bind_unique(self.root)?
            .compare_delete(pool_guard, key, old_row_id, ignore_del_mask, ts)
            .await
    }

    #[inline]
    async fn compare_exchange(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> Result<crate::index::IndexCompareExchange> {
        let index = self.layout.secondary_index(self.index_no)?;
        index
            .bind_unique(self.root)?
            .compare_exchange(pool_guard, key, old_row_id, new_row_id, ts)
            .await
    }

    #[inline]
    async fn scan_values(
        &self,
        pool_guard: &PoolGuard,
        values: &mut Vec<RowID>,
        ts: TrxID,
    ) -> Result<()> {
        let index = self.layout.secondary_index(self.index_no)?;
        index
            .bind_unique(self.root)?
            .scan_values(pool_guard, values, ts)
            .await
    }
}

struct BoundNonUniqueIndexNo {
    layout: Arc<TableRuntimeLayout>,
    index_no: usize,
    root: BlockID,
}

impl NonUniqueIndex for BoundNonUniqueIndexNo {
    #[inline]
    async fn lookup(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        res: &mut Vec<RowID>,
        ts: TrxID,
    ) -> Result<()> {
        let index = self.layout.secondary_index(self.index_no)?;
        index
            .bind_non_unique(self.root)?
            .lookup(pool_guard, key, res, ts)
            .await
    }

    #[inline]
    async fn lookup_unique(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> Result<Option<bool>> {
        let index = self.layout.secondary_index(self.index_no)?;
        index
            .bind_non_unique(self.root)?
            .lookup_unique(pool_guard, key, row_id, ts)
            .await
    }

    #[inline]
    async fn insert_if_not_exists(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> Result<IndexInsert> {
        let index = self.layout.secondary_index(self.index_no)?;
        index
            .bind_non_unique(self.root)?
            .insert_if_not_exists(pool_guard, key, row_id, merge_if_match_deleted, ts)
            .await
    }

    #[inline]
    async fn mask_as_deleted(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool> {
        let index = self.layout.secondary_index(self.index_no)?;
        index
            .bind_non_unique(self.root)?
            .mask_as_deleted(pool_guard, key, row_id, ts)
            .await
    }

    #[inline]
    async fn mask_as_active(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool> {
        let index = self.layout.secondary_index(self.index_no)?;
        index
            .bind_non_unique(self.root)?
            .mask_as_active(pool_guard, key, row_id, ts)
            .await
    }

    #[inline]
    async fn compare_delete(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool> {
        let index = self.layout.secondary_index(self.index_no)?;
        index
            .bind_non_unique(self.root)?
            .compare_delete(pool_guard, key, row_id, ignore_del_mask, ts)
            .await
    }

    #[inline]
    async fn scan_values(
        &self,
        pool_guard: &PoolGuard,
        values: &mut Vec<RowID>,
        ts: TrxID,
    ) -> Result<()> {
        let index = self.layout.secondary_index(self.index_no)?;
        index
            .bind_non_unique(self.root)?
            .scan_values(pool_guard, values, ts)
            .await
    }
}

fn bound_unique_index_no(table: &Table, index_no: usize) -> BoundUniqueIndexNo {
    let layout = table.layout_snapshot();
    BoundUniqueIndexNo {
        layout,
        index_no,
        root: active_secondary_root(table, index_no),
    }
}

fn bound_non_unique_index_no(table: &Table, index_no: usize) -> BoundNonUniqueIndexNo {
    let layout = table.layout_snapshot();
    BoundNonUniqueIndexNo {
        layout,
        index_no,
        root: active_secondary_root(table, index_no),
    }
}

async fn assert_row_in_lwc(
    table: &Table,
    guards: &PoolGuards,
    key: &SelectKey,
    sts: TrxID,
) -> RowID {
    let index = bound_unique_index_no(table, key.index_no);
    let Some((row_id, _)) = index
        .lookup(guards.index_guard(), &key.vals, sts)
        .await
        .expect("index lookup should succeed")
    else {
        panic!("row should exist");
    };
    match table.find_row(guards, row_id).await.unwrap() {
        RowLocation::LwcBlock { .. } => row_id,
        RowLocation::RowPage(..) => panic!("row should be in lwc"),
        RowLocation::NotFound => panic!("row should exist"),
    }
}

async fn unique_disk_tree_lookup(
    table: &Table,
    guards: &PoolGuards,
    key: &SelectKey,
) -> Option<RowID> {
    let root = active_secondary_root(table, key.index_no);
    let layout = table.layout_snapshot();
    let tree = layout
        .secondary_index(key.index_no)
        .unwrap()
        .disk_runtime()
        .open_unique_at(root, guards.disk_guard())
        .unwrap();
    tree.lookup(&key.vals).await.unwrap()
}

async fn non_unique_disk_tree_prefix_scan(
    table: &Table,
    guards: &PoolGuards,
    key: &SelectKey,
) -> Vec<RowID> {
    let root = active_secondary_root(table, key.index_no);
    let layout = table.layout_snapshot();
    let tree = layout
        .secondary_index(key.index_no)
        .unwrap()
        .disk_runtime()
        .open_non_unique_at(root, guards.disk_guard())
        .unwrap();
    tree.prefix_scan_entries(&key.vals)
        .await
        .unwrap()
        .into_iter()
        .map(|(_, row_id)| row_id)
        .collect()
}

async fn assert_unique_index_entry(
    table: &Table,
    guards: &PoolGuards,
    key: &SelectKey,
    sts: TrxID,
    expected_row_id: RowID,
    expected_deleted: bool,
) {
    let index = bound_unique_index_no(table, key.index_no);
    let Some((row_id, deleted)) = index
        .lookup(guards.index_guard(), &key.vals, sts)
        .await
        .expect("index lookup should succeed")
    else {
        panic!("index entry should exist");
    };
    assert_eq!(row_id, expected_row_id);
    assert_eq!(deleted, expected_deleted);
}

fn delete_marker_ts(marker: DeleteMarker) -> TrxID {
    match marker {
        DeleteMarker::Committed(ts) => ts,
        DeleteMarker::Ref(status) => status.ts(),
    }
}

async fn wait_gc_cutoff_after(session: &Session, ts: TrxID) {
    let trx_sys = session.engine().trx_sys.clone();
    for _ in 0..50 {
        if trx_sys.calc_min_active_sts_for_gc() > ts {
            return;
        }
        smol::Timer::after(Duration::from_millis(20)).await;
    }
    panic!("GC cutoff did not advance past {ts}");
}

async fn wait_checkpoint_ready(table_id: TableID, session: &Session) {
    let mut last_delay = None;
    for _ in 0..50 {
        match session.table_checkpoint_readiness(table_id).unwrap() {
            CheckpointReadiness::Ready => return,
            CheckpointReadiness::Delayed { reason } => {
                last_delay = Some(reason);
                smol::Timer::after(Duration::from_millis(20)).await;
            }
            readiness => panic!("checkpoint readiness is not retryable: {readiness:?}"),
        }
    }
    panic!(
        "checkpoint readiness stayed delayed after retries by {:?}",
        last_delay.unwrap()
    );
}

async fn wait_path_exists(path: &str, expected: bool) {
    for _ in 0..250 {
        if std::path::Path::new(path).exists() == expected {
            return;
        }
        smol::Timer::after(Duration::from_millis(50)).await;
    }
    panic!("path existence did not become {expected}: {path}");
}

async fn checkpoint_published(table_id: TableID, session: &mut Session) -> TrxID {
    let mut last_delay = None;
    for _ in 0..50 {
        match session.checkpoint_table(table_id).await.unwrap() {
            CheckpointOutcome::Published { checkpoint_ts } => {
                return checkpoint_ts;
            }
            CheckpointOutcome::Delayed { reason } => {
                last_delay = Some(reason);
                smol::Timer::after(Duration::from_millis(20)).await;
            }
            CheckpointOutcome::Cancelled { reason } => {
                panic!("checkpoint should publish, cancelled by {reason:?}")
            }
        }
    }
    panic!(
        "checkpoint should publish, delayed after retries by {:?}",
        last_delay.unwrap()
    )
}

fn assert_root_metadata_unchanged(before: &crate::file::table_file::ActiveRoot, table: &Table) {
    let after = table.file().active_root_unchecked();
    assert_eq!(after.root_ts, before.root_ts);
    assert_eq!(after.meta_block_id, before.meta_block_id);
    assert_eq!(after.pivot_row_id, before.pivot_row_id);
    assert_eq!(after.heap_redo_start_ts, before.heap_redo_start_ts);
    assert_eq!(after.deletion_cutoff_ts, before.deletion_cutoff_ts);
    assert_eq!(
        after.column_block_index_root,
        before.column_block_index_root
    );
    assert_eq!(after.secondary_index_roots, before.secondary_index_roots);
}

fn corrupt_page_checksum(path: impl AsRef<std::path::Path>, page_id: impl Into<u64>) {
    let page_id = page_id.into();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let offset = page_id * COW_FILE_PAGE_SIZE as u64 + (COW_FILE_PAGE_SIZE as u64 - 1);
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xFF;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.flush().unwrap();
}

fn rewrite_page_with_checksum(
    path: impl AsRef<std::path::Path>,
    page_id: impl Into<u64>,
    rewrite: impl FnOnce(&mut [u8]),
) {
    let page_id = page_id.into();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let offset = page_id * COW_FILE_PAGE_SIZE as u64;
    let mut page = vec![0u8; COW_FILE_PAGE_SIZE];
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.read_exact(&mut page).unwrap();
    rewrite(&mut page);
    write_block_checksum(&mut page);
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&page).unwrap();
    file.flush().unwrap();
}

fn corrupt_leaf_delete_codec(
    path: impl AsRef<std::path::Path>,
    page_id: impl Into<u64>,
    prefix_idx: usize,
) {
    rewrite_page_with_checksum(path, page_id, |page| {
        let byte_offset = leaf_entry_payload_offset(page, prefix_idx) + 35;
        page[byte_offset] = 0xFF;
    });
}

fn corrupt_leaf_row_codec(
    path: impl AsRef<std::path::Path>,
    page_id: impl Into<u64>,
    prefix_idx: usize,
) {
    rewrite_page_with_checksum(path, page_id, |page| {
        let byte_offset = leaf_entry_payload_offset(page, prefix_idx) + 32;
        page[byte_offset] = 0;
    });
}

fn corrupt_leaf_block_id(
    path: impl AsRef<std::path::Path>,
    page_id: impl Into<u64>,
    prefix_idx: usize,
) {
    rewrite_page_with_checksum(path, page_id, |page| {
        let byte_offset = leaf_entry_payload_offset(page, prefix_idx);
        page[byte_offset..byte_offset + 8].copy_from_slice(&SUPER_BLOCK_ID.to_le_bytes());
    });
}

fn corrupt_leaf_short_delete_section_header(
    path: impl AsRef<std::path::Path>,
    page_id: impl Into<u64>,
    prefix_idx: usize,
) {
    const LEAF_ENTRY_ENTRY_LEN_OFFSET: usize = 28;
    const LEAF_ENTRY_ROW_SECTION_LEN_OFFSET: usize = 30;
    const LEAF_ENTRY_HEADER_SIZE: usize = 32;
    const TRUNCATED_DELETE_SECTION_LEN: usize = 4;

    rewrite_page_with_checksum(path, page_id, |page| {
        let byte_offset = leaf_entry_payload_offset(page, prefix_idx);
        let row_section_len = u16::from_le_bytes(
            page[byte_offset + LEAF_ENTRY_ROW_SECTION_LEN_OFFSET
                ..byte_offset + LEAF_ENTRY_ROW_SECTION_LEN_OFFSET + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        let truncated_entry_len =
            (LEAF_ENTRY_HEADER_SIZE + row_section_len + TRUNCATED_DELETE_SECTION_LEN) as u16;
        page[byte_offset + LEAF_ENTRY_ENTRY_LEN_OFFSET
            ..byte_offset + LEAF_ENTRY_ENTRY_LEN_OFFSET + 2]
            .copy_from_slice(&truncated_entry_len.to_le_bytes());
    });
}

fn leaf_entry_payload_offset(page: &[u8], prefix_idx: usize) -> usize {
    const SEARCH_TYPE_PLAIN: u8 = 1;
    const SEARCH_TYPE_DELTA_U32: u8 = 2;
    const SEARCH_TYPE_DELTA_U16: u8 = 3;

    let payload_start = BLOCK_INTEGRITY_HEADER_SIZE;
    let search_type = page[payload_start + COLUMN_BLOCK_HEADER_SIZE];
    let (prefix_size, entry_offset_offset) = match search_type {
        SEARCH_TYPE_PLAIN => (10usize, 8usize),
        SEARCH_TYPE_DELTA_U32 => (6usize, 4usize),
        SEARCH_TYPE_DELTA_U16 => (4usize, 2usize),
        _ => panic!("invalid leaf search type {search_type}"),
    };
    let prefix_offset = payload_start + COLUMN_BLOCK_LEAF_HEADER_SIZE + prefix_idx * prefix_size;
    let entry_offset = u16::from_le_bytes(
        page[prefix_offset + entry_offset_offset..prefix_offset + entry_offset_offset + 2]
            .try_into()
            .unwrap(),
    ) as usize;
    payload_start + COLUMN_BLOCK_LEAF_HEADER_SIZE + entry_offset
}

fn corrupt_lwc_row_shape_fingerprint(path: impl AsRef<std::path::Path>, page_id: impl Into<u64>) {
    rewrite_page_with_checksum(path, page_id, |page| {
        let payload_start = BLOCK_INTEGRITY_HEADER_SIZE;
        page[payload_start] ^= 0xFF;
    });
}

async fn insert_rows(table_id: TableID, session: &mut Session, start: i32, count: i32, name: &str) {
    let mut trx = session.begin_trx().unwrap();
    for i in 0..count {
        let insert = vec![Val::from(start + i), Val::from(name)];
        trx = expect_trx_insert(table_id, trx, insert).await;
    }
    trx.commit().await.unwrap();
}

async fn insert_rows_direct(
    table_id: TableID,
    session: &mut Session,
    start: i32,
    count: i32,
    name: &str,
) {
    let mut trx = session.begin_trx().unwrap();
    for i in 0..count {
        let insert = vec![Val::from(start + i), Val::from(name)];
        let res = trx_insert_row_by_id(&mut trx, table_id, insert).await;
        assert!(res.is_ok());
    }
    trx.commit().await.unwrap();
}

async fn delete_key_range_and_wait_gc_cutoff(
    table_id: TableID,
    session: &mut Session,
    start: i32,
    count: i32,
) {
    let mut max_delete_cts = TrxID::new(0);
    for i in 0..count {
        let mut trx = session.begin_trx().unwrap();
        trx = expect_trx_delete(table_id, trx, &single_key(start + i)).await;
        let cts = trx.commit().await.unwrap();
        max_delete_cts = max_delete_cts.max(cts);
    }
    wait_gc_cutoff_after(session, max_delete_cts).await;
}

#[test]
fn test_secondary_index_common() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = EngineConfig::default()
            .storage_root(main_dir)
            .data_buffer(EvictableBufferPoolConfig::default().role(crate::buffer::PoolRole::Mem))
            .trx(TrxSysConfig::default().log_file_stem("redo_secidx1"))
            .build()
            .await
            .unwrap();
        let table_id = table4(&engine).await;
        {
            let mut session = engine.new_session().unwrap();
            let user_read_set = &[0usize, 1];
            let mut trx = session.begin_trx().unwrap();
            for i in 0i32..5i32 {
                let res =
                    trx_insert_row_by_id(&mut trx, table_id, vec![Val::from(i), Val::from(i)])
                        .await;
                assert!(res.is_ok());
            }
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let key = SelectKey::new(0, vec![Val::from(1i32)]);
            let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, user_read_set).await;
            trx.commit().await.unwrap();
            assert!(matches!(res, Ok(SelectMvcc::Found(_))));

            let mut trx = session.begin_trx().unwrap();
            let key = SelectKey::new(1, vec![Val::from(1i32)]);
            let res = trx
                .exec(async |stmt| {
                    stmt.table_index_scan_mvcc(table_id, &key, user_read_set)
                        .await
                })
                .await;
            trx.commit().await.unwrap();
            assert!(res.unwrap().unwrap_rows().len() == 1);

            let mut trx = session.begin_trx().unwrap();
            let key = SelectKey::new(0, vec![Val::from(1i32)]);
            let update = vec![UpdateCol {
                idx: 1,
                val: Val::from(0i32),
            }];
            let res = trx_update_row_by_id(&mut trx, table_id, &key, update).await;
            trx.commit().await.unwrap();
            assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));

            let mut trx = session.begin_trx().unwrap();
            let key = SelectKey::new(1, vec![Val::from(0i32)]);
            let res = trx
                .exec(async |stmt| {
                    stmt.table_index_scan_mvcc(table_id, &key, user_read_set)
                        .await
                })
                .await;
            trx.commit().await.unwrap();
            assert!(res.unwrap().unwrap_rows().len() == 2);

            let mut trx = session.begin_trx().unwrap();
            let key = SelectKey::new(0, vec![Val::from(0i32)]);
            let res = trx_delete_row_by_id(&mut trx, table_id, &key).await;
            trx.commit().await.unwrap();
            assert!(matches!(res, Ok(DeleteMvcc::Deleted)));

            let mut trx = session.begin_trx().unwrap();
            let key = SelectKey::new(1, vec![Val::from(0i32)]);
            let res = trx
                .exec(async |stmt| {
                    stmt.table_index_scan_mvcc(table_id, &key, user_read_set)
                        .await
                })
                .await;
            _ = trx.commit().await.unwrap();
            assert!(res.unwrap().unwrap_rows().len() == 1);
        }
    })
}

#[test]
fn test_checkpoint_cancelled_while_table_metadata_change_active() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut checkpoint_session = engine.new_session().unwrap();

        wait_checkpoint_ready(table_id, &checkpoint_session).await;
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
fn test_runtime_layout_install_retires_removed_index_after_old_snapshot_drops() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let old_layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
        assert_eq!(old_layout.metadata().idx.active_index_count(), 1);

        let metadata_without_indexes = Arc::new(
            TableMetadata::try_new_with_next_index_no(
                vec![
                    ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                    ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                ],
                vec![],
                old_layout.metadata().idx.next_index_no(),
            )
            .unwrap(),
        );
        let mut inactive_slots: Vec<Option<Arc<SecondaryIndex<EvictableBufferPool>>>> =
            Vec::with_capacity(old_layout.index_slot_count());
        inactive_slots.resize_with(old_layout.index_slot_count(), || None);
        let new_layout = TableRuntimeLayout::new(
            old_layout.generation() + 1,
            metadata_without_indexes,
            inactive_slots.into_boxed_slice(),
        )
        .unwrap();

        let installed = table_for_internal_assertion(&engine, table_id)
            .install_runtime_layout(old_layout.generation(), new_layout)
            .unwrap();
        assert_eq!(old_layout.metadata().idx.active_index_count(), 1);
        assert_eq!(installed.metadata().idx.active_index_count(), 0);
        assert_eq!(
            installed.metadata().idx.next_index_no(),
            old_layout.metadata().idx.next_index_no()
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
        assert!(table_for_internal_assertion(&engine, table_id).has_retired_secondary_indexes());

        let guards = PoolGuards::builder()
            .push(PoolRole::Index, engine.index_pool.pool_guard())
            .build();
        assert_eq!(
            table_for_internal_assertion(&engine, table_id)
                .cleanup_retired_secondary_indexes(&guards)
                .await
                .unwrap(),
            0
        );
        drop(old_layout);
        assert_eq!(
            table_for_internal_assertion(&engine, table_id)
                .cleanup_retired_secondary_indexes(&guards)
                .await
                .unwrap(),
            1
        );
        assert!(!table_for_internal_assertion(&engine, table_id).has_retired_secondary_indexes());
    })
}

#[test]
fn test_dropped_unique_index_purge_delete_is_noop() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let key = single_key(1i32);
        let row_id = insert_one_row(
            table_id,
            &mut session,
            vec![Val::from(1i32), Val::from("name")],
        )
        .await;
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;
        let mut hold_session = engine.new_session().unwrap();
        let hold_trx = hold_session.begin_trx().unwrap();
        expect_delete_committed(table_id, &mut session, &key).await;
        let min_active_sts = delete_marker_ts(
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .get(row_id)
                .unwrap(),
        ) + 1;
        let index = bound_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            key.index_no,
        );
        assert!(matches!(
            index
                .lookup(
                    session.pool_guards().index_guard(),
                    &key.vals,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some((actual_row_id, true)) if actual_row_id == row_id
        ));

        session.drop_index(table_id, 0).await.unwrap();

        let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();

        let deleted = table_for_internal_assertion(&engine, table_id)
            .accessor_with_layout(&layout)
            .delete_index(&session.pool_guards(), &key, row_id, true, min_active_sts)
            .await
            .unwrap();
        assert!(!deleted);
        hold_trx.commit().await.unwrap();
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
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;
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

        wait_gc_cutoff_after(&session, after_drop_root.effective_ts()).await;
        checkpoint_published(table_id, &mut session).await;
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

#[test]
fn test_dropped_non_unique_index_purge_delete_is_noop() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine =
            evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique").await;
        let table_id = create_non_unique_name_table_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        let pk = single_key(1i32);
        let row_id = insert_one_row(
            table_id,
            &mut session,
            vec![Val::from(1i32), Val::from("name")],
        )
        .await;
        let key = name_key("name");
        session.freeze_table(table_id, usize::MAX).await.unwrap();
        checkpoint_published(table_id, &mut session).await;
        let mut hold_session = engine.new_session().unwrap();
        let hold_trx = hold_session.begin_trx().unwrap();
        expect_delete_committed(table_id, &mut session, &pk).await;
        let min_active_sts = delete_marker_ts(
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .get(row_id)
                .unwrap(),
        ) + 1;
        let index = bound_non_unique_index_no(
            &table_for_internal_assertion(&engine, table_id),
            key.index_no,
        );
        assert!(matches!(
            index
                .lookup_unique(
                    session.pool_guards().index_guard(),
                    &key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap(),
            Some(false)
        ));

        session
            .drop_index(table_id, key.index_no as IndexNo)
            .await
            .unwrap();

        let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();

        let deleted = table_for_internal_assertion(&engine, table_id)
            .accessor_with_layout(&layout)
            .delete_index(&session.pool_guards(), &key, row_id, false, min_active_sts)
            .await
            .unwrap();
        assert!(!deleted);
        hold_trx.commit().await.unwrap();
    })
}

#[test]
fn test_runtime_layout_install_rejects_shrinking_index_slots() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
        let table_id = create_table2_for_test(&engine).await;
        let old_layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
        assert_eq!(old_layout.index_slot_count(), 1);

        let shrinking_metadata = Arc::new(
            TableMetadata::try_new(
                vec![
                    ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                    ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                ],
                vec![],
            )
            .expect("valid table metadata"),
        );
        let shrinking_layout = TableRuntimeLayout::new(
            old_layout.generation() + 1,
            shrinking_metadata,
            Vec::<Option<Arc<SecondaryIndex<EvictableBufferPool>>>>::new().into_boxed_slice(),
        )
        .unwrap();

        let result = table_for_internal_assertion(&engine, table_id)
            .install_runtime_layout(old_layout.generation(), shrinking_layout);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            format!("{err:?}").contains("new layout must not shrink sparse index slots"),
            "{err:?}"
        );
        assert_eq!(
            table_for_internal_assertion(&engine, table_id)
                .layout_snapshot()
                .generation(),
            old_layout.generation()
        );
    })
}

#[test]
fn test_secondary_index_rollback() {
    smol::block_on(async {
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = EngineConfig::default()
            .storage_root(main_dir)
            .data_buffer(EvictableBufferPoolConfig::default().role(crate::buffer::PoolRole::Mem))
            .trx(TrxSysConfig::default().log_file_stem("redo_secidx2"))
            .build()
            .await
            .unwrap();
        let table_id = table4(&engine).await;
        {
            let mut session = engine.new_session().unwrap();
            let user_read_set = &[0usize, 1];
            let mut trx = session.begin_trx().unwrap();
            for i in 0i32..5i32 {
                let res =
                    trx_insert_row_by_id(&mut trx, table_id, vec![Val::from(i), Val::from(i)])
                        .await;
                assert!(res.is_ok());
            }
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let res =
                trx_insert_row_by_id(&mut trx, table_id, vec![Val::from(5i32), Val::from(5i32)])
                    .await;
            assert!(res.is_ok());
            trx.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let key = SelectKey::new(0, vec![Val::from(5i32)]);
            let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, user_read_set).await;
            trx.commit().await.unwrap();
            assert!(matches!(res, Ok(SelectMvcc::NotFound)));

            let mut trx = session.begin_trx().unwrap();
            let key = SelectKey::new(0, vec![Val::from(1i32)]);
            let update = vec![UpdateCol {
                idx: 1,
                val: Val::from(0i32),
            }];
            let res = trx_update_row_by_id(&mut trx, table_id, &key, update).await;
            assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
            trx.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let key = SelectKey::new(0, vec![Val::from(1i32)]);
            let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, user_read_set).await;
            trx.commit().await.unwrap();
            assert!(matches!(res, Ok(SelectMvcc::Found(_))));
            let vals = res.unwrap().unwrap_found();
            assert!(vals[0] == Val::from(1i32) && vals[1] == Val::from(1i32));

            let mut trx = session.begin_trx().unwrap();
            let key = SelectKey::new(0, vec![Val::from(0i32)]);
            let res = trx_delete_row_by_id(&mut trx, table_id, &key).await;
            assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
            trx.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let key = SelectKey::new(0, vec![Val::from(0i32)]);
            let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, user_read_set).await;
            trx.commit().await.unwrap();
            assert!(matches!(res, Ok(SelectMvcc::Found(_))));
            let vals = res.unwrap().unwrap_found();
            assert!(vals[0] == Val::from(0i32) && vals[1] == Val::from(0i32));

            let mut trx = session.begin_trx().unwrap();
            let key = SelectKey::new(0, vec![Val::from(3i32)]);
            let res = trx_delete_row_by_id(&mut trx, table_id, &key).await;
            assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
            let res =
                trx_insert_row_by_id(&mut trx, table_id, vec![Val::from(3), Val::from(3)]).await;
            assert!(res.is_ok());
            trx.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let key = SelectKey::new(0, vec![Val::from(3i32)]);
            let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, user_read_set).await;
            _ = trx.commit().await.unwrap();
            assert!(matches!(res, Ok(SelectMvcc::Found(_))));
            let vals = res.unwrap().unwrap_found();
            assert!(vals[0] == Val::from(3i32) && vals[1] == Val::from(3i32));
        }
    })
}
