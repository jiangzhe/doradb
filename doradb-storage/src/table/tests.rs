use super::{DeleteInternal, FrozenPage, InsertRowIntoPage, UpdateRowInplace};
use crate::buffer::BufferPool;
use crate::buffer::frame::FrameKind;
use crate::buffer::page::{PAGE_SIZE, PageID};
use crate::buffer::{PoolGuards, PoolRole, test_frame_kind};
use crate::catalog::tests::table4;
use crate::conf::{EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig};
use crate::engine::Engine;
use crate::error::{BlockCorruptionCause, BlockKind, Error, FileKind, Result, StoragePoisonSource};
use crate::file::block_integrity::{BLOCK_INTEGRITY_HEADER_SIZE, write_block_checksum};
use crate::file::cow_file::{COW_FILE_PAGE_SIZE, SUPER_BLOCK_ID, tests::old_root_drop_count};
use crate::index::{
    COLUMN_BLOCK_HEADER_SIZE, COLUMN_BLOCK_LEAF_HEADER_SIZE, ColumnBlockIndex, IndexInsert,
    NonUniqueIndex, RowLocation, UniqueIndex, load_entry_deletion_deltas,
};
use crate::io::{
    IOKind, StorageBackendFileIdentity, StorageBackendOp, StorageBackendTestHook,
    install_storage_backend_test_hook,
};
use crate::latch::LatchFallbackMode;
use crate::row::ops::{DeleteMvcc, InsertMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
use crate::row::{RowID, RowPage, RowRead};
use crate::session::Session;
use crate::table::{
    CheckpointOutcome, CheckpointReadiness, DeleteMarker, Table, TableAccess, TablePersistence,
    TableRecover,
};
use crate::trx::row::LockRowForWrite;
use crate::trx::undo::RowUndoKind;
use crate::trx::ver_map::RowPageState;
use crate::trx::{ActiveTrx, MAX_SNAPSHOT_TS, TrxID};
use crate::value::Val;
use std::cell::Cell;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tempfile::TempDir;

thread_local! {
    static TEST_FORCE_LWC_BUILD_ERROR: Cell<bool> = const { Cell::new(false) };
    static TEST_FORCE_SECONDARY_SIDECAR_ERROR: Cell<bool> = const { Cell::new(false) };
}

const LIGHTWEIGHT_TEST_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const LIGHTWEIGHT_TEST_MAX_FILE_BYTES: usize = 32 * 1024 * 1024;
const LIGHTWEIGHT_TEST_READONLY_BUFFER_BYTES: usize = 32 * 1024 * 1024;

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

    fn on_complete(&self, op: StorageBackendOp, res: &mut std::io::Result<usize>) {
        if self.matches(op) {
            *res = Err(std::io::Error::from_raw_os_error(self.errno));
        }
    }
}

#[test]
fn test_mvcc_insert_normal() {
    smol::block_on(async {
        const SIZE: i32 = 10000;

        let sys = TestSys::new_evictable().await;

        let mut session = sys.try_new_session().unwrap();
        {
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            trx.commit().await.unwrap();
        }
        {
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            for i in 16..SIZE {
                let key = SelectKey::new(0, vec![Val::from(i)]);
                trx = sys
                    .trx_select(trx, &key, |vals| {
                        assert!(vals.len() == 2);
                        assert!(vals[0] == Val::from(i));
                        let s = format!("{}", i);
                        assert!(vals[1] == Val::from(&s[..]));
                    })
                    .await;
            }
            let _ = trx.commit().await.unwrap();
        }

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_mvcc_insert_dup_key() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        // dup key
        {
            // insert [1, "hello"]
            let insert = vec![Val::from(1i32), Val::from("hello")];
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            trx = sys.trx_insert(trx, insert).await;
            trx.commit().await.unwrap();

            // insert [1, "world"]
            let insert = vec![Val::from(1i32), Val::from("world")];
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let mut stmt = trx.start_stmt();
            let res = stmt.insert_row(&sys.table, insert).await;
            assert!(matches!(res, Ok(InsertMvcc::DuplicateKey)));
            trx = stmt.fail().await.unwrap();
            trx.rollback().await.unwrap();
        }
        // write conflict
        {
            // insert [2, "hello"], but not commit
            let insert1 = vec![Val::from(2i32), Val::from("hello")];
            let mut trx1 = session.try_begin_trx().unwrap().unwrap();
            let mut stmt1 = trx1.start_stmt();
            let res = stmt1.insert_row(&sys.table, insert1).await;
            assert!(matches!(res, Ok(InsertMvcc::Inserted(_))));
            trx1 = stmt1.succeed();

            // begin concurrent transaction and insert [2, "world"]
            let mut session2 = sys.try_new_session().unwrap();
            let insert2 = vec![Val::from(2i32), Val::from("world")];
            let trx2 = session2.try_begin_trx().unwrap().unwrap();
            let mut stmt2 = trx2.start_stmt();
            let res = stmt2.insert_row(&sys.table, insert2).await;
            // still dup key because circuit breaker on index search.
            assert!(matches!(res, Ok(InsertMvcc::DuplicateKey)));
            stmt2.fail().await.unwrap().rollback().await.unwrap();
            drop(session2);

            trx1.commit().await.unwrap();
            drop(session);
        }

        sys.clean_all();
    });
}

#[test]
fn test_mvcc_update_normal() {
    smol::block_on(async {
        const SIZE: i32 = 1000;

        let sys = TestSys::new_evictable().await;
        {
            let mut session = sys.try_new_session().unwrap();
            // insert 1000 rows
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            trx.commit().await.unwrap();

            // update 1 row with short value
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let k1 = single_key(1i32);
            let s1 = "hello";
            let update1 = vec![UpdateCol {
                idx: 1,
                val: Val::from(s1),
            }];
            trx = sys.trx_update(trx, &k1, update1).await;
            trx.commit().await.unwrap();

            // update 1 row with long value
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let k2 = single_key(100i32);
            let s2: String = (0..50_000).map(|_| '1').collect();
            let update2 = vec![UpdateCol {
                idx: 1,
                val: Val::from(&s2[..]),
            }];
            trx = sys.trx_update(trx, &k2, update2).await;

            // lookup this updated value inside same transaction
            trx = sys
                .trx_select(trx, &k2, |row| {
                    assert!(row.len() == 2);
                    assert!(row[0] == k2.vals[0]);
                    assert!(row[1] == Val::from(&s2[..]));
                })
                .await;

            trx.commit().await.unwrap();

            // lookup with a new transaction
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            trx = sys
                .trx_select(trx, &k2, |row| {
                    assert!(row.len() == 2);
                    assert!(row[0] == k2.vals[0]);
                    assert!(row[1] == Val::from(&s2[..]));
                })
                .await;

            let _ = trx.commit().await.unwrap();
        }
        sys.clean_all();
    });
}

#[test]
fn test_mvcc_delete_normal() {
    smol::block_on(async {
        const SIZE: i32 = 1000;

        let sys = TestSys::new_evictable().await;
        {
            let mut session = sys.try_new_session().unwrap();
            // insert 1000 rows
            // let mut trx = session.begin_trx(trx_sys);
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            trx.commit().await.unwrap();

            // delete 1 row
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let k1 = single_key(1i32);
            trx = sys.trx_delete(trx, &k1).await;

            // lookup row in same transaction
            trx = sys.trx_select_not_found(trx, &k1).await;
            trx.commit().await.unwrap();

            // lookup row in new transaction
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let k1 = single_key(1i32);
            trx = sys.trx_select_not_found(trx, &k1).await;
            let _ = trx.commit().await.unwrap();
        }
        sys.clean_all();
    });
}

#[test]
fn test_column_delete_basic() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(1i32);
        let mut reader_session = sys.try_new_session().unwrap();
        let trx = reader_session.try_begin_trx().unwrap().unwrap();
        let _ = assert_row_in_lwc(&sys.table, reader_session.pool_guards(), &key, trx.sts).await;
        trx.commit().await.unwrap();

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let res = stmt.delete_row(&sys.table, &key).await;
        assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
        trx = stmt.succeed();
        trx.commit().await.unwrap();

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        trx = sys.trx_select_not_found(trx, &key).await;
        trx.commit().await.unwrap();

        drop(reader_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_read_uses_readonly_buffer_pool() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(1i32);
        let mut reader_session = sys.try_new_session().unwrap();
        let trx = reader_session.try_begin_trx().unwrap().unwrap();
        let _ = assert_row_in_lwc(&sys.table, reader_session.pool_guards(), &key, trx.sts).await;
        trx.commit().await.unwrap();

        let allocated_after_route = sys.engine.disk_pool.allocated();
        assert!(allocated_after_route >= 1);

        sys.new_trx_select(&mut session, &key, |vals| {
            assert_eq!(vals[0], Val::from(1i32));
            assert_eq!(vals[1], Val::from("name"));
        })
        .await;
        let allocated_after_first = sys.engine.disk_pool.allocated();
        assert!(allocated_after_first >= allocated_after_route);

        sys.new_trx_select(&mut session, &key, |vals| {
            assert_eq!(vals[0], Val::from(1i32));
            assert_eq!(vals[1], Val::from("name"));
        })
        .await;
        assert_eq!(sys.engine.disk_pool.allocated(), allocated_after_first);

        drop(reader_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_find_row_returns_resolved_lwc_page_location() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(1i32);
        let trx = session.try_begin_trx().unwrap().unwrap();
        let index = sys.table.sec_idx()[key.index_no].unique().unwrap();
        let (row_id, _) = index
            .lookup(session.pool_guards().index_guard(), &key.vals, trx.sts)
            .await
            .unwrap()
            .unwrap();

        let active_root = sys.table.file().active_root();
        let column_index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.file().file_kind(),
            sys.table.file().sparse_file(),
            sys.table.disk_pool(),
            session.pool_guards().disk_guard(),
        );
        let resolved = column_index
            .locate_and_resolve_row(row_id)
            .await
            .unwrap()
            .unwrap();

        match sys.table.find_row(session.pool_guards(), row_id).await {
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_select_surfaces_persisted_corruption() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(1i32);
        let trx = session.try_begin_trx().unwrap().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, trx.sts).await;
        trx.commit().await.unwrap();

        let active_root = sys.table.file().active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.file().file_kind(),
            sys.table.file().sparse_file(),
            sys.table.disk_pool(),
            session.pool_guards().disk_guard(),
        );
        let entry = index.locate_block(row_id).await.unwrap().unwrap();
        let block_id = entry.block_id();

        let table_file_path = sys.engine.table_fs.table_file_path(sys.table.table_id());
        corrupt_page_checksum(table_file_path, block_id);

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let stmt = trx.start_stmt();
        let res = stmt.select_row_mvcc(&sys.table, &key, &[0, 1]).await;
        match res {
            Err(Error::BlockCorrupted {
                file_kind: FileKind::TableFile,
                block_kind: BlockKind::LwcBlock,
                block_id: page_id,
                cause: BlockCorruptionCause::ChecksumMismatch,
            }) => assert_eq!(page_id, block_id),
            other => panic!("expected persisted LWC corruption, got {other:?}"),
        }
        trx = stmt.fail().await.unwrap();
        trx.rollback().await.unwrap();

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_select_surfaces_column_block_index_row_metadata_corruption() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 4, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(1i32);
        let trx = session.try_begin_trx().unwrap().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, trx.sts).await;
        trx.commit().await.unwrap();

        let active_root = sys.table.file().active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.file().file_kind(),
            sys.table.file().sparse_file(),
            sys.table.disk_pool(),
            session.pool_guards().disk_guard(),
        );
        let entry = index.locate_block(row_id).await.unwrap().unwrap();

        let table_file_path = sys.engine.table_fs.table_file_path(sys.table.table_id());
        corrupt_leaf_row_codec(table_file_path, entry.leaf_page_id, 0);
        let _ = sys
            .table
            .disk_pool()
            .invalidate_block_id(sys.table.file().sparse_file().file_id(), entry.leaf_page_id);

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let stmt = trx.start_stmt();
        let res = stmt.select_row_mvcc(&sys.table, &key, &[0, 1]).await;
        match res {
            Err(Error::BlockCorrupted {
                file_kind: FileKind::TableFile,
                block_kind: BlockKind::ColumnBlockIndex,
                block_id: page_id,
                cause: BlockCorruptionCause::InvalidPayload,
            }) => assert_eq!(page_id, entry.leaf_page_id),
            other => panic!("expected persisted column-block-index corruption, got {other:?}"),
        }
        trx = stmt.fail().await.unwrap();
        trx.rollback().await.unwrap();

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_select_surfaces_column_block_index_zero_block_id_corruption() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 4, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(1i32);
        let trx = session.try_begin_trx().unwrap().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, trx.sts).await;
        trx.commit().await.unwrap();

        let active_root = sys.table.file().active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.file().file_kind(),
            sys.table.file().sparse_file(),
            sys.table.disk_pool(),
            session.pool_guards().disk_guard(),
        );
        let entry = index.locate_block(row_id).await.unwrap().unwrap();

        let table_file_path = sys.engine.table_fs.table_file_path(sys.table.table_id());
        corrupt_leaf_block_id(table_file_path, entry.leaf_page_id, 0);
        let _ = sys
            .table
            .disk_pool()
            .invalidate_block_id(sys.table.file().sparse_file().file_id(), entry.leaf_page_id);

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let stmt = trx.start_stmt();
        let res = stmt.select_row_mvcc(&sys.table, &key, &[0, 1]).await;
        match res {
            Err(Error::BlockCorrupted {
                file_kind: FileKind::TableFile,
                block_kind: BlockKind::ColumnBlockIndex,
                block_id: page_id,
                cause: BlockCorruptionCause::InvalidPayload,
            }) => assert_eq!(page_id, entry.leaf_page_id),
            other => panic!("expected persisted column-block-index corruption, got {other:?}"),
        }
        trx = stmt.fail().await.unwrap();
        trx.rollback().await.unwrap();

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_select_surfaces_row_shape_fingerprint_mismatch_corruption() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 4, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(1i32);
        let trx = session.try_begin_trx().unwrap().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, trx.sts).await;
        trx.commit().await.unwrap();

        let active_root = sys.table.file().active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.file().file_kind(),
            sys.table.file().sparse_file(),
            sys.table.disk_pool(),
            session.pool_guards().disk_guard(),
        );
        let entry = index.locate_block(row_id).await.unwrap().unwrap();

        let table_file_path = sys.engine.table_fs.table_file_path(sys.table.table_id());
        corrupt_lwc_row_shape_fingerprint(table_file_path, entry.block_id());
        let _ = sys
            .table
            .disk_pool()
            .invalidate_block_id(sys.table.file().sparse_file().file_id(), entry.block_id());

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let stmt = trx.start_stmt();
        let res = stmt.select_row_mvcc(&sys.table, &key, &[0, 1]).await;
        match res {
            Err(Error::BlockCorrupted {
                file_kind: FileKind::TableFile,
                block_kind: BlockKind::LwcBlock,
                block_id: page_id,
                cause: BlockCorruptionCause::InvalidPayload,
            }) => assert_eq!(page_id, entry.block_id()),
            other => panic!("expected persisted LWC invalid-payload corruption, got {other:?}"),
        }
        trx = stmt.fail().await.unwrap();
        trx.rollback().await.unwrap();

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_column_delete_rollback() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(2i32);
        let mut reader_session = sys.try_new_session().unwrap();
        let trx = reader_session.try_begin_trx().unwrap().unwrap();
        let old_row_id =
            assert_row_in_lwc(&sys.table, reader_session.pool_guards(), &key, trx.sts).await;
        trx.commit().await.unwrap();

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let res = stmt.delete_row(&sys.table, &key).await;
        assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
        assert_unique_index_entry(
            &sys.table,
            session.pool_guards(),
            &key,
            stmt.trx.sts,
            old_row_id,
            true,
        )
        .await;
        trx = stmt.succeed();
        trx.rollback().await.unwrap();
        assert_unique_index_entry(
            &sys.table,
            session.pool_guards(),
            &key,
            MAX_SNAPSHOT_TS,
            old_row_id,
            false,
        )
        .await;

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        trx = sys
            .trx_select(trx, &key, |row| {
                assert_eq!(row[0], Val::from(2i32));
            })
            .await;
        trx.commit().await.unwrap();

        drop(reader_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_column_delete_rollback_after_checkpoint() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;

        let key = single_key(3i32);
        let mut trx_delete = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx_delete.start_stmt();
        let res = stmt.delete_row(&sys.table, &key).await;
        assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
        trx_delete = stmt.succeed();

        sys.table.freeze(&session, usize::MAX).await;
        let mut checkpoint_session = sys.try_new_session().unwrap();
        checkpoint_published(&sys.table, &mut checkpoint_session).await;

        let mut reader_session = sys.try_new_session().unwrap();
        let trx = reader_session.try_begin_trx().unwrap().unwrap();
        let _ = assert_row_in_lwc(&sys.table, reader_session.pool_guards(), &key, trx.sts).await;
        trx.commit().await.unwrap();

        let stmt = trx_delete.start_stmt();
        let res = stmt.select_row_mvcc(&sys.table, &key, &[0, 1]).await;
        assert!(matches!(res, Ok(SelectMvcc::NotFound)));
        trx_delete = stmt.succeed();
        trx_delete.rollback().await.unwrap();

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        trx = sys
            .trx_select(trx, &key, |row| {
                assert_eq!(row[0], Val::from(3i32));
            })
            .await;
        trx.commit().await.unwrap();

        drop(reader_session);
        drop(checkpoint_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_column_delete_write_conflict() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(4i32);
        let trx = session.try_begin_trx().unwrap().unwrap();
        let _ = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, trx.sts).await;
        trx.commit().await.unwrap();

        let mut trx1 = session.try_begin_trx().unwrap().unwrap();
        let mut stmt1 = trx1.start_stmt();
        let res1 = stmt1.delete_row(&sys.table, &key).await;
        assert!(matches!(res1, Ok(DeleteMvcc::Deleted)));
        trx1 = stmt1.succeed();

        let mut session2 = sys.try_new_session().unwrap();
        let mut trx2 = session2.try_begin_trx().unwrap().unwrap();
        let mut stmt2 = trx2.start_stmt();
        let res2 = stmt2.delete_row(&sys.table, &key).await;
        assert!(matches!(res2, Ok(DeleteMvcc::WriteConflict)));
        trx2 = stmt2.fail().await.unwrap();
        trx2.rollback().await.unwrap();
        drop(session2);

        trx1.rollback().await.unwrap();

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_column_delete_mvcc_visibility() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(5i32);
        let trx = session.try_begin_trx().unwrap().unwrap();
        let _ = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, trx.sts).await;
        trx.commit().await.unwrap();

        let mut reader_session = sys.try_new_session().unwrap();
        let mut trx_reader = reader_session.try_begin_trx().unwrap().unwrap();

        let mut delete_session = sys.try_new_session().unwrap();
        let mut trx_delete = delete_session.try_begin_trx().unwrap().unwrap();
        let mut stmt_delete = trx_delete.start_stmt();
        let res = stmt_delete.delete_row(&sys.table, &key).await;
        assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
        trx_delete = stmt_delete.succeed();
        trx_delete.commit().await.unwrap();

        trx_reader = sys
            .trx_select(trx_reader, &key, |row| {
                assert_eq!(row[0], Val::from(5i32));
            })
            .await;
        trx_reader.commit().await.unwrap();

        let mut trx_new = session.try_begin_trx().unwrap().unwrap();
        trx_new = sys.trx_select_not_found(trx_new, &key).await;
        trx_new.commit().await.unwrap();

        drop(delete_session);
        drop(reader_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_delete_unique_conflicts_when_delete_committed_after_snapshot() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(5i32);
        let mut writer_session = sys.try_new_session().unwrap();
        let mut writer = writer_session.try_begin_trx().unwrap().unwrap();
        let writer_sts = writer.sts;
        let row_id =
            assert_row_in_lwc(&sys.table, writer_session.pool_guards(), &key, writer_sts).await;

        sys.new_trx_delete(&mut session, &key).await;
        let delete_cts = delete_marker_ts(sys.table.deletion_buffer().get(row_id).unwrap());
        assert!(delete_cts > writer_sts);

        writer = sys
            .trx_select(writer, &key, |row| {
                assert_eq!(row, vec![Val::from(5i32), Val::from("name")]);
            })
            .await;

        let mut stmt = writer.start_stmt();
        let res = stmt.delete_row(&sys.table, &key).await;
        assert!(matches!(res, Ok(DeleteMvcc::WriteConflict)));
        let writer = stmt.fail().await.unwrap();
        writer.rollback().await.unwrap();

        sys.new_trx_select_not_found(&mut session, &key).await;

        drop(writer_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_update_unique_same_key_reinserts_hot_and_preserves_old_snapshot() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 4, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(1i32);
        let mut old_reader_session = sys.try_new_session().unwrap();
        let mut old_reader = old_reader_session.try_begin_trx().unwrap().unwrap();
        let old_row_id = assert_row_in_lwc(
            &sys.table,
            old_reader_session.pool_guards(),
            &key,
            old_reader.sts,
        )
        .await;

        let mut writer = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = writer.start_stmt();
        let res = stmt
            .update_row(
                &sys.table,
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
            &sys.table,
            session.pool_guards(),
            &key,
            stmt.trx.sts,
            new_row_id,
            false,
        )
        .await;
        assert!(matches!(
            sys.table.find_row(session.pool_guards(), new_row_id).await,
            RowLocation::RowPage(_)
        ));
        match sys.table.deletion_buffer().get(old_row_id).unwrap() {
            DeleteMarker::Ref(status) => {
                assert!(Arc::ptr_eq(&status, &stmt.trx.status()));
            }
            DeleteMarker::Committed(_) => panic!("update should hold an in-flight delete marker"),
        }

        let res = stmt.select_row_mvcc(&sys.table, &key, &[0, 1]).await;
        assert!(matches!(
            res,
            Ok(SelectMvcc::Found(vals))
                if vals == vec![Val::from(1i32), Val::from("updated")]
        ));
        old_reader = sys
            .trx_select(old_reader, &key, |vals| {
                assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
            })
            .await;

        writer = stmt.succeed();
        writer.commit().await.unwrap();

        sys.new_trx_select(&mut session, &key, |vals| {
            assert_eq!(vals, vec![Val::from(1i32), Val::from("updated")]);
        })
        .await;
        old_reader = sys
            .trx_select(old_reader, &key, |vals| {
                assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
            })
            .await;
        old_reader.commit().await.unwrap();

        drop(old_reader_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_update_unique_conflicts_when_delete_committed_after_snapshot() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(5i32);
        let mut writer_session = sys.try_new_session().unwrap();
        let mut writer = writer_session.try_begin_trx().unwrap().unwrap();
        let writer_sts = writer.sts;
        let row_id =
            assert_row_in_lwc(&sys.table, writer_session.pool_guards(), &key, writer_sts).await;

        sys.new_trx_delete(&mut session, &key).await;
        let delete_cts = delete_marker_ts(sys.table.deletion_buffer().get(row_id).unwrap());
        assert!(delete_cts > writer_sts);

        writer = sys
            .trx_select(writer, &key, |row| {
                assert_eq!(row, vec![Val::from(5i32), Val::from("name")]);
            })
            .await;

        let mut stmt = writer.start_stmt();
        let res = stmt
            .update_row(
                &sys.table,
                &key,
                vec![UpdateCol {
                    idx: 1,
                    val: Val::from("updated"),
                }],
            )
            .await;
        assert!(matches!(res, Ok(UpdateMvcc::WriteConflict)));
        let writer = stmt.fail().await.unwrap();
        writer.rollback().await.unwrap();

        sys.new_trx_select_not_found(&mut session, &key).await;

        drop(writer_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_update_unique_key_change_preserves_old_and_new_key_visibility() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 4, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let old_key = single_key(2i32);
        let new_key = single_key(20i32);
        let mut old_reader_session = sys.try_new_session().unwrap();
        let mut old_reader = old_reader_session.try_begin_trx().unwrap().unwrap();
        let old_row_id = assert_row_in_lwc(
            &sys.table,
            old_reader_session.pool_guards(),
            &old_key,
            old_reader.sts,
        )
        .await;

        let mut writer = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = writer.start_stmt();
        let res = stmt
            .update_row(
                &sys.table,
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
            &sys.table,
            session.pool_guards(),
            &old_key,
            stmt.trx.sts,
            old_row_id,
            true,
        )
        .await;
        assert_unique_index_entry(
            &sys.table,
            session.pool_guards(),
            &new_key,
            stmt.trx.sts,
            new_row_id,
            false,
        )
        .await;
        writer = stmt.succeed();
        writer.commit().await.unwrap();

        sys.new_trx_select_not_found(&mut session, &old_key).await;
        sys.new_trx_select(&mut session, &new_key, |vals| {
            assert_eq!(vals, vec![Val::from(20i32), Val::from("moved")]);
        })
        .await;

        old_reader = sys
            .trx_select(old_reader, &old_key, |vals| {
                assert_eq!(vals, vec![Val::from(2i32), Val::from("name")]);
            })
            .await;
        old_reader = sys.trx_select_not_found(old_reader, &new_key).await;
        old_reader.commit().await.unwrap();

        drop(old_reader_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_update_unique_duplicate_rolls_back_cold_marker_and_hot_insert() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 4, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(1i32);
        let duplicate_key = single_key(2i32);
        let trx = session.try_begin_trx().unwrap().unwrap();
        let old_row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, trx.sts).await;
        trx.commit().await.unwrap();

        let trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let res = stmt
            .update_row(
                &sys.table,
                &key,
                vec![UpdateCol {
                    idx: 0,
                    val: Val::from(2i32),
                }],
            )
            .await;
        assert!(matches!(res, Ok(UpdateMvcc::DuplicateKey)));
        let trx = stmt.fail().await.unwrap();
        trx.rollback().await.unwrap();

        assert!(sys.table.deletion_buffer().get(old_row_id).is_none());
        sys.new_trx_select(&mut session, &key, |vals| {
            assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
        })
        .await;
        sys.new_trx_select(&mut session, &duplicate_key, |vals| {
            assert_eq!(vals, vec![Val::from(2i32), Val::from("name")]);
        })
        .await;

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_update_unique_claims_committed_deleted_cold_owner_with_visibility_bridge() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 1, 2, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let old_key = single_key(1i32);
        let claimed_key = single_key(2i32);
        let mut old_reader_session = sys.try_new_session().unwrap();
        let mut old_reader = old_reader_session.try_begin_trx().unwrap().unwrap();
        let _ = assert_row_in_lwc(
            &sys.table,
            old_reader_session.pool_guards(),
            &claimed_key,
            old_reader.sts,
        )
        .await;

        sys.new_trx_delete(&mut session, &claimed_key).await;

        let mut gap_reader_session = sys.try_new_session().unwrap();
        let mut gap_reader = gap_reader_session.try_begin_trx().unwrap().unwrap();

        sys.new_trx_update(
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

        sys.new_trx_select(&mut session, &claimed_key, |vals| {
            assert_eq!(vals, vec![Val::from(2i32), Val::from("claimed")]);
        })
        .await;
        old_reader = sys
            .trx_select(old_reader, &claimed_key, |vals| {
                assert_eq!(vals, vec![Val::from(2i32), Val::from("name")]);
            })
            .await;
        gap_reader = sys.trx_select_not_found(gap_reader, &claimed_key).await;

        old_reader.commit().await.unwrap();
        gap_reader.commit().await.unwrap();

        drop(gap_reader_session);
        drop(old_reader_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_update_unique_rejects_cold_owner_deleted_after_snapshot() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 1, 2, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let old_key = single_key(1i32);
        let claimed_key = single_key(2i32);
        let mut writer_session = sys.try_new_session().unwrap();
        let mut writer = writer_session.try_begin_trx().unwrap().unwrap();
        let writer_sts = writer.sts;
        let claimed_row_id = assert_row_in_lwc(
            &sys.table,
            writer_session.pool_guards(),
            &claimed_key,
            writer_sts,
        )
        .await;

        sys.new_trx_delete(&mut session, &claimed_key).await;
        let delete_cts = delete_marker_ts(sys.table.deletion_buffer().get(claimed_row_id).unwrap());
        assert!(delete_cts > writer_sts);
        assert_unique_index_entry(
            &sys.table,
            writer_session.pool_guards(),
            &claimed_key,
            MAX_SNAPSHOT_TS,
            claimed_row_id,
            true,
        )
        .await;

        writer = sys
            .trx_select(writer, &claimed_key, |vals| {
                assert_eq!(vals, vec![Val::from(2i32), Val::from("name")]);
            })
            .await;

        let mut stmt = writer.start_stmt();
        let res = stmt
            .update_row(
                &sys.table,
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
        assert!(matches!(res, Ok(UpdateMvcc::DuplicateKey)));
        let writer = stmt.fail().await.unwrap();
        writer.rollback().await.unwrap();

        assert_unique_index_entry(
            &sys.table,
            session.pool_guards(),
            &claimed_key,
            MAX_SNAPSHOT_TS,
            claimed_row_id,
            true,
        )
        .await;
        sys.new_trx_select(&mut session, &old_key, |vals| {
            assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
        })
        .await;
        sys.new_trx_select_not_found(&mut session, &claimed_key)
            .await;

        drop(writer_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_update_unique_claim_rollback_restores_deleted_cold_owner() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 1, 2, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let old_key = single_key(1i32);
        let claimed_key = single_key(2i32);
        let mut old_reader_session = sys.try_new_session().unwrap();
        let mut old_reader = old_reader_session.try_begin_trx().unwrap().unwrap();
        let claimed_row_id = assert_row_in_lwc(
            &sys.table,
            old_reader_session.pool_guards(),
            &claimed_key,
            old_reader.sts,
        )
        .await;

        sys.new_trx_delete(&mut session, &claimed_key).await;
        assert_unique_index_entry(
            &sys.table,
            session.pool_guards(),
            &claimed_key,
            MAX_SNAPSHOT_TS,
            claimed_row_id,
            true,
        )
        .await;

        let writer = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = writer.start_stmt();
        let res = stmt
            .update_row(
                &sys.table,
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
            &sys.table,
            session.pool_guards(),
            &claimed_key,
            stmt.trx.sts,
            new_row_id,
            false,
        )
        .await;
        assert!(matches!(
            sys.table
                .find_row(session.pool_guards(), claimed_row_id)
                .await,
            RowLocation::LwcBlock { .. }
        ));

        // Keep the statement changes in the transaction so transaction rollback
        // exercises index undo before row undo. That is the path where the
        // claimed deleted owner must still resolve as RowLocation::LwcBlock.
        let writer = stmt.succeed();
        writer.rollback().await.unwrap();

        assert_unique_index_entry(
            &sys.table,
            session.pool_guards(),
            &claimed_key,
            MAX_SNAPSHOT_TS,
            claimed_row_id,
            true,
        )
        .await;
        sys.new_trx_select(&mut session, &old_key, |vals| {
            assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
        })
        .await;
        sys.new_trx_select_not_found(&mut session, &claimed_key)
            .await;
        old_reader = sys
            .trx_select(old_reader, &claimed_key, |vals| {
                assert_eq!(vals, vec![Val::from(2i32), Val::from("name")]);
            })
            .await;
        old_reader.commit().await.unwrap();

        drop(old_reader_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_update_unique_claim_rollback_drops_purgeable_deleted_cold_owner() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 1, 2, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let old_key = single_key(1i32);
        let claimed_key = single_key(2i32);
        let reader = session.try_begin_trx().unwrap().unwrap();
        let claimed_row_id =
            assert_row_in_lwc(&sys.table, session.pool_guards(), &claimed_key, reader.sts).await;
        reader.commit().await.unwrap();

        let index = sys.table.sec_idx()[claimed_key.index_no].unique().unwrap();
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
        let delete_cts = 1;
        sys.table
            .deletion_buffer()
            .put_committed(claimed_row_id, delete_cts)
            .unwrap();
        sys.new_trx_select_not_found(&mut session, &claimed_key)
            .await;

        let writer = session.try_begin_trx().unwrap().unwrap();
        assert!(delete_cts < writer.sts);
        assert!(
            sys.table
                .deletion_buffer()
                .delete_marker_is_globally_purgeable(claimed_row_id, writer.sts)
        );
        let mut stmt = writer.start_stmt();
        let res = stmt
            .update_row(
                &sys.table,
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
            &sys.table,
            session.pool_guards(),
            &claimed_key,
            stmt.trx.sts,
            new_row_id,
            false,
        )
        .await;
        assert!(matches!(
            sys.table
                .find_row(session.pool_guards(), claimed_row_id)
                .await,
            RowLocation::LwcBlock { .. }
        ));

        // This is the stale GC attempt from the original delete. While the
        // replacement claim owns the unique key, GC observes a row-id mismatch
        // and skips the entry, so rollback must not recreate that skipped
        // delete-masked owner.
        let deleted = sys
            .table
            .accessor()
            .delete_index(
                session.pool_guards(),
                &claimed_key,
                claimed_row_id,
                true,
                MAX_SNAPSHOT_TS,
            )
            .await
            .unwrap();
        assert!(!deleted);

        let writer = stmt.succeed();
        writer.rollback().await.unwrap();

        // The composite index may still fall through to the checkpointed cold
        // root; MVCC reads filter that stale cold owner through the committed
        // deletion marker.
        sys.new_trx_select(&mut session, &old_key, |vals| {
            assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
        })
        .await;
        sys.new_trx_select_not_found(&mut session, &claimed_key)
            .await;

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_persists_committed_cold_delete_markers() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(6i32);
        let reader = session.try_begin_trx().unwrap().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, reader.sts).await;
        reader.commit().await.unwrap();

        sys.new_trx_delete(&mut session, &key).await;
        let marker = sys.table.deletion_buffer().get(row_id).unwrap();
        let marker_ts = delete_marker_ts(marker);
        wait_gc_cutoff_after(&session, marker_ts).await;
        let active_root = sys.table.file().active_root();
        let index_before = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.file().file_kind(),
            sys.table.file().sparse_file(),
            sys.table.disk_pool(),
            session.pool_guards().disk_guard(),
        );
        let entry_before = index_before
            .locate_block(row_id)
            .await
            .unwrap()
            .expect("persisted entry should exist before delete checkpoint");

        checkpoint_published(&sys.table, &mut session).await;

        let active_root = sys.table.file().active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.file().file_kind(),
            sys.table.file().sparse_file(),
            sys.table.disk_pool(),
            session.pool_guards().disk_guard(),
        );
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
        let deltas = load_entry_deletion_deltas(&index, &entry).await.unwrap();
        let expected_delta = (row_id - entry.start_row_id) as u32;
        assert!(deltas.contains(&expected_delta));

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_publishes_unique_secondary_disk_tree_root() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 3, "name").await;

        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let active_root = sys.table.file().active_root();
        assert_ne!(active_root.secondary_index_roots[0], SUPER_BLOCK_ID);
        let reader = session.try_begin_trx().unwrap().unwrap();
        for key_value in 0..3 {
            let key = single_key(key_value);
            let row_id =
                assert_row_in_lwc(&sys.table, session.pool_guards(), &key, reader.sts).await;
            assert_eq!(
                unique_disk_tree_lookup(&sys.table, session.pool_guards(), &key).await,
                Some(row_id)
            );
        }
        reader.commit().await.unwrap();

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_publishes_non_unique_secondary_disk_tree_entries_across_lwc_splits() {
    smol::block_on(async {
        let sys = TestSys::new_evictable_with_non_unique_name_index().await;
        let mut session = sys.try_new_session().unwrap();
        let name = "split-name-".repeat(120);
        let row_count = 80;
        insert_rows(&sys, &mut session, 0, row_count, &name).await;

        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let name_key = name_key(&name);
        let row_ids =
            non_unique_disk_tree_prefix_scan(&sys.table, session.pool_guards(), &name_key).await;
        assert_eq!(row_ids.len(), row_count as usize);

        let first_key = single_key(0i32);
        let last_key = single_key(row_count - 1);
        let first_row_id = unique_disk_tree_lookup(&sys.table, session.pool_guards(), &first_key)
            .await
            .unwrap();
        let last_row_id = unique_disk_tree_lookup(&sys.table, session.pool_guards(), &last_key)
            .await
            .unwrap();
        let active_root = sys.table.file().active_root();
        let column_index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.file().file_kind(),
            sys.table.file().sparse_file(),
            sys.table.disk_pool(),
            session.pool_guards().disk_guard(),
        );
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_redundant_live_unique_entries() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        let row_count = 4;
        insert_rows(&sys, &mut session, 0, row_count, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let index = sys.table.sec_idx()[0].unique().unwrap();
        assert_eq!(
            index
                .scan_mem_entries(session.pool_guards().index_guard())
                .await
                .unwrap()
                .len(),
            row_count as usize
        );

        let stats = sys
            .table
            .cleanup_secondary_mem_indexes(&mut session)
            .await
            .unwrap();
        assert!(!session.in_trx());
        assert_eq!(stats.indexes.len(), 1);
        assert_eq!(stats.indexes[0].index_no, 0);
        assert!(stats.indexes[0].unique);
        assert_eq!(stats.indexes[0].scanned, row_count as usize);
        assert_eq!(stats.indexes[0].removed, row_count as usize);
        assert_eq!(stats.indexes[0].retained, 0);
        assert!(
            index
                .scan_mem_entries(session.pool_guards().index_guard())
                .await
                .unwrap()
                .is_empty()
        );

        for key_value in 0..row_count {
            let key = single_key(key_value);
            let disk_row_id = unique_disk_tree_lookup(&sys.table, session.pool_guards(), &key)
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_secondary_mem_index_cleanup_requires_idle_session() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        let trx = session.try_begin_trx().unwrap().unwrap();

        let err = sys
            .table
            .cleanup_secondary_mem_indexes(&mut session)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::NotSupported("secondary MemIndex cleanup requires idle session")
        ));
        assert!(session.in_trx());

        trx.rollback().await.unwrap();
        assert!(!session.in_trx());

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_redundant_live_non_unique_entries() {
    smol::block_on(async {
        let sys = TestSys::new_evictable_with_non_unique_name_index().await;
        let mut session = sys.try_new_session().unwrap();
        let row_count = 5;
        insert_rows(&sys, &mut session, 0, row_count, "same-name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let index = sys.table.sec_idx()[1].non_unique().unwrap();
        assert_eq!(
            index
                .scan_mem_entries(session.pool_guards().index_guard())
                .await
                .unwrap()
                .len(),
            row_count as usize
        );

        let stats = sys
            .table
            .cleanup_secondary_mem_indexes(&mut session)
            .await
            .unwrap();
        assert_eq!(stats.indexes.len(), 2);
        assert_eq!(stats.indexes[1].index_no, 1);
        assert!(!stats.indexes[1].unique);
        assert_eq!(stats.indexes[1].scanned, row_count as usize);
        assert_eq!(stats.indexes[1].removed, row_count as usize);
        assert_eq!(stats.indexes[1].retained, 0);
        assert!(
            index
                .scan_mem_entries(session.pool_guards().index_guard())
                .await
                .unwrap()
                .is_empty()
        );

        let key = name_key("same-name");
        let disk_rows =
            non_unique_disk_tree_prefix_scan(&sys.table, session.pool_guards(), &key).await;
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_secondary_mem_index_cleanup_retains_unique_delete_shadow_without_delete_proof() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 1, "name").await;

        let current_key = single_key(0i32);
        let stale_key = single_key(-1i32);
        let index = sys.table.sec_idx()[0].unique().unwrap();
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

        let stats = sys
            .table
            .cleanup_secondary_mem_indexes(&mut session)
            .await
            .unwrap();
        assert_eq!(stats.indexes[0].scanned, 2);
        assert_eq!(stats.indexes[0].removed, 0);
        assert_eq!(stats.indexes[0].retained, 2);
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_unique_delete_shadow_with_purgeable_marker() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 1, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let current_key = single_key(0i32);
        let stale_key = single_key(-1i32);
        let row_id = unique_disk_tree_lookup(&sys.table, session.pool_guards(), &current_key)
            .await
            .unwrap();
        let index = sys.table.sec_idx()[0].unique().unwrap();
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
        sys.table
            .deletion_buffer()
            .put_committed(row_id, 1)
            .unwrap();

        let stats = sys
            .table
            .cleanup_secondary_mem_indexes(&mut session)
            .await
            .unwrap();
        assert_eq!(stats.indexes[0].scanned, 2);
        assert_eq!(stats.indexes[0].removed, 2);
        assert_eq!(stats.indexes[0].retained, 0);
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_unique_delete_shadow_with_matching_cold_entry() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 1, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let current_key = single_key(0i32);
        let row_id = unique_disk_tree_lookup(&sys.table, session.pool_guards(), &current_key)
            .await
            .unwrap();
        let index = sys.table.sec_idx()[0].unique().unwrap();
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

        let stats = sys
            .table
            .cleanup_secondary_mem_indexes(&mut session)
            .await
            .unwrap();
        assert_eq!(stats.indexes[0].scanned, 1);
        assert_eq!(stats.indexes[0].removed, 0);
        assert_eq!(stats.indexes[0].retained, 1);
        let entries = index
            .scan_mem_entries(session.pool_guards().index_guard())
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].deleted);

        sys.table
            .deletion_buffer()
            .put_committed(row_id, 1)
            .unwrap();
        let stats = sys
            .table
            .cleanup_secondary_mem_indexes(&mut session)
            .await
            .unwrap();
        assert_eq!(stats.indexes[0].scanned, 1);
        assert_eq!(stats.indexes[0].removed, 1);
        assert_eq!(stats.indexes[0].retained, 0);
        assert!(
            index
                .scan_mem_entries(session.pool_guards().index_guard())
                .await
                .unwrap()
                .is_empty()
        );

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_unique_delete_shadow_when_cold_row_key_differs() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 1, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let current_key = single_key(0i32);
        let stale_key = single_key(-1i32);
        let row_id = unique_disk_tree_lookup(&sys.table, session.pool_guards(), &current_key)
            .await
            .unwrap();
        assert_eq!(
            unique_disk_tree_lookup(&sys.table, session.pool_guards(), &stale_key).await,
            None
        );
        let index = sys.table.sec_idx()[0].unique().unwrap();
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

        let stats = sys
            .table
            .cleanup_secondary_mem_indexes(&mut session)
            .await
            .unwrap();
        assert_eq!(stats.indexes[0].scanned, 2);
        assert_eq!(stats.indexes[0].removed, 2);
        assert_eq!(stats.indexes[0].retained, 0);
        assert!(
            index
                .scan_mem_entries(session.pool_guards().index_guard())
                .await
                .unwrap()
                .is_empty()
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_secondary_mem_index_cleanup_propagates_cold_delete_overlay_proof_error() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 1, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let current_key = single_key(0i32);
        let stale_key = single_key(-1i32);
        let row_id = unique_disk_tree_lookup(&sys.table, session.pool_guards(), &current_key)
            .await
            .unwrap();
        let block_id = {
            let active_root = sys.table.file().active_root();
            let column_index = ColumnBlockIndex::new(
                active_root.column_block_index_root,
                active_root.pivot_row_id,
                sys.table.file().file_kind(),
                sys.table.file().sparse_file(),
                sys.table.disk_pool(),
                session.pool_guards().disk_guard(),
            );
            column_index
                .locate_block(row_id)
                .await
                .unwrap()
                .unwrap()
                .block_id()
        };
        let index = sys.table.sec_idx()[0].unique().unwrap();
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

        let table_file_path = sys.engine.table_fs.table_file_path(sys.table.table_id());
        corrupt_lwc_row_shape_fingerprint(table_file_path, block_id);
        let _ = sys
            .table
            .disk_pool()
            .invalidate_block_id(sys.table.file().sparse_file().file_id(), block_id);

        let err = sys
            .table
            .cleanup_secondary_mem_indexes(&mut session)
            .await
            .unwrap_err();
        match err {
            Error::BlockCorrupted {
                file_kind: FileKind::TableFile,
                block_kind: BlockKind::LwcBlock,
                block_id: err_block_id,
                ..
            } => assert_eq!(err_block_id, block_id),
            other => panic!("expected LWC block corruption, got {other:?}"),
        }
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_secondary_mem_index_cleanup_retains_non_unique_delete_mark_without_delete_proof() {
    smol::block_on(async {
        let sys = TestSys::new_evictable_with_non_unique_name_index().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 1, "current").await;

        let pk = single_key(0i32);
        let row_id = sys.table.sec_idx()[0]
            .unique()
            .unwrap()
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
        let index = sys.table.sec_idx()[stale_key.index_no]
            .non_unique()
            .unwrap();
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

        let stats = sys
            .table
            .cleanup_secondary_mem_indexes(&mut session)
            .await
            .unwrap();
        assert_eq!(stats.indexes[1].scanned, 2);
        assert_eq!(stats.indexes[1].removed, 0);
        assert_eq!(stats.indexes[1].retained, 2);
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_non_unique_delete_mark_with_purgeable_marker() {
    smol::block_on(async {
        let sys = TestSys::new_evictable_with_non_unique_name_index().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 1, "current").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let pk = single_key(0i32);
        let row_id = unique_disk_tree_lookup(&sys.table, session.pool_guards(), &pk)
            .await
            .unwrap();
        let stale_key = name_key("stale");
        let index = sys.table.sec_idx()[stale_key.index_no]
            .non_unique()
            .unwrap();
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
        sys.table
            .deletion_buffer()
            .put_committed(row_id, 1)
            .unwrap();

        let stats = sys
            .table
            .cleanup_secondary_mem_indexes(&mut session)
            .await
            .unwrap();
        assert_eq!(stats.indexes[1].scanned, 2);
        assert_eq!(stats.indexes[1].removed, 2);
        assert_eq!(stats.indexes[1].retained, 0);
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_non_unique_delete_mark_with_matching_cold_entry() {
    smol::block_on(async {
        let sys = TestSys::new_evictable_with_non_unique_name_index().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 1, "current").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let pk = single_key(0i32);
        let row_id = unique_disk_tree_lookup(&sys.table, session.pool_guards(), &pk)
            .await
            .unwrap();
        let current_key = name_key("current");
        let index = sys.table.sec_idx()[current_key.index_no]
            .non_unique()
            .unwrap();
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

        let stats = sys
            .table
            .cleanup_secondary_mem_indexes(&mut session)
            .await
            .unwrap();
        assert_eq!(stats.indexes[1].scanned, 1);
        assert_eq!(stats.indexes[1].removed, 0);
        assert_eq!(stats.indexes[1].retained, 1);
        let entries = index
            .scan_mem_entries(session.pool_guards().index_guard())
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].deleted);

        sys.table
            .deletion_buffer()
            .put_committed(row_id, 1)
            .unwrap();
        let stats = sys
            .table
            .cleanup_secondary_mem_indexes(&mut session)
            .await
            .unwrap();
        assert_eq!(stats.indexes[1].scanned, 1);
        assert_eq!(stats.indexes[1].removed, 1);
        assert_eq!(stats.indexes[1].retained, 0);
        assert!(
            index
                .scan_mem_entries(session.pool_guards().index_guard())
                .await
                .unwrap()
                .is_empty()
        );

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_secondary_mem_index_cleanup_removes_non_unique_delete_mark_when_cold_row_key_differs() {
    smol::block_on(async {
        let sys = TestSys::new_evictable_with_non_unique_name_index().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 1, "current").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let pk = single_key(0i32);
        let current_key = name_key("current");
        let stale_key = name_key("stale");
        let row_id = unique_disk_tree_lookup(&sys.table, session.pool_guards(), &pk)
            .await
            .unwrap();
        assert!(
            non_unique_disk_tree_prefix_scan(&sys.table, session.pool_guards(), &stale_key)
                .await
                .is_empty()
        );
        assert_eq!(
            non_unique_disk_tree_prefix_scan(&sys.table, session.pool_guards(), &current_key).await,
            vec![row_id]
        );
        let index = sys.table.sec_idx()[stale_key.index_no]
            .non_unique()
            .unwrap();
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

        let stats = sys
            .table
            .cleanup_secondary_mem_indexes(&mut session)
            .await
            .unwrap();
        assert_eq!(stats.indexes[1].scanned, 2);
        assert_eq!(stats.indexes[1].removed, 2);
        assert_eq!(stats.indexes[1].retained, 0);
        assert!(
            index
                .scan_mem_entries(session.pool_guards().index_guard())
                .await
                .unwrap()
                .is_empty()
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_deletion_checkpoint_updates_secondary_disk_tree_roots() {
    smol::block_on(async {
        let sys = TestSys::new_evictable_with_non_unique_name_index().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 2, "same-name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let delete_key = single_key(0i32);
        let keep_key = single_key(1i32);
        let deleted_row_id =
            unique_disk_tree_lookup(&sys.table, session.pool_guards(), &delete_key)
                .await
                .unwrap();
        let kept_row_id = unique_disk_tree_lookup(&sys.table, session.pool_guards(), &keep_key)
            .await
            .unwrap();

        sys.new_trx_delete(&mut session, &delete_key).await;
        let marker_ts = delete_marker_ts(sys.table.deletion_buffer().get(deleted_row_id).unwrap());
        wait_gc_cutoff_after(&session, marker_ts).await;
        checkpoint_published(&sys.table, &mut session).await;

        assert_eq!(
            unique_disk_tree_lookup(&sys.table, session.pool_guards(), &delete_key).await,
            None
        );
        assert_eq!(
            unique_disk_tree_lookup(&sys.table, session.pool_guards(), &keep_key).await,
            Some(kept_row_id)
        );
        let exact_rows = non_unique_disk_tree_prefix_scan(
            &sys.table,
            session.pool_guards(),
            &name_key("same-name"),
        )
        .await;
        assert_eq!(exact_rows, vec![kept_row_id]);

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_unique_checkpoint_overlap_keeps_new_disk_tree_owner() {
    smol::block_on(async {
        let sys = TestSys::new_lightweight_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        let old_row_id = insert_one_row(
            &sys.table,
            &mut session,
            vec![Val::from(1i32), Val::from("old")],
        )
        .await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(1i32);
        sys.new_trx_delete(&mut session, &key).await;
        let delete_ts = delete_marker_ts(sys.table.deletion_buffer().get(old_row_id).unwrap());
        let new_row_id = insert_one_row(
            &sys.table,
            &mut session,
            vec![Val::from(1i32), Val::from("new")],
        )
        .await;

        sys.table.freeze(&session, usize::MAX).await;
        wait_gc_cutoff_after(&session, delete_ts).await;
        checkpoint_published(&sys.table, &mut session).await;

        assert_eq!(
            unique_disk_tree_lookup(&sys.table, session.pool_guards(), &key).await,
            Some(new_row_id)
        );

        drop(session);
        sys.clean_all();
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
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 2, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(0i32);
        let row_id = unique_disk_tree_lookup(&sys.table, session.pool_guards(), &key)
            .await
            .unwrap();
        sys.new_trx_delete(&mut session, &key).await;
        let marker_ts = delete_marker_ts(sys.table.deletion_buffer().get(row_id).unwrap());
        wait_gc_cutoff_after(&session, marker_ts).await;
        let root_before = sys.table.file().active_root().clone();

        set_test_force_secondary_sidecar_error(true);
        let _reset = ResetSidecarHook;
        let err = sys.table.checkpoint(&mut session).await.unwrap_err();
        assert!(matches!(err, Error::InvalidState));

        let root_after = sys.table.file().active_root();
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_all_deleted_row_page_advances_without_column_index() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        delete_key_range_and_wait_gc_cutoff(&sys, &mut session, 0, 10).await;

        let root_before = sys.table.file().active_root().clone();
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let root_after = sys.table.file().active_root();
        assert!(root_after.pivot_row_id > root_before.pivot_row_id);
        assert_eq!(root_after.column_block_index_root, SUPER_BLOCK_ID);
        assert!(root_after.deletion_cutoff_ts > root_before.deletion_cutoff_ts);
        for i in 0..10 {
            sys.new_trx_select_not_found(&mut session, &single_key(i))
                .await;
        }

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_transition_delete_marker_waits_for_next_cutoff_range() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 1, "name").await;

        let key = single_key(0i32);
        let reader = session.try_begin_trx().unwrap().unwrap();
        let index = sys.table.sec_idx()[key.index_no].unique().unwrap();
        let (row_id, _) = index
            .lookup(session.pool_guards().index_guard(), &key.vals, reader.sts)
            .await
            .unwrap()
            .expect("row should exist before delete");
        assert!(matches!(
            sys.table.find_row(session.pool_guards(), row_id).await,
            RowLocation::RowPage(_)
        ));
        reader.commit().await.unwrap();

        let mut hold_session = sys.try_new_session().unwrap();
        let hold_trx = hold_session.try_begin_trx().unwrap().unwrap();
        let hold_sts = hold_trx.sts;

        let mut writer_session = sys.try_new_session().unwrap();
        sys.new_trx_delete(&mut writer_session, &key).await;
        assert!(sys.table.deletion_buffer().get(row_id).is_none());

        sys.table.freeze(&session, usize::MAX).await;
        let mut checkpoint_session = sys.try_new_session().unwrap();
        checkpoint_published(&sys.table, &mut checkpoint_session).await;

        let marker = sys.table.deletion_buffer().get(row_id).unwrap();
        let delete_cts = delete_marker_ts(marker);
        assert!(delete_cts >= hold_sts);

        let root_after_first = sys.table.file().active_root().clone();
        let index_after_first = ColumnBlockIndex::new(
            root_after_first.column_block_index_root,
            root_after_first.pivot_row_id,
            sys.table.file().file_kind(),
            sys.table.file().sparse_file(),
            sys.table.disk_pool(),
            session.pool_guards().disk_guard(),
        );
        let entry_after_first = index_after_first
            .locate_block(row_id)
            .await
            .unwrap()
            .expect("transition snapshot should persist the row into LWC");
        assert!(root_after_first.deletion_cutoff_ts <= delete_cts);
        assert!(
            load_entry_deletion_deltas(&index_after_first, &entry_after_first)
                .await
                .unwrap()
                .is_empty()
        );

        hold_trx.rollback().await.unwrap();
        wait_gc_cutoff_after(&checkpoint_session, delete_cts).await;
        checkpoint_published(&sys.table, &mut checkpoint_session).await;

        let root_after_second = sys.table.file().active_root();
        let index_after_second = ColumnBlockIndex::new(
            root_after_second.column_block_index_root,
            root_after_second.pivot_row_id,
            sys.table.file().file_kind(),
            sys.table.file().sparse_file(),
            sys.table.disk_pool(),
            session.pool_guards().disk_guard(),
        );
        let entry_after_second = index_after_second
            .locate_block(row_id)
            .await
            .unwrap()
            .expect("persisted entry should still exist");
        let deltas = load_entry_deletion_deltas(&index_after_second, &entry_after_second)
            .await
            .unwrap();
        let expected_delta = (row_id - entry_after_second.start_row_id) as u32;
        assert!(root_after_second.deletion_cutoff_ts > delete_cts);
        assert!(deltas.contains(&expected_delta));

        drop(checkpoint_session);
        drop(writer_session);
        drop(hold_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_unique_index_purge_uses_purgeable_delete_marker_fast_path() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 1, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(0i32);
        let reader = session.try_begin_trx().unwrap().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, reader.sts).await;
        reader.commit().await.unwrap();

        let index = sys.table.sec_idx()[key.index_no].unique().unwrap();
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
        sys.table
            .deletion_buffer()
            .put_committed(row_id, 10)
            .unwrap();

        let deleted = sys
            .table
            .accessor()
            .delete_index(session.pool_guards(), &key, row_id, true, 11)
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
                    11,
                )
                .await
                .unwrap(),
            IndexInsert::DuplicateKey(row_id, false)
        );

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_unique_index_purge_compares_persisted_key_when_marker_is_not_purgeable() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 1, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let current_key = single_key(0i32);
        let stale_key = single_key(-1i32);
        let reader = session.try_begin_trx().unwrap().unwrap();
        let row_id =
            assert_row_in_lwc(&sys.table, session.pool_guards(), &current_key, reader.sts).await;
        reader.commit().await.unwrap();

        let index = sys.table.sec_idx()[current_key.index_no].unique().unwrap();
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
        let deleted = sys
            .table
            .accessor()
            .delete_index(
                session.pool_guards(),
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
        sys.table
            .deletion_buffer()
            .put_committed(row_id, 100)
            .unwrap();
        let deleted = sys
            .table
            .accessor()
            .delete_index(session.pool_guards(), &current_key, row_id, true, 100)
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_non_unique_index_purge_compares_persisted_key_when_marker_is_not_purgeable() {
    smol::block_on(async {
        let sys = TestSys::new_evictable_with_non_unique_name_index().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 1, "current").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let pk = single_key(0i32);
        let current_key = name_key("current");
        let stale_key = name_key("stale");
        let reader = session.try_begin_trx().unwrap().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &pk, reader.sts).await;
        reader.commit().await.unwrap();

        let index = sys.table.sec_idx()[current_key.index_no]
            .non_unique()
            .unwrap();
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
        let deleted = sys
            .table
            .accessor()
            .delete_index(
                session.pool_guards(),
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
        sys.table
            .deletion_buffer()
            .put_committed(row_id, 200)
            .unwrap();
        let deleted = sys
            .table
            .accessor()
            .delete_index(session.pool_guards(), &current_key, row_id, false, 200)
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_index_purge_removes_delete_marked_unique_entry_when_row_is_not_found() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let session = sys.try_new_session().unwrap();
        let key = single_key(9999i32);
        let row_id = 9999;
        let index = sys.table.sec_idx()[key.index_no].unique().unwrap();
        let _ = index
            .insert_if_not_exists(
                session.pool_guards().index_guard(),
                &key.vals,
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
                    &key.vals,
                    row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );

        let deleted = sys
            .table
            .accessor()
            .delete_index(session.pool_guards(), &key, row_id, true, MAX_SNAPSHOT_TS)
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_unique_insert_rollback_restores_deleted_owner_even_when_row_missing() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        let key = single_key(10_001i32);
        let stale_row_id = 10_001;

        assert!(matches!(
            sys.table
                .find_row(session.pool_guards(), stale_row_id)
                .await,
            RowLocation::NotFound
        ));

        let index = sys.table.sec_idx()[key.index_no].unique().unwrap();
        assert!(
            index
                .insert_if_not_exists(
                    session.pool_guards().index_guard(),
                    &key.vals,
                    stale_row_id,
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
                    stale_row_id,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap()
        );
        assert_unique_index_entry(
            &sys.table,
            session.pool_guards(),
            &key,
            MAX_SNAPSHOT_TS,
            stale_row_id,
            true,
        )
        .await;

        let trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let new_row_id = unwrap_insert_result(
            stmt.insert_row(&sys.table, vec![Val::from(10_001i32), Val::from("reborn")])
                .await,
        );
        assert_ne!(new_row_id, stale_row_id);
        assert_unique_index_entry(
            &sys.table,
            session.pool_guards(),
            &key,
            stmt.trx.sts,
            new_row_id,
            false,
        )
        .await;

        let trx = stmt.fail().await.unwrap();
        trx.rollback().await.unwrap();

        assert_unique_index_entry(
            &sys.table,
            session.pool_guards(),
            &key,
            MAX_SNAPSHOT_TS,
            stale_row_id,
            true,
        )
        .await;
        sys.new_trx_select_not_found(&mut session, &key).await;
        sys.new_trx_insert(
            &mut session,
            vec![Val::from(10_001i32), Val::from("reclaimed")],
        )
        .await;
        sys.new_trx_select(&mut session, &key, |vals| {
            assert_eq!(vals, vec![Val::from(10_001i32), Val::from("reclaimed")]);
        })
        .await;

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_unique_insert_rollback_restores_delete_marked_stale_hot_owner() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        let live_key = single_key(1i32);
        let stale_key = single_key(2i32);
        sys.new_trx_insert(&mut session, vec![Val::from(1i32), Val::from("one")])
            .await;

        let reader = session.try_begin_trx().unwrap().unwrap();
        let old_row_id = sys.table.sec_idx()[live_key.index_no]
            .unique()
            .unwrap()
            .lookup(
                session.pool_guards().index_guard(),
                &live_key.vals,
                reader.sts,
            )
            .await
            .unwrap()
            .unwrap()
            .0;
        reader.commit().await.unwrap();
        assert!(matches!(
            sys.table.find_row(session.pool_guards(), old_row_id).await,
            RowLocation::RowPage(_)
        ));

        let index = sys.table.sec_idx()[stale_key.index_no].unique().unwrap();
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

        let trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let new_row_id = unwrap_insert_result(
            stmt.insert_row(&sys.table, vec![Val::from(2i32), Val::from("two")])
                .await,
        );
        assert_ne!(new_row_id, old_row_id);
        assert_unique_index_entry(
            &sys.table,
            session.pool_guards(),
            &stale_key,
            stmt.trx.sts,
            new_row_id,
            false,
        )
        .await;

        let trx = stmt.fail().await.unwrap();
        trx.rollback().await.unwrap();

        assert_unique_index_entry(
            &sys.table,
            session.pool_guards(),
            &stale_key,
            MAX_SNAPSHOT_TS,
            old_row_id,
            true,
        )
        .await;
        sys.new_trx_select(&mut session, &live_key, |vals| {
            assert_eq!(vals, vec![Val::from(1i32), Val::from("one")]);
        })
        .await;
        sys.new_trx_select_not_found(&mut session, &stale_key).await;
        sys.new_trx_insert(&mut session, vec![Val::from(2i32), Val::from("two-final")])
            .await;
        sys.new_trx_select(&mut session, &stale_key, |vals| {
            assert_eq!(vals, vec![Val::from(2i32), Val::from("two-final")]);
        })
        .await;

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_fails_when_eligible_delete_marker_has_no_column_index() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 4, "name").await;
        delete_key_range_and_wait_gc_cutoff(&sys, &mut session, 0, 4).await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let root_before = sys.table.file().active_root().clone();
        assert!(root_before.pivot_row_id > 0);
        assert_eq!(root_before.column_block_index_root, SUPER_BLOCK_ID);
        let marker_ts = root_before.deletion_cutoff_ts;
        sys.table
            .deletion_buffer()
            .put_committed(0, marker_ts)
            .unwrap();
        wait_gc_cutoff_after(&session, marker_ts).await;

        let err = sys.table.checkpoint(&mut session).await.unwrap_err();
        assert!(matches!(err, Error::InvalidState));
        let root_after = sys.table.file().active_root();
        assert_eq!(
            root_after.deletion_cutoff_ts,
            root_before.deletion_cutoff_ts
        );
        assert_eq!(
            root_after.column_block_index_root,
            root_before.column_block_index_root
        );

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_fails_when_eligible_delete_marker_cannot_be_located() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 1, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(0i32);
        let reader = session.try_begin_trx().unwrap().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, reader.sts).await;
        reader.commit().await.unwrap();

        let root_before = sys.table.file().active_root().clone();
        assert_ne!(root_before.column_block_index_root, SUPER_BLOCK_ID);
        let missing_row_id = row_id + 1;
        assert!(missing_row_id < root_before.pivot_row_id);
        let index = ColumnBlockIndex::new(
            root_before.column_block_index_root,
            root_before.pivot_row_id,
            sys.table.file().file_kind(),
            sys.table.file().sparse_file(),
            sys.table.disk_pool(),
            session.pool_guards().disk_guard(),
        );
        assert!(index.locate_block(missing_row_id).await.unwrap().is_none());

        let marker_ts = root_before.deletion_cutoff_ts;
        sys.table
            .deletion_buffer()
            .put_committed(missing_row_id, marker_ts)
            .unwrap();
        wait_gc_cutoff_after(&session, marker_ts).await;

        let err = sys.table.checkpoint(&mut session).await.unwrap_err();
        assert!(matches!(err, Error::InvalidState));
        assert_eq!(
            sys.table.file().active_root().deletion_cutoff_ts,
            root_before.deletion_cutoff_ts
        );

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_ignores_missing_old_delete_marker_below_previous_cutoff() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 1, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(0i32);
        let reader = session.try_begin_trx().unwrap().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, reader.sts).await;
        reader.commit().await.unwrap();

        let root_before = sys.table.file().active_root().clone();
        assert!(root_before.deletion_cutoff_ts > 0);
        let missing_row_id = row_id + 1;
        assert!(missing_row_id < root_before.pivot_row_id);
        let old_marker_ts = root_before.deletion_cutoff_ts - 1;
        sys.table
            .deletion_buffer()
            .put_committed(missing_row_id, old_marker_ts)
            .unwrap();
        wait_gc_cutoff_after(&session, root_before.deletion_cutoff_ts).await;

        checkpoint_published(&sys.table, &mut session).await;
        assert!(sys.table.file().active_root().deletion_cutoff_ts > root_before.deletion_cutoff_ts);

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_recover_cold_delete_rejects_already_deleted_with_different_cts() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(6i32);
        let reader = session.try_begin_trx().unwrap().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, reader.sts).await;
        reader.commit().await.unwrap();

        let active_root = sys.table.file().active_root();
        assert!(row_id < active_root.pivot_row_id);
        let cts = active_root.deletion_cutoff_ts;
        sys.table
            .recover_row_delete(session.pool_guards(), PageID::from(0u64), row_id, cts, true)
            .await
            .unwrap();
        sys.table
            .recover_row_delete(session.pool_guards(), PageID::from(0u64), row_id, cts, true)
            .await
            .unwrap();

        let err = sys
            .table
            .recover_row_delete(
                session.pool_guards(),
                PageID::from(0u64),
                row_id,
                cts + 1,
                true,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidState));

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_skips_cold_delete_markers_at_or_after_cutoff() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key = single_key(7i32);
        let reader = session.try_begin_trx().unwrap().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, reader.sts).await;
        reader.commit().await.unwrap();

        let mut hold_session = sys.try_new_session().unwrap();
        let hold_trx = hold_session.try_begin_trx().unwrap().unwrap();
        let hold_sts = hold_trx.sts;

        let mut writer_session = sys.try_new_session().unwrap();
        sys.new_trx_delete(&mut writer_session, &key).await;

        let marker = sys.table.deletion_buffer().get(row_id).unwrap();
        let delete_cts = match marker {
            DeleteMarker::Committed(ts) => ts,
            DeleteMarker::Ref(status) => status.ts(),
        };
        assert!(delete_cts >= hold_sts);

        let mut checkpoint_session = sys.try_new_session().unwrap();
        checkpoint_published(&sys.table, &mut checkpoint_session).await;

        let active_root = sys.table.file().active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.file().file_kind(),
            sys.table.file().sparse_file(),
            sys.table.disk_pool(),
            session.pool_guards().disk_guard(),
        );
        let entry = index
            .locate_block(row_id)
            .await
            .unwrap()
            .expect("persisted entry should exist");
        assert!(active_root.deletion_cutoff_ts <= delete_cts);
        assert!(
            load_entry_deletion_deltas(&index, &entry)
                .await
                .unwrap()
                .is_empty()
        );

        hold_trx.rollback().await.unwrap();
        drop(checkpoint_session);
        drop(writer_session);
        drop(hold_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_fails_on_invalid_v2_delete_metadata() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key1 = single_key(6i32);
        let reader = session.try_begin_trx().unwrap().unwrap();
        let row_id1 = assert_row_in_lwc(&sys.table, session.pool_guards(), &key1, reader.sts).await;
        reader.commit().await.unwrap();

        sys.new_trx_delete(&mut session, &key1).await;
        let marker1 = sys.table.deletion_buffer().get(row_id1).unwrap();
        let marker1_ts = delete_marker_ts(marker1);
        wait_gc_cutoff_after(&session, marker1_ts).await;
        checkpoint_published(&sys.table, &mut session).await;

        let active_root = sys.table.file().active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.file().file_kind(),
            sys.table.file().sparse_file(),
            sys.table.disk_pool(),
            session.pool_guards().disk_guard(),
        );
        let entry = index
            .locate_block(row_id1)
            .await
            .unwrap()
            .expect("persisted entry should exist");

        let key2 = single_key(7i32);
        let mut reader_session = sys.try_new_session().unwrap();
        let reader = reader_session.try_begin_trx().unwrap().unwrap();
        let row_id2 =
            assert_row_in_lwc(&sys.table, reader_session.pool_guards(), &key2, reader.sts).await;
        reader.commit().await.unwrap();
        let entry2 = index
            .locate_block(row_id2)
            .await
            .unwrap()
            .expect("second persisted entry should exist");
        assert_eq!(entry2.leaf_page_id, entry.leaf_page_id);
        drop(reader_session);

        sys.new_trx_delete(&mut session, &key2).await;
        let marker2 = sys.table.deletion_buffer().get(row_id2).unwrap();
        let marker2_ts = delete_marker_ts(marker2);
        wait_gc_cutoff_after(&session, marker2_ts).await;

        let table_file_path = sys.engine.table_fs.table_file_path(sys.table.table_id());
        corrupt_leaf_delete_codec(table_file_path, entry.leaf_page_id, 0);
        let _ = sys
            .table
            .disk_pool()
            .invalidate_block_id(sys.table.file().sparse_file().file_id(), entry.leaf_page_id);

        let err = sys.table.checkpoint(&mut session).await.unwrap_err();
        assert!(matches!(
            err,
            Error::BlockCorrupted {
                file_kind: FileKind::TableFile,
                block_kind: BlockKind::ColumnBlockIndex,
                block_id: page_id,
                cause: BlockCorruptionCause::InvalidPayload,
            } if page_id == entry.leaf_page_id
        ));

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_fails_on_short_v2_delete_section_header() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;

        let key1 = single_key(6i32);
        let reader = session.try_begin_trx().unwrap().unwrap();
        let row_id1 = assert_row_in_lwc(&sys.table, session.pool_guards(), &key1, reader.sts).await;
        reader.commit().await.unwrap();

        sys.new_trx_delete(&mut session, &key1).await;
        let marker1 = sys.table.deletion_buffer().get(row_id1).unwrap();
        let marker1_ts = delete_marker_ts(marker1);
        wait_gc_cutoff_after(&session, marker1_ts).await;
        checkpoint_published(&sys.table, &mut session).await;

        let active_root = sys.table.file().active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.file().file_kind(),
            sys.table.file().sparse_file(),
            sys.table.disk_pool(),
            session.pool_guards().disk_guard(),
        );
        let entry = index
            .locate_block(row_id1)
            .await
            .unwrap()
            .expect("persisted entry should exist");

        let key2 = single_key(7i32);
        let mut reader_session = sys.try_new_session().unwrap();
        let reader = reader_session.try_begin_trx().unwrap().unwrap();
        let row_id2 =
            assert_row_in_lwc(&sys.table, reader_session.pool_guards(), &key2, reader.sts).await;
        reader.commit().await.unwrap();
        let entry2 = index
            .locate_block(row_id2)
            .await
            .unwrap()
            .expect("second persisted entry should exist");
        assert_eq!(entry2.leaf_page_id, entry.leaf_page_id);
        drop(reader_session);

        sys.new_trx_delete(&mut session, &key2).await;
        let marker2 = sys.table.deletion_buffer().get(row_id2).unwrap();
        let marker2_ts = delete_marker_ts(marker2);
        wait_gc_cutoff_after(&session, marker2_ts).await;

        let table_file_path = sys.engine.table_fs.table_file_path(sys.table.table_id());
        corrupt_leaf_short_delete_section_header(table_file_path, entry.leaf_page_id, 0);
        let _ = sys
            .table
            .disk_pool()
            .invalidate_block_id(sys.table.file().sparse_file().file_id(), entry.leaf_page_id);

        let err = sys.table.checkpoint(&mut session).await.unwrap_err();
        assert!(matches!(
            err,
            Error::BlockCorrupted {
                file_kind: FileKind::TableFile,
                block_kind: BlockKind::ColumnBlockIndex,
                block_id: page_id,
                cause: BlockCorruptionCause::InvalidPayload,
            } if page_id == entry.leaf_page_id
        ));

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_row_page_transition_retries_update_delete() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        {
            let insert = vec![Val::from(1i32), Val::from("hello")];
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            trx = sys.trx_insert(trx, insert).await;
            trx.commit().await.unwrap();
        }
        let key = single_key(1i32);
        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let index = sys.table.sec_idx()[key.index_no].unique().unwrap();
        let (row_id, _) = index
            .lookup(session.pool_guards().index_guard(), &key.vals, stmt.trx.sts)
            .await
            .unwrap()
            .unwrap();
        let page_id = match sys.table.find_row(session.pool_guards(), row_id).await {
            RowLocation::RowPage(page_id) => page_id,
            RowLocation::NotFound => panic!("row should exist"),
            RowLocation::LwcBlock { .. } => unreachable!("lwc block"),
        };
        let page_guard = sys
            .engine
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

        let insert_page_guard = sys
            .engine
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
        let insert_res = sys.table.accessor().insert_row_to_page(
            &mut stmt,
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
        let res = sys
            .table
            .accessor()
            .update_row_inplace(&mut stmt, page_guard, &key, row_id, update)
            .await;
        assert!(matches!(res, UpdateRowInplace::RetryInTransition));
        trx = stmt.fail().await.unwrap();
        trx.rollback().await.unwrap();

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let page_guard = sys
            .engine
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
        let res = sys
            .table
            .accessor()
            .delete_row_internal(&mut stmt, page_guard, row_id, &key, false)
            .await;
        assert!(matches!(res, DeleteInternal::RetryInTransition));
        trx = stmt.fail().await.unwrap();
        trx.rollback().await.unwrap();

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_mvcc_rollback_insert_normal() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        {
            let mut session = sys.try_new_session().unwrap();
            // insert 1 row
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let insert = vec![Val::from(1i32), Val::from("hello")];
            trx = sys.trx_insert(trx, insert).await;
            // explicit rollback
            trx.rollback().await.unwrap();

            // select 1 row
            let key = single_key(1i32);
            _ = sys.new_trx_select_not_found(&mut session, &key).await;
        }
        sys.clean_all();
    });
}

#[test]
fn test_mvcc_insert_link_unique_index() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        {
            let mut session = sys.try_new_session().unwrap();
            // insert 1 row
            let insert = vec![Val::from(1i32), Val::from("hello")];
            sys.new_trx_insert(&mut session, insert).await;

            // we must hold a transaction before the deletion,
            // to prevent index GC.
            let trx_to_prevent_gc = sys
                .try_new_session()
                .unwrap()
                .try_begin_trx()
                .unwrap()
                .unwrap();
            // delete it
            let key = single_key(1i32);
            sys.new_trx_delete(&mut session, &key).await;

            // insert again, trigger insert+link
            let insert = vec![Val::from(1i32), Val::from("world")];
            sys.new_trx_insert(&mut session, insert).await;

            trx_to_prevent_gc.rollback().await.unwrap();

            // select 1 row
            let key = single_key(1i32);
            _ = sys
                .new_trx_select(&mut session, &key, |vals| {
                    assert!(vals[0] == Val::from(1i32));
                    assert!(vals[1] == Val::from("world"));
                })
                .await;
        }
        sys.clean_all();
    });
}

#[test]
fn test_mvcc_rollback_insert_link_unique_index() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        {
            let mut session = sys.try_new_session().unwrap();
            // insert 1 row
            let insert = vec![Val::from(1i32), Val::from("hello")];
            sys.new_trx_insert(&mut session, insert).await;

            // delete it
            let key = single_key(1i32);
            sys.new_trx_delete(&mut session, &key).await;

            // insert again, trigger insert+link
            let insert = vec![Val::from(1i32), Val::from("world")];
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            trx = sys.trx_insert(trx, insert).await;
            // explicit rollback
            trx.rollback().await.unwrap();

            // select 1 row
            let key = single_key(1i32);
            _ = sys.new_trx_select_not_found(&mut session, &key).await;
        }
        sys.clean_all();
    });
}

#[test]
fn test_mvcc_insert_link_update() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        {
            let mut session = sys.try_new_session().unwrap();
            // insert 1 row: v1=1, v2=hello
            let insert = vec![Val::from(1i32), Val::from("hello")];
            sys.new_trx_insert(&mut session, insert).await;

            // open one session and trnasaction to see this row
            let mut sess1 = sys.try_new_session().unwrap();
            let mut trx1 = sess1.try_begin_trx().unwrap().unwrap();

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
            sys.new_trx_update(&mut session, &key, update).await;

            // open session and transaction to see row 2
            let mut sess2 = sys.try_new_session().unwrap();
            let mut trx2 = sess2.try_begin_trx().unwrap().unwrap();

            // insert again, trigger insert+link
            let insert = vec![Val::from(1i32), Val::from("rust")];
            sys.new_trx_insert(&mut session, insert).await;

            // use transaction 1 to see version 1.
            let key = single_key(1i32);
            trx1 = sys
                .trx_select(trx1, &key, |vals| {
                    assert!(vals[0] == Val::from(1i32));
                    assert!(vals[1] == Val::from("hello"));
                })
                .await;
            _ = trx1.commit().await.unwrap();

            // use transaction 2 to see version 2.
            let key = single_key(2i32);
            trx2 = sys
                .trx_select(trx2, &key, |vals| {
                    assert!(vals[0] == Val::from(2i32));
                    assert!(vals[1] == Val::from("world"));
                })
                .await;
            _ = trx2.commit().await.unwrap();

            // use new transaction to see version 3.
            let key = single_key(1i32);
            _ = sys
                .new_trx_select(&mut session, &key, |vals| {
                    assert!(vals[0] == Val::from(1i32));
                    assert!(vals[1] == Val::from("rust"));
                })
                .await;
        }
        sys.clean_all();
    });
}

#[test]
fn test_mvcc_update_link_insert() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        {
            let mut session = sys.try_new_session().unwrap();
            // insert 1 row: v1=1, v2=hello
            let insert = vec![Val::from(1i32), Val::from("hello")];
            sys.new_trx_insert(&mut session, insert).await;
            println!("debug-only insert finish");

            // open one session and trnasaction to see this row
            let mut sess1 = sys.try_new_session().unwrap();
            let mut trx1 = sess1.try_begin_trx().unwrap().unwrap();

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
            sys.new_trx_update(&mut session, &key, update).await;
            println!("debug-only update finish");

            // open session and transaction to see row 2
            let mut sess2 = sys.try_new_session().unwrap();
            let mut trx2 = sess2.try_begin_trx().unwrap().unwrap();

            // insert v1=5, v2=rust
            let insert = vec![Val::from(5i32), Val::from("rust")];
            sys.new_trx_insert(&mut session, insert).await;
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
            sys.new_trx_update(&mut session, &key, update).await;
            println!("debug-only update2 finish");

            // use transaction 1 to see version 1.
            let key = single_key(1i32);
            trx1 = sys
                .trx_select(trx1, &key, |vals| {
                    assert!(vals[0] == Val::from(1i32));
                    assert!(vals[1] == Val::from("hello"));
                })
                .await;
            _ = trx1.commit().await;

            // use transaction 2 to see version 2.
            let key = single_key(2i32);
            trx2 = sys
                .trx_select(trx2, &key, |vals| {
                    assert!(vals[0] == Val::from(2i32));
                    assert!(vals[1] == Val::from("world"));
                })
                .await;
            _ = trx2.commit().await;

            // use new transaction to see version 3.
            let key = single_key(1i32);
            _ = sys.new_trx_select(&mut session, &key, |vals| {
                assert!(vals[0] == Val::from(1i32));
                assert!(vals[1] == Val::from("c++"));
            })
        }
        sys.clean_all();
    });
}

#[test]
fn test_mvcc_multi_update() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        {
            let mut session = sys.try_new_session().unwrap();
            // insert: v1
            let insert = vec![Val::from(1i32), Val::from("hello")];
            sys.new_trx_insert(&mut session, insert).await;

            // transaction to see version 1
            let mut sess1 = sys.try_new_session().unwrap();
            let mut trx1 = sess1.try_begin_trx().unwrap().unwrap();

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            // update 1: v2
            let key = single_key(1i32);
            let update = vec![UpdateCol {
                idx: 1,
                val: Val::from("rust"),
            }];
            trx = sys.trx_update(trx, &key, update).await;
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
            trx = sys.trx_update(trx, &key, update).await;
            // within transaction, query row
            // v2 not found
            let key = single_key(1i32);
            trx = sys.trx_select_not_found(trx, &key).await;
            // v3 found
            let key = single_key(2i32);
            trx = sys
                .trx_select(trx, &key, |vals| {
                    assert!(vals[0] == Val::from(2i32));
                    assert!(vals[1] == Val::from("world"));
                })
                .await;
            trx.commit().await.unwrap();

            //v1 found
            let key = single_key(1i32);
            trx1 = sys
                .trx_select(trx1, &key, |vals| {
                    assert!(vals[0] == Val::from(1i32));
                    assert!(vals[1] == Val::from("hello"));
                })
                .await;
            trx1.commit().await.unwrap();
        }
        sys.clean_all();
    });
}

#[test]
fn test_string_non_index_updates() {
    smol::block_on(async {
        const COUNT: usize = 100;
        const SIZE: usize = 500;
        let sys = TestSys::new_evictable().await;
        {
            let mut session = sys.try_new_session().unwrap();
            let value = vec![1u8; SIZE];
            let insert = vec![Val::from(1i32), Val::from(&[])];
            sys.new_trx_insert(&mut session, insert).await;
            let key = SelectKey::new(0, vec![Val::from(1i32)]);
            for i in SIZE - COUNT..SIZE {
                sys.new_trx_update(
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
        sys.clean_all();
    });
}

#[test]
fn test_string_index_updates() {
    use crate::catalog::tests::table3;
    smol::block_on(async {
        const COUNT: usize = 100;
        const SIZE: usize = 500;
        let sys = TestSys::new_evictable().await;
        {
            let table_id = table3(&sys.engine).await;
            let table = sys.engine.catalog().get_table(table_id).await.unwrap();
            let mut session = sys.try_new_session().unwrap();
            let s: String = std::iter::repeat_n('0', SIZE).collect();
            // insert single row.
            {
                let insert = vec![Val::from(&s[..0])];
                let mut stmt = session.try_begin_trx().unwrap().unwrap().start_stmt();
                let res = stmt.insert_row(&table, insert).await;
                assert!(matches!(res, Ok(InsertMvcc::Inserted(_))));
                stmt.succeed().commit().await.unwrap();
            }
            // perform updates.
            for i in 0..COUNT {
                let key = SelectKey::new(0, vec![Val::from(&s[..i])]);
                let update = vec![UpdateCol {
                    idx: 0,
                    val: Val::from(&s[..i + 1]),
                }];
                let mut stmt = session.try_begin_trx().unwrap().unwrap().start_stmt();
                let res = stmt.update_row(&table, &key, update).await;
                assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
                stmt.succeed().commit().await.unwrap();
            }
        }
        sys.clean_all();
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
        let sys = TestSys::new_evictable().await;
        {
            let table_id = table3(&sys.engine).await;
            let table = sys.engine.catalog().get_table(table_id).await.unwrap();
            let mut session = sys.try_new_session().unwrap();
            let s: String = std::iter::repeat_n('0', SIZE).collect();
            // insert 60 rows
            for i in 0usize..COUNT {
                let insert = vec![Val::from(&s[..BASE + i])];
                let mut stmt = session.try_begin_trx().unwrap().unwrap().start_stmt();
                let res = stmt.insert_row(&table, insert).await;
                assert!(matches!(res, Ok(InsertMvcc::Inserted(_))));
                stmt.succeed().commit().await.unwrap();
            }
            // perform updates to trigger out-of-place update.
            // try to update k=s[..BASE+DELTA] to s[..BASE+COUNT+DELTA]
            for i in 0..DELTA {
                let key = SelectKey::new(0, vec![Val::from(&s[..BASE + i])]);
                let update = vec![UpdateCol {
                    idx: 0,
                    val: Val::from(&s[..BASE + COUNT + i]),
                }];
                let mut stmt = session.try_begin_trx().unwrap().unwrap().start_stmt();
                let res = stmt.update_row(&table, &key, update).await;
                assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
                stmt.succeed().commit().await.unwrap();
            }
        }
        sys.clean_all();
    });
}

#[test]
fn test_evict_pool_insert_full() {
    smol::block_on(async {
        const SIZE: i32 = 800;

        // in-mem ~1000 pages, on-disk 2000 pages.
        let sys = TestSys::new_evictable().await;
        {
            let mut session = sys.try_new_session().unwrap();
            // insert 1000 rows
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            for i in 0..SIZE {
                // make string 1KB long, so a page can only hold about 60 rows.
                // if page is full, 17 pages are required.
                // if page is half full, 35 pages are required.
                let s: String = (0..1000).map(|_| 'a').collect();
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            let _ = trx.commit().await.unwrap();
        }
        sys.clean_all();
    });
}

#[test]
fn test_table_scan_uncommitted() {
    smol::block_on(async {
        const SIZE: i32 = 10000;

        let sys = TestSys::new_evictable().await;

        let mut session = sys.try_new_session().unwrap();
        {
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            _ = trx.commit().await.unwrap();
        }
        {
            let mut res_len = 0usize;
            sys.table
                .accessor()
                .table_scan_uncommitted(session.pool_guards(), |_metadata, _row| {
                    res_len += 1;
                    true
                })
                .await;
            println!("res.len()={}", res_len);
            assert!(res_len == SIZE as usize);
        }

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_table_scan_mvcc() {
    smol::block_on(async {
        const SIZE: i32 = 100;

        let sys = TestSys::new_evictable().await;

        // insert 100 rows and commit
        let mut session1 = sys.try_new_session().unwrap();
        {
            let mut trx = session1.try_begin_trx().unwrap().unwrap();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            _ = trx.commit().await.unwrap();
        }
        // we should see 100 committed rows.
        let mut session2 = sys.try_new_session().unwrap();
        {
            let trx = session2.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let mut res_len = 0usize;
            sys.table
                .accessor()
                .table_scan_mvcc(&stmt, &[0], |_| {
                    res_len += 1;
                    true
                })
                .await;
            println!("res.len()={}", res_len);
            assert!(res_len == SIZE as usize);
            stmt.succeed().commit().await.unwrap();
        }
        // insert 100 rows but not commit.
        let pending_trx = {
            let mut trx = session1.try_begin_trx().unwrap().unwrap();
            for i in SIZE..SIZE * 2 {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            trx
        };
        // we should see only 100 rows
        {
            let trx = session2.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let mut res_len = 0usize;
            sys.table
                .accessor()
                .table_scan_mvcc(&stmt, &[0], |_| {
                    res_len += 1;
                    true
                })
                .await;
            println!("res.len()={}", res_len);
            assert!(res_len == SIZE as usize);
            stmt.succeed().commit().await.unwrap();
        }
        // commit the pending transaction.
        pending_trx.commit().await.unwrap();
        // now we should see 200 rows.
        {
            let trx = session2.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let mut res_len = 0usize;
            sys.table
                .accessor()
                .table_scan_mvcc(&stmt, &[0], |_| {
                    res_len += 1;
                    true
                })
                .await;
            println!("res.len()={}", res_len);
            assert!(res_len == (SIZE * 2) as usize);
            stmt.succeed().commit().await.unwrap();
        }

        drop(session1);
        drop(session2);
        sys.clean_all();
    });
}

#[test]
fn test_table_freeze() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;

        let mut session1 = sys.try_new_session().unwrap();
        {
            let trx = session1.try_begin_trx().unwrap().unwrap();
            let insert = vec![Val::from(1), Val::from("1")];
            sys.trx_insert(trx, insert).await.commit().await.unwrap();
        }
        let row_pages = sys.table.total_row_pages(session1.pool_guards()).await;
        assert!(row_pages == 1);
        sys.table.freeze(&session1, 10).await;
        // after freezing, new row should be inserted into second page.
        {
            let trx = session1.try_begin_trx().unwrap().unwrap();
            let insert = vec![Val::from(2), Val::from("2")];
            sys.trx_insert(trx, insert).await.commit().await.unwrap();
        }
        let row_pages = sys.table.total_row_pages(session1.pool_guards()).await;
        assert!(row_pages == 2);
        sys.table.freeze(&session1, 10).await;

        // update row 1 will cause new insert into new page.
        {
            let mut stmt = session1.try_begin_trx().unwrap().unwrap().start_stmt();
            let key = SelectKey::new(0, vec![Val::from(1)]);
            let res = stmt
                .update_row(
                    &sys.table,
                    &key,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("3"),
                    }],
                )
                .await;
            assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
            stmt.succeed().commit().await.unwrap();
        }
        let row_pages = sys.table.total_row_pages(session1.pool_guards()).await;
        assert!(row_pages == 3);

        // update row 1 will just be in-place.
        {
            let mut stmt = session1.try_begin_trx().unwrap().unwrap().start_stmt();
            let key = SelectKey::new(0, vec![Val::from(1)]);
            let res = stmt
                .update_row(
                    &sys.table,
                    &key,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("4"),
                    }],
                )
                .await;
            assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
            stmt.succeed().commit().await.unwrap();
        }
        let row_pages = sys.table.total_row_pages(session1.pool_guards()).await;
        assert!(row_pages == 3);

        drop(session1);
        sys.clean_all();
    });
}

#[test]
fn test_transition_captures_uncommitted_lock_into_deletion_buffer() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 1, 1, "lock").await;

        let key = single_key(1i32);
        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let index = sys.table.sec_idx()[key.index_no].unique().unwrap();
        let (row_id, _) = index
            .lookup(session.pool_guards().index_guard(), &key.vals, stmt.trx.sts)
            .await
            .unwrap()
            .unwrap();
        let page_id = match sys.table.find_row(session.pool_guards(), row_id).await {
            RowLocation::RowPage(page_id) => page_id,
            RowLocation::NotFound => panic!("row should exist"),
            RowLocation::LwcBlock { .. } => unreachable!("row page expected"),
        };

        let page_guard = sys
            .engine
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
        let (ctx, page) = page_guard.ctx_and_page();
        let mut lock_row = sys
            .table
            .accessor()
            .lock_row_for_write(&mut stmt, &page_guard, row_id, Some(&key))
            .await;
        match &mut lock_row {
            LockRowForWrite::Ok(access) => {
                drop(access.take());
            }
            _ => panic!("lock should succeed"),
        }

        let frozen_page = FrozenPage {
            page_id,
            start_row_id: page.header.start_row_id,
            end_row_id: page.header.start_row_id + page.header.max_row_count as u64,
        };
        ctx.row_ver().unwrap().set_frozen();
        sys.table
            .set_frozen_pages_to_transition(session.pool_guards(), &[frozen_page], stmt.trx.sts)
            .await
            .unwrap();

        let marker = sys.table.deletion_buffer().get(row_id).unwrap();
        match marker {
            DeleteMarker::Ref(status) => {
                assert!(std::sync::Arc::ptr_eq(&status, &stmt.trx.status()));
            }
            DeleteMarker::Committed(_) => panic!("uncommitted lock should remain as marker ref"),
        }

        trx = stmt.fail().await.unwrap();
        trx.rollback().await.unwrap();

        drop(lock_row);
        drop(page_guard);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_basic_flow() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        let name = "x".repeat(1024);
        insert_rows(&sys, &mut session, 0, 200, &name).await;

        let old_root = sys.table.file().active_root().clone();
        sys.table.freeze(&session, usize::MAX).await;
        let (frozen_pages, _) = sys.table.collect_frozen_pages(session.pool_guards()).await;
        assert!(!frozen_pages.is_empty());

        checkpoint_published(&sys.table, &mut session).await;

        let new_root = sys.table.file().active_root();
        assert!(new_root.pivot_row_id > old_root.pivot_row_id);
        assert_ne!(new_root.column_block_index_root, SUPER_BLOCK_ID);
        assert!(new_root.deletion_cutoff_ts > old_root.deletion_cutoff_ts);

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_readiness_ready_when_root_crossed_gc_horizon() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let session = sys.try_new_session().unwrap();
        let root_cts = sys.table.file().active_root().trx_id;
        let min_active_sts = sys.engine.trx_sys.calc_min_active_sts_for_gc();
        assert!(root_cts < min_active_sts);
        assert!(matches!(
            sys.table.checkpoint_readiness(&session),
            CheckpointReadiness::Ready
        ));

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_readiness_delayed_reports_root_and_horizon() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 120, "readiness-delay").await;

        sys.table.freeze(&session, usize::MAX).await;
        let mut reader_session = sys.try_new_session().unwrap();
        let reader = reader_session.try_begin_trx().unwrap().unwrap();
        checkpoint_published(&sys.table, &mut session).await;

        let active_root_cts = sys.table.file().active_root().trx_id;
        let readiness = sys.table.checkpoint_readiness(&session);
        let CheckpointReadiness::Delayed { reason } = readiness else {
            panic!("expected delayed checkpoint readiness, got {readiness:?}");
        };
        assert_eq!(reason.root_cts, active_root_cts);
        assert_eq!(reason.min_active_sts, reader.sts);
        assert!(reason.root_cts >= reason.min_active_sts);

        reader.commit().await.unwrap();
        wait_gc_cutoff_after(&session, active_root_cts).await;
        assert!(matches!(
            sys.table.checkpoint_readiness(&session),
            CheckpointReadiness::Ready
        ));

        drop(reader_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_delayed_preserves_root_and_frozen_pages_until_ready() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 120, "delayed-root").await;

        sys.table.freeze(&session, usize::MAX).await;
        let mut reader_session = sys.try_new_session().unwrap();
        let reader = reader_session.try_begin_trx().unwrap().unwrap();
        checkpoint_published(&sys.table, &mut session).await;
        let root_protected_by_reader = sys.table.file().active_root().trx_id;

        insert_rows(&sys, &mut session, 1_000, 80, "delayed-frozen").await;
        sys.table.freeze(&session, usize::MAX).await;
        let (frozen_pages, _) = sys.table.collect_frozen_pages(session.pool_guards()).await;
        assert!(!frozen_pages.is_empty());
        let first_frozen_page = frozen_pages[0].page_id;
        let root_before_delay = sys.table.file().active_root().clone();

        let outcome = sys.table.checkpoint(&mut session).await.unwrap();
        let CheckpointOutcome::Delayed { reason } = outcome else {
            panic!("expected delayed checkpoint, got {outcome:?}");
        };
        assert_eq!(reason.root_cts, root_protected_by_reader);
        assert_eq!(reason.min_active_sts, reader.sts);
        assert_root_metadata_unchanged(&root_before_delay, &sys.table);

        let page_guard = sys
            .table
            .must_get_row_page_shared(session.pool_guards(), first_frozen_page)
            .await
            .unwrap();
        let (ctx, _) = page_guard.ctx_and_page();
        assert_eq!(ctx.row_ver().unwrap().state(), RowPageState::Frozen);
        drop(page_guard);
        let (still_frozen_pages, _) = sys.table.collect_frozen_pages(session.pool_guards()).await;
        assert_eq!(still_frozen_pages[0].page_id, first_frozen_page);

        reader.commit().await.unwrap();
        wait_gc_cutoff_after(&session, root_protected_by_reader).await;
        let checkpoint_ts = checkpoint_published(&sys.table, &mut session).await;
        let root_after_publish = sys.table.file().active_root();
        assert_eq!(root_after_publish.trx_id, checkpoint_ts);
        assert!(root_after_publish.pivot_row_id > root_before_delay.pivot_row_id);

        drop(reader_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_second_checkpoint_waits_for_previous_root_horizon() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 120, "second-delay").await;

        sys.table.freeze(&session, usize::MAX).await;
        let mut reader_session = sys.try_new_session().unwrap();
        let reader = reader_session.try_begin_trx().unwrap().unwrap();
        let first_checkpoint_ts = checkpoint_published(&sys.table, &mut session).await;
        assert_eq!(sys.table.file().active_root().trx_id, first_checkpoint_ts);

        let root_before_second = sys.table.file().active_root().clone();
        let outcome = sys.table.checkpoint(&mut session).await.unwrap();
        let CheckpointOutcome::Delayed { reason } = outcome else {
            panic!("expected second checkpoint to wait, got {outcome:?}");
        };
        assert_eq!(reason.root_cts, first_checkpoint_ts);
        assert_eq!(reason.min_active_sts, reader.sts);
        assert_root_metadata_unchanged(&root_before_second, &sys.table);

        reader.commit().await.unwrap();
        wait_gc_cutoff_after(&session, first_checkpoint_ts).await;
        let second_checkpoint_ts = checkpoint_published(&sys.table, &mut session).await;
        assert!(second_checkpoint_ts > first_checkpoint_ts);

        drop(reader_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_snapshot_consistency() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        let name = "y".repeat(256);
        insert_rows(&sys, &mut session, 0, 120, &name).await;

        sys.table.freeze(&session, 1).await;

        let mut read_trx = session.try_begin_trx().unwrap().unwrap();
        {
            let key = SelectKey::new(0, vec![Val::from(1)]);
            let stmt = read_trx.start_stmt();
            let res = stmt.select_row_mvcc(&sys.table, &key, &[0, 1]).await;
            assert!(matches!(res, Ok(SelectMvcc::Found(_))));
            read_trx = stmt.succeed();
        }

        let mut write_session = sys.try_new_session().unwrap();
        let mut write_trx = write_session.try_begin_trx().unwrap().unwrap();
        {
            let mut stmt = write_trx.start_stmt();
            let insert = vec![Val::from(10_000i32), Val::from("new")];
            let res = stmt.insert_row(&sys.table, insert).await;
            assert!(matches!(res, Ok(InsertMvcc::Inserted(_))));
            write_trx = stmt.succeed();
        }

        let mut checkpoint_session = sys.try_new_session().unwrap();
        checkpoint_published(&sys.table, &mut checkpoint_session).await;

        {
            let key = SelectKey::new(0, vec![Val::from(10_000i32)]);
            let stmt = read_trx.start_stmt();
            let res = stmt.select_row_mvcc(&sys.table, &key, &[0, 1]).await;
            assert!(matches!(res, Ok(SelectMvcc::NotFound)));
            read_trx = stmt.succeed();
        }

        write_trx.rollback().await.unwrap();
        read_trx.commit().await.unwrap();

        drop(session);
        drop(write_session);
        drop(checkpoint_session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_old_root_released_after_active_reader_purged() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        let name = "retained-root".repeat(64);
        insert_rows(&sys, &mut session, 0, 120, &name).await;

        sys.table.freeze(&session, usize::MAX).await;
        let retained_root_ptr = sys.table.file().active_root() as *const _ as usize;
        let drop_count_before = old_root_drop_count(retained_root_ptr);

        let mut read_session = sys.try_new_session().unwrap();
        let read_trx = read_session.try_begin_trx().unwrap().unwrap();

        let mut checkpoint_session = sys.try_new_session().unwrap();
        checkpoint_published(&sys.table, &mut checkpoint_session).await;

        assert_eq!(
            old_root_drop_count(retained_root_ptr),
            drop_count_before,
            "old root must stay retained while a pre-checkpoint transaction is active"
        );

        read_trx.commit().await.unwrap();
        sys.new_trx_insert(
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

        drop(read_session);
        drop(checkpoint_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_persistence_recovery() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let TestSys {
            engine,
            table,
            _temp_dir,
        } = sys;
        let table_id = table.table_id();
        let mut session = engine.try_new_session().unwrap();
        let name = "z".repeat(512);
        insert_rows_direct(&table, &mut session, 0, 150, &name).await;

        table.freeze(&session, usize::MAX).await;
        checkpoint_published(&table, &mut session).await;

        let root_before = table.file().active_root().clone();
        drop(table);

        let table_file = engine
            .table_fs
            .open_table_file(table_id, engine.disk_pool.clone_inner())
            .await
            .unwrap();
        let root_after = table_file.active_root();
        assert_eq!(root_after.pivot_row_id, root_before.pivot_row_id);
        assert_eq!(
            root_after.heap_redo_start_ts,
            root_before.heap_redo_start_ts
        );
        assert_eq!(
            root_after.deletion_cutoff_ts,
            root_before.deletion_cutoff_ts
        );

        drop(session);
        drop(table_file);
        drop(engine);
        drop(_temp_dir);
    });
}

#[test]
fn test_checkpoint_heartbeat() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        let name = "h".repeat(128);
        insert_rows(&sys, &mut session, 0, 40, &name).await;

        let root_before = sys.table.file().active_root().clone();
        checkpoint_published(&sys.table, &mut session).await;
        let root_after = sys.table.file().active_root();

        assert_eq!(root_after.pivot_row_id, root_before.pivot_row_id);
        assert!(root_after.heap_redo_start_ts > root_before.heap_redo_start_ts);
        assert!(root_after.deletion_cutoff_ts > root_before.deletion_cutoff_ts);
        assert_eq!(
            root_after.column_block_index_root,
            root_before.column_block_index_root
        );

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_gc_verification() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        let name = "g".repeat(1024);
        insert_rows(&sys, &mut session, 0, 200, &name).await;

        let allocated_before = sys.engine.mem_pool.allocated();
        sys.table.freeze(&session, usize::MAX).await;
        checkpoint_published(&sys.table, &mut session).await;
        let allocated_after = sys.engine.mem_pool.allocated();
        let mut reclaimed = allocated_after < allocated_before;
        for _ in 0..20 {
            smol::Timer::after(Duration::from_millis(200)).await;
            let allocated_now = sys.engine.mem_pool.allocated();
            if allocated_now < allocated_before {
                reclaimed = true;
                break;
            }
        }
        assert!(reclaimed, "row pages should be reclaimed after purge");

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_session_cached_insert_page_reuses_live_versioned_page() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let row_id = unwrap_insert_result(
            stmt.insert_row(&sys.table, vec![Val::from(1), Val::from("cached-row")])
                .await,
        );
        trx = stmt.succeed();
        trx.commit().await.unwrap();

        let (cached_page, cached_row_id) = session
            .load_active_insert_page(sys.table.table_id())
            .unwrap();
        assert_eq!(cached_row_id, row_id);
        assert!(
            sys.table
                .get_row_page_versioned_shared(session.pool_guards(), cached_page)
                .await
                .unwrap()
                .is_some()
        );
        session.save_active_insert_page(sys.table.table_id(), cached_page, cached_row_id);

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let next_row_id = unwrap_insert_result(
            stmt.insert_row(&sys.table, vec![Val::from(2), Val::from("still-cached")])
                .await,
        );
        trx = stmt.succeed();
        trx.commit().await.unwrap();

        let next_page_id = match sys.table.find_row(session.pool_guards(), next_row_id).await {
            RowLocation::RowPage(page_id) => page_id,
            RowLocation::LwcBlock { .. } | RowLocation::NotFound => {
                panic!("row should still be in the in-memory row store")
            }
        };
        assert_eq!(next_page_id, cached_page.page_id);

        let (next_cached_page, next_cached_row_id) = session
            .load_active_insert_page(sys.table.table_id())
            .unwrap();
        assert_eq!(next_cached_row_id, next_row_id);
        assert_eq!(next_cached_page, cached_page);

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_stale_session_cached_insert_page_falls_back_after_checkpoint_gc() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let row_id = unwrap_insert_result(
            stmt.insert_row(&sys.table, vec![Val::from(1), Val::from("cached-row")])
                .await,
        );
        trx = stmt.succeed();
        trx.commit().await.unwrap();

        let (cached_page, cached_row_id) = session
            .load_active_insert_page(sys.table.table_id())
            .unwrap();
        assert_eq!(cached_row_id, row_id);
        session.save_active_insert_page(sys.table.table_id(), cached_page, cached_row_id);

        sys.table.freeze(&session, usize::MAX).await;
        let mut checkpoint_session = sys.try_new_session().unwrap();
        checkpoint_published(&sys.table, &mut checkpoint_session).await;

        let mut reclaimed = false;
        for _ in 0..20 {
            if sys
                .table
                .get_row_page_versioned_shared(session.pool_guards(), cached_page)
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

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let post_gc_row_id = unwrap_insert_result(
            stmt.insert_row(&sys.table, vec![Val::from(2), Val::from("post-gc-row")])
                .await,
        );
        trx = stmt.succeed();
        trx.commit().await.unwrap();

        let key = single_key(2i32);
        sys.new_trx_select(&mut session, &key, |vals| {
            assert_eq!(vals, vec![Val::from(2), Val::from("post-gc-row")]);
        })
        .await;

        let (next_cached_page, next_cached_row_id) = session
            .load_active_insert_page(sys.table.table_id())
            .unwrap();
        assert_eq!(next_cached_row_id, post_gc_row_id);
        assert_ne!(next_cached_page, cached_page);

        drop(checkpoint_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_validated_row_page_shared_result_rejects_stale_reused_page_range() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let stale_row_id = unwrap_insert_result(
            stmt.insert_row(&sys.table, vec![Val::from(1), Val::from("cached-row")])
                .await,
        );
        trx = stmt.succeed();
        trx.commit().await.unwrap();

        let (stale_page, cached_row_id) = session
            .load_active_insert_page(sys.table.table_id())
            .unwrap();
        assert_eq!(cached_row_id, stale_row_id);

        sys.table.freeze(&session, usize::MAX).await;
        let mut checkpoint_session = sys.try_new_session().unwrap();
        checkpoint_published(&sys.table, &mut checkpoint_session).await;

        let mut reclaimed = false;
        for _ in 0..20 {
            if sys
                .table
                .get_row_page_versioned_shared(session.pool_guards(), stale_page)
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
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let mut stmt = trx.start_stmt();
            let row_id = unwrap_insert_result(
                stmt.insert_row(&sys.table, vec![Val::from(key), Val::from(&large[..])])
                    .await,
            );
            trx = stmt.succeed();
            trx.commit().await.unwrap();
            match sys.table.find_row(session.pool_guards(), row_id).await {
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

        let stale_guard = sys
            .table
            .accessor()
            .try_get_validated_row_page_shared_result(
                session.pool_guards(),
                stale_page.page_id,
                stale_row_id,
            )
            .await
            .unwrap();
        assert!(
            stale_guard.is_none(),
            "stale row id should not validate against the reused page range"
        );

        let reused_guard = sys
            .table
            .accessor()
            .try_get_validated_row_page_shared_result(
                session.pool_guards(),
                stale_page.page_id,
                reused_row_id,
            )
            .await
            .unwrap();
        assert!(
            reused_guard.is_some(),
            "reused row should validate on the reused page"
        );

        drop(reused_guard);
        drop(stale_guard);
        drop(checkpoint_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_mvcc_insert_surfaces_cached_insert_page_reload_error() {
    smol::block_on(async {
        let sys = TestSys::new_evictable_with_mem_size(9u64 * 1024 * 1024).await;
        let mut session = sys.try_new_session().unwrap();

        let large = "r".repeat(48 * 1024);
        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let row_id = unwrap_insert_result(
            stmt.insert_row(&sys.table, vec![Val::from(1), Val::from(&large[..])])
                .await,
        );
        trx = stmt.succeed();
        trx.commit().await.unwrap();

        let (cached_page, cached_row_id) = session
            .load_active_insert_page(sys.table.table_id())
            .unwrap();
        assert_eq!(cached_row_id, row_id);
        session.save_active_insert_page(sys.table.table_id(), cached_page, cached_row_id);

        let mut writer = sys.try_new_session().unwrap();
        for i in 2..258 {
            sys.new_trx_insert(&mut writer, vec![Val::from(i), Val::from(&large[..])])
                .await;
            if test_frame_kind(&sys.table.mem.mem_pool, cached_page.page_id) == FrameKind::Evicted {
                break;
            }
        }
        let mut evicted = false;
        for _ in 0..20 {
            if test_frame_kind(&sys.table.mem.mem_pool, cached_page.page_id) == FrameKind::Evicted {
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
            StorageBackendFileIdentity::from_path(sys._temp_dir.path().join("data.swp")).unwrap();
        let read_hook = Arc::new(FailingPageReadHook::for_page(
            mem_pool_file,
            cached_page.page_id,
            libc::EIO,
        ));
        let _hook = install_storage_backend_test_hook(read_hook.clone());

        let trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let res = stmt
            .insert_row(&sys.table, vec![Val::from(100), Val::from("reload-fails")])
            .await;
        let trx = stmt.fail().await.unwrap();
        trx.rollback().await.unwrap();
        assert!(
            matches!(res, Err(Error::IOError { .. })),
            "expected insert-page reload failure, got {res:?}"
        );
        assert!(
            read_hook.call_count() > 0,
            "cached insert page should reload from disk"
        );

        drop(writer);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_mvcc_rollback_poisons_runtime_on_row_page_reload_error() {
    smol::block_on(async {
        let sys = TestSys::new_evictable_with_mem_size(9u64 * 1024 * 1024).await;
        let mut session = sys.try_new_session().unwrap();

        let large = "r".repeat(48 * 1024);
        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let row_id = match stmt
            .insert_row(&sys.table, vec![Val::from(1), Val::from(&large[..])])
            .await
        {
            Ok(InsertMvcc::Inserted(row_id)) => row_id,
            res => panic!("res={res:?}"),
        };
        trx = stmt.succeed();

        let (cached_page, cached_row_id) = session
            .load_active_insert_page(sys.table.table_id())
            .unwrap();
        assert_eq!(cached_row_id, row_id);

        let mut writer = sys.try_new_session().unwrap();
        for i in 2..258 {
            sys.new_trx_insert(&mut writer, vec![Val::from(i), Val::from(&large[..])])
                .await;
            if test_frame_kind(&sys.table.mem.mem_pool, cached_page.page_id) == FrameKind::Evicted {
                break;
            }
        }
        let mut evicted = false;
        for _ in 0..20 {
            if test_frame_kind(&sys.table.mem.mem_pool, cached_page.page_id) == FrameKind::Evicted {
                evicted = true;
                break;
            }
            smol::Timer::after(Duration::from_millis(50)).await;
        }
        assert!(evicted, "rollback row page should be evicted before repro");

        let mem_pool_file =
            StorageBackendFileIdentity::from_path(sys._temp_dir.path().join("data.swp")).unwrap();
        let read_hook = Arc::new(FailingPageReadHook::for_page(
            mem_pool_file,
            cached_page.page_id,
            libc::EIO,
        ));
        let _hook = install_storage_backend_test_hook(read_hook);

        assert!(matches!(
            trx.rollback().await,
            Err(Error::StorageEnginePoisoned(
                StoragePoisonSource::RollbackAccess
            ))
        ));
        assert!(matches!(
            sys.engine.trx_sys.storage_poison_error(),
            Some(Error::StorageEnginePoisoned(
                StoragePoisonSource::RollbackAccess
            ))
        ));
        assert!(matches!(
            sys.engine.trx_sys.ensure_runtime_healthy(),
            Err(Error::StorageEnginePoisoned(
                StoragePoisonSource::RollbackAccess
            ))
        ));

        drop(writer);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_error_rollback() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        let name = "e".repeat(256);
        insert_rows(&sys, &mut session, 0, 80, &name).await;

        sys.table.freeze(&session, usize::MAX).await;
        let root_before = sys.table.file().active_root().clone();

        set_test_force_lwc_build_error(true);
        let res = sys.table.checkpoint(&mut session).await;
        set_test_force_lwc_build_error(false);
        assert!(res.is_err());

        let root_after = sys.table.file().active_root();
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_build_in_memory_secondary_indexes_reclaims_staged_indexes_on_error() {
    smol::block_on(async {
        use super::build_in_memory_secondary_indexes;
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
        let metadata = TableMetadata::new(
            vec![ColumnSpec::new(
                "id",
                ValKind::I32,
                ColumnAttributes::empty(),
            )],
            vec![
                IndexSpec::new("idx_a", vec![IndexKey::new(0)], IndexAttributes::PK),
                IndexSpec::new("idx_b", vec![IndexKey::new(0)], IndexAttributes::empty()),
            ],
        );

        let err = match build_in_memory_secondary_indexes(pool.guard(), &pool_guard, &metadata, 100)
            .await
        {
            Ok(_) => panic!("second secondary-index construction should fail in one-page pool"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::BufferPoolFull));
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

        let mut ddl_session = engine.try_new_session().unwrap();
        let mut index_specs = vec![IndexSpec::new(
            "idx_pk",
            vec![IndexKey::new(0)],
            IndexAttributes::PK,
        )];
        for idx in 0..12 {
            let name = format!("idx_name_{idx}");
            index_specs.push(IndexSpec::new(
                &name,
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

        let table = engine.catalog().get_table(table_id).await.unwrap();
        let mut session = engine.try_new_session().unwrap();
        let mut inserted = Vec::new();

        for batch in 0..96usize {
            let mut trx = session.try_begin_trx().unwrap().unwrap();
            for i in 0..64usize {
                let row_id = (batch * 64 + i) as i32;
                let seed = format!("{:08x}", row_id);
                let key = seed.repeat(64);
                let mut stmt = trx.start_stmt();
                let res = stmt
                    .insert_row(&table, vec![Val::from(row_id), Val::from(&key[..])])
                    .await;
                assert!(matches!(res, Ok(InsertMvcc::Inserted(_))), "res={res:?}");
                trx = stmt.succeed();
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
            let trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let res = stmt.select_row_mvcc(&table, &key, &[0, 1]).await;
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
            stmt.succeed().commit().await.unwrap();
        }

        let mut visible_rows = 0usize;
        table
            .accessor()
            .table_scan_uncommitted(session.pool_guards(), |_metadata, row| {
                if !row.is_deleted() {
                    visible_rows += 1;
                }
                true
            })
            .await;
        assert_eq!(visible_rows, inserted.len());

        drop(session);
        drop(table);
        drop(engine);
    });
}

struct TestSys {
    table: Arc<Table>,
    engine: Engine,
    _temp_dir: TempDir,
}

impl TestSys {
    #[inline]
    async fn new_evictable() -> Self {
        Self::new_evictable_with_mem_size(64u64 * 1024 * 1024).await
    }

    #[inline]
    async fn new_lightweight_evictable() -> Self {
        use crate::catalog::tests::table2;

        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = lightweight_test_engine_config(main_dir, "redo_testsys_lightweight")
            .build()
            .await
            .unwrap();
        let table_id = table2(&engine).await;
        let table = engine.catalog().get_table(table_id).await.unwrap();
        TestSys {
            engine,
            table,
            _temp_dir: temp_dir,
        }
    }

    #[inline]
    async fn new_evictable_with_mem_size(max_mem_size: u64) -> Self {
        use crate::catalog::tests::table2;
        // 64KB * 16
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = EngineConfig::default()
            .storage_root(main_dir)
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .max_mem_size(max_mem_size)
                    .max_file_size(128u64 * 1024 * 1024),
            )
            .trx(TrxSysConfig::default().log_file_stem("redo_testsys"))
            .file(
                FileSystemConfig::default()
                    .io_depth(16)
                    .readonly_buffer_size(128 * 1024 * 1024)
                    .data_dir("."),
            )
            .build()
            .await
            .unwrap();
        let table_id = table2(&engine).await;
        let table = engine.catalog().get_table(table_id).await.unwrap();
        TestSys {
            engine,
            table,
            _temp_dir: temp_dir,
        }
    }

    #[inline]
    async fn new_evictable_with_non_unique_name_index() -> Self {
        use crate::catalog::{
            ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableSpec,
        };
        use crate::value::ValKind;

        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = EngineConfig::default()
            .storage_root(main_dir)
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .max_mem_size(64u64 * 1024 * 1024)
                    .max_file_size(128u64 * 1024 * 1024),
            )
            .trx(TrxSysConfig::default().log_file_stem("redo_testsys_non_unique"))
            .file(
                FileSystemConfig::default()
                    .io_depth(16)
                    .readonly_buffer_size(128 * 1024 * 1024)
                    .data_dir("."),
            )
            .build()
            .await
            .unwrap();
        let mut ddl_session = engine.try_new_session().unwrap();
        let table_id = ddl_session
            .create_table(
                TableSpec::new(vec![
                    ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                    ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                ]),
                vec![
                    IndexSpec::new("idx_id", vec![IndexKey::new(0)], IndexAttributes::PK),
                    IndexSpec::new("idx_name", vec![IndexKey::new(1)], IndexAttributes::empty()),
                ],
            )
            .await
            .unwrap();
        drop(ddl_session);
        let table = engine.catalog().get_table(table_id).await.unwrap();
        TestSys {
            engine,
            table,
            _temp_dir: temp_dir,
        }
    }
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

impl TestSys {
    #[inline]
    fn clean_all(self) {
        drop(self);
    }

    #[inline]
    async fn new_trx_insert(&self, session: &mut Session, insert: Vec<Val>) {
        let mut trx = session.try_begin_trx().unwrap().unwrap();
        trx = self.trx_insert(trx, insert).await;
        trx.commit().await.unwrap();
    }

    #[inline]
    async fn trx_insert(&self, trx: ActiveTrx, insert: Vec<Val>) -> ActiveTrx {
        let mut stmt = trx.start_stmt();
        let res = stmt.insert_row(&self.table, insert).await;
        if !matches!(res, Ok(InsertMvcc::Inserted(_))) {
            panic!("res={:?}", res);
        }
        // assert!(res.is_ok());
        stmt.succeed()
    }

    #[inline]
    async fn new_trx_delete(&self, session: &mut Session, key: &SelectKey) {
        let mut trx = session.try_begin_trx().unwrap().unwrap();
        trx = self.trx_delete(trx, key).await;
        trx.commit().await.unwrap();
    }

    #[inline]
    async fn trx_delete(&self, trx: ActiveTrx, key: &SelectKey) -> ActiveTrx {
        let mut stmt = trx.start_stmt();
        let res = stmt.delete_row(&self.table, key).await;
        if !matches!(res, Ok(DeleteMvcc::Deleted)) {
            panic!("res={:?}", res);
        }
        // assert!(res.is_ok());
        stmt.succeed()
    }

    #[inline]
    async fn new_trx_update(&self, session: &mut Session, key: &SelectKey, update: Vec<UpdateCol>) {
        let mut trx = session.try_begin_trx().unwrap().unwrap();
        trx = self.trx_update(trx, key, update).await;
        trx.commit().await.unwrap();
    }

    #[inline]
    async fn trx_update(
        &self,
        trx: ActiveTrx,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> ActiveTrx {
        let mut stmt = trx.start_stmt();
        let res = stmt.update_row(&self.table, key, update).await;
        if !matches!(res, Ok(UpdateMvcc::Updated(_))) {
            panic!("res={:?}", res);
        }
        // assert!(res.is_ok());
        stmt.succeed()
    }

    #[inline]
    async fn new_trx_select<F: FnOnce(Vec<Val>)>(
        &self,
        session: &mut Session,
        key: &SelectKey,
        action: F,
    ) {
        let mut trx = session.try_begin_trx().unwrap().unwrap();
        trx = self.trx_select(trx, key, action).await;
        trx.commit().await.unwrap();
    }

    #[inline]
    async fn new_trx_select_not_found(&self, session: &mut Session, key: &SelectKey) {
        let mut trx = session.try_begin_trx().unwrap().unwrap();
        trx = self.trx_select_not_found(trx, key).await;
        trx.commit().await.unwrap();
    }

    #[inline]
    async fn trx_select_not_found(&self, trx: ActiveTrx, key: &SelectKey) -> ActiveTrx {
        let stmt = trx.start_stmt();
        let res = stmt.select_row_mvcc(&self.table, key, &[0, 1]).await;
        assert!(matches!(res, Ok(SelectMvcc::NotFound)));
        stmt.succeed()
    }

    #[inline]
    async fn trx_select<F: FnOnce(Vec<Val>)>(
        &self,
        trx: ActiveTrx,
        key: &SelectKey,
        action: F,
    ) -> ActiveTrx {
        let stmt = trx.start_stmt();
        let res = stmt.select_row_mvcc(&self.table, key, &[0, 1]).await;
        if !matches!(res, Ok(SelectMvcc::Found(_))) {
            panic!("res={:?}", res);
        }
        // assert!(res.is_ok());
        action(res.unwrap().unwrap_found());
        stmt.succeed()
    }

    #[inline]
    fn try_new_session(&self) -> Result<Session> {
        self.engine.try_new_session()
    }
}

async fn insert_one_row(table: &Table, session: &mut Session, values: Vec<Val>) -> RowID {
    let mut trx = session.try_begin_trx().unwrap().unwrap();
    let mut stmt = trx.start_stmt();
    let insert = stmt.insert_row(table, values).await;
    let Ok(InsertMvcc::Inserted(row_id)) = insert else {
        panic!("insert should succeed: {insert:?}");
    };
    trx = stmt.succeed();
    trx.commit().await.unwrap();
    row_id
}

fn single_key<V: Into<Val>>(value: V) -> SelectKey {
    SelectKey {
        index_no: 0,
        vals: vec![value.into()],
    }
}

fn name_key(value: &str) -> SelectKey {
    SelectKey {
        index_no: 1,
        vals: vec![Val::from(value)],
    }
}

fn unwrap_insert_result(res: Result<InsertMvcc>) -> RowID {
    match res {
        Ok(InsertMvcc::Inserted(row_id)) => row_id,
        res => panic!("unexpected insert result: {res:?}"),
    }
}

async fn assert_row_in_lwc(
    table: &Table,
    guards: &PoolGuards,
    key: &SelectKey,
    sts: TrxID,
) -> RowID {
    let index = table.sec_idx()[key.index_no].unique().unwrap();
    let Some((row_id, _)) = index
        .lookup(guards.index_guard(), &key.vals, sts)
        .await
        .expect("index lookup should succeed")
    else {
        panic!("row should exist");
    };
    match table.find_row(guards, row_id).await {
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
    let root = table.file().active_root().secondary_index_roots[key.index_no];
    let tree = table
        .storage
        .secondary_index_runtime(key.index_no)
        .unwrap()
        .open_unique_at(root, guards.disk_guard())
        .unwrap();
    tree.lookup(&key.vals).await.unwrap()
}

async fn non_unique_disk_tree_prefix_scan(
    table: &Table,
    guards: &PoolGuards,
    key: &SelectKey,
) -> Vec<RowID> {
    let root = table.file().active_root().secondary_index_roots[key.index_no];
    let tree = table
        .storage
        .secondary_index_runtime(key.index_no)
        .unwrap()
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
    let index = table.sec_idx()[key.index_no].unique().unwrap();
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

async fn checkpoint_published(table: &Table, session: &mut Session) -> TrxID {
    match table.checkpoint(session).await.unwrap() {
        CheckpointOutcome::Published { checkpoint_ts } => checkpoint_ts,
        CheckpointOutcome::Delayed { reason } => {
            panic!("checkpoint should publish, delayed by {reason:?}")
        }
    }
}

fn assert_root_metadata_unchanged(before: &crate::file::table_file::ActiveRoot, table: &Table) {
    let after = table.file().active_root();
    assert_eq!(after.trx_id, before.trx_id);
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

async fn insert_rows(sys: &TestSys, session: &mut Session, start: i32, count: i32, name: &str) {
    let mut trx = session.try_begin_trx().unwrap().unwrap();
    for i in 0..count {
        let insert = vec![Val::from(start + i), Val::from(name)];
        trx = sys.trx_insert(trx, insert).await;
    }
    trx.commit().await.unwrap();
}

async fn insert_rows_direct(
    table: &Table,
    session: &mut Session,
    start: i32,
    count: i32,
    name: &str,
) {
    let mut trx = session.try_begin_trx().unwrap().unwrap();
    for i in 0..count {
        let insert = vec![Val::from(start + i), Val::from(name)];
        let mut stmt = trx.start_stmt();
        let res = stmt.insert_row(table, insert).await;
        assert!(matches!(res, Ok(InsertMvcc::Inserted(_))));
        trx = stmt.succeed();
    }
    trx.commit().await.unwrap();
}

async fn delete_key_range_and_wait_gc_cutoff(
    sys: &TestSys,
    session: &mut Session,
    start: i32,
    count: i32,
) {
    let mut max_delete_cts = 0;
    for i in 0..count {
        let mut trx = session.try_begin_trx().unwrap().unwrap();
        trx = sys.trx_delete(trx, &single_key(start + i)).await;
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
            let table = engine.catalog().get_table(table_id).await.unwrap();

            let mut session = engine.try_new_session().unwrap();
            let user_read_set = &[0usize, 1];
            let trx = session.try_begin_trx().unwrap().unwrap();
            let mut stmt = trx.start_stmt();
            for i in 0i32..5i32 {
                let res = table
                    .accessor()
                    .insert_mvcc(&mut stmt, vec![Val::from(i), Val::from(i)])
                    .await;
                assert!(matches!(res, Ok(InsertMvcc::Inserted(_))));
            }
            stmt.succeed().commit().await.unwrap();

            let trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let key = SelectKey::new(0, vec![Val::from(1i32)]);
            let res = table
                .accessor()
                .index_lookup_unique_mvcc(&stmt, &key, user_read_set)
                .await;
            stmt.succeed().commit().await.unwrap();
            assert!(matches!(res, Ok(SelectMvcc::Found(_))));

            let trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let key = SelectKey::new(1, vec![Val::from(1i32)]);
            let res = table
                .accessor()
                .index_scan_mvcc(&stmt, &key, user_read_set)
                .await;
            stmt.succeed().commit().await.unwrap();
            assert!(res.unwrap().unwrap_rows().len() == 1);

            let trx = session.try_begin_trx().unwrap().unwrap();
            let mut stmt = trx.start_stmt();
            let key = SelectKey::new(0, vec![Val::from(1i32)]);
            let update = vec![UpdateCol {
                idx: 1,
                val: Val::from(0i32),
            }];
            let res = table
                .accessor()
                .update_unique_mvcc(&mut stmt, &key, update)
                .await;
            stmt.succeed().commit().await.unwrap();
            assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));

            let trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let key = SelectKey::new(1, vec![Val::from(0i32)]);
            let res = table
                .accessor()
                .index_scan_mvcc(&stmt, &key, user_read_set)
                .await;
            stmt.succeed().commit().await.unwrap();
            assert!(res.unwrap().unwrap_rows().len() == 2);

            let trx = session.try_begin_trx().unwrap().unwrap();
            let mut stmt = trx.start_stmt();
            let key = SelectKey::new(0, vec![Val::from(0i32)]);
            let res = table
                .accessor()
                .delete_unique_mvcc(&mut stmt, &key, false)
                .await;
            stmt.succeed().commit().await.unwrap();
            assert!(matches!(res, Ok(DeleteMvcc::Deleted)));

            let trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let key = SelectKey::new(1, vec![Val::from(0i32)]);
            let res = table
                .accessor()
                .index_scan_mvcc(&stmt, &key, user_read_set)
                .await;
            _ = stmt.succeed().commit().await.unwrap();
            assert!(res.unwrap().unwrap_rows().len() == 1);
        }
        drop(engine);
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
            let table = engine.catalog().get_table(table_id).await.unwrap();

            let mut session = engine.try_new_session().unwrap();
            let user_read_set = &[0usize, 1];
            let trx = session.try_begin_trx().unwrap().unwrap();
            let mut stmt = trx.start_stmt();
            for i in 0i32..5i32 {
                let res = table
                    .accessor()
                    .insert_mvcc(&mut stmt, vec![Val::from(i), Val::from(i)])
                    .await;
                assert!(matches!(res, Ok(InsertMvcc::Inserted(_))));
            }
            stmt.succeed().commit().await.unwrap();

            let trx = session.try_begin_trx().unwrap().unwrap();
            let mut stmt = trx.start_stmt();
            let res = table
                .accessor()
                .insert_mvcc(&mut stmt, vec![Val::from(5i32), Val::from(5i32)])
                .await;
            assert!(matches!(res, Ok(InsertMvcc::Inserted(_))));
            stmt.succeed().rollback().await.unwrap();

            let trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let key = SelectKey::new(0, vec![Val::from(5i32)]);
            let res = table
                .accessor()
                .index_lookup_unique_mvcc(&stmt, &key, user_read_set)
                .await;
            stmt.succeed().commit().await.unwrap();
            assert!(matches!(res, Ok(SelectMvcc::NotFound)));

            let trx = session.try_begin_trx().unwrap().unwrap();
            let mut stmt = trx.start_stmt();
            let key = SelectKey::new(0, vec![Val::from(1i32)]);
            let update = vec![UpdateCol {
                idx: 1,
                val: Val::from(0i32),
            }];
            let res = table
                .accessor()
                .update_unique_mvcc(&mut stmt, &key, update)
                .await;
            assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
            stmt.succeed().rollback().await.unwrap();

            let trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let key = SelectKey::new(0, vec![Val::from(1i32)]);
            let res = table
                .accessor()
                .index_lookup_unique_mvcc(&stmt, &key, user_read_set)
                .await;
            stmt.succeed().commit().await.unwrap();
            assert!(matches!(res, Ok(SelectMvcc::Found(_))));
            let vals = res.unwrap().unwrap_found();
            assert!(vals[0] == Val::from(1i32) && vals[1] == Val::from(1i32));

            let trx = session.try_begin_trx().unwrap().unwrap();
            let mut stmt = trx.start_stmt();
            let key = SelectKey::new(0, vec![Val::from(0i32)]);
            let res = table
                .accessor()
                .delete_unique_mvcc(&mut stmt, &key, false)
                .await;
            assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
            stmt.succeed().rollback().await.unwrap();

            let trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let key = SelectKey::new(0, vec![Val::from(0i32)]);
            let res = table
                .accessor()
                .index_lookup_unique_mvcc(&stmt, &key, user_read_set)
                .await;
            stmt.succeed().commit().await.unwrap();
            assert!(matches!(res, Ok(SelectMvcc::Found(_))));
            let vals = res.unwrap().unwrap_found();
            assert!(vals[0] == Val::from(0i32) && vals[1] == Val::from(0i32));

            let mut trx = session.try_begin_trx().unwrap().unwrap();
            let key = SelectKey::new(0, vec![Val::from(3i32)]);
            let mut stmt = trx.start_stmt();
            let res = table
                .accessor()
                .delete_unique_mvcc(&mut stmt, &key, false)
                .await;
            assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
            trx = stmt.succeed();
            stmt = trx.start_stmt();
            let res = table
                .accessor()
                .insert_mvcc(&mut stmt, vec![Val::from(3), Val::from(3)])
                .await;
            assert!(matches!(res, Ok(InsertMvcc::Inserted(_))));
            trx = stmt.succeed();
            trx.rollback().await.unwrap();

            let trx = session.try_begin_trx().unwrap().unwrap();
            let stmt = trx.start_stmt();
            let key = SelectKey::new(0, vec![Val::from(3i32)]);
            let res = table
                .accessor()
                .index_lookup_unique_mvcc(&stmt, &key, user_read_set)
                .await;
            _ = stmt.succeed().commit().await.unwrap();
            assert!(matches!(res, Ok(SelectMvcc::Found(_))));
            let vals = res.unwrap().unwrap_found();
            assert!(vals[0] == Val::from(3i32) && vals[1] == Val::from(3i32));
        }
        drop(engine);
    })
}
