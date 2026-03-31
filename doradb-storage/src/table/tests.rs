use super::{DeleteInternal, FrozenPage, InsertRowIntoPage, UpdateRowInplace};
use crate::buffer::BufferPool;
use crate::buffer::frame::FrameKind;
use crate::buffer::page::{PAGE_SIZE, PageID};
use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TableFileSystemConfig, TrxSysConfig};
use crate::engine::Engine;
use crate::error::{
    Error, PersistedFileKind, PersistedPageCorruptionCause, PersistedPageKind, Result,
    StoragePoisonSource,
};
use crate::file::build_test_fs_in;
use crate::file::cow_file::COW_FILE_PAGE_SIZE;
use crate::file::page_integrity::{PAGE_INTEGRITY_HEADER_SIZE, write_page_checksum};
use crate::index::{
    COLUMN_BLOCK_HEADER_SIZE, COLUMN_BLOCK_LEAF_PREFIX_SIZE, ColumnBlockIndex, RowLocation,
    UniqueIndex, load_entry_deletion_deltas,
};
use crate::io::{AIOKind, StorageBackendOp, StorageBackendTestHook, set_storage_backend_test_hook};
use crate::latch::LatchFallbackMode;
use crate::row::ops::{DeleteMvcc, InsertMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
use crate::row::{RowID, RowPage, RowRead};
use crate::session::Session;
use crate::table::{DeleteMarker, Table, TableAccess, TablePersistence};
use crate::trx::row::LockRowForWrite;
use crate::trx::undo::RowUndoKind;
use crate::trx::{ActiveTrx, TrxID};
use crate::value::Val;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};
use std::time::Duration;
use tempfile::TempDir;

static STORAGE_BACKEND_TEST_HOOK_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct InstalledStorageBackendTestHook {
    previous: Option<Arc<dyn StorageBackendTestHook>>,
    guard: Option<MutexGuard<'static, ()>>,
}

impl Drop for InstalledStorageBackendTestHook {
    #[inline]
    fn drop(&mut self) {
        let _ = set_storage_backend_test_hook(self.previous.take());
        drop(self.guard.take());
    }
}

fn install_storage_backend_test_hook(
    hook: Arc<dyn StorageBackendTestHook>,
) -> InstalledStorageBackendTestHook {
    let guard = STORAGE_BACKEND_TEST_HOOK_LOCK.lock().unwrap();
    InstalledStorageBackendTestHook {
        previous: set_storage_backend_test_hook(Some(hook)),
        guard: Some(guard),
    }
}

struct FailingPageReadHook {
    fd: RawFd,
    offset: usize,
    errno: i32,
    calls: AtomicUsize,
}

impl FailingPageReadHook {
    #[inline]
    fn for_page(fd: RawFd, page_id: PageID, errno: i32) -> Self {
        Self {
            fd,
            offset: page_id as usize * PAGE_SIZE,
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
        op.kind() == AIOKind::Read && op.fd() == self.fd && op.offset() == self.offset
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
        sys.table.data_checkpoint(&mut session).await.unwrap();

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
        sys.table.data_checkpoint(&mut session).await.unwrap();

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
        sys.table.data_checkpoint(&mut session).await.unwrap();

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
            sys.table.disk_pool(),
        );
        let resolved = column_index
            .locate_and_resolve_row(row_id)
            .await
            .unwrap()
            .unwrap();

        match sys.table.find_row(session.pool_guards(), row_id).await {
            RowLocation::LwcPage {
                page_id,
                row_idx,
                row_shape_fingerprint,
            } => {
                assert_eq!(page_id, resolved.block_id());
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
        sys.table.data_checkpoint(&mut session).await.unwrap();

        let key = single_key(1i32);
        let trx = session.try_begin_trx().unwrap().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, trx.sts).await;
        trx.commit().await.unwrap();

        let active_root = sys.table.file().active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.disk_pool(),
        );
        let entry = index.locate_block(row_id).await.unwrap().unwrap();
        let block_id = entry.block_id();

        let fs = build_test_fs_in(sys._temp_dir.path());
        corrupt_page_checksum(fs.table_file_path(sys.table.table_id()), block_id);

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let stmt = trx.start_stmt();
        let res = stmt.select_row_mvcc(&sys.table, &key, &[0, 1]).await;
        match res {
            Err(Error::PersistedPageCorrupted {
                file_kind: PersistedFileKind::TableFile,
                page_kind: PersistedPageKind::LwcPage,
                page_id,
                cause: PersistedPageCorruptionCause::ChecksumMismatch,
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
        sys.table.data_checkpoint(&mut session).await.unwrap();

        let key = single_key(1i32);
        let trx = session.try_begin_trx().unwrap().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, trx.sts).await;
        trx.commit().await.unwrap();

        let active_root = sys.table.file().active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.disk_pool(),
        );
        let entry = index.locate_block(row_id).await.unwrap().unwrap();

        let fs = build_test_fs_in(sys._temp_dir.path());
        corrupt_leaf_row_codec(
            fs.table_file_path(sys.table.table_id()),
            entry.leaf_page_id,
            0,
        );
        let _ = sys
            .table
            .disk_pool()
            .invalidate_block_id(entry.leaf_page_id);

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let stmt = trx.start_stmt();
        let res = stmt.select_row_mvcc(&sys.table, &key, &[0, 1]).await;
        match res {
            Err(Error::PersistedPageCorrupted {
                file_kind: PersistedFileKind::TableFile,
                page_kind: PersistedPageKind::ColumnBlockIndex,
                page_id,
                cause: PersistedPageCorruptionCause::InvalidPayload,
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
        sys.table.data_checkpoint(&mut session).await.unwrap();

        let key = single_key(1i32);
        let trx = session.try_begin_trx().unwrap().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, trx.sts).await;
        trx.commit().await.unwrap();

        let active_root = sys.table.file().active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.disk_pool(),
        );
        let entry = index.locate_block(row_id).await.unwrap().unwrap();

        let fs = build_test_fs_in(sys._temp_dir.path());
        corrupt_lwc_row_shape_fingerprint(
            fs.table_file_path(sys.table.table_id()),
            entry.block_id(),
        );
        let _ = sys.table.disk_pool().invalidate_block_id(entry.block_id());

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let stmt = trx.start_stmt();
        let res = stmt.select_row_mvcc(&sys.table, &key, &[0, 1]).await;
        match res {
            Err(Error::PersistedPageCorrupted {
                file_kind: PersistedFileKind::TableFile,
                page_kind: PersistedPageKind::LwcPage,
                page_id,
                cause: PersistedPageCorruptionCause::InvalidPayload,
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
        sys.table.data_checkpoint(&mut session).await.unwrap();

        let key = single_key(2i32);
        let mut reader_session = sys.try_new_session().unwrap();
        let trx = reader_session.try_begin_trx().unwrap().unwrap();
        let _ = assert_row_in_lwc(&sys.table, reader_session.pool_guards(), &key, trx.sts).await;
        trx.commit().await.unwrap();

        let mut trx = session.try_begin_trx().unwrap().unwrap();
        let mut stmt = trx.start_stmt();
        let res = stmt.delete_row(&sys.table, &key).await;
        assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
        trx = stmt.succeed();
        trx.rollback().await.unwrap();

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
        sys.table
            .data_checkpoint(&mut checkpoint_session)
            .await
            .unwrap();

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
        sys.table.data_checkpoint(&mut session).await.unwrap();

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
        sys.table.data_checkpoint(&mut session).await.unwrap();

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
fn test_checkpoint_for_deletion_persists_committed_markers() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        sys.table
            .checkpoint_for_new_data(&mut session)
            .await
            .unwrap();

        let key = single_key(6i32);
        let reader = session.try_begin_trx().unwrap().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, session.pool_guards(), &key, reader.sts).await;
        reader.commit().await.unwrap();

        sys.new_trx_delete(&mut session, &key).await;
        let marker = sys.table.deletion_buffer().get(row_id).unwrap();
        let marker_ts = match marker {
            DeleteMarker::Committed(ts) => ts,
            DeleteMarker::Ref(status) => status.ts(),
        };
        let trx_sys = session.engine().trx_sys.clone();
        // `checkpoint_for_deletion` selects markers with `cts < cutoff_ts`.
        // `cutoff_ts` comes from GC-visible min-active STS and can lag right after delete commit,
        // so we wait until this marker becomes eligible to avoid timing flakes.
        let mut ready = false;
        for _ in 0..50 {
            if trx_sys.calc_min_active_sts_for_gc() > marker_ts {
                ready = true;
                break;
            }
            smol::Timer::after(Duration::from_millis(20)).await;
        }
        assert!(
            ready,
            "deletion marker ts {} not yet below checkpoint cutoff",
            marker_ts
        );
        let active_root = sys.table.file().active_root();
        let index_before = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.disk_pool(),
        );
        let entry_before = index_before
            .locate_block(row_id)
            .await
            .unwrap()
            .expect("persisted entry should exist before delete checkpoint");

        sys.table
            .checkpoint_for_deletion(&mut session)
            .await
            .unwrap();

        let active_root = sys.table.file().active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.disk_pool(),
        );
        let entry = index
            .locate_block(row_id)
            .await
            .unwrap()
            .expect("persisted entry should exist");
        assert_eq!(entry.block_id(), entry_before.block_id());
        assert_eq!(entry.end_row_id(), entry_before.end_row_id());
        assert_eq!(entry.row_id_span(), entry_before.row_id_span());
        assert_eq!(entry.row_count(), entry_before.row_count());
        let deltas = load_entry_deletion_deltas(&index, &entry).await.unwrap();
        let expected_delta = (row_id - entry.start_row_id) as u32;
        assert!(deltas.contains(&expected_delta));

        drop(trx_sys);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_for_deletion_skips_markers_at_or_after_cutoff() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        sys.table
            .checkpoint_for_new_data(&mut session)
            .await
            .unwrap();

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
        sys.table
            .checkpoint_for_deletion(&mut checkpoint_session)
            .await
            .unwrap();

        let active_root = sys.table.file().active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.disk_pool(),
        );
        let entry = index
            .locate_block(row_id)
            .await
            .unwrap()
            .expect("persisted entry should exist");
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
fn test_checkpoint_for_deletion_fails_on_invalid_v2_delete_metadata() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(&session, usize::MAX).await;
        sys.table
            .checkpoint_for_new_data(&mut session)
            .await
            .unwrap();

        let key1 = single_key(6i32);
        let reader = session.try_begin_trx().unwrap().unwrap();
        let row_id1 = assert_row_in_lwc(&sys.table, session.pool_guards(), &key1, reader.sts).await;
        reader.commit().await.unwrap();

        sys.new_trx_delete(&mut session, &key1).await;
        let marker1 = sys.table.deletion_buffer().get(row_id1).unwrap();
        let marker1_ts = match marker1 {
            DeleteMarker::Committed(ts) => ts,
            DeleteMarker::Ref(status) => status.ts(),
        };
        let trx_sys = session.engine().trx_sys.clone();
        let mut ready = false;
        for _ in 0..50 {
            if trx_sys.calc_min_active_sts_for_gc() > marker1_ts {
                ready = true;
                break;
            }
            smol::Timer::after(Duration::from_millis(20)).await;
        }
        assert!(
            ready,
            "deletion marker ts {} not yet below checkpoint cutoff",
            marker1_ts
        );
        sys.table
            .checkpoint_for_deletion(&mut session)
            .await
            .unwrap();

        let active_root = sys.table.file().active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            sys.table.disk_pool(),
        );
        let entry = index
            .locate_block(row_id1)
            .await
            .unwrap()
            .expect("persisted entry should exist");

        let fs = build_test_fs_in(sys._temp_dir.path());
        corrupt_leaf_delete_codec(
            fs.table_file_path(sys.table.table_id()),
            entry.leaf_page_id,
            0,
        );
        let _ = sys
            .table
            .disk_pool()
            .invalidate_block_id(entry.leaf_page_id);

        let err = sys
            .table
            .checkpoint_for_deletion(&mut session)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::PersistedPageCorrupted {
                file_kind: PersistedFileKind::TableFile,
                page_kind: PersistedPageKind::ColumnBlockIndex,
                page_id,
                cause: PersistedPageCorruptionCause::InvalidPayload,
            } if page_id == entry.leaf_page_id
        ));

        drop(trx_sys);
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
            RowLocation::LwcPage { .. } => unreachable!("lwc page"),
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
            RowLocation::LwcPage { .. } => unreachable!("row page expected"),
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
fn test_data_checkpoint_basic_flow() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        let name = "x".repeat(1024);
        insert_rows(&sys, &mut session, 0, 200, &name).await;

        let old_root = sys.table.file().active_root().clone();
        sys.table.freeze(&session, usize::MAX).await;
        let (frozen_pages, _) = sys.table.collect_frozen_pages(session.pool_guards()).await;
        assert!(!frozen_pages.is_empty());

        sys.table.data_checkpoint(&mut session).await.unwrap();

        let new_root = sys.table.file().active_root();
        assert!(new_root.pivot_row_id > old_root.pivot_row_id);
        assert!(new_root.column_block_index_root > 0);

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_data_checkpoint_snapshot_consistency() {
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
        sys.table
            .data_checkpoint(&mut checkpoint_session)
            .await
            .unwrap();

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
fn test_data_checkpoint_persistence_recovery() {
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
        table.data_checkpoint(&mut session).await.unwrap();

        let root_before = table.file().active_root().clone();
        drop(table);

        let (table_file, _) = engine
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

        drop(session);
        drop(table_file);
        drop(engine);
        drop(_temp_dir);
    });
}

#[test]
fn test_data_checkpoint_heartbeat() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        let name = "h".repeat(128);
        insert_rows(&sys, &mut session, 0, 40, &name).await;

        let root_before = sys.table.file().active_root().clone();
        sys.table.data_checkpoint(&mut session).await.unwrap();
        let root_after = sys.table.file().active_root();

        assert_eq!(root_after.pivot_row_id, root_before.pivot_row_id);
        assert!(root_after.heap_redo_start_ts > root_before.heap_redo_start_ts);
        assert_eq!(
            root_after.column_block_index_root,
            root_before.column_block_index_root
        );

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_data_checkpoint_gc_verification() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        let name = "g".repeat(1024);
        insert_rows(&sys, &mut session, 0, 200, &name).await;

        let allocated_before = sys.engine.mem_pool.allocated();
        sys.table.freeze(&session, usize::MAX).await;
        sys.table.data_checkpoint(&mut session).await.unwrap();
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
            RowLocation::LwcPage { .. } | RowLocation::NotFound => {
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
        sys.table
            .data_checkpoint(&mut checkpoint_session)
            .await
            .unwrap();

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
        sys.table
            .data_checkpoint(&mut checkpoint_session)
            .await
            .unwrap();

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
                RowLocation::LwcPage { .. } | RowLocation::NotFound => {
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
            if sys.table.mem.mem_pool.test_frame_kind(cached_page.page_id) == FrameKind::Evicted {
                break;
            }
        }
        let mut evicted = false;
        for _ in 0..20 {
            if sys.table.mem.mem_pool.test_frame_kind(cached_page.page_id) == FrameKind::Evicted {
                evicted = true;
                break;
            }
            smol::Timer::after(Duration::from_millis(50)).await;
        }
        assert!(
            evicted,
            "cached insert page should be evicted before repro insert"
        );

        let read_hook = Arc::new(FailingPageReadHook::for_page(
            sys.table.mem.mem_pool.test_raw_fd(),
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
            matches!(res, Err(Error::IOError)),
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
            if sys.table.mem.mem_pool.test_frame_kind(cached_page.page_id) == FrameKind::Evicted {
                break;
            }
        }
        let mut evicted = false;
        for _ in 0..20 {
            if sys.table.mem.mem_pool.test_frame_kind(cached_page.page_id) == FrameKind::Evicted {
                evicted = true;
                break;
            }
            smol::Timer::after(Duration::from_millis(50)).await;
        }
        assert!(evicted, "rollback row page should be evicted before repro");

        let read_hook = Arc::new(FailingPageReadHook::for_page(
            sys.table.mem.mem_pool.test_raw_fd(),
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
fn test_data_checkpoint_error_rollback() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.try_new_session().unwrap();
        let name = "e".repeat(256);
        insert_rows(&sys, &mut session, 0, 80, &name).await;

        sys.table.freeze(&session, usize::MAX).await;
        let root_before = sys.table.file().active_root().clone();

        super::set_test_force_lwc_build_error(true);
        let res = sys.table.data_checkpoint(&mut session).await;
        super::set_test_force_lwc_build_error(false);
        assert!(res.is_err());

        let root_after = sys.table.file().active_root();
        assert_eq!(root_after.pivot_row_id, root_before.pivot_row_id);
        assert_eq!(
            root_after.heap_redo_start_ts,
            root_before.heap_redo_start_ts
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
                    .role(crate::buffer::PoolRole::Mem)
                    .max_mem_size(64u64 * 1024 * 1024)
                    .max_file_size(128u64 * 1024 * 1024),
            )
            .trx(
                TrxSysConfig::default()
                    .log_file_stem("redo_index_evict")
                    .skip_recovery(true),
            )
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
    async fn new_evictable_with_mem_size(max_mem_size: u64) -> Self {
        use crate::catalog::tests::table2;
        // 64KB * 16
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_path_buf();
        let engine = EngineConfig::default()
            .storage_root(main_dir)
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .role(crate::buffer::PoolRole::Mem)
                    .max_mem_size(max_mem_size)
                    .max_file_size(128u64 * 1024 * 1024),
            )
            .trx(
                TrxSysConfig::default()
                    .log_file_stem("redo_testsys")
                    .skip_recovery(true),
            )
            .file(
                TableFileSystemConfig::default()
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

fn single_key<V: Into<Val>>(value: V) -> SelectKey {
    SelectKey {
        index_no: 0,
        vals: vec![value.into()],
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
    guards: &crate::buffer::PoolGuards,
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
        RowLocation::LwcPage { .. } => row_id,
        RowLocation::RowPage(..) => panic!("row should be in lwc"),
        RowLocation::NotFound => panic!("row should exist"),
    }
}

fn corrupt_page_checksum(path: impl AsRef<std::path::Path>, page_id: u64) {
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
    page_id: u64,
    rewrite: impl FnOnce(&mut [u8]),
) {
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
    write_page_checksum(&mut page);
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&page).unwrap();
    file.flush().unwrap();
}

fn corrupt_leaf_delete_codec(path: impl AsRef<std::path::Path>, page_id: u64, prefix_idx: usize) {
    const DELETE_CODEC_OFFSET_IN_PREFIX: usize = 38;
    let byte_offset = PAGE_INTEGRITY_HEADER_SIZE
        + COLUMN_BLOCK_HEADER_SIZE
        + prefix_idx * COLUMN_BLOCK_LEAF_PREFIX_SIZE
        + DELETE_CODEC_OFFSET_IN_PREFIX;
    rewrite_page_with_checksum(path, page_id, |page| {
        page[byte_offset] = 0xFF;
    });
}

fn corrupt_leaf_row_codec(path: impl AsRef<std::path::Path>, page_id: u64, prefix_idx: usize) {
    const ROW_CODEC_OFFSET_IN_PREFIX: usize = 36;
    let byte_offset = PAGE_INTEGRITY_HEADER_SIZE
        + COLUMN_BLOCK_HEADER_SIZE
        + prefix_idx * COLUMN_BLOCK_LEAF_PREFIX_SIZE
        + ROW_CODEC_OFFSET_IN_PREFIX;
    rewrite_page_with_checksum(path, page_id, |page| {
        page[byte_offset] = 0;
    });
}

fn corrupt_lwc_row_shape_fingerprint(path: impl AsRef<std::path::Path>, page_id: u64) {
    rewrite_page_with_checksum(path, page_id, |page| {
        let payload_start = PAGE_INTEGRITY_HEADER_SIZE;
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
