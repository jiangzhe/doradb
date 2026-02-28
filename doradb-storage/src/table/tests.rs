use super::{DeleteInternal, FrozenPage, InsertRowIntoPage, UpdateRowInplace};
use crate::buffer::{BufferPool, EvictableBufferPoolConfig};
use crate::engine::{Engine, EngineConfig};
use crate::file::table_fs::TableFileSystemConfig;
use crate::index::{ColumnBlockIndex, RowLocation, UniqueIndex};
use crate::latch::LatchFallbackMode;
use crate::row::ops::{DeleteMvcc, InsertMvcc, SelectKey, UpdateCol};
use crate::row::{RowID, RowPage};
use crate::session::Session;
use crate::table::{DeleteMarker, Table, TableAccess, TablePersistence};
use crate::trx::row::LockRowForWrite;
use crate::trx::sys_conf::TrxSysConfig;
use crate::trx::undo::RowUndoKind;
use crate::trx::{ActiveTrx, TrxID};
use crate::value::Val;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn test_mvcc_insert_normal() {
    smol::block_on(async {
        const SIZE: i32 = 10000;

        let sys = TestSys::new_evictable().await;

        let mut session = sys.new_session();
        {
            let mut trx = session.begin_trx().unwrap();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            trx.commit().await.unwrap();
        }
        {
            let mut trx = session.begin_trx().unwrap();
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
        let mut session = sys.new_session();
        // dup key
        {
            // insert [1, "hello"]
            let insert = vec![Val::from(1i32), Val::from("hello")];
            let mut trx = session.begin_trx().unwrap();
            trx = sys.trx_insert(trx, insert).await;
            trx.commit().await.unwrap();

            // insert [1, "world"]
            let insert = vec![Val::from(1i32), Val::from("world")];
            let mut trx = session.begin_trx().unwrap();
            let mut stmt = trx.start_stmt();
            let res = stmt.insert_row(&sys.table, insert).await;
            assert!(res == InsertMvcc::DuplicateKey);
            trx = stmt.fail().await;
            trx.rollback().await;
        }
        // write conflict
        {
            // insert [2, "hello"], but not commit
            let insert1 = vec![Val::from(2i32), Val::from("hello")];
            let mut trx1 = session.begin_trx().unwrap();
            let mut stmt1 = trx1.start_stmt();
            let res = stmt1.insert_row(&sys.table, insert1).await;
            assert!(res.is_ok());
            trx1 = stmt1.succeed();

            // begin concurrent transaction and insert [2, "world"]
            let mut session2 = sys.new_session();
            let insert2 = vec![Val::from(2i32), Val::from("world")];
            let trx2 = session2.begin_trx().unwrap();
            let mut stmt2 = trx2.start_stmt();
            let res = stmt2.insert_row(&sys.table, insert2).await;
            // still dup key because circuit breaker on index search.
            assert!(res == InsertMvcc::DuplicateKey);
            stmt2.fail().await.rollback().await;
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
            let mut session = sys.new_session();
            // insert 1000 rows
            let mut trx = session.begin_trx().unwrap();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
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
            trx = sys.trx_update(trx, &k1, update1).await;
            trx.commit().await.unwrap();

            // update 1 row with long value
            let mut trx = session.begin_trx().unwrap();
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
            let mut trx = session.begin_trx().unwrap();
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
            let mut session = sys.new_session();
            // insert 1000 rows
            // let mut trx = session.begin_trx(trx_sys);
            let mut trx = session.begin_trx().unwrap();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            trx.commit().await.unwrap();

            // delete 1 row
            let mut trx = session.begin_trx().unwrap();
            let k1 = single_key(1i32);
            trx = sys.trx_delete(trx, &k1).await;

            // lookup row in same transaction
            trx = sys.trx_select_not_found(trx, &k1).await;
            trx.commit().await.unwrap();

            // lookup row in new transaction
            let mut trx = session.begin_trx().unwrap();
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
        let mut session = sys.new_session();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(usize::MAX).await;
        sys.table.data_checkpoint(&mut session).await.unwrap();

        let key = single_key(1i32);
        let trx = session.begin_trx().unwrap();
        let _ = assert_row_in_lwc(&sys.table, &key, trx.sts).await;
        trx.commit().await.unwrap();

        let mut trx = session.begin_trx().unwrap();
        let mut stmt = trx.start_stmt();
        let res = stmt.delete_row(&sys.table, &key).await;
        assert!(res.is_ok());
        trx = stmt.succeed();
        trx.commit().await.unwrap();

        let mut trx = session.begin_trx().unwrap();
        trx = sys.trx_select_not_found(trx, &key).await;
        trx.commit().await.unwrap();

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_lwc_read_uses_readonly_buffer_pool() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.new_session();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(usize::MAX).await;
        sys.table.data_checkpoint(&mut session).await.unwrap();

        let key = single_key(1i32);
        let trx = session.begin_trx().unwrap();
        let _ = assert_row_in_lwc(&sys.table, &key, trx.sts).await;
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

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_column_delete_rollback() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.new_session();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(usize::MAX).await;
        sys.table.data_checkpoint(&mut session).await.unwrap();

        let key = single_key(2i32);
        let trx = session.begin_trx().unwrap();
        let _ = assert_row_in_lwc(&sys.table, &key, trx.sts).await;
        trx.commit().await.unwrap();

        let mut trx = session.begin_trx().unwrap();
        let mut stmt = trx.start_stmt();
        let res = stmt.delete_row(&sys.table, &key).await;
        assert!(res.is_ok());
        trx = stmt.succeed();
        trx.rollback().await;

        let mut trx = session.begin_trx().unwrap();
        trx = sys
            .trx_select(trx, &key, |row| {
                assert_eq!(row[0], Val::from(2i32));
            })
            .await;
        trx.commit().await.unwrap();

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_column_delete_rollback_after_checkpoint() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.new_session();
        insert_rows(&sys, &mut session, 0, 10, "name").await;

        let key = single_key(3i32);
        let mut trx_delete = session.begin_trx().unwrap();
        let mut stmt = trx_delete.start_stmt();
        let res = stmt.delete_row(&sys.table, &key).await;
        assert!(res.is_ok());
        trx_delete = stmt.succeed();

        sys.table.freeze(usize::MAX).await;
        let mut checkpoint_session = sys.new_session();
        sys.table
            .data_checkpoint(&mut checkpoint_session)
            .await
            .unwrap();

        let trx = session.begin_trx().unwrap();
        let _ = assert_row_in_lwc(&sys.table, &key, trx.sts).await;
        trx.commit().await.unwrap();

        let stmt = trx_delete.start_stmt();
        let res = stmt.select_row_mvcc(&sys.table, &key, &[0, 1]).await;
        assert!(res.not_found());
        trx_delete = stmt.succeed();
        trx_delete.rollback().await;

        let mut trx = session.begin_trx().unwrap();
        trx = sys
            .trx_select(trx, &key, |row| {
                assert_eq!(row[0], Val::from(3i32));
            })
            .await;
        trx.commit().await.unwrap();

        drop(checkpoint_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_column_delete_write_conflict() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.new_session();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(usize::MAX).await;
        sys.table.data_checkpoint(&mut session).await.unwrap();

        let key = single_key(4i32);
        let trx = session.begin_trx().unwrap();
        let _ = assert_row_in_lwc(&sys.table, &key, trx.sts).await;
        trx.commit().await.unwrap();

        let mut trx1 = session.begin_trx().unwrap();
        let mut stmt1 = trx1.start_stmt();
        let res1 = stmt1.delete_row(&sys.table, &key).await;
        assert!(res1.is_ok());
        trx1 = stmt1.succeed();

        let mut session2 = sys.new_session();
        let mut trx2 = session2.begin_trx().unwrap();
        let mut stmt2 = trx2.start_stmt();
        let res2 = stmt2.delete_row(&sys.table, &key).await;
        assert!(matches!(res2, DeleteMvcc::WriteConflict));
        trx2 = stmt2.fail().await;
        trx2.rollback().await;
        drop(session2);

        trx1.rollback().await;

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_column_delete_mvcc_visibility() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.new_session();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(usize::MAX).await;
        sys.table.data_checkpoint(&mut session).await.unwrap();

        let key = single_key(5i32);
        let trx = session.begin_trx().unwrap();
        let _ = assert_row_in_lwc(&sys.table, &key, trx.sts).await;
        trx.commit().await.unwrap();

        let mut trx_reader = session.begin_trx().unwrap();

        let mut trx_delete = session.begin_trx().unwrap();
        let mut stmt_delete = trx_delete.start_stmt();
        let res = stmt_delete.delete_row(&sys.table, &key).await;
        assert!(res.is_ok());
        trx_delete = stmt_delete.succeed();
        trx_delete.commit().await.unwrap();

        trx_reader = sys
            .trx_select(trx_reader, &key, |row| {
                assert_eq!(row[0], Val::from(5i32));
            })
            .await;
        trx_reader.commit().await.unwrap();

        let mut trx_new = session.begin_trx().unwrap();
        trx_new = sys.trx_select_not_found(trx_new, &key).await;
        trx_new.commit().await.unwrap();

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_for_deletion_persists_committed_markers() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.new_session();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(usize::MAX).await;
        sys.table
            .checkpoint_for_new_data(&mut session)
            .await
            .unwrap();

        let key = single_key(6i32);
        let reader = session.begin_trx().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, &key, reader.sts).await;
        reader.commit().await.unwrap();

        sys.new_trx_delete(&mut session, &key).await;
        let marker = sys.table.deletion_buffer().get(row_id).unwrap();
        let marker_ts = match marker {
            DeleteMarker::Committed(ts) => ts,
            DeleteMarker::Ref(status) => status.ts(),
        };
        let trx_sys = session.engine().trx_sys;
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
        sys.table
            .checkpoint_for_deletion(&mut session)
            .await
            .unwrap();

        let active_root = sys.table.file.active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            &sys.table.disk_pool,
        );
        let (start_row_id, payload) = index
            .find_entry(row_id)
            .await
            .unwrap()
            .expect("payload should exist");
        assert!(payload.offloaded_ref().is_some());
        let bytes = index
            .read_offloaded_bitmap_bytes(&payload)
            .await
            .unwrap()
            .expect("offloaded bytes should exist");
        let deltas = decode_offloaded_deltas(&bytes);
        let expected_delta = (row_id - start_row_id) as u32;
        assert!(deltas.binary_search(&expected_delta).is_ok());

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_checkpoint_for_deletion_skips_markers_at_or_after_cutoff() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.new_session();
        insert_rows(&sys, &mut session, 0, 10, "name").await;
        sys.table.freeze(usize::MAX).await;
        sys.table
            .checkpoint_for_new_data(&mut session)
            .await
            .unwrap();

        let key = single_key(7i32);
        let reader = session.begin_trx().unwrap();
        let row_id = assert_row_in_lwc(&sys.table, &key, reader.sts).await;
        reader.commit().await.unwrap();

        let mut hold_session = sys.new_session();
        let hold_trx = hold_session.begin_trx().unwrap();
        let hold_sts = hold_trx.sts;

        let mut writer_session = sys.new_session();
        sys.new_trx_delete(&mut writer_session, &key).await;

        let marker = sys.table.deletion_buffer().get(row_id).unwrap();
        let delete_cts = match marker {
            DeleteMarker::Committed(ts) => ts,
            DeleteMarker::Ref(status) => status.ts(),
        };
        assert!(delete_cts >= hold_sts);

        let mut checkpoint_session = sys.new_session();
        sys.table
            .checkpoint_for_deletion(&mut checkpoint_session)
            .await
            .unwrap();

        let active_root = sys.table.file.active_root();
        let index = ColumnBlockIndex::new(
            active_root.column_block_index_root,
            active_root.pivot_row_id,
            &sys.table.disk_pool,
        );
        let (_start_row_id, payload) = index
            .find_entry(row_id)
            .await
            .unwrap()
            .expect("payload should exist");
        assert!(payload.offloaded_ref().is_none());

        hold_trx.rollback().await;
        drop(checkpoint_session);
        drop(writer_session);
        drop(hold_session);
        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_row_page_transition_retries_update_delete() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.new_session();
        {
            let insert = vec![Val::from(1i32), Val::from("hello")];
            let mut trx = session.begin_trx().unwrap();
            trx = sys.trx_insert(trx, insert).await;
            trx.commit().await.unwrap();
        }
        let key = single_key(1i32);
        let mut trx = session.begin_trx().unwrap();
        let mut stmt = trx.start_stmt();
        let index = sys.table.sec_idx[key.index_no].unique().unwrap();
        let (row_id, _) = index.lookup(&key.vals, stmt.trx.sts).await.unwrap();
        let page_id = match sys.table.blk_idx.find_row(row_id).await {
            RowLocation::RowPage(page_id) => page_id,
            RowLocation::NotFound => panic!("row should exist"),
            RowLocation::LwcPage(..) => unreachable!("lwc page"),
        };
        let page_guard = sys
            .engine
            .mem_pool
            .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
            .await
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
            .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
            .await
            .lock_shared_async()
            .await
            .unwrap();
        let insert = vec![Val::from(2i32), Val::from("insert")];
        let insert_res = sys.table.insert_row_to_page(
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
            .update_row_inplace(&mut stmt, page_guard, &key, row_id, update)
            .await;
        assert!(matches!(res, UpdateRowInplace::RetryInTransition));
        trx = stmt.fail().await;
        trx.rollback().await;

        let mut trx = session.begin_trx().unwrap();
        let mut stmt = trx.start_stmt();
        let page_guard = sys
            .engine
            .mem_pool
            .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
            .await
            .lock_shared_async()
            .await
            .unwrap();
        let res = sys
            .table
            .delete_row_internal(&mut stmt, page_guard, row_id, &key, false)
            .await;
        assert!(matches!(res, DeleteInternal::RetryInTransition));
        trx = stmt.fail().await;
        trx.rollback().await;

        drop(session);
        sys.clean_all();
    });
}

#[test]
fn test_mvcc_rollback_insert_normal() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        {
            let mut session = sys.new_session();
            // insert 1 row
            let mut trx = session.begin_trx().unwrap();
            let insert = vec![Val::from(1i32), Val::from("hello")];
            trx = sys.trx_insert(trx, insert).await;
            // explicit rollback
            trx.rollback().await;

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
            let mut session = sys.new_session();
            // insert 1 row
            let insert = vec![Val::from(1i32), Val::from("hello")];
            sys.new_trx_insert(&mut session, insert).await;

            // we must hold a transaction before the deletion,
            // to prevent index GC.
            let trx_to_prevent_gc = sys.new_session().begin_trx().unwrap();
            // delete it
            let key = single_key(1i32);
            sys.new_trx_delete(&mut session, &key).await;

            // insert again, trigger insert+link
            let insert = vec![Val::from(1i32), Val::from("world")];
            sys.new_trx_insert(&mut session, insert).await;

            trx_to_prevent_gc.rollback().await;

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
            let mut session = sys.new_session();
            // insert 1 row
            let insert = vec![Val::from(1i32), Val::from("hello")];
            sys.new_trx_insert(&mut session, insert).await;

            // delete it
            let key = single_key(1i32);
            sys.new_trx_delete(&mut session, &key).await;

            // insert again, trigger insert+link
            let insert = vec![Val::from(1i32), Val::from("world")];
            let mut trx = session.begin_trx().unwrap();
            trx = sys.trx_insert(trx, insert).await;
            // explicit rollback
            trx.rollback().await;

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
            let mut session = sys.new_session();
            // insert 1 row: v1=1, v2=hello
            let insert = vec![Val::from(1i32), Val::from("hello")];
            sys.new_trx_insert(&mut session, insert).await;

            // open one session and trnasaction to see this row
            let mut sess1 = sys.new_session();
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
            sys.new_trx_update(&mut session, &key, update).await;

            // open session and transaction to see row 2
            let mut sess2 = sys.new_session();
            let mut trx2 = sess2.begin_trx().unwrap();

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
            let mut session = sys.new_session();
            // insert 1 row: v1=1, v2=hello
            let insert = vec![Val::from(1i32), Val::from("hello")];
            sys.new_trx_insert(&mut session, insert).await;
            println!("debug-only insert finish");

            // open one session and trnasaction to see this row
            let mut sess1 = sys.new_session();
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
            sys.new_trx_update(&mut session, &key, update).await;
            println!("debug-only update finish");

            // open session and transaction to see row 2
            let mut sess2 = sys.new_session();
            let mut trx2 = sess2.begin_trx().unwrap();

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
            let mut session = sys.new_session();
            // insert: v1
            let insert = vec![Val::from(1i32), Val::from("hello")];
            sys.new_trx_insert(&mut session, insert).await;

            // transaction to see version 1
            let mut sess1 = sys.new_session();
            let mut trx1 = sess1.begin_trx().unwrap();

            let mut trx = session.begin_trx().unwrap();
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
            let mut session = sys.new_session();
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
            let mut session = sys.new_session();
            let s: String = std::iter::repeat_n('0', SIZE).collect();
            // insert single row.
            {
                let insert = vec![Val::from(&s[..0])];
                let mut stmt = session.begin_trx().unwrap().start_stmt();
                let res = stmt.insert_row(&table, insert).await;
                assert!(res.is_ok());
                stmt.succeed().commit().await.unwrap();
            }
            // perform updates.
            for i in 0..COUNT {
                let key = SelectKey::new(0, vec![Val::from(&s[..i])]);
                let update = vec![UpdateCol {
                    idx: 0,
                    val: Val::from(&s[..i + 1]),
                }];
                let mut stmt = session.begin_trx().unwrap().start_stmt();
                let res = stmt.update_row(&table, &key, update).await;
                assert!(res.is_ok());
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
            let mut session = sys.new_session();
            let s: String = std::iter::repeat_n('0', SIZE).collect();
            // insert 60 rows
            for i in 0usize..COUNT {
                let insert = vec![Val::from(&s[..BASE + i])];
                let mut stmt = session.begin_trx().unwrap().start_stmt();
                let res = stmt.insert_row(&table, insert).await;
                assert!(res.is_ok());
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
                let mut stmt = session.begin_trx().unwrap().start_stmt();
                let res = stmt.update_row(&table, &key, update).await;
                assert!(res.is_ok());
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
            let mut session = sys.new_session();
            // insert 1000 rows
            let mut trx = session.begin_trx().unwrap();
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

        let mut session = sys.new_session();
        {
            let mut trx = session.begin_trx().unwrap();
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
                .table_scan_uncommitted(0, |_metadata, _row| {
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
        let mut session1 = sys.new_session();
        {
            let mut trx = session1.begin_trx().unwrap();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            _ = trx.commit().await.unwrap();
        }
        // we should see 100 committed rows.
        let mut session2 = sys.new_session();
        {
            let trx = session2.begin_trx().unwrap();
            let stmt = trx.start_stmt();
            let mut res_len = 0usize;
            sys.table
                .table_scan_mvcc(&stmt, 0, &[0], |_| {
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
            let mut trx = session1.begin_trx().unwrap();
            for i in SIZE..SIZE * 2 {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            trx
        };
        // we should see only 100 rows
        {
            let trx = session2.begin_trx().unwrap();
            let stmt = trx.start_stmt();
            let mut res_len = 0usize;
            sys.table
                .table_scan_mvcc(&stmt, 0, &[0], |_| {
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
            let trx = session2.begin_trx().unwrap();
            let stmt = trx.start_stmt();
            let mut res_len = 0usize;
            sys.table
                .table_scan_mvcc(&stmt, 0, &[0], |_| {
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

        let mut session1 = sys.new_session();
        {
            let trx = session1.begin_trx().unwrap();
            let insert = vec![Val::from(1), Val::from("1")];
            sys.trx_insert(trx, insert).await.commit().await.unwrap();
        }
        let row_pages = sys.table.total_row_pages().await;
        assert!(row_pages == 1);
        sys.table.freeze(10).await;
        // after freezing, new row should be inserted into second page.
        {
            let trx = session1.begin_trx().unwrap();
            let insert = vec![Val::from(2), Val::from("2")];
            sys.trx_insert(trx, insert).await.commit().await.unwrap();
        }
        let row_pages = sys.table.total_row_pages().await;
        assert!(row_pages == 2);
        sys.table.freeze(10).await;

        // update row 1 will cause new insert into new page.
        {
            let mut stmt = session1.begin_trx().unwrap().start_stmt();
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
            assert!(res.is_ok());
            stmt.succeed().commit().await.unwrap();
        }
        let row_pages = sys.table.total_row_pages().await;
        assert!(row_pages == 3);

        // update row 1 will just be in-place.
        {
            let mut stmt = session1.begin_trx().unwrap().start_stmt();
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
            assert!(res.is_ok());
            stmt.succeed().commit().await.unwrap();
        }
        let row_pages = sys.table.total_row_pages().await;
        assert!(row_pages == 3);

        drop(session1);
        sys.clean_all();
    });
}

#[test]
fn test_transition_captures_uncommitted_lock_into_deletion_buffer() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.new_session();
        insert_rows(&sys, &mut session, 1, 1, "lock").await;

        let key = single_key(1i32);
        let mut trx = session.begin_trx().unwrap();
        let mut stmt = trx.start_stmt();
        let index = sys.table.sec_idx[key.index_no].unique().unwrap();
        let (row_id, _) = index.lookup(&key.vals, stmt.trx.sts).await.unwrap();
        let page_id = match sys.table.blk_idx.find_row(row_id).await {
            RowLocation::RowPage(page_id) => page_id,
            RowLocation::NotFound => panic!("row should exist"),
            RowLocation::LwcPage(..) => unreachable!("row page expected"),
        };

        let page_guard = sys
            .engine
            .mem_pool
            .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
            .await
            .lock_shared_async()
            .await
            .unwrap();
        let (ctx, page) = page_guard.ctx_and_page();
        let mut lock_row = sys
            .table
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
            .set_frozen_pages_to_transition(&[frozen_page], stmt.trx.sts)
            .await;

        let marker = sys.table.deletion_buffer().get(row_id).unwrap();
        match marker {
            DeleteMarker::Ref(status) => {
                assert!(std::sync::Arc::ptr_eq(&status, &stmt.trx.status()));
            }
            DeleteMarker::Committed(_) => panic!("uncommitted lock should remain as marker ref"),
        }

        trx = stmt.fail().await;
        trx.rollback().await;

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
        let mut session = sys.new_session();
        let name = "x".repeat(1024);
        insert_rows(&sys, &mut session, 0, 200, &name).await;

        let old_root = sys.table.file.active_root().clone();
        sys.table.freeze(usize::MAX).await;
        let pivot_row_id = sys.table.file.active_root().pivot_row_id;
        let (frozen_pages, _) = sys.table.collect_frozen_pages(pivot_row_id).await;
        assert!(!frozen_pages.is_empty());

        sys.table.data_checkpoint(&mut session).await.unwrap();

        let new_root = sys.table.file.active_root();
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
        let mut session = sys.new_session();
        let name = "y".repeat(256);
        insert_rows(&sys, &mut session, 0, 120, &name).await;

        sys.table.freeze(1).await;

        let mut read_trx = session.begin_trx().unwrap();
        {
            let key = SelectKey::new(0, vec![Val::from(1)]);
            let stmt = read_trx.start_stmt();
            let res = stmt.select_row_mvcc(&sys.table, &key, &[0, 1]).await;
            assert!(res.is_ok());
            read_trx = stmt.succeed();
        }

        let mut write_session = sys.new_session();
        let mut write_trx = write_session.begin_trx().unwrap();
        {
            let mut stmt = write_trx.start_stmt();
            let insert = vec![Val::from(10_000i32), Val::from("new")];
            let res = stmt.insert_row(&sys.table, insert).await;
            assert!(res.is_ok());
            write_trx = stmt.succeed();
        }

        let mut checkpoint_session = sys.new_session();
        sys.table
            .data_checkpoint(&mut checkpoint_session)
            .await
            .unwrap();

        {
            let key = SelectKey::new(0, vec![Val::from(10_000i32)]);
            let stmt = read_trx.start_stmt();
            let res = stmt.select_row_mvcc(&sys.table, &key, &[0, 1]).await;
            assert!(res.not_found());
            read_trx = stmt.succeed();
        }

        write_trx.rollback().await;
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
        let mut session = engine.new_session();
        let name = "z".repeat(512);
        insert_rows_direct(&table, &mut session, 0, 150, &name).await;

        table.freeze(usize::MAX).await;
        table.data_checkpoint(&mut session).await.unwrap();

        let root_before = table.file.active_root().clone();
        drop(table);

        let table_file = engine.table_fs.open_table_file(table_id).await.unwrap();
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
        let mut session = sys.new_session();
        let name = "h".repeat(128);
        insert_rows(&sys, &mut session, 0, 40, &name).await;

        let root_before = sys.table.file.active_root().clone();
        sys.table.data_checkpoint(&mut session).await.unwrap();
        let root_after = sys.table.file.active_root();

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
        let mut session = sys.new_session();
        let name = "g".repeat(1024);
        insert_rows(&sys, &mut session, 0, 200, &name).await;

        let allocated_before = sys.engine.mem_pool.allocated();
        sys.table.freeze(usize::MAX).await;
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
fn test_data_checkpoint_error_rollback() {
    smol::block_on(async {
        let sys = TestSys::new_evictable().await;
        let mut session = sys.new_session();
        let name = "e".repeat(256);
        insert_rows(&sys, &mut session, 0, 80, &name).await;

        sys.table.freeze(usize::MAX).await;
        let root_before = sys.table.file.active_root().clone();

        super::set_test_force_lwc_build_error(true);
        let res = sys.table.data_checkpoint(&mut session).await;
        super::set_test_force_lwc_build_error(false);
        assert!(res.is_err());

        let root_after = sys.table.file.active_root();
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

struct TestSys {
    engine: Engine,
    table: Table,
    _temp_dir: TempDir,
}

impl TestSys {
    #[inline]
    async fn new_evictable() -> Self {
        use crate::catalog::tests::table2;
        // 64KB * 16
        let temp_dir = TempDir::new().unwrap();
        let main_dir = temp_dir.path().to_string_lossy().to_string();
        let engine = EngineConfig::default()
            .main_dir(main_dir)
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .max_mem_size(64u64 * 1024 * 1024)
                    .max_file_size(128u64 * 1024 * 1024),
            )
            .trx(
                TrxSysConfig::default()
                    .log_file_prefix("redo_testsys")
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
        let mut trx = session.begin_trx().unwrap();
        trx = self.trx_insert(trx, insert).await;
        trx.commit().await.unwrap();
    }

    #[inline]
    async fn trx_insert(&self, trx: ActiveTrx, insert: Vec<Val>) -> ActiveTrx {
        let mut stmt = trx.start_stmt();
        let res = stmt.insert_row(&self.table, insert).await;
        if !res.is_ok() {
            panic!("res={:?}", res);
        }
        // assert!(res.is_ok());
        stmt.succeed()
    }

    #[inline]
    async fn new_trx_delete(&self, session: &mut Session, key: &SelectKey) {
        let mut trx = session.begin_trx().unwrap();
        trx = self.trx_delete(trx, key).await;
        trx.commit().await.unwrap();
    }

    #[inline]
    async fn trx_delete(&self, trx: ActiveTrx, key: &SelectKey) -> ActiveTrx {
        let mut stmt = trx.start_stmt();
        let res = stmt.delete_row(&self.table, key).await;
        if !res.is_ok() {
            panic!("res={:?}", res);
        }
        // assert!(res.is_ok());
        stmt.succeed()
    }

    #[inline]
    async fn new_trx_update(&self, session: &mut Session, key: &SelectKey, update: Vec<UpdateCol>) {
        let mut trx = session.begin_trx().unwrap();
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
        if !res.is_ok() {
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
        let mut trx = session.begin_trx().unwrap();
        trx = self.trx_select(trx, key, action).await;
        trx.commit().await.unwrap();
    }

    #[inline]
    async fn new_trx_select_not_found(&self, session: &mut Session, key: &SelectKey) {
        let mut trx = session.begin_trx().unwrap();
        trx = self.trx_select_not_found(trx, key).await;
        trx.commit().await.unwrap();
    }

    #[inline]
    async fn trx_select_not_found(&self, trx: ActiveTrx, key: &SelectKey) -> ActiveTrx {
        let stmt = trx.start_stmt();
        let res = stmt.select_row_mvcc(&self.table, key, &[0, 1]).await;
        assert!(res.not_found());
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
        if !res.is_ok() {
            panic!("res={:?}", res);
        }
        // assert!(res.is_ok());
        action(res.unwrap());
        stmt.succeed()
    }

    #[inline]
    fn new_session(&self) -> Session {
        self.engine.new_session()
    }
}

fn single_key<V: Into<Val>>(value: V) -> SelectKey {
    SelectKey {
        index_no: 0,
        vals: vec![value.into()],
    }
}

async fn assert_row_in_lwc(table: &Table, key: &SelectKey, sts: TrxID) -> RowID {
    let index = table.sec_idx[key.index_no].unique().unwrap();
    let (row_id, _) = index
        .lookup(&key.vals, sts)
        .await
        .expect("row should exist");
    match table.blk_idx.find_row(row_id).await {
        RowLocation::LwcPage(..) => row_id,
        RowLocation::RowPage(..) => panic!("row should be in lwc"),
        RowLocation::NotFound => panic!("row should exist"),
    }
}

fn decode_offloaded_deltas(bytes: &[u8]) -> Vec<u32> {
    assert!(!bytes.is_empty());
    assert!(bytes.len().is_multiple_of(std::mem::size_of::<u32>()));
    let mut deltas = bytes
        .chunks_exact(std::mem::size_of::<u32>())
        .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect::<Vec<_>>();
    deltas.sort_unstable();
    deltas
}

async fn insert_rows(sys: &TestSys, session: &mut Session, start: i32, count: i32, name: &str) {
    let mut trx = session.begin_trx().unwrap();
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
    let mut trx = session.begin_trx().unwrap();
    for i in 0..count {
        let insert = vec![Val::from(start + i), Val::from(name)];
        let mut stmt = trx.start_stmt();
        let res = stmt.insert_row(table, insert).await;
        assert!(res.is_ok());
        trx = stmt.succeed();
    }
    trx.commit().await.unwrap();
}
