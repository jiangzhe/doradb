use crate::buffer::EvictableBufferPoolConfig;
use crate::engine::{Engine, EngineConfig};
use crate::row::ops::{InsertMvcc, SelectKey, UpdateCol};
use crate::session::Session;
use crate::table::{Table, TableAccess};
use crate::trx::ActiveTrx;
use crate::trx::sys_conf::TrxSysConfig;
use crate::trx::tests::remove_files;
use crate::value::Val;

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
                        assert!(&vals[0] == &Val::from(i));
                        let s = format!("{}", i);
                        assert!(&vals[1] == &Val::from(&s[..]));
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
                .table_scan_uncommitted(sys.engine.data_pool, 0, |_metadata, _row| {
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
                .table_scan_mvcc(sys.engine.data_pool, &stmt, 0, &[0], |row| {
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
                .table_scan_mvcc(sys.engine.data_pool, &stmt, 0, &[0], |row| {
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
                .table_scan_mvcc(sys.engine.data_pool, &stmt, 0, &[0], |row| {
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
        sys.table.freeze(sys.engine.data_pool, 10).await;
        // after freezing, new row should be inserted into second page.
        {
            let trx = session1.begin_trx().unwrap();
            let insert = vec![Val::from(2), Val::from("2")];
            sys.trx_insert(trx, insert).await.commit().await.unwrap();
        }
        let row_pages = sys.table.total_row_pages().await;
        assert!(row_pages == 2);
        sys.table.freeze(sys.engine.data_pool, 10).await;

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

struct TestSys {
    engine: Engine,
    table: Table,
}

impl TestSys {
    #[inline]
    async fn new_evictable() -> Self {
        use crate::catalog::tests::table2;
        // 64KB * 16
        let engine = EngineConfig::default()
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .max_mem_size(64u64 * 1024 * 1024)
                    .max_file_size(128u64 * 1024 * 1024)
                    .file_path("databuffer_testsys.bin"),
            )
            .trx(
                TrxSysConfig::default()
                    .log_file_prefix("redo_testsys")
                    .skip_recovery(true),
            )
            .build()
            .await
            .unwrap();
        let table_id = table2(&engine).await;
        let table = engine.catalog().get_table(table_id).await.unwrap();
        TestSys { engine, table }
    }
}

impl TestSys {
    #[inline]
    fn clean_all(self) {
        drop(self);

        let _ = std::fs::remove_file("databuffer_testsys.bin");
        remove_files("redo_testsys*");
        remove_files("*.tbl");
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
