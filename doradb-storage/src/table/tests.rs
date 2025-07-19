use crate::buffer::EvictableBufferPoolConfig;
use crate::engine::{Engine, EngineConfig};
use crate::row::ops::{SelectKey, UpdateCol};
use crate::session::Session;
use crate::table::Table;
use crate::trx::sys_conf::TrxSysConfig;
use crate::trx::tests::remove_files;
use crate::trx::ActiveTrx;
use crate::value::Val;

#[test]
fn test_mvcc_insert_normal() {
    smol::block_on(async {
        const SIZE: i32 = 10000;

        let sys = TestSys::new_evictable().await;

        let mut session = sys.new_session();
        {
            let mut trx = session.begin_trx();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            session = trx.commit().await.unwrap();
        }
        {
            let mut trx = session.begin_trx();
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
            let mut trx = session.begin_trx();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            session = trx.commit().await.unwrap();

            // update 1 row with short value
            let mut trx = session.begin_trx();
            let k1 = single_key(1i32);
            let s1 = "hello";
            let update1 = vec![UpdateCol {
                idx: 1,
                val: Val::from(s1),
            }];
            trx = sys.trx_update(trx, &k1, update1).await;
            session = trx.commit().await.unwrap();

            // update 1 row with long value
            let mut trx = session.begin_trx();
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

            session = trx.commit().await.unwrap();

            // lookup with a new transaction
            let mut trx = session.begin_trx();
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
            let mut trx = session.begin_trx();
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            session = trx.commit().await.unwrap();

            // delete 1 row
            let mut trx = session.begin_trx();
            let k1 = single_key(1i32);
            trx = sys.trx_delete(trx, &k1).await;

            // lookup row in same transaction
            trx = sys.trx_select_not_found(trx, &k1).await;
            session = trx.commit().await.unwrap();

            // lookup row in new transaction
            let mut trx = session.begin_trx();
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
            let mut trx = session.begin_trx();
            let insert = vec![Val::from(1i32), Val::from("hello")];
            trx = sys.trx_insert(trx, insert).await;
            // explicit rollback
            session = trx.rollback().await;

            // select 1 row
            let key = single_key(1i32);
            _ = sys.new_trx_select_not_found(session, &key).await;
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
            session = sys.new_trx_insert(session, insert).await;

            // we must hold a transaction before the deletion,
            // to prevent index GC.
            let trx_to_prevent_gc = sys.new_session().begin_trx();
            // delete it
            let key = single_key(1i32);
            session = sys.new_trx_delete(session, &key).await;

            // insert again, trigger insert+link
            let insert = vec![Val::from(1i32), Val::from("world")];
            session = sys.new_trx_insert(session, insert).await;

            drop(trx_to_prevent_gc.rollback().await);

            // select 1 row
            let key = single_key(1i32);
            _ = sys
                .new_trx_select(session, &key, |vals| {
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
            session = sys.new_trx_insert(session, insert).await;

            // delete it
            let key = single_key(1i32);
            session = sys.new_trx_delete(session, &key).await;

            // insert again, trigger insert+link
            let insert = vec![Val::from(1i32), Val::from("world")];
            let mut trx = session.begin_trx();
            trx = sys.trx_insert(trx, insert).await;
            // explicit rollback
            session = trx.rollback().await;

            // select 1 row
            let key = single_key(1i32);
            _ = sys.new_trx_select_not_found(session, &key).await;
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
            session = sys.new_trx_insert(session, insert).await;

            // open one session and trnasaction to see this row
            let sess1 = sys.new_session();
            let mut trx1 = sess1.begin_trx();

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
            session = sys.new_trx_update(session, &key, update).await;

            // open session and transaction to see row 2
            let sess2 = sys.new_session();
            let mut trx2 = sess2.begin_trx();

            // insert again, trigger insert+link
            let insert = vec![Val::from(1i32), Val::from("rust")];
            session = sys.new_trx_insert(session, insert).await;

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
                .new_trx_select(session, &key, |vals| {
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
            session = sys.new_trx_insert(session, insert).await;
            println!("debug-only insert finish");

            // open one session and trnasaction to see this row
            let sess1 = sys.new_session();
            let mut trx1 = sess1.begin_trx();

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
            session = sys.new_trx_update(session, &key, update).await;
            println!("debug-only update finish");

            // open session and transaction to see row 2
            let sess2 = sys.new_session();
            let mut trx2 = sess2.begin_trx();

            // insert v1=5, v2=rust
            let insert = vec![Val::from(5i32), Val::from("rust")];
            session = sys.new_trx_insert(session, insert).await;
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
            session = sys.new_trx_update(session, &key, update).await;
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
            _ = sys.new_trx_select(session, &key, |vals| {
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
            session = sys.new_trx_insert(session, insert).await;

            // transaction to see version 1
            let sess1 = sys.new_session();
            let mut trx1 = sess1.begin_trx();

            let mut trx = session.begin_trx();
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
fn test_non_index_varchar_updates() {
    smol::block_on(async {
        const COUNT: usize = 100;
        const SIZE: usize = 500;
        let sys = TestSys::new_evictable().await;
        {
            let mut session = sys.new_session();
            let value = vec![1u8; SIZE];
            let insert = vec![Val::from(1i32), Val::from(&[])];
            session = sys.new_trx_insert(session, insert).await;
            let key = SelectKey::new(0, vec![Val::from(1i32)]);
            for i in SIZE - COUNT..SIZE {
                session = sys
                    .new_trx_update(
                        session,
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
fn test_evict_pool_insert_full() {
    smol::block_on(async {
        const SIZE: i32 = 800;

        // in-mem ~1000 pages, on-disk 2000 pages.
        let sys = TestSys::new_evictable().await;
        {
            let session = sys.new_session();
            // insert 1000 rows
            let mut trx = session.begin_trx();
            for i in 0..SIZE {
                // make string 1KB long, so a page can only hold about 60 rows.
                // if page is full, 17 pages are required.
                // if page is half full, 35 pages are required.
                println!("debug-only insert {}", i);
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
fn test_row_page_scan_rows_uncommitted() {
    smol::block_on(async {
        const SIZE: i32 = 10000;

        let sys = TestSys::new_evictable().await;

        let session = sys.new_session();
        {
            let mut trx = session.begin_trx();
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
                .scan_rows_uncommitted(sys.engine.data_pool, |_row| {
                    res_len += 1;
                    true
                })
                .await;
            println!("res.len()={}", res_len);
            assert!(res_len == SIZE as usize);
        }

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
            .unwrap()
            .init()
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
    }

    #[inline]
    async fn new_trx_insert(&self, session: Session, insert: Vec<Val>) -> Session {
        let mut trx = session.begin_trx();
        trx = self.trx_insert(trx, insert).await;
        trx.commit().await.unwrap()
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
    async fn new_trx_delete(&self, session: Session, key: &SelectKey) -> Session {
        let mut trx = session.begin_trx();
        trx = self.trx_delete(trx, key).await;
        trx.commit().await.unwrap()
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
    async fn new_trx_update(
        &self,
        session: Session,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> Session {
        let mut trx = session.begin_trx();
        trx = self.trx_update(trx, key, update).await;
        trx.commit().await.unwrap()
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
        session: Session,
        key: &SelectKey,
        action: F,
    ) -> Session {
        let mut trx = session.begin_trx();
        trx = self.trx_select(trx, key, action).await;
        trx.commit().await.unwrap()
    }

    #[inline]
    async fn new_trx_select_not_found(&self, session: Session, key: &SelectKey) -> Session {
        let mut trx = session.begin_trx();
        trx = self.trx_select_not_found(trx, key).await;
        trx.commit().await.unwrap()
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
