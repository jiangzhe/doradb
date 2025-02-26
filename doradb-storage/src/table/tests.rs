use crate::buffer::{BufferPool, EvictableBufferPool, EvictableBufferPoolConfig, FixedBufferPool};
use crate::catalog::{Catalog, IndexKey, IndexSchema, TableSchema};
use crate::lifetime::{StaticLifetime, StaticLifetimeRef};
use crate::row::ops::{SelectKey, UpdateCol};
use crate::session::Session;
use crate::table::{Table, TableID};
use crate::trx::sys::{TransactionSystem, TrxSysConfig};
use crate::trx::ActiveTrx;
use crate::value::{Val, ValKind};

#[test]
fn test_mvcc_insert_normal() {
    smol::block_on(async {
        const SIZE: i32 = 10000;

        let sys = TestSys::new_fixed().await;

        let mut session = Session::new();
        {
            let mut trx = sys.new_trx(session);
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            session = sys.commit(trx).await;
        }
        {
            let mut trx = sys.new_trx(session);
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
            let _ = sys.commit(trx).await;
        }
    });
}

#[test]
fn test_mvcc_update_normal() {
    smol::block_on(async {
        const SIZE: i32 = 1000;

        let sys = TestSys::new_fixed().await;
        {
            let mut session = Session::new();
            // insert 1000 rows
            let mut trx = sys.new_trx(session);
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            session = sys.commit(trx).await;

            // update 1 row with short value
            let mut trx = sys.new_trx(session);
            let k1 = single_key(1i32);
            let s1 = "hello";
            let update1 = vec![UpdateCol {
                idx: 1,
                val: Val::from(s1),
            }];
            trx = sys.trx_update(trx, &k1, update1).await;
            session = sys.commit(trx).await;

            // update 1 row with long value
            let mut trx = sys.new_trx(session);
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

            session = sys.commit(trx).await;

            // lookup with a new transaction
            let mut trx = sys.new_trx(session);
            trx = sys
                .trx_select(trx, &k2, |row| {
                    assert!(row.len() == 2);
                    assert!(row[0] == k2.vals[0]);
                    assert!(row[1] == Val::from(&s2[..]));
                })
                .await;

            let _ = sys.commit(trx).await;
        }
    });
}

#[test]
fn test_mvcc_delete_normal() {
    smol::block_on(async {
        const SIZE: i32 = 1000;

        let sys = TestSys::new_fixed().await;
        {
            let mut session = Session::new();
            // insert 1000 rows
            // let mut trx = session.begin_trx(trx_sys);
            let mut trx = sys.new_trx(session);
            for i in 0..SIZE {
                let s = format!("{}", i);
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
            }
            session = sys.commit(trx).await;

            // delete 1 row
            let mut trx = sys.new_trx(session);
            let k1 = single_key(1i32);
            trx = sys.trx_delete(trx, &k1).await;

            // lookup row in same transaction
            trx = sys.trx_select_not_found(trx, &k1).await;
            session = sys.commit(trx).await;

            // lookup row in new transaction
            let mut trx = sys.new_trx(session);
            let k1 = single_key(1i32);
            trx = sys.trx_select_not_found(trx, &k1).await;
            let _ = sys.commit(trx).await;
        }
    });
}

#[test]
fn test_mvcc_rollback_insert_normal() {
    smol::block_on(async {
        let sys = TestSys::new_fixed().await;
        {
            let mut session = Session::new();
            // insert 1 row
            let mut trx = sys.new_trx(session);
            let insert = vec![Val::from(1i32), Val::from("hello")];
            trx = sys.trx_insert(trx, insert).await;
            // explicit rollback
            session = sys.rollback(trx).await;

            // select 1 row
            let key = single_key(1i32);
            _ = sys.new_trx_select_not_found(session, &key).await;
        }
    });
}

#[test]
fn test_mvcc_insert_link_unique_index() {
    smol::block_on(async {
        let sys = TestSys::new_fixed().await;
        {
            let mut session = Session::new();
            // insert 1 row
            let insert = vec![Val::from(1i32), Val::from("hello")];
            session = sys.new_trx_insert(session, insert).await;

            // delete it
            let key = single_key(1i32);
            session = sys.new_trx_delete(session, &key).await;

            // insert again, trigger insert+link
            let insert = vec![Val::from(1i32), Val::from("world")];
            session = sys.new_trx_insert(session, insert).await;

            // select 1 row
            let key = single_key(1i32);
            _ = sys
                .new_trx_select(session, &key, |vals| {
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
        let sys = TestSys::new_fixed().await;
        {
            let mut session = Session::new();
            // insert 1 row
            let insert = vec![Val::from(1i32), Val::from("hello")];
            session = sys.new_trx_insert(session, insert).await;

            // delete it
            let key = single_key(1i32);
            session = sys.new_trx_delete(session, &key).await;

            // insert again, trigger insert+link
            let insert = vec![Val::from(1i32), Val::from("world")];
            let mut trx = sys.new_trx(session);
            trx = sys.trx_insert(trx, insert).await;
            // explicit rollback
            session = sys.rollback(trx).await;

            // select 1 row
            let key = single_key(1i32);
            _ = sys.new_trx_select_not_found(session, &key).await;
        }
    });
}

#[test]
fn test_mvcc_insert_link_update() {
    smol::block_on(async {
        let sys = TestSys::new_fixed().await;
        {
            let mut session = Session::new();
            // insert 1 row: v1=1, v2=hello
            let insert = vec![Val::from(1i32), Val::from("hello")];
            session = sys.new_trx_insert(session, insert).await;

            // open one session and trnasaction to see this row
            let sess1 = Session::new();
            let mut trx1 = sys.new_trx(sess1);

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
            let sess2 = Session::new();
            let mut trx2 = sys.new_trx(sess2);

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
            _ = sys.commit(trx1).await;

            // use transaction 2 to see version 2.
            let key = single_key(2i32);
            trx2 = sys
                .trx_select(trx2, &key, |vals| {
                    assert!(vals[0] == Val::from(2i32));
                    assert!(vals[1] == Val::from("world"));
                })
                .await;
            _ = sys.commit(trx2).await;

            // use new transaction to see version 3.
            let key = single_key(1i32);
            _ = sys
                .new_trx_select(session, &key, |vals| {
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
        let sys = TestSys::new_fixed().await;
        {
            let mut session = Session::new();
            // insert 1 row: v1=1, v2=hello
            let insert = vec![Val::from(1i32), Val::from("hello")];
            session = sys.new_trx_insert(session, insert).await;

            // open one session and trnasaction to see this row
            let sess1 = Session::new();
            let mut trx1 = sys.new_trx(sess1);

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
            let sess2 = Session::new();
            let mut trx2 = sys.new_trx(sess2);

            // insert v1=5, v2=rust
            let insert = vec![Val::from(5i32), Val::from("rust")];
            session = sys.new_trx_insert(session, insert).await;

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

            // use transaction 1 to see version 1.
            let key = single_key(1i32);
            trx1 = sys
                .trx_select(trx1, &key, |vals| {
                    assert!(vals[0] == Val::from(1i32));
                    assert!(vals[1] == Val::from("hello"));
                })
                .await;
            _ = sys.commit(trx1).await;

            // use transaction 2 to see version 2.
            let key = single_key(2i32);
            trx2 = sys
                .trx_select(trx2, &key, |vals| {
                    assert!(vals[0] == Val::from(2i32));
                    assert!(vals[1] == Val::from("world"));
                })
                .await;
            _ = sys.commit(trx2).await;

            // use new transaction to see version 3.
            let key = single_key(1i32);
            _ = sys.new_trx_select(session, &key, |vals| {
                assert!(vals[0] == Val::from(1i32));
                assert!(vals[1] == Val::from("c++"));
            })
        }
        drop(sys);
    });
}

#[test]
fn test_mvcc_multi_update() {
    smol::block_on(async {
        let sys = TestSys::new_fixed().await;
        {
            let mut session = Session::new();
            // insert: v1
            let insert = vec![Val::from(1i32), Val::from("hello")];
            session = sys.new_trx_insert(session, insert).await;

            // transaction to see version 1
            let sess1 = Session::new();
            let mut trx1 = sys.new_trx(sess1);

            let mut trx = sys.new_trx(session);
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
            sys.commit(trx).await;

            //v1 found
            let key = single_key(1i32);
            trx1 = sys
                .trx_select(trx1, &key, |vals| {
                    assert!(vals[0] == Val::from(1i32));
                    assert!(vals[1] == Val::from("hello"));
                })
                .await;
            sys.commit(trx1).await;
        }
    });
}

#[test]
fn test_evict_pool_insert_full() {
    smol::block_on(async {
        const SIZE: i32 = 1000;

        let sys = TestSys::new_evictable().await;
        {
            let session = Session::new();
            // insert 1000 rows
            let mut trx = sys.new_trx(session);
            for i in 0..SIZE {
                // make string 1KB long, so a page can only hold about 60 rows.
                let s: String = (0..1000).map(|_| 'a').collect();
                let insert = vec![Val::from(i), Val::from(&s[..])];
                trx = sys.trx_insert(trx, insert).await;
                println!("trx {}", i);
            }
            let _ = sys.commit(trx).await;
            println!("commit end");
        }
    });
}

async fn create_table<P: BufferPool>(buf_pool: &'static P, catalog: &Catalog<P>) -> TableID {
    catalog
        .create_table(
            buf_pool,
            TableSchema::new(
                vec![
                    ValKind::I32.nullable(false),
                    ValKind::VarByte.nullable(false),
                ],
                vec![IndexSchema::new(vec![IndexKey::new(0)], true)],
            ),
        )
        .await
}

struct TestSys<P: BufferPool> {
    buf_pool: &'static P,
    catalog: &'static Catalog<P>,
    trx_sys: &'static TransactionSystem,
    table: Table<P>,
}

impl TestSys<FixedBufferPool> {
    #[inline]
    async fn new_fixed() -> Self {
        // 64KB * 16
        let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024).unwrap();
        let catalog = Catalog::empty_static();
        let trx_sys = TrxSysConfig::default().build_static(buf_pool, catalog);
        let table_id = create_table(buf_pool, catalog).await;
        let table = catalog.get_table(table_id).unwrap();
        TestSys {
            buf_pool,
            catalog,
            trx_sys,
            table,
        }
    }
}

impl TestSys<EvictableBufferPool> {
    #[inline]
    async fn new_evictable() -> Self {
        // 64KB * 16
        let buf_pool = EvictableBufferPoolConfig::default()
            .max_mem_size(1024 * 1024)
            .max_file_size(1024 * 1024 * 32)
            .build_static()
            .unwrap();
        let catalog = Catalog::empty_static();
        let trx_sys = TrxSysConfig::default().build_static(buf_pool, catalog);
        let table_id = create_table(buf_pool, catalog).await;
        let table = catalog.get_table(table_id).unwrap();
        TestSys {
            buf_pool,
            catalog,
            trx_sys,
            table,
        }
    }
}

impl<P: BufferPool + 'static> TestSys<P> {
    #[inline]
    async fn new_trx_insert(&self, session: Session, insert: Vec<Val>) -> Session {
        let mut trx = session.begin_trx(self.trx_sys);
        trx = self.trx_insert(trx, insert).await;
        self.commit(trx).await
    }

    #[inline]
    async fn trx_insert(&self, trx: ActiveTrx, insert: Vec<Val>) -> ActiveTrx {
        let mut stmt = trx.start_stmt();
        let res = self
            .table
            .insert_row(self.buf_pool, &mut stmt, insert)
            .await;
        if !res.is_ok() {
            panic!("res={:?}", res);
        }
        // assert!(res.is_ok());
        stmt.succeed()
    }

    #[inline]
    async fn new_trx_delete(&self, session: Session, key: &SelectKey) -> Session {
        let mut trx = session.begin_trx(self.trx_sys);
        trx = self.trx_delete(trx, key).await;
        self.commit(trx).await
    }

    #[inline]
    async fn trx_delete(&self, trx: ActiveTrx, key: &SelectKey) -> ActiveTrx {
        let mut stmt = trx.start_stmt();
        let res = self.table.delete_row(self.buf_pool, &mut stmt, key).await;
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
        let mut trx = session.begin_trx(self.trx_sys);
        trx = self.trx_update(trx, key, update).await;
        self.commit(trx).await
    }

    #[inline]
    async fn trx_update(
        &self,
        trx: ActiveTrx,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> ActiveTrx {
        let mut stmt = trx.start_stmt();
        let res = self
            .table
            .update_row(self.buf_pool, &mut stmt, key, update)
            .await;
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
        let mut trx = session.begin_trx(self.trx_sys);
        trx = self.trx_select(trx, key, action).await;
        self.commit(trx).await
    }

    #[inline]
    async fn new_trx_select_not_found(&self, session: Session, key: &SelectKey) -> Session {
        let mut trx = session.begin_trx(self.trx_sys);
        trx = self.trx_select_not_found(trx, key).await;
        self.commit(trx).await
    }

    #[inline]
    async fn trx_select_not_found(&self, trx: ActiveTrx, key: &SelectKey) -> ActiveTrx {
        let stmt = trx.start_stmt();
        let res = self
            .table
            .select_row_mvcc(self.buf_pool, &stmt, key, &[0, 1])
            .await;
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
        let res = self
            .table
            .select_row_mvcc(self.buf_pool, &stmt, key, &[0, 1])
            .await;
        if !res.is_ok() {
            panic!("res={:?}", res);
        }
        // assert!(res.is_ok());
        action(res.unwrap());
        stmt.succeed()
    }

    #[inline]
    async fn commit(&self, trx: ActiveTrx) -> Session {
        self.trx_sys
            .commit(trx, self.buf_pool, self.catalog)
            .await
            .unwrap()
    }

    #[inline]
    async fn rollback(&self, trx: ActiveTrx) -> Session {
        self.trx_sys
            .rollback(trx, self.buf_pool, self.catalog)
            .await
    }

    #[inline]
    fn new_trx(&self, session: Session) -> ActiveTrx {
        session.begin_trx(self.trx_sys)
    }
}

impl<P: BufferPool + 'static> Drop for TestSys<P> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            StaticLifetime::drop_static(self.trx_sys);
            StaticLifetime::drop_static(self.catalog);
            StaticLifetimeRef::drop_static(self.buf_pool);
        }
    }
}

fn single_key<V: Into<Val>>(value: V) -> SelectKey {
    SelectKey {
        index_no: 0,
        vals: vec![value.into()],
    }
}
