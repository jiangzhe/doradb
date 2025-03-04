use crate::buffer::{BufferPool, FixedBufferPool};
use crate::catalog::{Catalog, IndexKey, IndexSchema, TableSchema};
use crate::lifetime::StaticLifetime;
use crate::row::ops::{SelectKey, SelectMvcc, UpdateCol};
use crate::session::Session;
use crate::table::{Table, TableID};
use crate::trx::sys::{TransactionSystem, TrxSysConfig};
use crate::trx::ActiveTrx;
use crate::value::{Val, ValKind};

#[test]
fn test_mvcc_insert_normal() {
    smol::block_on(async {
        const SIZE: i32 = 10000;

        let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024).unwrap();
        let catalog = Catalog::empty_static();
        let trx_sys = TrxSysConfig::default().build_static(buf_pool, catalog);
        let table_id = create_table(buf_pool, catalog).await;
        let table = catalog.get_table(table_id).unwrap();
        let mut session = Session::new();
        {
            let mut trx = session.begin_trx(trx_sys);
            for i in 0..SIZE {
                let s = format!("{}", i);
                let mut stmt = trx.start_stmt();
                let res = table
                    .insert_row(buf_pool, &mut stmt, vec![Val::from(i), Val::from(&s[..])])
                    .await;
                trx = stmt.succeed();
                assert!(res.is_ok());
            }
            session = trx_sys.commit(trx, buf_pool, &catalog).await.unwrap();
        }
        {
            let mut trx = session.begin_trx(trx_sys);
            for i in 16..SIZE {
                let mut stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(i)]);
                let res = table
                    .select_row_mvcc(buf_pool, &mut stmt, &key, &[0, 1])
                    .await;
                match res {
                    SelectMvcc::Ok(vals) => {
                        assert!(vals.len() == 2);
                        assert!(&vals[0] == &Val::from(i));
                        let s = format!("{}", i);
                        assert!(&vals[1] == &Val::from(&s[..]));
                    }
                    _ => panic!("select fail"),
                }
                trx = stmt.succeed();
            }
            let _ = trx_sys.commit(trx, buf_pool, &catalog).await.unwrap();
        }

        unsafe {
            StaticLifetime::drop_static(trx_sys);
            StaticLifetime::drop_static(catalog);
            StaticLifetime::drop_static(buf_pool);
        }
    });
}

#[test]
fn test_mvcc_update_normal() {
    smol::block_on(async {
        const SIZE: i32 = 1000;

        let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024).unwrap();
        let catalog = Catalog::empty_static();
        let trx_sys = TrxSysConfig::default().build_static(buf_pool, catalog);
        let table_id = create_table(buf_pool, catalog).await;
        {
            let table = catalog.get_table(table_id).unwrap();

            let mut session = Session::new();
            // insert 1000 rows
            let mut trx = session.begin_trx(trx_sys);
            for i in 0..SIZE {
                let s = format!("{}", i);
                let mut stmt = trx.start_stmt();
                let res = table
                    .insert_row(buf_pool, &mut stmt, vec![Val::from(i), Val::from(&s[..])])
                    .await;
                trx = stmt.succeed();
                assert!(res.is_ok());
            }
            session = trx_sys.commit(trx, buf_pool, &catalog).await.unwrap();

            // update 1 row with short value
            let mut trx = session.begin_trx(trx_sys);
            let k1 = single_key(1i32);
            let s1 = "hello";
            let update1 = vec![UpdateCol {
                idx: 1,
                val: Val::from(s1),
            }];
            let mut stmt = trx.start_stmt();
            let res = table.update_row(buf_pool, &mut stmt, &k1, update1).await;
            assert!(res.is_ok());
            trx = stmt.succeed();
            session = trx_sys.commit(trx, buf_pool, &catalog).await.unwrap();

            // update 1 row with long value
            let mut trx = session.begin_trx(trx_sys);
            let k2 = single_key(100i32);
            let s2: String = (0..50_000).map(|_| '1').collect();
            let update2 = vec![UpdateCol {
                idx: 1,
                val: Val::from(&s2[..]),
            }];
            let mut stmt = trx.start_stmt();
            let res = table.update_row(buf_pool, &mut stmt, &k2, update2).await;
            assert!(res.is_ok());
            trx = stmt.succeed();

            // lookup this updated value inside same transaction
            let stmt = trx.start_stmt();
            let res = table.select_row_mvcc(buf_pool, &stmt, &k2, &[0, 1]).await;
            assert!(res.is_ok());
            let row = res.unwrap();
            assert!(row.len() == 2);
            assert!(row[0] == k2.vals[0]);
            assert!(row[1] == Val::from(&s2[..]));
            trx = stmt.succeed();

            session = trx_sys.commit(trx, buf_pool, &catalog).await.unwrap();

            // lookup with a new transaction
            let mut trx = session.begin_trx(trx_sys);
            let stmt = trx.start_stmt();
            let res = table.select_row_mvcc(buf_pool, &stmt, &k2, &[0, 1]).await;
            assert!(res.is_ok());
            let row = res.unwrap();
            assert!(row.len() == 2);
            assert!(row[0] == k2.vals[0]);
            assert!(row[1] == Val::from(&s2[..]));
            trx = stmt.succeed();

            let _ = trx_sys.commit(trx, buf_pool, &catalog).await.unwrap();
        }
        unsafe {
            StaticLifetime::drop_static(trx_sys);
            StaticLifetime::drop_static(catalog);
            StaticLifetime::drop_static(buf_pool);
        }
    });
}

#[test]
fn test_mvcc_delete_normal() {
    smol::block_on(async {
        const SIZE: i32 = 1000;

        let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024).unwrap();
        let catalog = Catalog::empty_static();
        let trx_sys = TrxSysConfig::default().build_static(buf_pool, catalog);
        let table_id = create_table(buf_pool, catalog).await;
        {
            let table = catalog.get_table(table_id).unwrap();

            let mut session = Session::new();
            // insert 1000 rows
            let mut trx = session.begin_trx(trx_sys);
            for i in 0..SIZE {
                let s = format!("{}", i);
                let mut stmt = trx.start_stmt();
                let res = table
                    .insert_row(buf_pool, &mut stmt, vec![Val::from(i), Val::from(&s[..])])
                    .await;
                trx = stmt.succeed();
                assert!(res.is_ok());
            }
            session = trx_sys.commit(trx, buf_pool, &catalog).await.unwrap();

            // delete 1 row
            let mut trx = session.begin_trx(trx_sys);
            let k1 = single_key(1i32);
            let mut stmt = trx.start_stmt();
            let res = table.delete_row(buf_pool, &mut stmt, &k1).await;
            assert!(res.is_ok());
            trx = stmt.succeed();

            // lookup row in same transaction
            let stmt = trx.start_stmt();
            let res = table.select_row_mvcc(buf_pool, &stmt, &k1, &[0]).await;
            assert!(res.not_found());
            trx = stmt.succeed();
            session = trx_sys.commit(trx, buf_pool, &catalog).await.unwrap();

            // lookup row in new transaction
            let mut trx = session.begin_trx(trx_sys);
            let stmt = trx.start_stmt();
            let res = table.select_row_mvcc(buf_pool, &stmt, &k1, &[0]).await;
            assert!(res.not_found());
            trx = stmt.succeed();
            let _ = trx_sys.commit(trx, buf_pool, &catalog).await.unwrap();
        }
        unsafe {
            StaticLifetime::drop_static(trx_sys);
            StaticLifetime::drop_static(catalog);
            StaticLifetime::drop_static(buf_pool);
        }
    });
}

#[test]
fn test_mvcc_rollback_insert_normal() {
    smol::block_on(async {
        let sys = TestSys::new().await;
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
        let sys = TestSys::new().await;
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
        let sys = TestSys::new().await;
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
        let sys = TestSys::new().await;
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
        let sys = TestSys::new().await;
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
        let sys = TestSys::new().await;
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

async fn create_table<P: BufferPool>(buf_pool: P, catalog: &Catalog<P>) -> TableID {
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

struct TestSys {
    buf_pool: &'static FixedBufferPool,
    catalog: &'static Catalog<&'static FixedBufferPool>,
    trx_sys: &'static TransactionSystem,
    table: Table<&'static FixedBufferPool>,
}

impl TestSys {
    #[inline]
    async fn new() -> Self {
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

impl Drop for TestSys {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            StaticLifetime::drop_static(self.trx_sys);
            StaticLifetime::drop_static(self.catalog);
            StaticLifetime::drop_static(self.buf_pool);
        }
    }
}

fn single_key<V: Into<Val>>(value: V) -> SelectKey {
    SelectKey {
        index_no: 0,
        vals: vec![value.into()],
    }
}
