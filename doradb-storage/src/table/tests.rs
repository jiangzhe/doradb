use crate::buffer::FixedBufferPool;
use crate::catalog::Catalog;
use crate::row::ops::{SelectMvcc, UpdateCol};
use crate::session::Session;
use crate::table::Schema;
use crate::table::TableID;
use crate::trx::sys::{TransactionSystem, TrxSysConfig};
use crate::value::Layout;
use crate::value::Val;

#[test]
fn test_mvcc_insert_normal() {
    smol::block_on(async {
        const SIZE: i32 = 10000;

        let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024).unwrap();
        let trx_sys = TrxSysConfig::default().build_static();

        let (catalog, table_id) = create_table(buf_pool);
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
                trx = stmt.commit();
                assert!(res.is_ok());
            }
            session = trx_sys.commit(trx).await.unwrap();
        }
        {
            let mut trx = session.begin_trx(trx_sys);
            for i in 16..SIZE {
                let mut stmt = trx.start_stmt();
                let key = Val::from(i);
                let res = table.select_row(buf_pool, &mut stmt, key, &[0, 1]).await;
                match res {
                    SelectMvcc::Ok(vals) => {
                        assert!(vals.len() == 2);
                        assert!(&vals[0] == &Val::from(i));
                        let s = format!("{}", i);
                        assert!(&vals[1] == &Val::from(&s[..]));
                    }
                    _ => panic!("select fail"),
                }
                trx = stmt.commit();
            }
            let _ = trx_sys.commit(trx).await.unwrap();
        }

        unsafe {
            TransactionSystem::drop_static(trx_sys);
            FixedBufferPool::drop_static(buf_pool);
        }
    });
}

#[test]
fn test_mvcc_update() {
    smol::block_on(async {
        const SIZE: i32 = 1000;

        let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024).unwrap();
        let trx_sys = TrxSysConfig::default().build_static();
        {
            let (catalog, table_id) = create_table(buf_pool);
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
                trx = stmt.commit();
                assert!(res.is_ok());
            }
            session = trx_sys.commit(trx).await.unwrap();

            // update 1 row with short value
            let mut trx = session.begin_trx(trx_sys);
            let k1 = Val::from(1i32);
            let s1 = "hello";
            let update1 = vec![UpdateCol {
                idx: 1,
                val: Val::from(s1),
            }];
            let mut stmt = trx.start_stmt();
            let res = table.update_row(buf_pool, &mut stmt, k1, update1).await;
            assert!(res.is_ok());
            trx = stmt.commit();
            session = trx_sys.commit(trx).await.unwrap();

            // update 1 row with long value
            let mut trx = session.begin_trx(trx_sys);
            let k2 = Val::from(100i32);
            let s2: String = (0..50_000).map(|_| '1').collect();
            let update2 = vec![UpdateCol {
                idx: 1,
                val: Val::from(&s2[..]),
            }];
            let mut stmt = trx.start_stmt();
            let res = table.update_row(buf_pool, &mut stmt, k2, update2).await;
            assert!(res.is_ok());
            trx = stmt.commit();
            let _ = trx_sys.commit(trx).await.unwrap();
        }
        unsafe {
            TransactionSystem::drop_static(trx_sys);
            FixedBufferPool::drop_static(buf_pool);
        }
    });
}

#[test]
fn test_mvcc_delete_normal() {
    smol::block_on(async {
        const SIZE: i32 = 1000;

        let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024).unwrap();
        let trx_sys = TrxSysConfig::default().build_static();
        {
            let (catalog, table_id) = create_table(buf_pool);
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
                trx = stmt.commit();
                assert!(res.is_ok());
            }
            session = trx_sys.commit(trx).await.unwrap();

            // delete 1 row
            let mut trx = session.begin_trx(trx_sys);
            let k1 = Val::from(1i32);
            let mut stmt = trx.start_stmt();
            let res = table.delete_row(buf_pool, &mut stmt, k1).await;
            assert!(res.is_ok());
            trx = stmt.commit();
            let _ = trx_sys.commit(trx).await.unwrap();
        }
        unsafe {
            TransactionSystem::drop_static(trx_sys);
            FixedBufferPool::drop_static(buf_pool);
        }
    });
}

#[test]
fn test_mvcc_rollback_insert_normal() {
    smol::block_on(async {
        let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024).unwrap();
        let trx_sys = TrxSysConfig::default().build_static();
        {
            let (catalog, table_id) = create_table(buf_pool);
            let table = catalog.get_table(table_id).unwrap();

            let mut session = Session::new();
            // insert 1 row
            let mut trx = session.begin_trx(trx_sys);
            let mut stmt = trx.start_stmt();
            let res = table
                .insert_row(
                    buf_pool,
                    &mut stmt,
                    vec![Val::from(1i32), Val::from("hello")],
                )
                .await;
            assert!(res.is_ok());
            trx = stmt.rollback(buf_pool, &catalog);
            session = trx_sys.commit(trx).await.unwrap();

            // select 1 row
            let mut trx = session.begin_trx(trx_sys);
            let stmt = trx.start_stmt();
            let key = Val::from(1i32);
            let res = table.select_row(buf_pool, &stmt, key, &[0, 1]).await;
            assert!(res.not_found());
            trx = stmt.commit();
            _ = trx_sys.commit(trx).await.unwrap();
        }
        unsafe {
            TransactionSystem::drop_static(trx_sys);
            FixedBufferPool::drop_static(buf_pool);
        }
    });
}

#[test]
fn test_mvcc_move_insert() {
    smol::block_on(async {
        let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024).unwrap();
        let trx_sys = TrxSysConfig::default().build_static();
        {
            let (catalog, table_id) = create_table(buf_pool);
            let table = catalog.get_table(table_id).unwrap();

            let mut session = Session::new();
            // insert 1 row
            let mut trx = session.begin_trx(trx_sys);
            let mut stmt = trx.start_stmt();
            let res = table
                .insert_row(
                    buf_pool,
                    &mut stmt,
                    vec![Val::from(1i32), Val::from("hello")],
                )
                .await;
            assert!(res.is_ok());
            trx = stmt.commit();
            session = trx_sys.commit(trx).await.unwrap();

            // delete it
            let mut trx = session.begin_trx(trx_sys);
            let mut stmt = trx.start_stmt();
            let key = Val::from(1i32);
            let res = table.delete_row(buf_pool, &mut stmt, key).await;
            assert!(res.is_ok());
            trx = stmt.commit();
            session = trx_sys.commit(trx).await.unwrap();

            // insert again, trigger move+insert
            let mut trx = session.begin_trx(trx_sys);
            let mut stmt = trx.start_stmt();
            let res = table
                .insert_row(
                    buf_pool,
                    &mut stmt,
                    vec![Val::from(1i32), Val::from("world")],
                )
                .await;
            assert!(res.is_ok());
            trx = stmt.commit();
            session = trx_sys.commit(trx).await.unwrap();

            // select 1 row
            let mut trx = session.begin_trx(trx_sys);
            let stmt = trx.start_stmt();
            let key = Val::from(1i32);
            let res = table.select_row(buf_pool, &stmt, key, &[0, 1]).await;
            assert!(res.is_ok());
            let vals = res.unwrap();
            assert!(vals[1] == Val::from("world"));
            trx = stmt.commit();
            _ = trx_sys.commit(trx).await.unwrap();
        }
        unsafe {
            TransactionSystem::drop_static(trx_sys);
            FixedBufferPool::drop_static(buf_pool);
        }
    });
}

#[test]
fn test_mvcc_rollback_move_insert() {
    smol::block_on(async {
        let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024).unwrap();
        let trx_sys = TrxSysConfig::default().build_static();
        {
            let (catalog, table_id) = create_table(buf_pool);
            let table = catalog.get_table(table_id).unwrap();

            let mut session = Session::new();
            // insert 1 row
            let mut trx = session.begin_trx(trx_sys);
            let mut stmt = trx.start_stmt();
            let res = table
                .insert_row(
                    buf_pool,
                    &mut stmt,
                    vec![Val::from(1i32), Val::from("hello")],
                )
                .await;
            assert!(res.is_ok());
            println!("row_id={}", res.unwrap());
            trx = stmt.commit();
            session = trx_sys.commit(trx).await.unwrap();

            // delete it
            let mut trx = session.begin_trx(trx_sys);
            let mut stmt = trx.start_stmt();
            let key = Val::from(1i32);
            let res = table.delete_row(buf_pool, &mut stmt, key).await;
            assert!(res.is_ok());
            trx = stmt.commit();
            session = trx_sys.commit(trx).await.unwrap();

            // insert again, trigger move+insert
            let mut trx = session.begin_trx(trx_sys);
            let mut stmt = trx.start_stmt();
            let res = table
                .insert_row(
                    buf_pool,
                    &mut stmt,
                    vec![Val::from(1i32), Val::from("world")],
                )
                .await;
            assert!(res.is_ok());
            println!("row_id={}", res.unwrap());
            trx = stmt.rollback(buf_pool, &catalog);
            session = trx_sys.commit(trx).await.unwrap();

            // select 1 row
            let mut trx = session.begin_trx(trx_sys);
            let stmt = trx.start_stmt();
            let key = Val::from(1i32);
            let res = table.select_row(buf_pool, &stmt, key, &[0, 1]).await;
            assert!(res.not_found());
            trx = stmt.commit();
            _ = trx_sys.commit(trx).await.unwrap();
        }
        unsafe {
            TransactionSystem::drop_static(trx_sys);
            FixedBufferPool::drop_static(buf_pool);
        }
    });
}

fn create_table(buf_pool: &'static FixedBufferPool) -> (Catalog<FixedBufferPool>, TableID) {
    let catalog = Catalog::empty();
    let table_id = catalog.create_table(
        buf_pool,
        Schema::new(vec![Layout::Byte4, Layout::VarByte], 0),
    );
    (catalog, table_id)
}
