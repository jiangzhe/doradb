use doradb_storage::{
    ColumnAttributes, ColumnSpec, EngineConfig, IndexAttributes, IndexKey, IndexSpec, SelectKey,
    SelectMvcc, TableSpec, Val, ValKind,
};
use tempfile::TempDir;

#[test]
fn public_facade_supports_table_lookup_and_mvcc_dml() {
    smol::block_on(async {
        let root = TempDir::new().unwrap();
        let engine = EngineConfig::default()
            .storage_root(root.path().to_path_buf())
            .build()
            .await
            .unwrap();
        let mut session = engine.new_session().unwrap();

        let table_id = session
            .create_table(
                TableSpec::new(vec![
                    ColumnSpec::new("id", ValKind::I32, ColumnAttributes::INDEX),
                    ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                ]),
                vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
            )
            .await
            .unwrap();
        let table = session.get_table(table_id).await.unwrap();

        let mut trx = session.begin_trx().unwrap();
        let _inserted = trx
            .exec(async |stmt| {
                stmt.table_insert_mvcc(&table, vec![Val::I32(1), Val::from("alice")])
                    .await
            })
            .await
            .unwrap();
        trx.commit().await.unwrap();

        let mut trx = session.begin_trx().unwrap();
        let selected = trx
            .exec(async |stmt| {
                stmt.table_lookup_unique_mvcc(
                    &table,
                    &SelectKey::new(0, vec![Val::I32(1)]),
                    &[0, 1],
                )
                .await
            })
            .await
            .unwrap();
        assert_eq!(
            selected,
            SelectMvcc::Found(vec![Val::I32(1), Val::from("alice")])
        );
        trx.commit().await.unwrap();

        drop(table);
        drop(session);
        engine.shutdown().unwrap();
    });
}
