use doradb_storage::{
    BufferPoolCounters, BufferPoolRuntimeStats, BufferPoolStats, ColumnAttributes, ColumnSpec,
    DeleteMvcc, Engine, EngineConfig, IndexAttributes, IndexKey, IndexSpec, IoBackendStats,
    ScanMvcc, SelectKey, SelectMvcc, StorageIoStats, TableSpec, TransactionSystemStats, UpdateCol,
    UpdateMvcc, Val, ValKind, stats as storage_stats,
};
use tempfile::TempDir;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_facade_exports_stats_types_at_root_and_module_paths() {
        let root_stats = BufferPoolStats::default();
        let module_stats: storage_stats::BufferPoolStats = root_stats;
        assert_eq!(module_stats, storage_stats::BufferPoolStats::default());

        let _ = BufferPoolCounters::default();
        let _ = BufferPoolRuntimeStats::default();
        let _ = IoBackendStats::default();
        let _ = StorageIoStats::default();
        let _ = TransactionSystemStats::default();
    }

    #[test]
    fn public_facade_supports_table_id_mvcc_dml() {
        smol::block_on(async {
            let root = TempDir::new().unwrap();
            let engine: Engine = EngineConfig::default()
                .storage_root(root.path().to_path_buf())
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();

            let table_id = session
                .create_table(
                    TableSpec::new(vec![
                        ColumnSpec::new("id", ValKind::I32, ColumnAttributes::INDEX),
                        ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::INDEX),
                    ]),
                    vec![
                        IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK),
                        IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                    ],
                )
                .await
                .unwrap();

            let mut trx = session.begin_trx().unwrap();
            let _inserted = trx
                .exec(async |stmt| {
                    stmt.table_insert_mvcc(table_id, vec![Val::I32(1), Val::from("alice")])
                        .await
                })
                .await
                .unwrap();
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let selected = trx
                .exec(async |stmt| {
                    let key = SelectKey::new(0, vec![Val::I32(1)]);
                    stmt.table_lookup_unique_mvcc(table_id, key.index_no, &key.vals, &[0, 1])
                        .await
                })
                .await
                .unwrap();
            assert_eq!(
                selected,
                SelectMvcc::Found(vec![Val::I32(1), Val::from("alice")])
            );
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let updated = trx
                .exec(async |stmt| {
                    let key = SelectKey::new(0, vec![Val::I32(1)]);
                    stmt.table_update_unique_mvcc(
                        table_id,
                        key.index_no,
                        &key.vals,
                        vec![UpdateCol {
                            idx: 1,
                            val: Val::from("bob"),
                        }],
                    )
                    .await
                })
                .await
                .unwrap();
            assert!(matches!(updated, UpdateMvcc::Updated(_)));
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let rows = trx
                .exec(async |stmt| {
                    let mut rows = Vec::new();
                    stmt.table_scan_mvcc(table_id, &[0, 1], |row| {
                        rows.push(row);
                        true
                    })
                    .await?;
                    Ok(rows)
                })
                .await
                .unwrap();
            assert_eq!(rows, vec![vec![Val::I32(1), Val::from("bob")]]);

            let scanned = trx
                .exec(async |stmt| {
                    let key = SelectKey::new(1, vec![Val::from("bob")]);
                    stmt.table_index_scan_mvcc(table_id, key.index_no, &key.vals, &[0, 1])
                        .await
                })
                .await
                .unwrap();
            assert_eq!(
                scanned,
                ScanMvcc::Rows(vec![vec![Val::I32(1), Val::from("bob")]])
            );
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let deleted = trx
                .exec(async |stmt| {
                    let key = SelectKey::new(0, vec![Val::I32(1)]);
                    stmt.table_delete_unique_mvcc(table_id, key.index_no, &key.vals, false)
                        .await
                })
                .await
                .unwrap();
            assert_eq!(deleted, DeleteMvcc::Deleted);
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let selected = trx
                .exec(async |stmt| {
                    let key = SelectKey::new(0, vec![Val::I32(1)]);
                    stmt.table_lookup_unique_mvcc(table_id, key.index_no, &key.vals, &[0, 1])
                        .await
                })
                .await
                .unwrap();
            assert_eq!(selected, SelectMvcc::NotFound);
            trx.commit().await.unwrap();

            session.close().await.unwrap();
            engine.shutdown().unwrap();
        });
    }
}
