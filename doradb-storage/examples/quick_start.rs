use doradb_storage::{
    CallbackResult, Engine, EngineConfig, IndexID, ScanRowDecision, StorageColumnFlags,
    StorageColumnSpec, StorageIndexFlags, StorageIndexKey, StorageIndexSpec, StorageTableSpec,
    TableIndex, UpdateCol, Val, ValKind,
};
use futures::executor;
use std::error::Error;
use std::process::exit;
use std::result::Result as StdResult;
use tempfile::TempDir;

type ExampleResult<T> = StdResult<T, Box<dyn Error>>;

fn main() {
    if let Err(err) = executor::block_on(run()) {
        eprintln!("{err}");
        exit(1);
    }
}

async fn run() -> ExampleResult<()> {
    // Build an engine using a temporary storage root for this example run.
    let temp_dir = TempDir::new()?;
    let engine = Engine::bootstrap(EngineConfig::default().storage_root(temp_dir.path())).await?;
    let mut session = engine.new_session()?;

    // Create a table with a unique id index and a secondary name index.
    let table_id = session
        .create_table(
            StorageTableSpec::new(vec![
                StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
            ]),
            vec![
                StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK),
                StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::empty()),
            ],
        )
        .await?
        .table_id();

    let mut write_trx = session.begin_trx()?;
    // Insert two rows in one statement.
    write_trx
        .table_insert_batch_mvcc(
            table_id,
            vec![
                vec![Val::from(1i32), Val::from("alice")],
                vec![Val::from(2i32), Val::from("bob")],
            ],
        )
        .await?;

    let id_index = IndexID::new(0);
    let id_one = [Val::from(1i32)];
    // Update one row by its unique id key.
    let updated = write_trx
        .table_update_unique_mvcc(
            TableIndex(table_id, id_index),
            &id_one,
            vec![UpdateCol {
                idx: 1,
                val: Val::from("ada"),
            }],
        )
        .await?;
    assert!(updated.is_updated());

    let id_two = [Val::from(2i32)];
    // Delete one row by its unique id key.
    let deleted = write_trx
        .table_delete_unique_mvcc(TableIndex(table_id, id_index), &id_two)
        .await?;
    assert!(deleted.is_deleted());
    write_trx.commit().await?;

    let mut read_trx = session.begin_trx()?;
    let mut scanned_rows = Vec::new();
    // Stream visible rows from the table.
    let mut table_stream = read_trx
        .table_scan_mvcc_stream(table_id, &[0, 1], |_| -> CallbackResult<_> {
            Ok(ScanRowDecision::Include)
        })
        .await?;
    while let Some(vals) = table_stream.next().await? {
        scanned_rows.push(row_pair(vals));
    }
    drop(table_stream);
    scanned_rows.sort_unstable();
    assert_eq!(scanned_rows, vec![(1, String::from("ada"))]);

    // Lookup one row through the unique id index.
    let found = read_trx
        .table_lookup_unique_mvcc(TableIndex(table_id, id_index), &id_one, &[0, 1])
        .await?
        .unwrap_found();
    assert_eq!(row_pair(found), (1, String::from("ada")));

    let name_index = IndexID::new(1);
    let name_key = [Val::from("ada")];
    // Scan rows that match one secondary-index key.
    let mut matching_rows = read_trx
        .table_index_lookup_mvcc(TableIndex(table_id, name_index), &name_key, &[0, 1])
        .await?
        .unwrap_rows()
        .into_iter()
        .map(row_pair)
        .collect::<Vec<_>>();
    matching_rows.sort_unstable();
    assert_eq!(matching_rows, vec![(1, String::from("ada"))]);

    // Stream the same secondary-index match one row at a time.
    let mut stream = read_trx
        .table_index_scan_mvcc_stream(
            TableIndex(table_id, name_index),
            &name_key[..]..=&name_key[..],
            &[0, 1],
        )
        .await?;
    let mut streamed_rows = Vec::new();
    while let Some(vals) = stream.next().await? {
        streamed_rows.push(row_pair(vals));
    }
    drop(stream);
    streamed_rows.sort_unstable();
    assert_eq!(streamed_rows, vec![(1, String::from("ada"))]);
    read_trx.rollback().await?;

    // Drop the table after all transactions are finished.
    session.drop_table(table_id).await?;
    assert!(!session.list_table_ids()?.contains(&table_id));
    session.close().await?;
    engine.shutdown();

    println!("quick start example completed");
    Ok(())
}

fn row_pair(vals: Vec<Val>) -> (i32, String) {
    let id = vals[0].as_i32().expect("id must be i32");
    let name = vals[1].as_str().expect("name must be UTF-8").to_string();
    (id, name)
}
