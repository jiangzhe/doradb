use doradb_storage::{
    ColumnAttributes, ColumnSpec, EngineConfig, IndexAttributes, IndexKey, IndexSpec, SelectKey,
    TableSpec, UpdateCol, Val, ValKind,
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
    let engine = EngineConfig::default()
        .storage_root(temp_dir.path())
        .build()
        .await?;
    let mut session = engine.new_session()?;

    // Create a table with a unique id index and a secondary name index.
    let table_id = session
        .create_table(
            TableSpec::new(vec![
                ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
            ]),
            vec![
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK),
                IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
            ],
        )
        .await?;

    let mut write_trx = session.begin_trx()?;
    // Insert two rows in one statement.
    write_trx
        .exec(async |stmt| {
            stmt.table_insert_mvcc(table_id, vec![Val::from(1i32), Val::from("alice")])
                .await?;
            stmt.table_insert_mvcc(table_id, vec![Val::from(2i32), Val::from("bob")])
                .await?;
            Ok(())
        })
        .await?;

    let id_one = SelectKey::new(0, vec![Val::from(1i32)]);
    // Update one row by its unique id key.
    write_trx
        .exec(async |stmt| {
            let res = stmt
                .table_update_unique_mvcc(
                    table_id,
                    id_one.index_no,
                    &id_one.vals,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("ada"),
                    }],
                )
                .await?;
            assert!(res.is_updated());
            Ok(())
        })
        .await?;

    let id_two = SelectKey::new(0, vec![Val::from(2i32)]);
    // Delete one row by its unique id key.
    write_trx
        .exec(async |stmt| {
            let res = stmt
                .table_delete_unique_mvcc(table_id, id_two.index_no, &id_two.vals, false)
                .await?;
            assert!(res.is_deleted());
            Ok(())
        })
        .await?;
    write_trx.commit().await?;

    let mut read_trx = session.begin_trx()?;
    let mut scanned_rows = Vec::new();
    // Scan visible rows from the table.
    read_trx
        .exec(async |stmt| {
            stmt.table_scan_mvcc(table_id, &[0, 1], |vals| {
                scanned_rows.push(row_pair(vals));
                true
            })
            .await?;
            Ok(())
        })
        .await?;
    scanned_rows.sort_unstable();
    assert_eq!(scanned_rows, vec![(1, String::from("ada"))]);

    // Lookup one row through the unique id index.
    let found = read_trx
        .exec(async |stmt| {
            stmt.table_lookup_unique_mvcc(table_id, id_one.index_no, &id_one.vals, &[0, 1])
                .await
        })
        .await?
        .unwrap_found();
    assert_eq!(row_pair(found), (1, String::from("ada")));

    let name_key = SelectKey::new(1, vec![Val::from("ada")]);
    // Scan rows that match one secondary-index key.
    let mut matching_rows = read_trx
        .exec(async |stmt| {
            stmt.table_index_scan_mvcc(table_id, name_key.index_no, &name_key.vals, &[0, 1])
                .await
        })
        .await?
        .unwrap_rows()
        .into_iter()
        .map(row_pair)
        .collect::<Vec<_>>();
    matching_rows.sort_unstable();
    assert_eq!(matching_rows, vec![(1, String::from("ada"))]);

    // Stream the same secondary-index match one row at a time.
    let mut stream = read_trx
        .stream_stmt()
        .table_index_scan_mvcc(
            table_id,
            name_key.index_no,
            &name_key.vals[..]..=&name_key.vals[..],
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
    engine.shutdown()?;

    println!("quick start example completed");
    Ok(())
}

fn row_pair(vals: Vec<Val>) -> (i32, String) {
    let id = vals[0].as_i32().expect("id must be i32");
    let name = vals[1].as_str().expect("name must be UTF-8").to_string();
    (id, name)
}
