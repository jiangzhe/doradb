//! Shared untimed fixtures for CREATE and recovery acceptance measurements.
use crate::error::{BenchError, Result};
use crate::fixture::{IndexMode, RowPlacement, benchmark_table_spec};
use crate::plan::RecoveryFixture;
use doradb_storage::id::{TableID, TrxID};
use doradb_storage::{
    CallbackError, CallbackResult, CheckpointOutcome, Engine, FreezeOutcome, IndexID, RowMutation,
    StorageIndexFlags, StorageIndexKey, StorageIndexSpec, UpdateCol, Val,
};

/// Committed fixture identity and live placement after mutation.
pub(super) struct PreparedIndexTable {
    /// Public table identity.
    pub(super) table_id: TableID,
    /// Complete expected live row count.
    pub(super) rows: u64,
    /// Existing stable index identities.
    pub(super) indexes: Vec<IndexID>,
    /// Live rows in each storage tier after deletes and replacements.
    pub(super) placement: RowPlacement,
    /// Most recent successful preparation commit, absent for empty fixtures.
    pub(super) fence: Option<TrxID>,
}

/// Prepare deterministic contents before timing while retaining every verification identity.
pub(super) async fn prepare_index_fixture(
    engine: &Engine,
    fixture: RecoveryFixture,
) -> Result<Vec<PreparedIndexTable>> {
    let mut session = engine.new_session()?;
    let result = async {
        let mut tables = Vec::new();
        for _ in 0..fixture.tables {
            let mut placement = RowPlacement {
                hot_rows: fixture.rows - fixture.cold_rows,
                checkpointed_rows: fixture.cold_rows,
            };
            let mut fence = None;
            let specs = (0..fixture.indexes)
                .map(|slot| {
                    let columns = if fixture.composite {
                        if slot % 2 == 0 {
                            vec![1, 0]
                        } else {
                            vec![0, 1]
                        }
                    } else if fixture.index == IndexMode::Unique {
                        vec![0]
                    } else {
                        vec![1]
                    };
                    let flags = if fixture.index == IndexMode::Unique {
                        StorageIndexFlags::UK
                    } else {
                        StorageIndexFlags::empty()
                    };
                    StorageIndexSpec::new(
                        columns.into_iter().map(StorageIndexKey::new).collect(),
                        flags,
                    )
                })
                .collect();
            let table_id = session
                .create_table(benchmark_table_spec(), specs)
                .await?
                .table_id();
            for (checkpoint, start, end) in [
                (true, 0, fixture.cold_rows),
                (false, fixture.cold_rows, fixture.rows),
            ] {
                for batch in (start..end).step_by(1000) {
                    let mut trx = session.begin_trx()?;
                    for key in batch..batch.saturating_add(1000).min(end) {
                        let payload_key = if fixture.cardinality == 0 {
                            key
                        } else {
                            key % fixture.cardinality
                        };
                        let mut payload = vec![b'x'; fixture.value_bytes];
                        payload[..8].copy_from_slice(&payload_key.to_le_bytes());
                        trx.table_insert_mvcc(table_id, vec![Val::from(key), Val::from(payload)])
                            .await?;
                    }
                    fence = Some(trx.commit().await?);
                }
                if checkpoint && end != 0 {
                    let frozen = session.freeze_table(table_id, usize::MAX).await?;
                    if !matches!(frozen, FreezeOutcome::Frozen { .. }) {
                        return Err(BenchError::message(format!(
                            "recovery fixture freeze failed: {frozen:?}"
                        )));
                    }
                    let checkpoint = session.checkpoint_table_with_wait(table_id).await?;
                    if !matches!(checkpoint, CheckpointOutcome::Published { .. }) {
                        return Err(BenchError::message(format!(
                            "recovery fixture checkpoint failed: {checkpoint:?}"
                        )));
                    }
                }
            }
            let mut deleted = 0;
            if fixture.mutate_every != 0 {
                let mut trx = session.begin_trx()?;
                trx.table_mutate_mvcc(table_id, |row| -> CallbackResult<_, BenchError> {
                    let key = row.val(0)?.as_u64().ok_or_else(|| {
                        CallbackError::User(BenchError::message("recovery fixture key is not u64"))
                    })?;
                    if key % fixture.mutate_every == 0 {
                        deleted += 1;
                        if key < fixture.cold_rows {
                            placement.checkpointed_rows -= 1;
                        } else {
                            placement.hot_rows -= 1;
                        }
                        Ok(RowMutation::Delete)
                    } else if key % fixture.mutate_every == 1 {
                        if key < fixture.cold_rows {
                            placement.checkpointed_rows -= 1;
                            placement.hot_rows += 1;
                        }
                        Ok(RowMutation::Update(vec![UpdateCol {
                            idx: 1,
                            val: Val::from(vec![b'y'; fixture.value_bytes]),
                        }]))
                    } else {
                        Ok(RowMutation::Skip)
                    }
                })
                .await?;
                fence = Some(trx.commit().await?);
            }
            tables.push(PreparedIndexTable {
                placement,
                fence,
                table_id,
                rows: fixture.rows - deleted,
                indexes: (0..fixture.indexes)
                    .map(|index| IndexID::new(index as u32))
                    .collect(),
            });
        }
        Ok::<_, BenchError>(tables)
    }
    .await;
    let close = session.close().await;
    let tables = result?;
    close?;
    Ok(tables)
}
