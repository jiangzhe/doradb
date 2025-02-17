mod index;
mod storage;
mod table;

pub use index::*;
pub use storage::*;
pub use table::*;

use crate::buffer::BufferPool;
use crate::index::{BlockIndex, SecondaryIndex};
use crate::lifetime::StaticLifetime;
use crate::table::{Table, TableID};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Catalog contains metadata of user tables.
/// Initial implementation would be a in-mem hash-table.
pub struct Catalog<P: BufferPool> {
    table_id: AtomicU64,
    tables: Mutex<HashMap<TableID, TableMeta<P>>>,
}

impl<P: BufferPool> Catalog<P> {
    #[inline]
    pub fn empty() -> Self {
        Catalog {
            table_id: AtomicU64::new(1),
            tables: Mutex::new(HashMap::new()),
        }
    }

    #[inline]
    pub fn empty_static() -> &'static Self {
        let cat = Self::empty();
        StaticLifetime::new_static(cat)
    }

    #[inline]
    pub async fn create_table(&self, buf_pool: P, schema: TableSchema) -> TableID {
        let table_id = self.table_id.fetch_add(1, Ordering::SeqCst);
        let blk_idx = BlockIndex::new(buf_pool).await.unwrap();
        let sec_idx: Vec<_> = schema
            .indexes
            .iter()
            .enumerate()
            .map(|(index_no, index_schema)| {
                SecondaryIndex::new(index_no, index_schema, schema.user_types())
            })
            .collect();

        let mut g = self.tables.lock();
        let res = g.insert(
            table_id,
            TableMeta {
                schema: Arc::new(schema),
                blk_idx: Arc::new(blk_idx),
                sec_idx: Arc::from(sec_idx.into_boxed_slice()),
            },
        );
        debug_assert!(res.is_none());
        table_id
    }

    #[inline]
    pub fn get_table(&self, table_id: TableID) -> Option<Table<P>> {
        let g = self.tables.lock();
        g.get(&table_id).map(|meta| Table {
            table_id,
            schema: Arc::clone(&meta.schema),
            blk_idx: Arc::clone(&meta.blk_idx),
            sec_idx: Arc::clone(&meta.sec_idx),
        })
    }
}

unsafe impl<P: BufferPool> Send for Catalog<P> {}
unsafe impl<P: BufferPool> Sync for Catalog<P> {}
unsafe impl<P: BufferPool> StaticLifetime for Catalog<P> {}

pub struct TableCache<'a, P: BufferPool> {
    catalog: &'a Catalog<P>,
    map: HashMap<TableID, Option<Table<P>>>,
}

impl<'a, P: BufferPool> TableCache<'a, P> {
    #[inline]
    pub fn new(catalog: &'a Catalog<P>) -> Self {
        TableCache {
            catalog,
            map: HashMap::new(),
        }
    }

    #[inline]
    pub fn get_table(&mut self, table_id: TableID) -> &Option<Table<P>> {
        if self.map.contains_key(&table_id) {
            return &self.map[&table_id];
        }
        let table = self.catalog.get_table(table_id);
        self.map.entry(table_id).or_insert(table)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::value::ValKind;

    /// Table1 has single i32 column, with unique index of this column.
    #[inline]
    pub(crate) async fn table1<P: BufferPool>(buf_pool: P, catalog: &Catalog<P>) -> TableID {
        catalog
            .create_table(
                buf_pool,
                TableSchema::new(
                    vec![ValKind::I32.nullable(false)],
                    vec![IndexSchema::new(vec![IndexKey::new(0)], true)],
                ),
            )
            .await
    }
}
