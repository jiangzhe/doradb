use crate::buffer::BufferPool;
use crate::index::{BlockIndex, SecondaryIndex};
use crate::table::{Table, TableID, TableSchema};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Catalog contains metadata of user tables.
/// Initial implementation would be a in-mem hash-table.
pub struct Catalog<P> {
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
    pub fn create_table(&self, buf_pool: &P, schema: TableSchema) -> TableID {
        let table_id = self.table_id.fetch_add(1, Ordering::SeqCst);
        let blk_idx = BlockIndex::new(buf_pool).unwrap();
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

pub struct TableMeta<P> {
    pub schema: Arc<TableSchema>,
    pub blk_idx: Arc<BlockIndex<P>>,
    pub sec_idx: Arc<[SecondaryIndex]>,
}
