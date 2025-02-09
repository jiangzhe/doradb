use crate::buffer::BufferPool;
use crate::index::{BlockIndex, SecondaryIndex};
use crate::lifetime::StaticLifetime;
use crate::row::ops::{SelectKey, UpdateCol};
use crate::table::{Table, TableID};
use crate::value::{Layout, Val, ValKind, ValType};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::HashSet;
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

unsafe impl<P: BufferPool> StaticLifetime for Catalog<P> {}

pub struct TableMeta<P> {
    pub schema: Arc<TableSchema>,
    pub blk_idx: Arc<BlockIndex<P>>,
    pub sec_idx: Arc<[SecondaryIndex]>,
}

pub struct TableSchema {
    types: Vec<ValType>,
    // fix length is the total inline length of all columns.
    pub fix_len: usize,
    // index of var-length columns.
    pub var_cols: Vec<usize>,
    // index column id.
    pub indexes: Vec<IndexSchema>,
    // columns that are included in any index.
    pub user_index_cols: HashSet<usize>,
}

impl TableSchema {
    /// Create a new schema.
    /// RowID is not included in input, but will be created
    /// automatically.
    #[inline]
    pub fn new(user_types: Vec<ValType>, indexes: Vec<IndexSchema>) -> Self {
        debug_assert!(!user_types.is_empty());
        debug_assert!(indexes.iter().all(|is| {
            is.keys
                .iter()
                .all(|k| (k.user_col_idx as usize) < user_types.len())
        }));

        let mut types = Vec::with_capacity(user_types.len() + 1);
        types.push(ValType {
            kind: ValKind::U64,
            nullable: false,
        });
        types.extend(user_types);
        let mut fix_len = 0;
        let mut var_cols = vec![];
        for (idx, ty) in types.iter().enumerate() {
            fix_len += ty.kind.layout().inline_len();
            if !ty.kind.layout().is_fixed() {
                var_cols.push(idx);
            }
        }
        let mut user_index_cols = HashSet::new();
        for index in &indexes {
            for key in &index.keys {
                user_index_cols.insert(key.user_col_idx as usize);
            }
        }
        TableSchema {
            types,
            fix_len,
            var_cols,
            indexes,
            user_index_cols,
        }
    }

    /// Returns column count of this schema, including row id.
    #[inline]
    pub fn col_count(&self) -> usize {
        self.types.len()
    }

    /// Returns layouts of all columns, including row id.
    #[inline]
    pub fn types(&self) -> &[ValType] {
        &self.types
    }

    #[inline]
    pub fn user_types(&self) -> &[ValType] {
        &self.types[1..]
    }

    /// Returns whether the type is matched at given column index, row id is excluded.
    #[inline]
    pub fn user_col_type_match(&self, user_col_idx: usize, val: &Val) -> bool {
        self.col_type_match(user_col_idx + 1, val)
    }

    /// Returns whether the type is matched at given column index.
    #[inline]
    pub fn col_type_match(&self, col_idx: usize, val: &Val) -> bool {
        layout_match(val, self.layout(col_idx))
    }

    #[inline]
    pub fn index_layout_match(&self, index_no: usize, vals: &[Val]) -> bool {
        let index = &self.indexes[index_no];
        if index.keys.len() != vals.len() {
            return false;
        }
        index
            .keys
            .iter()
            .map(|k| self.user_layout(k.user_col_idx as usize))
            .zip(vals)
            .all(|(layout, val)| layout_match(val, layout))
    }

    #[inline]
    pub fn user_layout(&self, user_col_idx: usize) -> Layout {
        self.layout(user_col_idx + 1)
    }

    #[inline]
    pub fn layout(&self, col_idx: usize) -> Layout {
        self.types[col_idx].kind.layout()
    }

    #[inline]
    pub fn keys_for_insert(&self, row: &[Val]) -> Vec<SelectKey> {
        self.indexes
            .iter()
            .enumerate()
            .map(|(index_no, is)| {
                let vals: Vec<Val> = is
                    .keys
                    .iter()
                    .map(|k| row[k.user_col_idx as usize].clone())
                    .collect();
                SelectKey { index_no, vals }
            })
            .collect()
    }

    #[inline]
    pub fn index_may_change(&self, update: &[UpdateCol]) -> bool {
        update
            .iter()
            .any(|uc| self.user_index_cols.contains(&uc.idx))
    }
}

#[inline]
fn layout_match(val: &Val, layout: Layout) -> bool {
    match (val, layout) {
        (Val::Null, _) => true,
        (Val::Byte1(_), Layout::Byte1)
        | (Val::Byte2(_), Layout::Byte2)
        | (Val::Byte4(_), Layout::Byte4)
        | (Val::Byte8(_), Layout::Byte8)
        | (Val::VarByte(_), Layout::VarByte) => true,
        _ => false,
    }
}

pub struct IndexSchema {
    pub keys: Vec<IndexKey>,
    pub unique: bool,
}

impl IndexSchema {
    #[inline]
    pub fn new(keys: Vec<IndexKey>, unique: bool) -> Self {
        debug_assert!(!keys.is_empty());
        IndexSchema { keys, unique }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexKey {
    pub user_col_idx: u16,
    pub order: IndexOrder,
}

impl IndexKey {
    #[inline]
    pub fn new(user_col_idx: u16) -> Self {
        IndexKey {
            user_col_idx,
            order: IndexOrder::Asc,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexOrder {
    Asc,
    Desc,
}

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

    /// Table1 has single i32 column, with unique index of this column.
    #[inline]
    pub(crate) fn table1<P: BufferPool>(buf_pool: &P, catalog: &Catalog<P>) -> TableID {
        catalog.create_table(
            buf_pool,
            TableSchema::new(
                vec![ValKind::I32.nullable(false)],
                vec![IndexSchema::new(vec![IndexKey::new(0)], true)],
            ),
        )
    }
}
