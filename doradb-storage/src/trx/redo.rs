use crate::buffer::page::PageID;
use crate::error::Result;
use crate::row::ops::UpdateCol;
use crate::row::RowID;
use crate::serde::{Deser, Ser, SerdeCtx};
use crate::table::TableID;
use crate::trx::TrxID;
use crate::value::Val;
use doradb_catalog::{IndexID, SchemaID};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::mem;

/// Defines the code of redo operation on a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RowRedoCode {
    Insert = 1,
    Delete = 2,
    Update = 3,
}

impl From<u8> for RowRedoCode {
    #[inline]
    fn from(code: u8) -> Self {
        unsafe { mem::transmute(code) }
    }
}

/// Defines the kind of redo operation on a row.
pub enum RowRedoKind {
    Insert(Vec<Val>),
    Delete,
    Update(Vec<UpdateCol>),
}

impl RowRedoKind {
    /// Get the code of the redo operation on a row.
    #[inline]
    pub fn code(&self) -> RowRedoCode {
        match self {
            RowRedoKind::Insert(..) => RowRedoCode::Insert,
            RowRedoKind::Delete => RowRedoCode::Delete,
            RowRedoKind::Update(..) => RowRedoCode::Update,
        }
    }
}
impl Ser<'_> for RowRedoKind {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        mem::size_of::<u8>()
            + match self {
                RowRedoKind::Insert(vals) => vals.ser_len(ctx),
                RowRedoKind::Delete => 0,
                RowRedoKind::Update(cols) => cols.ser_len(ctx),
            }
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = ctx.ser_u8(out, idx, self.code() as u8);
        match self {
            RowRedoKind::Insert(vals) => {
                idx = vals.ser(ctx, out, idx);
            }
            RowRedoKind::Delete => (),
            RowRedoKind::Update(cols) => {
                idx = cols.ser(ctx, out, idx);
            }
        }
        idx
    }
}

impl Deser for RowRedoKind {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, data: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, code) = ctx.deser_u8(data, start_idx)?;
        match RowRedoCode::from(code) {
            RowRedoCode::Insert => {
                let (idx, vals) = Vec::<Val>::deser(ctx, data, idx)?;
                Ok((idx, RowRedoKind::Insert(vals)))
            }
            RowRedoCode::Delete => Ok((idx, RowRedoKind::Delete)),
            RowRedoCode::Update => {
                let (idx, cols) = Vec::<UpdateCol>::deser(ctx, data, idx)?;
                Ok((idx, RowRedoKind::Update(cols)))
            }
        }
    }
}

/// Represents a redo operation on a row.
pub struct RowRedo {
    pub page_id: PageID,
    pub row_id: RowID,
    pub kind: RowRedoKind,
}

impl Ser<'_> for RowRedo {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        mem::size_of::<PageID>() + mem::size_of::<RowID>() + self.kind.ser_len(ctx)
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = ctx.ser_u64(out, idx, self.page_id);
        idx = ctx.ser_u64(out, idx, self.row_id);
        idx = self.kind.ser(ctx, out, idx);
        idx
    }
}

impl Deser for RowRedo {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, data: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, page_id) = ctx.deser_u64(data, start_idx)?;
        let (idx, row_id) = ctx.deser_u64(data, idx)?;
        let (idx, kind) = RowRedoKind::deser(ctx, data, idx)?;
        Ok((
            idx,
            RowRedo {
                page_id,
                row_id,
                kind,
            },
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum DDLRedoCode {
    CreateSchema = 129,
    DropSchema = 130,
    CreateTable = 131,
    DropTable = 132,
    CreateIndex = 133,
    DropIndex = 134,
    CreateRowPage = 135,
}

impl From<u8> for DDLRedoCode {
    #[inline]
    fn from(code: u8) -> Self {
        unsafe { mem::transmute(code) }
    }
}

/// Represents a redo record of any DDL operation.
pub enum DDLRedo {
    CreateSchema(SchemaID),
    DropSchema(SchemaID),
    // Create a new table with given table id.
    // Actual metadata change is recorded in DML logs.
    CreateTable(TableID),
    DropTable(TableID),
    CreateIndex(IndexID),
    DropIndex(IndexID),
    CreateRowPage {
        table_id: TableID,
        page_id: PageID,
        start_row_id: RowID,
        end_row_id: RowID,
    },
}

impl DDLRedo {
    /// Get the code of the redo operation on a row.
    #[inline]
    pub fn code(&self) -> DDLRedoCode {
        match self {
            DDLRedo::CreateSchema { .. } => DDLRedoCode::CreateSchema,
            DDLRedo::DropSchema { .. } => DDLRedoCode::DropSchema,
            DDLRedo::CreateTable { .. } => DDLRedoCode::CreateTable,
            DDLRedo::DropTable { .. } => DDLRedoCode::DropTable,
            DDLRedo::CreateIndex { .. } => DDLRedoCode::CreateIndex,
            DDLRedo::DropIndex { .. } => DDLRedoCode::DropIndex,
            DDLRedo::CreateRowPage { .. } => DDLRedoCode::CreateRowPage,
        }
    }
}

impl Ser<'_> for DDLRedo {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<u8>()
            + match self {
                DDLRedo::CreateSchema(_) => mem::size_of::<SchemaID>(),
                DDLRedo::DropSchema(_) => mem::size_of::<SchemaID>(),
                DDLRedo::CreateTable(_) => mem::size_of::<TableID>(),
                DDLRedo::DropTable(_) => mem::size_of::<TableID>(),
                DDLRedo::CreateIndex(_) => mem::size_of::<IndexID>(),
                DDLRedo::DropIndex(_) => mem::size_of::<IndexID>(),
                DDLRedo::CreateRowPage { .. } => {
                    mem::size_of::<TableID>()
                        + mem::size_of::<PageID>()
                        + mem::size_of::<RowID>() * 2
                }
            }
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = ctx.ser_u8(out, idx, self.code() as u8);
        match self {
            DDLRedo::CreateSchema(schema_id) => {
                idx = ctx.ser_u64(out, idx, *schema_id);
            }
            DDLRedo::DropSchema(schema_id) => {
                idx = ctx.ser_u64(out, idx, *schema_id);
            }
            DDLRedo::CreateTable(table_id) => {
                idx = ctx.ser_u64(out, idx, *table_id);
            }
            DDLRedo::DropTable(table_id) => {
                idx = ctx.ser_u64(out, idx, *table_id);
            }
            DDLRedo::CreateIndex(index_id) => {
                idx = ctx.ser_u64(out, idx, *index_id);
            }
            DDLRedo::DropIndex(index_id) => {
                idx = ctx.ser_u64(out, idx, *index_id);
            }
            DDLRedo::CreateRowPage {
                table_id,
                page_id,
                start_row_id,
                end_row_id,
            } => {
                idx = ctx.ser_u64(out, idx, *table_id);
                idx = ctx.ser_u64(out, idx, *page_id);
                idx = ctx.ser_u64(out, idx, *start_row_id);
                idx = ctx.ser_u64(out, idx, *end_row_id);
            }
        }
        idx
    }
}

impl Deser for DDLRedo {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, data: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, code) = ctx.deser_u8(data, start_idx)?;
        match DDLRedoCode::from(code) {
            DDLRedoCode::CreateSchema => {
                let (idx, schema_id) = ctx.deser_u64(data, idx)?;
                Ok((idx, DDLRedo::CreateSchema(schema_id)))
            }
            DDLRedoCode::DropSchema => {
                let (idx, schema_id) = ctx.deser_u64(data, idx)?;
                Ok((idx, DDLRedo::DropSchema(schema_id)))
            }
            DDLRedoCode::CreateTable => {
                let (idx, table_id) = ctx.deser_u64(data, idx)?;
                Ok((idx, DDLRedo::CreateTable(table_id)))
            }
            DDLRedoCode::DropTable => {
                let (idx, table_id) = ctx.deser_u64(data, idx)?;
                Ok((idx, DDLRedo::DropTable(table_id)))
            }
            DDLRedoCode::CreateIndex => {
                let (idx, index_id) = ctx.deser_u64(data, idx)?;
                Ok((idx, DDLRedo::CreateIndex(index_id)))
            }
            DDLRedoCode::DropIndex => {
                let (idx, index_id) = ctx.deser_u64(data, idx)?;
                Ok((idx, DDLRedo::DropIndex(index_id)))
            }
            DDLRedoCode::CreateRowPage => {
                let (idx, table_id) = ctx.deser_u64(data, idx)?;
                let (idx, page_id) = ctx.deser_u64(data, idx)?;
                let (idx, start_row_id) = ctx.deser_u64(data, idx)?;
                let (idx, end_row_id) = ctx.deser_u64(data, idx)?;
                Ok((
                    idx,
                    DDLRedo::CreateRowPage {
                        table_id,
                        page_id,
                        start_row_id,
                        end_row_id,
                    },
                ))
            }
        }
    }
}

/// Represents the redo logs of a transaction.
#[derive(Default)]
pub struct RedoLogs {
    pub ddl: Vec<DDLRedo>,
    pub dml: BTreeMap<TableID, TableDML>,
}

impl RedoLogs {
    /// Returns true if the redo logs are empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ddl.is_empty() && self.dml.is_empty()
    }

    /// Clear all redo logs.
    #[inline]
    pub fn clear(&mut self) {
        self.ddl.clear();
        self.dml.clear();
    }

    /// Insert a redo entry into the redo logs.
    #[inline]
    pub fn insert_dml(&mut self, table_id: TableID, entry: RowRedo) {
        let table = self.dml.entry(table_id).or_default();
        table.insert(entry);
    }

    /// Merge redo logs from other.
    #[inline]
    pub fn merge(&mut self, other: RedoLogs) {
        if self.is_empty() {
            *self = other;
            return;
        }
        self.ddl.extend(other.ddl);
        for (table_id, table) in other.dml {
            match self.dml.entry(table_id) {
                Entry::Vacant(vac) => {
                    vac.insert(table);
                }
                Entry::Occupied(mut occ) => {
                    occ.get_mut().merge(table);
                }
            }
        }
    }
}

impl Ser<'_> for RedoLogs {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        self.ddl.ser_len(ctx) + self.dml.ser_len(ctx)
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = self.ddl.ser(ctx, out, idx);
        idx = self.dml.ser(ctx, out, idx);
        idx
    }
}

impl Deser for RedoLogs {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, data: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, ddl) = Vec::<DDLRedo>::deser(ctx, data, start_idx)?;
        let (idx, dml) = BTreeMap::<TableID, TableDML>::deser(ctx, data, idx)?;
        Ok((idx, RedoLogs { ddl, dml }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RedoTrxKind {
    User = 0,
    System = 1,
}

impl From<u8> for RedoTrxKind {
    #[inline]
    fn from(code: u8) -> Self {
        unsafe { mem::transmute(code) }
    }
}

/// Redo header of a transaction.
pub struct RedoHeader {
    pub cts: TrxID,
    pub trx_kind: RedoTrxKind,
}

impl Ser<'_> for RedoHeader {
    #[inline]
    fn ser_len(&self, _ctx: &SerdeCtx) -> usize {
        mem::size_of::<TrxID>() + mem::size_of::<u8>()
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        let mut idx = start_idx;
        idx = self.cts.ser(ctx, out, idx);
        idx = (self.trx_kind as u8).ser(ctx, out, idx);
        idx
    }
}

impl Deser for RedoHeader {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, data: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, cts) = TrxID::deser(ctx, data, start_idx)?;
        let (idx, code) = u8::deser(ctx, data, idx)?;
        Ok((
            idx,
            RedoHeader {
                cts,
                trx_kind: RedoTrxKind::from(code),
            },
        ))
    }
}

/// Represents the redo logs of a table.
#[derive(Default)]
pub struct TableDML {
    pub rows: BTreeMap<RowID, RowRedo>,
}

impl TableDML {
    /// Insert a redo entry into the table redo logs.
    #[inline]
    pub fn insert(&mut self, entry: RowRedo) {
        match self.rows.entry(entry.row_id) {
            Entry::Vacant(vac) => {
                vac.insert(entry);
            }
            Entry::Occupied(mut occ) => {
                let old = occ.get_mut();
                match (&mut old.kind, entry.kind) {
                    (RowRedoKind::Delete, _) => {
                        // Once the old RowID is deleted, there is impossible
                        // to have another operation on the same RowID.
                        unreachable!()
                    }
                    (RowRedoKind::Insert(..), RowRedoKind::Insert(..)) => {
                        // Same RowID cannot be inserted twice
                        unreachable!()
                    }
                    (RowRedoKind::Insert(vals), RowRedoKind::Update(upd_cols)) => {
                        // Apply update to inserted rows.
                        // Insert contains all columns, so updating indexed columns won't fail.
                        for upd_col in upd_cols {
                            vals[upd_col.idx] = upd_col.val;
                        }
                    }
                    (RowRedoKind::Insert(..), RowRedoKind::Delete) => {
                        // Insert and then delete the same RowID.
                        // Remove this entry.
                        occ.remove_entry();
                    }
                    (RowRedoKind::Update(vals), RowRedoKind::Update(upd_cols)) => {
                        // Apply update to updated rows.
                        // Update any indexed columns that are not included in new update.
                        for upd_col in upd_cols {
                            match vals.binary_search_by_key(&upd_col.idx, |val| val.idx) {
                                Ok(idx) => {
                                    vals[idx] = upd_col;
                                }
                                Err(idx) => {
                                    vals.insert(idx, upd_col);
                                }
                            }
                        }
                    }
                    (RowRedoKind::Update(..), RowRedoKind::Delete) => {
                        // Update and then delete the same RowID.
                        // Remove this entry.
                        occ.remove_entry();
                    }
                    (RowRedoKind::Update(..), RowRedoKind::Insert(..)) => {
                        // It's impossible to insert a row with same RowID.
                        // As an existing row is updated indicates that the RowID
                        // is already inserted.
                        unreachable!()
                    }
                }
            }
        }
    }

    /// Merge redo logs from other.
    #[inline]
    pub fn merge(&mut self, other: TableDML) {
        for entry in other.rows.into_values() {
            self.insert(entry);
        }
    }
}

impl Ser<'_> for TableDML {
    #[inline]
    fn ser_len(&self, ctx: &SerdeCtx) -> usize {
        self.rows.ser_len(ctx)
    }

    #[inline]
    fn ser(&self, ctx: &SerdeCtx, out: &mut [u8], start_idx: usize) -> usize {
        self.rows.ser(ctx, out, start_idx)
    }
}

impl Deser for TableDML {
    #[inline]
    fn deser(ctx: &mut SerdeCtx, data: &[u8], start_idx: usize) -> Result<(usize, Self)> {
        let (idx, rows) = BTreeMap::<RowID, RowRedo>::deser(ctx, data, start_idx)?;
        Ok((idx, TableDML { rows }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redo_log_insert_update_delete() {
        let mut redo_logs = RedoLogs::default();

        // Test case 1: Simple insert
        let insert_entry = RowRedo {
            page_id: 1,
            row_id: 100,
            kind: RowRedoKind::Insert(vec![Val::Byte8(42)]),
        };
        redo_logs.insert_dml(1, insert_entry);
        assert_eq!(redo_logs.dml.len(), 1);
        let table = redo_logs.dml.get(&1).unwrap();
        assert_eq!(table.rows.len(), 1);

        // Test case 2: Update after insert
        let update_entry = RowRedo {
            page_id: 1,
            row_id: 100,
            kind: RowRedoKind::Update(vec![UpdateCol {
                idx: 0,
                val: Val::Byte8(43),
            }]),
        };
        redo_logs.insert_dml(1, update_entry);
        let table = redo_logs.dml.get(&1).unwrap();
        if let RowRedoKind::Insert(vals) = &table.rows.get(&100).unwrap().kind {
            assert_eq!(vals[0], Val::Byte8(43));
        } else {
            panic!("Expected Insert kind");
        }

        // Test case 3: Delete after update
        let delete_entry = RowRedo {
            page_id: 1,
            row_id: 100,
            kind: RowRedoKind::Delete,
        };
        redo_logs.insert_dml(1, delete_entry);
        let table = redo_logs.dml.get(&1).unwrap();
        assert_eq!(table.rows.len(), 0);

        // Test case 4: Multiple updates
        let insert_entry = RowRedo {
            page_id: 1,
            row_id: 200,
            kind: RowRedoKind::Insert(vec![Val::Byte8(1), Val::Byte8(2)]),
        };
        redo_logs.insert_dml(1, insert_entry);

        let update1 = RowRedo {
            page_id: 1,
            row_id: 200,
            kind: RowRedoKind::Update(vec![UpdateCol {
                idx: 0,
                val: Val::Byte8(3),
            }]),
        };
        redo_logs.insert_dml(1, update1);

        let update2 = RowRedo {
            page_id: 1,
            row_id: 200,
            kind: RowRedoKind::Update(vec![UpdateCol {
                idx: 1,
                val: Val::Byte8(4),
            }]),
        };
        redo_logs.insert_dml(1, update2);

        let table = redo_logs.dml.get(&1).unwrap();
        if let RowRedoKind::Insert(vals) = &table.rows.get(&200).unwrap().kind {
            assert_eq!(vals[0], Val::Byte8(3));
            assert_eq!(vals[1], Val::Byte8(4));
        } else {
            panic!("Expected Insert kind");
        }

        // Test case 5: Multiple tables
        let another_insert = RowRedo {
            page_id: 2,
            row_id: 300,
            kind: RowRedoKind::Insert(vec![Val::Byte8(50)]),
        };
        redo_logs.insert_dml(2, another_insert);
        assert_eq!(redo_logs.dml.len(), 2);
    }

    #[test]
    fn test_redo_log_merge() {
        // 创建第一个 RedoLogs
        let mut redo_logs1 = RedoLogs::default();

        // 创建第二个 RedoLogs
        let mut redo_logs2 = RedoLogs::default();

        // 测试用例1：合并不同表的日志
        let insert1 = RowRedo {
            page_id: 1,
            row_id: 100,
            kind: RowRedoKind::Insert(vec![Val::Byte8(42)]),
        };
        redo_logs1.insert_dml(1, insert1);

        let insert2 = RowRedo {
            page_id: 2,
            row_id: 200,
            kind: RowRedoKind::Insert(vec![Val::Byte8(43)]),
        };
        redo_logs2.insert_dml(2, insert2);

        redo_logs1.merge(redo_logs2);
        assert_eq!(redo_logs1.dml.len(), 2);
        assert!(redo_logs1.dml.contains_key(&1));
        assert!(redo_logs1.dml.contains_key(&2));

        // 测试用例2：合并相同表中的不同行
        let mut redo_logs2 = RedoLogs::default();
        let insert3 = RowRedo {
            page_id: 1,
            row_id: 101,
            kind: RowRedoKind::Insert(vec![Val::Byte8(44)]),
        };
        redo_logs2.insert_dml(1, insert3);

        redo_logs1.merge(redo_logs2);
        let table1 = redo_logs1.dml.get(&1).unwrap();
        assert_eq!(table1.rows.len(), 2);
        if let RowRedoKind::Insert(vals) = &table1.rows.get(&100).unwrap().kind {
            assert_eq!(vals[0], Val::Byte8(42));
        }
        if let RowRedoKind::Insert(vals) = &table1.rows.get(&101).unwrap().kind {
            assert_eq!(vals[0], Val::Byte8(44));
        }

        // 测试用例3：合并相同表相同行的操作
        let mut redo_logs2 = RedoLogs::default();
        let update1 = RowRedo {
            page_id: 1,
            row_id: 100,
            kind: RowRedoKind::Update(vec![UpdateCol {
                idx: 0,
                val: Val::Byte8(45),
            }]),
        };
        redo_logs2.insert_dml(1, update1);

        redo_logs1.merge(redo_logs2);
        let table1 = redo_logs1.dml.get(&1).unwrap();
        if let RowRedoKind::Insert(vals) = &table1.rows.get(&100).unwrap().kind {
            assert_eq!(vals[0], Val::Byte8(45));
        }

        // 测试用例4：合并空日志
        let empty_logs = RedoLogs::default();
        let tables_count = redo_logs1.dml.len();
        redo_logs1.merge(empty_logs);
        assert_eq!(redo_logs1.dml.len(), tables_count);

        // 测试用例5：删除操作的合并
        let mut redo_logs2 = RedoLogs::default();
        let delete1 = RowRedo {
            page_id: 1,
            row_id: 101,
            kind: RowRedoKind::Delete,
        };
        redo_logs2.insert_dml(1, delete1);

        redo_logs1.merge(redo_logs2);
        let table1 = redo_logs1.dml.get(&1).unwrap();
        assert!(!table1.rows.contains_key(&101));
    }

    #[test]
    fn test_table_dml_serde() {
        let mut ctx = SerdeCtx::default();

        // 创建测试数据
        let mut table_dml = TableDML::default();

        // 测试用例1：插入操作
        let insert_entry = RowRedo {
            page_id: 1,
            row_id: 100,
            kind: RowRedoKind::Insert(vec![Val::Byte8(42)]),
        };
        table_dml.insert(insert_entry);

        // 测试用例2：更新操作
        let update_entry = RowRedo {
            page_id: 1,
            row_id: 200,
            kind: RowRedoKind::Update(vec![UpdateCol {
                idx: 0,
                val: Val::Byte8(43),
            }]),
        };
        table_dml.insert(update_entry);

        // 测试用例3：删除操作
        let delete_entry = RowRedo {
            page_id: 1,
            row_id: 300,
            kind: RowRedoKind::Delete,
        };
        table_dml.insert(delete_entry);

        // 序列化
        let mut buf = vec![0; table_dml.ser_len(&ctx)];
        table_dml.ser(&ctx, &mut buf, 0);

        // 反序列化
        let (_, deserialized) = TableDML::deser(&mut ctx, &buf, 0).unwrap();

        // 验证结果
        assert_eq!(deserialized.rows.len(), 3);

        // 验证插入操作
        let insert_redo = deserialized.rows.get(&100).unwrap();
        assert_eq!(insert_redo.page_id, 1);
        assert_eq!(insert_redo.row_id, 100);
        match &insert_redo.kind {
            RowRedoKind::Insert(vals) => {
                assert_eq!(vals.len(), 1);
                assert_eq!(vals[0], Val::Byte8(42));
            }
            _ => panic!("Expected Insert kind"),
        }

        // 验证更新操作
        let update_redo = deserialized.rows.get(&200).unwrap();
        assert_eq!(update_redo.page_id, 1);
        assert_eq!(update_redo.row_id, 200);
        match &update_redo.kind {
            RowRedoKind::Update(cols) => {
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0].idx, 0);
                assert_eq!(cols[0].val, Val::Byte8(43));
            }
            _ => panic!("Expected Update kind"),
        }

        // 验证删除操作
        let delete_redo = deserialized.rows.get(&300).unwrap();
        assert_eq!(delete_redo.page_id, 1);
        assert_eq!(delete_redo.row_id, 300);
        match &delete_redo.kind {
            RowRedoKind::Delete => (),
            _ => panic!("Expected Delete kind"),
        }

        // 测试用例4：测试序列化位置偏移
        let mut buf = vec![0; 4 + table_dml.ser_len(&ctx)]; // 添加4字节前缀
        table_dml.ser(&ctx, &mut buf, 4); // 从位置4开始序列化

        // 验证反序列化结果
        let (_, deserialized) = TableDML::deser(&mut ctx, &buf, 4).unwrap();
        assert_eq!(deserialized.rows.len(), 3);

        // 测试用例5：空TableDML的序列化和反序列化
        let empty_table_dml = TableDML::default();
        let mut buf = vec![0; empty_table_dml.ser_len(&ctx)];
        empty_table_dml.ser(&ctx, &mut buf, 0);

        let (_, deserialized) = TableDML::deser(&mut ctx, &buf, 0).unwrap();
        assert_eq!(deserialized.rows.len(), 0);
    }

    #[test]
    fn test_ddl_redo_serde() {
        let mut ctx = SerdeCtx::default();

        // 测试用例1：CreateTable
        let create_table = DDLRedo::CreateTable(1);
        let mut buf = vec![0; create_table.ser_len(&ctx)];
        create_table.ser(&ctx, &mut buf, 0);

        // 验证序列化结果
        assert_eq!(buf[0], DDLRedoCode::CreateTable as u8);

        // 验证反序列化结果
        let (_, deserialized) = DDLRedo::deser(&mut ctx, &buf, 0).unwrap();
        match deserialized {
            DDLRedo::CreateTable(table_id) => {
                assert_eq!(table_id, 1);
            }
            _ => panic!("Expected CreateTable"),
        }

        // 测试用例2：DropTable
        let drop_table = DDLRedo::DropTable(2);
        let mut buf = vec![0; drop_table.ser_len(&ctx)];
        drop_table.ser(&ctx, &mut buf, 0);

        // 验证序列化结果
        assert_eq!(buf[0], DDLRedoCode::DropTable as u8);

        // 验证反序列化结果
        let (_, deserialized) = DDLRedo::deser(&mut ctx, &buf, 0).unwrap();
        match deserialized {
            DDLRedo::DropTable(table_id) => {
                assert_eq!(table_id, 2);
            }
            _ => panic!("Expected DropTable"),
        }

        // 测试用例3：CreateIndex
        let create_index = DDLRedo::CreateIndex(1);
        let mut buf = vec![0; create_index.ser_len(&ctx)];
        create_index.ser(&ctx, &mut buf, 0);

        // 验证序列化结果
        assert_eq!(buf[0], DDLRedoCode::CreateIndex as u8);

        // 验证反序列化结果
        let (_, deserialized) = DDLRedo::deser(&mut ctx, &buf, 0).unwrap();
        match deserialized {
            DDLRedo::CreateIndex(index_id) => {
                assert_eq!(index_id, 1);
            }
            _ => panic!("Expected CreateIndex"),
        }

        // 测试用例4：DropIndex
        let drop_index = DDLRedo::DropIndex(2);
        let mut buf = vec![0; drop_index.ser_len(&ctx)];
        drop_index.ser(&ctx, &mut buf, 0);

        // 验证序列化结果
        assert_eq!(buf[0], DDLRedoCode::DropIndex as u8);

        // 验证反序列化结果
        let (_, deserialized) = DDLRedo::deser(&mut ctx, &buf, 0).unwrap();
        match deserialized {
            DDLRedo::DropIndex(index_id) => {
                assert_eq!(index_id, 2);
            }
            _ => panic!("Expected DropIndex"),
        }

        // 测试用例5：测试序列化位置偏移
        let drop_table = DDLRedo::DropTable(5);
        let mut buf = vec![0; 4 + drop_table.ser_len(&ctx)]; // 添加4字节前缀
        drop_table.ser(&ctx, &mut buf, 4); // 从位置4开始序列化

        // 验证序列化结果
        assert_eq!(buf[4], DDLRedoCode::DropTable as u8);

        // 验证反序列化结果
        let (_, deserialized) = DDLRedo::deser(&mut ctx, &buf, 4).unwrap();
        match deserialized {
            DDLRedo::DropTable(table_id) => {
                assert_eq!(table_id, 5);
            }
            _ => panic!("Expected DropTable"),
        }
    }
}
