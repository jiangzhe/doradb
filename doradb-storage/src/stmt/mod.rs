use crate::buffer::page::PageID;
use crate::catalog::storage::{
    ColumnObject, IndexColumnObject, IndexObject, SchemaObject, TableObject,
};
use crate::catalog::TableMetadata;
use crate::error::{Error, Result};
use crate::index::BlockIndex;
use crate::row::ops::{DeleteMvcc, InsertMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
use crate::row::RowID;
use crate::table::Table;
use crate::table::TableID;
use crate::trx::redo::DDLRedo;
use crate::trx::redo::RedoLogs;
use crate::trx::undo::{IndexUndoLogs, RowUndoKind, RowUndoLogs};
use crate::trx::ActiveTrx;
use crate::value::Val;
use doradb_catalog::{IndexSpec, SchemaID, TableSpec};
use semistr::SemiStr;
use std::mem;

pub enum StmtKind {
    CreateSchema(SemiStr),
    CreateTable(SchemaID, TableSpec),
}

pub struct Statement {
    // Transaction it belongs to.
    pub trx: ActiveTrx,
    // statement-level undo logs of row data.
    pub row_undo: RowUndoLogs,
    // statement-level index undo operations.
    pub index_undo: IndexUndoLogs,
    // statement-level redo logs.
    pub redo: RedoLogs,
}

impl Statement {
    /// Create a new statement.
    #[inline]
    pub fn new(trx: ActiveTrx) -> Self {
        Statement {
            trx,
            row_undo: RowUndoLogs::empty(),
            index_undo: IndexUndoLogs::empty(),
            redo: RedoLogs::default(),
        }
    }

    #[inline]
    pub fn update_last_undo(&mut self, kind: RowUndoKind) {
        let last_undo = self.row_undo.last_mut().unwrap();
        // Currently the update can only be applied on LOCK entry.
        debug_assert!(matches!(last_undo.kind, RowUndoKind::Lock));
        last_undo.kind = kind;
    }

    /// Succeed current statement and return transaction it belongs to.
    /// All undo and redo logs it holds will be merged into transaction buffer.
    #[inline]
    pub fn succeed(mut self) -> ActiveTrx {
        self.trx.row_undo.merge(&mut self.row_undo);
        self.trx.index_undo.merge(&mut self.index_undo);
        self.trx.redo.merge(mem::take(&mut self.redo));
        self.trx
    }

    /// Fail current statement and return transaction it belongs to.
    /// This will trigger statement-level rollback based on its undo.
    /// Redo logs will be discarded.
    #[inline]
    pub async fn fail(mut self) -> ActiveTrx {
        // rollback row data.
        // todo: group by page level may be better.
        let engine = self.trx.engine_weak().unwrap();
        self.row_undo.rollback(engine.data_pool).await;
        // rollback index data.
        self.index_undo
            .rollback(engine.data_pool, engine.catalog())
            .await;
        // clear redo logs.
        self.redo.clear();
        self.trx
    }

    #[inline]
    pub fn load_active_insert_page(&mut self, table_id: TableID) -> Option<(PageID, RowID)> {
        self.trx
            .session
            .as_mut()
            .and_then(|session| session.load_active_insert_page(table_id))
    }

    #[inline]
    pub fn save_active_insert_page(&mut self, table_id: TableID, page_id: PageID, row_id: RowID) {
        if let Some(session) = self.trx.session.as_mut() {
            session.save_active_insert_page(table_id, page_id, row_id);
        }
    }

    /// Create a new schema.
    #[inline]
    pub async fn create_schema(&mut self, schema_name: &str) -> Result<SchemaID> {
        let engine = self.trx.engine_weak().unwrap();
        // Check if schema exists
        if engine
            .catalog()
            .storage
            .schemas()
            .find_uncommitted_by_name(schema_name)
            .await
            .is_some()
        {
            return Err(Error::SchemaAlreadyExists);
        }

        // Prepare schema object
        let schema_id = engine.catalog().next_obj_id();
        let schema_object = SchemaObject {
            schema_id,
            schema_name: SemiStr::new(schema_name),
        };

        let mut schema_cache_g = engine.catalog().cache.schemas.write();

        let inserted = engine
            .catalog()
            .storage
            .schemas()
            .insert(self, &schema_object)
            .await;
        if !inserted {
            return Err(Error::SchemaAlreadyExists);
        }

        schema_cache_g.insert(schema_id, schema_object);

        // Finally add DDL redo log to redo log buffer
        let res = self
            .redo
            .ddl
            .replace(Box::new(DDLRedo::CreateSchema(schema_id)));
        debug_assert!(res.is_none());

        Ok(schema_id)
    }

    /// Create a new table.
    #[inline]
    pub async fn create_table(
        &mut self,
        schema_id: SchemaID,
        table_spec: TableSpec,
        index_specs: Vec<IndexSpec>,
    ) -> Result<TableID> {
        let engine = self.trx.engine_weak().unwrap();
        // Check if schema exists
        if engine
            .catalog()
            .storage
            .schemas()
            .find_uncommitted_by_id(schema_id)
            .await
            .is_none()
        {
            return Err(Error::SchemaNotFound);
        }

        // Check if table exists
        if engine
            .catalog()
            .storage
            .tables()
            .find_uncommitted_by_name(schema_id, &table_spec.table_name)
            .await
            .is_some()
        {
            return Err(Error::TableAlreadyExists);
        }

        // Prepare table object
        let table_id = engine.catalog().next_obj_id();
        let table_object = TableObject {
            table_id,
            table_name: table_spec.table_name.clone(),
            schema_id,
        };

        // Prepare column objects
        let column_objects: Vec<_> = table_spec
            .columns
            .iter()
            .enumerate()
            .map(|(col_no, col_spec)| ColumnObject {
                column_id: engine.catalog().next_obj_id(),
                column_name: col_spec.column_name.clone(),
                table_id,
                column_no: col_no as u16,
                column_type: col_spec.column_type.clone(),
                column_attributes: col_spec.column_attributes,
            })
            .collect();

        // Prepare index objects and index column objects
        let mut index_objects = vec![];
        let mut index_column_objects = vec![];

        for index_spec in &index_specs {
            let index_id = engine.catalog().next_obj_id();
            index_objects.push(IndexObject {
                index_id,
                table_id,
                index_name: index_spec.index_name.clone(),
                index_attributes: index_spec.index_attributes,
            });
            for (index_column_no, ik) in index_spec.index_cols.iter().enumerate() {
                let column = &column_objects[ik.col_no as usize];
                index_column_objects.push(IndexColumnObject {
                    column_id: column.column_id,
                    index_id,
                    column_no: column.column_no,
                    index_column_no: index_column_no as u16,
                    index_order: ik.order,
                });
            }
        }

        let mut table_cache_g = engine.catalog().cache.tables.write();

        // Insert table object, column objects, index objects, index column objects.

        let inserted = engine
            .catalog()
            .storage
            .tables()
            .insert(self, &table_object)
            .await;
        if !inserted {
            return Err(Error::TableAlreadyExists);
        }

        for column_object in column_objects {
            let inserted = engine
                .catalog()
                .storage
                .columns()
                .insert(self, &column_object)
                .await;
            debug_assert!(inserted);
        }
        for index_object in index_objects {
            let inserted = engine
                .catalog()
                .storage
                .indexes()
                .insert(self, &index_object)
                .await;
            debug_assert!(inserted);
        }
        for index_column_object in index_column_objects {
            let inserted = engine
                .catalog()
                .storage
                .index_columns()
                .insert(self, &index_column_object)
                .await;
            debug_assert!(inserted);
        }

        // Prepare in-memory representation of new table
        let table_metadata = TableMetadata::new(table_spec.columns, index_specs);
        let blk_idx = BlockIndex::new(engine.meta_pool, table_id).await;
        let table = Table::new(blk_idx, table_metadata);
        // Enable page committer so all row pages can be recovered.
        table
            .blk_idx
            .enable_page_committer(self.trx.engine_weak().unwrap().trx_sys);

        let res = table_cache_g.insert(table_id, table);
        debug_assert!(res.is_none());

        // Finally add DDL redo log to redo log buffer
        let res = self
            .redo
            .ddl
            .replace(Box::new(DDLRedo::CreateTable(table_id)));
        debug_assert!(res.is_none());

        Ok(table_id)
    }

    /// Insert a row into a table.
    #[inline]
    pub async fn insert_row(&mut self, table: &Table, cols: Vec<Val>) -> InsertMvcc {
        let engine = self.trx.engine_weak().unwrap();
        table.insert_row(engine.data_pool, self, cols).await
    }

    #[inline]
    pub async fn delete_row(&mut self, table: &Table, key: &SelectKey) -> DeleteMvcc {
        let engine = self.trx.engine_weak().unwrap();
        table.delete_row(engine.data_pool, self, key, false).await
    }

    #[inline]
    pub async fn select_row_mvcc(
        &self,
        table: &Table,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> SelectMvcc {
        let engine = self.trx.engine_weak().unwrap();
        table
            .select_row_mvcc(engine.data_pool, self, key, user_read_set)
            .await
    }

    #[inline]
    pub async fn update_row(
        &mut self,
        table: &Table,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> UpdateMvcc {
        let engine = self.trx.engine_weak().unwrap();
        table
            .update_row(engine.data_pool, self, key, update, false)
            .await
    }
}
