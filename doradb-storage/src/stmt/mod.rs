use crate::buffer::page::PageID;
use crate::buffer::BufferPool;
use crate::catalog::{row_id_spec, TableMetadata};
use crate::error::{Error, Result};
use crate::row::ops::{DeleteMvcc, InsertMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
use crate::row::RowID;
use crate::table::Table;
use crate::table::TableID;
use crate::trx::redo::DDLRedo;
use crate::trx::redo::RedoLogs;
use crate::trx::undo::{IndexUndoLogs, RowUndoKind, RowUndoLogs};
use crate::trx::ActiveTrx;
use crate::value::Val;
use doradb_catalog::{
    ColumnObject, IndexObject, IndexSpec, SchemaID, SchemaObject, TableObject, TableSpec,
};
use semistr::SemiStr;
use std::mem;

pub enum StmtKind {
    CreateSchema(SemiStr),
    CreateTable(SchemaID, TableSpec),
}

pub struct Statement<P: BufferPool> {
    // Transaction it belongs to.
    pub trx: ActiveTrx<P>,
    // statement-level undo logs of row data.
    pub row_undo: RowUndoLogs,
    // statement-level index undo operations.
    pub index_undo: IndexUndoLogs,
    // statement-level redo logs.
    pub redo: RedoLogs,
}

impl<P: BufferPool> Statement<P> {
    /// Create a new statement.
    #[inline]
    pub fn new(trx: ActiveTrx<P>) -> Self {
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
    pub fn succeed(mut self) -> ActiveTrx<P> {
        self.trx.row_undo.merge(&mut self.row_undo);
        self.trx.index_undo.merge(&mut self.index_undo);
        self.trx.redo.merge(mem::take(&mut self.redo));
        self.trx
    }

    /// Fail current statement and return transaction it belongs to.
    /// This will trigger statement-level rollback based on its undo.
    /// Redo logs will be discarded.
    #[inline]
    pub async fn fail(mut self) -> ActiveTrx<P> {
        // rollback row data.
        // todo: group by page level may be better.
        let engine = self.trx.engine().unwrap();
        self.row_undo.rollback(&engine.buf_pool).await;
        // rollback index data.
        self.index_undo.rollback(&engine.catalog);
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
        let engine = self.trx.engine().unwrap();
        // Check if schema exists
        if engine
            .catalog
            .storage
            .schemas
            .find_uncommitted_by_name(&engine.buf_pool, schema_name)
            .await
            .is_some()
        {
            return Err(Error::SchemaAlreadyExists);
        }

        // Prepare schema object
        let schema_id = engine.catalog.next_obj_id();
        let schema_object = SchemaObject {
            schema_id,
            schema_name: SemiStr::new(schema_name),
        };

        let mut schema_cache_g = engine.catalog.cache.schemas.write();

        let inserted = engine
            .catalog
            .storage
            .schemas
            .insert(&engine.buf_pool, self, &schema_object)
            .await;
        if !inserted {
            return Err(Error::SchemaAlreadyExists);
        }

        schema_cache_g.insert(schema_id, schema_object);

        // Finally add DDL redo log to redo log buffer
        self.redo.ddl.push(DDLRedo::CreateSchema(schema_id));

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
        let engine = self.trx.engine().unwrap();
        // Check if schema exists
        if engine
            .catalog
            .storage
            .schemas
            .find_uncommitted_by_id(&engine.buf_pool, schema_id)
            .await
            .is_none()
        {
            return Err(Error::SchemaNotFound);
        }

        // Check if table exists
        if engine
            .catalog
            .storage
            .tables
            .find_uncommitted_by_name(&engine.buf_pool, schema_id, &table_spec.table_name)
            .await
            .is_some()
        {
            return Err(Error::TableAlreadyExists);
        }

        // Prepare table object
        let table_id = engine.catalog.next_obj_id();
        let table_object = TableObject {
            table_id,
            table_name: table_spec.table_name.clone(),
            schema_id,
        };
        let row_id_spec = row_id_spec();

        // Prepare column objects
        let column_objects: Vec<_> = std::iter::once(&row_id_spec)
            .chain(table_spec.columns.iter())
            .enumerate()
            .map(|(col_no, col_spec)| ColumnObject {
                column_id: engine.catalog.next_obj_id(),
                column_name: col_spec.column_name.clone(),
                table_id,
                column_no: col_no as u16,
                column_type: col_spec.column_type.clone(),
                column_attributes: col_spec.column_attributes,
            })
            .collect();

        // Prepare index objects
        let index_objects: Vec<_> = index_specs
            .iter()
            .map(|index_spec| IndexObject {
                index_id: engine.catalog.next_obj_id(),
                table_id,
                index_name: index_spec.index_name.clone(),
                index_attributes: index_spec.index_attributes,
            })
            .collect();

        let mut table_cache_g = engine.catalog.cache.tables.write();

        // Insert table object, column objects, index objects
        let inserted = engine
            .catalog
            .storage
            .tables
            .insert(&engine.buf_pool, self, &table_object)
            .await;
        if !inserted {
            return Err(Error::TableAlreadyExists);
        }

        for column_object in column_objects {
            let inserted = engine
                .catalog
                .storage
                .columns
                .insert(&engine.buf_pool, self, &column_object)
                .await;
            debug_assert!(inserted);
        }
        for index_object in index_objects {
            let inserted = engine
                .catalog
                .storage
                .indexes
                .insert(&engine.buf_pool, self, &index_object)
                .await;
            debug_assert!(inserted);
        }

        // Prepare in-memory representation of new table
        let table_metadata = TableMetadata::new(table_spec.columns, index_specs);
        let table = Table::new(&engine.buf_pool, table_id, table_metadata).await;
        let res = table_cache_g.insert(table_id, table);
        debug_assert!(res.is_none());

        // Finally add DDL redo log to redo log buffer
        self.redo.ddl.push(DDLRedo::CreateTable(table_id));

        Ok(table_id)
    }

    /// Insert a row into a table.
    #[inline]
    pub async fn insert_row(&mut self, table: &Table<P>, cols: Vec<Val>) -> InsertMvcc {
        let engine = self.trx.engine().unwrap();
        table.insert_row(&engine.buf_pool, self, cols).await
    }

    #[inline]
    pub async fn delete_row(&mut self, table: &Table<P>, key: &SelectKey) -> DeleteMvcc {
        let engine = self.trx.engine().unwrap();
        table.delete_row(&engine.buf_pool, self, key).await
    }

    #[inline]
    pub async fn select_row_mvcc(
        &self,
        table: &Table<P>,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> SelectMvcc {
        let engine = self.trx.engine().unwrap();
        table
            .select_row_mvcc(&engine.buf_pool, self, key, user_read_set)
            .await
    }

    #[inline]
    pub async fn update_row(
        &mut self,
        table: &Table<P>,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> UpdateMvcc {
        let engine = self.trx.engine().unwrap();
        table.update_row(&engine.buf_pool, self, key, update).await
    }
}
