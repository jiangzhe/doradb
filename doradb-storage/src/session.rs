use crate::buffer::page::PageID;
use crate::catalog::{
    ColumnObject, IndexColumnObject, IndexObject, SchemaObject, TableMetadata, TableObject,
};
use crate::engine::EngineRef;
use crate::error::{Error, Result};
use crate::index::BlockIndex;
use crate::row::RowID;
use crate::table::Table;
use crate::table::TableID;
use crate::trx::redo::DDLRedo;
use crate::trx::{ActiveTrx, TrxID};
use doradb_catalog::{IndexSpec, SchemaID, TableSpec};
use parking_lot::Mutex;
use semistr::SemiStr;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub struct Session {
    state: Arc<SessionState>,
}

impl Session {
    #[inline]
    pub(crate) fn new(engine_ref: EngineRef) -> Self {
        Session {
            state: Arc::new(SessionState::new(engine_ref)),
        }
    }

    #[inline]
    pub fn engine(&self) -> &EngineRef {
        &self.state.engine_ref
    }

    #[inline]
    pub fn in_trx(&self) -> bool {
        self.state.in_trx.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn load_active_insert_page(&mut self, table_id: TableID) -> Option<(PageID, RowID)> {
        self.state.load_active_insert_page(table_id)
    }

    #[inline]
    pub fn save_active_insert_page(&mut self, table_id: TableID, page_id: PageID, row_id: RowID) {
        self.state
            .save_active_insert_page(table_id, page_id, row_id);
    }

    #[inline]
    pub fn begin_trx(&mut self) -> Option<ActiveTrx> {
        if self.state.in_trx.load(Ordering::Relaxed) {
            return None;
        }
        let trx = self
            .state
            .engine_ref
            .trx_sys
            .begin_trx(Arc::clone(&self.state));
        Some(trx)
    }

    /// Create a new schema.
    /// This is a session level method, because
    /// we automatically start a new transaction and
    /// commit once the creation is done.
    #[inline]
    pub async fn create_schema(
        &mut self,
        schema_name: &str,
        if_not_exists: bool,
    ) -> Result<SchemaID> {
        if self.in_trx() {
            return Err(Error::NotSupported("implicit commit due to DDL"));
        }
        let engine = self.engine().clone();
        // 1. Check if schema exists
        if let Some(schema) = engine
            .catalog()
            .storage
            .schemas()
            .find_uncommitted_by_name(schema_name)
            .await
        {
            if if_not_exists {
                return Ok(schema.schema_id);
            }
            return Err(Error::SchemaAlreadyExists);
        }

        // 2. Prepare schema object
        let schema_id = engine.catalog().next_obj_id();
        let schema_object = SchemaObject {
            schema_id,
            schema_name: SemiStr::new(schema_name),
        };

        // 3. Start transaction.
        let mut stmt = self.begin_trx().unwrap().start_stmt();

        // 4. lock and insert catalog
        let mut schema_cache_g = engine.catalog().cache.schemas.write().await;

        let inserted = engine
            .catalog()
            .storage
            .schemas()
            .insert(&mut stmt, &schema_object)
            .await;
        if !inserted {
            stmt.fail().await.rollback().await;
            return Err(Error::SchemaAlreadyExists);
        }

        schema_cache_g.insert(schema_id, schema_object);

        drop(schema_cache_g);

        // 5. Finally add DDL redo log to redo log buffer
        let res = stmt
            .redo
            .ddl
            .replace(Box::new(DDLRedo::CreateSchema(schema_id)));
        debug_assert!(res.is_none());

        // 6. Auto commit the transaction.
        stmt.succeed().commit().await?;
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
        if self.in_trx() {
            return Err(Error::NotSupported("implicit commit due to DDL"));
        }

        let engine = self.state.engine().clone();
        // 1. Check if schema exists
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

        // 2. Check if table exists
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

        // 3. Prepare a new table file.
        //    Use <table-id>.tbl as table name
        let table_id = engine.catalog().next_obj_id();

        let metadata = TableMetadata::new(table_spec.columns.clone(), index_specs.clone());
        let uninit_table_file = engine
            .table_fs
            .create_table_file(table_id, metadata, false)?;

        // 4. Prepare catalog related object
        let table_object = TableObject {
            table_id,
            table_name: table_spec.table_name.clone(),
            schema_id,
        };

        let column_objects: Vec<_> = table_spec
            .columns
            .iter()
            .enumerate()
            .map(|(col_no, col_spec)| ColumnObject {
                column_id: engine.catalog().next_obj_id(),
                column_name: col_spec.column_name.clone(),
                table_id,
                column_no: col_no as u16,
                column_type: col_spec.column_type,
                column_attributes: col_spec.column_attributes,
            })
            .collect();

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

        // 5. begin transaction
        let mut stmt = self.begin_trx().unwrap().start_stmt();

        // 6. lock metadata.
        let mut table_cache_g = engine.catalog().cache.tables.write().await;

        // 7. insert catalog related objects.
        let inserted = engine
            .catalog()
            .storage
            .tables()
            .insert(&mut stmt, &table_object)
            .await;
        if !inserted {
            uninit_table_file.try_delete();
            stmt.fail().await.rollback().await;
            return Err(Error::TableAlreadyExists);
        }

        for column_object in column_objects {
            let inserted = engine
                .catalog()
                .storage
                .columns()
                .insert(&mut stmt, &column_object)
                .await;
            debug_assert!(inserted);
        }
        for index_object in index_objects {
            let inserted = engine
                .catalog()
                .storage
                .indexes()
                .insert(&mut stmt, &index_object)
                .await;
            debug_assert!(inserted);
        }
        for index_column_object in index_column_objects {
            let inserted = engine
                .catalog()
                .storage
                .index_columns()
                .insert(&mut stmt, &index_column_object)
                .await;
            debug_assert!(inserted);
        }

        // 7. add DDL redo log to redo log buffer
        let res = stmt
            .redo
            .ddl
            .replace(Box::new(DDLRedo::CreateTable(table_id)));
        debug_assert!(res.is_none());

        // 8. commit current transaction implicitly.
        let cts = match stmt.succeed().commit().await {
            Ok(cts) => cts,
            Err(e) => {
                uninit_table_file.try_delete();
                return Err(e);
            }
        };

        // 9. commit file with cts.
        let (table_file, old_root) = uninit_table_file.commit(cts, true).await?;
        debug_assert!(old_root.is_none());

        // 10. Prepare in-memory representation of new table
        let blk_idx = BlockIndex::new(
            engine.meta_pool,
            table_id,
            table_file.active_root().row_id_bound,
            table_file.active_root_ptr(),
        )
        .await;
        let table = Table::new(engine.index_pool, blk_idx, table_file).await;
        // Enable page committer so all row pages can be recovered.
        table.blk_idx.enable_page_committer(engine.trx_sys);

        let res = table_cache_g.insert(table_id, table);
        debug_assert!(res.is_none());

        Ok(table_id)
    }
}

pub struct SessionState {
    engine_ref: EngineRef,
    in_trx: AtomicBool,
    last_cts: AtomicU64,
    active_insert_pages: Mutex<HashMap<TableID, (PageID, RowID)>>,
}

impl SessionState {
    #[inline]
    pub fn new(engine_ref: EngineRef) -> Self {
        SessionState {
            engine_ref,
            in_trx: AtomicBool::new(false),
            last_cts: AtomicU64::new(0),
            active_insert_pages: Mutex::new(HashMap::new()),
        }
    }

    #[inline]
    pub fn engine(&self) -> &EngineRef {
        &self.engine_ref
    }

    #[inline]
    pub fn last_cts(&self) -> Option<TrxID> {
        let trx_id = self.last_cts.load(Ordering::Relaxed);
        if trx_id == 0 {
            return None;
        }
        Some(trx_id)
    }

    #[inline]
    pub fn commit(&self, cts: TrxID) {
        self.last_cts.store(cts, Ordering::SeqCst);
        self.in_trx.store(false, Ordering::SeqCst);
    }

    #[inline]
    pub fn rollback(&self) {
        self.in_trx.store(false, Ordering::SeqCst);
    }

    #[inline]
    pub fn load_active_insert_page(&self, table_id: TableID) -> Option<(PageID, RowID)> {
        let mut g = self.active_insert_pages.lock();
        g.remove(&table_id)
    }

    #[inline]
    pub fn save_active_insert_page(&self, table_id: TableID, page_id: PageID, row_id: RowID) {
        let mut g = self.active_insert_pages.lock();
        let res = g.insert(table_id, (page_id, row_id));
        debug_assert!(res.is_none());
    }
}
