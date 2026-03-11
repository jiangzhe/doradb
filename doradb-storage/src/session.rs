use crate::buffer::page::PageID;
use crate::catalog::{ColumnObject, IndexColumnObject, IndexObject, TableMetadata, TableObject};
use crate::catalog::{IndexSpec, TableID, TableSpec};
use crate::engine::EngineRef;
use crate::error::{Error, Result};
use crate::index::BlockIndex;
use crate::row::RowID;
use crate::table::Table;
use crate::trx::redo::DDLRedo;
use crate::trx::{ActiveTrx, TrxID};
use parking_lot::Mutex;
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

    /// Create a new table.
    #[inline]
    pub async fn create_table(
        &mut self,
        table_spec: TableSpec,
        index_specs: Vec<IndexSpec>,
    ) -> Result<TableID> {
        if self.in_trx() {
            return Err(Error::NotSupported("implicit commit due to DDL"));
        }

        let engine = self.state.engine().clone();
        // 1. Prepare a new table file.
        //    User table file name is <table-id:016x>.tbl
        let table_id = engine.catalog().next_user_obj_id();

        let metadata = Arc::new(TableMetadata::new(
            table_spec.columns.clone(),
            index_specs.clone(),
        ));
        let uninit_table_file = engine
            .table_fs
            .create_table_file(table_id, metadata, false)?;

        // 2. Prepare catalog related object
        let table_object = TableObject { table_id };

        let column_objects: Vec<_> = table_spec
            .columns
            .iter()
            .enumerate()
            .map(|(col_no, col_spec)| ColumnObject {
                table_id,
                column_no: col_no as u16,
                column_name: col_spec.column_name.clone(),
                column_type: col_spec.column_type,
                column_attributes: col_spec.column_attributes,
            })
            .collect();

        let mut index_objects = vec![];
        let mut index_column_objects = vec![];

        for (index_no, index_spec) in index_specs.iter().enumerate() {
            index_objects.push(IndexObject {
                table_id,
                index_no: index_no as u16,
                index_name: index_spec.index_name.clone(),
                index_attributes: index_spec.index_attributes,
            });
            for (index_column_no, ik) in index_spec.index_cols.iter().enumerate() {
                index_column_objects.push(IndexColumnObject {
                    table_id,
                    index_no: index_no as u16,
                    index_column_no: index_column_no as u16,
                    column_no: ik.col_no,
                    index_order: ik.order,
                });
            }
        }

        // 3. begin transaction
        let mut stmt = self.begin_trx().unwrap().start_stmt();

        // 4. insert catalog related objects.
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

        // 5. add DDL redo log to redo log buffer
        let res = stmt
            .redo
            .ddl
            .replace(Box::new(DDLRedo::CreateTable(table_id)));
        debug_assert!(res.is_none());

        // 6. commit current transaction implicitly.
        let cts = match stmt.succeed().commit().await {
            Ok(cts) => cts,
            Err(e) => {
                uninit_table_file.try_delete();
                return Err(e);
            }
        };

        // 7. commit file with cts.
        let (table_file, old_root) = uninit_table_file.commit(cts, true).await?;
        debug_assert!(old_root.is_none());

        // 8. Prepare in-memory representation of new table
        let blk_idx = BlockIndex::new(
            engine.meta_pool,
            table_file.active_root().pivot_row_id,
            table_file.active_root().column_block_index_root,
        )
        .await;
        let table = Arc::new(
            Table::new(
                engine.mem_pool,
                engine.index_pool,
                engine.disk_pool,
                table_id,
                blk_idx,
                table_file,
            )
            .await,
        );
        // Enable page committer so all row pages can be recovered.
        table.enable_page_committer(engine.trx_sys);

        engine.catalog().insert_user_table(table);

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
