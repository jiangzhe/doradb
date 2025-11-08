use crate::bitmap::AllocMap;
use crate::catalog::table::TableMetadata;
use crate::error::Result;
use crate::io::file::SparseFile;
use crate::io::AIOManager;
use crate::trx::TrxID;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

/// Default page size of table file is 256KB.
pub const DEFAULT_TABLE_FILE_PAGE_SIZE: usize = 256 * 1024;

pub struct TableFile {
    // AIO Manager for IO operations.
    aio_mgr: Arc<AIOManager>,
    /// Underlying sparse file storing table metadata and data.
    file: SparseFile,
    /// Active root of this table file.
    /// This root can be switched once a modification of
    /// the file is committed.
    active_root: AtomicPtr<TableActiveRoot>,
}

impl TableFile {
    /// Create a table file.
    #[inline]
    pub fn new(
        aio_mgr: Arc<AIOManager>,
        file_path: impl AsRef<str>,
        initial_size: usize,
        trx_id: TrxID,
        metadata: TableMetadata,
    ) -> Result<Self> {
        debug_assert!(initial_size.is_multiple_of(DEFAULT_TABLE_FILE_PAGE_SIZE));
        let file = aio_mgr.create_sparse_file(file_path, initial_size)?;
        let initial_pages = initial_size / DEFAULT_TABLE_FILE_PAGE_SIZE;
        let alloc_map = AllocMap::new(initial_pages);
        // allocate page 0 as super page.
        let super_page_allocated = alloc_map.allocate_at(0);
        assert!(super_page_allocated);
        // construct active root.
        let active_root = Box::new(TableActiveRoot {
            page_no: 0,
            trx_id,
            alloc_map,
            metadata,
        });
        Ok(TableFile {
            aio_mgr,
            file,
            active_root: AtomicPtr::new(Box::leak(active_root)),
        })
    }

    /// Returns active root of the table file.
    #[inline]
    pub fn active_root(&self) -> &TableActiveRoot {
        let ptr = self.active_root.load(Ordering::Relaxed);
        unsafe { &*ptr }
    }
}

impl Drop for TableFile {
    #[inline]
    fn drop(&mut self) {
        self.aio_mgr.deregister_fd(self.file.as_raw_fd());
        let active_root_ptr = self.active_root.load(Ordering::Relaxed);
        unsafe {
            drop(Box::from_raw(active_root_ptr));
        }
    }
}

pub struct TableActiveRoot {
    /// root page number.
    page_no: usize,
    /// Version of this table file.
    trx_id: TrxID,
    /// Page allocation map.
    alloc_map: AllocMap,
    /// Metadata of this table.
    metadata: TableMetadata,
    // column index (todo)
    // statistics (todo)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::AIOManagerConfig;
    use doradb_catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
    use doradb_datatype::PreciseType;

    #[test]
    fn test_table_file_create() {
        let metadata = TableMetadata::new(
            vec![
                ColumnSpec::new("c0", PreciseType::Int(4, true), ColumnAttributes::empty()),
                ColumnSpec::new("c1", PreciseType::Int(8, true), ColumnAttributes::NULLABLE),
            ],
            vec![IndexSpec::new(
                "idx1",
                vec![IndexKey::new(0)],
                IndexAttributes::PK,
            )],
        );
        let aio_mgr = AIOManagerConfig::default().max_events(16).build().unwrap();
        let aio_mgr = Arc::new(aio_mgr);
        let table_file =
            TableFile::new(aio_mgr, "table_file1.tbl", 1024 * 1024 * 256, 1, metadata).unwrap();

        let active_root = table_file.active_root();
        assert_eq!(active_root.page_no, 0);
        assert_eq!(active_root.trx_id, 1);
        drop(table_file);
        let _ = std::fs::remove_file("table_file1.txt");
    }
}
