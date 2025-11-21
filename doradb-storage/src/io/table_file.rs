use crate::bitmap::AllocMap;
use crate::catalog::table::TableMetadata;
use crate::error::Result;
use crate::io::file::SparseFile;
use crate::trx::TrxID;
use std::sync::atomic::{AtomicPtr, Ordering};

/// Default page size of table file is 256KB.
pub const DEFAULT_TABLE_FILE_PAGE_SIZE: usize = 256 * 1024;

/// Table file which contains metadata and data of single table.
pub struct TableFile {
    /// Underlying sparse file storing table metadata and data.
    file: SparseFile,
    /// Active root of this table file.
    /// This root can be switched once a modification of
    /// the file is committed.
    active_root: AtomicPtr<ActiveRoot>,
}

impl TableFile {
    /// Create a table file.
    #[inline]
    pub fn new(
        file_path: impl AsRef<str>,
        initial_size: usize,
        trx_id: TrxID,
        metadata: TableMetadata,
    ) -> Result<Self> {
        debug_assert!(initial_size.is_multiple_of(DEFAULT_TABLE_FILE_PAGE_SIZE));
        let file = SparseFile::create(file_path, initial_size)?;
        let initial_pages = initial_size / DEFAULT_TABLE_FILE_PAGE_SIZE;
        // construct active root.
        let active_root = Box::new(ActiveRoot::new(trx_id, initial_pages, metadata));
        Ok(TableFile {
            file,
            active_root: AtomicPtr::new(Box::leak(active_root)),
        })
    }

    /// Returns active root of the table file.
    #[inline]
    pub fn active_root(&self) -> &ActiveRoot {
        let ptr = self.active_root.load(Ordering::Relaxed);
        unsafe { &*ptr }
    }
}

impl Drop for TableFile {
    #[inline]
    fn drop(&mut self) {
        let active_root_ptr = self.active_root.load(Ordering::Relaxed);
        unsafe {
            drop(Box::from_raw(active_root_ptr));
        }
    }
}

/// Active root of table file.
/// It contains bitmap of allocated pages,
/// table schema(metadata), column index and statistics.
#[derive(Clone)]
pub struct ActiveRoot {
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

impl ActiveRoot {
    /// Create a new active root.
    /// Page number is set to zero(the first page of this file)
    #[inline]
    pub fn new(trx_id: TrxID, max_pages: usize, metadata: TableMetadata) -> Self {
        const DEFALT_ROOT_PAGE_NO: usize = 0;

        let alloc_map = AllocMap::new(max_pages);
        let super_page_allocated = alloc_map.allocate_at(DEFALT_ROOT_PAGE_NO);
        assert!(super_page_allocated);

        ActiveRoot {
            page_no: DEFALT_ROOT_PAGE_NO,
            trx_id,
            alloc_map,
            metadata,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let table_file = TableFile::new("table_file1.tbl", 1024 * 1024 * 256, 1, metadata).unwrap();

        let active_root = table_file.active_root();
        assert_eq!(active_root.page_no, 0);
        assert_eq!(active_root.trx_id, 1);
        drop(table_file);
        let _ = std::fs::remove_file("table_file1.tbl");
    }
}
