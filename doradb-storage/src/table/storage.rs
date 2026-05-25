use super::ColumnDeletionBuffer;
use crate::buffer::ReadonlyBufferPool;
use crate::error::{DataIntegrityError, Result};
use crate::file::table_file::{ActiveRoot, TableFile};
use crate::index::SecondaryDiskTreeRuntime;
use crate::quiescent::QuiescentGuard;
use crate::trx::TrxReadProof;
use error_stack::Report;
use std::sync::Arc;

/// Persisted column-store attachments associated with a user table runtime.
pub(crate) struct ColumnStorage {
    pub(crate) file: Arc<TableFile>,
    pub(crate) disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    pub(crate) deletion_buffer: ColumnDeletionBuffer,
    secondary_indexes: Box<[Option<SecondaryDiskTreeRuntime>]>,
}

impl ColumnStorage {
    #[inline]
    pub(crate) fn new(
        file: Arc<TableFile>,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    ) -> Result<Self> {
        // `catalog_load_boundary`: table construction binds the loaded root to
        // initialize column storage and validate secondary root layout.
        let active_root = file.active_root_unchecked();
        let metadata = Arc::clone(&active_root.metadata);
        if active_root.secondary_index_roots.len() != metadata.idx.index_slot_count() {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach(format!(
                    "secondary root count mismatch: root_count={}, index_slot_count={}",
                    active_root.secondary_index_roots.len(),
                    metadata.idx.index_slot_count()
                ))
                .into());
        }
        let mut secondary_indexes = Vec::with_capacity(metadata.idx.index_slot_count());
        secondary_indexes.resize_with(metadata.idx.index_slot_count(), || None);
        for (index_no, _) in metadata.idx.active_indexes() {
            secondary_indexes[index_no] = Some(SecondaryDiskTreeRuntime::new(
                index_no,
                Arc::clone(&metadata),
                Arc::clone(&file),
                disk_pool.clone(),
            )?);
        }
        let secondary_indexes = secondary_indexes.into_boxed_slice();
        Ok(ColumnStorage {
            file,
            disk_pool,
            deletion_buffer: ColumnDeletionBuffer::new(),
            secondary_indexes,
        })
    }

    /// Returns the underlying table file for persisted column data.
    #[inline]
    pub(crate) fn file(&self) -> &Arc<TableFile> {
        &self.file
    }

    /// Bind one active root observation under a transaction read proof.
    #[inline]
    pub(crate) fn with_active_root<'ctx, R, F>(&self, _proof: &TrxReadProof<'ctx>, f: F) -> R
    where
        F: for<'root> FnOnce(&'root ActiveRoot) -> R,
    {
        let root = self.file().active_root_unchecked();
        f(root)
    }

    /// Returns the read-only buffer pool used for persisted blocks.
    #[inline]
    pub(crate) fn disk_pool(&self) -> &QuiescentGuard<ReadonlyBufferPool> {
        &self.disk_pool
    }

    /// Returns the deletion buffer tracking persisted-row tombstones.
    #[inline]
    pub(crate) fn deletion_buffer(&self) -> &ColumnDeletionBuffer {
        &self.deletion_buffer
    }

    /// Returns the reusable secondary DiskTree runtimes owned by this table.
    #[inline]
    pub(crate) fn secondary_index_runtimes(&self) -> &[Option<SecondaryDiskTreeRuntime>] {
        &self.secondary_indexes
    }
}
