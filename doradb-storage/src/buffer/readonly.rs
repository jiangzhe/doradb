use crate::buffer::BufferPool;
use crate::buffer::frame::{BufferFrame, BufferFrames, FrameKind};
use crate::buffer::guard::{FacadePageGuard, PageExclusiveGuard};
use crate::buffer::page::{BufferPage, Page, PageID, VersionedPageID};
use crate::buffer::util::{deallocate_frame_and_page_arrays, initialize_frame_and_page_arrays};
use crate::catalog::TableID;
use crate::error::Validation::Valid;
use crate::error::{Error, Result, Validation};
use crate::file::table_file::TableFile;
use crate::latch::LatchFallbackMode;
use crate::lifetime::StaticLifetime;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use std::mem;
use std::sync::Arc;

/// Physical cache identity for readonly table-file pages.
///
/// This intentionally excludes root version to preserve cache hits across
/// root swaps when physical blocks are unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReadonlyCacheKey {
    /// Table identity owning the block.
    pub table_id: TableID,
    /// Physical page/block id in the table file.
    pub block_id: PageID,
}

impl ReadonlyCacheKey {
    /// Builds a key from table id and physical block id.
    #[inline]
    pub fn new(table_id: TableID, block_id: PageID) -> Self {
        ReadonlyCacheKey { table_id, block_id }
    }
}

/// Global readonly cache owner shared across tables.
///
/// This type owns the frame/page arena and maintains a forward mapping
/// from physical on-disk identity to in-memory frame id.
///
/// Reverse lookup is stored inline in `BufferFrame` as readonly key metadata.
pub struct GlobalReadonlyBufferPool {
    frames: BufferFrames,
    pages: *mut Page,
    size: usize,
    mappings: DashMap<ReadonlyCacheKey, PageID>,
}

impl GlobalReadonlyBufferPool {
    /// Creates a global readonly pool with a target memory budget in bytes.
    #[inline]
    pub fn with_capacity(pool_size: usize) -> Result<Self> {
        let size = pool_size / (mem::size_of::<BufferFrame>() + mem::size_of::<Page>());
        let (frames, pages) = unsafe { initialize_frame_and_page_arrays(size)? };
        Ok(GlobalReadonlyBufferPool {
            frames: BufferFrames(frames),
            pages,
            size,
            mappings: DashMap::new(),
        })
    }

    /// Creates and leaks a global readonly pool for static-lifetime usage.
    #[inline]
    pub fn with_capacity_static(pool_size: usize) -> Result<&'static Self> {
        let pool = Self::with_capacity(pool_size)?;
        Ok(StaticLifetime::new_static(pool))
    }

    /// Returns total number of frame slots in this pool.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.size
    }

    /// Returns number of currently mapped cache entries.
    #[inline]
    pub fn allocated(&self) -> usize {
        self.mappings.len()
    }

    /// Looks up mapped frame id for a given physical cache key.
    #[inline]
    pub fn try_get_frame_id(&self, key: &ReadonlyCacheKey) -> Option<PageID> {
        self.mappings.get(key).map(|v| *v)
    }

    /// Looks up physical cache key by frame id.
    #[inline]
    pub fn try_get_key(&self, frame_id: PageID) -> Option<ReadonlyCacheKey> {
        if frame_id as usize >= self.size {
            return None;
        }
        let frame = self.frames.frame(frame_id);
        frame
            .readonly_key()
            .map(|(table_id, block_id)| ReadonlyCacheKey::new(table_id, block_id))
    }

    /// Binds a physical key to a frame id.
    ///
    /// Binding is idempotent for the same key/frame pair and returns
    /// `Error::InvalidState` for conflicting mapping attempts.
    #[inline]
    pub fn bind_frame(&self, key: ReadonlyCacheKey, frame_id: PageID) -> Result<()> {
        if frame_id as usize >= self.size {
            return Err(Error::InvalidArgument);
        }
        let frame = self.frames.frame(frame_id);
        match self.mappings.entry(key) {
            Entry::Occupied(occ) => {
                let existing = *occ.get();
                if existing != frame_id {
                    return Err(Error::InvalidState);
                }
                return match frame.readonly_key() {
                    Some((table_id, block_id))
                        if table_id == key.table_id && block_id == key.block_id =>
                    {
                        Ok(())
                    }
                    _ => Err(Error::InvalidState),
                };
            }
            Entry::Vacant(vac) => {
                if let Some((table_id, block_id)) = frame.readonly_key() {
                    if table_id != key.table_id || block_id != key.block_id {
                        return Err(Error::InvalidState);
                    }
                    // Forward map and frame metadata are out of sync.
                    return Err(Error::InvalidState);
                }
                vac.insert(frame_id);
            }
        }
        frame.set_readonly_key(key.table_id, key.block_id);

        frame.bump_generation();
        frame.set_kind(FrameKind::Hot);
        Ok(())
    }

    /// Invalidates a specific cache key and returns its old frame id.
    #[inline]
    pub fn invalidate_key(&self, key: &ReadonlyCacheKey) -> Option<PageID> {
        let frame_id = match self.mappings.remove(key) {
            Some((_, frame_id)) => frame_id,
            None => return None,
        };

        let frame = self.frames.frame(frame_id);
        let existing = frame.readonly_key();
        debug_assert_eq!(existing, Some((key.table_id, key.block_id)));
        frame.clear_readonly_key();
        frame.set_kind(FrameKind::Uninitialized);
        frame.bump_generation();
        Some(frame_id)
    }

    /// Invalidates one physical block from one table.
    #[inline]
    pub fn invalidate_block(&self, table_id: TableID, block_id: PageID) -> Option<PageID> {
        self.invalidate_key(&ReadonlyCacheKey::new(table_id, block_id))
    }

    /// Invalidates all cache entries belonging to one table.
    ///
    /// Returns the number of invalidated mappings.
    #[inline]
    pub fn invalidate_table(&self, table_id: TableID) -> usize {
        let keys = self
            .mappings
            .iter()
            .filter_map(|entry| {
                if entry.key().table_id == table_id {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let mut count = 0usize;
        for key in keys {
            if self.invalidate_key(&key).is_some() {
                count += 1;
            }
        }
        count
    }

    #[inline]
    async fn get_page_internal<T: 'static>(
        &'static self,
        frame_id: PageID,
        mode: LatchFallbackMode,
    ) -> FacadePageGuard<T> {
        let bf = self.frames.frame_ptr(frame_id);
        let g = BufferFrames::frame_ref(bf.clone())
            .latch
            .optimistic_fallback(mode)
            .await;
        FacadePageGuard::new(bf, g)
    }
}

impl Drop for GlobalReadonlyBufferPool {
    fn drop(&mut self) {
        unsafe {
            for frame_id in 0..self.size {
                let frame_ptr = self.frames.0.add(frame_id);
                std::ptr::drop_in_place(frame_ptr);
            }
            deallocate_frame_and_page_arrays(self.frames.0, self.pages, self.size);
        }
    }
}

unsafe impl Send for GlobalReadonlyBufferPool {}
unsafe impl Sync for GlobalReadonlyBufferPool {}
unsafe impl StaticLifetime for GlobalReadonlyBufferPool {}

/// Per-table readonly wrapper implementing the `BufferPool` contract.
///
/// This wrapper translates table-local `PageID` into global physical cache key
/// and delegates to `GlobalReadonlyBufferPool`.
pub struct ReadonlyBufferPool {
    table_id: TableID,
    _table_file: Arc<TableFile>,
    global: &'static GlobalReadonlyBufferPool,
}

impl ReadonlyBufferPool {
    /// Creates a per-table readonly pool wrapper.
    #[inline]
    pub fn new(
        table_id: TableID,
        table_file: Arc<TableFile>,
        global: &'static GlobalReadonlyBufferPool,
    ) -> Self {
        ReadonlyBufferPool {
            table_id,
            _table_file: table_file,
            global,
        }
    }

    #[inline]
    fn cache_key(&self, block_id: PageID) -> ReadonlyCacheKey {
        ReadonlyCacheKey::new(self.table_id, block_id)
    }

    /// Invalidates one block for this table from the global readonly cache.
    #[inline]
    pub fn invalidate_block_id(&self, block_id: PageID) -> Option<PageID> {
        self.global.invalidate_block(self.table_id, block_id)
    }
}

impl BufferPool for ReadonlyBufferPool {
    #[inline]
    fn capacity(&self) -> usize {
        self.global.capacity()
    }

    #[inline]
    fn allocated(&self) -> usize {
        self.global.allocated()
    }

    #[inline]
    async fn allocate_page<T: BufferPage>(&'static self) -> PageExclusiveGuard<T> {
        panic!("readonly buffer pool does not support page allocation")
    }

    #[inline]
    async fn allocate_page_at<T: BufferPage>(
        &'static self,
        _page_id: PageID,
    ) -> Result<PageExclusiveGuard<T>> {
        panic!("readonly buffer pool does not support page allocation")
    }

    #[inline]
    async fn get_page<T: BufferPage>(
        &'static self,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> FacadePageGuard<T> {
        let key = self.cache_key(page_id);
        let frame_id = self.global.try_get_frame_id(&key).unwrap_or_else(|| {
            panic!("readonly buffer pool cache miss loading is not implemented in phase 1")
        });
        self.global.get_page_internal(frame_id, mode).await
    }

    #[inline]
    async fn try_get_page_versioned<T: BufferPage>(
        &'static self,
        _id: VersionedPageID,
        _mode: LatchFallbackMode,
    ) -> Option<FacadePageGuard<T>> {
        // Versioned reads will be wired when read path integration starts.
        None
    }

    #[inline]
    fn deallocate_page<T: BufferPage>(&'static self, _g: PageExclusiveGuard<T>) {
        panic!("readonly buffer pool does not support page deallocation")
    }

    #[inline]
    async fn get_child_page<T: BufferPage>(
        &'static self,
        p_guard: &FacadePageGuard<T>,
        page_id: PageID,
        mode: LatchFallbackMode,
    ) -> Validation<FacadePageGuard<T>> {
        let g = self.get_page::<T>(page_id, mode).await;
        if p_guard.validate_bool() {
            return Valid(g);
        }
        if g.is_exclusive() {
            g.rollback_exclusive_version_change();
        }
        Validation::Invalid
    }
}

unsafe impl Send for ReadonlyBufferPool {}
unsafe impl Sync for ReadonlyBufferPool {}
unsafe impl StaticLifetime for ReadonlyBufferPool {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::page::Page;
    use crate::catalog::{ColumnAttributes, ColumnSpec, TableMetadata};
    use crate::file::table_fs::TableFileSystemConfig;
    use crate::lifetime::StaticLifetimeScope;
    use crate::value::ValKind;
    use tempfile::TempDir;

    #[test]
    fn test_global_readonly_mapping_and_invalidation() {
        let scope = StaticLifetimeScope::new();
        let global =
            scope.adopt(GlobalReadonlyBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap());
        let global = global.as_static();
        let key = ReadonlyCacheKey::new(7, 11);

        assert_eq!(global.allocated(), 0);
        global.bind_frame(key, 3).unwrap();
        assert_eq!(global.allocated(), 1);
        assert_eq!(global.try_get_frame_id(&key), Some(3));
        assert_eq!(global.try_get_key(3), Some(key));

        // Idempotent bind on the same key/frame pair is allowed.
        assert!(global.bind_frame(key, 3).is_ok());

        // Rebinding a different key to same frame id violates mapping invariant.
        let err = global
            .bind_frame(ReadonlyCacheKey::new(7, 12), 3)
            .unwrap_err();
        assert!(matches!(err, Error::InvalidState));

        assert_eq!(global.invalidate_key(&key), Some(3));
        assert_eq!(global.allocated(), 0);
    }

    #[test]
    fn test_global_invalidate_table() {
        let scope = StaticLifetimeScope::new();
        let global =
            scope.adopt(GlobalReadonlyBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap());
        let global = global.as_static();
        let k1 = ReadonlyCacheKey::new(1, 10);
        let k2 = ReadonlyCacheKey::new(1, 11);
        let k3 = ReadonlyCacheKey::new(2, 20);

        global.bind_frame(k1, 1).unwrap();
        global.bind_frame(k2, 2).unwrap();
        global.bind_frame(k3, 3).unwrap();

        assert_eq!(global.invalidate_table(1), 2);
        assert_eq!(global.try_get_frame_id(&k1), None);
        assert_eq!(global.try_get_frame_id(&k2), None);
        assert_eq!(global.try_get_frame_id(&k3), Some(3));
    }

    #[test]
    #[should_panic(expected = "readonly buffer pool does not support page allocation")]
    fn test_readonly_pool_allocate_page_panics() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let fs = TableFileSystemConfig::default()
                .with_main_dir(temp_dir.path())
                .build()
                .unwrap();
            let metadata = Arc::new(TableMetadata::new(
                vec![ColumnSpec::new(
                    "c0",
                    ValKind::U32,
                    ColumnAttributes::empty(),
                )],
                vec![],
            ));
            let table_file = fs.create_table_file(101, metadata, false).unwrap();
            let (table_file, old_root) = table_file.commit(1, false).await.unwrap();
            drop(old_root);

            let scope = StaticLifetimeScope::new();
            let global = scope
                .adopt(GlobalReadonlyBufferPool::with_capacity_static(64 * 1024 * 1024).unwrap());
            let pool = scope.adopt(StaticLifetime::new_static(ReadonlyBufferPool::new(
                101,
                table_file,
                global.as_static(),
            )));
            let _ = pool.as_static().allocate_page::<Page>().await;
        });
    }
}
