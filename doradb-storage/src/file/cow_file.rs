use crate::bitmap::AllocMap;
use crate::buffer::ReadonlyBufferPool;
use crate::buffer::page::PAGE_SIZE;
use crate::error::{BlockCorruptionCause, BlockKind, Error, FileKind, Result};
use crate::file::fs::BackgroundWriteRequest;
use crate::file::super_block::{SUPER_BLOCK_SIZE, SuperBlock};
use crate::file::{BlockKey, FileID, SparseFile, write_direct};
use crate::io::{DirectBuf, IOClient};
use crate::quiescent::QuiescentGuard;
use crate::trx::TrxID;
use std::collections::BTreeSet;
use std::fs;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::os::fd::{AsRawFd, RawFd};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};

pub use crate::file::BlockID;

/// Shared page size of CoW table files and multi-table files.
pub const COW_FILE_PAGE_SIZE: usize = PAGE_SIZE;
/// Reserved persisted block id that stores the colocated ping-pong super-block
/// pair for every CoW file.
///
/// Because block `0` is occupied by that super block, no other persisted
/// meta/data/blob/index block may legally use this id.
pub const SUPER_BLOCK_ID: BlockID = BlockID::new(0);
/// Sentinel persisted block id used only for cleared in-memory metadata.
pub const INVALID_BLOCK_ID: BlockID = BlockID::new(u64::MAX);

/// Minimal mutable operations required by CoW index/checkpoint writers.
pub(crate) trait MutableCowFile {
    fn allocate_block_id(&mut self) -> Result<BlockID>;
    fn rollback_allocated_block_id(&mut self, block_id: BlockID) -> Result<()>;
    fn record_gc_block(&mut self, block_id: BlockID);
    fn write_block(
        &self,
        block_id: BlockID,
        buf: DirectBuf,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// Shared in-memory active root for copy-on-write files.
///
/// `ActiveRoot<M>` stores generic CoW bookkeeping (`slot_no`, `meta_block_id`,
/// allocation map, legacy GC list) plus a file-specific payload `M`.
#[derive(Clone)]
pub struct ActiveRoot<M> {
    /// Active ping-pong super-block slot (0/1).
    pub slot_no: u64,
    /// Transaction/checkpoint id persisted in current super block.
    pub trx_id: TrxID,
    /// Active meta-block id referenced by the super block.
    pub meta_block_id: BlockID,
    /// Allocation map used for page-id assignment.
    pub alloc_map: AllocMap,
    /// Legacy persisted list of obsolete blocks.
    ///
    /// Current formats still serialize this list for compatibility. Future
    /// user-table block reclaim should be driven by root reachability rather
    /// than treating this list as the durable reclaim contract.
    pub gc_block_list: Vec<BlockID>,
    /// File-specific in-memory metadata payload.
    pub meta: M,
    /// Blocks allocated by this unpublished mutable root.
    ///
    /// This set is intentionally not persisted. It prevents rollback from
    /// clearing allocation bits inherited from the published root snapshot.
    newly_allocated_ids: BTreeSet<BlockID>,
}

impl<M> ActiveRoot<M> {
    /// Build one active root from already-parsed super/meta payload parts.
    #[inline]
    pub fn from_parts(
        slot_no: u64,
        trx_id: TrxID,
        meta_block_id: BlockID,
        alloc_map: AllocMap,
        gc_block_list: Vec<BlockID>,
        meta: M,
    ) -> Self {
        ActiveRoot {
            slot_no,
            trx_id,
            meta_block_id,
            alloc_map,
            gc_block_list,
            meta,
            newly_allocated_ids: BTreeSet::new(),
        }
    }

    /// Clone current root and flip to the opposite ping-pong super-block slot.
    #[inline]
    pub fn flip(&self) -> Self
    where
        M: Clone,
    {
        let mut new = self.clone();
        new.slot_no = 1 - self.slot_no;
        new.newly_allocated_ids.clear();
        new
    }

    /// Record one obsolete meta-block id in the legacy GC list.
    ///
    /// This is used during publish so the next root keeps a reclaim list
    /// for historical meta blocks.
    #[inline]
    pub fn push_gc_meta_block(&mut self, block_id: BlockID) {
        self.gc_block_list.push(block_id);
    }

    /// Allocate one new page id for copy-on-write publish.
    ///
    /// Returns `None` when the allocation bitmap cannot provide a new page.
    #[inline]
    pub fn try_allocate_block_id(&mut self) -> Option<BlockID> {
        let block_id = self.alloc_map.try_allocate().map(BlockID::from)?;
        self.newly_allocated_ids.insert(block_id);
        Some(block_id)
    }

    /// Roll back one block id allocated from this mutable root.
    ///
    /// This is only valid for blocks allocated by the current unpublished CoW
    /// fork. The bytes may already exist in the sparse file, but clearing the
    /// allocation bit keeps the future published root from marking an abandoned
    /// block as live.
    #[inline]
    pub fn rollback_allocated_block_id(&mut self, block_id: BlockID) -> Result<()> {
        if block_id == SUPER_BLOCK_ID {
            return Err(Error::InvalidState);
        }
        let idx = usize::try_from(block_id.as_u64()).map_err(|_| Error::InvalidState)?;
        if idx >= self.alloc_map.len() {
            return Err(Error::InvalidState);
        }
        if !self.newly_allocated_ids.contains(&block_id) {
            return Err(Error::InvalidState);
        }
        if !self.alloc_map.deallocate(idx) {
            return Err(Error::InvalidState);
        }
        self.newly_allocated_ids.remove(&block_id);
        Ok(())
    }

    /// Drop unpublished allocation ownership markers before publishing a root.
    #[inline]
    fn clear_newly_allocated_ids(&mut self) {
        self.newly_allocated_ids.clear();
    }
}

impl<M> Deref for ActiveRoot<M> {
    type Target = M;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.meta
    }
}

impl<M> DerefMut for ActiveRoot<M> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.meta
    }
}

/// Decoded meta-block payload used to assemble an active root.
///
/// `parse_meta_block` returns this value so `CowFile` can reconstruct one
/// full [`ActiveRoot`] without trait-based root logic.
pub struct ParsedMeta<M> {
    /// File-specific metadata payload.
    pub meta: M,
    /// Decoded allocation bitmap for the file.
    pub alloc_map: AllocMap,
    /// Decoded legacy obsolete page list.
    pub gc_block_list: Vec<BlockID>,
}

/// Serialization and parsing callbacks for one concrete CoW file type.
///
/// This keeps file-type-specific super/meta codecs explicit without introducing
/// extra trait hierarchies.
#[derive(Clone, Copy)]
pub struct CowCodec<M> {
    /// Parse one super-block image.
    pub parse_super_block: fn(&[u8]) -> Result<SuperBlock>,
    /// Parse one meta-block image.
    pub parse_meta_block: fn(BlockID, &[u8]) -> Result<ParsedMeta<M>>,
    /// Validate root invariants after parsing one meta block.
    pub validate_root: fn(BlockID, &ParsedMeta<M>) -> Result<()>,
    /// Build one meta-block image from active root.
    pub build_meta_block: fn(&ActiveRoot<M>) -> Result<DirectBuf>,
    /// Build one super-block image from active root.
    pub build_super_block: fn(&ActiveRoot<M>) -> Result<DirectBuf>,
}

/// Generic copy-on-write file abstraction shared by table and multi-table files.
///
/// `CowFile` owns file IO, active-root pointer management, and generic CoW
/// publish/load flow. Concrete file types provide format-specific behavior via
/// [`CowCodec`].
pub struct CowFile<M> {
    file: Arc<SparseFile>,
    active_root: AtomicPtr<ActiveRoot<M>>,
    mutable_inflight: AtomicBool,
    codec: CowCodec<M>,
}

impl<M> CowFile<M> {
    /// Create a new CoW file and truncate/create backing storage.
    ///
    /// The active root pointer is left uninitialized until caller publishes or
    /// loads one root.
    #[inline]
    pub(crate) fn create(
        file_path: impl AsRef<str>,
        initial_size: usize,
        file_id: FileID,
        codec: CowCodec<M>,
        trunc: bool,
    ) -> Result<Self> {
        let file = if trunc {
            SparseFile::create_or_trunc(file_path, initial_size, file_id)
        } else {
            SparseFile::create_or_fail(file_path, initial_size, file_id)
        }?;
        Ok(CowFile {
            file: Arc::new(file),
            active_root: AtomicPtr::new(std::ptr::null_mut()),
            mutable_inflight: AtomicBool::new(false),
            codec,
        })
    }

    /// Open an existing CoW file.
    ///
    /// The active root pointer is left uninitialized until caller loads and
    /// swaps one root.
    #[inline]
    pub(crate) fn open(
        file_path: impl AsRef<str>,
        file_id: FileID,
        codec: CowCodec<M>,
    ) -> Result<Self> {
        let file = SparseFile::open(file_path, file_id)?;
        Ok(CowFile {
            file: Arc::new(file),
            active_root: AtomicPtr::new(std::ptr::null_mut()),
            mutable_inflight: AtomicBool::new(false),
            codec,
        })
    }

    /// Return the current in-memory active root reference.
    ///
    /// Every call observes the root pointer currently published at that moment.
    /// Callers that need multiple fields from one logical root snapshot must
    /// bind one local reference and reuse it.
    #[inline]
    pub fn active_root(&self) -> &ActiveRoot<M> {
        Self::active_root_from_raw(self.load_active_root_raw())
    }

    /// Return a copy of the active-root atomic pointer.
    ///
    /// This is used by components that need lock-free pointer observation.
    #[inline]
    pub fn active_root_ptr(&self) -> AtomicPtr<ActiveRoot<M>> {
        AtomicPtr::new(self.load_active_root_raw())
    }

    /// Claim exclusive mutable access to this CoW file.
    ///
    /// This is called by [`MutableTableFile`] and [`MutableMultiTableFile`]
    /// constructors (including catalog checkpoint application). At most one
    /// mutable writer may be active at any time.
    ///
    /// # Panics
    ///
    /// Panics if another mutable writer already exists for this file handle.
    pub(crate) fn claim_mutable_writer(&self) {
        let acquired = self
            .mutable_inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        assert!(
            acquired,
            "concurrent mutable CoW file modification is not allowed"
        );
    }

    /// Release exclusive mutable access claimed by `claim_mutable_writer`.
    #[inline]
    pub(crate) fn release_mutable_writer(&self) {
        let released = self
            .mutable_inflight
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        assert!(
            released,
            "mutable CoW file writer release without active claim"
        );
    }

    /// Write one block using async direct IO.
    #[inline]
    pub(crate) async fn write_block(
        &self,
        background_writes: &IOClient<BackgroundWriteRequest>,
        block_id: BlockID,
        buf: DirectBuf,
    ) -> Result<()> {
        debug_assert!(buf.capacity() == COW_FILE_PAGE_SIZE);
        let offset = usize::from(block_id) * COW_FILE_PAGE_SIZE;
        self.write_at_offset(background_writes, offset, buf).await
    }

    #[inline]
    fn block_key(&self, offset: usize) -> BlockKey {
        BlockKey::new(
            self.file.file_id(),
            BlockID::from(offset / COW_FILE_PAGE_SIZE),
        )
    }

    /// Write one buffer at given byte offset.
    #[inline]
    pub(crate) async fn write_at_offset(
        &self,
        background_writes: &IOClient<BackgroundWriteRequest>,
        offset: usize,
        buf: DirectBuf,
    ) -> Result<()> {
        write_direct(
            self.block_key(offset),
            Arc::clone(&self.file),
            offset,
            buf,
            background_writes,
        )
        .await
    }

    #[inline]
    pub(crate) fn sparse_file(&self) -> &Arc<SparseFile> {
        &self.file
    }

    /// Replace active root with new root, returning previous-root guard if present.
    #[inline]
    pub fn swap_active_root(&self, active_root: ActiveRoot<M>) -> Option<OldCowRoot<M>> {
        let new = Self::allocate_active_root(active_root);
        let old = self.active_root.swap(new, Ordering::SeqCst);
        NonNull::new(old).map(OldCowRoot)
    }

    /// Load and validate the active root from on-disk super/meta blocks.
    ///
    /// This reads the ping-pong super-block pair from block 0, picks the newest
    /// valid slot, validates the referenced meta block through the configured
    /// codec, and rejects invalid root invariants before returning an
    /// in-memory [`ActiveRoot`].
    ///
    /// This is the intended runtime use of
    /// `QuiescentGuard<ReadonlyBufferPool>::read_block()`.
    /// The raw block reads here are acceptable because super/meta-block
    /// validation happens immediately through `pick_super_block`,
    /// `parse_meta_block`, and `validate_root`. New callers should prefer
    /// `read_validated_block()` unless they have an equivalent caller-owned
    /// validation path and have reviewed the raw-read inflight semantics.
    #[inline]
    pub(crate) async fn load_active_root_from_pool(
        &self,
        file_kind: FileKind,
        disk_pool: &QuiescentGuard<ReadonlyBufferPool>,
    ) -> Result<ActiveRoot<M>> {
        let _ = disk_pool.invalidate_block_id(self.file.file_id(), SUPER_BLOCK_ID);
        let pool_guard = disk_pool.pool_guard();
        let super_block_guard = disk_pool
            .read_block(file_kind, &self.file, &pool_guard, SUPER_BLOCK_ID)
            .await?;
        let super_block =
            Self::pick_super_block(super_block_guard.page(), self.codec.parse_super_block)?;
        drop(super_block_guard);

        let _ = disk_pool.invalidate_block_id(self.file.file_id(), super_block.body.meta_block_id);
        let meta_block_guard = disk_pool
            .read_block(
                file_kind,
                &self.file,
                &pool_guard,
                super_block.body.meta_block_id,
            )
            .await?;
        let parsed_meta =
            (self.codec.parse_meta_block)(super_block.body.meta_block_id, meta_block_guard.page())?;
        (self.codec.validate_root)(super_block.body.meta_block_id, &parsed_meta)?;
        drop(meta_block_guard);

        Ok(ActiveRoot::from_parts(
            super_block.header.slot_no,
            super_block.header.checkpoint_cts,
            super_block.body.meta_block_id,
            parsed_meta.alloc_map,
            parsed_meta.gc_block_list,
            parsed_meta.meta,
        ))
    }

    /// Publish a new active root via copy-on-write: meta block then super block.
    ///
    /// The sequence is:
    /// 1. add previous meta block to the legacy GC list,
    /// 2. allocate + write new meta block,
    /// 3. write next super block slot,
    /// 4. fsync and atomically swap active root pointer.
    #[inline]
    pub(crate) async fn publish_root(
        &self,
        background_writes: &IOClient<BackgroundWriteRequest>,
        mut new_root: ActiveRoot<M>,
    ) -> Result<Option<OldCowRoot<M>>> {
        let old_meta_block_id = new_root.meta_block_id;
        if old_meta_block_id != SUPER_BLOCK_ID {
            new_root.push_gc_meta_block(old_meta_block_id);
        }

        let new_meta_block_id = match new_root.try_allocate_block_id() {
            Some(block_id) => block_id,
            None => return Err(Error::InvalidState),
        };
        new_root.meta_block_id = new_meta_block_id;

        let meta_buf = (self.codec.build_meta_block)(&new_root)?;
        self.write_block(background_writes, new_meta_block_id, meta_buf)
            .await?;

        let super_buf = (self.codec.build_super_block)(&new_root)?;
        let offset = new_root.slot_no as usize * super_buf.capacity();
        self.write_at_offset(background_writes, offset, super_buf)
            .await?;

        self.fsync()?;
        new_root.clear_newly_allocated_ids();
        Ok(self.swap_active_root(new_root))
    }

    /// Force all pending writes to disk.
    #[inline]
    pub fn fsync(&self) -> Result<()> {
        self.file.syncer().fsync()
    }

    /// Delete underlying file by fd path.
    #[inline]
    pub(crate) fn delete(self) {
        let _ = remove_file_by_fd(self.file.as_raw_fd());
    }

    /// Pick latest valid super block from the reserved super-block image.
    #[inline]
    fn pick_super_block(
        buf: &[u8],
        parse_super_block: fn(&[u8]) -> Result<SuperBlock>,
    ) -> Result<SuperBlock> {
        debug_assert!(buf.len() == SUPER_BLOCK_SIZE * 2);
        let first = parse_super_block(&buf[..SUPER_BLOCK_SIZE]);
        let second = parse_super_block(&buf[SUPER_BLOCK_SIZE..]);
        match (first, second) {
            (Err(err), Err(_)) => Err(err),
            (Ok(root), Err(_)) | (Err(_), Ok(root)) => Ok(root),
            (Ok(l), Ok(r)) => {
                if l.header.checkpoint_cts < r.header.checkpoint_cts {
                    Ok(r)
                } else {
                    Ok(l)
                }
            }
        }
    }

    #[inline]
    fn load_active_root_raw(&self) -> *mut ActiveRoot<M> {
        self.active_root.load(Ordering::Relaxed)
    }

    #[inline]
    fn active_root_from_raw<'a>(ptr: *mut ActiveRoot<M>) -> &'a ActiveRoot<M> {
        match NonNull::new(ptr) {
            Some(ptr) => {
                // SAFETY: pointers come from `Box<_>` and are reclaimed only when swapped/dropped.
                unsafe { ptr.as_ref() }
            }
            None => panic!("active root is not initialized"),
        }
    }

    #[inline]
    fn allocate_active_root(active_root: ActiveRoot<M>) -> *mut ActiveRoot<M> {
        Box::into_raw(Box::new(active_root))
    }

    #[inline]
    fn reclaim_active_root(ptr: *mut ActiveRoot<M>) {
        if let Some(ptr) = NonNull::new(ptr) {
            // SAFETY: pointer was allocated by `allocate_active_root` and is reclaimed once.
            unsafe {
                drop(Box::from_raw(ptr.as_ptr()));
            }
        }
    }
}

#[inline]
pub(crate) fn validate_active_meta_block_id(
    alloc_map: &AllocMap,
    meta_block_id: BlockID,
    file_kind: FileKind,
    block_kind: BlockKind,
) -> Result<()> {
    let meta_block_idx = usize::from(meta_block_id);
    if meta_block_idx == usize::from(SUPER_BLOCK_ID)
        || meta_block_idx >= alloc_map.len()
        || !alloc_map.is_allocated(meta_block_idx)
    {
        return Err(Error::block_corrupted(
            file_kind,
            block_kind,
            meta_block_id,
            BlockCorruptionCause::InvalidRootInvariant,
        ));
    }
    Ok(())
}

impl<M> Drop for CowFile<M> {
    #[inline]
    fn drop(&mut self) {
        Self::reclaim_active_root(self.load_active_root_raw());
    }
}

/// Guard object that reclaims previous active root once dropped.
///
/// Returned from active-root swap to defer pointer reclamation until callers
/// finish using the old root snapshot.
pub struct OldCowRoot<M>(NonNull<ActiveRoot<M>>);

impl<M> Drop for OldCowRoot<M> {
    #[inline]
    fn drop(&mut self) {
        #[cfg(test)]
        tests::record_old_root_drop(self.0.as_ptr() as usize);

        // SAFETY: old roots are produced by active-root swap and reclaimed once.
        unsafe {
            drop(Box::from_raw(self.0.as_ptr()));
        }
    }
}

// SAFETY: `OldCowRoot` only owns a reclaim-on-drop pointer returned from the
// active-root swap path; moving it transfers that one-shot ownership.
unsafe impl<M: Send> Send for OldCowRoot<M> {}

// SAFETY: shared references to `OldCowRoot` do not expose the pointed
// `ActiveRoot`; the guard only reclaims the pointer when owned and dropped.
unsafe impl<M> Sync for OldCowRoot<M> {}

#[inline]
fn remove_file_by_fd(fd: RawFd) -> std::io::Result<()> {
    let proc_path = format!("/proc/self/fd/{}", fd);
    let real_path = fs::read_link(&proc_path)?;
    fs::remove_file(real_path)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    static OLD_ROOT_DROPS: OnceLock<Mutex<HashMap<usize, usize>>> = OnceLock::new();

    #[inline]
    fn old_root_drops() -> &'static Mutex<HashMap<usize, usize>> {
        OLD_ROOT_DROPS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    #[inline]
    pub(super) fn record_old_root_drop(ptr: usize) {
        let mut drops = old_root_drops().lock().unwrap();
        *drops.entry(ptr).or_default() += 1;
    }

    #[inline]
    pub(crate) fn old_root_drop_count(ptr: usize) -> usize {
        old_root_drops()
            .lock()
            .unwrap()
            .get(&ptr)
            .copied()
            .unwrap_or(0)
    }
}
