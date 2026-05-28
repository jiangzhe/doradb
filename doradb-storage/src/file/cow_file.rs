use crate::bitmap::AllocMap;
use crate::buffer::page::PAGE_SIZE;
use crate::buffer::{ReadonlyBufferPool, ReadonlyWriteLease, begin_write_barrier};
use crate::error::{DataIntegrityError, Error, FileKind, InternalError, ResourceError, Result};
use crate::file::fs::BackgroundWriteRequest;
use crate::file::super_block::{SUPER_BLOCK_SIZE, SuperBlock};
use crate::file::{BlockKey, SparseFile, write_direct, write_direct_with_lease};
use crate::id::{BlockID, FileID, TrxID};
use crate::io::{DirectBuf, IOClient};
use crate::quiescent::QuiescentGuard;
use crate::trx::MAX_SNAPSHOT_TS;
use error_stack::Report;
use std::collections::BTreeSet;
use std::fs;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::os::fd::{AsRawFd, RawFd};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};

/// Shared page size of CoW table files and multi-table files.
pub(crate) const COW_FILE_PAGE_SIZE: usize = PAGE_SIZE;
/// Reserved persisted block id that stores the colocated ping-pong super-block
/// pair for every CoW file.
///
/// Because block `0` is occupied by that super block, no other persisted
/// meta/data/blob/index block may legally use this id.
pub(crate) const SUPER_BLOCK_ID: BlockID = BlockID::new(0);
/// Sentinel persisted block id used only for cleared in-memory metadata.
pub(crate) const INVALID_BLOCK_ID: BlockID = BlockID::new(u64::MAX);

/// Minimal mutable operations required by CoW index/checkpoint writers.
pub(crate) trait MutableCowFile {
    fn allocate_block(&mut self) -> Result<BlockID>;
    fn rollback_allocated_block(&mut self, block_id: BlockID) -> Result<()>;
    fn write_block(
        &self,
        block_id: BlockID,
        buf: DirectBuf,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// File wrapper that can own one mutable CoW writer claim.
pub(crate) trait MutableWriterFile {
    fn claim_mutable_writer(&self);
    fn release_mutable_writer(&self);
}

/// Readonly-cache write barrier used by CoW block writes.
#[derive(Clone, Copy)]
pub(crate) enum CowWriteBarrier<'a> {
    /// Block same-key readonly misses while the physical block is written.
    ReadonlyPool(&'a QuiescentGuard<ReadonlyBufferPool>),
    /// Bypass readonly-cache write blocking when the caller has no relevant readonly path.
    Disabled,
}

impl<'a> CowWriteBarrier<'a> {
    /// Builds a user-table write barrier backed by the shared readonly pool.
    #[inline]
    pub(crate) fn readonly_pool(pool: &'a QuiescentGuard<ReadonlyBufferPool>) -> Self {
        Self::ReadonlyPool(pool)
    }

    #[inline]
    pub(crate) fn begin_write(
        self,
        file_id: FileID,
        block_id: BlockID,
    ) -> Result<Option<ReadonlyWriteLease>> {
        match self {
            CowWriteBarrier::ReadonlyPool(pool) => {
                begin_write_barrier(pool.clone(), file_id, block_id).map(Some)
            }
            CowWriteBarrier::Disabled => Ok(None),
        }
    }
}

/// RAII guard for exclusive mutable CoW file access.
pub(crate) struct MutableWriterClaim<F: MutableWriterFile> {
    file: Arc<F>,
}

impl<F: MutableWriterFile> MutableWriterClaim<F> {
    /// Claim exclusive mutable access for the lifetime of this guard.
    #[inline]
    pub(crate) fn new(file: &Arc<F>) -> Self {
        file.claim_mutable_writer();
        Self {
            file: Arc::clone(file),
        }
    }
}

impl<F: MutableWriterFile> Drop for MutableWriterClaim<F> {
    #[inline]
    fn drop(&mut self) {
        self.file.release_mutable_writer();
    }
}

/// Shared in-memory active root for copy-on-write files.
///
/// `ActiveRoot<M>` stores generic CoW bookkeeping (`slot_no`, `meta_block_id`,
/// allocation map) plus a file-specific payload `M`.
pub(crate) struct ActiveRoot<M> {
    /// Active ping-pong super-block slot (0/1).
    pub(crate) slot_no: u64,
    /// Root publication timestamp persisted in the current super block.
    ///
    /// This value can be a transaction STS or CTS depending on the publishing
    /// path.
    pub(crate) root_ts: TrxID,
    /// Active meta-block id referenced by the super block.
    pub(crate) meta_block_id: BlockID,
    /// Allocation map used for page-id assignment.
    pub(crate) alloc_map: AllocMap,
    /// File-specific in-memory metadata payload.
    pub(crate) meta: M,
    /// Runtime-only timestamp proving when this root became effective.
    ///
    /// This is deliberately not serialized. Loaded roots initialize it from
    /// the durable root timestamp, while newly published runtime roots install
    /// a post-publish fence after the active-root pointer swap.
    effective_ts: AtomicU64,
}

impl<M: Clone> Clone for ActiveRoot<M> {
    #[inline]
    fn clone(&self) -> Self {
        ActiveRoot {
            slot_no: self.slot_no,
            root_ts: self.root_ts,
            meta_block_id: self.meta_block_id,
            alloc_map: self.alloc_map.clone(),
            meta: self.meta.clone(),
            effective_ts: AtomicU64::new(self.effective_ts().as_u64()),
        }
    }
}

impl<M> ActiveRoot<M> {
    /// Build one active root from already-parsed super/meta payload parts.
    #[inline]
    pub(crate) fn from_parts(
        slot_no: u64,
        root_ts: TrxID,
        meta_block_id: BlockID,
        alloc_map: AllocMap,
        meta: M,
    ) -> Self {
        ActiveRoot {
            slot_no,
            root_ts,
            meta_block_id,
            alloc_map,
            meta,
            effective_ts: AtomicU64::new(root_ts.as_u64()),
        }
    }

    /// Clone current root and flip to the opposite ping-pong super-block slot.
    #[inline]
    pub(crate) fn flip(&self) -> Self
    where
        M: Clone,
    {
        let mut new = self.clone();
        new.slot_no = 1 - self.slot_no;
        new.effective_ts
            .store(MAX_SNAPSHOT_TS.as_u64(), Ordering::Relaxed);
        new
    }

    /// Return the runtime-only timestamp when this root became observable.
    #[inline]
    pub(crate) fn effective_ts(&self) -> TrxID {
        TrxID::new(self.effective_ts.load(Ordering::Acquire))
    }

    /// Block reclamation decisions until an unpublished root becomes effective.
    #[inline]
    fn block_reclamation_until_effective_ts_installed(&self) {
        self.effective_ts
            .store(MAX_SNAPSHOT_TS.as_u64(), Ordering::Release);
    }

    /// Install the post-publish effective timestamp for the current active root.
    #[inline]
    pub(crate) fn install_effective_ts(&self, effective_ts: TrxID) {
        debug_assert!(effective_ts < MAX_SNAPSHOT_TS);
        self.effective_ts
            .store(effective_ts.as_u64(), Ordering::Release);
    }
}

/// Mutable CoW root plus block ids allocated before the root is published.
///
/// `unpublished_blocks` is intentionally not persisted. It prevents rollback
/// from clearing allocation bits inherited from the published root snapshot.
pub(crate) struct MutableCowRoot<M> {
    pub(super) root: ActiveRoot<M>,
    pub(super) unpublished_blocks: BTreeSet<BlockID>,
}

impl<M> MutableCowRoot<M> {
    /// Build a mutable CoW root from an already prepared root snapshot.
    #[inline]
    pub(crate) fn from_root(root: ActiveRoot<M>) -> Self {
        Self {
            root,
            unpublished_blocks: BTreeSet::new(),
        }
    }

    /// Fork a published active root into a mutable unpublished root.
    #[inline]
    pub(crate) fn fork(active_root: &ActiveRoot<M>) -> Self
    where
        M: Clone,
    {
        Self::from_root(active_root.flip())
    }

    /// Replace the allocation map with one built from root-reachable blocks.
    ///
    /// `SUPER_BLOCK_ID` is always kept allocated.
    #[inline]
    pub(crate) fn rebuild_alloc_map_from_reachable(
        &mut self,
        reachable: &BTreeSet<BlockID>,
    ) -> Result<usize> {
        let old_allocated = self.root.alloc_map.allocated();
        let rebuilt = AllocMap::new(self.root.alloc_map.len());
        assert!(rebuilt.allocate_at(usize::from(SUPER_BLOCK_ID)));
        for block_id in reachable {
            let idx = usize::from(*block_id);
            if idx >= rebuilt.len() {
                return Err(Report::new(InternalError::CowFileAllocationInvariant)
                    .attach(format!(
                        "reachable block id exceeds allocation map: block_id={block_id}, alloc_map_len={}",
                        rebuilt.len()
                    ))
                    .into());
            }
            if *block_id != SUPER_BLOCK_ID {
                let allocated = rebuilt.allocate_at(idx);
                debug_assert!(allocated);
            }
        }
        let meta_block_idx = usize::from(self.root.meta_block_id);
        if meta_block_idx >= rebuilt.len() {
            return Err(Report::new(InternalError::CowFileAllocationInvariant)
                .attach(format!(
                    "meta block id exceeds allocation map: block_id={}, alloc_map_len={}",
                    self.root.meta_block_id,
                    rebuilt.len()
                ))
                .into());
        }
        let _ = rebuilt.allocate_at(meta_block_idx);
        let new_allocated = rebuilt.allocated();
        self.root.alloc_map = rebuilt;
        self.unpublished_blocks.retain(|block_id| {
            *block_id == self.root.meta_block_id || reachable.contains(block_id)
        });
        Ok(old_allocated.saturating_sub(new_allocated))
    }

    /// Allocate one new block for copy-on-write publish.
    ///
    /// Returns `None` when the allocation bitmap cannot provide a new block.
    #[inline]
    pub(crate) fn try_allocate_block(&mut self) -> Option<BlockID> {
        let block_id = self.root.alloc_map.try_allocate().map(BlockID::from)?;
        self.unpublished_blocks.insert(block_id);
        Some(block_id)
    }

    /// Roll back one block id allocated from this mutable root.
    ///
    /// This is only valid for blocks allocated by the current unpublished CoW
    /// fork. The bytes may already exist in the sparse file, but clearing the
    /// allocation bit keeps the future published root from marking an abandoned
    /// block as live.
    #[inline]
    pub(crate) fn rollback_allocated_block(&mut self, block_id: BlockID) -> Result<()> {
        if block_id == SUPER_BLOCK_ID {
            return Err(Report::new(InternalError::CowFileAllocationInvariant)
                .attach(format!("cannot roll back super block id {block_id}"))
                .into());
        }
        let idx = usize::try_from(block_id.as_u64()).map_err(|_| {
            Error::from(
                Report::new(InternalError::CowFileAllocationInvariant)
                    .attach(format!("block_id={block_id}")),
            )
        })?;
        if idx >= self.root.alloc_map.len() {
            return Err(Report::new(InternalError::CowFileAllocationInvariant)
                .attach(format!(
                    "block_id={block_id}, alloc_map_len={}",
                    self.root.alloc_map.len()
                ))
                .into());
        }
        if !self.unpublished_blocks.contains(&block_id) {
            return Err(Report::new(InternalError::CowFileAllocationInvariant)
                .attach(format!(
                    "block_id={block_id} was not allocated by this mutable root"
                ))
                .into());
        }
        if !self.root.alloc_map.deallocate(idx) {
            return Err(Report::new(InternalError::CowFileAllocationInvariant)
                .attach(format!("block_id={block_id} allocation bit was not set"))
                .into());
        }
        self.unpublished_blocks.remove(&block_id);
        Ok(())
    }
}

/// Allocates one CoW block id.
#[inline]
pub(crate) fn allocate_cow_block<M>(
    root: &mut MutableCowRoot<M>,
    capacity_context: &'static str,
) -> Result<BlockID> {
    root.try_allocate_block().ok_or_else(|| {
        Error::from(
            Report::new(ResourceError::StorageFileCapacityExceeded).attach(capacity_context),
        )
    })
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
pub(crate) struct ParsedMeta<M> {
    /// File-specific metadata payload.
    pub(crate) meta: M,
    /// Decoded allocation bitmap for the file.
    pub(crate) alloc_map: AllocMap,
}

/// Serialization and parsing callbacks for one concrete CoW file type.
///
/// This keeps file-type-specific super/meta codecs explicit without introducing
/// extra trait hierarchies.
#[derive(Clone, Copy)]
pub(crate) struct CowCodec<M> {
    /// Parse one super-block image.
    pub(crate) parse_super_block: fn(&[u8]) -> Result<SuperBlock>,
    /// Parse one meta-block image.
    pub(crate) parse_meta_block: fn(BlockID, &[u8]) -> Result<ParsedMeta<M>>,
    /// Validate root invariants after parsing one meta block.
    pub(crate) validate_root: fn(BlockID, &ParsedMeta<M>) -> Result<()>,
    /// Build one meta-block image from active root.
    pub(crate) build_meta_block: fn(&ActiveRoot<M>) -> Result<DirectBuf>,
    /// Build one super-block image from active root.
    pub(crate) build_super_block: fn(&ActiveRoot<M>) -> Result<DirectBuf>,
}

/// Generic copy-on-write file abstraction shared by table and multi-table files.
///
/// `CowFile` owns file IO, active-root pointer management, and generic CoW
/// publish/load flow. Concrete file types provide format-specific behavior via
/// [`CowCodec`].
pub(crate) struct CowFile<M> {
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
    /// This is the low-level unchecked current-root boundary for CoW file
    /// internals and callers explicitly classified as recovery/bootstrap,
    /// checkpoint, or test-only paths.
    /// Every call observes the root pointer currently published at that moment.
    /// Callers that need multiple fields from one logical root snapshot must
    /// bind one local reference and reuse it.
    #[inline]
    pub(crate) fn active_root_unchecked(&self) -> &ActiveRoot<M> {
        Self::active_root_from_raw(self.load_active_root_raw())
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
        self.write_block_with_lease(background_writes, block_id, buf, None)
            .await
    }

    /// Write one block using async direct IO while carrying a readonly write
    /// barrier lease in the backend-owned submission.
    #[inline]
    pub(crate) async fn write_block_with_lease(
        &self,
        background_writes: &IOClient<BackgroundWriteRequest>,
        block_id: BlockID,
        buf: DirectBuf,
        write_lease: Option<ReadonlyWriteLease>,
    ) -> Result<()> {
        debug_assert!(buf.capacity() == COW_FILE_PAGE_SIZE);
        let offset = usize::from(block_id) * COW_FILE_PAGE_SIZE;
        write_direct_with_lease(
            self.block_key(offset),
            Arc::clone(&self.file),
            offset,
            buf,
            background_writes,
            write_lease,
        )
        .await
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
    pub(crate) fn swap_active_root(&self, active_root: ActiveRoot<M>) -> Option<OldCowRoot<M>> {
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
        let _ = disk_pool.invalidate_block(self.file.file_id(), SUPER_BLOCK_ID);
        let pool_guard = disk_pool.pool_guard();
        let super_block_guard = disk_pool
            .read_block(file_kind, &self.file, &pool_guard, SUPER_BLOCK_ID)
            .await?;
        let super_block =
            Self::pick_super_block(super_block_guard.page(), self.codec.parse_super_block)?;
        drop(super_block_guard);

        let _ = disk_pool.invalidate_block(self.file.file_id(), super_block.body.meta_block_id);
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
            parsed_meta.meta,
        ))
    }

    /// Publish a new active root via copy-on-write: meta block then super block.
    ///
    /// The sequence is:
    /// 1. allocate + write new meta block,
    /// 2. write next super block slot,
    /// 3. fsync and atomically swap active root pointer.
    #[inline]
    pub(crate) async fn publish_root(
        &self,
        background_writes: &IOClient<BackgroundWriteRequest>,
        mut new_root: MutableCowRoot<M>,
        write_barrier: CowWriteBarrier<'_>,
    ) -> Result<Option<OldCowRoot<M>>> {
        new_root
            .root
            .block_reclamation_until_effective_ts_installed();
        let new_meta_block_id =
            allocate_cow_block(&mut new_root, "publish root could not allocate meta block")?;
        new_root.root.meta_block_id = new_meta_block_id;

        let meta_buf = (self.codec.build_meta_block)(&new_root.root)?;
        let write_lease = write_barrier.begin_write(self.file.file_id(), new_meta_block_id)?;
        self.write_block_with_lease(background_writes, new_meta_block_id, meta_buf, write_lease)
            .await?;

        let super_buf = (self.codec.build_super_block)(&new_root.root)?;
        let offset = new_root.root.slot_no as usize * super_buf.capacity();
        self.write_at_offset(background_writes, offset, super_buf)
            .await?;

        self.fsync()?;
        Ok(self.swap_active_root(new_root.root))
    }

    /// Force all pending writes to disk.
    #[inline]
    pub(crate) fn fsync(&self) -> Result<()> {
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
    block_kind: &'static str,
) -> Result<()> {
    let meta_block_idx = usize::from(meta_block_id);
    if meta_block_idx == usize::from(SUPER_BLOCK_ID)
        || meta_block_idx >= alloc_map.len()
        || !alloc_map.is_allocated(meta_block_idx)
    {
        return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
            .attach(format!(
                "file={file_kind}, block={block_kind}, block_id={meta_block_id}"
            ))
            .into());
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
pub(crate) struct OldCowRoot<M>(NonNull<ActiveRoot<M>>);

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
    use super::{ActiveRoot, MutableCowRoot, SUPER_BLOCK_ID};
    use crate::bitmap::AllocMap;
    use crate::error::InternalError;
    use crate::id::{BlockID, TrxID};
    use std::collections::BTreeSet;
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    static OLD_ROOT_DROPS: OnceLock<Mutex<HashMap<usize, usize>>> = OnceLock::new();

    #[test]
    fn rebuild_alloc_map_keeps_current_meta_block_allocated() {
        let alloc_map = AllocMap::new(16);
        assert!(alloc_map.allocate_at(usize::from(SUPER_BLOCK_ID)));
        assert!(alloc_map.allocate_at(usize::from(BlockID::new(2))));
        assert!(alloc_map.allocate_at(usize::from(BlockID::new(3))));
        assert!(alloc_map.allocate_at(usize::from(BlockID::new(7))));

        let mut root = MutableCowRoot::from_root(ActiveRoot::from_parts(
            0,
            TrxID::new(1),
            BlockID::new(7),
            alloc_map,
            (),
        ));
        root.unpublished_blocks.insert(BlockID::new(3));
        root.unpublished_blocks.insert(BlockID::new(7));

        let reachable = BTreeSet::from([BlockID::new(2)]);
        let reclaimed = root.rebuild_alloc_map_from_reachable(&reachable).unwrap();

        assert_eq!(reclaimed, 1);
        assert!(
            root.root
                .alloc_map
                .is_allocated(usize::from(SUPER_BLOCK_ID))
        );
        assert!(
            root.root
                .alloc_map
                .is_allocated(usize::from(BlockID::new(2)))
        );
        assert!(
            !root
                .root
                .alloc_map
                .is_allocated(usize::from(BlockID::new(3)))
        );
        assert!(
            root.root
                .alloc_map
                .is_allocated(usize::from(BlockID::new(7)))
        );
        assert!(!root.unpublished_blocks.contains(&BlockID::new(3)));
        assert!(root.unpublished_blocks.contains(&BlockID::new(7)));
    }

    #[test]
    fn rebuild_alloc_map_rejects_out_of_range_meta_block_id() {
        let alloc_map = AllocMap::new(8);
        assert!(alloc_map.allocate_at(usize::from(SUPER_BLOCK_ID)));

        let mut root = MutableCowRoot::from_root(ActiveRoot::from_parts(
            0,
            TrxID::new(1),
            BlockID::new(8),
            alloc_map,
            (),
        ));
        let err = root
            .rebuild_alloc_map_from_reachable(&BTreeSet::new())
            .unwrap_err();

        assert_eq!(
            err.report().downcast_ref::<InternalError>().copied(),
            Some(InternalError::CowFileAllocationInvariant)
        );
    }

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
