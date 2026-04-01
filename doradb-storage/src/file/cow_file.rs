use crate::bitmap::AllocMap;
use crate::buffer::page::PAGE_SIZE;
use crate::buffer::{BlockKey, PersistedFileID, ReadSubmission, ReadonlyBufferPool};
use crate::error::{
    Error, PersistedFileKind, PersistedPageCorruptionCause, PersistedPageKind, Result,
};
use crate::file::super_page::{SUPER_PAGE_SIZE, SuperPage};
use crate::file::{SparseFile, TableFsRequest, write_direct};
use crate::io::{AIOClient, DirectBuf};
use crate::trx::TrxID;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::os::fd::{AsRawFd, RawFd};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};

pub use crate::file::BlockID;

/// Shared page size of CoW table files and multi-table files.
pub const COW_FILE_PAGE_SIZE: usize = PAGE_SIZE;
/// Reserved persisted block id that stores the colocated ping-pong super-page
/// pair for every CoW file.
///
/// Because block `0` is occupied by that super block, no other persisted
/// meta/data/blob/index block may legally use this id.
pub const SUPER_BLOCK_ID: BlockID = BlockID::new(0);
/// Sentinel persisted block id used only for cleared in-memory metadata.
pub const INVALID_BLOCK_ID: BlockID = BlockID::new(u64::MAX);

/// Minimal mutable operations required by CoW index/checkpoint writers.
pub trait MutableCowFile {
    fn allocate_block_id(&mut self) -> Result<BlockID>;
    fn record_gc_block(&mut self, block_id: BlockID);
    fn write_block(
        &self,
        block_id: BlockID,
        buf: DirectBuf,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

/// Shared in-memory active root for copy-on-write files.
///
/// `ActiveRoot<M>` stores generic CoW bookkeeping (`slot_no`, `meta_block_id`,
/// allocation map, GC list) plus a file-specific payload `M`.
#[derive(Clone)]
pub struct ActiveRoot<M> {
    /// Active ping-pong super-page slot (0/1).
    pub slot_no: u64,
    /// Transaction/checkpoint id persisted in current super page.
    pub trx_id: TrxID,
    /// Active meta-page id referenced by the super page.
    pub meta_block_id: BlockID,
    /// Allocation map used for page-id assignment.
    pub alloc_map: AllocMap,
    /// Obsolete meta pages pending reclamation.
    pub gc_block_list: Vec<BlockID>,
    /// File-specific in-memory metadata payload.
    pub meta: M,
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
        }
    }

    /// Clone current root and flip to the opposite ping-pong super-page slot.
    #[inline]
    pub fn flip(&self) -> Self
    where
        M: Clone,
    {
        let mut new = self.clone();
        new.slot_no = 1 - self.slot_no;
        new
    }

    /// Record one obsolete meta-page id for future reclamation.
    ///
    /// This is used during publish so the next root keeps a reclaim list
    /// for historical meta pages.
    #[inline]
    pub fn push_gc_meta_block(&mut self, block_id: BlockID) {
        self.gc_block_list.push(block_id);
    }

    /// Allocate one new page id for copy-on-write publish.
    ///
    /// Returns `None` when the allocation bitmap cannot provide a new page.
    #[inline]
    pub fn try_allocate_block_id(&mut self) -> Option<BlockID> {
        self.alloc_map.try_allocate().map(BlockID::from)
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

/// Decoded meta-page payload used to assemble an active root.
///
/// `parse_meta_block` returns this value so `CowFile` can reconstruct one
/// full [`ActiveRoot`] without trait-based root logic.
pub struct ParsedMeta<M> {
    /// File-specific metadata payload.
    pub meta: M,
    /// Decoded allocation bitmap for the file.
    pub alloc_map: AllocMap,
    /// Decoded obsolete page list.
    pub gc_block_list: Vec<BlockID>,
}

/// Serialization and parsing callbacks for one concrete CoW file type.
///
/// This keeps file-type-specific super/meta codecs explicit without introducing
/// extra trait hierarchies.
#[derive(Clone, Copy)]
pub struct CowCodec<M> {
    /// Parse one super-page image.
    pub parse_super_page: fn(&[u8]) -> Result<SuperPage>,
    /// Parse one meta-page image.
    pub parse_meta_block: fn(BlockID, &[u8]) -> Result<ParsedMeta<M>>,
    /// Validate root invariants after parsing one meta page.
    pub validate_root: fn(BlockID, &ParsedMeta<M>) -> Result<()>,
    /// Build one meta-page image from active root.
    pub build_meta_block: fn(&ActiveRoot<M>) -> Result<DirectBuf>,
    /// Build one super-page image from active root.
    pub build_super_page: fn(&ActiveRoot<M>) -> Result<DirectBuf>,
}

/// Generic copy-on-write file abstraction shared by table and multi-table files.
///
/// `CowFile` owns file IO, active-root pointer management, and generic CoW
/// publish/load flow. Concrete file types provide format-specific behavior via
/// [`CowCodec`].
pub struct CowFile<M> {
    file: SparseFile,
    file_id: PersistedFileID,
    active_root: AtomicPtr<ActiveRoot<M>>,
    mutable_inflight: AtomicBool,
    io_client: AIOClient<TableFsRequest>,
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
        file_id: PersistedFileID,
        io_client: AIOClient<TableFsRequest>,
        codec: CowCodec<M>,
        trunc: bool,
    ) -> Result<Self> {
        let file = if trunc {
            SparseFile::create_or_trunc(file_path, initial_size)
        } else {
            SparseFile::create_or_fail(file_path, initial_size)
        }?;
        Ok(CowFile {
            file,
            file_id,
            active_root: AtomicPtr::new(std::ptr::null_mut()),
            mutable_inflight: AtomicBool::new(false),
            io_client,
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
        file_id: PersistedFileID,
        io_client: AIOClient<TableFsRequest>,
        codec: CowCodec<M>,
    ) -> Result<Self> {
        let file = SparseFile::open(file_path)?;
        Ok(CowFile {
            file,
            file_id,
            active_root: AtomicPtr::new(std::ptr::null_mut()),
            mutable_inflight: AtomicBool::new(false),
            io_client,
            codec,
        })
    }

    /// Return the current in-memory active root reference.
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
    pub async fn write_block(&self, block_id: BlockID, buf: DirectBuf) -> Result<()> {
        debug_assert!(buf.capacity() == COW_FILE_PAGE_SIZE);
        let offset = usize::from(block_id) * COW_FILE_PAGE_SIZE;
        self.write_at_offset(offset, buf).await
    }

    #[inline]
    fn block_key(&self, offset: usize) -> BlockKey {
        BlockKey::new(self.file_id, BlockID::from(offset / COW_FILE_PAGE_SIZE))
    }

    /// Write one buffer at given byte offset.
    #[inline]
    pub(crate) async fn write_at_offset(&self, offset: usize, buf: DirectBuf) -> Result<()> {
        write_direct(
            self.block_key(offset),
            self.file.as_raw_fd(),
            offset,
            buf,
            &self.io_client,
        )
        .await
    }

    #[inline]
    pub(crate) fn raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }

    #[inline]
    pub(crate) async fn queue_read(&self, req: ReadSubmission) -> Result<()> {
        match self.io_client.send_async(TableFsRequest::Read(req)).await {
            Ok(()) => Ok(()),
            Err(err) => {
                let TableFsRequest::Read(req) = err.into_inner() else {
                    unreachable!("table worker returned unexpected readonly load request");
                };
                req.fail(Error::SendError);
                Err(Error::SendError)
            }
        }
    }

    /// Replace active root with new root, returning previous-root guard if present.
    #[inline]
    pub fn swap_active_root(&self, active_root: ActiveRoot<M>) -> Option<OldCowRoot<M>> {
        let new = Self::allocate_active_root(active_root);
        let old = self.active_root.swap(new, Ordering::SeqCst);
        NonNull::new(old).map(OldCowRoot)
    }

    /// Load and validate the active root from on-disk super/meta pages.
    ///
    /// This reads the ping-pong super-page pair from block 0, picks the newest
    /// valid slot, validates the referenced meta page through the configured
    /// codec, and rejects invalid root invariants before returning an
    /// in-memory [`ActiveRoot`].
    ///
    /// This is the intended runtime use of `ReadonlyBufferPool::read_block()`.
    /// The raw block reads here are acceptable because super/meta-page
    /// validation happens immediately through `pick_super_page`,
    /// `parse_meta_block`, and `validate_root`. New callers should prefer
    /// `read_validated_block()` unless they have an equivalent caller-owned
    /// validation path and have reviewed the raw-read inflight semantics.
    #[inline]
    pub async fn load_active_root_from_pool(
        &self,
        disk_pool: &ReadonlyBufferPool,
    ) -> Result<ActiveRoot<M>> {
        let _ = disk_pool.invalidate_block_id(SUPER_BLOCK_ID);
        let pool_guard = disk_pool.pool_guard();
        let super_page_guard = disk_pool.read_block(&pool_guard, SUPER_BLOCK_ID).await?;
        let super_page =
            Self::pick_super_page(super_page_guard.page(), self.codec.parse_super_page)?;
        drop(super_page_guard);

        let _ = disk_pool.invalidate_block_id(super_page.body.meta_block_id);
        let meta_page_guard = disk_pool
            .read_block(&pool_guard, super_page.body.meta_block_id)
            .await?;
        let parsed_meta =
            (self.codec.parse_meta_block)(super_page.body.meta_block_id, meta_page_guard.page())?;
        (self.codec.validate_root)(super_page.body.meta_block_id, &parsed_meta)?;
        drop(meta_page_guard);

        Ok(ActiveRoot::from_parts(
            super_page.header.slot_no,
            super_page.header.checkpoint_cts,
            super_page.body.meta_block_id,
            parsed_meta.alloc_map,
            parsed_meta.gc_block_list,
            parsed_meta.meta,
        ))
    }

    /// Publish a new active root via copy-on-write: meta page then super page.
    ///
    /// The sequence is:
    /// 1. add previous meta page to GC list,
    /// 2. allocate + write new meta page,
    /// 3. write next super page slot,
    /// 4. fsync and atomically swap active root pointer.
    #[inline]
    pub async fn publish_root(&self, mut new_root: ActiveRoot<M>) -> Result<Option<OldCowRoot<M>>> {
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
        self.write_block(new_meta_block_id, meta_buf).await?;

        let super_buf = (self.codec.build_super_page)(&new_root)?;
        let offset = new_root.slot_no as usize * super_buf.capacity();
        self.write_at_offset(offset, super_buf).await?;

        self.fsync()?;
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

    /// Pick latest valid super page from the reserved super-block image.
    #[inline]
    fn pick_super_page(
        buf: &[u8],
        parse_super_page: fn(&[u8]) -> Result<SuperPage>,
    ) -> Result<SuperPage> {
        debug_assert!(buf.len() == SUPER_PAGE_SIZE * 2);
        let first = parse_super_page(&buf[..SUPER_PAGE_SIZE]);
        let second = parse_super_page(&buf[SUPER_PAGE_SIZE..]);
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
    file_kind: PersistedFileKind,
    page_kind: PersistedPageKind,
) -> Result<()> {
    let meta_block_idx = usize::from(meta_block_id);
    if meta_block_idx == usize::from(SUPER_BLOCK_ID)
        || meta_block_idx >= alloc_map.len()
        || !alloc_map.is_allocated(meta_block_idx)
    {
        return Err(Error::persisted_page_corrupted(
            file_kind,
            page_kind,
            meta_block_id,
            PersistedPageCorruptionCause::InvalidRootInvariant,
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
        // SAFETY: old roots are produced by active-root swap and reclaimed once.
        unsafe {
            drop(Box::from_raw(self.0.as_ptr()));
        }
    }
}

// SAFETY: `OldCowRoot` only owns a reclaim-on-drop pointer returned from the
// active-root swap path; moving it transfers that one-shot ownership.
unsafe impl<M: Send> Send for OldCowRoot<M> {}

#[inline]
fn remove_file_by_fd(fd: RawFd) -> std::io::Result<()> {
    let proc_path = format!("/proc/self/fd/{}", fd);
    let real_path = fs::read_link(&proc_path)?;
    fs::remove_file(real_path)
}
