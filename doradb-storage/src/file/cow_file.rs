use crate::bitmap::AllocMap;
use crate::buffer::page::{PAGE_SIZE, PageID};
use crate::error::{Error, Result};
use crate::file::super_page::{SUPER_PAGE_SIZE, SuperPage};
use crate::file::{
    FileIO, FileIOResult, FixedSizeBufferFreeList, SparseFile, read_page_direct, write_direct,
};
use crate::io::{AIOBuf, AIOClient, DirectBuf};
use crate::ptr::UnsafePtr;
use crate::trx::TrxID;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::os::fd::{AsRawFd, RawFd};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicPtr, Ordering};

/// Shared page size of CoW table files and multi-table files.
pub const COW_FILE_PAGE_SIZE: usize = PAGE_SIZE;

/// Shared in-memory active root for copy-on-write files.
///
/// `ActiveRoot<M>` stores generic CoW bookkeeping (`page_no`, `meta_page_id`,
/// allocation map, GC list) plus a file-specific payload `M`.
#[derive(Clone)]
pub struct ActiveRoot<M> {
    /// Active ping-pong super-page slot (0/1).
    pub page_no: PageID,
    /// Transaction/checkpoint id persisted in current super page.
    pub trx_id: TrxID,
    /// Active meta-page id referenced by the super page.
    pub meta_page_id: PageID,
    /// Allocation map used for page-id assignment.
    pub alloc_map: AllocMap,
    /// Obsolete meta pages pending reclamation.
    pub gc_page_list: Vec<PageID>,
    /// File-specific in-memory metadata payload.
    pub meta: M,
}

impl<M> ActiveRoot<M> {
    /// Build one active root from already-parsed super/meta payload parts.
    #[inline]
    pub fn from_parts(
        page_no: PageID,
        trx_id: TrxID,
        meta_page_id: PageID,
        alloc_map: AllocMap,
        gc_page_list: Vec<PageID>,
        meta: M,
    ) -> Self {
        ActiveRoot {
            page_no,
            trx_id,
            meta_page_id,
            alloc_map,
            gc_page_list,
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
        new.page_no = 1 - self.page_no;
        new
    }

    /// Record one obsolete meta-page id for future reclamation.
    ///
    /// This is used during publish so the next root keeps a reclaim list
    /// for historical meta pages.
    #[inline]
    pub fn push_gc_meta_page(&mut self, page_id: PageID) {
        self.gc_page_list.push(page_id);
    }

    /// Allocate one new page id for copy-on-write publish.
    ///
    /// Returns `None` when the allocation bitmap cannot provide a new page.
    #[inline]
    pub fn try_allocate_page_id(&mut self) -> Option<PageID> {
        self.alloc_map
            .try_allocate()
            .map(|page_id| page_id as PageID)
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
/// `parse_meta_page` returns this value so `CowFile` can reconstruct one
/// full [`ActiveRoot`] without trait-based root logic.
pub struct ParsedMeta<M> {
    /// File-specific metadata payload.
    pub meta: M,
    /// Decoded allocation bitmap for the file.
    pub alloc_map: AllocMap,
    /// Decoded obsolete page list.
    pub gc_page_list: Vec<PageID>,
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
    pub parse_meta_page: fn(&[u8]) -> Result<ParsedMeta<M>>,
    /// Build one meta-page image from active root.
    pub build_meta_page: fn(&ActiveRoot<M>) -> Result<DirectBuf>,
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
    active_root: AtomicPtr<ActiveRoot<M>>,
    io_client: AIOClient<FileIO>,
    buf_list: FixedSizeBufferFreeList,
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
        io_client: AIOClient<FileIO>,
        buf_list: FixedSizeBufferFreeList,
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
            active_root: AtomicPtr::new(std::ptr::null_mut()),
            io_client,
            buf_list,
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
        io_client: AIOClient<FileIO>,
        buf_list: FixedSizeBufferFreeList,
        codec: CowCodec<M>,
    ) -> Result<Self> {
        let file = SparseFile::open(file_path)?;
        Ok(CowFile {
            file,
            active_root: AtomicPtr::new(std::ptr::null_mut()),
            io_client,
            buf_list,
            codec,
        })
    }

    /// Reusable direct-buffer list bound to this file IO client.
    #[inline]
    pub fn buf_list(&self) -> &FixedSizeBufferFreeList {
        &self.buf_list
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

    /// Read one page using async direct IO.
    #[inline]
    async fn read_page(&self, page_id: PageID) -> Result<DirectBuf> {
        read_page_direct(
            self.file.as_raw_fd(),
            page_id,
            COW_FILE_PAGE_SIZE,
            &self.io_client,
            &self.buf_list,
        )
        .await
    }

    /// Reads one page directly into caller-owned memory.
    ///
    /// # Safety
    ///
    /// Caller must ensure `ptr` points to writable, sector-aligned memory
    /// of at least `COW_FILE_PAGE_SIZE` bytes, and remains valid until async completion.
    #[inline]
    pub async unsafe fn read_page_into_ptr(
        &self,
        page_id: PageID,
        ptr: UnsafePtr<u8>,
    ) -> Result<()> {
        let offset = page_id as usize * COW_FILE_PAGE_SIZE;
        // SAFETY: caller upholds pointer validity/alignment until promise resolves.
        let (fio, promise) = unsafe {
            FileIO::prepare_static_read(self.file.as_raw_fd(), offset, ptr, COW_FILE_PAGE_SIZE)
        };
        if self.io_client.send_async(fio).await.is_err() {
            return Err(Error::SendError);
        }
        let res = promise.wait_async().await;
        match res {
            FileIOResult::ReadStaticOk => Ok(()),
            FileIOResult::ReadOk(_) => panic!("invalid state"),
            FileIOResult::WriteOk => panic!("invalid state"),
            FileIOResult::Err(err) => Err(err.into()),
        }
    }

    /// Write one page using async direct IO.
    #[inline]
    pub async fn write_page(&self, page_id: PageID, buf: DirectBuf) -> Result<()> {
        debug_assert!(buf.capacity() == COW_FILE_PAGE_SIZE);
        let offset = page_id as usize * COW_FILE_PAGE_SIZE;
        self.write_at_offset(offset, buf, true).await
    }

    /// Write one buffer at given byte offset.
    #[inline]
    pub(crate) async fn write_at_offset(
        &self,
        offset: usize,
        buf: DirectBuf,
        recycle: bool,
    ) -> Result<()> {
        write_direct(
            self.file.as_raw_fd(),
            offset,
            buf,
            recycle,
            &self.io_client,
            &self.buf_list,
        )
        .await
    }

    /// Replace active root with new root, returning previous-root guard if present.
    #[inline]
    pub fn swap_active_root(&self, active_root: ActiveRoot<M>) -> Option<OldCowRoot<M>> {
        let new = Self::allocate_active_root(active_root);
        let old = self.active_root.swap(new, Ordering::SeqCst);
        NonNull::new(old).map(OldCowRoot)
    }

    /// Load active root from on-disk super/meta pages.
    ///
    /// This reads super-page pair from page 0, picks the latest valid one,
    /// then parses the referenced meta page with the configured codec.
    #[inline]
    pub async fn load_active_root(&self) -> Result<ActiveRoot<M>> {
        let super_buf = self.read_page(0).await?;
        let super_page =
            match Self::pick_super_page(super_buf.as_bytes(), self.codec.parse_super_page) {
                Ok(super_page) => super_page,
                Err(err) => {
                    self.buf_list.push(super_buf);
                    return Err(err);
                }
            };
        self.buf_list.push(super_buf);

        let meta_buf = self.read_page(super_page.body.meta_page_id).await?;
        let parsed_meta = match (self.codec.parse_meta_page)(meta_buf.as_bytes()) {
            Ok(meta) => meta,
            Err(err) => {
                self.buf_list.push(meta_buf);
                return Err(err);
            }
        };
        self.buf_list.push(meta_buf);

        Ok(ActiveRoot::from_parts(
            super_page.header.page_no,
            super_page.header.checkpoint_cts,
            super_page.body.meta_page_id,
            parsed_meta.alloc_map,
            parsed_meta.gc_page_list,
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
        let old_meta_page_id = new_root.meta_page_id;
        if old_meta_page_id != 0 {
            new_root.push_gc_meta_page(old_meta_page_id);
        }

        let new_meta_page_id = match new_root.try_allocate_page_id() {
            Some(page_id) => page_id,
            None => return Err(Error::InvalidState),
        };
        new_root.meta_page_id = new_meta_page_id;

        let meta_buf = (self.codec.build_meta_page)(&new_root)?;
        self.write_page(new_meta_page_id, meta_buf).await?;

        let super_buf = (self.codec.build_super_page)(&new_root)?;
        let offset = new_root.page_no as usize * super_buf.capacity();
        self.write_at_offset(offset, super_buf, false).await?;

        self.fsync();
        Ok(self.swap_active_root(new_root))
    }

    /// Force all pending writes to disk.
    #[inline]
    pub fn fsync(&self) {
        self.file.syncer().fsync();
    }

    /// Delete underlying file by fd path.
    #[inline]
    pub(crate) fn delete(self) {
        let _ = remove_file_by_fd(self.file.as_raw_fd());
    }

    /// Pick latest valid super page from page-0 image.
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

unsafe impl<M: Send> Send for OldCowRoot<M> {}

#[inline]
fn remove_file_by_fd(fd: RawFd) -> std::io::Result<()> {
    let proc_path = format!("/proc/self/fd/{}", fd);
    let real_path = fs::read_link(&proc_path)?;
    fs::remove_file(real_path)
}
