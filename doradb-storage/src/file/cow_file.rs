use crate::buffer::page::PAGE_SIZE;
use crate::error::{Error, Result};
use crate::file::super_page::SuperPage;
use crate::file::{
    FileIO, FileIOResult, FixedSizeBufferFreeList, SparseFile, read_page_direct, write_direct,
};
use crate::io::{AIOBuf, AIOClient, DirectBuf};
use crate::ptr::UnsafePtr;
use std::fs;
use std::os::fd::{AsRawFd, RawFd};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

/// Shared page size of CoW table files and multi-table files.
pub const COW_FILE_PAGE_SIZE: usize = PAGE_SIZE;

/// Root behavior required by generic CoW file implementation.
pub trait CoWRoot: Clone {
    /// Parsed meta payload referenced by super page.
    type Meta;

    /// Clone current root and flip to the next super-page slot.
    fn flip(&self) -> Self;

    /// Current super-page slot (0/1).
    fn page_no(&self) -> u64;

    /// Current meta-page id.
    fn meta_page_id(&self) -> u64;

    /// Set new meta-page id.
    fn set_meta_page_id(&mut self, page_id: u64);

    /// Record one obsolete meta-page id for future reclamation.
    fn push_gc_meta_page(&mut self, page_id: u64);

    /// Allocate one new page id for CoW publish.
    fn try_allocate_page_id(&mut self) -> Option<u64>;

    /// Pick latest valid super page from page-0 image.
    fn pick_super_page(buf: &[u8]) -> Result<SuperPage>;

    /// Parse meta payload from one meta page.
    fn parse_meta_page(buf: &[u8]) -> Result<Self::Meta>;

    /// Build root object from loaded super page and parsed meta payload.
    fn from_loaded(super_page: SuperPage, meta: Self::Meta) -> Result<Self>;

    /// Serialize current root meta payload into one page.
    fn build_meta_page(&self) -> Result<DirectBuf>;

    /// Serialize current root super page image.
    fn build_super_page(&self) -> Result<DirectBuf>;
}

/// Result type for generic mutable CoW commit.
pub(crate) type CoWCommitResult<F, R> =
    std::result::Result<(Arc<F>, Option<OldCoWRoot<R>>), (Arc<F>, Error)>;

/// Generic copy-on-write file abstraction shared by table and multi-table files.
pub struct CoWFile<R> {
    file: SparseFile,
    active_root: AtomicPtr<R>,
    io_client: AIOClient<FileIO>,
    buf_list: FixedSizeBufferFreeList,
}

impl<R> CoWFile<R> {
    /// Create a new CoW file.
    #[inline]
    pub(crate) fn create(
        file_path: impl AsRef<str>,
        initial_size: usize,
        io_client: AIOClient<FileIO>,
        buf_list: FixedSizeBufferFreeList,
        trunc: bool,
    ) -> Result<Self> {
        let file = if trunc {
            SparseFile::create_or_trunc(file_path, initial_size)
        } else {
            SparseFile::create_or_fail(file_path, initial_size)
        }?;
        Ok(CoWFile {
            file,
            active_root: AtomicPtr::new(std::ptr::null_mut()),
            io_client,
            buf_list,
        })
    }

    /// Open an existing CoW file.
    #[inline]
    pub(crate) fn open(
        file_path: impl AsRef<str>,
        io_client: AIOClient<FileIO>,
        buf_list: FixedSizeBufferFreeList,
    ) -> Result<Self> {
        let file = SparseFile::open(file_path)?;
        Ok(CoWFile {
            file,
            active_root: AtomicPtr::new(std::ptr::null_mut()),
            io_client,
            buf_list,
        })
    }

    /// Reusable direct-buffer list bound to this file IO client.
    #[inline]
    pub fn buf_list(&self) -> &FixedSizeBufferFreeList {
        &self.buf_list
    }

    /// Return current active root reference.
    #[inline]
    pub fn active_root(&self) -> &R {
        Self::active_root_from_raw(self.load_active_root_raw())
    }

    /// Return active-root pointer copy.
    #[inline]
    pub fn active_root_ptr(&self) -> AtomicPtr<R> {
        AtomicPtr::new(self.load_active_root_raw())
    }

    /// Read one page using async direct IO.
    #[inline]
    pub(crate) async fn read_page(&self, page_id: u64) -> Result<DirectBuf> {
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
    pub(crate) async unsafe fn read_page_into_ptr(
        &self,
        page_id: u64,
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
    pub(crate) async fn write_page(&self, page_id: u64, buf: DirectBuf) -> Result<()> {
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

    /// Replace active root with new root, returning previous root guard if present.
    #[inline]
    pub fn swap_active_root(&self, active_root: R) -> Option<OldCoWRoot<R>> {
        let new = Self::allocate_active_root(active_root);
        let old = self.active_root.swap(new, Ordering::SeqCst);
        NonNull::new(old).map(OldCoWRoot)
    }

    /// Force all writes to disk.
    #[inline]
    pub fn fsync(&self) {
        self.file.syncer().fsync();
    }

    /// Delete underlying file by fd path.
    #[inline]
    pub(crate) fn delete(self) {
        let _ = remove_file_by_fd(self.file.as_raw_fd());
    }

    #[inline]
    fn load_active_root_raw(&self) -> *mut R {
        self.active_root.load(Ordering::Relaxed)
    }

    #[inline]
    fn active_root_from_raw<'a>(ptr: *mut R) -> &'a R {
        match NonNull::new(ptr) {
            Some(ptr) => {
                // SAFETY: pointers come from `Box<R>` and are reclaimed only when swapped/dropped.
                unsafe { ptr.as_ref() }
            }
            None => panic!("active root is not initialized"),
        }
    }

    #[inline]
    fn allocate_active_root(active_root: R) -> *mut R {
        Box::into_raw(Box::new(active_root))
    }

    #[inline]
    fn reclaim_active_root(ptr: *mut R) {
        if let Some(ptr) = NonNull::new(ptr) {
            // SAFETY: pointer was allocated by `allocate_active_root` and is reclaimed once.
            unsafe {
                drop(Box::from_raw(ptr.as_ptr()));
            }
        }
    }
}

impl<R: CoWRoot> CoWFile<R> {
    /// Load active root from super/meta pages on disk.
    #[inline]
    pub async fn load_active_root(&self) -> Result<R> {
        let super_buf = self.read_page(0).await?;
        let super_page = match R::pick_super_page(super_buf.as_bytes()) {
            Ok(super_page) => super_page,
            Err(err) => {
                self.buf_list.push(super_buf);
                return Err(err);
            }
        };
        self.buf_list.push(super_buf);

        let meta_buf = self.read_page(super_page.body.meta_page_id).await?;
        let meta = match R::parse_meta_page(meta_buf.as_bytes()) {
            Ok(meta) => meta,
            Err(err) => {
                self.buf_list.push(meta_buf);
                return Err(err);
            }
        };
        self.buf_list.push(meta_buf);

        R::from_loaded(super_page, meta)
    }
}

impl<R> Drop for CoWFile<R> {
    #[inline]
    fn drop(&mut self) {
        Self::reclaim_active_root(self.load_active_root_raw());
    }
}

/// Access trait for wrappers (`TableFile`, `MultiTableFile`) over `CoWFile`.
pub trait CoWFileOwner<R> {
    /// Returns wrapped generic CoW file.
    fn cow_file(&self) -> &CoWFile<R>;
}

/// Generic mutable CoW handle used by wrapper-specific mutable file types.
pub struct MutableCoWFile<F, R> {
    file: Arc<F>,
    new_root: R,
}

impl<F, R> MutableCoWFile<F, R> {
    /// Create mutable CoW handle with caller-provided root snapshot.
    #[inline]
    pub(crate) fn new(file: Arc<F>, new_root: R) -> Self {
        MutableCoWFile { file, new_root }
    }

    /// Returns immutable reference to mutable root snapshot.
    #[inline]
    pub fn root(&self) -> &R {
        &self.new_root
    }

    /// Returns mutable reference to mutable root snapshot.
    #[inline]
    pub fn root_mut(&mut self) -> &mut R {
        &mut self.new_root
    }

    /// Returns wrapped file handle.
    #[inline]
    pub fn file(&self) -> &Arc<F> {
        &self.file
    }

    /// Consume mutable handle and return wrapped file.
    #[inline]
    pub fn into_file(self) -> Arc<F> {
        self.file
    }
}

impl<F, R> MutableCoWFile<F, R>
where
    F: CoWFileOwner<R>,
    R: CoWRoot,
{
    /// Fork a mutable CoW handle from current active root.
    #[inline]
    pub(crate) fn fork(file: &Arc<F>) -> Self {
        MutableCoWFile {
            file: Arc::clone(file),
            new_root: file.cow_file().active_root().flip(),
        }
    }

    /// Commit one CoW root publish: write meta page, write super page, fsync, swap root.
    #[inline]
    pub(crate) async fn commit(self) -> CoWCommitResult<F, R> {
        let MutableCoWFile { file, mut new_root } = self;
        let cow_file = file.cow_file();

        let old_meta_page_id = new_root.meta_page_id();
        if old_meta_page_id != 0 {
            new_root.push_gc_meta_page(old_meta_page_id);
        }

        let new_meta_page_id = match new_root.try_allocate_page_id() {
            Some(page_id) => page_id,
            None => return Err((file, Error::InvalidState)),
        };
        new_root.set_meta_page_id(new_meta_page_id);

        let meta_buf = match new_root.build_meta_page() {
            Ok(meta_buf) => meta_buf,
            Err(err) => return Err((file, err)),
        };
        if let Err(err) = cow_file.write_page(new_meta_page_id, meta_buf).await {
            return Err((file, err));
        }

        let super_buf = match new_root.build_super_page() {
            Ok(super_buf) => super_buf,
            Err(err) => return Err((file, err)),
        };
        let offset = new_root.page_no() as usize * super_buf.capacity();
        if let Err(err) = cow_file.write_at_offset(offset, super_buf, false).await {
            return Err((file, err));
        }

        cow_file.fsync();
        let old_root = cow_file.swap_active_root(new_root);
        Ok((file, old_root))
    }
}

/// Guard object that reclaims previous active root once dropped.
pub struct OldCoWRoot<R>(NonNull<R>);

impl<R> Drop for OldCoWRoot<R> {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: old roots are produced by active-root swap and reclaimed once.
        unsafe {
            drop(Box::from_raw(self.0.as_ptr()));
        }
    }
}

unsafe impl<R: Send> Send for OldCoWRoot<R> {}

#[inline]
fn remove_file_by_fd(fd: RawFd) -> std::io::Result<()> {
    let proc_path = format!("/proc/self/fd/{}", fd);
    let real_path = fs::read_link(&proc_path)?;
    fs::remove_file(real_path)
}
