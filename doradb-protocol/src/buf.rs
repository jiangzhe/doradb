use std::alloc::{Layout, alloc};
use std::cell::{Cell, UnsafeCell};
use std::mem::{ManuallyDrop, align_of};
use std::ops::{Deref, DerefMut};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

const VACUUM_RATE: f64 = 0.8;

#[derive(Debug, Error)]
pub enum Error {
    #[error("exceeds capacity with additional {0} bytes")]
    ExceedsCapacity(usize),
    #[error("invalid state")]
    InvalidState,
}

const MASK_UPDATE: usize = 1;
const MASK_WRITE: usize = 2;
const SHL_BITS_READ: usize = 2;

/// ByteBuffer is a single-thread append-only byte buffer.
/// The particular scenario to apply is reading network
/// byte stream and deserialize with zero-copy.
/// In such case, we need a continuous byte array to hold
/// two parts:
/// 1. immutable part for objects that are already deserialized
///    based on.
/// 2. mutable part for incoming bytes.
///
/// The memory layout is:
/// | processed bytes | readable | writable |
///                   ^          ^          ^
///               read idx   write idx    capacity
/// The read operation and write operation do not conflict,
/// but update(allocation and metadata change) operations can not
/// be invoked if read/write is in progress.
/// So there is one mask field to provide runtime guarantee of
/// this invariant.
pub struct ByteBuffer {
    arena: UnsafeCell<Box<[u8]>>,
    // used for read operation.
    r_idx: Cell<usize>,
    // used for write operation.
    w_idx: Cell<usize>,
    // mask to record reader and writer count.
    // least significant bit is write flag.
    // other bits represent reader count.
    mask: Cell<usize>,
    // vacuum threshold
    vacuum_threshold: Cell<usize>,
}

unsafe impl Send for ByteBuffer {}
unsafe impl Sync for ByteBuffer {}

impl ByteBuffer {
    /// Create a new string arena with given capacity.
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        let arena = alloc_arena(cap);
        ByteBuffer {
            arena: UnsafeCell::new(arena),
            r_idx: Cell::new(0),
            w_idx: Cell::new(0),
            mask: Cell::new(0),
            vacuum_threshold: Cell::new(calc_vacuum_threshold(cap)),
        }
    }

    /// Returns remaining capacity that are writable.
    #[inline]
    pub fn remaining_capacity(&self) -> usize {
        self.capacity() - self.w_idx.get()
    }

    /// Clear the content, with only read and write index reset.
    /// This method is unsafe because we must ensure there are no other
    /// immutable references held at same time.
    /// That is usually achieved by calling [`ByteBuffer::write`] method.
    ///
    /// # Safety
    ///
    /// User must guarantee there are no concurrent conflict operations.
    #[inline]
    pub unsafe fn clear(&self) {
        self.r_idx.set(0);
        self.w_idx.set(0);
    }

    /// Vacuum the arena, removing bytes that are already read.
    /// This method is unsafe because we must ensure there are no other
    /// immutable references held at same time.
    /// That is usually achieved by calling [`ByteBuffer::write`] method.
    ///
    /// # Safety
    ///
    /// User must guarantee there are no concurrent conflict operations.
    #[inline]
    pub unsafe fn vacuum(&self) {
        let r_idx = self.r_idx.get();
        if r_idx < self.vacuum_threshold.get() {
            return;
        }
        let w_idx = self.w_idx.get();
        let new_w_idx = w_idx - r_idx;
        unsafe {
            let arena = &mut **self.arena.get();
            arena.copy_within(r_idx..w_idx, 0);
        }
        self.r_idx.set(0);
        self.w_idx.set(new_w_idx);
    }

    /// Reserve additional capacity.
    /// This method is unsafe because we must ensure there are no other
    /// immutable references held at same time.
    /// That is usually achieved by calling [`ByteBuffer::write`] method.
    ///
    /// # Safety
    ///
    /// User must guarantee there are no concurrent conflict operations.
    #[inline]
    pub unsafe fn reserve(&self, len: usize) {
        if self.remaining_capacity() >= len {
            return;
        }
        // As we already own the mutability, we do not need to change read
        let readable = unsafe { self.readable_unchecked() };
        let new_cap = pow2_ge(readable.len() + len);
        let mut new_arena = alloc_arena(new_cap);
        let tgt = &mut *new_arena;
        tgt[..readable.len()].copy_from_slice(readable);
        self.r_idx.set(0);
        self.w_idx.set(readable.len());
        self.vacuum_threshold.set(calc_vacuum_threshold(new_cap));
        unsafe { self.arena.get().swap(&mut new_arena) };
    }

    /// Returns capacity of current arena.
    #[inline]
    pub fn capacity(&self) -> usize {
        unsafe { self.arena.get().as_ref().unwrap().len() }
    }

    /// Renew the arena for future usage.
    /// This is safe because all string refs associated to this arena
    /// must be dropped before this method call.
    #[inline]
    pub fn renew(self) -> Self {
        unsafe { self.clear() };
        // writer does not need to reset because must be false.
        self
    }

    /// Advance write index with given number.
    /// This method only conflict with alloc/meta operations.
    /// So we check first if we can continue.
    #[inline]
    pub fn advance_w_idx(&self, count: usize) -> Result<()> {
        let mask = self.mask.get();
        if mask & MASK_UPDATE == MASK_UPDATE {
            return Err(Error::InvalidState);
        }
        let w_idx = self.w_idx.get();
        if w_idx + count > self.capacity() {
            return Err(Error::InvalidState);
        }
        self.w_idx.set(w_idx + count);
        Ok(())
    }

    /// Returns immutable refernece of the readable area.
    /// This method only conflict with alloc/meta operations.
    /// So we check first if we can continue.
    /// Because immutable reference is returned, we should also
    /// return a read guard to keep the read count more than zero,
    /// so that concurrent alloc/meta operation will fail.
    #[inline]
    pub fn readable(&self) -> Result<ByteBufferReadGuard> {
        let mask = self.mask.get();
        if mask & MASK_UPDATE != 0 {
            // conflict with update
            return Err(Error::InvalidState);
        }
        self.mask.set(mask + (1 << SHL_BITS_READ));
        // # Safety
        //
        // no conflict when reading the buffer.
        let readable = unsafe { self.readable_unchecked() };
        Ok(ByteBufferReadGuard {
            inner: self,
            readers: 1,
            len: 0,
            readable,
        })
    }

    /// Returns immutable refernece of the readable area.
    /// This method is unsafe because we do not check if there are confliect
    /// operations in progress.
    ///
    /// # Safety
    ///
    /// User must guarantee there are no concurrent conflict operations.
    #[inline]
    unsafe fn readable_unchecked(&self) -> &[u8] {
        let w_idx = self.w_idx.get();
        let r_idx = self.r_idx.get();
        unsafe { &(**self.arena.get())[r_idx..w_idx] }
    }

    /// Returns mutable reference of the readable area.
    /// This method is unsafe because we do not check if there are confliect
    /// operations in progress.
    ///
    /// # Safety
    ///
    /// User must guarantee there are no concurrent conflict operations.
    #[allow(clippy::mut_from_ref)]
    #[inline]
    pub unsafe fn readable_mut(&self) -> &mut [u8] {
        let w_idx = self.w_idx.get();
        let r_idx = self.r_idx.get();
        unsafe { &mut (**self.arena.get())[r_idx..w_idx] }
    }

    /// Convenient method to get readable length directly.
    #[inline]
    pub fn readable_len(&self) -> usize {
        let w_idx = self.w_idx.get();
        let r_idx = self.r_idx.get();
        w_idx - r_idx
    }

    /// Acquire update lock.
    /// It will directly fail if the lock conflicts with other concurrent operations.
    #[inline]
    pub fn update(&self) -> Result<ByteBufferUpdateGuard> {
        let mask = self.mask.get();
        if mask != 0 {
            // no reader, no writer, no update
            return Err(Error::InvalidState);
        }
        self.mask.set(mask | MASK_UPDATE);
        Ok(ByteBufferUpdateGuard { inner: self })
    }

    /// Returns write index and writable area.
    /// This is safe because we check the conflicts internally.
    /// A write guard is returned to keep the write flag to true
    /// before dropping.
    #[inline]
    pub fn writable(&self) -> Result<ByteBufferWriteGuard> {
        let mask = self.mask.get();
        if mask & (MASK_UPDATE | MASK_WRITE) != 0 {
            // conflict with write and update
            return Err(Error::InvalidState);
        }
        self.mask.set(mask | MASK_WRITE);
        let w_idx = self.w_idx.get();
        let writable = unsafe { &mut (**self.arena.get())[w_idx..] };
        Ok(ByteBufferWriteGuard {
            inner: self,
            writable,
        })
    }

    /// The capacity of read operations, including
    /// parts of that are readable or writable.
    #[inline]
    pub fn readable_capacity(&self) -> usize {
        let r_idx = self.r_idx.get();
        let cap = self.capacity();
        cap - r_idx
    }

    /// Expose raw pointer and the length of underlying memory area.
    /// This method will leak the memory.
    /// Returns raw pointer, capacity, read index and write index.
    #[inline]
    pub fn into_raw_parts(self) -> (*mut u8, usize, usize, usize) {
        let r_idx = self.r_idx.get();
        let w_idx = self.w_idx.get();
        let mut arena = ManuallyDrop::new(self.arena.into_inner());
        let ptr = arena.as_mut_ptr();
        let len = arena.len();
        (ptr, len, r_idx, w_idx)
    }

    /// Create buffer from raw pointer and length.
    /// User should ensure the arguments are valid.
    ///
    /// # Safety
    ///
    /// User should guarantee the pointer and length are valid.
    #[inline]
    pub unsafe fn from_raw_parts(ptr: *mut u8, len: usize) -> Self {
        let arena = unsafe { Vec::from_raw_parts(ptr, len, len).into_boxed_slice() };
        ByteBuffer {
            arena: UnsafeCell::new(arena),
            r_idx: Cell::new(0),
            w_idx: Cell::new(0),
            mask: Cell::new(0),
            vacuum_threshold: Cell::new(calc_vacuum_threshold(len)),
        }
    }

    /// Add string to buffer.
    /// Return reference and read guard.
    #[inline]
    pub fn add_str(&self, s: impl AsRef<str>) -> Result<(&str, ByteBufferReadGuard)> {
        let (l, g) = self.add_bytes(s.as_ref().as_bytes())?;
        // SAFETY
        //
        // the bytes is identical to original input so it's guaranteed to be valid string.
        let s = unsafe { std::str::from_utf8_unchecked(g.tail(l)) };
        Ok((s, g))
    }

    /// Add bytes to buffer.
    /// Return reference and read guard.
    #[inline]
    pub fn add_bytes(&self, b: impl AsRef<[u8]>) -> Result<(usize, ByteBufferReadGuard)> {
        let b = b.as_ref();
        let rem_cap = self.remaining_capacity();
        if b.len() > rem_cap {
            return Err(Error::ExceedsCapacity(b.len() - rem_cap));
        }
        let mut wg = self.writable()?;
        wg[..b.len()].copy_from_slice(b);
        self.w_idx.set(self.w_idx.get() + b.len()); // advance write index.
        drop(wg);
        // now it's safe to read the content that are just written.
        let rg = self.readable()?;
        Ok((b.len(), rg))
    }

    /// Empty read guard.
    /// It can be used as start point to combine multiple readers.
    #[inline]
    pub fn empty_read(&self) -> ByteBufferReadGuard {
        ByteBufferReadGuard {
            inner: self,
            readers: 0,
            len: 0,
            readable: &[],
        }
    }
}

impl From<Vec<u8>> for ByteBuffer {
    #[inline]
    fn from(src: Vec<u8>) -> Self {
        let mut src = ManuallyDrop::new(src);
        let ptr = src.as_mut_ptr();
        let len = src.len();
        let cap = src.capacity();
        unsafe {
            let slice = std::slice::from_raw_parts_mut(ptr, cap);
            let arena = Box::from_raw(slice);
            ByteBuffer {
                arena: UnsafeCell::new(arena),
                r_idx: Cell::new(0),   // read index set to zero
                w_idx: Cell::new(len), // write index set to length of vector
                mask: Cell::new(0),
                vacuum_threshold: Cell::new(calc_vacuum_threshold(cap)),
            }
        }
    }
}

impl From<ByteBuffer> for Vec<u8> {
    #[inline]
    fn from(src: ByteBuffer) -> Self {
        let (ptr, len, _, w_idx) = src.into_raw_parts();
        // here we ignore read index.
        unsafe { Vec::from_raw_parts(ptr, w_idx, len) }
    }
}

pub struct ByteBufferReadGuard<'a> {
    inner: &'a ByteBuffer,
    readers: usize,
    len: usize,
    readable: &'a [u8],
}

impl<'a> ByteBufferReadGuard<'a> {
    /// Consume the read guard and advance read index of original buffer by given number.
    /// read index update will happen when dropping the guard.
    #[inline]
    pub fn advance(mut self, len: usize) {
        self.len = len;
    }

    #[inline]
    pub fn advance_to_end(mut self) {
        self.len = self.readable.len();
    }

    #[inline]
    pub fn head(&self, len: usize) -> &'a [u8] {
        &self.readable[..len]
    }

    #[inline]
    pub fn after_head(&self, len: usize) -> &'a [u8] {
        &self.readable[len..]
    }

    #[inline]
    pub fn tail(&self, len: usize) -> &'a [u8] {
        &self.readable[self.readable.len() - len..]
    }

    #[inline]
    pub fn all(&self) -> &'a [u8] {
        self.readable
    }
}

impl Deref for ByteBufferReadGuard<'_> {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.readable
    }
}

impl Drop for ByteBufferReadGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        if self.readers == 0 {
            // empty reader, just return
            return;
        }
        let mask = self.inner.mask.get();
        // Reader count must be non-negative.
        assert!(mask >= (self.readers << SHL_BITS_READ));
        self.inner.mask.set(mask - (self.readers << SHL_BITS_READ));
        // read index must be no more than write index.
        assert!(self.inner.r_idx.get() + self.len <= self.inner.w_idx.get());
        self.inner.r_idx.set(self.inner.r_idx.get() + self.len);
    }
}

pub struct ByteBufferWriteGuard<'a> {
    inner: &'a ByteBuffer,
    writable: &'a mut [u8],
}

impl Deref for ByteBufferWriteGuard<'_> {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.writable
    }
}

impl DerefMut for ByteBufferWriteGuard<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.writable
    }
}

impl Drop for ByteBufferWriteGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        let mask = self.inner.mask.get();
        debug_assert!(mask & MASK_WRITE == MASK_WRITE);
        self.inner.mask.set(mask & !MASK_WRITE);
    }
}

pub struct ByteBufferUpdateGuard<'a> {
    inner: &'a ByteBuffer,
}

impl ByteBufferUpdateGuard<'_> {
    #[inline]
    pub fn clear(&mut self) {
        unsafe { self.inner.clear() };
    }

    #[inline]
    pub fn reserve(&mut self, len: usize) {
        unsafe { self.inner.reserve(len) }
    }

    /// Reserve enough capacity.
    /// If new allocation is required, remove all read bytes.
    #[inline]
    pub fn vacuum(&mut self) {
        unsafe { self.inner.vacuum() }
    }

    #[inline]
    pub fn rollback_w_idx(&mut self, len: usize) -> Result<()> {
        let r_idx = self.inner.r_idx.get();
        let w_idx = self.inner.w_idx.get();
        if w_idx < len || r_idx > w_idx - len {
            return Err(Error::InvalidState);
        }
        self.inner.w_idx.set(w_idx - len);
        Ok(())
    }

    #[inline]
    pub fn readable_mut(&mut self) -> &mut [u8] {
        // SAFETY
        //
        // It's safe because we've acquired exclusive access to the buffer.
        unsafe { self.inner.readable_mut() }
    }
}

impl Drop for ByteBufferUpdateGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        let mask = self.inner.mask.get();
        self.inner.mask.set(mask & !MASK_UPDATE);
    }
}

#[inline]
fn alloc_arena(cap: usize) -> Box<[u8]> {
    let layout = Layout::from_size_align(cap, align_of::<u8>()).unwrap();
    unsafe {
        let ptr = alloc(layout);
        let vec = Vec::from_raw_parts(ptr, cap, cap);
        vec.into_boxed_slice()
    }
}

#[inline]
fn pow2_ge(n: usize) -> usize {
    let res = (1 << 63) >> n.leading_zeros();
    if res == n { res } else { res << 1 }
}

#[inline]
fn calc_vacuum_threshold(cap: usize) -> usize {
    (cap as f64 * VACUUM_RATE) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_buffer() {
        let sa = ByteBuffer::with_capacity(12);
        assert_eq!(sa.capacity(), 12);
        assert_eq!(sa.remaining_capacity(), 12);
        let (s1, rg) = sa.add_str("hello").unwrap();
        assert_eq!(s1, "hello");
        assert_eq!(sa.remaining_capacity(), 12 - 5);
        drop(rg);
        let (s2, rg) = sa.add_str("world").unwrap();
        assert_eq!(s2, "world");
        assert!(sa.add_str("rust").is_err());
        drop(rg);
        let sa2 = sa.renew();
        let (b1, rg) = sa2.add_bytes(b"short").unwrap();
        assert_eq!(rg.tail(b1), b"short");
        drop(rg);
        let b2 = sa2.add_bytes(b"veryloooooooooooooooooooooog");
        assert!(b2.is_err());
        drop(b2);
        sa2.update().unwrap().reserve(64);
        assert!(sa2.remaining_capacity() >= 64);
        sa2.update().unwrap().vacuum(); // not fit vacuum condition
        assert_eq!(sa2.remaining_capacity() + 5, sa2.capacity());
        sa2.update().unwrap().clear();
        assert_eq!(sa2.remaining_capacity(), sa2.capacity());
        let (ptr, len, _, _) = sa2.into_raw_parts();
        let sa3 = unsafe { ByteBuffer::from_raw_parts(ptr, len) };
        assert_eq!(sa3.capacity(), len);

        let vec = Vec::from(sa3);
        let sa4 = ByteBuffer::from(vec);
        assert_eq!(sa4.capacity(), len);
    }
}
