use crate::konst::{F32_ZERO, F64_ZERO, ValidF32, ValidF64};
use std::alloc::{Layout, alloc, alloc_zeroed};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::mem::{self, ManuallyDrop, MaybeUninit};
use std::ops::{Deref, DerefMut};

/// Extendable byte container.
pub trait BytesExtendable {
    /// Push single byte into the container.
    fn push_byte(&mut self, value: u8);

    /// Extend from a byte slice.
    fn extend_from_byte_slice(&mut self, values: &[u8]);

    fn extend_repeat_n(&mut self, val: u8, n: usize);

    /// Update last byte.
    fn update_last_byte(&mut self, value: u8);
}

impl BytesExtendable for Vec<u8> {
    #[inline]
    fn push_byte(&mut self, value: u8) {
        self.push(value);
    }

    #[inline]
    fn extend_from_byte_slice(&mut self, values: &[u8]) {
        self.extend_from_slice(values);
    }

    #[inline]
    fn extend_repeat_n(&mut self, val: u8, n: usize) {
        self.extend(std::iter::repeat_n(val, n))
    }

    #[inline]
    fn update_last_byte(&mut self, value: u8) {
        *self.last_mut().unwrap() = value;
    }
}

/// Memory comparable format ensure sort result of encoded value is
/// identical to original value.
///
/// 1. Unsigned integer: Use Bigendian encoding.
/// 2. Signed integer: Use Bigendian encoding, then flip most significant bit.
/// 3. Float-point number: Use Bigendian encoding, if positive, flip most significant bit,
///    Otherwise, flip all bits.
/// 4. variable length bytes/string: keep as is.
/// 5. component type.
///    a) Fixed-size types: Use same encoding described above.
///    b) Variable-size types: If it's last key, keep as is. Otherwise, use following encoding.
///
/// Variable-size type encoding:
///
/// 1. Split the value into segments. Each segment has at most 15 bytes.
///    If segment has less than 15 bytes, append 0x00 until it's length equal to 15.
/// 2. Append one byte at end of each segment:
///    a) if original length is less than 15, use original length as the value.
///    b) if original length is equal to 15 but it's not last segment, use 0xFF as the value.
///    c) if original length is equal to 15 but it's last segment, use 15 as the value.
pub trait MemCmpFormat {
    /// Returns estimated length of the type.
    /// This may return None if the length can only be determined by the runtime value.
    fn est_mcf_len() -> Option<usize>;

    /// Returns exact encoded length of this value.
    fn enc_mcf_len(&self) -> usize;

    /// Attach to end of the buffer with the memory comparable format.
    fn extend_mcf_to<T: BytesExtendable>(&self, buf: &mut T);

    /// Write to buffer with memory comparable format.
    /// Client must make sure the buffer length matches the format.
    fn copy_mcf_to(&self, buf: &mut [u8], start_idx: usize) -> usize;
}

pub const NULL_FLAG: u8 = 0x01;
pub const NON_NULL_FLAG: u8 = 0x02;
const FIX_SEG_FLAG: u8 = 0xff;
const SEG_LEN: usize = 15;
pub const MIN_VAR_MCF_LEN: usize = SEG_LEN + 1;
pub const MIN_VAR_NMCF_LEN: usize = MIN_VAR_MCF_LEN + 1;

/// Nullable memory comparable format.
pub trait NullableMemCmpFormat {
    /// Returns estimated length of the type.
    /// Should prepend 1-byte nullable flag, followed by
    /// memory comparable format.
    fn est_nmcf_len() -> Option<usize>;

    /// Returns exact encoded length of this value.
    fn enc_nmcf_len(&self) -> usize;

    /// Attach value to end of the buffer with the memory comparable format.
    fn extend_nmcf_to<T: BytesExtendable>(&self, buf: &mut T);

    /// Write to buffer with nullable memory comparable format.
    /// Client must make sure the buffer length matches the format.
    fn copy_nmcf_to(&self, buf: &mut [u8], start_idx: usize) -> usize;
}

pub struct Null;

impl NullableMemCmpFormat for Null {
    #[inline]
    fn est_nmcf_len() -> Option<usize> {
        Some(1)
    }

    #[inline]
    fn enc_nmcf_len(&self) -> usize {
        1
    }

    #[inline]
    fn extend_nmcf_to<T: BytesExtendable>(&self, buf: &mut T) {
        buf.push_byte(NULL_FLAG);
    }

    #[inline]
    fn copy_nmcf_to(&self, buf: &mut [u8], start_idx: usize) -> usize {
        buf[start_idx] = NULL_FLAG;
        start_idx + 1
    }
}

macro_rules! impl_nmcf_for {
    ($t1:ty) => {
        impl NullableMemCmpFormat for $t1 {
            #[inline]
            fn est_nmcf_len() -> Option<usize> {
                <Self as MemCmpFormat>::est_mcf_len().map(|n| n + 1)
            }

            #[inline]
            fn enc_nmcf_len(&self) -> usize {
                self.enc_mcf_len() + 1
            }

            #[inline]
            fn extend_nmcf_to<T: BytesExtendable>(&self, buf: &mut T) {
                buf.push_byte(NON_NULL_FLAG);
                self.extend_mcf_to(buf);
            }

            #[inline]
            fn copy_nmcf_to(&self, buf: &mut [u8], start_idx: usize) -> usize {
                buf[start_idx] = NON_NULL_FLAG;
                self.copy_mcf_to(buf, start_idx + 1)
            }
        }
    };
}

macro_rules! impl_mcf_for_u {
    ($t1:ty) => {
        impl MemCmpFormat for $t1 {
            #[inline]
            fn est_mcf_len() -> Option<usize> {
                Some(size_of::<$t1>())
            }

            #[inline]
            fn enc_mcf_len(&self) -> usize {
                size_of::<$t1>()
            }

            #[inline]
            fn extend_mcf_to<T: BytesExtendable>(&self, buf: &mut T) {
                let bs = self.to_be_bytes();
                buf.extend_from_byte_slice(&bs);
            }

            #[inline]
            fn copy_mcf_to(&self, buf: &mut [u8], start_idx: usize) -> usize {
                let bs = self.to_be_bytes();
                let end_idx = start_idx + self.enc_mcf_len();
                buf[start_idx..end_idx].copy_from_slice(&bs);
                end_idx
            }
        }
    };
}

impl_mcf_for_u!(u8);
impl_mcf_for_u!(u16);
impl_mcf_for_u!(u32);
impl_mcf_for_u!(u64);
impl_nmcf_for!(u8);
impl_nmcf_for!(u16);
impl_nmcf_for!(u32);
impl_nmcf_for!(u64);

macro_rules! impl_mcf_for_i {
    ($t1:ty) => {
        impl MemCmpFormat for $t1 {
            #[inline]
            fn est_mcf_len() -> Option<usize> {
                Some(size_of::<$t1>())
            }

            #[inline]
            fn enc_mcf_len(&self) -> usize {
                size_of::<$t1>()
            }

            #[inline]
            fn extend_mcf_to<T: BytesExtendable>(&self, buf: &mut T) {
                let mut bs = self.to_be_bytes();
                bs[0] ^= 0x80;
                buf.extend_from_byte_slice(&bs);
            }

            #[inline]
            fn copy_mcf_to(&self, buf: &mut [u8], start_idx: usize) -> usize {
                let mut bs = self.to_be_bytes();
                bs[0] ^= 0x80;
                let end_idx = start_idx + self.enc_mcf_len();
                buf[start_idx..end_idx].copy_from_slice(&bs);
                end_idx
            }
        }
    };
}

impl_mcf_for_i!(i8);
impl_mcf_for_i!(i16);
impl_mcf_for_i!(i32);
impl_mcf_for_i!(i64);
impl_nmcf_for!(i8);
impl_nmcf_for!(i16);
impl_nmcf_for!(i32);
impl_nmcf_for!(i64);

macro_rules! impl_mcf_for_f {
    ($t1:ty, $zero:expr, $mask:expr) => {
        impl MemCmpFormat for $t1 {
            #[inline]
            fn est_mcf_len() -> Option<usize> {
                Some(size_of::<$t1>())
            }

            #[inline]
            fn enc_mcf_len(&self) -> usize {
                size_of::<$t1>()
            }

            #[inline]
            fn extend_mcf_to<T: BytesExtendable>(&self, buf: &mut T) {
                let u = if *self >= $zero {
                    // flip msb
                    self.to_bits() ^ $mask
                } else {
                    // flip all bits
                    !(self.to_bits())
                };
                buf.extend_from_byte_slice(&u.to_be_bytes());
            }

            #[inline]
            fn copy_mcf_to(&self, buf: &mut [u8], start_idx: usize) -> usize {
                let u = if *self >= $zero {
                    self.to_bits() ^ $mask
                } else {
                    !(self.to_bits())
                };
                let end_idx = start_idx + self.enc_mcf_len();
                buf[start_idx..end_idx].copy_from_slice(&u.to_be_bytes());
                end_idx
            }
        }
    };
}

impl_mcf_for_f!(f32, 0.0f32, 0x8000_0000);
impl_mcf_for_f!(f64, 0.0f64, 0x8000_0000_0000_0000);
impl_nmcf_for!(f32);
impl_nmcf_for!(f64);

macro_rules! impl_mcf_for_vf {
    ($t1:ty, $zero:ident, $mask:expr) => {
        impl MemCmpFormat for $t1 {
            #[inline]
            fn est_mcf_len() -> Option<usize> {
                Some(size_of::<$t1>())
            }

            #[inline]
            fn enc_mcf_len(&self) -> usize {
                size_of::<$t1>()
            }

            #[inline]
            fn extend_mcf_to<T: BytesExtendable>(&self, buf: &mut T) {
                let u = if *self >= $zero {
                    // flip msb
                    self.to_bits() ^ $mask
                } else {
                    // flip all bits
                    !(self.to_bits())
                };
                buf.extend_from_byte_slice(&u.to_be_bytes());
            }

            #[inline]
            fn copy_mcf_to(&self, buf: &mut [u8], start_idx: usize) -> usize {
                let u = if *self >= $zero {
                    self.to_bits() ^ $mask
                } else {
                    !(self.to_bits())
                };
                let end_idx = start_idx + self.enc_mcf_len();
                buf[start_idx..end_idx].copy_from_slice(&u.to_be_bytes());
                end_idx
            }
        }
    };
}

impl_mcf_for_vf!(ValidF32, F32_ZERO, 0x8000_0000);
impl_mcf_for_vf!(ValidF64, F64_ZERO, 0x8000_0000_0000_0000);
impl_nmcf_for!(ValidF32);
impl_nmcf_for!(ValidF64);

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SegmentedBytes<'a>(pub &'a [u8]);

impl MemCmpFormat for SegmentedBytes<'_> {
    #[inline]
    fn est_mcf_len() -> Option<usize> {
        None
    }

    #[allow(clippy::manual_div_ceil)]
    #[inline]
    fn enc_mcf_len(&self) -> usize {
        let n_segs = (self.0.len().max(1) + SEG_LEN - 1) / SEG_LEN;
        n_segs * (SEG_LEN + 1)
    }

    #[inline]
    fn extend_mcf_to<T: BytesExtendable>(&self, buf: &mut T) {
        extend_segmented_bytes(self.0, buf)
    }

    #[inline]
    fn copy_mcf_to(&self, buf: &mut [u8], start_idx: usize) -> usize {
        let written = copy_segmented_bytes(self.0, &mut buf[start_idx..]);
        start_idx + written
    }
}

impl_nmcf_for!(SegmentedBytes<'_>);

#[inline]
fn extend_segmented_bytes<T: BytesExtendable>(bs: &[u8], buf: &mut T) {
    if bs.is_empty() {
        buf.extend_from_byte_slice(&[0; SEG_LEN + 1]); // last byte is zero
        return;
    }
    let mut chunks = bs.chunks_exact(SEG_LEN);
    if chunks.remainder().is_empty() {
        for c in chunks {
            buf.extend_from_byte_slice(c);
            buf.push_byte(FIX_SEG_FLAG);
        }
        // update last byte as segment length
        buf.update_last_byte(SEG_LEN as u8);
        // *buf.last_mut().unwrap() = SEG_LEN as u8;
    } else {
        for c in chunks.by_ref() {
            buf.extend_from_byte_slice(c);
            buf.push_byte(FIX_SEG_FLAG);
        }
        buf.extend_from_byte_slice(chunks.remainder());
        buf.extend_repeat_n(0x00, SEG_LEN - chunks.remainder().len());
        buf.push_byte(chunks.remainder().len() as u8);
    }
}

#[inline]
fn copy_segmented_bytes(bs: &[u8], mut buf: &mut [u8]) -> usize {
    if bs.is_empty() {
        buf[..SEG_LEN + 1].iter_mut().for_each(|b| *b = 0);
        return SEG_LEN + 1;
    }
    let mut chunks = bs.chunks_exact(SEG_LEN);
    if chunks.remainder().is_empty() {
        let mut offset = 0;
        for c in chunks {
            buf[offset..offset + SEG_LEN].copy_from_slice(c);
            buf[offset + SEG_LEN] = FIX_SEG_FLAG;
            offset += SEG_LEN + 1;
        }
        // update last byte as segment length
        buf[offset - 1] = SEG_LEN as u8;
        return offset;
    }
    let mut offset = 0usize;
    for c in chunks.by_ref() {
        debug_assert!(c.len() == SEG_LEN);
        buf[..SEG_LEN].copy_from_slice(c);
        buf[SEG_LEN] = FIX_SEG_FLAG;
        buf = &mut buf[SEG_LEN + 1..];
        offset += SEG_LEN + 1
    }
    let rem = chunks.remainder();
    buf[..rem.len()].copy_from_slice(rem);
    buf[rem.len()..SEG_LEN].iter_mut().for_each(|b| *b = 0);
    buf[SEG_LEN] = chunks.remainder().len() as u8;
    offset + SEG_LEN + 1
}

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NormalBytes<'a>(pub &'a [u8]);

impl MemCmpFormat for NormalBytes<'_> {
    #[inline]
    fn est_mcf_len() -> Option<usize> {
        None
    }

    #[inline]
    fn enc_mcf_len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    fn extend_mcf_to<T: BytesExtendable>(&self, buf: &mut T) {
        buf.extend_from_byte_slice(self.0);
    }

    #[inline]
    fn copy_mcf_to(&self, buf: &mut [u8], start_idx: usize) -> usize {
        let end_idx = start_idx + self.0.len();
        buf[start_idx..end_idx].copy_from_slice(self.0);
        end_idx
    }
}

impl_nmcf_for!(NormalBytes<'_>);

pub const MEM_CMP_KEY_LEN: usize = 32;
pub const MEM_CMP_KEY_INLINE: usize = MEM_CMP_KEY_LEN - mem::size_of::<usize>();
pub const MEM_CMP_KEY_HEAP_PREFIX: usize =
    MEM_CMP_KEY_LEN - mem::size_of::<usize>() - mem::size_of::<Box<[u8]>>();

/// MemCmpKey is a key which can be directly memory compared.
/// The underlying implementation is an inline key if length <= 24,
/// or a long key allocated on heap.
///
/// The memory layout is as below.
/// Inline:  | length(8) | data(24) |
/// On-heap: | length(8) | prefix(8) | ptr-to-heap(8) | capacity(8) |
///
/// Two kinds are identified by the length field: inline if length <= 24,
/// otherwise on heap.
///
/// The restriction of comparision is two keys must be of same source type.
/// e.g. Key::from(u32) can not be compared to Key::from(u64).
#[repr(transparent)]
pub struct MemCmpKey(Inner);

impl MemCmpKey {
    /// Get byte slice of the key.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        if self.0.len <= MEM_CMP_KEY_INLINE {
            unsafe { &self.0.u.i[..self.0.len] }
        } else {
            unsafe { &self.0.u.h.data[..self.0.len] }
        }
    }

    /// Create a guard for in-place modification.
    #[inline]
    pub fn modify_inplace(&mut self) -> ModifyInplaceGuard<'_> {
        ModifyInplaceGuard(self)
    }

    /// Returns a new key with all zeroed bytes.
    #[inline]
    pub fn zeroed(len: usize) -> Self {
        if len <= MEM_CMP_KEY_INLINE {
            return MemCmpKey(Inner::inline_zeroed(len));
        }
        MemCmpKey(Inner::heap_alloc(len, true))
    }

    #[inline]
    pub fn arbitrary(len: usize) -> Self {
        if len <= MEM_CMP_KEY_INLINE {
            return MemCmpKey(Inner::inline_zeroed(len));
        }
        MemCmpKey(Inner::heap_alloc(len, false))
    }

    /// Create a empty key.
    #[inline]
    pub fn empty() -> Self {
        MemCmpKey(Inner::inline_zeroed(0))
    }

    /// Get mutable byte slice of the key.
    /// This method is not exposed directly because
    /// we need to maintain prefix if key is on heap.
    /// So we derive a Guard to void miss handling it.
    #[inline]
    fn as_bytes_mut(&mut self) -> &mut [u8] {
        if self.0.len <= MEM_CMP_KEY_INLINE {
            unsafe { &mut self.0.u.i[..self.0.len] }
        } else {
            unsafe { &mut (*self.0.u.h).data[..self.0.len] }
        }
    }

    #[inline]
    fn update_prefix_if_on_heap(&mut self) {
        self.0.update_prefix_if_on_heap();
    }
}

impl From<&[u8]> for MemCmpKey {
    #[inline]
    fn from(value: &[u8]) -> Self {
        if value.len() <= MEM_CMP_KEY_INLINE {
            return MemCmpKey(Inner::inline(value));
        }
        MemCmpKey(Inner::heap(value))
    }
}

impl<const LEN: usize> From<&[u8; LEN]> for MemCmpKey {
    #[inline]
    fn from(value: &[u8; LEN]) -> Self {
        if value.len() <= MEM_CMP_KEY_INLINE {
            return MemCmpKey(Inner::inline(value));
        }
        MemCmpKey(Inner::heap(value))
    }
}

impl From<Nullable<&[u8]>> for MemCmpKey {
    #[inline]
    fn from(value: Nullable<&[u8]>) -> Self {
        // Nullable has one byte prefix so here use less instead of less than.
        if value.0.len() < MEM_CMP_KEY_INLINE {
            return MemCmpKey(Inner::inline_with_nullable_byte(value.0, NON_NULL_FLAG));
        }
        MemCmpKey(Inner::heap_with_nullable_byte(value.0, NON_NULL_FLAG))
    }
}

impl From<Null> for MemCmpKey {
    #[inline]
    fn from(_: Null) -> Self {
        MemCmpKey(Inner::inline(&[NULL_FLAG]))
    }
}

macro_rules! impl_mem_cmp_key_from_non_nullable {
    ($ty:ty) => {
        impl From<$ty> for MemCmpKey {
            #[inline]
            fn from(value: $ty) -> Self {
                let mut bytes = [0u8; mem::size_of::<$ty>()];
                value.copy_mcf_to(&mut bytes, 0);
                MemCmpKey::from(&bytes[..])
            }
        }
    };
}

impl_mem_cmp_key_from_non_nullable!(i8);
impl_mem_cmp_key_from_non_nullable!(i16);
impl_mem_cmp_key_from_non_nullable!(i32);
impl_mem_cmp_key_from_non_nullable!(i64);
impl_mem_cmp_key_from_non_nullable!(u8);
impl_mem_cmp_key_from_non_nullable!(u16);
impl_mem_cmp_key_from_non_nullable!(u32);
impl_mem_cmp_key_from_non_nullable!(u64);
impl_mem_cmp_key_from_non_nullable!(f32);
impl_mem_cmp_key_from_non_nullable!(f64);
impl_mem_cmp_key_from_non_nullable!(ValidF32);
impl_mem_cmp_key_from_non_nullable!(ValidF64);

#[repr(transparent)]
pub struct Nullable<T>(pub T);

macro_rules! impl_mem_cmp_key_from_nullable {
    ($ty:ty) => {
        impl From<Nullable<$ty>> for MemCmpKey {
            #[inline]
            fn from(value: Nullable<$ty>) -> Self {
                let mut bytes = [0u8; mem::size_of::<$ty>() + 1];
                value.0.copy_nmcf_to(&mut bytes, 0);
                MemCmpKey::from(&bytes[..])
            }
        }
    };
}

impl_mem_cmp_key_from_nullable!(i8);
impl_mem_cmp_key_from_nullable!(i16);
impl_mem_cmp_key_from_nullable!(i32);
impl_mem_cmp_key_from_nullable!(i64);
impl_mem_cmp_key_from_nullable!(u8);
impl_mem_cmp_key_from_nullable!(u16);
impl_mem_cmp_key_from_nullable!(u32);
impl_mem_cmp_key_from_nullable!(u64);
impl_mem_cmp_key_from_nullable!(f32);
impl_mem_cmp_key_from_nullable!(f64);
impl_mem_cmp_key_from_nullable!(ValidF32);
impl_mem_cmp_key_from_nullable!(ValidF64);

impl Deref for MemCmpKey {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl Hash for MemCmpKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl PartialEq for MemCmpKey {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes().eq(other.as_bytes())
    }
}

impl Eq for MemCmpKey {}

impl Ord for MemCmpKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl PartialOrd for MemCmpKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl BytesExtendable for MemCmpKey {
    #[inline]
    fn push_byte(&mut self, value: u8) {
        self.0.push_byte(value)
    }

    #[inline]
    fn extend_from_byte_slice(&mut self, values: &[u8]) {
        self.0.extend_from_byte_slice(values);
    }

    #[inline]
    fn extend_repeat_n(&mut self, val: u8, n: usize) {
        self.0.extend_repeat_n(val, n);
    }

    #[inline]
    fn update_last_byte(&mut self, value: u8) {
        self.0.update_last_byte(value);
    }
}

impl fmt::Debug for MemCmpKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_bytes().fmt(f)
    }
}

pub struct ModifyInplaceGuard<'a>(&'a mut MemCmpKey);

impl Deref for ModifyInplaceGuard<'_> {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_bytes()
    }
}

impl DerefMut for ModifyInplaceGuard<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_bytes_mut()
    }
}

impl Drop for ModifyInplaceGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.0.update_prefix_if_on_heap();
    }
}

#[repr(C)]
struct Inner {
    len: usize,
    u: InlineOrHeap,
}

impl Inner {
    #[inline]
    fn inline(value: &[u8]) -> Inner {
        debug_assert!(value.len() <= MEM_CMP_KEY_INLINE);
        let mut i = [0u8; MEM_CMP_KEY_INLINE];
        i[..value.len()].copy_from_slice(value);
        Inner {
            len: value.len(),
            u: InlineOrHeap { i },
        }
    }

    #[inline]
    fn inline_zeroed(len: usize) -> Inner {
        Inner {
            len,
            u: InlineOrHeap {
                i: [0u8; MEM_CMP_KEY_INLINE],
            },
        }
    }

    #[inline]
    fn inline_with_nullable_byte(value: &[u8], b: u8) -> Inner {
        // length should plus 1 for single byte prefix, so replace le with lt.
        debug_assert!(value.len() < MEM_CMP_KEY_INLINE);
        let mut i = [0u8; MEM_CMP_KEY_INLINE];
        i[0] = b;
        i[1..value.len() + 1].copy_from_slice(value);
        Inner {
            len: value.len() + 1,
            u: InlineOrHeap { i },
        }
    }

    #[inline]
    fn heap(value: &[u8]) -> Inner {
        debug_assert!(value.len() > MEM_CMP_KEY_INLINE);
        unsafe {
            let ptr = alloc(Layout::from_size_align_unchecked(value.len(), 1));
            std::ptr::copy_nonoverlapping(value.as_ptr(), ptr, value.len());
            let data = Vec::from_raw_parts(ptr, value.len(), value.len()).into_boxed_slice();
            let mut res = MaybeUninit::<Inner>::uninit();
            let inner = &mut res.assume_init_mut();
            inner.len = value.len();
            inner
                .u
                .update_heap_prefix(&value[..MEM_CMP_KEY_HEAP_PREFIX]);
            inner.u.init_heap_data(data);
            res.assume_init()
        }
    }

    #[inline]
    fn heap_alloc(len: usize, zeroed: bool) -> Inner {
        debug_assert!(len > MEM_CMP_KEY_INLINE);
        unsafe {
            let layout = Layout::from_size_align_unchecked(len, 1);
            let ptr = if zeroed {
                alloc_zeroed(layout)
            } else {
                alloc(layout)
            };
            let data = Vec::from_raw_parts(ptr, len, len).into_boxed_slice();
            Inner {
                len,
                u: InlineOrHeap {
                    h: ManuallyDrop::new(Heap {
                        data,
                        prefix: [0u8; MEM_CMP_KEY_HEAP_PREFIX],
                    }),
                },
            }
        }
    }

    #[inline]
    fn heap_with_nullable_byte(value: &[u8], b: u8) -> Inner {
        let len = value.len() + 1;
        debug_assert!(len > MEM_CMP_KEY_INLINE);
        unsafe {
            let ptr = alloc(Layout::from_size_align_unchecked(len, 1));
            // update first byte.
            *ptr = b;
            // copy data.
            std::ptr::copy_nonoverlapping(value.as_ptr(), ptr.add(1), value.len());
            let data = Vec::from_raw_parts(ptr, len, len).into_boxed_slice();
            let mut res = MaybeUninit::<Inner>::uninit();
            let inner = &mut res.assume_init_mut();
            inner.len = len;
            (*inner.u.h).prefix[0] = b;
            (*inner.u.h).prefix[1..].copy_from_slice(&value[..MEM_CMP_KEY_HEAP_PREFIX - 1]);
            inner.u.init_heap_data(data);
            res.assume_init()
        }
    }

    #[inline]
    fn update_prefix_if_on_heap(&mut self) {
        if self.len > MEM_CMP_KEY_INLINE {
            unsafe {
                let h = &mut *self.u.h;
                let bs = &h.data[..MEM_CMP_KEY_HEAP_PREFIX];
                h.prefix.copy_from_slice(bs);
            }
        }
    }
}

impl BytesExtendable for Inner {
    #[inline]
    fn push_byte(&mut self, value: u8) {
        match self.len.cmp(&MEM_CMP_KEY_INLINE) {
            Ordering::Less => {
                unsafe { self.u.i[self.len] = value };
                self.len += 1;
            }
            Ordering::Equal => {
                // allocate twice size as current.
                unsafe {
                    let ptr = alloc(Layout::from_size_align_unchecked(self.len * 2, 1));
                    std::ptr::copy_nonoverlapping(self.u.i.as_ptr(), ptr, self.len);
                    *ptr.add(self.len) = value;
                    let data = Vec::from_raw_parts(ptr, self.len, self.len).into_boxed_slice();
                    self.len += 1;
                    self.u.init_heap_data(data);
                    // it's ok to not update prefix, because prefix has same layout as inline bytes.
                }
            }
            Ordering::Greater => unsafe {
                let len = self.len;
                let data = &mut (*self.u.h).data;
                if data.len() > len {
                    // sufficient capacity.
                    data[len] = value;
                    self.len += 1;
                } else {
                    let new_len = len * 2;
                    let ptr = alloc(Layout::from_size_align_unchecked(new_len, 1));
                    std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, len);
                    *ptr.add(len) = value;
                    self.len += 1;
                    let old_box = std::mem::replace(
                        data,
                        Vec::from_raw_parts(ptr, new_len, new_len).into_boxed_slice(),
                    );
                    drop(old_box); // explicitly drop the old box.
                    // prefix not changed.
                }
            },
        }
    }

    #[inline]
    fn extend_from_byte_slice(&mut self, values: &[u8]) {
        unsafe {
            let old_len = self.len;
            let new_len = old_len + values.len();
            if old_len <= MEM_CMP_KEY_INLINE {
                // original collection is inline.
                if new_len > MEM_CMP_KEY_INLINE {
                    // new collection is on heap.
                    let ptr = alloc(Layout::from_size_align_unchecked(new_len, 1));
                    std::ptr::copy_nonoverlapping(self.u.i.as_ptr(), ptr, old_len);
                    std::ptr::copy_nonoverlapping(values.as_ptr(), ptr.add(old_len), values.len());
                    self.len = new_len;
                    // need to discard old value in data
                    std::ptr::write(
                        &mut (*self.u.h).data,
                        Vec::from_raw_parts(ptr, new_len, new_len).into_boxed_slice(),
                    );
                    self.update_prefix_if_on_heap();
                    return;
                }
                // new collection is still inline.
                self.u.i[old_len..new_len].copy_from_slice(values);
                self.len = new_len;
                return;
            }
            // copy data on heap.
            let data = &mut (*self.u.h).data;
            if data.len() >= new_len {
                // capacity is enough.
                data[old_len..new_len].copy_from_slice(values);
                self.len = new_len;
                return;
            }
            // Capacity is not enough.
            // Double the old capacity to avoid frequent reallocations with small extentions.
            let new_cap = new_len.max(old_len * 2);
            let ptr = alloc(Layout::from_size_align_unchecked(new_cap, 1));
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, old_len);
            std::ptr::copy_nonoverlapping(values.as_ptr(), ptr.add(old_len), values.len());
            let data = Vec::from_raw_parts(ptr, new_cap, new_cap).into_boxed_slice();
            self.len = new_len;
            self.u.replace_heap_data(data);
            // prefix not changed.
        }
    }

    #[inline]
    fn extend_repeat_n(&mut self, val: u8, n: usize) {
        unsafe {
            let old_len = self.len;
            let new_len = old_len + n;
            if old_len <= MEM_CMP_KEY_INLINE {
                // original collection is inline.
                if new_len > MEM_CMP_KEY_INLINE {
                    // new collection is on heap.
                    let ptr = alloc(Layout::from_size_align_unchecked(new_len, 1));
                    std::ptr::copy_nonoverlapping(self.u.i.as_ptr(), ptr, old_len);
                    std::ptr::write_bytes(ptr.add(old_len), val, n);
                    self.len = new_len;
                    // need to discard old value in data
                    std::ptr::write(
                        &mut (*self.u.h).data,
                        Vec::from_raw_parts(ptr, new_len, new_len).into_boxed_slice(),
                    );
                    self.update_prefix_if_on_heap();
                    return;
                }
                // new collection is still inline.
                self.u.i[old_len..new_len].fill(val);
                self.len = new_len;
                return;
            }
            // copy data on heap.
            let data = &mut (*self.u.h).data;
            if data.len() >= new_len {
                // capacity is enough.
                data[old_len..new_len].fill(val);
                self.len = new_len;
                return;
            }
            // Capacity is not enough.
            // Double the old capacity to avoid frequent reallocations with small extentions.
            let new_cap = new_len.max(old_len * 2);
            let ptr = alloc(Layout::from_size_align_unchecked(new_cap, 1));
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, old_len);
            std::ptr::write_bytes(ptr.add(old_len), val, n);
            let data = Vec::from_raw_parts(ptr, new_cap, new_cap).into_boxed_slice();
            self.len = new_len;
            self.u.replace_heap_data(data);
            // prefix not changed.
        }
    }

    #[inline]
    fn update_last_byte(&mut self, value: u8) {
        unsafe {
            if self.len <= MEM_CMP_KEY_INLINE {
                self.u.i[self.len - 1] = value;
            } else {
                let len = self.len;
                (*self.u.h).data[len - 1] = value;
            }
        }
    }
}

impl Drop for Inner {
    #[inline]
    fn drop(&mut self) {
        if self.len <= MEM_CMP_KEY_INLINE {
            return;
        }
        unsafe {
            ManuallyDrop::drop(&mut self.u.h);
        }
    }
}

unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

union InlineOrHeap {
    i: [u8; MEM_CMP_KEY_INLINE],
    h: ManuallyDrop<Heap>,
}

impl InlineOrHeap {
    /// # Safety
    ///
    /// Caller must guarantee the heap data is uninitialized.
    /// This method will not read or drop old value in data field.
    #[inline]
    unsafe fn init_heap_data(&mut self, data: Box<[u8]>) {
        unsafe { std::ptr::write(&mut (*self.h).data, data) };
    }

    #[inline]
    fn update_heap_prefix(&mut self, prefix: &[u8]) {
        debug_assert!(prefix.len() == MEM_CMP_KEY_HEAP_PREFIX);
        unsafe { (*self.h).prefix.copy_from_slice(prefix) };
    }

    /// # Safety
    ///
    /// Caller must guarantee the heap data is already intiailized.
    /// This method will replace old data with new data, and drop old
    /// data immediately.
    #[inline]
    unsafe fn replace_heap_data(&mut self, data: Box<[u8]>) {
        unsafe {
            let old_data = std::ptr::replace(&mut (*self.h).data, data);
            drop(old_data);
        }
    }
}

#[repr(C)]
struct Heap {
    prefix: [u8; MEM_CMP_KEY_HEAP_PREFIX],
    // Use boxed slice because we need capacity when extend the key.
    data: Box<[u8]>,
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use rand::rngs::ThreadRng;
    use rand_distr::{Distribution, StandardUniform};

    use super::*;

    #[test]
    fn test_mcf_sized() {
        // int
        run_test_mcf::<u8>();
        run_test_mcf::<u16>();
        run_test_mcf::<u32>();
        run_test_mcf::<u64>();

        run_test_mcf::<i8>();
        run_test_mcf::<i16>();
        run_test_mcf::<i32>();
        run_test_mcf::<i64>();

        // float
        run_test_mcf::<ValidF64>();

        // int + int
        run_test_mcf2::<i32, i32>();
        run_test_mcf2::<u32, u32>();
        run_test_mcf2::<i64, i64>();
        run_test_mcf2::<u64, u64>();

        // int + float
        run_test_mcf2::<i32, ValidF64>();
        run_test_mcf2::<ValidF64, u32>();
        run_test_mcf2::<u64, ValidF64>();
        run_test_mcf2::<ValidF64, i64>();
    }

    #[test]
    fn test_mcf_varlen() {
        run_test_mcf_varlen(gen_rand_bytes);
    }

    #[test]
    fn test_mcf_float() {
        let f0 = ValidF64::new(-1.0).unwrap();
        let mut buf0 = vec![];
        f0.extend_mcf_to(&mut buf0);
        assert_eq!(buf0.len(), 8);
        assert!(buf0[0] & 0x80 == 0);

        let f1 = ValidF64::new(-1.0).unwrap();
        let mut buf1 = vec![];
        NullableMemCmpFormat::extend_nmcf_to(&f1, &mut buf1);
        assert_eq!(buf1.len(), 9);
        assert_eq!(buf1[0], NON_NULL_FLAG);
        assert!(buf1[1] & 0x80 == 0);
    }

    fn run_test_mcf<T>()
    where
        T: MemCmpFormat + NullableMemCmpFormat + Ord,
        StandardUniform: Distribution<T>,
    {
        let mut r = rand::rng();
        let mut input1 = gen_input::<T>(&mut r);

        check_mcf_length(&input1[0]);

        let mut input2 = encode_mcf_input(&input1);

        sort_and_check_mcf(&mut input1, &mut input2);

        let mut input3 = gen_input::<T>(&mut r);

        check_nmcf_length(&input3[0]);

        let mut input4 = encode_nmcf_input(&input3);

        sort_and_check_nmcf(&mut input3, &mut input4);
    }

    fn run_test_mcf2<T, U>()
    where
        T: MemCmpFormat + NullableMemCmpFormat + Ord,
        U: MemCmpFormat + NullableMemCmpFormat + Ord,
        StandardUniform: Distribution<T> + Distribution<U>,
    {
        let mut r = rand::rng();

        // mcf
        let mut input1 = Vec::<(T, U)>::with_capacity(1024);
        for _ in 0..1024 {
            input1.push(r.sample(StandardUniform));
        }
        let mut input2 = Vec::with_capacity(1024);
        for (t, u) in &input1 {
            let mut buf = Vec::with_capacity(T::enc_mcf_len(t) + U::enc_mcf_len(u));
            t.extend_mcf_to(&mut buf);
            u.extend_mcf_to(&mut buf);
            input2.push(buf);
        }
        input1.sort();
        input2.sort();

        for ((t, u), a) in input1.iter().zip(input2) {
            let mut buf = Vec::with_capacity(T::enc_mcf_len(t) + U::enc_mcf_len(u));
            t.extend_mcf_to(&mut buf);
            u.extend_mcf_to(&mut buf);
            assert_eq!(buf, a);
        }

        // nmcf
        let mut input3 = Vec::<(T, U)>::with_capacity(1024);
        for _ in 0..1024 {
            input3.push(r.sample(StandardUniform));
        }
        let mut input4 = Vec::with_capacity(1024);
        for (t, v) in &input3 {
            let mut buf = Vec::with_capacity(T::enc_nmcf_len(t) + U::enc_nmcf_len(v));
            T::extend_nmcf_to(t, &mut buf);
            U::extend_nmcf_to(v, &mut buf);
            input4.push(buf);
        }
        input3.sort();
        input4.sort();

        for ((t, v), a) in input3.iter().zip(input4) {
            let mut buf = Vec::with_capacity(T::enc_nmcf_len(t) + U::enc_nmcf_len(v));
            T::extend_nmcf_to(t, &mut buf);
            U::extend_nmcf_to(v, &mut buf);
            assert_eq!(buf, a);
        }
    }

    fn run_test_mcf_varlen<F>(f: F)
    where
        F: Fn(&mut ThreadRng) -> Vec<u8>,
        F: Copy,
    {
        let mut r = rand::rng();

        let mut input1 = gen_varlen_input(&mut r, f);

        check_mcf_length(&SegmentedBytes(&input1[0]));

        let mut input2 = encode_varlen_mcf_input(&input1);

        sort_and_check_varlen_mcf(&mut input1, &mut input2);

        let input3 = gen_varlen_input(&mut r, f);

        let mut input3: Vec<_> = input3.iter().map(|v| SegmentedBytes(v)).collect();

        check_nmcf_length(&input3[0]);

        let mut input4 = encode_varlen_nmcf_input(&input3);

        sort_and_check_varlen_nmcf(&mut input3, &mut input4);
    }

    fn check_mcf_length<T>(value: &T)
    where
        T: MemCmpFormat + Ord + ?Sized,
    {
        if let Some(el) = T::est_mcf_len() {
            assert_eq!(el, T::enc_mcf_len(value));
        }
        let mut buf = vec![];
        value.extend_mcf_to(&mut buf);
        assert_eq!(buf.len(), T::enc_mcf_len(value));
    }

    fn check_nmcf_length<T>(value: &T)
    where
        T: NullableMemCmpFormat + Ord,
    {
        if let Some(el) = <SegmentedBytes as NullableMemCmpFormat>::est_nmcf_len() {
            assert_eq!(el, value.enc_nmcf_len());
        }
        let mut buf = vec![];
        value.extend_nmcf_to(&mut buf);
        assert_eq!(buf.len(), value.enc_nmcf_len());
    }

    fn gen_input<T>(r: &mut ThreadRng) -> Vec<T>
    where
        StandardUniform: Distribution<T>,
    {
        let mut input = Vec::with_capacity(1024);
        for _ in 0..1024 {
            input.push(r.sample(StandardUniform));
        }
        input
    }

    fn gen_varlen_input<U, F>(r: &mut ThreadRng, f: F) -> Vec<U>
    where
        F: Fn(&mut ThreadRng) -> U,
    {
        let mut input = Vec::with_capacity(1024);
        for _ in 0..1024 {
            input.push(f(r));
        }
        input
    }

    fn encode_mcf_input<T>(input: &[T]) -> Vec<Vec<u8>>
    where
        T: MemCmpFormat + Ord,
    {
        let mut input2 = Vec::with_capacity(1024);
        for i in input {
            let mut buf = Vec::with_capacity(T::enc_mcf_len(i));
            i.extend_mcf_to(&mut buf);
            // identical with write_mcf
            let mut buf2 = vec![0u8; T::enc_mcf_len(i)];
            i.copy_mcf_to(&mut buf2, 0);
            assert_eq!(buf, buf2);
            input2.push(buf);
        }
        input2
    }

    fn encode_varlen_mcf_input(input: &[Vec<u8>]) -> Vec<Vec<u8>> {
        let mut input2 = Vec::with_capacity(1024);
        for i in input {
            let mut buf = Vec::with_capacity(SegmentedBytes(i).enc_mcf_len());
            SegmentedBytes(i).extend_mcf_to(&mut buf);
            input2.push(buf);
        }
        input2
    }

    fn encode_nmcf_input<T>(input: &[T]) -> Vec<Vec<u8>>
    where
        T: NullableMemCmpFormat + Ord,
    {
        let mut input2 = Vec::with_capacity(1024);
        for i in input {
            let mut buf = Vec::with_capacity(T::enc_nmcf_len(i));
            T::extend_nmcf_to(i, &mut buf);
            input2.push(buf);
        }
        input2
    }

    fn encode_varlen_nmcf_input<T>(input: &[T]) -> Vec<Vec<u8>>
    where
        T: NullableMemCmpFormat + Ord,
    {
        let mut input2 = Vec::with_capacity(1024);
        for i in input {
            let mut buf = Vec::with_capacity(i.enc_nmcf_len());
            i.extend_nmcf_to(&mut buf);
            input2.push(buf);
        }
        input2
    }

    fn sort_and_check_mcf<T>(in1: &mut [T], in2: &mut [Vec<u8>])
    where
        T: MemCmpFormat + Ord,
    {
        in1.sort_by(|a, b| a.cmp(b));
        in2.sort();

        for (e, a) in in1.iter().zip(in2.iter()) {
            let mut buf = Vec::with_capacity(T::enc_mcf_len(e));
            e.extend_mcf_to(&mut buf);
            assert_eq!(&buf, a);
        }
    }

    fn sort_and_check_varlen_mcf(in1: &mut [Vec<u8>], in2: &mut [Vec<u8>]) {
        in1.sort_by(|a, b| a.cmp(b));
        in2.sort();

        for (e, a) in in1.iter().zip(in2.iter()) {
            let mut buf = Vec::with_capacity(SegmentedBytes(e).enc_mcf_len());
            SegmentedBytes(e).extend_mcf_to(&mut buf);
            assert_eq!(&buf, a);
        }
    }

    fn sort_and_check_nmcf<T>(in1: &mut [T], in2: &mut [Vec<u8>])
    where
        T: NullableMemCmpFormat + Ord,
    {
        in1.sort_by(|a, b| a.cmp(b));
        in2.sort();

        for (e, a) in in1.iter().zip(in2.iter()) {
            let mut buf = Vec::with_capacity(NullableMemCmpFormat::enc_nmcf_len(e));
            NullableMemCmpFormat::extend_nmcf_to(e, &mut buf);
            assert_eq!(&buf, a);
        }
    }

    fn sort_and_check_varlen_nmcf(in1: &mut [SegmentedBytes], in2: &mut [Vec<u8>]) {
        in1.sort_by(|a, b| a.cmp(b));
        in2.sort();

        for (e, a) in in1.iter().zip(in2.iter()) {
            let mut buf = Vec::with_capacity(e.enc_nmcf_len());
            e.extend_nmcf_to(&mut buf);
            assert_eq!(&buf, a);
        }
    }

    fn gen_rand_bytes(r: &mut ThreadRng) -> Vec<u8> {
        let len: u8 = r.sample(StandardUniform);
        let mut s = Vec::with_capacity(len as usize);
        for _ in 0..len {
            s.push(r.sample(StandardUniform));
        }
        s
    }

    impl Distribution<ValidF64> for StandardUniform {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> ValidF64 {
            ValidF64::new(rng.sample(StandardUniform)).unwrap()
        }
    }

    #[test]
    fn test_mem_cmp_key() {
        // inline key
        let k1 = MemCmpKey::from(&[1u8; 10]);
        assert_eq!(k1.as_bytes(), &[1u8; 10]);
        assert!(k1.0.len <= MEM_CMP_KEY_INLINE);

        // heap key
        let k2 = MemCmpKey::from(&[1u8; 30]);
        assert_eq!(k2.as_bytes(), &[1u8; 30]);
        assert!(k2.0.len > MEM_CMP_KEY_INLINE);

        // empty key
        let k3 = MemCmpKey::empty();
        assert_eq!(k3.as_bytes(), &[]);

        // zeroed key
        let k4 = MemCmpKey::zeroed(10);
        assert_eq!(k4.as_bytes(), &[0u8; 10]);
        let k5 = MemCmpKey::zeroed(30);
        assert_eq!(k5.as_bytes(), &[0u8; 30]);

        // from various types
        let k6 = MemCmpKey::from(10i32);
        let mut buf = vec![];
        10i32.extend_mcf_to(&mut buf);
        assert_eq!(k6.as_bytes(), &buf);

        let k7 = MemCmpKey::from(Nullable(10i32));
        let mut buf = vec![];
        10i32.extend_nmcf_to(&mut buf);
        assert_eq!(k7.as_bytes(), &buf);

        let k8 = MemCmpKey::from(Null);
        assert_eq!(k8.as_bytes(), &[NULL_FLAG]);

        // extend
        let mut k9 = MemCmpKey::from(&[1u8; 10]);
        k9.extend_from_byte_slice(&[2u8; 10]);
        assert_eq!(
            k9.as_bytes(),
            &[1u8, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]
        );
        assert!(k9.0.len <= MEM_CMP_KEY_INLINE);

        let mut k10 = MemCmpKey::from(&[1u8; 20]);
        k10.extend_from_byte_slice(&[2u8; 10]);
        let mut expected = vec![1u8; 20];
        expected.extend_from_slice(&[2u8; 10]);
        assert_eq!(k10.as_bytes(), &expected);
        assert!(k10.0.len > MEM_CMP_KEY_INLINE);

        // cmp
        let k11 = MemCmpKey::from(&[1u8; 10]);
        let k12 = MemCmpKey::from(&[1u8; 10]);
        assert_eq!(k11, k12);

        let k13 = MemCmpKey::from(&[1u8; 10]);
        let k14 = MemCmpKey::from(&[2u8; 10]);
        assert!(k13 < k14);

        let k15 = MemCmpKey::from(Nullable(&[1u8; 10][..]));
        let k16 = MemCmpKey::from(Nullable(&[2u8; 50][..]));
        assert!(k15 < k16);
        println!("k15 = {:?}", k15);

        // extenable
        let mut k17 = MemCmpKey::empty();
        k17.push_byte(0x00);
        k17.extend_repeat_n(0x01, 3);
        k17.update_last_byte(0x02);
        assert!(!k17.is_empty());
        assert!(k17.len() == 4);
        assert!(k17.as_bytes() == &[0x00, 0x01, 0x01, 0x02]);

        let mut k17 = MemCmpKey::empty();
        for _ in 0..64 {
            k17.push_byte(0x01);
        }
        assert!(k17.len() == 64);
        assert!(k17.as_bytes().iter().all(|b| *b == 0x01));
    }

    #[test]
    fn test_mem_cmp_key_drop() {
        // This test will panic if double free occurs
        let k = MemCmpKey::from(&[1u8; 25]);
        drop(k); // Explicit drop to test
    }

    #[test]
    fn test_mem_cmp_null() {
        assert!(Null::est_nmcf_len() == Some(1));
        assert!(Null.enc_nmcf_len() == 1);
        let mut buf = vec![];
        Null.extend_nmcf_to(&mut buf);
        assert!(buf.len() == 1);
        assert!(buf[0] == NULL_FLAG);
        let end_idx = Null.copy_nmcf_to(&mut buf, 0);
        assert!(end_idx == 1);
        assert!(buf[0] == NULL_FLAG);
    }

    #[test]
    fn test_mem_cmp_normal_bytes() {
        assert!(NormalBytes::est_mcf_len() == None);
        assert!(NormalBytes::est_nmcf_len() == None);
        let mut buf = vec![];
        NormalBytes(b"hello").extend_mcf_to(&mut buf);
        assert!(&buf[..] == b"hello");
    }

    #[test]
    fn test_mem_cmp_segmented_bytes() {
        let mut buf = vec![];
        // empty slice
        let sb = SegmentedBytes(&[0u8; 0]);

        buf.clear();
        sb.extend_mcf_to(&mut buf);
        assert!(buf.len() == SEG_LEN + 1);
        assert!(buf.iter().all(|v| *v == 0));
        let end_idx = sb.copy_mcf_to(&mut buf, 0);
        assert!(end_idx == SEG_LEN + 1);
        assert!(buf.iter().all(|v| *v == 0));

        buf.clear();
        sb.extend_nmcf_to(&mut buf);
        assert!(buf.len() == SEG_LEN + 1 + 1);
        assert!(buf[0] == NON_NULL_FLAG);
        assert!(buf[1..].iter().all(|v| *v == 0));
        let end_idx = sb.copy_nmcf_to(&mut buf, 0);
        assert!(end_idx == SEG_LEN + 1 + 1);
        assert!(buf[0] == NON_NULL_FLAG);
        assert!(buf[1..].iter().all(|v| *v == 0));

        // slice with mulit-segment length
        let sb = SegmentedBytes(&[0u8; SEG_LEN]);

        buf.clear();
        sb.extend_mcf_to(&mut buf);
        assert!(buf.len() == SEG_LEN + 1);
        assert!(buf[..buf.len() - 1].iter().all(|v| *v == 0));
        assert!(*buf.last().unwrap() == SEG_LEN as u8);
        let end_idx = sb.copy_mcf_to(&mut buf, 0);
        assert!(end_idx == SEG_LEN + 1);
        assert!(buf[..buf.len() - 1].iter().all(|v| *v == 0));
        assert!(*buf.last().unwrap() == SEG_LEN as u8);

        buf.clear();
        sb.extend_nmcf_to(&mut buf);
        assert!(buf.len() == SEG_LEN + 1 + 1);
        assert!(buf[0] == NON_NULL_FLAG);
        assert!(buf[1..buf.len() - 1].iter().all(|v| *v == 0));
        assert!(*buf.last().unwrap() == SEG_LEN as u8);
        let end_idx = sb.copy_nmcf_to(&mut buf, 0);
        assert!(end_idx == SEG_LEN + 1 + 1);
        assert!(buf[0] == NON_NULL_FLAG);
        assert!(buf[1..buf.len() - 1].iter().all(|v| *v == 0));
        assert!(*buf.last().unwrap() == SEG_LEN as u8);
    }
}
