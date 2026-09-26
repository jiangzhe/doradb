use std::alloc::{Layout, alloc};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::repeat_n;
use std::mem::size_of;
use std::mem::{self, ManuallyDrop, replace};
use std::ops::{Deref, DerefMut};
use std::ptr::{copy_nonoverlapping, replace as replace_ptr, write_bytes};

/// Nullable memcmp encoding marker for null values.
pub(crate) const NULL_FLAG: u8 = 0x01;
/// Nullable memcmp encoding marker for non-null values.
pub(crate) const NON_NULL_FLAG: u8 = 0x02;
const FIX_SEG_FLAG: u8 = 0xff;
const SEG_LEN: usize = 15;
/// Minimum encoded length for a variable-width memcmp value.
pub(crate) const MIN_VAR_MCF_LEN: usize = SEG_LEN + 1;
/// Minimum encoded length for a nullable variable-width memcmp value.
pub(crate) const MIN_VAR_NMCF_LEN: usize = MIN_VAR_MCF_LEN + 1;
/// Inline-or-heap memcmp key header size.
pub(crate) const MEM_CMP_KEY_LEN: usize = 32;
/// Maximum bytes stored inline in a memcmp key.
pub(crate) const MEM_CMP_KEY_INLINE: usize = MEM_CMP_KEY_LEN - size_of::<usize>();
/// Prefix bytes mirrored in heap-backed memcmp keys.
pub(crate) const MEM_CMP_KEY_HEAP_PREFIX: usize =
    MEM_CMP_KEY_LEN - size_of::<usize>() - size_of::<Box<[u8]>>();

/// Extendable byte container.
pub(crate) trait BytesExtendable {
    /// Push single byte into the container.
    fn push_byte(&mut self, value: u8);

    /// Extend from a byte slice.
    fn extend_from_byte_slice(&mut self, values: &[u8]);

    /// Extend by repeating one byte `n` times.
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
        self.extend(repeat_n(val, n))
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
/// 3. Floating-point number: Encode both zeros as positive zero and all NaNs as
///    all-one bytes, matching `OrderedFloat` equality and sorting NaNs last.
///    For other values, use big-endian encoding, flipping the most significant
///    bit for positive values and all bits for negative values.
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
pub(crate) trait MemCmpFormat {
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

/// Nullable memory comparable format.
pub(crate) trait NullableMemCmpFormat {
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

/// Marker value for null nullable memcmp encoding.
pub(crate) struct Null;

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
    ($t1:ty, $bits:ty, $encode:ident, $mask:expr) => {
        #[inline]
        fn $encode(value: $t1) -> [u8; size_of::<$t1>()] {
            let bits = if value.is_nan() {
                <$bits>::MAX
            } else if value == 0.0 {
                $mask
            } else if value > 0.0 {
                value.to_bits() ^ $mask
            } else {
                !value.to_bits()
            };
            bits.to_be_bytes()
        }

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
                buf.extend_from_byte_slice(&$encode(*self));
            }

            #[inline]
            fn copy_mcf_to(&self, buf: &mut [u8], start_idx: usize) -> usize {
                let end_idx = start_idx + self.enc_mcf_len();
                buf[start_idx..end_idx].copy_from_slice(&$encode(*self));
                end_idx
            }
        }
    };
}

impl_mcf_for_f!(f32, u32, encode_f32_mcf, 0x8000_0000);
impl_mcf_for_f!(f64, u64, encode_f64_mcf, 0x8000_0000_0000_0000);
impl_nmcf_for!(f32);
impl_nmcf_for!(f64);

/// Bytes encoded with segmented variable-width memcmp format.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct SegmentedBytes<'a>(
    /// Bytes encoded with segmented variable-width memcmp format.
    pub &'a [u8],
);

impl MemCmpFormat for SegmentedBytes<'_> {
    #[inline]
    fn est_mcf_len() -> Option<usize> {
        None
    }

    #[expect(clippy::manual_div_ceil, reason = "code style")]
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

/// Bytes encoded without segment terminators.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct NormalBytes<'a>(
    /// Bytes encoded as-is.
    pub &'a [u8],
);

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

/// Wrapper indicating that a value should be encoded with a non-null flag.
#[repr(transparent)]
pub(crate) struct Nullable<T>(
    /// Wrapped non-null value.
    pub T,
);

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
pub(crate) struct MemCmpKey(Inner);

impl MemCmpKey {
    /// Get byte slice of the key.
    #[inline]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        if self.0.len <= MEM_CMP_KEY_INLINE {
            // SAFETY: the inline representation stores `len` bytes in `u.i`.
            unsafe { &self.0.u.i[..self.0.len] }
        } else {
            // SAFETY: when `len` exceeds the inline threshold, the heap
            // representation owns at least `len` initialized bytes in `u.h.data`.
            unsafe { &self.0.u.h.data[..self.0.len] }
        }
    }

    /// Create a guard for in-place modification.
    #[inline]
    pub(crate) fn modify_inplace(&mut self) -> ModifyInplaceGuard<'_> {
        ModifyInplaceGuard(self)
    }

    /// Returns a new key with all zeroed bytes.
    #[inline]
    pub(crate) fn zeroed(len: usize) -> Self {
        if len <= MEM_CMP_KEY_INLINE {
            return MemCmpKey(Inner::inline_zeroed(len));
        }
        MemCmpKey(Inner::heap_alloc(len))
    }

    /// Returns a zero-initialized key with the requested length.
    #[inline]
    pub(crate) fn arbitrary(len: usize) -> Self {
        if len <= MEM_CMP_KEY_INLINE {
            return MemCmpKey(Inner::inline_zeroed(len));
        }
        MemCmpKey(Inner::heap_alloc(len))
    }

    /// Create a empty key.
    #[inline]
    pub(crate) fn empty() -> Self {
        MemCmpKey(Inner::inline_zeroed(0))
    }

    /// Get mutable byte slice of the key.
    /// This method is not exposed directly because
    /// we need to maintain prefix if key is on heap.
    /// So we derive a Guard to void miss handling it.
    #[inline]
    fn as_bytes_mut(&mut self) -> &mut [u8] {
        if self.0.len <= MEM_CMP_KEY_INLINE {
            // SAFETY: the inline representation stores `len` bytes in `u.i`.
            unsafe { &mut self.0.u.i[..self.0.len] }
        } else {
            // SAFETY: when `len` exceeds the inline threshold, the heap
            // representation owns at least `len` initialized bytes in `u.h.data`.
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

impl Clone for MemCmpKey {
    #[inline]
    fn clone(&self) -> Self {
        MemCmpKey::from(self.as_bytes())
    }
}

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

/// Mutable guard that refreshes heap prefixes when dropped.
pub(crate) struct ModifyInplaceGuard<'a>(&'a mut MemCmpKey);

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
        let prefix = heap_prefix(value);
        // SAFETY: this allocates `value.len()` bytes, copies exactly that many
        // initialized bytes into the allocation, then transfers ownership into
        // the heap variant. Null is rejected before copying.
        unsafe {
            let ptr = alloc(Layout::from_size_align_unchecked(value.len(), 1));
            assert!(
                !ptr.is_null(),
                "MemCmpKey allocation failed: bytes={}",
                value.len()
            );
            copy_nonoverlapping(value.as_ptr(), ptr, value.len());
            let data = Vec::from_raw_parts(ptr, value.len(), value.len()).into_boxed_slice();
            Inner::heap_inner(value.len(), prefix, data)
        }
    }

    #[inline]
    fn heap_alloc(len: usize) -> Inner {
        debug_assert!(len > MEM_CMP_KEY_INLINE);
        let data = vec![0u8; len].into_boxed_slice();
        Inner::heap_inner(len, [0u8; MEM_CMP_KEY_HEAP_PREFIX], data)
    }

    #[inline]
    fn heap_with_nullable_byte(value: &[u8], b: u8) -> Inner {
        let len = value.len() + 1;
        debug_assert!(len > MEM_CMP_KEY_INLINE);
        let mut prefix = [0u8; MEM_CMP_KEY_HEAP_PREFIX];
        prefix[0] = b;
        prefix[1..].copy_from_slice(&value[..MEM_CMP_KEY_HEAP_PREFIX - 1]);
        // SAFETY: this allocates `len` bytes, writes the nullable flag plus the
        // copied payload bytes, and transfers ownership into the heap variant.
        // Null is rejected before either write.
        unsafe {
            let ptr = alloc(Layout::from_size_align_unchecked(len, 1));
            assert!(
                !ptr.is_null(),
                "MemCmpKey nullable allocation failed: bytes={len}"
            );
            // update first byte.
            *ptr = b;
            // copy data.
            copy_nonoverlapping(value.as_ptr(), ptr.add(1), value.len());
            let data = Vec::from_raw_parts(ptr, len, len).into_boxed_slice();
            Inner::heap_inner(len, prefix, data)
        }
    }

    #[inline]
    fn heap_inner(len: usize, prefix: [u8; MEM_CMP_KEY_HEAP_PREFIX], data: Box<[u8]>) -> Inner {
        Inner {
            len,
            u: InlineOrHeap {
                h: ManuallyDrop::new(Heap { prefix, data }),
            },
        }
    }

    #[inline]
    fn update_prefix_if_on_heap(&mut self) {
        if self.len > MEM_CMP_KEY_INLINE {
            // SAFETY: when `len` exceeds the inline threshold, `u.h` is the
            // active representation and contains at least the prefix bytes.
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
                // SAFETY: the inline variant is active and `self.len <
                // MEM_CMP_KEY_INLINE`, so indexing `u.i[self.len]` stays in
                // bounds.
                unsafe { self.u.i[self.len] = value };
                self.len += 1;
            }
            Ordering::Equal => {
                // allocate twice size as current.
                // SAFETY: the inline variant is active, and this path allocates
                // a fresh heap buffer before initializing the heap variant.
                unsafe {
                    let old_len = self.len;
                    let new_len = old_len + 1;
                    let new_cap = old_len * 2;
                    let ptr = alloc(Layout::from_size_align_unchecked(new_cap, 1));
                    copy_nonoverlapping(self.u.i.as_ptr(), ptr, old_len);
                    *ptr.add(old_len) = value;
                    let data = boxed_slice_with_capacity(ptr, new_len, new_cap);
                    let prefix = heap_prefix(&data);
                    self.u.init_heap(prefix, data);
                    self.len = new_len;
                }
            }
            Ordering::Greater => {
                // SAFETY: the heap variant is active in this branch, and all raw
                // copies stay within the old and newly allocated heap buffers.
                unsafe {
                    let old_len = self.len;
                    let new_len = old_len + 1;
                    let data = &mut (*self.u.h).data;
                    if data.len() >= new_len {
                        // sufficient capacity.
                        data[old_len] = value;
                        self.len = new_len;
                    } else {
                        let new_cap = old_len * 2;
                        let ptr = alloc(Layout::from_size_align_unchecked(new_cap, 1));
                        copy_nonoverlapping(data.as_ptr(), ptr, old_len);
                        *ptr.add(old_len) = value;
                        let new_data = boxed_slice_with_capacity(ptr, new_len, new_cap);
                        self.len = new_len;
                        let old_box = replace(data, new_data);
                        drop(old_box); // explicitly drop the old box.
                        // prefix not changed.
                    }
                }
            }
        }
    }

    #[inline]
    fn extend_from_byte_slice(&mut self, values: &[u8]) {
        // SAFETY: each branch either writes within the active inline buffer or
        // allocates/replaces a heap buffer large enough for the copied bytes.
        unsafe {
            let old_len = self.len;
            let new_len = old_len + values.len();
            if old_len <= MEM_CMP_KEY_INLINE {
                // original collection is inline.
                if new_len > MEM_CMP_KEY_INLINE {
                    // new collection is on heap.
                    let ptr = alloc(Layout::from_size_align_unchecked(new_len, 1));
                    copy_nonoverlapping(self.u.i.as_ptr(), ptr, old_len);
                    copy_nonoverlapping(values.as_ptr(), ptr.add(old_len), values.len());
                    let data = Vec::from_raw_parts(ptr, new_len, new_len).into_boxed_slice();
                    let prefix = heap_prefix(&data);
                    self.len = new_len;
                    self.u.init_heap(prefix, data);
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
            copy_nonoverlapping(data.as_ptr(), ptr, old_len);
            copy_nonoverlapping(values.as_ptr(), ptr.add(old_len), values.len());
            let data = boxed_slice_with_capacity(ptr, new_len, new_cap);
            self.len = new_len;
            self.u.replace_heap_data(data);
            // prefix not changed.
        }
    }

    #[inline]
    fn extend_repeat_n(&mut self, val: u8, n: usize) {
        // SAFETY: each branch either writes within the active inline buffer or
        // allocates/replaces a heap buffer large enough for the repeated bytes.
        unsafe {
            let old_len = self.len;
            let new_len = old_len + n;
            if old_len <= MEM_CMP_KEY_INLINE {
                // original collection is inline.
                if new_len > MEM_CMP_KEY_INLINE {
                    // new collection is on heap.
                    let ptr = alloc(Layout::from_size_align_unchecked(new_len, 1));
                    copy_nonoverlapping(self.u.i.as_ptr(), ptr, old_len);
                    write_bytes(ptr.add(old_len), val, n);
                    let data = Vec::from_raw_parts(ptr, new_len, new_len).into_boxed_slice();
                    let prefix = heap_prefix(&data);
                    self.len = new_len;
                    self.u.init_heap(prefix, data);
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
            copy_nonoverlapping(data.as_ptr(), ptr, old_len);
            write_bytes(ptr.add(old_len), val, n);
            let data = boxed_slice_with_capacity(ptr, new_len, new_cap);
            self.len = new_len;
            self.u.replace_heap_data(data);
            // prefix not changed.
        }
    }

    #[inline]
    fn update_last_byte(&mut self, value: u8) {
        // SAFETY: callers only invoke this helper on non-empty keys, so
        // `self.len - 1` is in bounds for the active representation.
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
        // SAFETY: when `len` exceeds the inline threshold, `u.h` is the active
        // union variant and owns the heap allocation to drop.
        unsafe {
            ManuallyDrop::drop(&mut self.u.h);
        }
    }
}

union InlineOrHeap {
    i: [u8; MEM_CMP_KEY_INLINE],
    h: ManuallyDrop<Heap>,
}

impl InlineOrHeap {
    /// # Safety
    ///
    /// Caller must guarantee the active representation has no initialized heap
    /// allocation to drop.
    #[inline]
    unsafe fn init_heap(&mut self, prefix: [u8; MEM_CMP_KEY_HEAP_PREFIX], data: Box<[u8]>) {
        self.h = ManuallyDrop::new(Heap { prefix, data });
    }

    /// # Safety
    ///
    /// Caller must guarantee the heap data is already intiailized.
    /// This method will replace old data with new data, and drop old
    /// data immediately.
    #[inline]
    unsafe fn replace_heap_data(&mut self, data: Box<[u8]>) {
        // SAFETY: the caller guarantees `h.data` is initialized, so replacing it
        // and dropping the old boxed slice preserves ownership correctly.
        unsafe {
            let old_data = replace_ptr(&mut (*self.h).data, data);
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

#[inline]
fn extend_segmented_bytes<T: BytesExtendable>(bs: &[u8], buf: &mut T) {
    if bs.is_empty() {
        buf.extend_from_byte_slice(&[0; SEG_LEN + 1]); // last byte is zero
        return;
    }
    let (chunks, remainder) = bs.as_chunks::<SEG_LEN>();
    if remainder.is_empty() {
        for c in chunks {
            buf.extend_from_byte_slice(c);
            buf.push_byte(FIX_SEG_FLAG);
        }
        // update last byte as segment length
        buf.update_last_byte(SEG_LEN as u8);
        // *buf.last_mut().unwrap() = SEG_LEN as u8;
    } else {
        for c in chunks {
            buf.extend_from_byte_slice(c);
            buf.push_byte(FIX_SEG_FLAG);
        }
        buf.extend_from_byte_slice(remainder);
        buf.extend_repeat_n(0x00, SEG_LEN - remainder.len());
        buf.push_byte(remainder.len() as u8);
    }
}

#[inline]
fn copy_segmented_bytes(bs: &[u8], mut buf: &mut [u8]) -> usize {
    if bs.is_empty() {
        buf[..SEG_LEN + 1].iter_mut().for_each(|b| *b = 0);
        return SEG_LEN + 1;
    }
    let (chunks, remainder) = bs.as_chunks::<SEG_LEN>();
    if remainder.is_empty() {
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
    for c in chunks {
        debug_assert!(c.len() == SEG_LEN);
        buf[..SEG_LEN].copy_from_slice(c);
        buf[SEG_LEN] = FIX_SEG_FLAG;
        buf = &mut buf[SEG_LEN + 1..];
        offset += SEG_LEN + 1
    }
    buf[..remainder.len()].copy_from_slice(remainder);
    buf[remainder.len()..SEG_LEN]
        .iter_mut()
        .for_each(|b| *b = 0);
    buf[SEG_LEN] = remainder.len() as u8;
    offset + SEG_LEN + 1
}

/// Builds a boxed byte slice whose length is used as heap capacity.
///
/// # Safety
///
/// `ptr` must be allocated for `cap` bytes with alignment 1 and uniquely owned
/// by the caller. The first `initialized_len` bytes must already be
/// initialized, and `initialized_len` must not exceed `cap`.
#[inline]
unsafe fn boxed_slice_with_capacity(ptr: *mut u8, initialized_len: usize, cap: usize) -> Box<[u8]> {
    debug_assert!(initialized_len <= cap);
    if initialized_len < cap {
        // SAFETY: the caller guarantees `ptr` is valid for `cap` bytes, and
        // the range starts after the already initialized prefix.
        unsafe { write_bytes(ptr.add(initialized_len), 0, cap - initialized_len) };
    }
    // SAFETY: the full `cap` byte range is initialized before ownership is
    // transferred into the Vec.
    unsafe { Vec::from_raw_parts(ptr, cap, cap).into_boxed_slice() }
}

#[inline]
fn heap_prefix(data: &[u8]) -> [u8; MEM_CMP_KEY_HEAP_PREFIX] {
    debug_assert!(data.len() >= MEM_CMP_KEY_HEAP_PREFIX);
    let mut prefix = [0u8; MEM_CMP_KEY_HEAP_PREFIX];
    prefix.copy_from_slice(&data[..MEM_CMP_KEY_HEAP_PREFIX]);
    prefix
}

#[cfg(test)]
mod tests {
    use ordered_float::OrderedFloat;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use rand_distr::{Distribution, StandardUniform};
    use std::any::type_name;

    use super::*;

    fn heap_capacity(key: &MemCmpKey) -> usize {
        assert!(key.0.len > MEM_CMP_KEY_INLINE);
        // SAFETY: the assertion above proves the heap representation is active.
        unsafe { key.0.u.h.data.len() }
    }

    fn heap_spare_bytes(key: &MemCmpKey) -> &[u8] {
        assert!(key.0.len > MEM_CMP_KEY_INLINE);
        // SAFETY: the assertion above proves the heap representation is active,
        // and `Inner::len` is the logical prefix of the capacity slice.
        unsafe { &key.0.u.h.data[key.0.len..] }
    }

    fn heap_prefix_bytes(key: &MemCmpKey) -> &[u8; MEM_CMP_KEY_HEAP_PREFIX] {
        assert!(key.0.len > MEM_CMP_KEY_INLINE);
        // SAFETY: the assertion above proves the heap representation is active.
        unsafe { &key.0.u.h.prefix }
    }

    fn encode_mcf<T: MemCmpFormat + ?Sized>(value: &T) -> Vec<u8> {
        let mut encoded = Vec::new();
        value.extend_mcf_to(&mut encoded);
        assert_eq!(encoded.len(), value.enc_mcf_len());
        if let Some(length) = T::est_mcf_len() {
            assert_eq!(encoded.len(), length);
        }
        let mut copied = vec![0xa5; encoded.len() + 2];
        assert_eq!(value.copy_mcf_to(&mut copied, 1), encoded.len() + 1);
        assert_eq!(&copied[1..copied.len() - 1], encoded);
        assert_eq!(copied[0], 0xa5);
        assert_eq!(copied[copied.len() - 1], 0xa5);
        encoded
    }

    fn encode_nmcf<T: NullableMemCmpFormat>(value: &T) -> Vec<u8> {
        let mut encoded = Vec::new();
        value.extend_nmcf_to(&mut encoded);
        assert_eq!(encoded.len(), value.enc_nmcf_len());
        if let Some(length) = T::est_nmcf_len() {
            assert_eq!(encoded.len(), length);
        }
        let mut copied = vec![0xa5; encoded.len() + 2];
        assert_eq!(value.copy_nmcf_to(&mut copied, 1), encoded.len() + 1);
        assert_eq!(&copied[1..copied.len() - 1], encoded);
        assert_eq!(copied[0], 0xa5);
        assert_eq!(copied[copied.len() - 1], 0xa5);
        encoded
    }

    fn assert_float_fixture<T: MemCmpFormat + NullableMemCmpFormat>(value: T, expected: &[u8]) {
        assert_eq!(encode_mcf(&value), expected);
        let mut nullable = vec![NON_NULL_FLAG];
        nullable.extend_from_slice(expected);
        assert_eq!(encode_nmcf(&value), nullable);
    }

    fn assert_encoding_order<T: fmt::Debug>(
        input: Vec<T>,
        compare: impl Fn(&T, &T) -> Ordering,
        encode: impl Fn(&T) -> Vec<u8>,
    ) {
        let mut pairs: Vec<_> = input
            .into_iter()
            .map(|value| {
                let bytes = encode(&value);
                (value, bytes)
            })
            .collect();
        pairs.sort_by(|left, right| compare(&left.0, &right.0));
        for pair in pairs.windows(2) {
            // Compare native values directly: re-encoding sorted values alone
            // would accept an encoder that maps distinct values to equal bytes.
            assert_eq!(
                pair[0].1.cmp(&pair[1].1),
                compare(&pair[0].0, &pair[1].0),
                "type={}, left={:?}, right={:?}",
                type_name::<T>(),
                pair[0],
                pair[1],
            );
        }
    }

    fn gen_input<T>(rng: &mut StdRng) -> Vec<T>
    where
        StandardUniform: Distribution<T>,
    {
        (0..1024).map(|_| rng.sample(StandardUniform)).collect()
    }

    fn run_test_mcf<T>()
    where
        T: MemCmpFormat + NullableMemCmpFormat + Ord + fmt::Debug,
        StandardUniform: Distribution<T>,
    {
        let mut rng = StdRng::seed_from_u64(0x4d43_4601);
        assert_encoding_order(gen_input::<T>(&mut rng), T::cmp, encode_mcf);
        assert_encoding_order(gen_input::<T>(&mut rng), T::cmp, encode_nmcf);
    }

    fn run_test_mcf2<T, U>()
    where
        T: MemCmpFormat + NullableMemCmpFormat + Ord + fmt::Debug,
        U: MemCmpFormat + NullableMemCmpFormat + Ord + fmt::Debug,
        StandardUniform: Distribution<T> + Distribution<U>,
    {
        let mut rng = StdRng::seed_from_u64(0x4d43_4601);
        assert_encoding_order(
            gen_input::<(T, U)>(&mut rng),
            <(T, U)>::cmp,
            |(first, second)| {
                let mut bytes = encode_mcf(first);
                bytes.extend(encode_mcf(second));
                bytes
            },
        );
        assert_encoding_order(
            gen_input::<(T, U)>(&mut rng),
            <(T, U)>::cmp,
            |(first, second)| {
                let mut bytes = encode_nmcf(first);
                bytes.extend(encode_nmcf(second));
                bytes
            },
        );
    }

    fn gen_rand_bytes(rng: &mut StdRng) -> Vec<u8> {
        let len: u8 = rng.sample(StandardUniform);
        (0..len).map(|_| rng.sample(StandardUniform)).collect()
    }

    /// Purpose: Protect order-preserving integer and composite-key encodings.
    /// Expected: Sorting encoded bytes agrees with native ordering for plain and nullable formats.
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

        // int + int
        run_test_mcf2::<i32, i32>();
        run_test_mcf2::<u32, u32>();
        run_test_mcf2::<i64, i64>();
        run_test_mcf2::<u64, u64>();
    }

    /// Purpose: Protect order-preserving variable-length byte encodings.
    /// Expected: Sorting segmented encodings agrees with native byte ordering in both nullability formats.
    #[test]
    fn test_mcf_varlen() {
        let mut rng = StdRng::seed_from_u64(0x4d43_4601);
        let input = (0..1024).map(|_| gen_rand_bytes(&mut rng)).collect();
        assert_encoding_order(input, Vec::<u8>::cmp, |value| {
            encode_mcf(&SegmentedBytes(value))
        });
        let input = (0..1024).map(|_| gen_rand_bytes(&mut rng)).collect();
        assert_encoding_order(input, Vec::<u8>::cmp, |value| {
            encode_nmcf(&SegmentedBytes(value))
        });
    }

    /// Purpose: Protect float key ordering across reproducible arbitrary IEEE bit patterns.
    /// Expected: Plain and nullable encodings agree with value ordering and equality for both widths.
    #[test]
    fn test_mcf_float() {
        let mut rng = StdRng::seed_from_u64(0x4d43_4601);
        let values: Vec<_> = gen_input::<u32>(&mut rng)
            .into_iter()
            .map(f32::from_bits)
            .collect();
        let compare = |left: &f32, right: &f32| OrderedFloat(*left).cmp(&OrderedFloat(*right));
        assert_encoding_order(values.clone(), compare, encode_mcf);
        assert_encoding_order(values, compare, encode_nmcf);
        let values: Vec<_> = gen_input::<u64>(&mut rng)
            .into_iter()
            .map(f64::from_bits)
            .collect();
        let compare = |left: &f64, right: &f64| OrderedFloat(*left).cmp(&OrderedFloat(*right));
        assert_encoding_order(values.clone(), compare, encode_mcf);
        assert_encoding_order(values, compare, encode_nmcf);
    }

    /// Purpose: Protect canonical float key bytes at signed-zero, NaN, and numeric boundaries.
    /// Expected: Equivalent values share bytes, NaNs sort last, and numeric boundaries have exact encodings.
    #[test]
    fn test_mcf_float_boundaries() {
        let cases32: [(u32, u32); 16] = [
            (0xff80_0000, 0x007f_ffff), // Negative infinity.
            (0xff7f_ffff, 0x0080_0000), // Negative finite extreme.
            (0xbf80_0000, 0x407f_ffff), // Negative one.
            (0x8080_0000, 0x7f7f_ffff), // Negative minimum normal.
            (0x8000_0001, 0x7fff_fffe), // Negative minimum subnormal.
            (0x8000_0000, 0x8000_0000), // Negative zero.
            (0x0000_0000, 0x8000_0000), // Positive zero.
            (0x0000_0001, 0x8000_0001), // Positive minimum subnormal.
            (0x0080_0000, 0x8080_0000), // Positive minimum normal.
            (0x3f80_0000, 0xbf80_0000), // Positive one.
            (0x7f7f_ffff, 0xff7f_ffff), // Positive finite extreme.
            (0x7f80_0000, 0xff80_0000), // Positive infinity.
            (0x7f80_0001, 0xffff_ffff), // Positive signaling NaN.
            (0x7fc0_1234, 0xffff_ffff), // Positive quiet NaN with payload.
            (0xff80_0001, 0xffff_ffff), // Negative signaling NaN.
            (0xffc0_5678, 0xffff_ffff), // Negative quiet NaN with payload.
        ];
        for (bits, expected) in cases32 {
            assert_float_fixture(f32::from_bits(bits), &expected.to_be_bytes());
        }
        let values = cases32
            .into_iter()
            .map(|(bits, _)| f32::from_bits(bits))
            .collect::<Vec<_>>();
        let compare = |left: &f32, right: &f32| OrderedFloat(*left).cmp(&OrderedFloat(*right));
        assert_encoding_order(values.clone(), compare, encode_mcf);
        assert_encoding_order(values, compare, encode_nmcf);

        let cases64: [(u64, u64); 16] = [
            (0xfff0_0000_0000_0000, 0x000f_ffff_ffff_ffff),
            (0xffef_ffff_ffff_ffff, 0x0010_0000_0000_0000),
            (0xbff0_0000_0000_0000, 0x400f_ffff_ffff_ffff),
            (0x8010_0000_0000_0000, 0x7fef_ffff_ffff_ffff),
            (0x8000_0000_0000_0001, 0x7fff_ffff_ffff_fffe),
            (0x8000_0000_0000_0000, 0x8000_0000_0000_0000),
            (0x0000_0000_0000_0000, 0x8000_0000_0000_0000),
            (0x0000_0000_0000_0001, 0x8000_0000_0000_0001),
            (0x0010_0000_0000_0000, 0x8010_0000_0000_0000),
            (0x3ff0_0000_0000_0000, 0xbff0_0000_0000_0000),
            (0x7fef_ffff_ffff_ffff, 0xffef_ffff_ffff_ffff),
            (0x7ff0_0000_0000_0000, 0xfff0_0000_0000_0000),
            (0x7ff0_0000_0000_0001, 0xffff_ffff_ffff_ffff),
            (0x7ff8_0000_0000_1234, 0xffff_ffff_ffff_ffff),
            (0xfff0_0000_0000_0001, 0xffff_ffff_ffff_ffff),
            (0xfff8_0000_0000_5678, 0xffff_ffff_ffff_ffff),
        ];
        for (bits, expected) in cases64 {
            assert_float_fixture(f64::from_bits(bits), &expected.to_be_bytes());
        }
        let values = cases64
            .into_iter()
            .map(|(bits, _)| f64::from_bits(bits))
            .collect::<Vec<_>>();
        let compare = |left: &f64, right: &f64| OrderedFloat(*left).cmp(&OrderedFloat(*right));
        assert_encoding_order(values.clone(), compare, encode_mcf);
        assert_encoding_order(values, compare, encode_nmcf);
    }

    /// Purpose: Protect key construction, extension, and comparison across storage forms.
    /// Expected: Keys preserve their bytes and comparison semantics through inline and heap operations.
    #[test]
    fn test_mem_cmp_key() {
        // inline key
        let k1 = MemCmpKey::from(&[1u8; 10]);
        assert_eq!(k1.as_bytes(), &[1u8; 10]);
        assert!(k1.0.len <= MEM_CMP_KEY_INLINE);

        // heap key
        for len in [25, 30] {
            let input = vec![1u8; len];
            let key = MemCmpKey::from(input.as_slice());
            assert_eq!(key.as_bytes(), input, "heap constructor length={len}");
            assert!(key.0.len > MEM_CMP_KEY_INLINE);
            drop(key);
        }

        // empty key
        let k3 = MemCmpKey::empty();
        assert_eq!(k3.as_bytes(), b"");

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
        assert!(k17.as_bytes() == [0x00, 0x01, 0x01, 0x02]);

        let mut k17 = MemCmpKey::empty();
        for _ in 0..64 {
            k17.push_byte(0x01);
        }
        assert!(k17.len() == 64);
        assert!(k17.as_bytes().iter().all(|b| *b == 0x01));
    }

    /// Purpose: Protect byte appends at the inline key capacity boundary.
    /// Expected: Heap promotion preserves content and prefix while initializing spare capacity.
    #[test]
    fn test_mem_cmp_key_push_inline_to_heap_transition() {
        let mut key = MemCmpKey::empty();
        let mut expected = Vec::with_capacity(MEM_CMP_KEY_INLINE + 2);

        for idx in 0..MEM_CMP_KEY_INLINE {
            let value = (idx as u8).wrapping_add(1);
            key.push_byte(value);
            expected.push(value);
        }

        assert_eq!(key.0.len, MEM_CMP_KEY_INLINE);
        assert_eq!(key.as_bytes(), expected);

        key.push_byte(0xaa);
        expected.push(0xaa);

        assert_eq!(key.0.len, MEM_CMP_KEY_INLINE + 1);
        assert_eq!(key.as_bytes(), expected);
        assert_eq!(heap_capacity(&key), MEM_CMP_KEY_INLINE * 2);
        assert_eq!(
            heap_prefix_bytes(&key).as_slice(),
            &expected[..MEM_CMP_KEY_HEAP_PREFIX]
        );
        assert!(heap_spare_bytes(&key).iter().all(|b| *b == 0));
        assert_eq!(key, MemCmpKey::from(&expected[..]));

        key.push_byte(0xbb);
        expected.push(0xbb);
        assert_eq!(key.as_bytes(), expected);
        assert_eq!(heap_capacity(&key), MEM_CMP_KEY_INLINE * 2);
    }

    /// Purpose: Protect bulk extension that promotes a short inline key.
    /// Expected: Slice and repeated-byte extensions preserve content and initialize the heap prefix.
    #[test]
    fn test_mem_cmp_key_extend_short_inline_to_heap_initializes_prefix() {
        let mut key = MemCmpKey::from(&[0x11, 0x22][..]);
        let mut expected = vec![0x11, 0x22];
        expected.extend_from_slice(&[0x33; MEM_CMP_KEY_INLINE]);

        key.extend_from_byte_slice(&[0x33; MEM_CMP_KEY_INLINE]);

        assert_eq!(key.as_bytes(), expected);
        assert_eq!(
            heap_prefix_bytes(&key).as_slice(),
            &expected[..MEM_CMP_KEY_HEAP_PREFIX]
        );

        let mut repeated = MemCmpKey::from(&[0x44, 0x55][..]);
        let mut expected = vec![0x44, 0x55];
        expected.extend(repeat_n(0x66, MEM_CMP_KEY_INLINE));

        repeated.extend_repeat_n(0x66, MEM_CMP_KEY_INLINE);

        assert_eq!(repeated.as_bytes(), expected);
        assert_eq!(
            heap_prefix_bytes(&repeated).as_slice(),
            &expected[..MEM_CMP_KEY_HEAP_PREFIX]
        );
    }

    /// Purpose: Protect heap key initialization and guarded in-place mutation.
    /// Expected: New bytes are initialized and the cached prefix reflects completed mutations.
    #[test]
    fn test_mem_cmp_key_arbitrary_heap_is_initialized_and_updates_prefix() {
        let mut key = MemCmpKey::arbitrary(MEM_CMP_KEY_INLINE + 1);
        assert!(key.as_bytes().iter().all(|b| *b == 0));
        assert!(heap_prefix_bytes(&key).iter().all(|b| *b == 0));

        let expected: Vec<u8> = (0..key.len()).map(|idx| idx as u8 + 1).collect();
        {
            let mut guard = key.modify_inplace();
            guard.copy_from_slice(&expected);
        }

        assert_eq!(key.as_bytes(), expected);
        assert_eq!(
            heap_prefix_bytes(&key).as_slice(),
            &expected[..MEM_CMP_KEY_HEAP_PREFIX]
        );
    }

    /// Purpose: Protect reallocation when extending an existing heap key.
    /// Expected: Growth preserves appended content and initializes spare capacity for subsequent writes.
    #[test]
    fn test_mem_cmp_key_heap_growth_initializes_spare_capacity() {
        let base = vec![0x11; MEM_CMP_KEY_INLINE + 1];

        let mut key = MemCmpKey::from(&base[..]);
        let mut expected = base.clone();
        assert_eq!(heap_capacity(&key), expected.len());

        key.extend_from_byte_slice(&[0x22]);
        expected.push(0x22);
        assert_eq!(key.as_bytes(), expected);
        assert!(heap_capacity(&key) > key.0.len);
        assert!(heap_spare_bytes(&key).iter().all(|b| *b == 0));

        key.push_byte(0x33);
        expected.push(0x33);
        assert_eq!(key.as_bytes(), expected);

        let mut repeated = MemCmpKey::from(&base[..]);
        let mut expected = base;
        assert_eq!(heap_capacity(&repeated), expected.len());

        repeated.extend_repeat_n(0x44, 1);
        expected.push(0x44);
        assert_eq!(repeated.as_bytes(), expected);
        assert!(heap_capacity(&repeated) > repeated.0.len);
        assert!(heap_spare_bytes(&repeated).iter().all(|b| *b == 0));

        repeated.push_byte(0x55);
        expected.push(0x55);
        assert_eq!(repeated.as_bytes(), expected);
    }

    /// Purpose: Protect the nullable encoding of a null key.
    /// Expected: Both encoding paths emit only the null marker with the expected length.
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

    /// Purpose: Protect unsegmented byte encoding.
    /// Expected: Input bytes are copied unchanged and no fixed encoded length is advertised.
    #[test]
    fn test_mem_cmp_normal_bytes() {
        assert!(NormalBytes::est_mcf_len().is_none());
        assert!(NormalBytes::est_nmcf_len().is_none());
        let mut buf = vec![];
        NormalBytes(b"hello").extend_mcf_to(&mut buf);
        assert!(&buf[..] == b"hello");
    }

    /// Purpose: Protect empty and exact-segment byte encoding boundaries.
    /// Expected: Both encoding paths preserve padding, segment lengths, and nullable markers.
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
