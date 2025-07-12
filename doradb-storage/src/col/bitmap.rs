use crate::col::alloc::{align_u128, RawArray};
use crate::col::sel::Sel;
use crate::col::slice_ext::{OffsetPairMut, OffsetTripleMut, PairSliceExt};
use crate::error::{Error, Result};
use smallvec::{smallvec, SmallVec};
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug)]
pub enum Bitmap {
    Owned {
        inner: RawArray,
        len_u1: usize,
    },
    Borrowed {
        ptr: Arc<[u8]>,
        len_u1: usize,
        start_bytes: usize,
        end_bytes: usize,
    },
}

impl Bitmap {
    /// Create a new owned bitmap with given capacity.
    #[allow(clippy::manual_div_ceil)]
    #[inline]
    pub fn with_capacity(cap_u1: usize) -> Self {
        let cap_u8 = if cap_u1 == 0 { 1 } else { (cap_u1 + 7) / 8 };
        Bitmap::Owned {
            inner: RawArray::with_capacity(cap_u8),
            len_u1: 0,
        }
    }

    /// Create a new owned bitmap with given length.
    /// The returned bitmap already has length same as input
    /// and all bits are zeroed.
    #[allow(clippy::manual_div_ceil)]
    #[inline]
    pub fn zeroes(len_u1: usize) -> Self {
        let cap_u8 = if len_u1 == 0 { 1 } else { (len_u1 + 7) / 8 };
        Bitmap::Owned {
            inner: RawArray::zeroes(cap_u8),
            len_u1,
        }
    }

    /// Create a new owned bitmap with given length.
    /// The returned bitmap already has length same as input
    /// and all bits are ones.
    #[allow(clippy::manual_div_ceil)]
    #[inline]
    pub fn ones(len_u1: usize) -> Self {
        let cap_u8 = if len_u1 == 0 { 1 } else { (len_u1 + 7) / 8 };
        Bitmap::Owned {
            inner: RawArray::ones(cap_u8),
            len_u1,
        }
    }

    /// Create a new borrowed bitmap.
    #[allow(clippy::manual_div_ceil)]
    #[inline]
    pub fn new_borrowed(ptr: Arc<[u8]>, len_u1: usize, start_bytes: usize) -> Self {
        let end_bytes = align_u128(start_bytes + (len_u1 + 7) / 8);
        debug_assert!(end_bytes <= ptr.len());
        Bitmap::Borrowed {
            ptr,
            len_u1,
            start_bytes,
            end_bytes,
        }
    }

    /// Returns number of bit in this bitmap.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Bitmap::Owned { len_u1, .. } | Bitmap::Borrowed { len_u1, .. } => *len_u1,
        }
    }

    /// Returns whether the bitmap is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns whether the bitmap is owned.
    #[inline]
    pub fn is_owned(&self) -> bool {
        matches!(self, Bitmap::Owned { .. })
    }

    /// Returns total bytes(allocated memory) of this bitmap.
    #[inline]
    pub fn total_bytes(&self) -> usize {
        match self {
            Bitmap::Owned { inner, .. } => inner.cap_u8(),
            Bitmap::Borrowed {
                start_bytes,
                end_bytes,
                ..
            } => *end_bytes - *start_bytes,
        }
    }

    /// Returns raw byte slice.
    #[inline]
    pub fn raw(&self) -> &[u8] {
        match self {
            Bitmap::Owned { inner, .. } => inner.as_slice(),
            Bitmap::Borrowed {
                ptr,
                start_bytes,
                end_bytes,
                ..
            } => &ptr[*start_bytes..*end_bytes],
        }
    }

    /// Clear the bitmap.
    #[inline]
    pub fn clear(&mut self) {
        match self {
            Bitmap::Owned { len_u1, .. } | Bitmap::Borrowed { len_u1, .. } => *len_u1 = 0,
        }
    }

    /// Reserves given capacity.
    /// This method may allocate new memory if it's borrowed,
    /// or current capacity is not sufficient.
    #[allow(clippy::manual_div_ceil)]
    #[inline]
    pub fn reserve(&mut self, cap_u1: usize) {
        match self {
            Bitmap::Owned { inner, .. } => inner.reserve((cap_u1 + 7) / 8),
            Bitmap::Borrowed {
                ptr,
                len_u1,
                start_bytes,
                end_bytes,
            } => {
                let total_bytes = *end_bytes - *start_bytes;
                if cap_u1 / 8 <= total_bytes {
                    // current capacity is sufficient
                    return;
                }
                // allocate and copy all data
                let mut inner = RawArray::with_capacity((cap_u1 + 7) / 8);
                inner.as_slice_mut()[..total_bytes].copy_from_slice(&ptr[*start_bytes..*end_bytes]);
                let len_u1 = *len_u1;
                *self = Bitmap::Owned { inner, len_u1 };
            }
        }
    }

    /// Reserves given capacity and returns mutable u64 slice.
    #[inline]
    pub fn reserve_u64s(&mut self, cap_u1: usize) -> &mut [u64] {
        self.reserve(cap_u1);
        // # SAFETY
        //
        // length is guaranteed to be valid because already reserved.
        unsafe { self.u64s_mut_unchecked(cap_u1).0 }
    }

    /// Returns aligned 8-byte integers and its bit number.
    ///
    /// # Safety
    ///
    /// Caller must ensure length is within bound.
    #[allow(clippy::manual_div_ceil)]
    #[inline]
    pub unsafe fn u64s_unchecked(&self, len_u1: usize) -> (&[u64], usize) {
        unsafe {
            let len_u64 = (len_u1 + 63) / 64;
            match self {
                Bitmap::Owned { inner, len_u1 } => {
                    // # SAFETY
                    //
                    // Alignment and length are guaranteed to be valid.
                    let s = inner.cast_slice::<u64>(len_u64);
                    (s, *len_u1)
                }
                Bitmap::Borrowed { ptr, len_u1, .. } => {
                    // # SAFETY
                    //
                    // Alignment and length are guaranteed to be valid.
                    let s = std::slice::from_raw_parts(ptr.as_ptr() as *const u64, len_u64);
                    (s, *len_u1)
                }
            }
        }
    }

    /// Returns used u64 slice.
    #[inline]
    pub fn u64s(&self) -> (&[u64], usize) {
        // # SAFETY
        //
        // Length is within bound.
        unsafe { self.u64s_unchecked(self.len()) }
    }

    /// Returns aligned mutable 8-byte integers and its bit number.
    /// This method will allocate new memory if it's borrowed.
    ///
    /// # Safety
    ///
    /// Caller must ensure length is within bound.
    #[allow(clippy::manual_div_ceil)]
    #[inline]
    pub unsafe fn u64s_mut_unchecked(&mut self, len_u1: usize) -> (&mut [u64], usize) {
        unsafe {
            let len_u64 = (len_u1 + 63) / 64;
            match self.to_mut() {
                Bitmap::Owned { inner, len_u1 } => {
                    // # SAFETY
                    //
                    // Alignment and length are guaranteed to be valid.
                    let s = inner.cast_slice_mut::<u64>(len_u64);
                    (s, *len_u1)
                }
                Bitmap::Borrowed { .. } => unreachable!(),
            }
        }
    }

    /// Returns mutable used u64 slice.
    #[inline]
    pub fn u64s_mut(&mut self) -> (&mut [u64], usize) {
        // # SAFETY
        //
        // Length is within bound.
        unsafe { self.u64s_mut_unchecked(self.len()) }
    }

    /// Returns aligned byte slice and its bit number.
    #[allow(clippy::manual_div_ceil)]
    #[inline]
    pub fn u8s(&self, len_u1: usize) -> (&[u8], usize) {
        let len_u8 = (len_u1 + 7) / 8;
        match self {
            Bitmap::Owned { inner, len_u1 } => (&inner.as_slice()[..len_u8], *len_u1),
            Bitmap::Borrowed {
                ptr,
                len_u1,
                start_bytes,
                ..
            } => (&ptr[*start_bytes..*start_bytes + len_u8], *len_u1),
        }
    }

    /// Returns aligned mutable byte slice and its bit number.
    /// This method will allocate new memory if it's borrowed.
    #[allow(clippy::manual_div_ceil)]
    #[inline]
    pub fn u8s_mut(&mut self, len_u1: usize) -> (&mut [u8], usize) {
        let len_u8 = (len_u1 + 7) / 8;
        match self.to_mut() {
            Bitmap::Owned { inner, len_u1 } => (&mut inner.as_slice_mut()[..len_u8], *len_u1),
            Bitmap::Borrowed { .. } => unreachable!(),
        }
    }

    /// Get single value at given position.
    #[inline]
    pub fn get(&self, idx: usize) -> Result<bool> {
        let (bm, len) = self.u8s(self.len());
        if idx >= len {
            return Err(Error::IndexOutOfBound);
        }
        Ok(bitmap_u8s_get(bm, idx))
    }

    /// Set single value to the bitmap at given position.
    #[inline]
    pub fn set(&mut self, idx: usize, val: bool) -> Result<()> {
        let (bm, len) = self.u8s_mut(self.len());
        if idx >= len {
            return Err(Error::IndexOutOfBound);
        }
        bitmap_u8s_set(bm, idx, val);
        Ok(())
    }

    /// Returns iterator of bool values.
    #[inline]
    pub fn bools(&self) -> BoolIter<'_> {
        let (bm, len) = self.u64s();
        bitmap_bools(bm, len)
    }

    /// Returns value count of true.
    #[inline]
    pub fn true_count(&self) -> usize {
        let (bm, len) = self.u64s();
        bitmap_true_count(bm, len)
    }

    /// Returns value count of false.
    #[inline]
    pub fn false_count(&self) -> usize {
        let (bm, len) = self.u64s();
        bitmap_false_count(bm, len)
    }

    /// Returns range iterator of this bitmap.
    #[inline]
    pub fn range_iter(&self) -> RangeIter<'_> {
        let (bm, len) = self.u64s();
        bitmap_range_iter(bm, len)
    }

    #[inline]
    pub fn true_index_iter(&self) -> TrueIndexIter<'_> {
        let range_iter = self.range_iter();
        TrueIndexIter {
            range_iter,
            start: 0,
            end: 0,
        }
    }

    /// Convert the bitmap to owned.
    /// If it's already owned, this call is no-op.
    #[allow(clippy::manual_div_ceil)]
    #[inline]
    pub fn to_mut(&mut self) -> &mut Self {
        match self {
            Bitmap::Owned { .. } => self,
            Bitmap::Borrowed {
                ptr,
                len_u1,
                start_bytes,
                end_bytes,
            } => {
                let len_u1 = *len_u1;
                let len_u8 = (len_u1 + 7) / 8;
                let mut inner = RawArray::with_capacity(*end_bytes - *start_bytes);
                inner.as_slice_mut()[..len_u8]
                    .copy_from_slice(&ptr[*start_bytes..*start_bytes + len_u8]);
                *self = Bitmap::Owned { inner, len_u1 };
                self
            }
        }
    }

    /// Clone self to owned.
    #[allow(clippy::manual_div_ceil)]
    #[inline]
    pub fn to_owned(&self) -> Self {
        match self {
            Bitmap::Owned { inner, len_u1 } => Bitmap::Owned {
                inner: inner.clone(),
                len_u1: *len_u1,
            },
            Bitmap::Borrowed {
                ptr,
                len_u1,
                start_bytes,
                end_bytes,
            } => {
                let len_u1 = *len_u1;
                let len_u8 = (len_u1 + 7) / 8;
                let mut inner = RawArray::with_capacity(*end_bytes - *start_bytes);
                inner.as_slice_mut()[..len_u8]
                    .copy_from_slice(&ptr[*start_bytes..*start_bytes + len_u8]);
                Bitmap::Owned { inner, len_u1 }
            }
        }
    }

    /// Clone self to owned with atomic reference.
    #[allow(clippy::manual_div_ceil)]
    #[inline]
    pub fn clone_to_owned(this: &Arc<Self>) -> Arc<Self> {
        match this.as_ref() {
            Bitmap::Owned { .. } => Arc::clone(this),
            Bitmap::Borrowed {
                ptr,
                len_u1,
                start_bytes,
                end_bytes,
            } => {
                let len_u1 = *len_u1;
                let len_u8 = (len_u1 + 7) / 8;
                let mut inner = RawArray::with_capacity(*end_bytes - *start_bytes);
                inner.as_slice_mut()[..len_u8]
                    .copy_from_slice(&ptr[*start_bytes..*start_bytes + len_u8]);
                Arc::new(Bitmap::Owned { inner, len_u1 })
            }
        }
    }

    /// Shift bitmap to left with give length.
    #[inline]
    pub fn shift(&mut self, bits: usize) {
        let (bm, len) = self.u64s_mut();
        bitmap_u64s_shift(bm, len, bits);
        let new_len = len.saturating_sub(bits);
        unsafe { self.set_len(new_len) }
    }

    /// Intersects given bitmap to current one.
    /// Compiler will apply SIMD optimization automatically, so u64 bytemuck
    /// is unnecessary.
    #[inline]
    pub fn intersect(&mut self, that: &Self) -> Result<()> {
        let (this, this_len) = self.u64s_mut();
        let (that, that_len) = that.u64s();
        if this_len != that_len {
            return Err(Error::InvalidArgument);
        }
        bitmap_intersect(this, this_len, that, that_len);
        Ok(())
    }

    /// Intersects with selection. values outside of selection are set to false.
    #[inline]
    pub fn intersect_sel(&mut self, sel: &Sel) -> Result<()> {
        debug_assert_eq!(self.len(), sel.n_records());
        match sel {
            Sel::All(_) => Ok(()),
            Sel::None(_) => {
                self.reset_zero(self.len());
                Ok(())
            }
            Sel::Index {
                count,
                len,
                indexes,
            } => {
                let len_u1 = *len as usize;
                let mut vals: SmallVec<[(u16, bool); 6]> = smallvec![];
                for idx in &indexes[..*count as usize] {
                    vals.push((*idx, self.get(*idx as usize)?));
                }
                self.reset_zero(len_u1);
                let (u8s, _) = self.u8s_mut(len_u1);
                for (idx, val) in vals {
                    bitmap_u8s_set(u8s, idx as usize, val);
                }
                Ok(())
            }
            Sel::Bitmap(bm) => self.intersect(bm),
        }
    }

    /// Reset to zero.
    #[inline]
    fn reset_zero(&mut self, len_u1: usize) {
        let (u8s, _) = self.u8s_mut(len_u1);
        for u in u8s {
            *u = 0;
        }
    }

    /// Reset to one.
    #[inline]
    fn reset_one(&mut self, len_u1: usize) {
        let (u8s, _) = self.u8s_mut(len_u1);
        for u in u8s {
            *u = 0xff;
        }
    }

    /// Unions given bitmap to current one.
    /// Compiler will apply SIMD optimization automatically, so u64 bytemuck
    /// is unnecessary.
    #[inline]
    pub fn union(&mut self, that: &Self) -> Result<()> {
        let (this, this_len) = self.u64s_mut();
        let (that, that_len) = that.u64s();
        if this_len != that_len {
            return Err(Error::InvalidArgument);
        }
        bitmap_union(this, this_len, that, that_len);
        Ok(())
    }

    /// Union with selection. all unioned values should be set to true.
    #[inline]
    pub fn union_sel(&mut self, sel: &Sel) -> Result<()> {
        debug_assert_eq!(self.len(), sel.n_records());
        match sel {
            Sel::All(_) => {
                self.reset_one(self.len());
                Ok(())
            }
            Sel::None(_) => Ok(()),
            Sel::Index {
                count,
                len,
                indexes,
            } => {
                let len_u1 = *len as usize;
                let count = *count;
                let indexes = *indexes;
                let (u8s, _) = self.u8s_mut(len_u1);
                for idx in &indexes[..count as usize] {
                    bitmap_u8s_set(u8s, *idx as usize, true);
                }
                Ok(())
            }
            Sel::Bitmap(bm) => self.union(bm),
        }
    }

    /// Inverse all values in this bitmap
    #[inline]
    pub fn inverse(&mut self) {
        let (bm, _) = self.u64s_mut();
        bm.iter_mut().for_each(|x| *x = !*x)
    }

    /// Set length of this bitmap.
    ///
    /// # Safety
    ///
    /// Caller must ensure the given length is valid.
    #[inline]
    pub unsafe fn set_len(&mut self, new_len: usize) {
        if let Bitmap::Owned { len_u1, .. } = self {
            *len_u1 = new_len;
        } else {
            panic!("Length of borrowed bitmap is immutable")
        }
    }

    /// Add given flag to end of bitmap.
    #[inline]
    pub fn add(&mut self, val: bool) {
        let len = self.len();
        self.reserve(len + 1);
        let (bm, _) = self.u8s_mut(len + 1);
        bitmap_u8s_set(bm, len, val);
        unsafe { self.set_len(len + 1) }
    }

    /// Extend this bitmap with another one.
    /// The method is very fast if original length is multiply of 8.
    /// Otherwise, we need to do bit shifting for each byte.
    /// Although the performance is tuned, it's still much slower
    /// (4-5x slower) compared to the one of 8x length.
    #[inline]
    pub fn extend(&mut self, that: &Bitmap) {
        let (tbm, tlen) = that.u64s();
        // reserve space to make sure capacity is still multiple of 8
        let orig_len = self.len();
        let tgt_len = orig_len + tlen;
        let dst = self.reserve_u64s(tgt_len);
        bitmap_extend(dst, orig_len, tbm, tlen);
        unsafe { self.set_len(tgt_len) }
    }

    /// Extend this bitmap with another one of given range.
    #[inline]
    pub fn extend_range(&mut self, that: &Bitmap, range: Range<usize>) -> Result<()> {
        let (tbm, tlen) = that.u64s();
        if range.end > tlen {
            return Err(Error::IndexOutOfBound);
        }
        let range_len = range.end - range.start;
        let orig_len = self.len();
        let tgt_len = orig_len + range_len;
        let dst = self.reserve_u64s(tgt_len);
        bitmap_extend_range(dst, orig_len, tbm, range);
        unsafe { self.set_len(tgt_len) }
        Ok(())
    }

    /// Extend this bitmap with constant bool value.
    #[inline]
    pub fn extend_const(&mut self, val: bool, len: usize) {
        let orig_len = self.len();
        let tgt_len = orig_len + len;
        self.reserve(tgt_len);
        let (bm, _) = self.u8s_mut(tgt_len);
        if val {
            bitmap_extend_const(bm, orig_len, true, len);
        } else {
            bitmap_extend_const(bm, orig_len, false, len);
        }
        unsafe { self.set_len(tgt_len) }
    }

    /// Extend this bitmap with bool values.
    pub fn extend_bools<T>(&mut self, bools: T)
    where
        T: IntoIterator<Item = bool>,
    {
        for b in bools {
            self.add(b)
        }
    }

    /// Returns index of first true value.
    #[inline]
    pub fn first_true(&self) -> Option<usize> {
        let mut idx = 0;
        for (f, n) in self.range_iter() {
            if f {
                return Some(idx);
            } else {
                idx += n;
            }
        }
        None
    }
}

impl FromIterator<bool> for Bitmap {
    #[inline]
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = bool>,
    {
        let iter = iter.into_iter();
        let cap = match iter.size_hint() {
            (_, Some(hb)) => hb,
            (_, None) => 64,
        };
        let mut bm = Bitmap::with_capacity(cap);
        let (mut u64s, mut len) = bm.u64s_mut();
        for (idx, (v, l)) in PackBoolsToU64(Some(iter)).enumerate() {
            if idx < u64s.len() {
                u64s[idx] = v;
            } else {
                let new_len = if len == 0 { 64 } else { len * 2 };
                u64s = bm.reserve_u64s(new_len);
                u64s[idx] = v;
            }
            len += l;
        }
        // iterator exhausted
        unsafe { bm.set_len(len) };
        bm
    }
}

struct PackBoolsToU64<T>(Option<T>);

impl<T> Iterator for PackBoolsToU64<T>
where
    T: Iterator<Item = bool>,
{
    type Item = (u64, usize);
    #[inline]
    fn next(&mut self) -> Option<(u64, usize)> {
        if let Some(it) = self.0.as_mut() {
            let mut word = 0u64;
            let mut idx = 0;
            for v in it {
                word |= u64::from(v) << (idx & 63);
                idx += 1;
                if idx == 64 {
                    return Some((word, 64));
                }
            }
            // remove exhausted iterator
            let _ = self.0.take();
            if idx == 0 {
                None
            } else {
                Some((word, idx))
            }
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct BoolIter<'a> {
    bm: &'a [u64],
    len: usize,
    idx: usize,
}

impl Iterator for BoolIter<'_> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == self.len {
            None
        } else {
            let b = bitmap_u64s_get(self.bm, self.idx);
            self.idx += 1;
            Some(b)
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.len - self.idx))
    }
}

/* helper functions */

#[inline]
pub fn bitmap_u64s_get(bm: &[u64], idx: usize) -> bool {
    bm[idx / 64] & (1 << (idx & 63)) != 0
}

#[inline]
pub fn bitmap_u8s_get(bm: &[u8], idx: usize) -> bool {
    bm[idx / 8] & (1 << (idx & 7)) != 0
}

#[inline]
pub fn bitmap_bools(bm: &[u64], len: usize) -> BoolIter<'_> {
    BoolIter { bm, len, idx: 0 }
}

#[inline]
pub fn bitmap_true_count(bm: &[u64], len: usize) -> usize {
    let len_u64 = len / 64;
    let len_remained = len & 63;
    // sum of u64s
    let sum0: usize = bm[..len_u64].iter().map(|v| v.count_ones() as usize).sum();
    if len_remained == 0 {
        sum0
    } else {
        sum0 + (bm[len_u64] & ((1 << len_remained) - 1)).count_ones() as usize
    }
}

#[inline]
pub fn bitmap_false_count(bm: &[u64], len: usize) -> usize {
    let len_u64 = len / 64;
    let len_remained = len & 63;
    // sum of u64s
    let sum0 = bm[..len_u64].iter().map(|v| v.count_zeros() as usize).sum();
    if len_remained == 0 {
        sum0
    } else {
        sum0 + (bm[len_u64] | !((1 << len_remained) - 1)).count_zeros() as usize
    }
}

#[inline]
pub fn bitmap_range_iter(bm: &[u64], len: usize) -> RangeIter<'_> {
    if len == 0 {
        // empty iterator
        return RangeIter {
            u64s: &[],
            last_word_len: 0,
            word: 0,
            word_bits: 0,
            prev: false,
            n: 0,
        };
    }
    let prev = bm[0] & 1 != 0; // pre-read first value
    let last_word_len = if len & 63 == 0 { 64 } else { len & 63 };
    RangeIter {
        u64s: bm,
        last_word_len,
        word: 0,
        word_bits: 0,
        prev,
        n: 0,
    }
}

#[inline]
pub fn bitmap_intersect(this: &mut [u64], this_len: usize, that: &[u64], that_len: usize) {
    debug_assert!(this_len == that_len);
    this.iter_mut().zip(that.iter()).for_each(|(a, b)| *a &= b);
}

#[inline]
pub fn bitmap_union(this: &mut [u64], this_len: usize, that: &[u64], that_len: usize) {
    debug_assert!(this_len == that_len);
    this.iter_mut().zip(that.iter()).for_each(|(a, b)| *a |= b);
}

#[derive(Debug, Clone)]
pub struct RangeIter<'a> {
    u64s: &'a [u64],      // slice of u64
    last_word_len: usize, // length of last word
    word: u64,            // current u64 word to scan
    word_bits: usize,     // maximum bits in current word
    prev: bool,           // previous value (true/flase)
    n: usize,             // previous repeat number
}

impl RangeIter<'_> {
    #[inline]
    fn break_falses_in_word(&mut self) {
        debug_assert!(self.prev);
        let bits = self.word_bits.min(self.word.trailing_zeros() as usize);
        if bits == 64 {
            self.word = 0;
        } else {
            self.word >>= bits;
        }
        self.prev = false;
        self.n = bits;
        self.word_bits -= bits;
    }

    #[inline]
    fn continue_falses_in_word(&mut self) {
        debug_assert!(!self.prev);
        let bits = self.word_bits.min(self.word.trailing_zeros() as usize);
        if bits == 64 {
            self.word = 0;
        } else {
            self.word >>= bits;
        }
        self.word_bits -= bits;
        self.n += bits;
    }

    #[inline]
    fn break_trues_in_word(&mut self) {
        debug_assert!(!self.prev);
        let bits = self.word_bits.min(self.word.trailing_ones() as usize);
        if bits == 64 {
            self.word = 0;
        } else {
            self.word >>= bits;
        }
        self.prev = true;
        self.n = bits;
        self.word_bits -= bits;
    }

    #[inline]
    fn continue_trues_in_word(&mut self) {
        debug_assert!(self.prev);
        let bits = self.word_bits.min(self.word.trailing_ones() as usize);
        if bits == 64 {
            self.word = 0;
        } else {
            self.word >>= bits;
        }
        self.word_bits -= bits;
        self.n += bits;
    }
}

impl Iterator for RangeIter<'_> {
    type Item = (bool, usize);
    /// Returns bool value with its repeat number.
    /// The implementation scans the bitmap on two levels.
    /// u64 word level and bit level.
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.word_bits == 0 {
            'INIT_WORD: loop {
                match self.u64s.len() {
                    0 => {
                        if self.n == 0 {
                            // iterator exhausted
                            return None;
                        } else {
                            // output last item
                            let rg = (self.prev, self.n);
                            self.n = 0;
                            return Some(rg);
                        }
                    }
                    1 => {
                        // prepare last word
                        self.word = self.u64s[0];
                        self.word_bits = self.last_word_len;
                        self.u64s = &[];
                        if self.prev {
                            self.continue_trues_in_word();
                            if self.n == 0 {
                                self.prev = !self.prev;
                                self.continue_falses_in_word();
                            }
                        } else {
                            self.continue_falses_in_word();
                            if self.n == 0 {
                                self.prev = !self.prev;
                                self.continue_trues_in_word();
                            }
                        }
                        break 'INIT_WORD;
                    }
                    _ => {
                        // fast-scan current word
                        self.word = self.u64s[0];
                        self.u64s = &self.u64s[1..];
                        match self.word {
                            0 => {
                                if self.prev {
                                    // all falses and prev is true
                                    let rg = (self.prev, self.n);
                                    self.prev = false;
                                    self.n = 64;
                                    return Some(rg);
                                } else {
                                    // all falses and prev is also false
                                    self.n += 64;
                                }
                            }
                            0xffff_ffff_ffff_ffff => {
                                if !self.prev {
                                    // all trues and prev is false
                                    let rg = (self.prev, self.n);
                                    self.prev = true;
                                    self.n = 64;
                                    return Some(rg);
                                } else {
                                    // all trues and prev is also true
                                    self.n += 64;
                                }
                            }
                            _ => {
                                self.word_bits = 64;
                                if self.prev {
                                    self.continue_trues_in_word();
                                    if self.n == 0 {
                                        self.prev = !self.prev;
                                        self.continue_falses_in_word();
                                    }
                                } else {
                                    self.continue_falses_in_word();
                                    if self.n == 0 {
                                        self.prev = !self.prev;
                                        self.continue_trues_in_word();
                                    }
                                }
                                break 'INIT_WORD;
                            }
                        }
                    }
                }
            }
        }
        let ret = (self.prev, self.n);
        if self.prev {
            self.break_falses_in_word();
        } else {
            self.break_trues_in_word();
        }
        Some(ret)
    }
}

pub struct TrueIndexIter<'a> {
    range_iter: RangeIter<'a>,
    start: usize,
    end: usize,
}

impl Iterator for TrueIndexIter<'_> {
    type Item = usize;
    #[inline]
    fn next(&mut self) -> Option<usize> {
        if self.start < self.end {
            let idx = self.start;
            self.start += 1;
            return Some(idx);
        }
        for (flag, n) in self.range_iter.by_ref() {
            if flag {
                self.end += n;
                if self.start < self.end {
                    let idx = self.start;
                    self.start += 1;
                    return Some(idx);
                }
            } else {
                self.start += n;
                self.end += n;
            }
        }
        None
    }
}

#[inline]
pub fn bitmap_u8s_set(bm: &mut [u8], idx: usize, val: bool) {
    let bidx = idx / 8;
    if val {
        bm[bidx] |= 1 << (idx & 7);
    } else {
        bm[bidx] &= !(1 << (idx & 7));
    }
}

#[allow(clippy::manual_div_ceil)]
#[inline]
fn bitmap_u64s_shift(bs: &mut [u64], len: usize, shift_bits: usize) {
    if shift_bits >= len || shift_bits == 0 {
        return;
    }
    let orig_len_u64 = (len + 63) / 64;
    if shift_bits & 7 == 0 {
        // memcpy
        let offset_u8 = shift_bits / 8;
        let u8s = bytemuck::cast_slice_mut::<_, u8>(&mut bs[..orig_len_u64]);
        u8s.copy_within(offset_u8.., 0);
        return;
    }
    let offset_u64 = shift_bits / 64;
    let bits = shift_bits & 63;
    if offset_u64 == 0 {
        let bs = &mut bs[..orig_len_u64];
        // Use macro to unroll below expression for better performance.
        //
        // bs[..orig_len_u64].for_each_offset_pair(1, |(a, b)| {
        //     *a >>= bits;
        //     *a |= *b << rbits;
        // });
        macro_rules! shift_with_pair {
            ($bits:literal) => {
                bs[..orig_len_u64].for_each_offset_pair(1, |(a, b)| {
                    *a >>= $bits;
                    *a |= *b << (64 - $bits);
                })
            };
        }
        macro_rules! shift_branched_pairs {
            ($bits:expr, $($branch:literal),+) => {
                match $bits {
                    $(
                        $branch => shift_with_pair!($branch),
                    )+
                    _ => unreachable!(),
                }
            }
        }
        shift_branched_pairs!(
            bits, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
            23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44,
            45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63
        );
        bs[orig_len_u64 - 1] >>= bits;
    } else {
        let bs = &mut bs[..orig_len_u64];
        // Use macro to unroll below expression for better performance.
        //
        // bs.for_each_offset_triple(offset_u64, offset_u64+1, |(a, b, c)| {
        //     *a = (*b >> bits) | (*c << rbits);
        // });
        macro_rules! shift_with_triple {
            ($bits:literal) => {
                bs.for_each_offset_triple(
                    offset_u64,
                    offset_u64 + 1,
                    |(a, b, c): (&mut u64, &u64, &u64)| {
                        *a = (*b >> $bits) | (*c << (64 - $bits));
                    },
                )
            };
        }
        macro_rules! shift_branched_triples {
            ($bits:expr, $($branch:literal),+) => {
                match $bits {
                    $(
                        $branch => shift_with_triple!($branch),
                    )+
                    _ => unreachable!(),
                }
            }
        }
        shift_branched_triples!(
            bits, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
            23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44,
            45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63
        );
        bs[orig_len_u64 - offset_u64 - 1] = bs[orig_len_u64 - 1] >> bits;
        bs[orig_len_u64 - 1] = 0;
    }
}

#[allow(clippy::manual_div_ceil)]
#[inline]
fn bitmap_extend(dst: &mut [u64], dst_len: usize, src: &[u64], src_len: usize) {
    debug_assert!(src.len() * 64 >= src_len);
    debug_assert!(dst.len() * 64 >= dst_len + src_len);
    if src_len == 0 {
        // nothing to do
        return;
    }
    if dst_len & 7 == 0 {
        let src_len_u8 = (src_len + 7) / 8;
        let offset_u8 = dst_len / 8;
        let src_u8s = bytemuck::cast_slice::<_, u8>(src);
        let dst_u8s = bytemuck::cast_slice_mut::<_, u8>(dst);
        dst_u8s[offset_u8..offset_u8 + src_len_u8].copy_from_slice(&src_u8s[..src_len_u8]);
        return;
    }
    let rbits = dst_len & 63;
    let bits = 64 - rbits;
    let tgt_len = dst_len + src_len;
    let tgt_len_u64 = (tgt_len + 63) / 64;
    let src_len_u64 = (src_len + 63) / 64;
    let orig_dst_len_u64 = dst_len / 64;
    let tgt = &mut dst[orig_dst_len_u64..tgt_len_u64];
    let src = &src[..src_len_u64];
    // update first u64 of target
    let first_u64 = &mut tgt[0];
    *first_u64 &= (1 << rbits) - 1;
    *first_u64 |= src[0] << rbits;
    // update with src pairs
    // benchmark shows unrolling the bits literal with macros decrease
    // performance a bit(15~20%), so keep it as is.
    tgt[1..]
        .iter_mut()
        .zip(src.pairs())
        .for_each(|(a, (b, c))| {
            *a = *b >> bits;
            *a |= *c << rbits;
        });
    if src_len & 63 > bits || src_len & 63 == 0 {
        // addtional u64 to store the rest of bits
        tgt[tgt.len() - 1] = src[src_len_u64 - 1] >> bits;
    }
}

#[allow(clippy::manual_div_ceil)]
#[inline]
fn bitmap_extend_range(dst: &mut [u64], dst_len: usize, src: &[u64], range: Range<usize>) {
    debug_assert!(src.len() * 64 >= range.end);
    let tgt_len = dst_len + range.end - range.start;
    debug_assert!(dst.len() * 64 >= tgt_len);
    if range.start & 63 == 0 {
        // start aligned to byte bound, reuse copy_bits
        bitmap_extend(
            dst,
            dst_len,
            &src[range.start / 64..(range.end + 63) / 64],
            range.end - range.start,
        );
        return;
    }
    if dst_len & 7 == range.start & 7 {
        // last few dst bits and first src bits just compose one byte
        let dst_u8s = bytemuck::cast_slice_mut::<_, u8>(dst);
        let src_u8s = bytemuck::cast_slice::<_, u8>(src);

        let rbits = dst_len & 7;
        let dst_last_idx = dst_len / 8;
        let dst_last_byte = &mut dst_u8s[dst_last_idx];
        *dst_last_byte &= (1 << rbits) - 1;
        let src_first_byte = src_u8s[range.start / 64];
        *dst_last_byte |= src_first_byte & !((1 << rbits) - 1);
        let src_start = range.start / 8;
        let src_end = (range.end + 7) / 8;
        dst_u8s[dst_last_idx + 1..dst_last_idx + src_end - src_start]
            .copy_from_slice(&src_u8s[src_start + 1..src_end]);
        return;
    }
    // try u64
    let rbits = dst_len & 63;
    let src_start_bits = range.start & 63;
    if rbits < src_start_bits {
        // shift right to fill dst
        let shr_bits = src_start_bits - rbits;
        let shl_bits = 64 - shr_bits;
        let orig_dst_len_u64 = dst_len / 64;
        let tgt_len_u64 = (tgt_len + 63) / 64;
        let tgt = &mut dst[orig_dst_len_u64..tgt_len_u64];
        let src_start_u64 = range.start / 64;
        if range.end / 64 == src_start_u64 {
            // within u64 boundary
            // only require shift within single word
            tgt[0] &= (1 << rbits) - 1;
            let extra = (src[src_start_u64] >> shr_bits) & !((1 << rbits) - 1);
            tgt[0] |= extra;
            return;
        }
        let src_end_u64 = (range.end + 63) / 64;
        // save first u64 and then clear it, handling it after pair updates
        let first_u64 = tgt[0] & ((1 << rbits) - 1);
        tgt[0] = 0;
        // pair update
        tgt.iter_mut()
            .zip(src[src_start_u64..src_end_u64].pairs())
            .for_each(|(a, (b, c))| {
                *a = (*b >> shr_bits) | (*c << shl_bits);
            });
        // update first u64
        tgt[0] &= !((1 << rbits) - 1); // clear original bits
        tgt[0] |= first_u64; // update original value back
        if (range.end & 63) > shr_bits {
            // additional u64 to update last few bits from src
            tgt[tgt.len() - 1] = src[src_end_u64 - 1] >> shr_bits;
        }
    } else {
        // shift left to fill dst
        let shl_bits = rbits - src_start_bits;
        let shr_bits = 64 - shl_bits;
        let orig_dst_len_u64 = dst_len / 64;
        let tgt_len_u64 = (tgt_len + 63) / 64;
        let tgt = &mut dst[orig_dst_len_u64..tgt_len_u64];
        let src_start_u64 = range.start / 64;
        let src_end_u64 = (range.end + 63) / 64;
        // update first u64
        tgt[0] &= (1 << rbits) - 1;
        tgt[0] |= (src[src_start_u64] << shl_bits) & !((1 << rbits) - 1);
        if tgt.len() == 1 {
            return;
        }
        tgt[1..]
            .iter_mut()
            .zip(src[src_start_u64..src_end_u64].pairs())
            .for_each(|(a, (b, c))| {
                *a = b >> shr_bits;
                *a |= c << shl_bits;
            });
        // update last u64
        if (range.end & 63) > shr_bits {
            // additional u64 to update last few bits from src
            tgt[tgt.len() - 1] = src[src_end_u64 - 1] >> shr_bits;
        }
    }
}

#[allow(clippy::manual_div_ceil)]
#[inline]
fn bitmap_extend_const(dst: &mut [u8], dst_len: usize, src_val: bool, src_len: usize) {
    debug_assert!(dst.len() * 8 >= dst_len + src_len);
    if src_len == 0 {
        // nothing to do
        return;
    }
    if dst_len & 7 == 0 {
        // copy bytes
        let byte = if src_val { 0xff } else { 0x00 };
        let src_len_u8 = (src_len + 7) / 8;
        let dst_len_u8 = dst_len / 8;
        dst[dst_len_u8..dst_len_u8 + src_len_u8]
            .iter_mut()
            .for_each(|b| *b = byte);
        return;
    }
    let rbits = dst_len & 7;
    let tgt_len = dst_len + src_len;
    let tgt_len_u8 = (tgt_len + 7) / 8;
    let dst_len_u8 = dst_len / 8;
    // update first u8 of destination
    if src_val {
        dst[dst_len_u8] |= !((1 << rbits) - 1);
    } else {
        dst[dst_len_u8] &= (1 << rbits) - 1;
    }
    // update all other bytes
    let byte = if src_val { 0xff } else { 0 };
    dst[dst_len_u8 + 1..tgt_len_u8]
        .iter_mut()
        .for_each(|b| *b = byte);
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[test]
    fn test_bitmap_simple() -> Result<()> {
        let mut bm = Bitmap::with_capacity(10);
        assert_eq!(0, bm.len());
        assert!(bm.is_empty());
        bm.add(true);
        bm.add(false);
        bm.add(true);
        assert_eq!(3, bm.len());
        assert!(bm.get(0)?);
        assert!(!bm.get(1)?);
        assert!(bm.get(2)?);
        bm.clear();
        assert_eq!(0, bm.len());
        Ok(())
    }

    fn custom_bools(shift_n: usize) -> (Vec<bool>, Vec<bool>) {
        let bools = vec![
            true, false, true, true, false, false, true, false, true, true, true, false, false,
            true, false, true, true, false, true, true, false, false, true, false, true, true,
            true, false, false, true, false, true,
        ];
        let shifted = bools.iter().skip(shift_n).cloned().collect();
        (bools, shifted)
    }

    #[test]
    fn test_bitmap_shift() {
        for shift_bits in vec![1, 8, 9, 17, 21] {
            let mut bm = Bitmap::with_capacity(64);
            let (bools, expected) = custom_bools(shift_bits);
            for v in &bools {
                bm.add(*v);
            }
            // shift
            bm.shift(shift_bits);
            let shifted: Vec<_> = bm.bools().collect();
            assert_eq!(expected, shifted);
        }
    }

    // generate random bitmaps and test shift operations
    #[test]
    fn test_bitmap_rand_shift() {
        let mut rng = rand::rng();
        let mut bm = Bitmap::with_capacity(1024);
        let mut bools: Vec<bool> = Vec::new();
        for _ in 0..256 {
            bm.clear();
            bools.clear();
            let size: usize = rng.random_range(128..1024 * 10);
            for _ in 0..size {
                bools.push(rng.random());
            }
            for b in &bools {
                bm.add(*b);
            }
            let bits = rng.random_range(0..size - 127);
            bm.shift(bits);
            let shifted: Vec<_> = bm.bools().collect();
            let expected: Vec<_> = bools.iter().skip(bits).cloned().collect();
            assert_eq!(expected, shifted);
        }
    }

    #[test]
    fn test_bitmap_extend() {
        rand_bitmap_extend(1, 1);
        rand_bitmap_extend(32, 33);
        rand_bitmap_extend(31, 35);
        rand_bitmap_extend(31, 75);
        rand_bitmap_extend(75, 31);
        rand_bitmap_extend(75, 75);
        rand_bitmap_extend(110, 129);
        rand_bitmap_extend(59, 197);
        rand_bitmap_extend(35, 64);
    }

    // generate random bitmaps and test shift operations
    #[test]
    fn test_bitmap_rand_extend() {
        let mut rng = rand::rng();
        for _ in 0..1024 {
            let size1: usize = rng.random_range(0..256);
            let mut bools1 = Vec::with_capacity(size1);
            let size2: usize = rng.random_range(0..256);
            let mut bools2 = Vec::with_capacity(size2);
            for _ in 0..size1 {
                bools1.push(rng.random());
            }
            for _ in 0..size2 {
                bools2.push(rng.random());
            }
            let expected: Vec<_> = bools1.iter().chain(bools2.iter()).cloned().collect();
            let mut bm1 = Bitmap::with_capacity(1024);
            bm1.extend_bools(bools1);
            let mut bm2 = Bitmap::with_capacity(1024);
            bm2.extend_bools(bools2);
            bm1.extend(&bm2);
            let actual: Vec<_> = bm1.bools().collect();
            assert_eq!(
                expected,
                actual,
                "bm1={}, bm2={}, bools={}",
                bm1.len(),
                bm2.len(),
                expected.len()
            );
        }
    }

    #[test]
    fn test_bitmap_extend_const() {
        let mut bm1 = Bitmap::with_capacity(64);
        bm1.add(true);
        bm1.extend_const(false, 1);
        assert_eq!(vec![true, false], bm1.bools().collect::<Vec<_>>());
        bm1.extend_const(true, 10);
        assert_eq!(
            vec![true, false]
                .into_iter()
                .chain(vec![true; 10].into_iter())
                .collect::<Vec<_>>(),
            bm1.bools().collect::<Vec<_>>()
        );
        bm1.extend_const(false, 65);
        assert_eq!(
            vec![true, false]
                .into_iter()
                .chain(std::iter::repeat(true).take(10))
                .chain(std::iter::repeat(false).take(65))
                .collect::<Vec<_>>(),
            bm1.bools().collect::<Vec<_>>()
        );
        bm1.clear();
        bm1.extend_const(true, 8);
        assert_eq!(vec![true; 8], bm1.bools().collect::<Vec<_>>());
    }

    #[test]
    fn test_bitmap_extend_range() -> Result<()> {
        // case 1
        let mut bm1 = Bitmap::with_capacity(1024);
        let bm2 = Bitmap::from_iter(
            std::iter::repeat(true)
                .take(100)
                .chain(std::iter::repeat(false).take(200))
                .chain(std::iter::repeat(true).take(300)),
        );
        bm1.extend_range(&bm2, 0..5)?;
        assert_eq!(vec![true; 5], bm1.bools().collect::<Vec<_>>());
        bm1.extend_range(&bm2, 90..110)?;
        assert_eq!(
            std::iter::repeat(true)
                .take(15)
                .chain(std::iter::repeat(false).take(10))
                .collect::<Vec<_>>(),
            bm1.bools().collect::<Vec<_>>(),
        );
        bm1.extend_range(&bm2, 50..550)?;
        assert_eq!(525, bm1.len());
        assert_eq!(
            std::iter::repeat(true)
                .take(15)
                .chain(std::iter::repeat(false).take(10))
                .chain(std::iter::repeat(true).take(50))
                .chain(std::iter::repeat(false).take(200))
                .chain(std::iter::repeat(true).take(250))
                .collect::<Vec<_>>(),
            bm1.bools().collect::<Vec<_>>(),
        );
        // case 2
        let mut bm1 = Bitmap::from_iter(vec![true, false, true]);
        let bm2 = Bitmap::from_iter(vec![true, false, true, true, false, true]);
        bm1.extend_range(&bm2, 3..5)?;
        assert_eq!(
            vec![true, false, true, true, false],
            bm1.bools().collect::<Vec<_>>()
        );
        // case 3
        let mut bm1 = Bitmap::from_iter(vec![false; 40]);
        bm1.add(true);
        let bm2 = Bitmap::from_iter(vec![true; 64]);
        bm1.extend_range(&bm2, 10..15)?;
        assert_eq!(
            std::iter::repeat(false)
                .take(40)
                .chain(std::iter::repeat(true).take(6))
                .collect::<Vec<_>>(),
            bm1.bools().collect::<Vec<_>>(),
        );
        // case 4
        let mut bm1 = Bitmap::with_capacity(1024);
        let bm2 = Bitmap::from_iter(vec![true, true, true, false, true, true, true, true, true]);
        bm1.extend_range(&bm2, 3..4)?;
        assert_eq!(vec![false], bm1.bools().collect::<Vec<_>>());
        bm1.extend_range(&bm2, 5..7)?;
        assert_eq!(vec![false, true, true], bm1.bools().collect::<Vec<_>>());
        // case 5
        let mut bm1 = Bitmap::from_iter(vec![false, true, false, false, true]);
        let bm2 = Bitmap::from_iter(vec![true, true, false, false, true, false, false, true]);
        bm1.extend_range(&bm2, 2..5)?;
        assert_eq!(
            vec![false, true, false, false, true, false, false, true],
            bm1.bools().collect::<Vec<_>>()
        );
        // case 6
        let mut bm1 = Bitmap::from_iter(vec![false, true, false, false, true]);
        let bm2 = Bitmap::from_iter(vec![true; 128]);
        bm1.extend_range(&bm2, 2..68)?;
        let mut expected = vec![true; 71];
        expected[0] = false;
        expected[2] = false;
        expected[3] = false;
        assert_eq!(expected, bm1.bools().collect::<Vec<_>>());
        // case 7
        let mut bm1 = Bitmap::from_iter(vec![true; 40]);
        let bm2 = Bitmap::from_iter(vec![false; 128]);
        bm1.extend_range(&bm2, 35..75)?;
        assert_eq!(80, bm1.len());
        let expected: Vec<_> = std::iter::repeat(true)
            .take(40)
            .chain(std::iter::repeat(false).take(40))
            .collect();
        assert_eq!(expected, bm1.bools().collect::<Vec<_>>());
        Ok(())
    }

    #[test]
    fn test_bitmap_from_iter() -> Result<()> {
        let bm1 = Bitmap::from_iter(vec![true; 10]);
        assert_eq!(vec![true; 10], bm1.bools().collect::<Vec<_>>());
        Ok(())
    }

    #[test]
    fn test_bitmap_count() {
        for i in vec![1, 7, 8, 9, 31, 32, 33, 63, 64, 65, 1023, 1024, 1025] {
            rand_bitmap_count(i)
        }
        for _ in 0..256 {
            rand_bitmap_count(rand::rng().gen_range(0..4096))
        }
        let bools1: Vec<bool> = vec![];
        let bm1 = Bitmap::from_iter(bools1);
        assert_eq!(0, bm1.true_count());
    }

    #[test]
    fn test_bitmap_range_iter1() {
        // case 1
        let mut bm = Bitmap::with_capacity(1024);
        for _ in 0..4 {
            bm.add(true);
        }
        assert_eq!((true, 4), bm.range_iter().next().unwrap());
        for _ in 0..4 {
            bm.add(true);
        }
        assert_eq!((true, 8), bm.range_iter().next().unwrap());
        for _ in 0..16 {
            bm.add(true);
        }
        assert_eq!((true, 24), bm.range_iter().next().unwrap());
        // case 2
        let mut bm = Bitmap::with_capacity(1024);
        for _ in 0..4 {
            bm.add(false);
        }
        for _ in 0..2 {
            bm.add(true);
        }
        for _ in 0..4 {
            bm.add(true);
        }
        let mut iter = bm.range_iter();
        assert_eq!((false, 4), iter.next().unwrap());
        assert_eq!((true, 6), iter.next().unwrap());
        assert!(iter.next().is_none());
        // case 3
        let mut bm = Bitmap::with_capacity(1024);
        for _ in 0..4 {
            bm.add(true);
        }
        for _ in 0..8 {
            bm.add(false);
        }
        let mut iter = bm.range_iter();
        assert_eq!((true, 4), iter.next().unwrap());
        assert_eq!((false, 8), iter.next().unwrap());
        assert!(iter.next().is_none());
        // case 4
        let mut bm = Bitmap::with_capacity(1024);
        for _ in 0..7 {
            bm.add(false);
        }
        bm.add(true);
        let mut iter = bm.range_iter();
        assert_eq!((false, 7), iter.next().unwrap());
        assert_eq!((true, 1), iter.next().unwrap());
        assert!(iter.next().is_none());
        // case 5
        let mut bm = Bitmap::with_capacity(64);
        bm.add(true);
        for _ in 0..3 {
            bm.add(false);
        }
        bm.add(true);
        for _ in 0..3 {
            bm.add(false);
        }
        let mut iter = bm.range_iter();
        assert_eq!((true, 1), iter.next().unwrap());
        assert_eq!((false, 3), iter.next().unwrap());
        assert_eq!((true, 1), iter.next().unwrap());
        assert_eq!((false, 3), iter.next().unwrap());
        assert!(iter.next().is_none());
        // case 6
        let mut bm = Bitmap::with_capacity(64);
        bm.add(true);
        for _ in 0..3 {
            bm.add(false);
        }
        bm.add(true);
        let mut iter = bm.range_iter();
        assert_eq!((true, 1), iter.next().unwrap());
        assert_eq!((false, 3), iter.next().unwrap());
        assert_eq!((true, 1), iter.next().unwrap());
        assert!(iter.next().is_none());
        // case 7
        let mut bm = Bitmap::with_capacity(64);
        bm.add(true);
        for _ in 0..15 {
            bm.add(false);
        }
        bm.add(true);
        for _ in 0..3 {
            bm.add(false);
        }
        for _ in 0..7 {
            bm.add(true);
        }
        let mut iter = bm.range_iter();
        assert_eq!((true, 1), iter.next().unwrap());
        assert_eq!((false, 15), iter.next().unwrap());
        assert_eq!((true, 1), iter.next().unwrap());
        assert_eq!((false, 3), iter.next().unwrap());
        assert_eq!((true, 7), iter.next().unwrap());
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_bitmap_range_iter2() -> Result<()> {
        for bs in vec![
            vec![true],
            vec![false],
            vec![true, false],
            vec![false, true],
            vec![true, false, true],
            vec![false, true, false],
            vec![true; 31],
            vec![false; 31],
            vec![true; 33],
            vec![false; 33],
            vec![true; 65],
            vec![false; 65],
            vec![true; 65].into_iter().chain(vec![false; 31]).collect(),
            vec![false; 65].into_iter().chain(vec![true; 63]).collect(),
            vec![false; 253]
                .into_iter()
                .chain(vec![true; 311])
                .chain(vec![false; 471])
                .collect(),
        ] {
            let expected = bs.len();
            let mut bm = Bitmap::with_capacity(64);
            bm.extend_bools(bs);
            let mut actual = 0;
            for (_, n) in bm.range_iter() {
                actual += n;
            }
            assert_eq!(expected, actual);
        }
        Ok(())
    }

    #[test]
    fn test_bitmap_rand_range_iter() {
        let mut rng = rand::rng();
        for _ in 0..128 {
            let size: usize = rng.random_range(128..4096);
            let mut bools: Vec<bool> = Vec::with_capacity(size);
            for _ in 0..size {
                bools.push(rng.random());
            }
            let bm = Bitmap::from_iter(bools.clone());
            let mut base = 0;
            for (b, n) in bm.range_iter() {
                assert!(n != 0);
                for i in 0..n {
                    assert_eq!(b, bools[base + i]);
                }
                base += n;
            }
        }
    }

    #[test]
    fn test_bitmap_empty_range_iter() {
        let bm = Bitmap::with_capacity(64);
        let mut count = 0;
        for (_, n) in bm.range_iter() {
            count += n;
        }
        assert_eq!(0, count);
    }

    #[test]
    fn test_bitmap_merge() {
        let mut bm0 = Bitmap::from_iter(vec![true, false, true, false]);
        let bm1 = Bitmap::from_iter(vec![false, false, true]);
        assert!(bm0.intersect(&bm1).is_err());
        let bm2 = Bitmap::from_iter(vec![false, false, true, true]);
        bm0.intersect(&bm2).unwrap();
        assert_eq!(
            vec![false, false, true, false],
            bm0.bools().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_bitmap_reset() {
        let mut bm0 = Bitmap::zeroes(1024);
        let (u8s, _) = bm0.u8s(1024);
        for b in u8s {
            assert_eq!(0, *b);
        }
        bm0.reset_one(1024);
        let (u8s, _) = bm0.u8s(1024);
        for b in u8s {
            assert_eq!(0xff, *b);
        }
        let mut bm1 = Bitmap::ones(1024);
        let (u8s, _) = bm1.u8s(1024);
        for b in u8s {
            assert_eq!(0xff, *b);
        }
        bm1.reset_zero(1024);
        let (u8s, _) = bm1.u8s(1024);
        for b in u8s {
            assert_eq!(0, *b);
        }
    }

    #[test]
    fn test_bitmap_union() {
        let mut bm0 = Bitmap::zeroes(1024);
        bm0.union_sel(&Sel::All(1024)).unwrap();
        assert_eq!(1024, bm0.true_count());
        let mut bm0 = Bitmap::zeroes(1024);
        bm0.union_sel(&Sel::None(1024)).unwrap();
        assert_eq!(0, bm0.true_count());
        let mut bm0 = Bitmap::zeroes(1024);
        bm0.union_sel(&Sel::new_indexes(1024, vec![1, 5, 100]))
            .unwrap();
        assert_eq!(3, bm0.true_count());
        let mut bm0 = Bitmap::zeroes(1024);
        bm0.union_sel(&Sel::Bitmap(Arc::new(Bitmap::ones(1024))))
            .unwrap();
        assert_eq!(1024, bm0.true_count());
    }

    #[test]
    fn test_bitmap_borrow() {
        let raw = vec![0xffu8; 16].into_boxed_slice();
        let raw: Arc<[u8]> = Arc::from(raw);
        let mut bm = Bitmap::new_borrowed(Arc::clone(&raw), 128, 0);
        assert_eq!(16, bm.total_bytes());
        assert_eq!(vec![0xff; 16], bm.raw());
        let (u8s, _) = bm.u8s(16);
        assert_eq!(vec![0xff; 2], u8s);
        bm.inverse();
        assert_eq!(vec![0u8; 16], bm.raw());
        let bm = Bitmap::new_borrowed(Arc::clone(&raw), 128, 0);
        let bm = Arc::new(bm);
        let bm = Bitmap::clone_to_owned(&bm);
        assert!(bm.is_owned());
        assert_eq!(vec![0xff; 16], bm.raw());
        let mut bm = Bitmap::new_borrowed(raw, 128, 0);
        bm.reserve(64);
        assert!(!bm.is_owned());
        bm.reserve(256);
        assert!(bm.is_owned());
    }

    fn rand_bitmap_count(n: usize) {
        let mut rng = rand::rng();
        let mut bools1: Vec<bool> = Vec::with_capacity(n);
        for _ in 0..n {
            bools1.push(rng.random());
        }
        let expected_true: usize = bools1.iter().map(|b| if *b { 1 } else { 0 }).sum();
        let expected_false: usize = bools1.iter().map(|b| if *b { 0 } else { 1 }).sum();
        let mut bm1 = Bitmap::with_capacity(64);
        bm1.extend_bools(bools1);
        let actual_true = bm1.true_count();
        let actual_false = bm1.false_count();
        assert_eq!(expected_true, actual_true);
        assert_eq!(expected_false, actual_false);
    }

    fn rand_bitmap_extend(n1: usize, n2: usize) {
        let mut rng = rand::rng();
        let mut bools1: Vec<bool> = Vec::with_capacity(n1);
        for _ in 0..n1 {
            bools1.push(rng.random());
        }
        let mut bools2: Vec<bool> = Vec::with_capacity(n2);
        for _ in 0..n2 {
            bools2.push(rng.random());
        }
        let expected: Vec<_> = bools1.iter().chain(bools2.iter()).cloned().collect();
        let mut bm1 = Bitmap::with_capacity(64);
        bm1.extend_bools(bools1);
        let mut bm2 = Bitmap::with_capacity(64);
        bm2.extend_bools(bools2);
        bm1.extend(&bm2);
        let actual: Vec<_> = bm1.bools().collect();
        assert_eq!(expected, actual);
    }
}
