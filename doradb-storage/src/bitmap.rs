use parking_lot::Mutex;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Trait to extend u64 slice with bitmap functionalities.
/// To avoid naming conflicts, all methods are prefixed with "bitmap_".
pub trait Bitmap {
    /// Returns bool value at given bit.
    fn bitmap_get(&self, idx: usize) -> bool;

    /// Returns unit at given position.
    fn bitmap_unit(&self, unit_idx: usize) -> u64;

    /// Returns all units.
    fn bitmap_units(&self) -> &[u64];

    /// Set bit to true at given position.
    fn bitmap_set(&mut self, idx: usize) -> bool;

    /// Unset given bit to be false.
    fn bitmap_unset(&mut self, idx: usize) -> bool;

    /// Returns mutable units.
    fn bitmap_units_mut(&mut self) -> &mut [u64];

    /// Set the first zero bit to true within given range.
    #[inline]
    fn bitmap_set_first(&mut self, unit_start_idx: usize, unit_end_idx: usize) -> Option<usize> {
        if unit_start_idx >= unit_end_idx {
            return None;
        }
        let mut unit_idx = unit_start_idx;
        for v in &mut self.bitmap_units_mut()[unit_start_idx..unit_end_idx] {
            let bit_idx = (*v).trailing_ones();
            if bit_idx < 64 {
                *v |= 1 << bit_idx;
                return Some(unit_idx * 64 + bit_idx as usize);
            }
            unit_idx += 1;
        }
        None
    }

    /// Create a range iterator at most len bits.
    /// It collapses consecutive true/false values into ranges with counts.
    #[inline]
    fn bitmap_range_iter(&self, len: usize) -> BitmapRangeIter<'_> {
        debug_assert!(len <= self.bitmap_units().len() * 64);
        if len == 0 {
            // empty iterator
            return BitmapRangeIter {
                u64s: &[],
                last_word_len: 0,
                word: 0,
                word_bits: 0,
                prev: false,
                n: 0,
            };
        }
        let prev = self.bitmap_unit(0) & 1 != 0; // pre-read first value
        let last_word_len = if len & 63 == 0 { 64 } else { len & 63 };
        BitmapRangeIter {
            u64s: self.bitmap_units(),
            last_word_len,
            word: 0,
            word_bits: 0,
            prev,
            n: 0,
        }
    }

    /// Create index iterator with all true bits, stop at len.
    #[inline]
    fn bitmap_true_index_iter(&self, len: usize) -> BitmapTrueIndexIter<'_> {
        let range_iter = self.bitmap_range_iter(len);
        BitmapTrueIndexIter {
            range_iter,
            start: 0,
            end: 0,
        }
    }
}

impl Bitmap for [u64] {
    #[inline]
    fn bitmap_get(&self, idx: usize) -> bool {
        let unit_idx = idx / 64;
        let bit_idx = idx % 64;
        let v = self[unit_idx];
        v & (1 << bit_idx) == (1 << bit_idx)
    }

    #[inline]
    fn bitmap_unit(&self, unit_idx: usize) -> u64 {
        self[unit_idx]
    }

    #[inline]
    fn bitmap_units(&self) -> &[u64] {
        self
    }

    #[inline]
    fn bitmap_set(&mut self, idx: usize) -> bool {
        let unit_idx = idx / 64;
        let bit_idx = idx % 64;
        if self[unit_idx] & (1 << bit_idx) != 0 {
            return false;
        }
        self[unit_idx] |= 1 << bit_idx;
        true
    }

    #[inline]
    fn bitmap_unset(&mut self, idx: usize) -> bool {
        let unit_idx = idx / 64;
        let bit_idx = idx % 64;
        if self[unit_idx] & (1 << bit_idx) == 0 {
            return false;
        }
        self[unit_idx] &= !(1 << bit_idx);
        true
    }

    #[inline]
    fn bitmap_units_mut(&mut self) -> &mut [u64] {
        self
    }
}

/// Create a new bitmap with all zeros.
#[allow(clippy::manual_div_ceil)]
#[inline]
pub fn new_bitmap(nbr_of_bits: usize) -> Box<[u64]> {
    let len = (nbr_of_bits + 63) / 64;
    vec![0u64; len].into_boxed_slice()
}

#[derive(Debug, Clone)]
pub struct BitmapRangeIter<'a> {
    u64s: &'a [u64],      // slice of u64
    last_word_len: usize, // length of last word
    word: u64,            // current u64 word to scan
    word_bits: usize,     // maximum bits in current word
    prev: bool,           // previous value (true/flase)
    n: usize,             // previous repeat number
}

impl BitmapRangeIter<'_> {
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

impl Iterator for BitmapRangeIter<'_> {
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

pub struct BitmapTrueIndexIter<'a> {
    range_iter: BitmapRangeIter<'a>,
    start: usize,
    end: usize,
}

impl Iterator for BitmapTrueIndexIter<'_> {
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

#[derive(Clone)]
pub struct FreeBitmap {
    free_unit_idx: usize,
    bitmap: Box<[u64]>,
}

/// AllocMap is an allocation controller backed by bitmap.
pub struct AllocMap {
    inner: Mutex<FreeBitmap>,
    len: usize,
    allocated: AtomicUsize,
}

impl AllocMap {
    /// Create a new AllocMap.
    #[inline]
    pub fn new(len: usize) -> Self {
        AllocMap {
            inner: Mutex::new(FreeBitmap {
                free_unit_idx: 0,
                bitmap: new_bitmap(len),
            }),
            len,
            allocated: AtomicUsize::new(0),
        }
    }

    /// Returns number of maximum allocations.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns number of allocated objects.
    #[inline]
    pub fn allocated(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }

    /// Try to allocate a new object, returns index of object.
    #[allow(clippy::manual_div_ceil)]
    #[inline]
    pub fn try_allocate(&self) -> Option<usize> {
        let unit_end_idx = (self.len + 63) / 64;
        let mut g = self.inner.lock();
        let unit_start_idx = g.free_unit_idx;
        if let Some(idx) = g.bitmap.bitmap_set_first(unit_start_idx, unit_end_idx) {
            if idx < self.len {
                if idx / 64 != g.free_unit_idx {
                    // free unit exhausted.
                    g.free_unit_idx = idx / 64;
                }

                self.allocated.fetch_add(1, Ordering::Relaxed);
                return Some(idx);
            }
        }
        // Because when deallocating, free unit index is always moved
        // to the smallest free position, it's impossible to have free
        // bit among [0..free_unit_idx]
        None
    }

    /// Deallocate a object with its index.
    #[inline]
    pub fn deallocate(&self, idx: usize) -> bool {
        debug_assert!(idx < self.len);
        let unit_idx = idx / 64;

        let mut g = self.inner.lock();
        if g.bitmap.bitmap_unset(idx) {
            if g.free_unit_idx > unit_idx {
                g.free_unit_idx = unit_idx;
            }
            self.allocated.fetch_sub(1, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Allocate a new object at given index.
    #[inline]
    pub fn allocate_at(&self, idx: usize) -> bool {
        if idx >= self.len {
            return false;
        }
        let mut g = self.inner.lock();
        if g.bitmap.bitmap_set(idx) {
            // Do not update free_unit_idx.
            self.allocated.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Returns whether the object at given position is allocated.
    #[inline]
    pub fn is_allocated(&self, idx: usize) -> bool {
        let g = self.inner.lock();
        g.bitmap.bitmap_get(idx)
    }

    /// Returns allocated ranges.
    #[inline]
    pub fn allocated_ranges(&self) -> Vec<Range<usize>> {
        let mut res = vec![];
        let g = self.inner.lock();
        let mut idx = 0usize;
        for (flag, count) in g.bitmap.bitmap_range_iter(self.len) {
            if flag {
                res.push(idx..idx + count);
            }
            idx += count;
        }
        res
    }
}

impl Clone for AllocMap {
    #[inline]
    fn clone(&self) -> Self {
        let g = self.inner.lock();
        let inner = g.clone();
        let len = self.len;
        let allocated = self.allocated.load(Ordering::Relaxed);
        AllocMap {
            inner: Mutex::new(inner),
            len,
            allocated: AtomicUsize::new(allocated),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_bitmap_new() {
        let bm = new_bitmap(64);
        assert_eq!(bm.len(), 1);
        assert_eq!(bm[0], 0);

        let bm = new_bitmap(128);
        assert_eq!(bm.len(), 2);
        assert_eq!(bm[0], 0);
        assert_eq!(bm[1], 0);
    }

    #[test]
    fn test_bitmap_get_bit() {
        let mut bm = new_bitmap(128);
        assert!(!bm.bitmap_get(0));
        assert!(!bm.bitmap_get(63));
        assert!(!bm.bitmap_get(64));
        assert!(!bm.bitmap_get(127));

        bm.bitmap_set(0);
        bm.bitmap_set(63);
        bm.bitmap_set(64);
        bm.bitmap_set(127);

        assert!(bm.bitmap_get(0));
        assert!(bm.bitmap_get(63));
        assert!(bm.bitmap_get(64));
        assert!(bm.bitmap_get(127));
    }

    #[test]
    fn test_bitmap_get_unit() {
        let mut bm = new_bitmap(128);
        assert_eq!(bm.bitmap_unit(0), 0);
        assert_eq!(bm.bitmap_unit(1), 0);

        bm.bitmap_set(0);
        bm.bitmap_set(63);
        bm.bitmap_set(64);
        bm.bitmap_set(127);

        assert_eq!(bm.bitmap_unit(0), 1 | (1 << 63));
        assert_eq!(bm.bitmap_unit(1), 1 | (1 << 63));
    }

    #[test]
    fn test_bitmap_units() {
        let bm = new_bitmap(128);
        let units = bm.bitmap_units();
        assert_eq!(units.len(), 2);
        assert_eq!(units[0], 0);
        assert_eq!(units[1], 0);
    }

    #[test]
    fn test_bitmap_units_mut() {
        let mut bm = new_bitmap(128);
        let units = bm.bitmap_units_mut();
        units[0] = 0xFFFF;
        units[1] = 0xFFFF;

        assert_eq!(bm[0], 0xFFFF);
        assert_eq!(bm[1], 0xFFFF);
    }

    #[test]
    fn test_bitmap_set_bit() {
        let mut bm = new_bitmap(128);
        assert!(bm.bitmap_set(0));
        assert!(bm.bitmap_set(63));
        assert!(bm.bitmap_set(64));
        assert!(bm.bitmap_set(127));

        assert!(!bm.bitmap_set(0)); // Already set
        assert!(!bm.bitmap_set(63)); // Already set

        assert_eq!(bm[0], 1 | (1 << 63));
        assert_eq!(bm[1], 1 | (1 << 63));
    }

    #[test]
    fn test_bitmap_unset_bit() {
        let mut bm = new_bitmap(128);
        bm.bitmap_set(0);
        bm.bitmap_set(63);
        bm.bitmap_set(64);
        bm.bitmap_set(127);

        assert!(bm.bitmap_unset(0));
        assert!(bm.bitmap_unset(63));
        assert!(bm.bitmap_unset(64));
        assert!(bm.bitmap_unset(127));

        assert!(!bm.bitmap_unset(0)); // Already unset
        assert!(!bm.bitmap_unset(63)); // Already unset

        assert_eq!(bm[0], 0);
        assert_eq!(bm[1], 0);
    }

    #[test]
    fn test_bitmap_set_first() {
        let mut bm = new_bitmap(128);
        // Set all bits in first unit
        for i in 0..64 {
            bm.bitmap_set(i);
        }

        // Should find first zero in second unit
        assert_eq!(bm.bitmap_set_first(0, 2), Some(64));
        assert!(bm.bitmap_get(64));

        // Set all bits in second unit
        for i in 64..128 {
            bm.bitmap_set(i);
        }

        // No zero bits left
        assert_eq!(bm.bitmap_set_first(0, 2), None);
    }

    #[test]
    #[should_panic]
    fn test_bitmap_out_of_bounds() {
        let bm = new_bitmap(64);
        bm.bitmap_get(64); // Should panic
    }

    #[test]
    fn test_bitmap_range_iter_empty() {
        let bm = new_bitmap(0);
        let mut iter = bm.bitmap_range_iter(0);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_bitmap_range_iter_all_true() {
        let mut bm = new_bitmap(64);
        for i in 0..64 {
            bm.bitmap_set(i);
        }
        let mut iter = bm.bitmap_range_iter(64);
        assert_eq!(iter.next(), Some((true, 64)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_bitmap_range_iter_all_false() {
        let bm = new_bitmap(64);
        let mut iter = bm.bitmap_range_iter(64);
        assert_eq!(iter.next(), Some((false, 64)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_bitmap_range_iter_alternating() {
        let mut bm = new_bitmap(128);
        // Set every other bit
        for i in (0..128).step_by(2) {
            bm.bitmap_set(i);
        }
        let mut iter = bm.bitmap_range_iter(128);
        assert_eq!(iter.next(), Some((true, 1)));
        assert_eq!(iter.next(), Some((false, 1)));
        // Should repeat this pattern 64 times (128 bits total)
    }

    #[test]
    fn test_bitmap_range_iter_mixed() {
        let mut bm = new_bitmap(192); // 3 words
                                      // First word: all true
        for i in 0..64 {
            bm.bitmap_set(i);
        }
        // Second word: alternating
        for i in 64..128 {
            if i % 2 == 0 {
                bm.bitmap_set(i);
            }
        }
        // Third word: all false
        let mut iter = bm.bitmap_range_iter(192);
        assert_eq!(iter.next(), Some((true, 65)));
        for _ in 0..31 {
            assert_eq!(iter.next(), Some((false, 1)));
            assert_eq!(iter.next(), Some((true, 1)));
        }
        assert_eq!(iter.next(), Some((false, 65))); // last bit of second word and full third word.
    }

    #[test]
    fn test_bitmap_range_iter_partial() {
        let mut bm = new_bitmap(100); // Not multiple of 64
                                      // Set first and last bits
        bm.bitmap_set(0);
        bm.bitmap_set(99);
        let mut iter = bm.bitmap_range_iter(100);
        assert_eq!(iter.next(), Some((true, 1)));
        assert_eq!(iter.next(), Some((false, 98)));
        assert_eq!(iter.next(), Some((true, 1)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_bitmap_true_index_iter() {
        // Test empty bitmap
        let bm = new_bitmap(0);
        let mut iter = bm.bitmap_true_index_iter(0);
        assert_eq!(iter.next(), None);

        // Test all true bits
        let mut bm = new_bitmap(64);
        for i in 0..64 {
            bm.bitmap_set(i);
        }
        let iter = bm.bitmap_true_index_iter(64);
        assert_eq!(iter.collect::<Vec<_>>(), (0..64).collect::<Vec<_>>());

        // Test all false bits
        let bm = new_bitmap(64);
        let mut iter = bm.bitmap_true_index_iter(64);
        assert_eq!(iter.next(), None);

        // Test alternating bits
        let mut bm = new_bitmap(128);
        for i in (0..128).step_by(2) {
            bm.bitmap_set(i);
        }
        let iter = bm.bitmap_true_index_iter(128);
        assert_eq!(
            iter.collect::<Vec<_>>(),
            (0..128).step_by(2).collect::<Vec<_>>()
        );

        // Test mixed pattern
        let mut bm = new_bitmap(192);
        // First word: all true
        for i in 0..64 {
            bm.bitmap_set(i);
        }
        // Second word: alternating
        for i in 64..128 {
            if i % 2 == 0 {
                bm.bitmap_set(i);
            }
        }
        // Third word: all false
        let iter = bm.bitmap_true_index_iter(192);
        let mut expected = (0..64).collect::<Vec<_>>();
        expected.extend((64..128).step_by(2));
        assert_eq!(iter.collect::<Vec<_>>(), expected);

        // Test partial word
        let mut bm = new_bitmap(100);
        bm.bitmap_set(0);
        bm.bitmap_set(99);
        let iter = bm.bitmap_true_index_iter(100);
        assert_eq!(iter.collect::<Vec<_>>(), vec![0, 99]);
    }

    #[test]
    fn test_alloc_map_concurrent() {
        let bitmap = Arc::new(AllocMap::new(128));
        let mut handles = vec![];

        for i in 0..64 {
            let bitmap = Arc::clone(&bitmap);
            handles.push(thread::spawn(move || {
                assert!(bitmap.allocate_at(i * 2));
                assert!(bitmap.is_allocated(i * 2));
                assert!(bitmap.deallocate(i * 2));
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_alloc_map_ops() {
        let alloc_map = AllocMap::new(1024);
        for _ in 0..1000 {
            assert!(alloc_map.try_allocate().is_some());
        }
        assert!(!alloc_map.deallocate(1000));
        assert!(alloc_map.deallocate(500));
        for _ in 0..25 {
            assert!(alloc_map.try_allocate().is_some());
        }
        assert!(alloc_map.deallocate(500));
        assert!(alloc_map.try_allocate().is_some());

        assert!(!alloc_map.allocate_at(2000));
        assert!(!alloc_map.allocate_at(100));
    }
}
