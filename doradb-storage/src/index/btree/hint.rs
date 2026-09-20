use crate::index::btree::KeyHeadInt;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Number of persisted search hints stored in each B-tree node header.
pub(crate) const BTREE_HINTS_LEN: usize = 8;

/// BTreeHint is a search hint on each BTreeNode.
///
/// For example,
///
/// data = [1000, 1001, 1002, 1003,
///         1300, 1301, 1304, 1405,
///         1420, 1440, 1450, 1600,
///         1800, 1890, 1900, 1908,
///         1990, 2000, 2010, 2040]
/// Store 2-byte(string head) hints.
/// We pick [13, 14, 18, 19].
/// Given an arbitrary key, first search hints.
/// Find the position i where for all j <= i, hint[j] < head(key).
/// If key is 1410, hint[0] is picked.
/// If key is 1800, hint[1] is picked.
#[derive(Debug, Clone, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, align(32))]
pub(crate) struct BTreeHints([[u8; 4]; BTREE_HINTS_LEN]);

impl BTreeHints {
    /// Search interface of hints.
    /// It contains two implementations: avx2 or scalar.
    /// Use conditional compilation to choose suitable one.
    /// Maybe avx512f is better. Leave for future improvement.
    #[inline]
    pub(crate) fn search(&self, key: KeyHeadInt) -> (usize, usize) {
        let hints = self.heads();
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
        {
            // SAFETY: this cfg enables AVX2 and hints supplies eight readable lanes.
            unsafe { search_hints_avx2(&hints, key) }
        }
        #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
        {
            search_hints_scalar(&hints, key)
        }
    }

    /// Update one persisted hint with a key head.
    #[inline]
    pub(crate) fn update(&mut self, idx: usize, key: KeyHeadInt) {
        self.0[idx] = key.to_le_bytes();
    }

    #[inline]
    fn heads(&self) -> [KeyHeadInt; BTREE_HINTS_LEN] {
        self.0.map(KeyHeadInt::from_le_bytes)
    }
}

/// Search hints with AVX2 instruction.
///
/// Safety:
/// Target cpu must support avx2 instruction.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
unsafe fn search_hints_avx2(hints: &[u32; 8], key_head: u32) -> (usize, usize) {
    // SAFETY: the caller guarantees AVX2 support; the array supplies 32 readable
    // bytes for the unaligned load, and all remaining operations use registers.
    unsafe {
        use std::arch::x86_64::*;
        // 1. load data.
        // xor msb to reserve ordering for u32 to i32 conversion.
        let v_input = _mm256_loadu_si256(hints.as_ptr() as *const __m256i);
        let v_xor_mask = _mm256_set1_epi32(0x80000000u32 as i32);
        let v_hints = _mm256_xor_si256(v_input, v_xor_mask);

        // 2. broadcast search key(after xor) to avx register.
        let v_key = _mm256_set1_epi32((key_head ^ 0x80000000) as i32);

        // 3. find lower bound i, first slot hints[i] >= key_head.
        // a >= b can be calculated as !(b > a).
        // perform b > a.
        let v_lo = _mm256_cmpgt_epi32(v_key, v_hints);
        let mask_lo = _mm256_movemask_epi8(v_lo) as u32;
        // reverse mask.
        let i = (!mask_lo).trailing_zeros() as usize / 4;

        // 4. find upper bound j, first slot arr[j] > k.
        let v_up = _mm256_cmpgt_epi32(v_hints, v_key);
        let mask_up = _mm256_movemask_epi8(v_up) as u32;
        let j = mask_up.trailing_zeros() as usize / 4;
        (i, j)
    }
}

/// Search hints fallback method.
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline]
fn search_hints_scalar(hints: &[u32; 8], key_head: u32) -> (usize, usize) {
    let i = hints.partition_point(|&h| h < key_head);
    let j = hints.partition_point(|&h| h <= key_head);
    (i, j)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zerocopy::FromZeros as _;

    type HintProbe = (u32, (usize, usize));

    fn assert_hint_bounds() {
        let cases: [([u32; 8], &[HintProbe]); 6] = [
            (
                [100, 200, 300, 400, 500, 600, 700, 800],
                &[
                    (0, (0, 0)),
                    (50, (0, 0)),
                    (100, (0, 1)),
                    (150, (1, 1)),
                    (200, (1, 2)),
                    (550, (5, 5)),
                    (800, (7, 8)),
                    (900, (8, 8)),
                    (u32::MAX, (8, 8)),
                ],
            ),
            (
                [0, 0, 1, 1, 1, 10, u32::MAX, u32::MAX],
                &[
                    (0, (0, 2)),
                    (1, (2, 5)),
                    (2, (5, 5)),
                    (10, (5, 6)),
                    (u32::MAX - 1, (6, 6)),
                    (u32::MAX, (6, 8)),
                ],
            ),
            (
                [42; 8],
                &[(0, (0, 0)), (41, (0, 0)), (42, (0, 8)), (43, (8, 8))],
            ),
            ([0; 8], &[(0, (0, 8)), (1, (8, 8))]),
            ([u32::MAX; 8], &[(u32::MAX - 1, (0, 0)), (u32::MAX, (0, 8))]),
            (
                [
                    0,
                    0x7fff_fffe,
                    0x7fff_ffff,
                    0x8000_0000,
                    0x8000_0000,
                    0x8000_0001,
                    u32::MAX - 1,
                    u32::MAX,
                ],
                &[
                    (0, (0, 1)),
                    (0x7fff_fffd, (1, 1)),
                    (0x7fff_fffe, (1, 2)),
                    (0x7fff_ffff, (2, 3)),
                    (0x8000_0000, (3, 5)),
                    (0x8000_0001, (5, 6)),
                    (0x8000_0002, (6, 6)),
                    (u32::MAX - 1, (6, 7)),
                    (u32::MAX, (7, 8)),
                ],
            ),
        ];
        for (heads, probes) in cases {
            let mut hints = BTreeHints::new_zeroed();
            for (i, head) in heads.into_iter().enumerate() {
                hints.update(i, head);
            }
            for &(key, expected) in probes {
                #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
                // SAFETY: the cfg requires AVX2; heads contains all eight readable lanes.
                let actual = unsafe { search_hints_avx2(&heads, key) };
                #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
                let actual = search_hints_scalar(&heads, key);
                assert_eq!(actual, expected, "heads={heads:?}, key={key}");
                assert_eq!(
                    hints.search(key),
                    expected,
                    "persisted heads={heads:?}, key={key}"
                );
            }
        }
    }

    #[test]
    fn test_btree_hints_store_little_endian_heads() {
        let mut hints = BTreeHints::new_zeroed();
        hints.update(3, 0x0102_0304);

        assert_eq!(hints.0[3], 0x0102_0304u32.to_le_bytes());
        assert_eq!(hints.heads()[3], 0x0102_0304);
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    #[test]
    fn test_btree_search_hints_avx2() {
        assert_hint_bounds();
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    #[test]
    fn test_btree_search_hints_scalar() {
        assert_hint_bounds();
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    #[test]
    fn test_btree_search_hints_consistency() {
        use rand::SeedableRng;
        use rand_chacha::ChaCha8Rng;
        use rand_distr::{Distribution, Uniform};

        let heads = [
            1957388544, 1995132160, 2027565824, 2065447168, 2096984576, 2134202112, 2167514368,
            2202962944,
        ];
        // SAFETY: the cfg requires AVX2; heads contains all eight readable lanes.
        assert_eq!(unsafe { search_hints_avx2(&heads, 2202603776) }, (7, 7));

        const SEED: u64 = 312;
        let mut rng = ChaCha8Rng::seed_from_u64(SEED);
        let between = Uniform::new_inclusive(0u32, u32::MAX).unwrap();
        for case in 0..1000 {
            let mut heads = [0; BTREE_HINTS_LEN];
            for head in &mut heads {
                *head = between.sample(&mut rng);
            }
            heads.sort();
            for key in heads
                .into_iter()
                .chain((0..BTREE_HINTS_LEN * 4).map(|_| between.sample(&mut rng)))
            {
                let expected = (
                    heads.iter().filter(|&&h| h < key).count(),
                    heads.iter().filter(|&&h| h <= key).count(),
                );
                // SAFETY: the cfg requires AVX2; heads contains all eight readable lanes.
                let actual = unsafe { search_hints_avx2(&heads, key) };
                assert_eq!(
                    actual, expected,
                    "seed={SEED}, case={case}, heads={heads:?}, key={key}"
                );
            }
        }
    }
}
