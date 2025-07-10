use crate::index::btree_node::KeyHeadInt;

pub const BTREE_HINTS_LEN: usize = 8;

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
#[derive(Debug, Clone, Copy)]
#[repr(align(32))]
pub struct BTreeHints([KeyHeadInt; BTREE_HINTS_LEN]);

impl BTreeHints {
    /// Search interface of hints.
    /// It contains two implementations: avx2 or scalar.
    /// Use conditional compilation to choose suitable one.
    /// Maybe avx512f is better. Leave for future improvement.
    #[inline]
    pub fn search(&self, key: KeyHeadInt) -> (usize, usize) {
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
        {
            unsafe { search_hints_avx2(&self.0, key) }
        }
        #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
        {
            search_hints_scalar(&self.0, key)
        }
    }

    #[inline]
    pub fn update(&mut self, idx: usize, key: KeyHeadInt) {
        self.0[idx] = key;
    }
}

/// Search hints with AVX2 instruction.
///
/// Safety:
/// Target cpu must support avx2 instruction.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
unsafe fn search_hints_avx2(hints: &[u32; 8], key_head: u32) -> (usize, usize) {
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

/// Search hints fallback method.
#[inline]
fn search_hints_scalar(hints: &[u32; 8], key_head: u32) -> (usize, usize) {
    let i = hints.partition_point(|&h| h < key_head);
    let j = hints.partition_point(|&h| h <= key_head);
    (i, j)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    #[test]
    fn test_btree_search_hints_avx2() {
        let hints = [100, 200, 300, 400, 500, 600, 700, 800];
        for key in [50, 150, 200, 550, 800, 900] {
            let res = unsafe { search_hints_avx2(&hints, key) };
            println!("hints={:?}, key={}, res={:?}", hints, key, res);
        }
    }

    #[test]
    fn test_btree_search_hints_scalar() {
        let hints = [100, 200, 300, 400, 500, 600, 700, 800];
        for key in [50, 150, 200, 550, 800, 900] {
            let res = search_hints_scalar(&hints, key);
            println!("hints={:?}, key={}, res={:?}", hints, key, res);
        }
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    #[test]
    fn test_btree_search_hints_consistency() {
        use rand_distr::{Distribution, Uniform};
        // this particular test case checks the bounds of signed
        // and unsigned integers.
        let hints = [
            1957388544u32,
            1995132160,
            2027565824,
            2065447168,
            2096984576,
            2134202112,
            2167514368,
            2202962944,
        ];
        let head = 2202603776u32;
        let res1 = search_hints_scalar(&hints, head);
        println!("scalar res={:?}", res1);
        let res2 = unsafe { search_hints_avx2(&hints, head) };
        println!("avx2 res={:?}", res2);
        assert_eq!(res1, res2);

        // Generate random test cases.
        const COUNT: usize = 1000;
        for _ in 0..COUNT {
            let mut rng = rand::rng();
            let between = Uniform::new(0u32, u32::MAX).unwrap();
            let mut hints: Vec<u32> = (0..BTREE_HINTS_LEN)
                .map(|_| between.sample(&mut rng))
                .collect();
            hints.sort();
            let hints: [u32; BTREE_HINTS_LEN] = hints.try_into().unwrap();
            for _ in 0..BTREE_HINTS_LEN * 4 {
                let head = between.sample(&mut rng);
                let res1 = search_hints_scalar(&hints, head);
                // println!("scalar res={:?}", res1);
                let res2 = unsafe { search_hints_avx2(&hints, head) };
                // println!("avx2 res={:?}", res2);
                assert_eq!(res1, res2);
            }
        }
    }
}
