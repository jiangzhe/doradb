//! Default, unseeded XXH3 integrity digests and bindings, persisted as little-endian bytes.

use xxhash_rust::xxh3::{Xxh3Default, xxh3_64, xxh3_128};

/// Serialized width of physical checksums and 128-bit fingerprints.
pub(crate) const CHECKSUM_SIZE: usize = 16;

/// Incremental equivalent of [`checksum128`] for canonical fields.
pub(crate) struct ChecksumHasher(Xxh3Default);

impl ChecksumHasher {
    /// Start an unseeded digest.
    #[inline]
    pub(crate) fn new() -> Self {
        Self(Xxh3Default::new())
    }

    /// Append canonical bytes without adding framing.
    #[inline]
    pub(crate) fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    /// Finish with all 128 bits of the digest.
    #[inline]
    pub(crate) fn finalize(self) -> u128 {
        self.0.digest128()
    }
}

/// Hash a complete canonical byte stream with the default seed and secret.
#[inline]
pub(crate) fn checksum128(bytes: &[u8]) -> u128 {
    xxh3_128(bytes)
}

/// Hash canonical block-binding bytes with native XXH3-64 and the default seed and secret.
#[inline]
pub(crate) fn checksum64(bytes: &[u8]) -> u64 {
    xxh3_64(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_input(len: usize, offset: usize) -> Vec<u8> {
        let mut input = vec![0; len + offset];
        for (i, byte) in input[offset..].iter_mut().enumerate() {
            *byte = (i % 251) as u8;
        }
        input
    }

    /// Purpose: Fix the unseeded XXH3-128 contract across algorithm and physical-block boundaries.
    /// Expected: One-shot and partitioned, unaligned streaming match independent reference vectors.
    #[test]
    fn reference_vectors_and_streaming_partitions() {
        // Generated independently with upstream C libxxhash XXH3_128bits; input[i] = i % 251.
        let cases = [
            (0, 0x99aa06d3014798d8_6001c324468d497f),
            (1, 0xa6cd5e9392000f6a_c44bdff4074eecdb),
            (3, 0xe3b55f57945a17cf_5f4299fc161c9cbb),
            (4, 0xeb70bf5fc779e9e6_a6111d53e80a3db5),
            (8, 0xe1e4432a62217fe4_cfd50c61c8bb98c1),
            (9, 0x16c769d83e4aebce_907931979dca3746),
            (16, 0x72950631827607e2_842812cc870dcae2),
            (17, 0x685bc458b37d057f_c06e233df7729217),
            (128, 0x14792fc3af88dc6c_05321a0b64d67b41),
            (129, 0xdd5e74ac6b45f54e_bc30b63382b09a3b),
            (240, 0x65b5be86da5540e7_c92b68e16f83bbb6),
            (241, 0x1da1cb61bcb8a2a1_02e8cd95421c6d02),
            (1024, 0xd0ac1f7b93bf57b9_e5d78bafa45b2aa5),
            (65536, 0xf5e7bc5d3d8675bf_aaae63800707a868),
        ];
        for (len, expected) in cases {
            for offset in [0, 1, 7] {
                let input = reference_input(len, offset);
                let input = &input[offset..];
                assert_eq!(checksum128(input), expected, "len={len}, offset={offset}");
                for chunk_size in [1, 7, 64, 241, 1024] {
                    let mut hasher = ChecksumHasher::new();
                    hasher.update(&[]);
                    for chunk in input.chunks(chunk_size) {
                        hasher.update(chunk);
                    }
                    assert_eq!(hasher.finalize(), expected, "len={len}, chunk={chunk_size}");
                }
            }
        }
        assert_eq!(
            checksum128(&[]).to_le_bytes(),
            [
                0x7f, 0x49, 0x8d, 0x46, 0x24, 0xc3, 0x01, 0x60, 0xd8, 0x98, 0x47, 0x01, 0xd3, 0x06,
                0xaa, 0x99
            ]
        );
    }

    /// Purpose: Fix native XXH3-64 bindings across algorithm boundaries and unaligned inputs.
    /// Expected: Digests match independent reference vectors with explicit little-endian encoding.
    #[test]
    fn native_64_reference_vectors() {
        // Generated independently with C libxxhash 0.8.2 XXH3_64bits; input[i] = i % 251.
        let cases = [
            (0, 0x2d06800538d394c2),
            (1, 0xc44bdff4074eecdb),
            (3, 0x5f4299fc161c9cbb),
            (4, 0x60dab036a58211f2),
            (8, 0x3a1c2d7c85af88f8),
            (9, 0xe9612598145bb9dc),
            (16, 0x8355e3a6f61770db),
            (17, 0x9ef341a99de37328),
            (128, 0x85c6174c7ff4c46b),
            (129, 0xec7642b431ba3e5a),
            (240, 0x375a384d957fe865),
            (241, 0x02e8cd95421c6d02),
            (65536, 0xaaae63800707a868),
        ];
        for (len, expected) in cases {
            for offset in [0, 1, 7] {
                let input = reference_input(len, offset);
                assert_eq!(
                    checksum64(&input[offset..]),
                    expected,
                    "len={len}, offset={offset}"
                );
            }
        }
        assert_eq!(
            checksum64(&[]).to_le_bytes(),
            [0xc2, 0x94, 0xd3, 0x38, 0x05, 0x80, 0x06, 0x2d]
        );
    }
}
