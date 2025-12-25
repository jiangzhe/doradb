use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

pub const F64_ZERO: ValidF64 = ValidF64(0.0);
pub const F64_ONE: ValidF64 = ValidF64(1.0);

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct ValidF64(f64);

impl ValidF64 {
    #[inline]
    pub fn new(value: f64) -> Option<Self> {
        if value.is_infinite() || value.is_nan() {
            None
        } else {
            Some(ValidF64(value))
        }
    }

    #[inline]
    pub const fn value(&self) -> f64 {
        self.0
    }
}

impl PartialEq for ValidF64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

// we must ensure f64 is valid for equality check
impl Eq for ValidF64 {}

impl PartialOrd for ValidF64 {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ValidF64 {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap()
    }
}

impl Hash for ValidF64 {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0.to_bits())
    }
}

impl Deref for ValidF64 {
    type Target = f64;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub const F32_ZERO: ValidF32 = ValidF32(0.0);
pub const F32_ONE: ValidF32 = ValidF32(1.0);

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct ValidF32(f32);

impl ValidF32 {
    #[inline]
    pub fn new(value: f32) -> Option<Self> {
        if value.is_infinite() || value.is_nan() {
            None
        } else {
            Some(ValidF32(value))
        }
    }

    #[inline]
    pub const fn value(&self) -> f32 {
        self.0
    }
}

impl PartialEq for ValidF32 {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for ValidF32 {}

impl PartialOrd for ValidF32 {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ValidF32 {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap()
    }
}

impl Hash for ValidF32 {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(self.0.to_bits())
    }
}

impl Deref for ValidF32 {
    type Target = f32;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::f32;
    use std::f64;
    use std::hash::Hash;

    #[test]
    fn test_valid_f64_new() {
        // Valid finite values
        assert!(ValidF64::new(0.0).is_some());
        assert!(ValidF64::new(1.0).is_some());
        assert!(ValidF64::new(-1.0).is_some());
        assert!(ValidF64::new(3.14159).is_some());
        assert!(ValidF64::new(f64::MAX).is_some());
        assert!(ValidF64::new(f64::MIN_POSITIVE).is_some());
        assert!(ValidF64::new(f64::EPSILON).is_some());

        // Invalid values
        assert!(ValidF64::new(f64::NAN).is_none());
        assert!(ValidF64::new(f64::INFINITY).is_none());
        assert!(ValidF64::new(f64::NEG_INFINITY).is_none());
    }

    #[test]
    fn test_valid_f32_new() {
        // Valid finite values
        assert!(ValidF32::new(0.0f32).is_some());
        assert!(ValidF32::new(1.0f32).is_some());
        assert!(ValidF32::new(-1.0f32).is_some());
        assert!(ValidF32::new(3.14159f32).is_some());
        assert!(ValidF32::new(f32::MAX).is_some());
        assert!(ValidF32::new(f32::MIN_POSITIVE).is_some());
        assert!(ValidF32::new(f32::EPSILON).is_some());

        // Invalid values
        assert!(ValidF32::new(f32::NAN).is_none());
        assert!(ValidF32::new(f32::INFINITY).is_none());
        assert!(ValidF32::new(f32::NEG_INFINITY).is_none());
    }

    #[test]
    fn test_valid_f64_value() {
        let v = ValidF64::new(42.0).unwrap();
        assert_eq!(v.value(), 42.0);
        assert_eq!(*v, 42.0);

        let v = ValidF64::new(-3.14).unwrap();
        assert_eq!(v.value(), -3.14);
        assert_eq!(*v, -3.14);
    }

    #[test]
    fn test_valid_f32_value() {
        let v = ValidF32::new(42.0f32).unwrap();
        assert_eq!(v.value(), 42.0f32);
        assert_eq!(*v, 42.0f32);

        let v = ValidF32::new(-3.14f32).unwrap();
        assert_eq!(v.value(), -3.14f32);
        assert_eq!(*v, -3.14f32);
    }

    #[test]
    fn test_float_constants() {
        assert_eq!(F64_ZERO.value(), 0.0);
        assert_eq!(F64_ONE.value(), 1.0);
        assert_eq!(F32_ZERO.value(), 0.0f32);
        assert_eq!(F32_ONE.value(), 1.0f32);
    }

    #[test]
    fn test_float_eq() {
        let v1 = ValidF64::new(1.0).unwrap();
        let v2 = ValidF64::new(1.0).unwrap();
        let v3 = ValidF64::new(2.0).unwrap();
        assert_eq!(v1, v2);
        assert_ne!(v1, v3);

        let v1 = ValidF32::new(1.0f32).unwrap();
        let v2 = ValidF32::new(1.0f32).unwrap();
        let v3 = ValidF32::new(2.0f32).unwrap();
        assert_eq!(v1, v2);
        assert_ne!(v1, v3);
    }

    #[test]
    fn test_float_ord() {
        let v1 = ValidF64::new(1.0).unwrap();
        let v2 = ValidF64::new(2.0).unwrap();
        let v3 = ValidF64::new(3.0).unwrap();
        assert!(v1 < v2);
        assert!(v2 < v3);
        assert!(v3 > v1);
        assert_eq!(v1.cmp(&v1), Ordering::Equal);

        let v1 = ValidF32::new(1.0f32).unwrap();
        let v2 = ValidF32::new(2.0f32).unwrap();
        let v3 = ValidF32::new(3.0f32).unwrap();
        assert!(v1 < v2);
        assert!(v2 < v3);
        assert!(v3 > v1);
        assert_eq!(v1.cmp(&v1), Ordering::Equal);
    }

    #[test]
    fn test_float_hash() {
        let v1 = ValidF64::new(1.0).unwrap();
        let v2 = ValidF64::new(1.0).unwrap();
        let v3 = ValidF64::new(2.0).unwrap();

        let mut hasher1 = DefaultHasher::new();
        v1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        v2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        let mut hasher3 = DefaultHasher::new();
        v3.hash(&mut hasher3);
        let hash3 = hasher3.finish();

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);

        let v1 = ValidF32::new(1.0f32).unwrap();
        let v2 = ValidF32::new(1.0f32).unwrap();
        let v3 = ValidF32::new(2.0f32).unwrap();

        let mut hasher1 = DefaultHasher::new();
        v1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        v2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        let mut hasher3 = DefaultHasher::new();
        v3.hash(&mut hasher3);
        let hash3 = hasher3.finish();

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_float_deref() {
        let v = ValidF64::new(3.14).unwrap();
        // Test deref to f64 methods
        assert!(!v.is_nan());
        assert!(!v.is_infinite());
        assert!(v.is_finite());
        assert_eq!(v.abs(), 3.14);
        assert_eq!(v.floor(), 3.0);

        let v = ValidF32::new(3.14f32).unwrap();
        assert!(!v.is_nan());
        assert!(!v.is_infinite());
        assert!(v.is_finite());
        assert_eq!(v.abs(), 3.14f32);
        assert_eq!(v.floor(), 3.0f32);
    }

    #[test]
    fn test_float_edge_cases() {
        // Positive and negative zero
        let pos_zero = ValidF64::new(0.0).unwrap();
        let neg_zero = ValidF64::new(-0.0).unwrap();
        // In Rust, 0.0 == -0.0 is true
        assert_eq!(pos_zero, neg_zero);
        assert_eq!(pos_zero.value().to_bits(), 0u64);
        assert_eq!(neg_zero.value().to_bits(), 0x8000000000000000u64);

        let pos_zero = ValidF32::new(0.0f32).unwrap();
        let neg_zero = ValidF32::new(-0.0f32).unwrap();
        assert_eq!(pos_zero, neg_zero);
        assert_eq!(pos_zero.value().to_bits(), 0u32);
        assert_eq!(neg_zero.value().to_bits(), 0x80000000u32);

        // Maximum and minimum finite values
        assert!(ValidF64::new(f64::MAX).is_some());
        assert!(ValidF64::new(f64::MIN).is_some());
        assert!(ValidF32::new(f32::MAX).is_some());
        assert!(ValidF32::new(f32::MIN).is_some());

        // Subnormal numbers
        assert!(ValidF64::new(f64::MIN_POSITIVE / 2.0).is_some());
        assert!(ValidF32::new(f32::MIN_POSITIVE / 2.0f32).is_some());
    }

    #[test]
    fn test_float_memcmp_format() {
        use crate::memcmp::{MemCmpFormat, NullableMemCmpFormat};

        // Test ValidF64 MemCmpFormat
        let v = ValidF64::new(1.5).unwrap();
        assert_eq!(ValidF64::est_mcf_len(), Some(8));
        assert_eq!(v.enc_mcf_len(), 8);

        let mut buf = Vec::new();
        v.extend_mcf_to(&mut buf);
        assert_eq!(buf.len(), 8);

        let mut buf2 = vec![0u8; 8];
        v.copy_mcf_to(&mut buf2, 0);
        assert_eq!(buf, buf2);

        // Test ValidF64 NullableMemCmpFormat
        assert_eq!(ValidF64::est_nmcf_len(), Some(9));
        assert_eq!(v.enc_nmcf_len(), 9);

        let mut buf = Vec::new();
        v.extend_nmcf_to(&mut buf);
        assert_eq!(buf.len(), 9);
        assert_eq!(buf[0], crate::memcmp::NON_NULL_FLAG);

        let mut buf2 = vec![0u8; 9];
        v.copy_nmcf_to(&mut buf2, 0);
        assert_eq!(buf, buf2);

        // Test ValidF32 MemCmpFormat
        let v = ValidF32::new(1.5f32).unwrap();
        assert_eq!(ValidF32::est_mcf_len(), Some(4));
        assert_eq!(v.enc_mcf_len(), 4);

        let mut buf = Vec::new();
        v.extend_mcf_to(&mut buf);
        assert_eq!(buf.len(), 4);

        let mut buf2 = vec![0u8; 4];
        v.copy_mcf_to(&mut buf2, 0);
        assert_eq!(buf, buf2);

        // Test ValidF32 NullableMemCmpFormat
        assert_eq!(ValidF32::est_nmcf_len(), Some(5));
        assert_eq!(v.enc_nmcf_len(), 5);

        let mut buf = Vec::new();
        v.extend_nmcf_to(&mut buf);
        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], crate::memcmp::NON_NULL_FLAG);

        let mut buf2 = vec![0u8; 5];
        v.copy_nmcf_to(&mut buf2, 0);
        assert_eq!(buf, buf2);
    }

    #[test]
    fn test_float_ordering_consistency() {
        // Verify that ordering of ValidF64 matches ordering of f64
        let test_values = vec![
            -100.0,
            -10.0,
            -1.0,
            -0.5,
            0.0,
            0.5,
            1.0,
            10.0,
            100.0,
            f64::MIN_POSITIVE,
            f64::EPSILON,
        ];

        for i in 0..test_values.len() {
            for j in 0..test_values.len() {
                let a = test_values[i];
                let b = test_values[j];
                let va = ValidF64::new(a).unwrap();
                let vb = ValidF64::new(b).unwrap();

                assert_eq!(va.cmp(&vb), a.partial_cmp(&b).unwrap());
                assert_eq!(va < vb, a < b);
                assert_eq!(va > vb, a > b);
                assert_eq!(va == vb, a == b);
            }
        }

        // Same for ValidF32
        let test_values = vec![
            -100.0f32,
            -10.0f32,
            -1.0f32,
            -0.5f32,
            0.0f32,
            0.5f32,
            1.0f32,
            10.0f32,
            100.0f32,
            f32::MIN_POSITIVE,
            f32::EPSILON,
        ];

        for i in 0..test_values.len() {
            for j in 0..test_values.len() {
                let a = test_values[i];
                let b = test_values[j];
                let va = ValidF32::new(a).unwrap();
                let vb = ValidF32::new(b).unwrap();

                assert_eq!(va.cmp(&vb), a.partial_cmp(&b).unwrap());
                assert_eq!(va < vb, a < b);
                assert_eq!(va > vb, a > b);
                assert_eq!(va == vb, a == b);
            }
        }
    }

    #[test]
    fn test_float_random_values() {
        use rand::Rng;
        use rand::rngs::ThreadRng;
        use rand_distr::StandardUniform;

        // Helper to generate random valid f64
        fn gen_valid_f64(rng: &mut ThreadRng) -> ValidF64 {
            loop {
                let val: f64 = rng.sample(StandardUniform);
                if let Some(v) = ValidF64::new(val) {
                    return v;
                }
            }
        }

        // Helper to generate random valid f32
        fn gen_valid_f32(rng: &mut ThreadRng) -> ValidF32 {
            loop {
                let val: f32 = rng.sample(StandardUniform);
                if let Some(v) = ValidF32::new(val) {
                    return v;
                }
            }
        }

        let mut rng = rand::rng();

        // Test multiple random values for consistency
        for _ in 0..100 {
            let v1 = gen_valid_f64(&mut rng);
            let v2 = gen_valid_f64(&mut rng);
            let v3 = gen_valid_f64(&mut rng);

            // Test transitivity of ordering
            if v1 <= v2 && v2 <= v3 {
                assert!(v1 <= v3);
            }

            // Test hash consistency
            let mut hasher1 = DefaultHasher::new();
            v1.hash(&mut hasher1);
            let hash1 = hasher1.finish();

            let v1_clone = ValidF64::new(v1.value()).unwrap();
            let mut hasher2 = DefaultHasher::new();
            v1_clone.hash(&mut hasher2);
            let hash2 = hasher2.finish();

            assert_eq!(hash1, hash2);

            // Same for f32
            let v1 = gen_valid_f32(&mut rng);
            let v2 = gen_valid_f32(&mut rng);
            let v3 = gen_valid_f32(&mut rng);

            if v1 <= v2 && v2 <= v3 {
                assert!(v1 <= v3);
            }

            let mut hasher1 = DefaultHasher::new();
            v1.hash(&mut hasher1);
            let hash1 = hasher1.finish();

            let v1_clone = ValidF32::new(v1.value()).unwrap();
            let mut hasher2 = DefaultHasher::new();
            v1_clone.hash(&mut hasher2);
            let hash2 = hasher2.finish();

            assert_eq!(hash1, hash2);
        }
    }
}
