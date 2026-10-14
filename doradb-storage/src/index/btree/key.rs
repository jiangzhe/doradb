use crate::error::{ResourceError, ResourceResult};
use crate::id::RowID;
use crate::index::util::Maskable;
use crate::memcmp::{
    MemCmpFormat, MemCmpKey, NormalBytes, Null, Nullable, NullableMemCmpFormat, SegmentedBytes,
};
use crate::value::{Val, ValKind, ValType};
use error_stack::Report;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::ops::{Bound, RangeBounds};

/// Memory-comparable encoded key used by B-tree nodes.
pub(crate) type BTreeKey = MemCmpKey;

#[derive(Clone, Copy, Debug)]
enum KeySuffix<'a> {
    Inline { bytes: [u8; 4], len: u8 },
    Borrowed(&'a [u8]),
}

/// Encoded key borrowing its common prefix and any outlined suffix.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BTreeKeyView<'a> {
    prefix: &'a [u8],
    suffix: KeySuffix<'a>,
}

impl<'a> BTreeKeyView<'a> {
    /// Borrow a prefix and an outlined suffix in encoded byte order.
    #[inline]
    pub(super) fn borrowed(prefix: &'a [u8], suffix: &'a [u8]) -> Self {
        Self {
            prefix,
            suffix: KeySuffix::Borrowed(suffix),
        }
    }

    /// Decode a short slot head into an inline suffix without allocating.
    #[inline]
    pub(super) fn inline(prefix: &'a [u8], bytes: [u8; 4], len: u8) -> Self {
        assert!(len <= 4, "B-tree inline key suffix length exceeds head");
        Self {
            prefix,
            suffix: KeySuffix::Inline { bytes, len },
        }
    }

    #[inline]
    fn suffix(&self) -> &[u8] {
        match &self.suffix {
            KeySuffix::Inline { bytes, len } => &bytes[..usize::from(*len)],
            KeySuffix::Borrowed(bytes) => bytes,
        }
    }

    /// Full encoded length, including the common prefix.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.prefix.len() + self.suffix().len()
    }

    /// Compare the complete encoded key with a contiguous probe.
    #[inline]
    pub(crate) fn cmp_bytes(&self, other: &[u8]) -> Ordering {
        let common = self.prefix.len().min(other.len());
        let order = self.prefix[..common].cmp(&other[..common]);
        if !order.is_eq() {
            return order;
        }
        if common < self.prefix.len() {
            return Ordering::Greater;
        }
        self.suffix().cmp(&other[common..])
    }

    /// Materialize a key only when it must outlive its page guard.
    pub(crate) fn to_vec(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.len());
        bytes.extend_from_slice(self.prefix);
        bytes.extend_from_slice(self.suffix());
        bytes
    }

    /// Materialize the owned key used by foreground candidate batches.
    pub(crate) fn to_btree_key(self) -> BTreeKey {
        let mut key = BTreeKey::arbitrary(self.len());
        let mut bytes = key.modify_inplace();
        bytes[..self.prefix.len()].copy_from_slice(self.prefix);
        bytes[self.prefix.len()..].copy_from_slice(self.suffix());
        drop(bytes);
        key
    }

    /// Copy a fixed-width trailing scalar, including across the prefix boundary.
    pub(crate) fn copy_tail(&self, dst: &mut [u8]) -> Option<()> {
        if dst.len() > self.len() {
            return None;
        }
        let suffix = self.suffix();
        let suffix_len = dst.len().min(suffix.len());
        let prefix_len = dst.len() - suffix_len;
        dst[..prefix_len].copy_from_slice(&self.prefix[self.prefix.len() - prefix_len..]);
        dst[prefix_len..].copy_from_slice(&suffix[suffix.len() - suffix_len..]);
        Some(())
    }
}

/// Owned encoded key range used by B-tree scan streams.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KeyRange {
    lower: Bound<BTreeKey>,
    upper: Bound<BTreeKey>,
}

impl KeyRange {
    /// Test a borrowed entry against the inclusive or exclusive lower bound.
    #[inline]
    pub(crate) fn lower_accepts_view(&self, key: BTreeKeyView<'_>) -> bool {
        match &self.lower {
            Bound::Unbounded => true,
            Bound::Included(bound) => !key.cmp_bytes(bound.as_bytes()).is_lt(),
            Bound::Excluded(bound) => key.cmp_bytes(bound.as_bytes()).is_gt(),
        }
    }

    /// Test a borrowed entry against the inclusive or exclusive upper bound.
    #[inline]
    pub(crate) fn upper_accepts_view(&self, key: BTreeKeyView<'_>) -> bool {
        match &self.upper {
            Bound::Unbounded => true,
            Bound::Included(bound) => !key.cmp_bytes(bound.as_bytes()).is_gt(),
            Bound::Excluded(bound) => key.cmp_bytes(bound.as_bytes()).is_lt(),
        }
    }

    /// Create an encoded range from owned lower and upper bounds.
    #[inline]
    pub(crate) fn new(lower: Bound<BTreeKey>, upper: Bound<BTreeKey>) -> Self {
        Self { lower, upper }
    }

    /// Key bytes to seek before testing entries against the full range.
    #[inline]
    pub(crate) fn lower_seek_key(&self) -> &[u8] {
        match &self.lower {
            Bound::Unbounded => &[],
            Bound::Included(key) | Bound::Excluded(key) => key.as_bytes(),
        }
    }

    /// Return whether an encoded entry key satisfies the lower bound.
    #[inline]
    pub(crate) fn lower_accepts(&self, key: &[u8]) -> bool {
        match &self.lower {
            Bound::Unbounded => true,
            Bound::Included(bound) => key >= bound.as_bytes(),
            Bound::Excluded(bound) => key > bound.as_bytes(),
        }
    }

    /// Return whether an encoded entry key satisfies the upper bound.
    #[inline]
    pub(crate) fn upper_accepts(&self, key: &[u8]) -> bool {
        match &self.upper {
            Bound::Unbounded => true,
            Bound::Included(bound) => key <= bound.as_bytes(),
            Bound::Excluded(bound) => key < bound.as_bytes(),
        }
    }

    /// Clone this range with an exact consumed key excluded from its lower side.
    #[inline]
    pub(crate) fn resume_after(&self, key: BTreeKey) -> Self {
        debug_assert!(
            self.lower_accepts(key.as_bytes()),
            "index scan resume key must belong to the original lower range"
        );
        Self {
            lower: Bound::Excluded(key),
            upper: self.upper.clone(),
        }
    }
}

trait KeyEncoder {
    /// Returns estimated encoded length.
    /// If all keys are fixed-size, it will be exactly encoded byte number.
    /// Otherwise, returns None.
    fn est_encode_len(&self) -> Option<usize>;

    /// Returns encoded length of a given key.
    fn encode_len(&self, key: &Val) -> usize;

    /// Encodes a key and copy to given buffer at given start position.
    fn encode_copy(&self, key: &Val, buf: &mut [u8], start_idx: usize) -> usize;
}

/// Encoder for a single logical value into a memory-comparable B-tree key.
pub(crate) struct SingleKeyEncoder(ValType);

impl SingleKeyEncoder {
    /// Create a new key from single value.
    #[inline]
    fn encode_single(&self, key: &Val) -> BTreeKey {
        if key.is_null() {
            return BTreeKey::from(Null);
        }
        match (self.0.kind, self.0.nullable) {
            (ValKind::I8, false) => BTreeKey::from(key.as_i8().unwrap()),
            (ValKind::U8, false) => BTreeKey::from(key.as_u8().unwrap()),
            (ValKind::I16, false) => BTreeKey::from(key.as_i16().unwrap()),
            (ValKind::U16, false) => BTreeKey::from(key.as_u16().unwrap()),
            (ValKind::I32, false) => BTreeKey::from(key.as_i32().unwrap()),
            (ValKind::U32, false) => BTreeKey::from(key.as_u32().unwrap()),
            (ValKind::I64, false) => BTreeKey::from(key.as_i64().unwrap()),
            (ValKind::U64, false) => BTreeKey::from(key.as_u64().unwrap()),
            (ValKind::F32, false) => BTreeKey::from(key.as_f32().unwrap()),
            (ValKind::F64, false) => BTreeKey::from(key.as_f64().unwrap()),
            (ValKind::VarByte, false) => BTreeKey::from(key.as_bytes().unwrap()),
            (ValKind::I8, true) => BTreeKey::from(Nullable(key.as_i8().unwrap())),
            (ValKind::U8, true) => BTreeKey::from(Nullable(key.as_u8().unwrap())),
            (ValKind::I16, true) => BTreeKey::from(Nullable(key.as_i16().unwrap())),
            (ValKind::U16, true) => BTreeKey::from(Nullable(key.as_u16().unwrap())),
            (ValKind::I32, true) => BTreeKey::from(Nullable(key.as_i32().unwrap())),
            (ValKind::U32, true) => BTreeKey::from(Nullable(key.as_u32().unwrap())),
            (ValKind::I64, true) => BTreeKey::from(Nullable(key.as_i64().unwrap())),
            (ValKind::U64, true) => BTreeKey::from(Nullable(key.as_u64().unwrap())),
            (ValKind::F32, true) => BTreeKey::from(Nullable(key.as_f32().unwrap())),
            (ValKind::F64, true) => BTreeKey::from(Nullable(key.as_f64().unwrap())),
            (ValKind::VarByte, true) => BTreeKey::from(Nullable(key.as_bytes().unwrap())),
        }
    }
}

impl KeyEncoder for SingleKeyEncoder {
    #[inline]
    fn est_encode_len(&self) -> Option<usize> {
        match (self.0.kind, self.0.nullable) {
            (ValKind::I8, false) => i8::est_mcf_len(),
            (ValKind::U8, false) => u8::est_mcf_len(),
            (ValKind::I16, false) => i16::est_mcf_len(),
            (ValKind::U16, false) => u16::est_mcf_len(),
            (ValKind::I32, false) => i32::est_mcf_len(),
            (ValKind::U32, false) => u32::est_mcf_len(),
            (ValKind::I64, false) => i64::est_mcf_len(),
            (ValKind::U64, false) => u64::est_mcf_len(),
            (ValKind::F32, false) => f32::est_mcf_len(),
            (ValKind::F64, false) => f64::est_mcf_len(),
            (ValKind::VarByte, false) => None,
            // For nullable key, always return None for estimated encoded length.
            // Because Null with be encoded as exactly 1 byte.
            (_, true) => None,
        }
    }

    #[inline]
    fn encode_len(&self, key: &Val) -> usize {
        if key.is_null() {
            return 1;
        }
        match (self.0.kind, self.0.nullable) {
            (ValKind::I8, false) => i8::est_mcf_len().unwrap(),
            (ValKind::U8, false) => u8::est_mcf_len().unwrap(),
            (ValKind::I16, false) => i16::est_mcf_len().unwrap(),
            (ValKind::U16, false) => u16::est_mcf_len().unwrap(),
            (ValKind::I32, false) => i32::est_mcf_len().unwrap(),
            (ValKind::U32, false) => u32::est_mcf_len().unwrap(),
            (ValKind::I64, false) => i64::est_mcf_len().unwrap(),
            (ValKind::U64, false) => u64::est_mcf_len().unwrap(),
            (ValKind::F32, false) => f32::est_mcf_len().unwrap(),
            (ValKind::F64, false) => f64::est_mcf_len().unwrap(),
            (ValKind::VarByte, false) => NormalBytes(key.as_bytes().unwrap()).enc_mcf_len(),
            (ValKind::I8, true) => i8::est_nmcf_len().unwrap(),
            (ValKind::U8, true) => u8::est_nmcf_len().unwrap(),
            (ValKind::I16, true) => i16::est_nmcf_len().unwrap(),
            (ValKind::U16, true) => u16::est_nmcf_len().unwrap(),
            (ValKind::I32, true) => i32::est_nmcf_len().unwrap(),
            (ValKind::U32, true) => u32::est_nmcf_len().unwrap(),
            (ValKind::I64, true) => i64::est_nmcf_len().unwrap(),
            (ValKind::U64, true) => u64::est_nmcf_len().unwrap(),
            (ValKind::F32, true) => f32::est_nmcf_len().unwrap(),
            (ValKind::F64, true) => f64::est_nmcf_len().unwrap(),
            (ValKind::VarByte, true) => NormalBytes(key.as_bytes().unwrap()).enc_nmcf_len(),
        }
    }

    #[inline]
    fn encode_copy(&self, key: &Val, buf: &mut [u8], start_idx: usize) -> usize {
        if key.is_null() {
            return Null.copy_nmcf_to(buf, start_idx);
        }
        match (self.0.kind, self.0.nullable) {
            (ValKind::I8, false) => key.as_i8().unwrap().copy_mcf_to(buf, start_idx),
            (ValKind::U8, false) => key.as_u8().unwrap().copy_mcf_to(buf, start_idx),
            (ValKind::I16, false) => key.as_i16().unwrap().copy_mcf_to(buf, start_idx),
            (ValKind::U16, false) => key.as_u16().unwrap().copy_mcf_to(buf, start_idx),
            (ValKind::I32, false) => key.as_i32().unwrap().copy_mcf_to(buf, start_idx),
            (ValKind::U32, false) => key.as_u32().unwrap().copy_mcf_to(buf, start_idx),
            (ValKind::I64, false) => key.as_i64().unwrap().copy_mcf_to(buf, start_idx),
            (ValKind::U64, false) => key.as_u64().unwrap().copy_mcf_to(buf, start_idx),
            (ValKind::F32, false) => key.as_f32().unwrap().copy_mcf_to(buf, start_idx),
            (ValKind::F64, false) => key.as_f64().unwrap().copy_mcf_to(buf, start_idx),
            (ValKind::VarByte, false) => {
                NormalBytes(key.as_bytes().unwrap()).copy_mcf_to(buf, start_idx)
            }
            (ValKind::I8, true) => key.as_i8().unwrap().copy_nmcf_to(buf, start_idx),
            (ValKind::U8, true) => key.as_u8().unwrap().copy_nmcf_to(buf, start_idx),
            (ValKind::I16, true) => key.as_i16().unwrap().copy_nmcf_to(buf, start_idx),
            (ValKind::U16, true) => key.as_u16().unwrap().copy_nmcf_to(buf, start_idx),
            (ValKind::I32, true) => key.as_i32().unwrap().copy_nmcf_to(buf, start_idx),
            (ValKind::U32, true) => key.as_u32().unwrap().copy_nmcf_to(buf, start_idx),
            (ValKind::I64, true) => key.as_i64().unwrap().copy_nmcf_to(buf, start_idx),
            (ValKind::U64, true) => key.as_u64().unwrap().copy_nmcf_to(buf, start_idx),
            (ValKind::F32, true) => key.as_f32().unwrap().copy_nmcf_to(buf, start_idx),
            (ValKind::F64, true) => key.as_f64().unwrap().copy_nmcf_to(buf, start_idx),
            (ValKind::VarByte, true) => {
                NormalBytes(key.as_bytes().unwrap()).copy_nmcf_to(buf, start_idx)
            }
        }
    }
}

/// A Special encoder for variable-length string/bytes to be memory comparable
/// if it is followed by other keys.
pub(crate) struct SegmentedBytesEncoder {
    nullable: bool,
}

impl KeyEncoder for SegmentedBytesEncoder {
    #[inline]
    fn est_encode_len(&self) -> Option<usize> {
        None
    }

    #[inline]
    fn encode_len(&self, key: &Val) -> usize {
        if key.is_null() {
            return 1;
        }
        let bs = SegmentedBytes(key.as_bytes().unwrap());
        if self.nullable {
            bs.enc_nmcf_len()
        } else {
            bs.enc_mcf_len()
        }
    }

    #[inline]
    fn encode_copy(&self, key: &Val, buf: &mut [u8], start_idx: usize) -> usize {
        if key.is_null() {
            return Null.copy_nmcf_to(buf, start_idx);
        }
        let bs = SegmentedBytes(key.as_bytes().unwrap());
        if self.nullable {
            bs.copy_nmcf_to(buf, start_idx)
        } else {
            bs.copy_mcf_to(buf, start_idx)
        }
    }
}

/// Encoder for one non-final component of a composite B-tree key.
pub(crate) enum PrefixKeyEncoder {
    Single(SingleKeyEncoder),
    Segmented(SegmentedBytesEncoder),
}

impl KeyEncoder for PrefixKeyEncoder {
    #[inline]
    fn est_encode_len(&self) -> Option<usize> {
        match self {
            PrefixKeyEncoder::Single(e) => e.est_encode_len(),
            PrefixKeyEncoder::Segmented(_) => None,
        }
    }

    #[inline]
    fn encode_len(&self, key: &Val) -> usize {
        match self {
            PrefixKeyEncoder::Single(e) => e.encode_len(key),
            PrefixKeyEncoder::Segmented(e) => e.encode_len(key),
        }
    }

    #[inline]
    fn encode_copy(&self, key: &Val, buf: &mut [u8], start_idx: usize) -> usize {
        match self {
            PrefixKeyEncoder::Single(e) => e.encode_copy(key, buf, start_idx),
            PrefixKeyEncoder::Segmented(e) => e.encode_copy(key, buf, start_idx),
        }
    }
}

/// Encoder for composite memory-comparable keys.
pub(crate) struct MultiKeyEncoder {
    prefix: Box<[PrefixKeyEncoder]>,
    suffix: SingleKeyEncoder,
    encode_len: Option<usize>,
}

impl MultiKeyEncoder {
    /// Create a composite key encoder from ordered key value types.
    #[inline]
    fn new<T>(val_types: T) -> Self
    where
        T: IntoIterator<Item = ValType>,
        T::IntoIter: ExactSizeIterator + DoubleEndedIterator,
    {
        let mut val_types = val_types.into_iter();
        assert!(
            val_types.len() >= 2,
            "composite key encoder requires at least two types"
        );
        let last_ty = val_types.next_back().unwrap();
        let prefix: Vec<_> = val_types
            .map(|ty| {
                if let ValKind::VarByte = ty.kind {
                    // Prefix variable-length fields require segmented encoding.
                    PrefixKeyEncoder::Segmented(SegmentedBytesEncoder {
                        nullable: ty.nullable,
                    })
                } else {
                    PrefixKeyEncoder::Single(SingleKeyEncoder(ty))
                }
            })
            .collect();
        let suffix = SingleKeyEncoder(last_ty);
        let prefix_key_len = prefix
            .iter()
            .map(|e| e.est_encode_len())
            .try_fold(0usize, |acc, elem| elem.and_then(|b| acc.checked_add(b)));
        let suffix_key_len = suffix.est_encode_len();
        let encode_len = prefix_key_len.and_then(|p| suffix_key_len.and_then(|s| p.checked_add(s)));
        Self {
            prefix: prefix.into_boxed_slice(),
            suffix,
            encode_len,
        }
    }

    /// Encode all components into one memory-comparable key.
    #[inline]
    fn encode<V: Borrow<Val>>(&self, keys: &[V]) -> BTreeKey {
        debug_assert!(keys.len() == self.prefix.len() + 1);
        if let Some(encode_len) = self.encode_len {
            return encode_multi_keys(&self.prefix, &self.suffix, keys, encode_len);
        }
        // Calculate the precise length of encoded keys.
        let encode_len: usize = self
            .prefix
            .iter()
            .zip(keys)
            .map(|(e, k)| e.encode_len(k.borrow()))
            .sum::<usize>()
            + self.suffix.encode_len(keys.last().unwrap().borrow());
        encode_multi_keys(&self.prefix, &self.suffix, keys, encode_len)
    }

    /// Encode a pair of keys into a memory-comparable b-tree key.
    #[inline]
    fn encode_pair<P: Borrow<Val>, S: Borrow<Val>>(
        &self,
        prefix_key: &[P],
        suffix_key: S,
    ) -> BTreeKey {
        debug_assert!(prefix_key.len() == self.prefix.len());
        if let Some(encode_len) = self.encode_len {
            return encode_key_pair(
                &self.prefix,
                &self.suffix,
                prefix_key,
                suffix_key,
                encode_len,
            );
        }
        // Calculate the precise length of encoded keys.
        let encode_len: usize = self
            .prefix
            .iter()
            .zip(prefix_key)
            .map(|(e, k)| e.encode_len(k.borrow()))
            .sum::<usize>()
            + self.suffix.encode_len(suffix_key.borrow());
        encode_key_pair(
            &self.prefix,
            &self.suffix,
            prefix_key,
            suffix_key,
            encode_len,
        )
    }
}

/// Encoder for single-column and composite B-tree keys.
pub(crate) enum BTreeKeyEncoder {
    Single(SingleKeyEncoder),
    Multi(MultiKeyEncoder),
}

impl BTreeKeyEncoder {
    /// Create a B-tree key encoder based on types of keys.
    #[inline]
    pub(crate) fn new<T>(val_types: T) -> Self
    where
        T: IntoIterator<Item = ValType>,
        T::IntoIter: ExactSizeIterator + DoubleEndedIterator,
    {
        let mut val_types = val_types.into_iter();
        assert!(
            val_types.len() > 0,
            "BTreeKeyEncoder requires at least one type"
        );
        if val_types.len() == 1 {
            let ty = val_types.next_back().unwrap();
            return BTreeKeyEncoder::Single(SingleKeyEncoder(ty));
        }
        BTreeKeyEncoder::Multi(MultiKeyEncoder::new(val_types))
    }

    /// Compute exact encoded size, including nullable and segmented components.
    pub(crate) fn encoded_len(&self, keys: &[Val]) -> ResourceResult<usize> {
        match self {
            Self::Single(encoder) => {
                assert_eq!(keys.len(), 1, "hot-build single key arity");
                Ok(encoder.encode_len(&keys[0]))
            }
            Self::Multi(encoder) => {
                assert_eq!(
                    keys.len(),
                    encoder.prefix.len() + 1,
                    "hot-build composite key arity"
                );
                if let Some(len) = encoder.encode_len {
                    return Ok(len);
                }
                let suffix = encoder.suffix.encode_len(&keys[keys.len() - 1]);
                encoder
                    .prefix
                    .iter()
                    .zip(keys)
                    .try_fold(suffix, |len, (part, val)| {
                        len.checked_add(part.encode_len(val)).ok_or_else(|| {
                            Report::new(ResourceError::InsufficientMemory)
                                .attach("encoded key length overflow")
                        })
                    })
            }
        }
    }

    /// Encode using the exact length already computed for these unchanged values.
    /// Callers admitting scratch must charge outlined storage before this call.
    #[inline]
    pub(crate) fn encode_with_len<V: Borrow<Val>>(&self, keys: &[V], len: usize) -> BTreeKey {
        let key = match self {
            Self::Single(encoder) => {
                assert_eq!(keys.len(), 1, "BTreeKeyEncoder single key arity");
                encoder.encode_single(keys[0].borrow())
            }
            Self::Multi(encoder) => {
                assert_eq!(
                    keys.len(),
                    encoder.prefix.len() + 1,
                    "BTreeKeyEncoder composite key arity"
                );
                encode_multi_keys(&encoder.prefix, &encoder.suffix, keys, len)
            }
        };
        assert_eq!(
            key.as_bytes().len(),
            len,
            "BTreeKeyEncoder admitted key length"
        );
        key
    }

    /// Encode keys into a memory-comparable b-tree key.
    #[inline]
    pub(crate) fn encode<V: Borrow<Val>>(&self, keys: &[V]) -> BTreeKey {
        match self {
            BTreeKeyEncoder::Single(e) => {
                debug_assert!(keys.len() == 1);
                e.encode_single(keys[0].borrow())
            }
            BTreeKeyEncoder::Multi(e) => e.encode(keys),
        }
    }

    /// Encode a logical-key range into an owned B-tree key range.
    #[inline]
    pub(crate) fn encode_range<'r, R>(&self, range: R) -> KeyRange
    where
        R: RangeBounds<&'r [Val]>,
    {
        KeyRange::new(
            range.start_bound().map(|key| self.encode(key)),
            range.end_bound().map(|key| self.encode(key)),
        )
    }

    /// Encode a pair of keys into a memory-comparable b-tree key.
    #[inline]
    pub(crate) fn encode_pair<P: Borrow<Val>, S: Borrow<Val>>(
        &self,
        prefix_key: &[P],
        suffix_key: S,
    ) -> BTreeKey {
        match self {
            BTreeKeyEncoder::Single(_) => {
                panic!("unexpected single key encoder");
            }
            BTreeKeyEncoder::Multi(e) => e.encode_pair(prefix_key, suffix_key),
        }
    }

    /// Encode a non-unique logical-key range into owned exact-key bounds.
    #[inline]
    pub(crate) fn encode_non_unique_range<'r, R>(&self, range: R) -> KeyRange
    where
        R: RangeBounds<&'r [Val]>,
    {
        let lower = match range.start_bound() {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(key) => Bound::Included(self.encode_non_unique_lower_bound(key, true)),
            Bound::Excluded(key) => Bound::Excluded(self.encode_non_unique_lower_bound(key, false)),
        };
        let upper = match range.end_bound() {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(key) => Bound::Included(self.encode_non_unique_upper_bound(key, true)),
            Bound::Excluded(key) => Bound::Excluded(self.encode_non_unique_upper_bound(key, false)),
        };
        KeyRange::new(lower, upper)
    }

    /// Encode a non-unique logical-key equality scan into owned exact-key bounds.
    #[inline]
    pub(crate) fn encode_non_unique_equal_range(&self, key: &[Val]) -> KeyRange {
        self.encode_non_unique_range(key..=key)
    }

    #[inline]
    fn encode_non_unique_lower_bound(&self, key: &[Val], included: bool) -> BTreeKey {
        let row_id = if included {
            min_row_id_sentinel()
        } else {
            max_row_id_sentinel()
        };
        self.encode_pair(key, Val::from(row_id))
    }

    #[inline]
    fn encode_non_unique_upper_bound(&self, key: &[Val], included: bool) -> BTreeKey {
        let row_id = if included {
            max_row_id_sentinel()
        } else {
            min_row_id_sentinel()
        };
        self.encode_pair(key, Val::from(row_id))
    }
}

#[inline]
fn min_row_id_sentinel() -> RowID {
    RowID::new(0)
}

#[inline]
fn max_row_id_sentinel() -> RowID {
    RowID::MAX.value()
}

#[inline]
fn encode_multi_keys<V: Borrow<Val>>(
    prefix: &[PrefixKeyEncoder],
    suffix: &SingleKeyEncoder,
    keys: &[V],
    encode_len: usize,
) -> BTreeKey {
    let (last, prefix_keys) = keys
        .split_last()
        .unwrap_or_else(|| unreachable!("composite key encoder has a suffix"));
    encode_key_pair(prefix, suffix, prefix_keys, last.borrow(), encode_len)
}

#[inline]
fn encode_key_pair<P: Borrow<Val>, S: Borrow<Val>>(
    prefix: &[PrefixKeyEncoder],
    suffix: &SingleKeyEncoder,
    prefix_key: &[P],
    suffix_key: S,
    encode_len: usize,
) -> BTreeKey {
    let mut res = BTreeKey::zeroed(encode_len);
    let mut buf = res.modify_inplace();
    let mut start_idx = 0usize;
    for (encoder, key) in prefix.iter().zip(prefix_key) {
        start_idx = encoder.encode_copy(key.borrow(), &mut buf, start_idx);
    }
    let end_idx = suffix.encode_copy(suffix_key.borrow(), &mut buf, start_idx);
    assert_eq!(
        end_idx,
        buf.len(),
        "BTreeKeyEncoder exact composite key length"
    );
    drop(buf);
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memcmp::{NON_NULL_FLAG, NULL_FLAG};
    use std::f64::consts::PI;
    use std::slice::from_ref;

    /// Purpose: Compare borrowed compressed keys against complete byte-string oracles.
    /// Expected: Every prefix split, inline suffix, and probe boundary preserves ordering and owned conversions.
    #[test]
    fn test_borrowed_key_comparison_and_materialization() {
        let keys: &[&[u8]] = &[
            b"",
            b"a",
            b"abcd",
            b"abcde",
            b"abcdefghijklmnopqrstuvw",
            &[0, 0, 255, 0, 9],
        ];
        for &bytes in keys {
            for split in 0..=bytes.len() {
                let prefix = &bytes[..split];
                let suffix = &bytes[split..];
                let mut views = vec![BTreeKeyView::borrowed(prefix, suffix)];
                if suffix.len() <= 4 {
                    let mut head = [0; 4];
                    head[..suffix.len()].copy_from_slice(suffix);
                    views.push(BTreeKeyView::inline(prefix, head, suffix.len() as u8));
                }
                for view in views {
                    assert_eq!(view.len(), bytes.len());
                    assert_eq!(view.to_vec(), bytes);
                    assert_eq!(view.to_btree_key().as_bytes(), bytes);
                    for &probe in keys {
                        assert_eq!(
                            view.cmp_bytes(probe),
                            bytes.cmp(probe),
                            "key={bytes:?}, split={split}, probe={probe:?}"
                        );
                    }
                    for tail_len in 0..=bytes.len() + 1 {
                        let mut tail = vec![0; tail_len];
                        if tail_len <= bytes.len() {
                            assert_eq!(view.copy_tail(&mut tail), Some(()));
                            assert_eq!(tail, bytes[bytes.len() - tail_len..]);
                        } else {
                            assert_eq!(view.copy_tail(&mut tail), None);
                        }
                    }
                }
            }
        }
    }

    /// Purpose: Preserve inclusive and exclusive scan bounds for borrowed keys.
    /// Expected: Borrowed keys and contiguous key oracles agree for all endpoint kinds and prefix splits.
    #[test]
    fn test_borrowed_key_range_endpoints() {
        let endpoints = [
            Bound::Unbounded,
            Bound::Included(BTreeKey::from(b"abcd".as_slice())),
            Bound::Excluded(BTreeKey::from(b"abcd".as_slice())),
        ];
        for lower in &endpoints {
            for upper in &endpoints {
                let range = KeyRange::new(lower.clone(), upper.clone());
                for bytes in [b"abc".as_slice(), b"abcd", b"abcde"] {
                    for split in 0..=bytes.len() {
                        let key = BTreeKeyView::borrowed(&bytes[..split], &bytes[split..]);
                        assert_eq!(range.lower_accepts_view(key), range.lower_accepts(bytes));
                        assert_eq!(range.upper_accepts_view(key), range.upper_accepts(bytes));
                    }
                }
            }
        }
    }

    /// Purpose: Protect float equality through scalar and composite index-key encoding.
    /// Expected: Signed zeros and NaN variants share canonical bytes in both nullability formats and key positions.
    #[test]
    fn test_float_key_equivalence() {
        let cases = [
            (
                "f32 zero",
                ValKind::F32,
                [Val::from(-0.0f32), Val::from(0.0f32)],
                vec![0x80, 0, 0, 0],
            ),
            (
                "f64 zero",
                ValKind::F64,
                [Val::from(-0.0f64), Val::from(0.0f64)],
                vec![0x80, 0, 0, 0, 0, 0, 0, 0],
            ),
            (
                "f32 NaN",
                ValKind::F32,
                [
                    Val::from(f32::from_bits(0xffc0_1234)),
                    Val::from(f32::from_bits(0x7f80_0001)),
                ],
                vec![0xff; 4],
            ),
            (
                "f64 NaN",
                ValKind::F64,
                [
                    Val::from(f64::from_bits(0xfff8_0000_0000_1234)),
                    Val::from(f64::from_bits(0x7ff0_0000_0000_0001)),
                ],
                vec![0xff; 8],
            ),
        ];
        for (name, kind, values, bytes) in cases {
            assert_eq!(values[0], values[1], "{name}: value equality");
            for nullable in [false, true] {
                let ty = ValType::new(kind, nullable);
                let mut expected = if nullable {
                    vec![NON_NULL_FLAG]
                } else {
                    vec![]
                };
                expected.extend_from_slice(&bytes);
                let scalar = BTreeKeyEncoder::new(vec![ty]);
                for value in &values {
                    let mut appended = vec![];
                    value.encode_memcmp(ty, &mut appended);
                    assert_eq!(
                        appended, expected,
                        "{name}, nullable={nullable}: value encoding"
                    );
                    assert_eq!(
                        scalar.encode(from_ref(value)).as_bytes(),
                        expected,
                        "{name}, nullable={nullable}: scalar key"
                    );
                }
                for float_first in [false, true] {
                    let tag_ty = ValType::new(ValKind::U8, false);
                    let types = if float_first {
                        vec![ty, tag_ty]
                    } else {
                        vec![tag_ty, ty]
                    };
                    let composite = BTreeKeyEncoder::new(types);
                    let mut expected = expected.clone();
                    if float_first {
                        expected.push(7);
                    } else {
                        expected.insert(0, 7);
                    }
                    for value in &values {
                        let key = if float_first {
                            vec![value.clone(), Val::from(7u8)]
                        } else {
                            vec![Val::from(7u8), value.clone()]
                        };
                        assert_eq!(
                            composite.encode(&key).as_bytes(),
                            expected,
                            "{name}, nullable={nullable}, float_first={float_first}"
                        );
                    }
                }
            }
        }
    }

    /// Purpose: Protect scalar key encodings at numeric extremes, byte boundaries, and nulls.
    /// Expected: Encoded bytes match explicit fixtures and length and copy APIs agree.
    #[test]
    fn test_single_key_encoder_basic() {
        // Test all basic types with nullable and non-nullable variants
        let test_cases = vec![
            // Non-nullable
            (
                ValType::new(ValKind::I8, false),
                Val::from(i8::MIN),
                &[0u8][..],
            ),
            (
                ValType::new(ValKind::I8, false),
                Val::from(i8::MAX),
                &[0xFF],
            ),
            (
                ValType::new(ValKind::U8, false),
                Val::from(u8::MAX),
                &[0xFF],
            ),
            (
                ValType::new(ValKind::I16, false),
                Val::from(i16::MIN),
                &[0, 0],
            ),
            (
                ValType::new(ValKind::I16, false),
                Val::from(i16::MAX),
                &[0xFF, 0xFF],
            ),
            (
                ValType::new(ValKind::U16, false),
                Val::from(u16::MAX),
                &[0xFF, 0xFF],
            ),
            (
                ValType::new(ValKind::I32, false),
                Val::from(i32::MIN),
                &[0, 0, 0, 0],
            ),
            (
                ValType::new(ValKind::I32, false),
                Val::from(i32::MAX),
                &[0xFF, 0xFF, 0xFF, 0xFF],
            ),
            (
                ValType::new(ValKind::U32, false),
                Val::from(u32::MAX),
                &[0xFF, 0xFF, 0xFF, 0xFF],
            ),
            (
                ValType::new(ValKind::I64, false),
                Val::from(i64::MIN),
                &[0, 0, 0, 0, 0, 0, 0, 0],
            ),
            (
                ValType::new(ValKind::I64, false),
                Val::from(i64::MAX),
                &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            ),
            (
                ValType::new(ValKind::U64, false),
                Val::from(u64::MAX),
                &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            ),
            (
                ValType::new(ValKind::F32, false),
                Val::from(f32::MIN),
                &[0, 0x80, 0, 0],
            ),
            (
                ValType::new(ValKind::F32, false),
                Val::from(f32::MAX),
                &[255, 127, 255, 255],
            ),
            (
                ValType::new(ValKind::F64, false),
                Val::from(f64::MIN),
                &[0, 0x10, 0, 0, 0, 0, 0, 0],
            ),
            (
                ValType::new(ValKind::F64, false),
                Val::from(f64::MAX),
                &[0xFF, 0xEF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            ),
            (ValType::new(ValKind::VarByte, false), Val::from(b""), &[]),
            (
                ValType::new(ValKind::VarByte, false),
                Val::from(b"test"),
                b"test",
            ),
            // Nullable
            (ValType::new(ValKind::I8, true), Val::Null, &[NULL_FLAG]),
            (ValType::new(ValKind::U8, true), Val::Null, &[NULL_FLAG]),
            (ValType::new(ValKind::I16, true), Val::Null, &[NULL_FLAG]),
            (ValType::new(ValKind::U16, true), Val::Null, &[NULL_FLAG]),
            (ValType::new(ValKind::I32, true), Val::Null, &[NULL_FLAG]),
            (ValType::new(ValKind::U32, true), Val::Null, &[NULL_FLAG]),
            (ValType::new(ValKind::I64, true), Val::Null, &[NULL_FLAG]),
            (ValType::new(ValKind::U64, true), Val::Null, &[NULL_FLAG]),
            (ValType::new(ValKind::F32, true), Val::Null, &[NULL_FLAG]),
            (ValType::new(ValKind::F64, true), Val::Null, &[NULL_FLAG]),
            (
                ValType::new(ValKind::VarByte, true),
                Val::Null,
                &[NULL_FLAG],
            ),
        ];

        for (ty, val, expected) in test_cases {
            let encoder = SingleKeyEncoder(ty);
            let key = encoder.encode_single(&val);
            assert_eq!(key.as_bytes(), expected);

            // Test encode_len
            let len = encoder.encode_len(&val);
            assert_eq!(len, key.as_bytes().len());

            // Test encode_copy
            let mut buf = vec![0u8; len];
            let copied_len = encoder.encode_copy(&val, &mut buf, 0);
            assert_eq!(copied_len, len);
            assert_eq!(buf, key.as_bytes());

            // Test est_encode_len for non-nullable fixed-size types
            if !ty.nullable && ty.kind != ValKind::VarByte {
                assert_eq!(encoder.est_encode_len(), Some(len));
            }
        }
    }

    /// Purpose: Protect scalar and composite B-tree key encodings, including segmented and null keys.
    /// Expected: Encoded keys match the specified byte layout for every fixture.
    #[test]
    fn test_btree_key_encoder() {
        // Test single key encoders
        let test_cases = vec![
            (
                ValType::new(ValKind::I8, false),
                Val::from(42i8),
                &[0x2a_u8 ^ 0x80][..],
            ),
            (ValType::new(ValKind::U8, false), Val::from(42u8), &[42]),
            (
                ValType::new(ValKind::I16, false),
                Val::from(42i16),
                &[0x80, 42],
            ),
            (
                ValType::new(ValKind::U16, false),
                Val::from(42u16),
                &[0, 42],
            ),
            (
                ValType::new(ValKind::I32, false),
                Val::from(42i32),
                &[0x80, 0, 0, 42],
            ),
            (
                ValType::new(ValKind::U32, false),
                Val::from(42u32),
                &[0, 0, 0, 42],
            ),
            (
                ValType::new(ValKind::I64, false),
                Val::from(42i64),
                &[0x80, 0, 0, 0, 0, 0, 0, 42],
            ),
            (
                ValType::new(ValKind::U64, false),
                Val::from(42u64),
                &[0, 0, 0, 0, 0, 0, 0, 42],
            ),
            (
                ValType::new(ValKind::F32, false),
                Val::from(42.0f32),
                &[194, 40, 0, 0],
            ),
            (
                ValType::new(ValKind::F64, false),
                Val::from(42.0f64),
                &[192, 69, 0, 0, 0, 0, 0, 0],
            ),
            (
                ValType::new(ValKind::VarByte, false),
                Val::from(b"test"),
                b"test",
            ),
            (ValType::new(ValKind::I8, true), Val::Null, &[0x01]),
            (ValType::new(ValKind::VarByte, true), Val::Null, &[0x01]),
        ];

        for (ty, val, expected) in test_cases {
            let encoder = BTreeKeyEncoder::new(vec![ty]);
            let key = encoder.encode(from_ref(&val));
            assert_eq!(key.as_bytes(), expected);
        }

        // Test segmented bytes encoder
        let encoder = BTreeKeyEncoder::new(vec![
            ValType::new(ValKind::VarByte, false),
            ValType::new(ValKind::U8, true),
        ]);
        let key = encoder.encode(&[Val::from(b"segmented"), Val::Null]);
        assert_eq!(
            key.as_bytes(),
            &[
                b's', b'e', b'g', b'm', b'e', b'n', b't', b'e', b'd', 0, 0, 0, 0, 0, 0, 9, 0x01
            ]
        );
        let key = encoder.encode(&[Val::from(b"a very long key with three segments"), Val::Null]);
        assert_eq!(
            key.as_bytes(),
            &[
                b'a', b' ', b'v', b'e', b'r', b'y', b' ', b'l', b'o', b'n', b'g', b' ', b'k', b'e',
                b'y', 0xFF, b' ', b'w', b'i', b't', b'h', b' ', b't', b'h', b'r', b'e', b'e', b' ',
                b's', b'e', b'g', 0xFF, b'm', b'e', b'n', b't', b's', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                5, 0x01,
            ],
        );
        drop(key);

        // Test multi-key encoder
        let encoder = BTreeKeyEncoder::new(vec![
            ValType::new(ValKind::I32, false),
            ValType::new(ValKind::VarByte, false),
            ValType::new(ValKind::F64, false),
        ]);
        let keys = vec![Val::from(42i32), Val::from(b"multi"), Val::from(PI)];
        let key = encoder.encode(&keys);
        assert_eq!(
            key.as_bytes(),
            &[
                0x80, 0, 0, 42, b'm', b'u', b'l', b't', b'i', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5,
                0xc0, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18,
            ]
        );

        // Test with NULL values in multi-key
        let encoder = BTreeKeyEncoder::new(vec![
            ValType::new(ValKind::I32, true),
            ValType::new(ValKind::VarByte, true),
        ]);
        let keys = vec![Val::Null, Val::from(b"null_test")];
        let key = encoder.encode(&keys);
        assert_eq!(key.as_bytes(), b"\x01\x02null_test");
    }

    /// Purpose: Protect compatibility of the dedicated and general composite-key encoders.
    /// Expected: Both APIs produce the same specified encoding for a mixed-type nullable key.
    #[test]
    fn test_multi_key_encoder_matches_btree_multi_encoder() {
        let val_types = vec![
            ValType::new(ValKind::U32, false),
            ValType::new(ValKind::VarByte, false),
            ValType::new(ValKind::F64, true),
        ];
        let keys = vec![Val::from(42u32), Val::from(b"segmented-prefix"), Val::Null];

        let btree_encoder = BTreeKeyEncoder::new(val_types.clone());
        let multi_encoder = MultiKeyEncoder::new(val_types);

        let expected = [
            0, 0, 0, 42, b's', b'e', b'g', b'm', b'e', b'n', b't', b'e', b'd', b'-', b'p', b'r',
            b'e', b'f', b'i', 0xff, b'x', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x01,
        ];
        assert_eq!(multi_encoder.encode(&keys).as_bytes(), expected);
        assert_eq!(btree_encoder.encode(&keys).as_bytes(), expected);
    }
}
