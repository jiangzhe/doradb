use crate::value::{Val, ValKind, ValType};
use doradb_datatype::memcmp::{
    BytesExtendable, MemCmpFormat, MemCmpKey, NormalBytes, Null, Nullable, NullableMemCmpFormat,
    SegmentedBytes,
};
use std::borrow::Borrow;

pub type BTreeKey = MemCmpKey;

trait KeyEncoder {
    /// Returns estimated encoded length.
    /// If all keys are fixed-size, it will be exactly encoded byte number.
    /// Otherwise, returns None.
    fn est_encode_len(&self) -> Option<usize>;

    /// Returns encoded length of a given key.
    fn encode_len(&self, key: &Val) -> usize;

    /// Encodes a key and copy to given buffer at given start position.
    fn encode_copy(&self, key: &Val, buf: &mut [u8], start_idx: usize) -> usize;

    /// Encode a key and extend to a given collection.
    /// This method is not recommended, unless the keys are dynamically added.
    /// Otherwise, we can always create fixed-length container with encode_len()
    /// and then encode_copy() keys into it.
    #[allow(dead_code)]
    fn encode_extend<T: BytesExtendable>(&self, key: &Val, buf: &mut T);
}

pub struct SingleKeyEncoder(ValType);

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

    #[inline]
    fn encode_extend<T: BytesExtendable>(&self, key: &Val, buf: &mut T) {
        if key.is_null() {
            return Null.extend_nmcf_to(buf);
        }
        match (self.0.kind, self.0.nullable) {
            (ValKind::I8, false) => key.as_i8().unwrap().extend_mcf_to(buf),
            (ValKind::U8, false) => key.as_u8().unwrap().extend_mcf_to(buf),
            (ValKind::I16, false) => key.as_i16().unwrap().extend_mcf_to(buf),
            (ValKind::U16, false) => key.as_u16().unwrap().extend_mcf_to(buf),
            (ValKind::I32, false) => key.as_i32().unwrap().extend_mcf_to(buf),
            (ValKind::U32, false) => key.as_u32().unwrap().extend_mcf_to(buf),
            (ValKind::I64, false) => key.as_i64().unwrap().extend_mcf_to(buf),
            (ValKind::U64, false) => key.as_u64().unwrap().extend_mcf_to(buf),
            (ValKind::F32, false) => key.as_f32().unwrap().extend_mcf_to(buf),
            (ValKind::F64, false) => key.as_f64().unwrap().extend_mcf_to(buf),
            (ValKind::VarByte, false) => NormalBytes(key.as_bytes().unwrap()).extend_mcf_to(buf),
            (ValKind::I8, true) => key.as_i8().unwrap().extend_nmcf_to(buf),
            (ValKind::U8, true) => key.as_u8().unwrap().extend_nmcf_to(buf),
            (ValKind::I16, true) => key.as_i16().unwrap().extend_nmcf_to(buf),
            (ValKind::U16, true) => key.as_u16().unwrap().extend_nmcf_to(buf),
            (ValKind::I32, true) => key.as_i32().unwrap().extend_nmcf_to(buf),
            (ValKind::U32, true) => key.as_u32().unwrap().extend_nmcf_to(buf),
            (ValKind::I64, true) => key.as_i64().unwrap().extend_nmcf_to(buf),
            (ValKind::U64, true) => key.as_u64().unwrap().extend_nmcf_to(buf),
            (ValKind::F32, true) => key.as_f32().unwrap().extend_nmcf_to(buf),
            (ValKind::F64, true) => key.as_f64().unwrap().extend_nmcf_to(buf),
            (ValKind::VarByte, true) => NormalBytes(key.as_bytes().unwrap()).extend_nmcf_to(buf),
        }
    }
}

/// A Special encoder for variable-length string/bytes to be memory comparable
/// if it is followed by other keys.
pub struct SegmentedBytesEncoder {
    nullable: bool,
}

impl KeyEncoder for SegmentedBytesEncoder {
    #[inline]
    fn est_encode_len(&self) -> Option<usize> {
        None
    }

    #[inline]
    fn encode_len(&self, key: &Val) -> usize {
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

    #[inline]
    fn encode_extend<T: BytesExtendable>(&self, key: &Val, buf: &mut T) {
        if key.is_null() {
            Null.extend_nmcf_to(buf);
            return;
        }
        let bs = SegmentedBytes(key.as_bytes().unwrap());
        if self.nullable {
            bs.extend_nmcf_to(buf);
        } else {
            bs.extend_mcf_to(buf);
        }
    }
}

pub enum PrefixKeyEncoder {
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

    /// Encode a key and extend to given collection.
    #[inline]
    fn encode_extend<T: BytesExtendable>(&self, key: &Val, buf: &mut T) {
        match self {
            PrefixKeyEncoder::Single(e) => e.encode_extend(key, buf),
            PrefixKeyEncoder::Segmented(e) => e.encode_extend(key, buf),
        }
    }
}

pub enum BTreeKeyEncoder {
    Single(SingleKeyEncoder),
    Multi {
        prefix: Box<[PrefixKeyEncoder]>,
        suffix: SingleKeyEncoder,
        encode_len: Option<usize>,
    },
}

impl BTreeKeyEncoder {
    /// Create a B-tree key encoder based on types of keys.
    #[inline]
    pub fn new(mut val_types: Vec<ValType>) -> Self {
        debug_assert!(!val_types.is_empty());
        if val_types.len() == 1 {
            let ty = val_types.pop().unwrap();
            return BTreeKeyEncoder::Single(SingleKeyEncoder(ty));
        }
        let last_ty = val_types.pop().unwrap();

        let prefix: Vec<_> = val_types
            .into_iter()
            .map(|ty| {
                if let ValKind::VarByte = ty.kind {
                    // prefix var-length field key, should be encoded as segmented bytes.
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
            .try_fold(0, |acc, elem| elem.map(|b| acc + b));
        let suffix_key_len = suffix.est_encode_len();
        let encode_len = prefix_key_len.and_then(|p| suffix_key_len.map(|s| p + s));
        BTreeKeyEncoder::Multi {
            prefix: prefix.into_boxed_slice(),
            suffix,
            encode_len,
        }
    }

    /// Returns number of internal encoders.
    /// This is also total key number of this encoder.
    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            BTreeKeyEncoder::Single(_) => 1,
            BTreeKeyEncoder::Multi { prefix, .. } => prefix.len() + 1,
        }
    }

    /// Encode keys into a memory-comparable b-tree key.
    #[inline]
    pub fn encode<V: Borrow<Val>>(&self, keys: &[V]) -> BTreeKey {
        match self {
            BTreeKeyEncoder::Single(e) => {
                debug_assert!(keys.len() == 1);
                e.encode_single(keys[0].borrow())
            }
            BTreeKeyEncoder::Multi {
                prefix,
                suffix,
                encode_len,
            } => {
                debug_assert!(keys.len() == prefix.len() + 1);
                if let Some(encode_len) = encode_len {
                    return encode_multi_keys(prefix, suffix, keys, *encode_len);
                }
                // Calculate the precise length of encoded keys.
                let encode_len: usize = prefix
                    .iter()
                    .zip(keys)
                    .map(|(e, k)| e.encode_len(k.borrow()))
                    .sum::<usize>()
                    + suffix.encode_len(keys.last().unwrap().borrow());
                encode_multi_keys(prefix, suffix, keys, encode_len)
            }
        }
    }

    /// Encode partial key as a prefix.
    /// An optional suffix key length is provided to speed up the
    /// encoding.
    /// If we have known suffix length, we can canculate prefix length
    /// accordingly.
    #[inline]
    pub fn encode_prefix<V: Borrow<Val>>(&self, key: &[V], suffix_len: Option<usize>) -> BTreeKey {
        match self {
            BTreeKeyEncoder::Single(_) => {
                panic!("unexpected single key encoder")
            }
            BTreeKeyEncoder::Multi {
                prefix, encode_len, ..
            } => {
                debug_assert!(key.len() <= prefix.len());
                if let Some(encode_len) = encode_len
                    && let Some(suffix_len) = suffix_len
                {
                    debug_assert!(*encode_len >= suffix_len);
                    return encode_key_prefix(prefix, key, *encode_len - suffix_len);
                }
                let encode_len = prefix
                    .iter()
                    .zip(key)
                    .map(|(e, k)| e.encode_len(k.borrow()))
                    .sum::<usize>();
                encode_key_prefix(prefix, key, encode_len)
            }
        }
    }

    /// Encode a pair of keys into a memory-comparable b-tree key.
    #[inline]
    pub fn encode_pair<P: Borrow<Val>, S: Borrow<Val>>(
        &self,
        prefix_key: &[P],
        suffix_key: S,
    ) -> BTreeKey {
        match self {
            BTreeKeyEncoder::Single(_) => {
                panic!("unexpected single key encoder");
            }
            BTreeKeyEncoder::Multi {
                prefix,
                suffix,
                encode_len,
            } => {
                debug_assert!(prefix_key.len() == prefix.len());
                if let Some(encode_len) = encode_len {
                    return encode_key_pair(prefix, suffix, prefix_key, suffix_key, *encode_len);
                }
                // Calculate the precise length of encoded keys.
                let encode_len: usize = prefix
                    .iter()
                    .zip(prefix_key)
                    .map(|(e, k)| e.encode_len(k.borrow()))
                    .sum::<usize>()
                    + suffix.encode_len(suffix_key.borrow());
                encode_key_pair(prefix, suffix, prefix_key, suffix_key, encode_len)
            }
        }
    }
}

#[inline]
fn encode_multi_keys<V: Borrow<Val>>(
    prefix: &[PrefixKeyEncoder],
    suffix: &SingleKeyEncoder,
    keys: &[V],
    encode_len: usize,
) -> BTreeKey {
    let mut res = BTreeKey::zeroed(encode_len);
    let mut buf = res.modify_inplace();
    let mut start_idx = 0usize;
    for (encoder, key) in prefix.iter().zip(keys) {
        start_idx = encoder.encode_copy(key.borrow(), &mut buf, start_idx);
    }
    let end_idx = suffix.encode_copy(keys.last().unwrap().borrow(), &mut buf, start_idx);
    debug_assert!(end_idx == buf.len());
    drop(buf);
    res
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
    debug_assert!(end_idx == buf.len());
    drop(buf);
    res
}

#[inline]
fn encode_key_prefix<V: Borrow<Val>>(
    encoder: &[PrefixKeyEncoder],
    key: &[V],
    encode_len: usize,
) -> BTreeKey {
    let mut res = BTreeKey::zeroed(encode_len);
    let mut buf = res.modify_inplace();
    let mut idx = 0usize;
    for (e, k) in encoder.iter().zip(key) {
        idx = e.encode_copy(k.borrow(), &mut buf, idx);
    }
    debug_assert!(idx == buf.len());
    drop(buf);
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use doradb_datatype::memcmp::NULL_FLAG;

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
            // assert!(!key.is_empty());
            assert_eq!(key.as_bytes(), expected);

            // Test encode_len
            let len = encoder.encode_len(&val);
            assert_eq!(len, key.as_bytes().len());

            // Test encode_copy
            let mut buf = vec![0u8; len];
            let copied_len = encoder.encode_copy(&val, &mut buf, 0);
            assert_eq!(copied_len, len);
            assert_eq!(buf, key.as_bytes());

            // Test encode_extend
            let mut extended = Vec::new();
            encoder.encode_extend(&val, &mut extended);
            assert_eq!(extended, key.as_bytes());

            // Test est_encode_len for non-nullable fixed-size types
            if !ty.nullable && ty.kind != ValKind::VarByte {
                assert_eq!(encoder.est_encode_len(), Some(len));
            }
        }
    }

    #[test]
    fn test_btree_key_encoder() {
        // Test single key encoders
        let test_cases = vec![
            (
                ValType::new(ValKind::I8, false),
                Val::from(42i8),
                &[42u8 ^ 0x80][..],
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
            let key = encoder.encode(&[val.clone()]);
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
        let keys = vec![Val::from(42i32), Val::from(b"multi"), Val::from(3.14f64)];
        let key = encoder.encode(&keys);
        assert!(!key.is_empty());

        // Test with NULL values in multi-key
        let encoder = BTreeKeyEncoder::new(vec![
            ValType::new(ValKind::I32, true),
            ValType::new(ValKind::VarByte, true),
        ]);
        let keys = vec![Val::Null, Val::from(b"null_test")];
        let key = encoder.encode(&keys);
        assert!(!key.is_empty());
    }
}
