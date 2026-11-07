//! This module contains definition and functions of LWC(Lightweight Compression) Block.

pub(crate) mod block;
mod builder;
mod codec;
mod reader;
pub(crate) use builder::{AppendStop, LwcBuilder, LwcEncodeInput, LwcSourceCursor};
#[cfg(test)]
pub(crate) use codec::offset_validation_count;
pub(crate) use reader::{LwcData, PreparedLwcData};
#[cfg(test)]
pub(crate) use tests::LwcNullBitmapSer;

pub(crate) use block::*;

use crate::error::{DataIntegrityError, DataIntegrityResult};
use crate::file::cow_file::COW_FILE_PAGE_SIZE;
use crate::serde::Serde;
use crate::value::{MemVar, ValType};
use error_stack::Report;
use std::mem;

/// Physical row cap that guarantees inline identity and every deletion subset fit.
pub(crate) const MAX_LWC_ROWS: usize = 15_360;

const LWC_BLOCK_HEADER_SIZE: usize = mem::size_of::<LwcBlockHeader>();
const LWC_BLOCK_BODY_SIZE: usize = LWC_BLOCK_PAYLOAD_SIZE - LWC_BLOCK_HEADER_SIZE;

/// Abstract of primitive data which are lightweight compressed.
/// Provides methods for random access and batch access.
pub(crate) trait LwcPrimitiveData {
    type Value;
    type Iter: Iterator<Item = Self::Value>;

    /// Returns total number of values.
    fn len(&self) -> usize;

    /// Returns whether this data is empty.
    #[inline]
    #[expect(dead_code, reason = "reserved is_empty")]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns value at given position.
    /// None if index is out of range.
    fn value(&self, idx: usize) -> Option<Self::Value>;

    /// Extend all values to target collection.
    #[expect(dead_code, reason = "reserved extend_to")]
    fn extend_to<E: Extend<Self::Value>>(&self, target: &mut E);

    /// Returns the iterator over all values.
    fn iter(&self) -> Self::Iter;
}

/// Borrowed flat varbyte payload.
pub(crate) struct LwcBytes<'a> {
    offsets: &'a [[u8; 4]],
    data: &'a [u8],
}

impl<'a> LwcBytes<'a> {
    #[inline]
    fn slice(&self, idx: usize) -> Option<&'a [u8]> {
        if idx >= self.len() {
            return None;
        }
        let start = u32::from_le_bytes(self.offsets[idx]) as usize;
        let end = u32::from_le_bytes(self.offsets[idx + 1]) as usize;
        if start > end || end > self.data.len() {
            return None;
        }
        Some(&self.data[start..end])
    }

    /// Returns the varbyte value at `idx`.
    #[inline]
    pub(crate) fn value_at(&self, idx: usize) -> Option<MemVar> {
        // LWC writers accept bounded owned values; oversized stored bytes violate that contract.
        self.slice(idx).map(MemVar::from_bytes)
    }
}

impl<'a> LwcPrimitiveData for LwcBytes<'a> {
    type Value = MemVar;
    type Iter = LwcBytesIter<'a>;

    #[inline]
    fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    #[inline]
    fn value(&self, idx: usize) -> Option<Self::Value> {
        self.value_at(idx)
    }

    #[inline]
    fn extend_to<E: Extend<Self::Value>>(&self, target: &mut E) {
        target.extend(self.iter());
    }

    #[inline]
    fn iter(&self) -> Self::Iter {
        LwcBytesIter {
            offsets: self.offsets,
            data: self.data,
            idx: 0,
        }
    }
}

/// Iterator over borrowed flat varbyte values.
pub(crate) struct LwcBytesIter<'a> {
    offsets: &'a [[u8; 4]],
    data: &'a [u8],
    idx: usize,
}

impl<'a> Iterator for LwcBytesIter<'a> {
    type Item = MemVar;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let res = LwcBytes {
            offsets: self.offsets,
            data: self.data,
        }
        .value_at(self.idx);
        self.idx += 1;
        res
    }
}

/// Borrowed null bitmap payload.
#[derive(Debug)]
pub(crate) struct LwcNullBitmap<'a> {
    bytes: &'a [u8],
}

impl<'a> LwcNullBitmap<'a> {
    /// Parses a length-prefixed null bitmap and returns the remaining payload.
    #[inline]
    pub(crate) fn from_bytes(input: &'a [u8]) -> DataIntegrityResult<(Self, &'a [u8])> {
        let (pos, len) = input.deser_u16(0)?;
        let input = &input[pos..];
        let len = len as usize;
        if input.len() < len {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach("LWC null bitmap payload is shorter than declared length"));
        }
        let (bytes, rest) = input.split_at(len);
        Ok((LwcNullBitmap { bytes }, rest))
    }

    /// Returns whether `row_idx` is marked null.
    #[inline]
    pub(crate) fn is_null(&self, row_idx: usize) -> bool {
        let byte_idx = row_idx / 8;
        if byte_idx >= self.bytes.len() {
            return false;
        }
        let bit_mask = 1 << (row_idx % 8);
        self.bytes[byte_idx] & bit_mask != 0
    }

    /// Returns the bitmap length in bytes.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns the raw bitmap bytes.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    pub(crate) fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

/// Complete singleton block size excluding variable payloads, using flat singleton encodings.
pub(crate) fn singleton_base_bytes(columns: &[ValType]) -> usize {
    // TableColumnLayout bounds columns by the u16 ordinal domain; each
    // singleton contributes at most eight value bytes and fixed codec metadata.
    let mut bytes = COW_FILE_PAGE_SIZE - LWC_BLOCK_PAYLOAD_SIZE
        + LWC_BLOCK_HEADER_SIZE
        + columns.len() * mem::size_of::<u16>();
    for column in columns {
        if column.nullable {
            bytes += codec::null_prefix_len(1);
        }
        let values = if column.kind.is_fixed() {
            codec::flat_primitive_len(1, column.kind.inline_len())
        } else {
            codec::flat_bytes_len(1, 0)
        };
        bytes += values;
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::codec::{IntegerEncoding, IntegerEncodingPlan, LwcCode};
    use super::*;
    use crate::bitmap::Bitmap;
    use crate::component::panic_payload_description;
    use crate::compression::*;
    use crate::error::{DataIntegrityError, DataIntegrityResult};
    use crate::layout;
    use crate::primitive::Primitive;
    use crate::serde::{Ser, Serde};
    use crate::value::{Val, ValKind};
    use std::borrow::Cow;
    use std::fmt::Debug;
    use std::panic::catch_unwind;
    use zerocopy::{Immutable, IntoBytes};

    /// Serializer for flat floating-point and variable-byte LWC column payloads.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum LwcPrimitiveSer<'a> {
        FlatF32(Cow<'a, [[u8; 4]]>),
        FlatF64(Cow<'a, [[u8; 8]]>),
        Bytes(LwcBytesSer),
    }

    impl<'a> LwcPrimitiveSer<'a> {
        /// Creates an f32 flat serializer.
        #[inline]
        pub(crate) fn new_f32(input: &'a [f32]) -> Self {
            LwcPrimitiveSer::FlatF32(flat_f32_bytes(input))
        }

        /// Creates an f64 flat serializer.
        #[inline]
        pub(crate) fn new_f64(input: &'a [f64]) -> Self {
            LwcPrimitiveSer::FlatF64(flat_f64_bytes(input))
        }

        /// Creates a borrowed varbyte flat serializer.
        #[inline]
        pub(crate) fn new_bytes(offsets: &[u32], data: &[u8]) -> Self {
            Self::new_bytes_owned(offsets.to_vec(), data.to_vec())
        }

        /// Creates an owned varbyte flat serializer.
        #[inline]
        pub(crate) fn new_bytes_owned(offsets: Vec<u32>, data: Vec<u8>) -> Self {
            assert_eq!(
                offsets.first(),
                Some(&0),
                "LwcPrimitiveSer requires an origin offset"
            );
            LwcPrimitiveSer::Bytes(LwcBytesSer { offsets, data })
        }
    }

    impl<'a> Ser<'a> for LwcPrimitiveSer<'a> {
        #[inline]
        fn ser_len(&self) -> usize {
            1 + match self {
                LwcPrimitiveSer::FlatF32(f) => f.ser_len(),
                LwcPrimitiveSer::FlatF64(f) => f.ser_len(),
                LwcPrimitiveSer::Bytes(b) => b.ser_len(),
            }
        }

        #[inline]
        fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
            let idx = out.ser_u8(start_idx, LwcCode::Flat as u8);
            match self {
                LwcPrimitiveSer::FlatF32(f) => f.ser(out, idx),
                LwcPrimitiveSer::FlatF64(f) => f.ser(out, idx),
                LwcPrimitiveSer::Bytes(b) => b.ser(out, idx),
            }
        }
    }

    /// Owned serializer for flat varbyte payloads.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct LwcBytesSer {
        offsets: Vec<u32>,
        data: Vec<u8>,
    }

    impl Ser<'_> for LwcBytesSer {
        #[inline]
        fn ser_len(&self) -> usize {
            mem::size_of::<u64>() + self.offsets.len() * mem::size_of::<u32>() + self.data.len()
        }

        #[inline]
        fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
            debug_assert_ne!(self.offsets, []);
            let mut idx = out.ser_u64(start_idx, (self.offsets.len() - 1) as u64);
            for off in self.offsets.iter().copied() {
                idx = out.ser_u32(idx, off);
            }
            out.ser_byte_slice(idx, &self.data)
        }
    }

    /// Serializer for length-prefixed null bitmap payloads.
    pub(crate) struct LwcNullBitmapSer<'a> {
        bytes: &'a [u8],
    }

    impl<'a> LwcNullBitmapSer<'a> {
        /// Creates a null bitmap serializer after validating the length prefix fits.
        #[inline]
        pub(crate) fn new(bytes: &'a [u8]) -> Self {
            assert!(
                bytes.len() <= usize::from(u16::MAX),
                "LwcNullBitmapSer bitmap length must fit u16: actual={}",
                bytes.len()
            );
            LwcNullBitmapSer { bytes }
        }
    }

    impl Ser<'_> for LwcNullBitmapSer<'_> {
        #[inline]
        fn ser_len(&self) -> usize {
            mem::size_of::<u16>() + self.bytes.len()
        }

        #[inline]
        fn ser<S: Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
            let idx = out.ser_u16(start_idx, self.bytes.len() as u16);
            out.ser_byte_slice(idx, self.bytes)
        }
    }

    #[inline]
    fn flat_bytes_borrowed<'a, T, const N: usize>(input: &'a [T]) -> Cow<'a, [[u8; N]]>
    where
        [T]: IntoBytes + Immutable,
    {
        Cow::Borrowed(layout::slice_from_bytes::<[u8; N]>(input.as_bytes()))
    }

    #[cfg(target_endian = "little")]
    #[inline]
    fn flat_f32_bytes(input: &[f32]) -> Cow<'_, [[u8; 4]]> {
        flat_bytes_borrowed(input)
    }

    #[cfg(not(target_endian = "little"))]
    #[inline]
    fn flat_f32_bytes(input: &[f32]) -> Cow<'_, [[u8; 4]]> {
        Cow::Owned(input.iter().map(|v| v.to_le_bytes()).collect())
    }

    #[cfg(target_endian = "little")]
    #[inline]
    fn flat_f64_bytes(input: &[f64]) -> Cow<'_, [[u8; 8]]> {
        flat_bytes_borrowed(input)
    }

    #[cfg(not(target_endian = "little"))]
    #[inline]
    fn flat_f64_bytes(input: &[f64]) -> Cow<'_, [[u8; 8]]> {
        Cow::Owned(input.iter().map(|v| v.to_le_bytes()).collect())
    }

    #[inline]
    fn serialize_primitive<'a>(ser: &LwcPrimitiveSer<'a>) -> Vec<u8> {
        let mut bytes = vec![0u8; ser.ser_len()];
        ser.ser(&mut bytes[..], 0);
        bytes
    }

    impl<T> IntegerEncodingPlan<T>
    where
        T: BitPackable + Primitive,
    {
        /// Serializes the planned LWC integer payload with canonical null slots.
        /// Null-free columns borrow their values; other columns replace logical
        /// null slots with the final minimum in a temporary copy before writing.
        pub(crate) fn serialize_lwc(&self, values: &[T], nulls: Option<&[u64]>) -> Vec<u8> {
            let mut values = Cow::Borrowed(values);
            if let Some(nulls) = nulls {
                for idx in 0..values.len() {
                    if nulls.bitmap_get(idx) {
                        values.to_mut()[idx] = self.min();
                    }
                }
            }
            let mut bytes = vec![0; 1 + self.ser_len()];
            bytes[0] = match self.encoding() {
                IntegerEncoding::Flat => LwcCode::Flat as u8,
                IntegerEncoding::ForBitpacking { .. } => LwcCode::ForBitpacking as u8,
            };
            self.ser(&values, &mut bytes[..], 1);
            bytes
        }
    }

    #[track_caller]
    fn assert_invalid_payload<T>(case: &str, result: DataIntegrityResult<T>) {
        let err = match result {
            Ok(_) => panic!("{case}: malformed payload was accepted"),
            Err(err) => err,
        };
        assert_eq!(
            err.current_context(),
            &DataIntegrityError::InvalidPayload,
            "{case}: {err:?}"
        );
    }

    fn flat_payload(len: u64, data: &[u8]) -> Vec<u8> {
        let mut bytes = vec![LwcCode::Flat as u8];
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(data);
        bytes
    }

    fn bitpacked_payload(n_bits: u8, len: u64, min: &[u8], data: &[u8]) -> Vec<u8> {
        let mut bytes = vec![LwcCode::ForBitpacking as u8, n_bits];
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(min);
        bytes.extend_from_slice(data);
        bytes
    }

    fn assert_decoded_values(case: &str, kind: ValKind, bytes: &[u8], expected: &[Val]) {
        let ordinary = LwcData::from_bytes(kind, bytes)
            .unwrap_or_else(|err| panic!("{case}: ordinary decoder failed: {err:?}"));
        let prepared = PreparedLwcData::from_bytes(kind, bytes)
            .unwrap_or_else(|err| panic!("{case}: prepared decoder failed: {err:?}"));
        assert_eq!(ordinary.len(), expected.len(), "{case}: ordinary length");
        assert_eq!(prepared.len(), expected.len(), "{case}: prepared length");
        for (idx, value) in expected.iter().enumerate() {
            assert_eq!(
                ordinary.value(idx).as_ref(),
                Some(value),
                "{case}: ordinary row={idx}"
            );
            assert_eq!(
                prepared.value(bytes, idx).as_ref(),
                Some(value),
                "{case}: prepared row={idx}"
            );
        }
        for idx in [expected.len(), usize::MAX] {
            assert_eq!(
                ordinary.value(idx),
                None,
                "{case}: ordinary out-of-range row={idx}"
            );
            assert_eq!(
                prepared.value(bytes, idx),
                None,
                "{case}: prepared out-of-range row={idx}"
            );
        }
        if let LwcData::Bytes(values) = ordinary {
            let iterated: Vec<_> = values.iter().map(Val::VarByte).collect();
            assert_eq!(iterated, expected, "{case}: variable-byte iteration");
        }
    }

    fn encode_integer<T>(values: &[T]) -> Vec<u8>
    where
        T: BitPackable + Ord + Into<i128> + Primitive,
    {
        let min = values.iter().copied().min().unwrap_or(T::ZERO);
        let max = values.iter().copied().max().unwrap_or(T::ZERO);
        IntegerEncodingPlan::from_extrema(values.len(), min, max).serialize_lwc(values, None)
    }

    fn assert_payload_roundtrip<T>(kind: ValKind, bytes: &[u8], input: &[T])
    where
        T: Copy + TryInto<Val>,
        T::Error: Debug,
    {
        let expected: Vec<_> = input
            .iter()
            .copied()
            .map(|v| v.try_into().unwrap())
            .collect();
        let case = format!("{kind:?}, input={expected:?}");
        assert_decoded_values(&case, kind, bytes, &expected);
    }

    /// Purpose: Select integer encodings by full wire cost for every supported type.
    /// Expected: Empty and singleton inputs stay flat, constants compress, full ranges stay flat, and both readers preserve values.
    #[test]
    fn integer_encoding_full_cost_selection() {
        macro_rules! check_type {
            ($ty:ty, $kind:ident) => {{
                let unit = mem::size_of::<$ty>();
                for (case, values, code, length) in [
                    ("empty", vec![], LwcCode::Flat, 9),
                    ("singleton", vec![17 as $ty], LwcCode::Flat, 9 + unit),
                    (
                        "short wide range",
                        vec![0 as $ty, ((1u64 << (unit * 4)) - 1) as $ty],
                        LwcCode::Flat,
                        9 + 2 * unit,
                    ),
                    (
                        "constant",
                        vec![17 as $ty; 65],
                        LwcCode::ForBitpacking,
                        10 + unit + 9,
                    ),
                    (
                        "full range",
                        [<$ty>::MIN, <$ty>::MAX].repeat(32),
                        LwcCode::Flat,
                        9 + 64 * unit,
                    ),
                ] {
                    let bytes = encode_integer(&values);
                    assert_eq!(bytes[0], code as u8, "{}: {case}", stringify!($ty));
                    assert_eq!(bytes.len(), length, "{}: {case}", stringify!($ty));
                    if matches!(code, LwcCode::ForBitpacking) {
                        assert_eq!(bytes[1], 1);
                    }
                    assert_payload_roundtrip(ValKind::$kind, &bytes, &values);
                }
            }};
        }
        check_type!(i8, I8);
        check_type!(u8, U8);
        check_type!(i16, I16);
        check_type!(u16, U16);
        check_type!(i32, I32);
        check_type!(u32, U32);
        check_type!(i64, I64);
        check_type!(u64, U64);

        for bytes in [
            encode_integer(&[-7i8; 3]),
            encode_integer(&[7u8; 3]),
            encode_integer(&[-700i16; 2]),
            encode_integer(&[700u16; 2]),
        ] {
            assert_eq!(bytes[0], LwcCode::Flat as u8, "ties select flat");
        }
    }

    /// Purpose: Preserve width transitions, byte rounding, and signed and unsigned extreme bases.
    /// Expected: Wire widths and lengths match independent boundary cases and both readers recover exact values in order.
    #[test]
    fn integer_encoding_range_boundaries() {
        for (range, bits) in [
            (0, 1),
            (1, 1),
            (2, 2),
            (3, 2),
            (4, 4),
            (15, 4),
            (16, 8),
            (255, 8),
            (256, 16),
            (65_535, 16),
            (65_536, 32),
            (u64::from(u32::MAX), 32),
        ] {
            let values: Vec<_> = (0..65)
                .map(|i| u64::MAX - if i % 2 == 0 { range } else { 0 })
                .collect();
            let bytes = encode_integer(&values);
            assert_eq!(bytes[0], LwcCode::ForBitpacking as u8, "range={range}");
            assert_eq!(bytes[1], bits, "range={range}");
            assert_eq!(bytes.len(), 18 + (65 * usize::from(bits)).div_ceil(8));
            assert_payload_roundtrip(ValKind::U64, &bytes, &values);
        }
        for values in [
            [-1001i64, -1000].repeat(33),
            [-1, 1].repeat(33),
            [i64::MIN, i64::MIN + 1].repeat(33),
            [i64::MIN, i64::MAX].repeat(33),
        ] {
            assert_payload_roundtrip(ValKind::I64, &encode_integer(&values), &values);
        }
    }

    /// Purpose: Separate point-local offset checks from complete variable-byte validation.
    /// Expected: Both readers reject invalid endpoints; point reads check only their pair while preparation rejects malformed interior offsets.
    #[test]
    fn lwc_readers_reject_malformed_varbyte_offsets() {
        for (case, offsets, expected) in [
            ("nonzero origin", vec![1u32, 1, 3], None),
            ("unreferenced trailing data", vec![0, 1, 2], None),
            ("terminal past backing data", vec![0, 1, 4], None),
            (
                "descending offsets",
                vec![0, 2, 1, 3],
                Some(vec![Some("ab"), None, Some("bc")]),
            ),
            (
                "offset past backing data",
                vec![0, 4, 2, 3],
                Some(vec![None, None, Some("c")]),
            ),
        ] {
            let mut bytes = flat_payload((offsets.len() - 1) as u64, &[]);
            for offset in offsets {
                bytes.extend_from_slice(&offset.to_le_bytes());
            }
            bytes.extend_from_slice(b"abc");
            let before = offset_validation_count();
            let ordinary = LwcData::from_bytes(ValKind::VarByte, &bytes);
            if let Some(expected) = expected {
                let ordinary = ordinary.unwrap();
                assert_eq!(ordinary.len(), expected.len(), "{case}");
                for (row, value) in expected.into_iter().enumerate() {
                    assert_eq!(
                        ordinary.value(row),
                        value.map(|value| Val::try_from(value).unwrap()),
                        "{case}: row={row}"
                    );
                }
                assert_eq!(ordinary.value(ordinary.len()), None, "{case}");
            } else {
                assert_invalid_payload(case, ordinary);
            }
            assert_eq!(offset_validation_count(), before, "{case}: point access");
            assert_invalid_payload(case, PreparedLwcData::from_bytes(ValKind::VarByte, &bytes));
        }
    }

    /// Purpose: Round-trip ordinary and prepared columns across value kinds and compression ranges.
    /// Expected: Both decoders preserve varying integer, float, and byte values and reject out-of-range access.
    #[test]
    fn test_lwc_primitive_decoders_roundtrip() {
        // i8
        for input in [
            vec![-8i8, -7, -4, -1],
            vec![1i8, 0, 1, 0, 1, 0],
            vec![1i8, 2],
            vec![1i8, 2, 4],
            vec![1i8, 2, 4, 8],
            vec![1i8, 2, 4, 8, 16],
            vec![1i8, 2, 4, 8, 16, 32],
            vec![1i8, 2, 4, 16, -100, 100],
        ] {
            assert_payload_roundtrip(ValKind::I8, &encode_integer(&input), &input);
        }

        // u8
        for input in [
            vec![1u8, 2, 4, 7],
            vec![1u8, 2],
            vec![1u8, 2, 4],
            vec![1u8, 2, 4, 8],
            vec![1u8, 2, 4, 8, 16],
            vec![1u8, 2, 4, 8, 16, 32],
            vec![1u8, 2, 4, 16, 100, 200, 250],
        ] {
            assert_payload_roundtrip(ValKind::U8, &encode_integer(&input), &input);
        }

        // i16
        for input in [
            vec![-300i16, -299, -250, -200],
            vec![-1i16, 0],
            vec![-1i16, 0, 1],
            vec![-1i16, 0, 1, 4],
            vec![-1i16, 0, 1, 4, 8],
            vec![-1i16, 2, 3, 4, -8, 16],
            vec![-1i16, 2, 3, 4, -8, 1 << 14],
        ] {
            assert_payload_roundtrip(ValKind::I16, &encode_integer(&input), &input);
        }

        // u16
        for input in [
            vec![300u16, 301, 350, 400],
            vec![1u16, 2],
            vec![1u16, 2, 3, 4],
            vec![1u16, 2, 3, 4, 8],
            vec![1u16, 2, 3, 4, 8, 16],
            vec![1u16, 2, 3, 4, 8, 16, 32],
            vec![1u16, 2, 3, 4, 8, 16, 1 << 14],
        ] {
            assert_payload_roundtrip(ValKind::U16, &encode_integer(&input), &input);
        }

        // i32
        for input in [
            vec![-70_000i32, -69_999, -60_000, -50_000],
            vec![1i32, 2],
            vec![1i32, 2, 3, 4],
            vec![1i32, 2, 3, 4, 8],
            vec![1i32, 2, 3, 4, 8, 16],
            vec![1i32, 2, 3, 4, 8, 16, 32],
            vec![1i32, 2, 3, 4, 8, 16, 32, 128],
            vec![-2i32, -1, 0, 1, 2, 4, 8, 128, 1024],
            vec![-2i32, -1, 0, 1, 2, 4, 8, 128, 1 << 30],
        ] {
            assert_payload_roundtrip(ValKind::I32, &encode_integer(&input), &input);
        }

        // u32
        for input in [
            vec![70_000u32, 70_001, 80_000, 90_000],
            vec![1u32, 2],
            vec![1u32, 2, 3, 4],
            vec![1u32, 2, 3, 4, 8],
            vec![1u32, 2, 3, 4, 8, 16],
            vec![1u32, 2, 3, 4, 8, 16, 32],
            vec![1u32, 3, 7, 15, 31, 63, 127],
            vec![1u32, 3, 7, 15, 31, 63, 127, 1 << 14],
            vec![1u32, 3, 7, 15, 31, 63, 1 << 30],
        ] {
            assert_payload_roundtrip(ValKind::U32, &encode_integer(&input), &input);
        }

        // f32
        for input in [vec![1.5f32, 2.25, -3.5, 0.0], vec![1.25f32, -2.5, 9.75]] {
            assert_payload_roundtrip(
                ValKind::F32,
                &serialize_primitive(&LwcPrimitiveSer::new_f32(&input)),
                &input,
            );
        }

        // i64
        for input in [
            vec![-5_000_000_000i64, -4_999_999_999, -4_000_000_000],
            vec![1i64, 2],
            vec![1i64, 2, 3, 4],
            vec![1i64, 2, 3, 4, 8],
            vec![1i64, 2, 3, 4, 8, 16],
            vec![1i64, 2, 3, 4, 8, 16, 32],
            vec![1i64, 3, 7, 15, 31, 63, 127],
            vec![1i64, 3, 7, 15, 31, 63, 127, 1 << 14],
            vec![-2i64, -1, 0, 1, 2, 4, 8, 1024, 65536],
            vec![-2i64, -1, 0, 1, 2, 4, 8, 1024, 1 << 60],
        ] {
            assert_payload_roundtrip(ValKind::I64, &encode_integer(&input), &input);
        }

        // u64
        for input in [
            vec![5_000_000_000u64, 5_000_000_001, 6_000_000_000],
            vec![1, 1 << 1],
            vec![1, 1 << 1, 1 << 2],
            vec![1, 1 << 1, 1 << 2, 1 << 4],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8, 1 << 16],
            vec![1, 1 << 1, 1 << 2, 1 << 4, 1 << 8, 1 << 16, 1 << 32],
        ] {
            assert_payload_roundtrip(ValKind::U64, &encode_integer(&input), &input);
        }

        // f64
        for input in [
            vec![1.5f64, 2.25, -3.5, 0.0, 128.5],
            vec![1.25f64, -2.5, 9.75],
        ] {
            assert_payload_roundtrip(
                ValKind::F64,
                &serialize_primitive(&LwcPrimitiveSer::new_f64(&input)),
                &input,
            );
        }

        for (offsets, bytes, expected) in [
            (vec![0, 3, 3, 7], &b"abcdefg"[..], ["abc", "", "defg"]),
            (vec![0, 1, 3, 6], &b"abcdef"[..], ["a", "bc", "def"]),
        ] {
            assert_payload_roundtrip(
                ValKind::VarByte,
                &serialize_primitive(&LwcPrimitiveSer::new_bytes(&offsets, bytes)),
                &expected,
            );
        }
    }

    /// Purpose: Decode fixed wire fixtures independently of the primitive serializers.
    /// Expected: Ordinary and prepared readers preserve signed values and every supported packing width.
    #[test]
    fn test_lwc_decoders_read_known_payloads() {
        for (case, kind, bytes, expected) in [
            (
                "flat signed little-endian",
                ValKind::I16,
                flat_payload(2, &[0xd4, 0xfe, 0xd5, 0xfe]),
                vec![Val::I16(-300), Val::I16(-299)],
            ),
            (
                "one-bit deltas",
                ValKind::U8,
                bitpacked_payload(1, 3, &[10], &[0b0000_0010]),
                vec![Val::U8(10), Val::U8(11), Val::U8(10)],
            ),
            (
                "two-bit deltas",
                ValKind::U8,
                bitpacked_payload(2, 4, &[3], &[0b1110_0100]),
                vec![Val::U8(3), Val::U8(4), Val::U8(5), Val::U8(6)],
            ),
            (
                "four-bit signed deltas",
                ValKind::I8,
                bitpacked_payload(4, 3, &[0xf8], &[0x10, 0x0f]),
                vec![Val::I8(-8), Val::I8(-7), Val::I8(7)],
            ),
            (
                "byte deltas",
                ValKind::I16,
                bitpacked_payload(8, 2, &[0xd4, 0xfe], &[0, 255]),
                vec![Val::I16(-300), Val::I16(-45)],
            ),
            (
                "word deltas",
                ValKind::U32,
                bitpacked_payload(16, 2, &[0; 4], &[0, 0, 255, 255]),
                vec![Val::U32(0), Val::U32(65_535)],
            ),
            (
                "double-word signed deltas",
                ValKind::I64,
                bitpacked_payload(32, 2, &[255; 8], &[0, 0, 0, 0, 255, 255, 255, 255]),
                vec![Val::I64(-1), Val::I64(4_294_967_294)],
            ),
        ] {
            assert_decoded_values(case, kind, &bytes, &expected);
        }
    }

    /// Purpose: Preserve every supported integer-type and packed-width combination independently of writer selection.
    /// Expected: Both readers recover extreme bases and maximal deltas from manually packed fixtures, and reject widths that cannot compress their source type.
    #[test]
    fn all_integer_reader_widths_match_wire_fixtures() {
        macro_rules! check {
            ($ty:ty, $kind:ident) => {
                for bits in [1usize, 2, 4, 8, 16, 32, 64] {
                    let base = <$ty>::MIN;
                    let delta = if bits == 64 {
                        u64::MAX
                    } else {
                        (1u64 << bits) - 1
                    };
                    let deltas = [0, 1, delta];
                    let mut packed = vec![0u8; (3 * bits).div_ceil(8)];
                    for (idx, delta) in deltas.iter().enumerate() {
                        for bit in 0..bits {
                            let pos = idx * bits + bit;
                            packed[pos / 8] |= (((delta >> bit) & 1) as u8) << (pos % 8);
                        }
                    }
                    let bytes = bitpacked_payload(bits as u8, 3, &base.to_le_bytes(), &packed);
                    let case = format!("{} bits={bits}", stringify!($ty));
                    if bits >= mem::size_of::<$ty>() * 8 {
                        assert_invalid_payload(&case, LwcData::from_bytes(ValKind::$kind, &bytes));
                        assert_invalid_payload(
                            &case,
                            PreparedLwcData::from_bytes(ValKind::$kind, &bytes),
                        );
                    } else {
                        let expected: Vec<_> = deltas
                            .iter()
                            .map(|&delta| Val::from((base as i128 + i128::from(delta)) as $ty))
                            .collect();
                        assert_decoded_values(&case, ValKind::$kind, &bytes, &expected);
                    }
                }
            };
        }
        check!(i8, I8);
        check!(u8, U8);
        check!(i16, I16);
        check!(u16, U16);
        check!(i32, I32);
        check!(u32, U32);
        check!(i64, I64);
        check!(u64, U64);
    }

    /// Purpose: Bound persisted column counts by the block header's representation before sizing payloads.
    /// Expected: Both readers accept the maximum u16 count and reject a complete payload one row beyond it for every codec layout.
    #[test]
    fn lwc_readers_enforce_block_row_count_domain() {
        let max_rows = usize::from(u16::MAX);
        for rows in [max_rows, max_rows + 1] {
            for (case, kind, bytes, expected) in [
                (
                    "flat",
                    ValKind::U8,
                    flat_payload(rows as u64, &vec![7; rows]),
                    Val::U8(7),
                ),
                (
                    "bytes",
                    ValKind::VarByte,
                    flat_payload(rows as u64, &vec![0; (rows + 1) * 4]),
                    Val::try_from("").unwrap(),
                ),
                (
                    "FOR",
                    ValKind::U8,
                    bitpacked_payload(1, rows as u64, &[7], &vec![0; rows.div_ceil(8)]),
                    Val::U8(7),
                ),
            ] {
                let ordinary = LwcData::from_bytes(kind, &bytes);
                let prepared = PreparedLwcData::from_bytes(kind, &bytes);
                if rows > max_rows {
                    assert_invalid_payload(&format!("{case}: ordinary"), ordinary);
                    assert_invalid_payload(&format!("{case}: prepared"), prepared);
                    continue;
                }
                let ordinary = ordinary.unwrap();
                let prepared = prepared.unwrap();
                assert_eq!(ordinary.len(), rows, "{case}: ordinary count");
                assert_eq!(prepared.len(), rows, "{case}: prepared count");
                for row in [0, rows - 1] {
                    assert_eq!(
                        ordinary.value(row).as_ref(),
                        Some(&expected),
                        "{case}: ordinary row={row}"
                    );
                    assert_eq!(
                        prepared.value(&bytes, row).as_ref(),
                        Some(&expected),
                        "{case}: prepared row={row}"
                    );
                }
                assert_eq!(ordinary.value(rows), None, "{case}: ordinary end");
                assert_eq!(prepared.value(&bytes, rows), None, "{case}: prepared end");
            }
        }
    }

    /// Purpose: Reject malformed column headers, lengths, typed elements, and packing widths.
    /// Expected: Both readers report invalid payloads without truncating counts or ignoring excess bytes.
    #[test]
    fn test_lwc_decoders_reject_malformed_payloads() {
        for (case, kind, bytes) in [
            ("empty payload", ValKind::U8, vec![]),
            ("unknown codec", ValKind::U8, vec![255; 9]),
            (
                "FOR on float",
                ValKind::F32,
                bitpacked_payload(1, 1, &[0; 4], &[0]),
            ),
            (
                "FOR on bytes",
                ValKind::VarByte,
                bitpacked_payload(1, 1, &[0; 8], &[0]),
            ),
            (
                "flat maximum persisted count",
                ValKind::U64,
                flat_payload(u64::MAX, &[]),
            ),
            (
                "byte maximum persisted count",
                ValKind::VarByte,
                flat_payload(u64::MAX, &[]),
            ),
            (
                "FOR maximum persisted count",
                ValKind::U64,
                bitpacked_payload(32, u64::MAX, &[0; 8], &[]),
            ),
            ("flat truncated data", ValKind::U8, flat_payload(2, &[7])),
            ("flat excess data", ValKind::U8, flat_payload(1, &[7, 8])),
            (
                "flat oversized count",
                ValKind::U8,
                flat_payload(1u64 << 32, &[]),
            ),
            (
                "flat partial typed element",
                ValKind::I16,
                flat_payload(1, &[7]),
            ),
            (
                "bitpacked missing data",
                ValKind::U8,
                bitpacked_payload(1, 1, &[0], &[]),
            ),
            (
                "bitpacked excess data",
                ValKind::U8,
                bitpacked_payload(1, 1, &[0], &[0, 0]),
            ),
            (
                "bitpacked oversized count",
                ValKind::U8,
                bitpacked_payload(1, 1u64 << 32, &[0], &[]),
            ),
            (
                "unsupported packing width",
                ValKind::U8,
                bitpacked_payload(3, 1, &[0], &[0]),
            ),
            (
                "truncated variable-byte offsets",
                ValKind::VarByte,
                flat_payload(1, &[0; 7]),
            ),
        ] {
            assert_invalid_payload(
                &format!("{case}: ordinary"),
                LwcData::from_bytes(kind, &bytes),
            );
            assert_invalid_payload(
                &format!("{case}: prepared"),
                PreparedLwcData::from_bytes(kind, &bytes),
            );
        }
    }

    /// Purpose: Reject variable-byte builder offsets with no origin or a nonzero origin.
    /// Expected: Both borrowed and owned fixture construction assert the origin-offset contract.
    #[test]
    fn test_lwc_bytes_rejects_invalid_builder_offsets() {
        for (case, offsets, data) in [
            ("missing origin", vec![], vec![]),
            ("nonzero origin", vec![1, 2], vec![0]),
        ] {
            for (mode, result) in [
                (
                    "borrowed",
                    catch_unwind(|| LwcPrimitiveSer::new_bytes(&offsets, &data)),
                ),
                (
                    "owned",
                    catch_unwind(|| {
                        LwcPrimitiveSer::new_bytes_owned(offsets.clone(), data.clone())
                    }),
                ),
            ] {
                let panic = result
                    .expect_err("invalid fixture offsets must violate the constructor contract");
                let message = panic_payload_description(panic.as_ref());
                assert!(
                    message.contains("LwcPrimitiveSer requires an origin offset"),
                    "{case}: {mode}: {message}"
                );
            }
        }
    }

    /// Purpose: Guard variable-byte access at invalid offsets and extreme row indices.
    /// Expected: Indexed access and iteration return no invalid slice and index arithmetic cannot overflow.
    #[test]
    fn test_lwc_bytes_out_of_range() {
        for (case, offsets, data, expected_len) in [
            ("offset past data", vec![0u32, 5], vec![1, 2, 3], 1),
            ("descending offsets", vec![3, 1], vec![1, 2, 3], 1),
            ("missing offsets", vec![], vec![], 0),
            ("empty column", vec![0], vec![], 0),
        ] {
            let offsets: Vec<_> = offsets.into_iter().map(u32::to_le_bytes).collect();
            let bytes = LwcBytes {
                offsets: &offsets,
                data: &data,
            };
            assert_eq!(bytes.len(), expected_len, "{case}");
            for idx in [0, expected_len, usize::MAX] {
                assert!(bytes.value_at(idx).is_none(), "{case}: row={idx}");
            }
            assert!(bytes.iter().next().is_none(), "{case}: iteration");
        }
    }

    /// Purpose: Round-trip a null bitmap spanning multiple bytes.
    /// Expected: Serialization preserves the bitmap and null positions across byte boundaries.
    #[test]
    fn test_lwc_null_bitmap_serde() {
        let bytes = [0b0000_1010u8, 0b0000_0001];
        let ser = LwcNullBitmapSer::new(&bytes);
        let mut out = vec![0u8; ser.ser_len()];
        let idx = ser.ser(&mut out[..], 0);
        assert_eq!(idx, out.len());

        let (bitmap, rest) = LwcNullBitmap::from_bytes(&out).unwrap();
        assert_eq!(rest, []);
        assert_eq!(bitmap.as_bytes(), bytes);
        assert!(bitmap.is_null(1));
        assert!(bitmap.is_null(3));
        assert!(bitmap.is_null(8));
        assert!(!bitmap.is_null(0));
        assert!(!bitmap.is_null(2));
    }
}
