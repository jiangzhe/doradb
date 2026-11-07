//! LWC codec decisions and wire layouts shared by admission, writers and readers.

use crate::bitmap::Bitmap;
use crate::compression::*;
use crate::error::{DataIntegrityError, DataIntegrityResult};
use crate::primitive::{Primitive, decode_primitive, primitive_types};
use crate::serde::Serde;
use crate::value::{MemVar, Val, ValKind};
use error_stack::Report;
use std::mem;
use zerocopy::IntoBytes;

#[cfg(test)]
pub(crate) use tests::offset_validation_count;

/// Flat codec tag and physical element count.
pub(super) const FLAT_HEADER_LEN: usize = 1 + 8;
/// FOR codec tag, packed width and physical element count, before the typed minimum.
pub(super) const FOR_HEADER_LEN: usize = 1 + 1 + 8;

/// Serialized compression tag at the beginning of each LWC value payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum LwcCode {
    Flat = 1,
    ForBitpacking = 3,
}

impl LwcCode {
    fn decode(value: u8) -> DataIntegrityResult<Self> {
        match value {
            1 => Ok(Self::Flat),
            3 => Ok(Self::ForBitpacking),
            _ => Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("invalid LWC code {value}"))),
        }
    }
}

/// Integer wire encoding selected by complete serialized cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IntegerEncoding {
    Flat,
    ForBitpacking { bits: u8 },
}

/// Exact integer serialization plan, excluding the outer LWC compression tag.
/// The minimum also supplies the canonical value for physical null slots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IntegerEncodingPlan<T> {
    len: usize,
    min: T,
    encoding: IntegerEncoding,
}

impl<T: BitPackable + Ord + Into<i128>> IntegerEncodingPlan<T> {
    /// Plans from logical non-null extrema and the full physical row count.
    /// All-null columns supply zero extrema; empty columns always use flat.
    /// Compare complete flat/FOR serialized costs, keeping flat on ties. The
    /// selected width and minimum are reused when writing, so sizing stays exact.
    #[inline]
    pub(crate) fn from_extrema(len: usize, min: T, max: T) -> Self {
        let mut plan = Self {
            len,
            min,
            encoding: IntegerEncoding::Flat,
        };
        if len != 0 {
            // i128 represents every supported integer and its full positive range.
            let range = (max.into() - min.into()) as u64;
            if let Some(bits) = bitpacking_width(range, mem::size_of::<T>()) {
                let packed = Self {
                    encoding: IntegerEncoding::ForBitpacking { bits },
                    ..plan
                };
                if packed.ser_len() < plan.ser_len() {
                    plan = packed;
                }
            }
        }
        plan
    }
}

impl<T: Copy> IntegerEncodingPlan<T> {
    /// Returns the frozen physical row count paired with this plan.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Returns the selected encoding without recomputing statistics.
    #[inline]
    pub(crate) fn encoding(&self) -> IntegerEncoding {
        self.encoding
    }

    /// Returns the canonical null placeholder and FOR base.
    #[inline]
    pub(crate) fn min(&self) -> T {
        self.min
    }

    /// Returns the exact serialized length, excluding the LWC compression tag.
    #[inline]
    pub(crate) fn ser_len(&self) -> usize {
        let len = match self.encoding {
            IntegerEncoding::Flat => flat_primitive_len(self.len, mem::size_of::<T>()),
            IntegerEncoding::ForBitpacking { bits } => {
                for_len(self.len, mem::size_of::<T>(), usize::from(bits))
            }
        };
        len - 1
    }
}

impl<T> IntegerEncodingPlan<T>
where
    T: BitPackable + Primitive,
{
    /// Writes the selected integer encoding without the outer LWC tag.
    /// Flat stores an eight-byte count and little-endian values. FOR stores a
    /// width byte, count, typed minimum, and rounded packed delta bytes.
    /// The caller owns the value-range invariant, including null normalization.
    #[inline]
    pub(crate) fn ser<S: Serde + ?Sized>(
        &self,
        values: &[T],
        out: &mut S,
        start_idx: usize,
    ) -> usize {
        // The plan owns the physical row count for both encodings. Enforce the
        // slice pairing before writing any bytes, without rescanning its range.
        assert_eq!(
            values.len(),
            self.len,
            "integer encoding plan slice length mismatch: encoding={:?}",
            self.encoding
        );
        match self.encoding {
            IntegerEncoding::Flat => write_flat_values(values, out, start_idx),
            IntegerEncoding::ForBitpacking { bits } => {
                let idx = out.ser_u8(start_idx, bits);
                let idx = out.ser_u64(idx, self.len as u64);
                let idx = self.min.write_le(out, idx);
                let packed_len = self.ser_len() - 1 - 8 - mem::size_of::<T>();
                let (idx, to_pack) = out.ser_mut(idx, packed_len);
                match bits {
                    1 => for_b1_pack(values, self.min, to_pack),
                    2 => for_b2_pack(values, self.min, to_pack),
                    4 => for_b4_pack(values, self.min, to_pack),
                    8 => for_b8_pack(values, self.min, to_pack),
                    16 => for_b16_pack(values, self.min, to_pack),
                    32 => for_b32_pack(values, self.min, to_pack),
                    _ => unreachable!("integer encoding plan contains an unsupported width"),
                }
                idx
            }
        }
    }
}

/// Integer minimum in owned decoder metadata, retaining its signedness.
#[derive(Clone, Copy, Debug)]
pub(super) enum IntegerMinimum {
    Signed(i64),
    Unsigned(u64),
}

/// Payload regions and codec parameters; individual byte offsets need separate checks.
#[derive(Clone, Copy, Debug)]
pub(super) enum ValueLayout {
    FlatPrimitive {
        data_start: usize,
        unit_len: usize,
    },
    FlatVarByte {
        offsets_start: usize,
        data_start: usize,
    },
    ForBitpacking {
        data_start: usize,
        n_bits: usize,
        min: IntegerMinimum,
    },
}

/// Shared structural layout with validated extents and byte-offset endpoints.
/// Interior byte offsets remain unchecked until point access or full validation.
#[derive(Clone, Copy, Debug)]
pub(super) struct LwcLayout {
    pub(super) kind: ValKind,
    pub(super) len: usize,
    input_len: usize,
    pub(super) encoding: ValueLayout,
}

impl LwcLayout {
    /// Parses a value payload in constant time without scanning interior byte offsets.
    pub(super) fn from_bytes(kind: ValKind, input: &[u8]) -> DataIntegrityResult<Self> {
        let (_, tag) = input.deser_u8(0)?;
        let code = LwcCode::decode(tag)?;
        let count_start = match code {
            LwcCode::Flat => 1,
            LwcCode::ForBitpacking => 2,
        };
        let (_, count) = input.deser_u64(count_start)?;
        // Every column belongs to a block with a u16 row count. Validate that
        // representation before using the persisted count in extent arithmetic.
        let len = usize::from(
            u16::try_from(count)
                .map_err(|_| invalid_layout("LWC count exceeds u16 block row-count limit"))?,
        );
        let encoding = match code {
            LwcCode::Flat if kind == ValKind::VarByte => {
                let data_start = flat_bytes_len(len, 0);
                if data_start > input.len() {
                    return Err(invalid_layout(
                        "LWC varbyte payload is shorter than its offset table",
                    ));
                }
                // The extent includes N+1 offsets, even for an empty column.
                if u32::read_le(&input[FLAT_HEADER_LEN..FLAT_HEADER_LEN + 4]) != 0 {
                    return Err(invalid_layout("LWC varbyte origin offset is not zero"));
                }
                if u32::read_le(&input[data_start - 4..data_start]) as usize
                    != input.len() - data_start
                {
                    return Err(invalid_layout(
                        "LWC varbyte final offset does not match data length",
                    ));
                }
                ValueLayout::FlatVarByte {
                    offsets_start: FLAT_HEADER_LEN,
                    data_start,
                }
            }
            LwcCode::Flat => {
                let expected = flat_primitive_len(len, kind.inline_len());
                validate_extent(input.len(), expected)?;
                ValueLayout::FlatPrimitive {
                    data_start: FLAT_HEADER_LEN,
                    unit_len: kind.inline_len(),
                }
            }
            LwcCode::ForBitpacking => {
                let n_bits = usize::from(input[1]);
                if !matches!(n_bits, 1 | 2 | 4 | 8 | 16 | 32) || n_bits >= kind.inline_len() * 8 {
                    return Err(
                        Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                            "unexpected packing bits: val_kind={kind:?}, n_bits={n_bits}"
                        )),
                    );
                }
                let data_start = FOR_HEADER_LEN + kind.inline_len();
                let expected = for_len(len, kind.inline_len(), n_bits);
                validate_extent(input.len(), expected)?;
                let min = read_minimum(kind, &input[FOR_HEADER_LEN..data_start])?;
                ValueLayout::ForBitpacking {
                    data_start,
                    n_bits,
                    min,
                }
            }
        };
        Ok(Self {
            kind,
            len,
            input_len: input.len(),
            encoding,
        })
    }
}

/// Fully validated metadata for repeated decoding from the same immutable bytes.
/// Only complete validation may construct this type; prepared access trusts its offsets.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ValidatedLwcLayout {
    layout: LwcLayout,
}

impl ValidatedLwcLayout {
    /// Parses the shared structural layout and validates all interior byte offsets.
    pub(crate) fn from_bytes(kind: ValKind, input: &[u8]) -> DataIntegrityResult<Self> {
        let layout = LwcLayout::from_bytes(kind, input)?;
        if let ValueLayout::FlatVarByte {
            offsets_start,
            data_start,
        } = layout.encoding
        {
            validate_byte_offsets(&input[offsets_start..data_start], input.len() - data_start)?;
        }
        Ok(Self { layout })
    }

    /// Returns the validated physical element count.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.layout.len
    }

    /// Decodes a scalar from the same immutable input used during validation.
    #[inline]
    pub(crate) fn value(&self, input: &[u8], idx: usize) -> Option<Val> {
        let layout = &self.layout;
        debug_assert_eq!(input.len(), layout.input_len);
        if idx >= layout.len {
            return None;
        }
        match layout.encoding {
            ValueLayout::FlatPrimitive {
                data_start,
                unit_len,
            } => Some(decode_primitive(
                layout.kind,
                &input[data_start + idx * unit_len..data_start + (idx + 1) * unit_len],
            )),
            ValueLayout::FlatVarByte {
                offsets_start,
                data_start,
            } => {
                let start = offsets_start + idx * 4;
                let lo = u32::read_le(&input[start..start + 4]) as usize;
                let hi = u32::read_le(&input[start + 4..start + 8]) as usize;
                Some(Val::VarByte(MemVar::from_bytes(
                    &input[data_start + lo..data_start + hi],
                )))
            }
            ValueLayout::ForBitpacking {
                data_start,
                n_bits,
                min,
            } => {
                let data = &input[data_start..];
                let delta = match n_bits {
                    1 | 2 | 4 => {
                        let bit = idx * n_bits;
                        u64::from((data[bit / 8] >> (bit % 8)) & ((1 << n_bits) - 1))
                    }
                    8 => u64::from(data[idx]),
                    16 => u64::from(u16::read_le(&data[idx * 2..idx * 2 + 2])),
                    32 => u64::from(u32::read_le(&data[idx * 4..idx * 4 + 4])),
                    _ => unreachable!("validated LWC packing width"),
                };
                Some(decode_delta(layout.kind, min, delta))
            }
        }
    }
}

#[cold]
fn invalid_layout(message: &'static str) -> Report<DataIntegrityError> {
    Report::new(DataIntegrityError::InvalidPayload).attach(message)
}

fn validate_extent(actual: usize, expected: usize) -> DataIntegrityResult<()> {
    if actual != expected {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "LWC payload length mismatch: expected {expected}, actual {actual}"
            )),
        );
    }
    Ok(())
}

fn validate_byte_offsets(bytes: &[u8], data_len: usize) -> DataIntegrityResult<()> {
    #[cfg(test)]
    tests::record_offset_validation();
    let mut previous = None;
    for bytes in bytes.as_chunks::<4>().0 {
        let offset = u32::from_le_bytes(*bytes) as usize;
        if previous.is_none() && offset != 0
            || previous.is_some_and(|prev| offset < prev)
            || offset > data_len
        {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("invalid LWC varbyte offset: previous={previous:?}, current={offset}, data_len={data_len}")));
        }
        previous = Some(offset);
    }
    if previous != Some(data_len) {
        return Err(invalid_layout(
            "LWC varbyte final offset does not match data length",
        ));
    }
    Ok(())
}

macro_rules! integer_metadata {
    (integers { $($int:ident: $it:ty, $category:ident, $wide:ty;)* }
     floats { $($float:ident: $ft:ty;)* }) => {
        fn read_minimum(kind: ValKind, bytes: &[u8]) -> DataIntegrityResult<IntegerMinimum> {
            match kind {
                $(ValKind::$int => Ok(IntegerMinimum::$category(<$it>::read_le(bytes) as $wide)),)*
                _ => Err(invalid_layout("LWC FOR requires an integer value kind")),
            }
        }

        #[inline]
        fn decode_delta(kind: ValKind, min: IntegerMinimum, delta: u64) -> Val {
            match (kind, min) {
                $((ValKind::$int, IntegerMinimum::$category(min)) => Val::$int((min as $it).wrapping_add(delta as $it)),)*
                _ => unreachable!("validated LWC minimum agrees with its integer kind"),
            }
        }
    };
}

primitive_types!(integer_metadata);

/// Complete flat primitive extent, including the codec tag.
/// Callers bound rows by the block's u16 count and width by its primitive kind.
#[inline]
pub(super) fn flat_primitive_len(rows: usize, width: usize) -> usize {
    rows * width + FLAT_HEADER_LEN
}

/// Complete flat byte extent, including the tag, sentinel offset and payload.
/// Rows fit u16; data is bounded by admitted value lengths, or zero when parsing offsets.
#[inline]
pub(super) fn flat_bytes_len(rows: usize, data_len: usize) -> usize {
    (rows + 1) * 4 + FLAT_HEADER_LEN + data_len
}

/// Complete FOR extent, including the codec tag and typed minimum.
/// Rows fit u16; width and bits come from a supported primitive encoding.
#[inline]
fn for_len(rows: usize, width: usize, bits: usize) -> usize {
    (rows * bits).div_ceil(8) + width + FOR_HEADER_LEN
}

/// Null-bitmap prefix length, including its two-byte byte count.
#[inline]
pub(super) fn null_prefix_len(rows: usize) -> usize {
    2 + rows.div_ceil(8)
}

/// Writes the count and little-endian primitive values without a codec tag.
#[inline]
fn write_flat_values<T: Primitive, S: Serde + ?Sized>(
    values: &[T],
    out: &mut S,
    start: usize,
) -> usize {
    let pos = out.ser_u64(start, values.len() as u64);
    #[cfg(target_endian = "little")]
    {
        out.ser_byte_slice(pos, values.as_bytes())
    }
    #[cfg(not(target_endian = "little"))]
    {
        let mut pos = pos;
        for value in values {
            pos = value.write_le(out, pos);
        }
        pos
    }
}

/// Writes a complete flat primitive payload into its planned destination.
#[inline]
pub(super) fn write_flat<T: Primitive>(values: &[T], output: &mut [u8]) -> usize {
    let pos = output.ser_u8(0, LwcCode::Flat as u8);
    write_flat_values(values, output, pos)
}

/// Writes a frozen integer plan after canonicalizing its physical null slots.
/// Staged null slots may contain stale source values or zero placeholders.
/// Replace them with the final minimum so every packed delta fits the planned width.
pub(super) fn write_integer<T>(
    plan: &IntegerEncodingPlan<T>,
    values: &mut [T],
    nulls: Option<&[u64]>,
    output: &mut [u8],
) -> usize
where
    T: BitPackable + Primitive,
{
    if let Some(nulls) = nulls {
        for (pos, value) in values.iter_mut().enumerate() {
            if nulls.bitmap_get(pos) {
                *value = plan.min();
            }
        }
    }
    output[0] = match plan.encoding() {
        IntegerEncoding::Flat => LwcCode::Flat as u8,
        IntegerEncoding::ForBitpacking { .. } => LwcCode::ForBitpacking as u8,
    };
    plan.ser(values, output, 1)
}

/// Writes byte offsets and data directly into their planned final extent.
/// The builder supplies an origin offset followed by one offset per row.
/// Append-only staging establishes offset order and the terminal data length;
/// the encoder checks the actual byte length against the plan before this call.
pub(super) fn write_bytes(offsets: &[u32], data: &[u8], output: &mut [u8]) -> usize {
    let mut pos = output.ser_u8(0, LwcCode::Flat as u8);
    pos = output.ser_u64(pos, (offsets.len() - 1) as u64);
    for &offset in offsets {
        pos = output.ser_u32(pos, offset);
    }
    output.ser_byte_slice(pos, data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::{Deser, ForBitpackingDeser};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    thread_local! {
        static OFFSET_VALIDATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    /// Returns the number of complete offset validation attempts on this thread.
    pub(crate) fn offset_validation_count() -> usize {
        OFFSET_VALIDATIONS.get()
    }

    /// Records a validation attempt without affecting other test threads.
    pub(super) fn record_offset_validation() {
        OFFSET_VALIDATIONS.set(OFFSET_VALIDATIONS.get() + 1);
    }

    fn assert_flat_wire<T: Primitive>(values: &[T], payload: &[u8]) {
        let mut expected = vec![1];
        expected.extend_from_slice(&(values.len() as u64).to_le_bytes());
        expected.extend_from_slice(payload);
        let mut output = vec![0xa5; expected.len() + 4];
        assert_eq!(
            write_flat(values, &mut output[2..2 + expected.len()]),
            expected.len()
        );
        assert_eq!(&output[2..2 + expected.len()], expected);
        assert_eq!(&output[..2], &[0xa5; 2]);
        assert_eq!(&output[2 + expected.len()..], &[0xa5; 2]);
    }

    /// Purpose: Protect shared flat serialization against endian and float-bit changes.
    /// Expected: Signed extrema, unsigned maxima, NaN payloads and negative zero match independent wire bytes without overwriting adjacent data.
    #[test]
    fn flat_primitives_preserve_exact_wire_bits() {
        assert_flat_wire(&[-300i16, i16::MAX], &[0xd4, 0xfe, 0xff, 0x7f]);
        assert_flat_wire(&[u64::MAX], &[0xff; 8]);
        assert_flat_wire(
            &[f32::from_bits(0xffc0_1234), -0.0],
            &[0x34, 0x12, 0xc0, 0xff, 0, 0, 0, 0x80],
        );
        assert_flat_wire(
            &[f64::from_bits(0x7ff8_0000_0000_1234), -0.0],
            &[
                0x34, 0x12, 0, 0, 0, 0, 0xf8, 0x7f, 0, 0, 0, 0, 0, 0, 0, 0x80,
            ],
        );
    }

    /// Purpose: Write planned flat and FOR payloads into a bounded subrange without reselecting their encoding.
    /// Expected: Wire bytes match independent fixtures, decode exactly, and leave surrounding bytes untouched.
    #[test]
    fn integer_plan_writes_exact_wire_extent() {
        for (case, input, min, max, wire) in [
            ("empty flat", vec![], 0i16, 0, vec![0; 8]),
            (
                "singleton flat",
                vec![-300],
                -300,
                -300,
                vec![1, 0, 0, 0, 0, 0, 0, 0, 0xd4, 0xfe],
            ),
            (
                "preselected flat despite constant values",
                vec![7; 5],
                i16::MIN,
                i16::MAX,
                vec![5, 0, 0, 0, 0, 0, 0, 0, 7, 0, 7, 0, 7, 0, 7, 0, 7, 0],
            ),
            (
                "signed FOR with preselected base",
                vec![-1, 0, 1, -1, 0],
                -2,
                1,
                vec![2, 5, 0, 0, 0, 0, 0, 0, 0, 0xfe, 0xff, 0x79, 2],
            ),
        ] {
            let plan = IntegerEncodingPlan::from_extrema(input.len(), min, max);
            assert_eq!(plan.ser_len(), wire.len(), "{case}");
            let mut out = vec![0xa5; 3 + wire.len() + 5];
            let end = plan.ser(&input, &mut out[..], 3);
            assert_eq!(end, 3 + wire.len(), "{case}");
            assert_eq!(&out[..3], &[0xa5; 3], "{case}: prefix");
            assert_eq!(&out[3..end], wire, "{case}: wire");
            assert_eq!(&out[end..], &[0xa5; 5], "{case}: suffix");
            let (end, decoded) = match plan.encoding() {
                IntegerEncoding::Flat => Vec::<i16>::deser(&wire[..], 0).unwrap(),
                IntegerEncoding::ForBitpacking { .. } => {
                    let (end, values) = ForBitpackingDeser::<i16>::deser(&wire[..], 0).unwrap();
                    (end, values.0)
                }
            };
            assert_eq!(end, wire.len(), "{case}: read extent");
            assert_eq!(decoded, input, "{case}: values");
        }
    }

    /// Purpose: Enforce the plan's physical row count for both integer encodings before writing.
    /// Expected: Short and long slices fail the length contract and leave the destination unchanged.
    #[test]
    fn integer_plan_rejects_mismatched_slices_before_writing() {
        for (case, count, encoding) in [
            ("flat", 1, IntegerEncoding::Flat),
            ("FOR", 16, IntegerEncoding::ForBitpacking { bits: 1 }),
        ] {
            let plan = IntegerEncodingPlan::from_extrema(count, 7u64, 7);
            assert_eq!(plan.encoding(), encoding, "{case}");
            for actual in [count - 1, count + 1] {
                let input = vec![7; actual];
                let mut out = vec![0xa5; 256];
                let result = catch_unwind(AssertUnwindSafe(|| plan.ser(&input, &mut out[..], 3)));
                let panic = result.expect_err("slice mismatch must fail its internal contract");
                let message = panic.downcast_ref::<String>().unwrap();
                assert!(
                    message.contains("integer encoding plan slice length mismatch"),
                    "{case}: {message}"
                );
                assert!(
                    out.iter().all(|&byte| byte == 0xa5),
                    "{case}: actual={actual}"
                );
            }
        }
    }
}
