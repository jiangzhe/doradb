use crate::row::ops::UpdateCol;
use crate::value::{Val, ValRef};
#[cfg(test)]
pub(crate) use tests::BufferValues;

/// Repeatable full-row access by physical column ordinal.
///
/// Callers only access positions below `len()`. Length and values must remain
/// stable across validation, sizing, and mutation. Views borrow from this owner
/// and are not retained by consumers. Write inputs must be stored separately
/// from the mutable destination page; successful writes copy their bytes.
/// Writers reject nonempty byte slices overlapping that page with a release assertion.
pub(crate) trait RowValues {
    /// Returns the number of columns.
    fn len(&self) -> usize;

    /// Borrows the value at a physical column ordinal below `len()`.
    fn value(&self, index: usize) -> ValRef<'_>;
}

/// Repeatable sparse-update access in the original input order.
///
/// Callers only access positions below `len()`. Length, values, and target
/// ordinals must remain stable across validation, sizing, and mutation. Views
/// borrow from this owner and are not retained. Implementations must not sort
/// or deduplicate entries: validation checks their order. Write inputs must be
/// stored separately from the mutable destination page, which copies the bytes.
/// Writers reject nonempty byte slices overlapping that page with a release assertion.
pub(crate) trait UpdateValues {
    /// Returns the number of update entries.
    fn len(&self) -> usize;

    /// Borrows an entry and its target column ordinal at a position below `len()`.
    fn value(&self, index: usize) -> (usize, ValRef<'_>);
}

impl RowValues for [Val] {
    #[inline]
    fn len(&self) -> usize {
        <[Val]>::len(self)
    }

    #[inline]
    fn value(&self, index: usize) -> ValRef<'_> {
        self[index].view()
    }
}

impl UpdateValues for [UpdateCol] {
    #[inline]
    fn len(&self) -> usize {
        <[UpdateCol]>::len(self)
    }

    #[inline]
    fn value(&self, index: usize) -> (usize, ValRef<'_>) {
        (self[index].idx, self[index].val.view())
    }
}

#[cfg(test)]
mod tests {
    use super::{RowValues, UpdateValues};
    use crate::value::{ValKind, ValRef};
    use ordered_float::OrderedFloat;
    use std::ops::Range;

    struct Descriptor {
        column: usize,
        kind: Option<ValKind>,
        bytes: Range<usize>,
    }

    /// Test input resolving descriptors against a separate, reusable byte buffer.
    /// Construction and access never materialize `Val` or `MemVar` values.
    pub(crate) struct BufferValues {
        descriptors: Vec<Descriptor>,
        bytes: Vec<u8>,
    }

    impl BufferValues {
        /// Packs test inputs into descriptors and bytes while preserving their order.
        pub(crate) fn new<'a>(values: impl IntoIterator<Item = (usize, ValRef<'a>)>) -> Self {
            let mut result = Self {
                descriptors: Vec::new(),
                bytes: Vec::new(),
            };
            for (column, value) in values {
                let start = result.bytes.len();
                match value {
                    ValRef::Null => {}
                    ValRef::I8(v) => result.bytes.extend(v.to_le_bytes()),
                    ValRef::U8(v) => result.bytes.extend(v.to_le_bytes()),
                    ValRef::I16(v) => result.bytes.extend(v.to_le_bytes()),
                    ValRef::U16(v) => result.bytes.extend(v.to_le_bytes()),
                    ValRef::I32(v) => result.bytes.extend(v.to_le_bytes()),
                    ValRef::U32(v) => result.bytes.extend(v.to_le_bytes()),
                    ValRef::F32(v) => result.bytes.extend(v.to_bits().to_le_bytes()),
                    ValRef::I64(v) => result.bytes.extend(v.to_le_bytes()),
                    ValRef::U64(v) => result.bytes.extend(v.to_le_bytes()),
                    ValRef::F64(v) => result.bytes.extend(v.to_bits().to_le_bytes()),
                    ValRef::VarByte(v) => result.bytes.extend(v),
                }
                result.descriptors.push(Descriptor {
                    column,
                    kind: value.kind(),
                    bytes: start..result.bytes.len(),
                });
            }
            result
        }

        /// Overwrites the payload after consumption to detect escaped byte references.
        pub(crate) fn overwrite_bytes(&mut self) {
            self.bytes.fill(0xa5);
        }
    }

    impl RowValues for BufferValues {
        fn len(&self) -> usize {
            self.descriptors.len()
        }

        fn value(&self, index: usize) -> ValRef<'_> {
            let descriptor = &self.descriptors[index];
            let bytes = &self.bytes[descriptor.bytes.clone()];
            match descriptor.kind {
                None => ValRef::Null,
                Some(ValKind::I8) => ValRef::I8(i8::from_le_bytes(bytes.try_into().unwrap())),
                Some(ValKind::U8) => ValRef::U8(u8::from_le_bytes(bytes.try_into().unwrap())),
                Some(ValKind::I16) => ValRef::I16(i16::from_le_bytes(bytes.try_into().unwrap())),
                Some(ValKind::U16) => ValRef::U16(u16::from_le_bytes(bytes.try_into().unwrap())),
                Some(ValKind::I32) => ValRef::I32(i32::from_le_bytes(bytes.try_into().unwrap())),
                Some(ValKind::U32) => ValRef::U32(u32::from_le_bytes(bytes.try_into().unwrap())),
                Some(ValKind::F32) => ValRef::F32(OrderedFloat(f32::from_bits(
                    u32::from_le_bytes(bytes.try_into().unwrap()),
                ))),
                Some(ValKind::I64) => ValRef::I64(i64::from_le_bytes(bytes.try_into().unwrap())),
                Some(ValKind::U64) => ValRef::U64(u64::from_le_bytes(bytes.try_into().unwrap())),
                Some(ValKind::F64) => ValRef::F64(OrderedFloat(f64::from_bits(
                    u64::from_le_bytes(bytes.try_into().unwrap()),
                ))),
                Some(ValKind::VarByte) => ValRef::VarByte(bytes),
            }
        }
    }

    impl UpdateValues for BufferValues {
        fn len(&self) -> usize {
            self.descriptors.len()
        }

        fn value(&self, index: usize) -> (usize, ValRef<'_>) {
            (
                self.descriptors[index].column,
                RowValues::value(self, index),
            )
        }
    }

    #[test]
    fn test_buffer_values_repeatable_access() {
        let input = [
            (4, ValRef::VarByte(b"independent buffer")),
            (2, ValRef::Null),
        ];
        let values = BufferValues::new(input);
        assert_eq!(RowValues::len(&values), input.len());
        assert_eq!(UpdateValues::len(&values), input.len());
        for _ in 0..3 {
            for (index, expected) in input.iter().enumerate() {
                assert_eq!(UpdateValues::value(&values, index), *expected);
                assert_eq!(RowValues::value(&values, index), expected.1);
            }
        }
        let ValRef::VarByte(bytes) = RowValues::value(&values, 0) else {
            panic!("expected bytes")
        };
        assert_eq!(bytes.as_ptr(), values.bytes.as_ptr());
    }
}
