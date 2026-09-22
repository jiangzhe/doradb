use error_stack::Report;
use std::any::type_name;
use std::mem;
use std::result::Result as StdResult;
use thiserror::Error as ThisError;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Caller-neutral failures to view bytes through a zerocopy layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ThisError)]
pub(crate) enum LayoutError {
    /// The byte length or alignment does not match the requested type.
    #[error("byte layout mismatch")]
    Mismatch,
}

/// Result carrying a caller-neutral byte-layout report.
pub(crate) type LayoutResult<T> = StdResult<T, Report<LayoutError>>;

/// Returns the byte representation of a zerocopy value.
#[inline]
pub(crate) fn bytes_of<T>(value: &T) -> &[u8]
where
    T: IntoBytes + Immutable + ?Sized,
{
    value.as_bytes()
}

/// Views an exact byte slice as one typed zerocopy value.
#[inline]
pub(crate) fn try_ref_from_bytes<T>(bytes: &[u8]) -> LayoutResult<&T>
where
    T: FromBytes + KnownLayout + Immutable,
{
    T::ref_from_bytes(bytes).map_err(|_| {
        Report::new(LayoutError::Mismatch).attach(format!(
            "invalid byte layout for {}: len={}",
            type_name::<T>(),
            bytes.len()
        ))
    })
}

/// Views an exact mutable byte slice as one typed zerocopy value.
#[inline]
pub(crate) fn try_mut_from_bytes<T>(bytes: &mut [u8]) -> LayoutResult<&mut T>
where
    T: FromBytes + IntoBytes + KnownLayout,
{
    let len = bytes.len();
    T::mut_from_bytes(bytes).map_err(|_| {
        Report::new(LayoutError::Mismatch).attach(format!(
            "invalid mutable byte layout for {}: len={len}",
            type_name::<T>()
        ))
    })
}

/// Views an exact byte slice as a typed zerocopy slice.
#[inline]
pub(crate) fn try_slice_from_bytes<T>(bytes: &[u8]) -> LayoutResult<&[T]>
where
    [T]: FromBytes + KnownLayout<PointerMetadata = usize> + Immutable,
{
    let elem_len = mem::size_of::<T>();
    if elem_len == 0 || !bytes.len().is_multiple_of(elem_len) {
        return Err(Report::new(LayoutError::Mismatch).attach(format!(
            "invalid byte length {} for slice of {}",
            bytes.len(),
            type_name::<T>()
        )));
    }
    let count = bytes.len() / elem_len;
    <[T]>::ref_from_bytes_with_elems(bytes, count).map_err(|_| {
        Report::new(LayoutError::Mismatch).attach(format!(
            "invalid byte layout for slice of {}: len={}",
            type_name::<T>(),
            bytes.len()
        ))
    })
}

/// Views an exact mutable byte slice as a typed zerocopy slice.
#[inline]
pub(crate) fn try_slice_from_bytes_mut<T>(bytes: &mut [u8]) -> LayoutResult<&mut [T]>
where
    [T]: FromBytes + IntoBytes + KnownLayout<PointerMetadata = usize> + Immutable,
{
    let elem_len = mem::size_of::<T>();
    if elem_len == 0 || !bytes.len().is_multiple_of(elem_len) {
        return Err(Report::new(LayoutError::Mismatch).attach(format!(
            "invalid mutable byte length {} for slice of {}",
            bytes.len(),
            type_name::<T>()
        )));
    }
    let len = bytes.len();
    let count = bytes.len() / elem_len;
    <[T]>::mut_from_bytes_with_elems(bytes, count).map_err(|_| {
        Report::new(LayoutError::Mismatch).attach(format!(
            "invalid mutable byte layout for slice of {}: len={}",
            type_name::<T>(),
            len
        ))
    })
}

/// Views trusted exact bytes as a typed zerocopy value.
#[inline]
pub(crate) fn ref_from_bytes<T>(bytes: &[u8]) -> &T
where
    T: FromBytes + KnownLayout + Immutable,
{
    try_ref_from_bytes(bytes).expect("trusted bytes must match the requested zerocopy layout")
}

/// Views trusted exact mutable bytes as one typed zerocopy value.
///
/// # Panics
///
/// Panics when `bytes` does not have the exact size and alignment required by
/// `T`. Callers must establish that layout before choosing this asserting
/// helper over [`try_mut_from_bytes`].
#[inline]
pub(crate) fn mut_from_bytes<T>(bytes: &mut [u8]) -> &mut T
where
    T: FromBytes + IntoBytes + KnownLayout,
{
    try_mut_from_bytes(bytes)
        .expect("trusted bytes must match the requested mutable zerocopy layout")
}

/// Views trusted exact bytes as a typed zerocopy slice.
#[inline]
pub(crate) fn slice_from_bytes<T>(bytes: &[u8]) -> &[T]
where
    [T]: FromBytes + KnownLayout<PointerMetadata = usize> + Immutable,
{
    try_slice_from_bytes(bytes).expect("trusted bytes must match the requested zerocopy slice")
}

/// Views trusted exact mutable bytes as a typed zerocopy slice.
#[inline]
pub(crate) fn slice_from_bytes_mut<T>(bytes: &mut [u8]) -> &mut [T]
where
    [T]: FromBytes + IntoBytes + KnownLayout<PointerMetadata = usize> + Immutable,
{
    try_slice_from_bytes_mut(bytes)
        .expect("trusted bytes must match the requested mutable zerocopy slice")
}

#[cfg(test)]
mod tests {
    use super::*;
    use zerocopy::byteorder::little_endian::U32 as LeU32;

    /// Purpose: Protect conversion between a scalar and its borrowed byte view.
    /// Expected: The specified byte order and scalar value are preserved.
    #[test]
    fn test_bytes_of_and_ref_from_bytes_roundtrip() {
        let value = LeU32::new(0x0102_0304);
        let bytes = bytes_of(&value);

        assert_eq!(bytes, &[0x04, 0x03, 0x02, 0x01]);
        assert_eq!(ref_from_bytes::<LeU32>(bytes).get(), 0x0102_0304);
    }

    /// Purpose: Protect scalar mutation through a typed byte view.
    /// Expected: The backing bytes reflect the new value in the specified byte order.
    #[test]
    fn test_mut_from_bytes_updates_bytes() {
        let mut bytes = [0u8; mem::size_of::<LeU32>()];

        mut_from_bytes::<LeU32>(&mut bytes).set(0x1122_3344);

        assert_eq!(bytes, [0x44, 0x33, 0x22, 0x11]);
    }

    /// Purpose: Protect mutable scalar views from incompatible byte lengths.
    /// Expected: Any nonexact scalar length returns a layout mismatch.
    #[test]
    fn test_try_mut_from_bytes_rejects_wrong_len() {
        for len in [0, 3, 5] {
            let mut bytes = vec![0xa5; len];
            let error = try_mut_from_bytes::<LeU32>(&mut bytes).unwrap_err();
            assert_eq!(*error.current_context(), LayoutError::Mismatch, "len={len}");
            assert_eq!(
                bytes,
                vec![0xa5; len],
                "failed view changed bytes: len={len}"
            );
        }
    }

    /// Purpose: Protect typed slice views over complete encoded elements.
    /// Expected: Element count, order, and values match the input bytes.
    #[test]
    fn test_slice_from_bytes_roundtrip() {
        let bytes = [1u8, 0, 0, 0, 2, 0, 0, 0];
        let values = slice_from_bytes::<LeU32>(&bytes);

        assert_eq!(values.len(), 2);
        assert_eq!(values[0].get(), 1);
        assert_eq!(values[1].get(), 2);
    }

    /// Purpose: Protect mutation through a typed slice view.
    /// Expected: Each element update reaches the corresponding backing bytes.
    #[test]
    fn test_slice_from_bytes_mut_updates_bytes() {
        let mut bytes = [0u8; 2 * mem::size_of::<LeU32>()];
        let values = slice_from_bytes_mut::<LeU32>(&mut bytes);

        values[0].set(7);
        values[1].set(9);

        assert_eq!(bytes, [7, 0, 0, 0, 9, 0, 0, 0]);
    }

    /// Purpose: Protect immutable scalar views from incompatible byte lengths.
    /// Expected: Any nonexact scalar length returns a layout mismatch.
    #[test]
    fn test_try_ref_from_bytes_rejects_wrong_len() {
        for len in [0, 3, 5] {
            let bytes = vec![0xa5; len];
            let error = try_ref_from_bytes::<LeU32>(&bytes).unwrap_err();
            assert_eq!(*error.current_context(), LayoutError::Mismatch, "len={len}");
        }
    }

    /// Purpose: Protect immutable slice views from trailing partial elements.
    /// Expected: A nonintegral element count returns a layout mismatch.
    #[test]
    fn test_try_slice_from_bytes_rejects_partial_element() {
        for len in [1, 3, 5, 7] {
            let bytes = vec![0xa5; len];
            let error = try_slice_from_bytes::<LeU32>(&bytes).unwrap_err();
            assert_eq!(*error.current_context(), LayoutError::Mismatch, "len={len}");
        }
    }

    /// Purpose: Protect mutable slice views from trailing partial elements.
    /// Expected: A nonintegral element count returns a layout mismatch.
    #[test]
    fn test_try_slice_from_bytes_mut_rejects_partial_element() {
        for len in [1, 3, 5, 7] {
            let mut bytes = vec![0xa5; len];
            let error = try_slice_from_bytes_mut::<LeU32>(&mut bytes).unwrap_err();
            assert_eq!(*error.current_context(), LayoutError::Mismatch, "len={len}");
            assert_eq!(
                bytes,
                vec![0xa5; len],
                "failed view changed bytes: len={len}"
            );
        }
    }
}
