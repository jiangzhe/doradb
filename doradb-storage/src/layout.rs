use crate::error::{DataIntegrityError, Error, Result};
use error_stack::Report;
use std::mem;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[inline]
fn invalid_layout(message: impl Into<String>) -> Error {
    Report::new(DataIntegrityError::InvalidPayload)
        .attach(message.into())
        .into()
}

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
pub(crate) fn try_ref_from_bytes<T>(bytes: &[u8]) -> Result<&T>
where
    T: FromBytes + KnownLayout + Immutable,
{
    T::ref_from_bytes(bytes).map_err(|_| {
        invalid_layout(format!(
            "invalid byte layout for {}: len={}",
            std::any::type_name::<T>(),
            bytes.len()
        ))
    })
}

/// Views an exact mutable byte slice as one typed zerocopy value.
#[inline]
pub(crate) fn try_mut_from_bytes<T>(bytes: &mut [u8]) -> Result<&mut T>
where
    T: FromBytes + IntoBytes + KnownLayout,
{
    let len = bytes.len();
    T::mut_from_bytes(bytes).map_err(|_| {
        invalid_layout(format!(
            "invalid mutable byte layout for {}: len={len}",
            std::any::type_name::<T>()
        ))
    })
}

/// Views an exact byte slice as a typed zerocopy slice.
#[inline]
pub(crate) fn try_slice_from_bytes<T>(bytes: &[u8]) -> Result<&[T]>
where
    [T]: FromBytes + KnownLayout<PointerMetadata = usize> + Immutable,
{
    let elem_len = mem::size_of::<T>();
    if elem_len == 0 || !bytes.len().is_multiple_of(elem_len) {
        return Err(invalid_layout(format!(
            "invalid byte length {} for slice of {}",
            bytes.len(),
            std::any::type_name::<T>()
        )));
    }
    let count = bytes.len() / elem_len;
    <[T]>::ref_from_bytes_with_elems(bytes, count).map_err(|_| {
        invalid_layout(format!(
            "invalid byte layout for slice of {}: len={}",
            std::any::type_name::<T>(),
            bytes.len()
        ))
    })
}

/// Views an exact mutable byte slice as a typed zerocopy slice.
#[inline]
pub(crate) fn try_slice_from_bytes_mut<T>(bytes: &mut [u8]) -> Result<&mut [T]>
where
    [T]: FromBytes + IntoBytes + KnownLayout<PointerMetadata = usize> + Immutable,
{
    let elem_len = mem::size_of::<T>();
    if elem_len == 0 || !bytes.len().is_multiple_of(elem_len) {
        return Err(invalid_layout(format!(
            "invalid mutable byte length {} for slice of {}",
            bytes.len(),
            std::any::type_name::<T>()
        )));
    }
    let len = bytes.len();
    let count = bytes.len() / elem_len;
    <[T]>::mut_from_bytes_with_elems(bytes, count).map_err(|_| {
        invalid_layout(format!(
            "invalid mutable byte layout for slice of {}: len={}",
            std::any::type_name::<T>(),
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

    #[test]
    fn test_bytes_of_and_ref_from_bytes_roundtrip() {
        let value = LeU32::new(0x0102_0304);
        let bytes = bytes_of(&value);

        assert_eq!(bytes, &[0x04, 0x03, 0x02, 0x01]);
        assert_eq!(ref_from_bytes::<LeU32>(bytes).get(), 0x0102_0304);
    }

    #[test]
    fn test_try_mut_from_bytes_updates_bytes() {
        let mut bytes = [0u8; mem::size_of::<LeU32>()];

        try_mut_from_bytes::<LeU32>(&mut bytes)
            .unwrap()
            .set(0x1122_3344);

        assert_eq!(bytes, [0x44, 0x33, 0x22, 0x11]);
    }

    #[test]
    fn test_slice_from_bytes_roundtrip() {
        let bytes = [1u8, 0, 0, 0, 2, 0, 0, 0];
        let values = slice_from_bytes::<LeU32>(&bytes);

        assert_eq!(values.len(), 2);
        assert_eq!(values[0].get(), 1);
        assert_eq!(values[1].get(), 2);
    }

    #[test]
    fn test_slice_from_bytes_mut_updates_bytes() {
        let mut bytes = [0u8; 2 * mem::size_of::<LeU32>()];
        let values = slice_from_bytes_mut::<LeU32>(&mut bytes);

        values[0].set(7);
        values[1].set(9);

        assert_eq!(bytes, [7, 0, 0, 0, 9, 0, 0, 0]);
    }

    #[test]
    fn test_try_ref_from_bytes_rejects_wrong_len() {
        let err = try_ref_from_bytes::<LeU32>(&[0u8; 3]).unwrap_err();

        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidPayload)
        );
    }

    #[test]
    fn test_try_slice_from_bytes_rejects_partial_element() {
        let err = try_slice_from_bytes::<LeU32>(&[0u8; 5]).unwrap_err();

        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidPayload)
        );
    }

    #[test]
    fn test_try_slice_from_bytes_mut_rejects_partial_element() {
        let mut bytes = [0u8; 5];
        let err = try_slice_from_bytes_mut::<LeU32>(&mut bytes).unwrap_err();

        assert_eq!(
            err.data_integrity_error(),
            Some(DataIntegrityError::InvalidPayload)
        );
    }
}
