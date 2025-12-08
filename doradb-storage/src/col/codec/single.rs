use crate::col::repr::ByteRepr;
use smallvec::{SmallVec, smallvec};

/// Single codec encodes single value as bytes in case all
/// values of that attribute are identical.
#[derive(Debug, Clone)]
pub struct Single {
    pub data: SmallVec<[u8; 16]>,
    // pub valid: bool,
    pub len: u16,
}

#[allow(clippy::len_without_is_empty)]
impl Single {
    /// View value as given type.
    #[inline]
    pub fn view<T: ByteRepr>(&self) -> T {
        T::from_bytes(&self.data)
    }

    /// Create single codec with null value.
    #[inline]
    pub fn new_null(len: u16) -> Self {
        Single {
            data: smallvec![],
            len,
        }
    }

    /// Create single codec with given value.
    #[inline]
    pub fn new<T: ByteRepr>(val: T, len: u16) -> Self {
        Single {
            data: val.to_bytes(),
            len,
        }
    }

    /// Create single codec with bool value.
    #[inline]
    pub fn new_bool(val: bool, len: u16) -> Self {
        Single {
            data: smallvec![u8::from(val)],
            len,
        }
    }

    /// View value as bool.
    #[inline]
    pub fn view_bool(&self) -> bool {
        self.data[0] != 0u8
    }

    /// Create single codec from raw bytes.
    #[inline]
    pub fn new_raw(data: SmallVec<[u8; 16]>, len: u16) -> Self {
        Single {
            // valid: true,
            data,
            len,
        }
    }
}
