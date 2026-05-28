/// Implements common boilerplate for `u64`-backed identifier newtypes.
macro_rules! impl_id {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident;
        methods $method_vis:vis
    ) => {
        $(#[$meta])*
        #[repr(transparent)]
        #[derive(
            Debug,
            Default,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            zerocopy_derive::FromBytes,
            zerocopy_derive::IntoBytes,
            zerocopy_derive::KnownLayout,
            zerocopy_derive::Immutable,
        )]
        $vis struct $name(u64);

        impl $name {
            /// Creates an identifier from its raw `u64` representation.
            #[inline]
            $method_vis const fn new(raw: u64) -> Self {
                Self(raw)
            }

            /// Returns the raw `u64` representation.
            #[inline]
            $method_vis const fn as_u64(self) -> u64 {
                self.0
            }

            /// Returns the identifier as `usize`.
            #[inline]
            $method_vis const fn as_usize(self) -> usize {
                self.0 as usize
            }
        }

        impl From<u64> for $name {
            #[inline]
            fn from(value: u64) -> Self {
                Self(value)
            }
        }

        impl From<u32> for $name {
            #[inline]
            fn from(value: u32) -> Self {
                Self(value as u64)
            }
        }

        impl From<usize> for $name {
            #[inline]
            fn from(value: usize) -> Self {
                Self(value as u64)
            }
        }

        impl From<$name> for u64 {
            #[inline]
            fn from(value: $name) -> Self {
                value.as_u64()
            }
        }

        impl From<$name> for usize {
            #[inline]
            fn from(value: $name) -> Self {
                value.as_usize()
            }
        }

        impl ::std::fmt::Display for $name {
            #[inline]
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

macro_rules! impl_id_serde {
    ($name:ident) => {
        impl crate::serde::Ser<'_> for $name {
            #[inline]
            fn ser_len(&self) -> usize {
                ::std::mem::size_of::<u64>()
            }

            #[inline]
            fn ser<S: crate::serde::Serde + ?Sized>(&self, out: &mut S, start_idx: usize) -> usize {
                out.ser_u64(start_idx, self.as_u64())
            }
        }

        impl crate::serde::Deser for $name {
            #[inline]
            fn deser<S: crate::serde::Serde + ?Sized>(
                input: &S,
                start_idx: usize,
            ) -> crate::error::Result<(usize, Self)> {
                input
                    .deser_u64(start_idx)
                    .map(|(idx, raw)| (idx, Self::new(raw)))
            }
        }
    };
}

macro_rules! impl_id_bitpackable {
    ($name:ident) => {
        impl crate::compression::BitPackable for $name {
            const ZERO: Self = Self::new(0);

            #[inline]
            fn sub_to_u64(self, min: Self) -> u64 {
                self.as_u64().wrapping_sub(min.as_u64())
            }

            #[inline]
            fn sub_to_u32(self, min: Self) -> u32 {
                self.as_u64().wrapping_sub(min.as_u64()) as u32
            }

            #[inline]
            fn add_from_u32(self, delta: u32) -> Self {
                Self::new(self.as_u64().wrapping_add(delta as u64))
            }

            #[inline]
            fn sub_to_u16(self, min: Self) -> u16 {
                self.as_u64().wrapping_sub(min.as_u64()) as u16
            }

            #[inline]
            fn add_from_u16(self, delta: u16) -> Self {
                Self::new(self.as_u64().wrapping_add(delta as u64))
            }

            #[inline]
            fn sub_to_u8(self, min: Self) -> u8 {
                self.as_u64().wrapping_sub(min.as_u64()) as u8
            }

            #[inline]
            fn add_from_u8(self, delta: u8) -> Self {
                Self::new(self.as_u64().wrapping_add(delta as u64))
            }
        }
    };
}

use crate::value::Val;
use std::mem;
use std::ops::{Add, AddAssign, Sub, SubAssign};

impl_id! {
    /// Stable logical row identity within table data.
    pub struct RowID;
    methods pub
}

impl RowID {
    /// Largest representable raw row identifier.
    pub const MAX: Self = Self::new(u64::MAX);

    /// Adds a row-id delta, returning `None` on overflow.
    #[inline]
    pub fn checked_add(self, delta: u64) -> Option<Self> {
        self.as_u64().checked_add(delta).map(Self::new)
    }

    /// Subtracts a row identifier, returning `None` on underflow.
    #[inline]
    pub fn checked_sub(self, rhs: RowID) -> Option<u64> {
        self.as_u64().checked_sub(rhs.as_u64())
    }

    /// Adds a row-id delta, saturating on overflow.
    #[inline]
    pub fn saturating_add(self, delta: u64) -> Self {
        Self::new(self.as_u64().saturating_add(delta))
    }

    /// Returns the little-endian byte representation.
    #[inline]
    pub const fn to_le_bytes(self) -> [u8; mem::size_of::<u64>()] {
        self.as_u64().to_le_bytes()
    }
}

impl Add<u64> for RowID {
    type Output = Self;

    #[inline]
    fn add(self, rhs: u64) -> Self::Output {
        Self::new(self.as_u64() + rhs)
    }
}

impl Sub<RowID> for RowID {
    type Output = u64;

    #[inline]
    fn sub(self, rhs: RowID) -> Self::Output {
        self.as_u64() - rhs.as_u64()
    }
}

impl From<RowID> for Val {
    #[inline]
    fn from(value: RowID) -> Self {
        Val::from(value.as_u64())
    }
}

impl_id_serde!(RowID);
impl_id_bitpackable!(RowID);

impl_id! {
    /// Stable logical table identity and deterministic user table file identity.
    pub struct TableID;
    methods pub
}

impl TableID {
    /// Parses a table identifier from a string in the given radix.
    #[inline]
    pub fn from_str_radix(
        src: &str,
        radix: u32,
    ) -> std::result::Result<Self, std::num::ParseIntError> {
        u64::from_str_radix(src, radix).map(Self::new)
    }

    /// Adds a table-id delta, returning `None` on overflow.
    #[inline]
    pub fn checked_add(self, delta: u64) -> Option<Self> {
        self.as_u64().checked_add(delta).map(Self::new)
    }

    /// Adds a table-id delta, saturating on overflow.
    #[inline]
    pub fn saturating_add(self, delta: u64) -> Self {
        Self::new(self.as_u64().saturating_add(delta))
    }
}

impl Add<u64> for TableID {
    type Output = Self;

    #[inline]
    fn add(self, rhs: u64) -> Self::Output {
        Self::new(self.as_u64() + rhs)
    }
}

impl std::fmt::LowerHex for TableID {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::LowerHex::fmt(&self.as_u64(), f)
    }
}

impl From<TableID> for Val {
    #[inline]
    fn from(value: TableID) -> Self {
        Val::from(value.as_u64())
    }
}

impl_id_serde!(TableID);

impl_id! {
    /// Transaction timestamp and active transaction identity.
    pub struct TrxID;
    methods pub
}

impl TrxID {
    /// Creates a transaction timestamp from little-endian bytes.
    #[inline]
    pub const fn from_le_bytes(bytes: [u8; mem::size_of::<u64>()]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }

    /// Adds a transaction timestamp delta, returning `None` on overflow.
    #[inline]
    pub fn checked_add(self, delta: u64) -> Option<Self> {
        self.as_u64().checked_add(delta).map(Self::new)
    }

    /// Adds a transaction timestamp delta, saturating on overflow.
    #[inline]
    pub fn saturating_add(self, delta: u64) -> Self {
        Self::new(self.as_u64().saturating_add(delta))
    }

    /// Subtracts a transaction timestamp delta, saturating on underflow.
    #[inline]
    pub fn saturating_sub(self, delta: u64) -> Self {
        Self::new(self.as_u64().saturating_sub(delta))
    }

    /// Returns the little-endian byte representation.
    #[inline]
    pub const fn to_le_bytes(self) -> [u8; mem::size_of::<u64>()] {
        self.as_u64().to_le_bytes()
    }
}

impl Add<u64> for TrxID {
    type Output = Self;

    #[inline]
    fn add(self, rhs: u64) -> Self::Output {
        Self::new(self.as_u64() + rhs)
    }
}

impl_id_serde!(TrxID);

impl_id! {
    /// Engine-local session identity.
    pub struct SessionID;
    methods pub
}

impl_id! {
    /// Runtime buffer-managed page identity.
    ///
    /// This id is reserved for mutable or cached pages owned by the buffer layer.
    /// Persisted fixed-size file units use [`BlockID`] instead.
    pub struct PageID;
    methods pub
}

impl PageID {
    #[inline]
    pub const fn from_le_bytes(bytes: [u8; mem::size_of::<u64>()]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }

    /// Returns the little-endian byte representation.
    #[inline]
    pub const fn to_le_bytes(self) -> [u8; mem::size_of::<u64>()] {
        self.as_u64().to_le_bytes()
    }
}

impl Add<u64> for PageID {
    type Output = Self;

    #[inline]
    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl AddAssign<u64> for PageID {
    #[inline]
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl Sub<u64> for PageID {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: u64) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl SubAssign<u64> for PageID {
    #[inline]
    fn sub_assign(&mut self, rhs: u64) {
        self.0 -= rhs;
    }
}

impl Sub<PageID> for u64 {
    type Output = PageID;

    #[inline]
    fn sub(self, rhs: PageID) -> Self::Output {
        PageID(self - rhs.0)
    }
}

impl PartialEq<u64> for PageID {
    #[inline]
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialEq<i32> for PageID {
    #[inline]
    fn eq(&self, other: &i32) -> bool {
        *other >= 0 && self.0 == *other as u64
    }
}

impl PartialEq<PageID> for u64 {
    #[inline]
    fn eq(&self, other: &PageID) -> bool {
        *self == other.0
    }
}

impl PartialEq<PageID> for i32 {
    #[inline]
    fn eq(&self, other: &PageID) -> bool {
        *self >= 0 && *self as u64 == other.0
    }
}

impl_id_serde!(PageID);
impl_id_bitpackable!(PageID);

impl_id! {
    /// Physical file identity used by persisted-block mappings and shared-storage routing.
    pub(crate) struct FileID;
    methods pub(crate)
}

impl FileID {
    pub(crate) const MAX: Self = Self(u64::MAX);
}

impl From<TableID> for FileID {
    #[inline]
    fn from(value: TableID) -> Self {
        Self::new(value.as_u64())
    }
}

impl PartialEq<u64> for FileID {
    #[inline]
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialEq<i32> for FileID {
    #[inline]
    fn eq(&self, other: &i32) -> bool {
        *other >= 0 && self.0 == *other as u64
    }
}

impl PartialEq<FileID> for u64 {
    #[inline]
    fn eq(&self, other: &FileID) -> bool {
        *self == other.0
    }
}

impl PartialEq<FileID> for i32 {
    #[inline]
    fn eq(&self, other: &FileID) -> bool {
        *self >= 0 && *self as u64 == other.0
    }
}

impl_id_serde!(FileID);
impl_id_bitpackable!(FileID);

impl_id! {
    /// Persisted fixed-size file block identity.
    ///
    /// This id is reserved for physical blocks stored in copy-on-write files and
    /// readonly-cache lookups. Runtime buffer-managed pages use
    /// `crate::id::PageID` instead.
    pub(crate) struct BlockID;
    methods pub(crate)
}

impl Add<u64> for BlockID {
    type Output = Self;

    #[inline]
    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl BlockID {
    /// Returns the little-endian byte representation.
    #[inline]
    pub(crate) const fn to_le_bytes(self) -> [u8; mem::size_of::<u64>()] {
        self.as_u64().to_le_bytes()
    }
}

impl AddAssign<u64> for BlockID {
    #[inline]
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl Sub<u64> for BlockID {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: u64) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl SubAssign<u64> for BlockID {
    #[inline]
    fn sub_assign(&mut self, rhs: u64) {
        self.0 -= rhs;
    }
}

impl PartialEq<i32> for BlockID {
    #[inline]
    fn eq(&self, other: &i32) -> bool {
        *other >= 0 && self.0 == *other as u64
    }
}

impl PartialEq<BlockID> for i32 {
    #[inline]
    fn eq(&self, other: &BlockID) -> bool {
        *self >= 0 && *self as u64 == other.0
    }
}

impl_id_serde!(BlockID);
impl_id_bitpackable!(BlockID);
