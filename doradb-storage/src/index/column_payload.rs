use crate::buffer::page::PageID;
use crate::error::{Error, Result};
use crate::index::column_deletion_blob::COLUMN_DELETION_BLOB_PAGE_BODY_SIZE;
use crate::row::RowID;
use bytemuck::{Pod, Zeroable, cast_slice, cast_slice_mut};
use std::mem;

const DELETION_FIELD_SIZE: usize = 120;
const DELETION_HEADER_SIZE: usize = 2;
const DELETION_OFFLOAD_REF_OFFSET: usize = DELETION_HEADER_SIZE;
const DELETION_U16_OFFSET: usize = DELETION_HEADER_SIZE;
const DELETION_U32_OFFSET: usize = 4;
const DELETION_U16_CAPACITY: usize = (DELETION_FIELD_SIZE - DELETION_U16_OFFSET) / 2;
const DELETION_U32_CAPACITY: usize = (DELETION_FIELD_SIZE - DELETION_U32_OFFSET) / 4;
const DELETION_FLAG_U32: u8 = 0b1000_0000;
const DELETION_FLAG_OFFLOADED: u8 = 0b0100_0000;

const _: () = assert!(mem::size_of::<ColumnPagePayload>() == 128);

#[repr(C)]
/// Leaf payload that points to one persisted lightweight columnar page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnPagePayload {
    pub block_id: u64,
    pub deletion_field: [u8; DELETION_FIELD_SIZE],
}

// SAFETY: `ColumnPagePayload` is `repr(C)` and contains only plain-data fields.
unsafe impl Zeroable for ColumnPagePayload {}
// SAFETY: `ColumnPagePayload` has no padding-sensitive or pointer/reference fields.
unsafe impl Pod for ColumnPagePayload {}

impl ColumnPagePayload {
    /// Returns deletion-list view backed by the payload bytes.
    #[inline]
    pub fn deletion_list(&mut self) -> DeletionList<'_> {
        DeletionList::new(&mut self.deletion_field)
    }

    /// Returns decoded offloaded bitmap reference if this payload is offloaded.
    #[inline]
    pub fn offloaded_ref(&self) -> Option<BlobRef> {
        self.try_offloaded_ref().ok().flatten()
    }

    /// Sets offloaded bitmap reference.
    #[inline]
    pub fn set_offloaded_ref(&mut self, blob_ref: BlobRef) {
        self.deletion_field[0] &= !DELETION_FLAG_U32;
        self.deletion_field[0] |= DELETION_FLAG_OFFLOADED;
        self.deletion_field[1] = 0;
        self.deletion_field[DELETION_OFFLOAD_REF_OFFSET..DELETION_OFFLOAD_REF_OFFSET + 8]
            .copy_from_slice(&blob_ref.start_page_id.to_le_bytes());
        self.deletion_field[DELETION_OFFLOAD_REF_OFFSET + 8..DELETION_OFFLOAD_REF_OFFSET + 10]
            .copy_from_slice(&blob_ref.start_offset.to_le_bytes());
        self.deletion_field[DELETION_OFFLOAD_REF_OFFSET + 10..DELETION_OFFLOAD_REF_OFFSET + 14]
            .copy_from_slice(&blob_ref.byte_len.to_le_bytes());
    }

    /// Clears inline deletion bytes and stores only offloaded bitmap reference.
    #[inline]
    pub fn clear_inline_and_set_offloaded(&mut self, blob_ref: BlobRef) {
        self.deletion_field.fill(0);
        self.set_offloaded_ref(blob_ref);
    }

    #[inline]
    pub(crate) fn try_offloaded_ref(&self) -> Result<Option<BlobRef>> {
        if (self.deletion_field[0] & DELETION_FLAG_OFFLOADED) == 0 {
            return Ok(None);
        }
        let start_page_id = u64::from_le_bytes(
            self.deletion_field[DELETION_OFFLOAD_REF_OFFSET..DELETION_OFFLOAD_REF_OFFSET + 8]
                .try_into()?,
        );
        let start_offset = u16::from_le_bytes(
            self.deletion_field[DELETION_OFFLOAD_REF_OFFSET + 8..DELETION_OFFLOAD_REF_OFFSET + 10]
                .try_into()?,
        );
        let byte_len = u32::from_le_bytes(
            self.deletion_field[DELETION_OFFLOAD_REF_OFFSET + 10..DELETION_OFFLOAD_REF_OFFSET + 14]
                .try_into()?,
        );
        if start_page_id == 0
            || byte_len == 0
            || (start_offset as usize) >= COLUMN_DELETION_BLOB_PAGE_BODY_SIZE
        {
            return Err(Error::InvalidFormat);
        }
        Ok(Some(BlobRef {
            start_page_id,
            start_offset,
            byte_len,
        }))
    }
}

/// Reference to one offloaded bitmap byte range in linked immutable blob pages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlobRef {
    pub start_page_id: PageID,
    pub start_offset: u16,
    pub byte_len: u32,
}

/// Error returned by in-payload deletion-list operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeletionListError {
    Full,
}

/// Compact sorted deletion-delta list stored inside `ColumnPagePayload`.
pub struct DeletionList<'a> {
    data: &'a mut [u8; DELETION_FIELD_SIZE],
}

impl<'a> DeletionList<'a> {
    /// Creates a deletion-list view over the 120-byte payload field.
    #[inline]
    pub fn new(data: &'a mut [u8; DELETION_FIELD_SIZE]) -> Self {
        DeletionList { data }
    }

    #[allow(clippy::len_without_is_empty)]
    /// Returns number of stored deletion deltas.
    #[inline]
    pub fn len(&self) -> usize {
        self.data[1] as usize
    }

    /// Returns whether deletion rows are offloaded outside payload.
    #[inline]
    pub fn is_offloaded(&self) -> bool {
        (self.data[0] & DELETION_FLAG_OFFLOADED) != 0
    }

    /// Sets or clears offloaded flag.
    #[inline]
    pub fn set_offloaded(&mut self, offloaded: bool) {
        if offloaded {
            self.data[0] |= DELETION_FLAG_OFFLOADED;
        } else {
            self.data[0] &= !DELETION_FLAG_OFFLOADED;
        }
    }

    /// Returns whether `delta` exists in the sorted list.
    #[inline]
    pub fn contains(&self, delta: u32) -> bool {
        if self.is_format_u32() {
            self.deltas_u32().binary_search(&delta).is_ok()
        } else if delta > u16::MAX as u32 {
            false
        } else {
            let delta = delta as u16;
            self.deltas_u16().binary_search(&delta).is_ok()
        }
    }

    /// Inserts one deletion delta.
    ///
    /// Returns:
    /// - `Ok(true)` if inserted
    /// - `Ok(false)` if already exists
    /// - `Err(Full)` if in-payload capacity is exhausted
    pub fn add(&mut self, delta: u32) -> std::result::Result<bool, DeletionListError> {
        if !self.is_format_u32() && delta > u16::MAX as u32 {
            self.promote_to_u32()?;
        }

        if self.is_format_u32() {
            let count = self.len();
            let deltas = self.deltas_u32_mut();
            match deltas[..count].binary_search(&delta) {
                Ok(_) => Ok(false),
                Err(idx) => {
                    if count >= DELETION_U32_CAPACITY {
                        return Err(DeletionListError::Full);
                    }
                    if idx < count {
                        deltas.copy_within(idx..count, idx + 1);
                    }
                    deltas[idx] = delta;
                    self.set_count(count + 1);
                    Ok(true)
                }
            }
        } else {
            let count = self.len();
            let delta = delta as u16;
            let deltas = self.deltas_u16_mut();
            match deltas[..count].binary_search(&delta) {
                Ok(_) => Ok(false),
                Err(idx) => {
                    if count >= DELETION_U16_CAPACITY {
                        return Err(DeletionListError::Full);
                    }
                    if idx < count {
                        deltas.copy_within(idx..count, idx + 1);
                    }
                    deltas[idx] = delta;
                    self.set_count(count + 1);
                    Ok(true)
                }
            }
        }
    }

    /// Iterates deletion deltas as row-id offsets.
    #[inline]
    pub fn iter(&self) -> DeletionListIter<'_> {
        if self.is_format_u32() {
            DeletionListIter::U32 {
                data: self.deltas_u32(),
                index: 0,
            }
        } else {
            DeletionListIter::U16 {
                data: self.deltas_u16(),
                index: 0,
            }
        }
    }

    /// Iterates absolute row ids using `start_row_id + delta`.
    #[inline]
    pub fn iter_row_ids(&self, start_row_id: RowID) -> impl Iterator<Item = RowID> + '_ {
        self.iter().map(move |delta| start_row_id + delta as RowID)
    }

    #[inline]
    fn set_count(&mut self, count: usize) {
        self.data[1] = count as u8;
    }

    #[inline]
    fn is_format_u32(&self) -> bool {
        (self.data[0] & DELETION_FLAG_U32) != 0
    }

    #[inline]
    fn set_format_u32(&mut self, enabled: bool) {
        if enabled {
            self.data[0] |= DELETION_FLAG_U32;
        } else {
            self.data[0] &= !DELETION_FLAG_U32;
        }
    }

    #[inline]
    fn deltas_u16(&self) -> &[u16] {
        let count = self.len();
        debug_assert!(count <= DELETION_U16_CAPACITY);
        let end = DELETION_U16_OFFSET + count * 2;
        cast_slice(&self.data[DELETION_U16_OFFSET..end])
    }

    #[inline]
    fn deltas_u16_mut(&mut self) -> &mut [u16] {
        cast_slice_mut(&mut self.data[DELETION_U16_OFFSET..])
    }

    #[inline]
    fn deltas_u32(&self) -> &[u32] {
        let count = self.len();
        debug_assert!(count <= DELETION_U32_CAPACITY);
        let end = DELETION_U32_OFFSET + count * 4;
        cast_slice(&self.data[DELETION_U32_OFFSET..end])
    }

    #[inline]
    fn deltas_u32_mut(&mut self) -> &mut [u32] {
        cast_slice_mut(&mut self.data[DELETION_U32_OFFSET..])
    }

    fn promote_to_u32(&mut self) -> std::result::Result<(), DeletionListError> {
        let count = self.len();
        if count > DELETION_U32_CAPACITY {
            return Err(DeletionListError::Full);
        }
        let mut temp = [0u32; DELETION_U32_CAPACITY];
        for (idx, delta) in self.deltas_u16().iter().enumerate() {
            temp[idx] = *delta as u32;
        }
        self.deltas_u32_mut()[..count].copy_from_slice(&temp[..count]);
        self.set_format_u32(true);
        Ok(())
    }
}

/// Iterator over deletion deltas in either u16 or u32 encoding mode.
pub enum DeletionListIter<'a> {
    U16 { data: &'a [u16], index: usize },
    U32 { data: &'a [u32], index: usize },
}

impl<'a> Iterator for DeletionListIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            DeletionListIter::U16 { data, index } => {
                let value = data.get(*index).copied().map(u32::from);
                *index += 1;
                value
            }
            DeletionListIter::U32 { data, index } => {
                let value = data.get(*index).copied();
                *index += 1;
                value
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_deletion_payload() -> ColumnPagePayload {
        ColumnPagePayload {
            block_id: 0,
            deletion_field: [0u8; 120],
        }
    }

    #[test]
    fn test_column_page_payload_size() {
        assert_eq!(mem::size_of::<ColumnPagePayload>(), 128);
    }

    #[test]
    fn test_deletion_list_empty() {
        let mut payload = build_deletion_payload();
        let list = payload.deletion_list();
        assert_eq!(list.len(), 0);
        assert!(!list.contains(10));
        assert_eq!(list.iter().collect::<Vec<_>>(), Vec::<u32>::new());
    }

    #[test]
    fn test_deletion_list_add_u16_full() {
        let mut payload = build_deletion_payload();
        let mut list = payload.deletion_list();
        for delta in 0..DELETION_U16_CAPACITY as u32 {
            assert!(list.add(delta).unwrap());
        }
        assert_eq!(list.len(), DELETION_U16_CAPACITY);
        assert_eq!(
            list.add(DELETION_U16_CAPACITY as u32).unwrap_err(),
            DeletionListError::Full
        );
    }

    #[test]
    fn test_deletion_list_promotion_to_u32() {
        let mut payload = build_deletion_payload();
        let mut list = payload.deletion_list();
        assert!(list.add(10).unwrap());
        assert!(list.add(5).unwrap());
        assert!(list.add(u16::MAX as u32 + 4).unwrap());
        assert!(list.contains(u16::MAX as u32 + 4));
        assert_eq!(list.len(), 3);
        assert_eq!(
            list.iter().collect::<Vec<_>>(),
            vec![5, 10, u16::MAX as u32 + 4]
        );
    }

    #[test]
    fn test_deletion_list_promotion_full() {
        let mut payload = build_deletion_payload();
        let mut list = payload.deletion_list();
        for delta in 0..(DELETION_U32_CAPACITY as u32 + 1) {
            assert!(list.add(delta).unwrap());
        }
        assert_eq!(
            list.add(u16::MAX as u32 + 1).unwrap_err(),
            DeletionListError::Full
        );
    }

    #[test]
    fn test_deletion_list_sorted_and_duplicates() {
        let mut payload = build_deletion_payload();
        let mut list = payload.deletion_list();
        assert!(list.add(10).unwrap());
        assert!(list.add(3).unwrap());
        assert!(list.add(8).unwrap());
        assert!(!list.add(8).unwrap());
        assert_eq!(list.iter().collect::<Vec<_>>(), vec![3, 8, 10]);
    }

    #[test]
    fn test_deletion_list_offloaded_flag() {
        let mut payload = build_deletion_payload();
        let mut list = payload.deletion_list();
        assert!(!list.is_offloaded());
        list.set_offloaded(true);
        assert!(list.is_offloaded());
        list.set_offloaded(false);
        assert!(!list.is_offloaded());
    }

    #[test]
    fn test_deletion_list_iter_row_ids() {
        let mut payload = build_deletion_payload();
        let mut list = payload.deletion_list();
        assert!(list.add(2).unwrap());
        assert!(list.add(5).unwrap());
        let row_ids = list.iter_row_ids(100).collect::<Vec<_>>();
        assert_eq!(row_ids, vec![102, 105]);
    }

    #[test]
    fn test_blob_ref_encode_decode_roundtrip() {
        let mut payload = build_deletion_payload();
        let blob_ref = BlobRef {
            start_page_id: 77,
            start_offset: 9,
            byte_len: 2048,
        };
        payload.clear_inline_and_set_offloaded(blob_ref);
        assert_eq!(payload.offloaded_ref(), Some(blob_ref));
        assert_eq!(payload.try_offloaded_ref().unwrap(), Some(blob_ref));
        let list = payload.deletion_list();
        assert!(list.is_offloaded());
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn test_blob_ref_invalid_decode() {
        let mut payload = build_deletion_payload();
        payload.clear_inline_and_set_offloaded(BlobRef {
            start_page_id: 1,
            start_offset: 0,
            byte_len: 12,
        });
        payload.deletion_field[DELETION_OFFLOAD_REF_OFFSET..DELETION_OFFLOAD_REF_OFFSET + 8]
            .copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(payload.offloaded_ref(), None);
        assert!(matches!(
            payload.try_offloaded_ref(),
            Err(Error::InvalidFormat)
        ));
    }
}
