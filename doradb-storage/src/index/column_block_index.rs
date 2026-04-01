use crate::buffer::{PoolGuard, ReadonlyBlockGuard, ReadonlyBufferPool};
use crate::error::{
    Error, PersistedFileKind, PersistedPageCorruptionCause, PersistedPageKind, Result,
};
use crate::file::cow_file::{BlockID, COW_FILE_PAGE_SIZE, MutableCowFile, SUPER_BLOCK_ID};
use crate::file::page_integrity::{
    COLUMN_BLOCK_INDEX_PAGE_SPEC, PAGE_INTEGRITY_HEADER_SIZE, max_payload_len, validate_page,
    write_page_checksum, write_page_header,
};
use crate::index::column_deletion_blob::{
    BlobRef, COLUMN_AUX_BLOB_CODEC_U32_DELTA_LIST, COLUMN_AUX_BLOB_KIND_DELETE_DELTAS,
    ColumnDeletionBlobReader, ColumnDeletionBlobWriter,
};
use crate::io::DirectBuf;
use crate::row::RowID;
use bytemuck::{Pod, Zeroable, bytes_of, cast_slice, cast_slice_mut, try_cast_slice};
use std::future::Future;
use std::mem;
use std::pin::Pin;

/// Physical size of one persisted column block-index page.
pub const COLUMN_BLOCK_PAGE_SIZE: usize = COW_FILE_PAGE_SIZE;
/// Validated payload bytes available inside one column block-index page.
pub const COLUMN_BLOCK_NODE_PAYLOAD_SIZE: usize = max_payload_len(COLUMN_BLOCK_PAGE_SIZE);
/// Serialized byte width of [`ColumnBlockNodeHeader`].
pub const COLUMN_BLOCK_HEADER_SIZE: usize = mem::size_of::<ColumnBlockNodeHeader>();
/// Bytes available for either branch entries or leaf payload after the node header.
pub const COLUMN_BLOCK_DATA_SIZE: usize = COLUMN_BLOCK_NODE_PAYLOAD_SIZE - COLUMN_BLOCK_HEADER_SIZE;
/// Serialized byte width of one [`ColumnBlockBranchEntry`].
pub const COLUMN_BRANCH_ENTRY_SIZE: usize = mem::size_of::<ColumnBlockBranchEntry>();
/// Bytes occupied by the shared node header plus the leaf-only header extension.
pub const COLUMN_BLOCK_LEAF_HEADER_SIZE: usize =
    COLUMN_BLOCK_HEADER_SIZE + mem::size_of::<ColumnBlockLeafHeaderExt>();

const COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE: usize = mem::size_of::<ColumnBlockLeafHeaderExt>();
const COLUMN_BLOCK_LEAF_DATA_SIZE: usize =
    COLUMN_BLOCK_DATA_SIZE - COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE;
const COLUMN_BLOCK_LEAF_PREFIX_U16_SIZE: usize = mem::size_of::<u16>() + mem::size_of::<u16>();
const COLUMN_BLOCK_LEAF_PREFIX_U32_SIZE: usize = mem::size_of::<u32>() + mem::size_of::<u16>();
const COLUMN_BLOCK_LEAF_PREFIX_PLAIN_SIZE: usize = mem::size_of::<u64>() + mem::size_of::<u16>();
const COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE: usize = mem::size_of::<ColumnBlockLeafEntryHeader>();
const COLUMN_DELETE_SECTION_HEADER_SIZE: usize = mem::size_of::<DeleteSectionHeader>();
const COLUMN_BLOCK_MIN_LEAF_ENTRY_SIZE: usize = COLUMN_BLOCK_LEAF_PREFIX_U16_SIZE
    + COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE
    + mem::size_of::<SectionHeader>();
const COLUMN_BLOCK_LEAF_SEARCH_TYPE_PLAIN: u8 = 1;
const COLUMN_BLOCK_LEAF_SEARCH_TYPE_DELTA_U32: u8 = 2;
const COLUMN_BLOCK_LEAF_SEARCH_TYPE_DELTA_U16: u8 = 3;
const COLUMN_ROW_SECTION_VERSION: u8 = 1;
const COLUMN_DELETE_SECTION_VERSION: u8 = 1;
const COLUMN_DELETE_DOMAIN_ROW_ID_DELTA: u8 = 1;
const COLUMN_DELETE_DOMAIN_ORDINAL: u8 = 2;
const COLUMN_ROW_CODEC_DENSE: u8 = 1;
const COLUMN_ROW_CODEC_DELTA_LIST: u8 = 2;
const COLUMN_DELETE_CODEC_NONE: u8 = 0;
const COLUMN_DELETE_CODEC_INLINE_DELTA_LIST: u8 = 1;
const COLUMN_DELETE_CODEC_EXTERNAL_BLOB: u8 = 2;
const COLUMN_BLOB_REF_SIZE: usize =
    mem::size_of::<BlockID>() + mem::size_of::<u16>() + mem::size_of::<u32>();
const LEGACY_INLINE_DELETE_FIELD_SIZE: usize = 120;
const LEGACY_INLINE_DELETE_U16_OFFSET: usize = 4;
const LEGACY_INLINE_DELETE_U32_OFFSET: usize = 4;
const LEGACY_INLINE_DELETE_U16_CAPACITY: usize =
    (LEGACY_INLINE_DELETE_FIELD_SIZE - LEGACY_INLINE_DELETE_U16_OFFSET) / 2;
const LEGACY_INLINE_DELETE_U32_CAPACITY: usize =
    (LEGACY_INLINE_DELETE_FIELD_SIZE - LEGACY_INLINE_DELETE_U32_OFFSET) / 4;
const ROW_SHAPE_FINGERPRINT_VERSION: u8 = 1;
const ROW_SHAPE_KIND_DENSE: u8 = 1;
const ROW_SHAPE_KIND_PRESENT_DELTA_LIST: u8 = 2;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Pod, Zeroable)]
struct ColumnBlockLeafHeaderExt {
    search_type: u8,
    reserved: [u8; 7],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Pod, Zeroable)]
struct ColumnBlockLeafEntryHeader {
    block_id: [u8; 8],
    row_shape_fingerprint: [u8; 16],
    row_id_span: [u8; 4],
    entry_len: [u8; 2],
    row_section_len: [u8; 2],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColumnBlockLeafSearchType {
    Plain,
    DeltaU32,
    DeltaU16,
}

impl ColumnBlockLeafSearchType {
    #[inline]
    fn encode(self) -> u8 {
        match self {
            ColumnBlockLeafSearchType::Plain => COLUMN_BLOCK_LEAF_SEARCH_TYPE_PLAIN,
            ColumnBlockLeafSearchType::DeltaU32 => COLUMN_BLOCK_LEAF_SEARCH_TYPE_DELTA_U32,
            ColumnBlockLeafSearchType::DeltaU16 => COLUMN_BLOCK_LEAF_SEARCH_TYPE_DELTA_U16,
        }
    }

    #[inline]
    fn decode(raw: u8) -> Result<Self> {
        match raw {
            COLUMN_BLOCK_LEAF_SEARCH_TYPE_PLAIN => Ok(ColumnBlockLeafSearchType::Plain),
            COLUMN_BLOCK_LEAF_SEARCH_TYPE_DELTA_U32 => Ok(ColumnBlockLeafSearchType::DeltaU32),
            COLUMN_BLOCK_LEAF_SEARCH_TYPE_DELTA_U16 => Ok(ColumnBlockLeafSearchType::DeltaU16),
            _ => Err(Error::InvalidFormat),
        }
    }

    #[inline]
    fn prefix_size(self) -> usize {
        match self {
            ColumnBlockLeafSearchType::Plain => COLUMN_BLOCK_LEAF_PREFIX_PLAIN_SIZE,
            ColumnBlockLeafSearchType::DeltaU32 => COLUMN_BLOCK_LEAF_PREFIX_U32_SIZE,
            ColumnBlockLeafSearchType::DeltaU16 => COLUMN_BLOCK_LEAF_PREFIX_U16_SIZE,
        }
    }
}

/// Worst-case bytes reserved per leaf search prefix.
pub const COLUMN_BLOCK_LEAF_PREFIX_SIZE: usize = COLUMN_BLOCK_LEAF_PREFIX_PLAIN_SIZE;
/// Maximum number of logical entries that can fit in one leaf node.
pub const COLUMN_BLOCK_MAX_ENTRIES: usize =
    COLUMN_BLOCK_LEAF_DATA_SIZE / COLUMN_BLOCK_MIN_LEAF_ENTRY_SIZE;
/// Maximum number of children that can fit in one branch node.
pub const COLUMN_BLOCK_MAX_BRANCH_ENTRIES: usize =
    COLUMN_BLOCK_DATA_SIZE / COLUMN_BRANCH_ENTRY_SIZE;

const _: () = assert!(mem::size_of::<ColumnBlockLeafHeaderExt>() == 8);
const _: () = assert!(mem::size_of::<ColumnBlockLeafEntryHeader>() == 32);
const _: () = assert!(mem::size_of::<ColumnBlockBranchEntry>() == 16);
const _: () = assert!(mem::size_of::<ColumnBlockNode>() == COLUMN_BLOCK_NODE_PAYLOAD_SIZE);

/// Persisted delete-domain tag stored in v2 leaf prefixes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnDeleteDomain {
    /// Delete payload units are `start_row_id`-relative row-id deltas.
    RowIdDelta,
    /// Delete payload units are row ordinals inside the authoritative row set.
    Ordinal,
}

impl ColumnDeleteDomain {
    #[inline]
    fn encode(self) -> u8 {
        match self {
            ColumnDeleteDomain::RowIdDelta => COLUMN_DELETE_DOMAIN_ROW_ID_DELTA,
            ColumnDeleteDomain::Ordinal => COLUMN_DELETE_DOMAIN_ORDINAL,
        }
    }

    #[inline]
    fn decode(raw: u8) -> Result<Self> {
        match raw {
            COLUMN_DELETE_DOMAIN_ROW_ID_DELTA => Ok(ColumnDeleteDomain::RowIdDelta),
            COLUMN_DELETE_DOMAIN_ORDINAL => Ok(ColumnDeleteDomain::Ordinal),
            _ => Err(Error::InvalidFormat),
        }
    }
}

impl ColumnBlockLeafHeaderExt {
    #[inline]
    fn new(search_type: ColumnBlockLeafSearchType) -> Self {
        ColumnBlockLeafHeaderExt {
            search_type: search_type.encode(),
            reserved: [0; 7],
        }
    }

    #[inline]
    fn search_type(&self) -> Result<ColumnBlockLeafSearchType> {
        ColumnBlockLeafSearchType::decode(self.search_type)
    }
}

impl ColumnBlockLeafEntryHeader {
    #[inline]
    fn block_id(&self) -> BlockID {
        BlockID::from(u64::from_le_bytes(self.block_id))
    }

    #[inline]
    fn row_shape_fingerprint(&self) -> u128 {
        u128::from_le_bytes(self.row_shape_fingerprint)
    }

    #[inline]
    fn row_id_span(&self) -> u32 {
        u32::from_le_bytes(self.row_id_span)
    }

    #[inline]
    fn entry_len(&self) -> u16 {
        u16::from_le_bytes(self.entry_len)
    }

    #[inline]
    fn row_section_len(&self) -> u16 {
        u16::from_le_bytes(self.row_section_len)
    }

    #[inline]
    fn end_row_id(&self, start_row_id: RowID) -> Result<RowID> {
        start_row_id
            .checked_add(self.row_id_span() as RowID)
            .ok_or(Error::InvalidFormat)
    }

    fn from_encoded(entry: &EncodedLeafEntry) -> Result<Self> {
        let entry_len = storage_len_u16(entry.payload_len())?;
        let row_section_len = storage_len_u16(entry.row_section.len())?;
        Ok(ColumnBlockLeafEntryHeader {
            block_id: entry.block_id.to_le_bytes(),
            row_shape_fingerprint: entry.row_shape_fingerprint.to_le_bytes(),
            row_id_span: entry.row_id_span.to_le_bytes(),
            entry_len: entry_len.to_le_bytes(),
            row_section_len: row_section_len.to_le_bytes(),
        })
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Pod, Zeroable)]
struct DeleteSectionHeader {
    kind: u8,
    version: u8,
    domain: u8,
    aux: u8,
    del_count: [u8; 2],
    reserved: [u8; 2],
}

impl DeleteSectionHeader {
    #[inline]
    fn new(kind: u8, domain: ColumnDeleteDomain, del_count: u16, aux: u8) -> Self {
        DeleteSectionHeader {
            kind,
            version: COLUMN_DELETE_SECTION_VERSION,
            domain: domain.encode(),
            aux,
            del_count: del_count.to_le_bytes(),
            reserved: [0; 2],
        }
    }

    #[inline]
    fn del_count(&self) -> u16 {
        u16::from_le_bytes(self.del_count)
    }

    #[inline]
    fn domain(&self) -> Result<ColumnDeleteDomain> {
        ColumnDeleteDomain::decode(self.domain)
    }
}

#[repr(C)]
/// Header stored at the beginning of each on-disk column block-index node.
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct ColumnBlockNodeHeader {
    /// Tree height of this node. `0` denotes a leaf.
    pub height: u32,
    /// Number of encoded entries stored in the node payload.
    pub count: u32,
    /// Inclusive lower row-id bound covered by this node.
    pub start_row_id: RowID,
    /// Creation timestamp associated with this copy-on-write node version.
    pub create_ts: u64,
}

#[repr(C)]
/// Branch entry mapping a child lower bound to a child page id.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Pod, Zeroable)]
pub struct ColumnBlockBranchEntry {
    /// Inclusive lower row-id bound routed to the child subtree.
    pub start_row_id: RowID,
    /// Page id of the child node.
    pub page_id: BlockID,
}

#[repr(C)]
/// In-memory view of one persisted column block-index node payload.
///
/// Leaves store a leaf-only header extension at the front of `data`, while
/// branch nodes interpret the same region entirely as branch entries.
#[derive(Clone)]
pub struct ColumnBlockNode {
    /// Fixed-size persisted header shared by branch and leaf nodes.
    pub header: ColumnBlockNodeHeader,
    data: [u8; COLUMN_BLOCK_DATA_SIZE],
}

impl ColumnBlockNode {
    #[inline]
    fn new_boxed(height: u32, start_row_id: RowID, create_ts: u64) -> Box<Self> {
        // SAFETY: `ColumnBlockNode` contains only integer and byte-array fields,
        // so the all-zero bit pattern is valid for the whole value.
        let mut node = unsafe { Box::<ColumnBlockNode>::new_zeroed().assume_init() };
        node.header = ColumnBlockNodeHeader {
            height,
            count: 0,
            start_row_id,
            create_ts,
        };
        if height == 0 {
            // Leaf nodes reserve the first bytes of `data` for the leaf-only
            // header extension, while branch nodes use the full region for
            // branch entries. Seed a valid default so a freshly allocated leaf
            // always decodes with a valid `search_type` before leaf encoding
            // rewrites it with the actual compact prefix mode.
            let header = ColumnBlockLeafHeaderExt::new(ColumnBlockLeafSearchType::Plain);
            node.data[..COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE].copy_from_slice(bytes_of(&header));
        }
        node
    }

    #[inline]
    fn data_ref(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    fn branch_entries_mut(&mut self) -> &mut [ColumnBlockBranchEntry] {
        branch_entries_from_bytes_mut(&mut self.data, self.header.count as usize)
    }
}

/// Validated row-shape metadata for one logical leaf entry before the backing
/// LWC page id is assigned in the table file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnBlockEntryShape {
    start_row_id: RowID,
    end_row_id: RowID,
    row_ids: Vec<RowID>,
    delete_deltas: Vec<u32>,
    delete_domain: ColumnDeleteDomain,
    row_shape_fingerprint: u128,
}

impl ColumnBlockEntryShape {
    /// Builds one validated phase-1 logical entry shape.
    pub(crate) fn new(
        start_row_id: RowID,
        end_row_id: RowID,
        row_ids: Vec<RowID>,
        mut delete_deltas: Vec<u32>,
    ) -> Result<Self> {
        if start_row_id >= end_row_id || row_ids.is_empty() {
            return Err(Error::InvalidArgument);
        }
        validate_row_ids(&row_ids, start_row_id, end_row_id)?;
        delete_deltas.sort_unstable();
        delete_deltas.dedup();
        let row_shape_fingerprint =
            row_shape_fingerprint_for_row_ids(start_row_id, end_row_id, &row_ids)?;
        Ok(ColumnBlockEntryShape {
            start_row_id,
            end_row_id,
            row_ids,
            delete_deltas,
            delete_domain: ColumnDeleteDomain::RowIdDelta,
            row_shape_fingerprint,
        })
    }

    #[inline]
    pub(crate) fn start_row_id(&self) -> RowID {
        self.start_row_id
    }

    #[inline]
    pub(crate) fn end_row_id(&self) -> RowID {
        self.end_row_id
    }

    #[inline]
    pub(crate) fn row_shape_fingerprint(&self) -> u128 {
        self.row_shape_fingerprint
    }

    pub(crate) fn set_end_row_id(&mut self, end_row_id: RowID) -> Result<()> {
        validate_row_ids(&self.row_ids, self.start_row_id, end_row_id)?;
        self.end_row_id = end_row_id;
        self.row_shape_fingerprint =
            row_shape_fingerprint_for_row_ids(self.start_row_id, self.end_row_id, &self.row_ids)?;
        Ok(())
    }

    #[inline]
    pub(crate) fn with_delete_domain(mut self, delete_domain: ColumnDeleteDomain) -> Self {
        self.delete_domain = delete_domain;
        self
    }

    #[inline]
    pub(crate) fn with_block_id(self, block_id: impl Into<BlockID>) -> ColumnBlockEntryInput {
        ColumnBlockEntryInput {
            start_row_id: self.start_row_id,
            end_row_id: self.end_row_id,
            block_id: block_id.into(),
            row_ids: self.row_ids,
            delete_deltas: self.delete_deltas,
            delete_domain: self.delete_domain,
            row_shape_fingerprint: self.row_shape_fingerprint,
        }
    }
}

/// Fully materialized logical leaf entry used by v2 builders and rewrite flows
/// after the backing LWC page id is known.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnBlockEntryInput {
    start_row_id: RowID,
    end_row_id: RowID,
    block_id: BlockID,
    row_ids: Vec<RowID>,
    delete_deltas: Vec<u32>,
    delete_domain: ColumnDeleteDomain,
    row_shape_fingerprint: u128,
}

impl ColumnBlockEntryInput {
    #[inline]
    pub(crate) fn start_row_id(&self) -> RowID {
        self.start_row_id
    }
}

/// One full logical-entry replacement keyed by leaf `start_row_id`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnBlockEntryPatch {
    /// Existing leaf-entry key to replace.
    pub start_row_id: RowID,
    /// Replacement logical entry for that leaf slot.
    pub entry: ColumnBlockEntryInput,
}

/// One resolved leaf entry from the persisted column block-index tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnLeafEntry {
    /// Page id of the leaf node that owns this entry.
    pub leaf_page_id: BlockID,
    /// Inclusive lower row-id bound of the entry.
    pub start_row_id: RowID,
    block_id: BlockID,
    end_row_id: RowID,
    row_count: u16,
    del_count: u16,
    row_id_span: u32,
    first_present_delta: u32,
    delete_domain: ColumnDeleteDomain,
    delete_blob_ref: Option<BlobRef>,
    row_shape_fingerprint: u128,
}

impl ColumnLeafEntry {
    /// Returns the persisted LWC block page id.
    #[inline]
    pub fn block_id(&self) -> BlockID {
        self.block_id
    }

    /// Returns the exclusive coverage upper bound of this persisted entry.
    #[inline]
    pub fn end_row_id(&self) -> RowID {
        self.end_row_id
    }

    /// Returns the decoded persisted row count.
    #[inline]
    pub fn row_count(&self) -> u16 {
        self.row_count
    }

    /// Returns the decoded persisted delete count.
    #[inline]
    pub fn del_count(&self) -> u16 {
        self.del_count
    }

    /// Returns the persisted row-id coverage span.
    #[inline]
    pub fn row_id_span(&self) -> u32 {
        self.row_id_span
    }

    /// Returns the delta from `start_row_id` to the first present persisted row.
    #[inline]
    pub fn first_present_delta(&self) -> u32 {
        self.first_present_delta
    }

    /// Returns the persisted delete domain used for this leaf entry.
    #[inline]
    pub fn delete_domain(&self) -> ColumnDeleteDomain {
        self.delete_domain
    }

    /// Returns the referenced delete blob when this entry uses external delete
    /// storage.
    #[inline]
    pub fn deletion_blob_ref(&self) -> Option<BlobRef> {
        self.delete_blob_ref
    }

    /// Returns the canonical row-shape fingerprint bound to this persisted
    /// block-index leaf entry.
    #[inline]
    pub fn row_shape_fingerprint(&self) -> u128 {
        self.row_shape_fingerprint
    }
}

/// Runtime row resolution result for one persisted columnar row lookup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedColumnRow {
    leaf_page_id: BlockID,
    block_id: BlockID,
    row_idx: usize,
    row_shape_fingerprint: u128,
}

impl ResolvedColumnRow {
    /// Returns the leaf page that produced this resolution result.
    #[inline]
    pub fn leaf_page_id(&self) -> BlockID {
        self.leaf_page_id
    }

    /// Returns the persisted LWC block page id that stores the row values.
    #[inline]
    pub fn block_id(&self) -> BlockID {
        self.block_id
    }

    /// Returns the resolved ordinal inside the persisted LWC page.
    #[inline]
    pub fn row_idx(&self) -> usize {
        self.row_idx
    }

    /// Returns the expected canonical row-shape fingerprint for the resolved
    /// persisted LWC page.
    #[inline]
    pub fn row_shape_fingerprint(&self) -> u128 {
        self.row_shape_fingerprint
    }
}

/// One authoritative delete-delta rewrite keyed by leaf `start_row_id`.
#[derive(Clone, Copy, Debug)]
pub struct ColumnDeleteDeltaPatch<'a> {
    /// Existing leaf-entry key to rewrite.
    pub start_row_id: RowID,
    /// Replacement delete deltas in ascending row-id-delta order.
    pub delete_deltas: &'a [u32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LogicalRowSet {
    Dense {
        row_id_span: u32,
    },
    DeltaList {
        row_id_span: u32,
        first_present_delta: u32,
        deltas: Vec<u32>,
    },
}

impl LogicalRowSet {
    fn from_row_ids(start_row_id: RowID, end_row_id: RowID, row_ids: &[RowID]) -> Result<Self> {
        validate_row_ids(row_ids, start_row_id, end_row_id)?;
        let span_u64 = end_row_id
            .checked_sub(start_row_id)
            .ok_or(Error::InvalidArgument)?;
        if span_u64 == 0 || span_u64 > u32::MAX as u64 {
            return Err(Error::InvalidArgument);
        }
        let row_id_span = span_u64 as u32;
        let dense = row_ids.len() == row_id_span as usize
            && row_ids
                .iter()
                .enumerate()
                .all(|(idx, row_id)| *row_id == start_row_id + idx as RowID);
        if dense {
            return Ok(LogicalRowSet::Dense { row_id_span });
        }
        let deltas = row_ids
            .iter()
            .map(|row_id| {
                let delta = row_id
                    .checked_sub(start_row_id)
                    .ok_or(Error::InvalidArgument)?;
                if delta > u32::MAX as u64 {
                    return Err(Error::InvalidArgument);
                }
                Ok(delta as u32)
            })
            .collect::<Result<Vec<_>>>()?;
        let first_present_delta = *deltas.first().ok_or(Error::InvalidArgument)?;
        Ok(LogicalRowSet::DeltaList {
            row_id_span,
            first_present_delta,
            deltas,
        })
    }

    fn contains_delta(&self, delta: u32) -> bool {
        match self {
            LogicalRowSet::Dense { row_id_span } => delta < *row_id_span,
            LogicalRowSet::DeltaList { deltas, .. } => deltas.binary_search(&delta).is_ok(),
        }
    }

    #[inline]
    fn row_id_span(&self) -> u32 {
        match self {
            LogicalRowSet::Dense { row_id_span } | LogicalRowSet::DeltaList { row_id_span, .. } => {
                *row_id_span
            }
        }
    }

    #[inline]
    fn row_count(&self) -> usize {
        match self {
            LogicalRowSet::Dense { row_id_span } => *row_id_span as usize,
            LogicalRowSet::DeltaList { deltas, .. } => deltas.len(),
        }
    }

    fn ordinal_for_delta(&self, delta: u32) -> Option<u32> {
        match self {
            LogicalRowSet::Dense { row_id_span } => (delta < *row_id_span).then_some(delta),
            LogicalRowSet::DeltaList { deltas, .. } => deltas
                .binary_search(&delta)
                .ok()
                .and_then(|idx| u32::try_from(idx).ok()),
        }
    }

    fn delta_for_ordinal(&self, ordinal: u32) -> Option<u32> {
        let idx = ordinal as usize;
        match self {
            LogicalRowSet::Dense { row_id_span } => (ordinal < *row_id_span).then_some(ordinal),
            LogicalRowSet::DeltaList { deltas, .. } => deltas.get(idx).copied(),
        }
    }
}

fn row_shape_fingerprint_for_row_ids(
    start_row_id: RowID,
    end_row_id: RowID,
    row_ids: &[RowID],
) -> Result<u128> {
    let row_set = LogicalRowSet::from_row_ids(start_row_id, end_row_id, row_ids)?;
    logical_row_shape_fingerprint(start_row_id, &row_set)
}

fn logical_row_shape_fingerprint(start_row_id: RowID, row_set: &LogicalRowSet) -> Result<u128> {
    let row_count = u32::try_from(row_set.row_count()).map_err(|_| Error::InvalidArgument)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[ROW_SHAPE_FINGERPRINT_VERSION]);
    let sparse_deltas = match row_set {
        LogicalRowSet::Dense { .. } => {
            hasher.update(&[ROW_SHAPE_KIND_DENSE]);
            None
        }
        LogicalRowSet::DeltaList { deltas, .. } => {
            hasher.update(&[ROW_SHAPE_KIND_PRESENT_DELTA_LIST]);
            Some(deltas.as_slice())
        }
    };
    hasher.update(&start_row_id.to_le_bytes());
    hasher.update(&row_count.to_le_bytes());
    if let Some(deltas) = sparse_deltas {
        for delta in deltas {
            hasher.update(&delta.to_le_bytes());
        }
    }
    let mut truncated = [0u8; mem::size_of::<u128>()];
    truncated.copy_from_slice(&hasher.finalize().as_bytes()[..mem::size_of::<u128>()]);
    Ok(u128::from_le_bytes(truncated))
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LogicalDeleteSet {
    None {
        domain: ColumnDeleteDomain,
    },
    Inline {
        domain: ColumnDeleteDomain,
        row_id_deltas: Vec<u32>,
    },
    External {
        domain: ColumnDeleteDomain,
        del_count: u16,
        blob_ref: BlobRef,
        row_id_deltas: Option<Vec<u32>>,
    },
}

impl LogicalDeleteSet {
    #[inline]
    fn domain(&self) -> ColumnDeleteDomain {
        match self {
            LogicalDeleteSet::None { domain }
            | LogicalDeleteSet::Inline { domain, .. }
            | LogicalDeleteSet::External { domain, .. } => *domain,
        }
    }

    #[inline]
    fn del_count(&self) -> Result<u16> {
        match self {
            LogicalDeleteSet::None { .. } => Ok(0),
            LogicalDeleteSet::Inline { row_id_deltas, .. } => {
                storage_count_u16(row_id_deltas.len())
            }
            LogicalDeleteSet::External { del_count, .. } => Ok(*del_count),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LogicalLeafEntry {
    start_row_id: RowID,
    block_id: BlockID,
    row_set: LogicalRowSet,
    delete_set: LogicalDeleteSet,
    row_shape_fingerprint: u128,
}

impl LogicalLeafEntry {
    fn new(
        start_row_id: RowID,
        block_id: BlockID,
        row_set: LogicalRowSet,
        delete_set: LogicalDeleteSet,
        row_shape_fingerprint: u128,
    ) -> Self {
        LogicalLeafEntry {
            start_row_id,
            block_id,
            row_set,
            delete_set,
            row_shape_fingerprint,
        }
    }
}

#[derive(Clone, Debug)]
enum ResolvedLeafPatch {
    Entry {
        start_row_id: RowID,
        entry: LogicalLeafEntry,
    },
    DeleteSet {
        start_row_id: RowID,
        delete_set: LogicalDeleteSet,
    },
}

impl ResolvedLeafPatch {
    #[inline]
    fn start_row_id(&self) -> RowID {
        match self {
            ResolvedLeafPatch::Entry { start_row_id, .. } => *start_row_id,
            ResolvedLeafPatch::DeleteSet { start_row_id, .. } => *start_row_id,
        }
    }

    fn apply(&self, entry: &mut LogicalLeafEntry) -> Result<()> {
        match self {
            ResolvedLeafPatch::Entry {
                start_row_id,
                entry: replacement,
            } => {
                if replacement.start_row_id != *start_row_id {
                    return Err(Error::InvalidArgument);
                }
                *entry = replacement.clone();
            }
            ResolvedLeafPatch::DeleteSet { delete_set, .. } => {
                entry.delete_set = delete_set.clone();
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct EncodedLeafEntry {
    start_row_id: RowID,
    block_id: BlockID,
    row_id_span: u32,
    row_shape_fingerprint: u128,
    row_section: Vec<u8>,
    delete_section: Vec<u8>,
}

impl EncodedLeafEntry {
    fn from_logical(entry: &LogicalLeafEntry) -> Result<Self> {
        let row_section = encode_row_section(&entry.row_set, entry.delete_set.domain())?;
        let delete_section = encode_delete_section(&entry.row_set, &entry.delete_set)?;
        Ok(EncodedLeafEntry {
            start_row_id: entry.start_row_id,
            block_id: entry.block_id,
            row_id_span: entry.row_set.row_id_span(),
            row_shape_fingerprint: entry.row_shape_fingerprint,
            row_section,
            delete_section,
        })
    }

    #[inline]
    fn payload_len(&self) -> usize {
        COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + self.row_section.len() + self.delete_section.len()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SectionHeader {
    kind: u8,
    version: u8,
    flags: u8,
    aux: u8,
}

impl SectionHeader {
    #[inline]
    fn encode(self) -> [u8; 4] {
        [self.kind, self.version, self.flags, self.aux]
    }

    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 {
            return Err(Error::InvalidFormat);
        }
        Ok(SectionHeader {
            kind: bytes[0],
            version: bytes[1],
            flags: bytes[2],
            aux: bytes[3],
        })
    }
}

#[derive(Clone, Debug)]
struct LeafEntryView<'a> {
    start_row_id: RowID,
    entry_header: ColumnBlockLeafEntryHeader,
    row_section: &'a [u8],
    delete_section: Option<&'a [u8]>,
    row_header: SectionHeader,
    delete_header: Option<DeleteSectionHeader>,
}

#[derive(Clone, Copy, Debug)]
struct DecodedLeafPrefix {
    start_row_id: RowID,
    entry_offset: u16,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Pod, Zeroable)]
struct ColumnBlockLeafPrefixPlain {
    start_row_id: [u8; 8],
    entry_offset: [u8; 2],
}

impl ColumnBlockLeafPrefixPlain {
    #[inline]
    fn start_row_id(&self) -> RowID {
        u64::from_le_bytes(self.start_row_id)
    }

    #[inline]
    fn entry_offset(&self) -> u16 {
        u16::from_le_bytes(self.entry_offset)
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Pod, Zeroable)]
struct ColumnBlockLeafPrefixDeltaU32 {
    start_row_delta: [u8; 4],
    entry_offset: [u8; 2],
}

impl ColumnBlockLeafPrefixDeltaU32 {
    #[inline]
    fn start_row_delta(&self) -> u32 {
        u32::from_le_bytes(self.start_row_delta)
    }

    #[inline]
    fn entry_offset(&self) -> u16 {
        u16::from_le_bytes(self.entry_offset)
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Pod, Zeroable)]
struct ColumnBlockLeafPrefixDeltaU16 {
    start_row_delta: [u8; 2],
    entry_offset: [u8; 2],
}

impl ColumnBlockLeafPrefixDeltaU16 {
    #[inline]
    fn start_row_delta(&self) -> u16 {
        u16::from_le_bytes(self.start_row_delta)
    }

    #[inline]
    fn entry_offset(&self) -> u16 {
        u16::from_le_bytes(self.entry_offset)
    }
}

#[derive(Clone, Copy)]
enum LeafPrefixPlane<'a> {
    Plain {
        header_start_row_id: RowID,
        prefixes: &'a [ColumnBlockLeafPrefixPlain],
    },
    DeltaU32 {
        header_start_row_id: RowID,
        prefixes: &'a [ColumnBlockLeafPrefixDeltaU32],
    },
    DeltaU16 {
        header_start_row_id: RowID,
        prefixes: &'a [ColumnBlockLeafPrefixDeltaU16],
    },
}

impl<'a> LeafPrefixPlane<'a> {
    #[inline]
    fn search_type(&self) -> ColumnBlockLeafSearchType {
        match self {
            LeafPrefixPlane::Plain { .. } => ColumnBlockLeafSearchType::Plain,
            LeafPrefixPlane::DeltaU32 { .. } => ColumnBlockLeafSearchType::DeltaU32,
            LeafPrefixPlane::DeltaU16 { .. } => ColumnBlockLeafSearchType::DeltaU16,
        }
    }

    #[inline]
    fn header_start_row_id(&self) -> RowID {
        match self {
            LeafPrefixPlane::Plain {
                header_start_row_id,
                ..
            }
            | LeafPrefixPlane::DeltaU32 {
                header_start_row_id,
                ..
            }
            | LeafPrefixPlane::DeltaU16 {
                header_start_row_id,
                ..
            } => *header_start_row_id,
        }
    }

    #[inline]
    fn count(&self) -> usize {
        match self {
            LeafPrefixPlane::Plain { prefixes, .. } => prefixes.len(),
            LeafPrefixPlane::DeltaU32 { prefixes, .. } => prefixes.len(),
            LeafPrefixPlane::DeltaU16 { prefixes, .. } => prefixes.len(),
        }
    }

    #[inline]
    fn prefix_bytes_len(&self) -> usize {
        self.count() * self.search_type().prefix_size()
    }

    fn prefix(&self, idx: usize) -> Result<DecodedLeafPrefix> {
        match self {
            LeafPrefixPlane::Plain { prefixes, .. } => prefixes
                .get(idx)
                .map(|prefix| DecodedLeafPrefix {
                    start_row_id: prefix.start_row_id(),
                    entry_offset: prefix.entry_offset(),
                })
                .ok_or(Error::InvalidFormat),
            LeafPrefixPlane::DeltaU32 {
                header_start_row_id,
                prefixes,
            } => prefixes
                .get(idx)
                .and_then(|prefix| {
                    header_start_row_id
                        .checked_add(prefix.start_row_delta() as RowID)
                        .map(|start_row_id| DecodedLeafPrefix {
                            start_row_id,
                            entry_offset: prefix.entry_offset(),
                        })
                })
                .ok_or(Error::InvalidFormat),
            LeafPrefixPlane::DeltaU16 {
                header_start_row_id,
                prefixes,
            } => prefixes
                .get(idx)
                .and_then(|prefix| {
                    header_start_row_id
                        .checked_add(prefix.start_row_delta() as RowID)
                        .map(|start_row_id| DecodedLeafPrefix {
                            start_row_id,
                            entry_offset: prefix.entry_offset(),
                        })
                })
                .ok_or(Error::InvalidFormat),
        }
    }

    fn search(&self, row_id: RowID) -> Result<Option<usize>> {
        match self {
            LeafPrefixPlane::Plain { prefixes, .. } => Ok(search_prefix_slice(
                prefixes.binary_search_by_key(&row_id, |prefix| prefix.start_row_id()),
            )),
            LeafPrefixPlane::DeltaU32 {
                header_start_row_id,
                prefixes,
            } => {
                let Some(delta) = row_id.checked_sub(*header_start_row_id) else {
                    return Ok(None);
                };
                let key = u32::try_from(delta).unwrap_or(u32::MAX);
                Ok(search_prefix_slice(
                    prefixes.binary_search_by_key(&key, |prefix| prefix.start_row_delta()),
                ))
            }
            LeafPrefixPlane::DeltaU16 {
                header_start_row_id,
                prefixes,
            } => {
                let Some(delta) = row_id.checked_sub(*header_start_row_id) else {
                    return Ok(None);
                };
                let key = u16::try_from(delta).unwrap_or(u16::MAX);
                Ok(search_prefix_slice(
                    prefixes.binary_search_by_key(&key, |prefix| prefix.start_row_delta()),
                ))
            }
        }
    }

    fn search_exact(&self, start_row_id: RowID) -> Result<usize> {
        match self {
            LeafPrefixPlane::Plain { prefixes, .. } => prefixes
                .binary_search_by_key(&start_row_id, |prefix| prefix.start_row_id())
                .map_err(|_| Error::InvalidFormat),
            LeafPrefixPlane::DeltaU32 {
                header_start_row_id,
                prefixes,
            } => {
                let delta = start_row_id
                    .checked_sub(*header_start_row_id)
                    .ok_or(Error::InvalidFormat)?;
                let delta = u32::try_from(delta).map_err(|_| Error::InvalidFormat)?;
                prefixes
                    .binary_search_by_key(&delta, |prefix| prefix.start_row_delta())
                    .map_err(|_| Error::InvalidFormat)
            }
            LeafPrefixPlane::DeltaU16 {
                header_start_row_id,
                prefixes,
            } => {
                let delta = start_row_id
                    .checked_sub(*header_start_row_id)
                    .ok_or(Error::InvalidFormat)?;
                let delta = u16::try_from(delta).map_err(|_| Error::InvalidFormat)?;
                prefixes
                    .binary_search_by_key(&delta, |prefix| prefix.start_row_delta())
                    .map_err(|_| Error::InvalidFormat)
            }
        }
    }
}

trait ColumnBlockNodeRead {
    fn header_ref(&self) -> &ColumnBlockNodeHeader;
    fn data_ref(&self) -> &[u8];

    #[inline]
    fn is_leaf(&self) -> bool {
        self.header_ref().height == 0
    }
    #[inline]
    fn branch_entries(&self) -> &[ColumnBlockBranchEntry] {
        branch_entries_from_bytes(self.data_ref(), self.header_ref().count as usize)
    }

    #[inline]
    fn leaf_data_ref(&self) -> &[u8] {
        &self.data_ref()[COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE..]
    }
}

impl ColumnBlockNodeRead for ColumnBlockNode {
    #[inline]
    fn header_ref(&self) -> &ColumnBlockNodeHeader {
        &self.header
    }

    #[inline]
    fn data_ref(&self) -> &[u8] {
        &self.data
    }
}

/// Snapshot reader and copy-on-write rewrite façade for one persisted column
/// block-index tree root.
pub struct ColumnBlockIndex<'a> {
    disk_pool: &'a ReadonlyBufferPool,
    disk_pool_guard: &'a PoolGuard,
    root_block_id: BlockID,
    end_row_id: RowID,
}

#[derive(Clone, Debug)]
struct NodeRewriteResult {
    entries: Vec<ColumnBlockBranchEntry>,
    touched: bool,
}

#[inline]
fn invalid_node_payload(file_kind: PersistedFileKind, page_id: BlockID) -> Error {
    Error::persisted_page_corrupted(
        file_kind,
        PersistedPageKind::ColumnBlockIndex,
        page_id,
        PersistedPageCorruptionCause::InvalidPayload,
    )
}

#[inline]
fn validate_node_payload(
    page: &[u8],
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<(&[u8], ColumnBlockNodeHeader)> {
    let payload = validate_page(page, COLUMN_BLOCK_INDEX_PAGE_SPEC).map_err(|cause| {
        Error::persisted_page_corrupted(
            file_kind,
            PersistedPageKind::ColumnBlockIndex,
            page_id,
            cause,
        )
    })?;
    let header =
        bytemuck::try_from_bytes::<ColumnBlockNodeHeader>(&payload[..COLUMN_BLOCK_HEADER_SIZE])
            .map_err(|_| invalid_node_payload(file_kind, page_id))?;
    let count = header.count as usize;
    if (header.height == 0 && count > COLUMN_BLOCK_MAX_ENTRIES)
        || (header.height > 0 && count > COLUMN_BLOCK_MAX_BRANCH_ENTRIES)
    {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    Ok((payload, *header))
}

#[inline]
pub(crate) fn validate_persisted_column_block_index_page(
    page: &[u8],
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<()> {
    validate_node_payload(page, file_kind, page_id).map(|_| ())
}

#[inline]
fn validated_node_payload(page: &[u8]) -> &[u8] {
    let payload_start = PAGE_INTEGRITY_HEADER_SIZE;
    &page[payload_start..payload_start + COLUMN_BLOCK_NODE_PAYLOAD_SIZE]
}

#[inline]
fn branch_entries_from_bytes(data: &[u8], count: usize) -> &[ColumnBlockBranchEntry] {
    let bytes_len = count * mem::size_of::<ColumnBlockBranchEntry>();
    cast_slice(&data[..bytes_len])
}

#[inline]
fn branch_entries_from_bytes_mut(data: &mut [u8], count: usize) -> &mut [ColumnBlockBranchEntry] {
    let bytes_len = count * mem::size_of::<ColumnBlockBranchEntry>();
    cast_slice_mut(&mut data[..bytes_len])
}

struct ValidatedColumnBlockNode {
    guard: ReadonlyBlockGuard,
    header: ColumnBlockNodeHeader,
}

impl ValidatedColumnBlockNode {
    #[inline]
    fn try_from_guard(
        guard: ReadonlyBlockGuard,
        file_kind: PersistedFileKind,
        page_id: BlockID,
    ) -> Result<Self> {
        let (_, header) = validate_node_payload(guard.page(), file_kind, page_id)?;
        Ok(ValidatedColumnBlockNode { guard, header })
    }

    fn leaf_header_ext(
        &self,
        file_kind: PersistedFileKind,
        page_id: BlockID,
    ) -> Result<&ColumnBlockLeafHeaderExt> {
        bytemuck::try_from_bytes::<ColumnBlockLeafHeaderExt>(
            &self.data_ref()[..COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE],
        )
        .map_err(|_| invalid_node_payload(file_kind, page_id))
    }

    fn leaf_prefix_plane(
        &self,
        file_kind: PersistedFileKind,
        page_id: BlockID,
    ) -> Result<LeafPrefixPlane<'_>> {
        let search_type = self
            .leaf_header_ext(file_kind, page_id)?
            .search_type()
            .map_err(|_| invalid_node_payload(file_kind, page_id))?;
        let count = self.header.count as usize;
        let data = self.leaf_data_ref();
        let prefix_bytes_len = count
            .checked_mul(search_type.prefix_size())
            .ok_or_else(|| invalid_node_payload(file_kind, page_id))?;
        if prefix_bytes_len > data.len() {
            return Err(invalid_node_payload(file_kind, page_id));
        }
        let prefix_bytes = &data[..prefix_bytes_len];
        let plane = match search_type {
            ColumnBlockLeafSearchType::Plain => LeafPrefixPlane::Plain {
                header_start_row_id: self.header.start_row_id,
                prefixes: try_cast_slice(prefix_bytes)
                    .map_err(|_| invalid_node_payload(file_kind, page_id))?,
            },
            ColumnBlockLeafSearchType::DeltaU32 => LeafPrefixPlane::DeltaU32 {
                header_start_row_id: self.header.start_row_id,
                prefixes: try_cast_slice(prefix_bytes)
                    .map_err(|_| invalid_node_payload(file_kind, page_id))?,
            },
            ColumnBlockLeafSearchType::DeltaU16 => LeafPrefixPlane::DeltaU16 {
                header_start_row_id: self.header.start_row_id,
                prefixes: try_cast_slice(prefix_bytes)
                    .map_err(|_| invalid_node_payload(file_kind, page_id))?,
            },
        };
        validate_leaf_prefixes(&plane, data, file_kind, page_id)?;
        Ok(plane)
    }

    fn leaf_entry_view(
        &self,
        idx: usize,
        file_kind: PersistedFileKind,
        page_id: BlockID,
    ) -> Result<LeafEntryView<'_>> {
        let prefixes = self.leaf_prefix_plane(file_kind, page_id)?;
        let prefix = prefixes
            .prefix(idx)
            .map_err(|_| invalid_node_payload(file_kind, page_id))?;
        let data = self.leaf_data_ref();
        let prefix_end = prefixes.prefix_bytes_len();
        let entry_bytes =
            leaf_entry_slice(data, prefix_end, prefix.entry_offset, file_kind, page_id)?;
        let entry_header = *bytemuck::try_from_bytes::<ColumnBlockLeafEntryHeader>(
            &entry_bytes[..COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE],
        )
        .map_err(|_| invalid_node_payload(file_kind, page_id))?;
        let row_section_end =
            COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + entry_header.row_section_len() as usize;
        let row_section = &entry_bytes[COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE..row_section_end];
        let row_header = SectionHeader::decode(row_section)
            .map_err(|_| invalid_node_payload(file_kind, page_id))?;
        if row_header.version != COLUMN_ROW_SECTION_VERSION {
            return Err(invalid_node_payload(file_kind, page_id));
        }
        let delete_section = if row_section_end == entry_bytes.len() {
            None
        } else {
            Some(&entry_bytes[row_section_end..])
        };
        let delete_header = match delete_section {
            Some(bytes) => {
                if bytes.len() < COLUMN_DELETE_SECTION_HEADER_SIZE {
                    return Err(invalid_node_payload(file_kind, page_id));
                }
                let header = *bytemuck::try_from_bytes::<DeleteSectionHeader>(
                    &bytes[..COLUMN_DELETE_SECTION_HEADER_SIZE],
                )
                .map_err(|_| invalid_node_payload(file_kind, page_id))?;
                if header.version != COLUMN_DELETE_SECTION_VERSION {
                    return Err(invalid_node_payload(file_kind, page_id));
                }
                Some(header)
            }
            None => None,
        };
        Ok(LeafEntryView {
            start_row_id: prefix.start_row_id,
            entry_header,
            row_section,
            delete_section,
            row_header,
            delete_header,
        })
    }
}

impl ColumnBlockNodeRead for ValidatedColumnBlockNode {
    #[inline]
    fn header_ref(&self) -> &ColumnBlockNodeHeader {
        &self.header
    }

    #[inline]
    fn data_ref(&self) -> &[u8] {
        let payload = validated_node_payload(self.guard.page());
        &payload[COLUMN_BLOCK_HEADER_SIZE..COLUMN_BLOCK_HEADER_SIZE + COLUMN_BLOCK_DATA_SIZE]
    }
}

impl<'a> ColumnBlockIndex<'a> {
    /// Creates a column block-index view for one root block snapshot.
    #[inline]
    pub fn new(
        root_block_id: BlockID,
        end_row_id: RowID,
        disk_pool: &'a ReadonlyBufferPool,
        disk_pool_guard: &'a PoolGuard,
    ) -> Self {
        ColumnBlockIndex {
            disk_pool,
            disk_pool_guard,
            root_block_id,
            end_row_id,
        }
    }

    /// Returns current column block-index root block id.
    #[inline]
    pub fn root_block_id(&self) -> BlockID {
        self.root_block_id
    }

    /// Returns exclusive upper row-id boundary covered by this index snapshot.
    #[inline]
    pub fn end_row_id(&self) -> RowID {
        self.end_row_id
    }

    #[inline]
    pub(crate) fn file_kind(&self) -> PersistedFileKind {
        self.disk_pool.persisted_file_kind()
    }

    #[inline]
    async fn read_node(&self, page_id: BlockID) -> Result<ValidatedColumnBlockNode> {
        let g = self
            .disk_pool
            .read_validated_block(
                self.disk_pool_guard,
                page_id,
                validate_persisted_column_block_index_page,
            )
            .await?;
        ValidatedColumnBlockNode::try_from_guard(g, self.file_kind(), page_id)
    }

    #[inline]
    fn read_entry_view<'n>(
        &self,
        node: &'n ValidatedColumnBlockNode,
        entry: &ColumnLeafEntry,
    ) -> Result<LeafEntryView<'n>> {
        let prefixes = node.leaf_prefix_plane(self.file_kind(), entry.leaf_page_id)?;
        let idx = prefixes
            .search_exact(entry.start_row_id)
            .map_err(|_| invalid_node_payload(self.file_kind(), entry.leaf_page_id))?;
        node.leaf_entry_view(idx, self.file_kind(), entry.leaf_page_id)
    }

    /// Finds the persisted leaf entry whose coverage contains `row_id`.
    pub async fn locate_block(&self, row_id: RowID) -> Result<Option<ColumnLeafEntry>> {
        if self.root_block_id == SUPER_BLOCK_ID || row_id >= self.end_row_id {
            return Ok(None);
        }
        let mut page_id = self.root_block_id;
        loop {
            let node = self.read_node(page_id).await?;
            if node.is_leaf() {
                let prefixes = node.leaf_prefix_plane(self.file_kind(), page_id)?;
                let idx = match search_start_row_id(&prefixes, row_id)? {
                    Some(idx) => idx,
                    None => return Ok(None),
                };
                let view = node.leaf_entry_view(idx, self.file_kind(), page_id)?;
                if !entry_contains_row_id(&view, row_id, self.file_kind(), page_id)? {
                    return Ok(None);
                }
                return Ok(Some(build_leaf_entry(page_id, &view, self.file_kind())?));
            }
            let entries = node.branch_entries();
            let idx = match search_branch_entry(entries, row_id) {
                Some(idx) => idx,
                None => return Ok(None),
            };
            page_id = entries[idx].page_id;
        }
    }

    /// Resolves one row id against an already-located persisted leaf entry.
    pub async fn resolve_row(
        &self,
        row_id: RowID,
        entry: &ColumnLeafEntry,
    ) -> Result<Option<ResolvedColumnRow>> {
        let node = self.read_node(entry.leaf_page_id).await?;
        let view = self.read_entry_view(&node, entry)?;
        let Some(row_idx) =
            resolve_row_idx_in_view(&view, row_id, self.file_kind(), entry.leaf_page_id)?
        else {
            return Ok(None);
        };
        Ok(Some(build_resolved_row(entry.leaf_page_id, &view, row_idx)))
    }

    /// Locates and resolves one persisted row id in a single tree descent.
    pub async fn locate_and_resolve_row(&self, row_id: RowID) -> Result<Option<ResolvedColumnRow>> {
        if self.root_block_id == SUPER_BLOCK_ID || row_id >= self.end_row_id {
            return Ok(None);
        }
        let mut page_id = self.root_block_id;
        loop {
            let node = self.read_node(page_id).await?;
            if node.is_leaf() {
                let prefixes = node.leaf_prefix_plane(self.file_kind(), page_id)?;
                let idx = match search_start_row_id(&prefixes, row_id)? {
                    Some(idx) => idx,
                    None => return Ok(None),
                };
                let view = node.leaf_entry_view(idx, self.file_kind(), page_id)?;
                let Some(row_idx) =
                    resolve_row_idx_in_view(&view, row_id, self.file_kind(), page_id)?
                else {
                    return Ok(None);
                };
                return Ok(Some(build_resolved_row(page_id, &view, row_idx)));
            }
            let entries = node.branch_entries();
            let idx = match search_branch_entry(entries, row_id) {
                Some(idx) => idx,
                None => return Ok(None),
            };
            page_id = entries[idx].page_id;
        }
    }

    /// Loads validated delete deltas for one persisted entry.
    pub(crate) async fn load_delete_deltas(&self, entry: &ColumnLeafEntry) -> Result<Vec<u32>> {
        let node = self.read_node(entry.leaf_page_id).await?;
        let view = self.read_entry_view(&node, entry)?;
        let row_set = decode_logical_row_set(&view, self.file_kind(), entry.leaf_page_id)?;
        let delete_set = self
            .decode_logical_delete_set(&view, &row_set, entry.leaf_page_id)
            .await?;
        Ok(match delete_set {
            LogicalDeleteSet::None { .. } => Vec::new(),
            LogicalDeleteSet::Inline { row_id_deltas, .. } => row_id_deltas,
            LogicalDeleteSet::External { row_id_deltas, .. } => {
                row_id_deltas.ok_or(Error::InvalidState)?
            }
        })
    }

    /// Loads the authoritative persisted row-id set for one validated leaf
    /// entry.
    pub async fn load_entry_row_ids(&self, entry: &ColumnLeafEntry) -> Result<Vec<RowID>> {
        let node = self.read_node(entry.leaf_page_id).await?;
        let view = self.read_entry_view(&node, entry)?;
        let row_set = decode_logical_row_set(&view, self.file_kind(), entry.leaf_page_id)?;
        decode_row_ids_from_row_set(
            view.start_row_id,
            &row_set,
            self.file_kind(),
            entry.leaf_page_id,
        )
    }

    async fn decode_logical_delete_set(
        &self,
        view: &LeafEntryView<'_>,
        row_set: &LogicalRowSet,
        page_id: BlockID,
    ) -> Result<LogicalDeleteSet> {
        let delete_set = decode_logical_delete_set_base(view, row_set, self.file_kind(), page_id)?;
        let LogicalDeleteSet::External {
            domain,
            del_count,
            blob_ref,
            ..
        } = delete_set
        else {
            return Ok(delete_set);
        };

        let reader = ColumnDeletionBlobReader::new(self.disk_pool, self.disk_pool_guard);
        let (header, payload) =
            reader
                .read_framed_blob(blob_ref)
                .await
                .map_err(|err| match err {
                    Error::InvalidFormat => Error::persisted_page_corrupted(
                        self.file_kind(),
                        PersistedPageKind::ColumnDeletionBlob,
                        blob_ref.start_page_id,
                        PersistedPageCorruptionCause::InvalidPayload,
                    ),
                    other => other,
                })?;
        if header.blob_kind() != COLUMN_AUX_BLOB_KIND_DELETE_DELTAS
            || header.codec_kind() != COLUMN_AUX_BLOB_CODEC_U32_DELTA_LIST
            || header.codec_version()
                != view
                    .delete_header
                    .ok_or_else(|| invalid_node_payload(self.file_kind(), page_id))?
                    .version
        {
            return Err(Error::persisted_page_corrupted(
                self.file_kind(),
                PersistedPageKind::ColumnDeletionBlob,
                blob_ref.start_page_id,
                PersistedPageCorruptionCause::InvalidPayload,
            ));
        }
        let row_id_deltas = decode_delete_rows(
            &payload,
            del_count,
            domain,
            row_set,
            self.file_kind(),
            page_id,
        )
        .map_err(|err| match err {
            Error::InvalidFormat => Error::persisted_page_corrupted(
                self.file_kind(),
                PersistedPageKind::ColumnDeletionBlob,
                blob_ref.start_page_id,
                PersistedPageCorruptionCause::InvalidPayload,
            ),
            other => other,
        })?;
        Ok(LogicalDeleteSet::External {
            domain,
            del_count,
            blob_ref,
            row_id_deltas: Some(row_id_deltas),
        })
    }

    async fn load_rewrite_context(
        &self,
        start_row_id: RowID,
    ) -> Result<(LogicalRowSet, ColumnDeleteDomain)> {
        if self.root_block_id == SUPER_BLOCK_ID {
            return Err(Error::InvalidArgument);
        }
        let mut page_id = self.root_block_id;
        loop {
            let node = self.read_node(page_id).await?;
            if node.is_leaf() {
                let prefixes = node.leaf_prefix_plane(self.file_kind(), page_id)?;
                let idx = prefixes
                    .search_exact(start_row_id)
                    .map_err(|_| Error::InvalidArgument)?;
                let view = node.leaf_entry_view(idx, self.file_kind(), page_id)?;
                let row_set = decode_logical_row_set(&view, self.file_kind(), page_id)?;
                let delete_domain = decode_default_delete_domain_from_row_header(
                    view.row_header,
                    self.file_kind(),
                    page_id,
                )?;
                return Ok((row_set, delete_domain));
            }
            let entries = node.branch_entries();
            let idx = search_branch_entry(entries, start_row_id).ok_or(Error::InvalidArgument)?;
            page_id = entries[idx].page_id;
        }
    }

    /// Collects all leaf entries in ascending `start_row_id` order.
    pub async fn collect_leaf_entries(&self) -> Result<Vec<ColumnLeafEntry>> {
        if self.root_block_id == SUPER_BLOCK_ID {
            return Ok(Vec::new());
        }
        let mut stack = vec![self.root_block_id];
        let mut entries = Vec::new();
        let mut last_end = None;
        while let Some(page_id) = stack.pop() {
            let node = self.read_node(page_id).await?;
            if node.is_leaf() {
                let prefixes = node.leaf_prefix_plane(self.file_kind(), page_id)?;
                for idx in 0..prefixes.count() {
                    let view = node.leaf_entry_view(idx, self.file_kind(), page_id)?;
                    let entry = build_leaf_entry(page_id, &view, self.file_kind())?;
                    if let Some(prev_end) = last_end
                        && entry.start_row_id < prev_end
                    {
                        return Err(invalid_node_payload(self.file_kind(), page_id));
                    }
                    last_end = Some(entry.end_row_id());
                    entries.push(entry);
                }
                continue;
            }
            let branch_entries = node.branch_entries();
            for entry in branch_entries.iter().rev() {
                if entry.page_id == SUPER_BLOCK_ID {
                    return Err(invalid_node_payload(self.file_kind(), page_id));
                }
                stack.push(entry.page_id);
            }
        }
        Ok(entries)
    }

    /// Replaces sorted delete-delta sets keyed by `start_row_id`.
    pub async fn batch_replace_delete_deltas<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        patches: &[ColumnDeleteDeltaPatch<'_>],
        create_ts: u64,
    ) -> Result<BlockID> {
        if patches.is_empty() {
            return Ok(self.root_block_id);
        }
        if self.root_block_id == SUPER_BLOCK_ID || !delete_delta_patches_sorted_unique(patches) {
            return Err(Error::InvalidArgument);
        }

        let resolved = {
            let mut writer = ColumnDeletionBlobWriter::new(mutable_file);
            let mut resolved = Vec::with_capacity(patches.len());
            for patch in patches {
                let (row_set, delete_domain) =
                    self.load_rewrite_context(patch.start_row_id).await?;
                let delete_set = build_rewrite_delete_set(
                    &mut writer,
                    &row_set,
                    patch.delete_deltas,
                    delete_domain,
                )
                .await?;
                resolved.push(ResolvedLeafPatch::DeleteSet {
                    start_row_id: patch.start_row_id,
                    delete_set,
                });
            }
            writer.finish().await?;
            resolved
        };

        let root_height = self.read_node(self.root_block_id).await?.header.height;
        let res = self
            .rewrite_subtree_with_patches(mutable_file, self.root_block_id, &resolved, create_ts)
            .await?;
        if !res.touched {
            return Err(Error::InvalidState);
        }
        self.finalize_root_rewrite(mutable_file, root_height, res.entries, create_ts)
            .await
    }

    /// Replaces sorted logical entries keyed by existing `start_row_id`.
    pub async fn batch_replace_entries<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        patches: &[ColumnBlockEntryPatch],
        create_ts: u64,
    ) -> Result<BlockID> {
        if patches.is_empty() {
            return Ok(self.root_block_id);
        }
        if self.root_block_id == SUPER_BLOCK_ID
            || !patches_sorted_unique_by_start_row_id(patches, |patch| patch.start_row_id)
        {
            return Err(Error::InvalidArgument);
        }

        let resolved = {
            let mut writer = ColumnDeletionBlobWriter::new(mutable_file);
            let mut resolved = Vec::with_capacity(patches.len());
            for patch in patches {
                let logical = build_logical_entry_from_input(&mut writer, &patch.entry).await?;
                resolved.push(ResolvedLeafPatch::Entry {
                    start_row_id: patch.start_row_id,
                    entry: logical,
                });
            }
            writer.finish().await?;
            resolved
        };
        let root_height = self.read_node(self.root_block_id).await?.header.height;
        let res = self
            .rewrite_subtree_with_patches(mutable_file, self.root_block_id, &resolved, create_ts)
            .await?;
        if !res.touched {
            return Err(Error::InvalidState);
        }
        self.finalize_root_rewrite(mutable_file, root_height, res.entries, create_ts)
            .await
    }

    /// Appends sorted logical entries and returns the new root page id.
    pub async fn batch_insert<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[ColumnBlockEntryInput],
        new_end_row_id: RowID,
        create_ts: u64,
    ) -> Result<BlockID> {
        if entries.is_empty() {
            return Ok(self.root_block_id);
        }
        if !entry_inputs_sorted(entries)
            || entries
                .first()
                .is_none_or(|entry| entry.start_row_id() < self.end_row_id)
            || new_end_row_id < self.end_row_id
        {
            return Err(Error::InvalidArgument);
        }

        let logical_entries = {
            let mut writer = ColumnDeletionBlobWriter::new(mutable_file);
            let mut logical_entries = Vec::with_capacity(entries.len());
            for entry in entries {
                logical_entries.push(build_logical_entry_from_input(&mut writer, entry).await?);
            }
            writer.finish().await?;
            logical_entries
        };
        if self.root_block_id == SUPER_BLOCK_ID {
            return self
                .build_tree_from_logical_entries(mutable_file, &logical_entries, create_ts)
                .await;
        }

        let root_height = self.read_node(self.root_block_id).await?.header.height;
        let new_root_entries = self
            .append_rightmost_path(mutable_file, &logical_entries, create_ts)
            .await?;
        self.finalize_root_rewrite(mutable_file, root_height, new_root_entries, create_ts)
            .await
    }

    fn rewrite_subtree_with_patches<'b, M: MutableCowFile + 'b>(
        &'b self,
        mutable_file: &'b mut M,
        page_id: BlockID,
        patches: &'b [ResolvedLeafPatch],
        create_ts: u64,
    ) -> Pin<Box<dyn Future<Output = Result<NodeRewriteResult>> + 'b>> {
        Box::pin(async move {
            if patches.is_empty() {
                return Ok(NodeRewriteResult {
                    entries: Vec::new(),
                    touched: false,
                });
            }
            let node = self.read_node(page_id).await?;
            if node.is_leaf() {
                return self
                    .rewrite_leaf_with_patches(mutable_file, page_id, &node, patches, create_ts)
                    .await;
            }
            self.rewrite_branch_with_patches(mutable_file, page_id, &node, patches, create_ts)
                .await
        })
    }

    async fn rewrite_leaf_with_patches<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        page_id: BlockID,
        node: &ValidatedColumnBlockNode,
        patches: &[ResolvedLeafPatch],
        create_ts: u64,
    ) -> Result<NodeRewriteResult> {
        let mut entries = self.decode_logical_leaf_entries(node, page_id)?;
        let start_row_ids: Vec<RowID> = entries.iter().map(|entry| entry.start_row_id).collect();
        for patch in patches {
            let idx = start_row_ids
                .binary_search(&patch.start_row_id())
                .map_err(|_| Error::InvalidArgument)?;
            let mut replacement = entries[idx].clone();
            patch.apply(&mut replacement)?;
            entries[idx] = replacement;
        }
        let new_entries = self
            .write_leaf_pages_from_logical_entries(mutable_file, &entries, create_ts)
            .await?;
        self.record_obsolete_node(mutable_file, page_id)?;
        Ok(NodeRewriteResult {
            entries: new_entries,
            touched: true,
        })
    }

    async fn rewrite_branch_with_patches<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        page_id: BlockID,
        node: &ValidatedColumnBlockNode,
        patches: &[ResolvedLeafPatch],
        create_ts: u64,
    ) -> Result<NodeRewriteResult> {
        let old_entries = node.branch_entries();
        let mut combined = Vec::with_capacity(old_entries.len() + patches.len());
        let mut patch_idx = 0usize;
        let mut touched = false;
        for (child_idx, entry) in old_entries.iter().enumerate() {
            let next_start = old_entries
                .get(child_idx + 1)
                .map(|next| next.start_row_id)
                .unwrap_or(RowID::MAX);
            let start_idx = patch_idx;
            while patch_idx < patches.len() && patches[patch_idx].start_row_id() < next_start {
                patch_idx += 1;
            }
            if start_idx == patch_idx {
                combined.push(*entry);
                continue;
            }
            let child = self
                .rewrite_subtree_with_patches(
                    mutable_file,
                    entry.page_id,
                    &patches[start_idx..patch_idx],
                    create_ts,
                )
                .await?;
            if !child.touched {
                return Err(Error::InvalidState);
            }
            touched = true;
            combined.extend(child.entries);
        }
        if patch_idx != patches.len() {
            return Err(Error::InvalidArgument);
        }
        if !touched {
            return Ok(NodeRewriteResult {
                entries: Vec::new(),
                touched: false,
            });
        }
        let new_entries = self
            .write_branch_pages(mutable_file, &combined, node.header.height, create_ts)
            .await?;
        self.record_obsolete_node(mutable_file, page_id)?;
        Ok(NodeRewriteResult {
            entries: new_entries,
            touched: true,
        })
    }

    async fn build_tree_from_logical_entries<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[LogicalLeafEntry],
        create_ts: u64,
    ) -> Result<BlockID> {
        let leaf_entries = self
            .write_leaf_pages_from_logical_entries(mutable_file, entries, create_ts)
            .await?;
        self.finalize_root_rewrite(mutable_file, 0, leaf_entries, create_ts)
            .await
    }

    async fn append_rightmost_path<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[LogicalLeafEntry],
        create_ts: u64,
    ) -> Result<Vec<ColumnBlockBranchEntry>> {
        let mut path = Vec::new();
        let mut page_id = self.root_block_id;
        loop {
            let node = self.read_node(page_id).await?;
            path.push((page_id, node.header.height));
            if node.is_leaf() {
                break;
            }
            page_id = node
                .branch_entries()
                .last()
                .map(|entry| entry.page_id)
                .ok_or(Error::InvalidState)?;
        }

        let (leaf_page_id, _) = path.pop().ok_or(Error::InvalidState)?;
        let leaf_node = self.read_node(leaf_page_id).await?;
        let mut combined = self.decode_logical_leaf_entries(&leaf_node, leaf_page_id)?;
        combined.extend_from_slice(entries);
        let mut child_entries = self
            .write_leaf_pages_from_logical_entries(mutable_file, &combined, create_ts)
            .await?;
        self.record_obsolete_node(mutable_file, leaf_page_id)?;

        for (branch_page_id, height) in path.into_iter().rev() {
            let branch_node = self.read_node(branch_page_id).await?;
            let old_entries = branch_node.branch_entries();
            let last_idx = old_entries
                .len()
                .checked_sub(1)
                .ok_or(Error::InvalidState)?;
            let mut combined_entries =
                Vec::with_capacity(old_entries.len() - 1 + child_entries.len());
            combined_entries.extend_from_slice(&old_entries[..last_idx]);
            combined_entries.extend(child_entries);
            child_entries = self
                .write_branch_pages(mutable_file, &combined_entries, height, create_ts)
                .await?;
            self.record_obsolete_node(mutable_file, branch_page_id)?;
        }
        Ok(child_entries)
    }

    async fn finalize_root_rewrite<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        old_root_height: u32,
        entries: Vec<ColumnBlockBranchEntry>,
        create_ts: u64,
    ) -> Result<BlockID> {
        if entries.is_empty() {
            return Err(Error::InvalidState);
        }
        if entries.len() == 1 {
            return Ok(entries[0].page_id);
        }
        self.build_branch_levels(mutable_file, entries, old_root_height + 1, create_ts)
            .await
    }

    fn decode_logical_leaf_entries(
        &self,
        node: &ValidatedColumnBlockNode,
        page_id: BlockID,
    ) -> Result<Vec<LogicalLeafEntry>> {
        let prefixes = node.leaf_prefix_plane(self.file_kind(), page_id)?;
        let mut entries = Vec::with_capacity(prefixes.count());
        for idx in 0..prefixes.count() {
            let view = node.leaf_entry_view(idx, self.file_kind(), page_id)?;
            let row_set = decode_logical_row_set(&view, self.file_kind(), page_id)?;
            let delete_set =
                decode_logical_delete_set_base(&view, &row_set, self.file_kind(), page_id)?;
            entries.push(LogicalLeafEntry::new(
                view.start_row_id,
                view.entry_header.block_id(),
                row_set,
                delete_set,
                view.entry_header.row_shape_fingerprint(),
            ));
        }
        Ok(entries)
    }

    async fn write_leaf_pages_from_logical_entries<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[LogicalLeafEntry],
        create_ts: u64,
    ) -> Result<Vec<ColumnBlockBranchEntry>> {
        let encoded = entries
            .iter()
            .map(EncodedLeafEntry::from_logical)
            .collect::<Result<Vec<_>>>()?;
        let mut leaf_entries = Vec::new();
        let mut start = 0usize;
        while start < encoded.len() {
            let mut end = start;
            let mut selected_search_type = None;
            while end < encoded.len() {
                let search_type = select_leaf_search_type(&encoded[start..=end])?;
                let next_len = leaf_chunk_encoded_len(&encoded[start..=end], search_type)?;
                if end > start && next_len > COLUMN_BLOCK_LEAF_DATA_SIZE {
                    break;
                }
                if next_len > COLUMN_BLOCK_LEAF_DATA_SIZE {
                    return Err(Error::InvalidArgument);
                }
                selected_search_type = Some(search_type);
                end += 1;
            }
            let chunk = &encoded[start..end];
            let (page_id, mut node) =
                self.allocate_node(mutable_file, 0, chunk[0].start_row_id, create_ts)?;
            node.header.count = chunk.len() as u32;
            encode_leaf_chunk(
                node.data_mut(),
                chunk,
                selected_search_type.ok_or(Error::InvalidState)?,
            )?;
            self.write_node(mutable_file, page_id, &node).await?;
            leaf_entries.push(ColumnBlockBranchEntry {
                start_row_id: chunk[0].start_row_id,
                page_id,
            });
            start = end;
        }
        Ok(leaf_entries)
    }

    async fn write_branch_pages<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[ColumnBlockBranchEntry],
        height: u32,
        create_ts: u64,
    ) -> Result<Vec<ColumnBlockBranchEntry>> {
        let mut res = Vec::new();
        for chunk in entries.chunks(COLUMN_BLOCK_MAX_BRANCH_ENTRIES) {
            let (page_id, mut node) =
                self.allocate_node(mutable_file, height, chunk[0].start_row_id, create_ts)?;
            node.header.count = chunk.len() as u32;
            node.branch_entries_mut().copy_from_slice(chunk);
            self.write_node(mutable_file, page_id, &node).await?;
            res.push(ColumnBlockBranchEntry {
                start_row_id: chunk[0].start_row_id,
                page_id,
            });
        }
        Ok(res)
    }

    async fn build_branch_levels<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        mut entries: Vec<ColumnBlockBranchEntry>,
        mut height: u32,
        create_ts: u64,
    ) -> Result<BlockID> {
        loop {
            if entries.len() == 1 {
                return Ok(entries[0].page_id);
            }
            entries = self
                .write_branch_pages(mutable_file, &entries, height, create_ts)
                .await?;
            height += 1;
        }
    }

    /// Allocate a new node page for copy-on-write updates.
    #[inline]
    pub fn allocate_node<M: MutableCowFile>(
        &self,
        table_file: &mut M,
        height: u32,
        start_row_id: RowID,
        create_ts: u64,
    ) -> Result<(BlockID, Box<ColumnBlockNode>)> {
        let page_id = table_file.allocate_block_id()?;
        let node = ColumnBlockNode::new_boxed(height, start_row_id, create_ts);
        Ok((page_id, node))
    }

    /// Record an obsolete node page to be reclaimed after commit.
    #[inline]
    pub fn record_obsolete_node<M: MutableCowFile>(
        &self,
        table_file: &mut M,
        page_id: BlockID,
    ) -> Result<()> {
        if page_id == SUPER_BLOCK_ID {
            return Err(Error::InvalidState);
        }
        table_file.record_gc_block(page_id);
        Ok(())
    }

    async fn write_node<M: MutableCowFile>(
        &self,
        mutable_file: &M,
        page_id: BlockID,
        node: &ColumnBlockNode,
    ) -> Result<()> {
        let mut buf = DirectBuf::zeroed(COLUMN_BLOCK_PAGE_SIZE);
        let payload_start = write_page_header(buf.data_mut(), COLUMN_BLOCK_INDEX_PAGE_SPEC);
        let payload_end = payload_start + COLUMN_BLOCK_NODE_PAYLOAD_SIZE;
        let dst = &mut buf.data_mut()[payload_start..payload_end];
        dst[..COLUMN_BLOCK_HEADER_SIZE].copy_from_slice(bytes_of(&node.header));
        dst[COLUMN_BLOCK_HEADER_SIZE..COLUMN_BLOCK_HEADER_SIZE + COLUMN_BLOCK_DATA_SIZE]
            .copy_from_slice(node.data_ref());
        write_page_checksum(buf.data_mut());
        mutable_file.write_block(page_id, buf).await
    }
}

fn validate_row_ids(row_ids: &[RowID], start_row_id: RowID, end_row_id: RowID) -> Result<()> {
    if row_ids.is_empty() || start_row_id >= end_row_id {
        return Err(Error::InvalidArgument);
    }
    let mut prev = None;
    for row_id in row_ids {
        if *row_id < start_row_id || *row_id >= end_row_id {
            return Err(Error::InvalidArgument);
        }
        if let Some(prev_row_id) = prev
            && *row_id <= prev_row_id
        {
            return Err(Error::InvalidArgument);
        }
        prev = Some(*row_id);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DecodedRowSectionMetadata {
    row_count: u16,
    first_present_delta: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DecodedDeleteSectionMetadata {
    delete_codec: u8,
    delete_domain: ColumnDeleteDomain,
    del_count: u16,
    blob_ref: Option<BlobRef>,
}

fn leaf_entry_slice(
    data: &[u8],
    prefix_end: usize,
    entry_offset: u16,
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<&[u8]> {
    let offset = entry_offset as usize;
    let header_end = offset
        .checked_add(COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE)
        .ok_or_else(|| invalid_node_payload(file_kind, page_id))?;
    if offset < prefix_end || header_end > data.len() {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    let header = bytemuck::try_from_bytes::<ColumnBlockLeafEntryHeader>(&data[offset..header_end])
        .map_err(|_| invalid_node_payload(file_kind, page_id))?;
    let entry_len = header.entry_len() as usize;
    if entry_len < COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + header.row_section_len() as usize {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    let end = offset
        .checked_add(entry_len)
        .ok_or_else(|| invalid_node_payload(file_kind, page_id))?;
    if end > data.len() {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    Ok(&data[offset..end])
}

fn validate_leaf_prefixes(
    prefixes: &LeafPrefixPlane<'_>,
    data: &[u8],
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<()> {
    if prefixes.count() == 0 {
        return Ok(());
    }
    let prefix_end = prefixes.prefix_bytes_len();
    let mut ranges = Vec::with_capacity(prefixes.count());
    let mut last_end = None;
    for idx in 0..prefixes.count() {
        let prefix = prefixes
            .prefix(idx)
            .map_err(|_| invalid_node_payload(file_kind, page_id))?;
        let entry_bytes =
            leaf_entry_slice(data, prefix_end, prefix.entry_offset, file_kind, page_id)?;
        let entry_header = bytemuck::try_from_bytes::<ColumnBlockLeafEntryHeader>(
            &entry_bytes[..COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE],
        )
        .map_err(|_| invalid_node_payload(file_kind, page_id))?;
        if entry_header.block_id() == SUPER_BLOCK_ID {
            return Err(invalid_node_payload(file_kind, page_id));
        }
        if entry_header.row_id_span() == 0 {
            return Err(invalid_node_payload(file_kind, page_id));
        }
        let end_row_id = entry_header
            .end_row_id(prefix.start_row_id)
            .map_err(|_| invalid_node_payload(file_kind, page_id))?;
        if idx == 0 && prefix.start_row_id != prefixes.header_start_row_id() {
            return Err(invalid_node_payload(file_kind, page_id));
        }
        if let Some(prev_end) = last_end
            && prefix.start_row_id < prev_end
        {
            return Err(invalid_node_payload(file_kind, page_id));
        }

        let row_section_end =
            COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + entry_header.row_section_len() as usize;
        let row_section = &entry_bytes[COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE..row_section_end];
        let row_header = SectionHeader::decode(row_section)
            .map_err(|_| invalid_node_payload(file_kind, page_id))?;
        if row_header.version != COLUMN_ROW_SECTION_VERSION {
            return Err(invalid_node_payload(file_kind, page_id));
        }
        let row_meta = decode_row_section_metadata(
            row_section,
            row_header,
            entry_header.row_id_span(),
            file_kind,
            page_id,
        )?;
        if u32::from(row_meta.row_count) > entry_header.row_id_span() {
            return Err(invalid_node_payload(file_kind, page_id));
        }

        let delete_section = if row_section_end == entry_bytes.len() {
            None
        } else {
            Some(&entry_bytes[row_section_end..])
        };
        let delete_meta =
            decode_delete_section_metadata(delete_section, row_header, file_kind, page_id)?;
        if delete_meta.del_count > row_meta.row_count {
            return Err(invalid_node_payload(file_kind, page_id));
        }
        last_end = Some(end_row_id);
        ranges.push((
            prefix.entry_offset as usize,
            prefix.entry_offset as usize + entry_bytes.len(),
        ));
    }
    ranges.sort_unstable_by_key(|range| range.0);
    for pair in ranges.windows(2) {
        if pair[0].1 > pair[1].0 {
            return Err(invalid_node_payload(file_kind, page_id));
        }
    }
    Ok(())
}

fn entry_contains_row_id(
    view: &LeafEntryView<'_>,
    row_id: RowID,
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<bool> {
    Ok(resolve_row_idx_in_view(view, row_id, file_kind, page_id)?.is_some())
}

fn resolve_row_idx_in_view(
    view: &LeafEntryView<'_>,
    row_id: RowID,
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<Option<usize>> {
    let delta_u64 = match row_id.checked_sub(view.start_row_id) {
        Some(delta) => delta,
        None => return Ok(None),
    };
    if delta_u64 >= view.entry_header.row_id_span() as u64 {
        return Ok(None);
    }
    let row_meta = decode_row_section_metadata(
        view.row_section,
        view.row_header,
        view.entry_header.row_id_span(),
        file_kind,
        page_id,
    )?;
    let delta = delta_u64 as u32;
    if delta < row_meta.first_present_delta {
        return Ok(None);
    }
    match view.row_header.kind {
        COLUMN_ROW_CODEC_DENSE => Ok(Some(delta as usize)),
        COLUMN_ROW_CODEC_DELTA_LIST => {
            resolve_delta_list_row_idx(view, row_meta, delta, file_kind, page_id)
        }
        _ => Err(invalid_node_payload(file_kind, page_id)),
    }
}

fn resolve_delta_list_row_idx(
    view: &LeafEntryView<'_>,
    row_meta: DecodedRowSectionMetadata,
    delta: u32,
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<Option<usize>> {
    let deltas = decode_u32_row_deltas(
        &view.row_section[mem::size_of::<SectionHeader>()..],
        row_meta.row_count,
        view.entry_header.row_id_span(),
        row_meta.first_present_delta,
        file_kind,
        page_id,
    )?;
    Ok(deltas.binary_search(&delta).ok())
}

fn decode_logical_row_set(
    view: &LeafEntryView<'_>,
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<LogicalRowSet> {
    let row_meta = decode_row_section_metadata(
        view.row_section,
        view.row_header,
        view.entry_header.row_id_span(),
        file_kind,
        page_id,
    )?;
    match view.row_header.kind {
        COLUMN_ROW_CODEC_DENSE => Ok(LogicalRowSet::Dense {
            row_id_span: view.entry_header.row_id_span(),
        }),
        COLUMN_ROW_CODEC_DELTA_LIST => {
            let deltas = decode_u32_row_deltas(
                &view.row_section[mem::size_of::<SectionHeader>()..],
                row_meta.row_count,
                view.entry_header.row_id_span(),
                row_meta.first_present_delta,
                file_kind,
                page_id,
            )?;
            Ok(LogicalRowSet::DeltaList {
                row_id_span: view.entry_header.row_id_span(),
                first_present_delta: row_meta.first_present_delta,
                deltas,
            })
        }
        _ => Err(invalid_node_payload(file_kind, page_id)),
    }
}

fn decode_row_ids_from_row_set(
    start_row_id: RowID,
    row_set: &LogicalRowSet,
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<Vec<RowID>> {
    let mut row_ids = Vec::with_capacity(row_set.row_count());
    match row_set {
        LogicalRowSet::Dense { row_id_span } => {
            for delta in 0..*row_id_span {
                row_ids.push(
                    start_row_id
                        .checked_add(delta as RowID)
                        .ok_or_else(|| invalid_node_payload(file_kind, page_id))?,
                );
            }
        }
        LogicalRowSet::DeltaList { deltas, .. } => {
            for delta in deltas {
                row_ids.push(
                    start_row_id
                        .checked_add(*delta as RowID)
                        .ok_or_else(|| invalid_node_payload(file_kind, page_id))?,
                );
            }
        }
    }
    Ok(row_ids)
}

fn decode_logical_delete_set_base(
    view: &LeafEntryView<'_>,
    row_set: &LogicalRowSet,
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<LogicalDeleteSet> {
    let delete_meta =
        decode_delete_section_metadata(view.delete_section, view.row_header, file_kind, page_id)?;
    match delete_meta.delete_codec {
        COLUMN_DELETE_CODEC_NONE => Ok(LogicalDeleteSet::None {
            domain: delete_meta.delete_domain,
        }),
        COLUMN_DELETE_CODEC_INLINE_DELTA_LIST => {
            let bytes = view
                .delete_section
                .ok_or_else(|| invalid_node_payload(file_kind, page_id))?;
            let row_id_deltas = decode_delete_rows(
                &bytes[COLUMN_DELETE_SECTION_HEADER_SIZE..],
                delete_meta.del_count,
                delete_meta.delete_domain,
                row_set,
                file_kind,
                page_id,
            )?;
            Ok(LogicalDeleteSet::Inline {
                domain: delete_meta.delete_domain,
                row_id_deltas,
            })
        }
        COLUMN_DELETE_CODEC_EXTERNAL_BLOB => Ok(LogicalDeleteSet::External {
            domain: delete_meta.delete_domain,
            del_count: delete_meta.del_count,
            blob_ref: delete_meta.blob_ref.ok_or(Error::InvalidState)?,
            row_id_deltas: None,
        }),
        _ => Err(invalid_node_payload(file_kind, page_id)),
    }
}

fn build_leaf_entry(
    leaf_page_id: BlockID,
    view: &LeafEntryView<'_>,
    file_kind: PersistedFileKind,
) -> Result<ColumnLeafEntry> {
    let row_meta = decode_row_section_metadata(
        view.row_section,
        view.row_header,
        view.entry_header.row_id_span(),
        file_kind,
        leaf_page_id,
    )?;
    let delete_meta = decode_delete_section_metadata(
        view.delete_section,
        view.row_header,
        file_kind,
        leaf_page_id,
    )?;
    Ok(ColumnLeafEntry {
        leaf_page_id,
        start_row_id: view.start_row_id,
        block_id: view.entry_header.block_id(),
        end_row_id: view
            .entry_header
            .end_row_id(view.start_row_id)
            .map_err(|_| invalid_node_payload(file_kind, leaf_page_id))?,
        row_count: row_meta.row_count,
        del_count: delete_meta.del_count,
        row_id_span: view.entry_header.row_id_span(),
        first_present_delta: row_meta.first_present_delta,
        delete_domain: delete_meta.delete_domain,
        delete_blob_ref: delete_meta.blob_ref,
        row_shape_fingerprint: view.entry_header.row_shape_fingerprint(),
    })
}

fn build_resolved_row(
    leaf_page_id: BlockID,
    view: &LeafEntryView<'_>,
    row_idx: usize,
) -> ResolvedColumnRow {
    ResolvedColumnRow {
        leaf_page_id,
        block_id: view.entry_header.block_id(),
        row_idx,
        row_shape_fingerprint: view.entry_header.row_shape_fingerprint(),
    }
}

fn decode_default_delete_domain_from_row_header(
    row_header: SectionHeader,
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<ColumnDeleteDomain> {
    if row_header.flags != 0 {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    decode_delete_domain_or_invalid_node(row_header.aux, file_kind, page_id)
}

fn decode_row_section_metadata(
    row_section: &[u8],
    row_header: SectionHeader,
    row_id_span: u32,
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<DecodedRowSectionMetadata> {
    let _delete_domain =
        decode_default_delete_domain_from_row_header(row_header, file_kind, page_id)?;
    if row_id_span == 0 {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    match row_header.kind {
        COLUMN_ROW_CODEC_DENSE => {
            if row_section.len() != mem::size_of::<SectionHeader>() || row_id_span > u16::MAX as u32
            {
                return Err(invalid_node_payload(file_kind, page_id));
            }
            Ok(DecodedRowSectionMetadata {
                row_count: row_id_span as u16,
                first_present_delta: 0,
            })
        }
        COLUMN_ROW_CODEC_DELTA_LIST => {
            let payload = &row_section[mem::size_of::<SectionHeader>()..];
            if payload.is_empty() || !payload.len().is_multiple_of(mem::size_of::<u32>()) {
                return Err(invalid_node_payload(file_kind, page_id));
            }
            let row_count = u16::try_from(payload.len() / mem::size_of::<u32>())
                .map_err(|_| invalid_node_payload(file_kind, page_id))?;
            let first_present_delta = u32::from_le_bytes(
                payload[..mem::size_of::<u32>()]
                    .try_into()
                    .map_err(|_| invalid_node_payload(file_kind, page_id))?,
            );
            decode_u32_row_deltas(
                payload,
                row_count,
                row_id_span,
                first_present_delta,
                file_kind,
                page_id,
            )?;
            Ok(DecodedRowSectionMetadata {
                row_count,
                first_present_delta,
            })
        }
        _ => Err(invalid_node_payload(file_kind, page_id)),
    }
}

fn decode_delete_section_metadata(
    delete_section: Option<&[u8]>,
    row_header: SectionHeader,
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<DecodedDeleteSectionMetadata> {
    let default_domain =
        decode_default_delete_domain_from_row_header(row_header, file_kind, page_id)?;
    let Some(bytes) = delete_section else {
        return Ok(DecodedDeleteSectionMetadata {
            delete_codec: COLUMN_DELETE_CODEC_NONE,
            delete_domain: default_domain,
            del_count: 0,
            blob_ref: None,
        });
    };
    if bytes.len() < COLUMN_DELETE_SECTION_HEADER_SIZE {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    let header = bytemuck::try_from_bytes::<DeleteSectionHeader>(
        &bytes[..COLUMN_DELETE_SECTION_HEADER_SIZE],
    )
    .map_err(|_| invalid_node_payload(file_kind, page_id))?;
    if header.version != COLUMN_DELETE_SECTION_VERSION || header.reserved != [0; 2] {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    let delete_domain = header
        .domain()
        .map_err(|_| invalid_node_payload(file_kind, page_id))?;
    if delete_domain != default_domain {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    let del_count = header.del_count();
    if del_count == 0 {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    match header.kind {
        COLUMN_DELETE_CODEC_INLINE_DELTA_LIST => {
            let expected_len =
                COLUMN_DELETE_SECTION_HEADER_SIZE + del_count as usize * mem::size_of::<u32>();
            if header.aux != 0 || bytes.len() != expected_len {
                return Err(invalid_node_payload(file_kind, page_id));
            }
            Ok(DecodedDeleteSectionMetadata {
                delete_codec: COLUMN_DELETE_CODEC_INLINE_DELTA_LIST,
                delete_domain,
                del_count,
                blob_ref: None,
            })
        }
        COLUMN_DELETE_CODEC_EXTERNAL_BLOB => {
            if header.aux != COLUMN_AUX_BLOB_KIND_DELETE_DELTAS
                || bytes.len() != COLUMN_DELETE_SECTION_HEADER_SIZE + COLUMN_BLOB_REF_SIZE
            {
                return Err(invalid_node_payload(file_kind, page_id));
            }
            Ok(DecodedDeleteSectionMetadata {
                delete_codec: COLUMN_DELETE_CODEC_EXTERNAL_BLOB,
                delete_domain,
                del_count,
                blob_ref: Some(
                    decode_blob_ref(&bytes[COLUMN_DELETE_SECTION_HEADER_SIZE..])
                        .map_err(|_| invalid_node_payload(file_kind, page_id))?,
                ),
            })
        }
        _ => Err(invalid_node_payload(file_kind, page_id)),
    }
}

#[inline]
fn decode_delete_domain_or_invalid_node(
    raw: u8,
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<ColumnDeleteDomain> {
    ColumnDeleteDomain::decode(raw).map_err(|_| invalid_node_payload(file_kind, page_id))
}

fn encode_row_section(
    row_set: &LogicalRowSet,
    delete_domain: ColumnDeleteDomain,
) -> Result<Vec<u8>> {
    match row_set {
        LogicalRowSet::Dense { row_id_span } => {
            if *row_id_span > u16::MAX as u32 {
                return Err(Error::InvalidArgument);
            }
            Ok(SectionHeader {
                kind: COLUMN_ROW_CODEC_DENSE,
                version: COLUMN_ROW_SECTION_VERSION,
                flags: 0,
                aux: delete_domain.encode(),
            }
            .encode()
            .to_vec())
        }
        LogicalRowSet::DeltaList { deltas, .. } => {
            let mut bytes = Vec::with_capacity(4 + deltas.len() * mem::size_of::<u32>());
            bytes.extend_from_slice(
                &SectionHeader {
                    kind: COLUMN_ROW_CODEC_DELTA_LIST,
                    version: COLUMN_ROW_SECTION_VERSION,
                    flags: 0,
                    aux: delete_domain.encode(),
                }
                .encode(),
            );
            for delta in deltas {
                bytes.extend_from_slice(&delta.to_le_bytes());
            }
            Ok(bytes)
        }
    }
}

fn encode_delete_section(
    row_set: &LogicalRowSet,
    delete_set: &LogicalDeleteSet,
) -> Result<Vec<u8>> {
    match delete_set {
        LogicalDeleteSet::None { .. } => Ok(Vec::new()),
        LogicalDeleteSet::Inline {
            domain,
            row_id_deltas,
        } => {
            let delete_values = encode_delete_values(row_id_deltas, row_set, *domain)?;
            if !inline_delete_values_fit(&delete_values)? {
                return Err(Error::InvalidArgument);
            }
            let header = DeleteSectionHeader::new(
                COLUMN_DELETE_CODEC_INLINE_DELTA_LIST,
                *domain,
                storage_count_u16(delete_values.len())?,
                0,
            );
            let mut bytes = Vec::with_capacity(
                COLUMN_DELETE_SECTION_HEADER_SIZE + mem::size_of_val(delete_values.as_slice()),
            );
            bytes.extend_from_slice(bytes_of(&header));
            for value in delete_values {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Ok(bytes)
        }
        LogicalDeleteSet::External {
            domain, blob_ref, ..
        } => {
            let header = DeleteSectionHeader::new(
                COLUMN_DELETE_CODEC_EXTERNAL_BLOB,
                *domain,
                delete_set.del_count()?,
                COLUMN_AUX_BLOB_KIND_DELETE_DELTAS,
            );
            let mut bytes =
                Vec::with_capacity(COLUMN_DELETE_SECTION_HEADER_SIZE + COLUMN_BLOB_REF_SIZE);
            bytes.extend_from_slice(bytes_of(&header));
            encode_blob_ref(blob_ref, &mut bytes);
            Ok(bytes)
        }
    }
}

fn encode_leaf_chunk(
    buf: &mut [u8],
    entries: &[EncodedLeafEntry],
    search_type: ColumnBlockLeafSearchType,
) -> Result<()> {
    let leaf_start_row_id = entries.first().ok_or(Error::InvalidArgument)?.start_row_id;
    buf.fill(0);
    let (leaf_header, leaf_data) = buf.split_at_mut(COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE);
    leaf_header.copy_from_slice(bytes_of(&ColumnBlockLeafHeaderExt::new(search_type)));
    let prefix_bytes_len = entries.len() * search_type.prefix_size();
    let mut prefix_cursor = 0usize;
    let mut arena_end = leaf_data.len();
    for entry in entries {
        let entry_range = reserve_tail(&mut arena_end, entry.payload_len(), leaf_data.len())?;
        if arena_end < prefix_bytes_len {
            return Err(Error::InvalidArgument);
        }
        let entry_header = ColumnBlockLeafEntryHeader::from_encoded(entry)?;
        let entry_bytes = &mut leaf_data[entry_range.0..entry_range.1];
        entry_bytes[..COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE].copy_from_slice(bytes_of(&entry_header));
        let row_section_end = COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + entry.row_section.len();
        entry_bytes[COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE..row_section_end]
            .copy_from_slice(&entry.row_section);
        entry_bytes[row_section_end..].copy_from_slice(&entry.delete_section);

        encode_leaf_prefix(
            &mut leaf_data[prefix_cursor..prefix_cursor + search_type.prefix_size()],
            search_type,
            leaf_start_row_id,
            entry.start_row_id,
            storage_len_u16(entry_range.0)?,
        )?;
        prefix_cursor += search_type.prefix_size();
    }
    Ok(())
}

fn select_leaf_search_type(entries: &[EncodedLeafEntry]) -> Result<ColumnBlockLeafSearchType> {
    let leaf_start_row_id = entries.first().ok_or(Error::InvalidArgument)?.start_row_id;
    let mut max_delta = 0u64;
    for entry in entries {
        let delta = entry
            .start_row_id
            .checked_sub(leaf_start_row_id)
            .ok_or(Error::InvalidArgument)?;
        max_delta = max_delta.max(delta);
    }
    Ok(if max_delta <= u16::MAX as u64 {
        ColumnBlockLeafSearchType::DeltaU16
    } else if max_delta <= u32::MAX as u64 {
        ColumnBlockLeafSearchType::DeltaU32
    } else {
        ColumnBlockLeafSearchType::Plain
    })
}

fn leaf_chunk_encoded_len(
    entries: &[EncodedLeafEntry],
    search_type: ColumnBlockLeafSearchType,
) -> Result<usize> {
    let mut total = entries
        .len()
        .checked_mul(search_type.prefix_size())
        .ok_or(Error::InvalidArgument)?;
    for entry in entries {
        total = total
            .checked_add(entry.payload_len())
            .ok_or(Error::InvalidArgument)?;
    }
    Ok(total)
}

fn encode_leaf_prefix(
    dst: &mut [u8],
    search_type: ColumnBlockLeafSearchType,
    leaf_start_row_id: RowID,
    start_row_id: RowID,
    entry_offset: u16,
) -> Result<()> {
    match search_type {
        ColumnBlockLeafSearchType::Plain => {
            if dst.len() != COLUMN_BLOCK_LEAF_PREFIX_PLAIN_SIZE {
                return Err(Error::InvalidArgument);
            }
            dst[..8].copy_from_slice(&start_row_id.to_le_bytes());
            dst[8..10].copy_from_slice(&entry_offset.to_le_bytes());
        }
        ColumnBlockLeafSearchType::DeltaU32 => {
            if dst.len() != COLUMN_BLOCK_LEAF_PREFIX_U32_SIZE {
                return Err(Error::InvalidArgument);
            }
            let delta = start_row_id
                .checked_sub(leaf_start_row_id)
                .ok_or(Error::InvalidArgument)?;
            let delta = u32::try_from(delta).map_err(|_| Error::InvalidArgument)?;
            dst[..4].copy_from_slice(&delta.to_le_bytes());
            dst[4..6].copy_from_slice(&entry_offset.to_le_bytes());
        }
        ColumnBlockLeafSearchType::DeltaU16 => {
            if dst.len() != COLUMN_BLOCK_LEAF_PREFIX_U16_SIZE {
                return Err(Error::InvalidArgument);
            }
            let delta = start_row_id
                .checked_sub(leaf_start_row_id)
                .ok_or(Error::InvalidArgument)?;
            let delta = u16::try_from(delta).map_err(|_| Error::InvalidArgument)?;
            dst[..2].copy_from_slice(&delta.to_le_bytes());
            dst[2..4].copy_from_slice(&entry_offset.to_le_bytes());
        }
    }
    Ok(())
}

fn reserve_tail(arena_end: &mut usize, len: usize, total_len: usize) -> Result<(usize, usize)> {
    if len == 0 || *arena_end > total_len || len > *arena_end {
        return Err(Error::InvalidArgument);
    }
    let start = *arena_end - len;
    let end = *arena_end;
    *arena_end = start;
    Ok((start, end))
}

fn decode_blob_ref(bytes: &[u8]) -> Result<BlobRef> {
    if bytes.len() != COLUMN_BLOB_REF_SIZE {
        return Err(Error::InvalidFormat);
    }
    let start_page_id = BlockID::from(u64::from_le_bytes(bytes[0..8].try_into()?));
    let start_offset = u16::from_le_bytes(bytes[8..10].try_into()?);
    let byte_len = u32::from_le_bytes(bytes[10..14].try_into()?);
    if start_page_id == SUPER_BLOCK_ID || byte_len == 0 {
        return Err(Error::InvalidFormat);
    }
    Ok(BlobRef {
        start_page_id,
        start_offset,
        byte_len,
    })
}

fn encode_blob_ref(blob_ref: &BlobRef, out: &mut Vec<u8>) {
    out.extend_from_slice(&blob_ref.start_page_id.to_le_bytes());
    out.extend_from_slice(&blob_ref.start_offset.to_le_bytes());
    out.extend_from_slice(&blob_ref.byte_len.to_le_bytes());
}

fn decode_u32_row_deltas(
    bytes: &[u8],
    expected_count: u16,
    row_id_span: u32,
    first_present_delta: u32,
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<Vec<u32>> {
    let deltas = decode_u32_bytes_strict(bytes, expected_count)?;
    if deltas.first().copied() != Some(first_present_delta)
        || deltas.iter().any(|delta| *delta >= row_id_span)
    {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    Ok(deltas)
}

fn decode_delete_rows(
    bytes: &[u8],
    expected_count: u16,
    delete_domain: ColumnDeleteDomain,
    row_set: &LogicalRowSet,
    file_kind: PersistedFileKind,
    page_id: BlockID,
) -> Result<Vec<u32>> {
    let delete_values = decode_u32_bytes_strict(bytes, expected_count)?;
    match delete_domain {
        ColumnDeleteDomain::RowIdDelta => {
            if delete_values
                .iter()
                .any(|delta| !row_set.contains_delta(*delta))
            {
                return Err(invalid_node_payload(file_kind, page_id));
            }
            Ok(delete_values)
        }
        ColumnDeleteDomain::Ordinal => {
            if delete_values
                .iter()
                .any(|ordinal| (*ordinal as usize) >= row_set.row_count())
            {
                return Err(invalid_node_payload(file_kind, page_id));
            }
            let mut row_id_deltas = Vec::with_capacity(delete_values.len());
            for ordinal in delete_values {
                row_id_deltas.push(
                    row_set
                        .delta_for_ordinal(ordinal)
                        .ok_or_else(|| invalid_node_payload(file_kind, page_id))?,
                );
            }
            Ok(row_id_deltas)
        }
    }
}

fn decode_u32_bytes_strict(bytes: &[u8], expected_count: u16) -> Result<Vec<u32>> {
    if bytes.len() != expected_count as usize * mem::size_of::<u32>() {
        return Err(Error::InvalidFormat);
    }
    let mut res = Vec::with_capacity(expected_count as usize);
    let mut prev = None;
    for chunk in bytes.chunks_exact(mem::size_of::<u32>()) {
        let value = u32::from_le_bytes(chunk.try_into()?);
        if let Some(prev_value) = prev
            && value <= prev_value
        {
            return Err(Error::InvalidFormat);
        }
        prev = Some(value);
        res.push(value);
    }
    Ok(res)
}

fn encode_u32_values_bytes(values: &[u32]) -> Result<Vec<u8>> {
    storage_count_u16(values.len())?;
    if !delete_deltas_sorted_unique(values) {
        return Err(Error::InvalidArgument);
    }
    let mut out = Vec::with_capacity(mem::size_of_val(values));
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}

fn encode_delete_values(
    row_id_deltas: &[u32],
    row_set: &LogicalRowSet,
    delete_domain: ColumnDeleteDomain,
) -> Result<Vec<u32>> {
    if !delete_deltas_sorted_unique(row_id_deltas) {
        return Err(Error::InvalidArgument);
    }
    match delete_domain {
        ColumnDeleteDomain::RowIdDelta => {
            if row_id_deltas
                .iter()
                .any(|delta| !row_set.contains_delta(*delta))
            {
                return Err(Error::InvalidArgument);
            }
            Ok(row_id_deltas.to_vec())
        }
        ColumnDeleteDomain::Ordinal => {
            let mut ordinals = Vec::with_capacity(row_id_deltas.len());
            for delta in row_id_deltas {
                ordinals.push(
                    row_set
                        .ordinal_for_delta(*delta)
                        .ok_or(Error::InvalidArgument)?,
                );
            }
            Ok(ordinals)
        }
    }
}

async fn build_rewrite_delete_set<M: MutableCowFile>(
    writer: &mut ColumnDeletionBlobWriter<'_, M>,
    row_set: &LogicalRowSet,
    delete_deltas: &[u32],
    delete_domain: ColumnDeleteDomain,
) -> Result<LogicalDeleteSet> {
    if delete_deltas.is_empty() {
        return Ok(LogicalDeleteSet::None {
            domain: delete_domain,
        });
    }
    let delete_values = encode_delete_values(delete_deltas, row_set, delete_domain)?;
    if inline_delete_values_fit(&delete_values)? {
        return Ok(LogicalDeleteSet::Inline {
            domain: delete_domain,
            row_id_deltas: delete_deltas.to_vec(),
        });
    }
    let bytes = encode_u32_values_bytes(&delete_values)?;
    let blob_ref = writer.append_delete_payload(&bytes).await?;
    Ok(LogicalDeleteSet::External {
        domain: delete_domain,
        del_count: storage_count_u16(delete_deltas.len())?,
        blob_ref,
        row_id_deltas: Some(delete_deltas.to_vec()),
    })
}

async fn build_logical_entry_from_input<M: MutableCowFile>(
    writer: &mut ColumnDeletionBlobWriter<'_, M>,
    input: &ColumnBlockEntryInput,
) -> Result<LogicalLeafEntry> {
    if input.block_id == SUPER_BLOCK_ID {
        return Err(Error::InvalidArgument);
    }
    let row_set =
        LogicalRowSet::from_row_ids(input.start_row_id, input.end_row_id, &input.row_ids)?;
    let delete_set =
        build_rewrite_delete_set(writer, &row_set, &input.delete_deltas, input.delete_domain)
            .await?;
    Ok(LogicalLeafEntry::new(
        input.start_row_id,
        input.block_id,
        row_set,
        delete_set,
        input.row_shape_fingerprint,
    ))
}

fn inline_delete_values_fit(values: &[u32]) -> Result<bool> {
    storage_count_u16(values.len())?;
    let fits_u16 = values.iter().all(|value| *value <= u16::MAX as u32);
    Ok(if fits_u16 {
        values.len() <= LEGACY_INLINE_DELETE_U16_CAPACITY
    } else {
        values.len() <= LEGACY_INLINE_DELETE_U32_CAPACITY
    })
}

#[inline]
fn storage_count_u16(len: usize) -> Result<u16> {
    u16::try_from(len).map_err(|_| Error::InvalidArgument)
}

#[inline]
fn storage_len_u16(len: usize) -> Result<u16> {
    u16::try_from(len).map_err(|_| Error::InvalidArgument)
}

fn entry_inputs_sorted(entries: &[ColumnBlockEntryInput]) -> bool {
    entries
        .windows(2)
        .all(|pair| pair[0].start_row_id() < pair[1].start_row_id())
}

fn delete_deltas_sorted_unique(deltas: &[u32]) -> bool {
    deltas.windows(2).all(|pair| pair[0] < pair[1])
}

fn delete_delta_patches_sorted_unique(patches: &[ColumnDeleteDeltaPatch<'_>]) -> bool {
    patches_sorted_unique_by_start_row_id(patches, |patch| patch.start_row_id)
        && patches
            .iter()
            .all(|patch| delete_deltas_sorted_unique(patch.delete_deltas))
}

fn patches_sorted_unique_by_start_row_id<P, F>(patches: &[P], mut key: F) -> bool
where
    F: FnMut(&P) -> RowID,
{
    patches.windows(2).all(|pair| key(&pair[0]) < key(&pair[1]))
}

#[inline]
fn search_prefix_slice(search: std::result::Result<usize, usize>) -> Option<usize> {
    match search {
        Ok(idx) => Some(idx),
        Err(0) => None,
        Err(idx) => Some(idx - 1),
    }
}

fn search_start_row_id(prefixes: &LeafPrefixPlane<'_>, row_id: RowID) -> Result<Option<usize>> {
    prefixes.search(row_id)
}

fn search_branch_entry(entries: &[ColumnBlockBranchEntry], row_id: RowID) -> Option<usize> {
    match entries.binary_search_by_key(&row_id, |entry| entry.start_row_id) {
        Ok(idx) => Some(idx),
        Err(0) => None,
        Err(idx) => Some(idx - 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{global_readonly_pool_scope, table_readonly_pool};
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableMetadata,
    };
    use crate::file::build_test_fs;
    use crate::file::table_file::MutableTableFile;
    use crate::index::load_entry_deletion_deltas;
    use crate::value::ValKind;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    fn metadata() -> Arc<TableMetadata> {
        Arc::new(TableMetadata::new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U64,
                ColumnAttributes::empty(),
            )],
            vec![IndexSpec::new(
                "idx1",
                vec![IndexKey::new(0)],
                IndexAttributes::PK,
            )],
        ))
    }

    fn dense_entry(
        start: RowID,
        end: RowID,
        block_id: impl Into<BlockID>,
    ) -> ColumnBlockEntryInput {
        let row_ids: Vec<RowID> = (start..end).collect();
        ColumnBlockEntryShape::new(start, end, row_ids, Vec::new())
            .unwrap()
            .with_block_id(block_id)
    }

    fn sparse_entry(
        start: RowID,
        end: RowID,
        row_ids: Vec<RowID>,
        block_id: impl Into<BlockID>,
    ) -> ColumnBlockEntryInput {
        ColumnBlockEntryShape::new(start, end, row_ids, Vec::new())
            .unwrap()
            .with_block_id(block_id)
    }

    #[test]
    fn test_leaf_entry_header_roundtrip_uses_u16_lengths() {
        let encoded = EncodedLeafEntry {
            start_row_id: 10,
            block_id: BlockID::from(1001),
            row_id_span: 32,
            row_shape_fingerprint: 42,
            row_section: vec![0u8; 4 + 7 * mem::size_of::<u32>()],
            delete_section: vec![
                0u8;
                COLUMN_DELETE_SECTION_HEADER_SIZE + 2 * mem::size_of::<u32>()
            ],
        };
        let header = ColumnBlockLeafEntryHeader::from_encoded(&encoded).unwrap();
        assert_eq!(header.block_id(), 1001);
        assert_eq!(header.row_id_span(), 32);
        assert_eq!(header.entry_len(), 32 + 32 + 16);
        assert_eq!(header.row_section_len(), 32);
        assert_eq!(header.row_shape_fingerprint(), 42);
        assert_eq!(mem::size_of::<ColumnBlockLeafEntryHeader>(), 32);
        assert_eq!(mem::size_of::<ColumnBlockLeafHeaderExt>(), 8);
        assert_eq!(COLUMN_BLOCK_LEAF_HEADER_SIZE, 32);
        assert_eq!(COLUMN_BLOCK_LEAF_PREFIX_PLAIN_SIZE, 10);
        assert_eq!(COLUMN_BLOCK_LEAF_PREFIX_U32_SIZE, 6);
        assert_eq!(COLUMN_BLOCK_LEAF_PREFIX_U16_SIZE, 4);
    }

    #[test]
    fn test_row_shape_fingerprint_is_deterministic_and_canonical() {
        let dense_row_ids: Vec<RowID> = (10..20).collect();
        let sparse_row_ids = vec![12, 15, 18];

        let dense = row_shape_fingerprint_for_row_ids(10, 20, &dense_row_ids).unwrap();
        assert_eq!(
            dense,
            row_shape_fingerprint_for_row_ids(10, 20, &dense_row_ids).unwrap()
        );

        let sparse = row_shape_fingerprint_for_row_ids(10, 20, &sparse_row_ids).unwrap();
        assert_ne!(dense, sparse);
        assert_eq!(
            sparse,
            row_shape_fingerprint_for_row_ids(10, 99, &sparse_row_ids).unwrap()
        );
    }

    #[test]
    fn test_decode_delete_section_metadata_rejects_short_header() {
        let row_header = SectionHeader {
            kind: COLUMN_ROW_CODEC_DENSE,
            version: COLUMN_ROW_SECTION_VERSION,
            flags: 0,
            aux: ColumnDeleteDomain::Ordinal.encode(),
        };
        let err = decode_delete_section_metadata(
            Some(&[0u8; COLUMN_DELETE_SECTION_HEADER_SIZE - 1]),
            row_header,
            PersistedFileKind::TableFile,
            BlockID::from(42),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error::PersistedPageCorrupted {
                file_kind: PersistedFileKind::TableFile,
                page_kind: PersistedPageKind::ColumnBlockIndex,
                page_id,
                cause: PersistedPageCorruptionCause::InvalidPayload,
            } if page_id == BlockID::from(42)
        ));
    }

    #[test]
    fn test_encode_row_section_rejects_row_count_above_u16() {
        assert!(matches!(
            encode_row_section(
                &LogicalRowSet::Dense {
                    row_id_span: u16::MAX as u32 + 1,
                },
                ColumnDeleteDomain::RowIdDelta
            ),
            Err(Error::InvalidArgument)
        ));
    }

    #[test]
    fn test_logical_delete_set_rejects_delete_count_above_u16() {
        let delete_set = LogicalDeleteSet::Inline {
            domain: ColumnDeleteDomain::RowIdDelta,
            row_id_deltas: (0..=u16::MAX as u32).collect(),
        };
        assert!(matches!(
            delete_set.del_count(),
            Err(Error::InvalidArgument)
        ));
    }

    #[test]
    fn test_batch_insert_and_locate_sparse_membership() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata();
            let table = fs.create_table_file(1, metadata, false).unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 1, &table);
            let disk_pool_guard = disk_pool.pool_guard();
            let mut mutable = MutableTableFile::fork(&table);
            let index = ColumnBlockIndex::new(SUPER_BLOCK_ID, 0, &disk_pool, &disk_pool_guard);
            let entries = vec![
                sparse_entry(10, 20, vec![12, 15, 18], 1001),
                dense_entry(20, 24, 1002),
            ];
            let root_block_id = index
                .batch_insert(&mut mutable, &entries, 24, 2)
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let index = ColumnBlockIndex::new(root_block_id, 24, &disk_pool, &disk_pool_guard);
            assert!(index.locate_block(10).await.unwrap().is_none());
            assert_eq!(
                index.locate_block(12).await.unwrap().unwrap().block_id(),
                1001
            );
            assert!(index.locate_block(19).await.unwrap().is_none());
            assert_eq!(
                index.locate_block(22).await.unwrap().unwrap().block_id(),
                1002
            );
        });
    }

    async fn assert_search_type_lookup(
        entries: Vec<ColumnBlockEntryInput>,
        end_row_id: RowID,
        probe_row_id: RowID,
        expected_search_type: ColumnBlockLeafSearchType,
        expected_block_id: impl Into<BlockID>,
    ) {
        let (_temp_dir, fs) = build_test_fs();
        let metadata = metadata();
        let table = fs.create_table_file(1, metadata, false).unwrap();
        let (table, old_root) = table.commit(1, false).await.unwrap();
        drop(old_root);
        let global = global_readonly_pool_scope(64 * 1024 * 1024);
        let disk_pool = table_readonly_pool(&global, 1, &table);
        let disk_pool_guard = disk_pool.pool_guard();
        let mut mutable = MutableTableFile::fork(&table);
        let root_block_id = ColumnBlockIndex::new(SUPER_BLOCK_ID, 0, &disk_pool, &disk_pool_guard)
            .batch_insert(&mut mutable, &entries, end_row_id, 2)
            .await
            .unwrap();
        let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

        let index = ColumnBlockIndex::new(root_block_id, end_row_id, &disk_pool, &disk_pool_guard);
        let entry = index.locate_block(probe_row_id).await.unwrap().unwrap();
        assert_eq!(entry.block_id(), expected_block_id.into());
        let node = index.read_node(entry.leaf_page_id).await.unwrap();
        let header = node
            .leaf_header_ext(index.file_kind(), entry.leaf_page_id)
            .unwrap();
        assert_eq!(header.search_type().unwrap(), expected_search_type);
        assert!(
            index
                .locate_and_resolve_row(probe_row_id)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_leaf_search_type_selection_and_lookup_variants() {
        smol::block_on(async {
            let delta_u16_start = 1_000u64 + u16::MAX as u64;
            assert_search_type_lookup(
                vec![
                    dense_entry(1_000, 1_001, 1001),
                    dense_entry(delta_u16_start, delta_u16_start + 1, 1002),
                ],
                delta_u16_start + 1,
                delta_u16_start,
                ColumnBlockLeafSearchType::DeltaU16,
                1002,
            )
            .await;

            let delta_u32_start = 1_000u64 + u16::MAX as u64 + 1;
            assert_search_type_lookup(
                vec![
                    dense_entry(1_000, 1_001, 2001),
                    dense_entry(delta_u32_start, delta_u32_start + 1, 2002),
                ],
                delta_u32_start + 1,
                delta_u32_start,
                ColumnBlockLeafSearchType::DeltaU32,
                2002,
            )
            .await;

            let plain_start = 1_000u64 + u32::MAX as u64 + 1;
            assert_search_type_lookup(
                vec![
                    dense_entry(1_000, 1_001, 3001),
                    dense_entry(plain_start, plain_start + 1, 3002),
                ],
                plain_start + 1,
                plain_start,
                ColumnBlockLeafSearchType::Plain,
                3002,
            )
            .await;
        });
    }

    #[test]
    fn test_load_entry_row_ids_roundtrip_dense_and_sparse() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata();
            let table = fs.create_table_file(1, metadata, false).unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 1, &table);
            let disk_pool_guard = disk_pool.pool_guard();
            let mut mutable = MutableTableFile::fork(&table);
            let root_block_id =
                ColumnBlockIndex::new(SUPER_BLOCK_ID, 0, &disk_pool, &disk_pool_guard)
                    .batch_insert(
                        &mut mutable,
                        &[
                            dense_entry(0, 4, 1001),
                            sparse_entry(10, 20, vec![12, 15, 18], 1002),
                        ],
                        20,
                        2,
                    )
                    .await
                    .unwrap();
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let index = ColumnBlockIndex::new(root_block_id, 20, &disk_pool, &disk_pool_guard);
            let dense = index.locate_block(2).await.unwrap().unwrap();
            let sparse = index.locate_block(15).await.unwrap().unwrap();

            assert_eq!(
                index.load_entry_row_ids(&dense).await.unwrap(),
                vec![0, 1, 2, 3]
            );
            assert_eq!(
                dense.row_shape_fingerprint(),
                row_shape_fingerprint_for_row_ids(0, 4, &[0, 1, 2, 3]).unwrap()
            );
            assert_eq!(
                index.load_entry_row_ids(&sparse).await.unwrap(),
                vec![12, 15, 18]
            );
            assert_eq!(
                sparse.row_shape_fingerprint(),
                row_shape_fingerprint_for_row_ids(10, 20, &[12, 15, 18]).unwrap()
            );
        });
    }

    #[test]
    fn test_resolve_row_dense_and_sparse() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata();
            let table = fs.create_table_file(1, metadata, false).unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 1, &table);
            let disk_pool_guard = disk_pool.pool_guard();
            let mut mutable = MutableTableFile::fork(&table);
            let entries = vec![
                dense_entry(0, 4, 1001),
                sparse_entry(10, 20, vec![12, 15, 18], 1002),
            ];
            let root_block_id =
                ColumnBlockIndex::new(SUPER_BLOCK_ID, 0, &disk_pool, &disk_pool_guard)
                    .batch_insert(&mut mutable, &entries, 20, 2)
                    .await
                    .unwrap();
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let index = ColumnBlockIndex::new(root_block_id, 20, &disk_pool, &disk_pool_guard);
            let dense_entry = index.locate_block(2).await.unwrap().unwrap();
            let dense_resolved = index.resolve_row(2, &dense_entry).await.unwrap().unwrap();
            assert_eq!(dense_resolved.block_id(), 1001);
            assert_eq!(dense_resolved.row_idx(), 2);
            assert_eq!(dense_resolved.leaf_page_id(), dense_entry.leaf_page_id);
            assert_eq!(
                dense_resolved.row_shape_fingerprint(),
                dense_entry.row_shape_fingerprint()
            );

            let sparse_entry = index.locate_block(15).await.unwrap().unwrap();
            let sparse_resolved = index.resolve_row(15, &sparse_entry).await.unwrap().unwrap();
            assert_eq!(sparse_resolved.block_id(), 1002);
            assert_eq!(sparse_resolved.row_idx(), 1);
            assert_eq!(sparse_resolved.leaf_page_id(), sparse_entry.leaf_page_id);
            assert_eq!(
                sparse_resolved.row_shape_fingerprint(),
                sparse_entry.row_shape_fingerprint()
            );
            assert!(
                index
                    .resolve_row(14, &sparse_entry)
                    .await
                    .unwrap()
                    .is_none()
            );

            let one_descent = index.locate_and_resolve_row(18).await.unwrap().unwrap();
            assert_eq!(one_descent.block_id(), 1002);
            assert_eq!(one_descent.row_idx(), 2);
            assert_eq!(
                one_descent.row_shape_fingerprint(),
                sparse_entry.row_shape_fingerprint()
            );
        });
    }

    #[test]
    fn test_collect_leaf_entries_and_replace_entry() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata();
            let table = fs.create_table_file(1, metadata, false).unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 1, &table);
            let disk_pool_guard = disk_pool.pool_guard();
            let mut mutable = MutableTableFile::fork(&table);
            let root_v1 = ColumnBlockIndex::new(SUPER_BLOCK_ID, 0, &disk_pool, &disk_pool_guard)
                .batch_insert(
                    &mut mutable,
                    &[dense_entry(0, 4, 1001), dense_entry(4, 8, 1002)],
                    8,
                    2,
                )
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let mut mutable = MutableTableFile::fork(&table);
            let replacement = sparse_entry(4, 10, vec![4, 5, 8, 9], 2002);
            let root_v2 = ColumnBlockIndex::new(root_v1, 8, &disk_pool, &disk_pool_guard)
                .batch_replace_entries(
                    &mut mutable,
                    &[ColumnBlockEntryPatch {
                        start_row_id: 4,
                        entry: replacement,
                    }],
                    3,
                )
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(3, false).await.unwrap();

            let index = ColumnBlockIndex::new(root_v2, 10, &disk_pool, &disk_pool_guard);
            let entries = index.collect_leaf_entries().await.unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[1].block_id(), 2002);
            assert_eq!(entries[1].end_row_id(), 10);
            assert_eq!(entries[1].row_count(), 4);
            assert_eq!(entries[1].del_count(), 0);
            assert!(index.locate_block(7).await.unwrap().is_none());
            assert_eq!(
                index.locate_block(8).await.unwrap().unwrap().block_id(),
                2002
            );
        });
    }

    #[test]
    fn test_batch_replace_delete_deltas_roundtrip_inline() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata();
            let table = fs.create_table_file(1, metadata, false).unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 1, &table);
            let disk_pool_guard = disk_pool.pool_guard();
            let mut mutable = MutableTableFile::fork(&table);
            let root_v1 = ColumnBlockIndex::new(SUPER_BLOCK_ID, 0, &disk_pool, &disk_pool_guard)
                .batch_insert(&mut mutable, &[dense_entry(0, 8, 1001)], 8, 2)
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let mut mutable = MutableTableFile::fork(&table);
            let root_v2 = ColumnBlockIndex::new(root_v1, 8, &disk_pool, &disk_pool_guard)
                .batch_replace_delete_deltas(
                    &mut mutable,
                    &[ColumnDeleteDeltaPatch {
                        start_row_id: 0,
                        delete_deltas: &[1, 3, 6],
                    }],
                    3,
                )
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(3, false).await.unwrap();

            let index = ColumnBlockIndex::new(root_v2, 8, &disk_pool, &disk_pool_guard);
            let entry = index.locate_block(0).await.unwrap().unwrap();
            assert_eq!(entry.block_id(), 1001);
            assert_eq!(entry.row_count(), 8);
            assert_eq!(entry.del_count(), 3);
            assert!(entry.deletion_blob_ref().is_none());
            let loaded = load_entry_deletion_deltas(&index, &entry).await.unwrap();
            assert_eq!(loaded, BTreeSet::from([1u32, 3, 6]));
        });
    }

    #[test]
    fn test_batch_replace_entries_roundtrip_ordinal_delete_domain() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata();
            let table = fs.create_table_file(1, metadata, false).unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 1, &table);
            let disk_pool_guard = disk_pool.pool_guard();
            let mut mutable = MutableTableFile::fork(&table);
            let root_v1 = ColumnBlockIndex::new(SUPER_BLOCK_ID, 0, &disk_pool, &disk_pool_guard)
                .batch_insert(&mut mutable, &[dense_entry(0, 8, 1001)], 8, 2)
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let replacement = ColumnBlockEntryShape::new(0, 8, (0..8).collect(), vec![1, 3, 6])
                .unwrap()
                .with_delete_domain(ColumnDeleteDomain::Ordinal)
                .with_block_id(1001);
            let mut mutable = MutableTableFile::fork(&table);
            let root_v2 = ColumnBlockIndex::new(root_v1, 8, &disk_pool, &disk_pool_guard)
                .batch_replace_entries(
                    &mut mutable,
                    &[ColumnBlockEntryPatch {
                        start_row_id: 0,
                        entry: replacement,
                    }],
                    3,
                )
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(3, false).await.unwrap();

            let index = ColumnBlockIndex::new(root_v2, 8, &disk_pool, &disk_pool_guard);
            let entry = index.locate_block(0).await.unwrap().unwrap();
            assert_eq!(entry.delete_domain(), ColumnDeleteDomain::Ordinal);
            assert_eq!(
                load_entry_deletion_deltas(&index, &entry).await.unwrap(),
                BTreeSet::from([1u32, 3, 6])
            );
        });
    }

    #[test]
    fn test_batch_replace_delete_deltas_preserves_ordinal_domain() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata();
            let table = fs.create_table_file(1, metadata, false).unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 1, &table);
            let disk_pool_guard = disk_pool.pool_guard();

            let seed = ColumnBlockEntryShape::new(0, 8, (0..8).collect(), vec![1, 3])
                .unwrap()
                .with_delete_domain(ColumnDeleteDomain::Ordinal)
                .with_block_id(1001);
            let mut mutable = MutableTableFile::fork(&table);
            let root_v1 = ColumnBlockIndex::new(SUPER_BLOCK_ID, 0, &disk_pool, &disk_pool_guard)
                .batch_insert(&mut mutable, &[seed], 8, 2)
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let mut mutable = MutableTableFile::fork(&table);
            let root_v2 = ColumnBlockIndex::new(root_v1, 8, &disk_pool, &disk_pool_guard)
                .batch_replace_delete_deltas(
                    &mut mutable,
                    &[ColumnDeleteDeltaPatch {
                        start_row_id: 0,
                        delete_deltas: &[1, 3, 6],
                    }],
                    3,
                )
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(3, false).await.unwrap();

            let index = ColumnBlockIndex::new(root_v2, 8, &disk_pool, &disk_pool_guard);
            let entry = index.locate_block(0).await.unwrap().unwrap();
            assert_eq!(entry.delete_domain(), ColumnDeleteDomain::Ordinal);
            assert_eq!(
                load_entry_deletion_deltas(&index, &entry).await.unwrap(),
                BTreeSet::from([1u32, 3, 6])
            );
        });
    }

    #[test]
    fn test_batch_insert_splits_leaf_pages() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata();
            let table = fs.create_table_file(1, metadata, false).unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 1, &table);
            let disk_pool_guard = disk_pool.pool_guard();
            let mut mutable = MutableTableFile::fork(&table);
            let mut entries = Vec::new();
            for idx in 0..(COLUMN_BLOCK_MAX_ENTRIES + 32) as u64 {
                entries.push(dense_entry(idx * 2, idx * 2 + 2, 10_000 + idx));
            }
            let root = ColumnBlockIndex::new(SUPER_BLOCK_ID, 0, &disk_pool, &disk_pool_guard)
                .batch_insert(
                    &mut mutable,
                    &entries,
                    entries.last().unwrap().end_row_id,
                    2,
                )
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let index = ColumnBlockIndex::new(
                root,
                entries.last().unwrap().end_row_id,
                &disk_pool,
                &disk_pool_guard,
            );
            let collected = index.collect_leaf_entries().await.unwrap();
            assert_eq!(collected.len(), entries.len());
            assert_eq!(
                index.locate_block(0).await.unwrap().unwrap().block_id(),
                10_000
            );
            assert_eq!(
                index
                    .locate_block((COLUMN_BLOCK_MAX_ENTRIES as u64) * 2)
                    .await
                    .unwrap()
                    .unwrap()
                    .block_id(),
                BlockID::from(10_000 + COLUMN_BLOCK_MAX_ENTRIES as u64)
            );
        });
    }
}
