use crate::buffer::{PoolGuard, ReadonlyBlockGuard, ReadonlyBufferPool};
use crate::checksum::checksum64;
use crate::error::{
    DataIntegrityError, DataIntegrityResult, MultiDomainResultExt, ResourceError, ResourceResult,
    RuntimeError, RuntimeOrFatalResult, RuntimeOrFatalResultExt, RuntimeResult,
};
use crate::file::block_integrity::{
    BLOCK_INTEGRITY_HEADER_SIZE, BLOCK_INTEGRITY_TRAILER_SIZE, COLUMN_BLOCK_INDEX_BLOCK_SPEC,
    max_payload_len, validate_block, write_block_checksum, write_block_header,
};
use crate::file::cow_file::{COW_FILE_PAGE_SIZE, MutableCowFile, SUPER_BLOCK_ID};
use crate::file::{FileKind, SparseFile};
use crate::id::{BlockID, RowID, TableID, TrxID};
use crate::index::identity_set::{EncodedRowSet, IdentitySetSeed, RowSetRef};
use crate::index::ordinal_deletion_set::{
    OrdinalDeletionSet, OrdinalDeletionSetRef, deletion_body_bound,
};
use crate::io::DirectBuf;
use crate::layout;
use crate::lwc::MAX_LWC_ROWS;
use crate::quiescent::QuiescentGuard;
use error_stack::{Report, ResultExt};
use std::collections::BTreeSet;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::sync::Arc;
#[cfg(feature = "profiling")]
use std::sync::atomic::{AtomicUsize, Ordering};
use zerocopy::byteorder::little_endian::{U32 as LeU32, U64 as LeU64};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

/// Physical size of one persisted column block-index page.
pub(crate) const COLUMN_BLOCK_PAGE_SIZE: usize = COW_FILE_PAGE_SIZE;
/// Validated payload bytes available inside one column block-index page.
pub(crate) const COLUMN_BLOCK_NODE_PAYLOAD_SIZE: usize = max_payload_len(COLUMN_BLOCK_PAGE_SIZE);
/// Serialized byte width of [`ColumnBlockNodeHeader`].
pub(crate) const COLUMN_BLOCK_HEADER_SIZE: usize = mem::size_of::<ColumnBlockNodeHeader>();
/// Bytes available for either branch entries or leaf payload after the node header.
pub(crate) const COLUMN_BLOCK_DATA_SIZE: usize =
    COLUMN_BLOCK_NODE_PAYLOAD_SIZE - COLUMN_BLOCK_HEADER_SIZE;
/// Serialized byte width of one [`ColumnBlockBranchEntry`].
pub(crate) const COLUMN_BRANCH_ENTRY_SIZE: usize = mem::size_of::<ColumnBlockBranchEntry>();
/// Bytes occupied by the shared node header plus the leaf-only header extension.
pub(crate) const COLUMN_BLOCK_LEAF_HEADER_SIZE: usize =
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
const COLUMN_DELETE_SECTION_VERSION: u8 = 2;
const COLUMN_STANDALONE_FIXED_SIZE: usize = BLOCK_INTEGRITY_HEADER_SIZE
    + BLOCK_INTEGRITY_TRAILER_SIZE
    + COLUMN_BLOCK_LEAF_HEADER_SIZE
    + COLUMN_BLOCK_LEAF_PREFIX_U16_SIZE
    + COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE
    + mem::size_of::<SectionHeader>()
    + COLUMN_DELETE_SECTION_HEADER_SIZE;
const _: () = assert!(
    COLUMN_STANDALONE_FIXED_SIZE + 4 * MAX_LWC_ROWS + deletion_body_bound(MAX_LWC_ROWS)
        <= COLUMN_BLOCK_PAGE_SIZE
);
const BLOCK_BINDING_FORMAT_TAG: &[u8; 8] = b"LWCBIND2";

/// Maximum number of logical entries that can fit in one leaf node.
pub(crate) const COLUMN_BLOCK_MAX_ENTRIES: usize =
    COLUMN_BLOCK_LEAF_DATA_SIZE / COLUMN_BLOCK_MIN_LEAF_ENTRY_SIZE;
/// Maximum number of children that can fit in one branch node.
pub(crate) const COLUMN_BLOCK_MAX_BRANCH_ENTRIES: usize =
    COLUMN_BLOCK_DATA_SIZE / COLUMN_BRANCH_ENTRY_SIZE;

const _: () = assert!(mem::size_of::<ColumnBlockLeafHeaderExt>() == 8);
const _: () = assert!(mem::size_of::<ColumnBlockLeafEntryHeader>() == 24);
const _: () = assert!(mem::size_of::<ColumnBlockBranchEntry>() == 16);
const _: () = assert!(mem::size_of::<ColumnBlockNode>() == COLUMN_BLOCK_NODE_PAYLOAD_SIZE);

#[repr(C)]
#[derive(
    Clone, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
struct ColumnBlockLeafHeaderExt {
    search_type: u8,
    reserved: [u8; 7],
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
    fn search_type(&self) -> DataIntegrityResult<ColumnBlockLeafSearchType> {
        ColumnBlockLeafSearchType::decode(self.search_type)
    }
}

#[repr(C)]
#[derive(
    Clone, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
struct ColumnBlockLeafEntryHeader {
    block_id: [u8; 8],
    block_binding_value: [u8; 8],
    row_id_span: [u8; 4],
    entry_len: [u8; 2],
    row_section_len: [u8; 2],
}

impl ColumnBlockLeafEntryHeader {
    #[inline]
    fn block_id(&self) -> BlockID {
        BlockID::from(u64::from_le_bytes(self.block_id))
    }

    #[inline]
    fn block_binding_value(&self) -> u64 {
        u64::from_le_bytes(self.block_binding_value)
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
    fn end_row_id(&self, start_row_id: RowID) -> DataIntegrityResult<RowID> {
        start_row_id
            .checked_add(u64::from(self.row_id_span()))
            .ok_or_else(|| {
                Report::new(DataIntegrityError::InvalidPayload)
                    .attach("column block leaf row id span overflows")
            })
    }

    fn from_encoded(entry: &EncodedLeafEntry) -> Self {
        let entry_len = storage_len_u16(entry.payload_len());
        let row_section_len = storage_len_u16(entry.row_section.len());
        ColumnBlockLeafEntryHeader {
            block_id: entry.block_id.to_le_bytes(),
            block_binding_value: entry.block_binding_value.to_le_bytes(),
            row_id_span: entry.row_id_span.to_le_bytes(),
            entry_len: entry_len.to_le_bytes(),
            row_section_len: row_section_len.to_le_bytes(),
        }
    }
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
    fn decode(raw: u8) -> DataIntegrityResult<Self> {
        match raw {
            COLUMN_BLOCK_LEAF_SEARCH_TYPE_PLAIN => Ok(ColumnBlockLeafSearchType::Plain),
            COLUMN_BLOCK_LEAF_SEARCH_TYPE_DELTA_U32 => Ok(ColumnBlockLeafSearchType::DeltaU32),
            COLUMN_BLOCK_LEAF_SEARCH_TYPE_DELTA_U16 => Ok(ColumnBlockLeafSearchType::DeltaU16),
            _ => Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!("invalid column block leaf search type {raw}"))),
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

#[repr(C)]
#[derive(
    Clone, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
struct DeleteSectionHeader {
    kind: u8,
    version: u8,
    flags: [u8; 2],
    del_count: [u8; 2],
    reserved: [u8; 2],
}

impl DeleteSectionHeader {
    #[inline]
    fn new(kind: u8, del_count: u16) -> Self {
        Self {
            kind,
            version: COLUMN_DELETE_SECTION_VERSION,
            flags: [0; 2],
            del_count: del_count.to_le_bytes(),
            reserved: [0; 2],
        }
    }

    #[inline]
    fn del_count(&self) -> u16 {
        u16::from_le_bytes(self.del_count)
    }
}

/// Header stored at the beginning of each on-disk column block-index node.
#[repr(C)]
#[derive(Clone, Debug, Default, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
pub(crate) struct ColumnBlockNodeHeader {
    /// Tree height of this node. `0` denotes a leaf.
    height: LeU32,
    /// Number of encoded entries stored in the node payload.
    count: LeU32,
    /// Inclusive lower row-id bound covered by this node.
    start_row_id: LeU64,
    /// Creation timestamp associated with this copy-on-write node version.
    create_ts: LeU64,
}

impl ColumnBlockNodeHeader {
    /// Creates a persisted node header with host-order inputs encoded as little-endian fields.
    #[inline]
    pub(crate) fn new(height: u32, count: u32, start_row_id: RowID, create_ts: TrxID) -> Self {
        ColumnBlockNodeHeader {
            height: LeU32::new(height),
            count: LeU32::new(count),
            start_row_id: LeU64::new(start_row_id.as_u64()),
            create_ts: LeU64::new(create_ts.as_u64()),
        }
    }

    #[inline]
    fn height(&self) -> u32 {
        self.height.get()
    }

    #[inline]
    fn count(&self) -> u32 {
        self.count.get()
    }

    #[inline]
    fn set_count(&mut self, count: u32) {
        self.count.set(count);
    }

    #[inline]
    fn start_row_id(&self) -> RowID {
        RowID::new(self.start_row_id.get())
    }
}

/// Branch entry mapping a child lower bound to a child block id.
#[repr(C)]
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
pub(crate) struct ColumnBlockBranchEntry {
    /// Inclusive lower row-id bound routed to the child subtree.
    start_row_id: LeU64,
    /// Block id of the child node.
    block_id: LeU64,
}

impl ColumnBlockBranchEntry {
    #[inline]
    fn new(start_row_id: RowID, block_id: BlockID) -> Self {
        ColumnBlockBranchEntry {
            start_row_id: LeU64::new(start_row_id.as_u64()),
            block_id: LeU64::new(block_id.as_u64()),
        }
    }

    #[inline]
    fn start_row_id(&self) -> RowID {
        RowID::new(self.start_row_id.get())
    }

    #[inline]
    fn block_id(&self) -> BlockID {
        BlockID::from(self.block_id.get())
    }
}

trait ColumnBlockNodeRead {
    fn header_ref(&self) -> &ColumnBlockNodeHeader;

    fn data_ref(&self) -> &[u8];

    #[inline]
    fn is_leaf(&self) -> bool {
        self.header_ref().height() == 0
    }

    #[inline]
    fn branch_entries(&self) -> &[ColumnBlockBranchEntry] {
        branch_entries_from_bytes(self.data_ref(), self.header_ref().count() as usize)
    }

    #[inline]
    fn leaf_data_ref(&self) -> &[u8] {
        &self.data_ref()[COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE..]
    }
}

/// In-memory view of one persisted column block-index node payload.
///
/// Leaves store a leaf-only header extension at the front of `data`, while
/// branch nodes interpret the same region entirely as branch entries.
#[repr(C)]
#[derive(Clone)]
pub(crate) struct ColumnBlockNode {
    /// Fixed-size persisted header shared by branch and leaf nodes.
    pub(crate) header: ColumnBlockNodeHeader,
    data: [u8; COLUMN_BLOCK_DATA_SIZE],
}

impl ColumnBlockNode {
    #[inline]
    fn new_boxed(height: u32, start_row_id: RowID, create_ts: TrxID) -> Box<Self> {
        // SAFETY: `ColumnBlockNode` contains only integer and byte-array fields,
        // so the all-zero bit pattern is valid for the whole value.
        let mut node = unsafe { Box::<ColumnBlockNode>::new_zeroed().assume_init() };
        node.header = ColumnBlockNodeHeader::new(height, 0, start_row_id, create_ts);
        if height == 0 {
            // Leaf nodes reserve the first bytes of `data` for the leaf-only
            // header extension, while branch nodes use the full region for
            // branch entries. Seed a valid default so a freshly allocated leaf
            // always decodes with a valid `search_type` before leaf encoding
            // rewrites it with the actual compact prefix mode.
            let header = ColumnBlockLeafHeaderExt::new(ColumnBlockLeafSearchType::Plain);
            node.data[..COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE]
                .copy_from_slice(layout::bytes_of(&header));
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
        branch_entries_from_bytes_mut(&mut self.data, self.header.count() as usize)
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

/// Validated row-shape metadata for one logical leaf entry before the backing
/// LWC block id is assigned in the table file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ColumnBlockEntryShape {
    start_row_id: RowID,
    end_row_id: RowID,
    row_set: EncodedRowSet,
    deletions: OrdinalDeletionSet,
    block_binding_value: u64,
}

impl ColumnBlockEntryShape {
    /// Freezes identity after coverage is finalized and before LWC submission.
    pub(crate) fn new(
        table_id: TableID,
        start_row_id: RowID,
        end_row_id: RowID,
        row_ids: &[RowID],
        seeds: &[IdentitySetSeed],
    ) -> ResourceResult<Self> {
        if row_ids.len() > MAX_LWC_ROWS {
            return Err(
                Report::new(ResourceError::ColumnBlockEntryCapacityExceeded).attach(format!(
                    "physical row count {} exceeds {MAX_LWC_ROWS}",
                    row_ids.len()
                )),
            );
        }
        let row_set =
            EncodedRowSet::plan(start_row_id, end_row_id, row_ids, seeds, 4 * row_ids.len())?;
        let deletions = OrdinalDeletionSet::empty(row_set.row_count() as u16);
        let block_binding_value = calculate_block_binding_value(
            table_id,
            start_row_id,
            end_row_id,
            row_set.row_count() as u32,
        );
        Ok(ColumnBlockEntryShape {
            start_row_id,
            end_row_id,
            row_set,
            deletions,
            block_binding_value,
        })
    }

    /// Returns the inclusive lower row-id bound of this entry shape.
    #[inline]
    pub(crate) fn start_row_id(&self) -> RowID {
        self.start_row_id
    }

    /// Returns the exclusive upper row-id bound of this entry shape.
    #[inline]
    pub(crate) fn end_row_id(&self) -> RowID {
        self.end_row_id
    }

    /// Returns the block binding value for the entry shape.
    #[inline]
    pub(crate) fn block_binding_value(&self) -> u64 {
        self.block_binding_value
    }

    /// Attaches the backing LWC block id, producing a complete leaf-entry input.
    #[inline]
    pub(crate) fn with_block_id(self, block_id: impl Into<BlockID>) -> ColumnBlockEntryInput {
        ColumnBlockEntryInput {
            start_row_id: self.start_row_id,
            end_row_id: self.end_row_id,
            block_id: block_id.into(),
            row_set: self.row_set,
            deletions: self.deletions,
            block_binding_value: self.block_binding_value,
        }
    }
}

/// Fully materialized logical leaf entry used by builders and rewrite flows
/// after the backing LWC block id is known.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ColumnBlockEntryInput {
    start_row_id: RowID,
    end_row_id: RowID,
    block_id: BlockID,
    row_set: EncodedRowSet,
    deletions: OrdinalDeletionSet,
    block_binding_value: u64,
}

impl ColumnBlockEntryInput {
    /// Returns the inclusive lower row-id bound of this completed entry input.
    #[inline]
    pub(crate) fn start_row_id(&self) -> RowID {
        self.start_row_id
    }

    /// Returns the exclusive upper row-id bound of this completed entry input.
    #[inline]
    pub(crate) fn end_row_id(&self) -> RowID {
        self.end_row_id
    }
}

/// One resolved leaf entry from the persisted column block-index tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ColumnLeafEntry {
    /// Block id of the leaf node that owns this entry.
    pub(crate) leaf_block_id: BlockID,
    /// Inclusive lower row-id bound of the entry.
    pub(crate) start_row_id: RowID,
    block_id: BlockID,
    end_row_id: RowID,
    row_count: u16,
    del_count: u16,
    row_id_span: u32,
    first_present_delta: u32,
    block_binding_value: u64,
}

impl ColumnLeafEntry {
    /// Returns the persisted LWC block id.
    #[inline]
    pub(crate) fn block_id(&self) -> BlockID {
        self.block_id
    }

    /// Returns the exclusive coverage upper bound of this persisted entry.
    #[inline]
    pub(crate) fn end_row_id(&self) -> RowID {
        self.end_row_id
    }

    /// Returns the decoded persisted row count.
    #[inline]
    pub(crate) fn row_count(&self) -> u16 {
        self.row_count
    }

    /// Returns the decoded persisted delete count.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "pending dead-code audit"))]
    pub(crate) fn del_count(&self) -> u16 {
        self.del_count
    }

    /// Returns the block binding value bound to this persisted
    /// block-index leaf entry.
    #[inline]
    pub(crate) fn block_binding_value(&self) -> u64 {
        self.block_binding_value
    }
}

/// Scan-ready metadata decoded while its owning column-index leaf is resident.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ColumnBlockScanEntry {
    /// Inclusive lower row-id coverage bound.
    pub(crate) start_row_id: RowID,
    /// Exclusive upper row-id coverage bound.
    pub(crate) end_row_id: RowID,
    /// Persisted LWC block id.
    pub(crate) block_id: BlockID,
    /// Number of logical rows stored in the LWC block.
    pub(crate) row_count: u16,
    /// Persisted row-id coverage width.
    pub(crate) row_id_span: u32,
    /// Table/bounds/count binding shared with the LWC header.
    pub(crate) block_binding_value: u64,
    /// Minimal resolver used only while row-id based metadata needs ordinals.
    pub(crate) identity: EncodedRowSet,
    /// Persisted visibility base for the block.
    pub(crate) deletes: OrdinalDeletionSet,
}

/// Runtime row resolution result for one persisted columnar row lookup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedColumnRow {
    leaf_block_id: BlockID,
    block_id: BlockID,
    row_idx: usize,
    block_binding_value: u64,
    durable_deleted: bool,
}

impl ResolvedColumnRow {
    /// Returns the leaf block that produced this resolution result.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved leaf_block_id"))]
    pub(crate) fn leaf_block_id(&self) -> BlockID {
        self.leaf_block_id
    }

    /// Returns the persisted LWC block id that stores the row values.
    #[inline]
    pub(crate) fn block_id(&self) -> BlockID {
        self.block_id
    }

    /// Returns the resolved ordinal inside the persisted LWC block.
    #[inline]
    pub(crate) fn row_idx(&self) -> usize {
        self.row_idx
    }

    /// Returns the expected block binding value for the resolved
    /// persisted LWC block.
    #[inline]
    pub(crate) fn block_binding_value(&self) -> u64 {
        self.block_binding_value
    }

    /// Returns whether the row belongs to the resolved entry's durable delete set.
    ///
    /// This is committed base state, not unconditional MVCC invisibility. A
    /// surviving `ColumnDeletionBuffer` marker is newer authority and must be
    /// interpreted first; this bit is final only when that marker is absent.
    #[inline]
    pub(crate) fn durable_deleted(&self) -> bool {
        self.durable_deleted
    }
}

/// One complete ordinal deletion replacement keyed by leaf `start_row_id`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ColumnDeletionPatch<'a> {
    /// Existing leaf-entry key to rewrite.
    pub(crate) start_row_id: RowID,
    /// Replacement set with the entry's physical row count.
    pub(crate) deletions: &'a OrdinalDeletionSet,
}

type LogicalRowSet = EncodedRowSet;

#[derive(Clone, Debug, Eq, PartialEq)]
struct LogicalLeafEntry {
    start_row_id: RowID,
    block_id: BlockID,
    row_set: LogicalRowSet,
    delete_set: OrdinalDeletionSet,
    block_binding_value: u64,
}

impl LogicalLeafEntry {
    fn new(
        start_row_id: RowID,
        block_id: BlockID,
        row_set: LogicalRowSet,
        delete_set: OrdinalDeletionSet,
        block_binding_value: u64,
    ) -> Self {
        LogicalLeafEntry {
            start_row_id,
            block_id,
            row_set,
            delete_set,
            block_binding_value,
        }
    }
}

#[derive(Clone, Debug)]
struct ResolvedLeafPatch {
    start_row_id: RowID,
    delete_set: OrdinalDeletionSet,
}

impl ResolvedLeafPatch {
    #[inline]
    fn start_row_id(&self) -> RowID {
        self.start_row_id
    }

    fn apply(&self, entry: &mut LogicalLeafEntry) {
        entry.delete_set = self.delete_set.clone();
    }
}

#[derive(Clone)]
struct EncodedLeafEntry {
    start_row_id: RowID,
    block_id: BlockID,
    row_id_span: u32,
    block_binding_value: u64,
    row_section: Vec<u8>,
    delete_section: Vec<u8>,
}

impl EncodedLeafEntry {
    fn from_logical(entry: &LogicalLeafEntry) -> Self {
        let row_section = encode_row_section(&entry.row_set);
        let delete_section = encode_delete_section(&entry.delete_set);
        EncodedLeafEntry {
            start_row_id: entry.start_row_id,
            block_id: entry.block_id,
            row_id_span: entry.row_set.row_id_span(),
            block_binding_value: entry.block_binding_value,
            row_section,
            delete_section,
        }
    }

    #[inline]
    fn payload_len(&self) -> usize {
        COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + self.row_section.len() + self.delete_section.len()
    }
}

#[repr(C)]
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
    fn decode(bytes: &[u8]) -> DataIntegrityResult<Self> {
        if bytes.len() < 4 {
            return Err(
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "column block section header is too short: len={}",
                    bytes.len()
                )),
            );
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
    entry_header: &'a ColumnBlockLeafEntryHeader,
    row_section: &'a [u8],
    delete_section: Option<&'a [u8]>,
    row_header: SectionHeader,
    delete_header: Option<&'a DeleteSectionHeader>,
}

#[derive(Clone, Copy, Debug)]
struct DecodedLeafPrefix {
    start_row_id: RowID,
    entry_offset: u16,
}

#[repr(C)]
#[derive(
    Clone, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
struct ColumnBlockLeafPrefixPlain {
    start_row_id: [u8; 8],
    entry_offset: [u8; 2],
}

impl ColumnBlockLeafPrefixPlain {
    #[inline]
    fn start_row_id(&self) -> RowID {
        RowID::new(u64::from_le_bytes(self.start_row_id))
    }

    #[inline]
    fn entry_offset(&self) -> u16 {
        u16::from_le_bytes(self.entry_offset)
    }
}

#[repr(C)]
#[derive(
    Clone, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
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
#[derive(
    Clone, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
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

    fn prefix(&self, idx: usize) -> DataIntegrityResult<DecodedLeafPrefix> {
        match self {
            LeafPrefixPlane::Plain { prefixes, .. } => prefixes
                .get(idx)
                .map(|prefix| DecodedLeafPrefix {
                    start_row_id: prefix.start_row_id(),
                    entry_offset: prefix.entry_offset(),
                })
                .ok_or_else(|| {
                    Report::new(DataIntegrityError::InvalidPayload)
                        .attach("missing plain column block leaf prefix")
                }),
            LeafPrefixPlane::DeltaU32 {
                header_start_row_id,
                prefixes,
            } => prefixes
                .get(idx)
                .and_then(|prefix| {
                    header_start_row_id
                        .checked_add(u64::from(prefix.start_row_delta()))
                        .map(|start_row_id| DecodedLeafPrefix {
                            start_row_id,
                            entry_offset: prefix.entry_offset(),
                        })
                })
                .ok_or_else(|| {
                    Report::new(DataIntegrityError::InvalidPayload)
                        .attach("invalid u32-delta column block leaf prefix")
                }),
            LeafPrefixPlane::DeltaU16 {
                header_start_row_id,
                prefixes,
            } => prefixes
                .get(idx)
                .and_then(|prefix| {
                    header_start_row_id
                        .checked_add(u64::from(prefix.start_row_delta()))
                        .map(|start_row_id| DecodedLeafPrefix {
                            start_row_id,
                            entry_offset: prefix.entry_offset(),
                        })
                })
                .ok_or_else(|| {
                    Report::new(DataIntegrityError::InvalidPayload)
                        .attach("invalid u16-delta column block leaf prefix")
                }),
        }
    }

    fn search(&self, row_id: RowID) -> DataIntegrityResult<Option<usize>> {
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

    fn search_exact(&self, start_row_id: RowID) -> DataIntegrityResult<usize> {
        match self {
            LeafPrefixPlane::Plain { prefixes, .. } => prefixes
                .binary_search_by_key(&start_row_id, |prefix| prefix.start_row_id())
                .map_err(|_| {
                    Report::new(DataIntegrityError::InvalidPayload)
                        .attach("missing exact plain column block leaf prefix")
                }),
            LeafPrefixPlane::DeltaU32 {
                header_start_row_id,
                prefixes,
            } => {
                let delta = start_row_id
                    .checked_sub(*header_start_row_id)
                    .ok_or_else(|| {
                        Report::new(DataIntegrityError::InvalidPayload)
                            .attach("u32-delta column block leaf prefix underflow")
                    })?;
                let delta = u32::try_from(delta).map_err(|_| {
                    Report::new(DataIntegrityError::InvalidPayload)
                        .attach("u32-delta column block leaf prefix overflow")
                })?;
                prefixes
                    .binary_search_by_key(&delta, |prefix| prefix.start_row_delta())
                    .map_err(|_| {
                        Report::new(DataIntegrityError::InvalidPayload)
                            .attach("missing exact u32-delta column block leaf prefix")
                    })
            }
            LeafPrefixPlane::DeltaU16 {
                header_start_row_id,
                prefixes,
            } => {
                let delta = start_row_id
                    .checked_sub(*header_start_row_id)
                    .ok_or_else(|| {
                        Report::new(DataIntegrityError::InvalidPayload)
                            .attach("u16-delta column block leaf prefix underflow")
                    })?;
                let delta = u16::try_from(delta).map_err(|_| {
                    Report::new(DataIntegrityError::InvalidPayload)
                        .attach("u16-delta column block leaf prefix overflow")
                })?;
                prefixes
                    .binary_search_by_key(&delta, |prefix| prefix.start_row_delta())
                    .map_err(|_| {
                        Report::new(DataIntegrityError::InvalidPayload)
                            .attach("missing exact u16-delta column block leaf prefix")
                    })
            }
        }
    }
}

#[derive(Clone, Debug)]
struct NodeRewriteResult {
    entries: Vec<ColumnBlockBranchEntry>,
    touched: bool,
}

struct ValidatedColumnBlockNode {
    guard: ReadonlyBlockGuard,
}

impl ValidatedColumnBlockNode {
    #[inline]
    fn from_validated_guard(guard: ReadonlyBlockGuard) -> Self {
        ValidatedColumnBlockNode { guard }
    }

    fn leaf_prefix_plane(&self) -> DataIntegrityResult<LeafPrefixPlane<'_>> {
        leaf_prefix_plane_from_bytes(self.header_ref(), self.data_ref())
    }

    fn leaf_entry_view(&self, idx: usize) -> DataIntegrityResult<LeafEntryView<'_>> {
        let prefixes = self.leaf_prefix_plane()?;
        self.leaf_entry_view_with_prefixes(&prefixes, idx)
    }

    fn leaf_entry_view_with_prefixes<'n>(
        &'n self,
        prefixes: &LeafPrefixPlane<'_>,
        idx: usize,
    ) -> DataIntegrityResult<LeafEntryView<'n>> {
        let prefix = prefixes.prefix(idx).map_err(|_| invalid_node_payload())?;
        let data = self.leaf_data_ref();
        let prefix_end = prefixes.prefix_bytes_len();
        let entry_bytes = leaf_entry_slice(data, prefix_end, prefix.entry_offset)?;
        let entry_header = persisted_column_index_layout(
            layout::try_ref_from_bytes::<ColumnBlockLeafEntryHeader>(
                &entry_bytes[..COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE],
            ),
            "leaf_entry_header",
        )?;
        let row_section_end =
            COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + entry_header.row_section_len() as usize;
        let row_section = &entry_bytes[COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE..row_section_end];
        let row_header = SectionHeader::decode(row_section).map_err(|_| invalid_node_payload())?;
        if row_header.version != COLUMN_ROW_SECTION_VERSION {
            return Err(invalid_node_payload());
        }
        let delete_section = if row_section_end == entry_bytes.len() {
            None
        } else {
            Some(&entry_bytes[row_section_end..])
        };
        let delete_header = match delete_section {
            Some(bytes) => {
                if bytes.len() < COLUMN_DELETE_SECTION_HEADER_SIZE {
                    return Err(invalid_node_payload());
                }
                let header = persisted_column_index_layout(
                    layout::try_ref_from_bytes::<DeleteSectionHeader>(
                        &bytes[..COLUMN_DELETE_SECTION_HEADER_SIZE],
                    ),
                    "delete_section_header",
                )?;
                if header.version != COLUMN_DELETE_SECTION_VERSION {
                    return Err(invalid_node_payload());
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
        let payload = validated_node_payload(self.guard.page());
        layout::ref_from_bytes(&payload[..COLUMN_BLOCK_HEADER_SIZE])
    }

    #[inline]
    fn data_ref(&self) -> &[u8] {
        let payload = validated_node_payload(self.guard.page());
        &payload[COLUMN_BLOCK_HEADER_SIZE..COLUMN_BLOCK_HEADER_SIZE + COLUMN_BLOCK_DATA_SIZE]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DecodedRowSectionMetadata {
    row_count: u16,
    first_present_delta: u32,
}

/// Snapshot reader and copy-on-write rewrite façade for one persisted column
/// block-index tree root.
pub(crate) struct ColumnBlockIndex<'a> {
    file_kind: FileKind,
    file: &'a Arc<SparseFile>,
    disk_pool: &'a QuiescentGuard<ReadonlyBufferPool>,
    disk_pool_guard: &'a PoolGuard,
    root_block_id: BlockID,
    end_row_id: RowID,
    #[cfg(feature = "profiling")]
    logical_read_counter: Option<&'a AtomicUsize>,
}

impl<'a> ColumnBlockIndex<'a> {
    /// Creates a column block-index view for one root block snapshot.
    #[inline]
    pub(crate) fn new(
        root_block_id: BlockID,
        end_row_id: RowID,
        file_kind: FileKind,
        file: &'a Arc<SparseFile>,
        disk_pool: &'a QuiescentGuard<ReadonlyBufferPool>,
        disk_pool_guard: &'a PoolGuard,
    ) -> Self {
        ColumnBlockIndex {
            file_kind,
            file,
            disk_pool,
            disk_pool_guard,
            root_block_id,
            end_row_id,
            #[cfg(feature = "profiling")]
            logical_read_counter: None,
        }
    }

    /// Attaches a cache-independent counter for successful logical node reads.
    #[inline]
    #[cfg(feature = "profiling")]
    pub(crate) fn with_logical_read_counter(mut self, counter: &'a AtomicUsize) -> Self {
        self.logical_read_counter = Some(counter);
        self
    }

    /// Returns the file kind used for validation diagnostics and reads.
    #[inline]
    pub(crate) fn file_kind(&self) -> FileKind {
        self.file_kind
    }

    #[inline]
    async fn read_node(&self, block_id: BlockID) -> RuntimeOrFatalResult<ValidatedColumnBlockNode> {
        let g = self
            .disk_pool
            .read_validated_block(
                self.file_kind,
                self.file,
                self.disk_pool_guard,
                block_id,
                validate_persisted_column_block_index_page,
            )
            .await
            .change_runtime_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=read_column_block_index_node, file={}, block_id={block_id}",
                    self.file_kind
                )
            })?;
        #[cfg(feature = "profiling")]
        if let Some(counter) = self.logical_read_counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        Ok(ValidatedColumnBlockNode::from_validated_guard(g))
    }

    #[inline]
    fn node_result<T>(
        &self,
        block_id: BlockID,
        result: DataIntegrityResult<T>,
    ) -> DataIntegrityResult<T> {
        result.attach_with(|| {
            format!(
                "file={}, block=column_block_index, block_id={block_id}",
                self.file_kind
            )
        })
    }

    #[inline]
    fn read_entry_view<'n>(
        &self,
        node: &'n ValidatedColumnBlockNode,
        entry: &ColumnLeafEntry,
    ) -> RuntimeResult<LeafEntryView<'n>> {
        let prefixes = self
            .node_result(entry.leaf_block_id, node.leaf_prefix_plane())
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=read_column_block_entry, start_row_id={}",
                    entry.start_row_id
                )
            })?;
        let idx = prefixes
            .search_exact(entry.start_row_id)
            .attach_with(|| {
                format!(
                    "file={}, block=column_block_index, block_id={}",
                    self.file_kind(),
                    entry.leaf_block_id
                )
            })
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=read_column_block_entry, start_row_id={}",
                    entry.start_row_id
                )
            })?;
        self.node_result(entry.leaf_block_id, node.leaf_entry_view(idx))
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=read_column_block_entry, start_row_id={}",
                    entry.start_row_id
                )
            })
    }

    /// Finds the persisted leaf entry whose coverage contains `row_id`.
    pub(crate) async fn locate_block(
        &self,
        row_id: RowID,
    ) -> RuntimeOrFatalResult<Option<ColumnLeafEntry>> {
        if self.root_block_id == SUPER_BLOCK_ID || row_id >= self.end_row_id {
            return Ok(None);
        }
        let mut block_id = self.root_block_id;
        loop {
            let node = self.read_node(block_id).await?;
            if node.is_leaf() {
                let prefixes = self
                    .node_result(block_id, node.leaf_prefix_plane())
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| format!("operation=locate_column_block, row_id={row_id}"))?;
                let idx = match search_start_row_id(&prefixes, row_id)
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| format!("operation=locate_column_block, row_id={row_id}"))?
                {
                    Some(idx) => idx,
                    None => return Ok(None),
                };
                let view = self
                    .node_result(block_id, node.leaf_entry_view(idx))
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| format!("operation=locate_column_block, row_id={row_id}"))?;
                if !self
                    .node_result(block_id, entry_contains_row_id(&view, row_id))
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| format!("operation=locate_column_block, row_id={row_id}"))?
                {
                    return Ok(None);
                }
                return Ok(Some(
                    self.node_result(block_id, build_leaf_entry(block_id, &view))
                        .change_context(RuntimeError::IndexAccess)
                        .attach_with(|| {
                            format!("operation=locate_column_block, row_id={row_id}")
                        })?,
                ));
            }
            let entries = node.branch_entries();
            let idx = match search_branch_entry(entries, row_id) {
                Some(idx) => idx,
                None => return Ok(None),
            };
            block_id = entries[idx].block_id();
        }
    }

    /// Locates and resolves one persisted row id in a single tree descent.
    pub(crate) async fn locate_and_resolve_row(
        &self,
        row_id: RowID,
    ) -> RuntimeOrFatalResult<Option<ResolvedColumnRow>> {
        if self.root_block_id == SUPER_BLOCK_ID || row_id >= self.end_row_id {
            return Ok(None);
        }
        let mut block_id = self.root_block_id;
        loop {
            let node = self.read_node(block_id).await?;
            if node.is_leaf() {
                let prefixes = self
                    .node_result(block_id, node.leaf_prefix_plane())
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| format!("operation=resolve_column_row, row_id={row_id}"))?;
                let idx = match search_start_row_id(&prefixes, row_id)
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| format!("operation=resolve_column_row, row_id={row_id}"))?
                {
                    Some(idx) => idx,
                    None => return Ok(None),
                };
                let view = self
                    .node_result(block_id, node.leaf_entry_view(idx))
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| format!("operation=resolve_column_row, row_id={row_id}"))?;
                let row_set = row_set_in_view(&view);
                let delta = match row_id.checked_sub(view.start_row_id) {
                    Some(delta) if delta < u64::from(view.entry_header.row_id_span()) => {
                        delta as u32
                    }
                    Some(_) | None => return Ok(None),
                };
                let Some(row_idx) = row_set
                    .ordinal_for_delta(delta)
                    .map(|ordinal| ordinal as usize)
                else {
                    return Ok(None);
                };
                let durable_deleted =
                    deletions_in_view(&view, row_set.row_count()).contains(row_idx as u16);
                return Ok(Some(build_resolved_row(
                    block_id,
                    &view,
                    row_idx,
                    durable_deleted,
                )));
            }
            let entries = node.branch_entries();
            let idx = match search_branch_entry(entries, row_id) {
                Some(idx) => idx,
                None => return Ok(None),
            };
            block_id = entries[idx].block_id();
        }
    }

    /// Loads compact identity and durable ordinal deletions from one admitted leaf.
    pub(crate) async fn load_entry_identity_and_deletions(
        &self,
        entry: &ColumnLeafEntry,
    ) -> RuntimeOrFatalResult<(EncodedRowSet, OrdinalDeletionSet)> {
        let node = self.read_node(entry.leaf_block_id).await?;
        let view = self.read_entry_view(&node, entry)?;
        let identity = row_set_in_view(&view);
        let deletions = deletions_in_view(&view, identity.row_count());
        Ok((identity.to_owned(), deletions.to_owned()))
    }

    async fn load_rewrite_context(
        &self,
        start_row_id: RowID,
    ) -> RuntimeOrFatalResult<LogicalRowSet> {
        assert_ne!(
            self.root_block_id, SUPER_BLOCK_ID,
            "column block-index invariant violated: rewrite context requested from empty root, start_row_id={start_row_id}"
        );
        let mut block_id = self.root_block_id;
        loop {
            let node = self.read_node(block_id).await?;
            if node.is_leaf() {
                let prefixes = self
                    .node_result(block_id, node.leaf_prefix_plane())
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| {
                        format!(
                            "operation=load_column_rewrite_context, start_row_id={start_row_id}"
                        )
                    })?;
                let idx = prefixes
                    .search_exact(start_row_id)
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| {
                        format!(
                            "operation=load_column_rewrite_context, start_row_id={start_row_id}"
                        )
                    })?;
                let view = self
                    .node_result(block_id, node.leaf_entry_view(idx))
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| {
                        format!(
                            "operation=load_column_rewrite_context, start_row_id={start_row_id}"
                        )
                    })?;
                let row_set = self
                    .node_result(block_id, decode_logical_row_set(&view))
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| {
                        format!(
                            "operation=load_column_rewrite_context, start_row_id={start_row_id}"
                        )
                    })?;
                return Ok(row_set);
            }
            let entries = node.branch_entries();
            let idx = search_branch_entry(entries, start_row_id).unwrap_or_else(|| {
                panic!(
                    "column block-index invariant violated: branch cannot route start_row_id={start_row_id}, block_id={block_id}"
                )
            });
            block_id = entries[idx].block_id();
        }
    }

    /// Collects all leaf entries in ascending `start_row_id` order.
    pub(crate) async fn collect_leaf_entries(&self) -> RuntimeOrFatalResult<Vec<ColumnLeafEntry>> {
        if self.root_block_id == SUPER_BLOCK_ID {
            return Ok(Vec::new());
        }
        let mut stack = vec![self.root_block_id];
        let mut entries = Vec::new();
        let mut last_end = None;
        while let Some(block_id) = stack.pop() {
            let node = self.read_node(block_id).await?;
            if node.is_leaf() {
                let prefixes = self
                    .node_result(block_id, node.leaf_prefix_plane())
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=collect_column_leaf_entries")?;
                for idx in 0..prefixes.count() {
                    let view = self
                        .node_result(block_id, node.leaf_entry_view_with_prefixes(&prefixes, idx))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=collect_column_leaf_entries")?;
                    let entry = self
                        .node_result(block_id, build_leaf_entry(block_id, &view))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=collect_column_leaf_entries")?;
                    if let Some(prev_end) = last_end
                        && entry.start_row_id < prev_end
                    {
                        return Err(invalid_node_payload()
                            .attach(format!(
                                "file={}, block=column_block_index, block_id={block_id}",
                                self.file_kind()
                            ))
                            .change_context(RuntimeError::IndexAccess)
                            .attach("operation=collect_column_leaf_entries")
                            .into());
                    }
                    last_end = Some(entry.end_row_id());
                    entries.push(entry);
                }
                continue;
            }
            let branch_entries = node.branch_entries();
            for entry in branch_entries.iter().rev() {
                let child_block_id = entry.block_id();
                if child_block_id == SUPER_BLOCK_ID {
                    return Err(invalid_node_payload()
                        .attach(format!(
                            "file={}, block=column_block_index, block_id={block_id}",
                            self.file_kind()
                        ))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=collect_column_leaf_entries")
                        .into());
                }
                stack.push(child_block_id);
            }
        }
        Ok(entries)
    }

    /// Collects scan-ready cold-block metadata in ascending row-id order.
    ///
    /// Each leaf prefix plane and entry payload is decoded while its owning
    /// node is already resident. Compact ordinal deletes travel with the identity.
    pub(crate) async fn collect_scan_entries(
        &self,
    ) -> RuntimeOrFatalResult<Vec<ColumnBlockScanEntry>> {
        if self.root_block_id == SUPER_BLOCK_ID {
            return Ok(Vec::new());
        }
        let mut stack = vec![self.root_block_id];
        let mut entries = Vec::new();
        let mut last_end = None;
        while let Some(block_id) = stack.pop() {
            let node = self.read_node(block_id).await?;
            if node.is_leaf() {
                let prefixes = self
                    .node_result(block_id, node.leaf_prefix_plane())
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=collect_column_scan_entries")?;
                for idx in 0..prefixes.count() {
                    let view = self
                        .node_result(block_id, node.leaf_entry_view_with_prefixes(&prefixes, idx))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=collect_column_scan_entries")?;
                    let entry = self
                        .node_result(block_id, build_scan_entry(&view))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=collect_column_scan_entries")?;
                    if let Some(prev_end) = last_end
                        && entry.start_row_id < prev_end
                    {
                        return Err(invalid_node_payload()
                            .attach(format!(
                                "file={}, block=column_block_index, block_id={block_id}",
                                self.file_kind()
                            ))
                            .change_context(RuntimeError::IndexAccess)
                            .attach("operation=collect_column_scan_entries")
                            .into());
                    }
                    last_end = Some(entry.end_row_id);
                    entries.push(entry);
                }
                continue;
            }
            for entry in node.branch_entries().iter().rev() {
                let child_block_id = entry.block_id();
                if child_block_id == SUPER_BLOCK_ID {
                    return Err(invalid_node_payload()
                        .attach(format!(
                            "file={}, block=column_block_index, block_id={block_id}",
                            self.file_kind()
                        ))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=collect_column_scan_entries")
                        .into());
                }
                stack.push(child_block_id);
            }
        }
        Ok(entries)
    }

    /// Collect all blocks reachable from this column block-index root.
    ///
    /// The traversal validates every visited index node, leaf payload metadata,
    /// and LWC block reference before adding
    /// the block ids to `out`.
    pub(crate) async fn collect_reachable_blocks(
        &self,
        out: &mut BTreeSet<BlockID>,
    ) -> RuntimeOrFatalResult<()> {
        if self.root_block_id == SUPER_BLOCK_ID {
            return Ok(());
        }
        let mut stack = vec![self.root_block_id];
        while let Some(block_id) = stack.pop() {
            out.insert(block_id);
            let node = self.read_node(block_id).await?;
            if node.is_leaf() {
                let prefixes = self
                    .node_result(block_id, node.leaf_prefix_plane())
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=collect_column_reachable_blocks")?;
                for idx in 0..prefixes.count() {
                    let view = self
                        .node_result(block_id, node.leaf_entry_view_with_prefixes(&prefixes, idx))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=collect_column_reachable_blocks")?;
                    out.insert(view.entry_header.block_id());
                }
                continue;
            }
            let branch_entries = node.branch_entries();
            for entry in branch_entries.iter().rev() {
                let child_block_id = entry.block_id();
                if child_block_id == SUPER_BLOCK_ID {
                    return Err(invalid_node_payload()
                        .attach(format!(
                            "file={}, block=column_block_index, block_id={block_id}",
                            self.file_kind()
                        ))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=collect_column_reachable_blocks")
                        .into());
                }
                stack.push(child_block_id);
            }
        }
        Ok(())
    }

    /// Replaces complete ordinal deletion sets keyed by `start_row_id`.
    pub(crate) async fn batch_replace_deletions<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        patches: &[ColumnDeletionPatch<'_>],
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<BlockID> {
        if patches.is_empty() {
            return Ok(self.root_block_id);
        }
        assert!(
            self.root_block_id != SUPER_BLOCK_ID && deletion_patches_sorted_unique(patches),
            "column block-index invariant violated: invalid delete-delta batch, root_block_id={}, patch_count={}",
            self.root_block_id,
            patches.len()
        );

        let mut resolved = Vec::with_capacity(patches.len());
        for patch in patches {
            let row_set = self.load_rewrite_context(patch.start_row_id).await?;
            assert_eq!(
                usize::from(patch.deletions.row_count()),
                row_set.row_count(),
                "column deletion replacement physical count: start_row_id={}",
                patch.start_row_id
            );
            resolved.push(ResolvedLeafPatch {
                start_row_id: patch.start_row_id,
                delete_set: patch.deletions.clone(),
            });
        }

        let root_height = self
            .read_node(self.root_block_id)
            .await?
            .header_ref()
            .height();
        let res = self
            .rewrite_subtree_with_patches(mutable_file, self.root_block_id, &resolved, create_ts)
            .await?;
        // Every validated patch resolves through this root before rewriting,
        // so a non-empty patch set must touch at least one descendant leaf.
        assert!(
            res.touched,
            "column block-index rewrite missed validated root: root_block_id={}, patch_count={}",
            self.root_block_id,
            patches.len()
        );
        self.finalize_root_rewrite(mutable_file, root_height, res.entries, create_ts)
            .await
    }

    /// Appends sorted logical entries and returns the new root block id.
    pub(crate) async fn batch_insert<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[ColumnBlockEntryInput],
        new_end_row_id: RowID,
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<BlockID> {
        if entries.is_empty() {
            return Ok(self.root_block_id);
        }
        assert!(
            entry_inputs_sorted(entries)
                && entries
                    .first()
                    .is_some_and(|entry| entry.start_row_id() >= self.end_row_id)
                && new_end_row_id >= self.end_row_id,
            "column block-index invariant violated: invalid insert batch, root_block_id={}, entry_count={}, old_end_row_id={}, new_end_row_id={new_end_row_id}",
            self.root_block_id,
            entries.len(),
            self.end_row_id
        );

        let logical_entries: Vec<_> = entries.iter().map(build_logical_entry_from_input).collect();
        if self.root_block_id == SUPER_BLOCK_ID {
            return self
                .build_tree_from_logical_entries(mutable_file, &logical_entries, create_ts)
                .await;
        }

        let root_height = self
            .read_node(self.root_block_id)
            .await?
            .header_ref()
            .height();
        let new_root_entries = self
            .append_rightmost_path(mutable_file, &logical_entries, create_ts)
            .await?;
        self.finalize_root_rewrite(mutable_file, root_height, new_root_entries, create_ts)
            .await
    }

    fn rewrite_subtree_with_patches<'b, M: MutableCowFile + 'b>(
        &'b self,
        mutable_file: &'b mut M,
        block_id: BlockID,
        patches: &'b [ResolvedLeafPatch],
        create_ts: TrxID,
    ) -> Pin<Box<dyn Future<Output = RuntimeOrFatalResult<NodeRewriteResult>> + Send + 'b>> {
        Box::pin(async move {
            if patches.is_empty() {
                return Ok(NodeRewriteResult {
                    entries: Vec::new(),
                    touched: false,
                });
            }
            let node = self.read_node(block_id).await?;
            if node.is_leaf() {
                return self
                    .rewrite_leaf_with_patches(mutable_file, block_id, &node, patches, create_ts)
                    .await;
            }
            self.rewrite_branch_with_patches(mutable_file, block_id, &node, patches, create_ts)
                .await
        })
    }

    async fn rewrite_leaf_with_patches<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        block_id: BlockID,
        node: &ValidatedColumnBlockNode,
        patches: &[ResolvedLeafPatch],
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<NodeRewriteResult> {
        let mut entries = self.decode_logical_leaf_entries(node, block_id)?;
        let start_row_ids: Vec<RowID> = entries.iter().map(|entry| entry.start_row_id).collect();
        for patch in patches {
            let idx = start_row_ids
                .binary_search(&patch.start_row_id())
                .unwrap_or_else(|_| {
                    panic!(
                        "column block-index invariant violated: patch missed leaf, block_id={block_id}, start_row_id={}",
                        patch.start_row_id()
                    )
                });
            let mut replacement = entries[idx].clone();
            patch.apply(&mut replacement);
            entries[idx] = replacement;
        }
        let new_entries = self
            .write_leaf_pages_from_logical_entries(mutable_file, &entries, create_ts)
            .await?;
        Ok(NodeRewriteResult {
            entries: new_entries,
            touched: true,
        })
    }

    async fn rewrite_branch_with_patches<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        _block_id: BlockID,
        node: &ValidatedColumnBlockNode,
        patches: &[ResolvedLeafPatch],
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<NodeRewriteResult> {
        let old_entries = node.branch_entries();
        let mut combined = Vec::with_capacity(old_entries.len() + patches.len());
        let mut patch_idx = 0usize;
        let mut touched = false;
        for (child_idx, entry) in old_entries.iter().enumerate() {
            let next_start = old_entries
                .get(child_idx + 1)
                .map(|next| next.start_row_id())
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
                    entry.block_id(),
                    &patches[start_idx..patch_idx],
                    create_ts,
                )
                .await?;
            // The branch range above selected at least one validated patch for
            // this child, which therefore must touch its routed subtree.
            assert!(
                child.touched,
                "column block-index rewrite missed routed child: branch_block_id={}, patch_count={}",
                entry.block_id(),
                patch_idx - start_idx
            );
            touched = true;
            combined.extend(child.entries);
        }
        assert_eq!(
            patch_idx,
            patches.len(),
            "column block-index invariant violated: branch rewrite left patches unapplied"
        );
        if !touched {
            return Ok(NodeRewriteResult {
                entries: Vec::new(),
                touched: false,
            });
        }
        let new_entries = self
            .write_branch_pages(
                mutable_file,
                &combined,
                node.header_ref().height(),
                create_ts,
            )
            .await?;
        Ok(NodeRewriteResult {
            entries: new_entries,
            touched: true,
        })
    }

    async fn build_tree_from_logical_entries<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[LogicalLeafEntry],
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<BlockID> {
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
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<Vec<ColumnBlockBranchEntry>> {
        let mut path = Vec::new();
        let mut block_id = self.root_block_id;
        loop {
            let node = self.read_node(block_id).await?;
            path.push((block_id, node.header_ref().height()));
            if node.is_leaf() {
                break;
            }
            block_id = node
                .branch_entries()
                .last()
                .map(|entry| entry.block_id())
                .unwrap_or_else(|| {
                    panic!(
                        "column index-path invariant violated: rightmost branch is empty, block_id={block_id}"
                    )
                });
        }

        let (leaf_block_id, _) = path.pop().unwrap_or_else(|| {
            panic!("column index-path invariant violated: rightmost path is empty")
        });
        let leaf_node = self.read_node(leaf_block_id).await?;
        let mut combined = self.decode_logical_leaf_entries(&leaf_node, leaf_block_id)?;
        combined.extend_from_slice(entries);
        let mut child_entries = self
            .write_leaf_pages_from_logical_entries(mutable_file, &combined, create_ts)
            .await?;

        for (branch_block_id, height) in path.into_iter().rev() {
            let branch_node = self.read_node(branch_block_id).await?;
            let old_entries = branch_node.branch_entries();
            let last_idx = old_entries.len().checked_sub(1).unwrap_or_else(|| {
                panic!(
                    "column index-path invariant violated: rightmost branch is empty, block_id={branch_block_id}"
                )
            });
            let mut combined_entries =
                Vec::with_capacity(old_entries.len() - 1 + child_entries.len());
            combined_entries.extend_from_slice(&old_entries[..last_idx]);
            combined_entries.extend(child_entries);
            child_entries = self
                .write_branch_pages(mutable_file, &combined_entries, height, create_ts)
                .await?;
        }
        Ok(child_entries)
    }

    async fn finalize_root_rewrite<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        old_root_height: u32,
        entries: Vec<ColumnBlockBranchEntry>,
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<BlockID> {
        assert!(
            !entries.is_empty(),
            "column index-path invariant violated: root rewrite produced no entries, old_root_height={old_root_height}, create_ts={create_ts}"
        );
        if entries.len() == 1 {
            return Ok(entries[0].block_id());
        }
        self.build_branch_levels(mutable_file, entries, old_root_height + 1, create_ts)
            .await
    }

    fn decode_logical_leaf_entries(
        &self,
        node: &ValidatedColumnBlockNode,
        block_id: BlockID,
    ) -> RuntimeResult<Vec<LogicalLeafEntry>> {
        let prefixes = self
            .node_result(block_id, node.leaf_prefix_plane())
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!("operation=decode_column_block_leaf_entries, block_id={block_id}")
            })?;
        let mut entries = Vec::with_capacity(prefixes.count());
        for idx in 0..prefixes.count() {
            let view = self
                .node_result(block_id, node.leaf_entry_view(idx))
                .change_context(RuntimeError::IndexAccess)
                .attach_with(|| {
                    format!("operation=decode_column_block_leaf_entries, block_id={block_id}")
                })?;
            let row_set = self
                .node_result(block_id, decode_logical_row_set(&view))
                .change_context(RuntimeError::IndexAccess)
                .attach_with(|| {
                    format!("operation=decode_column_block_leaf_entries, block_id={block_id}")
                })?;
            let delete_set = self
                .node_result(
                    block_id,
                    decode_logical_delete_set_base(&view, row_set.as_ref()),
                )
                .change_context(RuntimeError::IndexAccess)
                .attach_with(|| {
                    format!("operation=decode_column_block_leaf_entries, block_id={block_id}")
                })?;
            entries.push(LogicalLeafEntry::new(
                view.start_row_id,
                view.entry_header.block_id(),
                row_set,
                delete_set,
                view.entry_header.block_binding_value(),
            ));
        }
        Ok(entries)
    }

    async fn write_leaf_pages_from_logical_entries<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[LogicalLeafEntry],
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<Vec<ColumnBlockBranchEntry>> {
        let encoded = entries
            .iter()
            .map(EncodedLeafEntry::from_logical)
            .collect::<Vec<_>>();
        let mut leaf_entries = Vec::new();
        let mut start = 0usize;
        while start < encoded.len() {
            let mut end = start;
            let mut selected_search_type = None;
            while end < encoded.len() {
                let search_type = select_leaf_search_type(&encoded[start..=end]);
                let next_len = leaf_chunk_encoded_len(&encoded[start..=end], search_type);
                if end > start && next_len > COLUMN_BLOCK_LEAF_DATA_SIZE {
                    break;
                }
                if next_len > COLUMN_BLOCK_LEAF_DATA_SIZE {
                    return Err(Report::new(ResourceError::ColumnBlockEntryCapacityExceeded)
                        .attach(format!(
                            "encoded_len={next_len}, capacity={COLUMN_BLOCK_LEAF_DATA_SIZE}"
                        ))
                        .change_context(RuntimeError::IndexAccess)
                        .attach("operation=write_column_block_leaf")
                        .into());
                }
                selected_search_type = Some(search_type);
                end += 1;
            }
            let chunk = &encoded[start..end];
            let (block_id, mut node) =
                self.allocate_node(mutable_file, 0, chunk[0].start_row_id, create_ts)?;
            node.header.set_count(chunk.len() as u32);
            let search_type = selected_search_type.unwrap_or_else(|| {
                panic!(
                    "column block-index invariant violated: leaf search type is missing, start_row_id={}, chunk_len={}",
                    chunk[0].start_row_id,
                    chunk.len()
                )
            });
            encode_leaf_chunk(node.data_mut(), chunk, search_type);
            self.write_node(mutable_file, block_id, &node).await?;
            leaf_entries.push(ColumnBlockBranchEntry::new(chunk[0].start_row_id, block_id));
            start = end;
        }
        Ok(leaf_entries)
    }

    async fn write_branch_pages<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[ColumnBlockBranchEntry],
        height: u32,
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<Vec<ColumnBlockBranchEntry>> {
        let mut res = Vec::new();
        for chunk in entries.chunks(COLUMN_BLOCK_MAX_BRANCH_ENTRIES) {
            let (block_id, mut node) =
                self.allocate_node(mutable_file, height, chunk[0].start_row_id(), create_ts)?;
            node.header.set_count(chunk.len() as u32);
            node.branch_entries_mut().copy_from_slice(chunk);
            self.write_node(mutable_file, block_id, &node).await?;
            res.push(ColumnBlockBranchEntry::new(
                chunk[0].start_row_id(),
                block_id,
            ));
        }
        Ok(res)
    }

    async fn build_branch_levels<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        mut entries: Vec<ColumnBlockBranchEntry>,
        mut height: u32,
        create_ts: TrxID,
    ) -> RuntimeOrFatalResult<BlockID> {
        loop {
            if entries.len() == 1 {
                return Ok(entries[0].block_id());
            }
            entries = self
                .write_branch_pages(mutable_file, &entries, height, create_ts)
                .await?;
            height += 1;
        }
    }

    /// Allocate a new node block for copy-on-write updates.
    #[inline]
    pub(crate) fn allocate_node<M: MutableCowFile>(
        &self,
        table_file: &mut M,
        height: u32,
        start_row_id: RowID,
        create_ts: TrxID,
    ) -> RuntimeResult<(BlockID, Box<ColumnBlockNode>)> {
        let block_id = table_file
            .allocate_block()
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=allocate_column_block_index_node, height={height}, start_row_id={start_row_id}"
                )
            })?;
        let node = ColumnBlockNode::new_boxed(height, start_row_id, create_ts);
        Ok((block_id, node))
    }

    async fn write_node<M: MutableCowFile>(
        &self,
        mutable_file: &M,
        block_id: BlockID,
        node: &ColumnBlockNode,
    ) -> RuntimeOrFatalResult<()> {
        let mut buf = DirectBuf::zeroed(COLUMN_BLOCK_PAGE_SIZE);
        let payload_start = write_block_header(buf.data_mut(), COLUMN_BLOCK_INDEX_BLOCK_SPEC);
        let payload_end = payload_start + COLUMN_BLOCK_NODE_PAYLOAD_SIZE;
        let dst = &mut buf.data_mut()[payload_start..payload_end];
        dst[..COLUMN_BLOCK_HEADER_SIZE].copy_from_slice(layout::bytes_of(&node.header));
        dst[COLUMN_BLOCK_HEADER_SIZE..COLUMN_BLOCK_HEADER_SIZE + COLUMN_BLOCK_DATA_SIZE]
            .copy_from_slice(node.data_ref());
        write_block_checksum(buf.data_mut());
        mutable_file
            .write_block(block_id, buf)
            .await
            .map_err(|bridge| bridge.into_runtime_or_fatal(RuntimeError::IndexAccess))
            .attach("write column block node")
    }
}

/// Validates one persisted column block-index page.
#[inline]
pub(crate) fn validate_persisted_column_block_index_page(
    page: &[u8],
    file_kind: FileKind,
    block_id: BlockID,
) -> DataIntegrityResult<()> {
    validate_node_payload(page)
        .attach_with(|| format!("file={file_kind}, block=column_block_index, block_id={block_id}"))
        .map(|_| ())
}

#[inline]
fn invalid_node_payload() -> Report<DataIntegrityError> {
    Report::new(DataIntegrityError::InvalidPayload)
}

#[inline]
fn persisted_column_index_layout<T>(
    result: layout::LayoutResult<T>,
    field: &'static str,
) -> DataIntegrityResult<T> {
    result
        .change_context(DataIntegrityError::InvalidPayload)
        .attach_with(|| format!("format=column_block_index, field={field}"))
}

#[inline]
fn validate_node_payload(page: &[u8]) -> DataIntegrityResult<&[u8]> {
    if page.len() != COLUMN_BLOCK_PAGE_SIZE {
        return Err(
            invalid_node_payload().attach("column block-index page has invalid physical length")
        );
    }
    let payload = validate_block(page, COLUMN_BLOCK_INDEX_BLOCK_SPEC)?;
    let header = persisted_column_index_layout(
        layout::try_ref_from_bytes::<ColumnBlockNodeHeader>(&payload[..COLUMN_BLOCK_HEADER_SIZE]),
        "node_header",
    )?;
    let count = header.count() as usize;
    if count == 0
        || (header.height() == 0 && count > COLUMN_BLOCK_MAX_ENTRIES)
        || (header.height() > 0 && count > COLUMN_BLOCK_MAX_BRANCH_ENTRIES)
    {
        return Err(invalid_node_payload());
    }
    let data = &payload[COLUMN_BLOCK_HEADER_SIZE..];
    if header.height() == 0 {
        let prefixes = leaf_prefix_plane_from_bytes(header, data)?;
        validate_leaf_prefixes(&prefixes, &data[COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE..])?;
    } else {
        let entries = branch_entries_from_bytes(data, count);
        if entries[0].start_row_id() != header.start_row_id()
            || entries.iter().any(|e| e.block_id() == SUPER_BLOCK_ID)
            || entries
                .windows(2)
                .any(|p| p[0].start_row_id() >= p[1].start_row_id())
            || data[count * COLUMN_BRANCH_ENTRY_SIZE..]
                .iter()
                .any(|b| *b != 0)
        {
            return Err(invalid_node_payload());
        }
    }
    Ok(payload)
}

#[inline]
fn validated_node_payload(page: &[u8]) -> &[u8] {
    let payload_start = BLOCK_INTEGRITY_HEADER_SIZE;
    &page[payload_start..payload_start + COLUMN_BLOCK_NODE_PAYLOAD_SIZE]
}

#[inline]
fn branch_entries_from_bytes(data: &[u8], count: usize) -> &[ColumnBlockBranchEntry] {
    let bytes_len = count * mem::size_of::<ColumnBlockBranchEntry>();
    layout::slice_from_bytes(&data[..bytes_len])
}

#[inline]
fn branch_entries_from_bytes_mut(data: &mut [u8], count: usize) -> &mut [ColumnBlockBranchEntry] {
    let bytes_len = count * mem::size_of::<ColumnBlockBranchEntry>();
    layout::slice_from_bytes_mut(&mut data[..bytes_len])
}

/// Binds an index entry and LWC block to their table, coverage, and row count.
/// Interior membership, values, deletion state, and the physical codec are not
/// part of this signature. Finalize coverage before building either object.
fn calculate_block_binding_value(
    table_id: TableID,
    start_row_id: RowID,
    end_row_id: RowID,
    row_count: u32,
) -> u64 {
    let mut payload = [0u8; 36];
    payload[..8].copy_from_slice(BLOCK_BINDING_FORMAT_TAG);
    payload[8..16].copy_from_slice(&table_id.as_u64().to_le_bytes());
    payload[16..24].copy_from_slice(&start_row_id.to_le_bytes());
    payload[24..32].copy_from_slice(&end_row_id.to_le_bytes());
    payload[32..].copy_from_slice(&row_count.to_le_bytes());
    checksum64(&payload)
}

fn leaf_entry_slice(
    data: &[u8],
    prefix_end: usize,
    entry_offset: u16,
) -> DataIntegrityResult<&[u8]> {
    let offset = entry_offset as usize;
    let header_end = offset
        .checked_add(COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE)
        .ok_or_else(invalid_node_payload)?;
    if offset < prefix_end || header_end > data.len() {
        return Err(invalid_node_payload());
    }
    let header = persisted_column_index_layout(
        layout::try_ref_from_bytes::<ColumnBlockLeafEntryHeader>(&data[offset..header_end]),
        "leaf_entry_header",
    )?;
    let entry_len = header.entry_len() as usize;
    if entry_len < COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + header.row_section_len() as usize {
        return Err(invalid_node_payload());
    }
    let end = offset
        .checked_add(entry_len)
        .ok_or_else(invalid_node_payload)?;
    if end > data.len() {
        return Err(invalid_node_payload());
    }
    Ok(&data[offset..end])
}

fn leaf_prefix_plane_from_bytes<'a>(
    header: &ColumnBlockNodeHeader,
    node_data: &'a [u8],
) -> DataIntegrityResult<LeafPrefixPlane<'a>> {
    let ext = persisted_column_index_layout(
        layout::try_ref_from_bytes::<ColumnBlockLeafHeaderExt>(
            &node_data[..COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE],
        ),
        "leaf_header_extension",
    )?;
    if ext.reserved != [0; 7] {
        return Err(invalid_node_payload());
    }
    let search_type = ext.search_type()?;
    let data = &node_data[COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE..];
    let count = header.count() as usize;
    let prefix_bytes_len = count
        .checked_mul(search_type.prefix_size())
        .ok_or_else(invalid_node_payload)?;
    if prefix_bytes_len > data.len() {
        return Err(invalid_node_payload());
    }
    let prefix_bytes = &data[..prefix_bytes_len];
    let plane = match search_type {
        ColumnBlockLeafSearchType::Plain => LeafPrefixPlane::Plain {
            header_start_row_id: header.start_row_id(),
            prefixes: persisted_column_index_layout(
                layout::try_slice_from_bytes(prefix_bytes),
                "plain_leaf_prefixes",
            )?,
        },
        ColumnBlockLeafSearchType::DeltaU32 => LeafPrefixPlane::DeltaU32 {
            header_start_row_id: header.start_row_id(),
            prefixes: persisted_column_index_layout(
                layout::try_slice_from_bytes(prefix_bytes),
                "u32_delta_leaf_prefixes",
            )?,
        },
        ColumnBlockLeafSearchType::DeltaU16 => LeafPrefixPlane::DeltaU16 {
            header_start_row_id: header.start_row_id(),
            prefixes: persisted_column_index_layout(
                layout::try_slice_from_bytes(prefix_bytes),
                "u16_delta_leaf_prefixes",
            )?,
        },
    };
    Ok(plane)
}

fn validate_leaf_prefixes(prefixes: &LeafPrefixPlane<'_>, data: &[u8]) -> DataIntegrityResult<()> {
    if prefixes.count() == 0 {
        return Ok(());
    }
    let prefix_end = prefixes.prefix_bytes_len();
    let mut ranges = Vec::with_capacity(prefixes.count());
    let mut last_end = None;
    for idx in 0..prefixes.count() {
        let prefix = prefixes.prefix(idx).map_err(|_| invalid_node_payload())?;
        let entry_bytes = leaf_entry_slice(data, prefix_end, prefix.entry_offset)?;
        let entry_header = persisted_column_index_layout(
            layout::try_ref_from_bytes::<ColumnBlockLeafEntryHeader>(
                &entry_bytes[..COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE],
            ),
            "leaf_entry_header",
        )?;
        if entry_header.block_id() == SUPER_BLOCK_ID {
            return Err(invalid_node_payload());
        }
        if entry_header.row_id_span() == 0 {
            return Err(invalid_node_payload());
        }
        let end_row_id = entry_header
            .end_row_id(prefix.start_row_id)
            .map_err(|_| invalid_node_payload())?;
        if idx == 0 && prefix.start_row_id != prefixes.header_start_row_id() {
            return Err(invalid_node_payload());
        }
        if let Some(prev_end) = last_end
            && prefix.start_row_id < prev_end
        {
            return Err(invalid_node_payload());
        }

        let row_section_end =
            COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + entry_header.row_section_len() as usize;
        let row_section = &entry_bytes[COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE..row_section_end];
        let row_header = SectionHeader::decode(row_section).map_err(|_| invalid_node_payload())?;
        if row_header.version != COLUMN_ROW_SECTION_VERSION {
            return Err(invalid_node_payload());
        }
        if row_header.flags != 0 || row_header.aux != 0 {
            return Err(invalid_node_payload());
        }
        let rows = RowSetRef::validate(
            row_header.kind,
            &row_section[4..],
            entry_header.row_id_span(),
        )?;
        let row_meta = DecodedRowSectionMetadata {
            row_count: rows.row_count(),
            first_present_delta: rows.delta_for_ordinal(0).ok_or_else(invalid_node_payload)?,
        };
        if usize::from(row_meta.row_count) > MAX_LWC_ROWS
            || row_section.len() - mem::size_of::<SectionHeader>()
                > 4 * usize::from(row_meta.row_count)
            || u32::from(row_meta.row_count) > entry_header.row_id_span()
        {
            return Err(invalid_node_payload());
        }

        let delete_section = if row_section_end == entry_bytes.len() {
            None
        } else {
            Some(&entry_bytes[row_section_end..])
        };
        validate_delete_section(delete_section, row_meta.row_count)?;
        last_end = Some(end_row_id);
        ranges.push((
            prefix.entry_offset as usize,
            prefix.entry_offset as usize + entry_bytes.len(),
        ));
    }
    ranges.sort_unstable_by_key(|range| range.0);
    let mut end = prefix_end;
    for (start, next_end) in ranges {
        if start < end || data[end..start].iter().any(|b| *b != 0) {
            return Err(invalid_node_payload());
        }
        end = next_end;
    }
    if data[end..].iter().any(|b| *b != 0) {
        return Err(invalid_node_payload());
    }
    Ok(())
}

#[inline]
fn entry_contains_row_id(view: &LeafEntryView<'_>, row_id: RowID) -> DataIntegrityResult<bool> {
    Ok(resolve_row_idx_in_view(view, row_id)?.is_some())
}

#[inline]
fn resolve_row_idx_in_view(
    view: &LeafEntryView<'_>,
    row_id: RowID,
) -> DataIntegrityResult<Option<usize>> {
    let Some(delta) = row_id
        .checked_sub(view.start_row_id)
        .and_then(|d| u32::try_from(d).ok())
    else {
        return Ok(None);
    };
    Ok(row_set_in_view(view)
        .ordinal_for_delta(delta)
        .map(usize::from))
}

#[inline]
fn row_set_in_view<'a>(view: &LeafEntryView<'a>) -> RowSetRef<'a> {
    RowSetRef::from_admitted(
        view.row_header.kind,
        &view.row_section[mem::size_of::<SectionHeader>()..],
        view.entry_header.row_id_span(),
    )
}

fn decode_logical_row_set(view: &LeafEntryView<'_>) -> DataIntegrityResult<LogicalRowSet> {
    Ok(row_set_in_view(view).to_owned())
}

#[inline]
fn deletions_in_view<'a>(view: &LeafEntryView<'a>, row_count: u16) -> OrdinalDeletionSetRef<'a> {
    let section = view
        .delete_header
        .zip(view.delete_section)
        .map(|(header, bytes)| (header.kind, &bytes[COLUMN_DELETE_SECTION_HEADER_SIZE..]));
    OrdinalDeletionSetRef::from_admitted(row_count, section)
}

fn decode_logical_delete_set_base(
    view: &LeafEntryView<'_>,
    row_set: RowSetRef<'_>,
) -> DataIntegrityResult<OrdinalDeletionSet> {
    Ok(deletions_in_view(view, row_set.row_count()).to_owned())
}

fn build_leaf_entry(
    leaf_block_id: BlockID,
    view: &LeafEntryView<'_>,
) -> DataIntegrityResult<ColumnLeafEntry> {
    let row_meta = decode_row_section_metadata(
        view.row_section,
        view.row_header,
        view.entry_header.row_id_span(),
    )?;
    let deletions = deletions_in_view(view, row_meta.row_count);
    Ok(ColumnLeafEntry {
        leaf_block_id,
        start_row_id: view.start_row_id,
        block_id: view.entry_header.block_id(),
        end_row_id: view
            .entry_header
            .end_row_id(view.start_row_id)
            .map_err(|_| invalid_node_payload())?,
        row_count: row_meta.row_count,
        del_count: deletions.len() as u16,
        row_id_span: view.entry_header.row_id_span(),
        first_present_delta: row_meta.first_present_delta,
        block_binding_value: view.entry_header.block_binding_value(),
    })
}

fn build_scan_entry(view: &LeafEntryView<'_>) -> DataIntegrityResult<ColumnBlockScanEntry> {
    let row_set = decode_logical_row_set(view)?;
    let row_count = u16::try_from(row_set.row_count()).map_err(|_| invalid_node_payload())?;
    let deletes = deletions_in_view(view, row_count).to_owned();
    Ok(ColumnBlockScanEntry {
        start_row_id: view.start_row_id,
        end_row_id: view
            .entry_header
            .end_row_id(view.start_row_id)
            .map_err(|_| invalid_node_payload())?,
        block_id: view.entry_header.block_id(),
        row_count,
        row_id_span: view.entry_header.row_id_span(),
        block_binding_value: view.entry_header.block_binding_value(),
        identity: row_set,
        deletes,
    })
}

fn build_resolved_row(
    leaf_block_id: BlockID,
    view: &LeafEntryView<'_>,
    row_idx: usize,
    durable_deleted: bool,
) -> ResolvedColumnRow {
    ResolvedColumnRow {
        leaf_block_id,
        block_id: view.entry_header.block_id(),
        row_idx,
        block_binding_value: view.entry_header.block_binding_value(),
        durable_deleted,
    }
}

fn decode_row_section_metadata(
    row_section: &[u8],
    row_header: SectionHeader,
    row_id_span: u32,
) -> DataIntegrityResult<DecodedRowSectionMetadata> {
    // All callers hold an admitted immutable frame. Full grammar validation is
    // performed once in validate_leaf_prefixes, before cache publication.
    let rows = RowSetRef::from_admitted(
        row_header.kind,
        &row_section[mem::size_of::<SectionHeader>()..],
        row_id_span,
    );
    Ok(DecodedRowSectionMetadata {
        row_count: rows.row_count(),
        first_present_delta: rows.delta_for_ordinal(0).ok_or_else(invalid_node_payload)?,
    })
}

fn validate_delete_section(bytes: Option<&[u8]>, row_count: u16) -> DataIntegrityResult<()> {
    let Some(bytes) = bytes else {
        return Ok(());
    };
    if bytes.len() < COLUMN_DELETE_SECTION_HEADER_SIZE {
        return Err(invalid_node_payload());
    }
    let header = persisted_column_index_layout(
        layout::try_ref_from_bytes::<DeleteSectionHeader>(
            &bytes[..COLUMN_DELETE_SECTION_HEADER_SIZE],
        ),
        "delete_section_header",
    )?;
    if header.version != COLUMN_DELETE_SECTION_VERSION
        || header.flags != [0; 2]
        || header.reserved != [0; 2]
    {
        return Err(invalid_node_payload());
    }
    OrdinalDeletionSetRef::validate(
        row_count,
        header.del_count(),
        header.kind,
        &bytes[COLUMN_DELETE_SECTION_HEADER_SIZE..],
    )?;
    Ok(())
}

fn encode_row_section(row_set: &LogicalRowSet) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4 + row_set.body().len());
    bytes.extend_from_slice(
        &SectionHeader {
            kind: row_set.codec(),
            version: COLUMN_ROW_SECTION_VERSION,
            flags: 0,
            aux: 0,
        }
        .encode(),
    );
    bytes.extend_from_slice(row_set.body());
    bytes
}

fn encode_delete_section(delete_set: &OrdinalDeletionSet) -> Vec<u8> {
    let Some(codec) = delete_set.codec() else {
        return Vec::new();
    };
    let header = DeleteSectionHeader::new(codec, storage_count_u16(delete_set.len()));
    let mut bytes = Vec::with_capacity(COLUMN_DELETE_SECTION_HEADER_SIZE + delete_set.body().len());
    bytes.extend_from_slice(layout::bytes_of(&header));
    bytes.extend_from_slice(delete_set.body());
    bytes
}

fn encode_leaf_chunk(
    buf: &mut [u8],
    entries: &[EncodedLeafEntry],
    search_type: ColumnBlockLeafSearchType,
) {
    let leaf_start_row_id = entries
        .first()
        .unwrap_or_else(|| {
            panic!("column block-index invariant violated: cannot encode an empty leaf chunk")
        })
        .start_row_id;
    buf.fill(0);
    let (leaf_header, leaf_data) = buf.split_at_mut(COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE);
    leaf_header.copy_from_slice(layout::bytes_of(&ColumnBlockLeafHeaderExt::new(
        search_type,
    )));
    let prefix_bytes_len = entries.len() * search_type.prefix_size();
    let mut prefix_cursor = 0usize;
    let mut arena_end = leaf_data.len();
    for entry in entries {
        let entry_range = reserve_tail(&mut arena_end, entry.payload_len(), leaf_data.len());
        assert!(
            arena_end >= prefix_bytes_len,
            "column block-index invariant violated: leaf payload overlaps prefix plane"
        );
        let entry_header = ColumnBlockLeafEntryHeader::from_encoded(entry);
        let entry_bytes = &mut leaf_data[entry_range.0..entry_range.1];
        entry_bytes[..COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE]
            .copy_from_slice(layout::bytes_of(&entry_header));
        let row_section_end = COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + entry.row_section.len();
        entry_bytes[COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE..row_section_end]
            .copy_from_slice(&entry.row_section);
        entry_bytes[row_section_end..].copy_from_slice(&entry.delete_section);

        encode_leaf_prefix(
            &mut leaf_data[prefix_cursor..prefix_cursor + search_type.prefix_size()],
            search_type,
            leaf_start_row_id,
            entry.start_row_id,
            storage_len_u16(entry_range.0),
        );
        prefix_cursor += search_type.prefix_size();
    }
}

fn select_leaf_search_type(entries: &[EncodedLeafEntry]) -> ColumnBlockLeafSearchType {
    let leaf_start_row_id = entries
        .first()
        .unwrap_or_else(|| {
            panic!(
                "column block-index invariant violated: cannot select search type for empty leaf"
            )
        })
        .start_row_id;
    let mut max_delta = 0u64;
    for entry in entries {
        let delta = entry
            .start_row_id
            .checked_sub(leaf_start_row_id)
            .unwrap_or_else(|| {
                panic!("column block-index invariant violated: leaf entry order regressed")
            });
        max_delta = max_delta.max(delta);
    }
    if max_delta <= u16::MAX as u64 {
        ColumnBlockLeafSearchType::DeltaU16
    } else if max_delta <= u32::MAX as u64 {
        ColumnBlockLeafSearchType::DeltaU32
    } else {
        ColumnBlockLeafSearchType::Plain
    }
}

fn leaf_chunk_encoded_len(
    entries: &[EncodedLeafEntry],
    search_type: ColumnBlockLeafSearchType,
) -> usize {
    let mut total = entries
        .len()
        .checked_mul(search_type.prefix_size())
        .unwrap_or_else(|| panic!("column block-index invariant violated: prefix size overflow"));
    for entry in entries {
        total = total
            .checked_add(entry.payload_len())
            .unwrap_or_else(|| panic!("column block-index invariant violated: leaf size overflow"));
    }
    total
}

fn encode_leaf_prefix(
    dst: &mut [u8],
    search_type: ColumnBlockLeafSearchType,
    leaf_start_row_id: RowID,
    start_row_id: RowID,
    entry_offset: u16,
) {
    match search_type {
        ColumnBlockLeafSearchType::Plain => {
            assert_eq!(
                dst.len(),
                COLUMN_BLOCK_LEAF_PREFIX_PLAIN_SIZE,
                "column block-index invariant violated: plain prefix has invalid size"
            );
            dst[..8].copy_from_slice(&start_row_id.to_le_bytes());
            dst[8..10].copy_from_slice(&entry_offset.to_le_bytes());
        }
        ColumnBlockLeafSearchType::DeltaU32 => {
            assert_eq!(
                dst.len(),
                COLUMN_BLOCK_LEAF_PREFIX_U32_SIZE,
                "column block-index invariant violated: u32 prefix has invalid size"
            );
            let delta = start_row_id
                .checked_sub(leaf_start_row_id)
                .unwrap_or_else(|| {
                    panic!("column block-index invariant violated: u32 prefix delta underflow")
                });
            let delta = u32::try_from(delta).unwrap_or_else(|_| {
                panic!("column block-index invariant violated: u32 prefix delta overflow")
            });
            dst[..4].copy_from_slice(&delta.to_le_bytes());
            dst[4..6].copy_from_slice(&entry_offset.to_le_bytes());
        }
        ColumnBlockLeafSearchType::DeltaU16 => {
            assert_eq!(
                dst.len(),
                COLUMN_BLOCK_LEAF_PREFIX_U16_SIZE,
                "column block-index invariant violated: u16 prefix has invalid size"
            );
            let delta = start_row_id
                .checked_sub(leaf_start_row_id)
                .unwrap_or_else(|| {
                    panic!("column block-index invariant violated: u16 prefix delta underflow")
                });
            let delta = u16::try_from(delta).unwrap_or_else(|_| {
                panic!("column block-index invariant violated: u16 prefix delta overflow")
            });
            dst[..2].copy_from_slice(&delta.to_le_bytes());
            dst[2..4].copy_from_slice(&entry_offset.to_le_bytes());
        }
    }
}

fn reserve_tail(arena_end: &mut usize, len: usize, total_len: usize) -> (usize, usize) {
    assert!(
        len != 0 && *arena_end <= total_len && len <= *arena_end,
        "column block-index invariant violated: invalid leaf arena reservation: arena_end={}, len={len}, total_len={total_len}",
        *arena_end
    );
    let start = *arena_end - len;
    let end = *arena_end;
    *arena_end = start;
    (start, end)
}

fn build_logical_entry_from_input(input: &ColumnBlockEntryInput) -> LogicalLeafEntry {
    assert_ne!(
        input.block_id, SUPER_BLOCK_ID,
        "column entry points to empty-root sentinel: start_row_id={}",
        input.start_row_id
    );
    LogicalLeafEntry::new(
        input.start_row_id,
        input.block_id,
        input.row_set.clone(),
        input.deletions.clone(),
        input.block_binding_value,
    )
}

#[inline]
fn storage_count_u16(len: usize) -> u16 {
    u16::try_from(len).unwrap_or_else(|_| {
        panic!("column block-index invariant violated: count {len} exceeds u16 storage")
    })
}

#[inline]
fn storage_len_u16(len: usize) -> u16 {
    u16::try_from(len).unwrap_or_else(|_| {
        panic!("column block-index invariant violated: length {len} exceeds u16 storage")
    })
}

fn entry_inputs_sorted(entries: &[ColumnBlockEntryInput]) -> bool {
    entries
        .windows(2)
        .all(|pair| pair[0].start_row_id() < pair[1].start_row_id())
}

fn deletion_patches_sorted_unique(patches: &[ColumnDeletionPatch<'_>]) -> bool {
    patches_sorted_unique_by_start_row_id(patches, |patch| patch.start_row_id)
}

fn patches_sorted_unique_by_start_row_id<P, F>(patches: &[P], mut key: F) -> bool
where
    F: FnMut(&P) -> RowID,
{
    patches.windows(2).all(|pair| key(&pair[0]) < key(&pair[1]))
}

#[inline]
fn search_prefix_slice(search: StdResult<usize, usize>) -> Option<usize> {
    match search {
        Ok(idx) => Some(idx),
        Err(0) => None,
        Err(idx) => Some(idx - 1),
    }
}

fn search_start_row_id(
    prefixes: &LeafPrefixPlane<'_>,
    row_id: RowID,
) -> DataIntegrityResult<Option<usize>> {
    prefixes.search(row_id)
}

fn search_branch_entry(entries: &[ColumnBlockBranchEntry], row_id: RowID) -> Option<usize> {
    match entries.binary_search_by_key(&row_id, |entry| entry.start_row_id()) {
        Ok(idx) => Some(idx),
        Err(0) => None,
        Err(idx) => Some(idx - 1),
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::buffer::{global_readonly_pool_scope, table_readonly_pool};
    use crate::catalog::{
        StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexKey,
        StorageIndexSpec, TableMetadata,
    };
    // Tests below inspect the existing ColumnBlockIndex public orchestration
    // boundary over persisted DataIntegrity, buffer/file IO, and rewrite Internal.
    use crate::error::{DataIntegrityError, DiscloseError, Error};
    use crate::file::cow_file::SUPER_BLOCK_ID;
    use crate::file::cow_file::tests::rewrite_page_with_checksum;
    use crate::file::table_file::MutableTableFile;
    use crate::file::test_block_id;
    use crate::file::{FileKind, build_test_fs};
    use crate::layout::LayoutError;
    use crate::table::test_user_table_id;
    use crate::value::ValKind;
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::slice;
    use std::sync::Arc;

    const COLUMN_ROW_CODEC_DENSE: u8 = 1;

    /// Builds a checksummed adaptive bitmap leaf for readonly-admission tests.
    pub(crate) fn adaptive_leaf_fixture() -> Vec<u8> {
        let row_set =
            RowSetRef::validate(6, &[5, 0, 0, 0, 0, 0, 0b10101011, 0, 0, 0, 0, 0, 0, 0], 64)
                .unwrap()
                .to_owned();
        let entry = LogicalLeafEntry::new(
            RowID::new(0),
            test_block_id(10),
            row_set,
            OrdinalDeletionSet::empty(5),
            0,
        );
        let encoded = EncodedLeafEntry::from_logical(&entry);
        encoded_leaf_page(&[encoded])
    }

    /// Builds an admitted leaf with inline deletion bytes for cache-corruption tests.
    pub(crate) fn inline_deletion_leaf_fixture() -> Vec<u8> {
        let input =
            dense_entry_with_deletions(RowID::new(0), RowID::new(8), &[1, 4], test_block_id(10));
        let encoded = EncodedLeafEntry::from_logical(&build_logical_entry_from_input(&input));
        encoded_leaf_page(&[encoded])
    }

    /// Corrupts the codec of an existing nonempty inline deletion section.
    pub(crate) fn corrupt_leaf_delete_codec(
        path: impl AsRef<Path>,
        page_id: impl Into<u64>,
        prefix_idx: usize,
    ) {
        rewrite_page_with_checksum(path, page_id, |page| {
            let offset = leaf_entry_payload_offset(page, prefix_idx);
            let row_len =
                u16::from_le_bytes(page[offset + 22..offset + 24].try_into().unwrap()) as usize;
            page[offset + COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + row_len] = 0xff;
        });
    }

    /// Corrupts the reserved row-header byte for an integrity test.
    pub(crate) fn corrupt_leaf_reserved(
        path: impl AsRef<Path>,
        page_id: impl Into<u64>,
        prefix_idx: usize,
    ) {
        rewrite_page_with_checksum(path, page_id, |page| {
            let byte_offset = leaf_entry_payload_offset(page, prefix_idx)
                + COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE
                + 3;
            page[byte_offset] = 0xFF;
        });
    }

    /// Corrupts leaf row codec for an integrity test.
    pub(crate) fn corrupt_leaf_row_codec(
        path: impl AsRef<Path>,
        page_id: impl Into<u64>,
        prefix_idx: usize,
    ) {
        rewrite_page_with_checksum(path, page_id, |page| {
            let byte_offset =
                leaf_entry_payload_offset(page, prefix_idx) + COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE;
            page[byte_offset] = 0;
        });
    }

    /// Corrupts leaf block id for an integrity test.
    pub(crate) fn corrupt_leaf_block_id(
        path: impl AsRef<Path>,
        page_id: impl Into<u64>,
        prefix_idx: usize,
    ) {
        rewrite_page_with_checksum(path, page_id, |page| {
            let byte_offset = leaf_entry_payload_offset(page, prefix_idx);
            page[byte_offset..byte_offset + 8].copy_from_slice(&SUPER_BLOCK_ID.to_le_bytes());
        });
    }

    /// Corrupts leaf short delete section header for an integrity test.
    pub(crate) fn corrupt_leaf_short_delete_section_header(
        path: impl AsRef<Path>,
        page_id: impl Into<u64>,
        prefix_idx: usize,
    ) {
        const LEAF_ENTRY_ENTRY_LEN_OFFSET: usize = 20;
        const LEAF_ENTRY_ROW_SECTION_LEN_OFFSET: usize = 22;
        const LEAF_ENTRY_HEADER_SIZE: usize = 24;
        const TRUNCATED_DELETE_SECTION_LEN: usize = 4;

        rewrite_page_with_checksum(path, page_id, |page| {
            let byte_offset = leaf_entry_payload_offset(page, prefix_idx);
            let row_section_len = u16::from_le_bytes(
                page[byte_offset + LEAF_ENTRY_ROW_SECTION_LEN_OFFSET
                    ..byte_offset + LEAF_ENTRY_ROW_SECTION_LEN_OFFSET + 2]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let truncated_entry_len =
                (LEAF_ENTRY_HEADER_SIZE + row_section_len + TRUNCATED_DELETE_SECTION_LEN) as u16;
            page[byte_offset + LEAF_ENTRY_ENTRY_LEN_OFFSET
                ..byte_offset + LEAF_ENTRY_ENTRY_LEN_OFFSET + 2]
                .copy_from_slice(&truncated_entry_len.to_le_bytes());
        });
    }

    fn encoded_leaf_page(entries: &[EncodedLeafEntry]) -> Vec<u8> {
        let mut page = vec![0; COLUMN_BLOCK_PAGE_SIZE];
        let start = write_block_header(&mut page, COLUMN_BLOCK_INDEX_BLOCK_SPEC);
        let header = ColumnBlockNodeHeader::new(
            0,
            entries.len() as u32,
            entries[0].start_row_id,
            TrxID::new(1),
        );
        page[start..start + COLUMN_BLOCK_HEADER_SIZE].copy_from_slice(layout::bytes_of(&header));
        encode_leaf_chunk(
            &mut page[start + COLUMN_BLOCK_HEADER_SIZE..start + COLUMN_BLOCK_NODE_PAYLOAD_SIZE],
            entries,
            select_leaf_search_type(entries),
        );
        write_block_checksum(&mut page);
        page
    }

    fn dense_logical(start: u64, count: u16) -> LogicalLeafEntry {
        LogicalLeafEntry::new(
            RowID::new(start),
            BlockID::new(1_000_000 + start),
            RowSetRef::validate(1, &[], u32::from(count))
                .unwrap()
                .to_owned(),
            OrdinalDeletionSet::empty(count),
            calculate_block_binding_value(
                test_user_table_id(1),
                RowID::new(start),
                RowID::new(start + u64::from(count)),
                u32::from(count),
            ),
        )
    }

    fn leaf_entry_payload_offset(page: &[u8], prefix_idx: usize) -> usize {
        const SEARCH_TYPE_PLAIN: u8 = 1;
        const SEARCH_TYPE_DELTA_U32: u8 = 2;
        const SEARCH_TYPE_DELTA_U16: u8 = 3;

        let payload_start = BLOCK_INTEGRITY_HEADER_SIZE;
        let search_type = page[payload_start + COLUMN_BLOCK_HEADER_SIZE];
        let (prefix_size, entry_offset_offset) = match search_type {
            SEARCH_TYPE_PLAIN => (10usize, 8usize),
            SEARCH_TYPE_DELTA_U32 => (6usize, 4usize),
            SEARCH_TYPE_DELTA_U16 => (4usize, 2usize),
            _ => panic!("invalid leaf search type {search_type}"),
        };
        let prefix_offset =
            payload_start + COLUMN_BLOCK_LEAF_HEADER_SIZE + prefix_idx * prefix_size;
        let entry_offset = u16::from_le_bytes(
            page[prefix_offset + entry_offset_offset..prefix_offset + entry_offset_offset + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        payload_start + COLUMN_BLOCK_LEAF_HEADER_SIZE + entry_offset
    }

    fn test_row_ids<const N: usize>(values: [u64; N]) -> Vec<RowID> {
        values.into_iter().map(RowID::new).collect()
    }

    fn test_row_id_range(start: u64, end: u64) -> Vec<RowID> {
        (start..end).map(RowID::new).collect()
    }

    fn metadata() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new(
                vec![StorageColumnSpec::new(
                    ValKind::U64,
                    StorageColumnFlags::empty(),
                )],
                vec![StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0)],
                    StorageIndexFlags::PK,
                )],
            )
            .expect("valid table metadata"),
        )
    }

    fn dense_entry(
        start: RowID,
        end: RowID,
        block_id: impl Into<BlockID>,
    ) -> ColumnBlockEntryInput {
        let row_ids = test_row_id_range(start.as_u64(), end.as_u64());
        ColumnBlockEntryShape::new(test_user_table_id(1), start, end, &row_ids, &[])
            .unwrap()
            .with_block_id(block_id)
    }

    fn assert_column_index_corruption(err: Error, block_id: BlockID, expected: DataIntegrityError) {
        assert_eq!(
            err.report().downcast_ref::<DataIntegrityError>().copied(),
            Some(expected)
        );
        let report = format!("{err:?}");
        assert!(report.contains("table_file"), "{report}");
        assert!(report.contains("column_block_index"), "{report}");
        assert!(report.contains(&format!("block_id={block_id}")), "{report}");
    }

    fn sparse_entry(
        start: RowID,
        end: RowID,
        row_ids: Vec<RowID>,
        block_id: impl Into<BlockID>,
    ) -> ColumnBlockEntryInput {
        ColumnBlockEntryShape::new(test_user_table_id(1), start, end, &row_ids, &[])
            .unwrap()
            .with_block_id(block_id)
    }

    fn dense_entry_with_deletions(
        start: RowID,
        end: RowID,
        ordinals: &[u16],
        block_id: BlockID,
    ) -> ColumnBlockEntryInput {
        let mut input = dense_entry(start, end, block_id);
        input.deletions =
            OrdinalDeletionSet::from_ordinals(input.row_set.row_count() as u16, ordinals).unwrap();
        input
    }

    async fn assert_search_type_lookup(
        entries: Vec<ColumnBlockEntryInput>,
        end_row_id: RowID,
        probe_row_id: RowID,
        expected_search_type: ColumnBlockLeafSearchType,
        expected_block_id: impl Into<BlockID>,
    ) {
        let (_temp_dir, fs) = build_test_fs();
        let background_writes = fs.background_writes();
        let metadata = metadata();
        let table = fs
            .create_table_file(test_user_table_id(1), metadata, false)
            .unwrap();
        let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
        drop(old_root);
        let global = global_readonly_pool_scope(64 * 1024 * 1024);
        let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
        let disk_pool_guard = disk_pool.create_base_guard();
        let mut mutable = MutableTableFile::fork(
            &table,
            background_writes,
            disk_pool.global_pool().clone(),
            disk_pool_guard.clone(),
        );
        let root_block_id = ColumnBlockIndex::new(
            SUPER_BLOCK_ID,
            RowID::new(0),
            disk_pool.file_kind(),
            disk_pool.sparse_file(),
            disk_pool.global_pool(),
            &disk_pool_guard,
        )
        .batch_insert(&mut mutable, &entries, end_row_id, TrxID::new(2))
        .await
        .unwrap();
        let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

        let index = ColumnBlockIndex::new(
            root_block_id,
            end_row_id,
            disk_pool.file_kind(),
            disk_pool.sparse_file(),
            disk_pool.global_pool(),
            &disk_pool_guard,
        );
        let entry = index.locate_block(probe_row_id).await.unwrap().unwrap();
        assert_eq!(entry.block_id(), expected_block_id.into());
        let node = index.read_node(entry.leaf_block_id).await.unwrap();
        let header = layout::ref_from_bytes::<ColumnBlockLeafHeaderExt>(
            &node.data_ref()[..COLUMN_BLOCK_LEAF_HEADER_EXT_SIZE],
        );
        assert_eq!(header.search_type().unwrap(), expected_search_type);
        assert!(
            index
                .locate_and_resolve_row(probe_row_id)
                .await
                .unwrap()
                .is_some()
        );
    }

    /// Purpose: Repack growing middle entries, propagate leaf splits through a full branch, and retain old-root visibility.
    /// Expected: Root height grows, untouched children stay referenced, and later all-deleted shrinkage preserves identity and neighboring lookups.
    #[test]
    fn deletion_growth_splits_full_branch_and_preserves_old_root() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata(), false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let guard = pool.create_base_guard();
            let make_index = |root, end| {
                ColumnBlockIndex::new(
                    root,
                    RowID::new(end),
                    pool.file_kind(),
                    pool.sparse_file(),
                    pool.global_pool(),
                    &guard,
                )
            };
            let empty = make_index(SUPER_BLOCK_ID, 0);
            let mut mutable = MutableTableFile::fork(
                &table,
                fs.background_writes(),
                pool.global_pool().clone(),
                guard.clone(),
            );
            // Compact dense identities fill one leaf; a later bitmap expands its middle entry.
            let count = MAX_LWC_ROWS as u16;
            let entries: Vec<_> = (0..1920)
                .map(|i| dense_logical(i * u64::from(count), count))
                .collect();
            let mut children = empty
                .write_leaf_pages_from_logical_entries(&mut mutable, &entries, TrxID::new(2))
                .await
                .unwrap();
            assert_eq!(children.len(), 1);
            let mut next = 1920 * u64::from(count);
            for _ in 1..COLUMN_BLOCK_MAX_BRANCH_ENTRIES {
                children.extend(
                    empty
                        .write_leaf_pages_from_logical_entries(
                            &mut mutable,
                            &[dense_logical(next, 1)],
                            TrxID::new(2),
                        )
                        .await
                        .unwrap(),
                );
                next += 1;
            }
            let branches = empty
                .write_branch_pages(&mut mutable, &children, 1, TrxID::new(2))
                .await
                .unwrap();
            assert_eq!(branches.len(), 1);
            let original = make_index(branches[0].block_id(), next);
            let target = entries[960].start_row_id;
            let single = OrdinalDeletionSet::from_ordinals(count, &[1]).unwrap();
            let small_root = original
                .batch_replace_deletions(
                    &mut mutable,
                    &[ColumnDeletionPatch {
                        start_row_id: target,
                        deletions: &single,
                    }],
                    TrxID::new(3),
                )
                .await
                .unwrap();
            let small = make_index(small_root, next);
            assert_eq!(
                small
                    .read_node(small_root)
                    .await
                    .unwrap()
                    .header_ref()
                    .height(),
                1
            );
            let ordinals: Vec<_> = (0..count).step_by(2).collect();
            let bitmap = OrdinalDeletionSet::from_ordinals(count, &ordinals).unwrap();
            let grown_root = small
                .batch_replace_deletions(
                    &mut mutable,
                    &[ColumnDeletionPatch {
                        start_row_id: target,
                        deletions: &bitmap,
                    }],
                    TrxID::new(4),
                )
                .await
                .unwrap();
            let grown = make_index(grown_root, next);
            assert_eq!(
                grown
                    .read_node(grown_root)
                    .await
                    .unwrap()
                    .header_ref()
                    .height(),
                2
            );
            let all =
                OrdinalDeletionSet::from_ordinals(count, &(0..count).collect::<Vec<_>>()).unwrap();
            let shrunk_root = grown
                .batch_replace_deletions(
                    &mut mutable,
                    &[ColumnDeletionPatch {
                        start_row_id: target,
                        deletions: &all,
                    }],
                    TrxID::new(5),
                )
                .await
                .unwrap();
            let shrunk = make_index(shrunk_root, next);
            for (index, expected_deletes) in [
                (&original, 0),
                (&small, 1),
                (&grown, ordinals.len()),
                (&shrunk, usize::from(count)),
            ] {
                let mut refs = Vec::new();
                let mut stack = vec![index.root_block_id];
                while let Some(block_id) = stack.pop() {
                    let node = index.read_node(block_id).await.unwrap();
                    if node.header_ref().height() == 1 {
                        refs.extend_from_slice(node.branch_entries());
                    } else {
                        stack.extend(
                            node.branch_entries()
                                .iter()
                                .rev()
                                .map(ColumnBlockBranchEntry::block_id),
                        );
                    }
                }
                let split = usize::from(expected_deletes > 1);
                assert_eq!(refs.len(), children.len() + split);
                assert_eq!(
                    &refs[1 + split..],
                    &children[1..],
                    "untouched child references"
                );
                for expected in &entries {
                    let entry = index
                        .locate_block(expected.start_row_id)
                        .await
                        .unwrap()
                        .unwrap();
                    let (identity, deletions) = index
                        .load_entry_identity_and_deletions(&entry)
                        .await
                        .unwrap();
                    assert_eq!(identity, expected.row_set);
                    assert_eq!(entry.block_binding_value(), expected.block_binding_value);
                    assert_eq!(entry.block_id(), expected.block_id);
                    assert_eq!(
                        deletions.len(),
                        if entry.start_row_id == target {
                            expected_deletes
                        } else {
                            0
                        }
                    );
                }
                for ordinal in [0, 1, 2, count - 1] {
                    let resolved = index
                        .locate_and_resolve_row(target + u64::from(ordinal))
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(resolved.row_idx(), usize::from(ordinal));
                    let deleted = match expected_deletes {
                        0 => false,
                        1 => ordinal == 1,
                        n if n == usize::from(count) => true,
                        _ => ordinal % 2 == 0,
                    };
                    assert_eq!(resolved.durable_deleted(), deleted);
                }
            }
        });
    }

    /// Purpose: Recalculate compact prefix widths when a deletion splits an exactly packed leaf.
    /// Expected: A u32-prefix leaf splits into a u32-prefix leaf and a single-entry u16-prefix leaf with intact offsets.
    #[test]
    fn deletion_split_reselects_prefix_width() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata(), false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let guard = pool.create_base_guard();
            let index = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                pool.file_kind(),
                pool.sparse_file(),
                pool.global_pool(),
                &guard,
            );
            let mut mutable = MutableTableFile::fork(
                &table,
                fs.background_writes(),
                pool.global_pool().clone(),
                guard.clone(),
            );
            let entries: Vec<_> = (0..1925).map(|i| dense_logical(i * 128, 128)).collect();
            let root = index
                .build_tree_from_logical_entries(&mut mutable, &entries, TrxID::new(2))
                .await
                .unwrap();
            let index = ColumnBlockIndex::new(
                root,
                RowID::new(1925 * 128),
                pool.file_kind(),
                pool.sparse_file(),
                pool.global_pool(),
                &guard,
            );
            let original = index.read_node(root).await.unwrap();
            assert_eq!(original.header_ref().height(), 0);
            assert_eq!(original.header_ref().count(), 1925);
            assert_eq!(
                original.leaf_prefix_plane().unwrap().search_type(),
                ColumnBlockLeafSearchType::DeltaU32
            );
            drop(original);
            // The leaf has 22 spare bytes. This deletion bitmap must force the final entry out.
            let deletes =
                OrdinalDeletionSet::from_ordinals(128, &(1..128).step_by(2).collect::<Vec<_>>())
                    .unwrap();
            assert!(COLUMN_DELETE_SECTION_HEADER_SIZE + deletes.body().len() > 22);
            let updated = index
                .batch_replace_deletions(
                    &mut mutable,
                    &[ColumnDeletionPatch {
                        start_row_id: entries[900].start_row_id,
                        deletions: &deletes,
                    }],
                    TrxID::new(3),
                )
                .await
                .unwrap();
            let branch = index.read_node(updated).await.unwrap();
            assert_eq!(branch.header_ref().height(), 1);
            assert_eq!(branch.branch_entries().len(), 2);
            for (child, (search, count)) in branch.branch_entries().iter().zip([
                (ColumnBlockLeafSearchType::DeltaU32, 1924),
                (ColumnBlockLeafSearchType::DeltaU16, 1),
            ]) {
                let leaf = index.read_node(child.block_id()).await.unwrap();
                assert_eq!(leaf.leaf_prefix_plane().unwrap().search_type(), search);
                assert_eq!(leaf.header_ref().count(), count);
            }
            let current = ColumnBlockIndex::new(
                updated,
                RowID::new(1925 * 128),
                pool.file_kind(),
                pool.sparse_file(),
                pool.global_pool(),
                &guard,
            );
            for entry in entries {
                let row = current
                    .locate_and_resolve_row(entry.start_row_id + 1)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(row.row_idx(), 1);
                assert_eq!(row.block_binding_value(), entry.block_binding_value);
                assert_eq!(
                    row.durable_deleted(),
                    entry.start_row_id == RowID::new(900 * 128)
                );
            }
        });
    }

    /// Purpose: Independently account for the worst supported identity and deletion bodies in a complete leaf.
    /// Expected: The cap serializes in 63,588 bytes including framing, while 16,384 rows cannot meet the bound.
    #[test]
    fn standalone_capacity_bound_matches_serialization() {
        assert_eq!(COLUMN_STANDALONE_FIXED_SIZE, 104);
        assert_eq!(104 + 4 * 16_384 + deletion_body_bound(16_384), 67_820);
        for count in [1, 63, 64, 65, 4096, MAX_LWC_ROWS] {
            let rows: Vec<_> = (0..count).map(|i| RowID::new(i as u64 * 100003)).collect();
            let mut input = sparse_entry(
                RowID::new(0),
                RowID::new(count as u64 * 100003),
                rows,
                test_block_id(1001),
            );
            input.deletions = OrdinalDeletionSet::from_ordinals(
                count as u16,
                &(0..count as u16).step_by(2).collect::<Vec<_>>(),
            )
            .unwrap();
            let encoded = EncodedLeafEntry::from_logical(&build_logical_entry_from_input(&input));
            let actual = 32
                + 32
                + leaf_chunk_encoded_len(
                    slice::from_ref(&encoded),
                    ColumnBlockLeafSearchType::DeltaU16,
                );
            assert!(actual <= 104 + 4 * count + deletion_body_bound(count));
            if count == MAX_LWC_ROWS {
                assert_eq!(input.row_set.body().len(), 61_440);
                assert_eq!(input.deletions.body().len(), 2_044);
                assert_eq!(actual, 63_588);
            }
            let page = encoded_leaf_page(&[encoded]);
            validate_persisted_column_block_index_page(
                &page,
                FileKind::TableFile,
                test_block_id(1),
            )
            .unwrap();
        }
    }

    /// Purpose: Enforce new-format count and representation bounds even for structurally valid integer sets.
    /// Expected: Oversized dense identity, inefficient identity bitmap, and oversized deletion list all fail admission.
    #[test]
    fn rejects_valid_codecs_outside_capacity_contract() {
        let input = dense_entry(RowID::new(0), RowID::new(8), test_block_id(1001));
        let base = EncodedLeafEntry::from_logical(&build_logical_entry_from_input(&input));
        let mut dense = base.clone();
        dense.row_id_span = MAX_LWC_ROWS as u32 + 1;
        let mut bitmap = base.clone();
        bitmap.row_id_span = 64;
        bitmap.row_section = vec![6, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        let mut deletion = base;
        deletion.row_id_span = MAX_LWC_ROWS as u32;
        deletion.delete_section = vec![3, 2, 0, 0, 0x4c, 4, 0, 0]; // 1,100 sorted ordinals.
        deletion
            .delete_section
            .extend((0..1100u16).flat_map(u16::to_le_bytes));
        for (case, encoded) in [
            ("physical count", dense),
            ("identity body", bitmap),
            ("deletion body", deletion),
        ] {
            let page = encoded_leaf_page(&[encoded]);
            let err = validate_persisted_column_block_index_page(
                &page,
                FileKind::TableFile,
                test_block_id(1),
            )
            .unwrap_err();
            assert_eq!(
                err.current_context(),
                &DataIntegrityError::InvalidPayload,
                "{case}"
            );
        }
    }

    /// Purpose: Validate deletion framing and redundant cardinality before a leaf enters the cache.
    /// Expected: Unknown codecs, versions, flags, reserved bytes, and mismatched counts return typed payload errors.
    #[test]
    fn rejects_corrupt_inline_deletion_headers() {
        let original = inline_deletion_leaf_fixture();
        let offset =
            leaf_entry_payload_offset(&original, 0) + COLUMN_BLOCK_LEAF_ENTRY_HEADER_SIZE + 4;
        for (field, value) in [
            (0, 0xff),
            (1, 1),
            (2, 1),
            (3, 1),
            (4, 0),
            (4, 3),
            (5, 1),
            (6, 1),
            (7, 1),
        ] {
            let mut page = original.clone();
            page[offset + field] = value;
            write_block_checksum(&mut page);
            let err = validate_persisted_column_block_index_page(
                &page,
                FileKind::TableFile,
                test_block_id(1),
            )
            .unwrap_err();
            assert_eq!(
                err.current_context(),
                &DataIntegrityError::InvalidPayload,
                "field={field}"
            );
        }
    }

    /// Purpose: Protect translation from row-id deltas to dense and sparse scan ordinals.
    /// Expected: Present deltas map to their physical positions and gaps or out-of-range deltas are absent.
    #[test]
    fn scan_row_identity_translates_dense_and_sparse_ordinals() {
        let dense = EncodedRowSet::plan(
            RowID::new(0),
            RowID::new(4),
            &test_row_id_range(0, 4),
            &[],
            4 * MAX_LWC_ROWS,
        )
        .unwrap();
        assert_eq!(dense.body(), []);
        assert_eq!(dense.ordinal_for_delta(0), Some(0));
        assert_eq!(dense.ordinal_for_delta(3), Some(3));
        assert_eq!(dense.ordinal_for_delta(4), None);
        assert_eq!(dense.ordinal_for_delta(u32::MAX), None);

        let sparse = EncodedRowSet::plan(
            RowID::new(0),
            RowID::new(10),
            &test_row_ids([1, 4, 9]),
            &[],
            4 * MAX_LWC_ROWS,
        )
        .unwrap();
        assert_eq!(sparse.ordinal_for_delta(1), Some(0));
        assert_eq!(sparse.ordinal_for_delta(4), Some(1));
        assert_eq!(sparse.ordinal_for_delta(9), Some(2));
        assert_eq!(sparse.ordinal_for_delta(2), None);
    }

    /// Purpose: Protect error adaptation for a malformed persisted column-index header.
    /// Expected: The integrity error retains its layout cause and identifies the affected field.
    #[test]
    fn test_column_index_layout_adaptation_preserves_layout_source() {
        let err = match persisted_column_index_layout(
            layout::try_ref_from_bytes::<ColumnBlockNodeHeader>(&[0u8; 1]),
            "test_node_header",
        ) {
            Ok(_) => panic!("short column-index header must fail"),
            Err(err) => err,
        };

        assert_eq!(err.current_context(), &DataIntegrityError::InvalidPayload);
        assert_eq!(
            err.downcast_ref::<LayoutError>().copied(),
            Some(LayoutError::Mismatch)
        );
        assert!(format!("{err:?}").contains("field=test_node_header"));
    }

    /// Purpose: Reject malformed physical page lengths before parsing node or codec metadata.
    /// Expected: Checksummed short and oversized frames return typed integrity errors without slicing panics.
    #[test]
    fn column_index_rejects_invalid_physical_length() {
        for len in [
            48,
            72,
            COLUMN_BLOCK_PAGE_SIZE - 1,
            COLUMN_BLOCK_PAGE_SIZE + 1,
        ] {
            let mut page = vec![0; len];
            write_block_header(&mut page, COLUMN_BLOCK_INDEX_BLOCK_SPEC);
            write_block_checksum(&mut page);
            let err = validate_persisted_column_block_index_page(
                &page,
                FileKind::TableFile,
                test_block_id(1),
            )
            .unwrap_err();
            assert_eq!(err.current_context(), &DataIntegrityError::InvalidPayload);
        }
    }

    /// Purpose: Reject the previous index envelope before parsing any legacy deletion metadata.
    /// Expected: A checksummed old-version node fails with InvalidVersion.
    #[test]
    fn column_index_rejects_previous_binding_format() {
        let mut page = adaptive_leaf_fixture();
        page[8..16].copy_from_slice(&(COLUMN_BLOCK_INDEX_BLOCK_SPEC.version - 1).to_le_bytes());
        write_block_checksum(&mut page);
        let err = validate_persisted_column_block_index_page(
            &page,
            FileKind::TableFile,
            test_block_id(7),
        )
        .unwrap_err();
        assert_eq!(err.current_context(), &DataIntegrityError::InvalidVersion);
    }

    /// Purpose: Reject an empty column-index payload even when its integrity framing is valid.
    /// Expected: Page validation reports an invalid payload.
    #[test]
    fn test_persisted_empty_column_index_page_is_invalid_payload() {
        let mut page = DirectBuf::zeroed(COLUMN_BLOCK_PAGE_SIZE);
        write_block_header(page.data_mut(), COLUMN_BLOCK_INDEX_BLOCK_SPEC);
        write_block_checksum(page.data_mut());

        let err = validate_persisted_column_block_index_page(
            page.data(),
            FileKind::TableFile,
            test_block_id(42),
        )
        .unwrap_err();
        assert_eq!(err.current_context(), &DataIntegrityError::InvalidPayload);
    }

    /// Purpose: Protect the compact persisted leaf-entry header layout.
    /// Expected: Header fields retain their metadata and the entry and prefix layouts have the specified sizes.
    #[test]
    fn test_leaf_entry_header_roundtrip_uses_u16_lengths() {
        let encoded = EncodedLeafEntry {
            start_row_id: RowID::new(10),
            block_id: test_block_id(1001),
            row_id_span: 32,
            block_binding_value: 0x0807_0605_0403_0201,
            row_section: vec![0u8; 4 + 7 * mem::size_of::<u32>()],
            delete_section: vec![
                0u8;
                COLUMN_DELETE_SECTION_HEADER_SIZE + 2 * mem::size_of::<u32>()
            ],
        };
        let header = ColumnBlockLeafEntryHeader::from_encoded(&encoded);
        assert_eq!(header.block_id(), 1001);
        assert_eq!(header.row_id_span(), 32);
        assert_eq!(header.entry_len(), 24 + 32 + 16);
        assert_eq!(header.row_section_len(), 32);
        assert_eq!(header.block_binding_value(), encoded.block_binding_value);
        assert_eq!(&layout::bytes_of(&header)[8..16], &[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(mem::size_of::<ColumnBlockLeafEntryHeader>(), 24);
        assert_eq!(mem::size_of::<ColumnBlockLeafHeaderExt>(), 8);
        assert_eq!(COLUMN_BLOCK_LEAF_HEADER_SIZE, 32);
        assert_eq!(COLUMN_BLOCK_LEAF_PREFIX_PLAIN_SIZE, 10);
        assert_eq!(COLUMN_BLOCK_LEAF_PREFIX_U32_SIZE, 6);
        assert_eq!(COLUMN_BLOCK_LEAF_PREFIX_U16_SIZE, 4);
    }

    /// Purpose: Fix the binding hash's domain tag, field widths, byte order, and complete digest.
    /// Expected: The binding matches the independent XXH3-64 reference for the canonical input.
    #[test]
    fn block_binding_value_has_fixed_input_layout() {
        let payload = [
            b'L', b'W', b'C', b'B', b'I', b'N', b'D', b'2', 1, 2, 3, 4, 5, 6, 7, 8, 17, 18, 19, 20,
            21, 22, 23, 24, 33, 34, 35, 36, 37, 38, 39, 40, 49, 50, 51, 52,
        ];
        let expected = 0xa48c_ea28_8740_e390;
        assert_eq!(checksum64(&payload), expected);
        assert_eq!(
            calculate_block_binding_value(
                TableID::new(0x0807_0605_0403_0201),
                RowID::new(0x1817_1615_1413_1211),
                RowID::new(0x2827_2625_2423_2221),
                0x3433_3231,
            ),
            expected
        );
    }

    /// Purpose: Bind table, final coverage, and count without depending on row membership or deletes.
    /// Expected: Every bound field affects the value; equal summaries and index-only changes preserve it.
    #[test]
    fn block_binding_value_tracks_only_table_bounds_and_count() {
        let make_shape = |table, start, end, rows: &[RowID], deletes: Vec<u16>| {
            let mut shape = ColumnBlockEntryShape::new(
                TableID::new(table),
                RowID::new(start),
                RowID::new(end),
                rows,
                &[],
            )
            .unwrap();
            shape.deletions =
                OrdinalDeletionSet::from_ordinals(rows.len() as u16, &deletes).unwrap();
            shape
        };
        let rows = test_row_ids([12, 15, 18]);
        let base = make_shape(1, 10, 20, &rows, Vec::new());
        let binding = base.block_binding_value();
        for shape in [
            make_shape(2, 10, 20, &rows, Vec::new()),
            make_shape(1, 11, 20, &rows, Vec::new()),
            make_shape(1, 10, 99, &rows, Vec::new()),
            make_shape(1, 10, 20, &rows[..2], Vec::new()),
        ] {
            assert_ne!(binding, shape.block_binding_value(), "{shape:?}");
        }
        assert_eq!(
            binding,
            make_shape(1, 10, 20, &test_row_ids([12, 16, 18]), vec![1]).block_binding_value()
        );
        assert_eq!(
            binding,
            base.clone()
                .with_block_id(test_block_id(1001))
                .block_binding_value
        );
        assert_eq!(
            binding,
            base.with_block_id(test_block_id(1002)).block_binding_value
        );
    }

    /// Purpose: Protect deletion-section decoding against truncated metadata.
    /// Expected: Decoding reports invalid payload with column-index corruption context.
    #[test]
    fn test_decode_delete_section_metadata_rejects_short_header() {
        let err = validate_delete_section(Some(&[0u8; COLUMN_DELETE_SECTION_HEADER_SIZE - 1]), 8)
            .unwrap_err();
        assert_column_index_corruption(
            err.attach(format!(
                "file={}, block=column_block_index, block_id={}",
                FileKind::TableFile,
                test_block_id(42)
            ))
            .disclose(),
            test_block_id(42),
            DataIntegrityError::InvalidPayload,
        );
    }

    /// Purpose: Enforce the common physical row cap before allocating index bytes.
    /// Expected: Cap-plus-one identity returns the typed capacity error even for dense rows.
    #[test]
    fn test_encode_row_section_rejects_row_count_above_cap() {
        for count in [MAX_LWC_ROWS as u64 + 1, 65_536] {
            let err = ColumnBlockEntryShape::new(
                test_user_table_id(1),
                RowID::new(0),
                RowID::new(count),
                &test_row_id_range(0, count),
                &[],
            )
            .unwrap_err();
            assert_eq!(
                err.current_context(),
                &ResourceError::ColumnBlockEntryCapacityExceeded,
                "count={count}"
            );
        }
    }

    /// Purpose: Protect block lookup for adjacent sparse and dense row entries.
    /// Expected: Present rows resolve to their owning blocks while gaps in sparse membership remain absent.
    #[test]
    fn test_batch_insert_and_locate_sparse_membership() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = metadata();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata, false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let disk_pool_guard = disk_pool.create_base_guard();
            let mut mutable = MutableTableFile::fork(
                &table,
                background_writes,
                disk_pool.global_pool().clone(),
                disk_pool_guard.clone(),
            );
            let index = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            let entries = vec![
                sparse_entry(
                    RowID::new(10),
                    RowID::new(20),
                    test_row_ids([12, 15, 18]),
                    test_block_id(1001),
                ),
                dense_entry(RowID::new(20), RowID::new(24), test_block_id(1002)),
            ];
            let root_block_id = index
                .batch_insert(&mut mutable, &entries, RowID::new(24), TrxID::new(2))
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

            let index = ColumnBlockIndex::new(
                root_block_id,
                RowID::new(24),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            assert!(index.locate_block(RowID::new(10)).await.unwrap().is_none());
            assert_eq!(
                index
                    .locate_block(RowID::new(12))
                    .await
                    .unwrap()
                    .unwrap()
                    .block_id(),
                1001
            );
            assert!(index.locate_block(RowID::new(19)).await.unwrap().is_none());
            assert_eq!(
                index
                    .locate_block(RowID::new(22))
                    .await
                    .unwrap()
                    .unwrap()
                    .block_id(),
                1002
            );
        });
    }

    /// Purpose: Protect leaf-search encoding selection at row-id delta width boundaries.
    /// Expected: Each boundary selects the appropriate representation and still resolves the target row.
    #[test]
    fn test_leaf_search_type_selection_and_lookup_variants() {
        smol::block_on(async {
            let delta_u16_start = 1_000u64 + u16::MAX as u64;
            assert_search_type_lookup(
                vec![
                    dense_entry(RowID::new(1_000), RowID::new(1_001), test_block_id(1001)),
                    dense_entry(
                        RowID::new(delta_u16_start),
                        RowID::new(delta_u16_start + 1),
                        test_block_id(1002),
                    ),
                ],
                RowID::new(delta_u16_start + 1),
                RowID::new(delta_u16_start),
                ColumnBlockLeafSearchType::DeltaU16,
                test_block_id(1002),
            )
            .await;

            let delta_u32_start = 1_000u64 + u16::MAX as u64 + 1;
            assert_search_type_lookup(
                vec![
                    dense_entry(RowID::new(1_000), RowID::new(1_001), test_block_id(2001)),
                    dense_entry(
                        RowID::new(delta_u32_start),
                        RowID::new(delta_u32_start + 1),
                        test_block_id(2002),
                    ),
                ],
                RowID::new(delta_u32_start + 1),
                RowID::new(delta_u32_start),
                ColumnBlockLeafSearchType::DeltaU32,
                test_block_id(2002),
            )
            .await;

            let plain_start = 1_000u64 + u32::MAX as u64 + 1;
            assert_search_type_lookup(
                vec![
                    dense_entry(RowID::new(1_000), RowID::new(1_001), test_block_id(3001)),
                    dense_entry(
                        RowID::new(plain_start),
                        RowID::new(plain_start + 1),
                        test_block_id(3002),
                    ),
                ],
                RowID::new(plain_start + 1),
                RowID::new(plain_start),
                ColumnBlockLeafSearchType::Plain,
                test_block_id(3002),
            )
            .await;
        });
    }

    /// Purpose: Protect persisted membership loading and scan identities for dense and sparse entries.
    /// Expected: Loaded row IDs and scan identities preserve the original membership and shape metadata.
    #[test]
    fn test_load_entry_row_ids_roundtrip_dense_and_sparse() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = metadata();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata, false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let disk_pool_guard = disk_pool.create_base_guard();
            let mut mutable = MutableTableFile::fork(
                &table,
                background_writes,
                disk_pool.global_pool().clone(),
                disk_pool_guard.clone(),
            );
            let root_block_id = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_insert(
                &mut mutable,
                &[
                    dense_entry(RowID::new(0), RowID::new(4), test_block_id(1001)),
                    sparse_entry(
                        RowID::new(10),
                        RowID::new(20),
                        test_row_ids([12, 15, 18]),
                        test_block_id(1002),
                    ),
                ],
                RowID::new(20),
                TrxID::new(2),
            )
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

            let index = ColumnBlockIndex::new(
                root_block_id,
                RowID::new(20),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            let dense = index.locate_block(RowID::new(2)).await.unwrap().unwrap();
            let sparse = index.locate_block(RowID::new(15)).await.unwrap().unwrap();

            let (identity, deletes) = index
                .load_entry_identity_and_deletions(&dense)
                .await
                .unwrap();
            assert!(deletes.is_empty());
            let dense_row_ids: Vec<_> = identity
                .as_ref()
                .iter_deltas()
                .map(|delta| dense.start_row_id + u64::from(delta))
                .collect();
            assert_eq!(dense_row_ids, test_row_ids([0, 1, 2, 3]));
            assert_eq!(
                dense.block_binding_value(),
                calculate_block_binding_value(
                    test_user_table_id(1),
                    RowID::new(0),
                    RowID::new(4),
                    4
                )
            );
            let (identity, deletes) = index
                .load_entry_identity_and_deletions(&sparse)
                .await
                .unwrap();
            assert!(deletes.is_empty());
            let sparse_row_ids: Vec<_> = identity
                .as_ref()
                .iter_deltas()
                .map(|delta| sparse.start_row_id + u64::from(delta))
                .collect();
            assert_eq!(sparse_row_ids, test_row_ids([12, 15, 18]));
            assert_eq!(
                sparse.block_binding_value(),
                calculate_block_binding_value(
                    test_user_table_id(1),
                    RowID::new(10),
                    RowID::new(20),
                    3
                )
            );
            let scan_entries = index.collect_scan_entries().await.unwrap();
            assert_eq!(scan_entries.len(), 2);
            assert_eq!(scan_entries[0].identity.codec(), COLUMN_ROW_CODEC_DENSE);
            assert_eq!(scan_entries[0].identity.row_id_span(), 4);
            assert_eq!(scan_entries[0].identity.body(), []);
            assert_eq!(
                scan_entries[1].identity,
                EncodedRowSet::plan(
                    RowID::new(0),
                    RowID::new(10),
                    &test_row_ids([2, 5, 8]),
                    &[],
                    4 * MAX_LWC_ROWS
                )
                .unwrap()
            );
            assert!(scan_entries[0].deletes.is_empty());
        });
    }

    /// Purpose: Protect physical row resolution for dense and sparse column blocks.
    /// Expected: Present rows retain their block, ordinal, and metadata; sparse gaps remain absent.
    #[test]
    fn test_resolve_row_dense_and_sparse() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = metadata();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata, false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let disk_pool_guard = disk_pool.create_base_guard();
            let mut mutable = MutableTableFile::fork(
                &table,
                background_writes,
                disk_pool.global_pool().clone(),
                disk_pool_guard.clone(),
            );
            let entries = vec![
                dense_entry(RowID::new(0), RowID::new(4), test_block_id(1001)),
                sparse_entry(
                    RowID::new(10),
                    RowID::new(20),
                    test_row_ids([12, 15, 18]),
                    test_block_id(1002),
                ),
            ];
            let root_block_id = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_insert(&mut mutable, &entries, RowID::new(20), TrxID::new(2))
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

            let index = ColumnBlockIndex::new(
                root_block_id,
                RowID::new(20),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            let dense_entry = index.locate_block(RowID::new(2)).await.unwrap().unwrap();
            let dense_resolved = index
                .locate_and_resolve_row(RowID::new(2))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(dense_resolved.block_id(), 1001);
            assert_eq!(dense_resolved.row_idx(), 2);
            assert_eq!(dense_resolved.leaf_block_id(), dense_entry.leaf_block_id);
            assert!(!dense_resolved.durable_deleted());
            assert_eq!(
                dense_resolved.block_binding_value(),
                dense_entry.block_binding_value()
            );

            let sparse_entry = index.locate_block(RowID::new(15)).await.unwrap().unwrap();
            let sparse_resolved = index
                .locate_and_resolve_row(RowID::new(15))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(sparse_resolved.block_id(), 1002);
            assert_eq!(sparse_resolved.row_idx(), 1);
            assert_eq!(sparse_resolved.leaf_block_id(), sparse_entry.leaf_block_id);
            assert!(!sparse_resolved.durable_deleted());
            assert_eq!(
                sparse_resolved.block_binding_value(),
                sparse_entry.block_binding_value()
            );
            assert!(
                index
                    .locate_and_resolve_row(RowID::new(14))
                    .await
                    .unwrap()
                    .is_none()
            );

            let one_descent = index
                .locate_and_resolve_row(RowID::new(18))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(one_descent.block_id(), 1002);
            assert_eq!(one_descent.row_idx(), 2);
            assert_eq!(
                one_descent.block_binding_value(),
                sparse_entry.block_binding_value()
            );
        });
    }

    /// Purpose: Protect persisted replacement of an inline row-id deletion set.
    /// Expected: The new deletion set remains inline and determines durable deletion without changing row membership.
    #[test]
    fn test_batch_replace_deletions_roundtrip_inline() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = metadata();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata, false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let disk_pool_guard = disk_pool.create_base_guard();
            let mut mutable = MutableTableFile::fork(
                &table,
                background_writes,
                disk_pool.global_pool().clone(),
                disk_pool_guard.clone(),
            );
            let root_v1 = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_insert(
                &mut mutable,
                &[dense_entry(
                    RowID::new(0),
                    RowID::new(8),
                    test_block_id(1001),
                )],
                RowID::new(8),
                TrxID::new(2),
            )
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

            let mut mutable = MutableTableFile::fork(
                &table,
                background_writes,
                disk_pool.global_pool().clone(),
                disk_pool_guard.clone(),
            );
            let root_v2 = ColumnBlockIndex::new(
                root_v1,
                RowID::new(8),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_replace_deletions(
                &mut mutable,
                &[ColumnDeletionPatch {
                    start_row_id: RowID::new(0),
                    deletions: &OrdinalDeletionSet::from_ordinals(8, &[1, 3, 6]).unwrap(),
                }],
                TrxID::new(3),
            )
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(3), false).await.unwrap();

            let index = ColumnBlockIndex::new(
                root_v2,
                RowID::new(8),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            let entry = index.locate_block(RowID::new(0)).await.unwrap().unwrap();
            assert_eq!(entry.block_id(), 1001);
            assert_eq!(entry.row_count(), 8);
            assert_eq!(entry.del_count(), 3);
            let (_, deletions) = index
                .load_entry_identity_and_deletions(&entry)
                .await
                .unwrap();
            let loaded: BTreeSet<_> = deletions.iter().collect();
            assert_eq!(loaded, BTreeSet::from([1u16, 3, 6]));
            assert!(
                index
                    .locate_and_resolve_row(RowID::new(1))
                    .await
                    .unwrap()
                    .unwrap()
                    .durable_deleted()
            );
            assert!(
                !index
                    .locate_and_resolve_row(RowID::new(2))
                    .await
                    .unwrap()
                    .unwrap()
                    .durable_deleted()
            );
        });
    }

    /// Purpose: Protect reachability and ready scan membership for large inline deletion sets.
    /// Expected: Only index and value blocks are reachable and scan ordinals preserve durable deletions.
    #[test]
    fn test_collect_reachable_blocks_with_inline_deletions() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = metadata();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata, false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let disk_pool_guard = disk_pool.create_base_guard();

            let entry = dense_entry_with_deletions(
                RowID::new(0),
                RowID::new(96),
                &(0..80).collect::<Vec<_>>(),
                test_block_id(1001),
            );
            let mut mutable = MutableTableFile::fork(
                &table,
                background_writes,
                disk_pool.global_pool().clone(),
                disk_pool_guard.clone(),
            );
            let root = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_insert(&mut mutable, &[entry], RowID::new(96), TrxID::new(2))
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

            let index = ColumnBlockIndex::new(
                root,
                RowID::new(96),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            let entry = index.locate_block(RowID::new(0)).await.unwrap().unwrap();
            assert!(
                index
                    .locate_and_resolve_row(RowID::new(40))
                    .await
                    .unwrap()
                    .unwrap()
                    .durable_deleted()
            );
            assert!(
                !index
                    .locate_and_resolve_row(RowID::new(90))
                    .await
                    .unwrap()
                    .unwrap()
                    .durable_deleted()
            );
            let mut reachable = BTreeSet::new();
            index
                .collect_reachable_blocks(&mut reachable)
                .await
                .unwrap();
            assert!(reachable.contains(&root));
            assert!(reachable.contains(&entry.block_id()));
            assert_eq!(reachable.len(), 2);

            let scan_entries = index.collect_scan_entries().await.unwrap();
            assert_eq!(
                scan_entries[0].deletes.iter().collect::<Vec<_>>(),
                (0..80).collect::<Vec<u16>>()
            );
        });
    }

    /// Purpose: Replace an existing durable ordinal set with a complete new set.
    /// Expected: Replacement retains physical identity and the requested deleted positions.
    #[test]
    fn test_batch_replace_deletions_replaces_existing_set() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = metadata();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata, false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let disk_pool_guard = disk_pool.create_base_guard();

            let seed = dense_entry_with_deletions(
                RowID::new(0),
                RowID::new(8),
                &[1, 3],
                test_block_id(1001),
            );
            let mut mutable = MutableTableFile::fork(
                &table,
                background_writes,
                disk_pool.global_pool().clone(),
                disk_pool_guard.clone(),
            );
            let root_v1 = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_insert(&mut mutable, &[seed], RowID::new(8), TrxID::new(2))
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

            let mut mutable = MutableTableFile::fork(
                &table,
                background_writes,
                disk_pool.global_pool().clone(),
                disk_pool_guard.clone(),
            );
            let root_v2 = ColumnBlockIndex::new(
                root_v1,
                RowID::new(8),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_replace_deletions(
                &mut mutable,
                &[ColumnDeletionPatch {
                    start_row_id: RowID::new(0),
                    deletions: &OrdinalDeletionSet::from_ordinals(8, &[6]).unwrap(),
                }],
                TrxID::new(3),
            )
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(3), false).await.unwrap();

            let index = ColumnBlockIndex::new(
                root_v2,
                RowID::new(8),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            let entry = index.locate_block(RowID::new(0)).await.unwrap().unwrap();
            assert_eq!(entry.del_count(), 1);
            assert!(
                !index
                    .locate_and_resolve_row(RowID::new(1))
                    .await
                    .unwrap()
                    .unwrap()
                    .durable_deleted()
            );
            let (_, deletions) = index
                .load_entry_identity_and_deletions(&entry)
                .await
                .unwrap();
            assert_eq!(
                deletions.iter().collect::<BTreeSet<_>>(),
                BTreeSet::from([6u16])
            );
        });
    }

    /// Purpose: Protect column-index lookups after a batch exceeds a single leaf's entry capacity.
    /// Expected: All entries remain enumerable and lookups on both sides of the capacity boundary resolve correctly.
    #[test]
    fn test_batch_insert_splits_leaf_pages() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let background_writes = fs.background_writes();
            let metadata = metadata();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata, false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let disk_pool_guard = disk_pool.create_base_guard();
            let mut mutable = MutableTableFile::fork(
                &table,
                background_writes,
                disk_pool.global_pool().clone(),
                disk_pool_guard.clone(),
            );
            let mut entries = Vec::new();
            for idx in 0..(COLUMN_BLOCK_MAX_ENTRIES + 32) as u64 {
                entries.push(dense_entry(
                    RowID::new(idx * 2),
                    RowID::new(idx * 2 + 2),
                    10_000 + idx,
                ));
            }
            let root = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            )
            .batch_insert(
                &mut mutable,
                &entries,
                entries.last().unwrap().end_row_id,
                TrxID::new(2),
            )
            .await
            .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();

            let index = ColumnBlockIndex::new(
                root,
                entries.last().unwrap().end_row_id,
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_pool_guard,
            );
            let collected = index.collect_leaf_entries().await.unwrap();
            assert_eq!(collected.len(), entries.len());
            assert_eq!(
                index
                    .locate_block(RowID::new(0))
                    .await
                    .unwrap()
                    .unwrap()
                    .block_id(),
                10_000
            );
            assert_eq!(
                index
                    .locate_block(RowID::new((COLUMN_BLOCK_MAX_ENTRIES as u64) * 2))
                    .await
                    .unwrap()
                    .unwrap()
                    .block_id(),
                BlockID::from(10_000 + COLUMN_BLOCK_MAX_ENTRIES as u64)
            );
        });
    }

    /// Purpose: Preserve list and adaptive identity bytes through ordinal deletion rewrites.
    /// Expected: Reopened entries retain codec bytes, binding values, ordinals, and physical membership while deletion state changes.
    #[test]
    fn mixed_identity_delete_rewrite_preserves_compact_bytes() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let table = fs
                .create_table_file(test_user_table_id(1), metadata(), false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(1), &table);
            let guard = disk_pool.create_base_guard();
            let values = [
                test_row_ids([0, 2, 7]),
                (1000..2000)
                    .filter(|i| i % 97 != 0)
                    .map(RowID::new)
                    .collect(),
            ];
            let bounds = [(0, 10), (1000, 2200)];
            let mut inputs = Vec::new();
            for (idx, ((start, end), rows)) in bounds.iter().zip(&values).enumerate() {
                let mut shape = ColumnBlockEntryShape::new(
                    test_user_table_id(1),
                    RowID::new(*start),
                    RowID::new(*end),
                    rows,
                    &[],
                )
                .unwrap();
                if idx == 0 {
                    // Literal legacy u32 list fixture, kept byte-compatible with tag 2.
                    shape.row_set =
                        RowSetRef::validate(2, &[0, 0, 0, 0, 2, 0, 0, 0, 7, 0, 0, 0], 10)
                            .unwrap()
                            .to_owned();
                }
                inputs.push(shape.with_block_id(test_block_id(1001 + idx as i32)));
            }
            let empty = ColumnBlockIndex::new(
                SUPER_BLOCK_ID,
                RowID::new(0),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &guard,
            );
            let mut mutable = MutableTableFile::fork(
                &table,
                fs.background_writes(),
                disk_pool.global_pool().clone(),
                guard.clone(),
            );
            let root = empty
                .batch_insert(&mut mutable, &inputs, RowID::new(2200), TrxID::new(2))
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(TrxID::new(2), false).await.unwrap();
            let index = ColumnBlockIndex::new(
                root,
                RowID::new(2200),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &guard,
            );
            let mut mutable = MutableTableFile::fork(
                &table,
                fs.background_writes(),
                disk_pool.global_pool().clone(),
                guard.clone(),
            );
            let deleted = [vec![0u16, 2], vec![0, values[1].len() as u16 - 1]];
            let replacements: Vec<_> = values
                .iter()
                .zip(&deleted)
                .map(|(rows, ordinals)| {
                    OrdinalDeletionSet::from_ordinals(rows.len() as u16, ordinals).unwrap()
                })
                .collect();
            let patches: Vec<_> = bounds
                .iter()
                .zip(&replacements)
                .map(|((start, _), deletions)| ColumnDeletionPatch {
                    start_row_id: RowID::new(*start),
                    deletions,
                })
                .collect();
            let updated = index
                .batch_replace_deletions(&mut mutable, &patches, TrxID::new(3))
                .await
                .unwrap();
            assert_ne!(updated, root);
            let (_table, _old_root) = mutable.commit(TrxID::new(3), false).await.unwrap();
            let index = ColumnBlockIndex::new(
                updated,
                RowID::new(2200),
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &guard,
            );
            let entries = index.collect_leaf_entries().await.unwrap();
            let scans = index.collect_scan_entries().await.unwrap();
            for (idx, entry) in entries.iter().enumerate() {
                let node = index.read_node(entry.leaf_block_id).await.unwrap();
                let view = index.read_entry_view(&node, entry).unwrap();
                let identity = row_set_in_view(&view).to_owned();
                assert_eq!(identity, inputs[idx].row_set);
                assert_eq!(entry.block_binding_value(), inputs[idx].block_binding_value);
                let (identity, deletes) = index
                    .load_entry_identity_and_deletions(entry)
                    .await
                    .unwrap();
                let loaded: Vec<_> = identity
                    .as_ref()
                    .iter_deltas()
                    .map(|delta| entry.start_row_id + u64::from(delta))
                    .collect();
                assert_eq!(deletes.iter().collect::<Vec<_>>(), deleted[idx]);
                assert_eq!(loaded, values[idx]);
                for (ordinal, row) in loaded.iter().enumerate() {
                    let resolved = index.locate_and_resolve_row(*row).await.unwrap().unwrap();
                    let delta = row.checked_sub(entry.start_row_id).unwrap() as u32;
                    assert_eq!(resolved.row_idx(), ordinal);
                    assert_eq!(
                        resolved.durable_deleted(),
                        deleted[idx].contains(&(ordinal as u16))
                    );
                    assert_eq!(
                        scans[idx].identity.ordinal_for_delta(delta),
                        Some(ordinal as u32)
                    );
                }
                let identity = &scans[idx].identity;
                assert_ne!(identity.body(), []);
                let clone = identity.clone();
                assert_eq!(clone.body().as_ptr(), identity.body().as_ptr());
            }
        });
    }

    /// Purpose: Preserve logical rows across whole lists and adaptive codecs with different partitions.
    /// Expected: Every encoding yields the source deltas and the same membership and ordinal mapping.
    #[test]
    fn compact_rows_match_whole_lists_across_partitions() {
        for deltas in [
            (0..1000).collect::<Vec<u32>>(),
            (0..1000).filter(|i| i % 11 != 0).collect(),
            vec![1, 2, 70000, 70001],
        ] {
            let end = u64::from(*deltas.last().unwrap()) + 20;
            let values: Vec<_> = deltas
                .iter()
                .map(|delta| RowID::new(u64::from(*delta)))
                .collect();
            let bytes: Vec<_> = deltas
                .iter()
                .flat_map(|delta| delta.to_le_bytes())
                .collect();
            let legacy = RowSetRef::validate(2, &bytes, end as u32)
                .unwrap()
                .to_owned();
            let mut seeds = Vec::new();
            for (idx, chunk) in values.chunks(31).enumerate() {
                IdentitySetSeed::append_page(&mut seeds, chunk, idx * 31);
            }
            for hints in [&[][..], seeds.as_slice()] {
                let adaptive = EncodedRowSet::plan(
                    RowID::new(0),
                    RowID::new(end),
                    &values,
                    hints,
                    4 * MAX_LWC_ROWS,
                )
                .unwrap();
                for rows in [&legacy, &adaptive] {
                    assert_eq!(rows.as_ref().iter_deltas().collect::<Vec<_>>(), deltas);
                    for (ordinal, delta) in deltas.iter().copied().enumerate() {
                        assert_eq!(rows.as_ref().ordinal_for_delta(delta), Some(ordinal as u16));
                        assert_eq!(rows.as_ref().delta_for_ordinal(ordinal as u16), Some(delta));
                    }
                }
            }
        }
    }
}
