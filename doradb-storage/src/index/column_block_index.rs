use crate::buffer::ReadonlyBufferPool;
use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::buffer::page::{Page, PageID};
use crate::error::{
    Error, PersistedFileKind, PersistedPageCorruptionCause, PersistedPageKind, Result,
};
use crate::file::cow_file::{COW_FILE_PAGE_SIZE, MutableCowFile};
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

pub const COLUMN_BLOCK_PAGE_SIZE: usize = COW_FILE_PAGE_SIZE;
pub const COLUMN_BLOCK_NODE_PAYLOAD_SIZE: usize = max_payload_len(COLUMN_BLOCK_PAGE_SIZE);
pub const COLUMN_BLOCK_HEADER_SIZE: usize = mem::size_of::<ColumnBlockNodeHeader>();
pub const COLUMN_BLOCK_DATA_SIZE: usize = COLUMN_BLOCK_NODE_PAYLOAD_SIZE - COLUMN_BLOCK_HEADER_SIZE;
pub const COLUMN_BRANCH_ENTRY_SIZE: usize = mem::size_of::<ColumnBlockBranchEntry>();

const COLUMN_LEAF_PREFIX_VERSION: u8 = 1;
const COLUMN_ROW_SECTION_VERSION: u8 = 1;
const COLUMN_DELETE_SECTION_VERSION: u8 = 1;
const COLUMN_DELETE_DOMAIN_ROW_ID_DELTA: u8 = 1;
const COLUMN_DELETE_DOMAIN_ORDINAL: u8 = 2;
const COLUMN_ROW_CODEC_NONE: u8 = 0;
const COLUMN_ROW_CODEC_DENSE: u8 = 1;
const COLUMN_ROW_CODEC_DELTA_LIST: u8 = 2;
const COLUMN_DELETE_CODEC_NONE: u8 = 0;
const COLUMN_DELETE_CODEC_INLINE_DELTA_LIST: u8 = 1;
const COLUMN_DELETE_CODEC_EXTERNAL_BLOB: u8 = 2;
const COLUMN_BLOB_REF_SIZE: usize =
    mem::size_of::<PageID>() + mem::size_of::<u16>() + mem::size_of::<u32>();
const LEGACY_INLINE_DELETE_FIELD_SIZE: usize = 120;
const LEGACY_INLINE_DELETE_U16_OFFSET: usize = 4;
const LEGACY_INLINE_DELETE_U32_OFFSET: usize = 4;
const LEGACY_INLINE_DELETE_U16_CAPACITY: usize =
    (LEGACY_INLINE_DELETE_FIELD_SIZE - LEGACY_INLINE_DELETE_U16_OFFSET) / 2;
const LEGACY_INLINE_DELETE_U32_CAPACITY: usize =
    (LEGACY_INLINE_DELETE_FIELD_SIZE - LEGACY_INLINE_DELETE_U32_OFFSET) / 4;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Pod, Zeroable)]
struct ColumnBlockLeafPrefix {
    start_row_id: [u8; 8],
    block_id: [u8; 8],
    row_id_span: [u8; 4],
    first_present_delta: [u8; 4],
    row_count: [u8; 2],
    del_count: [u8; 2],
    row_section_offset: [u8; 2],
    row_section_len: [u8; 2],
    delete_section_offset: [u8; 2],
    delete_section_len: [u8; 2],
    row_codec: u8,
    delete_codec: u8,
    delete_domain: u8,
    prefix_version: u8,
    flags: u8,
    reserved: [u8; 7],
}

pub const COLUMN_BLOCK_LEAF_PREFIX_SIZE: usize = mem::size_of::<ColumnBlockLeafPrefix>();
pub const COLUMN_BLOCK_MAX_ENTRIES: usize = COLUMN_BLOCK_DATA_SIZE / COLUMN_BLOCK_LEAF_PREFIX_SIZE;
pub const COLUMN_BLOCK_MAX_BRANCH_ENTRIES: usize =
    COLUMN_BLOCK_DATA_SIZE / COLUMN_BRANCH_ENTRY_SIZE;

const _: () = assert!(mem::size_of::<ColumnBlockLeafPrefix>() == 48);
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

#[repr(C)]
/// Header stored at the beginning of each on-disk column block-index node.
#[derive(Clone, Copy, Debug, Default, Pod, Zeroable)]
pub struct ColumnBlockNodeHeader {
    pub height: u32,
    pub count: u32,
    pub start_row_id: RowID,
    pub create_ts: u64,
}

#[repr(C)]
/// Branch entry mapping a child lower bound to a child page id.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Pod, Zeroable)]
pub struct ColumnBlockBranchEntry {
    pub start_row_id: RowID,
    pub page_id: PageID,
}

#[repr(C)]
/// Fixed-size node payload protected by the shared page-integrity envelope.
#[derive(Clone)]
pub struct ColumnBlockNode {
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

    #[inline]
    fn branch_entries_mut(&mut self) -> &mut [ColumnBlockBranchEntry] {
        branch_entries_from_bytes_mut(&mut self.data, self.header.count as usize)
    }
}

/// Row-shape metadata carried alongside one freshly built LWC page before the
/// page id is assigned in the table file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnBlockEntryShape {
    start_row_id: RowID,
    end_row_id: RowID,
    row_ids: Vec<RowID>,
    delete_deltas: Vec<u32>,
    delete_domain: ColumnDeleteDomain,
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
        Ok(ColumnBlockEntryShape {
            start_row_id,
            end_row_id,
            row_ids,
            delete_deltas,
            delete_domain: ColumnDeleteDomain::RowIdDelta,
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

    pub(crate) fn set_end_row_id(&mut self, end_row_id: RowID) -> Result<()> {
        validate_row_ids(&self.row_ids, self.start_row_id, end_row_id)?;
        self.end_row_id = end_row_id;
        Ok(())
    }

    #[inline]
    pub(crate) fn with_delete_domain(mut self, delete_domain: ColumnDeleteDomain) -> Self {
        self.delete_domain = delete_domain;
        self
    }

    #[inline]
    pub(crate) fn with_block_id(self, block_id: PageID) -> ColumnBlockEntryInput {
        ColumnBlockEntryInput {
            start_row_id: self.start_row_id,
            end_row_id: self.end_row_id,
            block_id,
            row_ids: self.row_ids,
            delete_deltas: self.delete_deltas,
            delete_domain: self.delete_domain,
        }
    }
}

/// Fully materialized phase-1 logical entry used by v2 builders and rewrite flows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnBlockEntryInput {
    start_row_id: RowID,
    end_row_id: RowID,
    block_id: PageID,
    row_ids: Vec<RowID>,
    delete_deltas: Vec<u32>,
    delete_domain: ColumnDeleteDomain,
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
    pub start_row_id: RowID,
    pub entry: ColumnBlockEntryInput,
}

/// One resolved leaf entry from the persisted column block-index tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnLeafEntry {
    pub leaf_page_id: PageID,
    pub start_row_id: RowID,
    block_id: PageID,
    end_row_id: RowID,
    row_count: u16,
    del_count: u16,
    row_id_span: u32,
    first_present_delta: u32,
    row_section_offset: u16,
    row_section_len: u16,
    delete_section_offset: u16,
    delete_section_len: u16,
    row_section_kind: u8,
    row_section_version: u8,
    row_section_flags: u8,
    row_section_aux: u8,
    delete_section_kind: u8,
    delete_section_version: u8,
    delete_section_flags: u8,
    delete_section_aux: u8,
    delete_domain: ColumnDeleteDomain,
    delete_blob_ref: Option<BlobRef>,
}

impl ColumnLeafEntry {
    /// Returns the persisted LWC block page id.
    #[inline]
    pub fn block_id(&self) -> PageID {
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
}

/// Runtime row resolution result for one persisted columnar row lookup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedColumnRow {
    leaf_page_id: PageID,
    block_id: PageID,
    row_idx: usize,
}

impl ResolvedColumnRow {
    /// Returns the leaf page that produced this resolution result.
    #[inline]
    pub fn leaf_page_id(&self) -> PageID {
        self.leaf_page_id
    }

    /// Returns the persisted LWC block page id that stores the row values.
    #[inline]
    pub fn block_id(&self) -> PageID {
        self.block_id
    }

    /// Returns the resolved ordinal inside the persisted LWC page.
    #[inline]
    pub fn row_idx(&self) -> usize {
        self.row_idx
    }
}

/// One authoritative delete-delta rewrite keyed by leaf `start_row_id`.
#[derive(Clone, Copy, Debug)]
pub struct ColumnDeleteDeltaPatch<'a> {
    pub start_row_id: RowID,
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
    block_id: PageID,
    row_set: LogicalRowSet,
    delete_set: LogicalDeleteSet,
}

impl LogicalLeafEntry {
    fn new(
        start_row_id: RowID,
        block_id: PageID,
        row_set: LogicalRowSet,
        delete_set: LogicalDeleteSet,
    ) -> Self {
        LogicalLeafEntry {
            start_row_id,
            block_id,
            row_set,
            delete_set,
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
    block_id: PageID,
    row_id_span: u32,
    first_present_delta: u32,
    row_count: u16,
    del_count: u16,
    row_section_kind: u8,
    delete_section_kind: u8,
    delete_domain: u8,
    row_section: Vec<u8>,
    delete_section: Vec<u8>,
}

impl EncodedLeafEntry {
    fn from_logical(entry: &LogicalLeafEntry) -> Result<Self> {
        let (row_section_kind, row_section, row_id_span, first_present_delta, row_count) =
            encode_row_section(&entry.row_set)?;
        let (delete_section_kind, delete_domain, delete_section) =
            encode_delete_section(&entry.row_set, &entry.delete_set)?;
        Ok(EncodedLeafEntry {
            start_row_id: entry.start_row_id,
            block_id: entry.block_id,
            row_id_span,
            first_present_delta,
            row_count,
            del_count: entry.delete_set.del_count()?,
            row_section_kind,
            delete_section_kind,
            delete_domain: delete_domain.encode(),
            row_section,
            delete_section,
        })
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        COLUMN_BLOCK_LEAF_PREFIX_SIZE + self.row_section.len() + self.delete_section.len()
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
    prefix: ColumnBlockLeafPrefix,
    row_section: &'a [u8],
    delete_section: Option<&'a [u8]>,
    row_header: SectionHeader,
    delete_header: Option<SectionHeader>,
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

pub struct ColumnBlockIndex<'a> {
    disk_pool: &'a ReadonlyBufferPool,
    root_page_id: PageID,
    end_row_id: RowID,
}

#[derive(Clone, Debug)]
struct NodeRewriteResult {
    entries: Vec<ColumnBlockBranchEntry>,
    touched: bool,
}

#[inline]
fn invalid_node_payload(file_kind: PersistedFileKind, page_id: PageID) -> Error {
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
    page_id: PageID,
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
    page_id: PageID,
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
    guard: PageSharedGuard<Page>,
    header: ColumnBlockNodeHeader,
}

impl ValidatedColumnBlockNode {
    #[inline]
    fn try_from_guard(
        guard: PageSharedGuard<Page>,
        file_kind: PersistedFileKind,
        page_id: PageID,
    ) -> Result<Self> {
        let (_, header) = validate_node_payload(guard.page(), file_kind, page_id)?;
        Ok(ValidatedColumnBlockNode { guard, header })
    }

    fn leaf_prefixes(
        &self,
        file_kind: PersistedFileKind,
        page_id: PageID,
    ) -> Result<&[ColumnBlockLeafPrefix]> {
        let count = self.header.count as usize;
        let prefix_bytes = count * COLUMN_BLOCK_LEAF_PREFIX_SIZE;
        let data = self.data_ref();
        let prefixes = try_cast_slice(&data[..prefix_bytes])
            .map_err(|_| invalid_node_payload(file_kind, page_id))?;
        validate_leaf_prefixes(prefixes, self.header.start_row_id, data, file_kind, page_id)?;
        Ok(prefixes)
    }

    fn leaf_entry_view(
        &self,
        idx: usize,
        file_kind: PersistedFileKind,
        page_id: PageID,
    ) -> Result<LeafEntryView<'_>> {
        let prefixes = self.leaf_prefixes(file_kind, page_id)?;
        let prefix = *prefixes
            .get(idx)
            .ok_or_else(|| invalid_node_payload(file_kind, page_id))?;
        let data = self.data_ref();
        let prefix_end = prefixes.len() * COLUMN_BLOCK_LEAF_PREFIX_SIZE;
        let row_section = section_slice(
            data,
            prefix_end,
            prefix.row_section_offset(),
            prefix.row_section_len(),
            file_kind,
            page_id,
        )?;
        let row_header = SectionHeader::decode(row_section)
            .map_err(|_| invalid_node_payload(file_kind, page_id))?;
        if row_header.kind != prefix.row_codec() || row_header.version != COLUMN_ROW_SECTION_VERSION
        {
            return Err(invalid_node_payload(file_kind, page_id));
        }
        let delete_section = if prefix.delete_section_len() == 0 {
            None
        } else {
            Some(section_slice(
                data,
                prefix_end,
                prefix.delete_section_offset(),
                prefix.delete_section_len(),
                file_kind,
                page_id,
            )?)
        };
        let delete_header = match delete_section {
            Some(bytes) => {
                let header = SectionHeader::decode(bytes)
                    .map_err(|_| invalid_node_payload(file_kind, page_id))?;
                if header.kind != prefix.delete_codec()
                    || header.version != COLUMN_DELETE_SECTION_VERSION
                {
                    return Err(invalid_node_payload(file_kind, page_id));
                }
                Some(header)
            }
            None => None,
        };
        Ok(LeafEntryView {
            prefix,
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

impl ColumnBlockLeafPrefix {
    #[inline]
    fn start_row_id(&self) -> RowID {
        u64::from_le_bytes(self.start_row_id)
    }

    #[inline]
    fn block_id(&self) -> PageID {
        u64::from_le_bytes(self.block_id)
    }

    #[inline]
    fn row_id_span(&self) -> u32 {
        u32::from_le_bytes(self.row_id_span)
    }

    #[inline]
    fn first_present_delta(&self) -> u32 {
        u32::from_le_bytes(self.first_present_delta)
    }

    #[inline]
    fn row_count(&self) -> u16 {
        u16::from_le_bytes(self.row_count)
    }

    #[inline]
    fn del_count(&self) -> u16 {
        u16::from_le_bytes(self.del_count)
    }

    #[inline]
    fn row_section_offset(&self) -> u16 {
        u16::from_le_bytes(self.row_section_offset)
    }

    #[inline]
    fn row_section_len(&self) -> u16 {
        u16::from_le_bytes(self.row_section_len)
    }

    #[inline]
    fn delete_section_offset(&self) -> u16 {
        u16::from_le_bytes(self.delete_section_offset)
    }

    #[inline]
    fn delete_section_len(&self) -> u16 {
        u16::from_le_bytes(self.delete_section_len)
    }

    #[inline]
    fn row_codec(&self) -> u8 {
        self.row_codec
    }

    #[inline]
    fn delete_codec(&self) -> u8 {
        self.delete_codec
    }

    #[inline]
    fn delete_domain(&self) -> u8 {
        self.delete_domain
    }

    #[inline]
    fn end_row_id(&self) -> Result<RowID> {
        self.start_row_id()
            .checked_add(self.row_id_span() as RowID)
            .ok_or(Error::InvalidFormat)
    }

    fn from_encoded(
        entry: &EncodedLeafEntry,
        row_section_offset: u16,
        row_section_len: u16,
        delete_section_offset: u16,
        delete_section_len: u16,
    ) -> Self {
        ColumnBlockLeafPrefix {
            start_row_id: entry.start_row_id.to_le_bytes(),
            block_id: entry.block_id.to_le_bytes(),
            row_id_span: entry.row_id_span.to_le_bytes(),
            first_present_delta: entry.first_present_delta.to_le_bytes(),
            row_count: entry.row_count.to_le_bytes(),
            del_count: entry.del_count.to_le_bytes(),
            row_section_offset: row_section_offset.to_le_bytes(),
            row_section_len: row_section_len.to_le_bytes(),
            delete_section_offset: delete_section_offset.to_le_bytes(),
            delete_section_len: delete_section_len.to_le_bytes(),
            row_codec: entry.row_section_kind,
            delete_codec: entry.delete_section_kind,
            delete_domain: entry.delete_domain,
            prefix_version: COLUMN_LEAF_PREFIX_VERSION,
            flags: 0,
            reserved: [0; 7],
        }
    }
}

impl<'a> ColumnBlockIndex<'a> {
    /// Creates a column block-index view for one root page snapshot.
    #[inline]
    pub fn new(root_page_id: PageID, end_row_id: RowID, disk_pool: &'a ReadonlyBufferPool) -> Self {
        ColumnBlockIndex {
            disk_pool,
            root_page_id,
            end_row_id,
        }
    }

    /// Returns current column block-index root page id.
    #[inline]
    pub fn root_page_id(&self) -> PageID {
        self.root_page_id
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
    async fn read_node(&self, page_id: PageID) -> Result<ValidatedColumnBlockNode> {
        let g = self
            .disk_pool
            .get_validated_page_shared(page_id, validate_persisted_column_block_index_page)
            .await?;
        ValidatedColumnBlockNode::try_from_guard(g, self.file_kind(), page_id)
    }

    /// Finds the persisted leaf entry whose coverage contains `row_id`.
    pub async fn locate_block(&self, row_id: RowID) -> Result<Option<ColumnLeafEntry>> {
        if self.root_page_id == 0 || row_id >= self.end_row_id {
            return Ok(None);
        }
        let mut page_id = self.root_page_id;
        loop {
            let node = self.read_node(page_id).await?;
            if node.is_leaf() {
                let prefixes = node.leaf_prefixes(self.file_kind(), page_id)?;
                let idx = match search_start_row_id(prefixes, row_id) {
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
        let prefixes = node.leaf_prefixes(self.file_kind(), entry.leaf_page_id)?;
        let idx = prefixes
            .binary_search_by_key(&entry.start_row_id, |prefix| prefix.start_row_id())
            .map_err(|_| invalid_node_payload(self.file_kind(), entry.leaf_page_id))?;
        let view = node.leaf_entry_view(idx, self.file_kind(), entry.leaf_page_id)?;
        let Some(row_idx) =
            resolve_row_idx_in_view(&view, row_id, self.file_kind(), entry.leaf_page_id)?
        else {
            return Ok(None);
        };
        Ok(Some(build_resolved_row(entry.leaf_page_id, &view, row_idx)))
    }

    /// Locates and resolves one persisted row id in a single tree descent.
    pub async fn locate_and_resolve_row(&self, row_id: RowID) -> Result<Option<ResolvedColumnRow>> {
        if self.root_page_id == 0 || row_id >= self.end_row_id {
            return Ok(None);
        }
        let mut page_id = self.root_page_id;
        loop {
            let node = self.read_node(page_id).await?;
            if node.is_leaf() {
                let prefixes = node.leaf_prefixes(self.file_kind(), page_id)?;
                let idx = match search_start_row_id(prefixes, row_id) {
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

    /// Reads delete payload bytes from an external blob reference.
    pub async fn read_delete_payload_bytes(
        &self,
        entry: &ColumnLeafEntry,
    ) -> Result<Option<Vec<u8>>> {
        let blob_ref = match entry.deletion_blob_ref() {
            Some(blob_ref) => blob_ref,
            None => return Ok(None),
        };
        let reader = ColumnDeletionBlobReader::new(self.disk_pool);
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
        if entry.delete_section_kind != COLUMN_DELETE_CODEC_EXTERNAL_BLOB
            || entry.delete_section_aux != COLUMN_AUX_BLOB_KIND_DELETE_DELTAS
            || header.blob_kind() != entry.delete_section_aux
            || header.codec_kind() != COLUMN_AUX_BLOB_CODEC_U32_DELTA_LIST
            || header.codec_version() != entry.delete_section_version
        {
            return Err(Error::persisted_page_corrupted(
                self.file_kind(),
                PersistedPageKind::ColumnDeletionBlob,
                blob_ref.start_page_id,
                PersistedPageCorruptionCause::InvalidPayload,
            ));
        }
        Ok(Some(payload))
    }

    /// Loads validated delete deltas for one persisted entry.
    pub(crate) async fn load_delete_deltas(&self, entry: &ColumnLeafEntry) -> Result<Vec<u32>> {
        let node = self.read_node(entry.leaf_page_id).await?;
        let prefixes = node.leaf_prefixes(self.file_kind(), entry.leaf_page_id)?;
        let idx = prefixes
            .binary_search_by_key(&entry.start_row_id, |prefix| prefix.start_row_id())
            .map_err(|_| invalid_node_payload(self.file_kind(), entry.leaf_page_id))?;
        let view = node.leaf_entry_view(idx, self.file_kind(), entry.leaf_page_id)?;
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

    async fn decode_logical_delete_set(
        &self,
        view: &LeafEntryView<'_>,
        row_set: &LogicalRowSet,
        page_id: PageID,
    ) -> Result<LogicalDeleteSet> {
        let delete_domain = decode_delete_domain_or_invalid_node(
            view.prefix.delete_domain(),
            self.file_kind(),
            page_id,
        )?;
        match view.prefix.delete_codec() {
            COLUMN_DELETE_CODEC_NONE => Ok(LogicalDeleteSet::None {
                domain: delete_domain,
            }),
            COLUMN_DELETE_CODEC_INLINE_DELTA_LIST => {
                let bytes = view
                    .delete_section
                    .ok_or_else(|| invalid_node_payload(self.file_kind(), page_id))?;
                let row_id_deltas = decode_delete_rows(
                    &bytes[4..],
                    view.prefix.del_count(),
                    delete_domain,
                    row_set,
                    self.file_kind(),
                    page_id,
                )?;
                Ok(LogicalDeleteSet::Inline {
                    domain: delete_domain,
                    row_id_deltas,
                })
            }
            COLUMN_DELETE_CODEC_EXTERNAL_BLOB => {
                let bytes = view
                    .delete_section
                    .ok_or_else(|| invalid_node_payload(self.file_kind(), page_id))?;
                let blob_ref = decode_blob_ref(&bytes[4..])
                    .map_err(|_| invalid_node_payload(self.file_kind(), page_id))?;
                let reader = ColumnDeletionBlobReader::new(self.disk_pool);
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
                    view.prefix.del_count(),
                    delete_domain,
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
                    domain: delete_domain,
                    del_count: view.prefix.del_count(),
                    blob_ref,
                    row_id_deltas: Some(row_id_deltas),
                })
            }
            _ => Err(invalid_node_payload(self.file_kind(), page_id)),
        }
    }

    async fn load_rewrite_context(
        &self,
        start_row_id: RowID,
    ) -> Result<(LogicalRowSet, ColumnDeleteDomain)> {
        if self.root_page_id == 0 {
            return Err(Error::InvalidArgument);
        }
        let mut page_id = self.root_page_id;
        loop {
            let node = self.read_node(page_id).await?;
            if node.is_leaf() {
                let prefixes = node.leaf_prefixes(self.file_kind(), page_id)?;
                let idx = prefixes
                    .binary_search_by_key(&start_row_id, |prefix| prefix.start_row_id())
                    .map_err(|_| Error::InvalidArgument)?;
                let view = node.leaf_entry_view(idx, self.file_kind(), page_id)?;
                let row_set = decode_logical_row_set(&view, self.file_kind(), page_id)?;
                let delete_domain = decode_delete_domain_or_invalid_node(
                    view.prefix.delete_domain(),
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
        if self.root_page_id == 0 {
            return Ok(Vec::new());
        }
        let mut stack = vec![self.root_page_id];
        let mut entries = Vec::new();
        let mut last_end = None;
        while let Some(page_id) = stack.pop() {
            let node = self.read_node(page_id).await?;
            if node.is_leaf() {
                let prefixes = node.leaf_prefixes(self.file_kind(), page_id)?;
                for idx in 0..prefixes.len() {
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
                if entry.page_id == 0 {
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
    ) -> Result<PageID> {
        if patches.is_empty() {
            return Ok(self.root_page_id);
        }
        if self.root_page_id == 0 || !delete_delta_patches_sorted_unique(patches) {
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

        let root_height = self.read_node(self.root_page_id).await?.header.height;
        let res = self
            .rewrite_subtree_with_patches(mutable_file, self.root_page_id, &resolved, create_ts)
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
    ) -> Result<PageID> {
        if patches.is_empty() {
            return Ok(self.root_page_id);
        }
        if self.root_page_id == 0
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
        let root_height = self.read_node(self.root_page_id).await?.header.height;
        let res = self
            .rewrite_subtree_with_patches(mutable_file, self.root_page_id, &resolved, create_ts)
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
    ) -> Result<PageID> {
        if entries.is_empty() {
            return Ok(self.root_page_id);
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
        if self.root_page_id == 0 {
            return self
                .build_tree_from_logical_entries(mutable_file, &logical_entries, create_ts)
                .await;
        }

        let root_height = self.read_node(self.root_page_id).await?.header.height;
        let new_root_entries = self
            .append_rightmost_path(mutable_file, &logical_entries, create_ts)
            .await?;
        self.finalize_root_rewrite(mutable_file, root_height, new_root_entries, create_ts)
            .await
    }

    fn rewrite_subtree_with_patches<'b, M: MutableCowFile + 'b>(
        &'b self,
        mutable_file: &'b mut M,
        page_id: PageID,
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
        page_id: PageID,
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
        page_id: PageID,
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
    ) -> Result<PageID> {
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
        let mut page_id = self.root_page_id;
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
    ) -> Result<PageID> {
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
        page_id: PageID,
    ) -> Result<Vec<LogicalLeafEntry>> {
        let prefixes = node.leaf_prefixes(self.file_kind(), page_id)?;
        let mut entries = Vec::with_capacity(prefixes.len());
        for idx in 0..prefixes.len() {
            let view = node.leaf_entry_view(idx, self.file_kind(), page_id)?;
            let row_set = decode_logical_row_set(&view, self.file_kind(), page_id)?;
            let delete_set =
                decode_logical_delete_set_without_blob(&view, &row_set, self.file_kind(), page_id)?;
            entries.push(LogicalLeafEntry::new(
                view.prefix.start_row_id(),
                view.prefix.block_id(),
                row_set,
                delete_set,
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
            let mut used = 0usize;
            let mut end = start;
            while end < encoded.len() {
                let next = encoded[end].encoded_len();
                if end > start && used + next > COLUMN_BLOCK_DATA_SIZE {
                    break;
                }
                if next > COLUMN_BLOCK_DATA_SIZE {
                    return Err(Error::InvalidArgument);
                }
                used += next;
                end += 1;
            }
            let chunk = &encoded[start..end];
            let (page_id, mut node) =
                self.allocate_node(mutable_file, 0, chunk[0].start_row_id, create_ts)?;
            node.header.count = chunk.len() as u32;
            encode_leaf_chunk(node.data_mut(), chunk)?;
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
    ) -> Result<PageID> {
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
    ) -> Result<(PageID, Box<ColumnBlockNode>)> {
        let page_id = table_file.allocate_page_id()?;
        let node = ColumnBlockNode::new_boxed(height, start_row_id, create_ts);
        Ok((page_id, node))
    }

    /// Record an obsolete node page to be reclaimed after commit.
    #[inline]
    pub fn record_obsolete_node<M: MutableCowFile>(
        &self,
        table_file: &mut M,
        page_id: PageID,
    ) -> Result<()> {
        if page_id == 0 {
            return Err(Error::InvalidState);
        }
        table_file.record_gc_page(page_id);
        Ok(())
    }

    async fn write_node<M: MutableCowFile>(
        &self,
        mutable_file: &M,
        page_id: PageID,
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
        mutable_file.write_page(page_id, buf).await
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

fn section_slice(
    data: &[u8],
    prefix_end: usize,
    offset: u16,
    len: u16,
    file_kind: PersistedFileKind,
    page_id: PageID,
) -> Result<&[u8]> {
    let offset = offset as usize;
    let len = len as usize;
    if offset == 0 || len == 0 {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    let end = offset
        .checked_add(len)
        .ok_or_else(|| invalid_node_payload(file_kind, page_id))?;
    if offset < prefix_end || end > data.len() {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    Ok(&data[offset..end])
}

fn validate_leaf_prefixes(
    prefixes: &[ColumnBlockLeafPrefix],
    header_start_row_id: RowID,
    data: &[u8],
    file_kind: PersistedFileKind,
    page_id: PageID,
) -> Result<()> {
    if prefixes.is_empty() {
        return Ok(());
    }
    let prefix_end = prefixes.len() * COLUMN_BLOCK_LEAF_PREFIX_SIZE;
    let mut ranges = Vec::with_capacity(prefixes.len() * 2);
    let mut last_end = None;
    for (idx, prefix) in prefixes.iter().enumerate() {
        if ColumnDeleteDomain::decode(prefix.delete_domain()).is_err() {
            return Err(invalid_node_payload(file_kind, page_id));
        }
        if prefix.prefix_version != COLUMN_LEAF_PREFIX_VERSION
            || prefix.row_codec() == COLUMN_ROW_CODEC_NONE
            || prefix.row_id_span() == 0
            || prefix.row_count() == 0
            || u32::from(prefix.row_count()) > prefix.row_id_span()
            || prefix.del_count() > prefix.row_count()
        {
            return Err(invalid_node_payload(file_kind, page_id));
        }
        let start = prefix.start_row_id();
        let end = prefix
            .end_row_id()
            .map_err(|_| invalid_node_payload(file_kind, page_id))?;
        if idx == 0 && start != header_start_row_id {
            return Err(invalid_node_payload(file_kind, page_id));
        }
        if let Some(prev_end) = last_end
            && start < prev_end
        {
            return Err(invalid_node_payload(file_kind, page_id));
        }
        last_end = Some(end);

        let row_range = validate_present_section_range(
            prefix_end,
            prefix.row_section_offset(),
            prefix.row_section_len(),
            data.len(),
            file_kind,
            page_id,
        )?;
        let row_header = SectionHeader::decode(&data[row_range.0..row_range.0 + 4])
            .map_err(|_| invalid_node_payload(file_kind, page_id))?;
        match prefix.row_codec() {
            COLUMN_ROW_CODEC_DENSE => {
                if row_header.kind != COLUMN_ROW_CODEC_DENSE
                    || row_header.version != COLUMN_ROW_SECTION_VERSION
                    || row_range.1 - row_range.0 != 4
                    || prefix.first_present_delta() != 0
                    || u32::from(prefix.row_count()) != prefix.row_id_span()
                {
                    return Err(invalid_node_payload(file_kind, page_id));
                }
            }
            COLUMN_ROW_CODEC_DELTA_LIST => {
                let expected_len = 4 + prefix.row_count() as usize * mem::size_of::<u32>();
                if row_header.kind != COLUMN_ROW_CODEC_DELTA_LIST
                    || row_header.version != COLUMN_ROW_SECTION_VERSION
                    || row_range.1 - row_range.0 != expected_len
                {
                    return Err(invalid_node_payload(file_kind, page_id));
                }
            }
            _ => return Err(invalid_node_payload(file_kind, page_id)),
        }
        ranges.push(row_range);

        match prefix.delete_codec() {
            COLUMN_DELETE_CODEC_NONE => {
                if prefix.del_count() != 0
                    || prefix.delete_section_offset() != 0
                    || prefix.delete_section_len() != 0
                {
                    return Err(invalid_node_payload(file_kind, page_id));
                }
            }
            COLUMN_DELETE_CODEC_INLINE_DELTA_LIST => {
                if prefix.del_count() == 0 {
                    return Err(invalid_node_payload(file_kind, page_id));
                }
                let delete_range = validate_present_section_range(
                    prefix_end,
                    prefix.delete_section_offset(),
                    prefix.delete_section_len(),
                    data.len(),
                    file_kind,
                    page_id,
                )?;
                let delete_header =
                    SectionHeader::decode(&data[delete_range.0..delete_range.0 + 4])
                        .map_err(|_| invalid_node_payload(file_kind, page_id))?;
                let expected_len = 4 + prefix.del_count() as usize * mem::size_of::<u32>();
                if delete_header.kind != COLUMN_DELETE_CODEC_INLINE_DELTA_LIST
                    || delete_header.version != COLUMN_DELETE_SECTION_VERSION
                    || delete_range.1 - delete_range.0 != expected_len
                {
                    return Err(invalid_node_payload(file_kind, page_id));
                }
                ranges.push(delete_range);
            }
            COLUMN_DELETE_CODEC_EXTERNAL_BLOB => {
                if prefix.del_count() == 0 {
                    return Err(invalid_node_payload(file_kind, page_id));
                }
                let delete_range = validate_present_section_range(
                    prefix_end,
                    prefix.delete_section_offset(),
                    prefix.delete_section_len(),
                    data.len(),
                    file_kind,
                    page_id,
                )?;
                let delete_header =
                    SectionHeader::decode(&data[delete_range.0..delete_range.0 + 4])
                        .map_err(|_| invalid_node_payload(file_kind, page_id))?;
                if delete_header.kind != COLUMN_DELETE_CODEC_EXTERNAL_BLOB
                    || delete_header.version != COLUMN_DELETE_SECTION_VERSION
                    || delete_header.aux != COLUMN_AUX_BLOB_KIND_DELETE_DELTAS
                    || delete_range.1 - delete_range.0 != 4 + COLUMN_BLOB_REF_SIZE
                {
                    return Err(invalid_node_payload(file_kind, page_id));
                }
                ranges.push(delete_range);
            }
            _ => return Err(invalid_node_payload(file_kind, page_id)),
        }
    }
    ranges.sort_unstable_by_key(|range| range.0);
    for pair in ranges.windows(2) {
        if pair[0].1 > pair[1].0 {
            return Err(invalid_node_payload(file_kind, page_id));
        }
    }
    Ok(())
}

fn validate_present_section_range(
    prefix_end: usize,
    offset: u16,
    len: u16,
    data_len: usize,
    file_kind: PersistedFileKind,
    page_id: PageID,
) -> Result<(usize, usize)> {
    let offset = offset as usize;
    let len = len as usize;
    if offset == 0 || len == 0 {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    let end = offset
        .checked_add(len)
        .ok_or_else(|| invalid_node_payload(file_kind, page_id))?;
    if offset < prefix_end || end > data_len {
        return Err(invalid_node_payload(file_kind, page_id));
    }
    Ok((offset, end))
}

fn entry_contains_row_id(
    view: &LeafEntryView<'_>,
    row_id: RowID,
    file_kind: PersistedFileKind,
    page_id: PageID,
) -> Result<bool> {
    Ok(resolve_row_idx_in_view(view, row_id, file_kind, page_id)?.is_some())
}

fn resolve_row_idx_in_view(
    view: &LeafEntryView<'_>,
    row_id: RowID,
    file_kind: PersistedFileKind,
    page_id: PageID,
) -> Result<Option<usize>> {
    let start_row_id = view.prefix.start_row_id();
    let delta_u64 = match row_id.checked_sub(start_row_id) {
        Some(delta) => delta,
        None => return Ok(None),
    };
    if delta_u64 >= view.prefix.row_id_span() as u64 {
        return Ok(None);
    }
    let delta = delta_u64 as u32;
    if delta < view.prefix.first_present_delta() {
        return Ok(None);
    }
    match view.row_header.kind {
        COLUMN_ROW_CODEC_DENSE => Ok(Some(delta as usize)),
        COLUMN_ROW_CODEC_DELTA_LIST => resolve_delta_list_row_idx(view, delta, file_kind, page_id),
        _ => Err(invalid_node_payload(file_kind, page_id)),
    }
}

fn resolve_delta_list_row_idx(
    view: &LeafEntryView<'_>,
    delta: u32,
    file_kind: PersistedFileKind,
    page_id: PageID,
) -> Result<Option<usize>> {
    let deltas = decode_u32_row_deltas(
        &view.row_section[4..],
        view.prefix.row_count(),
        view.prefix.row_id_span(),
        view.prefix.first_present_delta(),
        file_kind,
        page_id,
    )?;
    Ok(deltas.binary_search(&delta).ok())
}

fn decode_logical_row_set(
    view: &LeafEntryView<'_>,
    file_kind: PersistedFileKind,
    page_id: PageID,
) -> Result<LogicalRowSet> {
    match view.row_header.kind {
        COLUMN_ROW_CODEC_DENSE => Ok(LogicalRowSet::Dense {
            row_id_span: view.prefix.row_id_span(),
        }),
        COLUMN_ROW_CODEC_DELTA_LIST => {
            let deltas = decode_u32_row_deltas(
                &view.row_section[4..],
                view.prefix.row_count(),
                view.prefix.row_id_span(),
                view.prefix.first_present_delta(),
                file_kind,
                page_id,
            )?;
            Ok(LogicalRowSet::DeltaList {
                row_id_span: view.prefix.row_id_span(),
                first_present_delta: view.prefix.first_present_delta(),
                deltas,
            })
        }
        _ => Err(invalid_node_payload(file_kind, page_id)),
    }
}

fn decode_logical_delete_set_without_blob(
    view: &LeafEntryView<'_>,
    row_set: &LogicalRowSet,
    file_kind: PersistedFileKind,
    page_id: PageID,
) -> Result<LogicalDeleteSet> {
    let delete_domain =
        decode_delete_domain_or_invalid_node(view.prefix.delete_domain(), file_kind, page_id)?;
    match view.prefix.delete_codec() {
        COLUMN_DELETE_CODEC_NONE => Ok(LogicalDeleteSet::None {
            domain: delete_domain,
        }),
        COLUMN_DELETE_CODEC_INLINE_DELTA_LIST => {
            let bytes = view
                .delete_section
                .ok_or_else(|| invalid_node_payload(file_kind, page_id))?;
            let row_id_deltas = decode_delete_rows(
                &bytes[4..],
                view.prefix.del_count(),
                delete_domain,
                row_set,
                file_kind,
                page_id,
            )?;
            Ok(LogicalDeleteSet::Inline {
                domain: delete_domain,
                row_id_deltas,
            })
        }
        COLUMN_DELETE_CODEC_EXTERNAL_BLOB => {
            let bytes = view
                .delete_section
                .ok_or_else(|| invalid_node_payload(file_kind, page_id))?;
            let blob_ref = decode_blob_ref(&bytes[4..])
                .map_err(|_| invalid_node_payload(file_kind, page_id))?;
            Ok(LogicalDeleteSet::External {
                domain: delete_domain,
                del_count: view.prefix.del_count(),
                blob_ref,
                row_id_deltas: None,
            })
        }
        _ => Err(invalid_node_payload(file_kind, page_id)),
    }
}

fn build_leaf_entry(
    leaf_page_id: PageID,
    view: &LeafEntryView<'_>,
    file_kind: PersistedFileKind,
) -> Result<ColumnLeafEntry> {
    let delete_blob_ref = if view.prefix.delete_codec() == COLUMN_DELETE_CODEC_EXTERNAL_BLOB {
        Some(
            decode_blob_ref(
                &view
                    .delete_section
                    .ok_or_else(|| invalid_node_payload(file_kind, leaf_page_id))?[4..],
            )
            .map_err(|_| invalid_node_payload(file_kind, leaf_page_id))?,
        )
    } else {
        None
    };
    Ok(ColumnLeafEntry {
        leaf_page_id,
        start_row_id: view.prefix.start_row_id(),
        block_id: view.prefix.block_id(),
        end_row_id: view
            .prefix
            .end_row_id()
            .map_err(|_| invalid_node_payload(file_kind, leaf_page_id))?,
        row_count: view.prefix.row_count(),
        del_count: view.prefix.del_count(),
        row_id_span: view.prefix.row_id_span(),
        first_present_delta: view.prefix.first_present_delta(),
        row_section_offset: view.prefix.row_section_offset(),
        row_section_len: view.prefix.row_section_len(),
        delete_section_offset: view.prefix.delete_section_offset(),
        delete_section_len: view.prefix.delete_section_len(),
        row_section_kind: view.row_header.kind,
        row_section_version: view.row_header.version,
        row_section_flags: view.row_header.flags,
        row_section_aux: view.row_header.aux,
        delete_section_kind: view
            .delete_header
            .map(|header| header.kind)
            .unwrap_or(COLUMN_DELETE_CODEC_NONE),
        delete_section_version: view.delete_header.map(|header| header.version).unwrap_or(0),
        delete_section_flags: view.delete_header.map(|header| header.flags).unwrap_or(0),
        delete_section_aux: view.delete_header.map(|header| header.aux).unwrap_or(0),
        delete_domain: decode_delete_domain_or_invalid_node(
            view.prefix.delete_domain(),
            file_kind,
            leaf_page_id,
        )?,
        delete_blob_ref,
    })
}

fn build_resolved_row(
    leaf_page_id: PageID,
    view: &LeafEntryView<'_>,
    row_idx: usize,
) -> ResolvedColumnRow {
    ResolvedColumnRow {
        leaf_page_id,
        block_id: view.prefix.block_id(),
        row_idx,
    }
}

#[inline]
fn decode_delete_domain_or_invalid_node(
    raw: u8,
    file_kind: PersistedFileKind,
    page_id: PageID,
) -> Result<ColumnDeleteDomain> {
    ColumnDeleteDomain::decode(raw).map_err(|_| invalid_node_payload(file_kind, page_id))
}

fn encode_row_section(row_set: &LogicalRowSet) -> Result<(u8, Vec<u8>, u32, u32, u16)> {
    match row_set {
        LogicalRowSet::Dense { row_id_span } => {
            let header = SectionHeader {
                kind: COLUMN_ROW_CODEC_DENSE,
                version: COLUMN_ROW_SECTION_VERSION,
                flags: 0,
                aux: 0,
            };
            Ok((
                COLUMN_ROW_CODEC_DENSE,
                header.encode().to_vec(),
                *row_id_span,
                0,
                u16::try_from(*row_id_span).map_err(|_| Error::InvalidArgument)?,
            ))
        }
        LogicalRowSet::DeltaList {
            row_id_span,
            first_present_delta,
            deltas,
        } => {
            let mut bytes = Vec::with_capacity(4 + deltas.len() * mem::size_of::<u32>());
            bytes.extend_from_slice(
                &SectionHeader {
                    kind: COLUMN_ROW_CODEC_DELTA_LIST,
                    version: COLUMN_ROW_SECTION_VERSION,
                    flags: 0,
                    aux: 0,
                }
                .encode(),
            );
            for delta in deltas {
                bytes.extend_from_slice(&delta.to_le_bytes());
            }
            Ok((
                COLUMN_ROW_CODEC_DELTA_LIST,
                bytes,
                *row_id_span,
                *first_present_delta,
                storage_count_u16(deltas.len())?,
            ))
        }
    }
}

fn encode_delete_section(
    row_set: &LogicalRowSet,
    delete_set: &LogicalDeleteSet,
) -> Result<(u8, ColumnDeleteDomain, Vec<u8>)> {
    match delete_set {
        LogicalDeleteSet::None { domain } => Ok((COLUMN_DELETE_CODEC_NONE, *domain, Vec::new())),
        LogicalDeleteSet::Inline {
            domain,
            row_id_deltas,
        } => {
            let delete_values = encode_delete_values(row_id_deltas, row_set, *domain)?;
            if !inline_delete_values_fit(&delete_values)? {
                return Err(Error::InvalidArgument);
            }
            let mut bytes = Vec::with_capacity(4 + mem::size_of_val(delete_values.as_slice()));
            bytes.extend_from_slice(
                &SectionHeader {
                    kind: COLUMN_DELETE_CODEC_INLINE_DELTA_LIST,
                    version: COLUMN_DELETE_SECTION_VERSION,
                    flags: 0,
                    aux: 0,
                }
                .encode(),
            );
            for value in delete_values {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            Ok((COLUMN_DELETE_CODEC_INLINE_DELTA_LIST, *domain, bytes))
        }
        LogicalDeleteSet::External {
            domain, blob_ref, ..
        } => {
            let mut bytes = Vec::with_capacity(4 + COLUMN_BLOB_REF_SIZE);
            bytes.extend_from_slice(
                &SectionHeader {
                    kind: COLUMN_DELETE_CODEC_EXTERNAL_BLOB,
                    version: COLUMN_DELETE_SECTION_VERSION,
                    flags: 0,
                    aux: COLUMN_AUX_BLOB_KIND_DELETE_DELTAS,
                }
                .encode(),
            );
            encode_blob_ref(blob_ref, &mut bytes);
            Ok((COLUMN_DELETE_CODEC_EXTERNAL_BLOB, *domain, bytes))
        }
    }
}

fn encode_leaf_chunk(buf: &mut [u8], entries: &[EncodedLeafEntry]) -> Result<()> {
    buf.fill(0);
    let prefix_bytes_len = entries.len() * COLUMN_BLOCK_LEAF_PREFIX_SIZE;
    let mut prefix_cursor = 0usize;
    let mut arena_end = buf.len();
    for entry in entries {
        let row_range = reserve_tail(&mut arena_end, entry.row_section.len(), buf.len())?;
        buf[row_range.0..row_range.1].copy_from_slice(&entry.row_section);
        let delete_range = if entry.delete_section.is_empty() {
            None
        } else {
            let range = reserve_tail(&mut arena_end, entry.delete_section.len(), buf.len())?;
            buf[range.0..range.1].copy_from_slice(&entry.delete_section);
            Some(range)
        };
        if arena_end < prefix_bytes_len {
            return Err(Error::InvalidArgument);
        }
        let prefix = ColumnBlockLeafPrefix::from_encoded(
            entry,
            row_range.0 as u16,
            entry.row_section.len() as u16,
            delete_range.map(|range| range.0 as u16).unwrap_or(0),
            delete_range
                .map(|range| (range.1 - range.0) as u16)
                .unwrap_or(0),
        );
        let end = prefix_cursor + COLUMN_BLOCK_LEAF_PREFIX_SIZE;
        buf[prefix_cursor..end].copy_from_slice(bytes_of(&prefix));
        prefix_cursor = end;
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
    let start_page_id = u64::from_le_bytes(bytes[0..8].try_into()?);
    let start_offset = u16::from_le_bytes(bytes[8..10].try_into()?);
    let byte_len = u32::from_le_bytes(bytes[10..14].try_into()?);
    if start_page_id == 0 || byte_len == 0 {
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
    page_id: PageID,
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
    page_id: PageID,
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
    if input.block_id == 0 {
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

fn search_start_row_id(prefixes: &[ColumnBlockLeafPrefix], row_id: RowID) -> Option<usize> {
    match prefixes.binary_search_by_key(&row_id, |prefix| prefix.start_row_id()) {
        Ok(idx) => Some(idx),
        Err(0) => None,
        Err(idx) => Some(idx - 1),
    }
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

    fn dense_entry(start: RowID, end: RowID, block_id: PageID) -> ColumnBlockEntryInput {
        let row_ids: Vec<RowID> = (start..end).collect();
        ColumnBlockEntryShape::new(start, end, row_ids, Vec::new())
            .unwrap()
            .with_block_id(block_id)
    }

    fn sparse_entry(
        start: RowID,
        end: RowID,
        row_ids: Vec<RowID>,
        block_id: PageID,
    ) -> ColumnBlockEntryInput {
        ColumnBlockEntryShape::new(start, end, row_ids, Vec::new())
            .unwrap()
            .with_block_id(block_id)
    }

    #[test]
    fn test_leaf_prefix_count_roundtrip_uses_u16() {
        let encoded = EncodedLeafEntry {
            start_row_id: 10,
            block_id: 1001,
            row_id_span: 32,
            first_present_delta: 3,
            row_count: 7,
            del_count: 2,
            row_section_kind: COLUMN_ROW_CODEC_DELTA_LIST,
            delete_section_kind: COLUMN_DELETE_CODEC_INLINE_DELTA_LIST,
            delete_domain: COLUMN_DELETE_DOMAIN_ROW_ID_DELTA,
            row_section: vec![0u8; 4 + 7 * mem::size_of::<u32>()],
            delete_section: vec![0u8; 4 + 2 * mem::size_of::<u32>()],
        };
        let prefix = ColumnBlockLeafPrefix::from_encoded(&encoded, 128, 32, 160, 12);
        assert_eq!(prefix.row_count(), 7);
        assert_eq!(prefix.del_count(), 2);
        assert_eq!(mem::size_of::<ColumnBlockLeafPrefix>(), 48);
    }

    #[test]
    fn test_encode_row_section_rejects_row_count_above_u16() {
        assert!(matches!(
            encode_row_section(&LogicalRowSet::Dense {
                row_id_span: u16::MAX as u32 + 1,
            }),
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
            let mut mutable = MutableTableFile::fork(&table);
            let index = ColumnBlockIndex::new(0, 0, &disk_pool);
            let entries = vec![
                sparse_entry(10, 20, vec![12, 15, 18], 1001),
                dense_entry(20, 24, 1002),
            ];
            let root_page_id = index
                .batch_insert(&mut mutable, &entries, 24, 2)
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let index = ColumnBlockIndex::new(root_page_id, 24, &disk_pool);
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
            let mut mutable = MutableTableFile::fork(&table);
            let entries = vec![
                dense_entry(0, 4, 1001),
                sparse_entry(10, 20, vec![12, 15, 18], 1002),
            ];
            let root_page_id = ColumnBlockIndex::new(0, 0, &disk_pool)
                .batch_insert(&mut mutable, &entries, 20, 2)
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let index = ColumnBlockIndex::new(root_page_id, 20, &disk_pool);
            let dense_entry = index.locate_block(2).await.unwrap().unwrap();
            let dense_resolved = index.resolve_row(2, &dense_entry).await.unwrap().unwrap();
            assert_eq!(dense_resolved.block_id(), 1001);
            assert_eq!(dense_resolved.row_idx(), 2);
            assert_eq!(dense_resolved.leaf_page_id(), dense_entry.leaf_page_id);

            let sparse_entry = index.locate_block(15).await.unwrap().unwrap();
            let sparse_resolved = index.resolve_row(15, &sparse_entry).await.unwrap().unwrap();
            assert_eq!(sparse_resolved.block_id(), 1002);
            assert_eq!(sparse_resolved.row_idx(), 1);
            assert_eq!(sparse_resolved.leaf_page_id(), sparse_entry.leaf_page_id);
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
            let mut mutable = MutableTableFile::fork(&table);
            let root_v1 = ColumnBlockIndex::new(0, 0, &disk_pool)
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
            let root_v2 = ColumnBlockIndex::new(root_v1, 8, &disk_pool)
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

            let index = ColumnBlockIndex::new(root_v2, 10, &disk_pool);
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
            let mut mutable = MutableTableFile::fork(&table);
            let root_v1 = ColumnBlockIndex::new(0, 0, &disk_pool)
                .batch_insert(&mut mutable, &[dense_entry(0, 8, 1001)], 8, 2)
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let mut mutable = MutableTableFile::fork(&table);
            let root_v2 = ColumnBlockIndex::new(root_v1, 8, &disk_pool)
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

            let index = ColumnBlockIndex::new(root_v2, 8, &disk_pool);
            let entry = index.locate_block(0).await.unwrap().unwrap();
            assert_eq!(entry.block_id(), 1001);
            assert_eq!(entry.row_count(), 8);
            assert_eq!(entry.del_count(), 3);
            assert!(
                index
                    .read_delete_payload_bytes(&entry)
                    .await
                    .unwrap()
                    .is_none()
            );
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
            let mut mutable = MutableTableFile::fork(&table);
            let root_v1 = ColumnBlockIndex::new(0, 0, &disk_pool)
                .batch_insert(&mut mutable, &[dense_entry(0, 8, 1001)], 8, 2)
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let replacement = ColumnBlockEntryShape::new(0, 8, (0..8).collect(), vec![1, 3, 6])
                .unwrap()
                .with_delete_domain(ColumnDeleteDomain::Ordinal)
                .with_block_id(1001);
            let mut mutable = MutableTableFile::fork(&table);
            let root_v2 = ColumnBlockIndex::new(root_v1, 8, &disk_pool)
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

            let index = ColumnBlockIndex::new(root_v2, 8, &disk_pool);
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

            let seed = ColumnBlockEntryShape::new(0, 8, (0..8).collect(), vec![1, 3])
                .unwrap()
                .with_delete_domain(ColumnDeleteDomain::Ordinal)
                .with_block_id(1001);
            let mut mutable = MutableTableFile::fork(&table);
            let root_v1 = ColumnBlockIndex::new(0, 0, &disk_pool)
                .batch_insert(&mut mutable, &[seed], 8, 2)
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let mut mutable = MutableTableFile::fork(&table);
            let root_v2 = ColumnBlockIndex::new(root_v1, 8, &disk_pool)
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

            let index = ColumnBlockIndex::new(root_v2, 8, &disk_pool);
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
            let mut mutable = MutableTableFile::fork(&table);
            let mut entries = Vec::new();
            for idx in 0..(COLUMN_BLOCK_MAX_ENTRIES + 32) as u64 {
                entries.push(dense_entry(idx * 2, idx * 2 + 2, 10_000 + idx));
            }
            let root = ColumnBlockIndex::new(0, 0, &disk_pool)
                .batch_insert(
                    &mut mutable,
                    &entries,
                    entries.last().unwrap().end_row_id,
                    2,
                )
                .await
                .unwrap();
            let (_table, _old_root) = mutable.commit(2, false).await.unwrap();

            let index = ColumnBlockIndex::new(root, entries.last().unwrap().end_row_id, &disk_pool);
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
                10_000 + COLUMN_BLOCK_MAX_ENTRIES as u64
            );
        });
    }
}
