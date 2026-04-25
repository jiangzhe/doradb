//! Persisted copy-on-write secondary-index trees.
//!
//! `DiskTree` is the checkpoint-owned cold layer for user-table secondary
//! indexes. It reuses `BTreeNode` as the fixed-size block image, writes the
//! shared block-integrity checksum trailer before persistence, and loads blocks
//! through the readonly buffer pool so validated shared guards can be used
//! directly by readers.
//!
//! Unique trees store encoded logical keys with `RowID` owner values. Non-unique
//! trees store a key-only exact-entry set where the row id is appended to the
//! encoded key and the leaf value is zero-width.

use crate::buffer::{PoolGuard, ReadonlyBlockGuard, ReadonlyBufferPool};
use crate::catalog::{IndexSpec, TableMetadata};
use crate::error::{ConfigError, DataIntegrityError, Error, FileKind, InternalError, Result};
use crate::file::SparseFile;
use crate::file::block_integrity::{validate_block_checksum, write_block_checksum};
use crate::file::cow_file::{BlockID, COW_FILE_PAGE_SIZE, MutableCowFile, SUPER_BLOCK_ID};
use crate::index::btree::BTreeKeyEncoder;
use crate::index::btree::algo::{
    KnownFenceNodeParams, PackedNodeEntry, PackedNodePlanParams, pack_fixed_entries,
    plan_sibling_node,
};
use crate::index::btree::{BTreeNil, BTreeU64, BTreeValue, BTreeValuePackable};
use crate::index::btree::{BTreeNode, LookupChild};
use crate::index::util::Maskable;
use crate::io::DirectBuf;
use crate::quiescent::QuiescentGuard;
use crate::row::RowID;
use crate::value::{Val, ValKind, ValType};
use error_stack::{Report, ResultExt};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::marker::PhantomData;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;

/// Physical size of one persisted secondary DiskTree block.
pub(crate) const DISK_TREE_BLOCK_SIZE: usize = COW_FILE_PAGE_SIZE;
const ROW_ID_SIZE: usize = mem::size_of::<RowID>();

const _: () = assert!(DISK_TREE_BLOCK_SIZE == mem::size_of::<BTreeNode>());

#[inline]
fn invalid_index_spec(message: impl Into<String>) -> Error {
    Report::new(ConfigError::InvalidIndexSpec)
        .attach(message.into())
        .into()
}

#[inline]
fn secondary_index_kind_mismatch(message: impl Into<String>) -> Error {
    Report::new(InternalError::SecondaryIndexKindMismatch)
        .attach(message.into())
        .into()
}

#[inline]
fn disk_tree_rewrite_invariant(message: impl Into<String>) -> Error {
    Report::new(InternalError::DiskTreeRewriteInvariant)
        .attach(message.into())
        .into()
}

#[inline]
fn disk_tree_batch_order_invariant(message: impl Into<String>) -> Error {
    Report::new(InternalError::DiskTreeBatchOrderInvariant)
        .attach(message.into())
        .into()
}

#[inline]
fn invalid_payload(message: impl Into<String>) -> Error {
    Report::new(DataIntegrityError::InvalidPayload)
        .attach(message.into())
        .into()
}

/// One unique DiskTree batch insertion item with an already-encoded key.
pub(crate) struct UniqueDiskTreeEncodedPut<'a> {
    /// Encoded logical secondary key in durable DiskTree order.
    pub key: &'a [u8],
    /// Latest checkpointed owner row id.
    pub row_id: RowID,
}

/// One unique DiskTree conditional delete item with an already-encoded key.
pub(crate) struct UniqueDiskTreeEncodedDelete<'a> {
    /// Encoded logical secondary key in durable DiskTree order.
    pub key: &'a [u8],
    /// Owner row id that must still match before the mapping is removed.
    pub expected_old_row_id: RowID,
}

/// One non-unique DiskTree exact-entry item with an already-encoded exact key.
pub(crate) struct NonUniqueDiskTreeEncodedExact<'a> {
    /// Encoded exact key, including the row-id suffix.
    pub key: &'a [u8],
}

/// Normalized logical entry used by the shared rewrite engine.
///
/// Unique entries carry a row-id owner. Non-unique entries store only the
/// encoded exact key, including the row-id suffix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LogicalEntry {
    key: Vec<u8>,
    row_id: Option<RowID>,
}

impl LogicalEntry {
    #[inline]
    fn unique(key: Vec<u8>, row_id: RowID) -> Self {
        Self {
            key,
            row_id: Some(row_id),
        }
    }

    #[inline]
    fn non_unique(key: Vec<u8>) -> Self {
        Self { key, row_id: None }
    }
}

/// In-memory payload for one child block while a rewrite is in progress.
///
/// Newly written blocks cannot be reached through a published root yet, so the
/// rewrite carries their logical payload beside the block id. This lets parent
/// compaction merge or collapse those blocks without reading them through the
/// readonly pool.
#[derive(Clone, Debug, PartialEq, Eq)]
enum BranchEntryPayload {
    /// Leaf entries stored by the child block.
    Leaf(Vec<LogicalEntry>),
    /// Flattened child entries stored by the branch block.
    Branch(Vec<BranchEntry>),
}

/// Lower-fence key, block id, and rewrite metadata for one child subtree.
///
/// Branch blocks encode the first child in the lower fence and subsequent
/// children as slot key/value pairs. This flattened form lets rewrites split,
/// rebuild, compact, and collapse branch levels without exposing that layout to
/// callers. `rewrite_allocated` is true only for blocks allocated by the current
/// mutable root, which makes it safe to roll back abandoned intermediate blocks.
#[derive(Clone, Debug, PartialEq, Eq)]
struct BranchEntry {
    key: Vec<u8>,
    block_id: BlockID,
    height: u16,
    effective_space: Option<usize>,
    rewrite_allocated: bool,
    payload: Option<BranchEntryPayload>,
}

impl BranchEntry {
    /// Reference an existing persisted block from the published root snapshot.
    #[inline]
    fn persisted(key: Vec<u8>, block_id: BlockID, height: u16) -> Self {
        Self {
            key,
            block_id,
            height,
            effective_space: None,
            rewrite_allocated: false,
            payload: None,
        }
    }

    /// Describe a leaf block allocated and written by the current rewrite.
    #[inline]
    fn rewritten_leaf(
        key: Vec<u8>,
        block_id: BlockID,
        effective_space: usize,
        entries: Vec<LogicalEntry>,
    ) -> Self {
        Self {
            key,
            block_id,
            height: 0,
            effective_space: Some(effective_space),
            rewrite_allocated: true,
            payload: Some(BranchEntryPayload::Leaf(entries)),
        }
    }

    /// Describe a branch block allocated and written by the current rewrite.
    #[inline]
    fn rewritten_branch(
        key: Vec<u8>,
        block_id: BlockID,
        height: u16,
        effective_space: usize,
        children: Vec<BranchEntry>,
    ) -> Self {
        Self {
            key,
            block_id,
            height,
            effective_space: Some(effective_space),
            rewrite_allocated: true,
            payload: Some(BranchEntryPayload::Branch(children)),
        }
    }
}

/// Logical payload for one replacement block that has not been written yet.
#[derive(Clone, Debug, PartialEq, Eq)]
enum RewriteEntryPayload {
    /// Leaf entries to write into one leaf block.
    Leaf(Vec<LogicalEntry>),
    /// Child entries to write into one branch block after children materialize.
    Branch(Vec<RewriteEntry>),
}

/// Lower-fence key and payload for one pending replacement block.
#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingRewriteEntry {
    key: Vec<u8>,
    upper_fence: Option<Vec<u8>>,
    height: u16,
    payload: RewriteEntryPayload,
}

/// Tracks rewrite block ids until a materialization tree succeeds.
///
/// DiskTree materialization recursively writes children before parents. If any
/// async write fails, every block id allocated for that unpublished tree must be
/// returned to the mutable CoW root. Success disarms the guard and leaves the
/// ids owned by the returned `BranchEntry` payload.
struct RewriteAllocationGuard<'a, M: MutableCowFile> {
    mutable_file: &'a mut M,
    block_ids: Vec<BlockID>,
}

impl<'a, M: MutableCowFile> RewriteAllocationGuard<'a, M> {
    #[inline]
    fn new(mutable_file: &'a mut M) -> Self {
        Self {
            mutable_file,
            block_ids: Vec::new(),
        }
    }

    #[inline]
    fn file(&self) -> &M {
        self.mutable_file
    }

    #[inline]
    fn allocate_block_id(&mut self) -> Result<BlockID> {
        let block_id = self.mutable_file.allocate_block_id()?;
        self.block_ids.push(block_id);
        Ok(block_id)
    }

    #[inline]
    fn disarm(mut self) {
        self.block_ids.clear();
    }
}

impl<M: MutableCowFile> Drop for RewriteAllocationGuard<'_, M> {
    #[inline]
    fn drop(&mut self) {
        for block_id in self.block_ids.drain(..).rev() {
            let res = self.mutable_file.rollback_allocated_block_id(block_id);
            debug_assert!(
                res.is_ok(),
                "DiskTree rewrite allocation rollback failed for block_id={block_id}"
            );
        }
    }
}

/// One child entry while a rewrite is still being planned.
#[derive(Clone, Debug, PartialEq, Eq)]
enum RewriteEntry {
    /// Existing or already materialized block with a known block id.
    Block(BranchEntry),
    /// Replacement block that should be written only after sibling absorption
    /// has decided the final lightweight rewrite window.
    Pending(PendingRewriteEntry),
}

impl RewriteEntry {
    #[inline]
    fn key(&self) -> &[u8] {
        match self {
            RewriteEntry::Block(entry) => &entry.key,
            RewriteEntry::Pending(entry) => &entry.key,
        }
    }

    #[inline]
    fn height(&self) -> u16 {
        match self {
            RewriteEntry::Block(entry) => entry.height,
            RewriteEntry::Pending(entry) => entry.height,
        }
    }

    #[inline]
    fn is_pending(&self) -> bool {
        matches!(self, RewriteEntry::Pending(_))
    }
}

/// Result of rewriting one subtree.
///
/// A rewrite can collapse, preserve, or split a subtree. The parent only needs
/// the replacement child entries that should be linked into the next level.
#[derive(Clone, Debug)]
struct NodeRewriteResult {
    entries: Vec<RewriteEntry>,
}

/// Pending unique-tree operation for one encoded logical key.
#[derive(Clone, Debug)]
enum UniqueDiskTreeOp {
    /// Store or replace the durable owner row id.
    Put(RowID),
    /// Remove the key only when the stored owner still matches.
    ConditionalDelete(RowID),
}

/// Operation variant consumed by the shared rewrite engine.
#[derive(Clone, Debug)]
enum DiskTreeOperationKind {
    /// Unique-tree point operation.
    Unique(UniqueDiskTreeOp),
    /// Non-unique exact-key insert/delete represented as final presence.
    NonUniqueSetPresent(bool),
}

/// Encoded mutation routed through the CoW rewrite path.
///
/// The key is already encoded in durable B-tree order. Batch writers normalize
/// logical inputs before constructing this representation.
#[derive(Clone, Debug)]
pub(crate) struct DiskTreeOperation {
    key: Vec<u8>,
    kind: DiskTreeOperationKind,
}

/// Specializes the shared DiskTree engine for unique and non-unique trees.
///
/// The storage and rewrite mechanics are identical: leaves contain encoded keys,
/// branches contain child block ids, and blocks are validated as `BTreeNode`
/// images. This trait defines the leaf value type and how logical entries are
/// interpreted for each durable secondary-index contract.
pub(crate) trait DiskTreeSpec: Copy + 'static {
    /// Value encoded in leaf slots.
    ///
    /// Unique trees use `BTreeU64` row-id owners. Non-unique trees use
    /// zero-width `BTreeNil` because the row id is part of the exact key.
    type LeafValue: BTreeValue;

    /// Validate a full persisted block before publishing a readonly guard.
    fn validate_persisted_block(block: &[u8], file_kind: FileKind, block_id: BlockID)
    -> Result<()>;
    /// Convert a normalized logical entry into the leaf value stored on disk.
    fn leaf_value(entry: &LogicalEntry) -> Result<Self::LeafValue>;
    /// Decode one leaf slot from a validated block into a normalized entry.
    fn leaf_entry(
        node: &BTreeNode,
        idx: usize,
        file_kind: FileKind,
        block_id: BlockID,
    ) -> Result<LogicalEntry>;
    /// Apply encoded batch operations to a sorted entry set.
    ///
    /// Implementations must return sorted unique keys because the writer builds
    /// new leaf blocks directly from the returned sequence.
    fn apply_operations(
        entries: &[LogicalEntry],
        operations: &[DiskTreeOperation],
    ) -> Result<Vec<LogicalEntry>>;
}

/// Unique DiskTree specialization: encoded logical key -> latest owner `RowID`.
#[derive(Clone, Copy)]
pub(crate) struct UniqueDiskTreeSpec;

impl DiskTreeSpec for UniqueDiskTreeSpec {
    type LeafValue = BTreeU64;

    #[inline]
    fn validate_persisted_block(
        block: &[u8],
        file_kind: FileKind,
        block_id: BlockID,
    ) -> Result<()> {
        validate_disk_tree_block::<Self>(block, file_kind, block_id)
    }

    #[inline]
    fn leaf_value(entry: &LogicalEntry) -> Result<Self::LeafValue> {
        let row_id = entry
            .row_id
            .ok_or_else(|| disk_tree_rewrite_invariant("unique leaf entry missing row id"))?;
        if row_id.is_deleted() {
            return Err(disk_tree_rewrite_invariant(
                "unique leaf entry row id is delete-marked",
            ));
        }
        Ok(BTreeU64::from(row_id))
    }

    #[inline]
    fn leaf_entry(
        node: &BTreeNode,
        idx: usize,
        file_kind: FileKind,
        block_id: BlockID,
    ) -> Result<LogicalEntry> {
        let key = node
            .key_checked(idx)
            .ok_or_else(|| invalid_node_payload(file_kind, block_id))?;
        let row_id = node.value::<BTreeU64>(idx);
        if row_id.is_deleted() {
            return Err(invalid_node_payload(file_kind, block_id));
        }
        Ok(LogicalEntry::unique(key, row_id.to_u64()))
    }

    fn apply_operations(
        entries: &[LogicalEntry],
        operations: &[DiskTreeOperation],
    ) -> Result<Vec<LogicalEntry>> {
        let mut map = BTreeMap::new();
        for entry in entries {
            let row_id = entry
                .row_id
                .ok_or_else(|| invalid_payload("unique DiskTree entry is missing row id"))?;
            map.insert(entry.key.clone(), row_id);
        }
        for op in operations {
            match op.kind {
                DiskTreeOperationKind::Unique(UniqueDiskTreeOp::Put(row_id)) => {
                    map.insert(op.key.clone(), row_id);
                }
                DiskTreeOperationKind::Unique(UniqueDiskTreeOp::ConditionalDelete(expected)) => {
                    if map.get(&op.key).is_some_and(|row_id| *row_id == expected) {
                        map.remove(&op.key);
                    }
                }
                DiskTreeOperationKind::NonUniqueSetPresent(_) => {
                    return Err(disk_tree_rewrite_invariant(
                        "unique DiskTree received non-unique operation",
                    ));
                }
            }
        }
        Ok(map
            .into_iter()
            .map(|(key, row_id)| LogicalEntry::unique(key, row_id))
            .collect())
    }
}

/// Non-unique DiskTree specialization: exact encoded key presence only.
///
/// The row id is appended to the encoded logical key, so no leaf value bytes are
/// needed and no durable delete mask exists.
#[derive(Clone, Copy)]
pub(crate) struct NonUniqueDiskTreeSpec;

impl DiskTreeSpec for NonUniqueDiskTreeSpec {
    type LeafValue = BTreeNil;

    #[inline]
    fn validate_persisted_block(
        block: &[u8],
        file_kind: FileKind,
        block_id: BlockID,
    ) -> Result<()> {
        validate_disk_tree_block::<Self>(block, file_kind, block_id)
    }

    #[inline]
    fn leaf_value(entry: &LogicalEntry) -> Result<Self::LeafValue> {
        if entry.row_id.is_some() {
            return Err(disk_tree_rewrite_invariant(
                "non-unique leaf entry unexpectedly has row id",
            ));
        }
        Ok(BTreeNil)
    }

    #[inline]
    fn leaf_entry(
        node: &BTreeNode,
        idx: usize,
        file_kind: FileKind,
        block_id: BlockID,
    ) -> Result<LogicalEntry> {
        let key = node
            .key_checked(idx)
            .ok_or_else(|| invalid_node_payload(file_kind, block_id))?;
        Ok(LogicalEntry::non_unique(key))
    }

    fn apply_operations(
        entries: &[LogicalEntry],
        operations: &[DiskTreeOperation],
    ) -> Result<Vec<LogicalEntry>> {
        let mut set = BTreeSet::new();
        for entry in entries {
            if entry.row_id.is_some() {
                return Err(invalid_payload(
                    "non-unique DiskTree entry unexpectedly has row id",
                ));
            }
            set.insert(entry.key.clone());
        }
        for op in operations {
            match op.kind {
                DiskTreeOperationKind::NonUniqueSetPresent(true) => {
                    set.insert(op.key.clone());
                }
                DiskTreeOperationKind::NonUniqueSetPresent(false) => {
                    set.remove(&op.key);
                }
                DiskTreeOperationKind::Unique(_) => {
                    return Err(disk_tree_rewrite_invariant(
                        "non-unique DiskTree received unique operation",
                    ));
                }
            }
        }
        Ok(set.into_iter().map(LogicalEntry::non_unique).collect())
    }
}

/// Readonly guard for a block after DiskTree-specific validation.
///
/// Keeping the guard alive keeps the block resident and immutable while callers
/// read the no-copy `BTreeNode` view.
struct ValidatedDiskTreeNode<F: DiskTreeSpec> {
    guard: ReadonlyBlockGuard,
    _marker: PhantomData<F>,
}

impl<F: DiskTreeSpec> ValidatedDiskTreeNode<F> {
    #[inline]
    fn node(&self) -> &BTreeNode {
        btree_node_from_block(self.guard.page())
            .expect("validated DiskTree block must be a BTreeNode block")
    }
}

#[inline]
fn invalid_node_payload(file_kind: FileKind, block_id: BlockID) -> Error {
    Report::new(DataIntegrityError::InvalidPayload)
        .attach(format!(
            "file={file_kind}, block=secondary-disk-tree, block_id={block_id}"
        ))
        .into()
}

/// View a validated block as an immutable B-tree node image.
#[inline]
fn btree_node_from_block(block: &[u8]) -> Option<&BTreeNode> {
    bytemuck::try_from_bytes::<BTreeNode>(block).ok()
}

/// Safely view a zeroed direct buffer as a mutable B-tree node image.
///
/// Writers build nodes directly inside the final block buffer so the checksum
/// can be computed over exactly the bytes that will be written.
#[inline]
fn btree_node_from_block_mut(block: &mut [u8]) -> Result<&mut BTreeNode> {
    bytemuck::try_from_bytes_mut::<BTreeNode>(block)
        .map_err(|_| Report::new(InternalError::MutableBlockViewMismatch).into())
}

#[inline]
fn validate_checksum(block: &[u8], file_kind: FileKind, block_id: BlockID) -> Result<()> {
    if block.len() != DISK_TREE_BLOCK_SIZE {
        return Err(invalid_node_payload(file_kind, block_id));
    }
    validate_block_checksum(block)
        .attach_with(|| format!("file={file_kind}, block=secondary-disk-tree, block_id={block_id}"))
        .map_err(Error::from)?;
    Ok(())
}

/// Validate one persisted DiskTree block before handing it to a reader.
///
/// Validation is intentionally layered: checksum first, then no-copy BTreeNode
/// casting, then logical layout checks for branch pointers or spec-specific
/// leaf values.
fn validate_disk_tree_block<F: DiskTreeSpec>(
    block: &[u8],
    file_kind: FileKind,
    block_id: BlockID,
) -> Result<()> {
    validate_checksum(block, file_kind, block_id)?;
    let node =
        btree_node_from_block(block).ok_or_else(|| invalid_node_payload(file_kind, block_id))?;
    let valid_layout = if node.is_leaf() {
        node.validate_persisted_layout::<F::LeafValue>()
    } else {
        node.validate_persisted_layout::<BTreeU64>()
    };
    if !valid_layout {
        return Err(invalid_node_payload(file_kind, block_id));
    }
    if !node.is_leaf() {
        validate_branch_children(node, file_kind, block_id)?;
    }
    if node.is_leaf() {
        for idx in 0..node.count() {
            F::leaf_entry(node, idx, file_kind, block_id)?;
        }
    }
    Ok(())
}

/// Reject branch blocks that point at the empty-root sentinel.
///
/// `SUPER_BLOCK_ID` means an empty root, not a valid child block. Once a tree has
/// branches, every child pointer must refer to a real persisted block.
fn validate_branch_children(
    node: &BTreeNode,
    file_kind: FileKind,
    block_id: BlockID,
) -> Result<()> {
    if BlockID::from(node.lower_fence_value().to_u64()) == SUPER_BLOCK_ID {
        return Err(invalid_node_payload(file_kind, block_id));
    }
    for idx in 0..node.count() {
        if BlockID::from(node.value::<BTreeU64>(idx).to_u64()) == SUPER_BLOCK_ID {
            return Err(invalid_node_payload(file_kind, block_id));
        }
    }
    Ok(())
}

/// Resolve the physical value types that form encoded DiskTree keys.
///
/// Non-unique trees append `RowID` to the logical key so exact entries sort by
/// `(logical_key, row_id)` while using the same key encoder as runtime indexes.
fn index_key_types(
    metadata: &TableMetadata,
    index_spec: &IndexSpec,
    append_row_id: bool,
) -> Result<Vec<ValType>> {
    if index_spec.index_cols.is_empty() {
        return Err(invalid_index_spec("index has no key columns"));
    }
    let mut types = Vec::with_capacity(index_spec.index_cols.len() + usize::from(append_row_id));
    for key in &index_spec.index_cols {
        let col_no = key.col_no as usize;
        let ty =
            metadata.col_types().get(col_no).copied().ok_or_else(|| {
                invalid_index_spec(format!("index column {col_no} is out of range"))
            })?;
        types.push(ty);
    }
    if append_row_id {
        types.push(ValType::new(ValKind::U64, false));
    }
    Ok(types)
}

/// Ensure caller-provided batches are already in strict durable key order.
///
/// DiskTree rewrite code assumes sorted unique operation keys so it can route
/// work to child ranges without an additional sort or duplicate-resolution pass.
fn validate_sorted_unique_keys<'a>(keys: impl IntoIterator<Item = &'a [u8]>) -> Result<()> {
    let mut prev = None;
    for key in keys {
        if prev.is_some_and(|prev_key: &[u8]| prev_key >= key) {
            return Err(disk_tree_batch_order_invariant(
                "keys are not strictly sorted and unique",
            ));
        }
        prev = Some(key);
    }
    Ok(())
}

/// Ensure caller-provided batches are in durable key order, allowing equal
/// adjacent keys for multi-operation unique conditional deletes.
fn validate_sorted_keys<'a>(keys: impl IntoIterator<Item = &'a [u8]>) -> Result<()> {
    let mut prev = None;
    for key in keys {
        if prev.is_some_and(|prev_key: &[u8]| prev_key > key) {
            return Err(disk_tree_batch_order_invariant("keys are not sorted"));
        }
        prev = Some(key);
    }
    Ok(())
}

/// Extract the row-id suffix from a non-unique exact key.
#[inline]
fn unpack_row_id_from_exact_key(key: &[u8]) -> Result<RowID> {
    if key.len() < ROW_ID_SIZE {
        return Err(invalid_payload(format!(
            "non-unique DiskTree exact key length {} is shorter than row id suffix size {ROW_ID_SIZE}",
            key.len()
        )));
    }
    Ok(BTreeU64::unpack(&key[key.len() - ROW_ID_SIZE..]).to_u64())
}

/// Validate already-encoded non-unique exact keys before staging them.
///
/// The trailing row-id suffix is part of the durable exact key contract; reject
/// malformed keys here so readers never discover them through prefix scans.
fn validate_sorted_non_unique_exact_keys(
    entries: &[NonUniqueDiskTreeEncodedExact<'_>],
) -> Result<()> {
    let mut prev = None;
    for entry in entries {
        unpack_row_id_from_exact_key(entry.key)?;
        if prev.is_some_and(|prev_key: &[u8]| prev_key >= entry.key) {
            return Err(disk_tree_batch_order_invariant(
                "non-unique exact keys are not strictly sorted and unique",
            ));
        }
        prev = Some(entry.key);
    }
    Ok(())
}

/// Fixed runtime shape shared by root snapshots of one persisted DiskTree.
///
/// The runtime owns the shape and IO context that do not change when a
/// checkpoint publishes a new root. Individual [`DiskTree`] snapshots borrow it
/// and supply the root block id that was current when they were opened.
pub(crate) struct DiskTreeRuntime<F: DiskTreeSpec> {
    file_kind: FileKind,
    file: Arc<SparseFile>,
    disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    encoder: BTreeKeyEncoder,
    _marker: PhantomData<F>,
}

impl<F: DiskTreeSpec> DiskTreeRuntime<F> {
    #[inline]
    fn from_shape(
        encoder: BTreeKeyEncoder,
        file_kind: FileKind,
        file: Arc<SparseFile>,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    ) -> Self {
        Self {
            file_kind,
            file,
            disk_pool,
            encoder,
            _marker: PhantomData,
        }
    }

    /// Open a typed view over one root snapshot.
    ///
    /// `SUPER_BLOCK_ID` is accepted here and interpreted by read/write paths as
    /// the empty tree sentinel.
    #[inline]
    pub(crate) fn open<'a>(
        &'a self,
        root_block_id: BlockID,
        disk_pool_guard: &'a PoolGuard,
    ) -> DiskTree<'a, F> {
        DiskTree::from_root_snapshot(root_block_id, self, disk_pool_guard)
    }

    #[inline]
    pub(crate) fn disk_pool_guard(&self) -> PoolGuard {
        self.disk_pool.pool_guard()
    }
}

/// Fixed runtime shape for persisted unique secondary DiskTrees.
pub(crate) type UniqueDiskTreeRuntime = DiskTreeRuntime<UniqueDiskTreeSpec>;

impl UniqueDiskTreeRuntime {
    /// Create a reusable unique DiskTree runtime for one index shape.
    #[inline]
    pub(crate) fn new(
        index_spec: &IndexSpec,
        metadata: &TableMetadata,
        file_kind: FileKind,
        file: Arc<SparseFile>,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    ) -> Result<Self> {
        if !index_spec.unique() {
            return Err(secondary_index_kind_mismatch(
                "unique DiskTree runtime received non-unique index spec",
            ));
        }
        let encoder = BTreeKeyEncoder::new(index_key_types(metadata, index_spec, false)?);
        Ok(Self::from_shape(encoder, file_kind, file, disk_pool))
    }
}

/// Fixed runtime shape for persisted non-unique secondary DiskTrees.
pub(crate) type NonUniqueDiskTreeRuntime = DiskTreeRuntime<NonUniqueDiskTreeSpec>;

impl NonUniqueDiskTreeRuntime {
    /// Create a reusable non-unique DiskTree runtime for one index shape.
    #[inline]
    pub(crate) fn new(
        index_spec: &IndexSpec,
        metadata: &TableMetadata,
        file_kind: FileKind,
        file: Arc<SparseFile>,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    ) -> Result<Self> {
        if index_spec.unique() {
            return Err(secondary_index_kind_mismatch(
                "non-unique DiskTree runtime received unique index spec",
            ));
        }
        let encoder = BTreeKeyEncoder::new(index_key_types(metadata, index_spec, true)?);
        Ok(Self::from_shape(encoder, file_kind, file, disk_pool))
    }
}

/// Shared root-snapshot view over one persisted DiskTree.
///
/// The view is immutable: reads use the readonly buffer pool, while writes build
/// replacement CoW blocks through a mutable table-file fork and return a new root
/// block id for the caller to publish later.
pub(crate) struct DiskTree<'a, F: DiskTreeSpec> {
    root_block_id: BlockID,
    runtime: &'a DiskTreeRuntime<F>,
    disk_pool_guard: &'a PoolGuard,
}

impl<'a, F: DiskTreeSpec> DiskTree<'a, F> {
    #[inline]
    fn from_root_snapshot(
        root_block_id: BlockID,
        runtime: &'a DiskTreeRuntime<F>,
        disk_pool_guard: &'a PoolGuard,
    ) -> Self {
        Self {
            root_block_id,
            runtime,
            disk_pool_guard,
        }
    }

    #[inline]
    fn encoder(&self) -> &BTreeKeyEncoder {
        &self.runtime.encoder
    }

    #[inline]
    fn file_kind(&self) -> FileKind {
        self.runtime.file_kind
    }

    /// Read and validate one persisted block as a DiskTree node.
    ///
    /// The returned guard owns the readonly-buffer reference, so callers can use
    /// the no-copy node view without copying entries out of the block.
    #[inline]
    async fn read_node(&self, block_id: BlockID) -> Result<ValidatedDiskTreeNode<F>> {
        let guard = self
            .runtime
            .disk_pool
            .read_validated_block(
                self.runtime.file_kind,
                &self.runtime.file,
                self.disk_pool_guard,
                block_id,
                F::validate_persisted_block,
            )
            .await?;
        Ok(ValidatedDiskTreeNode {
            guard,
            _marker: PhantomData,
        })
    }

    /// Search one encoded key from the current root snapshot.
    ///
    /// Branch traversal follows child block ids until a leaf is reached. Empty
    /// roots and leaf misses return `Ok(None)`.
    async fn lookup_encoded_entry(&self, key: &[u8]) -> Result<Option<LogicalEntry>> {
        if self.root_block_id == SUPER_BLOCK_ID {
            return Ok(None);
        }
        let mut block_id = self.root_block_id;
        loop {
            let guard = self.read_node(block_id).await?;
            let node = guard.node();
            if node.is_leaf() {
                return match node.search_key(key) {
                    Ok(idx) => Ok(Some(F::leaf_entry(node, idx, self.file_kind(), block_id)?)),
                    Err(_) => Ok(None),
                };
            }
            match lookup_child_block(node, key) {
                Some(child_block_id) => block_id = child_block_id,
                None => return Ok(None),
            }
        }
    }

    /// Collect all logical entries in durable key order.
    ///
    /// This is intentionally simple for Phase 1 and powers scan APIs and rewrite
    /// tests. Validation checks that the traversal observes strictly increasing
    /// leaf keys.
    async fn collect_entries(&self) -> Result<Vec<LogicalEntry>> {
        if self.root_block_id == SUPER_BLOCK_ID {
            return Ok(Vec::new());
        }
        let mut stack = vec![self.root_block_id];
        let mut entries: Vec<LogicalEntry> = Vec::new();
        while let Some(block_id) = stack.pop() {
            let guard = self.read_node(block_id).await?;
            let node = guard.node();
            if node.is_leaf() {
                for idx in 0..node.count() {
                    let entry = F::leaf_entry(node, idx, self.file_kind(), block_id)?;
                    if entries.last().is_some_and(|prev| prev.key >= entry.key) {
                        return Err(invalid_node_payload(self.file_kind(), block_id));
                    }
                    entries.push(entry);
                }
            } else {
                let branch_entries = branch_entries_from_node(node, self.file_kind(), block_id)?;
                for entry in branch_entries.into_iter().rev() {
                    stack.push(entry.block_id);
                }
            }
        }
        Ok(entries)
    }

    /// Visit logical entries from the first key greater than or equal to
    /// `start_key`, stopping when the visitor returns `false`.
    async fn scan_entries_from<V>(&self, start_key: &[u8], mut visitor: V) -> Result<()>
    where
        V: FnMut(LogicalEntry) -> Result<bool>,
    {
        if self.root_block_id == SUPER_BLOCK_ID {
            return Ok(());
        }
        let mut stack = vec![self.root_block_id];
        let mut last_key: Option<Vec<u8>> = None;
        while let Some(block_id) = stack.pop() {
            let guard = self.read_node(block_id).await?;
            let node = guard.node();
            if node.is_leaf() {
                for idx in node.lower_bound_slot_idx(start_key)..node.count() {
                    let entry = F::leaf_entry(node, idx, self.file_kind(), block_id)?;
                    if last_key
                        .as_ref()
                        .is_some_and(|prev| prev.as_slice() >= entry.key.as_slice())
                    {
                        return Err(invalid_node_payload(self.file_kind(), block_id));
                    }
                    last_key = Some(entry.key.clone());
                    if !visitor(entry)? {
                        return Ok(());
                    }
                }
            } else {
                let start_idx = node
                    .lower_bound_child_entry_idx(start_key)
                    .ok_or_else(|| invalid_node_payload(self.file_kind(), block_id))?;
                let branch_entries = branch_entries_from_node(node, self.file_kind(), block_id)?;
                if start_idx >= branch_entries.len() {
                    return Err(invalid_node_payload(self.file_kind(), block_id));
                }
                for entry in branch_entries.into_iter().skip(start_idx).rev() {
                    stack.push(entry.block_id);
                }
            }
        }
        Ok(())
    }

    /// Rewrite the touched CoW paths and return the replacement root block id.
    ///
    /// The method does not publish the new root or record replaced blocks for GC;
    /// callers own root publication in table metadata.
    fn rewrite_root<'b, M: MutableCowFile + 'b>(
        &'b self,
        mutable_file: &'b mut M,
        operations: &'b [DiskTreeOperation],
        create_ts: u64,
    ) -> Pin<Box<dyn Future<Output = Result<BlockID>> + 'b>> {
        Box::pin(async move {
            if operations.is_empty() {
                // An empty companion batch should preserve the exact root block.
                // This avoids producing a new tree image for checkpoint no-ops.
                return Ok(self.root_block_id);
            }
            if self.root_block_id == SUPER_BLOCK_ID {
                // Empty roots have no block to rewrite. Apply the batch against
                // an empty logical set and build the resulting tree from scratch.
                let entries = F::apply_operations(&[], operations)?;
                return self
                    .build_tree_from_entries(mutable_file, &entries, create_ts)
                    .await;
            }
            let res = self
                .rewrite_subtree(
                    mutable_file,
                    self.root_block_id,
                    operations,
                    create_ts,
                    None,
                )
                .await?;
            // Finalization decides whether the rewritten result is empty, can
            // promote a single child, or needs new branch levels above it.
            self.finalize_root_rewrite(mutable_file, res.entries, create_ts)
                .await
        })
    }

    /// Rewrite one subtree that intersects a sorted operation range.
    ///
    /// Leaf rewrites materialize the leaf entries, apply spec-specific logical
    /// operations, and return pending replacement leaves. Branch rewrites route
    /// operations into affected children and plan replacement branch entries
    /// before any new blocks are written.
    fn rewrite_subtree<'b, M: MutableCowFile + 'b>(
        &'b self,
        mutable_file: &'b mut M,
        block_id: BlockID,
        operations: &'b [DiskTreeOperation],
        create_ts: u64,
        range_upper_fence: Option<&'b [u8]>,
    ) -> Pin<Box<dyn Future<Output = Result<NodeRewriteResult>> + 'b>> {
        Box::pin(async move {
            let guard = self.read_node(block_id).await?;
            let node = guard.node();
            if node.is_leaf() {
                // Leaves are already validated, so each slot can be decoded into
                // the spec's logical entry form before applying batch semantics.
                let mut entries = Vec::with_capacity(node.count());
                for idx in 0..node.count() {
                    entries.push(F::leaf_entry(node, idx, self.file_kind(), block_id)?);
                }
                let entries = F::apply_operations(&entries, operations)?;
                return Ok(NodeRewriteResult {
                    entries: self.pack_leaf_rewrite_entries(&entries, range_upper_fence)?,
                });
            }
            let entries = branch_entries_from_node(node, self.file_kind(), block_id)?;
            // Flatten the branch while the guard is alive, then release it before
            // recursive children perform mutable writes through the CoW fork.
            drop(guard);
            self.rewrite_branch(
                entries,
                mutable_file,
                operations,
                create_ts,
                range_upper_fence,
            )
            .await
        })
    }

    /// Rewrite children below one branch and rebuild the parent-level entries.
    ///
    /// `old_entries` is the branch's flattened child list. Operations are split
    /// by each child's upper fence and only affected children are rewritten.
    async fn rewrite_branch<M: MutableCowFile>(
        &self,
        old_entries: Vec<BranchEntry>,
        mutable_file: &mut M,
        operations: &[DiskTreeOperation],
        create_ts: u64,
        range_upper_fence: Option<&[u8]>,
    ) -> Result<NodeRewriteResult> {
        let mut combined = Vec::with_capacity(old_entries.len() + operations.len());
        let mut op_idx = 0usize;
        for (child_idx, entry) in old_entries.iter().enumerate() {
            let start_idx = op_idx;
            let upper = old_entries
                .get(child_idx + 1)
                .map(|next| next.key.as_slice())
                .or(range_upper_fence);
            // Child ranges are half-open: this child's lower fence through the
            // next child's lower fence. The last child owns the remaining suffix.
            while op_idx < operations.len()
                && upper.is_none_or(|upper| operations[op_idx].key.as_slice() < upper)
            {
                op_idx += 1;
            }
            if start_idx == op_idx {
                // No operation falls in this child range, so reuse the existing
                // child block and preserve copy-on-write locality.
                combined.push(RewriteEntry::Block(entry.clone()));
                continue;
            }
            // Only touched children are recursively rewritten. A child rewrite
            // may shrink, preserve, or split into multiple replacement children.
            let child = self
                .rewrite_subtree(
                    mutable_file,
                    entry.block_id,
                    &operations[start_idx..op_idx],
                    create_ts,
                    upper,
                )
                .await?;
            combined.extend(child.entries);
        }
        if op_idx != operations.len() {
            // Sorted operations should all be consumed by the existing branch
            // ranges. Leftovers indicate invalid caller ordering or routing.
            return Err(disk_tree_rewrite_invariant(
                "operations were not consumed by branch ranges",
            ));
        }
        if combined.is_empty() {
            return Ok(NodeRewriteResult {
                entries: Vec::new(),
            });
        }
        // Lightweight rewrite compaction is anchored to pending child output.
        // It may absorb one or more immediate right siblings, but only when the
        // accepted window can be materialized within the pending anchor's write
        // budget. Unrelated sparse regions are left as their original blocks.
        let combined = self
            .absorb_rewrite_siblings(combined, range_upper_fence)
            .await?;
        if combined.is_empty() {
            return Ok(NodeRewriteResult {
                entries: Vec::new(),
            });
        }
        let height = parent_height_from_rewrite_children(&combined)?;
        let entries = self.pack_branch_rewrite_entries(combined, height, range_upper_fence)?;
        Ok(NodeRewriteResult { entries })
    }

    /// Build an entire tree from a sorted logical entry list.
    ///
    /// Used when creating a tree from an empty root or when a rewrite collapses
    /// into a full rebuild of one logical entry set.
    async fn build_tree_from_entries<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[LogicalEntry],
        create_ts: u64,
    ) -> Result<BlockID> {
        let leaf_entries = self.pack_leaf_rewrite_entries(entries, None)?;
        self.finalize_root_rewrite(mutable_file, leaf_entries, create_ts)
            .await
    }

    /// Convert rewritten child entries into the final root block id.
    ///
    /// Empty entries become `SUPER_BLOCK_ID`, a single child is promoted as the
    /// root, and larger sets are wrapped in new branch levels.
    async fn finalize_root_rewrite<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: Vec<RewriteEntry>,
        create_ts: u64,
    ) -> Result<BlockID> {
        if entries.is_empty() {
            // A rewrite that removes every logical entry returns the empty-root
            // sentinel instead of writing an empty node block.
            return Ok(SUPER_BLOCK_ID);
        }
        // Multiple children need branch levels above them; a single child is
        // already a root candidate. In both cases, root-only single-child branch
        // chains are collapsed before returning the final block id.
        let root_entry = self.build_branch_levels(entries)?;
        self.collapse_root_chain(mutable_file, root_entry, create_ts)
            .await
    }

    /// Build branch levels until the tree has a single root block.
    fn build_branch_levels(&self, mut entries: Vec<RewriteEntry>) -> Result<RewriteEntry> {
        loop {
            if entries.is_empty() {
                // This is defensive: callers normally handle empty rewrites
                // before entering the branch-level builder.
                return Err(disk_tree_rewrite_invariant(
                    "branch-level builder received empty rewrite entries",
                ));
            }
            if entries.len() == 1 {
                // Stop as soon as the current level has a single root candidate.
                return Ok(entries.remove(0));
            }
            // Pack the current level into parent branch blocks, then repeat with
            // the returned lower-fence entries until one root block remains.
            let height = parent_height_from_rewrite_children(&entries)?;
            entries = self.pack_branch_rewrite_entries(entries, height, None)?;
        }
    }

    /// Collapse root-only branch wrappers that have exactly one child.
    ///
    /// This keeps delete-heavy rewrites from returning a tall chain of branch
    /// nodes above a single leaf. Only blocks allocated by this rewrite are
    /// rolled back; old published blocks remain allocated for the normal CoW/GC
    /// lifecycle.
    fn collapse_root_chain<'b, M: MutableCowFile + 'b>(
        &'b self,
        mutable_file: &'b mut M,
        mut entry: RewriteEntry,
        create_ts: u64,
    ) -> Pin<Box<dyn Future<Output = Result<BlockID>> + 'b>> {
        Box::pin(async move {
            loop {
                if entry.height() == 0 {
                    let root_entry = self
                        .retarget_rewrite_entry_upper_fence(mutable_file, entry, None)
                        .await?;
                    return Ok(self
                        .materialize_rewrite_entry(mutable_file, root_entry, create_ts)
                        .await?
                        .block_id);
                }
                let payload = self.rewrite_entry_payload(&entry).await?;
                match payload {
                    RewriteEntryPayload::Branch(mut children) if children.len() == 1 => {
                        // Promote the only child as the next root candidate. A
                        // pending wrapper is never written; a materialized wrapper
                        // allocated by this rewrite can be rolled back.
                        if let RewriteEntry::Block(wrapper) = &entry {
                            self.rollback_rewritten_entry_block(mutable_file, wrapper)?;
                        }
                        entry = children.remove(0);
                    }
                    RewriteEntryPayload::Branch(_) => {
                        let root_entry = self
                            .retarget_rewrite_entry_upper_fence(mutable_file, entry, None)
                            .await?;
                        return Ok(self
                            .materialize_rewrite_entry(mutable_file, root_entry, create_ts)
                            .await?
                            .block_id);
                    }
                    RewriteEntryPayload::Leaf(_) => {
                        return Err(invalid_payload(
                            "DiskTree root rewrite expected branch payload",
                        ));
                    }
                }
            }
        })
    }

    /// Absorb right siblings into pending rewrite anchors without extra writes.
    ///
    /// The scan only starts from pending output produced by a touched subtree.
    /// Persisted siblings to the right may be absorbed whole, but only when the
    /// repacked window still materializes to no more blocks than the pending
    /// anchors already require. A non-absorbable sibling remains unchanged and
    /// becomes the barrier for that anchor; later pending entries can start their
    /// own windows.
    async fn absorb_rewrite_siblings(
        &self,
        entries: Vec<RewriteEntry>,
        range_upper_fence: Option<&[u8]>,
    ) -> Result<Vec<RewriteEntry>> {
        if entries.len() < 2 {
            return Ok(entries);
        }

        let mut compacted = Vec::with_capacity(entries.len());
        let mut idx = 0usize;
        while idx < entries.len() {
            if !entries[idx].is_pending() {
                compacted.push(entries[idx].clone());
                idx += 1;
                continue;
            }

            let height = entries[idx].height();
            let mut write_budget = 1usize;
            let mut planned = vec![entries[idx].clone()];
            idx += 1;

            while idx < entries.len() && entries[idx].height() == height {
                let mut candidate_window = planned.clone();
                candidate_window.push(entries[idx].clone());
                let candidate_budget = write_budget + usize::from(entries[idx].is_pending());
                let candidate_upper_fence = entries
                    .get(idx + 1)
                    .map(|entry| entry.key())
                    .or(range_upper_fence);
                let candidate = self
                    .repack_rewrite_window(&candidate_window, height, candidate_upper_fence)
                    .await?;
                if candidate.len() > candidate_budget {
                    break;
                }
                planned = candidate;
                write_budget = candidate_budget;
                idx += 1;
            }

            compacted.extend(planned);
        }
        Ok(compacted)
    }

    /// Repack one already-selected same-height window into pending entries.
    async fn repack_rewrite_window(
        &self,
        window: &[RewriteEntry],
        height: u16,
        upper_fence: Option<&[u8]>,
    ) -> Result<Vec<RewriteEntry>> {
        if window.is_empty() {
            return Err(disk_tree_rewrite_invariant(
                "rewrite repack received empty window",
            ));
        }
        if height == 0 {
            let mut entries = Vec::new();
            for entry in window {
                match self.rewrite_entry_payload(entry).await? {
                    RewriteEntryPayload::Leaf(mut leaf_entries) => {
                        entries.append(&mut leaf_entries);
                    }
                    RewriteEntryPayload::Branch(_) => {
                        return Err(invalid_payload(
                            "DiskTree leaf repack received branch payload",
                        ));
                    }
                }
            }
            validate_logical_entries_sorted(&entries)?;
            return self.pack_leaf_rewrite_entries(&entries, upper_fence);
        }

        let mut children = Vec::new();
        for entry in window {
            match self.rewrite_entry_payload(entry).await? {
                RewriteEntryPayload::Branch(mut child_entries) => {
                    children.append(&mut child_entries);
                }
                RewriteEntryPayload::Leaf(_) => {
                    return Err(invalid_payload(
                        "DiskTree branch repack received leaf payload",
                    ));
                }
            }
        }
        validate_rewrite_entries_for_height(&children, height - 1)?;
        self.pack_branch_rewrite_entries(children, height, upper_fence)
    }

    /// Materialize one rewrite entry's payload without reading newly written blocks.
    async fn rewrite_entry_payload(&self, entry: &RewriteEntry) -> Result<RewriteEntryPayload> {
        match entry {
            RewriteEntry::Pending(entry) => Ok(entry.payload.clone()),
            RewriteEntry::Block(entry) => match self.branch_entry_payload(entry).await? {
                BranchEntryPayload::Leaf(entries) => Ok(RewriteEntryPayload::Leaf(entries)),
                BranchEntryPayload::Branch(children) => Ok(RewriteEntryPayload::Branch(
                    children.into_iter().map(RewriteEntry::Block).collect(),
                )),
            },
        }
    }

    /// Materialize one branch entry's payload without reading newly written blocks.
    async fn branch_entry_payload(&self, entry: &BranchEntry) -> Result<BranchEntryPayload> {
        if let Some(payload) = &entry.payload {
            return Ok(payload.clone());
        }

        let guard = self.read_node(entry.block_id).await?;
        let node = guard.node();
        if node.height() != usize::from(entry.height) {
            return Err(invalid_node_payload(self.file_kind(), entry.block_id));
        }
        if node.is_leaf() {
            let mut entries = Vec::with_capacity(node.count());
            for idx in 0..node.count() {
                entries.push(F::leaf_entry(node, idx, self.file_kind(), entry.block_id)?);
            }
            Ok(BranchEntryPayload::Leaf(entries))
        } else {
            Ok(BranchEntryPayload::Branch(branch_entries_from_node(
                node,
                self.file_kind(),
                entry.block_id,
            )?))
        }
    }

    /// Roll back a direct block allocated by this rewrite if it was abandoned.
    #[inline]
    fn rollback_rewritten_entry_block<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entry: &BranchEntry,
    ) -> Result<()> {
        if entry.rewrite_allocated {
            mutable_file.rollback_allocated_block_id(entry.block_id)?;
        }
        Ok(())
    }

    /// Retarget a rewrite entry to a different upper fence before materializing.
    ///
    /// Root promotion uses this to ensure the published root is open-ended. If
    /// the candidate is an existing finite block, its payload is converted back
    /// to pending form so the block can be written with the new fence.
    async fn retarget_rewrite_entry_upper_fence<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entry: RewriteEntry,
        upper_fence: Option<Vec<u8>>,
    ) -> Result<RewriteEntry> {
        match entry {
            RewriteEntry::Pending(mut entry) => {
                entry.upper_fence = upper_fence;
                Ok(RewriteEntry::Pending(entry))
            }
            RewriteEntry::Block(entry) => {
                let payload = match self.branch_entry_payload(&entry).await? {
                    BranchEntryPayload::Leaf(entries) => RewriteEntryPayload::Leaf(entries),
                    BranchEntryPayload::Branch(children) => RewriteEntryPayload::Branch(
                        children.into_iter().map(RewriteEntry::Block).collect(),
                    ),
                };
                self.rollback_rewritten_entry_block(mutable_file, &entry)?;
                Ok(RewriteEntry::Pending(PendingRewriteEntry {
                    key: entry.key,
                    upper_fence,
                    height: entry.height,
                    payload,
                }))
            }
        }
    }

    /// Split sorted logical leaf entries into pending leaf blocks.
    ///
    /// `upper_fence` is the exclusive high fence of the whole run. The final
    /// planned leaf stores this fence exactly, so bounded subtrees can use the
    /// same common-prefix compression that materialization will write.
    fn pack_leaf_rewrite_entries(
        &self,
        entries: &[LogicalEntry],
        upper_fence: Option<&[u8]>,
    ) -> Result<Vec<RewriteEntry>> {
        validate_logical_entries_sorted(entries)?;
        let slot_entries = entries
            .iter()
            .map(|entry| {
                Ok(PackedNodeEntry {
                    key: entry.key.as_slice(),
                    value: F::leaf_value(entry)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut rewrite_entries = Vec::new();
        let mut start = 0usize;
        while start < entries.len() {
            let plan = plan_sibling_node(
                PackedNodePlanParams {
                    lower_fence: &entries[start].key,
                    upper_fence,
                    min_slots: 1,
                },
                &slot_entries[start..],
            )?;
            let end = start + plan.packed;
            rewrite_entries.push(RewriteEntry::Pending(PendingRewriteEntry {
                key: entries[start].key.clone(),
                upper_fence: plan.upper_fence.map(<[u8]>::to_vec),
                height: 0,
                payload: RewriteEntryPayload::Leaf(entries[start..end].to_vec()),
            }));
            start = end;
        }
        Ok(rewrite_entries)
    }

    /// Split child entries into pending branch blocks at `height`.
    ///
    /// `upper_fence` is the exclusive high fence of this branch-entry run. It is
    /// used only by the final planned branch block unless the run must split
    /// earlier on a following child lower fence.
    fn pack_branch_rewrite_entries(
        &self,
        entries: Vec<RewriteEntry>,
        height: u16,
        upper_fence: Option<&[u8]>,
    ) -> Result<Vec<RewriteEntry>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        if height == 0 {
            return Err(disk_tree_rewrite_invariant(
                "branch rewrite packing received leaf height",
            ));
        }
        validate_rewrite_entries_for_height(&entries, height - 1)?;
        let slot_entries = entries
            .iter()
            .map(|entry| PackedNodeEntry {
                key: entry.key(),
                value: BTreeU64::INVALID_VALUE,
            })
            .collect::<Vec<_>>();
        let mut rewrite_entries = Vec::new();
        let mut start = 0usize;
        while start < entries.len() {
            let first = &entries[start];
            let plan = plan_sibling_node(
                PackedNodePlanParams {
                    lower_fence: first.key(),
                    upper_fence,
                    min_slots: usize::from(start + 1 < entries.len()),
                },
                &slot_entries[start + 1..],
            )?;
            let end = start + 1 + plan.packed;
            rewrite_entries.push(RewriteEntry::Pending(PendingRewriteEntry {
                key: first.key().to_vec(),
                upper_fence: plan.upper_fence.map(<[u8]>::to_vec),
                height,
                payload: RewriteEntryPayload::Branch(entries[start..end].to_vec()),
            }));
            start = end;
        }
        Ok(rewrite_entries)
    }

    /// Materialize a planned rewrite entry into an allocated block.
    fn materialize_rewrite_entry<'b, M: MutableCowFile + 'b>(
        &'b self,
        mutable_file: &'b mut M,
        entry: RewriteEntry,
        create_ts: u64,
    ) -> Pin<Box<dyn Future<Output = Result<BranchEntry>> + 'b>> {
        Box::pin(async move {
            let mut allocations = RewriteAllocationGuard::new(mutable_file);
            let entry = self
                .materialize_rewrite_entry_with_allocations(&mut allocations, entry, create_ts)
                .await?;
            allocations.disarm();
            Ok(entry)
        })
    }

    fn materialize_rewrite_entry_with_allocations<'b, 'm, M: MutableCowFile + 'm>(
        &'b self,
        allocations: &'b mut RewriteAllocationGuard<'m, M>,
        entry: RewriteEntry,
        create_ts: u64,
    ) -> Pin<Box<dyn Future<Output = Result<BranchEntry>> + 'b>>
    where
        'm: 'b,
    {
        Box::pin(async move {
            match entry {
                RewriteEntry::Block(entry) => Ok(entry),
                RewriteEntry::Pending(entry) => match entry {
                    PendingRewriteEntry {
                        key,
                        upper_fence,
                        height,
                        payload: RewriteEntryPayload::Leaf(entries),
                    } => {
                        self.write_one_leaf_block(
                            allocations,
                            key,
                            upper_fence,
                            height,
                            entries,
                            create_ts,
                        )
                        .await
                    }
                    PendingRewriteEntry {
                        key,
                        upper_fence,
                        height,
                        payload: RewriteEntryPayload::Branch(children),
                    } => {
                        let children = self
                            .materialize_rewrite_entries(allocations, children, create_ts)
                            .await?;
                        self.write_one_branch_block(
                            allocations,
                            key,
                            upper_fence,
                            height,
                            children,
                            create_ts,
                        )
                        .await
                    }
                },
            }
        })
    }

    async fn materialize_rewrite_entries<'m, M: MutableCowFile + 'm>(
        &self,
        allocations: &mut RewriteAllocationGuard<'m, M>,
        entries: Vec<RewriteEntry>,
        create_ts: u64,
    ) -> Result<Vec<BranchEntry>> {
        let mut materialized = Vec::with_capacity(entries.len());
        for entry in entries {
            materialized.push(
                self.materialize_rewrite_entry_with_allocations(allocations, entry, create_ts)
                    .await?,
            );
        }
        Ok(materialized)
    }

    /// Write one already planned leaf block with its stored finite upper fence.
    async fn write_one_leaf_block<'m, M: MutableCowFile + 'm>(
        &self,
        allocations: &mut RewriteAllocationGuard<'m, M>,
        key: Vec<u8>,
        upper_fence: Option<Vec<u8>>,
        height: u16,
        entries: Vec<LogicalEntry>,
        create_ts: u64,
    ) -> Result<BranchEntry> {
        if entries.is_empty() || height != 0 || key.as_slice() != entries[0].key.as_slice() {
            return Err(disk_tree_rewrite_invariant(
                "leaf block materialization received invalid entry shape",
            ));
        }
        let slot_entries = entries
            .iter()
            .map(|entry| {
                Ok(PackedNodeEntry {
                    key: entry.key.as_slice(),
                    value: F::leaf_value(entry)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut buf = DirectBuf::zeroed(DISK_TREE_BLOCK_SIZE);
        let effective_space = {
            let node = btree_node_from_block_mut(buf.data_mut())?;
            pack_fixed_entries(
                node,
                KnownFenceNodeParams {
                    height: 0,
                    ts: create_ts,
                    lower_fence: &key,
                    lower_fence_value: BTreeU64::INVALID_VALUE,
                    upper_fence: upper_fence.as_deref(),
                    hints_enabled: true,
                },
                &slot_entries,
            )?
            .effective_space
        };
        let block_id = allocations.allocate_block_id()?;
        self.write_node_block(allocations.file(), block_id, buf)
            .await?;
        Ok(BranchEntry::rewritten_leaf(
            key,
            block_id,
            effective_space,
            entries,
        ))
    }

    /// Write one already planned branch block with its stored finite upper fence.
    async fn write_one_branch_block<'m, M: MutableCowFile + 'm>(
        &self,
        allocations: &mut RewriteAllocationGuard<'m, M>,
        key: Vec<u8>,
        upper_fence: Option<Vec<u8>>,
        height: u16,
        children: Vec<BranchEntry>,
        create_ts: u64,
    ) -> Result<BranchEntry> {
        if height == 0 || children.is_empty() || key.as_slice() != children[0].key.as_slice() {
            return Err(disk_tree_rewrite_invariant(
                "branch block materialization received invalid child shape",
            ));
        }
        validate_branch_entries_for_height(&children, height - 1)?;
        let first = &children[0];
        let slot_entries = children
            .iter()
            .skip(1)
            .map(|entry| {
                if entry.block_id == SUPER_BLOCK_ID {
                    return Err(disk_tree_rewrite_invariant(
                        "branch child points to empty-root sentinel",
                    ));
                }
                Ok(PackedNodeEntry {
                    key: entry.key.as_slice(),
                    value: BTreeU64::from(entry.block_id.as_u64()),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut buf = DirectBuf::zeroed(DISK_TREE_BLOCK_SIZE);
        let effective_space = {
            let node = btree_node_from_block_mut(buf.data_mut())?;
            pack_fixed_entries(
                node,
                KnownFenceNodeParams {
                    height,
                    ts: create_ts,
                    lower_fence: &key,
                    lower_fence_value: BTreeU64::from(first.block_id.as_u64()),
                    upper_fence: upper_fence.as_deref(),
                    hints_enabled: true,
                },
                &slot_entries,
            )?
            .effective_space
        };
        let block_id = allocations.allocate_block_id()?;
        self.write_node_block(allocations.file(), block_id, buf)
            .await?;
        Ok(BranchEntry::rewritten_branch(
            key,
            block_id,
            height,
            effective_space,
            children,
        ))
    }

    /// Finalize checksum and persist one fully formed DiskTree node block.
    async fn write_node_block<M: MutableCowFile>(
        &self,
        mutable_file: &M,
        block_id: BlockID,
        mut buf: DirectBuf,
    ) -> Result<()> {
        // Compute integrity after all node bytes, fences, values, and hints have
        // reached their final persisted representation.
        write_block_checksum(buf.data_mut());
        mutable_file.write_block(block_id, buf).await
    }
}

/// Calculate a branch height from a homogeneous rewrite child-entry run.
fn parent_height_from_rewrite_children(entries: &[RewriteEntry]) -> Result<u16> {
    let first = entries
        .first()
        .ok_or_else(|| disk_tree_rewrite_invariant("rewrite child run is empty"))?;
    validate_rewrite_entries_for_height(entries, first.height())?;
    first
        .height()
        .checked_add(1)
        .ok_or_else(|| disk_tree_rewrite_invariant("rewrite child height overflow"))
}

/// Validate that logical leaf entries are strictly sorted by encoded key.
fn validate_logical_entries_sorted(entries: &[LogicalEntry]) -> Result<()> {
    let mut prev = None;
    for entry in entries {
        if prev.is_some_and(|prev_key: &[u8]| prev_key >= entry.key.as_slice()) {
            return Err(invalid_payload(
                "DiskTree logical leaf entries are not strictly sorted",
            ));
        }
        prev = Some(entry.key.as_slice());
    }
    Ok(())
}

/// Validate a flattened branch-entry run before writing it into branch blocks.
fn validate_branch_entries_for_height(entries: &[BranchEntry], height: u16) -> Result<()> {
    if entries.is_empty() {
        return Err(disk_tree_rewrite_invariant("branch entry run is empty"));
    }
    let mut prev = None;
    for entry in entries {
        if entry.height != height || entry.block_id == SUPER_BLOCK_ID {
            return Err(disk_tree_rewrite_invariant(format!(
                "branch entry has invalid height or block id: expected_height={height}, actual_height={}, block_id={}",
                entry.height, entry.block_id
            )));
        }
        if prev.is_some_and(|prev_key: &[u8]| prev_key >= entry.key.as_slice()) {
            return Err(disk_tree_rewrite_invariant(
                "branch entries are not strictly sorted",
            ));
        }
        prev = Some(entry.key.as_slice());
    }
    Ok(())
}

/// Validate flattened rewrite entries before planning branch blocks.
fn validate_rewrite_entries_for_height(entries: &[RewriteEntry], height: u16) -> Result<()> {
    if entries.is_empty() {
        return Err(disk_tree_rewrite_invariant("rewrite entry run is empty"));
    }
    let mut prev = None;
    for entry in entries {
        if entry.height() != height {
            return Err(disk_tree_rewrite_invariant(format!(
                "rewrite entry height mismatch: expected_height={height}, actual_height={}",
                entry.height()
            )));
        }
        if let RewriteEntry::Block(entry) = entry
            && entry.block_id == SUPER_BLOCK_ID
        {
            return Err(disk_tree_rewrite_invariant(
                "rewrite entry points to empty-root sentinel",
            ));
        }
        if prev.is_some_and(|prev_key: &[u8]| prev_key >= entry.key()) {
            return Err(disk_tree_rewrite_invariant(
                "rewrite entries are not strictly sorted",
            ));
        }
        prev = Some(entry.key());
    }
    Ok(())
}

/// Resolve the child block selected by a branch-node key lookup.
#[inline]
fn lookup_child_block(node: &BTreeNode, key: &[u8]) -> Option<BlockID> {
    match node.lookup_child(key) {
        LookupChild::Slot(_, block_id) | LookupChild::LowerFence(block_id) => {
            Some(BlockID::from(u64::from(block_id)))
        }
        LookupChild::NotFound => None,
    }
}

/// Decode a branch node into flattened lower-fence child entries.
///
/// `BTreeNode` stores the first child in the lower fence and later children in
/// slots. The rewrite path uses this normalized sequence when routing batch
/// operations and rebuilding parent levels.
fn branch_entries_from_node(
    node: &BTreeNode,
    file_kind: FileKind,
    block_id: BlockID,
) -> Result<Vec<BranchEntry>> {
    let child_height = node
        .height()
        .checked_sub(1)
        .ok_or_else(|| invalid_node_payload(file_kind, block_id))?;
    let child_height =
        u16::try_from(child_height).map_err(|_| invalid_node_payload(file_kind, block_id))?;
    let mut entries = Vec::with_capacity(node.count() + 1);
    entries.push(BranchEntry::persisted(
        node.lower_fence_key().as_bytes().to_vec(),
        BlockID::from(node.lower_fence_value().to_u64()),
        child_height,
    ));
    for idx in 0..node.count() {
        let key = node
            .key_checked(idx)
            .ok_or_else(|| invalid_node_payload(file_kind, block_id))?;
        entries.push(BranchEntry::persisted(
            key,
            BlockID::from(node.value::<BTreeU64>(idx).to_u64()),
            child_height,
        ));
    }
    Ok(entries)
}

/// Root-snapshot view for a persisted unique secondary index.
///
/// Lookups read the immutable checkpoint root supplied at construction time.
/// Mutations are staged through `UniqueDiskTreeBatchWriter` and return a
/// replacement root block id for the caller to publish.
pub(crate) type UniqueDiskTree<'a> = DiskTree<'a, UniqueDiskTreeSpec>;

impl<'a> UniqueDiskTree<'a> {
    /// Look up one logical key and return its checkpointed owner row id.
    ///
    /// The lookup is exact and does not consult mutable in-memory index state or
    /// MVCC visibility; callers are expected to merge this cold-layer answer
    /// with newer index layers when serving user transactions.
    #[inline]
    pub(crate) async fn lookup(&self, key: &[Val]) -> Result<Option<RowID>> {
        let key = self.encoder().encode(key);
        self.lookup_encoded(key.as_bytes()).await
    }

    /// Look up one already-encoded logical key.
    #[inline]
    pub(crate) async fn lookup_encoded(&self, key: &[u8]) -> Result<Option<RowID>> {
        match self.lookup_encoded_entry(key).await? {
            Some(entry) => Ok(Some(entry.row_id.ok_or_else(|| {
                invalid_payload("unique DiskTree lookup entry is missing row id")
            })?)),
            None => Ok(None),
        }
    }

    /// Scan encoded logical keys and row ids in durable key order.
    #[inline]
    pub(crate) async fn scan_entries(&self) -> Result<Vec<(Vec<u8>, RowID)>> {
        self.collect_entries()
            .await?
            .into_iter()
            .map(|entry| {
                Ok((
                    entry.key,
                    entry.row_id.ok_or_else(|| {
                        invalid_payload("unique DiskTree scan entry is missing row id")
                    })?,
                ))
            })
            .collect()
    }

    /// Open a copy-on-write batch writer for this unique DiskTree root.
    ///
    /// The writer accumulates sorted logical operations and writes replacement
    /// blocks into the supplied mutable table-file fork when `finish` is called.
    #[inline]
    pub(crate) fn batch_writer<'w, M: MutableCowFile>(
        &'w self,
        mutable_file: &'w mut M,
        create_ts: u64,
    ) -> UniqueDiskTreeBatchWriter<'w, 'a, M> {
        UniqueDiskTreeBatchWriter {
            tree: self,
            mutable_file,
            operations: BTreeMap::new(),
            create_ts,
        }
    }
}

/// Root-snapshot view for a persisted non-unique secondary index.
///
/// Exact entries are encoded as `(logical_key, row_id)` keys with no leaf value
/// bytes. Prefix APIs decode the row-id suffix from matching exact keys.
pub(crate) type NonUniqueDiskTree<'a> = DiskTree<'a, NonUniqueDiskTreeSpec>;

impl<'a> NonUniqueDiskTree<'a> {
    /// Return whether one exact `(logical_key, row_id)` entry exists.
    #[inline]
    pub(crate) async fn contains_exact(&self, key: &[Val], row_id: RowID) -> Result<bool> {
        let key = self.encoder().encode_pair(key, Val::from(row_id));
        self.contains_exact_encoded(key.as_bytes()).await
    }

    /// Return whether one already-encoded exact `(logical_key, row_id)` entry
    /// exists in the current root snapshot.
    #[inline]
    pub(crate) async fn contains_exact_encoded(&self, key: &[u8]) -> Result<bool> {
        Ok(self.lookup_encoded_entry(key).await?.is_some())
    }

    /// Prefix-scan one logical key and return encoded exact keys with row ids.
    ///
    /// Composite secondary-index reads use the encoded exact key to merge
    /// MemIndex and DiskTree entries without duplicating key encoders outside the
    /// concrete DiskTree reader.
    #[inline]
    pub(crate) async fn prefix_scan_entries(&self, key: &[Val]) -> Result<Vec<(Vec<u8>, RowID)>> {
        let prefix = self.encoder().encode_prefix(key, Some(ROW_ID_SIZE));
        let prefix_bytes = prefix.as_bytes();
        let mut entries = Vec::new();
        self.scan_entries_from(prefix_bytes, |entry| {
            if !entry.key.starts_with(prefix_bytes) {
                return Ok(false);
            }
            let row_id = unpack_row_id_from_exact_key(&entry.key)?;
            entries.push((entry.key, row_id));
            Ok(true)
        })
        .await?;
        Ok(entries)
    }

    /// Scan encoded exact keys and row ids in durable exact-key order.
    #[inline]
    pub(crate) async fn scan_entries(&self) -> Result<Vec<(Vec<u8>, RowID)>> {
        self.collect_entries()
            .await?
            .into_iter()
            .map(|entry| {
                let row_id = unpack_row_id_from_exact_key(&entry.key)?;
                Ok((entry.key, row_id))
            })
            .collect()
    }

    /// Open a copy-on-write batch writer for this non-unique DiskTree root.
    #[inline]
    pub(crate) fn batch_writer<'w, M: MutableCowFile>(
        &'w self,
        mutable_file: &'w mut M,
        create_ts: u64,
    ) -> NonUniqueDiskTreeBatchWriter<'w, 'a, M> {
        NonUniqueDiskTreeBatchWriter {
            tree: self,
            mutable_file,
            operations: BTreeMap::new(),
            create_ts,
        }
    }
}

/// Mutable batch writer for one unique DiskTree root.
///
/// The writer coalesces work by encoded logical key, preserves per-key operation
/// order, and writes replacement CoW paths only when `finish` is awaited.
pub(crate) struct UniqueDiskTreeBatchWriter<'w, 'a, M: MutableCowFile> {
    tree: &'w UniqueDiskTree<'a>,
    mutable_file: &'w mut M,
    operations: BTreeMap<Vec<u8>, Vec<UniqueDiskTreeOp>>,
    create_ts: u64,
}

impl<M: MutableCowFile> UniqueDiskTreeBatchWriter<'_, '_, M> {
    /// Add already-encoded, strictly sorted logical-key put work to this writer.
    pub(crate) fn batch_put_encoded(
        &mut self,
        entries: &[UniqueDiskTreeEncodedPut<'_>],
    ) -> Result<()> {
        validate_sorted_unique_keys(entries.iter().map(|entry| entry.key))?;
        for entry in entries {
            self.operations
                .entry(entry.key.to_vec())
                .or_default()
                .push(UniqueDiskTreeOp::Put(entry.row_id));
        }
        Ok(())
    }

    /// Add already-encoded logical-key conditional delete work to this writer.
    ///
    /// Equal adjacent keys are accepted so a checkpoint can try multiple
    /// expected old owners for the same key without re-encoding.
    pub(crate) fn batch_conditional_delete_encoded(
        &mut self,
        entries: &[UniqueDiskTreeEncodedDelete<'_>],
    ) -> Result<()> {
        validate_sorted_keys(entries.iter().map(|entry| entry.key))?;
        for entry in entries {
            self.operations.entry(entry.key.to_vec()).or_default().push(
                UniqueDiskTreeOp::ConditionalDelete(entry.expected_old_row_id),
            );
        }
        Ok(())
    }

    /// Write touched CoW paths and return the final root block id.
    ///
    /// This does not publish the root into table metadata; the caller owns that
    /// higher-level checkpoint state transition.
    pub(crate) async fn finish(self) -> Result<BlockID> {
        let Self {
            tree,
            mutable_file,
            operations,
            create_ts,
        } = self;
        let mut flattened = Vec::new();
        for (key, ops) in operations {
            for op in ops {
                flattened.push(DiskTreeOperation {
                    key: key.clone(),
                    kind: DiskTreeOperationKind::Unique(op),
                });
            }
        }
        tree.rewrite_root(mutable_file, &flattened, create_ts).await
    }
}

/// Mutable batch writer for one non-unique DiskTree root.
///
/// Operations record the desired final presence of exact encoded keys. When the
/// same exact key appears more than once in the writer, the latest staged
/// presence wins.
pub(crate) struct NonUniqueDiskTreeBatchWriter<'w, 'a, M: MutableCowFile> {
    tree: &'w NonUniqueDiskTree<'a>,
    mutable_file: &'w mut M,
    operations: BTreeMap<Vec<u8>, bool>,
    create_ts: u64,
}

impl<M: MutableCowFile> NonUniqueDiskTreeBatchWriter<'_, '_, M> {
    /// Add already-encoded, strictly sorted exact-entry insert work.
    pub(crate) fn batch_insert_encoded(
        &mut self,
        entries: &[NonUniqueDiskTreeEncodedExact<'_>],
    ) -> Result<()> {
        validate_sorted_non_unique_exact_keys(entries)?;
        for entry in entries {
            self.operations.insert(entry.key.to_vec(), true);
        }
        Ok(())
    }

    /// Add already-encoded, strictly sorted exact-entry delete work.
    pub(crate) fn batch_exact_delete_encoded(
        &mut self,
        entries: &[NonUniqueDiskTreeEncodedExact<'_>],
    ) -> Result<()> {
        validate_sorted_non_unique_exact_keys(entries)?;
        for entry in entries {
            self.operations.insert(entry.key.to_vec(), false);
        }
        Ok(())
    }

    /// Write touched CoW paths and return the final root block id.
    ///
    /// This returns `SUPER_BLOCK_ID` if the staged operations make the tree
    /// empty, otherwise it returns the root block of the replacement tree.
    pub(crate) async fn finish(self) -> Result<BlockID> {
        let Self {
            tree,
            mutable_file,
            operations,
            create_ts,
        } = self;
        let flattened = operations
            .into_iter()
            .map(|(key, present)| DiskTreeOperation {
                key,
                kind: DiskTreeOperationKind::NonUniqueSetPresent(present),
            })
            .collect::<Vec<_>>();
        tree.rewrite_root(mutable_file, &flattened, create_ts).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{global_readonly_pool_scope, table_readonly_pool};
    use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey};
    use crate::error::{DataIntegrityError, Error, FileKind};
    use crate::file::build_test_fs;
    use crate::file::table_file::MutableTableFile;
    use crate::value::ValKind;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn metadata_with_indexes() -> Arc<TableMetadata> {
        Arc::new(TableMetadata::new(
            vec![
                ColumnSpec::new("c0", ValKind::U32, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::U64, ColumnAttributes::empty()),
            ],
            vec![
                IndexSpec::new("idx_unique", vec![IndexKey::new(0)], IndexAttributes::UK),
                IndexSpec::new(
                    "idx_non_unique",
                    vec![IndexKey::new(0)],
                    IndexAttributes::empty(),
                ),
            ],
        ))
    }

    fn assert_disk_tree_corruption(err: Error, expected: DataIntegrityError) {
        assert_eq!(err.data_integrity_error(), Some(expected));
        let report = format!("{err:?}");
        assert!(report.contains("table-file"), "{report}");
        assert!(report.contains("secondary-disk-tree"), "{report}");
    }

    fn metadata_with_varbyte_unique_index() -> Arc<TableMetadata> {
        Arc::new(TableMetadata::new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::VarByte,
                ColumnAttributes::empty(),
            )],
            vec![IndexSpec::new(
                "idx_unique",
                vec![IndexKey::new(0)],
                IndexAttributes::UK,
            )],
        ))
    }

    fn long_varbyte_key(idx: u32) -> Vec<u8> {
        const KEY_LEN: usize = 1024;
        let mut key = vec![0u8; KEY_LEN];
        key[..mem::size_of::<u32>()].copy_from_slice(&idx.to_be_bytes());
        for (pos, byte) in key[mem::size_of::<u32>()..].iter_mut().enumerate() {
            *byte = (idx as u8).wrapping_mul(31).wrapping_add(pos as u8);
        }
        key
    }

    macro_rules! unique_runtime {
        ($metadata:ident, $disk_pool:ident) => {
            UniqueDiskTreeRuntime::new(
                &$metadata.index_specs[0],
                $metadata.as_ref(),
                $disk_pool.file_kind(),
                Arc::clone($disk_pool.sparse_file()),
                $disk_pool.global_pool().clone(),
            )
            .unwrap()
        };
    }

    macro_rules! non_unique_runtime {
        ($metadata:ident, $disk_pool:ident) => {
            NonUniqueDiskTreeRuntime::new(
                &$metadata.index_specs[1],
                $metadata.as_ref(),
                $disk_pool.file_kind(),
                Arc::clone($disk_pool.sparse_file()),
                $disk_pool.global_pool().clone(),
            )
            .unwrap()
        };
    }

    struct UniqueDiskTreePut<'a> {
        key: &'a [Val],
        row_id: RowID,
    }

    struct UniqueDiskTreeDelete<'a> {
        key: &'a [Val],
        expected_old_row_id: RowID,
    }

    struct NonUniqueDiskTreeExact<'a> {
        key: &'a [Val],
        row_id: RowID,
    }

    trait UniqueDiskTreeBatchWriterTestExt {
        fn batch_put(&mut self, entries: &[UniqueDiskTreePut<'_>]) -> Result<()>;

        fn batch_conditional_delete(&mut self, entries: &[UniqueDiskTreeDelete<'_>]) -> Result<()>;
    }

    impl<M: MutableCowFile> UniqueDiskTreeBatchWriterTestExt for UniqueDiskTreeBatchWriter<'_, '_, M> {
        fn batch_put(&mut self, entries: &[UniqueDiskTreePut<'_>]) -> Result<()> {
            let encoded = entries
                .iter()
                .map(|entry| {
                    (
                        self.tree.encoder().encode(entry.key).as_bytes().to_vec(),
                        entry.row_id,
                    )
                })
                .collect::<Vec<_>>();
            let encoded_entries = encoded
                .iter()
                .map(|(key, row_id)| UniqueDiskTreeEncodedPut {
                    key,
                    row_id: *row_id,
                })
                .collect::<Vec<_>>();
            self.batch_put_encoded(&encoded_entries)
        }

        fn batch_conditional_delete(&mut self, entries: &[UniqueDiskTreeDelete<'_>]) -> Result<()> {
            let encoded = entries
                .iter()
                .map(|entry| {
                    (
                        self.tree.encoder().encode(entry.key).as_bytes().to_vec(),
                        entry.expected_old_row_id,
                    )
                })
                .collect::<Vec<_>>();
            let encoded_entries = encoded
                .iter()
                .map(|(key, row_id)| UniqueDiskTreeEncodedDelete {
                    key,
                    expected_old_row_id: *row_id,
                })
                .collect::<Vec<_>>();
            self.batch_conditional_delete_encoded(&encoded_entries)
        }
    }

    trait NonUniqueDiskTreeBatchWriterTestExt {
        fn batch_insert(&mut self, entries: &[NonUniqueDiskTreeExact<'_>]) -> Result<()>;

        fn batch_exact_delete(&mut self, entries: &[NonUniqueDiskTreeExact<'_>]) -> Result<()>;
    }

    impl<M: MutableCowFile> NonUniqueDiskTreeBatchWriterTestExt
        for NonUniqueDiskTreeBatchWriter<'_, '_, M>
    {
        fn batch_insert(&mut self, entries: &[NonUniqueDiskTreeExact<'_>]) -> Result<()> {
            let encoded = encode_non_unique_exact_batch(self.tree, entries)?;
            let encoded_entries = encoded
                .iter()
                .map(|key| NonUniqueDiskTreeEncodedExact { key })
                .collect::<Vec<_>>();
            self.batch_insert_encoded(&encoded_entries)
        }

        fn batch_exact_delete(&mut self, entries: &[NonUniqueDiskTreeExact<'_>]) -> Result<()> {
            let encoded = encode_non_unique_exact_batch(self.tree, entries)?;
            let encoded_entries = encoded
                .iter()
                .map(|key| NonUniqueDiskTreeEncodedExact { key })
                .collect::<Vec<_>>();
            self.batch_exact_delete_encoded(&encoded_entries)
        }
    }

    trait NonUniqueDiskTreeTestExt {
        fn prefix_scan<'b>(
            &'b self,
            key: &'b [Val],
        ) -> Pin<Box<dyn Future<Output = Result<Vec<RowID>>> + 'b>>;
    }

    impl NonUniqueDiskTreeTestExt for NonUniqueDiskTree<'_> {
        fn prefix_scan<'b>(
            &'b self,
            key: &'b [Val],
        ) -> Pin<Box<dyn Future<Output = Result<Vec<RowID>>> + 'b>> {
            Box::pin(async move {
                Ok(self
                    .prefix_scan_entries(key)
                    .await?
                    .into_iter()
                    .map(|(_, row_id)| row_id)
                    .collect())
            })
        }
    }

    fn encode_non_unique_exact_batch(
        tree: &NonUniqueDiskTree<'_>,
        entries: &[NonUniqueDiskTreeExact<'_>],
    ) -> Result<Vec<Vec<u8>>> {
        let encoded = entries
            .iter()
            .map(|entry| {
                tree.encoder()
                    .encode_pair(entry.key, Val::from(entry.row_id))
                    .as_bytes()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        validate_sorted_unique_keys(encoded.iter().map(Vec::as_slice))?;
        Ok(encoded)
    }

    struct FailingDiskTreeWriteFile {
        inner: MutableTableFile,
        fail_leaf_at: Option<usize>,
        fail_branch_at: Option<usize>,
        leaf_writes: AtomicUsize,
        branch_writes: AtomicUsize,
    }

    impl FailingDiskTreeWriteFile {
        fn new(
            inner: MutableTableFile,
            fail_leaf_at: Option<usize>,
            fail_branch_at: Option<usize>,
        ) -> Self {
            Self {
                inner,
                fail_leaf_at,
                fail_branch_at,
                leaf_writes: AtomicUsize::new(0),
                branch_writes: AtomicUsize::new(0),
            }
        }

        fn allocated_blocks(&self) -> usize {
            self.inner.root().alloc_map.allocated()
        }

        fn leaf_writes(&self) -> usize {
            self.leaf_writes.load(Ordering::Acquire)
        }

        fn branch_writes(&self) -> usize {
            self.branch_writes.load(Ordering::Acquire)
        }

        fn should_fail(&self, buf: &DirectBuf) -> bool {
            let Some(node) = btree_node_from_block(buf.data()) else {
                return false;
            };
            if node.is_leaf() {
                let write_idx = self.leaf_writes.fetch_add(1, Ordering::AcqRel);
                self.fail_leaf_at == Some(write_idx)
            } else {
                let write_idx = self.branch_writes.fetch_add(1, Ordering::AcqRel);
                self.fail_branch_at == Some(write_idx)
            }
        }
    }

    impl MutableCowFile for FailingDiskTreeWriteFile {
        fn allocate_block_id(&mut self) -> Result<BlockID> {
            self.inner.allocate_block_id()
        }

        fn rollback_allocated_block_id(&mut self, block_id: BlockID) -> Result<()> {
            self.inner.rollback_allocated_block_id(block_id)
        }

        fn record_gc_block(&mut self, block_id: BlockID) {
            self.inner.record_gc_block(block_id)
        }

        fn write_block(
            &self,
            block_id: BlockID,
            buf: DirectBuf,
        ) -> impl Future<Output = Result<()>> + Send {
            let should_fail = self.should_fail(&buf);
            async move {
                if should_fail {
                    return Err(Report::new(InternalError::InjectedTestFailure).into());
                }
                self.inner.write_block(block_id, buf).await
            }
        }
    }

    #[derive(Debug)]
    struct NodeSummary {
        is_leaf: bool,
        height: usize,
        has_upper_fence: bool,
        common_prefix_len: usize,
    }

    async fn collect_node_summaries<F: DiskTreeSpec>(
        tree: &DiskTree<'_, F>,
    ) -> Result<Vec<NodeSummary>> {
        if tree.root_block_id == SUPER_BLOCK_ID {
            return Ok(Vec::new());
        }
        let mut summaries = Vec::new();
        let mut stack = vec![tree.root_block_id];
        while let Some(block_id) = stack.pop() {
            let guard = tree.read_node(block_id).await?;
            let node = guard.node();
            let child_entries = if node.is_leaf() {
                None
            } else {
                Some(branch_entries_from_node(node, tree.file_kind(), block_id)?)
            };
            summaries.push(NodeSummary {
                is_leaf: node.is_leaf(),
                height: node.height(),
                has_upper_fence: !node.has_no_upper_fence(),
                common_prefix_len: node.common_prefix().len(),
            });
            drop(guard);
            if let Some(child_entries) = child_entries {
                for entry in child_entries.into_iter().rev() {
                    stack.push(entry.block_id);
                }
            }
        }
        Ok(summaries)
    }

    #[derive(Debug)]
    struct LeafBlock {
        block_id: BlockID,
        upper_fence: Option<Vec<u8>>,
        common_prefix_len: usize,
        keys: Vec<Vec<u8>>,
    }

    async fn collect_leaf_blocks<F: DiskTreeSpec>(
        tree: &DiskTree<'_, F>,
    ) -> Result<Vec<LeafBlock>> {
        if tree.root_block_id == SUPER_BLOCK_ID {
            return Ok(Vec::new());
        }
        let mut leaves = Vec::new();
        let mut stack = vec![tree.root_block_id];
        while let Some(block_id) = stack.pop() {
            let guard = tree.read_node(block_id).await?;
            let node = guard.node();
            if node.is_leaf() {
                let mut keys = Vec::with_capacity(node.count());
                for idx in 0..node.count() {
                    keys.push(F::leaf_entry(node, idx, tree.file_kind(), block_id)?.key);
                }
                let upper_fence = if node.has_no_upper_fence() {
                    None
                } else {
                    Some(node.upper_fence_key().as_bytes().to_vec())
                };
                leaves.push(LeafBlock {
                    block_id,
                    upper_fence,
                    common_prefix_len: node.common_prefix().len(),
                    keys,
                });
            } else {
                let branch_entries = branch_entries_from_node(node, tree.file_kind(), block_id)?;
                for entry in branch_entries.into_iter().rev() {
                    stack.push(entry.block_id);
                }
            }
        }
        Ok(leaves)
    }

    fn long_encoded_key(idx: usize) -> Vec<u8> {
        let mut key = format!("branch-prefix-{idx:06}-").into_bytes();
        key.resize(4096, b'x');
        key
    }

    #[test]
    fn test_empty_unique_root_reads_empty() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(301, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 301, &table);
            let guard = disk_pool.pool_guard();
            let runtime = unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);
            assert_eq!(tree.lookup(&[Val::from(1u32)]).await.unwrap(), None);
            assert!(tree.scan_entries().await.unwrap().is_empty());
            drop(table);
            drop(fs);
        });
    }

    #[test]
    fn test_empty_non_unique_root_reads_empty() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(302, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 302, &table);
            let guard = disk_pool.pool_guard();
            let runtime = non_unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);
            assert!(!tree.contains_exact(&[Val::from(1u32)], 10).await.unwrap());
            assert!(
                tree.prefix_scan(&[Val::from(1u32)])
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(tree.scan_entries().await.unwrap().is_empty());
            drop(table);
            drop(fs);
        });
    }

    #[test]
    fn test_unique_batch_put_lookup_scan_and_conditional_delete() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(303, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 303, &table);
            let guard = disk_pool.pool_guard();
            let mut mutable =
                crate::file::table_file::MutableTableFile::fork(&table, fs.background_writes());
            let runtime = unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);
            let key1 = [Val::from(1u32)];
            let key2 = [Val::from(2u32)];
            let key3 = [Val::from(3u32)];
            let mut writer = tree.batch_writer(&mut mutable, 2);
            writer
                .batch_put(&[
                    UniqueDiskTreePut {
                        key: &key1,
                        row_id: 10,
                    },
                    UniqueDiskTreePut {
                        key: &key2,
                        row_id: 20,
                    },
                    UniqueDiskTreePut {
                        key: &key3,
                        row_id: 30,
                    },
                ])
                .unwrap();
            let root = writer.finish().await.unwrap();
            assert_ne!(root, SUPER_BLOCK_ID);

            let tree = runtime.open(root, &guard);
            assert_eq!(tree.lookup(&key2).await.unwrap(), Some(20));
            let rows = tree
                .scan_entries()
                .await
                .unwrap()
                .into_iter()
                .map(|(_, row_id)| row_id)
                .collect::<Vec<_>>();
            assert_eq!(rows, vec![10, 20, 30]);

            let mut writer = tree.batch_writer(&mut mutable, 3);
            writer
                .batch_conditional_delete(&[
                    UniqueDiskTreeDelete {
                        key: &key1,
                        expected_old_row_id: 999,
                    },
                    UniqueDiskTreeDelete {
                        key: &key2,
                        expected_old_row_id: 20,
                    },
                ])
                .unwrap();
            let new_root = writer.finish().await.unwrap();
            let new_tree = runtime.open(new_root, &guard);
            assert_eq!(new_tree.lookup(&key1).await.unwrap(), Some(10));
            assert_eq!(new_tree.lookup(&key2).await.unwrap(), None);
            assert_eq!(tree.lookup(&key2).await.unwrap(), Some(20));
            assert!(mutable.root().gc_block_list.is_empty());
        });
    }

    #[test]
    fn test_unique_disk_tree_leaf_fences_enable_common_prefix() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(310, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 310, &table);
            let guard = disk_pool.pool_guard();
            let mut mutable =
                crate::file::table_file::MutableTableFile::fork(&table, fs.background_writes());
            let runtime = unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);

            const ENTRY_COUNT: u32 = 10_000;
            let keys = (0..ENTRY_COUNT)
                .map(|idx| [Val::from(0x1234_0000u32 + idx)])
                .collect::<Vec<_>>();
            let puts = keys
                .iter()
                .enumerate()
                .map(|(idx, key)| UniqueDiskTreePut {
                    key,
                    row_id: idx as RowID + 100,
                })
                .collect::<Vec<_>>();
            let mut writer = tree.batch_writer(&mut mutable, 2);
            writer.batch_put(&puts).unwrap();
            let root = writer.finish().await.unwrap();
            let tree = runtime.open(root, &guard);

            let summaries = collect_node_summaries(&tree).await.unwrap();
            assert!(summaries.iter().any(|summary| {
                summary.is_leaf && summary.has_upper_fence && summary.common_prefix_len > 0
            }));
            assert_eq!(tree.lookup(&keys[0]).await.unwrap(), Some(100));
            assert_eq!(tree.lookup(&keys[4096]).await.unwrap(), Some(4196));
            assert_eq!(
                tree.lookup(&keys[ENTRY_COUNT as usize - 1]).await.unwrap(),
                Some(ENTRY_COUNT as RowID + 99)
            );

            let scanned = tree.scan_entries().await.unwrap();
            assert_eq!(scanned.len(), ENTRY_COUNT as usize);
            assert!(scanned.windows(2).all(|pair| pair[0].0 < pair[1].0));

            let mut writer = tree.batch_writer(&mut mutable, 3);
            writer
                .batch_conditional_delete(&[
                    UniqueDiskTreeDelete {
                        key: &keys[4096],
                        expected_old_row_id: 4196,
                    },
                    UniqueDiskTreeDelete {
                        key: &keys[8192],
                        expected_old_row_id: 8292,
                    },
                ])
                .unwrap();
            let rewritten_root = writer.finish().await.unwrap();
            let rewritten_tree = runtime.open(rewritten_root, &guard);
            assert_eq!(rewritten_tree.lookup(&keys[4096]).await.unwrap(), None);
            assert_eq!(rewritten_tree.lookup(&keys[8192]).await.unwrap(), None);
            assert_eq!(tree.lookup(&keys[4096]).await.unwrap(), Some(4196));
            assert_eq!(tree.lookup(&keys[8192]).await.unwrap(), Some(8292));
            assert!(mutable.root().gc_block_list.is_empty());
        });
    }

    #[test]
    fn test_non_unique_disk_tree_prefix_scan_crosses_finite_fence_leaves() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(311, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 311, &table);
            let guard = disk_pool.pool_guard();
            let mut mutable =
                crate::file::table_file::MutableTableFile::fork(&table, fs.background_writes());
            let runtime = non_unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);

            const ENTRY_COUNT: usize = 10_000;
            let key = [Val::from(0x1234_5678u32)];
            let entries = (0..ENTRY_COUNT)
                .map(|idx| NonUniqueDiskTreeExact {
                    key: &key,
                    row_id: idx as RowID + 1_000,
                })
                .collect::<Vec<_>>();
            let mut writer = tree.batch_writer(&mut mutable, 2);
            writer.batch_insert(&entries).unwrap();
            let root = writer.finish().await.unwrap();
            let tree = runtime.open(root, &guard);

            let summaries = collect_node_summaries(&tree).await.unwrap();
            assert!(summaries.iter().any(|summary| {
                summary.is_leaf && summary.has_upper_fence && summary.common_prefix_len > 0
            }));
            let rows = tree.prefix_scan(&key).await.unwrap();
            assert_eq!(rows.len(), ENTRY_COUNT);
            assert_eq!(rows.first().copied(), Some(1_000));
            assert_eq!(rows.last().copied(), Some(ENTRY_COUNT as RowID + 999));
            assert!(rows.windows(2).all(|pair| pair[0] < pair[1]));
        });
    }

    #[test]
    fn test_disk_tree_branch_fences_enable_common_prefix() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(312, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 312, &table);
            let guard = disk_pool.pool_guard();
            let mut mutable =
                crate::file::table_file::MutableTableFile::fork(&table, fs.background_writes());
            let runtime = unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);

            const ENTRY_COUNT: usize = 400;
            let keys = (0..ENTRY_COUNT).map(long_encoded_key).collect::<Vec<_>>();
            let puts = keys
                .iter()
                .enumerate()
                .map(|(idx, key)| UniqueDiskTreeEncodedPut {
                    key,
                    row_id: idx as RowID + 5_000,
                })
                .collect::<Vec<_>>();
            let mut writer = tree.batch_writer(&mut mutable, 2);
            writer.batch_put_encoded(&puts).unwrap();
            let root = writer.finish().await.unwrap();
            let tree = runtime.open(root, &guard);

            let summaries = collect_node_summaries(&tree).await.unwrap();
            assert!(summaries.iter().any(|summary| summary.height >= 2));
            assert!(summaries.iter().any(|summary| {
                !summary.is_leaf && summary.has_upper_fence && summary.common_prefix_len > 0
            }));
            for idx in [0usize, 127, 255, 399] {
                assert_eq!(
                    tree.lookup_encoded(&keys[idx]).await.unwrap(),
                    Some(idx as RowID + 5_000)
                );
            }
            let scanned = tree.scan_entries().await.unwrap();
            assert_eq!(scanned.len(), ENTRY_COUNT);
            assert!(scanned.windows(2).all(|pair| pair[0].0 < pair[1].0));
        });
    }

    #[test]
    fn test_non_unique_batch_insert_prefix_scan_and_delete() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(304, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 304, &table);
            let guard = disk_pool.pool_guard();
            let mut mutable =
                crate::file::table_file::MutableTableFile::fork(&table, fs.background_writes());
            let runtime = non_unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);
            let key1 = [Val::from(1u32)];
            let key2 = [Val::from(2u32)];
            let mut writer = tree.batch_writer(&mut mutable, 2);
            writer
                .batch_insert(&[
                    NonUniqueDiskTreeExact {
                        key: &key1,
                        row_id: 10,
                    },
                    NonUniqueDiskTreeExact {
                        key: &key1,
                        row_id: 11,
                    },
                    NonUniqueDiskTreeExact {
                        key: &key2,
                        row_id: 20,
                    },
                ])
                .unwrap();
            let root = writer.finish().await.unwrap();
            let tree = runtime.open(root, &guard);
            assert!(tree.contains_exact(&key1, 10).await.unwrap());
            assert_eq!(tree.prefix_scan(&key1).await.unwrap(), vec![10, 11]);

            let mut writer = tree.batch_writer(&mut mutable, 3);
            writer
                .batch_exact_delete(&[
                    NonUniqueDiskTreeExact {
                        key: &key1,
                        row_id: 10,
                    },
                    NonUniqueDiskTreeExact {
                        key: &key1,
                        row_id: 999,
                    },
                ])
                .unwrap();
            writer
                .batch_insert(&[NonUniqueDiskTreeExact {
                    key: &key1,
                    row_id: 12,
                }])
                .unwrap();
            let new_root = writer.finish().await.unwrap();
            let new_tree = runtime.open(new_root, &guard);
            assert_eq!(new_tree.prefix_scan(&key1).await.unwrap(), vec![11, 12]);
            assert!(!new_tree.contains_exact(&key1, 10).await.unwrap());
            assert_eq!(tree.prefix_scan(&key1).await.unwrap(), vec![10, 11]);
            assert!(mutable.root().gc_block_list.is_empty());
        });
    }

    #[test]
    fn test_non_unique_prefix_scan_streams_from_lower_bound() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(309, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 309, &table);
            let guard = disk_pool.pool_guard();
            let mut mutable =
                crate::file::table_file::MutableTableFile::fork(&table, fs.background_writes());
            let runtime = non_unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);

            const ENTRY_COUNT: u32 = 65_536;
            let keys = (0..ENTRY_COUNT)
                .map(|idx| [Val::from(idx)])
                .collect::<Vec<_>>();
            let entries = keys
                .iter()
                .enumerate()
                .map(|(idx, key)| NonUniqueDiskTreeExact {
                    key,
                    row_id: idx as RowID + 1000,
                })
                .collect::<Vec<_>>();
            let allocated_before = mutable.root().alloc_map.allocated();
            let mut writer = tree.batch_writer(&mut mutable, 2);
            writer.batch_insert(&entries).unwrap();
            let root = writer.finish().await.unwrap();
            assert_ne!(root, SUPER_BLOCK_ID);
            let tree_blocks = mutable.root().alloc_map.allocated() - allocated_before;
            assert!(tree_blocks > 8);

            let tree = runtime.open(root, &guard);
            let key = [Val::from(7u32)];
            let stats_before = disk_pool.global_stats();
            assert_eq!(tree.prefix_scan(&key).await.unwrap(), vec![1007]);
            let delta = disk_pool.global_stats().delta_since(stats_before);
            assert!(
                delta.cache_misses < tree_blocks / 2,
                "prefix scan loaded {} readonly blocks out of {} DiskTree blocks",
                delta.cache_misses,
                tree_blocks
            );
        });
    }

    #[test]
    fn test_disk_tree_rewrite_rolls_back_leaf_allocations_on_write_failure() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(310, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 310, &table);
            let guard = disk_pool.pool_guard();
            let inner = MutableTableFile::fork(&table, fs.background_writes());
            let allocated_before = inner.root().alloc_map.allocated();
            let mut mutable = FailingDiskTreeWriteFile::new(inner, Some(1), None);
            let runtime = unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);

            const ENTRY_COUNT: u32 = 8192;
            let keys = (0..ENTRY_COUNT)
                .map(|idx| [Val::from(idx)])
                .collect::<Vec<_>>();
            let puts = keys
                .iter()
                .enumerate()
                .map(|(idx, key)| UniqueDiskTreePut {
                    key,
                    row_id: idx as RowID + 100,
                })
                .collect::<Vec<_>>();

            let mut writer = tree.batch_writer(&mut mutable, 2);
            writer.batch_put(&puts).unwrap();
            let err = writer.finish().await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<InternalError>().copied(),
                Some(InternalError::InjectedTestFailure)
            );
            assert_eq!(mutable.leaf_writes(), 2);
            assert_eq!(mutable.branch_writes(), 0);
            assert_eq!(mutable.allocated_blocks(), allocated_before);
        });
    }

    #[test]
    fn test_disk_tree_rewrite_rolls_back_child_allocations_on_branch_write_failure() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(311, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 311, &table);
            let guard = disk_pool.pool_guard();
            let inner = MutableTableFile::fork(&table, fs.background_writes());
            let allocated_before = inner.root().alloc_map.allocated();
            let mut mutable = FailingDiskTreeWriteFile::new(inner, None, Some(0));
            let runtime = unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);

            const ENTRY_COUNT: u32 = 8192;
            let keys = (0..ENTRY_COUNT)
                .map(|idx| [Val::from(idx)])
                .collect::<Vec<_>>();
            let puts = keys
                .iter()
                .enumerate()
                .map(|(idx, key)| UniqueDiskTreePut {
                    key,
                    row_id: idx as RowID + 100,
                })
                .collect::<Vec<_>>();

            let mut writer = tree.batch_writer(&mut mutable, 2);
            writer.batch_put(&puts).unwrap();
            let err = writer.finish().await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<InternalError>().copied(),
                Some(InternalError::InjectedTestFailure)
            );
            assert!(
                mutable.leaf_writes() > 1,
                "branch failure test should materialize multiple leaves first"
            );
            assert_eq!(mutable.branch_writes(), 1);
            assert_eq!(mutable.allocated_blocks(), allocated_before);
        });
    }

    #[test]
    fn test_encoded_batch_writer_apis() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(305, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 305, &table);
            let guard = disk_pool.pool_guard();
            let mut mutable =
                crate::file::table_file::MutableTableFile::fork(&table, fs.background_writes());

            let unique_runtime = unique_runtime!(metadata, disk_pool);
            let unique_tree = unique_runtime.open(SUPER_BLOCK_ID, &guard);
            let key1 = [Val::from(1u32)];
            let key2 = [Val::from(2u32)];
            let encoded_unique1 = unique_tree.encoder().encode(&key1).as_bytes().to_vec();
            let encoded_unique2 = unique_tree.encoder().encode(&key2).as_bytes().to_vec();

            {
                let mut writer = unique_tree.batch_writer(&mut mutable, 2);
                assert!(
                    writer
                        .batch_put_encoded(&[
                            UniqueDiskTreeEncodedPut {
                                key: &encoded_unique2,
                                row_id: 20,
                            },
                            UniqueDiskTreeEncodedPut {
                                key: &encoded_unique1,
                                row_id: 10,
                            },
                        ])
                        .as_ref()
                        .is_err_and(|err| err.is_kind(crate::error::ErrorKind::Internal)
                            && err
                                .report()
                                .downcast_ref::<crate::error::InternalError>()
                                .copied()
                                == Some(crate::error::InternalError::DiskTreeBatchOrderInvariant))
                );
            }
            let mut writer = unique_tree.batch_writer(&mut mutable, 2);
            writer
                .batch_put_encoded(&[
                    UniqueDiskTreeEncodedPut {
                        key: &encoded_unique1,
                        row_id: 10,
                    },
                    UniqueDiskTreeEncodedPut {
                        key: &encoded_unique2,
                        row_id: 20,
                    },
                ])
                .unwrap();
            let unique_root = writer.finish().await.unwrap();
            let unique_tree = unique_runtime.open(unique_root, &guard);
            let mut writer = unique_tree.batch_writer(&mut mutable, 3);
            writer
                .batch_conditional_delete_encoded(&[
                    UniqueDiskTreeEncodedDelete {
                        key: &encoded_unique1,
                        expected_old_row_id: 999,
                    },
                    UniqueDiskTreeEncodedDelete {
                        key: &encoded_unique1,
                        expected_old_row_id: 10,
                    },
                ])
                .unwrap();
            let unique_root = writer.finish().await.unwrap();
            let unique_tree = unique_runtime.open(unique_root, &guard);
            assert_eq!(unique_tree.lookup(&key1).await.unwrap(), None);
            assert_eq!(unique_tree.lookup(&key2).await.unwrap(), Some(20));

            let non_unique_runtime = non_unique_runtime!(metadata, disk_pool);
            let non_unique_tree = non_unique_runtime.open(SUPER_BLOCK_ID, &guard);
            let encoded_exact10 = non_unique_tree
                .encoder()
                .encode_pair(&key1, Val::from(10u64))
                .as_bytes()
                .to_vec();
            let encoded_exact11 = non_unique_tree
                .encoder()
                .encode_pair(&key1, Val::from(11u64))
                .as_bytes()
                .to_vec();
            let malformed_exact = [0u8; ROW_ID_SIZE - 1];
            {
                let mut writer = non_unique_tree.batch_writer(&mut mutable, 4);
                assert!(
                    writer
                        .batch_insert_encoded(&[NonUniqueDiskTreeEncodedExact {
                            key: &malformed_exact
                        }])
                        .as_ref()
                        .is_err_and(|err| err.data_integrity_error()
                            == Some(DataIntegrityError::InvalidPayload))
                );
            }
            {
                let mut writer = non_unique_tree.batch_writer(&mut mutable, 4);
                assert!(
                    writer
                        .batch_exact_delete_encoded(&[NonUniqueDiskTreeEncodedExact {
                            key: &malformed_exact
                        }])
                        .as_ref()
                        .is_err_and(|err| err.data_integrity_error()
                            == Some(DataIntegrityError::InvalidPayload))
                );
            }
            {
                let mut writer = non_unique_tree.batch_writer(&mut mutable, 4);
                assert!(
                    writer
                        .batch_insert_encoded(&[
                            NonUniqueDiskTreeEncodedExact {
                                key: &encoded_exact11
                            },
                            NonUniqueDiskTreeEncodedExact {
                                key: &encoded_exact10
                            },
                        ])
                        .as_ref()
                        .is_err_and(|err| err.is_kind(crate::error::ErrorKind::Internal)
                            && err
                                .report()
                                .downcast_ref::<crate::error::InternalError>()
                                .copied()
                                == Some(crate::error::InternalError::DiskTreeBatchOrderInvariant))
                );
            }
            let mut writer = non_unique_tree.batch_writer(&mut mutable, 4);
            writer
                .batch_insert_encoded(&[
                    NonUniqueDiskTreeEncodedExact {
                        key: &encoded_exact10,
                    },
                    NonUniqueDiskTreeEncodedExact {
                        key: &encoded_exact11,
                    },
                ])
                .unwrap();
            let non_unique_root = writer.finish().await.unwrap();
            let non_unique_tree = non_unique_runtime.open(non_unique_root, &guard);
            let mut writer = non_unique_tree.batch_writer(&mut mutable, 5);
            writer
                .batch_exact_delete_encoded(&[NonUniqueDiskTreeEncodedExact {
                    key: &encoded_exact10,
                }])
                .unwrap();
            let non_unique_root = writer.finish().await.unwrap();
            let non_unique_tree = non_unique_runtime.open(non_unique_root, &guard);
            assert_eq!(non_unique_tree.prefix_scan(&key1).await.unwrap(), vec![11]);
        });
    }

    #[test]
    fn test_unique_delete_all_entries_returns_empty_root() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(306, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 306, &table);
            let guard = disk_pool.pool_guard();
            let mut mutable =
                crate::file::table_file::MutableTableFile::fork(&table, fs.background_writes());
            let runtime = unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);
            let key1 = [Val::from(1u32)];
            let key2 = [Val::from(2u32)];
            let key3 = [Val::from(3u32)];
            let mut writer = tree.batch_writer(&mut mutable, 2);
            writer
                .batch_put(&[
                    UniqueDiskTreePut {
                        key: &key1,
                        row_id: 10,
                    },
                    UniqueDiskTreePut {
                        key: &key2,
                        row_id: 20,
                    },
                    UniqueDiskTreePut {
                        key: &key3,
                        row_id: 30,
                    },
                ])
                .unwrap();
            let root = writer.finish().await.unwrap();
            assert_ne!(root, SUPER_BLOCK_ID);

            let tree = runtime.open(root, &guard);
            let rows = tree
                .scan_entries()
                .await
                .unwrap()
                .into_iter()
                .map(|(_, row_id)| row_id)
                .collect::<Vec<_>>();
            assert_eq!(rows, vec![10, 20, 30]);

            let mut writer = tree.batch_writer(&mut mutable, 3);
            writer
                .batch_conditional_delete(&[
                    UniqueDiskTreeDelete {
                        key: &key1,
                        expected_old_row_id: 10,
                    },
                    UniqueDiskTreeDelete {
                        key: &key2,
                        expected_old_row_id: 20,
                    },
                    UniqueDiskTreeDelete {
                        key: &key3,
                        expected_old_row_id: 30,
                    },
                ])
                .unwrap();
            let empty_root = writer.finish().await.unwrap();
            assert_eq!(empty_root, SUPER_BLOCK_ID);

            let empty_tree = runtime.open(empty_root, &guard);
            assert_eq!(empty_tree.lookup(&key1).await.unwrap(), None);
            assert_eq!(empty_tree.lookup(&key2).await.unwrap(), None);
            assert_eq!(empty_tree.lookup(&key3).await.unwrap(), None);
            assert!(empty_tree.scan_entries().await.unwrap().is_empty());

            let rows = tree
                .scan_entries()
                .await
                .unwrap()
                .into_iter()
                .map(|(_, row_id)| row_id)
                .collect::<Vec<_>>();
            assert_eq!(rows, vec![10, 20, 30]);
            assert!(mutable.root().gc_block_list.is_empty());
        });
    }

    #[test]
    fn test_non_unique_delete_all_exact_entries_returns_empty_root() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(307, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 307, &table);
            let guard = disk_pool.pool_guard();
            let mut mutable =
                crate::file::table_file::MutableTableFile::fork(&table, fs.background_writes());
            let runtime = non_unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);
            let key1 = [Val::from(1u32)];
            let key2 = [Val::from(2u32)];
            let mut writer = tree.batch_writer(&mut mutable, 2);
            writer
                .batch_insert(&[
                    NonUniqueDiskTreeExact {
                        key: &key1,
                        row_id: 10,
                    },
                    NonUniqueDiskTreeExact {
                        key: &key1,
                        row_id: 11,
                    },
                    NonUniqueDiskTreeExact {
                        key: &key2,
                        row_id: 20,
                    },
                ])
                .unwrap();
            let root = writer.finish().await.unwrap();
            assert_ne!(root, SUPER_BLOCK_ID);

            let tree = runtime.open(root, &guard);
            assert_eq!(tree.prefix_scan(&key1).await.unwrap(), vec![10, 11]);
            assert_eq!(tree.prefix_scan(&key2).await.unwrap(), vec![20]);

            let mut writer = tree.batch_writer(&mut mutable, 3);
            writer
                .batch_exact_delete(&[
                    NonUniqueDiskTreeExact {
                        key: &key1,
                        row_id: 10,
                    },
                    NonUniqueDiskTreeExact {
                        key: &key1,
                        row_id: 11,
                    },
                    NonUniqueDiskTreeExact {
                        key: &key2,
                        row_id: 20,
                    },
                ])
                .unwrap();
            let empty_root = writer.finish().await.unwrap();
            assert_eq!(empty_root, SUPER_BLOCK_ID);

            let empty_tree = runtime.open(empty_root, &guard);
            assert!(!empty_tree.contains_exact(&key1, 10).await.unwrap());
            assert!(!empty_tree.contains_exact(&key1, 11).await.unwrap());
            assert!(!empty_tree.contains_exact(&key2, 20).await.unwrap());
            assert!(empty_tree.prefix_scan(&key1).await.unwrap().is_empty());
            assert!(empty_tree.prefix_scan(&key2).await.unwrap().is_empty());
            assert!(empty_tree.scan_entries().await.unwrap().is_empty());

            assert_eq!(tree.prefix_scan(&key1).await.unwrap(), vec![10, 11]);
            assert_eq!(tree.prefix_scan(&key2).await.unwrap(), vec![20]);
            assert!(mutable.root().gc_block_list.is_empty());
        });
    }

    #[test]
    fn test_unique_delete_sparse_remaining_entries_compacts_to_one_leaf() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(308, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 308, &table);
            let guard = disk_pool.pool_guard();
            let mut mutable =
                crate::file::table_file::MutableTableFile::fork(&table, fs.background_writes());
            let runtime = unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);

            const ENTRY_COUNT: u32 = 8192;
            const KEEP_EVERY: usize = 100;
            let keys = (0..ENTRY_COUNT)
                .map(|idx| [Val::from(idx)])
                .collect::<Vec<_>>();
            let puts = keys
                .iter()
                .enumerate()
                .map(|(idx, key)| UniqueDiskTreePut {
                    key,
                    row_id: idx as RowID + 100,
                })
                .collect::<Vec<_>>();
            let mut writer = tree.batch_writer(&mut mutable, 2);
            writer.batch_put(&puts).unwrap();
            let root = writer.finish().await.unwrap();
            assert_ne!(root, SUPER_BLOCK_ID);

            let tree = runtime.open(root, &guard);
            let root_guard = tree.read_node(root).await.unwrap();
            assert!(!root_guard.node().is_leaf());
            drop(root_guard);

            let allocated_before_delete = mutable.root().alloc_map.allocated();
            let deletes = keys
                .iter()
                .enumerate()
                .filter(|(idx, _)| idx % KEEP_EVERY != 0)
                .map(|(idx, key)| UniqueDiskTreeDelete {
                    key,
                    expected_old_row_id: idx as RowID + 100,
                })
                .collect::<Vec<_>>();
            let expected_rows = (0..ENTRY_COUNT as usize)
                .filter(|idx| idx % KEEP_EVERY == 0)
                .map(|idx| idx as RowID + 100)
                .collect::<Vec<_>>();

            let mut writer = tree.batch_writer(&mut mutable, 3);
            writer.batch_conditional_delete(&deletes).unwrap();
            let compacted_root = writer.finish().await.unwrap();
            assert_ne!(compacted_root, SUPER_BLOCK_ID);
            assert_eq!(
                mutable.root().alloc_map.allocated(),
                allocated_before_delete + 1
            );

            let compacted_tree = runtime.open(compacted_root, &guard);
            let compacted_guard = compacted_tree.read_node(compacted_root).await.unwrap();
            assert!(compacted_guard.node().is_leaf());
            assert!(compacted_guard.node().has_no_upper_fence());
            assert_eq!(compacted_guard.node().count(), expected_rows.len());
            drop(compacted_guard);

            let rows = compacted_tree
                .scan_entries()
                .await
                .unwrap()
                .into_iter()
                .map(|(_, row_id)| row_id)
                .collect::<Vec<_>>();
            assert_eq!(rows, expected_rows);
            assert_eq!(compacted_tree.lookup(&keys[1]).await.unwrap(), None);
            assert_eq!(
                tree.scan_entries().await.unwrap().len(),
                ENTRY_COUNT as usize
            );
            assert!(mutable.root().gc_block_list.is_empty());
        });
    }

    #[test]
    fn test_disk_tree_rewrite_absorbs_immediate_right_sibling_without_extra_writes() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_varbyte_unique_index();
            let table = fs
                .create_table_file(313, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 313, &table);
            let guard = disk_pool.pool_guard();
            let mut mutable =
                crate::file::table_file::MutableTableFile::fork(&table, fs.background_writes());
            let runtime = unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);

            // Long single-column keys make a small fixture produce multiple
            // leaves, keeping this test focused on rewrite behavior rather
            // than bulk-loading 10k short keys.
            const ENTRY_COUNT: u32 = 192;
            let keys = (0..ENTRY_COUNT)
                .map(|idx| [Val::from(long_varbyte_key(idx))])
                .collect::<Vec<_>>();
            let encoded_keys = keys
                .iter()
                .map(|key| tree.encoder().encode(key).as_bytes().to_vec())
                .collect::<Vec<_>>();
            let row_by_key = encoded_keys
                .iter()
                .enumerate()
                .map(|(idx, key)| (key.clone(), idx as RowID + 100))
                .collect::<std::collections::BTreeMap<_, _>>();
            let puts = keys
                .iter()
                .enumerate()
                .map(|(idx, key)| UniqueDiskTreePut {
                    key,
                    row_id: idx as RowID + 100,
                })
                .collect::<Vec<_>>();
            let mut writer = tree.batch_writer(&mut mutable, 2);
            writer.batch_put(&puts).unwrap();
            let root = writer.finish().await.unwrap();
            let tree = runtime.open(root, &guard);
            let root_guard = tree.read_node(root).await.unwrap();
            assert_eq!(root_guard.node().height(), 1);
            drop(root_guard);

            let leaves = collect_leaf_blocks(&tree).await.unwrap();
            assert!(
                leaves.len() >= 3,
                "expected at least three leaves, got {}",
                leaves.len()
            );
            let anchor_idx = leaves.len() - 2;
            assert!(leaves[anchor_idx].keys.len() > 1);
            let kept_anchor_key = leaves[anchor_idx].keys[0].clone();
            let right_block_id = leaves[anchor_idx + 1].block_id;
            let right_keys = leaves[anchor_idx + 1].keys.clone();
            let deletes = leaves[anchor_idx]
                .keys
                .iter()
                .skip(1)
                .map(|key| UniqueDiskTreeEncodedDelete {
                    key,
                    expected_old_row_id: *row_by_key.get(key).unwrap(),
                })
                .collect::<Vec<_>>();

            let allocated_before = mutable.root().alloc_map.allocated();
            let mut writer = tree.batch_writer(&mut mutable, 3);
            writer.batch_conditional_delete_encoded(&deletes).unwrap();
            let rewritten_root = writer.finish().await.unwrap();
            let allocated_delta = mutable.root().alloc_map.allocated() - allocated_before;

            let rewritten_tree = runtime.open(rewritten_root, &guard);
            let rewritten_leaves = collect_leaf_blocks(&rewritten_tree).await.unwrap();
            assert_eq!(rewritten_leaves.len(), leaves.len() - 1);
            assert!(
                !rewritten_leaves
                    .iter()
                    .any(|leaf| leaf.block_id == right_block_id),
                "absorbed right sibling should not remain linked by block id"
            );
            let expected_writes = if rewritten_leaves.len() == 1 { 1 } else { 2 };
            assert_eq!(allocated_delta, expected_writes);

            let rewritten_key_set = rewritten_leaves
                .iter()
                .flat_map(|leaf| leaf.keys.iter())
                .collect::<std::collections::BTreeSet<_>>();
            for key in &right_keys {
                assert!(
                    rewritten_key_set.contains(key),
                    "absorbed right sibling key should remain in the rewritten leaves"
                );
            }

            let first_right_key = right_keys.first().unwrap();
            let last_right_key = right_keys.last().unwrap();
            for key in [&kept_anchor_key, first_right_key, last_right_key] {
                assert_eq!(
                    rewritten_tree.lookup_encoded(key.as_slice()).await.unwrap(),
                    Some(*row_by_key.get(key).unwrap())
                );
            }
        });
    }

    #[test]
    fn test_disk_tree_rewrite_keeps_non_absorbable_right_sibling_unchanged() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(314, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 314, &table);
            let guard = disk_pool.pool_guard();
            let mut mutable =
                crate::file::table_file::MutableTableFile::fork(&table, fs.background_writes());
            let runtime = unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);

            const ENTRY_COUNT: u32 = 10_000;
            let keys = (0..ENTRY_COUNT)
                .map(|idx| [Val::from(idx)])
                .collect::<Vec<_>>();
            let encoded_keys = keys
                .iter()
                .map(|key| tree.encoder().encode(key).as_bytes().to_vec())
                .collect::<Vec<_>>();
            let row_by_key = encoded_keys
                .iter()
                .enumerate()
                .map(|(idx, key)| (key.clone(), idx as RowID + 100))
                .collect::<std::collections::BTreeMap<_, _>>();
            let puts = keys
                .iter()
                .enumerate()
                .map(|(idx, key)| UniqueDiskTreePut {
                    key,
                    row_id: idx as RowID + 100,
                })
                .collect::<Vec<_>>();
            let mut writer = tree.batch_writer(&mut mutable, 2);
            writer.batch_put(&puts).unwrap();
            let root = writer.finish().await.unwrap();
            let tree = runtime.open(root, &guard);
            let root_guard = tree.read_node(root).await.unwrap();
            assert_eq!(root_guard.node().height(), 1);
            drop(root_guard);

            let leaves = collect_leaf_blocks(&tree).await.unwrap();
            assert!(
                leaves.len() >= 3,
                "expected at least three leaves, got {}",
                leaves.len()
            );
            let right_block_id = leaves[1].block_id;
            let deleted_key = leaves[0].keys[0].clone();
            let deletes = [UniqueDiskTreeEncodedDelete {
                key: deleted_key.as_slice(),
                expected_old_row_id: *row_by_key.get(&deleted_key).unwrap(),
            }];

            let allocated_before = mutable.root().alloc_map.allocated();
            let mut writer = tree.batch_writer(&mut mutable, 3);
            writer.batch_conditional_delete_encoded(&deletes).unwrap();
            let rewritten_root = writer.finish().await.unwrap();
            let allocated_delta = mutable.root().alloc_map.allocated() - allocated_before;

            let rewritten_tree = runtime.open(rewritten_root, &guard);
            let rewritten_leaves = collect_leaf_blocks(&rewritten_tree).await.unwrap();
            assert_ne!(rewritten_leaves[0].block_id, leaves[0].block_id);
            assert_eq!(
                rewritten_leaves[0].upper_fence,
                Some(leaves[1].keys[0].clone())
            );
            assert!(
                rewritten_leaves[0].common_prefix_len > 0,
                "bounded rewritten leaf should use the right sibling key as its upper fence"
            );
            assert!(
                rewritten_leaves
                    .iter()
                    .any(|leaf| leaf.block_id == right_block_id),
                "right sibling that cannot fit in the pending write budget must remain unchanged"
            );
            assert_eq!(allocated_delta, 2);
            assert_eq!(
                rewritten_tree.lookup_encoded(&deleted_key).await.unwrap(),
                None
            );
        });
    }

    #[test]
    fn test_disk_tree_batches_reject_unsorted_duplicates() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(305, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 305, &table);
            let guard = disk_pool.pool_guard();
            let mut mutable =
                crate::file::table_file::MutableTableFile::fork(&table, fs.background_writes());
            let runtime = unique_runtime!(metadata, disk_pool);
            let tree = runtime.open(SUPER_BLOCK_ID, &guard);
            let key1 = [Val::from(1u32)];
            let key2 = [Val::from(2u32)];
            let mut writer = tree.batch_writer(&mut mutable, 2);
            let err = writer
                .batch_put(&[
                    UniqueDiskTreePut {
                        key: &key2,
                        row_id: 20,
                    },
                    UniqueDiskTreePut {
                        key: &key1,
                        row_id: 10,
                    },
                ])
                .unwrap_err();
            assert!(err.is_kind(crate::error::ErrorKind::Internal));
            assert_eq!(
                err.report()
                    .downcast_ref::<crate::error::InternalError>()
                    .copied(),
                Some(crate::error::InternalError::DiskTreeBatchOrderInvariant)
            );

            let err = writer
                .batch_put(&[
                    UniqueDiskTreePut {
                        key: &key1,
                        row_id: 10,
                    },
                    UniqueDiskTreePut {
                        key: &key1,
                        row_id: 11,
                    },
                ])
                .unwrap_err();
            assert!(err.is_kind(crate::error::ErrorKind::Internal));
            assert_eq!(
                err.report()
                    .downcast_ref::<crate::error::InternalError>()
                    .copied(),
                Some(crate::error::InternalError::DiskTreeBatchOrderInvariant)
            );
        });
    }

    #[test]
    fn test_disk_tree_block_checksum_trailer_rejects_corruption() {
        let mut buf = DirectBuf::zeroed(DISK_TREE_BLOCK_SIZE);
        {
            let node = btree_node_from_block_mut(buf.data_mut()).unwrap();
            node.init(0, 1, b"a", BTreeU64::INVALID_VALUE, &[], false);
            node.insert_at::<BTreeU64>(0, b"a", BTreeU64::from(7));
        }
        write_block_checksum(buf.data_mut());

        validate_disk_tree_block::<UniqueDiskTreeSpec>(
            buf.data(),
            FileKind::TableFile,
            BlockID::from(1u64),
        )
        .unwrap();

        let mut payload_corrupted = buf.data().to_vec();
        payload_corrupted[0] ^= 0xff;
        let err = validate_disk_tree_block::<UniqueDiskTreeSpec>(
            &payload_corrupted,
            FileKind::TableFile,
            BlockID::from(1u64),
        )
        .unwrap_err();
        assert_disk_tree_corruption(err, DataIntegrityError::ChecksumMismatch);

        let mut footer_corrupted = buf.data().to_vec();
        let checksum_idx = crate::file::block_integrity::checksum_offset(footer_corrupted.len());
        footer_corrupted[checksum_idx] ^= 0xff;
        let err = validate_disk_tree_block::<UniqueDiskTreeSpec>(
            &footer_corrupted,
            FileKind::TableFile,
            BlockID::from(1u64),
        )
        .unwrap_err();
        assert_disk_tree_corruption(err, DataIntegrityError::ChecksumMismatch);
    }
}
