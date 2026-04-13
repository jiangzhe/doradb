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

#![allow(dead_code)]

use crate::buffer::{PoolGuard, ReadonlyBlockGuard, ReadonlyBufferPool};
use crate::catalog::{IndexSpec, TableMetadata};
use crate::error::{BlockCorruptionCause, BlockKind, Error, FileKind, Result};
use crate::file::SparseFile;
use crate::file::block_integrity::{validate_block_checksum, write_block_checksum};
use crate::file::cow_file::{BlockID, COW_FILE_PAGE_SIZE, MutableCowFile, SUPER_BLOCK_ID};
use crate::index::btree_key::BTreeKeyEncoder;
use crate::index::btree_node::{BTreeNode, LookupChild};
use crate::index::btree_value::{BTreeNil, BTreeU64, BTreeValue, BTreeValuePackable};
use crate::index::util::Maskable;
use crate::io::DirectBuf;
use crate::quiescent::QuiescentGuard;
use crate::row::RowID;
use crate::value::{Val, ValKind, ValType};
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

/// One unique DiskTree batch insertion item.
pub(crate) struct UniqueDiskTreePut<'a> {
    /// Logical secondary key values.
    pub key: &'a [Val],
    /// Latest checkpointed owner row id.
    pub row_id: RowID,
}

/// One unique DiskTree conditional delete item.
pub(crate) struct UniqueDiskTreeDelete<'a> {
    /// Logical secondary key values.
    pub key: &'a [Val],
    /// Owner row id that must still match before the mapping is removed.
    pub expected_old_row_id: RowID,
}

/// One non-unique DiskTree exact-entry item.
pub(crate) struct NonUniqueDiskTreeExact<'a> {
    /// Logical secondary key values.
    pub key: &'a [Val],
    /// Exact entry row id suffix.
    pub row_id: RowID,
}

/// Normalized logical entry used by the shared rewrite engine.
///
/// Unique entries carry a row-id owner. Non-unique entries store only the
/// encoded exact key, including the row-id suffix.
#[derive(Clone, Debug, PartialEq, Eq)]
struct LogicalEntry {
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

/// Lower-fence key and block id for one child subtree.
///
/// Branch blocks encode the first child in the lower fence and subsequent
/// children as slot key/value pairs. This flattened form lets rewrites split and
/// rebuild branch levels without exposing that layout to callers.
#[derive(Clone, Debug, PartialEq, Eq)]
struct BranchEntry {
    key: Vec<u8>,
    block_id: BlockID,
}

/// Result of rewriting one subtree.
///
/// A rewrite can collapse, preserve, or split a subtree. The parent only needs
/// the replacement child entries that should be linked into the next level.
#[derive(Clone, Debug)]
struct NodeRewriteResult {
    entries: Vec<BranchEntry>,
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
struct DiskTreeOperation {
    key: Vec<u8>,
    kind: DiskTreeOperationKind,
}

/// Specializes the shared DiskTree engine for unique and non-unique trees.
///
/// The storage and rewrite mechanics are identical: leaves contain encoded keys,
/// branches contain child block ids, and blocks are validated as `BTreeNode`
/// images. This trait defines the leaf value type and how logical entries are
/// interpreted for each durable secondary-index contract.
trait DiskTreeSpec: Copy + 'static {
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
struct UniqueDiskTreeSpec;

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
        let row_id = entry.row_id.ok_or(Error::InvalidArgument)?;
        if row_id.is_deleted() {
            return Err(Error::InvalidArgument);
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
            let row_id = entry.row_id.ok_or(Error::InvalidFormat)?;
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
                    return Err(Error::InvalidArgument);
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
struct NonUniqueDiskTreeSpec;

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
            return Err(Error::InvalidArgument);
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
                return Err(Error::InvalidFormat);
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
                DiskTreeOperationKind::Unique(_) => return Err(Error::InvalidArgument),
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
    Error::block_corrupted(
        file_kind,
        BlockKind::SecondaryDiskTree,
        block_id,
        BlockCorruptionCause::InvalidPayload,
    )
}

#[inline]
fn corrupted_block(file_kind: FileKind, block_id: BlockID, cause: BlockCorruptionCause) -> Error {
    Error::block_corrupted(file_kind, BlockKind::SecondaryDiskTree, block_id, cause)
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
    bytemuck::try_from_bytes_mut::<BTreeNode>(block).map_err(|_| Error::InternalError)
}

#[inline]
fn validate_checksum(block: &[u8], file_kind: FileKind, block_id: BlockID) -> Result<()> {
    if block.len() != DISK_TREE_BLOCK_SIZE {
        return Err(corrupted_block(
            file_kind,
            block_id,
            BlockCorruptionCause::InvalidPayload,
        ));
    }
    validate_block_checksum(block).map_err(|cause| corrupted_block(file_kind, block_id, cause))?;
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
        return Err(Error::InvalidArgument);
    }
    let mut types = Vec::with_capacity(index_spec.index_cols.len() + usize::from(append_row_id));
    for key in &index_spec.index_cols {
        let col_no = key.col_no as usize;
        let ty = metadata
            .col_types()
            .get(col_no)
            .copied()
            .ok_or(Error::InvalidArgument)?;
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
            return Err(Error::InvalidArgument);
        }
        prev = Some(key);
    }
    Ok(())
}

/// Extract the row-id suffix from a non-unique exact key.
#[inline]
fn unpack_row_id_from_exact_key(key: &[u8]) -> Result<RowID> {
    if key.len() < ROW_ID_SIZE {
        return Err(Error::InvalidFormat);
    }
    Ok(BTreeU64::unpack(&key[key.len() - ROW_ID_SIZE..]).to_u64())
}

/// Shared root-snapshot view over one persisted DiskTree.
///
/// The view is immutable: reads use the readonly buffer pool, while writes build
/// replacement CoW blocks through a mutable table-file fork and return a new root
/// block id for the caller to publish later.
struct DiskTree<'a, F: DiskTreeSpec> {
    root_block_id: BlockID,
    file_kind: FileKind,
    file: &'a Arc<SparseFile>,
    disk_pool: &'a QuiescentGuard<ReadonlyBufferPool>,
    disk_pool_guard: &'a PoolGuard,
    encoder: BTreeKeyEncoder,
    _marker: PhantomData<F>,
}

impl<'a, F: DiskTreeSpec> DiskTree<'a, F> {
    /// Create a typed view over one root snapshot.
    ///
    /// `SUPER_BLOCK_ID` is accepted here and interpreted by read/write paths as
    /// the empty tree sentinel.
    #[inline]
    fn new(
        root_block_id: BlockID,
        encoder: BTreeKeyEncoder,
        file_kind: FileKind,
        file: &'a Arc<SparseFile>,
        disk_pool: &'a QuiescentGuard<ReadonlyBufferPool>,
        disk_pool_guard: &'a PoolGuard,
    ) -> Self {
        Self {
            root_block_id,
            file_kind,
            file,
            disk_pool,
            disk_pool_guard,
            encoder,
            _marker: PhantomData,
        }
    }

    /// Read and validate one persisted block as a DiskTree node.
    ///
    /// The returned guard owns the readonly-buffer reference, so callers can use
    /// the no-copy node view without copying entries out of the block.
    #[inline]
    async fn read_node(&self, block_id: BlockID) -> Result<ValidatedDiskTreeNode<F>> {
        let guard = self
            .disk_pool
            .read_validated_block(
                self.file_kind,
                self.file,
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
    async fn lookup_encoded(&self, key: &[u8]) -> Result<Option<LogicalEntry>> {
        if self.root_block_id == SUPER_BLOCK_ID {
            return Ok(None);
        }
        let mut block_id = self.root_block_id;
        loop {
            let guard = self.read_node(block_id).await?;
            let node = guard.node();
            if node.is_leaf() {
                return match node.search_key(key) {
                    Ok(idx) => Ok(Some(F::leaf_entry(node, idx, self.file_kind, block_id)?)),
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
                    let entry = F::leaf_entry(node, idx, self.file_kind, block_id)?;
                    if entries.last().is_some_and(|prev| prev.key >= entry.key) {
                        return Err(invalid_node_payload(self.file_kind, block_id));
                    }
                    entries.push(entry);
                }
            } else {
                let branch_entries = branch_entries_from_node(node, self.file_kind, block_id)?;
                for entry in branch_entries.into_iter().rev() {
                    stack.push(entry.block_id);
                }
            }
        }
        Ok(entries)
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
                return Ok(self.root_block_id);
            }
            if self.root_block_id == SUPER_BLOCK_ID {
                let entries = F::apply_operations(&[], operations)?;
                return self
                    .build_tree_from_entries(mutable_file, &entries, create_ts)
                    .await;
            }
            let root = self.read_node(self.root_block_id).await?;
            let root_height = root.node().height() as u16;
            drop(root);
            let res = self
                .rewrite_subtree(mutable_file, self.root_block_id, operations, create_ts)
                .await?;
            self.finalize_root_rewrite(mutable_file, root_height, res.entries, create_ts)
                .await
        })
    }

    /// Rewrite one subtree that intersects a sorted operation range.
    ///
    /// Leaf rewrites materialize the leaf entries, apply spec-specific logical
    /// operations, and write replacement leaves. Branch rewrites route operations
    /// into affected children and rebuild the branch level from child results.
    fn rewrite_subtree<'b, M: MutableCowFile + 'b>(
        &'b self,
        mutable_file: &'b mut M,
        block_id: BlockID,
        operations: &'b [DiskTreeOperation],
        create_ts: u64,
    ) -> Pin<Box<dyn Future<Output = Result<NodeRewriteResult>> + 'b>> {
        Box::pin(async move {
            let guard = self.read_node(block_id).await?;
            let node = guard.node();
            if node.is_leaf() {
                let mut entries = Vec::with_capacity(node.count());
                for idx in 0..node.count() {
                    entries.push(F::leaf_entry(node, idx, self.file_kind, block_id)?);
                }
                let entries = F::apply_operations(&entries, operations)?;
                let new_entries = self
                    .write_leaf_blocks_from_entries(mutable_file, &entries, create_ts)
                    .await?;
                return Ok(NodeRewriteResult {
                    entries: new_entries,
                });
            }
            let entries = branch_entries_from_node(node, self.file_kind, block_id)?;
            drop(guard);
            self.rewrite_branch(entries, mutable_file, operations, create_ts)
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
    ) -> Result<NodeRewriteResult> {
        let mut combined = Vec::with_capacity(old_entries.len() + operations.len());
        let mut op_idx = 0usize;
        for (child_idx, entry) in old_entries.iter().enumerate() {
            let start_idx = op_idx;
            let upper = old_entries
                .get(child_idx + 1)
                .map(|next| next.key.as_slice());
            while op_idx < operations.len()
                && upper.is_none_or(|upper| operations[op_idx].key.as_slice() < upper)
            {
                op_idx += 1;
            }
            if start_idx == op_idx {
                combined.push(entry.clone());
                continue;
            }
            let child = self
                .rewrite_subtree(
                    mutable_file,
                    entry.block_id,
                    &operations[start_idx..op_idx],
                    create_ts,
                )
                .await?;
            combined.extend(child.entries);
        }
        if op_idx != operations.len() {
            return Err(Error::InvalidArgument);
        }
        let height = if combined.is_empty() {
            1
        } else {
            self.read_node(combined[0].block_id).await?.node().height() as u16 + 1
        };
        let entries = self
            .write_branch_blocks(mutable_file, &combined, height, create_ts)
            .await?;
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
        let leaf_entries = self
            .write_leaf_blocks_from_entries(mutable_file, entries, create_ts)
            .await?;
        self.finalize_root_rewrite(mutable_file, 0, leaf_entries, create_ts)
            .await
    }

    /// Convert rewritten child entries into the final root block id.
    ///
    /// Empty entries become `SUPER_BLOCK_ID`, a single child is promoted as the
    /// root, and larger sets are wrapped in new branch levels.
    async fn finalize_root_rewrite<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        old_root_height: u16,
        entries: Vec<BranchEntry>,
        create_ts: u64,
    ) -> Result<BlockID> {
        if entries.is_empty() {
            return Ok(SUPER_BLOCK_ID);
        }
        if entries.len() == 1 {
            return Ok(entries[0].block_id);
        }
        self.build_branch_levels(mutable_file, entries, old_root_height + 1, create_ts)
            .await
    }

    /// Build branch levels until the tree has a single root block.
    async fn build_branch_levels<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        mut entries: Vec<BranchEntry>,
        mut height: u16,
        create_ts: u64,
    ) -> Result<BlockID> {
        loop {
            if entries.is_empty() {
                return Ok(SUPER_BLOCK_ID);
            }
            if entries.len() == 1 {
                return Ok(entries[0].block_id);
            }
            entries = self
                .write_branch_blocks(mutable_file, &entries, height, create_ts)
                .await?;
            height += 1;
        }
    }

    /// Write sorted logical entries into one or more leaf blocks.
    ///
    /// Each leaf stores as many entries as fit in the shared `BTreeNode` layout
    /// and returns one `BranchEntry` keyed by that leaf's lower fence.
    async fn write_leaf_blocks_from_entries<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[LogicalEntry],
        create_ts: u64,
    ) -> Result<Vec<BranchEntry>> {
        let mut branch_entries = Vec::new();
        let mut start = 0usize;
        while start < entries.len() {
            let mut buf = DirectBuf::zeroed(DISK_TREE_BLOCK_SIZE);
            let end = {
                let node = btree_node_from_block_mut(buf.data_mut())?;
                node.init(
                    0,
                    create_ts,
                    &entries[start].key,
                    BTreeU64::INVALID_VALUE,
                    &[],
                    true,
                );
                let mut end = start;
                while end < entries.len() {
                    if !node.can_insert::<F::LeafValue>(&entries[end].key) {
                        break;
                    }
                    let idx = node.count();
                    node.insert_at::<F::LeafValue>(
                        idx,
                        &entries[end].key,
                        F::leaf_value(&entries[end])?,
                    );
                    end += 1;
                }
                node.update_hints();
                end
            };
            if end == start {
                return Err(Error::InvalidArgument);
            }
            let block_id = mutable_file.allocate_block_id()?;
            self.write_node_block(mutable_file, block_id, buf).await?;
            branch_entries.push(BranchEntry {
                key: entries[start].key.clone(),
                block_id,
            });
            start = end;
        }
        Ok(branch_entries)
    }

    /// Write flattened child entries into one or more branch blocks.
    ///
    /// The first child is stored as the lower fence; remaining children are slot
    /// entries whose values encode child block ids.
    async fn write_branch_blocks<M: MutableCowFile>(
        &self,
        mutable_file: &mut M,
        entries: &[BranchEntry],
        height: u16,
        create_ts: u64,
    ) -> Result<Vec<BranchEntry>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        if height == 0 {
            return Err(Error::InvalidArgument);
        }
        let mut parent_entries = Vec::new();
        let mut start = 0usize;
        while start < entries.len() {
            let first = &entries[start];
            if first.block_id == SUPER_BLOCK_ID {
                return Err(Error::InvalidArgument);
            }
            let mut buf = DirectBuf::zeroed(DISK_TREE_BLOCK_SIZE);
            let end = {
                let node = btree_node_from_block_mut(buf.data_mut())?;
                node.init(
                    height,
                    create_ts,
                    &first.key,
                    BTreeU64::from(first.block_id.as_u64()),
                    &[],
                    true,
                );
                let mut end = start + 1;
                while end < entries.len() {
                    if entries[end].block_id == SUPER_BLOCK_ID {
                        return Err(Error::InvalidArgument);
                    }
                    if !node.can_insert::<BTreeU64>(&entries[end].key) {
                        break;
                    }
                    let idx = node.count();
                    node.insert_at::<BTreeU64>(
                        idx,
                        &entries[end].key,
                        BTreeU64::from(entries[end].block_id.as_u64()),
                    );
                    end += 1;
                }
                node.update_hints();
                end
            };
            if end == start + 1 && end < entries.len() {
                return Err(Error::InvalidArgument);
            }
            let block_id = mutable_file.allocate_block_id()?;
            self.write_node_block(mutable_file, block_id, buf).await?;
            parent_entries.push(BranchEntry {
                key: first.key.clone(),
                block_id,
            });
            start = end;
        }
        Ok(parent_entries)
    }

    /// Finalize checksum and persist one fully formed DiskTree node block.
    async fn write_node_block<M: MutableCowFile>(
        &self,
        mutable_file: &M,
        block_id: BlockID,
        mut buf: DirectBuf,
    ) -> Result<()> {
        write_block_checksum(buf.data_mut());
        mutable_file.write_block(block_id, buf).await
    }
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
    let mut entries = Vec::with_capacity(node.count() + 1);
    entries.push(BranchEntry {
        key: node.lower_fence_key().as_bytes().to_vec(),
        block_id: BlockID::from(node.lower_fence_value().to_u64()),
    });
    for idx in 0..node.count() {
        let key = node
            .key_checked(idx)
            .ok_or_else(|| invalid_node_payload(file_kind, block_id))?;
        entries.push(BranchEntry {
            key,
            block_id: BlockID::from(node.value::<BTreeU64>(idx).to_u64()),
        });
    }
    Ok(entries)
}

/// Root-snapshot view for a persisted unique secondary index.
///
/// Lookups read the immutable checkpoint root supplied at construction time.
/// Mutations are staged through `UniqueDiskTreeBatchWriter` and return a
/// replacement root block id for the caller to publish.
pub(crate) struct UniqueDiskTree<'a> {
    tree: DiskTree<'a, UniqueDiskTreeSpec>,
}

impl<'a> UniqueDiskTree<'a> {
    /// Create a unique DiskTree view for one checkpoint root.
    ///
    /// The index spec must be unique. `SUPER_BLOCK_ID` is accepted as the empty
    /// root sentinel and does not trigger any block read during construction.
    #[inline]
    pub(crate) fn new(
        root_block_id: BlockID,
        index_spec: &IndexSpec,
        metadata: &TableMetadata,
        file_kind: FileKind,
        file: &'a Arc<SparseFile>,
        disk_pool: &'a QuiescentGuard<ReadonlyBufferPool>,
        disk_pool_guard: &'a PoolGuard,
    ) -> Result<Self> {
        if !index_spec.unique() {
            return Err(Error::InvalidArgument);
        }
        let encoder = BTreeKeyEncoder::new(index_key_types(metadata, index_spec, false)?);
        Ok(Self {
            tree: DiskTree::new(
                root_block_id,
                encoder,
                file_kind,
                file,
                disk_pool,
                disk_pool_guard,
            ),
        })
    }

    /// Look up one logical key and return its checkpointed owner row id.
    ///
    /// The lookup is exact and does not consult mutable in-memory index state or
    /// MVCC visibility; callers are expected to merge this cold-layer answer
    /// with newer index layers when serving user transactions.
    #[inline]
    pub(crate) async fn lookup(&self, key: &[Val]) -> Result<Option<RowID>> {
        let key = self.tree.encoder.encode(key);
        match self.tree.lookup_encoded(key.as_bytes()).await? {
            Some(entry) => Ok(Some(entry.row_id.ok_or(Error::InvalidFormat)?)),
            None => Ok(None),
        }
    }

    /// Scan encoded logical keys and row ids in durable key order.
    #[inline]
    pub(crate) async fn scan_entries(&self) -> Result<Vec<(Vec<u8>, RowID)>> {
        self.tree
            .collect_entries()
            .await?
            .into_iter()
            .map(|entry| Ok((entry.key, entry.row_id.ok_or(Error::InvalidFormat)?)))
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
pub(crate) struct NonUniqueDiskTree<'a> {
    tree: DiskTree<'a, NonUniqueDiskTreeSpec>,
}

impl<'a> NonUniqueDiskTree<'a> {
    /// Create a non-unique DiskTree view for one checkpoint root.
    ///
    /// The index spec must be non-unique. The encoder appends `RowID` to the
    /// physical key type list so exact entries sort by logical key, then row id.
    #[inline]
    pub(crate) fn new(
        root_block_id: BlockID,
        index_spec: &IndexSpec,
        metadata: &TableMetadata,
        file_kind: FileKind,
        file: &'a Arc<SparseFile>,
        disk_pool: &'a QuiescentGuard<ReadonlyBufferPool>,
        disk_pool_guard: &'a PoolGuard,
    ) -> Result<Self> {
        if index_spec.unique() {
            return Err(Error::InvalidArgument);
        }
        let encoder = BTreeKeyEncoder::new(index_key_types(metadata, index_spec, true)?);
        Ok(Self {
            tree: DiskTree::new(
                root_block_id,
                encoder,
                file_kind,
                file,
                disk_pool,
                disk_pool_guard,
            ),
        })
    }

    /// Return whether one exact `(logical_key, row_id)` entry exists.
    #[inline]
    pub(crate) async fn contains_exact(&self, key: &[Val], row_id: RowID) -> Result<bool> {
        let key = self.tree.encoder.encode_pair(key, Val::from(row_id));
        Ok(self.tree.lookup_encoded(key.as_bytes()).await?.is_some())
    }

    /// Prefix-scan one logical key and return row ids in exact-key order.
    ///
    /// This reads all entries in Phase 1 and filters by encoded logical-key
    /// prefix; later range-scan support can route directly through leaf ranges.
    #[inline]
    pub(crate) async fn prefix_scan(&self, key: &[Val]) -> Result<Vec<RowID>> {
        let prefix = self.tree.encoder.encode_prefix(key, Some(ROW_ID_SIZE));
        let mut row_ids = Vec::new();
        for entry in self.tree.collect_entries().await? {
            if entry.key.starts_with(prefix.as_bytes()) {
                row_ids.push(unpack_row_id_from_exact_key(&entry.key)?);
            }
        }
        Ok(row_ids)
    }

    /// Scan encoded exact keys and row ids in durable exact-key order.
    #[inline]
    pub(crate) async fn scan_entries(&self) -> Result<Vec<(Vec<u8>, RowID)>> {
        self.tree
            .collect_entries()
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
    /// Add sorted logical-key put work to this writer.
    ///
    /// Input entries must be strictly sorted by encoded key. Later operations on
    /// the same key are preserved after earlier operations already staged here.
    pub(crate) fn batch_put(&mut self, entries: &[UniqueDiskTreePut<'_>]) -> Result<()> {
        let mut encoded = Vec::with_capacity(entries.len());
        for entry in entries {
            encoded.push((
                self.tree.tree.encoder.encode(entry.key).as_bytes().to_vec(),
                entry.row_id,
            ));
        }
        validate_sorted_unique_keys(encoded.iter().map(|(key, _)| key.as_slice()))?;
        for (key, row_id) in encoded {
            self.operations
                .entry(key)
                .or_default()
                .push(UniqueDiskTreeOp::Put(row_id));
        }
        Ok(())
    }

    /// Add sorted logical-key conditional delete work to this writer.
    ///
    /// A delete removes the durable mapping only when the stored row id still
    /// matches `expected_old_row_id`; missing or changed owners are left intact.
    pub(crate) fn batch_conditional_delete(
        &mut self,
        entries: &[UniqueDiskTreeDelete<'_>],
    ) -> Result<()> {
        let mut encoded = Vec::with_capacity(entries.len());
        for entry in entries {
            encoded.push((
                self.tree.tree.encoder.encode(entry.key).as_bytes().to_vec(),
                entry.expected_old_row_id,
            ));
        }
        validate_sorted_unique_keys(encoded.iter().map(|(key, _)| key.as_slice()))?;
        for (key, row_id) in encoded {
            self.operations
                .entry(key)
                .or_default()
                .push(UniqueDiskTreeOp::ConditionalDelete(row_id));
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
        tree.tree
            .rewrite_root(mutable_file, &flattened, create_ts)
            .await
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
    /// Add sorted exact-entry insert work to this writer.
    pub(crate) fn batch_insert(&mut self, entries: &[NonUniqueDiskTreeExact<'_>]) -> Result<()> {
        let encoded = self.encode_exact_batch(entries)?;
        for key in encoded {
            self.operations.insert(key, true);
        }
        Ok(())
    }

    /// Add sorted exact-entry delete work to this writer.
    pub(crate) fn batch_exact_delete(
        &mut self,
        entries: &[NonUniqueDiskTreeExact<'_>],
    ) -> Result<()> {
        let encoded = self.encode_exact_batch(entries)?;
        for key in encoded {
            self.operations.insert(key, false);
        }
        Ok(())
    }

    /// Encode and validate a sorted batch of logical exact entries.
    #[inline]
    fn encode_exact_batch(&self, entries: &[NonUniqueDiskTreeExact<'_>]) -> Result<Vec<Vec<u8>>> {
        let mut encoded = Vec::with_capacity(entries.len());
        for entry in entries {
            encoded.push(
                self.tree
                    .tree
                    .encoder
                    .encode_pair(entry.key, Val::from(entry.row_id))
                    .as_bytes()
                    .to_vec(),
            );
        }
        validate_sorted_unique_keys(encoded.iter().map(Vec::as_slice))?;
        Ok(encoded)
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
        tree.tree
            .rewrite_root(mutable_file, &flattened, create_ts)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{global_readonly_pool_scope, table_readonly_pool};
    use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey};
    use crate::file::build_test_fs;
    use crate::value::ValKind;

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
            let tree = UniqueDiskTree::new(
                SUPER_BLOCK_ID,
                &metadata.index_specs[0],
                &metadata,
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &guard,
            )
            .unwrap();
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
            let tree = NonUniqueDiskTree::new(
                SUPER_BLOCK_ID,
                &metadata.index_specs[1],
                &metadata,
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &guard,
            )
            .unwrap();
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
            let tree = UniqueDiskTree::new(
                SUPER_BLOCK_ID,
                &metadata.index_specs[0],
                &metadata,
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &guard,
            )
            .unwrap();
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

            let tree = UniqueDiskTree::new(
                root,
                &metadata.index_specs[0],
                &metadata,
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &guard,
            )
            .unwrap();
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
            let new_tree = UniqueDiskTree::new(
                new_root,
                &metadata.index_specs[0],
                &metadata,
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &guard,
            )
            .unwrap();
            assert_eq!(new_tree.lookup(&key1).await.unwrap(), Some(10));
            assert_eq!(new_tree.lookup(&key2).await.unwrap(), None);
            assert_eq!(tree.lookup(&key2).await.unwrap(), Some(20));
            assert!(mutable.root().gc_block_list.is_empty());
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
            let tree = NonUniqueDiskTree::new(
                SUPER_BLOCK_ID,
                &metadata.index_specs[1],
                &metadata,
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &guard,
            )
            .unwrap();
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
            let tree = NonUniqueDiskTree::new(
                root,
                &metadata.index_specs[1],
                &metadata,
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &guard,
            )
            .unwrap();
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
            let new_tree = NonUniqueDiskTree::new(
                new_root,
                &metadata.index_specs[1],
                &metadata,
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &guard,
            )
            .unwrap();
            assert_eq!(new_tree.prefix_scan(&key1).await.unwrap(), vec![11, 12]);
            assert!(!new_tree.contains_exact(&key1, 10).await.unwrap());
            assert_eq!(tree.prefix_scan(&key1).await.unwrap(), vec![10, 11]);
            assert!(mutable.root().gc_block_list.is_empty());
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
            let tree = UniqueDiskTree::new(
                SUPER_BLOCK_ID,
                &metadata.index_specs[0],
                &metadata,
                disk_pool.file_kind(),
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &guard,
            )
            .unwrap();
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
            assert!(matches!(err, Error::InvalidArgument));

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
            assert!(matches!(err, Error::InvalidArgument));
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
        assert!(matches!(
            err,
            Error::BlockCorrupted {
                cause: BlockCorruptionCause::ChecksumMismatch,
                ..
            }
        ));

        let mut footer_corrupted = buf.data().to_vec();
        let checksum_idx = crate::file::block_integrity::checksum_offset(footer_corrupted.len());
        footer_corrupted[checksum_idx] ^= 0xff;
        let err = validate_disk_tree_block::<UniqueDiskTreeSpec>(
            &footer_corrupted,
            FileKind::TableFile,
            BlockID::from(1u64),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error::BlockCorrupted {
                cause: BlockCorruptionCause::ChecksumMismatch,
                ..
            }
        ));
    }
}
