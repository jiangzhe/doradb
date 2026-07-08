//! Secondary-index scan stream plumbing.
//!
//! MemIndex and DiskTree source streams share spec-driven cursors, while
//! composite streams merge hot/cold batches through secondary-index code.

use super::disk_tree::{
    DiskTreeEntry, DiskTreeLeaf, DiskTreeNodeCursor, DiskTreeSpec, NonUniqueDiskTreeSpec,
    UniqueDiskTreeSpec, invalid_node_payload, unpack_row_id_from_exact_key,
};
use super::mem_index::MemIndexEntry;
use crate::buffer::BufferPool;
use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::error::{Error, InternalError, Result};
use crate::id::RowID;
use crate::index::btree::{BTreeByte, BTreeNode, BTreeNodeCursor, BTreeU64, KeyRange};
use crate::index::util::Maskable;
use error_stack::Report;
use std::future::Future;
use std::marker::PhantomData;
use std::mem;

/// Async batch stream returned by indexes.
pub(crate) trait IndexBatchStream<T> {
    /// Return the next non-empty batch, or `None` when exhausted.
    fn next_batch(&mut self) -> impl Future<Output = Result<Option<Vec<T>>>>;
}

/// Leaf item returned by an index scan cursor.
pub(crate) trait IndexScanLeaf {
    /// Return the B-tree node carried by this leaf.
    fn node(&self) -> &BTreeNode;

    /// Return the first slot that can match the scan lower bound.
    fn start_slot_idx(&self, lower_seek_key: &[u8]) -> usize;

    /// Copy one slot key out of this leaf, preserving source-specific errors.
    fn key_checked(&self, slot_idx: usize) -> Result<Vec<u8>>;
}

impl IndexScanLeaf for PageSharedGuard<BTreeNode> {
    #[inline]
    fn node(&self) -> &BTreeNode {
        self.page()
    }

    #[inline]
    fn start_slot_idx(&self, lower_seek_key: &[u8]) -> usize {
        self.page().lower_bound_slot_idx(lower_seek_key)
    }

    #[inline]
    fn key_checked(&self, slot_idx: usize) -> Result<Vec<u8>> {
        self.node().key_checked(slot_idx).ok_or_else(|| {
            Error::from(
                Report::new(InternalError::IndexKeyMissing).attach(format!("slot_idx={slot_idx}")),
            )
        })
    }
}

impl<F: DiskTreeSpec> IndexScanLeaf for DiskTreeLeaf<F> {
    #[inline]
    fn node(&self) -> &BTreeNode {
        self.guard.node()
    }

    #[inline]
    fn start_slot_idx(&self, _lower_seek_key: &[u8]) -> usize {
        self.start_idx
    }

    #[inline]
    fn key_checked(&self, slot_idx: usize) -> Result<Vec<u8>> {
        self.node()
            .key_checked(slot_idx)
            .ok_or_else(|| invalid_node_payload(self.file_kind, self.block_id))
    }
}

/// Cursor adapter used by generic index scan streams.
pub(crate) trait IndexLeafCursor {
    /// Leaf item returned by this cursor.
    type Leaf: IndexScanLeaf;

    /// Seek to the first leaf that may contain `key`.
    fn seek(&mut self, key: &[u8]) -> impl Future<Output = Result<()>>;

    /// Return the next leaf item.
    fn next_leaf(&mut self) -> impl Future<Output = Result<Option<Self::Leaf>>>;
}

impl<'a, P: BufferPool> IndexLeafCursor for BTreeNodeCursor<'a, P> {
    type Leaf = PageSharedGuard<BTreeNode>;

    #[inline]
    async fn seek(&mut self, key: &[u8]) -> Result<()> {
        BTreeNodeCursor::seek(self, key).await
    }

    #[inline]
    async fn next_leaf(&mut self) -> Result<Option<Self::Leaf>> {
        BTreeNodeCursor::next(self).await
    }
}

impl<'a, F: DiskTreeSpec> IndexLeafCursor for DiskTreeNodeCursor<'a, F> {
    type Leaf = DiskTreeLeaf<F>;

    #[inline]
    async fn seek(&mut self, key: &[u8]) -> Result<()> {
        DiskTreeNodeCursor::seek(self, key).await
    }

    #[inline]
    async fn next_leaf(&mut self) -> Result<Option<Self::Leaf>> {
        DiskTreeNodeCursor::next_leaf(self).await
    }
}

/// Complete source and projection specification for an index scan stream.
pub(crate) trait IndexScanSpec {
    /// Cursor that produces leaf items for the stream.
    type Cursor: IndexLeafCursor<Leaf = Self::Leaf>;
    /// Leaf item consumed by the stream.
    type Leaf: IndexScanLeaf;
    /// Output item produced by this stream.
    type Output;

    /// Project one accepted slot into an output item.
    fn project(
        leaf: &Self::Leaf,
        slot_idx: usize,
        encoded_key: Vec<u8>,
    ) -> Result<Option<Self::Output>>;
}

/// Stream specification for encoded unique MemIndex entries.
pub(crate) struct UniqueMemIndexEntryScanSpec<'a, P: 'static> {
    _marker: PhantomData<&'a P>,
}

impl<'a, P: BufferPool> IndexScanSpec for UniqueMemIndexEntryScanSpec<'a, P> {
    type Cursor = BTreeNodeCursor<'a, P>;
    type Leaf = PageSharedGuard<BTreeNode>;
    type Output = MemIndexEntry;

    #[inline]
    fn project(
        leaf: &PageSharedGuard<BTreeNode>,
        slot_idx: usize,
        encoded_key: Vec<u8>,
    ) -> Result<Option<Self::Output>> {
        let value = leaf.node().value::<BTreeU64>(slot_idx);
        Ok(Some(MemIndexEntry {
            encoded_key,
            row_id: value.value().to_row_id(),
            deleted: value.is_deleted(),
        }))
    }
}

/// Stream specification for unique MemIndex row ids.
pub(crate) struct UniqueMemIndexRowIdScanSpec<'a, P: 'static> {
    _marker: PhantomData<&'a P>,
}

impl<'a, P: BufferPool> IndexScanSpec for UniqueMemIndexRowIdScanSpec<'a, P> {
    type Cursor = BTreeNodeCursor<'a, P>;
    type Leaf = PageSharedGuard<BTreeNode>;
    type Output = RowID;

    #[inline]
    fn project(
        leaf: &PageSharedGuard<BTreeNode>,
        slot_idx: usize,
        _encoded_key: Vec<u8>,
    ) -> Result<Option<Self::Output>> {
        let value = leaf.node().value::<BTreeU64>(slot_idx);
        Ok(Some(value.value().to_row_id()))
    }
}

/// Stream specification for encoded non-unique MemIndex exact entries.
pub(crate) struct NonUniqueMemIndexEntryScanSpec<'a, P: 'static> {
    _marker: PhantomData<&'a P>,
}

impl<'a, P: BufferPool> IndexScanSpec for NonUniqueMemIndexEntryScanSpec<'a, P> {
    type Cursor = BTreeNodeCursor<'a, P>;
    type Leaf = PageSharedGuard<BTreeNode>;
    type Output = MemIndexEntry;

    #[inline]
    fn project(
        leaf: &PageSharedGuard<BTreeNode>,
        slot_idx: usize,
        encoded_key: Vec<u8>,
    ) -> Result<Option<Self::Output>> {
        validate_non_unique_exact_key(&encoded_key, slot_idx)?;
        let node = leaf.node();
        let slot = node.slot(slot_idx);
        let value = node.value_for_slot::<BTreeByte>(slot);
        let row_id = node.unpack_value::<BTreeU64>(slot).to_row_id();
        Ok(Some(MemIndexEntry {
            encoded_key,
            row_id,
            deleted: value.is_deleted(),
        }))
    }
}

/// Stream specification for live non-unique MemIndex row ids.
pub(crate) struct NonUniqueMemIndexRowIdScanSpec<'a, P: 'static> {
    _marker: PhantomData<&'a P>,
}

impl<'a, P: BufferPool> IndexScanSpec for NonUniqueMemIndexRowIdScanSpec<'a, P> {
    type Cursor = BTreeNodeCursor<'a, P>;
    type Leaf = PageSharedGuard<BTreeNode>;
    type Output = RowID;

    #[inline]
    fn project(
        leaf: &PageSharedGuard<BTreeNode>,
        slot_idx: usize,
        encoded_key: Vec<u8>,
    ) -> Result<Option<Self::Output>> {
        validate_non_unique_exact_key(&encoded_key, slot_idx)?;
        let node = leaf.node();
        let slot = node.slot(slot_idx);
        let value = node.value_for_slot::<BTreeByte>(slot);
        if value.is_deleted() {
            return Ok(None);
        }
        Ok(Some(node.unpack_value::<BTreeU64>(slot).to_row_id()))
    }
}

#[inline]
fn validate_non_unique_exact_key(encoded_key: &[u8], slot_idx: usize) -> Result<()> {
    if encoded_key.len() < mem::size_of::<RowID>() {
        return Err(Report::new(InternalError::MemIndexKeyMalformed)
            .attach(format!(
                "slot_idx={slot_idx}, key_len={}",
                encoded_key.len()
            ))
            .into());
    }
    Ok(())
}

/// Stream specification for unique DiskTree entries.
pub(crate) struct UniqueDiskTreeEntryScanSpec<'a> {
    _marker: PhantomData<&'a UniqueDiskTreeSpec>,
}

impl<'a> IndexScanSpec for UniqueDiskTreeEntryScanSpec<'a> {
    type Cursor = DiskTreeNodeCursor<'a, UniqueDiskTreeSpec>;
    type Leaf = DiskTreeLeaf<UniqueDiskTreeSpec>;
    type Output = DiskTreeEntry;

    #[inline]
    fn project(
        leaf: &Self::Leaf,
        slot_idx: usize,
        encoded_key: Vec<u8>,
    ) -> Result<Option<Self::Output>> {
        let row_id = leaf.node().value::<BTreeU64>(slot_idx);
        if row_id.is_deleted() {
            return Err(invalid_node_payload(leaf.file_kind, leaf.block_id));
        }
        Ok(Some(DiskTreeEntry {
            encoded_key,
            row_id: row_id.to_row_id(),
        }))
    }
}

/// Stream specification for non-unique DiskTree exact entries.
pub(crate) struct NonUniqueDiskTreeEntryScanSpec<'a> {
    _marker: PhantomData<&'a NonUniqueDiskTreeSpec>,
}

impl<'a> IndexScanSpec for NonUniqueDiskTreeEntryScanSpec<'a> {
    type Cursor = DiskTreeNodeCursor<'a, NonUniqueDiskTreeSpec>;
    type Leaf = DiskTreeLeaf<NonUniqueDiskTreeSpec>;
    type Output = DiskTreeEntry;

    #[inline]
    fn project(
        _leaf: &Self::Leaf,
        _slot_idx: usize,
        encoded_key: Vec<u8>,
    ) -> Result<Option<Self::Output>> {
        let row_id = unpack_row_id_from_exact_key(&encoded_key)?;
        Ok(Some(DiskTreeEntry {
            encoded_key,
            row_id,
        }))
    }
}

/// Generic leaf-bounded stream over index slots.
pub(crate) struct IndexScanStream<S: IndexScanSpec> {
    cursor: S::Cursor,
    range: KeyRange,
    started: bool,
    exhausted: bool,
    _spec: PhantomData<S>,
}

impl<S> IndexScanStream<S>
where
    S: IndexScanSpec,
{
    /// Create an index scan stream over an encoded key range.
    #[inline]
    pub(super) fn new(cursor: S::Cursor, range: KeyRange) -> Self {
        Self {
            cursor,
            range,
            started: false,
            exhausted: false,
            _spec: PhantomData,
        }
    }

    /// Return the next leaf-bounded projected batch.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<Vec<S::Output>>> {
        if self.exhausted {
            return Ok(None);
        }
        if !self.started {
            self.cursor.seek(self.range.lower_seek_key()).await?;
            self.started = true;
        }
        while let Some(leaf) = self.cursor.next_leaf().await? {
            let node = leaf.node();
            let mut outputs = Vec::new();
            for idx in leaf.start_slot_idx(self.range.lower_seek_key())..node.count() {
                let encoded_key = leaf.key_checked(idx)?;
                if !self.range.lower_accepts(&encoded_key) {
                    continue;
                }
                if !self.range.upper_accepts(&encoded_key) {
                    self.exhausted = true;
                    break;
                }
                if let Some(output) = S::project(&leaf, idx, encoded_key)? {
                    outputs.push(output);
                }
            }
            if !outputs.is_empty() {
                return Ok(Some(outputs));
            }
            if self.exhausted {
                return Ok(None);
            }
        }
        self.exhausted = true;
        Ok(None)
    }
}

impl<S> IndexBatchStream<S::Output> for IndexScanStream<S>
where
    S: IndexScanSpec,
{
    #[inline]
    async fn next_batch(&mut self) -> Result<Option<Vec<S::Output>>> {
        IndexScanStream::next_batch(self).await
    }
}

/// Leaf-bounded stream of encoded unique MemIndex entries.
pub(crate) type UniqueMemIndexEntryStream<'a, P> =
    IndexScanStream<UniqueMemIndexEntryScanSpec<'a, P>>;

/// Row-id stream over unique MemIndex entries.
pub(crate) type UniqueMemIndexBatchStream<'a, P> =
    IndexScanStream<UniqueMemIndexRowIdScanSpec<'a, P>>;

/// Leaf-bounded stream of encoded non-unique MemIndex entries.
pub(crate) type NonUniqueMemIndexEntryStream<'a, P> =
    IndexScanStream<NonUniqueMemIndexEntryScanSpec<'a, P>>;

/// Row-id stream over live non-unique MemIndex entries.
pub(crate) type NonUniqueMemIndexBatchStream<'a, P> =
    IndexScanStream<NonUniqueMemIndexRowIdScanSpec<'a, P>>;

/// Bounded stream of unique DiskTree entries copied leaf by leaf.
pub(crate) type UniqueDiskTreeEntryStream<'a> = IndexScanStream<UniqueDiskTreeEntryScanSpec<'a>>;

/// Bounded stream of non-unique DiskTree entries copied leaf by leaf.
pub(crate) type NonUniqueDiskTreeEntryStream<'a> =
    IndexScanStream<NonUniqueDiskTreeEntryScanSpec<'a>>;
