//! Secondary-index scan stream plumbing.
//!
//! MemIndex and DiskTree source streams share spec-driven cursors, while
//! composite streams merge hot/cold batches through secondary-index code.

use super::disk_tree::{
    DiskTreeLeaf, DiskTreeNodeCursor, DiskTreeSpec, NonUniqueDiskTreeSpec, UniqueDiskTreeSpec,
    invalid_node_payload, unpack_row_id_from_exact_key,
};
use crate::buffer::BufferPool;
use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::error::{Error, InternalError, Result};
use crate::id::RowID;
use crate::index::btree::{BTreeKey, BTreeNode, BTreeNodeCursor, BTreeU64, KeyRange};
use crate::index::util::Maskable;
use error_stack::Report;
use std::borrow::Borrow;
use std::future::Future;
use std::marker::PhantomData;
use std::mem;

/// Async batch stream returned by indexes.
pub(crate) trait IndexBatchStream<T> {
    /// Return the next non-empty batch, or `None` when exhausted.
    fn next_batch(&mut self) -> impl Future<Output = Result<Option<Vec<T>>>>;
}

/// Candidate emitted by a secondary-index scan before row MVCC visibility.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexLookupCandidate {
    /// Encoded B-tree key that identified this candidate.
    pub(crate) encoded_key: BTreeKey,
    /// Row identifier referenced by the encoded index entry.
    pub(crate) row_id: RowID,
}

/// Leaf item returned by an index scan cursor.
pub(crate) trait IndexScanLeaf {
    /// Return the B-tree node carried by this leaf.
    fn node(&self) -> &BTreeNode;

    /// Return the first slot that can match the scan lower bound.
    fn start_slot_idx(&self, lower_seek_key: &[u8]) -> usize;

    /// Copy one slot key out of this leaf, preserving source-specific errors.
    fn key_checked(&self, slot_idx: usize) -> Result<BTreeKey>;
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
    fn key_checked(&self, slot_idx: usize) -> Result<BTreeKey> {
        self.node().btree_key_checked(slot_idx).ok_or_else(|| {
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
    fn key_checked(&self, slot_idx: usize) -> Result<BTreeKey> {
        self.node()
            .btree_key_checked(slot_idx)
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
        encoded_key: BTreeKey,
    ) -> Result<Option<Self::Output>>;
}

/// Stream specification for unique MemIndex lookup candidates.
pub(crate) struct UniqueMemIndexLookupCandidateScanSpec<'a, P: 'static> {
    _marker: PhantomData<&'a P>,
}

impl<'a, P: BufferPool> IndexScanSpec for UniqueMemIndexLookupCandidateScanSpec<'a, P> {
    type Cursor = BTreeNodeCursor<'a, P>;
    type Leaf = PageSharedGuard<BTreeNode>;
    type Output = IndexLookupCandidate;

    #[inline]
    fn project(
        leaf: &PageSharedGuard<BTreeNode>,
        slot_idx: usize,
        encoded_key: BTreeKey,
    ) -> Result<Option<Self::Output>> {
        let value = leaf.node().value::<BTreeU64>(slot_idx);
        Ok(Some(IndexLookupCandidate {
            encoded_key,
            row_id: value.value().to_row_id(),
        }))
    }
}

/// Stream specification for non-unique MemIndex lookup candidates.
pub(crate) struct NonUniqueMemIndexLookupCandidateScanSpec<'a, P: 'static> {
    _marker: PhantomData<&'a P>,
}

impl<'a, P: BufferPool> IndexScanSpec for NonUniqueMemIndexLookupCandidateScanSpec<'a, P> {
    type Cursor = BTreeNodeCursor<'a, P>;
    type Leaf = PageSharedGuard<BTreeNode>;
    type Output = IndexLookupCandidate;

    #[inline]
    fn project(
        leaf: &PageSharedGuard<BTreeNode>,
        slot_idx: usize,
        encoded_key: BTreeKey,
    ) -> Result<Option<Self::Output>> {
        validate_non_unique_exact_key(encoded_key.as_bytes(), slot_idx)?;
        let node = leaf.node();
        let slot = node.slot(slot_idx);
        Ok(Some(IndexLookupCandidate {
            encoded_key,
            row_id: node.unpack_value::<BTreeU64>(slot).to_row_id(),
        }))
    }
}

/// Stream specification for unique DiskTree lookup candidates.
pub(crate) struct UniqueDiskTreeCandidateScanSpec<'a> {
    _marker: PhantomData<&'a UniqueDiskTreeSpec>,
}

impl<'a> IndexScanSpec for UniqueDiskTreeCandidateScanSpec<'a> {
    type Cursor = DiskTreeNodeCursor<'a, UniqueDiskTreeSpec>;
    type Leaf = DiskTreeLeaf<UniqueDiskTreeSpec>;
    type Output = IndexLookupCandidate;

    #[inline]
    fn project(
        leaf: &Self::Leaf,
        slot_idx: usize,
        encoded_key: BTreeKey,
    ) -> Result<Option<Self::Output>> {
        let row_id = leaf.node().value::<BTreeU64>(slot_idx);
        if row_id.is_deleted() {
            return Err(invalid_node_payload(leaf.file_kind, leaf.block_id));
        }
        Ok(Some(IndexLookupCandidate {
            encoded_key,
            row_id: row_id.to_row_id(),
        }))
    }
}

/// Stream specification for non-unique DiskTree lookup candidates.
pub(crate) struct NonUniqueDiskTreeCandidateScanSpec<'a> {
    _marker: PhantomData<&'a NonUniqueDiskTreeSpec>,
}

impl<'a> IndexScanSpec for NonUniqueDiskTreeCandidateScanSpec<'a> {
    type Cursor = DiskTreeNodeCursor<'a, NonUniqueDiskTreeSpec>;
    type Leaf = DiskTreeLeaf<NonUniqueDiskTreeSpec>;
    type Output = IndexLookupCandidate;

    #[inline]
    fn project(
        _leaf: &Self::Leaf,
        _slot_idx: usize,
        encoded_key: BTreeKey,
    ) -> Result<Option<Self::Output>> {
        let row_id = unpack_row_id_from_exact_key(encoded_key.as_bytes())?;
        Ok(Some(IndexLookupCandidate {
            encoded_key,
            row_id,
        }))
    }
}

/// Generic leaf-bounded stream over index slots.
pub(crate) struct IndexScanStream<S: IndexScanSpec, R> {
    cursor: S::Cursor,
    range: R,
    started: bool,
    exhausted: bool,
    _spec: PhantomData<S>,
}

impl<S, R> IndexScanStream<S, R>
where
    S: IndexScanSpec,
    R: Borrow<KeyRange>,
{
    /// Create an index scan stream over an encoded key range.
    #[inline]
    pub(super) fn new(cursor: S::Cursor, range: R) -> Self {
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
            let lower_seek_key = self.range.borrow().lower_seek_key().to_vec();
            self.cursor.seek(&lower_seek_key).await?;
            self.started = true;
        }
        while let Some(leaf) = self.cursor.next_leaf().await? {
            let node = leaf.node();
            let mut outputs = Vec::new();
            let range = self.range.borrow();
            for idx in leaf.start_slot_idx(range.lower_seek_key())..node.count() {
                let encoded_key = leaf.key_checked(idx)?;
                if !range.lower_accepts(encoded_key.as_bytes()) {
                    continue;
                }
                if !range.upper_accepts(encoded_key.as_bytes()) {
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

impl<S, R> IndexBatchStream<S::Output> for IndexScanStream<S, R>
where
    S: IndexScanSpec,
    R: Borrow<KeyRange>,
{
    #[inline]
    async fn next_batch(&mut self) -> Result<Option<Vec<S::Output>>> {
        IndexScanStream::next_batch(self).await
    }
}

/// Lookup-candidate stream over unique MemIndex entries.
pub(crate) type UniqueMemIndexCandidateStream<'a, P> =
    IndexScanStream<UniqueMemIndexLookupCandidateScanSpec<'a, P>, &'a KeyRange>;

/// Lookup-candidate stream over non-unique MemIndex entries.
pub(crate) type NonUniqueMemIndexCandidateStream<'a, P> =
    IndexScanStream<NonUniqueMemIndexLookupCandidateScanSpec<'a, P>, &'a KeyRange>;

/// Bounded stream of unique DiskTree lookup candidates copied leaf by leaf.
pub(crate) type UniqueDiskTreeCandidateStream<'a, 'r> =
    IndexScanStream<UniqueDiskTreeCandidateScanSpec<'a>, &'r KeyRange>;

/// Bounded stream of non-unique DiskTree lookup candidates copied leaf by leaf.
pub(crate) type NonUniqueDiskTreeCandidateStream<'a, 'r> =
    IndexScanStream<NonUniqueDiskTreeCandidateScanSpec<'a>, &'r KeyRange>;

#[inline]
pub(crate) fn validate_non_unique_exact_key(encoded_key: &[u8], slot_idx: usize) -> Result<()> {
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
