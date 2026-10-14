//! Secondary-index scan stream plumbing.
//!
//! MemIndex and DiskTree source streams share spec-driven cursors, while
//! composite streams merge hot/cold batches through secondary-index code.

use super::disk_tree::{
    DiskTreeLeaf, DiskTreeNodeCursor, DiskTreeSpec, NonUniqueDiskTreeSpec, UniqueDiskTreeSpec,
};
use crate::buffer::BufferPool;
use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::error::{
    DataIntegrityError, RuntimeError, RuntimeOrFatalError, RuntimeOrFatalResult, RuntimeResult,
};
use crate::id::RowID;
use crate::index::btree::{
    BTreeEntryView, BTreeKey, BTreeKeyView, BTreeNode, BTreeNodeCursor, BTreeU64, KeyRange,
};
use crate::index::util::Maskable;
use error_stack::Report;
use std::borrow::Borrow;
use std::future::Future;
use std::marker::PhantomData;
use std::mem;
use std::result::Result as StdResult;

/// Async batch stream returned by indexes.
pub(crate) trait IndexBatchStream<T> {
    /// Return the next non-empty batch, or `None` when exhausted.
    fn next_batch(&mut self) -> impl Future<Output = RuntimeOrFatalResult<Option<Vec<T>>>>;
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
    /// Return the number of occupied slots without exposing unchecked persisted access.
    fn slot_count(&self) -> usize;

    /// Return the first slot that can match the scan lower bound.
    fn start_slot_idx(&self, lower_seek_key: &[u8]) -> usize;

    /// Borrow one checked key, preserving source-specific errors.
    fn key_view(&self, slot_idx: usize) -> RuntimeResult<BTreeKeyView<'_>>;
}

impl IndexScanLeaf for PageSharedGuard<BTreeNode> {
    #[inline]
    fn slot_count(&self) -> usize {
        self.page().count()
    }

    #[inline]
    fn start_slot_idx(&self, lower_seek_key: &[u8]) -> usize {
        self.page().lower_bound_slot_idx(lower_seek_key)
    }

    #[inline]
    fn key_view(&self, slot_idx: usize) -> RuntimeResult<BTreeKeyView<'_>> {
        // MemIndex leaves are in-memory typed pages; an occupied slot is
        // published atomically with its key while this shared guard is held.
        Ok(self.page().key_view_checked(slot_idx).unwrap_or_else(|| {
            panic!("MemIndex key missing from occupied slot: slot_idx={slot_idx}")
        }))
    }
}

impl<F: DiskTreeSpec> IndexScanLeaf for DiskTreeLeaf<F> {
    #[inline]
    fn slot_count(&self) -> usize {
        self.guard.view().count()
    }

    #[inline]
    fn start_slot_idx(&self, _lower_seek_key: &[u8]) -> usize {
        self.start_idx
    }

    #[inline]
    fn key_view(&self, slot_idx: usize) -> RuntimeResult<BTreeKeyView<'_>> {
        self.guard.view().key(slot_idx)
    }
}

/// Cursor adapter used by generic index scan streams.
pub(crate) trait IndexLeafCursor {
    /// Native access failures from the backing index.
    type Error: Into<RuntimeOrFatalError>;

    /// Leaf item returned by this cursor.
    type Leaf: IndexScanLeaf;

    /// Seek to the first leaf that may contain `key`.
    fn seek(&mut self, key: &[u8]) -> impl Future<Output = StdResult<(), Self::Error>>;

    /// Return the next leaf item.
    fn next_leaf(&mut self) -> impl Future<Output = StdResult<Option<Self::Leaf>, Self::Error>>;
}

impl<'a, P: BufferPool> IndexLeafCursor for BTreeNodeCursor<'a, P> {
    type Leaf = PageSharedGuard<BTreeNode>;
    type Error = P::Error;

    #[inline]
    async fn seek(&mut self, key: &[u8]) -> StdResult<(), Self::Error> {
        BTreeNodeCursor::seek(self, key).await
    }

    #[inline]
    async fn next_leaf(&mut self) -> StdResult<Option<Self::Leaf>, Self::Error> {
        BTreeNodeCursor::next(self).await
    }
}

impl<'a, F: DiskTreeSpec> IndexLeafCursor for DiskTreeNodeCursor<'a, F> {
    type Leaf = DiskTreeLeaf<F>;
    type Error = RuntimeOrFatalError;

    #[inline]
    async fn seek(&mut self, key: &[u8]) -> RuntimeOrFatalResult<()> {
        DiskTreeNodeCursor::seek(self, key).await
    }

    #[inline]
    async fn next_leaf(&mut self) -> RuntimeOrFatalResult<Option<Self::Leaf>> {
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
    /// Projector that turns accepted leaf slots into output items.
    type Projector: IndexScanProjector<Leaf = Self::Leaf, Output = Self::Output>;
}

/// Projection strategy for accepted index scan slots.
pub(crate) trait IndexScanProjector {
    /// Leaf item consumed by the projector.
    type Leaf: IndexScanLeaf;
    /// Output item produced by this projector.
    type Output;

    /// Project one accepted slot into an output item.
    fn project(
        leaf: &Self::Leaf,
        slot_idx: usize,
        encoded_key: BTreeKeyView<'_>,
    ) -> RuntimeResult<Option<Self::Output>>;
}

/// Projector for unique MemIndex lookup candidates.
pub(crate) struct UniqueMemIndexLookupCandidateProjector;

impl IndexScanProjector for UniqueMemIndexLookupCandidateProjector {
    type Leaf = PageSharedGuard<BTreeNode>;
    type Output = IndexLookupCandidate;

    #[inline]
    fn project(
        leaf: &PageSharedGuard<BTreeNode>,
        slot_idx: usize,
        encoded_key: BTreeKeyView<'_>,
    ) -> RuntimeResult<Option<Self::Output>> {
        let entry = BTreeEntryView {
            key: encoded_key,
            value: leaf.page().value::<BTreeU64>(slot_idx),
        };
        Ok(Some(IndexLookupCandidate {
            encoded_key: entry.key.to_btree_key(),
            row_id: entry.value.value().to_row_id(),
        }))
    }
}

/// Projector for non-unique MemIndex lookup candidates.
pub(crate) struct NonUniqueMemIndexLookupCandidateProjector;

impl IndexScanProjector for NonUniqueMemIndexLookupCandidateProjector {
    type Leaf = PageSharedGuard<BTreeNode>;
    type Output = IndexLookupCandidate;

    #[inline]
    fn project(
        leaf: &PageSharedGuard<BTreeNode>,
        slot_idx: usize,
        encoded_key: BTreeKeyView<'_>,
    ) -> RuntimeResult<Option<Self::Output>> {
        validate_non_unique_exact_key(encoded_key.len(), slot_idx);
        let node = leaf.page();
        let slot = node.slot(slot_idx);
        Ok(Some(IndexLookupCandidate {
            encoded_key: encoded_key.to_btree_key(),
            row_id: node.unpack_value::<BTreeU64>(slot).to_row_id(),
        }))
    }
}

/// Projector for unique DiskTree lookup candidates.
pub(crate) struct UniqueDiskTreeCandidateProjector;

impl IndexScanProjector for UniqueDiskTreeCandidateProjector {
    type Leaf = DiskTreeLeaf<UniqueDiskTreeSpec>;
    type Output = IndexLookupCandidate;

    #[inline]
    fn project(
        leaf: &Self::Leaf,
        slot_idx: usize,
        encoded_key: BTreeKeyView<'_>,
    ) -> RuntimeResult<Option<Self::Output>> {
        let row_id = leaf.guard.view().row_id(slot_idx)?;
        Ok(Some(IndexLookupCandidate {
            encoded_key: encoded_key.to_btree_key(),
            row_id,
        }))
    }
}

/// Projector for non-unique DiskTree lookup candidates.
pub(crate) struct NonUniqueDiskTreeCandidateProjector;

impl IndexScanProjector for NonUniqueDiskTreeCandidateProjector {
    type Leaf = DiskTreeLeaf<NonUniqueDiskTreeSpec>;
    type Output = IndexLookupCandidate;

    #[inline]
    fn project(
        leaf: &Self::Leaf,
        slot_idx: usize,
        encoded_key: BTreeKeyView<'_>,
    ) -> RuntimeResult<Option<Self::Output>> {
        let mut suffix = [0; mem::size_of::<RowID>()];
        encoded_key.copy_tail(&mut suffix).ok_or_else(|| {
            Report::new(DataIntegrityError::InvalidPayload)
                .change_context(RuntimeError::IndexAccess)
                .attach(format!("operation=project_secondary_disk_tree_lookup_candidate, file={}, block_id={}, slot_idx={slot_idx}, exact key has no row id", leaf.file_kind, leaf.block_id))
        })?;
        let row_id = RowID::new(u64::from_be_bytes(suffix));
        Ok(Some(IndexLookupCandidate {
            encoded_key: encoded_key.to_btree_key(),
            row_id,
        }))
    }
}

/// Stream specification for unique MemIndex lookup candidates.
pub(crate) struct UniqueMemIndexLookupCandidateScanSpec<'a, P: 'static> {
    _marker: PhantomData<&'a P>,
}

impl<'a, P: BufferPool> IndexScanSpec for UniqueMemIndexLookupCandidateScanSpec<'a, P> {
    type Cursor = BTreeNodeCursor<'a, P>;
    type Leaf = PageSharedGuard<BTreeNode>;
    type Output = IndexLookupCandidate;
    type Projector = UniqueMemIndexLookupCandidateProjector;
}

/// Stream specification for non-unique MemIndex lookup candidates.
pub(crate) struct NonUniqueMemIndexLookupCandidateScanSpec<'a, P: 'static> {
    _marker: PhantomData<&'a P>,
}

impl<'a, P: BufferPool> IndexScanSpec for NonUniqueMemIndexLookupCandidateScanSpec<'a, P> {
    type Cursor = BTreeNodeCursor<'a, P>;
    type Leaf = PageSharedGuard<BTreeNode>;
    type Output = IndexLookupCandidate;
    type Projector = NonUniqueMemIndexLookupCandidateProjector;
}

/// Stream specification for unique DiskTree lookup candidates.
pub(crate) struct UniqueDiskTreeCandidateScanSpec<'a> {
    _marker: PhantomData<&'a UniqueDiskTreeSpec>,
}

impl<'a> IndexScanSpec for UniqueDiskTreeCandidateScanSpec<'a> {
    type Cursor = DiskTreeNodeCursor<'a, UniqueDiskTreeSpec>;
    type Leaf = DiskTreeLeaf<UniqueDiskTreeSpec>;
    type Output = IndexLookupCandidate;
    type Projector = UniqueDiskTreeCandidateProjector;
}

/// Stream specification for non-unique DiskTree lookup candidates.
pub(crate) struct NonUniqueDiskTreeCandidateScanSpec<'a> {
    _marker: PhantomData<&'a NonUniqueDiskTreeSpec>,
}

impl<'a> IndexScanSpec for NonUniqueDiskTreeCandidateScanSpec<'a> {
    type Cursor = DiskTreeNodeCursor<'a, NonUniqueDiskTreeSpec>;
    type Leaf = DiskTreeLeaf<NonUniqueDiskTreeSpec>;
    type Output = IndexLookupCandidate;
    type Projector = NonUniqueDiskTreeCandidateProjector;
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
    pub(crate) async fn next_batch(&mut self) -> RuntimeOrFatalResult<Option<Vec<S::Output>>> {
        if self.exhausted {
            return Ok(None);
        }
        if !self.started {
            let lower_seek_key = self.range.borrow().lower_seek_key();
            self.cursor
                .seek(lower_seek_key)
                .await
                .map_err(Into::<RuntimeOrFatalError>::into)?;
            self.started = true;
        }
        while let Some(leaf) = self
            .cursor
            .next_leaf()
            .await
            .map_err(Into::<RuntimeOrFatalError>::into)?
        {
            let mut outputs = Vec::new();
            let range = self.range.borrow();
            for idx in leaf.start_slot_idx(range.lower_seek_key())..leaf.slot_count() {
                let encoded_key = leaf.key_view(idx)?;
                if !range.lower_accepts_view(encoded_key) {
                    continue;
                }
                if !range.upper_accepts_view(encoded_key) {
                    self.exhausted = true;
                    break;
                }
                if let Some(output) = S::Projector::project(&leaf, idx, encoded_key)? {
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
    async fn next_batch(&mut self) -> RuntimeOrFatalResult<Option<Vec<S::Output>>> {
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

/// Validate that an encoded non-unique key contains a trailing row id.
#[inline]
pub(crate) fn validate_non_unique_exact_key(key_len: usize, slot_idx: usize) {
    // Non-unique MemIndex keys are formed by appending the row id before the
    // slot is published, so every exact key must retain that suffix.
    assert!(
        key_len >= mem::size_of::<RowID>(),
        "non-unique MemIndex key is missing its row-id suffix: slot_idx={slot_idx}, key_len={}",
        key_len
    );
}
