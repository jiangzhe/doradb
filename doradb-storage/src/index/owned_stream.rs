//! Owned secondary-index scan streams.
//!
//! These streams keep persistent cursor state for public transaction streams
//! without borrowing temporary bound-index views.

use super::disk_tree::{
    DiskTreeLeaf, DiskTreeNodeCursorState, NonUniqueDiskTreeSpec, UniqueDiskTreeSpec,
    invalid_node_payload, unpack_row_id_from_exact_key,
};
use super::index_stream::{
    IndexLeafCursor, IndexScanLeaf, IndexScanSpec, IndexScanStream, validate_non_unique_exact_key,
};
use super::secondary_index::{SecondaryIndex, SecondaryIndexCandidateStream};
use crate::buffer::guard::PageSharedGuard;
use crate::buffer::{BufferPool, PoolGuard, PoolGuards};
use crate::error::Result;
use crate::id::BlockID;
use crate::index::btree::{BTreeKey, BTreeNode, BTreeNodeCursorState, BTreeU64};
use crate::index::util::Maskable;
use crate::index::{IndexBatchStream, IndexLookupCandidate, KeyRange};
use std::marker::PhantomData;
use std::sync::Arc;

struct OwnedUniqueMemIndexCursor<P: 'static> {
    index: Arc<SecondaryIndex<P>>,
    index_pool_guard: PoolGuard,
    state: BTreeNodeCursorState,
}

impl<P: BufferPool> OwnedUniqueMemIndexCursor<P> {
    #[inline]
    fn new(index: Arc<SecondaryIndex<P>>, index_pool_guard: PoolGuard) -> Self {
        Self {
            index,
            index_pool_guard,
            state: BTreeNodeCursorState::new(0),
        }
    }
}

impl<P: BufferPool> IndexLeafCursor for OwnedUniqueMemIndexCursor<P> {
    type Leaf = PageSharedGuard<BTreeNode>;

    #[inline]
    async fn seek(&mut self, key: &[u8]) -> Result<()> {
        let mem = match self.index.as_ref() {
            SecondaryIndex::Unique { mem, .. } => mem,
            SecondaryIndex::NonUnique { .. } => {
                unreachable!("owned unique MemIndex cursor bound to non-unique index")
            }
        };
        self.state
            .seek(mem.tree(), &self.index_pool_guard, key)
            .await
    }

    #[inline]
    async fn next_leaf(&mut self) -> Result<Option<Self::Leaf>> {
        let mem = match self.index.as_ref() {
            SecondaryIndex::Unique { mem, .. } => mem,
            SecondaryIndex::NonUnique { .. } => {
                unreachable!("owned unique MemIndex cursor bound to non-unique index")
            }
        };
        self.state.next(mem.tree(), &self.index_pool_guard).await
    }
}

struct OwnedNonUniqueMemIndexCursor<P: 'static> {
    index: Arc<SecondaryIndex<P>>,
    index_pool_guard: PoolGuard,
    state: BTreeNodeCursorState,
}

impl<P: BufferPool> OwnedNonUniqueMemIndexCursor<P> {
    #[inline]
    fn new(index: Arc<SecondaryIndex<P>>, index_pool_guard: PoolGuard) -> Self {
        Self {
            index,
            index_pool_guard,
            state: BTreeNodeCursorState::new(0),
        }
    }
}

impl<P: BufferPool> IndexLeafCursor for OwnedNonUniqueMemIndexCursor<P> {
    type Leaf = PageSharedGuard<BTreeNode>;

    #[inline]
    async fn seek(&mut self, key: &[u8]) -> Result<()> {
        let mem = match self.index.as_ref() {
            SecondaryIndex::NonUnique { mem, .. } => mem,
            SecondaryIndex::Unique { .. } => {
                unreachable!("owned non-unique MemIndex cursor bound to unique index")
            }
        };
        self.state
            .seek(mem.tree(), &self.index_pool_guard, key)
            .await
    }

    #[inline]
    async fn next_leaf(&mut self) -> Result<Option<Self::Leaf>> {
        let mem = match self.index.as_ref() {
            SecondaryIndex::NonUnique { mem, .. } => mem,
            SecondaryIndex::Unique { .. } => {
                unreachable!("owned non-unique MemIndex cursor bound to unique index")
            }
        };
        self.state.next(mem.tree(), &self.index_pool_guard).await
    }
}

struct OwnedUniqueDiskTreeCursor<P: 'static> {
    index: Arc<SecondaryIndex<P>>,
    disk_pool_guard: PoolGuard,
    state: DiskTreeNodeCursorState,
}

impl<P: BufferPool> OwnedUniqueDiskTreeCursor<P> {
    #[inline]
    fn new(index: Arc<SecondaryIndex<P>>, disk_pool_guard: PoolGuard, root: BlockID) -> Self {
        Self {
            index,
            disk_pool_guard,
            state: DiskTreeNodeCursorState::new(root),
        }
    }
}

impl<P: BufferPool> IndexLeafCursor for OwnedUniqueDiskTreeCursor<P> {
    type Leaf = DiskTreeLeaf<UniqueDiskTreeSpec>;

    #[inline]
    async fn seek(&mut self, key: &[u8]) -> Result<()> {
        self.state.seek(key).await
    }

    #[inline]
    async fn next_leaf(&mut self) -> Result<Option<Self::Leaf>> {
        let runtime = match self.index.as_ref() {
            SecondaryIndex::Unique { disk, .. } => disk.unique_runtime(),
            SecondaryIndex::NonUnique { .. } => {
                unreachable!("owned unique DiskTree cursor bound to non-unique index")
            }
        };
        self.state.next_leaf(runtime, &self.disk_pool_guard).await
    }
}

struct OwnedNonUniqueDiskTreeCursor<P: 'static> {
    index: Arc<SecondaryIndex<P>>,
    disk_pool_guard: PoolGuard,
    state: DiskTreeNodeCursorState,
}

impl<P: BufferPool> OwnedNonUniqueDiskTreeCursor<P> {
    #[inline]
    fn new(index: Arc<SecondaryIndex<P>>, disk_pool_guard: PoolGuard, root: BlockID) -> Self {
        Self {
            index,
            disk_pool_guard,
            state: DiskTreeNodeCursorState::new(root),
        }
    }
}

impl<P: BufferPool> IndexLeafCursor for OwnedNonUniqueDiskTreeCursor<P> {
    type Leaf = DiskTreeLeaf<NonUniqueDiskTreeSpec>;

    #[inline]
    async fn seek(&mut self, key: &[u8]) -> Result<()> {
        self.state.seek(key).await
    }

    #[inline]
    async fn next_leaf(&mut self) -> Result<Option<Self::Leaf>> {
        let runtime = match self.index.as_ref() {
            SecondaryIndex::NonUnique { disk, .. } => disk.non_unique_runtime(),
            SecondaryIndex::Unique { .. } => {
                unreachable!("owned non-unique DiskTree cursor bound to unique index")
            }
        };
        self.state.next_leaf(runtime, &self.disk_pool_guard).await
    }
}

struct OwnedUniqueMemIndexLookupCandidateScanSpec<P: 'static> {
    _marker: PhantomData<P>,
}

impl<P: BufferPool> IndexScanSpec for OwnedUniqueMemIndexLookupCandidateScanSpec<P> {
    type Cursor = OwnedUniqueMemIndexCursor<P>;
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

struct OwnedNonUniqueMemIndexLookupCandidateScanSpec<P: 'static> {
    _marker: PhantomData<P>,
}

impl<P: BufferPool> IndexScanSpec for OwnedNonUniqueMemIndexLookupCandidateScanSpec<P> {
    type Cursor = OwnedNonUniqueMemIndexCursor<P>;
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

struct OwnedUniqueDiskTreeCandidateScanSpec<P: 'static> {
    _marker: PhantomData<P>,
}

impl<P: BufferPool> IndexScanSpec for OwnedUniqueDiskTreeCandidateScanSpec<P> {
    type Cursor = OwnedUniqueDiskTreeCursor<P>;
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

struct OwnedNonUniqueDiskTreeCandidateScanSpec<P: 'static> {
    _marker: PhantomData<P>,
}

impl<P: BufferPool> IndexScanSpec for OwnedNonUniqueDiskTreeCandidateScanSpec<P> {
    type Cursor = OwnedNonUniqueDiskTreeCursor<P>;
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

type OwnedUniqueMemIndexCandidateStream<P> =
    IndexScanStream<OwnedUniqueMemIndexLookupCandidateScanSpec<P>, Arc<KeyRange>>;
type OwnedNonUniqueMemIndexCandidateStream<P> =
    IndexScanStream<OwnedNonUniqueMemIndexLookupCandidateScanSpec<P>, Arc<KeyRange>>;
type OwnedUniqueDiskTreeCandidateStream<P> =
    IndexScanStream<OwnedUniqueDiskTreeCandidateScanSpec<P>, Arc<KeyRange>>;
type OwnedNonUniqueDiskTreeCandidateStream<P> =
    IndexScanStream<OwnedNonUniqueDiskTreeCandidateScanSpec<P>, Arc<KeyRange>>;

/// Owned persistent lookup-candidate stream for user-table secondary scans.
pub(crate) struct OwnedSecondaryIndexCandidateStream<P: BufferPool + 'static> {
    inner: OwnedSecondaryIndexCandidateStreamKind<P>,
}

enum OwnedSecondaryIndexCandidateStreamKind<P: BufferPool + 'static> {
    Unique(
        SecondaryIndexCandidateStream<
            OwnedUniqueMemIndexCandidateStream<P>,
            OwnedUniqueDiskTreeCandidateStream<P>,
        >,
    ),
    NonUnique(
        SecondaryIndexCandidateStream<
            OwnedNonUniqueMemIndexCandidateStream<P>,
            OwnedNonUniqueDiskTreeCandidateStream<P>,
        >,
    ),
}

impl<P: BufferPool + 'static> OwnedSecondaryIndexCandidateStream<P> {
    /// Create a persistent candidate stream over one proof-gated root.
    #[inline]
    pub(crate) fn new(
        index: Arc<SecondaryIndex<P>>,
        pool_guards: PoolGuards,
        root: BlockID,
        range: KeyRange,
    ) -> Self {
        let range = Arc::new(range);
        let inner = match index.as_ref() {
            SecondaryIndex::Unique { .. } => {
                let mem = OwnedUniqueMemIndexCandidateStream::new(
                    OwnedUniqueMemIndexCursor::new(
                        Arc::clone(&index),
                        pool_guards.index_guard().clone(),
                    ),
                    Arc::clone(&range),
                );
                let disk = OwnedUniqueDiskTreeCandidateStream::new(
                    OwnedUniqueDiskTreeCursor::new(index, pool_guards.disk_guard().clone(), root),
                    range,
                );
                OwnedSecondaryIndexCandidateStreamKind::Unique(SecondaryIndexCandidateStream::new(
                    mem, disk,
                ))
            }
            SecondaryIndex::NonUnique { .. } => {
                let mem = OwnedNonUniqueMemIndexCandidateStream::new(
                    OwnedNonUniqueMemIndexCursor::new(
                        Arc::clone(&index),
                        pool_guards.index_guard().clone(),
                    ),
                    Arc::clone(&range),
                );
                let disk = OwnedNonUniqueDiskTreeCandidateStream::new(
                    OwnedNonUniqueDiskTreeCursor::new(
                        index,
                        pool_guards.disk_guard().clone(),
                        root,
                    ),
                    range,
                );
                OwnedSecondaryIndexCandidateStreamKind::NonUnique(
                    SecondaryIndexCandidateStream::new(mem, disk),
                )
            }
        };
        Self { inner }
    }
}

impl<P: BufferPool + 'static> IndexBatchStream<IndexLookupCandidate>
    for OwnedSecondaryIndexCandidateStream<P>
{
    #[inline]
    async fn next_batch(&mut self) -> Result<Option<Vec<IndexLookupCandidate>>> {
        match &mut self.inner {
            OwnedSecondaryIndexCandidateStreamKind::Unique(stream) => stream.next_batch().await,
            OwnedSecondaryIndexCandidateStreamKind::NonUnique(stream) => stream.next_batch().await,
        }
    }
}
