mod block_index;
mod block_index_root;
mod btree;
mod column_block_index;
mod column_deletion_blob;
pub(crate) mod disk_tree;
mod index_stream;
mod mem_index;
mod non_unique_index;
mod owned_stream;
mod row_page_index;
mod secondary_index;
mod unique_index;
pub(crate) mod util;

use crate::buffer::{BufferPool, PoolGuard, PoolGuards};
use crate::error::RuntimeResult;
use crate::id::BlockID;
use crate::table::TableRootSnapshot;
use crate::trx::{Transaction, TrxReadProof};
use std::marker::PhantomData;
use std::sync::Arc;

pub(crate) use block_index::BlockIndex;
pub(crate) use btree::{BTreeKey, BTreeKeyEncoder, KeyRange};
#[cfg(test)]
pub(crate) use column_block_index::{
    COLUMN_BLOCK_HEADER_SIZE, COLUMN_BLOCK_LEAF_HEADER_SIZE, COLUMN_BLOCK_NODE_PAYLOAD_SIZE,
    ColumnBlockNodeHeader, validate_persisted_column_block_index_page,
};
pub(crate) use column_block_index::{
    ColumnBlockEntryShape, ColumnBlockIndex, ColumnDeleteDeltaPatch, ColumnLeafEntry,
    ResolvedColumnRow,
};
#[cfg(test)]
pub(crate) use column_deletion_blob::{
    COLUMN_DELETION_BLOB_PAGE_HEADER_SIZE, validate_persisted_blob_page,
};
pub(crate) use index_stream::{IndexBatchStream, IndexLookupCandidate};
pub(crate) use mem_index::MemIndexEntry;
pub(crate) use non_unique_index::{GuardedNonUniqueMemIndex, IndexMask, NonUniqueMemIndex};
pub(crate) use owned_stream::OwnedSecondaryIndexCandidateStream;
pub(crate) use row_page_index::RowLocation;
#[cfg(test)]
pub(crate) use row_page_index::RowPageIndexNode;
pub(crate) use secondary_index::{
    InMemorySecondaryIndex, IndexCompareExchange, IndexInsert, NonUniqueSecondaryIndex,
    SecondaryDiskTreeRuntime, SecondaryIndex, UniqueInsertAttempt, UniqueSecondaryIndex,
};
pub(crate) use unique_index::{GuardedUniqueMemIndex, UniqueMemIndex};

/// Proof-bound secondary-index root with no standalone address accessor.
struct ProvenIndexRoot<'op> {
    block_id: BlockID,
    _proof: PhantomData<&'op TrxReadProof<'op>>,
}

impl<'op> ProvenIndexRoot<'op> {
    #[inline]
    fn new(block_id: BlockID, _proof: &TrxReadProof<'op>) -> Self {
        Self {
            block_id,
            _proof: PhantomData,
        }
    }
}

/// Borrowed executable state for one foreground current-index read.
pub(crate) struct CurrentIndexReadHandle<'op, 'idx, P: BufferPool + 'static> {
    index: &'idx SecondaryIndex<P>,
    guards: &'op PoolGuards,
    root: ProvenIndexRoot<'op>,
}

impl<'op, 'idx, P: BufferPool + 'static> CurrentIndexReadHandle<'op, 'idx, P> {
    /// Creates a borrowed handle from one proof-gated current index root.
    #[inline]
    pub(crate) fn new(
        index: &'idx SecondaryIndex<P>,
        guards: &'op PoolGuards,
        root: BlockID,
        proof: &TrxReadProof<'op>,
    ) -> Self {
        Self {
            index,
            guards,
            root: ProvenIndexRoot::new(root, proof),
        }
    }

    /// Creates a handle from one proof-bearing table-root snapshot.
    #[inline]
    pub(crate) fn from_snapshot(
        index: &'idx SecondaryIndex<P>,
        guards: &'op PoolGuards,
        snapshot: &'op TableRootSnapshot<'_>,
        index_no: usize,
    ) -> Self {
        Self {
            index,
            guards,
            root: ProvenIndexRoot {
                block_id: snapshot.secondary_index_root(index_no),
                _proof: PhantomData,
            },
        }
    }

    /// Returns whether the admitted current index is unique.
    #[inline]
    pub(crate) fn is_unique(&self) -> bool {
        self.index.is_unique()
    }

    /// Returns the admitted current index's key encoder.
    #[inline]
    pub(crate) fn key_encoder(&self) -> Arc<BTreeKeyEncoder> {
        self.index.key_encoder()
    }

    /// Binds a unique executable view for the lifetime of this handle borrow.
    #[inline]
    pub(crate) fn bind_unique(&self) -> RuntimeResult<UniqueSecondaryIndex<'_, 'op, P>> {
        self.index
            .bind_unique_unchecked(self.guards, self.root.block_id)
    }

    /// Binds a non-unique executable view for the lifetime of this handle borrow.
    #[inline]
    pub(crate) fn bind_non_unique(&self) -> RuntimeResult<NonUniqueSecondaryIndex<'_, 'op, P>> {
        self.index
            .bind_non_unique_unchecked(self.guards, self.root.block_id)
    }
}

/// Owned executable state retained by one caller-driven index stream.
pub(crate) struct OwnedCurrentIndexReadHandle<'trx, P: BufferPool + 'static> {
    index: Arc<SecondaryIndex<P>>,
    index_pool_guard: PoolGuard,
    disk_pool_guard: PoolGuard,
    root: BlockID,
    _transaction: PhantomData<&'trx mut Transaction>,
}

impl<'trx, P: BufferPool + 'static> OwnedCurrentIndexReadHandle<'trx, P> {
    /// Creates an owned handle from one proof-gated current index root.
    #[inline]
    pub(crate) fn new(
        index: Arc<SecondaryIndex<P>>,
        index_pool_guard: PoolGuard,
        disk_pool_guard: PoolGuard,
        root: BlockID,
        _proof: &TrxReadProof<'_>,
        _transaction: PhantomData<&'trx mut Transaction>,
    ) -> Self {
        Self {
            index,
            index_pool_guard,
            disk_pool_guard,
            root,
            _transaction: PhantomData,
        }
    }
}
