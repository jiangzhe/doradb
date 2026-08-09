//! Operation-local borrowed secondary-index streams.
//!
//! Unlike caller-driven streams, mutation streams are created and fully
//! consumed inside one statement operation. They therefore borrow the index
//! runtime, pool guards, captured root snapshot, and encoded range while
//! owning only traversal state, candidate batches, and the MemTree restart
//! key required after a row mutation changes the tree.

use super::index_stream::{
    NonUniqueDiskTreeCandidateStream, NonUniqueMemIndexCandidateStream,
    UniqueDiskTreeCandidateStream, UniqueMemIndexCandidateStream,
};
use super::non_unique_index::NonUniqueMemIndex;
use super::secondary_index::{SecondaryIndex, SecondaryIndexCandidateStream};
use super::unique_index::UniqueMemIndex;
use super::{BTreeKey, IndexBatchStream, IndexLookupCandidate, KeyRange};
use crate::buffer::{BufferPool, PoolGuard, PoolGuards};
use crate::error::RuntimeResult;
use crate::table::TableRootSnapshot;

enum MutationMemSource<'scan, P: BufferPool + 'static> {
    Unique {
        mem: &'scan UniqueMemIndex<P>,
        index_pool_guard: &'scan PoolGuard,
    },
    NonUnique {
        mem: &'scan NonUniqueMemIndex<P>,
        index_pool_guard: &'scan PoolGuard,
    },
}

impl<P: BufferPool + 'static> MutationMemSource<'_, P> {
    async fn next_batch(
        &self,
        range: &KeyRange,
    ) -> RuntimeResult<Option<Vec<IndexLookupCandidate>>> {
        match self {
            Self::Unique {
                mem,
                index_pool_guard,
            } => {
                let mut stream = UniqueMemIndexCandidateStream::new(
                    mem.tree().cursor(index_pool_guard, 0),
                    range,
                );
                stream.next_batch().await
            }
            Self::NonUnique {
                mem,
                index_pool_guard,
            } => {
                let mut stream = NonUniqueMemIndexCandidateStream::new(
                    mem.tree().cursor(index_pool_guard, 0),
                    range,
                );
                stream.next_batch().await
            }
        }
    }
}

struct MutationMemCandidateSource<'scan, P: BufferPool + 'static> {
    source: MutationMemSource<'scan, P>,
    original_range: &'scan KeyRange,
    last_batch_key: Option<BTreeKey>,
}

impl<P: BufferPool + 'static> IndexBatchStream<IndexLookupCandidate>
    for MutationMemCandidateSource<'_, P>
{
    async fn next_batch(&mut self) -> RuntimeResult<Option<Vec<IndexLookupCandidate>>> {
        let entries = match &self.last_batch_key {
            Some(key) => {
                let range = self.original_range.resume_after(key.clone());
                self.source.next_batch(&range).await?
            }
            None => self.source.next_batch(self.original_range).await?,
        };
        if let Some(entries) = &entries {
            assert!(
                !entries.is_empty(),
                "leaf-bounded MemTree candidate stream returned an empty batch"
            );
            // The dual-tree merger cannot poll this source again until it has
            // emitted every entry in this batch, so this is a consumed-key
            // boundary by the time it is used for the next restart.
            self.last_batch_key = Some(
                entries
                    .last()
                    .expect("non-empty MemTree candidate batch must have a final entry")
                    .encoded_key
                    .clone(),
            );
        }
        Ok(entries)
    }
}

enum MutationDiskSource<'scan> {
    Unique(UniqueDiskTreeCandidateStream<'scan, 'scan>),
    NonUnique(NonUniqueDiskTreeCandidateStream<'scan, 'scan>),
}

impl IndexBatchStream<IndexLookupCandidate> for MutationDiskSource<'_> {
    #[inline]
    async fn next_batch(&mut self) -> RuntimeResult<Option<Vec<IndexLookupCandidate>>> {
        match self {
            Self::Unique(stream) => stream.next_batch().await,
            Self::NonUnique(stream) => stream.next_batch().await,
        }
    }
}

/// Operation-local dual-tree mutation stream with restartable MemTree state.
pub(crate) struct BorrowedIndexMutationStream<'scan, 'ctx, P: BufferPool + 'static> {
    inner: SecondaryIndexCandidateStream<
        MutationMemCandidateSource<'scan, P>,
        MutationDiskSource<'scan>,
    >,
    // Retain the root authority for at least as long as the DiskTree cursor.
    _snapshot: &'scan TableRootSnapshot<'ctx>,
}

impl<'scan, 'ctx, P: BufferPool + 'static> BorrowedIndexMutationStream<'scan, 'ctx, P> {
    /// Create a mutation stream over one captured table root and encoded range.
    pub(crate) fn new(
        index: &'scan SecondaryIndex<P>,
        guards: &'scan PoolGuards,
        snapshot: &'scan TableRootSnapshot<'ctx>,
        range: &'scan KeyRange,
    ) -> RuntimeResult<Self> {
        let root = snapshot.secondary_index_root(index.index_no());
        let (mem_source, disk) = match index {
            SecondaryIndex::Unique { mem, disk } => (
                MutationMemSource::Unique {
                    mem,
                    index_pool_guard: guards.index_guard(),
                },
                MutationDiskSource::Unique(
                    disk.open_unique_at(root, guards.disk_guard())?
                        .scan_candidate_stream(range),
                ),
            ),
            SecondaryIndex::NonUnique { mem, disk } => (
                MutationMemSource::NonUnique {
                    mem,
                    index_pool_guard: guards.index_guard(),
                },
                MutationDiskSource::NonUnique(
                    disk.open_non_unique_at(root, guards.disk_guard())?
                        .scan_candidate_stream(range),
                ),
            ),
        };
        let mem = MutationMemCandidateSource {
            source: mem_source,
            original_range: range,
            last_batch_key: None,
        };
        Ok(Self {
            inner: SecondaryIndexCandidateStream::new(mem, disk),
            _snapshot: snapshot,
        })
    }
}

impl<P: BufferPool + 'static> IndexBatchStream<IndexLookupCandidate>
    for BorrowedIndexMutationStream<'_, '_, P>
{
    #[inline]
    async fn next_batch(&mut self) -> RuntimeResult<Option<Vec<IndexLookupCandidate>>> {
        self.inner.next_batch().await
    }
}
