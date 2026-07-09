//! Shared MemIndex entry and cleanup-scan plumbing.

use crate::buffer::guard::PageGuard;
use crate::buffer::{BufferPool, PoolGuard};
use crate::error::{Error, InternalError, Result};
use crate::id::{RowID, TrxID};
use crate::index::btree::{
    BTreeByte, BTreeKey, BTreeKeyEncoder, BTreeNode, BTreeNodeCursor, BTreeSlot, BTreeU64,
    GenericBTree,
};
use crate::index::util::Maskable;
use crate::quiescent::QuiescentGuard;
use crate::value::ValType;
use error_stack::Report;
use std::marker::PhantomData;
use std::mem;

/// Shared in-memory BTree-backed secondary-index storage.
pub(crate) struct MemIndex<P: 'static> {
    tree: GenericBTree<P>,
    encoder: BTreeKeyEncoder,
}

impl<P: BufferPool> MemIndex<P> {
    /// Build a MemIndex from prepared key value types.
    #[inline]
    pub(crate) async fn new_with_types(
        index_pool: QuiescentGuard<P>,
        index_pool_guard: &PoolGuard,
        types: Vec<ValType>,
        ts: TrxID,
    ) -> Result<Self> {
        let encoder = BTreeKeyEncoder::new(types);
        let tree = GenericBTree::new(index_pool, index_pool_guard, true, ts).await?;
        Ok(Self::with_encoder(tree, encoder))
    }

    /// Wrap an already-created BTree with a key encoder.
    #[inline]
    pub(crate) fn with_encoder(tree: GenericBTree<P>, encoder: BTreeKeyEncoder) -> Self {
        Self { tree, encoder }
    }

    /// Return the backing BTree.
    #[inline]
    pub(crate) fn tree(&self) -> &GenericBTree<P> {
        &self.tree
    }

    /// Return the key encoder.
    #[inline]
    pub(crate) fn encoder(&self) -> &BTreeKeyEncoder {
        &self.encoder
    }

    /// Destroy this MemIndex and reclaim all backing tree pages.
    #[inline]
    pub(crate) async fn destroy(self, pool_guard: &PoolGuard) -> Result<()> {
        self.tree.destory(pool_guard).await
    }

    /// Scan MemIndex entries with encoded keys and delete state.
    #[inline]
    pub(crate) async fn scan_encoded_entries<S>(
        &self,
        pool_guard: &PoolGuard,
    ) -> Result<Vec<MemIndexEntry>>
    where
        S: MemIndexEntryScanSpec,
    {
        let mut entries = Vec::new();
        let mut cursor = self.tree.cursor(pool_guard, 0);
        cursor.seek(&[]).await?;
        while let Some(guard) = cursor.next().await? {
            let node = guard.page();
            for idx in 0..node.count() {
                S::push_entry(node, idx, &mut entries)?;
            }
        }
        Ok(entries)
    }

    /// Create a leaf-bounded scanner for cleanup candidates.
    #[inline]
    pub(crate) fn cleanup_scan<'a, S>(
        &'a self,
        pool_guard: &'a PoolGuard,
        pivot_row_id: RowID,
        clean_live_entries: bool,
    ) -> MemIndexCleanupScan<'a, P, S>
    where
        S: MemIndexCleanupSpec,
    {
        MemIndexCleanupScan::new(&self.tree, pool_guard, pivot_row_id, clean_live_entries)
    }
}

/// Encoded MemIndex state for one secondary-index entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemIndexEntry {
    /// Encoded MemIndex key in BTree order.
    pub(crate) encoded_key: BTreeKey,
    /// Row id stored by the entry, with any delete bit stripped.
    pub(crate) row_id: RowID,
    /// Whether the MemIndex entry is delete-marked.
    pub(crate) deleted: bool,
}

/// MemIndex-specific encoded-entry decoder used by full scans.
pub(crate) trait MemIndexEntryScanSpec {
    /// Push one decoded MemIndex entry into `entries`.
    fn push_entry(
        node: &BTreeNode,
        slot_idx: usize,
        entries: &mut Vec<MemIndexEntry>,
    ) -> Result<()>;
}

/// Encoded-entry decoder for unique MemIndex leaves.
pub(crate) struct UniqueMemIndexEntryScanSpec;

impl MemIndexEntryScanSpec for UniqueMemIndexEntryScanSpec {
    #[inline]
    fn push_entry(
        node: &BTreeNode,
        slot_idx: usize,
        entries: &mut Vec<MemIndexEntry>,
    ) -> Result<()> {
        let encoded_key = node
            .btree_key_checked(slot_idx)
            .ok_or_else(|| index_key_missing(slot_idx))?;
        let value = node.value::<BTreeU64>(slot_idx);
        entries.push(MemIndexEntry {
            encoded_key,
            row_id: value.value().to_row_id(),
            deleted: value.is_deleted(),
        });
        Ok(())
    }
}

/// Encoded-entry decoder for non-unique MemIndex leaves.
pub(crate) struct NonUniqueMemIndexEntryScanSpec;

impl MemIndexEntryScanSpec for NonUniqueMemIndexEntryScanSpec {
    #[inline]
    fn push_entry(
        node: &BTreeNode,
        slot_idx: usize,
        entries: &mut Vec<MemIndexEntry>,
    ) -> Result<()> {
        push_non_unique_encoded_entry(node, node.slot(slot_idx), entries)
    }
}

/// Bounded batch of MemIndex entries selected for cleanup processing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MemIndexCleanupBatch {
    /// Cleanup candidates copied out of one MemIndex leaf.
    pub(crate) entries: Vec<MemIndexEntry>,
    /// Live entries skipped before encoded-key allocation.
    pub(crate) skipped_live: usize,
    /// Hot delete overlays skipped before encoded-key allocation.
    pub(crate) skipped_hot_deleted: usize,
}

/// Slot state decoded before cleanup skip decisions.
pub(crate) struct MemIndexCleanupSlot {
    row_id: RowID,
    deleted: bool,
}

/// MemIndex-specific slot decoder used by cleanup scans.
pub(crate) trait MemIndexCleanupSpec {
    /// Decode the row id and delete state for one MemIndex slot.
    fn slot_state(node: &BTreeNode, slot_idx: usize) -> Result<MemIndexCleanupSlot>;
}

/// Cleanup decoder for unique MemIndex leaves.
pub(crate) struct UniqueMemIndexCleanupSpec;

impl MemIndexCleanupSpec for UniqueMemIndexCleanupSpec {
    #[inline]
    fn slot_state(node: &BTreeNode, slot_idx: usize) -> Result<MemIndexCleanupSlot> {
        let value = node.value::<BTreeU64>(slot_idx);
        Ok(MemIndexCleanupSlot {
            row_id: value.value().to_row_id(),
            deleted: value.is_deleted(),
        })
    }
}

/// Cleanup decoder for non-unique MemIndex leaves.
pub(crate) struct NonUniqueMemIndexCleanupSpec;

impl MemIndexCleanupSpec for NonUniqueMemIndexCleanupSpec {
    #[inline]
    fn slot_state(node: &BTreeNode, slot_idx: usize) -> Result<MemIndexCleanupSlot> {
        let slot = node.slot(slot_idx);
        let key_len = node.slot_key_len(slot);
        if key_len < mem::size_of::<RowID>() {
            return Err(Report::new(InternalError::MemIndexKeyMalformed)
                .attach(format!("slot_idx={slot_idx}, key_len={key_len}"))
                .into());
        }
        let value = node.value_for_slot::<BTreeByte>(slot);
        Ok(MemIndexCleanupSlot {
            row_id: node.unpack_value::<BTreeU64>(slot).to_row_id(),
            deleted: value.is_deleted(),
        })
    }
}

/// Leaf-bounded cleanup scanner for MemIndex entries.
pub(crate) struct MemIndexCleanupScan<'a, P: 'static, S> {
    cursor: BTreeNodeCursor<'a, P>,
    pivot_row_id: RowID,
    clean_live_entries: bool,
    started: bool,
    _spec: PhantomData<S>,
}

impl<'a, P, S> MemIndexCleanupScan<'a, P, S>
where
    P: BufferPool,
    S: MemIndexCleanupSpec,
{
    /// Create a cleanup scanner over all leaves in a MemIndex BTree.
    #[inline]
    pub(crate) fn new(
        tree: &'a GenericBTree<P>,
        pool_guard: &'a PoolGuard,
        pivot_row_id: RowID,
        clean_live_entries: bool,
    ) -> Self {
        Self {
            cursor: tree.cursor(pool_guard, 0),
            pivot_row_id,
            clean_live_entries,
            started: false,
            _spec: PhantomData,
        }
    }

    /// Return the next leaf-bounded cleanup candidate batch.
    #[inline]
    pub(crate) async fn next_batch(&mut self) -> Result<Option<MemIndexCleanupBatch>> {
        if !self.started {
            self.cursor.seek(&[]).await?;
            self.started = true;
        }
        let Some(guard) = self.cursor.next().await? else {
            return Ok(None);
        };

        let node = guard.page();
        let mut batch = MemIndexCleanupBatch::default();
        for idx in 0..node.count() {
            let slot = S::slot_state(node, idx)?;
            if slot.row_id >= self.pivot_row_id {
                if slot.deleted {
                    batch.skipped_hot_deleted += 1;
                } else {
                    batch.skipped_live += 1;
                }
                continue;
            }
            if !slot.deleted && !self.clean_live_entries {
                batch.skipped_live += 1;
                continue;
            }
            let encoded_key = node
                .btree_key_checked(idx)
                .ok_or_else(|| index_key_missing(idx))?;
            batch.entries.push(MemIndexEntry {
                encoded_key,
                row_id: slot.row_id,
                deleted: slot.deleted,
            });
        }
        Ok(Some(batch))
    }
}

/// Push one encoded non-unique exact MemIndex entry.
#[inline]
pub(crate) fn push_non_unique_encoded_entry(
    node: &BTreeNode,
    slot: &BTreeSlot,
    entries: &mut Vec<MemIndexEntry>,
) -> Result<()> {
    let key_len = node.slot_key_len(slot);
    if key_len < mem::size_of::<RowID>() {
        return Err(Report::new(InternalError::MemIndexKeyMalformed)
            .attach(format!("key_len={key_len}"))
            .into());
    }
    let mut encoded_key = BTreeKey::arbitrary(key_len);
    let mut buf = encoded_key.modify_inplace();
    node.copy_slot_key(slot, &mut buf);
    drop(buf);
    let row_id = node.unpack_value::<BTreeU64>(slot).to_row_id();
    let value = node.value_for_slot::<BTreeByte>(slot);
    entries.push(MemIndexEntry {
        encoded_key,
        row_id,
        deleted: value.is_deleted(),
    });
    Ok(())
}

#[inline]
fn index_key_missing(slot_idx: usize) -> Error {
    Error::from(Report::new(InternalError::IndexKeyMissing).attach(format!("slot_idx={slot_idx}")))
}
