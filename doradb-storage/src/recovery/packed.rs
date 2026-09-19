//! Flat recovery transport. Only the coordinator constructs and rebases ranges;
//! workers resolve immutable views and page writers copy their input bytes.

use crate::id::{RowID, TrxID};
use crate::log::redo::RowRedoCode;
use crate::row::{RowValues, UpdateValues};
use crate::value::ValRef;
use ordered_float::OrderedFloat;
use std::mem::size_of;
use std::ops::Range;
#[cfg(test)]
pub(crate) use tests::{OwnedReplayOp, pack_test_ops};
#[cfg(test)]
pub(super) use tests::{decode_test_op, pool_snapshot};

/// Allocation-free value descriptor, relative to its enclosing group's or batch's bytes.
#[derive(Clone, Copy, Debug)]
pub(super) enum PackedValue {
    Null,
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    F32(OrderedFloat<f32>),
    I64(i64),
    U64(u64),
    F64(OrderedFloat<f64>),
    Bytes { offset: usize, len: usize },
}

impl PackedValue {
    /// Capture a decoded value and the checked start of its bytes in the group.
    #[inline]
    pub(super) fn from_wire(value: ValRef<'_>, offset: usize) -> Self {
        match value {
            ValRef::Null => Self::Null,
            ValRef::I8(v) => Self::I8(v),
            ValRef::U8(v) => Self::U8(v),
            ValRef::I16(v) => Self::I16(v),
            ValRef::U16(v) => Self::U16(v),
            ValRef::I32(v) => Self::I32(v),
            ValRef::U32(v) => Self::U32(v),
            ValRef::F32(v) => Self::F32(v),
            ValRef::I64(v) => Self::I64(v),
            ValRef::U64(v) => Self::U64(v),
            ValRef::F64(v) => Self::F64(v),
            ValRef::VarByte(v) => Self::Bytes {
                offset,
                len: v.len(),
            },
        }
    }

    #[inline]
    fn resolve(self, payload: &[u8]) -> ValRef<'_> {
        match self {
            Self::Null => ValRef::Null,
            Self::I8(v) => ValRef::I8(v),
            Self::U8(v) => ValRef::U8(v),
            Self::I16(v) => ValRef::I16(v),
            Self::U16(v) => ValRef::U16(v),
            Self::I32(v) => ValRef::I32(v),
            Self::U32(v) => ValRef::U32(v),
            Self::F32(v) => ValRef::F32(v),
            Self::I64(v) => ValRef::I64(v),
            Self::U64(v) => ValRef::U64(v),
            Self::F64(v) => ValRef::F64(v),
            // Construction validates the end before publishing the descriptor.
            Self::Bytes { offset, len } => ValRef::VarByte(&payload[offset..offset + len]),
        }
    }

    #[inline]
    fn append(self, source: &[u8], payload: &mut Vec<u8>) -> Self {
        match self {
            Self::Bytes { offset, len } => {
                let rebased = Self::Bytes {
                    offset: payload.len(),
                    len,
                };
                payload.extend_from_slice(&source[offset..offset + len]);
                rebased
            }
            scalar => scalar,
        }
    }
}

/// Original sparse-update ordinal and allocation-free value.
#[derive(Clone, Copy, Debug)]
pub(super) struct PackedUpdate {
    /// Original encoded ordinal; decoding never sorts or deduplicates updates.
    pub(super) idx: usize,
    /// Descriptor addressing the enclosing owner's bytes.
    pub(super) val: PackedValue,
}

/// Repeatable full-row view borrowing descriptors and their byte owner.
#[derive(Clone, Copy)]
pub(crate) struct PackedRow<'a> {
    pub(super) values: &'a [PackedValue],
    pub(super) payload: &'a [u8],
}

impl RowValues for PackedRow<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    fn value(&self, index: usize) -> ValRef<'_> {
        self.values[index].resolve(self.payload)
    }
}

/// Repeatable sparse-update view retaining the encoded entry order.
#[derive(Clone, Copy)]
pub(crate) struct PackedUpdates<'a> {
    pub(super) updates: &'a [PackedUpdate],
    pub(super) payload: &'a [u8],
}

impl UpdateValues for PackedUpdates<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.updates.len()
    }

    #[inline]
    fn value(&self, index: usize) -> (usize, ValRef<'_>) {
        let update = self.updates[index];
        (update.idx, update.val.resolve(self.payload))
    }
}

/// Borrowed operation used during admission and synchronous page mutation.
pub(crate) struct ReplayOp<'a> {
    /// Original commit timestamp for replay diagnostics.
    pub(crate) cts: TrxID,
    /// Payload row identity, independent of the routing map key.
    pub(crate) row_id: RowID,
    /// Borrowed values resolved against exactly one owner.
    pub(crate) kind: ReplayKind<'a>,
    pub(super) payload_bytes: usize,
}

impl ReplayOp<'_> {
    /// Full used-storage contribution, including the operation descriptor.
    #[inline]
    pub(super) fn used_bytes(&self) -> usize {
        let (values, updates) = match &self.kind {
            ReplayKind::Insert(row) => (row.len(), 0),
            ReplayKind::Update(row) => (0, row.len()),
            ReplayKind::Delete => (0, 0),
        };
        storage_bytes(1, values, updates, self.payload_bytes)
    }
}

/// Resolved mutation with no owning row or value allocations.
pub(crate) enum ReplayKind<'a> {
    Insert(PackedRow<'a>),
    Update(PackedUpdates<'a>),
    Delete,
}

impl ReplayKind<'_> {
    /// Original redo code for mutation error diagnostics.
    #[inline]
    pub(crate) fn code(&self) -> RowRedoCode {
        match self {
            Self::Insert(_) => RowRedoCode::Insert,
            Self::Update(_) => RowRedoCode::Update,
            Self::Delete => RowRedoCode::Delete,
        }
    }
}

struct PackedReplayOp {
    cts: TrxID,
    row_id: RowID,
    kind: PackedReplayKind,
}

enum PackedReplayKind {
    Insert(Range<usize>),
    Update(Range<usize>),
    Delete,
}

/// Independently owned page work; no group bytes, table handles, or guards escape here.
#[derive(Default)]
pub(crate) struct PackedPageBatch {
    ops: Vec<PackedReplayOp>,
    values: Vec<PackedValue>,
    updates: Vec<PackedUpdate>,
    payload: Vec<u8>,
    oversized: bool,
}

impl PackedPageBatch {
    /// Number of admitted operations, including deletes.
    #[inline]
    pub(super) fn len(&self) -> usize {
        self.ops.len()
    }

    /// Full used storage in constant time, excluding spare vector capacity.
    #[inline]
    pub(super) fn used_bytes(&self) -> usize {
        storage_bytes(
            self.ops.len(),
            self.values.len(),
            self.updates.len(),
            self.payload.len(),
        )
    }

    #[inline]
    fn retained_bytes(&self) -> usize {
        storage_bytes(
            self.ops.capacity(),
            self.values.capacity(),
            self.updates.capacity(),
            self.payload.capacity(),
        )
    }

    /// Copy one admitted operation directly into reusable storage, then publish it.
    pub(super) fn append(&mut self, op: &ReplayOp<'_>, target: usize) {
        let used = op.used_bytes();
        // Reserve all destination vectors before copying. Geometric Vec growth
        // amortizes allocation across rows and reuse preserves those capacities.
        self.ops.reserve(1);
        self.payload.reserve(op.payload_bytes);
        let kind = match &op.kind {
            ReplayKind::Insert(row) => {
                let start = self.values.len();
                self.values.reserve(row.values.len());
                self.values.extend(
                    row.values
                        .iter()
                        .map(|v| v.append(row.payload, &mut self.payload)),
                );
                PackedReplayKind::Insert(start..self.values.len())
            }
            ReplayKind::Update(row) => {
                let start = self.updates.len();
                self.updates.reserve(row.updates.len());
                self.updates
                    .extend(row.updates.iter().map(|v| PackedUpdate {
                        idx: v.idx,
                        val: v.val.append(row.payload, &mut self.payload),
                    }));
                PackedReplayKind::Update(start..self.updates.len())
            }
            ReplayKind::Delete => PackedReplayKind::Delete,
        };
        self.oversized |= used > target;
        self.ops.push(PackedReplayOp {
            cts: op.cts,
            row_id: op.row_id,
            kind,
        });
    }

    /// Resolve operations in admission order, borrowing only this batch.
    #[inline]
    pub(crate) fn operations(&self) -> impl Iterator<Item = ReplayOp<'_>> {
        self.ops.iter().map(|op| ReplayOp {
            cts: op.cts,
            row_id: op.row_id,
            payload_bytes: 0, // Only admission views need the precomputed payload length.
            kind: match &op.kind {
                PackedReplayKind::Insert(range) => ReplayKind::Insert(PackedRow {
                    values: &self.values[range.clone()],
                    payload: &self.payload,
                }),
                PackedReplayKind::Update(range) => ReplayKind::Update(PackedUpdates {
                    updates: &self.updates[range.clone()],
                    payload: &self.payload,
                }),
                PackedReplayKind::Delete => ReplayKind::Delete,
            },
        })
    }

    #[inline]
    fn clear(&mut self) {
        self.ops.clear();
        self.values.clear();
        self.updates.clear();
        self.payload.clear();
        self.oversized = false;
    }
}

/// Coordinator-only LIFO cache, independent of page identity and insertion history.
pub(super) struct BatchPool {
    batches: Vec<PackedPageBatch>,
    retained: usize,
    max_bytes: usize,
    max_entries: usize,
    high_water: usize,
}

impl BatchPool {
    /// Bind idle capacity limits; zero bytes disables recycling.
    pub(super) fn new(target: usize, max_bytes: usize, max_entries: usize) -> Self {
        Self {
            batches: Vec::new(),
            retained: 0,
            max_bytes,
            max_entries,
            high_water: target.saturating_mul(2),
        }
    }

    /// Reuse the most recently collected batch, or start with empty vectors.
    #[inline]
    pub(super) fn acquire(&mut self) -> PackedPageBatch {
        let Some(batch) = self.batches.pop() else {
            return PackedPageBatch::default();
        };
        self.retained -= batch.retained_bytes();
        batch
    }

    /// Best-effort recycling after exclusive successful completion collection.
    pub(super) fn recycle(&mut self, mut batch: PackedPageBatch) {
        let bytes = batch.retained_bytes();
        if batch.oversized
            || bytes > self.high_water
            || self.batches.len() >= self.max_entries
            || self.max_bytes == 0
        {
            return;
        }
        // The pool always retains at most max_bytes; compare the remaining budget
        // before adding this batch's actual capacity.
        if bytes > self.max_bytes - self.retained {
            return;
        }
        batch.clear();
        self.retained += bytes;
        self.batches.push(batch);
    }

    /// Release idle transport before hot-index reconstruction or after failure.
    pub(super) fn clear(&mut self) {
        self.batches = Vec::new();
        self.retained = 0;
    }
}

#[inline]
fn storage_bytes(ops: usize, values: usize, updates: usize, payload: usize) -> usize {
    // Counts describe live, nonzero-sized vector storage or an admission view
    // into it, plus one operation descriptor. Payload bytes were bounded during
    // decoding. These sizes are internal accounting, not unchecked wire lengths.
    ops * size_of::<PackedReplayOp>()
        + values * size_of::<PackedValue>()
        + updates * size_of::<PackedUpdate>()
        + payload
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::USER_TABLE_ID_START;
    use crate::id::PageID;
    use crate::log::block_group::TrxLog;
    use crate::log::redo::{RedoHeader, RedoLogs, RedoTrxKind, RowRedo};
    use crate::recovery::decode::{
        DecodedGroup, DecodedRow, DecodedTable, DecodedTrxKind, decode_log,
    };

    /// Owning fixture input, serialized through the real packed recovery decoder.
    pub(crate) struct OwnedReplayOp {
        /// Commit timestamp for the fixture.
        pub(crate) cts: TrxID,
        /// Owning reference row.
        pub(crate) row: RowRedo,
    }

    /// Build worker input from owning test operations using production packing.
    pub(crate) fn pack_test_ops(ops: impl IntoIterator<Item = OwnedReplayOp>) -> PackedPageBatch {
        let mut batch = PackedPageBatch::default();
        for op in ops {
            let cts = op.cts;
            let (group, row) = decode_test_op(op);
            batch.append(&group.operation(&row, cts), usize::MAX);
        }
        batch
    }

    /// Decode the fixture as a one-row ordinary user-table transaction.
    pub(in crate::recovery) fn decode_test_op(op: OwnedReplayOp) -> (DecodedGroup, DecodedRow) {
        let mut redo = RedoLogs::default();
        redo.insert_dml(USER_TABLE_ID_START, op.row);
        let log = TrxLog::new(
            RedoHeader {
                cts: op.cts,
                trx_kind: RedoTrxKind::User,
            },
            redo,
        );
        let mut group = decode_log(&log);
        let DecodedTrxKind::Dml(mut tables) = group.transactions.pop().unwrap().kind else {
            panic!()
        };
        let DecodedTable::User(mut rows) = tables.remove(&USER_TABLE_ID_START).unwrap() else {
            panic!()
        };
        (group, rows.pop_first().unwrap().1)
    }

    /// Inspect entry count and actual retained capacity in scheduler tests.
    pub(in crate::recovery) fn pool_snapshot(pool: &BatchPool) -> (usize, usize) {
        (pool.batches.len(), pool.retained)
    }

    fn sample_batch() -> PackedPageBatch {
        use crate::log::redo::RowRedoKind;
        use crate::value::Val;
        pack_test_ops([OwnedReplayOp {
            cts: TrxID::new(1),
            row: RowRedo {
                row_id: RowID::new(1),
                kind: RowRedoKind::Insert(
                    PageID::new(2),
                    vec![Val::Null, Val::from("sample bytes"), Val::I32(17)],
                ),
            },
        }])
    }

    #[test]
    fn recycling_uses_actual_capacity_caps_and_clears_all_lengths() {
        let batch = sample_batch();
        let bytes = batch.retained_bytes();
        assert!(bytes >= batch.used_bytes());
        let pointers = (
            batch.ops.as_ptr(),
            batch.values.as_ptr(),
            batch.payload.as_ptr(),
        );
        let mut pool = BatchPool::new(bytes, bytes, 1);
        pool.recycle(batch);
        assert_eq!(pool_snapshot(&pool), (1, bytes)); // Equality fits the byte and entry cap.
        pool.recycle(sample_batch());
        assert_eq!(pool_snapshot(&pool), (1, bytes));
        let reused = pool.acquire();
        assert_eq!(pool_snapshot(&pool), (0, 0));
        assert_eq!(reused.used_bytes(), 0);
        assert_eq!(
            pointers,
            (
                reused.ops.as_ptr(),
                reused.values.as_ptr(),
                reused.payload.as_ptr()
            )
        );
        pool.recycle(reused);
        pool.clear();
        assert_eq!(pool_snapshot(&pool), (0, 0));
        assert_eq!(pool.acquire().retained_bytes(), 0);
        for (target, cap, entries) in [
            (bytes, 0, 1),
            (bytes, bytes - 1, 1),
            (bytes, bytes, 0),
            (1, bytes, 1),
        ] {
            let mut pool = BatchPool::new(target, cap, entries);
            pool.recycle(sample_batch());
            assert_eq!(pool_snapshot(&pool), (0, 0));
        }
        let mut oversized = sample_batch();
        oversized.oversized = true;
        pool.recycle(oversized);
        assert_eq!(pool_snapshot(&pool), (0, 0));
        // Overflow-safe high-water threshold retains an otherwise eligible batch.
        let mut huge = BatchPool::new(usize::MAX, usize::MAX, usize::MAX);
        huge.recycle(sample_batch());
        assert_eq!(pool_snapshot(&huge), (1, bytes));
    }

    #[test]
    fn spare_capacity_and_aggregate_pool_limits_are_accounted() {
        let mut batch = sample_batch();
        let used = batch.used_bytes();
        batch.updates.reserve(64); // Even unused vectors count their actual capacity.
        let capacity = batch.retained_bytes();
        assert!(capacity > used * 2);
        let mut pool = BatchPool::new(used, usize::MAX, 4);
        pool.recycle(batch);
        assert_eq!(pool_snapshot(&pool), (0, 0));
        let capacity = sample_batch().retained_bytes();
        let mut pool = BatchPool::new(capacity, capacity * 2, 4);
        pool.recycle(sample_batch());
        pool.recycle(sample_batch());
        pool.recycle(sample_batch());
        assert_eq!(pool_snapshot(&pool), (2, capacity * 2));
    }
}
