//! Bounded persisted reads for completed cold trees. Traversal and admission
//! live here; cross-tier strategy selection and coverage live in `cross_tier`.
//! Lookup batches borrow keys or leaf slots; branch keys are never flattened.
use super::merge::execution_error;
use super::sealed_cold::ColdReadAuthority;
use super::{BuildOutcome, MemoryReservation};
use crate::error::{RuntimeError, RuntimeOrFatalError, RuntimeOrFatalResult, RuntimeResult};
use crate::id::{BlockID, RowID};
use crate::index::btree::{BTreeEntryView, BTreeU64};
use crate::index::disk_tree::{
    DISK_TREE_BLOCK_SIZE, DiskTreeNodeGuard, DiskTreeNodeView, UniqueDiskTreeSpec,
};
use crate::runtime::yield_now;
use error_stack::ResultExt;
use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

#[cfg(test)]
pub(crate) use tests::{collect as test_collect, scalar_lookup, scalar_next};

// A validated key is prefix + suffix; each component is bounded by a block.
/// Maximum decoded prefix-plus-suffix capacity, checked before allocation.
pub(crate) const COLD_KEY_BYTES: usize = 2 * DISK_TREE_BLOCK_SIZE;

/// Maximum borrowed keys or leaf slots processed by one lookup batch.
pub(super) const LOOKUP_BATCH_ENTRIES: usize = 128;

/// Forward frontier, borrowed query references and ordered result slots.
pub(super) const FORWARD_LOOKUP_BYTES: usize = LOOKUP_BATCH_ENTRIES
    * (size_of::<LookupSpan>() + size_of::<&[u8]>() + size_of::<Option<RowID>>());

struct LookupSpan {
    block: BlockID,
    height: u16,
    keys: Range<usize>,
}

/// Borrowed encoded slots from one validated leaf; no key payload is copied.
pub(super) struct ColdBatch<'a> {
    node: DiskTreeNodeView<'a, UniqueDiskTreeSpec>,
    slots: Range<usize>,
}

impl ColdBatch<'_> {
    /// Number of entries in this borrowed view.
    #[inline]
    pub(super) fn len(&self) -> usize {
        self.slots.len()
    }

    /// Compare a complete encoded cold key with a borrowed hot key or endpoint.
    #[inline]
    pub(super) fn compare_key(&self, index: usize, key: &[u8]) -> RuntimeResult<Ordering> {
        assert!(index < self.len(), "cold batch comparison index");
        self.node.compare_slot_key(self.slots.start + index, key)
    }

    /// Row belonging to the selected leaf slot.
    #[inline]
    pub(super) fn row_id(&self, index: usize) -> RuntimeResult<RowID> {
        assert!(index < self.len(), "cold batch row index");
        self.node.row_id(self.slots.start + index)
    }

    /// Check one entry once before repeated comparisons with resident hot keys.
    #[inline]
    pub(super) fn entry(&self, index: usize) -> RuntimeResult<BTreeEntryView<'_, BTreeU64>> {
        assert!(index < self.len(), "cold batch entry index");
        self.node.leaf_entry(self.slots.start + index)
    }

    /// Restrict a view to keys preceding the exclusive range endpoint.
    pub(super) fn before(mut self, upper: Option<&[u8]>) -> RuntimeResult<Self> {
        if let Some(upper) = upper {
            let (mut low, mut high) = (self.slots.start, self.slots.end);
            while low < high {
                let mid = low + (high - low) / 2;
                if self.node.compare_slot_key(mid, upper)?.is_lt() {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            self.slots.end = low;
        }
        Ok(self)
    }
}

/// Shared reservation retained by accepted jobs through their last persisted read.
/// Each worker pins at most one block. Checked entry views borrow that pin;
/// admission reserves traversal and lookup scratch plus owned range boundaries.
#[derive(Clone)]
pub(crate) struct ColdReadAdmission {
    reservation: Arc<MemoryReservation>,
}

impl ColdReadAdmission {
    /// Admit bounded workers, traversal paths, selected cuts and job bookkeeping.
    pub(crate) fn new(
        cold: &ColdReadAuthority,
        workers: usize,
        ranges: usize,
        reverse_lookup_bytes: usize,
    ) -> RuntimeOrFatalResult<Self> {
        let per_reader = (usize::from(cold.height()) + 1) * size_of::<Frame>();
        let per_reader = per_reader
            .checked_add(FORWARD_LOOKUP_BYTES.max(reverse_lookup_bytes))
            .ok_or_else(|| execution_error("cold lookup admission size overflow"))?;
        let bytes = workers
            .checked_mul(per_reader)
            .and_then(|bytes| bytes.checked_add(ranges.checked_mul(COLD_KEY_BYTES + 1024)?))
            .ok_or_else(|| execution_error("cold reader admission size overflow"))?;
        Ok(Self {
            reservation: Arc::new(
                cold.budget()
                    .reserve(bytes, "cross-tier readers and jobs")
                    .change_context(RuntimeError::IndexAccess)?,
            ),
        })
    }

    /// Admitted bytes, separate from readonly-pool pins and storage IO buffers.
    #[cfg(feature = "profiling")]
    pub(crate) fn bytes(&self) -> usize {
        self.reservation.bytes()
    }
}

struct Frame {
    block: BlockID,
    height: u16,
    next: usize,
    end: usize,
}

/// One forward cursor with a height-bounded path and one pinned leaf.
pub(crate) struct ColdReader {
    cold: ColdReadAuthority,
    path: Vec<Frame>,
    pending: Vec<LookupSpan>,
    leaf: Option<DiskTreeNodeGuard<UniqueDiskTreeSpec>>,
    slot: usize,
    /// Exact full-key comparisons made by build searches and ordered scanning.
    pub(crate) comparisons: u64,
    /// Cold keys inspected by an ordered cursor or decoded by a range scan.
    pub(crate) scanned: u64,
    /// Persisted node accesses by this reader, including cache hits.
    #[cfg(any(test, feature = "profiling"))]
    pub(crate) node_reads: u64,
    // Keep admission until the path and pinned node have been destroyed.
    _admission: ColdReadAdmission,
}

impl ColdReader {
    /// Allocate only after the owning plan has admitted every concurrent reader.
    pub(crate) fn new(cold: ColdReadAuthority, admission: ColdReadAdmission) -> Self {
        Self {
            path: Vec::with_capacity(usize::from(cold.height()) + 1),
            pending: Vec::new(),
            cold,
            _admission: admission,
            leaf: None,
            slot: 0,
            comparisons: 0,
            scanned: 0,
            #[cfg(any(test, feature = "profiling"))]
            node_reads: 0,
        }
    }

    async fn read(
        &mut self,
        block: BlockID,
        height: u16,
    ) -> RuntimeOrFatalResult<DiskTreeNodeGuard<UniqueDiskTreeSpec>> {
        #[cfg(any(test, feature = "profiling"))]
        {
            self.node_reads += 1;
        }
        let tree = self.cold.tree()?;
        let read_use = self.cold.read_use(&self._admission.reservation);
        let result = tree.read_node(block).await;
        read_use.finish(matches!(&result, Err(RuntimeOrFatalError::Fatal(_))));
        let node = result?;
        if node.view().height() != usize::from(height) {
            return Err(execution_error("sealed tree child height mismatch"));
        }
        Ok(node)
    }

    /// Resolve sorted borrowed keys with one visit per required subtree.
    /// Results retain input order, including repeated keys and misses.
    /// Yield after traversal, then observe cooperative stop at the batch boundary.
    pub(super) async fn lookup_batch(
        &mut self,
        keys: &[&[u8]],
        hits: &mut [Option<RowID>],
        stop: &AtomicBool,
    ) -> RuntimeOrFatalResult<BuildOutcome<()>> {
        assert!(
            keys.len() <= LOOKUP_BATCH_ENTRIES,
            "cold lookup batch capacity"
        );
        assert_eq!(keys.len(), hits.len(), "cold lookup result length");
        // The merge establishes sorted input; tests may also supply sorted slices.
        debug_assert!(keys.is_sorted());
        if stop.load(AtomicOrdering::Acquire) {
            return Ok(BuildOutcome::Stopped);
        }
        hits.fill(None);
        self.leaf = None;
        self.path.clear();
        self.pending.clear();
        if keys.is_empty() {
            return Ok(BuildOutcome::Complete(()));
        }
        let Some(block) = self.cold.root() else {
            return Ok(BuildOutcome::Complete(()));
        };
        if self.pending.capacity() == 0 {
            self.pending = Vec::with_capacity(LOOKUP_BATCH_ENTRIES);
        }
        self.pending.push(LookupSpan {
            block,
            height: self.cold.height(),
            keys: 0..keys.len(),
        });
        while let Some(span) = self.pending.pop() {
            let guard = self.read(span.block, span.height).await?;
            let node = guard.view();
            if node.is_leaf() {
                for index in span.keys {
                    let slot = lower_bound(node, keys[index], false, &mut self.comparisons)?;
                    if slot < node.count() {
                        self.comparisons += 1;
                        if node.compare_slot_key(slot, keys[index])?.is_eq() {
                            hits[index] = Some(node.row_id(slot)?);
                        }
                    }
                }
            } else {
                let pending_start = self.pending.len();
                let mut start = span.keys.start;
                while start < span.keys.end {
                    let child = lower_bound(node, keys[start], true, &mut self.comparisons)?;
                    // Equal separator keys belong to the right child. Search
                    // query endpoints without copying any branch separator.
                    let end = if child == node.count() {
                        span.keys.end
                    } else {
                        let mut low = start + 1;
                        let mut high = span.keys.end;
                        while low < high {
                            let mid = low + (high - low) / 2;
                            self.comparisons += 1;
                            if node.compare_slot_key(child, keys[mid])?.is_gt() {
                                low = mid + 1;
                            } else {
                                high = mid;
                            }
                        }
                        low
                    };
                    // Nonempty disjoint query ranges bound this frontier by
                    // the admitted batch size, regardless of tree height.
                    assert!(
                        self.pending.len() < LOOKUP_BATCH_ENTRIES,
                        "cold lookup frontier capacity"
                    );
                    self.pending.push(LookupSpan {
                        block: child_block(node, child)?,
                        height: span.height - 1,
                        keys: start..end,
                    });
                    start = end;
                }
                self.pending[pending_start..].reverse();
            }
            drop(guard);
        }
        yield_now().await;
        Ok(if stop.load(AtomicOrdering::Acquire) {
            BuildOutcome::Stopped
        } else {
            BuildOutcome::Complete(())
        })
    }

    /// Seek from the exact sealed root, retaining only child coordinates.
    pub(crate) async fn seek(&mut self, key: &[u8]) -> RuntimeOrFatalResult<()> {
        self.leaf = None;
        self.path.clear();
        let Some(mut block) = self.cold.root() else {
            return Ok(());
        };
        let mut height = self.cold.height();
        loop {
            let guard = self.read(block, height).await?;
            let node = guard.view();
            if node.is_leaf() {
                self.slot = lower_bound(node, key, false, &mut self.comparisons)?;
                self.leaf = Some(guard);
                return Ok(());
            }
            let child = lower_bound(node, key, true, &mut self.comparisons)?;
            let next = child_block(node, child)?;
            self.path.push(Frame {
                block,
                height,
                next: child + 1,
                end: node.count() + 1,
            });
            block = next;
            height -= 1;
        }
    }

    /// Advance to the next leaf while bounding pins independently of tree width.
    async fn advance_leaf(&mut self) -> RuntimeOrFatalResult<bool> {
        self.leaf = None;
        while let Some(mut frame) = self.path.pop() {
            if frame.next == frame.end {
                continue;
            }
            let guard = self.read(frame.block, frame.height).await?;
            let mut block = child_block(guard.view(), frame.next)?;
            let mut height = frame.height - 1;
            frame.next += 1;
            self.path.push(frame);
            drop(guard);
            loop {
                let guard = self.read(block, height).await?;
                let node = guard.view();
                if node.is_leaf() {
                    self.slot = 0;
                    self.leaf = Some(guard);
                    return Ok(true);
                }
                let next = child_block(node, 0)?;
                self.path.push(Frame {
                    block,
                    height,
                    next: 1,
                    end: node.count() + 1,
                });
                block = next;
                height -= 1;
            }
        }
        Ok(false)
    }

    /// Borrow the next bounded leaf view; advancing requires releasing it.
    pub(super) async fn next_batch(&mut self) -> RuntimeOrFatalResult<Option<ColdBatch<'_>>> {
        while self
            .leaf
            .as_ref()
            .is_none_or(|leaf| self.slot == leaf.view().count())
        {
            if !self.advance_leaf().await? {
                return Ok(None);
            }
        }
        let node = self
            .leaf
            .as_ref()
            .unwrap_or_else(|| unreachable!("nonempty cold leaf"))
            .view();
        let start = self.slot;
        self.slot += LOOKUP_BATCH_ENTRIES.min(node.count() - start);
        Ok(Some(ColdBatch {
            node,
            slots: start..self.slot,
        }))
    }

    /// Monotonic lower-bound lookup in the current leaf, reseeking across gaps.
    /// This avoids scanning unrelated cold intervals between distant hot keys.
    pub(crate) async fn seek_match(&mut self, key: &[u8]) -> RuntimeOrFatalResult<Option<RowID>> {
        // A seek may select the first leaf even for keys below its lower
        // fence. Monotonic hot input can reuse that position without another IO.
        if let Some(leaf) = &self.leaf
            && self.slot < leaf.view().count()
        {
            self.comparisons += 1;
            match leaf.view().compare_slot_key(self.slot, key)? {
                Ordering::Equal => {
                    return Ok(Some(leaf.view().row_id(self.slot)?));
                }
                Ordering::Greater => return Ok(None),
                Ordering::Less => (),
            }
        }
        let in_leaf = self.leaf.as_ref().is_some_and(|leaf| {
            key.starts_with(leaf.view().common_prefix()) && leaf.view().within_boundary(key)
        });
        if !in_leaf {
            // Adjacent leaves are the usual ordered case. One bounded advance
            // followed by a root seek handles arbitrarily large gaps efficiently.
            if self.leaf.is_some() {
                self.advance_leaf().await?;
            }
            if !self.leaf.as_ref().is_some_and(|leaf| {
                key.starts_with(leaf.view().common_prefix()) && leaf.view().within_boundary(key)
            }) {
                self.seek(key).await?;
            }
        }
        let Some(leaf) = &self.leaf else {
            return Ok(None);
        };
        let node = leaf.view();
        while self.slot < node.count() {
            self.comparisons += 1;
            self.scanned += 1;
            match node.compare_slot_key(self.slot, key)? {
                Ordering::Less => self.slot += 1,
                Ordering::Equal => {
                    return Ok(Some(node.row_id(self.slot)?));
                }
                Ordering::Greater => break,
            }
        }
        Ok(None)
    }
}

/// One disjoint half-open cold interval. Open endpoints cover both outer tails.
#[derive(Clone)]
pub(crate) struct ColdRange {
    /// Inclusive lower key, absent for the first range.
    pub(crate) lower: Option<Arc<Vec<u8>>>,
    /// Exclusive upper key, absent for the last range.
    pub(crate) upper: Option<Arc<Vec<u8>>>,
}

struct Span {
    range: ColdRange,
    block: BlockID,
    height: u16,
    start: usize,
    end: Option<usize>,
    terminal: bool,
}

/// Refine a bounded structural frontier; only selected separator keys are copied.
/// Subtree spans are split before leaf slots, independently of hot-key placement.
pub(crate) async fn plan_ranges(
    cold: ColdReadAuthority,
    admission: ColdReadAdmission,
    count: usize,
    stop: Arc<AtomicBool>,
) -> RuntimeOrFatalResult<BuildOutcome<Vec<ColdRange>>> {
    let mut spans = Vec::with_capacity(count);
    if let Some(block) = cold.root() {
        spans.push(Span {
            range: ColdRange {
                lower: None,
                upper: None,
            },
            block,
            height: cold.height(),
            start: 0,
            end: None,
            terminal: false,
        });
    }
    let mut reader = ColdReader::new(cold.clone(), admission);
    while spans.len() < count {
        if stop.load(AtomicOrdering::Acquire) {
            return Ok(BuildOutcome::Stopped);
        }
        let Some(index) = spans
            .iter()
            .enumerate()
            .filter(|(_, span)| !span.terminal)
            .max_by_key(|(_, span)| {
                (
                    span.height,
                    span.end.unwrap_or(usize::MAX).saturating_sub(span.start),
                )
            })
            .map(|(index, _)| index)
        else {
            break;
        };
        let span = &mut spans[index];
        let guard = reader.read(span.block, span.height).await?;
        let node = guard.view();
        let end = *span
            .end
            .get_or_insert(node.count() + usize::from(!node.is_leaf()));
        if end - span.start < 2 {
            if node.is_leaf() {
                span.terminal = true;
            } else {
                span.block = child_block(node, span.start)?;
                span.height -= 1;
                span.start = 0;
                span.end = None;
            }
        } else {
            let mid = span.start + (end - span.start) / 2;
            let boundary = Arc::new(decode_key(node, mid - usize::from(!node.is_leaf()))?);
            let right = Span {
                range: ColdRange {
                    lower: Some(boundary.clone()),
                    upper: span.range.upper.take(),
                },
                block: span.block,
                height: span.height,
                start: mid,
                end: Some(end),
                terminal: false,
            };
            span.range.upper = Some(boundary);
            span.end = Some(mid);
            spans.push(right);
        }
        drop(guard);
        yield_now().await;
    }
    spans.sort_unstable_by(|left, right| left.range.lower.cmp(&right.range.lower));
    Ok(BuildOutcome::Complete(
        spans.into_iter().map(|span| span.range).collect(),
    ))
}

fn lower_bound(
    node: DiskTreeNodeView<'_, UniqueDiskTreeSpec>,
    key: &[u8],
    upper: bool,
    comparisons: &mut u64,
) -> RuntimeResult<usize> {
    let (mut low, mut high) = (0, node.count());
    while low < high {
        let mid = low + (high - low) / 2;
        *comparisons += 1;
        let ordering = node.compare_slot_key(mid, key)?;
        if ordering.is_lt() || (upper && ordering.is_eq()) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    Ok(low)
}

#[inline]
fn child_block(
    node: DiskTreeNodeView<'_, UniqueDiskTreeSpec>,
    child: usize,
) -> RuntimeResult<BlockID> {
    node.child_block(child)
}

fn decode_key(
    node: DiskTreeNodeView<'_, UniqueDiskTreeSpec>,
    slot: usize,
) -> RuntimeOrFatalResult<Vec<u8>> {
    let key = node.key(slot)?;
    if key.len() > COLD_KEY_BYTES {
        return Err(execution_error("sealed key exceeds admitted decode bound"));
    }
    Ok(key.to_vec())
}

#[cfg(test)]
mod tests {
    use super::super::complete_work;
    use super::*;
    use crate::index::BTreeKey;
    use crate::index::build::IndexBuildEntry;
    use crate::index::build::cross_tier::test_with_cold;

    /// Scalar reference retained only for tests and release calibration.
    pub(crate) async fn scalar_lookup(
        reader: &mut ColdReader,
        key: &[u8],
    ) -> RuntimeOrFatalResult<Option<RowID>> {
        let Some(mut block) = reader.cold.root() else {
            return Ok(None);
        };
        let mut height = reader.cold.height();
        loop {
            let guard = reader.read(block, height).await?;
            let node = guard.view();
            if node.is_leaf() {
                let slot = lower_bound(node, key, false, &mut reader.comparisons)?;
                if slot == node.count() {
                    return Ok(None);
                }
                reader.comparisons += 1;
                return Ok(if node.compare_slot_key(slot, key)?.is_eq() {
                    Some(node.row_id(slot)?)
                } else {
                    None
                });
            }
            let child = lower_bound(node, key, true, &mut reader.comparisons)?;
            block = child_block(node, child)?;
            height -= 1;
        }
    }

    /// Decode a single key using the original scalar scan for calibration.
    pub(crate) async fn scalar_next(
        reader: &mut ColdReader,
    ) -> RuntimeOrFatalResult<Option<(Vec<u8>, RowID)>> {
        loop {
            if let Some(leaf) = &reader.leaf {
                let node = leaf.view();
                if reader.slot < node.count() {
                    let key = decode_key(node, reader.slot)?;
                    let row = node.row_id(reader.slot)?;
                    reader.slot += 1;
                    return Ok(Some((key, row)));
                }
            }
            if !reader.advance_leaf().await? {
                return Ok(None);
            }
        }
    }

    /// Collect test-owned keys through the production borrowed batch interface.
    pub(crate) async fn collect(reader: &mut ColdReader) -> Vec<(Vec<u8>, RowID)> {
        let mut entries = Vec::new();
        while let Some(batch) = reader.next_batch().await.unwrap() {
            for index in 0..batch.len() {
                entries.push((
                    decode_key(batch.node, batch.slots.start + index).unwrap(),
                    batch.row_id(index).unwrap(),
                ));
            }
        }
        entries
    }

    fn fixture_key(value: u32, width: usize) -> Vec<u8> {
        let mut bytes = vec![11; 24];
        bytes.extend_from_slice(&value.to_be_bytes());
        bytes.resize(width.max(bytes.len()), 7);
        bytes
    }

    /// Purpose: Exercise shared subtree routing and borrowed leaf batches across key and batch boundaries.
    /// Expected: Every hit, miss, repeated query and clipped leaf agrees with an independent ordered map, without growing lookup scratch.
    #[test]
    fn batched_reads_match_ordered_map() {
        use std::collections::BTreeMap;
        smol::block_on(async {
            for width in [32, 512] {
                let oracle: BTreeMap<_, _> = (0..4096u32)
                    .step_by(2)
                    .map(|value| (fixture_key(value, width), RowID::new(u64::from(value) + 10)))
                    .collect();
                let entries = oracle
                    .iter()
                    .map(|(key, row)| IndexBuildEntry {
                        key: BTreeKey::from(key.as_slice()),
                        row_id: *row,
                    })
                    .collect();
                test_with_cold(entries, async |cold| {
                    let admission = ColdReadAdmission::new(&cold, 1, 1, 0).unwrap();
                    let mut reader = ColdReader::new(cold, admission);
                    let stop = AtomicBool::new(false);
                    let mut queries: Vec<_> =
                        (0..4100).map(|value| fixture_key(value, width)).collect();
                    queries.extend([vec![], vec![10], vec![12], fixture_key(128, width)]);
                    queries.sort();
                    for count in [1, 127, 128] {
                        for chunk in queries.chunks(count) {
                            let keys: Vec<_> = chunk.iter().map(Vec::as_slice).collect();
                            let mut hits = vec![None; keys.len()];
                            complete_work(
                                reader.lookup_batch(&keys, &mut hits, &stop).await.unwrap(),
                            );
                            for (key, hit) in keys.iter().zip(hits) {
                                assert_eq!(
                                    hit,
                                    oracle.get(*key).copied(),
                                    "width={width}, count={count}, key={key:?}"
                                );
                            }
                            assert!(reader.pending.capacity() <= LOOKUP_BATCH_ENTRIES);
                            assert!(reader.leaf.is_none());
                        }
                    }
                    complete_work(reader.lookup_batch(&[], &mut [], &stop).await.unwrap());
                    reader.seek(&[]).await.unwrap();
                    let upper = fixture_key(513, width);
                    let mut actual = Vec::new();
                    while let Some(batch) = reader.next_batch().await.unwrap() {
                        assert!(batch.len() <= LOOKUP_BATCH_ENTRIES);
                        let batch = batch.before(Some(&upper)).unwrap();
                        if batch.len() == 0 {
                            break;
                        }
                        for index in 0..batch.len() {
                            let value = u32::try_from(actual.len()).unwrap() * 2;
                            assert!(
                                batch
                                    .compare_key(index, &fixture_key(value, width))
                                    .unwrap()
                                    .is_eq()
                            );
                            actual.push(batch.row_id(index).unwrap());
                        }
                    }
                    assert_eq!(
                        actual,
                        oracle
                            .range(..upper)
                            .map(|(_, row)| *row)
                            .collect::<Vec<_>>()
                    );
                    stop.store(true, AtomicOrdering::Release);
                    let reads = reader.node_reads;
                    assert_eq!(
                        reader
                            .lookup_batch(&[&queries[0]], &mut [None], &stop)
                            .await
                            .unwrap(),
                        BuildOutcome::Stopped
                    );
                    assert_eq!(reader.node_reads, reads, "stop precedes additional reads");
                })
                .await;
            }
        });
    }

    /// Purpose: Distinguish shared batch traversal from a loop of scalar point lookups.
    /// Expected: A leaf-root batch visits the persisted node once while returning the same results as all scalar probes.
    #[test]
    fn lookup_batch_shares_node_accesses() {
        smol::block_on(async {
            let entries = (0..512u32)
                .map(|value| IndexBuildEntry {
                    key: BTreeKey::from(value.to_be_bytes().as_slice()),
                    row_id: RowID::new(u64::from(value)),
                })
                .collect();
            test_with_cold(entries, async |cold| {
                assert_eq!(cold.height(), 0);
                let admission = ColdReadAdmission::new(&cold, 2, 1, 0).unwrap();
                let mut scalar = ColdReader::new(cold.clone(), admission.clone());
                let mut batch = ColdReader::new(cold, admission);
                let keys: Vec<_> = (0..128u32).map(u32::to_be_bytes).collect();
                let borrowed: Vec<_> = keys.iter().map(|key| key.as_slice()).collect();
                let mut expected = Vec::new();
                for key in &borrowed {
                    expected.push(scalar_lookup(&mut scalar, key).await.unwrap());
                }
                let mut hits = vec![None; borrowed.len()];
                complete_work(
                    batch
                        .lookup_batch(&borrowed, &mut hits, &AtomicBool::new(false))
                        .await
                        .unwrap(),
                );
                assert_eq!(hits, expected);
                assert_eq!(scalar.node_reads, 128);
                assert_eq!(batch.node_reads, 1);
            })
            .await;
        });
    }

    /// Purpose: Reject overflow in common batch admission before allocating scratch or reading a tree.
    /// Expected: Oversized per-worker scratch and worker counts fail without charging the cold budget.
    #[test]
    fn lookup_admission_checks_overflow() {
        smol::block_on(async {
            test_with_cold(vec![], async |cold| {
                assert!(ColdReadAdmission::new(&cold, 1, 1, usize::MAX).is_err());
                assert!(ColdReadAdmission::new(&cold, usize::MAX, 1, 0).is_err());
                assert_eq!(cold.budget().used(), 0);
            })
            .await;
        });
    }

    /// Purpose: Retain reader admission after its initiating owner and an earlier reader are dropped.
    /// Expected: Shared readers preserve one charge and exact sealed-tree contents until the final reader releases the reservation.
    #[test]
    fn readers_retain_admission_until_last_release() {
        smol::block_on(async {
            let expected: Vec<_> = (0..512u32)
                .map(|value| {
                    let mut key = value.to_be_bytes().to_vec();
                    key.resize(256, 7);
                    (key, RowID::new(u64::from(value) + 100_000))
                })
                .collect();
            let entries = expected
                .iter()
                .map(|(key, row_id)| IndexBuildEntry {
                    key: BTreeKey::from(key.as_slice()),
                    row_id: *row_id,
                })
                .collect();
            test_with_cold(entries, async |cold| {
                let budget = cold.budget().clone();
                let admission = ColdReadAdmission::new(&cold, 2, 1, 0).unwrap();
                let charged = budget.used();
                assert!(charged > 0);
                let mut probe = ColdReader::new(cold.clone(), admission.clone());
                let mut scan = ColdReader::new(cold, admission.clone());
                assert_eq!(budget.used(), charged, "reader clones share one charge");
                drop(admission);
                assert_eq!(budget.used(), charged, "readers retain planner admission");
                let (key, row) = &expected[expected.len() / 2];
                let mut hit = [None];
                complete_work(
                    probe
                        .lookup_batch(&[key], &mut hit, &AtomicBool::new(false))
                        .await
                        .unwrap(),
                );
                assert_eq!(hit, [Some(*row)]);
                drop(probe);
                assert_eq!(
                    budget.used(),
                    charged,
                    "the remaining reader stays admitted"
                );
                scan.seek(&[]).await.unwrap();
                assert_eq!(collect(&mut scan).await, expected);
                assert!(scan.next_batch().await.unwrap().is_none());
                assert_eq!(
                    budget.used(),
                    charged,
                    "exhaustion retains reader ownership"
                );
                drop(scan);
                assert_eq!(
                    budget.used(),
                    0,
                    "the final reader releases the reservation"
                );
            })
            .await;
        });
    }
}
