use super::IndexLifecycleInstallContext;
use crate::buffer::EvictableBufferPool;
use crate::catalog::{ID_DOMAIN_END, IndexRef, IndexSlot, SecondaryIndexSlot, TableMetadata};
use crate::error::{DataIntegrityError, DataIntegrityResult, OperationError, OperationResult};
use crate::file::table_file::ActiveRoot;
use crate::id::TrxID;
use crate::index::SecondaryIndex;
use error_stack::Report;
use std::collections::{BTreeMap, btree_map::Entry};
use std::sync::Arc;

/// Authority-qualified placement selected for one CREATE INDEX operation.
///
/// A placement is a tentative capability produced from the Table lifecycle
/// state, not a caller-selected slot hint. Installation must revalidate the
/// represented lifecycle condition before publishing the new active layout;
/// a stale placement must fail rather than weaken slot-reuse safety.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexPlacement {
    /// Allocate a slot at or beyond the current durable slot count.
    Append(IndexSlot),
    /// Reuse one in-range slot whose durable state is exactly `Vacant`.
    ReuseVacant(IndexSlot),
    /// Reuse the hole left by one exact retired generation.
    ///
    /// The contained [`IndexRef`] identifies the old retired generation whose
    /// physical slot is being reclaimed; it is not the identity allocated to
    /// the new index. This placement is legal only while the lifecycle still
    /// contains that exact generation, its DROP is checkpoint-covered, its
    /// runtime is vacant after destruction, and no provisional CREATE overlays
    /// the slot. CREATE allocates a fresh `IndexID`, and installation validates
    /// all of these conditions again before consuming the retirement.
    ReuseRetired(IndexRef),
}

impl IndexPlacement {
    /// Returns the selected physical slot.
    #[inline]
    pub(crate) const fn slot(self) -> IndexSlot {
        match self {
            Self::Append(slot) | Self::ReuseVacant(slot) => slot,
            Self::ReuseRetired(index) => index.slot(),
        }
    }
}

/// Current metadata plus the process-local effective index-ID watermark.
pub(crate) struct CurrentDefinitionAllocatorView {
    metadata: Arc<TableMetadata>,
    effective_next_index_id: u64,
}

impl CurrentDefinitionAllocatorView {
    /// Returns the current authoritative metadata owner.
    #[inline]
    pub(crate) fn metadata(&self) -> &Arc<TableMetadata> {
        &self.metadata
    }

    /// Returns the exclusive process-local stable-ID watermark.
    #[inline]
    pub(crate) const fn effective_next_index_id(&self) -> u64 {
        self.effective_next_index_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetirementDurability {
    RecoveryUnclassified,
    AwaitingCheckpoint { drop_cts: TrxID },
    CheckpointCovered,
}

enum RuntimeRetirement {
    Retained {
        layout_generation: u64,
        runtime: Arc<SecondaryIndex<EvictableBufferPool>>,
    },
    Destroying {
        layout_generation: u64,
    },
    Vacant,
}

struct RetiredSlot {
    index: IndexRef,
    durability: RetirementDurability,
    runtime: RuntimeRetirement,
}

enum SlotBase {
    Unallocated,
    DurableVacant,
    Retired(RetiredSlot),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProvisionalCreate {
    index: IndexRef,
    create_cts: TrxID,
}

struct SlotLifecycle {
    base: SlotBase,
    provisional: Option<ProvisionalCreate>,
}

/// One runtime selected for asynchronous exact destruction.
pub(crate) struct RetiredIndexCleanupJob {
    index: IndexRef,
    layout_generation: u64,
    runtime: Arc<SecondaryIndex<EvictableBufferPool>>,
}

impl RetiredIndexCleanupJob {
    /// Returns the exact retired generation being destroyed.
    #[inline]
    pub(crate) const fn index(&self) -> IndexRef {
        self.index
    }

    /// Returns the layout generation that retired this runtime.
    #[inline]
    pub(crate) const fn layout_generation(&self) -> u64 {
        self.layout_generation
    }

    /// Consumes the cleanup job and returns its runtime owner.
    #[inline]
    pub(crate) fn into_runtime(self) -> Arc<SecondaryIndex<EvictableBufferPool>> {
        self.runtime
    }
}

/// Table-owned authority for every non-active secondary-index slot condition.
pub(crate) struct TableIndexLifecycleState {
    effective_next_index_id: u64,
    by_slot: BTreeMap<IndexSlot, SlotLifecycle>,
}

impl TableIndexLifecycleState {
    /// Reconstruct lifecycle state from one loaded active root.
    pub(crate) fn from_active_root(active_root: &ActiveRoot) -> DataIntegrityResult<Self> {
        let metadata = active_root.metadata.as_ref();
        if active_root.secondary_index_slots.len() != metadata.idx.index_slot_count() {
            return Err(
                Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                    "index lifecycle root shape mismatch: root_slots={}, metadata_slots={}",
                    active_root.secondary_index_slots.len(),
                    metadata.idx.index_slot_count()
                )),
            );
        }
        let mut by_slot = BTreeMap::new();
        for (raw_slot, state) in active_root
            .secondary_index_slots
            .iter()
            .copied()
            .enumerate()
        {
            let slot = IndexSlot::try_from(raw_slot).map_err(|_| {
                Report::new(DataIntegrityError::InvalidRootInvariant)
                    .attach("index lifecycle root slot exceeds u16 domain")
            })?;
            let base = match state {
                SecondaryIndexSlot::Active { .. } => continue,
                SecondaryIndexSlot::Vacant => SlotBase::DurableVacant,
                SecondaryIndexSlot::Retired(index_id) => SlotBase::Retired(RetiredSlot {
                    index: IndexRef::new(index_id, slot),
                    durability: RetirementDurability::RecoveryUnclassified,
                    runtime: RuntimeRetirement::Vacant,
                }),
            };
            by_slot.insert(
                slot,
                SlotLifecycle {
                    base,
                    provisional: None,
                },
            );
        }
        Ok(Self {
            effective_next_index_id: metadata.idx.next_index_id(),
            by_slot,
        })
    }

    /// Return the current allocator view after validating the complete definition.
    pub(crate) fn current_allocator_view(
        &mut self,
        layout_metadata: &Arc<TableMetadata>,
        active_root: &ActiveRoot,
    ) -> DataIntegrityResult<CurrentDefinitionAllocatorView> {
        self.validate_definition(layout_metadata, active_root)?;
        let durable_next = layout_metadata.idx.next_index_id();
        if durable_next > ID_DOMAIN_END {
            return Err(
                Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                    "durable next_index_id exceeds domain: value={durable_next}"
                )),
            );
        }
        self.effective_next_index_id = self.effective_next_index_id.max(durable_next);
        if self.effective_next_index_id > ID_DOMAIN_END {
            return Err(
                Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                    "effective next_index_id exceeds domain: value={}",
                    self.effective_next_index_id
                )),
            );
        }
        Ok(CurrentDefinitionAllocatorView {
            metadata: Arc::clone(layout_metadata),
            effective_next_index_id: self.effective_next_index_id,
        })
    }

    /// Select the lowest safe slot without exposing the reusable-slot collection.
    pub(crate) fn select_create_placement(
        &self,
        durable_slot_count: u32,
    ) -> OperationResult<(IndexPlacement, bool)> {
        let mut skipped_runtime_blocker = false;
        for (slot, lifecycle) in &self.by_slot {
            if lifecycle.provisional.is_some() {
                continue;
            }
            match &lifecycle.base {
                SlotBase::DurableVacant => {
                    return Ok((IndexPlacement::ReuseVacant(*slot), skipped_runtime_blocker));
                }
                SlotBase::Retired(retired)
                    if retired.durability == RetirementDurability::CheckpointCovered =>
                {
                    match retired.runtime {
                        RuntimeRetirement::Vacant => {
                            return Ok((
                                IndexPlacement::ReuseRetired(retired.index),
                                skipped_runtime_blocker,
                            ));
                        }
                        RuntimeRetirement::Retained { .. }
                        | RuntimeRetirement::Destroying { .. } => {
                            skipped_runtime_blocker = true;
                        }
                    }
                }
                SlotBase::Unallocated | SlotBase::Retired(_) => {}
            }
        }

        let mut raw_slot = durable_slot_count;
        loop {
            if raw_slot > u32::from(u16::MAX) {
                return Err(Report::new(OperationError::InvalidMetadata)
                    .attach("index slot domain exhausted"));
            }
            let slot = IndexSlot::new(raw_slot as u16);
            if self
                .by_slot
                .get(&slot)
                .is_none_or(|lifecycle| lifecycle.provisional.is_none())
            {
                return Ok((IndexPlacement::Append(slot), skipped_runtime_blocker));
            }
            raw_slot = raw_slot.checked_add(1).ok_or_else(|| {
                Report::new(OperationError::InvalidMetadata)
                    .attach("index slot allocation arithmetic overflow")
            })?;
        }
    }

    /// Installs one finalized CREATE transition beside the layout swap.
    pub(super) fn install_created_index(
        &mut self,
        placement: IndexPlacement,
        index: IndexRef,
        context: IndexLifecycleInstallContext<'_>,
    ) -> Option<()> {
        let IndexLifecycleInstallContext {
            old_metadata,
            new_metadata,
            active_root,
        } = context;
        if placement.slot() != index.slot()
            || new_metadata
                .idx
                .index_spec(index.slot())
                .is_none_or(|spec| spec.index != index)
        {
            return None;
        }
        match placement {
            IndexPlacement::Append(slot) => {
                if u32::from(slot) < old_metadata.idx.index_slot_count_u32()
                    || self.by_slot.contains_key(&slot)
                {
                    return None;
                }
            }
            IndexPlacement::ReuseVacant(slot) => {
                let lifecycle = self.by_slot.get(&slot)?;
                if lifecycle.provisional.is_some()
                    || !matches!(lifecycle.base, SlotBase::DurableVacant)
                {
                    return None;
                }
            }
            IndexPlacement::ReuseRetired(expected) => {
                // `expected` is the exact retired generation that authorized
                // this placement. Matching only its slot would permit a stale
                // plan to cross an ABA generation change, so revalidate the
                // complete lifecycle proof.
                let lifecycle = self.by_slot.get(&expected.slot())?;
                let SlotBase::Retired(retired) = &lifecycle.base else {
                    return None;
                };
                if lifecycle.provisional.is_some()
                    || retired.index != expected
                    || retired.durability != RetirementDurability::CheckpointCovered
                    || !matches!(retired.runtime, RuntimeRetirement::Vacant)
                {
                    return None;
                }
            }
        }
        self.by_slot.remove(&index.slot());
        self.effective_next_index_id = self
            .effective_next_index_id
            .max(new_metadata.idx.next_index_id());
        self.synchronize_vacant_root_slots(active_root)?;
        Some(())
    }

    /// Installs one finalized DROP transition beside the layout swap.
    pub(super) fn install_dropped_index(
        &mut self,
        index: IndexRef,
        drop_cts: TrxID,
        old_layout_generation: u64,
        old_runtime: Arc<SecondaryIndex<EvictableBufferPool>>,
        context: IndexLifecycleInstallContext<'_>,
    ) -> Option<()> {
        let IndexLifecycleInstallContext {
            old_metadata,
            new_metadata,
            active_root,
        } = context;
        if old_metadata
            .idx
            .index_spec(index.slot())
            .is_none_or(|spec| spec.index != index)
            || new_metadata.idx.index_spec(index.slot()).is_some()
            || self.by_slot.contains_key(&index.slot())
            || active_root
                .secondary_index_slots
                .get(index.slot().as_usize())
                .copied()
                != Some(SecondaryIndexSlot::Retired(index.id()))
        {
            return None;
        }
        self.by_slot.insert(
            index.slot(),
            SlotLifecycle {
                base: SlotBase::Retired(RetiredSlot {
                    index,
                    durability: RetirementDurability::AwaitingCheckpoint { drop_cts },
                    runtime: RuntimeRetirement::Retained {
                        layout_generation: old_layout_generation,
                        runtime: old_runtime,
                    },
                }),
                provisional: None,
            },
        );
        Some(())
    }

    /// Record a replay-visible CREATE whose table root does not prove installation.
    pub(crate) fn reserve_provisional_create(
        &mut self,
        index: IndexRef,
        create_cts: TrxID,
        active_root: &ActiveRoot,
    ) -> DataIntegrityResult<()> {
        for (raw_slot, state) in active_root
            .secondary_index_slots
            .iter()
            .copied()
            .enumerate()
        {
            let Some(durable_id) = state.index_id() else {
                continue;
            };
            let slot = IndexSlot::try_from(raw_slot).map_err(|_| {
                Report::new(DataIntegrityError::InvalidRootInvariant)
                    .attach("durable index generation slot exceeds u16 domain")
            })?;
            let same_slot_conflicts =
                slot == index.slot() && matches!(state, SecondaryIndexSlot::Active { .. });
            if same_slot_conflicts || durable_id == index.id() {
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                    "provisional reservation conflicts with durable generation: index={index}, durable_id={durable_id}, durable_slot={slot}"
                )));
            }
        }
        let widened_next = u64::from(index.id().get()) + 1;
        if widened_next > ID_DOMAIN_END {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach("provisional index id exceeds stable domain"));
        }

        if self.by_slot.iter().any(|(slot, candidate)| {
            *slot != index.slot()
                && candidate
                    .provisional
                    .is_some_and(|provisional| provisional.index.id() == index.id())
        }) {
            return Err(
                Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                    "provisional index id has conflicting slots: index={index}"
                )),
            );
        }
        let durable_state = active_root
            .secondary_index_slots
            .get(index.slot().as_usize())
            .copied();
        let lifecycle = self
            .by_slot
            .entry(index.slot())
            .or_insert_with(|| SlotLifecycle {
                base: SlotBase::Unallocated,
                provisional: None,
            });
        match (&lifecycle.base, durable_state) {
            (SlotBase::Unallocated, None)
            | (SlotBase::DurableVacant, Some(SecondaryIndexSlot::Vacant))
            | (SlotBase::Retired(_), Some(SecondaryIndexSlot::Retired(_))) => {}
            _ => {
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                    "provisional reservation has invalid underlying slot: index={index}, durable_state={durable_state:?}"
                )));
            }
        }
        if let SlotBase::Retired(retired) = &lifecycle.base
            && retired.durability != RetirementDurability::RecoveryUnclassified
            && retired.durability != RetirementDurability::CheckpointCovered
        {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                "provisional reservation targets non-reusable retirement: index={index}, retired={}",
                retired.index
            )));
        }
        let requested = ProvisionalCreate { index, create_cts };
        if let Some(existing) = lifecycle.provisional {
            if existing != requested {
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                    "provisional index slot has conflicting replay records: existing_index={}, existing_create_cts={}, new_index={index}, new_create_cts={create_cts}",
                    existing.index, existing.create_cts
                )));
            }
            return Ok(());
        }
        lifecycle.provisional = Some(requested);
        self.effective_next_index_id = self.effective_next_index_id.max(widened_next);
        Ok(())
    }

    /// Classify one replay-visible root-proven DROP as awaiting checkpoint coverage.
    pub(crate) fn record_replayed_drop(
        &mut self,
        index: IndexRef,
        drop_cts: TrxID,
    ) -> DataIntegrityResult<()> {
        let lifecycle = self.by_slot.get_mut(&index.slot()).ok_or_else(|| {
            Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                "replayed DROP has no retired root entry: index={index}"
            ))
        })?;
        let SlotBase::Retired(retired) = &mut lifecycle.base else {
            return Err(
                Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                    "replayed DROP targets non-retired slot: index={index}"
                )),
            );
        };
        if retired.index != index {
            return Err(
                Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                    "replayed DROP generation mismatch: marker={index}, retired={}",
                    retired.index
                )),
            );
        }
        match retired.durability {
            RetirementDurability::RecoveryUnclassified => {
                retired.durability = RetirementDurability::AwaitingCheckpoint { drop_cts };
            }
            RetirementDurability::AwaitingCheckpoint { drop_cts: existing }
                if existing == drop_cts => {}
            RetirementDurability::AwaitingCheckpoint { .. }
            | RetirementDurability::CheckpointCovered => {
                return Err(
                    Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                        "replayed DROP has contradictory retirement classification: index={index}"
                    )),
                );
            }
        }
        Ok(())
    }

    /// Finish restart classification after catalog replay and metadata agreement.
    ///
    /// Retired root entries start as `RecoveryUnclassified` because the root
    /// proves their exact generation but not whether the corresponding DROP is
    /// below the durable replay boundary. Replay changes every root-proven DROP
    /// still in the retained suffix to `AwaitingCheckpoint`; therefore any
    /// retirement left unclassified here is already checkpoint-covered.
    ///
    /// Provisional CREATE overlays are validated after that classification so
    /// a marker may legally reserve an older covered retired hole. The marker
    /// must still be rejected over a replay-visible DROP or any other base that
    /// is not independently reusable. Success guarantees that no recovery-only
    /// classification remains and that every lifecycle entry agrees with the
    /// loaded active root.
    pub(crate) fn finish_recovery(&mut self, active_root: &ActiveRoot) -> DataIntegrityResult<()> {
        for lifecycle in self.by_slot.values_mut() {
            // Complete the negative replay proof: a matching retained DROP
            // would already have classified this exact retirement above.
            if let SlotBase::Retired(retired) = &mut lifecycle.base
                && retired.durability == RetirementDurability::RecoveryUnclassified
            {
                retired.durability = RetirementDurability::CheckpointCovered;
            }
            // A provisional marker blocks allocation but does not make its
            // underlying slot safe. Require that base to be reusable on its
            // own before accepting the recovered overlay.
            if lifecycle.provisional.is_some()
                && !matches!(
                    lifecycle.base,
                    SlotBase::Unallocated | SlotBase::DurableVacant
                )
                && !matches!(
                    lifecycle.base,
                    SlotBase::Retired(RetiredSlot {
                        durability: RetirementDurability::CheckpointCovered,
                        runtime: RuntimeRetirement::Vacant,
                        ..
                    })
                )
            {
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                    .attach("provisional CREATE overlays a non-reusable retired slot"));
            }
        }
        // Close recovery by checking exact root identities and by rejecting
        // any out-of-range entry that is not a provisional append reservation.
        self.validate_root_entries(active_root)
    }

    /// Apply one successfully published catalog replay boundary.
    pub(crate) fn apply_checkpoint(
        &mut self,
        catalog_replay_start_ts: TrxID,
        active_root: &ActiveRoot,
    ) -> DataIntegrityResult<()> {
        for (slot, lifecycle) in &mut self.by_slot {
            if let SlotBase::Retired(retired) = &mut lifecycle.base
                && let RetirementDurability::AwaitingCheckpoint { drop_cts } = retired.durability
                && drop_cts < catalog_replay_start_ts
            {
                let root_state = active_root
                    .secondary_index_slots
                    .get(slot.as_usize())
                    .copied();
                if root_state != Some(SecondaryIndexSlot::Retired(retired.index.id())) {
                    return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(
                        format!(
                            "checkpoint-covered retirement root mismatch: index={}, root_state={root_state:?}",
                            retired.index
                        ),
                    ));
                }
                retired.durability = RetirementDurability::CheckpointCovered;
            }
            if lifecycle
                .provisional
                .is_some_and(|provisional| provisional.create_cts < catalog_replay_start_ts)
            {
                lifecycle.provisional = None;
            }
        }
        self.by_slot.retain(|_, lifecycle| {
            lifecycle.provisional.is_some() || !matches!(lifecycle.base, SlotBase::Unallocated)
        });
        Ok(())
    }

    /// Move one uniquely owned retired runtime into the Destroying sentinel.
    pub(crate) fn take_cleanup_job(&mut self) -> Option<RetiredIndexCleanupJob> {
        for lifecycle in self.by_slot.values_mut() {
            let SlotBase::Retired(retired) = &mut lifecycle.base else {
                continue;
            };
            let RuntimeRetirement::Retained {
                layout_generation,
                runtime,
            } = &retired.runtime
            else {
                continue;
            };
            if Arc::strong_count(runtime) != 1 {
                continue;
            }
            let layout_generation = *layout_generation;
            let runtime = Arc::clone(runtime);
            retired.runtime = RuntimeRetirement::Destroying { layout_generation };
            return Some(RetiredIndexCleanupJob {
                index: retired.index,
                layout_generation,
                runtime,
            });
        }
        None
    }

    /// Complete one exact Destroying-to-Vacant transition.
    pub(crate) fn finish_cleanup_job(
        &mut self,
        index: IndexRef,
        layout_generation: u64,
    ) -> Option<()> {
        let lifecycle = self.by_slot.get_mut(&index.slot())?;
        let SlotBase::Retired(retired) = &mut lifecycle.base else {
            return None;
        };
        if retired.index != index
            || !matches!(
                retired.runtime,
                RuntimeRetirement::Destroying {
                    layout_generation: generation
                } if generation == layout_generation
            )
        {
            return None;
        }
        retired.runtime = RuntimeRetirement::Vacant;
        Some(())
    }

    /// Returns whether a retained or destroying runtime remains.
    pub(crate) fn has_retired_runtimes(&self) -> bool {
        self.by_slot.values().any(|lifecycle| {
            matches!(
                lifecycle.base,
                SlotBase::Retired(RetiredSlot {
                    runtime: RuntimeRetirement::Retained { .. }
                        | RuntimeRetirement::Destroying { .. },
                    ..
                })
            )
        })
    }

    /// Consume every retained runtime during terminal table destruction.
    pub(crate) fn into_retained_runtimes(
        self,
    ) -> Vec<(IndexRef, u64, Arc<SecondaryIndex<EvictableBufferPool>>)> {
        let mut runtimes = Vec::new();
        for lifecycle in self.by_slot.into_values() {
            let SlotBase::Retired(retired) = lifecycle.base else {
                continue;
            };
            match retired.runtime {
                RuntimeRetirement::Retained {
                    layout_generation,
                    runtime,
                } => runtimes.push((retired.index, layout_generation, runtime)),
                RuntimeRetirement::Destroying { layout_generation } => panic!(
                    "retired secondary index destruction still in flight during table destroy: index={}, layout_generation={layout_generation}",
                    retired.index
                ),
                RuntimeRetirement::Vacant => {}
            }
        }
        runtimes
    }

    fn synchronize_vacant_root_slots(&mut self, active_root: &ActiveRoot) -> Option<()> {
        for (raw_slot, state) in active_root
            .secondary_index_slots
            .iter()
            .copied()
            .enumerate()
        {
            if state != SecondaryIndexSlot::Vacant {
                continue;
            }
            let slot = IndexSlot::try_from(raw_slot).ok()?;
            match self.by_slot.entry(slot) {
                Entry::Vacant(entry) => {
                    entry.insert(SlotLifecycle {
                        base: SlotBase::DurableVacant,
                        provisional: None,
                    });
                }
                Entry::Occupied(mut entry) => {
                    let lifecycle = entry.get_mut();
                    if matches!(lifecycle.base, SlotBase::Unallocated) {
                        lifecycle.base = SlotBase::DurableVacant;
                    } else if !matches!(lifecycle.base, SlotBase::DurableVacant) {
                        return None;
                    }
                }
            }
        }
        Some(())
    }

    fn validate_definition(
        &self,
        layout_metadata: &Arc<TableMetadata>,
        active_root: &ActiveRoot,
    ) -> DataIntegrityResult<()> {
        if active_root.metadata.as_ref() != layout_metadata.as_ref() {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach("index lifecycle metadata/root mismatch"));
        }
        self.validate_root_entries(active_root)
    }

    fn validate_root_entries(&self, active_root: &ActiveRoot) -> DataIntegrityResult<()> {
        for (raw_slot, root_state) in active_root
            .secondary_index_slots
            .iter()
            .copied()
            .enumerate()
        {
            let slot = IndexSlot::try_from(raw_slot).map_err(|_| {
                Report::new(DataIntegrityError::InvalidRootInvariant)
                    .attach("index lifecycle root slot exceeds u16 domain")
            })?;
            let lifecycle = self.by_slot.get(&slot);
            let matches = match root_state {
                SecondaryIndexSlot::Active { .. } => lifecycle.is_none(),
                SecondaryIndexSlot::Vacant => {
                    lifecycle.is_some_and(|entry| matches!(entry.base, SlotBase::DurableVacant))
                }
                SecondaryIndexSlot::Retired(index_id) => lifecycle.is_some_and(|entry| {
                    matches!(
                        &entry.base,
                        SlotBase::Retired(retired) if retired.index == IndexRef::new(index_id, slot)
                    )
                }),
            };
            if !matches {
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                    "index lifecycle disagrees with root: slot={slot}, root_state={root_state:?}"
                )));
            }
        }
        for (slot, lifecycle) in &self.by_slot {
            if slot.as_usize() < active_root.secondary_index_slots.len() {
                continue;
            }
            if !matches!(lifecycle.base, SlotBase::Unallocated) || lifecycle.provisional.is_none() {
                return Err(
                    Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
                        "out-of-range lifecycle slot lacks provisional-only state: slot={slot}"
                    )),
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        ColumnID, ColumnOrdinal, IndexID, StorageColumnFlags, StorageIndexFlags, StorageIndexKey,
        StorageIndexSpec, TableColumnMetadata,
    };
    use crate::error::OperationError;
    use crate::file::table_file::ActiveRoot;
    use crate::value::ValKind;

    fn metadata(next_index_id: u64, slot_count: u32) -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_from_persisted_parts(
                0,
                1,
                vec![TableColumnMetadata {
                    id: ColumnID::new(0),
                    ordinal: ColumnOrdinal::new(0),
                    value_kind: ValKind::U32,
                    flags: StorageColumnFlags::empty(),
                }],
                next_index_id,
                slot_count,
                vec![],
            )
            .unwrap(),
        )
    }

    fn root(next_index_id: u64, slots: Vec<SecondaryIndexSlot>) -> ActiveRoot {
        let metadata = metadata(next_index_id, slots.len() as u32);
        let mut root = ActiveRoot::new(TrxID::new(20), 128, metadata);
        root.secondary_index_slots = slots;
        root
    }

    #[test]
    fn checkpoint_strictly_gates_recovered_retirement_reuse() {
        let retired = IndexRef::new(IndexID::new(4), IndexSlot::new(0));
        let root = root(5, vec![SecondaryIndexSlot::Retired(retired.id())]);
        let mut state = TableIndexLifecycleState::from_active_root(&root).unwrap();
        state.record_replayed_drop(retired, TrxID::new(30)).unwrap();
        state.finish_recovery(&root).unwrap();

        assert_eq!(
            state.select_create_placement(1).unwrap().0,
            IndexPlacement::Append(IndexSlot::new(1))
        );
        state.apply_checkpoint(TrxID::new(30), &root).unwrap();
        assert_eq!(
            state.select_create_placement(1).unwrap().0,
            IndexPlacement::Append(IndexSlot::new(1)),
            "an equal replay boundary must not cover the DROP"
        );
        state.apply_checkpoint(TrxID::new(31), &root).unwrap();
        assert_eq!(
            state.select_create_placement(1).unwrap().0,
            IndexPlacement::ReuseRetired(retired)
        );
    }

    #[test]
    fn lowest_reusable_slot_skips_provisional_and_preserves_effective_watermark() {
        let retired0 = IndexRef::new(IndexID::new(4), IndexSlot::new(0));
        let retired2 = IndexRef::new(IndexID::new(5), IndexSlot::new(2));
        let root = root(
            6,
            vec![
                SecondaryIndexSlot::Retired(retired0.id()),
                SecondaryIndexSlot::Vacant,
                SecondaryIndexSlot::Retired(retired2.id()),
            ],
        );
        let mut state = TableIndexLifecycleState::from_active_root(&root).unwrap();
        state.finish_recovery(&root).unwrap();
        state
            .reserve_provisional_create(
                IndexRef::new(IndexID::new(6), IndexSlot::new(0)),
                TrxID::new(40),
                &root,
            )
            .unwrap();
        state
            .reserve_provisional_create(
                IndexRef::new(IndexID::new(6), IndexSlot::new(0)),
                TrxID::new(40),
                &root,
            )
            .unwrap();
        let err = state
            .reserve_provisional_create(
                IndexRef::new(IndexID::new(6), IndexSlot::new(0)),
                TrxID::new(41),
                &root,
            )
            .unwrap_err();
        assert_eq!(
            *err.current_context(),
            DataIntegrityError::InvalidRootInvariant
        );
        state
            .reserve_provisional_create(
                IndexRef::new(IndexID::new(7), IndexSlot::new(1)),
                TrxID::new(40),
                &root,
            )
            .unwrap();

        assert_eq!(
            state.select_create_placement(3).unwrap().0,
            IndexPlacement::ReuseRetired(retired2)
        );
        state.apply_checkpoint(TrxID::new(40), &root).unwrap();
        assert_eq!(
            state.select_create_placement(3).unwrap().0,
            IndexPlacement::ReuseRetired(retired2),
            "equal replay floor retains provisional reservations"
        );
        state.apply_checkpoint(TrxID::new(41), &root).unwrap();
        assert_eq!(
            state.select_create_placement(3).unwrap().0,
            IndexPlacement::ReuseRetired(retired0)
        );
        let view = state
            .current_allocator_view(&Arc::clone(&root.metadata), &root)
            .unwrap();
        assert_eq!(view.effective_next_index_id(), 8);
    }

    #[test]
    fn provisional_max_id_keeps_exact_widened_exhaustion_after_release() {
        let root = root(1, vec![SecondaryIndexSlot::Vacant]);
        let mut state = TableIndexLifecycleState::from_active_root(&root).unwrap();
        state
            .reserve_provisional_create(
                IndexRef::new(IndexID::new(u32::MAX), IndexSlot::new(0)),
                TrxID::new(50),
                &root,
            )
            .unwrap();
        state.apply_checkpoint(TrxID::new(51), &root).unwrap();
        let view = state
            .current_allocator_view(&Arc::clone(&root.metadata), &root)
            .unwrap();
        assert_eq!(view.effective_next_index_id(), ID_DOMAIN_END);
        let placement = state.select_create_placement(1).unwrap().0;
        let err = view
            .metadata()
            .try_with_finalized_created_index(
                StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::empty()),
                view.effective_next_index_id(),
                placement,
            )
            .unwrap_err();
        assert_eq!(*err.current_context(), OperationError::IndexIdExhausted);
    }
}
