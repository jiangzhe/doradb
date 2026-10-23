//! Ownership of a completed private cold tree and its accepted read lifetimes.
//!
//! Construction validates the handoff before creating this artifact. Read leases
//! retain resources without retaining construction inputs or validation strategies.
use super::merge::execution_error;
use super::{MemoryBudget, MemoryReservation};
use crate::buffer::PoolGuard;
use crate::catalog::{IndexRef, SecondaryIndexRoot, SecondaryIndexSlot, TableMetadata};
use crate::error::RuntimeOrFatalResult;
use crate::file::table_file::MutableTableFile;
use crate::id::{BlockID, TableID, TrxID};
use crate::index::SecondaryDiskTreeRuntime;
use crate::index::disk_tree::UniqueDiskTree;
use parking_lot::Mutex;
use std::mem::forget;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Compact identity of a single completed cold invocation and index generation.
#[derive(Clone)]
pub(crate) struct ColdBuildIdentity {
    invocation: Arc<()>,
    table: TableID,
    index: IndexRef,
    timestamp: TrxID,
}

impl ColdBuildIdentity {
    /// Capture the invocation and catalog binding checked by cold construction.
    pub(super) fn new(
        invocation: Arc<()>,
        table: TableID,
        index: IndexRef,
        timestamp: TrxID,
    ) -> Self {
        Self {
            invocation,
            table,
            index,
            timestamp,
        }
    }

    /// Equality requires the exact invocation as well as its catalog binding.
    pub(super) fn matches(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.invocation, &other.invocation)
            && self.table == other.table
            && self.index == other.index
            && self.timestamp == other.timestamp
    }
}

/// Immutable root facts retained after construction validates the sealed handoff.
pub(super) struct SealedColdMetadata {
    /// Exact invocation and catalog binding verified by the builder.
    pub(super) identity: ColdBuildIdentity,
    /// Completed root; empty input has no root.
    pub(super) root: SecondaryIndexRoot,
    /// Whether the completed tree stores unique logical keys.
    pub(super) unique: bool,
}

struct SealedRoot {
    metadata: SealedColdMetadata,
    uncertain_read: AtomicBool,
    // Read leases keep the writer claim and private allocation map alive even
    // if an invariant unwind detaches the owning CREATE observer.
    file: Mutex<Option<MutableTableFile>>,
    // Private extraction handle. Only the enclosing root is shared with readers.
    runtime: Arc<SecondaryDiskTreeRuntime>,
    guard: PoolGuard,
    budget: MemoryBudget,
}

impl Drop for SealedRoot {
    fn drop(&mut self) {
        if self.uncertain_read.load(Ordering::Acquire)
            && let Some(file) = self.file.get_mut().take()
        {
            // As with an unsealed Fatal builder, retain the allocation map and
            // writer claim when an invariant failure cannot certify reclamation.
            forget(file);
        }
    }
}

/// Scope of one accepted persisted read, including its admission validator.
/// An interrupted read cannot certify that backend use has finished.
pub(super) struct ColdReadUse<'a> {
    root: &'a SealedRoot,
    reservation: &'a Arc<MemoryReservation>,
    settled: bool,
}

impl ColdReadUse<'_> {
    /// Mark authoritative read completion; Fatal may retain kernel-owned work.
    #[inline]
    pub(super) fn finish(mut self, fatal: bool) {
        self.settled = !fatal;
    }
}

impl Drop for ColdReadUse<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.root.uncertain_read.store(true, Ordering::Release);
            // The backend may still run persisted validation after a Fatal
            // notification. Its bounded allowance must survive that ownership.
            forget(self.reservation.clone());
        }
    }
}

/// Read-only lease on an exact checked unique root, with no construction inputs.
#[derive(Clone)]
pub(crate) struct ColdReadAuthority(Arc<SealedRoot>);

impl ColdReadAuthority {
    /// Retain uncertain allocation ownership if a read unwinds or is abandoned.
    #[inline]
    pub(super) fn read_use<'a>(
        &'a self,
        reservation: &'a Arc<MemoryReservation>,
    ) -> ColdReadUse<'a> {
        ColdReadUse {
            root: &self.0,
            reservation,
            settled: false,
        }
    }

    /// Compact identity used by validation evidence.
    #[inline]
    pub(super) fn identity(&self) -> &ColdBuildIdentity {
        &self.0.metadata.identity
    }

    /// Exact cold coverage certified before construction storage was released.
    #[inline]
    pub(crate) fn entries(&self) -> usize {
        self.0.metadata.root.stats().entries as usize
    }

    /// Invocation budget, reused after releasing cold construction allocations.
    #[inline]
    pub(crate) fn budget(&self) -> &MemoryBudget {
        &self.0.budget
    }

    /// Height of the completed root; bounds traversal without retaining nodes.
    #[inline]
    pub(crate) fn height(&self) -> u16 {
        self.0.metadata.root.stats().height
    }

    /// Optional completed root, never the super block.
    #[inline]
    pub(crate) fn root(&self) -> Option<BlockID> {
        self.0.metadata.root.block_id()
    }

    /// Open persisted unique reads using this lease's operation provenance.
    pub(crate) fn tree(&self) -> RuntimeOrFatalResult<UniqueDiskTree<'_>> {
        self.0
            .runtime
            .open_unique_at(self.0.metadata.root.block_id(), &self.0.guard)
            .map_err(Into::into)
    }
}

/// Move-only publication authority. Read leases cannot extract or modify its fork.
pub(crate) struct SealedColdBuild {
    root: Arc<SealedRoot>,
}

impl SealedColdBuild {
    /// Retain validated metadata and resources after releasing construction inputs.
    /// The caller must complete its fallible seal checks before this handoff.
    pub(super) fn new(
        metadata: SealedColdMetadata,
        file: MutableTableFile,
        runtime: SecondaryDiskTreeRuntime,
        guard: PoolGuard,
        budget: MemoryBudget,
    ) -> Self {
        Self {
            root: Arc::new(SealedRoot {
                metadata,
                uncertain_read: AtomicBool::new(false),
                file: Mutex::new(Some(file)),
                runtime: Arc::new(runtime),
                guard,
                budget,
            }),
        }
    }

    /// Exact CREATE validation contract retained by the allocation owner.
    pub(crate) fn validation_binding(&self) -> Option<(&ColdBuildIdentity, usize)> {
        let metadata = &self.root.metadata;
        metadata
            .unique
            .then_some((&metadata.identity, metadata.root.stats().entries as usize))
    }

    /// Issue read authority only for a fully checked unique cold tree.
    pub(crate) fn unique_reader(&self) -> Option<ColdReadAuthority> {
        self.root
            .metadata
            .unique
            .then(|| ColdReadAuthority(Arc::clone(&self.root)))
    }

    /// Install private metadata while retaining allocation ownership in CREATE.
    pub(crate) fn replace_metadata(
        &mut self,
        metadata: Arc<TableMetadata>,
        slots: Vec<SecondaryIndexSlot>,
    ) {
        self.root
            .file
            .lock()
            .as_mut()
            .unwrap_or_else(|| unreachable!("sealed fork retained"))
            .replace_metadata_and_secondary_index_slots(metadata, slots);
    }

    /// Reject finalization without changing ownership until every read lease is gone.
    pub(super) fn validate_finish_reading(&mut self) -> RuntimeOrFatalResult<()> {
        let root = Arc::get_mut(&mut self.root).ok_or_else(|| {
            execution_error("cold finalization requires exclusive read ownership")
        })?;
        if root.uncertain_read.load(Ordering::Acquire) {
            return Err(execution_error(
                "cold finalization cannot release uncertain reads",
            ));
        }
        Ok(())
    }

    /// Recover the runtime and fork after exclusive, settled ownership is checked.
    /// The caller must retain both results before its next await.
    pub(super) fn into_parts(self) -> (SecondaryDiskTreeRuntime, MutableTableFile) {
        let mut root = Arc::try_unwrap(self.root)
            .unwrap_or_else(|_| unreachable!("validated cold finalization has no read leases"));
        let file = root
            .file
            .get_mut()
            .take()
            .unwrap_or_else(|| unreachable!("cold finalization retains its private fork"));
        // SealedRoot's Drop protects uncertain reads. Keep that policy intact
        // while moving the runtime out through its private, unexposed Arc.
        let runtime = Arc::clone(&root.runtime);
        drop(root);
        let runtime = Arc::try_unwrap(runtime)
            .unwrap_or_else(|_| unreachable!("sealed runtime handle never escapes its root"));
        (runtime, file)
    }
}
