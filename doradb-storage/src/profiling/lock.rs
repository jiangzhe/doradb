use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative logical-lock work and current physical representation statistics.
///
/// Monotonic counters describe completed structural work. Current values are
/// point-in-time observations and peak values are monotonic high-water marks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogicalLockStats {
    /// Requests satisfied by an existing exact claim without manager access.
    pub owner_local_exact_covered_hits: u64,
    /// Fresh exact claims published under an unchanged physical family mode.
    pub owner_local_covered_publications: u64,
    /// Exact conversions that preserved the physical family mode.
    pub owner_local_mode_preserving_conversions: u64,
    /// Exact releases that preserved the physical family mode.
    pub owner_local_mode_preserving_releases: u64,
    /// Shared resource-state transitions.
    pub resource_transitions: u64,
    /// Fixed compatibility mode slots examined by shared transitions.
    pub mode_slots_examined: u64,
    /// Immediately accepted first-family physical acquisitions.
    pub immediate_physical_acquisitions: u64,
    /// Successfully strengthened physical family modes.
    pub physical_upgrades: u64,
    /// Requests appended to intrusive FIFO queues.
    pub enqueued_waiters: u64,
    /// Intrusive FIFO append, detach, or unlink mutations.
    pub queue_link_mutations: u64,
    /// Queued cancellations that removed the FIFO head.
    pub cancelled_head_waiters: u64,
    /// Queued cancellations that removed a middle entry.
    pub cancelled_middle_waiters: u64,
    /// Queued cancellations that removed the FIFO tail.
    pub cancelled_tail_waiters: u64,
    /// Provisional physical holders accepted by their notified observer.
    pub provisional_observations: u64,
    /// Waiters promoted into provisional physical holders.
    pub promoted_waiters: u64,
    /// Exact claims visited by indexed scope close.
    pub scope_close_claims_visited: u64,
    /// Scope-close claims that removed their family's last physical entry.
    pub scope_close_physical_changes: u64,
    /// Success-only completion objects allocated for blocked requests.
    pub completion_allocations: u64,
    /// Waiter slab vector growth events.
    pub waiter_slab_growths: u64,
    /// Waiter slab vacant-slot reuse events.
    pub waiter_slab_reuses: u64,
    /// Physical resources currently retained by the manager.
    pub current_physical_resources: u64,
    /// Maximum simultaneously retained physical resources.
    pub peak_physical_resources: u64,
    /// Physical family entries currently retained by the manager.
    pub current_physical_families: u64,
    /// Maximum simultaneously retained physical family entries.
    pub peak_physical_families: u64,
    /// FIFO-linked waiters currently retained.
    pub current_linked_waiters: u64,
    /// Maximum simultaneously FIFO-linked waiters.
    pub peak_linked_waiters: u64,
    /// Waiter nodes in queued or provisional state.
    pub current_live_waiter_nodes: u64,
    /// Maximum simultaneously live waiter nodes.
    pub peak_live_waiter_nodes: u64,
}

/// Component-owned atomic counters and physical representation peaks.
#[derive(Default)]
pub(crate) struct LockManagerStats {
    /// Requests satisfied by an existing exact claim without manager access.
    pub(crate) owner_local_exact_covered_hits: AtomicU64,
    /// Fresh exact claims published under an unchanged physical family mode.
    pub(crate) owner_local_covered_publications: AtomicU64,
    /// Exact conversions that preserved the physical family mode.
    pub(crate) owner_local_mode_preserving_conversions: AtomicU64,
    /// Exact releases that preserved the physical family mode.
    pub(crate) owner_local_mode_preserving_releases: AtomicU64,
    /// Shared resource-state transitions.
    pub(crate) resource_transitions: AtomicU64,
    /// Fixed compatibility mode slots examined by shared transitions.
    pub(crate) mode_slots_examined: AtomicU64,
    /// Immediately accepted first-family physical acquisitions.
    pub(crate) immediate_physical_acquisitions: AtomicU64,
    /// Successfully strengthened physical family modes.
    pub(crate) physical_upgrades: AtomicU64,
    /// Requests appended to intrusive FIFO queues.
    pub(crate) enqueued_waiters: AtomicU64,
    /// Intrusive FIFO append, detach, or unlink mutations.
    pub(crate) queue_link_mutations: AtomicU64,
    /// Queued cancellations that removed the FIFO head.
    pub(crate) cancelled_head_waiters: AtomicU64,
    /// Queued cancellations that removed a middle entry.
    pub(crate) cancelled_middle_waiters: AtomicU64,
    /// Queued cancellations that removed the FIFO tail.
    pub(crate) cancelled_tail_waiters: AtomicU64,
    /// Provisional physical holders accepted by their notified observer.
    pub(crate) provisional_observations: AtomicU64,
    /// Waiters promoted into provisional physical holders.
    pub(crate) promoted_waiters: AtomicU64,
    /// Exact claims visited by indexed scope close.
    pub(crate) scope_close_claims_visited: AtomicU64,
    /// Scope-close claims that removed their family's last physical entry.
    pub(crate) scope_close_physical_changes: AtomicU64,
    /// Success-only completion objects allocated for blocked requests.
    pub(crate) completion_allocations: AtomicU64,
    /// Waiter slab vector growth events.
    pub(crate) waiter_slab_growths: AtomicU64,
    /// Waiter slab vacant-slot reuse events.
    pub(crate) waiter_slab_reuses: AtomicU64,
    /// Physical resources currently retained by the manager.
    pub(crate) current_physical_resources: AtomicU64,
    /// Maximum simultaneously retained physical resources.
    pub(crate) peak_physical_resources: AtomicU64,
    /// Physical family entries currently retained by the manager.
    pub(crate) current_physical_families: AtomicU64,
    /// Maximum simultaneously retained physical family entries.
    pub(crate) peak_physical_families: AtomicU64,
    /// FIFO-linked waiters currently retained.
    pub(crate) current_linked_waiters: AtomicU64,
    /// Maximum simultaneously FIFO-linked waiters.
    pub(crate) peak_linked_waiters: AtomicU64,
    /// Waiter nodes in queued or provisional state.
    pub(crate) current_live_waiter_nodes: AtomicU64,
    /// Maximum simultaneously live waiter nodes.
    pub(crate) peak_live_waiter_nodes: AtomicU64,
}

impl LockManagerStats {
    /// Read independently sampled counters and representation peaks.
    #[inline]
    pub(crate) fn snapshot(&self) -> LogicalLockStats {
        LogicalLockStats {
            owner_local_exact_covered_hits: load(&self.owner_local_exact_covered_hits),
            owner_local_covered_publications: load(&self.owner_local_covered_publications),
            owner_local_mode_preserving_conversions: load(
                &self.owner_local_mode_preserving_conversions,
            ),
            owner_local_mode_preserving_releases: load(&self.owner_local_mode_preserving_releases),
            resource_transitions: load(&self.resource_transitions),
            mode_slots_examined: load(&self.mode_slots_examined),
            immediate_physical_acquisitions: load(&self.immediate_physical_acquisitions),
            physical_upgrades: load(&self.physical_upgrades),
            enqueued_waiters: load(&self.enqueued_waiters),
            queue_link_mutations: load(&self.queue_link_mutations),
            cancelled_head_waiters: load(&self.cancelled_head_waiters),
            cancelled_middle_waiters: load(&self.cancelled_middle_waiters),
            cancelled_tail_waiters: load(&self.cancelled_tail_waiters),
            provisional_observations: load(&self.provisional_observations),
            promoted_waiters: load(&self.promoted_waiters),
            scope_close_claims_visited: load(&self.scope_close_claims_visited),
            scope_close_physical_changes: load(&self.scope_close_physical_changes),
            completion_allocations: load(&self.completion_allocations),
            waiter_slab_growths: load(&self.waiter_slab_growths),
            waiter_slab_reuses: load(&self.waiter_slab_reuses),
            current_physical_resources: load(&self.current_physical_resources),
            peak_physical_resources: load(&self.peak_physical_resources),
            current_physical_families: load(&self.current_physical_families),
            peak_physical_families: load(&self.peak_physical_families),
            current_linked_waiters: load(&self.current_linked_waiters),
            peak_linked_waiters: load(&self.peak_linked_waiters),
            current_live_waiter_nodes: load(&self.current_live_waiter_nodes),
            peak_live_waiter_nodes: load(&self.peak_live_waiter_nodes),
        }
    }

    /// Merge completed owner-local work into manager diagnostics.
    #[inline]
    pub(crate) fn record_family(&self, family: FamilyLockStats) {
        add(
            &self.owner_local_exact_covered_hits,
            family.repeated_exact_covered,
        );
        add(
            &self.owner_local_covered_publications,
            family.family_covered_publications,
        );
        add(
            &self.owner_local_mode_preserving_conversions,
            family.physical_mode_preserving_conversions,
        );
        add(
            &self.owner_local_mode_preserving_releases,
            family.physical_mode_preserving_releases,
        );
        add(
            &self.scope_close_claims_visited,
            family.close_claims_visited,
        );
        add(
            &self.scope_close_physical_changes,
            family.scope_close_physical_changes,
        );
    }
}

/// Owner-local logical-lock path counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct FamilyLockStats {
    /// Acquisitions covered by the same exact logical claim.
    pub(crate) repeated_exact_covered: u64,
    /// Fresh exact claims published under an existing physical family holder.
    pub(crate) family_covered_publications: u64,
    /// Owner-local conversions that preserve the physical family mode.
    pub(crate) physical_mode_preserving_conversions: u64,
    /// Physical manager acquisition or conversion transitions.
    pub(crate) manager_acquires: u64,
    /// Physical family removals from the manager.
    pub(crate) physical_family_removals: u64,
    /// Fresh accepted logical claim identities.
    pub(crate) accepted_fresh_claims: u64,
    /// Exact logical claims converted to a covering mode.
    pub(crate) conversions: u64,
    /// Exact logical scopes closed through their cleanup indexes.
    pub(crate) scopes_closed: u64,
    /// Claims visited while closing exact logical scopes.
    pub(crate) close_claims_visited: u64,
    /// Scope-close claims that changed physical family state.
    pub(crate) scope_close_physical_changes: u64,
    /// Releases that left the family/resource physical mode unchanged.
    pub(crate) physical_mode_preserving_releases: u64,
}

/// Add one owner-observed logical-lock count.
#[inline]
pub(crate) fn add(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

/// Increase a diagnostic current count and its lifetime peak.
#[inline]
pub(crate) fn increment_current(current: &AtomicU64, peak: &AtomicU64) {
    let value = current.fetch_add(1, Ordering::Relaxed) + 1;
    peak.fetch_max(value, Ordering::Relaxed);
}

/// Decrease a diagnostic current count.
#[inline]
pub(crate) fn decrement_current(current: &AtomicU64) {
    current.fetch_sub(1, Ordering::Relaxed);
}

#[inline]
fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}
