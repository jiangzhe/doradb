//! Caller-owned orchestration from captured rows to an uninstalled packed tree.
use super::cold_validation::ColdValidation;
use super::merge::HotMergePreparation;
use super::tree_builder::{
    HotPackedBuild, HotPackedOutcome, HotPackedSpec, ReadyHotTree, StagedPageCleanup,
};
use super::{HotBuildPolicy, HotBuildSource, HotLocalSort};
use crate::buffer::{BufferPool, PoolGuard};
use crate::component::panic_payload_description;
use crate::error::{FatalError, RuntimeOrFatalResult};
use crate::poison::EnginePoisoner;
#[cfg(feature = "profiling")]
use crate::profiling::{
    HotBuildMeasurements, HotMergeMeasurements, HotPackedMeasurements, clock::Instant,
};
use crate::quiescent::QuiescentGuard;
use crate::runtime::thread_pool::ThreadPool;
use error_stack::Report;
use futures::FutureExt;
use std::mem;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

#[cfg(test)]
pub(crate) use tests::{Point as TestPoint, observe as test_observe};

#[derive(Clone, Copy, PartialEq, Eq)]
enum BuildPhase {
    New,
    Extraction,
    MergePreparation,
    PackedConstruction,
    Ready,
    Settlement,
    Settled,
}

/// One per-index construction attempt, retained independently of borrowed futures.
///
/// The caller supplies stable source authority and the destination pool.
/// Build returns an uninstalled tree so the caller can validate before installing
/// or aborting it. After either decision, errors, or cancellation, call settle
/// before publication or storage teardown. Dropping this owner does not run cleanup.
#[must_use = "retain the pipeline and settle accepted work before storage teardown"]
pub(crate) struct HotIndexBuild<P: BufferPool + 'static> {
    source: Arc<HotBuildSource>,
    index_pool: QuiescentGuard<P>,
    index_guard: PoolGuard,
    thread_pool: QuiescentGuard<ThreadPool>,
    poisoner: QuiescentGuard<EnginePoisoner>,
    policy: HotBuildPolicy,
    cold_validation: ColdValidation,
    phase: BuildPhase,
    sort: Option<HotLocalSort>,
    preparation: Option<HotMergePreparation>,
    packing: Option<HotPackedBuild<P>>,
    cleanup: Option<StagedPageCleanup<P>>,
    #[cfg(test)]
    hooks: tests::Hooks<P>,
    #[cfg(feature = "profiling")]
    extraction: Option<HotBuildMeasurements>,
    #[cfg(feature = "profiling")]
    cleanup_elapsed_nanos: u64,
}

impl<P: BufferPool + 'static> HotIndexBuild<P> {
    /// Share one captured key shape between extraction and the retained build owner.
    pub(crate) fn new(
        source: Arc<HotBuildSource>,
        index_pool: QuiescentGuard<P>,
        index_guard: PoolGuard,
        thread_pool: QuiescentGuard<ThreadPool>,
        poisoner: QuiescentGuard<EnginePoisoner>,
        policy: HotBuildPolicy,
        cold_validation: ColdValidation,
    ) -> Self {
        assert!(
            !matches!(&cold_validation, ColdValidation::Required(_))
                || (source.key.unique && source.key.duplicates == super::DuplicateCheck::Collect),
            "required cold validation needs checked unique hot extraction"
        );
        let sort = Some(HotLocalSort::new(
            source.clone(),
            thread_pool.clone(),
            policy,
        ));
        Self {
            source,
            index_pool,
            index_guard,
            thread_pool,
            poisoner,
            policy,
            cold_validation,
            phase: BuildPhase::New,
            sort,
            preparation: None,
            packing: None,
            cleanup: None,
            #[cfg(test)]
            hooks: tests::Hooks::default(),
            #[cfg(feature = "profiling")]
            extraction: None,
            #[cfg(feature = "profiling")]
            cleanup_elapsed_nanos: 0,
        }
    }

    /// Borrow the captured source retained through construction and settlement.
    #[inline]
    pub(crate) fn source(&self) -> &HotBuildSource {
        &self.source
    }

    /// Return the current stage for caller-owned diagnostics.
    #[inline]
    pub(crate) fn phase(&self) -> &'static str {
        match self.phase {
            BuildPhase::New => "new",
            BuildPhase::Extraction => "extraction",
            BuildPhase::MergePreparation => "merge_preparation",
            BuildPhase::PackedConstruction => "packed_construction",
            BuildPhase::Ready => "ready",
            BuildPhase::Settlement => "settlement",
            BuildPhase::Settled => "settled",
        }
    }

    /// Extract, merge and construct once, leaving installation to the caller.
    ///
    /// Cancellation abandons this attempt; retain this owner and call settle.
    /// Construction panics become Fatal with the stage owners still available
    /// for settlement. Installation and reclamation occur outside this catch.
    pub(crate) async fn build(
        &mut self,
    ) -> RuntimeOrFatalResult<HotPackedOutcome<ReadyHotTree<P>>> {
        assert!(
            self.phase == BuildPhase::New,
            "hot index pipeline build requires a fresh attempt"
        );
        self.phase = BuildPhase::Extraction;
        match AssertUnwindSafe(self.construct()).catch_unwind().await {
            Ok(result) => result,
            Err(payload) => {
                let report = Report::new(FatalError::ThreadPoolTaskPanic).attach(format!(
                    "operation=hot_index_build, phase={}, table_id={}, index={}, panic={}",
                    self.phase(),
                    self.source.table.table_id(),
                    self.source.key.index,
                    panic_payload_description(payload.as_ref())
                ));
                mem::forget(payload);
                Err(self
                    .poisoner
                    .poison_and_get_first(report)
                    .into_report()
                    .into())
            }
        }
    }

    async fn construct(&mut self) -> RuntimeOrFatalResult<HotPackedOutcome<ReadyHotTree<P>>> {
        let runs = self
            .sort
            .as_mut()
            .unwrap_or_else(|| unreachable!("new pipeline owns extraction"))
            .execute()
            .await?;
        #[cfg(feature = "profiling")]
        {
            self.extraction = Some(runs.measurements);
        }
        self.sort = None;
        self.phase = BuildPhase::MergePreparation;
        self.preparation = Some(HotMergePreparation::new(
            Arc::new(runs),
            self.thread_pool.clone(),
            self.policy.max_workers,
        )?);
        let plan = self
            .preparation
            .as_mut()
            .unwrap_or_else(|| unreachable!("pipeline owns merge preparation"))
            .execute()
            .await?;
        self.preparation = None;
        self.phase = BuildPhase::PackedConstruction;
        let (packing, cleanup) = HotPackedBuild::new(
            self.index_pool.clone(),
            self.index_guard.clone(),
            plan,
            self.thread_pool.clone(),
            self.poisoner.clone(),
            HotPackedSpec {
                unique: self.source.key.unique,
                ts: self.source.key.build_ts,
                cold_validation: self.cold_validation.clone(),
            },
        );
        self.packing = Some(packing);
        self.cleanup = Some(cleanup);
        #[cfg(test)]
        {
            self.hooks.arm_cleanup(self.cleanup.as_ref().unwrap());
            self.hooks.at(tests::Point::Build);
        }
        let outcome = self
            .packing
            .as_mut()
            .unwrap_or_else(|| unreachable!("pipeline owns packed construction"))
            .execute()
            .await;
        self.packing = None;
        let outcome = outcome?;
        self.phase = BuildPhase::Ready;
        #[cfg(test)]
        if matches!(outcome, HotPackedOutcome::Complete(_)) {
            self.hooks.at(tests::Point::Ready);
        }
        Ok(outcome)
    }

    /// Drain accepted stage jobs and reclaim detached pages after the owner decision.
    ///
    /// Accepted jobs and the ready-tree owner supply progress; their completion
    /// slots and the cleanup predicate remain authoritative through poison and
    /// shutdown. The caller retains this pipeline and live storage until this
    /// obligation completes. Cancellation of this borrow permits resuming settle.
    /// Drop or abort an uninstalled ready tree before waiting. Cleanup invariant
    /// panics propagate directly and must never be followed by a cleanup retry.
    pub(crate) async fn settle(&mut self) -> RuntimeOrFatalResult<()> {
        self.phase = BuildPhase::Settlement;
        #[cfg(feature = "profiling")]
        let started = Instant::now();
        let mut result = Ok(());
        if let Some(sort) = &mut self.sort {
            result = merge_build_result(result, sort.settle().await);
        }
        self.sort = None;
        if let Some(preparation) = &mut self.preparation {
            result = merge_build_result(result, preparation.settle().await);
        }
        self.preparation = None;
        if let Some(packing) = &mut self.packing {
            result = merge_build_result(result, packing.settle().await);
        }
        self.packing = None;
        if let Some(cleanup) = &mut self.cleanup {
            #[cfg(test)]
            self.hooks.at(tests::Point::Cleanup);
            result = merge_build_result(result, cleanup.run().await.map_err(Into::into));
        }
        #[cfg(feature = "profiling")]
        {
            self.cleanup_elapsed_nanos += started.elapsed().as_nanos() as u64;
        }
        self.phase = BuildPhase::Settled;
        result
    }

    /// Collect component measurements after the caller installs and settles a build.
    #[cfg(feature = "profiling")]
    #[inline]
    pub(crate) fn measurements<'a>(
        &self,
        ready: &'a ReadyHotTree<P>,
    ) -> (
        HotBuildMeasurements,
        &'a HotMergeMeasurements,
        &'a HotPackedMeasurements,
        u64,
    ) {
        assert!(
            self.phase == BuildPhase::Settled,
            "hot index measurements require settled construction"
        );
        let extraction = self
            .extraction
            .unwrap_or_else(|| unreachable!("ready tree requires completed extraction"));
        let (merge, packed) = ready.measurements();
        (extraction, merge, packed, self.cleanup_elapsed_nanos)
    }
}

/// Combine construction and settlement results with Fatal and first-source precedence.
#[inline]
pub(crate) fn merge_build_result<T>(
    result: RuntimeOrFatalResult<T>,
    cleanup: RuntimeOrFatalResult<()>,
) -> RuntimeOrFatalResult<T> {
    match (result, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => Err(error.merge_cleanup(cleanup)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shared construction edges observed by caller integration tests.
    #[derive(Clone, Copy)]
    pub(crate) enum Point {
        Build,
        Ready,
        Cleanup,
    }

    type Observer = Arc<dyn Fn(Point) + Send + Sync>;
    type CleanupHook<P> =
        Arc<dyn Fn(&StagedPageCleanup<P>) -> Option<flume::Receiver<()>> + Send + Sync>;

    /// Build-local observers and the lifetime of an armed cleanup gate.
    pub(super) struct Hooks<P: 'static> {
        observer: Option<Observer>,
        cleanup: Option<CleanupHook<P>>,
        listener: Option<flume::Receiver<()>>,
    }

    impl<P> Default for Hooks<P> {
        fn default() -> Self {
            Self {
                observer: None,
                cleanup: None,
                listener: None,
            }
        }
    }

    impl<P> Hooks<P> {
        /// Retain the notification receiver before any detached allocation.
        pub(super) fn arm_cleanup(&mut self, cleanup: &StagedPageCleanup<P>) {
            if let Some(hook) = &self.cleanup {
                self.listener = hook(cleanup);
            }
        }

        /// Observe a reached construction or cleanup boundary.
        pub(super) fn at(&self, point: Point) {
            if let Some(observer) = &self.observer {
                observer(point);
            }
        }
    }

    /// Attach build-local stage observers and allocation/reclamation gates.
    pub(crate) fn observe<P: BufferPool>(
        build: &mut HotIndexBuild<P>,
        observer: impl Fn(Point) + Send + Sync + 'static,
        cleanup: impl Fn(&StagedPageCleanup<P>) -> Option<flume::Receiver<()>> + Send + Sync + 'static,
    ) {
        build.hooks.observer = Some(Arc::new(observer));
        build.hooks.cleanup = Some(Arc::new(cleanup));
    }
}
