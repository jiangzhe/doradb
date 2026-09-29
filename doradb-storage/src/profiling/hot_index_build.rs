use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Stage measurements; worker durations are sums, not stage wall durations.
///
/// Counts, byte sizes, and nanosecond timings use `u64`; overflow is assumed
/// impossible for supported workloads.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HotBuildMeasurements {
    /// Wall duration of source and descriptor capture.
    pub capture_elapsed_nanos: u64,
    /// Sum of worker extraction and encoding durations.
    pub extraction_worker_time_nanos: u64,
    /// Sum of synchronous owned-entry sort durations.
    pub sort_worker_time_nanos: u64,
    /// Sum of optional local duplicate-check durations.
    pub duplicate_worker_time_nanos: u64,
    /// Longest individual job duration.
    pub max_job_elapsed_nanos: u64,
    /// Longest individual synchronous sort.
    pub max_sort_elapsed_nanos: u64,
    /// Span from first extraction start to last extraction finish.
    pub extraction_wall_elapsed_nanos: u64,
    /// Span from first sort start to last sort finish; may overlap extraction.
    pub sort_wall_elapsed_nanos: u64,
    /// Span of local checking across workers; may overlap other stages.
    pub duplicate_wall_elapsed_nanos: u64,
    /// Coordinator wall duration from submission through final collection.
    pub pipeline_wall_elapsed_nanos: u64,
    /// Capture plus pipeline wall duration.
    pub total_elapsed_nanos: u64,
    /// High-water mark of admitted bulk scratch capacity, excluding bookkeeping
    /// and transient overhead outside the build budget.
    pub scratch_peak_bytes: u64,
    /// Number of captured pages after pivot exclusion.
    pub source_pages: u64,
    /// Total live entries retained across nonempty runs.
    pub entries: u64,
    /// Configured maximum outstanding extraction jobs.
    pub workers: u64,
    /// Configured soft page target for each run.
    pub page_target: u64,
    /// Number of deterministic groups, including empty results.
    pub planned_groups: u64,
    /// Number of retained sorted runs.
    pub nonempty_runs: u64,
}

/// Engine-lifetime snapshot of successfully completed hot-row extraction builds.
///
/// Counts and nanosecond duration sums use ordinary `u64` arithmetic, assuming
/// no overflow. They are monotonic and support before/after deltas.
/// Maxima are lifetime high-water marks and must not be subtracted. Failed or
/// cancelled builds publish no sample. A completed extraction is not a published
/// index; later merge, validation, packing, and publication are outside this report.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HotIndexBuildStats {
    /// Number of successful extraction samples published after child settlement.
    pub completed_builds: u64,
    /// Accumulated `source_pages` across completed builds.
    pub source_pages: u64,
    /// Accumulated `entries` across completed builds.
    pub entries: u64,
    /// Accumulated `planned_groups` across completed builds.
    pub planned_groups: u64,
    /// Accumulated `nonempty_runs` across completed builds.
    pub nonempty_runs: u64,
    /// Sum of per-build `capture_elapsed_nanos`, in nanoseconds.
    pub capture_elapsed_nanos: u64,
    /// Sum of per-build `extraction_worker_time_nanos`, in nanoseconds.
    pub extraction_worker_time_nanos: u64,
    /// Sum of per-build `sort_worker_time_nanos`, in nanoseconds.
    pub sort_worker_time_nanos: u64,
    /// Sum of per-build `duplicate_worker_time_nanos`, in nanoseconds.
    pub duplicate_worker_time_nanos: u64,
    /// Sum of per-build `extraction_wall_elapsed_nanos`, in nanoseconds.
    pub extraction_wall_elapsed_nanos: u64,
    /// Sum of per-build `sort_wall_elapsed_nanos`, in nanoseconds.
    pub sort_wall_elapsed_nanos: u64,
    /// Sum of per-build `duplicate_wall_elapsed_nanos`, in nanoseconds.
    pub duplicate_wall_elapsed_nanos: u64,
    /// Sum of per-build `pipeline_wall_elapsed_nanos`, in nanoseconds.
    pub pipeline_wall_elapsed_nanos: u64,
    /// Sum of per-build `total_elapsed_nanos`, in nanoseconds.
    pub total_elapsed_nanos: u64,
    /// Engine-lifetime maximum `max_job_elapsed_nanos`, in nanoseconds.
    pub max_job_elapsed_nanos: u64,
    /// Engine-lifetime maximum `max_sort_elapsed_nanos`, in nanoseconds.
    pub max_sort_elapsed_nanos: u64,
    /// Largest per-build admitted bulk scratch capacity, in bytes, excluding
    /// bookkeeping and transient overhead outside the build budget.
    pub scratch_peak_bytes: u64,
}

/// Shared engine recorder; one short update per successful extraction.
#[derive(Default)]
pub(crate) struct HotIndexBuildProfiler(Mutex<HotIndexBuildStats>);

impl HotIndexBuildProfiler {
    /// Read a coherent snapshot without resetting counters or maxima.
    pub(crate) fn snapshot(&self) -> HotIndexBuildStats {
        *self.0.lock()
    }

    /// Publish once after all accepted children have completed successfully.
    pub(crate) fn record(&self, sample: HotBuildMeasurements) {
        let mut stats = self.0.lock();
        stats.completed_builds += 1;
        stats.source_pages += sample.source_pages;
        stats.entries += sample.entries;
        stats.planned_groups += sample.planned_groups;
        stats.nonempty_runs += sample.nonempty_runs;
        stats.capture_elapsed_nanos += sample.capture_elapsed_nanos;
        stats.extraction_worker_time_nanos += sample.extraction_worker_time_nanos;
        stats.sort_worker_time_nanos += sample.sort_worker_time_nanos;
        stats.duplicate_worker_time_nanos += sample.duplicate_worker_time_nanos;
        stats.extraction_wall_elapsed_nanos += sample.extraction_wall_elapsed_nanos;
        stats.sort_wall_elapsed_nanos += sample.sort_wall_elapsed_nanos;
        stats.duplicate_wall_elapsed_nanos += sample.duplicate_wall_elapsed_nanos;
        stats.pipeline_wall_elapsed_nanos += sample.pipeline_wall_elapsed_nanos;
        stats.total_elapsed_nanos += sample.total_elapsed_nanos;
        stats.max_job_elapsed_nanos = stats
            .max_job_elapsed_nanos
            .max(sample.max_job_elapsed_nanos);
        stats.max_sort_elapsed_nanos = stats
            .max_sort_elapsed_nanos
            .max(sample.max_sort_elapsed_nanos);
        stats.scratch_peak_bytes = stats.scratch_peak_bytes.max(sample.scratch_peak_bytes);
    }
}

/// Per-worker stage boundaries transferred with an accepted result.
#[derive(Default)]
pub(crate) struct HotBuildWorkerProfile {
    stages: [Option<(Instant, Instant)>; 3],
    job_elapsed_nanos: u64,
}

impl HotBuildWorkerProfile {
    /// Finish local timing; skipped duplicate checks have no stage interval.
    pub(crate) fn finish(
        started: Instant,
        extracted: Instant,
        sorted: Instant,
        completed: Instant,
        checked_duplicates: bool,
    ) -> Self {
        Self {
            stages: [
                Some((started, extracted)),
                Some((extracted, sorted)),
                checked_duplicates.then_some((sorted, completed)),
            ],
            job_elapsed_nanos: (completed - started).as_nanos() as u64,
        }
    }
}

/// Owner-retained timing state, independent of the borrowed extraction future.
pub(crate) struct HotBuildProfile {
    sample: HotBuildMeasurements,
    started: Option<Instant>,
    stage_spans: [Option<(Instant, Instant)>; 3],
}

impl HotBuildProfile {
    /// Capture the fixed source and run plan before submission starts.
    pub(crate) fn new(sample: HotBuildMeasurements) -> Self {
        Self {
            sample,
            started: None,
            stage_spans: [None; 3],
        }
    }

    /// Start once, preserving the original boundary across observer cancellation.
    pub(crate) fn start(&mut self) {
        self.started.get_or_insert_with(Instant::now);
    }

    /// Collect a completed worker without clock reads or cross-worker locks.
    pub(crate) fn collect(&mut self, worker: &HotBuildWorkerProfile, entries: usize) {
        let mut durations = [0; 3];
        for ((span, stage), duration) in self
            .stage_spans
            .iter_mut()
            .zip(worker.stages)
            .zip(&mut durations)
        {
            if let Some((start, end)) = stage {
                *duration = (end - start).as_nanos() as u64;
                *span = Some(match *span {
                    None => (start, end),
                    Some((first, last)) => (first.min(start), last.max(end)),
                });
            }
        }
        self.sample.entries += entries as u64;
        self.sample.extraction_worker_time_nanos += durations[0];
        self.sample.sort_worker_time_nanos += durations[1];
        self.sample.duplicate_worker_time_nanos += durations[2];
        self.sample.max_job_elapsed_nanos = self
            .sample
            .max_job_elapsed_nanos
            .max(worker.job_elapsed_nanos);
        self.sample.max_sort_elapsed_nanos = self.sample.max_sort_elapsed_nanos.max(durations[1]);
    }

    /// Complete a successful build after all child results have been collected.
    pub(crate) fn finish(
        &mut self,
        scratch_peak_bytes: usize,
        nonempty_runs: usize,
    ) -> HotBuildMeasurements {
        let started = self
            .started
            .unwrap_or_else(|| unreachable!("hot-build profiling starts before completion"));
        self.sample.pipeline_wall_elapsed_nanos = started.elapsed().as_nanos() as u64;
        let elapsed = self
            .stage_spans
            .map(|span| span.map_or(0, |(start, end)| (end - start).as_nanos() as u64));
        self.sample.extraction_wall_elapsed_nanos = elapsed[0];
        self.sample.sort_wall_elapsed_nanos = elapsed[1];
        self.sample.duplicate_wall_elapsed_nanos = elapsed[2];
        self.sample.total_elapsed_nanos =
            self.sample.capture_elapsed_nanos + self.sample.pipeline_wall_elapsed_nanos;
        self.sample.scratch_peak_bytes = scratch_peak_bytes as u64;
        self.sample.nonempty_runs = nonempty_runs as u64;
        self.sample
    }
}

/// One fully consumed hot merge, independent of extraction and publication stats.
/// Worker sums include cooperative scheduling; batch durations cover only fused
/// synchronous merge/check work. Consumer work outside pulls is reported separately.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HotMergeMeasurements {
    /// Total input entries.
    pub entries: u64,
    /// Retained nonempty runs.
    pub runs: u64,
    /// Outstanding-job limit.
    pub workers: u64,
    /// Planned partition count.
    pub partitions: u64,
    /// Maximum entries per pull.
    pub batch_entries: u64,
    /// Whether source policy requires duplicate validation.
    pub checked: bool,
    /// Preparation wall time through the boundary barrier.
    pub boundary_wall_nanos: u64,
    /// Sum of interior cut durations.
    pub cut_worker_nanos: u64,
    /// Longest interior cut duration.
    pub max_cut_nanos: u64,
    /// Consumption wall time including consumers and settlement.
    pub consumption_wall_nanos: u64,
    /// Preparation plus consumption start-to-first-output latency.
    pub first_batch_nanos: u64,
    /// Sum of synchronous fused merge/check intervals.
    pub merge_check_nanos: u64,
    /// Longest synchronous pull.
    pub max_batch_nanos: u64,
    /// Longest job, including its consumer.
    pub max_job_nanos: u64,
    /// Sum of whole-job durations, including setup, consumer work and waits.
    pub job_worker_nanos: u64,
    /// Sum of consumer durations excluding synchronous merge/check work.
    /// Includes consumer awaits and cooperative yields, not kernel setup.
    pub consumer_worker_nanos: u64,
    /// Actual duplicate equality calls, including boundary checks.
    pub duplicate_comparisons: u64,
    /// Total retained boundary-position allocation capacity.
    pub boundary_bytes: u64,
    /// Maximum reference allocation capacity in any one partition.
    pub max_reference_bytes: u64,
    /// Upper bound on simultaneous reference capacity at the admitted worker limit.
    pub active_reference_bytes: u64,
    /// Incremental validation fields in the retained plan, streams and completions.
    pub validation_bytes: u64,
    /// Shared admission high-water, including resident source storage.
    pub scratch_peak_bytes: u64,
}

/// Local batch counters; no per-entry clocks or shared writes.
#[derive(Clone, Copy, Default)]
pub(crate) struct HotMergeWorkerProfile {
    /// Start of the first returned batch, relative to job start.
    pub(crate) first_batch: Option<Instant>,
    /// Accumulated fused merge/check time.
    pub(crate) merge_check_nanos: u64,
    /// Longest synchronous pull.
    pub(crate) max_batch_nanos: u64,
    /// Whole consumer job duration.
    pub(crate) job_nanos: u64,
    /// Consumer work and waits outside synchronous pulls.
    pub(crate) consumer_nanos: u64,
    /// Incremental equality calls, excluding shared cuts.
    pub(crate) duplicate_comparisons: u64,
    /// Allocated reusable reference capacity.
    pub(crate) reference_bytes: u64,
}

/// Per-level allocation and occupancy evidence for a privately packed tree.
#[derive(Clone, Debug, Default)]
pub(crate) struct HotPackedLevel {
    /// Height of the constructed pages.
    pub(crate) height: u16,
    /// Detached pages materialized at this level (including the temporary root).
    pub(crate) pages: usize,
    /// Sum of effective bytes, including page headers and excluding integrity trailers.
    pub(crate) occupied_bytes: usize,
    /// Sum of page allocation wait and execution time.
    pub(crate) allocation_nanos: u64,
    /// Sum of append packing time after allocation.
    pub(crate) packing_nanos: u64,
}

/// Component-only construction measurements; publication remains caller-owned.
#[derive(Clone, Debug, Default)]
pub(crate) struct HotPackedMeasurements {
    /// Per-level page counts, occupancy and materialization time.
    pub(crate) levels: Vec<HotPackedLevel>,
    /// Sum of leaf candidate planning time, separate from merge/check and allocation.
    pub(crate) leaf_planning_nanos: u64,
    /// Global root-fit checks and parent-group planning wall time, including yields.
    pub(crate) parent_planning_nanos: u64,
    /// Direct-parent submission through final collection wall time.
    pub(crate) direct_parent_nanos: u64,
    /// Serial higher-level construction wall time, excluding global planning.
    pub(crate) serial_upper_nanos: u64,
    /// Root guard acquisition and synchronous ownership transfer time.
    pub(crate) install_nanos: u64,
    /// Longest uninterrupted leaf/parent planning or page append interval.
    /// Excludes allocation waits and time suspended at cooperative yields.
    pub(crate) max_sync_nanos: u64,
    /// Longest merge/leaf or parent worker interval, including asynchronous waits.
    pub(crate) max_job_nanos: u64,
    /// Peak shared scratch admission including retained input runs.
    pub(crate) scratch_peak_bytes: usize,
}

/// Recovery-only completed index measurements, distinct from extraction samples.
/// Worker sums and overlapping stage spans are attribution within phase wall time.
/// Extraction and merge counts, work, and wall spans add across indexes. Settings,
/// byte capacities, longest intervals, and merge first-batch latency use maxima.
/// Source capture is charged once per table here; selected extraction samples
/// reuse captured descriptors and carry zero capture time.
/// Occupancy includes temporary-root materialization, which installation reclaims.
/// Counts and nanosecond duration sums assume no overflow.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryHotIndexMeasurements {
    /// Accumulated extraction samples for installed indexes; settings and peaks use maxima.
    pub extraction: HotBuildMeasurements,
    /// Accumulated merge samples; limits and high-water values use maxima.
    pub merge: HotMergeMeasurements,
    /// Indexes installed and cleaned successfully.
    pub completed_builds: u64,
    /// Source finalization wall time, counted once per table including index-free tables.
    pub capture_elapsed_nanos: u64,
    /// Detached leaf pages materialized, including a temporary leaf root.
    pub leaf_pages: u64,
    /// Detached branch pages materialized, including the temporary branch root.
    pub branch_pages: u64,
    /// Effective occupied leaf bytes including headers.
    pub leaf_occupied_bytes: u64,
    /// Effective occupied branch bytes including headers.
    pub branch_occupied_bytes: u64,
    /// Sum of page allocation work and waits.
    pub allocation_nanos: u64,
    /// Sum of leaf and branch append packing work.
    pub packing_nanos: u64,
    /// Sum of leaf planning work.
    pub leaf_planning_nanos: u64,
    /// Sum of parent planning wall spans including yields.
    pub parent_planning_nanos: u64,
    /// Sum of direct-parent construction wall spans.
    pub direct_parent_nanos: u64,
    /// Sum of upper-level construction wall spans.
    pub serial_upper_nanos: u64,
    /// Sum of fixed-root installation wall time.
    pub install_nanos: u64,
    /// Sum of terminal cleanup wall time.
    pub cleanup_nanos: u64,
    /// Longest uninterrupted construction planning or packing interval.
    pub max_sync_nanos: u64,
    /// Longest construction worker including waits.
    pub max_job_nanos: u64,
    /// Maximum shared scratch admission across completed indexes; excludes pool pages.
    pub scratch_peak_bytes: u64,
}

impl RecoveryHotIndexMeasurements {
    /// Accumulate one installed and cleaned index, assuming no arithmetic overflow.
    pub(crate) fn record(
        &mut self,
        extraction: HotBuildMeasurements,
        merge: &HotMergeMeasurements,
        packed: &HotPackedMeasurements,
        cleanup_nanos: u64,
    ) {
        self.completed_builds += 1;
        self.cleanup_nanos += cleanup_nanos;
        self.extraction.capture_elapsed_nanos += extraction.capture_elapsed_nanos;
        self.extraction.extraction_worker_time_nanos += extraction.extraction_worker_time_nanos;
        self.extraction.sort_worker_time_nanos += extraction.sort_worker_time_nanos;
        self.extraction.duplicate_worker_time_nanos += extraction.duplicate_worker_time_nanos;
        self.extraction.max_job_elapsed_nanos = self
            .extraction
            .max_job_elapsed_nanos
            .max(extraction.max_job_elapsed_nanos);
        self.extraction.max_sort_elapsed_nanos = self
            .extraction
            .max_sort_elapsed_nanos
            .max(extraction.max_sort_elapsed_nanos);
        self.extraction.extraction_wall_elapsed_nanos += extraction.extraction_wall_elapsed_nanos;
        self.extraction.sort_wall_elapsed_nanos += extraction.sort_wall_elapsed_nanos;
        self.extraction.duplicate_wall_elapsed_nanos += extraction.duplicate_wall_elapsed_nanos;
        self.extraction.pipeline_wall_elapsed_nanos += extraction.pipeline_wall_elapsed_nanos;
        self.extraction.total_elapsed_nanos += extraction.total_elapsed_nanos;
        self.extraction.scratch_peak_bytes = self
            .extraction
            .scratch_peak_bytes
            .max(extraction.scratch_peak_bytes);
        self.extraction.source_pages += extraction.source_pages;
        self.extraction.entries += extraction.entries;
        self.extraction.workers = self.extraction.workers.max(extraction.workers);
        self.extraction.page_target = self.extraction.page_target.max(extraction.page_target);
        self.extraction.planned_groups += extraction.planned_groups;
        self.extraction.nonempty_runs += extraction.nonempty_runs;
        self.merge.entries += merge.entries;
        self.merge.runs += merge.runs;
        self.merge.workers = self.merge.workers.max(merge.workers);
        self.merge.partitions += merge.partitions;
        self.merge.batch_entries = self.merge.batch_entries.max(merge.batch_entries);
        self.merge.boundary_wall_nanos += merge.boundary_wall_nanos;
        self.merge.cut_worker_nanos += merge.cut_worker_nanos;
        self.merge.max_cut_nanos = self.merge.max_cut_nanos.max(merge.max_cut_nanos);
        self.merge.consumption_wall_nanos += merge.consumption_wall_nanos;
        self.merge.first_batch_nanos = self.merge.first_batch_nanos.max(merge.first_batch_nanos);
        self.merge.merge_check_nanos += merge.merge_check_nanos;
        self.merge.max_batch_nanos = self.merge.max_batch_nanos.max(merge.max_batch_nanos);
        self.merge.max_job_nanos = self.merge.max_job_nanos.max(merge.max_job_nanos);
        self.merge.job_worker_nanos += merge.job_worker_nanos;
        self.merge.consumer_worker_nanos += merge.consumer_worker_nanos;
        self.merge.duplicate_comparisons += merge.duplicate_comparisons;
        self.merge.boundary_bytes = self.merge.boundary_bytes.max(merge.boundary_bytes);
        self.merge.max_reference_bytes = self
            .merge
            .max_reference_bytes
            .max(merge.max_reference_bytes);
        self.merge.active_reference_bytes = self
            .merge
            .active_reference_bytes
            .max(merge.active_reference_bytes);
        self.merge.validation_bytes = self.merge.validation_bytes.max(merge.validation_bytes);
        self.merge.scratch_peak_bytes = self.merge.scratch_peak_bytes.max(merge.scratch_peak_bytes);
        self.merge.checked |= merge.checked;
        self.leaf_planning_nanos += packed.leaf_planning_nanos;
        self.parent_planning_nanos += packed.parent_planning_nanos;
        self.direct_parent_nanos += packed.direct_parent_nanos;
        self.serial_upper_nanos += packed.serial_upper_nanos;
        self.install_nanos += packed.install_nanos;
        self.max_sync_nanos = self.max_sync_nanos.max(packed.max_sync_nanos);
        self.max_job_nanos = self.max_job_nanos.max(packed.max_job_nanos);
        self.scratch_peak_bytes = self
            .scratch_peak_bytes
            .max(packed.scratch_peak_bytes as u64);
        for level in &packed.levels {
            if level.height == 0 {
                self.leaf_pages += level.pages as u64;
                self.leaf_occupied_bytes += level.occupied_bytes as u64;
            } else {
                self.branch_pages += level.pages as u64;
                self.branch_occupied_bytes += level.occupied_bytes as u64;
            }
            self.allocation_nanos += level.allocation_nanos;
            self.packing_nanos += level.packing_nanos;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        HotBuildMeasurements, HotBuildProfile, HotBuildWorkerProfile, HotIndexBuildProfiler,
    };
    use std::time::{Duration, Instant};

    /// Purpose: Distinguish accumulated recovery installation work from per-build maxima.
    /// Expected: Counts and stage work add, peaks take maxima, and temporary-root occupancy is counted by height.
    #[test]
    fn recovery_aggregate_adds_work_and_preserves_peaks() {
        use super::{
            HotMergeMeasurements, HotPackedLevel, HotPackedMeasurements,
            RecoveryHotIndexMeasurements,
        };
        let mut stats = RecoveryHotIndexMeasurements::default();
        for peak in [4096, 2048] {
            stats.record(
                HotBuildMeasurements {
                    entries: 7,
                    max_sort_elapsed_nanos: peak,
                    ..Default::default()
                },
                &HotMergeMeasurements {
                    partitions: 2,
                    max_job_nanos: peak,
                    ..Default::default()
                },
                &HotPackedMeasurements {
                    levels: vec![
                        HotPackedLevel {
                            height: 0,
                            pages: 2,
                            occupied_bytes: 100,
                            ..Default::default()
                        },
                        HotPackedLevel {
                            height: 1,
                            pages: 1,
                            occupied_bytes: 30,
                            ..Default::default()
                        },
                    ],
                    scratch_peak_bytes: peak as usize,
                    ..Default::default()
                },
                5,
            );
        }
        assert_eq!(stats.completed_builds, 2);
        assert_eq!(
            (
                stats.extraction.entries,
                stats.merge.partitions,
                stats.cleanup_nanos
            ),
            (14, 4, 10)
        );
        assert_eq!(
            (
                stats.extraction.max_sort_elapsed_nanos,
                stats.merge.max_job_nanos,
                stats.scratch_peak_bytes
            ),
            (4096, 4096, 4096)
        );
        assert_eq!(
            (
                stats.leaf_pages,
                stats.branch_pages,
                stats.leaf_occupied_bytes,
                stats.branch_occupied_bytes
            ),
            (4, 2, 200, 60)
        );
    }

    /// Purpose: Aggregate overlapping stage intervals without inventing work for skipped checks.
    /// Expected: Worker durations add, wall spans cover their endpoints, maxima select the longest job, and skipped checking stays zero.
    #[test]
    fn worker_sums_and_stage_spans() {
        let origin = Instant::now();
        let at = |n| origin + Duration::from_nanos(n);
        for checked in [false, true] {
            let mut profile = HotBuildProfile::new(HotBuildMeasurements::default());
            profile.collect(
                &HotBuildWorkerProfile::finish(at(0), at(10), at(15), at(19), checked),
                7,
            );
            profile.collect(
                &HotBuildWorkerProfile::finish(at(5), at(12), at(23), at(25), checked),
                3,
            );
            assert_eq!(profile.sample.entries, 10);
            assert_eq!(profile.sample.extraction_worker_time_nanos, 17);
            assert_eq!(profile.sample.sort_worker_time_nanos, 16);
            assert_eq!(profile.sample.max_job_elapsed_nanos, 20);
            assert_eq!(profile.sample.max_sort_elapsed_nanos, 11);
            assert_eq!(profile.stage_spans[0], Some((at(0), at(12))));
            assert_eq!(profile.stage_spans[1], Some((at(10), at(23))));
            assert_eq!(
                profile.sample.duplicate_worker_time_nanos,
                if checked { 6 } else { 0 }
            );
            assert_eq!(profile.stage_spans[2], checked.then_some((at(15), at(25))));
        }
    }

    /// Purpose: Preserve completed-build totals and independent lifetime maxima.
    /// Expected: Successive snapshots add counts and nanosecond durations and retain larger prior maxima.
    #[test]
    fn completed_snapshots_and_maxima() {
        let recorder = HotIndexBuildProfiler::default();
        let first = HotBuildMeasurements {
            source_pages: 2,
            entries: 7,
            planned_groups: 2,
            nonempty_runs: 1,
            capture_elapsed_nanos: 3,
            extraction_worker_time_nanos: 11,
            sort_worker_time_nanos: 5,
            duplicate_worker_time_nanos: 2,
            extraction_wall_elapsed_nanos: 7,
            sort_wall_elapsed_nanos: 4,
            duplicate_wall_elapsed_nanos: 2,
            pipeline_wall_elapsed_nanos: 12,
            total_elapsed_nanos: 15,
            max_job_elapsed_nanos: 10,
            max_sort_elapsed_nanos: 4,
            scratch_peak_bytes: 8192,
            ..HotBuildMeasurements::default()
        };
        recorder.record(first);
        recorder.record(HotBuildMeasurements {
            max_job_elapsed_nanos: 6,
            scratch_peak_bytes: 4096,
            ..first
        });
        let stats = recorder.snapshot();
        assert_eq!(
            (
                stats.completed_builds,
                stats.source_pages,
                stats.entries,
                stats.planned_groups,
                stats.nonempty_runs
            ),
            (2, 4, 14, 4, 2)
        );
        assert_eq!(
            (
                stats.capture_elapsed_nanos,
                stats.extraction_worker_time_nanos,
                stats.sort_worker_time_nanos,
                stats.duplicate_worker_time_nanos
            ),
            (6, 22, 10, 4)
        );
        assert_eq!(
            (
                stats.extraction_wall_elapsed_nanos,
                stats.sort_wall_elapsed_nanos,
                stats.duplicate_wall_elapsed_nanos,
                stats.pipeline_wall_elapsed_nanos,
                stats.total_elapsed_nanos
            ),
            (14, 8, 4, 24, 30)
        );
        assert_eq!(
            (
                stats.max_job_elapsed_nanos,
                stats.max_sort_elapsed_nanos,
                stats.scratch_peak_bytes
            ),
            (10, 4, 8192)
        );
    }
}
