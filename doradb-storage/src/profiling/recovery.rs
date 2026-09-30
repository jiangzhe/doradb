use super::{RecoveryHotIndexMeasurements, duration_nanos, measurement_error};
use crate::error::{DiscloseError, RuntimeResult as Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Immutable diagnostics for one successful engine bootstrap.
///
/// The outer intervals partition bootstrap wall time. Redo metrics are nested
/// attribution within transaction bootstrap, not additional elapsed time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Internal startup envelope through ready owner assembly.
    pub bootstrap_elapsed: Duration,
    /// Configuration, root setup, and components preceding the catalog.
    pub engine_setup_elapsed: Duration,
    /// Complete catalog construction, including checkpoint loading.
    pub catalog_bootstrap_elapsed: Duration,
    /// Complete transaction-system component construction.
    pub transaction_bootstrap_elapsed: Duration,
    /// Remaining workers, header durability, layout handling, and owner assembly.
    pub runtime_startup_elapsed: Duration,
    /// Intervals nested within transaction-system construction.
    pub phases: RecoveryPhaseTimings,
    /// Observed replay and reconstruction work.
    pub work: RecoveryWorkCounts,
    /// Consumer-side redo stream attribution.
    pub redo: RecoveryRedoMetrics,
    /// Successfully installed hot indexes, with overlapping stage attribution.
    pub hot_indexes: RecoveryHotIndexMeasurements,
}

impl RecoveryReport {
    /// Completes derived intervals and counts from disjoint recovery measurements.
    pub(crate) fn finish_transaction(&mut self, elapsed: Duration) {
        self.transaction_bootstrap_elapsed = elapsed;
        let phases = &mut self.phases;
        let accounted = phases.preparation_elapsed
            + phases.user_table_bootstrap_elapsed
            + phases.redo_planning_elapsed
            + phases.redo_replay_elapsed
            + phases.validation_elapsed
            + phases.absent_file_cleanup_elapsed
            + phases.hot_index_rebuild_elapsed
            + phases.redo_repair_planning_elapsed
            + phases.redo_finalize_elapsed;
        // Phase measurements are disjoint intervals inside transaction bootstrap.
        phases.other_elapsed = elapsed.saturating_sub(accounted);
        let work = &mut self.work;
        work.user_row_ops_applied =
            work.hot_inserts + work.hot_updates + work.hot_deletes + work.cold_deletes;
        work.catalog_row_ops_skipped = work.catalog_row_ops_seen - work.catalog_row_ops_applied;
        work.user_row_ops_skipped = work.user_row_ops_seen - work.user_row_ops_applied;
        let redo = &mut self.redo;
        let nested =
            redo.receive_wait_elapsed + redo.group_decode_elapsed + redo.reader_shutdown_elapsed;
        // Receives, decoding, and shutdown are disjoint work within refill;
        // all refill calls finish inside the coordinator's replay interval.
        redo.stream_other_elapsed = redo.stream_refill_elapsed.saturating_sub(nested);
        redo.apply_and_dispatch_elapsed = phases
            .redo_replay_elapsed
            .saturating_sub(redo.stream_refill_elapsed);
    }
}

/// Elapsed intervals within transaction-system bootstrap.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryPhaseTimings {
    /// Recovery resources, redo discovery, and coordinator construction.
    pub preparation_elapsed: Duration,
    /// Checkpointed user tables, cleanup, and replay-bound seeding.
    pub user_table_bootstrap_elapsed: Duration,
    /// Replay-suffix planning and read-ahead launch.
    pub redo_planning_elapsed: Duration,
    /// Redo stream consumption, application, and final parallel replay drain.
    pub redo_replay_elapsed: Duration,
    /// Catalog, descriptor, table-root, and index lifecycle validation.
    pub validation_elapsed: Duration,
    /// Post-replay provisional-file cleanup.
    pub absent_file_cleanup_elapsed: Duration,
    /// Replay-sidecar consumption and final hot-index reconstruction.
    pub hot_index_rebuild_elapsed: Duration,
    /// Accepted-prefix repair and startup-file policy selection; excludes later repair IO.
    pub redo_repair_planning_elapsed: Duration,
    /// Construction of writable redo startup resources.
    pub redo_finalize_elapsed: Duration,
    /// Remaining transaction-component construction time.
    pub other_elapsed: Duration,
}

/// Integral work observed during recovery; excluded segments have no invented row counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryWorkCounts {
    /// Redo segment filenames discovered at startup.
    pub redo_segments_discovered: u64,
    /// Segments selected for body replay.
    pub redo_segments_selected: u64,
    /// Decoded catalog RowRedo entries, before filtering.
    pub catalog_row_ops_seen: u64,
    /// Successfully applied catalog RowRedo entries, including DDL payloads.
    pub catalog_row_ops_applied: u64,
    /// Decoded catalog RowRedo entries excluded by replay boundaries.
    pub catalog_row_ops_skipped: u64,
    /// Decoded user RowRedo entries, before filtering.
    pub user_row_ops_seen: u64,
    /// Successfully applied user RowRedo entries.
    pub user_row_ops_applied: u64,
    /// Decoded user RowRedo entries excluded by replay boundaries.
    pub user_row_ops_skipped: u64,
    /// Successfully replayed hot inserts.
    pub hot_inserts: u64,
    /// Successfully replayed hot updates.
    pub hot_updates: u64,
    /// Successfully replayed hot deletes.
    pub hot_deletes: u64,
    /// Successfully replayed cold deletes.
    pub cold_deletes: u64,
    /// User tables loaded from the catalog checkpoint.
    pub checkpoint_user_tables: u64,
    /// Successfully allocated replay pages, including pages later dropped.
    pub hot_pages_reconstructed: u64,
    /// Final hot pages visited by index reconstruction.
    pub index_rebuild_pages: u64,
    /// Successful insertions across all active hot indexes.
    pub index_entries_inserted: u64,
}

/// Nested consumer-side stream measurements; worker execution overlaps replay.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryRedoMetrics {
    /// Complete consumer refill calls, including waits, decoding, and reader termination.
    pub stream_refill_elapsed: Duration,
    /// Channel receive time, including scheduling and immediate receive overhead.
    pub receive_wait_elapsed: Duration,
    /// Transaction-frame deserialization, timed once per validated group.
    pub group_decode_elapsed: Duration,
    /// Reader stop and join time inside stream termination.
    pub reader_shutdown_elapsed: Duration,
    /// Refill time excluding receives, decoding, and reader shutdown.
    pub stream_other_elapsed: Duration,
    /// Consumer replay time excluding refill; includes dispatch, filtering, and final drain.
    /// This is elapsed time, not summed worker CPU time.
    pub apply_and_dispatch_elapsed: Duration,
    /// Complete validated groups decoded.
    pub groups_decoded: u64,
    /// Decoded transactions, including those later filtered.
    pub transactions_decoded: u64,
    /// Data blocks received by the consumer, including terminal/tail blocks.
    pub data_blocks_consumed: u64,
    /// Full buffer bytes consumed, excluding metadata and unused read-ahead.
    pub consumed_bytes: u64,
    /// Logical payload bytes in complete validated groups.
    pub validated_payload_bytes: u64,
}

/// Strict benchmark representation of storage `RecoveryMeasurements`; durations are u64 nanoseconds.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryMeasurements {
    /// Internal startup envelope through ready owner assembly.
    pub bootstrap_elapsed_nanos: u64,
    /// Configuration, root setup, and components preceding the catalog.
    pub engine_setup_elapsed_nanos: u64,
    /// Complete catalog construction, including checkpoint loading.
    pub catalog_bootstrap_elapsed_nanos: u64,
    /// Complete transaction-system component construction.
    pub transaction_bootstrap_elapsed_nanos: u64,
    /// Remaining workers, header durability, layout handling, and owner assembly.
    pub runtime_startup_elapsed_nanos: u64,
    /// Intervals nested within transaction-system construction.
    pub phases: RecoveryPhaseMeasurements,
    /// Observed replay and reconstruction work.
    pub work: RecoveryWorkCounts,
    /// Consumer-side redo stream attribution.
    pub redo: RecoveryRedoMeasurements,
    /// Completed-index attribution; absent in older reports recorded without profiling.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hot_indexes: Option<RecoveryHotIndexMeasurements>,
}

impl TryFrom<&RecoveryReport> for RecoveryMeasurements {
    type Error = crate::Error;

    /// Validate and copy one immutable successful storage report.
    fn try_from(report: &RecoveryReport) -> crate::Result<Self> {
        let result = Self {
            bootstrap_elapsed_nanos: duration_nanos(report.bootstrap_elapsed),
            engine_setup_elapsed_nanos: duration_nanos(report.engine_setup_elapsed),
            catalog_bootstrap_elapsed_nanos: duration_nanos(report.catalog_bootstrap_elapsed),
            transaction_bootstrap_elapsed_nanos: duration_nanos(
                report.transaction_bootstrap_elapsed,
            ),
            runtime_startup_elapsed_nanos: duration_nanos(report.runtime_startup_elapsed),
            phases: RecoveryPhaseMeasurements::from_storage(&report.phases),
            work: report.work,
            redo: RecoveryRedoMeasurements::from_storage(&report.redo),
            hot_indexes: Some(report.hot_indexes),
        };
        validate_recovery_report(&result).map_err(DiscloseError::disclose)?;
        Ok(result)
    }
}

/// Strict benchmark representation of storage `RecoveryPhaseMeasurements`; durations are u64 nanoseconds.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPhaseMeasurements {
    /// Recovery resources, redo discovery, and coordinator construction.
    pub preparation_elapsed_nanos: u64,
    /// Checkpointed user tables, cleanup, and replay-bound seeding.
    pub user_table_bootstrap_elapsed_nanos: u64,
    /// Replay-suffix planning and read-ahead launch.
    pub redo_planning_elapsed_nanos: u64,
    /// Redo stream consumption, application, and termination.
    pub redo_replay_elapsed_nanos: u64,
    /// Catalog, descriptor, table-root, and index lifecycle validation.
    pub validation_elapsed_nanos: u64,
    /// Post-replay provisional-file cleanup.
    pub absent_file_cleanup_elapsed_nanos: u64,
    /// Replay-sidecar consumption and final hot-index reconstruction.
    pub hot_index_rebuild_elapsed_nanos: u64,
    /// Accepted-prefix repair and startup-file policy selection; excludes later repair IO.
    pub redo_repair_planning_elapsed_nanos: u64,
    /// Construction of writable redo startup resources.
    pub redo_finalize_elapsed_nanos: u64,
    /// Remaining transaction-component construction time.
    pub other_elapsed_nanos: u64,
}

impl RecoveryPhaseMeasurements {
    fn from_storage(report: &RecoveryPhaseTimings) -> Self {
        Self {
            preparation_elapsed_nanos: duration_nanos(report.preparation_elapsed),
            user_table_bootstrap_elapsed_nanos: duration_nanos(report.user_table_bootstrap_elapsed),
            redo_planning_elapsed_nanos: duration_nanos(report.redo_planning_elapsed),
            redo_replay_elapsed_nanos: duration_nanos(report.redo_replay_elapsed),
            validation_elapsed_nanos: duration_nanos(report.validation_elapsed),
            absent_file_cleanup_elapsed_nanos: duration_nanos(report.absent_file_cleanup_elapsed),
            hot_index_rebuild_elapsed_nanos: duration_nanos(report.hot_index_rebuild_elapsed),
            redo_repair_planning_elapsed_nanos: duration_nanos(report.redo_repair_planning_elapsed),
            redo_finalize_elapsed_nanos: duration_nanos(report.redo_finalize_elapsed),
            other_elapsed_nanos: duration_nanos(report.other_elapsed),
        }
    }
}

/// Strict benchmark representation of storage `RecoveryRedoMeasurements`; durations are u64 nanoseconds.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryRedoMeasurements {
    /// Complete consumer refill calls, including waits, decoding, and reader termination.
    pub stream_refill_elapsed_nanos: u64,
    /// Channel receive time, including scheduling and immediate receive overhead.
    pub receive_wait_elapsed_nanos: u64,
    /// Transaction-frame deserialization, timed once per validated group.
    pub group_decode_elapsed_nanos: u64,
    /// Reader stop and join time inside stream termination.
    pub reader_shutdown_elapsed_nanos: u64,
    /// Refill time excluding receives, decoding, and reader shutdown.
    pub stream_other_elapsed_nanos: u64,
    /// Replay time excluding refill; includes application, dispatch, and filtering.
    pub apply_and_dispatch_elapsed_nanos: u64,
    /// Complete validated groups decoded.
    pub groups_decoded: u64,
    /// Decoded transactions, including those later filtered.
    pub transactions_decoded: u64,
    /// Data blocks received by the consumer, including terminal/tail blocks.
    pub data_blocks_consumed: u64,
    /// Full buffer bytes consumed, excluding metadata and unused read-ahead.
    pub consumed_bytes: u64,
    /// Logical payload bytes in complete validated groups.
    pub validated_payload_bytes: u64,
}

impl RecoveryRedoMeasurements {
    fn from_storage(report: &RecoveryRedoMetrics) -> Self {
        Self {
            stream_refill_elapsed_nanos: duration_nanos(report.stream_refill_elapsed),
            receive_wait_elapsed_nanos: duration_nanos(report.receive_wait_elapsed),
            group_decode_elapsed_nanos: duration_nanos(report.group_decode_elapsed),
            reader_shutdown_elapsed_nanos: duration_nanos(report.reader_shutdown_elapsed),
            stream_other_elapsed_nanos: duration_nanos(report.stream_other_elapsed),
            apply_and_dispatch_elapsed_nanos: duration_nanos(report.apply_and_dispatch_elapsed),
            groups_decoded: report.groups_decoded,
            transactions_decoded: report.transactions_decoded,
            data_blocks_consumed: report.data_blocks_consumed,
            consumed_bytes: report.consumed_bytes,
            validated_payload_bytes: report.validated_payload_bytes,
        }
    }
}

/// Successful hot-row mutations in a replay batch or collected recovery work.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RowReplayCounts {
    /// Successfully applied inserts.
    pub(crate) inserts: u64,
    /// Successfully applied updates.
    pub(crate) updates: u64,
    /// Successfully applied deletes.
    pub(crate) deletes: u64,
}

/// Value-only terminal report; no table or pool guard crosses the channel.
#[derive(Default)]
pub(crate) struct RecoveryHotIndexReport {
    /// Validated hot source pages, counted once per table.
    pub(crate) pages: u64,
    /// Entries installed and cleaned across all selected indexes.
    pub(crate) entries: u64,
    /// Completed index stage measurements without resource owners.
    pub(crate) measurements: RecoveryHotIndexMeasurements,
}

fn check_recovery_sum(actual: u64, components: &[u64]) -> Result<()> {
    let expected = components.iter().sum::<u64>();
    if actual != expected {
        return Err(measurement_error("recovery metric accounting mismatch"));
    }
    Ok(())
}

fn validate_recovery_report(report: &RecoveryMeasurements) -> Result<()> {
    check_recovery_sum(
        report.bootstrap_elapsed_nanos,
        &[
            report.engine_setup_elapsed_nanos,
            report.catalog_bootstrap_elapsed_nanos,
            report.transaction_bootstrap_elapsed_nanos,
            report.runtime_startup_elapsed_nanos,
        ],
    )?;
    let phases = &report.phases;
    check_recovery_sum(
        report.transaction_bootstrap_elapsed_nanos,
        &[
            phases.preparation_elapsed_nanos,
            phases.user_table_bootstrap_elapsed_nanos,
            phases.redo_planning_elapsed_nanos,
            phases.redo_replay_elapsed_nanos,
            phases.validation_elapsed_nanos,
            phases.absent_file_cleanup_elapsed_nanos,
            phases.hot_index_rebuild_elapsed_nanos,
            phases.redo_repair_planning_elapsed_nanos,
            phases.redo_finalize_elapsed_nanos,
            phases.other_elapsed_nanos,
        ],
    )?;
    validate_recovery_redo(phases.redo_replay_elapsed_nanos, &report.redo)?;
    validate_recovery_work(&report.work)
}

fn validate_recovery_redo(
    replay_elapsed_nanos: u64,
    redo: &RecoveryRedoMeasurements,
) -> Result<()> {
    check_recovery_sum(
        replay_elapsed_nanos,
        &[
            redo.stream_refill_elapsed_nanos,
            redo.apply_and_dispatch_elapsed_nanos,
        ],
    )?;
    check_recovery_sum(
        redo.stream_refill_elapsed_nanos,
        &[
            redo.receive_wait_elapsed_nanos,
            redo.group_decode_elapsed_nanos,
            redo.reader_shutdown_elapsed_nanos,
            redo.stream_other_elapsed_nanos,
        ],
    )?;
    if redo.consumed_bytes < redo.validated_payload_bytes {
        return Err(measurement_error(
            "recovery validated payload bytes exceed consumed bytes",
        ));
    }
    Ok(())
}

fn validate_recovery_work(work: &RecoveryWorkCounts) -> Result<()> {
    check_recovery_sum(
        work.catalog_row_ops_seen,
        &[work.catalog_row_ops_applied, work.catalog_row_ops_skipped],
    )?;
    check_recovery_sum(
        work.user_row_ops_seen,
        &[work.user_row_ops_applied, work.user_row_ops_skipped],
    )?;
    check_recovery_sum(
        work.user_row_ops_applied,
        &[
            work.hot_inserts,
            work.hot_updates,
            work.hot_deletes,
            work.cold_deletes,
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiling::{HotBuildMeasurements, HotMergeMeasurements};

    /// Purpose: Finalize nested recovery timings when measured components exceed their envelope.
    /// Expected: Valid residual intervals remain exact and negative residuals become zero.
    #[test]
    fn recovery_timing_residuals_saturate_at_zero() {
        for (case, elapsed, replay, refill, receive, expected) in [
            ("positive", 20, 9, 7, 2, [8, 5, 2]),
            ("equal", 6, 3, 3, 3, [0, 0, 0]),
            ("exceeds envelope", 2, 3, 5, 7, [0, 0, 0]),
        ] {
            let mut report = RecoveryReport {
                phases: RecoveryPhaseTimings {
                    preparation_elapsed: Duration::from_nanos(3),
                    redo_replay_elapsed: Duration::from_nanos(replay),
                    ..RecoveryPhaseTimings::default()
                },
                redo: RecoveryRedoMetrics {
                    stream_refill_elapsed: Duration::from_nanos(refill),
                    receive_wait_elapsed: Duration::from_nanos(receive),
                    ..RecoveryRedoMetrics::default()
                },
                ..RecoveryReport::default()
            };
            report.finish_transaction(Duration::from_nanos(elapsed));
            assert_eq!(
                [
                    report.phases.other_elapsed.as_nanos(),
                    report.redo.stream_other_elapsed.as_nanos(),
                    report.redo.apply_and_dispatch_elapsed.as_nanos(),
                ],
                expected,
                "{case}"
            );
        }
    }

    /// Purpose: Preserve the recovery report schema while reusing storage measurement types.
    /// Expected: Measurements round-trip losslessly and unknown fields at every nested level are rejected.
    #[test]
    fn recovery_measurements_round_trip_with_strict_fields() {
        let storage = RecoveryReport {
            hot_indexes: RecoveryHotIndexMeasurements {
                extraction: HotBuildMeasurements {
                    entries: u64::MAX,
                    source_pages: 7,
                    ..HotBuildMeasurements::default()
                },
                merge: HotMergeMeasurements {
                    checked: true,
                    partitions: 3,
                    ..HotMergeMeasurements::default()
                },
                completed_builds: 2,
                scratch_peak_bytes: 4096,
                ..RecoveryHotIndexMeasurements::default()
            },
            ..RecoveryReport::default()
        };
        let report = RecoveryMeasurements::try_from(&storage).unwrap();
        assert_eq!(report.hot_indexes, Some(storage.hot_indexes));
        let encoded = toml::to_string(&report).unwrap();
        assert_eq!(
            toml::from_str::<RecoveryMeasurements>(&encoded).unwrap(),
            report
        );
        for path in [
            "",
            "work",
            "phases",
            "redo",
            "hot_indexes",
            "hot_indexes.extraction",
            "hot_indexes.merge",
        ] {
            let invalid = if path.is_empty() {
                format!("unknown_measurement = 1\n{encoded}")
            } else {
                let header = format!("[{path}]\n");
                assert!(encoded.contains(&header), "missing {path}");
                encoded.replacen(&header, &format!("{header}unknown_measurement = 1\n"), 1)
            };
            let error = toml::from_str::<RecoveryMeasurements>(&invalid).unwrap_err();
            assert!(
                error.to_string().contains("unknown_measurement"),
                "{path}: {error}"
            );
        }
        let mut historical = report;
        historical.hot_indexes = None;
        let encoded = toml::to_string(&historical).unwrap();
        assert!(!encoded.contains("hot_indexes"));
        assert_eq!(
            toml::from_str::<RecoveryMeasurements>(&encoded).unwrap(),
            historical
        );
    }

    /// Purpose: Preserve recovery timing precision across conversion and serialization.
    /// Expected: Representable durations remain lossless through their largest supported value.
    #[test]
    fn recovery_duration_conversion_and_numeric_round_trip_preserve_precision() {
        let duration = Duration::from_nanos(u64::MAX);
        let storage = RecoveryReport {
            bootstrap_elapsed: duration,
            transaction_bootstrap_elapsed: duration,
            phases: RecoveryPhaseTimings {
                redo_replay_elapsed: duration,
                ..RecoveryPhaseTimings::default()
            },
            redo: RecoveryRedoMetrics {
                stream_refill_elapsed: duration,
                receive_wait_elapsed: duration,
                ..RecoveryRedoMetrics::default()
            },
            ..RecoveryReport::default()
        };
        let report = RecoveryMeasurements::try_from(&storage).unwrap();
        assert_eq!(report.bootstrap_elapsed_nanos, u64::MAX);
        assert_eq!(report.phases.redo_replay_elapsed_nanos, u64::MAX);
        assert_eq!(report.redo.receive_wait_elapsed_nanos, u64::MAX);
        let encoded = toml::to_string(&report).unwrap();
        assert!(encoded.contains(&format!("bootstrap_elapsed_nanos = {}\n", u64::MAX)));
        assert_eq!(
            toml::from_str::<RecoveryMeasurements>(&encoded).unwrap(),
            report
        );
    }

    /// Purpose: Keep top-level recovery timings associated with their original components.
    /// Expected: Conversion preserves each component's duration without exchanging fields.
    #[test]
    fn recovery_report_duration_conversion_preserves_field_mapping() {
        let storage = RecoveryReport {
            bootstrap_elapsed: Duration::from_nanos(110),
            engine_setup_elapsed: Duration::from_nanos(11),
            catalog_bootstrap_elapsed: Duration::from_nanos(22),
            transaction_bootstrap_elapsed: Duration::from_nanos(33),
            runtime_startup_elapsed: Duration::from_nanos(44),
            phases: RecoveryPhaseTimings {
                other_elapsed: Duration::from_nanos(33),
                ..RecoveryPhaseTimings::default()
            },
            ..RecoveryReport::default()
        };
        let report = RecoveryMeasurements::try_from(&storage).unwrap();
        assert_eq!(report.bootstrap_elapsed_nanos, 110);
        assert_eq!(report.engine_setup_elapsed_nanos, 11);
        assert_eq!(report.catalog_bootstrap_elapsed_nanos, 22);
        assert_eq!(report.transaction_bootstrap_elapsed_nanos, 33);
        assert_eq!(report.runtime_startup_elapsed_nanos, 44);
    }

    /// Purpose: Keep recovery phase timings associated with their original phases.
    /// Expected: Conversion preserves each phase's duration without exchanging fields.
    #[test]
    fn recovery_phase_duration_conversion_preserves_field_mapping() {
        let storage = RecoveryPhaseTimings {
            preparation_elapsed: Duration::from_nanos(1),
            user_table_bootstrap_elapsed: Duration::from_nanos(2),
            redo_planning_elapsed: Duration::from_nanos(3),
            redo_replay_elapsed: Duration::from_nanos(4),
            validation_elapsed: Duration::from_nanos(5),
            absent_file_cleanup_elapsed: Duration::from_nanos(6),
            hot_index_rebuild_elapsed: Duration::from_nanos(7),
            redo_repair_planning_elapsed: Duration::from_nanos(8),
            redo_finalize_elapsed: Duration::from_nanos(9),
            other_elapsed: Duration::from_nanos(10),
        };
        let phases = RecoveryPhaseMeasurements::from_storage(&storage);
        assert_eq!(phases.preparation_elapsed_nanos, 1);
        assert_eq!(phases.user_table_bootstrap_elapsed_nanos, 2);
        assert_eq!(phases.redo_planning_elapsed_nanos, 3);
        assert_eq!(phases.redo_replay_elapsed_nanos, 4);
        assert_eq!(phases.validation_elapsed_nanos, 5);
        assert_eq!(phases.absent_file_cleanup_elapsed_nanos, 6);
        assert_eq!(phases.hot_index_rebuild_elapsed_nanos, 7);
        assert_eq!(phases.redo_repair_planning_elapsed_nanos, 8);
        assert_eq!(phases.redo_finalize_elapsed_nanos, 9);
        assert_eq!(phases.other_elapsed_nanos, 10);
    }

    /// Purpose: Keep validated redo payload within consumed input.
    /// Expected: Accounting accepts covered payloads and rejects payload sizes exceeding
    /// consumed bytes.
    #[test]
    fn recovery_redo_payload_cannot_exceed_consumed_bytes() {
        for (consumed_bytes, validated_payload_bytes, valid) in [
            (0, 0, true),
            (64, 64, true),
            (4096, 128, true),
            (u64::MAX, u64::MAX, true),
            (u64::MAX, 0, true),
            (0, 1, false),
            (127, 128, false),
            (u64::MAX - 1, u64::MAX, false),
        ] {
            let storage = RecoveryReport {
                redo: RecoveryRedoMetrics {
                    consumed_bytes,
                    validated_payload_bytes,
                    ..RecoveryRedoMetrics::default()
                },
                ..RecoveryReport::default()
            };
            let result = RecoveryMeasurements::try_from(&storage);
            assert_eq!(
                result.is_ok(),
                valid,
                "consumed_bytes={consumed_bytes}, validated_payload_bytes={validated_payload_bytes}"
            );
            if !valid {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("recovery validated payload bytes exceed consumed bytes")
                );
            }
        }
    }

    /// Purpose: Require complete and internally consistent recovery metrics.
    /// Expected: Balanced reports are accepted while inconsistent work or timing totals are rejected.
    #[test]
    fn recovery_conversion_rejects_inconsistent_accounting() {
        let mut report = RecoveryReport::default();
        assert!(RecoveryMeasurements::try_from(&report).is_ok());
        report.work.user_row_ops_seen = 1;
        assert!(RecoveryMeasurements::try_from(&report).is_err());
        report.work.user_row_ops_skipped = 1;
        assert!(RecoveryMeasurements::try_from(&report).is_ok());
        report.redo.receive_wait_elapsed = Duration::from_nanos(1);
        assert!(RecoveryMeasurements::try_from(&report).is_err());
    }
}
