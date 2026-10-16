# Backlog: Calibrate completed-root cross-tier validation selection

## Summary

Recalibrate automatic cross-tier validation selection after completed-root reads and parallel cold preparation. Task 000330 optimized batched forward lookup and checked borrowed traversal; task 000331 changes preparation latency and memory but intentionally preserves selector weights. Measure crossover points rather than inferring them from total CREATE time.

## Reference

docs/tasks/000331-parallel-cold-extraction-and-direct-run-consumption.md; docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md Phase 3; docs/backlogs/closed/000104-stream-parallel-create-index-cold-build.md; docs/tasks/000330-completed-root-cross-tier-validation.md; target/performance-000331/report.md

## Deferred From (Optional)

docs/tasks/000331-parallel-cold-extraction-and-direct-run-consumption.md; docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md Phase 3; docs/backlogs/closed/000104-stream-parallel-create-index-cold-build.md

## Deferral Context (Optional)

- Defer Reason: Selector calibration is explicitly deferred from the resident producer phase and needs a dedicated matrix across strategy, shape and cache conditions.
- Findings: Task 000330 established sealed completed-root authority, bounded readers, reverse range certificates, batched lookups and borrowed entry views. Task 000331 exposes source and merge measurements and removes serial cold preparation. Its mixed/hot control observations do not establish optimal cross-tier thresholds: source spans overlap and public CREATE also contains hot packing and publication. Earlier calibration predates the read-path changes.
- Direction Hint: Retain the exact completion/input bindings and error precedence. Use the optimized read paths with automatic selection unchanged as the control, then justify any crossover policy with direct validation measurements; do not tune from a single total-CREATE sample.

## Scope Hint

Compare automatic and forced forward, ordered and reverse completed-root validation at fixed input across hot/cold ratios, absolute sizes, key widths, duplicate distributions, skew, overlap and cache budgets. Separate preparation, validation, hot construction and publication timing. Adjust selection only when repeated matched evidence supports the choice.

## Acceptance Hint

Use at least three fresh interleaved release observations per chosen case and complete membership/multiplicity oracles outside timing. Report chosen strategy, public latency, validation wall/worker time, logical and physical reads, CPU, admitted scratch and sampled RSS. Exercise strategy boundaries and all duplicate/error precedence cases; all forced strategies must agree with the independent oracle. Document uncertainty and retain raw observations, normalized plans and source/binary identities.

## Notes (Optional)


