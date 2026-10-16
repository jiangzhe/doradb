# Backlog: Reduce resident cold build CPU and small-worker overhead

## Summary

Archived after the user withdrew this broad deferral. Task 000331 evaluated and
removed the forced single-run policy; backlog 000212 now retains future unified
run sizing and shared merge improvements. The proposal below is historical, and
remaining measured limits are not treated as fixed.

Investigate remaining CPU cost, small-worker latency and allocator retention in resident cold CREATE after task 000331. Parallel preparation improves targeted four-worker latency, but additional resident runs introduce merge/transport work and one-worker results remain slower than the serial-input baseline. Record this tradeoff explicitly rather than claiming universal speedup.

## Reference

docs/tasks/000331-parallel-cold-extraction-and-direct-run-consumption.md; docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md Phase 3; docs/backlogs/closed/000104-stream-parallel-create-index-cold-build.md; docs/tasks/000330-completed-root-cross-tier-validation.md

## Deferred From (Optional)

docs/tasks/000331-parallel-cold-extraction-and-direct-run-consumption.md; docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md Phase 3; docs/backlogs/closed/000104-stream-parallel-create-index-cold-build.md

## Deferral Context (Optional)

- Defer Reason: The resident producer acceptance targets repeatable parallel cold latency gains. Further tuning of its serial/merge and allocator costs is separate from the approved 4W/8P source-partition policy.
- Findings:
  The initial candidate repeatedly validated full leaf metadata and scanned past assigned ranges. Task 000331 fixed this with validated cache endpoints and binary lower-bound seeks. Remaining overhead must be measured against the final binary: multiple resident runs require global merge work even with one worker, and sampled RSS can rise despite reduced admitted scratch. Metrics distinguish source worker sums, overlapping spans, merge checking and unchanged packing work; these do not by themselves identify every CPU cost.
  
  Final matched observations: W1 unique/non-unique median latency regressed 2.0%/15.4%; W4 CPU rose 75.48 to 103.44 ms and 80.09 to 104.52 ms despite 42.5%/26.2% latency reductions. W4 cold scratch fell while sampled extra RSS rose. The 90%-hot non-unique median regressed 34.46 to 65.23 ms, with identical cold output bytes/counts, source preparation improving 5.90 to 4.98 ms, transaction sync rising 2.55 to 8.35 ms and backend waits 3.86 to 19.05 ms. These overlapping waits do not fully establish causality. Include publication/storage-wait variability in subsequent profiling; retain this observed regression instead of attributing it entirely to source work.
- Direction Hint: Profile before choosing an optimization. Consider reducing avoidable per-row work and allocator/admission contention while preserving exact reservations and output progress capacity. Do not restore an unadmitted global vector or bypass exhaustive duplicate validation. Keep cross-tier selector calibration in its separate follow-up.

## Scope Hint

Profile cold projection/encoding, local run ownership, scheduling, shared admission, co-rank/merge checks and allocator retention with workers 1, 2 and 4. Separate inline and outlined keys, resident input skew, input size and cold/hot ratios. Keep the approved partition algorithm as the baseline; any scheduling/partition policy change needs explicit design evidence.

## Acceptance Hint

Provide at least three fresh interleaved release comparisons per target and retain public latency, process CPU, stage work, actual concurrency, admitted scratch and sampled RSS. Verify complete contents/multiplicity. Preserve all budget-before-allocation, bounded queue, exact coverage, duplicate selection, cancellation/settlement and sealed-key-drop tests. A measured tradeoff with explicit limits is acceptable if an optimization is rejected.

## Notes (Optional)

Duplicate detection also matched backlog 000095 by general token overlap. That item concerns secondary MemIndex garbage-collection proof batching and compare-delete, not CREATE input production; it is unrelated to this measured follow-up. The source umbrella 000104 is being split as explicitly required by task 000331.

## Close Reason

- Type: replaced
- Detail: The broad performance deferral was withdrawn. Task 000331 evaluated a forced single-run policy for one worker and then restored the common 4W rule after workload-dependent results and higher narrow-key scratch peaks. The existing single-run consumer bypass remains. Backlog 000212 now records memory-bounded run formation independent of worker count and shared resident/external merge improvements; 000213 retains selector calibration. Other CPU/RSS and publication observations remain in the task and are not claimed as fixed.
- Closed By: backlog close
- Reference: docs/tasks/000331-parallel-cold-extraction-and-direct-run-consumption.md; docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md Phase 3; docs/backlogs/closed/000104-stream-parallel-create-index-cold-build.md; docs/backlogs/000212-external-memory-create-index-cold-runs.md

- Closed At: 2026-10-16
