# Backlog: External-memory CREATE INDEX cold runs

## Summary

Task 000331 completes bounded parallel extraction and direct consumption of resident cold runs. All encoded cold keys still remain resident until durable construction seals. CREATE therefore rejects inputs whose admitted keys and progress capacity exceed the cold scratch budget. Design one bounded run-generation and consumption framework for resident and external sorting, including single-worker builds, without weakening completed-root authority or accepted-work cleanup.

Include DiskTree checkpoint reconstruction from task 000332 as a use case for
the shared sorted-input and bulk-construction interfaces.

## Reference

docs/tasks/000331-parallel-cold-extraction-and-direct-run-consumption.md; docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md Phase 3; docs/backlogs/closed/000104-stream-parallel-create-index-cold-build.md; docs/tasks/000330-completed-root-cross-tier-validation.md; docs/tasks/000332-parallel-disk-tree-checkpoint-reconciliation.md

## Deferred From (Optional)

docs/tasks/000331-parallel-cold-extraction-and-direct-run-consumption.md; docs/rfcs/0033-parallel-disk-tree-construction-and-checkpoint-application.md Phase 3; docs/backlogs/closed/000104-stream-parallel-create-index-cold-build.md; docs/tasks/000332-parallel-disk-tree-checkpoint-reconciliation.md

## Deferral Context (Optional)

- Defer Reason: RFC 0033 limits the current producer to resident runs. Spill storage, memory-bounded run formation and merge lifecycle require a separate design. Task 000331 withdrew its forced single-run policy for one worker so these decisions can be designed together.
- Findings: The current captured source uses P=4W ranges selected from the first level with N>=8P entries or a leaf fallback. Descriptor batches are bounded at 64 and metadata guards do not cross LWC awaits. ColdLocalSort retains admitted runs; DiskBulkBuild accepts them directly through co-rank/loser-tree consumption. The sealed artifact releases all key owners before hot work. Resident key capacity remains the dominant scale-dependent allocation; bounded extraction queues do not make the build external-memory.
- Direction Hint: Preserve exact source/merge coverage, physical key/RowID identity, deterministic earliest duplicate evidence, native source errors and execution-error precedence. Reuse completed-root validation and retained write settlement. Avoid replacing the resident vector with unbounded writer queues, spill bookkeeping or validation keys.

### Single-worker findings from task 000331

One worker already processes multiple runs serially. Forcing one large run saved
per-entry merge work, but did not consistently improve sorting or memory use:

- Ordered narrow keys and newly tested random narrow keys benefited. For 1M
  random rows, median local sort/check plus merge/check fell 58.59→43.78 ms.
- With 200k composite keys and 31 repeated payload prefixes, the same work rose
  23.22→27.51 ms at 128-byte payloads and 58.87→72.31 ms at 512 bytes. Distinct
  leading payloads made the 512-byte penalty much smaller. Width alone is not
  a sufficient run-sizing rule; cache effects were not independently measured.
- Larger entry-vector replacements raised narrow-key scratch peaks about 15 MiB
  despite removing merge buffers. All input still remained resident.
- Deletion-heavy combined sort/merge work was nearly unchanged. Separate redo
  fsync variability affected public timings; do not attribute it to sort work.

Task 000331 restores P=4W at every worker count and retains direct consumption
when the actual nonempty run count is one. This is an interim resident policy,
not an external-sort design. The figures above describe the withdrawn experiment,
not performance claims for a new run-sizing policy.

### Checkpoint findings from task 000332

Reconstruction already streams the old DiskTree through a bounded reader, merges
resident PUT/DELETE runs, and packs surviving entries without retaining the whole
result. Its construction pipeline is separate from CREATE's DiskBulkBuild, whose
key references, fences and output coverage depend on resident input coordinates.
External sorting remains deferred; this shared-interface opportunity should be
considered before extending either builder independently.

## Scope Hint

Design a bounded representation for cold keys exceeding scratch and readonly-cache capacity, including run creation, spill storage, bounded merge fan-in, duplicate checking, durable construction and cancellation/cleanup. Account for descriptors, decoded values, queued work, merge references, pinned frames, write buffers, allocation ledgers and temporary files. Keep final index size distinct from build scratch.

### Unified run and merge direction

- Separate source partitions, active workers, sorted runs and merge fan-in.
  Workers bound concurrency; admitted bytes and a measured run-size target bound
  each run. One source range may emit many runs, even with one worker.
- Reuse the local comparator/sorter and a common sorted-input interface for
  resident runs, spilled runs and DiskTree scans. Preserve the no-merge path when
  input naturally produces one run; avoid a separate single-worker sorting algorithm.
- Bound merge fan-in by admitted read buffers and progress capacity. Support
  intermediate spill passes and direct final-merge consumption by tree construction.
- Budget spill/read buffers, run metadata, merge progress and output capacity
  together. Under memory pressure, spill or release completed run storage before
  accepting more input; bounded descriptor queues alone do not bound retained keys.
- Decouple exact source completion certificates from emitted run count. Adapt
  resident co-rank random access deliberately for external readers rather than
  turning it into unbounded disk seeks or materializing all spilled keys.
- Evaluate skipping co-rank partitioning for a single consumer and concatenating
  runs whose validated physical-key fences prove non-overlap. Preserve boundary
  duplicate checks and completion authority. These are future candidates, not
  implemented or measured improvements.

### Shared construction with checkpoint reconstruction

- Design around bounded sorted cursors or batches: refill may await I/O, while
  comparisons within loaded buffers remain synchronous. Make borrowed-key
  lifetimes explicit and retain only bounded lookahead and owned fences across
  refills; avoid requiring permanent references into resident runs.
- Separate ordering from CREATE uniqueness validation and checkpoint mutation
  reduction. Both should feed sorted final entries into common bulk construction,
  without materializing all checkpoint survivors into resident runs. Preserve
  deterministic provenance and equal-key handling across batch and run boundaries.
- Distinguish exact source/mutation consumption from output cardinality: checkpoint
  reduction can remove entries or replace owners. Preserve owner-sensitive unique
  DELETE handling, exact nonunique deletion and complete mutation groups.
- Keep CREATE sealing and checkpoint root publication with their callers. Retain
  successful write settlement, failure cleanup, empty-result handling and the
  checkpoint's unchanged-root behavior. Keep partial rewrite's subtree-reuse events
  as a specialized extension of the entry-stream design.
- Review size-dependent construction metadata as well as key storage, including
  parent descriptors and allocation ledgers. Spilling keys alone must not move
  unbounded retention into the shared builder.

## Acceptance Hint

Demonstrate complete unique/non-unique CREATE with input larger than configured scratch and cache; record end-to-end latency, CPU, I/O, scratch and sampled RSS. Verify exact contents and multiplicity outside timing. Cover duplicates across spill/merge boundaries, durable/current deletes, source corruption, admission and I/O failures, observer detachment, poison/shutdown and retained cleanup. No partial catalog/root/layout publication or temporary-file/allocation leaks are allowed.

Compare workers 1/2/4 across ordered and shuffled input, narrow and outlined keys,
low/high prefix repetition, and resident/spilling sizes. Include tight budgets
and replacement-growth peaks. Attribute local sorting, merging, packing and
publication separately; verify complete contents and multiplicity after timing.

Validate the shared interfaces against checkpoint reconstruction with an old tree
larger than scratch/cache, PUT/DELETE groups crossing batch boundaries, empty and
unchanged results, and matching unique/nonunique contents. The design must support
resident and spilled mutation inputs without retaining the reconstructed index,
while preserving old-root readability and atomic table-checkpoint publication.

## Notes (Optional)

The single-worker findings supersede that portion of the withdrawn broad
performance proposal in closed backlog 000214. Selector calibration remains in
000213; this item does not claim to resolve filesystem/VM fsync variability.

