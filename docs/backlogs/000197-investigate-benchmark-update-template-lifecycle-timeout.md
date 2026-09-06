# Backlog: Investigate benchmark update template lifecycle timeout under workspace tests

## Summary

Investigate an intermittent timeout in the existing benchmark lifecycle test
`checked_in_update_template_executes_end_to_end` during workspace validation.
One run exceeded the ten-second nextest watchdog; a focused 100-iteration stress
run and a subsequent workspace run passed. The cause remains unknown.

## Reference

- `doradb-bench/tests/lifecycle.rs`: `checked_in_update_template_executes_end_to_end`
  and `run_bench`.
- `doradb-bench/templates/update-rand.toml`, `engine-defaults.toml`, and
  `doradb-bench/src/workload/update.rs`.
- `.config/nextest.toml`: default ten-second per-test timeout with immediate
  termination; `docs/process/unit-test.md`: a passing rerun does not resolve a flake.
- [Task 000299](../tasks/000299-unify-unique-key-mvcc-mutation-api.md), final
  validation on 2026-09-07 after point/range bookkeeping extraction.

## Deferred From (Optional)

[Task 000299](../tasks/000299-unify-unique-key-mvcc-mutation-api.md), final
workspace validation of the small duplication cleanup.

## Deferral Context (Optional)

- Defer Reason:
  The failure did not reproduce in focused stress or the subsequent workspace
  run, and there is insufficient evidence to choose a corrective change. Preserve
  it as an unresolved validation finding for separate investigation rather than
  altering benchmark behavior or increasing timeouts during the ownership-helper
  cleanup. No attribution to the cleanup or a preexisting defect is established.
- Findings:
  - The first workspace run executed 1,963 tests: 1,962 passed and the update
    template lifecycle test timed out. Its captured output contained only the
    test harness start, so it did not identify the stalled benchmark phase.
  - The style audit was also running during that invocation; resource contention
    is a hypothesis, not an established cause.
  - The unchanged failing test passed 100/100 focused stress iterations. A second
    standard workspace run passed 1,963/1,963 tests. The libaio storage suite
    passed 1,847/1,847 tests.
  - The template inserts 1,000 rows, then runs seeded range updates with key
    changes using two threads and four sessions. It includes one warmup and
    three measured runs, with fsync enabled through the shared engine defaults.
  - This test invokes the child through `Command::output`, which waits without
    a test-local deadline and buffers output until exit. The ChildGuard timeout
    helper elsewhere in the test file is not used by `run_bench`.
  - Benchmark sources and timeout configuration were unchanged by task 000299.
    Passing reruns have not explained the original timeout.
- Direction Hint:
  Reproduce under workspace concurrency and compare against the branch before
  the cleanup. Identify whether the child stalls in startup, mutation, commit,
  shutdown, or output collection. Prefer diagnostics tied to production lifecycle
  predicates and a bounded child watchdog that preserves useful failure evidence.
  Distinguish scheduling/I/O contention from a lost wakeup or deadlock before
  choosing a fix. Do not treat retries or a larger timeout as the resolution.

## Scope Hint

Diagnose and fix the benchmark lifecycle timeout or its test orchestration,
with any engine correction limited to a demonstrated cause. Preserve the
checked-in template's end-to-end coverage and keep diagnostic artifacts temporary.

## Acceptance Hint

Establish a reproducible cause or sufficient captured lifecycle evidence,
implement a targeted correction, and validate with focused stress plus workspace
concurrency. Failed child runs must provide actionable diagnostics and clean up
subprocesses. A passing rerun alone is insufficient to close this item.

## Notes (Optional)
