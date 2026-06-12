# Backlog: Redesign redo GC and purge worker ownership

## Summary

The shutdown ordering issue in task 000175 exposed a broader design problem in transaction background worker ownership. Today redo IO emits `GC::Commit` messages to a separate GC analyzer thread, and worker shutdown manually coordinates redo IO, GC, cleanup, and purge. A narrow fix can avoid the early `GC::Stop` race, but a better design should re-evaluate whether GC analysis needs a dedicated thread or should be grouped/merged with purge dispatch.

## Reference

Deferred from review work on `docs/tasks/000175-remove-multi-stream-redo.md`.

Relevant current paths and behavior:
- `TransactionSystemWorkers::shutdown` closes `group_commit`, pushes `Commit::Shutdown`, and currently has to coordinate redo IO and GC shutdown.
- `RedoLog::io_loop` / `FileProcessor::sync_io` emit `GC::Commit` after commit groups reach ordered completion.
- `RedoLog::gc_loop` analyzes committed transaction payloads and sends `Purge::Next` when min-active STS may change.
- `PurgeDispatcher`/purge workers consume purge work and must preserve the invariant that global visible STS is not advanced for work that did not run.

User direction: do not just patch the early-stop bug. Consider a cleaner worker design, including whether purge dispatcher or purge workers can own the GC-analysis role and whether redo IO, GC, and purge handles should be grouped.

## Deferred From (Optional)

docs/tasks/000175-remove-multi-stream-redo.md

## Deferral Context (Optional)

- Defer Reason: Task 000175 is focused on removing multi-stream redo and related cleanup. The shutdown issue revealed a larger background-worker topology question. Applying only a local `GC::Stop` reorder or sender-lifetime patch now could preserve a suboptimal split between redo GC analysis and purge scheduling, so this needs separate design attention.
- Findings:
  Current findings to preserve:
  - Sending `GC::Stop` before `io_thread.join()` is unsafe because `io_loop` can still emit a final `GC::Commit`; `gc_loop` would exit at the stop marker and skip later commits.
  - Removing `GC::Stop` only works if the GC channel can close. With the current shape, `RedoLog` owns a long-lived `Sender<GC>`, so `gc_loop` would not exit just because the IO thread stopped.
  - A minimal sender-lifetime fix would move the GC sender into the redo IO worker and let `gc_loop` exit on channel close after draining commits.
  - IO and GC handles do not need to be exposed as separate fields; they can be represented by one redo worker group that joins IO first and GC second.
  - The broader design question remains: GC analysis may be better owned by purge dispatch because it only exists to update GC buckets and trigger purge progression.
  - Prior purge-dispatch review established that failed purge task handoff must be treated as an invariant violation; global visible STS must not advance for buckets whose purge task did not run.
- Direction Hint:
  Prefer a holistic design over a narrow stop-message patch. Compare at least these directions:
  1. Redo worker group: redo IO owns the commit-handoff sender, GC remains a thread, and grouped shutdown joins IO before GC.
  2. Purge-owned GC analysis: redo IO sends committed payload batches to purge dispatcher, which performs GC analysis and schedules purge work in one pipeline.
  3. Unified transaction background worker group: redo, GC analysis, purge dispatch, and purge executors have one explicit owner with clear drain barriers.
  
  The design should decide based on correctness invariants, shutdown simplicity, latency/backpressure, and error propagation. Avoid committing to a one-off `GC::Stop` reorder unless it is explicitly chosen as a tactical patch while the broader refactor remains open.

## Scope Hint

Design/refactor transaction background worker ownership around redo IO, GC analysis, and purge dispatch. Evaluate whether to keep a GC analyzer thread, group it with redo IO, or fold GC analysis into the purge dispatcher/purge worker pipeline. The future work should define ownership of `Sender<GC>`/equivalent commit handoff, shutdown/drain barriers, error handling, and worker join order.

## Acceptance Hint

A future task or RFC presents a decision-complete design and, if approved, implementation that preserves these invariants: every committed transaction payload handed off by redo IO is analyzed before worker shutdown completes; no explicit stop marker can overtake pending commit handoffs; purge/global-visible advancement only happens after required bucket work actually ran; shutdown does not deadlock. Validation includes targeted regression coverage for pending final commit handoff during shutdown plus `cargo fmt`, strict clippy, and `cargo nextest run -p doradb-storage`.

## Notes (Optional)


## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```
