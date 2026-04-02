# Backlog: Investigate CI panic in test_log_recover_dml at btree_node.rs:850

## Summary

Investigate intermittent CI panic during trx recovery test with out-of-range slot slice in BTree hint search path; determine concrete root cause and reproducibility.

## Reference

thread 'trx::recover::tests::test_log_recover_dml' panicked at doradb-storage/src/index/btree_node.rs:850:41: range end index 927 out of range for slice of length 926

## Scope Hint

Trace call paths to btree_node::search_key_with_hints during recovery/DML tests, validate latch/optimistic-read invariants, and identify whether stale count/hint window computation or another race/corruption path causes the panic.

## Acceptance Hint

Root cause is identified with evidence (stack/context or deterministic reproducer), and a concrete fix direction plus regression coverage strategy is documented without relying on unproven assumptions.

## Notes (Optional)

Do not assume optimistic read is the root cause; treat it as one hypothesis only.

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

## Close Reason

- Type: already-implemented
- Detail: This panic is no longer actionable. The strongest available fix reference is task 000056 / commit `f9bf477` (`recovery cutoff and consistency checks`), which materially changed recovery replay/bootstrap behavior in `doradb-storage/src/trx/recover.rs` and explicitly reran the `test_log_recover_` suite. `search_key_with_hints` itself has not changed since hints were introduced, which supports the conclusion that the intermittent panic was fixed indirectly by the recovery logic change rather than by a direct B-tree hint-path patch.
- Closed By: backlog close
- Reference: docs/tasks/000056-recovery-cutoff-and-consistency-checks.md; commit f9bf477 feat: recovery cutoff and consistency checks (#401)

- Closed At: 2026-04-03
