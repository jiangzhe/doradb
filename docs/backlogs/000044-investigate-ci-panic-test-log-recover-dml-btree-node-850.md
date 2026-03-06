# Backlog: Investigate CI panic in test_log_recover_dml at btree_node.rs:850

Filename rule:
- Open backlog item: `docs/backlogs/<6digits>-<follow-up-topic>.md`.
- Closed/archived backlog item: `docs/backlogs/closed/<6digits>-<follow-up-topic>.md`.
- Next id storage: `docs/backlogs/next-id` (single 6-digit line).
- Next id helpers:
  - `tools/backlog.rs init-next-id`
  - `tools/backlog.rs alloc-id`
- Close helpers:
  - `tools/backlog.rs close-doc --id <6digits> --type <type> --detail <text>`
  - `tools/doc-id.rs search-by-id --kind backlog --id <6digits> --scope open`

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
