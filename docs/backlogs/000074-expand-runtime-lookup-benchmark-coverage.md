# Backlog: Expand Runtime Lookup Benchmark Coverage

## Summary

Broaden runtime lookup benchmarking from hot-cache block-index lookup to cold-cache and end-to-end persisted point-read measurement.

## Reference

Task 000097 proved parity between the compatibility and resolved lookup paths after fixing the benchmark design, but the current example still measures cached block-index lookup rather than full persisted point-read cost. See docs/tasks/000097-runtime-lookup-alignment.md and docs/rfcs/0011-redesign-column-block-index-program.md phase 3 benchmark intent.

## Deferred From (Optional)

docs/tasks/000097-runtime-lookup-alignment.md; docs/rfcs/0011-redesign-column-block-index-program.md

## Deferral Context (Optional)

- Defer Reason: Keep phase-3 resolve scoped to the shipped runtime lookup alignment and document synchronization work.
- Findings: The current example now benchmarks the actual one-descent resolved path and exits cleanly, but it intentionally exercises hot-cache block-index lookup and does not include cold-cache misses or full persisted row decode.
- Direction Hint: Future follow-up should add cold-cache runs and full LWC row fetch/decode coverage before using benchmark data to justify later negative-lookup or delete-hint fields.

## Scope Hint

Benchmark-only follow-up. Extend measurement coverage without changing runtime lookup semantics or persisted formats.

## Acceptance Hint

The runtime lookup benchmark can report hot-cache index lookup, cold-cache lookup, and end-to-end persisted row-read cost including LWC row fetch/decode.

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
