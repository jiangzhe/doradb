# Backlog: Correct readonly-buffer-pool warm benchmark and investigate io_uring cold-read latency

## Summary

Record the deferred follow-up from task 000091: first correct the readonly-buffer-pool benchmark so the warm phase reflects true cache-hit behavior, then re-measure and investigate the remaining io_uring cold-read latency gap against libaio.

## Reference

1. Source task document: docs/tasks/000091-make-io-uring-default-and-finish-async-io-cleanup.md. 2. Manual benchmark follow-up on 2026-03-26 showed the current warm phase still misses because cache_bytes is smaller than the dataset, and the remaining backend gap is concentrated in serialized cold-read waits.

## Scope Hint

Update the readonly benchmark methodology or defaults so the warm phase actually fits in cache, rerun io_uring vs libaio under the corrected setup, and investigate only the remaining cold-path latency in the readonly miss-load path without broad async-IO redesign.

## Acceptance Hint

The benchmark clearly distinguishes true cache-hit warm behavior from cold/miss behavior, follow-up measurements are reproducible on one machine, and any remaining io_uring regression is isolated to a documented root cause or fixed with validated parity against libaio.

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

## Close Reason

- Type: implemented
- Detail: Implemented via docs/tasks/000092-benchmark-and-improve-iouring-single-miss-latency.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-03-26
