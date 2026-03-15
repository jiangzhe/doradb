# Backlog: Investigate bench_btree example stack overflow on smoke run

## Summary

`cargo run -p doradb-storage --example bench_btree` overflows the process
stack during the smoke run used by task 000069 resolve. The example prints
BTree and `std::map` results, then aborts with a stack overflow before
completing the remaining bench phase.

## Reference

docs/tasks/000069-buffer-pool-arena-ownership-and-lifetime-free-page-guards.md

## Scope Hint

Identify which bench phase overflows, confirm whether it is specific to the example defaults or current platform, and fix or narrow the default smoke invocation so the example can be used as a stable regression check.

## Acceptance Hint

Running `cargo run -p doradb-storage --example bench_btree` completes without
stack overflow on the default example path, or the task documents and validates
a smaller stable smoke command if the full default example is intentionally too
large.

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
