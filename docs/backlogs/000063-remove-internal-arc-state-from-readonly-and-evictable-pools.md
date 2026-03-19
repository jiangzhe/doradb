# Backlog: Remove Internal Arc State From Readonly And Evictable Pools

## Summary

Track the post-IO-generalization cleanup to remove redundant pool-internal Arc-owned runtime state from the readonly and evictable pools once detached miss/load tasks are replaced by the generalized IO-owned completion model.

## Reference

1. Related backlog item: docs/backlogs/000053-io-thread-owned-completion-callback-framework-for-file-io.md. 2. User discussion during catalog separation and worker extraction follow-up: detached readonly task state uses Arc::clone today as an IO-failure workaround and should be generalized away before pool-state cleanup. 3. Current implementation sites: doradb-storage/src/buffer/readonly.rs and doradb-storage/src/buffer/evict.rs.

## Scope Hint

After the generic IO-owned completion design lands, refactor readonly and evictable worker/runtime state so quiescent guards own lifetime directly instead of mixing keepalive guards with Arc-cloned sub-state. Revisit detached-task state, worker runtime structs, and false-sharing-sensitive counters; evaluate CachePadded where layout or hot counters justify it.

## Acceptance Hint

A follow-up task or RFC removes redundant internal Arc state from readonly and evictable pool runtimes where quiescent ownership already provides lifetime safety, documents the remaining shared-state exceptions, and explicitly evaluates CachePadded placement for hot concurrent fields.

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
