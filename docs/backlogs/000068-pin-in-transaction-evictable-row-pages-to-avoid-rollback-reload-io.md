# Backlog: Pin in-transaction evictable row pages to avoid rollback reload IO

## Summary

Active transactions should pin touched evictable row pages until commit or rollback so ordinary rollback does not depend on reload I/O for in-flight row pages.

## Reference

Follow-up to task 000088 rollback-access poisoning: rollback row-page access failures now poison the storage engine instead of panicking.

## Scope Hint

Add pin and unpin lifecycle management for user-table row pages touched by active transactions; keep the change at the application/runtime policy layer without redesigning the buffer-pool API.

## Acceptance Hint

Rollback of a normal active transaction on user-table pages does not require evictable-page reload I/O in steady-state application behavior.

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
