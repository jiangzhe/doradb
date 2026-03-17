# Backlog: Add pool-brand identity to retained page guards and arena guards

## Summary

Current fixed, evictable, and readonly pool paths retain caller-supplied PoolGuard values in returned page guards and ArenaGuard handles, so a foreign guard can keep the wrong pool alive while the target arena tears down. Add a shared provenance or branding model so retained guards always prove the target pool identity and wrong-pool misuse is rejected or repaired consistently across buffer pools.

## Reference

Current gap is documented in docs/tasks/000070-replace-arena-lease-with-quiescent-guard-and-simplify-quiescent-drain-storage.md and visible in doradb-storage/src/buffer/arena.rs, doradb-storage/src/buffer/fixed.rs, doradb-storage/src/buffer/evict.rs, doradb-storage/src/buffer/readonly.rs, and doradb-storage/src/buffer/mod.rs.

## Scope Hint

Define one shared pool-provenance contract for retained buffer-pool guards. Decide mismatch policy once, update page-guard and arena-handle construction to retain a validated or pool-minted branded guard, and apply it consistently across fixed, evictable, and readonly pools instead of patching only one implementation.

## Acceptance Hint

Wrong-pool guards can no longer keep the wrong arena alive. Returned page guards and arena handles always retain keepalive for the target pool, and all buffer-pool implementations use the same provenance contract.

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
- Detail: Implemented via docs/tasks/000072-add-buffer-pool-identity-validation-for-guard-provenance.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-03-17
