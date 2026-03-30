# Backlog: Refine Column Block Index Inline Delete Field and Delete-Surface Cleanup

## Summary

Refine the column block-index leaf delete section around its final inline footprint and delete-surface shape. Revisit the occupied inline budget, codec/domain handling, ordinal interaction, and any stale helper structs, constants, or functions that still reflect earlier compatibility assumptions rather than the current serialized format.

## Reference

- docs/rfcs/0011-redesign-column-block-index-program.md
- docs/tasks/000098-later-domain-evolution-and-auxiliary-payload-expansion.md
- doradb-storage/src/index/column_block_index.rs

## Deferred From (Optional)

docs/tasks/000098-later-domain-evolution-and-auxiliary-payload-expansion.md; docs/rfcs/0011-redesign-column-block-index-program.md phase 4

## Deferral Context (Optional)

- Defer Reason: Task 000098 focused on correctness for ordinal-domain persistence, domain-preserving delete rewrites, and narrow compatibility cleanup. Finalizing the inline delete-field footprint and doing broader delete-surface retirement would broaden the task into a separate format/API cleanup decision that should be handled explicitly.
- Findings:
  - The live inline delete section serializes a 4-byte section header plus little-endian u32 values; leaf validation also expects `4 + del_count * size_of::<u32>()` bytes.
  - `inline_delete_values_fit` still carries a stale u16-capacity branch even though the serialized inline form is u32-only.
  - `ColumnDeleteDomain` still carries real semantics because both `RowIdDelta` and `Ordinal` are legal persisted leaf domains, authoritative rewrites can emit `Ordinal`, and delete-only rewrites preserve the existing domain.
  - Some delete-surface cleanup already landed, but there is still room to remove stale constants/structs/functions once the final inline delete-field design is fixed.
  - Removing delete codec/domain surface entirely would not be a pure cleanup; it would require an explicit decision about the final on-disk contract and likely an RFC/task update.
- Direction Hint:
  Future work should explicitly decide the final inline delete-section design before more cleanup:
  1. If inline delete remains a u32-only section under the current budget, remove stale u16-capacity logic/constants and simplify the delete-section helpers around that single representation.
  2. If the design should collapse toward fewer delete codecs or a single persisted domain, update the RFC/task contract first and then remove now-unnecessary structs/functions in one coordinated pass.
  3. Keep cleanup grouped around real semantic decisions so the code no longer preserves dead compatibility branches or misleading legacy names.

## Scope Hint

Column block-index leaf delete-section semantics only: inline occupied space, codec/domain representation, ordinal-domain interaction, and removal of stale delete-surface structs/functions/constants that are no longer justified by the final design. Keep broader LWC row-id ownership and unrelated runtime work out of scope unless the chosen delete-field design directly requires them.

## Acceptance Hint

A follow-up task can state one final delete-section shape for inline storage, align sizing logic and serialized bytes with that shape, and remove stale delete-surface scaffolding without changing unrelated leaf/runtime behavior. Any retained delete codec/domain surface is explicitly justified by the final design; any removed surface is backed by tests and updated design docs if the on-disk contract changes.

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
