# Backlog: Admit expected managed definition versions through execution

## Summary

Design an execution admission boundary that accepts opaque expected managed table definition versions, validates them while acquiring engine-owned target metadata authority, and retains that authority through query or DML execution.

## Reference

docs/rfcs/0031-compact-numeric-catalog-table-definitions.md, Phase 7, conversation decisions [U7] and [U25]; docs/tasks/000294-managed-table-bindings-and-versioned-resolution.md. The implemented resolver returns an optimistic point-in-time version and releases every lock before returning.

## Deferred From (Optional)

docs/rfcs/0031-compact-numeric-catalog-table-definitions.md Phase 7; docs/tasks/000294-managed-table-bindings-and-versioned-resolution.md

## Deferral Context (Optional)

- Defer Reason: Phase 7 deliberately delivers binding lookup, cache invalidation, and an optional coherent read snapshot. Protecting subsequent planning and execution requires a broader transaction/admission contract, so it was explicitly excluded from that phase and retained as RFC future work.
- Findings: TableDefinitionVersion is opaque and currently contains (TableID, storage_epoch). A narrow binding resolution reads the admitted managed runtime without projecting numeric schema or accessing descriptor rows. All supported managed descriptor replacements advance storage_epoch, but comparison at a separate resolution call leaves a race before execution. ResolvedTableIndex is also non-pinning and revalidates exact index identity at admission; a new definition boundary must compose with index runtime retirement instead of treating an optimistic token as a retained snapshot. Future descriptor-only changes must extend the private version to include descriptor revision, preferably cached as a fixed-size current-state stamp.
- Direction Hint: Prefer accepting expected versions at the engine execution/admission boundary, acquiring existing target metadata-S claims in canonical TableID order, validating every expected version before effects, and retaining engine-owned admission until terminal execution cleanup. Specify how multi-table plans and existing transaction admission compose, return a typed stale-definition outcome, and keep lock/gate ownership out of caller-owned tokens. An immutable layout pin is an alternative only with an explicit index/runtime reclamation design. Coordinate future descriptor-revision stamps and current-state caching with backlog 000192.

## Scope Hint

Design and implement expected-definition-version admission for query/DML execution, including canonical multi-table lock acquisition, typed stale rejection, and retained engine authority through cancellation and terminal cleanup. Preserve the cheap optimistic binding resolver as a separate existing contract.

## Acceptance Hint

Deterministic tests race managed index DDL and DROP/recreate against version admission; stale expected versions fail before effects, accepted execution keeps its definition valid through completion, multi-table claims follow canonical order, and cancellation/failure releases all authority. Verify compatibility with exact index tokens, runtime reclamation, and a projection-free version check. Document descriptor-only version extension requirements without claiming that DDL already exists.

## Notes (Optional)


