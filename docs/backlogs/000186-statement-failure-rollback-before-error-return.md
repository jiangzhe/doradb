# Backlog: Make statement failures roll back before returning errors

## Summary

Make a public Transaction::exec statement expose at most one DML attempt. That DML consumes the statement's effectful capability, and a failure rolls back every effect accumulated by the statement before returning the error to callback code. Catching the error therefore cannot run another DML or allow partial effects from the failed statement to merge.

## Reference

Transaction::exec currently lends one mutable Statement to an arbitrary callback and decides success from that callback's final Result. The callback can invoke multiple DML methods, and an individual method can return Err after producing partial row, index, or redo effects. Callback code can catch that Err, invoke more DML, and still return Ok. Task 000271 exposed the same lifecycle gap for statement-owned deferred updates, but the consistency-boundary issue applies to effectful Statement APIs generally. The same Statement facade is also used by private catalog staging, where one logical catalog operation deliberately batches multiple row mutations, so a public one-DML restriction cannot be imposed accidentally on private batching.

## Deferred From (Optional)

docs/tasks/000271-index-mutation-unique-driver-key-changes.md

## Deferral Context (Optional)

- Defer Reason: Task 000271 should settle ownership for its deferred updates without redesigning the error contract of every effectful Statement method. Eager statement abort changes public and private lifecycle semantics and requires a repository-wide API audit.
- Findings: Public statement rollback currently starts only when the enclosing Transaction::exec callback returns Err. Transaction::exec lends &mut Statement, so one callback can invoke multiple public DMLs. Private stage_statement merges statement effects even when its callback returns Err so whole-transaction rollback owns them, and catalog staging intentionally performs batches through the shared facade. An inner operation error can therefore be observed and suppressed by callback code, after which Transaction::exec sees Ok and merges partial effects. This is existing behavior; deferred update ownership only made the boundary ambiguity more visible.
- Direction Hint: Prefer a public one-shot effectful capability plus a statement-level failed or aborting state, not operation-local savepoints. Transaction::exec should lend a by-value Statement or separate DmlStatement token whose DML entry points consume the capability at attempt start, making a second public DML unrepresentable where practical. On DML error, use the existing index-before-row rollback order before returning and latch failure in carrier-owned state so callback suppression cannot reclassify the statement as successful. Split or explicitly mode the private catalog facade so intentional internal batches retain their separately defined whole-transaction rollback behavior. Define how Transaction::exec reports a latched failure when the initiating Error was already returned and caught, while preserving fatal rollback precedence and cancellation-safe ownership.

## Scope Hint

Design and implement a one-DML public Statement API and eagerly aborting lifecycle. Audit and migrate public Transaction::exec call sites; make every public DML attempt consume a one-shot effectful capability; roll back index effects before row effects before the first DML failure escapes; preserve the initiating error unless rollback fails fatally; and latch the statement as failed so callback success cannot merge effects. Keep read-only usage and private catalog batching behind explicit, separately documented capabilities, and reconcile cancellation, redo, logical locks, and fatal retention.

## Acceptance Hint

A public Transaction::exec callback cannot invoke two DML attempts through one Statement capability, including after the first attempt returns an error. A partially effectful DML failure returns only after the complete statement has rolled back. Catching that error and returning Ok from the callback cannot make Transaction::exec report or merge a successful DML statement. Successful rollback preserves the initiating error; rollback failure takes fatal precedence and poisons storage. Compile-time or deterministic runtime coverage proves the one-DML boundary, and behavioral tests cover row, index, redo, callback, validation, storage, cancellation, read-only use, and explicitly separate private-transaction batching.

## Notes (Optional)

Consume the public DML capability when an attempt begins, so admission or validation failure cannot be followed by a second DML in the same statement. Planning should decide whether multiple read-only operations before the single DML remain supported. One public DML call may still mutate many rows internally; the restriction is on public DML invocations, not physical row effects.
