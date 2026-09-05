# Backlog: Generalize Callback Error Boundaries For Public Programmable APIs

## Summary

Promote the managed DDL engine-or-user result boundary into general public CallbackError and CallbackResult types, then use it for managed interpretation and the existing callback-driven mutation and table-scan APIs so application errors remain typed and distinct from DoraDB errors.

## Reference

Identified while implementing docs/tasks/000294-managed-table-bindings-and-versioned-resolution.md. ManagedDdlError and ManagedDdlResult already preserve engine and interpreter failures, while Transaction::table_mutate_mvcc, Transaction::table_index_mutate_mvcc, and Transaction::table_scan_mvcc_stream require callbacks to return DoraDB's public Result and therefore cannot naturally return an application-owned error. LazyRow::val is itself fallible with DoraDB Error, so programmable row callbacks need a carrier that supports both domains inside the callback.

## Deferred From (Optional)

docs/tasks/000294-managed-table-bindings-and-versioned-resolution.md; docs/rfcs/0031-compact-numeric-catalog-table-definitions.md Phase 7

## Deferral Context (Optional)

- Defer Reason: Task 000294 introduces the useful managed DDL boundary but migrating existing programmable DML and stream APIs is a broader public API and transaction-execution change. Deferring the migration keeps managed binding implementation focused while recording that the newly introduced catalog-specific names should not become the long-term abstraction.
- Findings: The affected public callbacks are table_mutate_mvcc, table_index_mutate_mvcc, and table_scan_mvcc_stream. DoraDB's public Error exposes inspection but no public application-error constructor, so the current callback signatures cannot represent a typed caller failure. ManagedTableInterpreter methods can continue returning a plain user Result because interpretation itself does not perform fallible engine row access, while programmable row callbacks should return the dual-domain CallbackResult because LazyRow::val can produce Error. Mutation callbacks can fail after statement effects exist, so generic transaction execution must preserve the user arm after successful rollback and replace it with an engine fatal error if rollback fails. Stream construction and next can independently fail before, during, or after callback execution and must preserve terminal cleanup behavior.
- Direction Hint: Prefer general types in error.rs such as CallbackError<E> with Engine(Error) and User(E), plus CallbackResult<T, E>. Implement From<Error> for the general error so LazyRow engine failures work with question-mark propagation, but do not add a blanket From<E> because it overlaps when E is Error; provide an explicit user wrapper or extension helper instead. Generalize or add a callback-aware Transaction execution path with existing rollback precedence. Carry the type through scan and mutation internals without flattening either domain. Address the ambiguous error type of closures that only return Ok, potentially through an Infallible alias, explicit helper, or callback-output design that permits a bare decision. Update task 000294 and RFC documentation if the managed names are replaced before they are finalized.

## Scope Hint

Move the dual-domain public error carrier to the general error boundary, with engine and user callback variants, borrowed and consuming accessors, Display and standard Error integration, and ergonomic conversion from DoraDB Error. Replace the managed-DDL-specific carrier or retain only intentional compatibility aliases. Migrate sequential mutation, index-range mutation, programmable table-scan stream construction, and stream next results through transaction execution, statement rollback, table access, index mutation, scan cursors, and examples. Design an explicit ergonomic path for infallible callbacks and for wrapping application errors without ambiguous generic inference.

## Acceptance Hint

Managed DDL and all three public programmable row APIs use one general engine-or-user result boundary; application-defined errors are returned intact without conversion into DoraDB Error; LazyRow and all later validation, projection, I/O, and lifecycle failures remain classified as engine errors; a mutation callback error rolls back all statement effects and is preserved when rollback succeeds, while fatal rollback failure takes engine precedence; scan callback errors terminate and release the stream checkout; infallible callback examples remain concise and type-inference behavior is covered; public documentation, examples, benchmarks, and focused plus workspace tests are updated.

## Notes (Optional)

This is intentionally a coordinated breaking public signature change. The crate is version 0.1.0 and task 000294's managed result names are not yet finalized, making this the least costly point to establish the general boundary.

## Close Reason

- Type: implemented
- Detail: Implemented via docs/tasks/000297-generalize-public-callback-error-boundaries.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-09-05
