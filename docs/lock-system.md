# Lock System

Doradb uses logical locks to protect table definitions and coordinate access
to table data. MVCC determines which row version a reader sees, while row
write ownership detects conflicting changes. Logical locks complement both:
a transaction can hold a table lock and still encounter a row write conflict.

Internal latches protect brief access to shared memory. Logical locks instead
last for an operation, transaction, or session.

## What is protected

Each table has two independent resources:

- **Table metadata**: table existence, schema, and the runtime layout used by
  an operation.
- **Table data**: coordination between row writers, whole-table operations,
  and maintenance.

Ordinary reads protect metadata but rely on MVCC for data visibility. Schema
changes (DDL) take exclusive metadata protection before changing a table's
definition.

## Modes and compatibility

Metadata supports shared (`S`) and exclusive (`X`) locks. Data also supports
intention shared (`IS`), used by maintenance and selected catalog reads, and
intention exclusive (`IX`), used by row writers.

Compatibility describes which modes different sessions can hold together.

Metadata:

| Held / Requested | `S` | `X` |
| --- | --- | --- |
| `S` | yes | no |
| `X` | no | no |

Data:

| Held / Requested | `IS` | `IX` | `S` | `X` |
| --- | --- | --- | --- | --- |
| `IS` | yes | yes | yes | no |
| `IX` | yes | yes | no | no |
| `S` | yes | no | yes | no |
| `X` | no | no | no | no |

Multiple row writers can hold `IX` and resolve conflicts at the row level.
A data `S` lock excludes those writers. A data `X` lock excludes all other
data-lock holders, but does not by itself block ordinary MVCC reads, which
acquire no data lock.

**Coverage** answers a different question: does a held mode already provide
the requested protection? Every mode covers itself; metadata `X` covers
`S`; data `X` covers all data modes; and data `S` and `IX` each cover
`IS`. Neither `S` nor `IX` covers the other, and they cannot be combined
into another mode.

## Ownership and lifetimes

A session and the work it starts form a **lock family**. Within that family,
each transaction or operation retains its own claim to the protection it
needs. Completing one scope must not release another scope's locks.

| Owner | When protection ends |
| --- | --- |
| Explicit session lock | Explicit unlock or session cleanup; it can span transactions. |
| Transaction | Commit or rollback cleanup completes. |
| DDL or maintenance operation | The operation and its required cleanup complete. |
| Shared read snapshot | The snapshot closes and its active readers drain. |
| Short catalog read | The lookup or definition read ends. |

A new claim within a family is allowed only when every other claim on that
resource covers the request. For example:

- A session holding data `X` can start a transaction that requests `IX`.
  Committing the transaction leaves the session's `X` held.
- A session holding data `S` cannot start a row write requiring `IX`.
  The request fails with a family conflict.

Lock changes within one family are serialized. While a transaction or another
operation is active, the session cannot start new work that changes state or
acquires locks.
Read-only diagnostics remain available, and parallel snapshot readers can
share protection established for their snapshot.

Explicit session unlock requires an idle session. Transaction locks have no
early unlock: they remain until transaction cleanup, before the session
becomes available again.

## Locks used by operations

These are the modes requested on the target user table. An existing covering
claim can satisfy the request.

| Operation | Metadata | Data | Lifetime |
| --- | --- | --- | --- |
| MVCC lookup or scan | `S` | none | Transaction |
| Insert, point update/delete, index-driven mutation | `S` | `IX` | Transaction |
| Full-table mutation | `S` | `X` | Transaction |
| Explicit table lock | `S` | Requested `S` or `X` | Session or transaction |
| Table freeze/checkpoint | `S` | `IS` | Maintenance operation |
| CREATE TABLE with a new identity | `X` | none | DDL operation |
| DROP TABLE, CREATE INDEX, DROP INDEX | `X` | `X` | DDL operation |
| Shared read snapshot | `S` on each selected table | none | Snapshot |

A transaction acquires metadata protection when it first accesses a table and
keeps it for later accesses. Once accepted, that claim remains held even if
subsequent table resolution or validation fails; ending the individual
statement does not release it.

DDL also protects the catalog tables it changes. Binding lookups and definition
reads use short operation scopes to protect the catalog and target metadata
they inspect.

DDL rejects a target table explicitly locked by the same session. Maintenance
uses ordinary family coverage and holds a separate claim for its own lifetime.

## Acquisition and waiting

A repeated request by the same owner for a covered mode reuses its claim;
repeated calls do not require additional unlocks. A covered request by another
scope in the family establishes that scope's own claim.

A new request blocked by other sessions waits in arrival (FIFO) order. When
locks are released, compatible requests at the front can proceed together. An
exclusive request behind shared holders therefore prevents new sessions'
shared requests from continually overtaking it. Covered requests within an
already-protected family can continue because they add no stronger
protection.

Upgrades are immediate-only. A comparable stronger request, such as `IX` to
`X`, must satisfy family coverage and be immediately grantable. If stronger
protection against other sessions is needed, the queue must also be empty.
A blocked upgrade fails while retaining the old claim. Incomparable requests,
such as `S` to `IX`, are unsupported.

Operations acquire metadata protection before data protection and row write
ownership. For multiple tables, callers should use a consistent acquisition
order and request the strongest known mode first. The lock system does not
reorder separate calls or resolve deadlocks; fresh requests can wait
indefinitely.

## Cancellation and completion

Cancelling a pending acquisition removes that request, including when a grant
races with cancellation. An unfinished multi-lock acquisition releases only
the new claims it acquired; older claims remain held. Merely leaving a wait
pending does not cancel it.

Accepted transaction locks remain through commit or rollback cleanup.
Cancellation that requires rollback keeps the session unavailable until that
cleanup finishes.

DDL and effectful maintenance can be cancelled during preparation. After the
engine accepts the operation, it owns completion and cleanup: the caller
stopping its wait does not release the operation's locks.

A fatal engine failure (poison) aborts blocked acquisitions, but does not
globally revoke accepted claims. Graceful shutdown coordinates the completion
of lock owners and does not automatically cancel every lock wait. Some owners,
such as shared snapshots, have their own shutdown cancellation policy.

## Boundaries and further reading

The logical lock system provides table-level coordination within one engine.
It has no row, gap, or next-key locks, no automatic lock escalation, and no
distributed ownership. Lock state is volatile and is not restored during
recovery. Waiting upgrades, lock timeouts, and deadlock detection are not
supported.

- [Public API: Explicit table locks](./public-api.md#explicit-table-locks)
- [Transaction System](./transaction-system.md)
- [Checkpoint](./checkpoint.md)
- [Shutdown and Engine Poison](./shutdown-and-poison.md)
