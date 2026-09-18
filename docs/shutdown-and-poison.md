# Shutdown and Engine Poison

This document describes the engine-wide model and guarantees for shutdown,
fatal runtime poison, and the work affected by either condition. Subsystem
documents and code own their local algorithms and state machines.

Here, *poison* means Doradb's explicit engine-level `EnginePoisoner`, unrelated
to standard-library mutex poisoning.

## Lifecycle, Health, and Ownership

Three independent questions determine what an operation may do:

1. **Lifecycle:** is the engine `Running`, `ShuttingDown`, or `Shutdown`?
2. **Health:** is this engine instance healthy or irreversibly poisoned?
3. **Ownership:** is the work reversible preparation, accepted execution, or
   terminal cleanup?

Shutdown closes admission and coordinates teardown. Poison records that normal
storage work is no longer safe and that some waits may have lost their source
of progress. Neither transition implies the other.

| Lifecycle and health | New foreground work | Existing work and inspection |
| --- | --- | --- |
| Running, healthy | Admitted | Normal execution and inspection |
| Running, poisoned | Rejected as Fatal | Existing ownership still applies; selected read-only diagnostics remain available |
| ShuttingDown, healthy or poisoned | Rejected as Lifecycle shutdown | Existing owners drain; no new inspection admission |
| Shutdown | Unavailable | Terminal engine; no runnable work remains |

Lifecycle moves only from `Running` to `ShuttingDown` to `Shutdown`. Poison is
sticky for one engine instance, has no reset, and is not persisted as storage
metadata. A fresh instance requires release of the old instance's storage
ownership, normally through shutdown or otherwise through process termination,
followed by a new bootstrap and recovery attempt.

## Engine Poison

### Failure and notification

A subsystem poisons the engine when its owning policy determines that normal
execution cannot safely continue, for example after an irreversible durability
failure or an inability to undo partially completed work. An ordinary I/O or
operation error is not automatically fatal.

The engine retains the first fatal report, including its source chain and
diagnostic context, before making poison visible. Repeated failures do not
replace that reason. Admission checks and poison-aware waiters report this
canonical failure; an operation interrupted by a later failure may report its
own failure to its caller. Fatal reports remain Fatal throughout propagation
and appear publicly as `ErrorKind::Fatal`.

The first transition also wakes registered poison-aware waiters. This is a
one-shot notification, not the source of truth: a waiter must check persistent
health state even if it registers after the notification. Later poison calls
do not provide another wake.

### Effect on the engine

Poison rejects normal admission, wakes affected reversible waits, and lets
those attempts unwind through their existing cleanup owners. Selected
read-only diagnostics remain available while lifecycle admission is open;
they cannot create new runtime work.

Poison does not start shutdown, stop all workers, revoke accepted locks, or
cancel work already owned by a service. It also does not release state whose
cleanup cannot be proved safe. Shutdown remains the engine owner's
responsibility, and poisoning does not guarantee that teardown can finish.

`Session::close()` requires a healthy runtime; it is not a poison-bypass
cleanup API. Engine shutdown and existing transaction terminal or abandonment
paths retain responsibility for cleanup after poison.

## Admission and Work Ownership

### Foreground admission

Ordinary engine and session entry first acquires lifecycle admission, validates
the session or transaction, and checks health where required. Before releasing
that short-lived admission, it records the operation or observer that shutdown
must drain. This prevents work from disappearing between admission and shutdown
coordination.

Lifecycle is checked before health at this boundary. A running poisoned engine
therefore rejects normal work as Fatal, while already-closed lifecycle
admission returns Lifecycle shutdown. This ordering is local to admission;
there is no universal precedence between poison and shutdown.

### Preparation and acceptance

Preparation is reversible while its owner can remove the attempt's partial
state safely. Queued lock requests and waits for execution capacity are
examples. Poison-aware cancellation uses that existing ownership to unwind.

Acceptance transfers responsibility to an execution owner, such as ordered
commit, submitted I/O, a finite pool job, or mandatory execution. The handoff
must be explicit. For mandatory caller admission, acquiring the permit settles
the poison race and acceptance transfers the prepared operation without an
intervening await.

After acceptance, poison cannot retroactively cancel the work. Its owner must
complete it, report failure, perform terminal cleanup, or retain unsafe state.
Dropping a result observer does not cancel accepted execution. Returning early
while a service still owns buffers or transaction state would violate this
ownership contract.

### Terminal cleanup

Commit, rollback, and abandonment reuse authority established by the active
transaction rather than requesting new foreground admission. Existing
transactions can therefore settle after clean shutdown starts, and rollback
can proceed despite poison.

A commit that observes poison before its ordered handoff runs mandatory
rollback before returning Fatal. After the handoff, ordered commit and its
cleanup path own the outcome. Cleanup failures preserve unsafe ownership
before publishing poison or waking dependent waiters; retained failed state
must never be reported as normal completion.

## Wait Policy

A wait's semantic owner knows what can make progress and who owns cancellation
cleanup. Events, completions, latches, and gates do not choose shutdown or
poison policy themselves.

For a reversible wait whose progress can be stranded by poison, register the
applicable listeners before rechecking the authoritative predicate and engine
health. After either normal progress or a poison wake, recheck health before
accepting work or retrying. Notifications request reassessment; they do not
prove success. Do not await between the final successful health check and
acceptance.

This closes missed-wakeup races and makes poison visible before that final
check win over a simultaneous normal wake. Poison arriving after the accepted
boundary does not revoke the result. Healthy immediate operations retain their
own admission and acceptance boundary rather than inheriting every slow-wait
check.

A poison wake only makes a future runnable. The caller must still poll or drop
it to release its guards and other ownership. Retaining an unpolled future can
therefore continue to block shutdown.

### Production Wait Classification

These are behavioral categories, not an inventory of individual wait sites.
Each subsystem documents its exact progress source and cleanup owner.

| Category | Poison behavior | Shutdown behavior |
| --- | --- | --- |
| Reversible foreground acquisition or retry | Affected waits unwind on poison through existing guards | Family-specific: ordinary lock and row-prepare waits drain; snapshot acquisition has an abort policy |
| Maintenance progress observation | Ends with Fatal when poison is observed | Observes shutdown and ends the wait |
| Accepted service requests and jobs | Follow the service's completion or safe-retention outcome | Drain before workers and dependencies stop |
| Terminal cleanup | Continues; may itself poison or retain unsafe state | Internal cleanup authority remains available until drained |
| Service progress and lifecycle coordination | Follow their own progress policy; unrelated poison is not a stop signal | Their owner coordinates admission closure, drain, and worker stop |
| Generic events, completions, latches, and gates | No built-in poison policy | No built-in shutdown policy |

Clean shutdown interrupts only wait families that explicitly observe it.
Maintenance observations check health before shutdown and retain an already
visible Fatal report. Ordinary lock and row-prepare waits remain owned by their
active operation until completion or cancellation. These differences reflect
ownership, not a universal cancellation rule.

Poison can still be published during `ShuttingDown`. It may help reversible
work unwind, but shutdown waits for the resulting ownership transitions;
poison is not itself a drain signal and does not clear blockers.

## Graceful Shutdown

### APIs and sequence

`Engine::shutdown()` is synchronous, blocking, and idempotent. It returns
normally after foreground and mandatory work have drained and component
shutdown has finished. It has no timeout and no typed poison result: a poisoned
engine follows the same ownership and drain contract.

`Engine::try_shutdown()` starts the same irreversible transition. It waits for
short-lived admission tokens, then probes current blockers instead of waiting
for active session work, observers, or mandatory permits. A
`LifecycleError::ShutdownBusy` result leaves the engine in `ShuttingDown` with
admission closed. Later calls continue teardown; they do not reopen the engine.
If no blockers remain, the call completes component shutdown. Both APIs do no
work after `Shutdown`.

Shutdown proceeds conceptually as follows:

1. Close engine and mandatory caller admission and enter `ShuttingDown`.
2. Drain in-flight admission and existing foreground owners, observers, and
   accepted mandatory work, requesting owned cleanup where appropriate.
3. Stop components in reverse dependency order, preserving the services needed
   by accepted work and terminal cleanup until they finish.
4. Release storage-root ownership and publish terminal `Shutdown` state.

Redo completion can still require internal cleanup. That cleanup must drain
before its runner stops; finite jobs must drain while their storage and buffer
services remain available. The poisoner outlives components that can report
fatal state, and the storage-root lease is released last. A later engine can
then acquire the root even while the old, shut-down `Engine` value exists.

### Blockers and caller responsibility

Active operations and transactions, inspection observers, and accepted
mandatory or cleanup work hold shutdown-visible ownership. Caller-retained
futures and unsafe failed operations can retain these blockers indefinitely.
Idle sessions, weak public handles, and dropped result observers do not by
themselves block shutdown.

Shutdown neither drops caller futures nor forces accepted resources free.
Applications should stop submitting work, finish or drop operation futures,
settle transactions, and explicitly shut down at a controlled blocking point.
Dropping `Engine` invokes blocking shutdown and can also wait indefinitely.

### Teardown failures

Component hooks run under terminal panic containment so a hook failure does
not prevent remaining hooks from running. The engine becomes terminal before
the first panic is propagated; owner drop during an existing unwind suppresses
that additional panic. This does not make the engine reusable or turn a
teardown panic into a recoverable poison result.

Final owner destruction normally waits for outstanding component guards.
After a contained hook panic, unsafe owners and their retained dependencies
may be leaked rather than destroyed unsafely. Failed bootstrap uses the same
reverse teardown for components that were successfully registered. Detailed
component ordering and ownership rules belong to
[Engine Component Lifetime](engine-component-lifetime.md).

## Review Contract for New Waits

Every new potentially unbounded engine wait must document these properties at
its semantic owner, in the owning subsystem document or code:

1. **Progress producer:** who can make progress, including after failure?
2. **Authoritative result:** what predicate or completion establishes success
   or failure, and what notification requests a recheck?
3. **Poison behavior:** does the wait reject, observe, unwind, or continue?
4. **Shutdown behavior:** does it cancel, drain, or participate in teardown?
5. **Cleanup owner:** who removes partial state or retains unsafe ownership?

Also identify the acceptance boundary and justify any non-observation of
poison or shutdown. Use the categories above; keep subsystem algorithms and
new wait-family details with their owner rather than expanding this overview.

## Limits and Further Reading

Neither mechanism provides forced cancellation, bounded shutdown latency,
automatic recovery, lock timeouts, deadlock detection, or a unified cancellation
policy. Poison cannot be reset, and accepted or retained ownership cannot be
released merely to make shutdown finish.

- [Engine Component Lifetime](engine-component-lifetime.md): component ownership
  and teardown details.
- [Storage Error Model](error-spec.md): error domains and fatal propagation.
- [Public API](public-api.md#shutdown): application shutdown usage.
- [Lock System](lock-system.md) and [Transaction System](transaction-system.md):
  foreground ownership, completion, and cancellation.
- [Checkpoint](checkpoint.md), [Data Checkpoint](data-checkpoint.md), and
  [Buffer Pool](buffer-pool.md): subsystem progress and failure handling.
- Implementation entry points: [engine lifecycle](../doradb-storage/src/engine.rs),
  [poison publication and waiting](../doradb-storage/src/poison.rs), and
  [component teardown](../doradb-storage/src/component.rs).
