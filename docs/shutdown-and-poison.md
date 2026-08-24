# Shutdown and Engine Poison

This document is the canonical contract for engine shutdown, fatal runtime
poison, and waits that can observe either condition. Subsystem documents should
describe their local state machines and refer here for lifecycle, error,
cancellation, and wakeup policy.

In this document, *poison* means Doradb's explicit engine-level
`EnginePoisoner`. It is unrelated to standard-library mutex poisoning.

## The Three Independent Questions

Correct behavior depends on three independent facts:

1. **Engine lifecycle**: is the engine `Running`, `ShuttingDown`, or `Shutdown`?
2. **Engine health**: is this engine instance healthy or irreversibly
   `Poisoned`?
3. **Work ownership**: is the work still reversible preparation, already
   accepted execution, or terminal cleanup?

Poison and shutdown are deliberately separate axes. Poison says that normal
storage work must no longer be admitted and that a reversible wait may have
lost its progress producer. Shutdown says that owner teardown has started and
new lifecycle admission is closed. Neither transition implies the other.

| Lifecycle and health | Foreground admission | Already accepted work | Inspection |
| --- | --- | --- | --- |
| `Running` and healthy | admitted | runs normally | available |
| `Running` and poisoned | rejected as Fatal | follows its ownership policy | selected read-only diagnostics remain available |
| `ShuttingDown`, healthy or poisoned | rejected as Lifecycle shutdown | drains or performs terminal cleanup | no new inspection admission |
| `Shutdown` | unavailable | no runnable work may remain; the graph is terminal | unavailable |

Both axes are monotonic for one engine instance. Lifecycle never moves
backward, poison has no reset or epoch, and poison state is not persisted as a
storage-format field. A fresh instance requires the old instance to release
storage ownership, normally through shutdown and otherwise through process
termination, followed by a new bootstrap/recovery attempt.

## Engine Poison

### State and publication

`EnginePoisoner` contains three pieces of state:

- an atomic sticky poisoned flag;
- the first source-bearing `Fatal` report, retained in shared form; and
- a one-shot event used only to wake waits that must reconsider engine health.

Publication follows this order:

1. capture the caller's complete Fatal report, including source frames and
   attachments;
2. store it as the canonical reason only if no earlier reason exists;
3. publish the poisoned flag with release ordering; and
4. only for the first healthy-to-poisoned transition, notify all listeners
   registered at that time.

The reason is stored before the flag becomes observable. A health check that
loads the flag with acquire ordering can therefore reconstruct the canonical
report immediately; `poisoned == true` with no stored reason is an invariant
violation.

Concurrent or repeated poison calls do not replace the canonical reason and do
not emit another poison wake. Each publisher still retains and receives its
own local Fatal report. This distinction is intentional:

- engine admission and unrelated waiters report the first canonical failure;
- a producer that discovers a later fatal failure may return its own local
  failure to the direct caller; and
- later context may be attached while forwarding either report, but its Fatal
  domain and underlying source chain must not be replaced.

The event is not durable state. A listener installed after the one transition
will not be notified by a later poison call. The atomic health flag and stored
reason are the durable truth; every poison-aware wait must use listener
registration plus sticky health rechecks.

At the API level, `poison_error()` and `ensure_healthy()` reconstruct the
canonical first report, `listener()` supplies only the wake hint, `poison()`
captures a local typed report, and `poison_shared()` republishes an already
captured shared Fatal without rebuilding it.

### Fatal reasons

The current Fatal classifications describe the policy boundary that decided
continued normal execution was unsafe:

| Fatal reason | Typical boundary |
| --- | --- |
| `Poisoned` | a catalog, table, or index owner crossed a retry boundary or could no longer compensate or publish one unambiguous state |
| `RedoWrite` | redo data or redo-header write could not preserve ordered durability |
| `RedoSync` | a required redo or rotated-file seal sync failed |
| `StorageIo` | the shared storage backend could no longer make reliable submit/wait progress |
| `CheckpointWrite` | checkpoint work failed after transition or publication became irreversible |
| `CatalogWrite` | catalog-backed checkpoint metadata failed after its irreversible gate |
| `PurgeDeallocate` | purge could not safely destroy or deallocate retired storage |
| `PurgeAccess` | purge could not access state required for safe reclamation |
| `RollbackAccess` | rollback could not access state required to undo or release ownership safely |
| `MandatoryTaskPanic` | accepted mandatory execution panicked under its supervisor |
| `ThreadPoolTaskPanic` | an accepted finite CPU task panicked under its worker supervisor |
| `ThreadPoolUnavailable` | CPU-task workers were unexpectedly unavailable before acceptance |

Fatal is a policy decision, not a synonym for I/O failure. The owning policy
boundary stacks the appropriate `FatalError` over the initiating I/O,
Runtime, DataIntegrity, Resource, panic, or invariant evidence. An already
Fatal report passes through unchanged.

### What poison does

Poison has four effects:

1. normal effectful admission and ordinary healthy-runtime observation reject
   with the canonical Fatal report;
2. mandatory caller-capacity waiters and semantic waits whose progress may be
   stranded are woken;
3. affected reversible attempts unwind through their existing ownership
   guards; and
4. future health checks keep returning the canonical first Fatal report.

At a public boundary, poison remains `ErrorKind::Fatal`. It must not be
reinterpreted as Runtime, Lifecycle, Operation, or a generic catalog-access
error. Pairwise carriers and `QuadError` preserve the native Fatal arm until
the public facade discloses it.

The narrow internal propagation contracts are:

- poison-aware logical-lock acquisition combines only Operation and Fatal;
- prepare-or-poison waiting and hot row-lock acquisition return Fatal at their
  narrow layer, while affected hot delete/update operations combine it with
  their existing Operation result;
- user-table seams where Operation, Runtime, and Fatal meet use the existing
  `QuadError` arms without a public-error round trip;
- catalog mutation and private DDL statement staging preserve Runtime versus
  Fatal until their existing public disclosure boundary; and
- an Operation or Lifecycle arm proven impossible by an internal catalog
  invariant remains an assertion rather than a conversion target for Fatal.

Caller-owned operation, phase, table, row, and lock context may be attached at
each boundary. Those attachments supplement the canonical first report; they
must not erase its original Fatal context or source frames.

### What poison does not do

Poison does **not**:

- move engine lifecycle to `ShuttingDown`;
- call `Engine::shutdown()` or stop component workers;
- revoke accepted logical-lock claims;
- globally drain the lock manager or session registry;
- cancel accepted DDL, maintenance, redo, I/O, or mandatory execution;
- interrupt commit/rollback ownership after its accepted handoff;
- release unsafe rollback or failed-precommit state;
- provide a timeout, deadlock victim, client-cancellation, or thread
  preemption mechanism; or
- guarantee that shutdown can eventually complete.

The last point is essential. A rollback-access failure or panic policy may
retain ownership in `FailedRetained` because dropping it would be unsafe.
That retained session operation remains a shutdown blocker indefinitely.
Poison protects correctness; it is not a promise of recoverable in-process
teardown.

## Admission and Acceptance Boundaries

### Normal foreground admission

Normal engine, session, and non-terminal transaction entry follows this
logical order:

1. acquire lifecycle admission while the engine is still `Running`;
2. upgrade and validate the exact session or transaction state;
3. check engine health where the operation requires a healthy runtime;
4. publish a stable session operation or observer record; and
5. release the short-lived lifecycle admission token.

The lifecycle admission token closes the race with shutdown. Once it is
released, the stable session entry or observer count is the durable proof that
shutdown must drain that work. A poison failure drops the admission token and
does not publish a new operation.

Lifecycle is checked before health on ordinary engine/session admission.
Consequently, a running poisoned engine normally returns Fatal, while an
engine whose shutdown admission is already closed returns Lifecycle shutdown
even if poison was also published. This is admission ordering, not a global
rule that Lifecycle always outranks Fatal.

`Session::list_table_ids()` and the transaction-system, storage-I/O,
buffer-pool, mandatory-runtime, and logical-lock statistics snapshots are
explicit exceptions. They use lifecycle-pinned, read-only inspection that
skips health validation, so operators can diagnose a poisoned engine while it
is still running. They still fail after session close, registry removal, or
engine shutdown and must not create new runtime work.

`Session::close()` is ordinary healthy-runtime admission. It can be used while
healthy; an open session on a running poisoned engine reports Fatal instead of
acting as a poison-bypass teardown API. Engine shutdown and the existing
terminal/abandonment machinery own poisoned-engine cleanup.

### Reversible preparation

Preparation is reversible while an exact guard or owner can synchronously
remove everything the attempt has published. Examples include a queued
logical-lock request, a caller waiting for mandatory capacity, and a row-write
attempt waiting for a foreign preparing transaction.

If poison can strand the normal progress producer, the semantic waiter must
race poison and return Fatal only after its existing guard has made cleanup
inevitable. Poison does not introduce a second rollback state machine.

### Accepted execution

An operation becomes accepted at its documented consuming handoff. Examples
are:

- consuming a mandatory caller permit and transferring the prepared owner;
- enqueueing a prepared transaction into ordered group commit;
- submitting I/O with an owner and completion path;
- successfully enqueueing a finite CPU task with its completion path;
- entering an irreversible checkpoint or DDL publication section; and
- claiming a transaction for terminal rollback.

Poison published after that handoff does not retroactively cancel the work.
The accepted owner must finish, publish its own error, retain unsafe state, or
run terminal cleanup according to its domain contract. Shutdown drains the
same owner.

Mandatory caller admission illustrates the boundary. Before acceptance, its
capacity wait races engine poison and is closed by shutdown. A successfully
acquired permit is the poison-race linearization point; synchronous acceptance
then transfers the complete operation to engine ownership with no intervening
await. Later poison cannot detach the accepted task, and dropping its result
observer cannot cancel it.

### Terminal and cleanup authority

Terminal paths reuse already established authority instead of asking for new
foreground admission. This allows an active transaction to commit or roll
back after clean shutdown has started, and allows abandoned, terminal
rollback, and failed-precommit cleanup to drain a poisoned engine.

When commit observes poison before its ordered handoff, it claims the
transaction, queues mandatory rollback, waits for that cleanup, and then
returns the Fatal report. Explicit rollback does not reject merely because the
engine is poisoned. Once ordered commit owns a precommit transaction, its redo
completion and failed-precommit cleanup own the terminal outcome.

Cleanup may itself publish poison. It must first preserve or retain all unsafe
ownership, then publish poison, then wake dependent waiters or completion
observers. Normal terminal resolution must never be published for retained
failed state.

## The Semantic Wait Protocol

`Event`, `Completion`, latches, and gates are policy-neutral primitives. They
do not know whether engine poison or shutdown should cancel a particular
operation. That decision belongs to the semantic waiter that knows the
predicate, progress producer, accepted boundary, and cleanup owner.

For a reversible wait whose producer may stop making progress after poison,
the required protocol begins only after the operation has established that it
will actually block:

1. install or retain registration for the primary predicate/completion;
2. register an engine-poison listener;
3. recheck sticky engine health and the primary predicate;
4. race primary progress against the poison listener; and
5. recheck sticky engine health before accepting state or retrying normal
   work.

If clean shutdown is also a cancellation source for that semantic family, its
listener is installed before the same predicate recheck and its sticky
lifecycle state is checked alongside health.

This is a predicate protocol, not an event protocol:

- primary progress stored before listener registration is found by the
  predicate/completion recheck;
- poison published before or during listener registration is found by the
  health recheck;
- if primary progress and poison are both ready, the final health check makes
  poison win when it was published before the wait's acceptance check; and
- poison published after the final successful health check does not revoke the
  accepted boundary.

There must be no `.await` between the final health check and consuming the
accepted state or returning permission to retry. Synchronous instructions may
still interleave with a concurrent publisher; the contract linearizes a later
poison after the final successful check and therefore does not roll back the
accepted result.

A poison wake makes the future runnable; it does not poll or drop the future
on the caller's behalf. A caller that retains a woken future without polling
or dropping it continues to retain its guard, queue node, page pin, session
operation, or other shutdown blocker.

## Concrete Foreground Wait Contracts

### Logical-lock acquisition

Logical-lock poison policy is attached to `PendingClaimGuard`, not
`LockManager` and not the success-only `Completion<()>`. The manager remains a
health-agnostic arbitration service.

Existing exact claims, family-covered claims, immediate grants, and immediate
conversions do not register a poison listener or perform an additional health
load. Their existing session/transaction/operation admission is the health
gate. Only a fresh request that actually reaches
`PendingGuardState::Waiting` enters poison-aware logic:

1. the guard stores the exact `WaitNodeID`, `PendingClaimToken`, and completion;
2. it registers the poison listener and rechecks health;
3. it races the success completion against poison;
4. if poison is observed, it returns the canonical first Fatal report and
   guard drop cancels the exact queued or provisional manager state;
5. if completion is observed, it checks health before owner-side transfer;
6. it publishes the family/resource and exact-scope indexes, observes the
   provisional manager grant, and becomes `FreshGranted`;
7. it checks health once more before consuming the pending token; and
8. successful token consumption disarms the guard and commits the accepted
   claim.

The two post-completion checks cover different ownership windows. The first
prevents publication after poison. The second lets guard drop undo both local
indexes and the physical family if poison arrives during synchronous
publication or provisional observation. There is no await between the last
check and token acceptance.

Consequently, poison that races an admitted acquisition which never blocks
does not retroactively cancel its immediate result. This is the deliberate
fast-path acceptance boundary; the exact claim remains owned until its normal
scope cleanup.

Cancellation remains token-exact:

- a queued node is unlinked and the next compatible FIFO prefix is promoted;
- a promoted-but-unobserved provisional family and node are removed;
- partial owner-index publication is rolled back;
- a newly adopted physical family is released; and
- releasing the original blocker later cannot resurrect the cancelled waiter.

For a multi-resource acquisition, ordinary Fatal propagation drops the
existing `FreshClaimsGuard`. It releases only the fresh accepted prefix from
that same unfinished attempt, in reverse order. A newly published exact claim
is fresh even when an existing family holder physically covers it, so that
claim is recorded. Claims that existed before the attempt, acquisitions that
return `LockGrant::Existing`, and successfully disarmed attempts are not
released. Single-claim callers retain their existing enclosing-scope cleanup
policy. There is no poison-specific branch in `FreshClaimsGuard::Drop` and no
global lock-manager drain.

Clean shutdown does not cancel a logical-lock wait. The active session owner
remains a shutdown blocker until its future completes, is polled after poison,
or is dropped by its caller. Lock timeout, lease, deadlock detection, and
victim selection remain separate policy.

### Hot- and cold-row prepare waiting

Row waiting applies only when a foreign row owner is already in ordered
prepare. A foreign ordinary active owner remains an immediate
`WriteConflict`; same-owner reuse and non-conflict results remain immediate.

`SharedTrxStatus::prepare_listener` losslessly distinguishes three states:

- not preparing, which follows ordinary row classification;
- a registered primary listener, wrapped as a poison-aware registered token;
  and
- prepare completion winning registration, represented by a poison-aware
  recheck-only token.

Both preparing outcomes travel through row mutation as an opaque
`PoisonAwareListener`. The name describes the semantic protocol rather than
its internal representation: the value is move-only, its raw listener and
state are private, and it implements neither `Future` nor `Clone`. Production
code therefore cannot directly await prepare completion or reuse one result to
authorize multiple retries. Dropping the token is legal cancellation, but it
does not grant permission to retry the row operation.

The only production consumer is `EnginePoisoner::wait_or_poison`, reached by
the shared `TrxRuntime::wait_prepare_or_poison` helper and invoked only for a
`Preparing` result:

- for a recheck-only token, it performs one sticky health check before
  permitting retry, without registering a listener or selecting futures;
- for a registered token, it first registers the poison listener, rechecks
  sticky health, races prepare completion against poison, and checks sticky
  health again before permitting retry.

Registering the poison listener before the first health recheck closes the
lost-wakeup window. The final recheck makes either selected event only a prompt
to inspect authoritative health; selection itself does not determine success.
Because the token is consumed by this operation, a successful return is the
single-use authority to retry from authoritative row state.

Failed-precommit fatal cleanup publishes poison before it releases prepare
waiters. A registered waiter and a completion-won-registration waiter
therefore return the canonical Fatal report instead of touching retained undo
or masking the failure as `WriteConflict`. Successful commit publishes its
CTS before wake; successful rollback removes row/deletion ownership before
wake. Either successful outcome causes a complete authoritative retry rather
than assuming what the wake meant.

The hot path drops row access but deliberately retains the shared row-page
guard during the expected-short prepare wait; other rows remain accessible and
the page cannot be evicted. Fatal unwind drops that guard normally. Cold point
and scan paths release deletion-buffer entries, index handles, row-location
state, decoded block guards, and other operation-local state before awaiting,
then restart from authoritative row location and marker state. Callback
at-most-once scan ownership remains with the surrounding mutation state.

`LockUndo::Ok`, invalid-index, ordinary conflict, and row-page transition
outcomes never enter the helper. An uncontended hot delete/update therefore
adds no poison load, listener allocation, or second-future selection. Clean
shutdown does not cancel a prepare wait; its active transaction or operation
continues to block graceful shutdown until it finishes or unwinds.

As with immediate logical-lock acquisition, poison racing after healthy
operation admission does not add retroactive cancellation to an uncontended
row mutation. Later durability, terminal, or admission boundaries retain their
own health checks.

### Row-page transition routing

A writer or row-undo rollback that finds its original hot row page in
`TRANSITION` cannot retry until the checkpoint publishes a cold route. An
exact-generation page miss is the same unresolved route for rollback while
the pivot still classifies the row as hot. Checkpoint failure after transition
may prevent publication, so the shared table waiter races route-epoch progress
with poison and checks health before and after the race. The checkpoint's
irreversible guard is responsible for poisoning if it exits without a safe
route publication.

The caller releases page-state and row guards before waiting and retries from
the authoritative pivot after progress; the route epoch is only a wake hint.
Foreground mutation retains its statement owner. Rollback retains the current
boxed undo in `RowUndoLogs`, and the enclosing statement effects, terminal
claim, abandoned cleanup job, or failed-precommit payload owns cancellation or
fatal retention. A final successful health check authorizes the immediate
synchronous retry. Clean shutdown does not cancel either accepted owner;
shutdown drains the session operation or mandatory cleanup task.

### Maintenance progress and checkpoint retry

Caller-side waits for GC-horizon progress, completed purge progress, active
root release, and frozen-page retry are reversible observations. Their normal
producers can stop after a fatal purge, rollback, or checkpoint failure, and
the observation itself should not prevent clean shutdown.

These waits therefore register the applicable progress, transaction-terminal,
and table-lifecycle listeners together with poison and engine-shutdown
listeners, then recheck every sticky predicate. Poison returns Fatal; shutdown
returns the wait family's documented shutdown error; normal progress causes
authoritative reanalysis. The current helpers check health before shutdown, so
an already-observed poison is retained as Fatal when both conditions are
visible.

Checkpoint retry detaches only listener state before sleeping and releases the
strong table runtime. Its session observer remains the shutdown-visible owner
until the wait returns or is cancelled. Maintenance notifications are hints;
completion means only that retry or reanalysis may now be useful.

## Production Wait Classification

The table below classifies the production wait families. A new wait must fit
one row or add a new documented category.

| Wait family | Progress producer and primary wake | Poison behavior | Shutdown behavior | Cancellation or cleanup owner |
| --- | --- | --- | --- | --- |
| Queued logical-lock acquisition | blocker release promotes FIFO prefix and completes success-only waiter | race poison only after entering `Waiting`; return first Fatal and cancel exact pending state | no direct cancellation; graceful session drain waits | `PendingClaimGuard`, then `FreshClaimsGuard` for an acquired prefix |
| Read-snapshot metadata acquisition | blocker release grants metadata-S, or the exact snapshot entry publishes sticky abort and wakes its listener | the underlying logical-lock wait retains its poison-aware Fatal behavior and cancels exact pending state | close, abandonment, or shutdown requests snapshot abort; a retained checked-out build remains a visible blocker until polled or dropped | pending acquisition guard first, then build checkout and snapshot terminal claim close the accepted prefix |
| Hot/cold foreign prepare | owner commit or rollback drops the injected prepare notifier | registered and completion-race paths check poison before retry | no direct cancellation; active owner drains | row access/CDB guards plus statement/transaction owner |
| Row-page transition route | checkpoint publishes a newer route epoch; pivot is authoritative | route-or-poison race; fatal checkpoint guard supplies poison | no direct cancellation; active or mandatory owner drains | foreground row attempt, or vector-owned row undo plus statement/terminal/precommit owner |
| GC/purge progress and checkpoint retry | monotonic progress, transaction terminal state, or table lifecycle change | poison terminates observation as Fatal | shutdown listener terminates observation | detached listeners and `SessionObserverPin` |
| Mandatory caller capacity | permit release or admission close | capacity wait races poison; a won permit is acceptance | admission close wakes with Lifecycle shutdown | prepared caller owner before acceptance; mandatory supervisor after it |
| I/O, page-I/O, redo, group-commit, mandatory-result, and CPU-task completions | owning service publishes success or a typed completion failure; a successful CPU-pool send is acceptance | CPU submission uses the poisoner's atomic fast check and returns cached poison when observed; a racing poison may admit bounded extra work, and accepted ownership is still drained | after outer mandatory drain, private FIFO stop messages follow accepted CPU work and the component owner joins every worker | request owner, completion bridge, and service-specific quarantine/retention; CPU job plus checkpoint queue own encode cleanup |
| DDL/maintenance table/catalog gates and table-drop publish drain | active prepared or accepted scope releases its lease and notifies gate/lifecycle change | do not preempt; failure follows compensation, poison, or retention policy | shutdown waits for the voluntary session operation or accepted mandatory owner | prepared/accepted scope and RAII pending/lease guards |
| Rollback, abandoned, terminal, and failed-precommit cleanup | mandatory internal task completes or retains terminal state | never cancelled by poison; cleanup may publish poison itself | internal admission drains before runner stop | exact cleanup job, terminal claim, and fatal-retention owner |
| Buffer allocation, residency, and eviction progress | deallocation, load completion, or evictor progress; poison alone does not stop these producers | unrelated poison does not replace local progress/completion policy | foreground drain is graceful; pool flags wake any remaining service waiters once component teardown starts | reservation/page guards and pool worker owner |
| Session close and engine/session drain | exact operation transition or observer release | poison is not a drain signal; close requires healthy admission at entry | this is lifecycle coordination itself | session lifecycle entry, exact cleanup hint, and engine coordinator |
| Background worker idle/channel waits | request arrival, channel close, stop marker, or worker wake flag | a worker-specific fatal exit may publish poison; unrelated poison is not a generic stop request | component hook closes ingress/signals stop and joins | registered worker component owner |
| Final quiescent owner release | final `QuiescentGuard` drop decrements the guard count | poison is irrelevant to guard ownership | after shutdown hooks, normal owner drop waits for zero; degraded teardown may leak instead | each guard and the final `QuiescentBox` owner |
| Generic `Event`, `Completion`, latch, mutex, RW lock, notifier, and exclusive gate | primitive-specific state transition | none unless the semantic caller adds it | none unless the semantic caller adds it | primitive guard or higher-level semantic owner |

Service completions are intentionally different from reversible arbitration.
Once I/O or redo has accepted buffers, transaction payloads, or request slots,
returning early on a separate poison event could free or reuse state while the
service still owns it. Backend progress failure instead fails every safely
completable request, quarantines submitted state when safe ownership cannot be
proved, publishes poison, and lets the service's completion/retention contract
settle the owner.

Policy-neutral table/catalog gates and latches also do not observe poison by
default. Their current progress owners are accepted or RAII-protected and must
release the gate even while the engine is poisoned. If a future use can be
stranded because its producer stops on poison, that semantic use must add a
poison race without changing the generic primitive.

## Graceful Shutdown

### Lifecycle states and APIs

Engine lifecycle is monotonic:

```text
Running -> ShuttingDown -> Shutdown
```

`Engine::shutdown()` is synchronous, blocking, and idempotent. It returns
normally only after the foreground/mandatory drain and reverse component
shutdown finish. It has no typed poison result; a poisoned engine follows the
same drain contract.

`Engine::try_shutdown()` initiates the same irreversible transition, waits for
short-lived engine admission tokens to leave, and performs one blocker probe.
It returns `LifecycleError::ShutdownBusy` rather than waiting for session
operations, observers, or mandatory permits. A Busy result does **not** reopen
the engine: lifecycle remains `ShuttingDown`, engine and mandatory caller
admission remain closed, and a later `try_shutdown()` or `shutdown()` continues
the same teardown.

Both APIs are safe to call again after `Shutdown`; repeated calls do no work.
Neither API uses poison to force blockers away.

`Engine::drop` invokes the blocking shutdown path. Dropping the owner at an
uncontrolled point can therefore block indefinitely. Applications should end
foreground work and call explicit shutdown where blocker diagnostics and the
blocking location can be observed.

### Coordinator drain

Blocking shutdown performs these steps:

1. atomically close engine lifecycle admission and publish
   `Running -> ShuttingDown`;
2. close mandatory caller admission;
3. wait for short-lived engine admission tokens to drain;
4. wait for already accepted mandatory caller permits to reach terminal
   handling;
5. scan the session registry for the first current blocker, preferring an
   active operation over standalone observers in the same session;
6. for blocking shutdown, arm that exact session's lifecycle event under its
   mutex, recheck the blocker, request abort/drain for an exact read snapshot,
   release all registry/state guards, perform synchronous checked-in snapshot
   cleanup or queue at most one transaction cleanup hint, wait, and rescan;
7. remove idle registry-owned sessions;
8. shut down registered components in reverse dependency order; and
9. publish `Shutdown`, release the owner shutdown mutex, and apply the
   aggregate panic policy.

`try_shutdown()` uses the same first-blocker classification but installs no
listener. It may synchronously clean one checked-in snapshot or queue one exact
transaction cleanup hint and still reports Busy for the blocker sampled by that
call. A later call observes the resulting terminal edge. Its
diagnostic attachment includes `session_blocker`, `operation_state`,
`observer_count`, `cleanup_queued`, `mandatory_callers`, and
`mandatory_internal`.

The listener-before-recheck protocol prevents a session transition from being
lost between inspection and sleep. Transaction cleanup messages carry the exact
`(SessionOperationKey, TrxID)`, while synchronous snapshot cleanup validates the
exact typed entry and operation key; stale work cannot claim a replacement
operation.

### What blocks shutdown

Shutdown-visible owners include:

- active engine admission tokens;
- session operations in `Voluntary`, `Mandatory`, `CleanupReady`,
  `Completing`, or `FailedRetained` state;
- standalone `SessionObserverPin`s;
- accepted mandatory caller permits;
- mandatory internal cleanup permits; and
- any reversible future whose already-published session operation still owns
  locks, waiters, effects, or prepared resources.

`Terminal` operations, idle session entries, weak public session/transaction
handles, and dropped mandatory result observers do not by themselves block
shutdown. An accepted mandatory task continues to block through its permit
even if its result observer was dropped.

Shutdown has no timeout and does not drop caller futures. A clean logical-lock
or row-prepare wait, a caller-retained future that has not been resumed or
dropped, or an irreversible `FailedRetained` operation can therefore keep
blocking. This is the cost of preserving accepted ownership and avoiding
unsafe forced cleanup.

### Terminal work during shutdown

After foreground admission closes, existing terminal authority remains valid:

- an active transaction may commit or roll back;
- dropping a transaction/session may publish abandonment and queue cleanup;
- terminal rollback and failed-precommit jobs use mandatory internal admission;
- redo drains before internal cleanup admission closes; and
- internal cleanup drains before the mandatory runner stops.

Non-terminal transaction checkout and new session/operation/observer
registration are rejected. Clean shutdown does not synthesize poison or turn
ordinary lifecycle rejection into Fatal.

### Component teardown and panic containment

After foreground owners drain, components shut down in this exact reverse
registration order:

```text
TransactionRedoWorkers
-> MandatoryRuntimeWorkers
-> TransactionPurgeWorkers
-> TransactionSystem
-> Catalog
-> LockManager
-> SharedPoolEvictorWorkers
-> FileSystemWorkers
-> MemPool
-> IndexPool
-> MetaPool
-> DiskPool
-> FileSystem
-> MandatoryRuntime
-> ThreadPoolWorkers
-> ThreadPool
-> EnginePoisoner
-> StorageRootLease
```

The ordering keeps the poisoner available to every earlier component that may
report fatal state and releases the storage-root lease only after all
subordinate activity has stopped. The detailed dependency and per-component
authority audit lives in [Engine Component Lifetime](engine-component-lifetime.md).

Required rotated-file redo seals remain part of live ordered durability and
poison on write or sync failure. Final sealing of the active redo file after a
clean shutdown drain is best effort: failure at that shutdown-only boundary
does not poison storage or invalidate commits that already completed.

Each component shutdown hook is invoked at most once and independently wrapped
in terminal panic containment. A hook panic marks that owner suspect, retains
the first original payload, reports later payloads, and does not prevent later
hooks from running. Lifecycle is published as `Shutdown` before the first
payload is resumed or suppressed. Teardown-hook panic uses this terminal panic
policy; it is not converted into a recoverable poison result.

Containment does not claim that storage mutation implements `UnwindSafe`.
It is valid only because the graph is terminal and never exposed for reuse.
An active hook must close ingress and signal or join all required workers
before exposing a captured payload. Later payloads are reported and forgotten
without running arbitrary payload destructors.

An explicit shutdown caller on a non-unwinding thread receives the first
payload through resumed unwind. Owner drop during an existing unwind reports
and suppresses it to avoid a double panic. The graph is terminal in either
case and must not be reused. Final registry drop may deliberately leak a
suspect owner and its quiescent dependency closure instead of hanging or
dropping through unsafe outstanding guards.

Completion of the `StorageRootLease` shutdown hook releases root ownership, so
another engine may acquire the root even while the shut-down `Engine` value
itself remains allocated.
A failed bootstrap has no foreground drain; it shuts down only the components
that were successfully registered, in the same reverse dependency order, and
releases the root lease last.

### Final component-owner release

Explicit shutdown runs every component hook but does not immediately destroy
the `Engine` value or every registry allocation. On final owner drop, field
order first drops `EngineInner`, releasing the registry-owned session states,
`EngineCore`, and their runtime `QuiescentGuard`s. It then drops
`ComponentRegistry` and its component owners in reverse order.

In a panic-free graph, each `QuiescentBox` waits until its outstanding guard
count reaches zero. Access handles and build-shelf provisions are already gone,
so a sampled zero cannot increase. This strict final wait exposes hidden guard
lifetime defects instead of racing owner destruction.

After a contained shutdown-hook panic, final drop uses the degraded policy:
the suspect owner is leaked, and any otherwise non-suspect owner with a
nonzero acquire-ordered guard-count sample is also leaked as part of the
bounded dependency closure. Independent zero-guard owners are still dropped.
Surviving public session and transaction handles are weak and cannot retain or
recover component capabilities after registry ownership is removed.

## Poison and Shutdown Races

There is no universal "poison always wins" or "shutdown always wins" rule.
Each boundary has a documented linearization point:

- ordinary admission checks lifecycle before health, so already-closed
  lifecycle admission returns Shutdown;
- poison-aware foreground waits perform a final health check, so poison
  published before that check wins over a simultaneously ready normal wake;
- a mandatory permit won before poison is accepted and is not revoked;
- ordered commit, submitted I/O, and accepted mandatory execution use their
  own completion/retention outcome after handoff;
- maintenance observation checks poison and shutdown together and gives an
  already-visible Fatal report its documented precedence; and
- shutdown itself never returns the stored poison report and never clears it.

Poison during `ShuttingDown` still stores the first reason and wakes registered
poison-aware waits. It can help a reversible foreground owner unwind, but the
shutdown coordinator observes only the resulting session/permit transitions.
Poison does not wake the shutdown listener, and shutdown does not wake a
poison-only waiter unless that wait family separately observes shutdown.

## Review Contract for New Waits

Every new potentially unbounded engine wait must document these five
properties at its semantic owner:

1. **Progress producer**: which owner can make the predicate true, and can it
   stop after poison or shutdown?
2. **Primary wake or result**: which predicate, epoch, completion, channel, or
   lifecycle transition is authoritative, and does it transport failure?
3. **Poison behavior**: ignore, observe before entry, race while waiting, or
   continue as accepted/terminal work; justify any non-observation.
4. **Shutdown behavior**: reject, wake and unwind, drain without cancellation,
   or run as part of component teardown.
5. **Cancellation and cleanup owner**: which exact guard, token, scope, job, or
   retention object removes every partial state exactly once?

The review must also identify the acceptance linearization point. If poison is
added to a slow wait, preserve the uncontended path unless a separate safety
argument requires an unconditional health read or listener.

Use this implementation checklist:

- register listeners before the predicate recheck;
- treat notifications as hints and sticky state as truth;
- recheck health after either branch of a primary-versus-poison race;
- place no await between final health validation and acceptance;
- never return early from submitted service work while its producer still owns
  buffers, payloads, or raw-state obligations;
- preserve Fatal as Fatal through every internal carrier;
- use existing RAII/token cleanup instead of parallel poison-only rollback;
- state explicitly whether clean shutdown is allowed to interrupt the wait;
  and
- update the production classification table when introducing a genuinely new
  wait family.

Concurrency tests must control semantic race points with hooks, channels,
barriers, or production predicates rather than sleeps. Cover, as applicable:

- poison before listener registration;
- poison between registration and recheck;
- primary completion and poison becoming ready together;
- promotion/publication/observation before final acceptance;
- poison immediately before and immediately after the final healthy check;
- cancellation of queued, provisional, and partially published state;
- preservation of pre-existing ownership and release of only a fresh prefix;
- clean shutdown's documented cancel-or-drain behavior;
- first-Fatal source frames, attachments, and public `ErrorKind::Fatal`; and
- absence of listener allocation or extra health loads on protected fast
  paths.

## Explicit Non-Guarantees

This contract does not provide:

- a unified cancellation context spanning poison, shutdown, deadlines, client
  cancellation, or deadlock victims;
- forced cancellation or preemption of caller futures;
- bounded shutdown latency;
- automatic recovery or unpoisoning of an engine instance;
- lock-wait timeouts, leases, escalation, or deadlock detection;
- a global fairness guarantee beyond each subsystem's documented policy; or
- permission to release accepted or retained ownership merely to make
  shutdown finish.

Those changes affect ownership across multiple subsystems and require a
separate design boundary.

## References

- [Storage Architecture](architecture.md)
- [Engine Component Lifetime](engine-component-lifetime.md)
- [Storage Error Model](error-spec.md)
- [Lock System](lock-system.md)
- [Transaction System](transaction-system.md)
- [Checkpoint](checkpoint.md)
- [Data Checkpoint](data-checkpoint.md)
- [Buffer Pool](buffer-pool.md)
- [Observability Logging](observability-logging.md)
- [Task 000264: Integrate Engine Poison with Foreground Waiters](tasks/000264-engine-poison-foreground-waiters.md)
