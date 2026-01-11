# Implement Three States of Row Page

## Background

As described in `docs/data-checkpoint.md`, the `RowPage` needs to support three states to enable non-blocking persistence and data checkpointing:

1.  **Active**: Normal operational state. Insert, Update, Delete allowed.
2.  **Frozen**: Pending stabilization. Inserts/Updates are rejected (redirected). Deletes/Locks are allowed.
3.  **Transition**: Being converted to LWC. All writes (Insert, Update, Delete) must backoff and retry.

Currently, `RowVersionMap` has a `frozen: AtomicBool` which only supports `Active` and `Frozen` (conceptually). We need to extend this to support the `Transition` state and enforce the corresponding blocking behavior.

## Goals

1.  Modify `RowVersionMap` to support 3 states: `Active`, `Frozen`, `Transition`.
2.  Update `Table` operations (`insert`, `update`, `delete`) to respect these states.
    -   `Active`: No change.
    -   `Frozen`: Existing behavior (reject insert/update-inplace).
    -   `Transition`: Reject/Block *all* write operations (including Delete and Lock), prompting a retry.
3.  Add tests to verify the state transitions and blocking behavior.

## Implementation Steps

### 1. Update `RowVersionMap`

File: `doradb-storage/src/trx/ver_map.rs`

- Define a `RowPageState` enum or constants.
- Replace `frozen: AtomicBool` with `state: AtomicU8`.
- Implement methods:
    - `is_frozen()`: Returns true if state is `Frozen` OR `Transition` (for backward compatibility of some logic, or strictly `Frozen` depending on usage).
    - `is_transition()`: Returns true if state is `Transition`.
    - `set_frozen()`: CAS state from `Active` to `Frozen`.
    - `set_transition()`: CAS state from `Frozen` to `Transition`.
    - `state()`: Returns current state.

### 2. Update Table Operations

File: `doradb-storage/src/table/mod.rs`

- **Insert**:
    - `insert_row_to_page`: Check state. If `Frozen` or `Transition`, return `NoSpaceOrFrozen`.
    - *Note*: Insert usually finds a new page if the current one is frozen, so `Transition` behaves similarly to `Frozen` for inserts (redirect to another page).

- **Update**:
    - `update_row_inplace`:
        - Check state.
        - If `Transition`: Return a new `Retry` signal or handle backoff.
        - If `Frozen`: Return `NoFreeSpaceOrFrozen` (existing behavior).

- **Delete**:
    - `delete_row_internal`:
        - Add state check.
        - If `Transition`: Return `Retry` signal.
        - If `Frozen`: Allow (existing behavior).

- **Retry Logic**:
    - Update return types (`UpdateRowInplace`, `DeleteInternal`) to include a `Retry` variant.
    - Update calling loops (`update_unique_mvcc`, `delete_unique_mvcc`, etc.) to handle `Retry` by backing off (e.g., `yield_now` or short sleep) and retrying.

### 3. Verification

- Create a test case in `doradb-storage/src/table/tests.rs` (or similar).
- Simulate a page entering `Transition` state.
- Spawn a thread trying to delete/update a row on that page.
- Assert that the operation blocks or retries until the state changes (or timeout).

## Detailed Design

### State Representation

```rust
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowPageState {
    Active = 0,
    Frozen = 1,
    Transition = 2,
}
```

### Backoff Strategy

Since `Table` methods are async and the project uses `smol`, we can use `smol::future::yield_now().await` or `smol::Timer::after(Duration::from_millis(1)).await` in the retry loop.

## Impact

- **Storage Engine**: Core transaction logic updates.
- **Performance**: `Transition` state is transient; blocking should be minimal. `AtomicU8` load is as cheap as `AtomicBool`.
