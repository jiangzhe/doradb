# Task: Hitchhike System Transaction

## Summary

Optimize system transactions (specifically for row page creation) to return immediately after submitting logs to the commit group, without waiting for fsync. This "hitchhiking" approach reduces latency by relying on subsequent user transactions or the background sync thread for persistence. Additionally, track the creation CTS in `RowVersionMap` to ensure correct visibility, using the CTS from the redo log header during recovery.

## Context

Currently, system transactions (used for `CreateRowPage`) follow the same commit path as user transactions, which includes waiting for the log to be persisted (fsync). This introduces unnecessary latency for page creation. Since the system transaction's effect (new page) is only meaningful when used by subsequent user transactions (which insert data into it), we can safely return early. If the system transaction is not persisted, the dependent user transaction (which *must* wait for fsync) will also not be persisted (or will ensure the system transaction is persisted first due to log ordering).

## Goals

1.  Enable system transactions to return the assigned Commit Timestamp (CTS) immediately after the log is added to the commit group, without waiting for fsync.
2.  Add `create_cts` to `RowVersionMap` to track when the page was created.
3.  Update the recovery process to restore `create_cts` from the redo log (using the log header's CTS) and populate `RecoverMap`/`RowVersionMap`.

## Non-Goals

-   Handling multiple log streams/partitions scenarios where causal consistency is harder to maintain.
-   Optimizing other types of DDL or system transactions beyond `CreateRowPage` (though the mechanism might be reusable).

## Plan

### 1. Async Commit for System Transactions

-   **Modify `LogPartition::commit`**:
    -   Add a parameter (e.g., `wait_sync: bool`) to control whether to await the sync listener.
    -   If `wait_sync` is false, return the generated `cts` immediately after adding the group to the queue.
-   **Modify `TransactionSystem::commit_sys`**:
    -   Update to call `partition.commit` with `wait_sync = false`.
    -   Return `Result<TrxID>` instead of `Result<()>`.

### 2. Update `RowVersionMap`

-   **Modify `RowVersionMap` struct**:
    -   Add `create_cts: AtomicU64` (initialized to 0).
-   **Modify `RowVersionMap` methods**:
    -   Add a method `set_create_cts(cts: TrxID)` to update the timestamp.

### 3. Integration

-   **System Transaction Commit**:
    -   In the caller of `commit_sys` (likely where `SysTrx` is used), receive the returned `cts`.
    -   Use this `cts` to update the `RowVersionMap` of the newly created page.

### 4. Recovery

-   **Modify `RecoverMap`**:
    -   Add `create_cts: TrxID` field to hold the recovered creation timestamp.
-   **Modify `LogRecovery::replay_ddl`**:
    -   For `DDLRedo::CreateRowPage`, extract the transaction's CTS from `log.header.cts`.
    -   Pass this `cts` to `init_recover_map`.
-   **Modify `RecoverMap` initialization**:
    -   Ensure `init_recover_map` sets the `create_cts`.
-   **Modify `RecoverMap` to `RowVersionMap` conversion**:
    -   When converting/swapping `RecoverMap` to `RowVersionMap` (if applicable) or when initializing `RowVersionMap` from `RecoverMap`, ensure `create_cts` is transferred.

## Impacts

-   `doradb-storage/src/trx/log.rs`: `LogPartition::commit` logic.
-   `doradb-storage/src/trx/sys.rs`: `commit_sys` signature and usage.
-   `doradb-storage/src/trx/ver_map.rs`: `RowVersionMap` struct.
-   `doradb-storage/src/trx/recover.rs`: Recovery logic for `CreateRowPage`.

## Open Questions

-   None.
