# Task: Define Main Storage Directory

## Summary

This task defines a main directory for the storage engine. Currently, the buffer pool, transaction system, and table file system each have their own separate file location settings. This makes file management difficult and prevents unit tests from running in parallel without interfering with each other. This change will introduce a main storage directory configuration at the storage engine level and have sub-systems inherit this setting. This will create a single root directory for the running engine. For unit tests, we can then point each test case to a randomly-generated directory and clean it up when it's finished, which will allow all tests to run in parallel.

## Context

The storage engine is composed of several components that interact with the filesystem:

- The `EvictableBufferPool` in `doradb-storage/src/buffer/evict.rs` manages a buffer file, which has a `file_path` configuration that defaults to a local file `"databuffer.bin"`.
- The `TransactionSystem` in `doradb-storage/src/trx/mod.rs` uses redo logs, which have a `log_file_prefix` that defaults to `"redo.log"`.
- The `TableFileSystem` in `doradb-storage/src/file/table_fs.rs` manages table files within a `base_dir` that defaults to the current directory (`.`).

These disparate configurations make it hard to manage all the files created by the database, and it's especially problematic for testing, where multiple test instances can conflict with each other.

## Goals

- Add a `main_dir` property to `EngineConfig` in `doradb-storage/src/engine.rs`.
- The `main_dir` will serve as the root directory for all files created by the storage engine.
- Update `EvictableBufferPoolConfig`, `TrxSysConfig`, and `TableFileSystemConfig` to use the `main_dir` from `EngineConfig`. The file paths for these components should be relative to `main_dir`.
- Update the `build` method in `EngineConfig` to propagate the `main_dir` to the sub-configs before building the components.
- Ensure that existing tests are updated to create and use temporary directories for each test, and that they clean up after themselves. This will enable parallel test execution.

## Non-Goals

- This task will not change the file format of any of the existing files.
- This task will not introduce a new configuration file format. The changes will be made to the existing Rust structs.
- This task will not involve refactoring the logging or error handling of the storage engine.

## Plan

1.  **Modify `EngineConfig`**:
    - Add a new field `main_dir: String` to `EngineConfig` in `doradb-storage/src/engine.rs`. We will not maintain backward compatibility, instead letting the compiler point out all locations that need to be updated. A new method `main_dir(mut self, main_dir: impl Into<String>) -> Self` will be added.

2.  **Update `EngineConfig::build`**:
    - In the `build` method of `EngineConfig`, construct the absolute paths for the sub-systems and update their respective configurations (`EvictableBufferPoolConfig`, `TrxSysConfig`, `TableFileSystemConfig`).
    - For example, the `file_path` in `data_buffer` would become something like `main_dir/databuffer.bin`.

3.  **Update `TableFileSystemConfig`**:
    - The `base_dir` in `TableFileSystemConfig` will be updated to use the `main_dir`.

4.  **Update `EvictableBufferPoolConfig`**:
    - The `file_path` in `EvictableBufferPoolConfig` will be updated.

5.  **Update `TrxSysConfig`**:
    - The `log_file_prefix` in `TrxSysConfig` will be updated to include the main directory.

6.  **Update Tests**:
    - Go through the tests in the `doradb-storage` crate, particularly in `doradb-storage/src/buffer/evict.rs` and other modules that create an `Engine`.
    - Use a library like `tempfile` to create a temporary directory for each test.
    - Pass the path of the temporary directory to the `EngineConfig`.
    - Implement a RAII guard, conditionally compiled with `#[cfg(test)]`, that holds the `Engine` instance. When the guard is dropped at the end of the test, it will automatically clean up the temporary directory and all its contents. This will make the tests cleaner and ensure no test artifacts are left behind.
    - For example, in `doradb-storage/src/buffer/evict.rs`, the hardcoded file paths like `"data1.bin"` will be removed.

## Impacts

- `doradb-storage/src/engine.rs`: `EngineConfig` will be modified.
- `doradb-storage/src/buffer/evict.rs`: `EvictableBufferPoolConfig` and its usage in tests will be changed.
- `doradb-storage/src/trx/sys_conf.rs`: `TrxSysConfig` will be changed.
- `doradb-storage/src/file/table_fs.rs`: `TableFileSystemConfig` will be changed.
- All test files that construct an `Engine` or its components will need to be updated to use temporary directories.

## Open Questions

- Are there any other places where file paths are configured that I might have missed? A codebase search for ".log", ".bin", ".tbl" file extensions might be helpful to ensure all paths are covered.
