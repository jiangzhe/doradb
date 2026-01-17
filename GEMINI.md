# DoraDB - Gemini Context

## Project Overview

**DoraDB** is a high-performance database project currently focusing on building a robust **storage engine** from scratch in Rust.

*   **Current Status**: The project is in a major transition. The primary focus is the `doradb-storage` crate, which implements an asynchronous, HTAP-optimized storage engine.
*   **Legacy Code**: The `legacy/` directory contains previous implementations of the SQL layer, server, and query planner. These are currently inactive and excluded from the main workspace but serve as a reference.
*   **Goal**: To build a fast, hybrid storage engine with full transactional support, suitable for both transactional (OLTP) and analytical (OLAP) workloads.

## Architecture (`doradb-storage`)

The storage engine uses a unique architecture designed for modern hardware and mixed workloads:

*   **Hybrid Data Layout**:
    *   **In-Memory Row Pages**: Store hot data for fast, in-place updates.
    *   **LWC (LightWeight Columnar) Pages**: Store warm data on disk using lightweight compression (bitpacking) for efficient scanning and random access.
    *   **Column Pages**: (Future) For cold archival data.
*   **Unified Addressing**: A persistent **RowID** identifies a row throughout its lifecycle. A **Block Index** maps RowIDs to physical locations (memory pages or disk blocks).
*   **Transactions**: Implements a **No-Steal / No-Force** recovery model. Committed transactions are recorded in a **Redo Log**.
*   **Concurrency**: Heavy use of **Async IO** (`libaio`) and asynchronous synchronization primitives.
*   **Indexing**:
    *   **Secondary Index**: Composite of a **MemTree** (LSM-like, in-memory) and a **DiskTree** (Copy-on-Write B+Tree on disk).
    *   **Block Index**: Specialized B+Tree for RowID mappings.

## Building and Running

### Prerequisites
*   **OS**: Linux (Due to `libaio` dependency).
*   **System Libraries**: `libaio1`, `libaio-dev` (Required unless building with `--no-default-features`).
    *   Ubuntu/Debian: `sudo apt-get install libaio1 libaio-dev`
*   **Rust**: Stable toolchain (Edition 2024).

### Key Commands

Run these commands from the project root:

*   **Build Storage Engine**:
    ```bash
    cargo build -p doradb-storage
    ```
    *To build without `libaio` (e.g., in environments where it cannot be installed):*
    ```bash
    cargo build -p doradb-storage --no-default-features
    ```

*   **Run Tests**:
    **Note**: Tests typically require running in a single thread due to concurrent file/resource access patterns in the test suite.
    ```bash
    cargo test -p doradb-storage
    ```
    *To run tests without `libaio` support:*
    ```bash
    cargo test -p doradb-storage --no-default-features
    ```

*   **Run Benchmarks/Examples**:
    ```bash
    cargo run -p doradb-storage --example bench_btree
    ```

## Development Conventions

### Code Style
*   **Formatting**: Standard `rustfmt`.
*   **Edition**: Rust 2024.
*   **Conventions**: Idiomatic Rust. Async/Await is used extensively.

### Issue Tracking & Contribution
*   **Tooling**: Use the GitHub CLI (`gh`) for all issue and PR management.
*   **Issues**:
    *   **Labels**: Strictly use `type:*` (e.g., `type:bug`, `type:feature`) and `priority:*` (e.g., `priority:high`, `priority:medium`) labels.
    *   **Workflow**: Find unassigned issues -> Assign to self -> Implement -> PR.
*   **Pull Requests**:
    *   **Format**: Use Conventional Commits for titles (e.g., `feat: add wal`, `fix: buffer overflow`).
    *   **Linking**: Always include "Fixes #<issue_id>" in the PR body.
    *   **Review**: PRs go through a review process (human or Gemini AI).

## Directory Structure

*   **`doradb-storage/`**: **(Active)** The core storage engine crate.
    *   `src/`: Source code.
        *   `buffer/`: Buffer pool manager.
        *   `catalog/`: Metadata management.
        *   `file/`: Page-based file storage (CoW B+Tree).
        *   `index/`: Index implementations.
        *   `io/`: Async Direct IO bindings.
        *   `trx/`: Transaction manager and logging.
    *   `docs/`: Detailed architectural documentation.
    *   `examples/`: Runnable benchmarks.
*   **`legacy/`**: **(Inactive)** Old full-stack implementation (SQL, Server, Plan). Do not modify unless migrating logic.
*   **`.github/`**: CI/CD workflows and Gemini Agent configuration.

## Key Files
*   `doradb-storage/Cargo.toml`: Active dependency manifest.
*   `docs/architecture.md`: The definitive guide to the storage engine's design.
*   `AGENTS.md`: Guidelines for AI agents working in this repo.
*   `docs/process/unit-test.md`: Guidelines for unit testing.
