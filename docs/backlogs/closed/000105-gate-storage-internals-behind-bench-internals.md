# Backlog: Gate storage internals for examples behind bench-internals

## Summary

Some examples and benchmarks currently pressure storage implementation details to stay public, including layout constructors, row-page index allocation APIs, and frame sizing types. Add a feature-gated internal benchmark facade so the default library API exposes only supported public interfaces while examples can still opt into benchmark-only internals.

## Reference

User review during docs/tasks/000152-split-table-metadata-column-index-layouts.md. Current examples include doradb-storage/examples/bench_block_index.rs, which constructs TableColumnLayout and RowPageIndex directly, and doradb-storage/examples/bench_readonly_single_miss_latency.rs, which depends on BufferFrame size/layout visibility.

## Deferred From (Optional)

docs/tasks/000152-split-table-metadata-column-index-layouts.md

## Deferral Context (Optional)

- Defer Reason: Task 000152 is focused on splitting table metadata into column and index layouts and tightening call sites. Reworking the crate public API surface and feature-gating examples is related cleanup, but it broadens the task into Cargo features, example configuration, and API design.
- Findings: During task 000152, some APIs needed to remain public primarily because examples build outside the crate privacy boundary. TableColumnLayout constructors are useful for bench_block_index, RowPageIndex-style direct allocation is benchmark-oriented, and BufferFrame visibility is used for frame/page size accounting in bench_readonly_single_miss_latency. These are implementation concerns rather than stable end-user library APIs.
- Direction Hint: Prefer explicit feature-gated exposure over implicit broad public visibility. Add a bench-internals feature, gather all benchmark/example-only exports in one place, and make examples depend on that facade. Keep core implementation details crate-private in normal builds, and avoid delegating many public methods only to satisfy benchmark setup.

## Scope Hint

Introduce a bench-internals Cargo feature and centralize benchmark/example-only re-exports behind one explicit facade module. Move implementation-detail types, constructors, and methods back to crate-private visibility by default where they are not part of the supported library API. Update examples and benchmarks to build through the facade when the feature is enabled, and avoid scattering public visibility changes across core modules only for examples.

## Acceptance Hint

Default cargo check for doradb-storage succeeds without exposing example-only internals as public API. Examples or benchmark targets that require internals are gated on bench-internals and compile with that feature. The feature exposes internals through a centralized module or re-export list instead of ad hoc public methods on core types. Documentation or comments make clear that bench-internals is not a stable library API.

## Notes (Optional)


## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```

## Close Reason

- Type: implemented
- Detail: Implemented via docs/tasks/000157-narrow-storage-public-api.md by removing example targets and hiding default storage internals instead of adding a bench-internals facade.
- Closed By: backlog close
- Reference: docs/tasks/000157-narrow-storage-public-api.md
- Closed At: 2026-05-26
