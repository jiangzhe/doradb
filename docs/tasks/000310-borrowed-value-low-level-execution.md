---
id: 000310
title: Borrowed Value Views for Low-Level Row Execution
status: implemented
created: 2026-09-18
github_issue: 1078
---

# Task: Borrowed Value Views for Low-Level Row Execution

## Summary

Added internal borrowed value views so owned and independently buffered inputs
share row validation, space calculation, and page writes. This provides a
prerequisite for allocation-light recovery without changing payload ownership.

## Context

Source Backlogs:

- docs/backlogs/000202-recovery-payload-allocation-bottleneck-bulk-recycling.md

Issue Labels:

- type:task
- priority:medium
- codex

[Task 000309](000309-pipelined-recovery-with-parallel-page-replay.md) identified
allocator sensitivity in parallel recovery. This task separates value consumption
from ownership so later recovery payloads can use different storage. There is no
parent RFC.

## Goals

- Share validation, sizing, and row writes across owned and borrowed inputs
  without allocating intermediate values.
- Preserve value fidelity, diagnostics, space accounting, and mutation behavior.

## Non-Goals

- Allocation-light decoding, packed batches, recycling, or a measured recovery
  performance improvement.
- Changes to public APIs, persistent formats, index interfaces, undo/redo ownership,
  or recovery scheduling.

## Rejected Alternatives

Per-value polymorphism alone cannot resolve offsets into a containing buffer.
An engine-wide generic value model would unnecessarily expand the ownership and
lifetime changes beyond the consumption boundary.

## Plan

Use `ValRef` for borrowed values and repeatable `RowValues` / `UpdateValues`
accessors for full rows and sparse updates. Existing owned slices implement the
same interfaces. Consolidate validation, sizing, and column writes around these
inputs while preserving shared and exclusive mutation behavior.

Writers copy bytes into page-owned storage and retain no input views. Nonempty
byte inputs must be separate from the destination page; enforce this before
column mutation in release builds as well as debug builds.

## Implementation Notes

Implemented the shared interfaces and consolidated duplicate entry points.
Foreground, catalog, rollback, and recovery callers retain their existing
ownership responsibilities.

Review identified that page-backed inputs could reach an overlapping byte copy.
Writers now reject those inputs before column mutation; external inputs and empty
slices remain accepted. No new unsafe operation was introduced.

Validation on 2026-09-18:

- All 2,100 workspace tests passed.
- Four focused writer tests passed in debug and release builds, including alias
  rejection and owned/borrowed parity.
- Formatting, strict workspace Clippy, and the branch style audit passed.
- Focused row-module coverage reached 97.55%; unsafe operation counts were unchanged.

## Impacts

Internal value, row, validation, and recovery consumers now support borrowed
inputs. Public ownership contracts, storage layouts, and recovery behavior remain
unchanged. No throughput improvement is claimed.

## Test Cases

- Value fidelity, repeatable access, and matching validation errors for owned and
  borrowed inputs.
- Shared/exclusive writes, null and inline/outlined transitions, space accounting,
  and independence from input-buffer lifetime.
- Destination-page alias rejection before column mutation, with external and empty
  inputs accepted in debug and release builds.
- Recovery failure handling and existing foreground, rollback, catalog, and
  recovery regressions.

## Open Questions

[Backlog 000202](../backlogs/000202-recovery-payload-allocation-bottleneck-bulk-recycling.md)
remains open for allocation-light decoding, packed batches, bounded retained
capacity, bulk recycling, and performance measurement.
