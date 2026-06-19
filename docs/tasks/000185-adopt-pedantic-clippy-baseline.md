---
id: 000185
title: Adopt Pedantic Clippy Baseline
status: implemented
created: 2026-06-19
github_issue: 727
---

# Task: Adopt Pedantic Clippy Baseline

## Summary

Enable `clippy::pedantic` for the active `doradb-storage` crate through the
workspace lint manifest, collect and group the current warning surface, keep the
existing strict clippy gate passing with an explicit deferred-lint allow list,
and fix the first low-risk mechanical pedantic warnings. The final state should
make selected easy pedantic rules enforced immediately while documenting and
deferring the warning families that need domain-specific cleanup.

## Context

`doradb-storage` originally enabled `#![warn(clippy::all)]` and
`#![warn(clippy::undocumented_unsafe_blocks)]` in `doradb-storage/src/lib.rs`.
The repository lint gate is:

```bash
cargo clippy -p doradb-storage --all-targets -- -D warnings
```

`docs/process/lint.md` currently says `clippy::pedantic` is not enforced and
that future adoption should be selective. The source backlog asks for warning
classification, project-specific enable/disable policy, compatibility with the
existing `-D warnings` baseline, and concrete implementation steps.

Source Backlogs:
- docs/backlogs/000001-pedantic-clippy-adoption-policy.md

Issue Labels:
- type:task
- priority:medium
- codex

Round 1 evidence used this command on the dispatch checkout:

```bash
cargo clippy -p doradb-storage --all-targets --message-format=json -- -W clippy::pedantic
```

Toolchain for that inventory:

```text
rustc 1.95.0 (59807616e 2026-04-14)
clippy 0.1.95 (59807616e1 2026-04-14)
cargo 1.95.0 (f2d3ce0bd 2026-03-21)
```

Initial result: 2,932 pedantic warnings across 65 lint ids. The implementer must
recompute this inventory from the task worktree before editing because counts
can change with branch content and Clippy versions.

Observed initial lint-id counts:

```text
584 clippy::doc_markdown
477 clippy::cast_possible_truncation
174 clippy::semicolon_if_nothing_returned
146 clippy::must_use_candidate
130 clippy::cast_lossless
108 clippy::manual_let_else
 97 clippy::cast_sign_loss
 82 clippy::uninlined_format_args
 78 clippy::too_many_lines
 75 clippy::needless_pass_by_value
 69 clippy::cast_possible_wrap
 64 clippy::return_self_not_must_use
 62 clippy::trivially_copy_pass_by_ref
 58 clippy::unused_self
 58 clippy::missing_errors_doc
 53 clippy::redundant_closure_for_method_calls
 52 clippy::ptr_as_ptr
 45 clippy::match_same_arms
 42 clippy::cast_precision_loss
 39 clippy::elidable_lifetime_names
 29 clippy::similar_names
 28 clippy::ptr_cast_constness
 27 clippy::ref_as_ptr
 25 clippy::unnecessary_wraps
 24 clippy::single_match_else
 24 clippy::needless_continue
 24 clippy::map_unwrap_or
 19 clippy::ignored_unit_patterns
 18 clippy::items_after_statements
 18 clippy::cast_ptr_alignment
 16 clippy::if_not_else
 15 clippy::cloned_instead_of_copied
 14 clippy::unused_async
 14 clippy::checked_conversions
 13 clippy::redundant_else
 11 clippy::range_plus_one
 11 clippy::bool_to_int_with_if
  8 clippy::unreadable_literal
  8 clippy::struct_field_names
  8 clippy::same_length_and_capacity
  8 clippy::match_wildcard_for_single_variants
  8 clippy::explicit_iter_loop
  7 clippy::wildcard_imports
  7 clippy::manual_assert
  6 clippy::unnested_or_patterns
  6 clippy::unnecessary_semicolon
  6 clippy::borrow_as_ptr
  4 clippy::used_underscore_binding
  4 clippy::no_effect_underscore_binding
  4 clippy::missing_fields_in_debug
  2 clippy::verbose_bit_mask
  2 clippy::struct_excessive_bools
  2 clippy::needless_for_each
  2 clippy::missing_panics_doc
  2 clippy::match_wild_err_arm
  2 clippy::large_stack_arrays
  2 clippy::float_cmp
  2 clippy::explicit_deref_methods
  2 clippy::default_trait_access
  2 clippy::case_sensitive_file_extension_comparisons
  1 clippy::stable_sort_primitive
  1 clippy::should_panic_without_expect
  1 clippy::format_push_string
  1 clippy::duration_suboptimal_units
  1 clippy::decimal_bitwise_operands
```

Major initial hotspots by file included `compression/bitpacking.rs`,
`value.rs`, `index/btree/node.rs`, `lwc/mod.rs`, `id.rs`,
`index/btree/mod.rs`, `index/disk_tree.rs`, `row/mod.rs`,
`index/column_block_index.rs`, and `table/access.rs`. These include
format-sensitive, unsafe-sensitive, and storage-layout-sensitive code, so this
task should keep the first cleanup pass conservative.

The adopted policy for this task is:

1. Enable the pedantic lint group through workspace Cargo lint settings.
2. Temporarily allow all currently violated pedantic lint ids to restore the
   strict clippy gate.
3. Fix low-risk lint ids first.
4. Remove fixed lint ids from the final allow list.
5. Leave only still-violated, intentionally deferred pedantic lint ids in the
   final workspace allow list.

This is an explicit exception to the normal preference for scoped lint
suppression because the current pedantic warning surface is too large for
item-by-item annotations. Do not allow `clippy::pedantic` as a group; allow only
the concrete deferred lint ids.

## Goals

1. Add `clippy::pedantic` to the workspace lint manifest and make
   `doradb-storage` inherit workspace lints.
2. Collect the current pedantic warning inventory from the task worktree and
   group it by lint id and frequency.
3. Add temporary workspace `allow` entries for all currently violated pedantic
   lint ids so the existing strict clippy command can run with pedantic enabled.
4. Fix low-risk mechanical warning families and remove their lint ids from the
   final allow list when they reach zero warnings.
5. Keep the existing lint gate command unchanged and passing:
   `cargo clippy -p doradb-storage --all-targets -- -D warnings`.
6. Update `docs/process/lint.md` so it accurately describes the new pedantic
   baseline and deferred-lint policy.
7. Keep the source backlog traceable for resolve-time closure.

## Non-Goals

1. Full remediation of all pedantic warnings.
2. Large public API or internal signature rewrites for
   `needless_pass_by_value`, `trivially_copy_pass_by_ref`, `unused_self`,
   `unused_async`, or `unnecessary_wraps`.
3. Numeric-cast hardening across storage formats, ids, compression code,
   indexes, recovery, or serialization.
4. Raw-pointer, alignment, or unsafe-adjacent refactors in buffer, row, value,
   index, I/O, LWC, transaction, or file paths.
5. Large function decomposition solely to satisfy `too_many_lines`.
6. Documentation-wide rewrites solely for `doc_markdown`, `missing_errors_doc`,
   `missing_panics_doc`, `must_use_candidate`, or `return_self_not_must_use`.
7. CI or pre-commit command changes beyond what naturally happens because the
   workspace lint manifest enables pedantic under the existing strict clippy
   command.

## Plan

1. Refresh the warning inventory in the task worktree.
   - Run:
     ```bash
     cargo clippy -p doradb-storage --all-targets --message-format=json -- -W clippy::pedantic
     ```
   - Parse diagnostics by lint id and frequency.
   - Keep the grouped result for implementation notes during `task resolve`.

2. Enable pedantic in Cargo manifests.
   - Add `clippy::pedantic` to `Cargo.toml` under `[workspace.lints.clippy]`.
   - Keep the existing `clippy::all` and `clippy::undocumented_unsafe_blocks`
     warnings in the same workspace lint table.
   - Add `[lints] workspace = true` to `doradb-storage/Cargo.toml`.
   - Give lint groups lower priority than concrete lint ids so the deferred
     allow entries override `clippy::pedantic`.

3. Add a temporary concrete allow list.
   - Use the recomputed inventory, not the Round 1 counts, as the source of
     truth.
   - Add only concrete lint ids to `[workspace.lints.clippy]`, for example
     `doc_markdown = "allow"` and `cast_possible_truncation = "allow"`.
   - Do not allow `clippy::pedantic` as a group.
   - Keep lint names sorted consistently so future removals are reviewable.

4. Fix low-risk mechanical lint families first.
   - Preferred initial candidates:
     - `clippy::uninlined_format_args`
     - `clippy::cloned_instead_of_copied`
     - `clippy::needless_continue`
     - `clippy::unnecessary_semicolon`
     - `clippy::unnested_or_patterns`
     - `clippy::range_plus_one`
     - `clippy::default_trait_access`
     - `clippy::explicit_deref_methods`
     - `clippy::duration_suboptimal_units`
     - `clippy::decimal_bitwise_operands`
     - `clippy::stable_sort_primitive`
     - `clippy::should_panic_without_expect`
   - Consider `clippy::semicolon_if_nothing_returned`,
     `clippy::redundant_closure_for_method_calls`,
     `clippy::manual_let_else`, `clippy::map_unwrap_or`,
     `clippy::if_not_else`, `clippy::redundant_else`,
     `clippy::single_match_else`, `clippy::explicit_iter_loop`, and
     `clippy::needless_for_each` only when each edit is local and does not make
     control flow less clear.
   - After each cleanup group, rerun clippy or a focused clippy check and remove
     that lint id from the workspace allow list only if it is clean across all
     targets.

5. Defer behavior-sensitive lint families.
   - Defer documentation and public API policy:
     `doc_markdown`, `missing_errors_doc`, `missing_panics_doc`,
     `must_use_candidate`, `return_self_not_must_use`.
   - Defer numeric conversion policy:
     `cast_possible_truncation`, `cast_lossless`, `cast_sign_loss`,
     `cast_possible_wrap`, `cast_precision_loss`, `checked_conversions`,
     `bool_to_int_with_if`, `unreadable_literal`.
   - Defer pointer, alignment, and unsafe-adjacent policy:
     `ptr_as_ptr`, `ptr_cast_constness`, `ref_as_ptr`, `cast_ptr_alignment`,
     `borrow_as_ptr`, `same_length_and_capacity`.
   - Defer API/signature and async shape:
     `needless_pass_by_value`, `trivially_copy_pass_by_ref`, `unused_self`,
     `unused_async`, `unnecessary_wraps`.
   - Defer structural/style lints that need design review or broad churn:
     `too_many_lines`, `similar_names`, `items_after_statements`,
     `struct_field_names`, `struct_excessive_bools`, `wildcard_imports`,
     `match_wildcard_for_single_variants`, `match_same_arms`,
     `missing_fields_in_debug`, `manual_assert`, `float_cmp`,
     `large_stack_arrays`, `case_sensitive_file_extension_comparisons`,
     `format_push_string`, `verbose_bit_mask`, `used_underscore_binding`,
     `no_effect_underscore_binding`, `match_wild_err_arm`,
     `ignored_unit_patterns`, `elidable_lifetime_names`.
   - The implementer may fix an item from a deferred family if it is obviously
     local and safe, but the task does not require reducing those families.

6. Update lint documentation.
   - Update `docs/process/lint.md` so `Pedantic Lints` no longer says pedantic
     is not enforced.
   - Document that pedantic is enabled in the workspace lint manifest with an
     explicit project allow list for deferred lint ids.
   - State that new code should avoid introducing warnings for lint ids that
     are not in the allow list, and should prefer fixing/removing deferred
     allows when touching nearby code.
   - Keep the lint command itself unchanged unless implementation discovers a
     concrete command mismatch.

7. Validate.
   - Run `cargo fmt`.
   - Run:
     ```bash
     cargo clippy -p doradb-storage --all-targets -- -D warnings
     ```
   - Run:
     ```bash
     cargo nextest run -p doradb-storage
     ```
   - If implementation touches backend-neutral I/O or backend-specific files,
     also run:
     ```bash
     cargo nextest run -p doradb-storage --no-default-features --features libaio
     ```

8. Resolve-time synchronization.
   - During `task resolve`, record the recomputed warning inventory, fixed lint
     ids, final deferred allow list, and validation results in
     `Implementation Notes`.
   - Close `docs/backlogs/000001-pedantic-clippy-adoption-policy.md` as
     implemented after behavior is verified.
   - If substantial deferred work remains actionable beyond the final allow
     list, create follow-up backlog items for those categories rather than
     expanding this task.

## Implementation Notes

Implemented on 2026-06-19.

1. Recomputed the task-worktree pedantic inventory before editing:
   2,932 diagnostics across 65 lint ids. The recomputed counts matched the
   Round 1 inventory in this document.
2. Enabled `clippy::pedantic` in the workspace lint manifest, moved the
   existing `clippy::all` and `clippy::undocumented_unsafe_blocks` warnings
   there, made `doradb-storage` inherit workspace lints, and kept the workspace
   lint table at the bottom of the root `Cargo.toml`.
3. Added a concrete workspace deferred allow list for the still-violated
   pedantic lint ids. The final deferred baseline is 2,873 diagnostics across
   55 lint ids. `clippy::uninlined_format_args` is intentionally deferred by
   project preference, and `clippy::range_plus_one` is intentionally deferred
   because inclusive-range rewrites can affect performance-sensitive code.
4. Fixed these low-risk mechanical lint ids and removed them from the allow
   list after focused verification showed zero diagnostics:
   `clippy::cloned_instead_of_copied`,
   `clippy::decimal_bitwise_operands`,
   `clippy::default_trait_access`,
   `clippy::duration_suboptimal_units`,
   `clippy::explicit_deref_methods`, `clippy::needless_continue`,
   `clippy::should_panic_without_expect`, `clippy::stable_sort_primitive`,
   `clippy::unnecessary_semicolon`, and `clippy::unnested_or_patterns`.
5. Final deferred allow list:
   ```text
   clippy::bool_to_int_with_if
   clippy::borrow_as_ptr
   clippy::case_sensitive_file_extension_comparisons
   clippy::cast_lossless
   clippy::cast_possible_truncation
   clippy::cast_possible_wrap
   clippy::cast_precision_loss
   clippy::cast_ptr_alignment
   clippy::cast_sign_loss
   clippy::checked_conversions
   clippy::doc_markdown
   clippy::elidable_lifetime_names
   clippy::explicit_iter_loop
   clippy::float_cmp
   clippy::format_push_string
   clippy::if_not_else
   clippy::ignored_unit_patterns
   clippy::items_after_statements
   clippy::large_stack_arrays
   clippy::manual_assert
   clippy::manual_let_else
   clippy::map_unwrap_or
   clippy::match_same_arms
   clippy::match_wild_err_arm
   clippy::match_wildcard_for_single_variants
   clippy::missing_errors_doc
   clippy::missing_fields_in_debug
   clippy::missing_panics_doc
   clippy::must_use_candidate
   clippy::needless_for_each
   clippy::needless_pass_by_value
   clippy::no_effect_underscore_binding
   clippy::ptr_as_ptr
   clippy::ptr_cast_constness
   clippy::range_plus_one
   clippy::redundant_closure_for_method_calls
   clippy::redundant_else
   clippy::ref_as_ptr
   clippy::return_self_not_must_use
   clippy::same_length_and_capacity
   clippy::semicolon_if_nothing_returned
   clippy::similar_names
   clippy::single_match_else
   clippy::struct_excessive_bools
   clippy::struct_field_names
   clippy::too_many_lines
   clippy::trivially_copy_pass_by_ref
   clippy::uninlined_format_args
   clippy::unnecessary_wraps
   clippy::unreadable_literal
   clippy::unused_async
   clippy::unused_self
   clippy::used_underscore_binding
   clippy::verbose_bit_mask
   clippy::wildcard_imports
   ```
6. Updated `docs/process/lint.md` to document that pedantic is enabled under
   the unchanged strict clippy command and that the workspace allow list is a
   concrete deferred baseline, not a blanket `clippy::pedantic` suppression.
7. Resolve synchronization:
   - Closed source backlog
     `docs/backlogs/000001-pedantic-clippy-adoption-policy.md` as implemented;
     it now lives under `docs/backlogs/closed/`.
   - Created follow-up backlog
     `docs/backlogs/000129-reduce-deferred-pedantic-clippy-allow-list.md` for
     future category-sized reductions of the deferred allow list.
8. Validation passed:
   - `cargo fmt`
   - `cargo clippy -p doradb-storage --all-targets -- -D warnings`
   - Focused fixed-lint Clippy check excluding deferred
     `clippy::range_plus_one` and `clippy::uninlined_format_args`
   - `cargo nextest run -p doradb-storage`: 1003 passed
   - `cargo nextest run -p doradb-storage --no-default-features --features libaio`:
     1001 passed
   - `git diff --check`

## Impacts

1. Cargo manifests
   - Add workspace Clippy lint settings to the root `Cargo.toml`.
   - Make `doradb-storage/Cargo.toml` inherit workspace lints.
   - Remove the old crate-level lint attributes from `doradb-storage/src/lib.rs`.
2. `doradb-storage/src/**`
   - Low-risk mechanical warning fixes across modules touched by selected lint
     families.
3. `doradb-storage/examples/**`
   - Included by `--all-targets`; fix easy warnings here when covered by the
     selected mechanical lint families.
4. `docs/process/lint.md`
   - Update pedantic policy and allow-list guidance.
5. `docs/backlogs/000001-pedantic-clippy-adoption-policy.md`
   - Source backlog to close during resolve.

Potential unsafe-sensitive areas:

1. `doradb-storage/src/buffer/**`
2. `doradb-storage/src/index/**`
3. `doradb-storage/src/io/**`
4. `doradb-storage/src/lwc/**`
5. `doradb-storage/src/row/**`
6. `doradb-storage/src/trx/**`
7. `doradb-storage/src/value.rs`

Avoid semantic cleanup in these areas unless the warning fix is mechanically
local and preserves existing invariants.

## Test Cases

1. Pedantic inventory command runs successfully and produces grouped lint-id
   counts for the task worktree.
2. The workspace lint manifest enables `clippy::pedantic`, and
   `doradb-storage` inherits workspace lints.
3. Final workspace allow list contains only concrete still-violated deferred
   pedantic lint ids.
4. Lint ids fixed by this task are removed from the workspace allow list and
   produce zero warnings under `--all-targets`.
5. `docs/process/lint.md` describes the new pedantic policy accurately.
6. `cargo fmt` passes.
7. `cargo clippy -p doradb-storage --all-targets -- -D warnings` passes.
8. `cargo nextest run -p doradb-storage` passes.
9. If I/O/backend files are touched,
   `cargo nextest run -p doradb-storage --no-default-features --features libaio`
   passes or the task resolve notes explain why it was not run.

## Open Questions

1. Follow-up reduction of the deferred pedantic allow list is tracked in
   `docs/backlogs/000129-reduce-deferred-pedantic-clippy-allow-list.md`.
2. `clippy::range_plus_one` and `clippy::uninlined_format_args` remain
   intentionally allowed unless project lint policy changes.
