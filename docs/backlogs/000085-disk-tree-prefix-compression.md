# Backlog: Persist DiskTree upper fences for prefix compression

## Summary

Persist meaningful upper fences for secondary DiskTree leaf and branch nodes so BTreeNode common-prefix compression can take effect for clustered encoded keys.

## Reference

Current task discussion and implementation in docs/tasks/000119-composite-secondary-index-core.md and docs/rfcs/0014-dual-tree-secondary-index.md; DiskTree currently initializes persisted nodes with an empty upper_fence.

## Deferred From (Optional)

docs/tasks/000119-composite-secondary-index-core.md; docs/rfcs/0014-dual-tree-secondary-index.md

## Deferral Context (Optional)

- Defer Reason: The current fix is scoped to removing full-tree materialization from non-unique prefix scans; changing persisted node fence semantics is a broader storage-format and traversal hardening task.
- Findings: DiskTree leaf and branch writers currently call BTreeNode::init with upper_fence set to an empty slice, so common_prefix_len(lower_fence, upper_fence) is normally zero and BTreeNode prefix compression does not take effect. The streamed prefix scan now uses common-prefix-aware lower-bound helpers, so it should benefit once real upper fences are persisted.
- Direction Hint: Persist real upper fences for DiskTree nodes, then verify exact lookup and range traversal against nonempty node common prefixes before relying on prefix-compressed persisted blocks.

## Scope Hint

Evaluate DiskTree leaf and branch block writing, exact lookup, and streamed range traversal with nonempty upper fences; keep compatibility with current root snapshot semantics.

## Acceptance Hint

DiskTree tests demonstrate nonempty BTreeNode common prefixes for clustered keys, exact lookups and non-unique prefix scans remain correct, and prefix scans benefit from the existing lower-bound helpers.

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
