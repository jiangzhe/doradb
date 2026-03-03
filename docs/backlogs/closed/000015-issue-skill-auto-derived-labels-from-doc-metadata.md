# Backlog: Issue Skill Auto-Derived Labels from Doc Metadata

## Summary

Enhance issue creation workflow to optionally derive labels from task/RFC document metadata instead of requiring fully manual label input.

## Reference

1. Source task document: `docs/tasks/000022-issue-skill-github-automation.md`.
2. Open question: metadata-derived labeling support.

## Scope Hint

- Define metadata source format and precedence rules.
- Preserve explicit override capability.
- Validate output still satisfies required label taxonomy.

## Acceptance Hint

`create-issue-from-doc` supports optional metadata-derived labels with tests and deterministic conflict behavior.

## Close Reason

- Type: implemented
- Detail: Implemented via docs/tasks/000045-support-metadata-derived-labels-in-issue-skill.md
- Closed By: task resolve
- Reference: docs/tasks/000045-support-metadata-derived-labels-in-issue-skill.md
- Closed At: 2026-03-03
