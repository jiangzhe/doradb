# Issue Tracking with gh (GitHub CLI)
**IMPORTANT**: This project uses **GitHub Issues** via `gh` CLI for ALL issue tracking. Do NOT use markdown TODOs within files or local tracking files.

## Quick Start

```bash
# Find unassigned, open issues (Ready work)
gh issue list --state open -S "no:assignee" --json number,title,labels,body

# Create new issues (Labels replace strict types/priorities)
gh issue create --title "Issue title" --body "Description" --label "type:bug,priority:high"
gh issue create --title "Subtask" --body "Related to #123" --label "type:task"

# Claim a task (Assign to self)
gh issue edit <number> --add-assignee "@me"

# Check your current tasks
gh issue list --assignee "@me" --state open --json number,title

# Complete work
gh issue close <number> --comment "Completed"
```

## Labels & Taxonomy

Since GitHub does not have strict fields for type/priority, use **Labels**:

**Type Labels:**
- `type:bug` - Something broken
- `type:feature` - New functionality
- `type:task` - Work item (tests, docs, refactoring)
- `type:epic` - Large feature tracking
- `type:maintenance` - Maintenance

**Priority Labels:**
- `priority:critical` (P0) - Security, data loss, broken builds
- `priority:high` (P1) - Major features, important bugs
- `priority:medium` (P2) - Default, nice-to-have
- `priority:low` (P3) - Polish, optimization

## Epic & Subtask Management

**IMPORTANT**: GitHub uses flat Issues. Hierarchy is simulated using Labels and References.
To create an Epic structure, you must **capture the Parent ID** and reference it in the children.

### Workflow: Create Epic and Children

Since you are an AI, you must execute this in a sequence where you capture the output of the first command.

#### 1. Create the Epic (Parent)
Create the parent issue first and define it as an `epic`.
**Crucial**: You must use `--json number -q .number` to extract the ID cleanly if you are scripting, or read the output carefully.

```bash
# Create Epic and capture the ID (example for bash/scripting)
# Note: If running interactively, just create it and note the number (e.g., 42)
gh issue create \
  --title "Epic: Refactor Database Layer" \
  --body "High-level goal for the refactoring." \
  --label "type:epic" \
  --json number -q .number
# Output: 42 (Assume this is the ID)
```

#### 2. Create Sub-issues (Children)
Create individual tasks and link them to the Epic by adding `Part of #<EpicID>` in the body. This creates a bi-directional link in GitHub's timeline.

```bash
# Create sub-tasks referencing the parent (e.g., #42)
gh issue create \
  --title "Design Schema" \
  --body "Define new tables. Part of #42" \
  --label "type:task"

gh issue create \
  --title "Migration Script" \
  --body "Write SQL migration. Part of #42" \
  --label "type:task"
```

#### 3. (Optional) Advanced: Create a Task List in Epic
For better visibility, you can update the Epic's body to verify the list of sub-issues.

```bash
# Update Epic body to include a tracking list
gh issue edit 42 --body "High-level goal.

### Subtasks
- [ ] #43 Design Schema
- [ ] #44 Migration Script"
```

### Epic Rules

- ✅ **Labeling**: Always tag the parent with `type:epic` and children with `type:task` or `type:feature`.
- ✅ **Linking**: You MUST mention `Part of #<ParentID>` in the child issue's body. This ensures the Epic's timeline shows the connections.
- ❌ **No Nested Epics**: Avoid creating Epics inside Epics (Grandparent -> Parent -> Child). Keep hierarchy flat (Epic -> Tasks) for simplicity.

## Workflow for AI Agents

1. **Check ready work**:
   Run `gh issue list --state open -S "no:assignee" --json number,title,labels,body` to see backlog items that need attention.

2. **Claim your task**:
   Run `gh issue edit <number> --add-assignee "@me"` to signal you are working on it.

3. **Work on it**: Implement, test, document.

4. **Discover new work?**
   If you find a bug or missing feature while working, create it immediately:
   `gh issue create --title "Found bug" --body "Discovered while working on #<parent-id>" --label "type:bug"`

5. **Commit & Reference**:
   When committing code, include the issue reference in your commit message to link them automatically:
   `git commit -m "feat: implement login logic (Fixes #42)"`

6. **Complete**:
   Run `gh issue close <number>` (or let the commit message "Fixes #..." handle it automatically after push).

## CLI Rules & Data Format

- **JSON Output**: Always use `--json` when listing issues to get machine-readable output.
  - Example: `gh issue list --json number,title,state,body,labels`
- **Non-Interactive**: Always provide required flags (`--title`, `--body`) when creating issues to avoid interactive prompts hanging the session.
- **Filtering**: Use `--label "..."` to filter lists (e.g., `gh issue list --label "priority:high"`).

## CLI Help

Run `gh <command> --help` to see all available flags for any command.

## Important Rules

- ✅ Use `gh` CLI for ALL task tracking interactions.
- ✅ Always claim an issue (`--add-assignee "@me"`) before starting code modification.
- ✅ Use **Labels** strictly to define Type and Priority.
- ✅ Link related work by mentioning "Ref #<number>" in the issue body.
- ❌ Do NOT create markdown TODO lists in source code.
- ❌ Do NOT try to edit/manage a local tracking file (GitHub is the source of truth).
- ❌ Do NOT commit issue state; only commit code. Issue state is handled via API.
