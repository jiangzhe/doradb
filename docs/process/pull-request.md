# Pull Request Workflow (gh pr)

**IMPORTANT**: Use `gh pr` commands to manage code submissions. Do NOT rely solely on `git push`. PRs are required for code review and merging.

## Quick Start

```bash
# 1. Create a PR (Push branch first!)
git push -u origin <branch-name>
gh pr create --title "feat: add login" --body "Fixes #42. Implements auth." --base main

# 2. Check CI/CD status
gh pr checks --watch

# 3. List PRs waiting for review
gh pr list --state open --json number,title,headRefName,url

# 4. Checkout a PR locally to review/fix
gh pr checkout <number>

# 5. Merge (if authorized)
gh pr merge <number> --squash --delete-branch
```

## PR Workflow for AI Agents

1.  **Push Code**:
    Before creating a PR, ensure your local branch is pushed to the remote:
    `git push -u origin <current-branch>`

2.  **Create the PR**:
    Run `gh pr create` with explicit flags to avoid interactive prompts.
    *   **Linking Issues**: You MUST include "Fixes #<issue-number>" or "Closes #<issue-number>" in the `--body` to auto-close the issue upon merge.
    *   **Example**:
        ```bash
        gh pr create \
          --title "feat: implementation summary" \
          --body "Detailed description of changes. Fixes #123." \
          --base main
        ```

3.  **Wait for Checks (CI)**:
    After creating, verify that automated tests pass:
    `gh pr checks --watch` (This will wait until checks pass or fail).

4.  **Handle Feedback**:
    If changes are requested:
    1.  Edit files locally.
    2.  `git commit -am "fix: address review comments"`
    3.  `git push` (The PR updates automatically).
    4.  Comment on the PR: `gh pr comment <number> --body "Fixed review items."`

5.  **Merge**:
    If you have permission to merge and checks pass:
    `gh pr merge <number> --squash --delete-branch`
    *   **Squash**: Combines all commits into one clean commit.
    *   **Delete branch**: Keeps the repo clean.

## PR Rules

- ✅ **Always Push First**: `gh` cannot create a PR if the branch doesn't exist on the remote.
- ✅ **Link Issues**: Always reference the issue ID in the body (e.g., `Fixes #12`).
- ✅ **Descriptive Titles**: Use Conventional Commits format (e.g., `fix:`, `feat:`, `chore:`) for PR titles.
- ✅ **Non-Interactive**: Always provide `--title` and `--body` to prevent the CLI from opening a text editor or browser.
- ❌ **Do NOT Force Push**: Avoid `git push -f` unless strictly necessary for a rebase on a shared branch.
- ❌ **Do NOT Merge Broken Builds**: Check `gh pr checks` before attempting to merge.
