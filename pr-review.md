# Setup

You’re conducting a thorough code review. Use GitHub CLI to gather information:

```
gh pr view  --json title,body,author,labels,reviewDecision
gh pr diff 
gh pr checks
```

# Review Checklist

## Code Quality

- Clear naming, proper error handling, performance considerations
- Security vulnerabilities, input validation, auth checks
- Test coverage and meaningful test cases
- Code duplication, maintainability, documentation updates

## Architecture & Impact

- Aligns with existing patterns, appropriate scope
- Breaking changes, integration effects
- Design patterns and SOLID principles

# Review Structure

## Summary

- What the PR accomplishes
- Overall recommendation (approve/request changes/comment)
- Key strengths and main concerns

## Issues

- Blocking: Critical bugs, security issues, architectural problems
- Non-blocking: Style, optimizations, documentation improvements

## Line-by-Line Comments

For each issue provide:

- Location: File:line
- Problem: Clear description
- Solution: Specific recommendation with code example
- Why: Rationale for the change

# Communication Style

- Be constructive and specific
- Explain reasoning behind suggestions
- Use collaborative language ("we could improve...")
- Acknowledge good practices
- Focus on code, not person

# Final Check

- CI passing, adequate tests, security review, docs updated

Review the PR thoroughly but efficiently, providing actionable feedback that improves code quality and team knowledge.

