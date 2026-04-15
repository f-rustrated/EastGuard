---
name: update-pr
description: >
  Update a PR description by comparing the PR branch against main. Generates a structured
  summary from the diff. Use when the user says "add description to PR", "update PR body",
  "describe this PR", or after creating a PR that needs a body.
---

# Update PR Description

Generate a structured PR description by diffing the PR branch against main.

## Step 1: Identify the PR

**IMPORTANT: All `gh` commands on this repo must use REST API (`gh api`). GraphQL-based commands (`gh pr list`, `gh pr view`, `gh pr edit`) fail with a Projects Classic deprecation error.**

```sh
# Get owner/repo
REPO=$(gh repo view --json nameWithOwner --jq '.nameWithOwner')

# If user gave a PR number, use it. Otherwise find the current branch's PR:
BRANCH=$(git branch --show-current)
gh api "repos/$REPO/pulls?head=$(echo $REPO | cut -d/ -f1):$BRANCH&state=open" --jq '.[0] | {number, title}'
```

## Step 2: Gather changes

```sh
# Get the base branch from the PR
BASE=$(gh api "repos/$REPO/pulls/$PR_NUMBER" --jq '.base.ref')

# Full diff summary
git diff $BASE...HEAD --stat

# Detailed diff for understanding changes
git diff $BASE...HEAD

# Commit messages for context
git log $BASE...HEAD --oneline
```

## Step 3: Write the description

Structure:
```
## Summary
<1-3 sentences: what this PR does and why>

Closes #X, #Y, #Z

### <Change group 1> (#issue)
- bullet points of what changed

### <Change group 2> (#issue)
- bullet points

## Test plan
- [x] tests pass
- [x] clippy clean
- [ ] manual verification steps if applicable
```

## Step 4: Update via REST API

**IMPORTANT: Do NOT use `gh pr edit --body`. It fails with a GraphQL Projects Classic error on this repo.**

Always use the REST API:

```sh
gh api "repos/$REPO/pulls/$PR_NUMBER" \
  --method PATCH \
  --field body='...' \
  --jq '.html_url'
```

## Notes

- Group related changes by issue number when multiple issues are addressed
- Keep bullets concise — the diff speaks for itself
- Include `Closes #N` to auto-close linked issues on merge
- Use `->` instead of `→` in the body (REST API field escaping)
