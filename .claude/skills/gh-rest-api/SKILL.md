---
name: gh-rest-api
description: Enforces using `gh api` REST endpoints instead of `gh` CLI subcommands for ALL GitHub interactions (reads and writes) in the east-guard repo. Use this skill whenever you're about to run any `gh pr`, `gh issue`, `gh release`, `gh repo`, or `gh label` command — even for listing or viewing. Also use when the user asks you to check PRs, create issues, update labels, post comments, or any other GitHub operation. This applies to the f-rustrated/EastGuard repository specifically.
---

# GitHub REST API Enforcement — east-guard

## The rule

Use `gh api` for every GitHub interaction. Never use `gh pr`, `gh issue`, `gh release`, or other convenience subcommands.

The repo is `f-rustrated/EastGuard`.

**CRITICAL: Case-sensitive repo name.** The GitHub REST API requires exact casing: `EastGuard`, NOT `east-guard`, `eastguard`, or `Eastguard`. A wrong case returns 404. Always use `f-rustrated/EastGuard` exactly.

## Common operations reference

### Pull Requests

**List PRs:**
```bash
gh api repos/f-rustrated/EastGuard/pulls --method GET -f state=open
```

**View a PR:**
```bash
gh api repos/f-rustrated/EastGuard/pulls/NUMBER
```

**Create a PR:**
```bash
gh api repos/f-rustrated/EastGuard/pulls --method POST \
  -f title='PR title' \
  -f body='PR description' \
  -f head='branch-name' \
  -f base='main'
```

**Update a PR (title, body, state):**
```bash
gh api repos/f-rustrated/EastGuard/pulls/NUMBER --method PATCH \
  -f body='Updated description'
```

**List PR comments:**
```bash
gh api repos/f-rustrated/EastGuard/pulls/NUMBER/comments
```

**Add PR comment:**
```bash
gh api repos/f-rustrated/EastGuard/pulls/NUMBER/comments --method POST \
  -f body='Comment text'
```

**List PR reviews:**
```bash
gh api repos/f-rustrated/EastGuard/pulls/NUMBER/reviews
```

**Merge a PR:**
```bash
gh api repos/f-rustrated/EastGuard/pulls/NUMBER/merge --method PUT \
  -f merge_method='squash'
```

**PR diff:**
```bash
gh api repos/f-rustrated/EastGuard/pulls/NUMBER -H "Accept: application/vnd.github.v3.diff"
```

**PR files changed:**
```bash
gh api repos/f-rustrated/EastGuard/pulls/NUMBER/files
```

### Issues

**List issues:**
```bash
gh api repos/f-rustrated/EastGuard/issues --method GET -f state=open
```

**View an issue:**
```bash
gh api repos/f-rustrated/EastGuard/issues/NUMBER
```

**Create an issue:**
```bash
gh api repos/f-rustrated/EastGuard/issues --method POST \
  -f title='Issue title' \
  -f body='Issue description'
```

**Create an issue with labels (or any array field):**
```bash
gh api repos/f-rustrated/EastGuard/issues --method POST \
  --input - <<< '{"title":"Issue title","body":"Issue description","labels":["bug","priority"]}'
```

> **`-f` sends strings only.** Fields like `labels` and `assignees` require JSON arrays. Use `--input -` with a full JSON body whenever the payload includes array or non-string fields. Never use `-f labels='["..."]'` — GitHub rejects the string with HTTP 422.

**Update an issue:**
```bash
gh api repos/f-rustrated/EastGuard/issues/NUMBER --method PATCH \
  -f body='Updated body'
```

**Add issue comment:**
```bash
gh api repos/f-rustrated/EastGuard/issues/NUMBER/comments --method POST \
  -f body='Comment text'
```

**Close an issue:**
```bash
gh api repos/f-rustrated/EastGuard/issues/NUMBER --method PATCH \
  -f state='closed'
```

### Labels

**List labels:**
```bash
gh api repos/f-rustrated/EastGuard/labels
```

**Add label to issue/PR:**
```bash
gh api repos/f-rustrated/EastGuard/issues/NUMBER/labels --method POST \
  --input - <<< '{"labels":["bug","priority"]}'
```

### Releases

**List releases:**
```bash
gh api repos/f-rustrated/EastGuard/releases
```

**Create release:**
```bash
gh api repos/f-rustrated/EastGuard/releases --method POST \
  -f tag_name='v1.0.0' \
  -f name='Release 1.0.0' \
  -f body='Release notes'
```

### Branches & Refs

**List branches:**
```bash
gh api repos/f-rustrated/EastGuard/branches
```

**Get branch protection:**
```bash
gh api repos/f-rustrated/EastGuard/branches/main/protection
```

### CI / Checks

**List check runs for a ref:**
```bash
gh api repos/f-rustrated/EastGuard/commits/REF/check-runs
```

**List workflow runs:**
```bash
gh api repos/f-rustrated/EastGuard/actions/runs
```

## Pagination

REST API paginates by default (30 items). Use `-f per_page=100` for larger result sets. For full pagination, use `--paginate`:

```bash
gh api repos/f-rustrated/EastGuard/pulls --paginate -f state=all -f per_page=100
```

## JSON filtering

Use `jq`-style queries with `--jq`:

```bash
gh api repos/f-rustrated/EastGuard/pulls --jq '.[].title'
gh api repos/f-rustrated/EastGuard/pulls/42 --jq '.body'
```

## Edge cases

- **Array fields (`labels`, `assignees`, etc.)**: `-f` always sends strings. Use `--input -` with a JSON body for any payload containing arrays or non-string types. Never do `-f labels='["bug"]'` — GitHub returns HTTP 422.
- **Creating PRs with long bodies**: Use `--input -` with heredoc or pipe from file instead of `-f body=` to avoid shell escaping issues.
- **Binary content / assets**: Use `gh api` with `--input` for upload endpoints.
- **Rate limiting**: REST API has separate rate limits from GraphQL. Check `X-RateLimit-Remaining` header if doing bulk operations.
