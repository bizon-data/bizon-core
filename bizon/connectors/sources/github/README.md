# GitHub Source

Fetches developer activity data from the GitHub REST and GraphQL APIs using **GitHub App authentication**, which has higher rate limits than a personal access token (5,000–12,500 req/hr per installation).

## Authentication

Uses GitHub App auth (JWT + installation access tokens):
1. Generates a JWT signed with the app's private key
2. Auto-discovers all installations via `GET /app/installations`
3. Exchanges the JWT for short-lived installation tokens per installation
4. Caches tokens and refreshes them at the 50-minute mark (60 min expiry)

No `installation_id` is required — the source discovers all installations automatically.

## Streams

The `GitHubStreams` enum in `src/source.py` is authoritative (it also covers `members`, `pull_request_comments`, `repo_content`, `organization_teams`, `organization_team_members`, `pull_request_timeline` and the comment-reaction streams). The main ones:

| Stream | Endpoint | ID Field | Notes |
|--------|----------|----------|-------|
| `repositories` | `GET /installation/repositories` | `id` | All repos across all installations |
| `pull_requests` | GraphQL `repository.pullRequests` | `id` | Multi-repo, all states, REST-shaped records |
| `pull_request_reviews` | GraphQL `pullRequest.reviews` | `id` | Nested: iterates PRs then reviews |
| `pull_request_review_comments` | GraphQL `pullRequest.reviews.comments` | `id` | Inline code review comments on PR diffs |
| `commits` | `GET /repos/{repo}/commits` | `sha` | Multi-repo |
| `deployments` | GraphQL `repository.deployments` | `id` | Multi-repo |
| `workflow_runs` | `GET /repos/{repo}/actions/runs` | `id` | Multi-repo, wrapped response |
| `workflow_jobs` | `GET /repos/{repo}/actions/runs/{run_id}/jobs` | `id` | Nested under runs: lists runs per repo/page, then resolves each run's jobs. `filter=all` captures every run attempt, not just the latest. |

## Incremental limitations

- **`workflow_jobs`**: incremental sync lists workflow runs by `created >= last_run` (same
  filter as `workflow_runs`) and resolves jobs from those runs. A run created *before* the
  cutoff whose jobs complete *after* it won't be re-ingested by a later incremental sync,
  since job completion time isn't part of the run-listing filter. Re-runs create a new run
  (new `created_at`), so they ARE captured going forward; `filter=all` on the jobs endpoint
  also captures prior attempts of a re-listed run.
- `repositories`, `members`, `organization_teams` and `organization_team_members` always do
  a full read, even in incremental mode.
- Each incremental run re-scans `lookback_days` (default 2) before the watermark, so expect
  duplicate rows and deduplicate downstream.

## Resilience

REST requests retry rate limits, connection drops, timeouts, and GitHub 5xx responses.
When the underlying `HTTPAdapter` exhausts its 5xx attempts, it raises `RetryError`; the
source treats that as retryable too, so the outer exponential backoff gets a chance to
recover instead of failing immediately. An error that remains after those retries is still
raised, so a failed repo fails the run instead of advancing the incremental watermark past
data that was never fetched.

GraphQL pages that keep failing with a 5xx or a query-cost error are retried at a smaller
page size on the same cursor. `commits` returns full commit objects, and large commit pages
can fail with HTTP 500, so `per_page: 50` is a reasonable setting for that stream.

## Config

```yaml
source:
  name: github
  stream: repositories
  app_id: <YOUR_GITHUB_APP_ID>
  private_key: <YOUR_GITHUB_APP_PRIVATE_KEY_PEM>   # newlines may be escaped as \n
  # Optional:
  # repos:                         # Explicit repo list (otherwise auto-discovers)
  #   - owner/repo1
  #   - owner/repo2
  # per_page: 100                  # Results per page (max 100)
  # start_date: "2024-01-01"       # Only fetch data after this date
  # concurrent_repos: 5            # Repos fetched in parallel
```

See `config/` for complete examples.

## Setup

1. [Create a GitHub App](https://docs.github.com/en/apps/creating-github-apps) with the required permissions:
   - **Repository**: Contents (read), Pull requests (read), Deployments (read), Actions (read), Metadata (read)
   - **Organization**: Members (read), for the `members` and team streams
2. Generate a private key for the app
3. Install the app on the target orgs/repos

## Dependencies

Install with `pip install 'bizon[github]'`:

- `PyJWT` — JWT generation for GitHub App auth
- `cryptography` — RS256 signing for the JWT
