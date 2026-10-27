# PostHog source

Reads a PostHog project through the private API, authenticated with a personal API key sent as a bearer token.

## Streams

| Stream | Endpoint | Sync modes |
|---|---|---|
| `events` | `/api/projects/{project_id}/query/` (HogQL) | full refresh, incremental |
| `persons` | `/api/projects/{project_id}/persons/` | full refresh |
| `event_definitions` | `/api/projects/{project_id}/event_definitions/` | full refresh |
| `property_definitions` | `/api/projects/{project_id}/property_definitions/` | full refresh |
| `cohorts` | `/api/projects/{project_id}/cohorts/` | full refresh |

## Configuration

| Field | Default | Notes |
|---|---|---|
| `api_key` | required | Personal API key. |
| `project_id` | required | Numeric project id. |
| `base_url` | `https://us.posthog.com` | Use `https://eu.posthog.com` for EU Cloud, or your self-hosted URL. |
| `start_date` | required for `events` | ISO-8601 start of the events backfill, e.g. `2024-01-01T00:00:00Z`. |
| `batch_size` | `10000` | Rows per events query and per persons page. A page must fit the destination buffer, so lower it for wide events. |
| `metadata_page_size` | `100` | Page size for the definitions and cohorts endpoints. |
| `events_window_minutes` | `60` | Width of the time window each events query scans. |
| `request_timeout_seconds` | `120` | Read timeout per request. |

## How the events stream works

PostHog discourages the `/events/` REST endpoint for bulk extraction, so events are read with HogQL through `/query/`.

- A run fixes its upper bound when it starts, then walks half-open `[start, stop)` time windows of `events_window_minutes`. Incremental runs start from the previous successful run, so successive runs cover disjoint ranges.
- Inside a window, rows are paged with an ascending `(timestamp, uuid)` cursor rather than `OFFSET`, which keeps paging stable when several events share a timestamp.
- The query API caps execution time (about 10 seconds) and rate (240/minute, 2400/hour, 3 concurrent). When a window comes back as an asynchronous result, times out, or fails with a retryable status, the window is halved and retried.
- `properties` columns returned as JSON strings are parsed into objects, and lone UTF-16 surrogates are replaced so the records stay serialisable.

`persons` is mutable state, so it is a full-refresh snapshot; dedupe or merge it downstream on `id`. Persons only exist for identified profiles, so anonymous `distinct_id`s in `events` may have no matching person.

For very large projects, PostHog's native batch exports are an alternative to this read-only extractor.

## References

- [PostHog API overview](https://posthog.com/docs/api)
- [Query API](https://posthog.com/docs/api/queries)
- [Batch exports](https://posthog.com/docs/cdp/batch-exports)
