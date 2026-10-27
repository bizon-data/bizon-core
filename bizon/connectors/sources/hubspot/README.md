# HubSpot source

Syncs HubSpot CRM objects, engagements, associations and the legacy v1 email-event and form-submission APIs.
Authenticates with OAuth (`authentication.type: oauth`) or a private-app token (`api_key`).

## Streams

| Stream | Sync | Notes |
|--------|------|-------|
| `contacts`, `companies`, `deals` | full refresh, incremental | v3 list API; incremental goes through search on the last-modified property (`cursor_field` overrides it) |
| `custom_object` | full refresh, incremental | Same as above for any object set by `object_type` (e.g. `2-1234567`) |
| `contacts_search`, `companies_search`, `deals_search` | full refresh, incremental | Search only, with `start_date`, `page_size` (max 200) and `full_scan` |
| `engagements_emails` | full refresh, incremental | Search on `hs_lastmodifieddate`, plus associated contact/company/deal/ticket ids |
| `engagements_notes`, `engagements_calls`, `engagements_tasks`, `engagements_meetings` | full refresh | Inline contact/company/deal associations, paged past HubSpot's 100 per type |
| `line_items`, `deal_pipelines`, `properties_contacts`, `properties_companies`, `properties_deals` | full refresh | |
| `owners` | full refresh | Active then archived owners; the record id is `<id>:<active\|archived>` |
| `archived_contacts`, `archived_deals`, `archived_companies` | full refresh, incremental | Whole archived list filtered on `archivedAt`; use incremental to accumulate history |
| `association_deals`, `association_new_contacts` | full refresh | v4 associations, one row per (id, associated id, association type) |
| `contacts_merged_audit` | full refresh | Merged contacts with their v1 merge-audit trail |
| `email_events`, `form_submissions` | full refresh, incremental | v1 APIs, re-reading the last 6 hours on each incremental run |

Every search-based stream pages by `hs_object_id` rather than the search `after` cursor, so it is not capped at
10,000 results, and bounds its window by the job's start time so consecutive runs tile.

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `properties.strategy` | `all` | `all` fetches the whole property catalog; `selected` sends `properties.selected_properties`, which must exist |
| `start_date` | unset | Lower bound for a full refresh or a first incremental run, on streams that take it. Unset fetches everything |
| `max_requests_per_second` | unset | Client-side pacing. Search endpoints are rate limited per account without rate-limit headers |
| `http` | 50 retries, backoff 1, `(10, 60)` timeout | bizon's HTTP policy (`bizon.source.config.HttpConfig`) |
| `authentication.params.expiry_skew_seconds` | `120` | How long before expiry the OAuth token is refreshed |

A request rejected with `401 EXPIRED_AUTHENTICATION` forces a token refresh and is replayed once.

See `config/` for examples.
