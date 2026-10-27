# Noticeable Source

Bizon source connector for the Noticeable GraphQL API. Fetches publications, email events, and subscriptions.

## Streams

| Stream | Description |
|--------|-------------|
| `email_opened_events` | Email open tracking events |
| `publications` | Published changelog entries |
| `publication_comments` | Comments on publications |
| `email_subscriptions` | Email subscriber list |

## Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `project_id` | `str` | Yes | Noticeable project ID |

## Example Pipeline Config

```yaml
name: noticeable publications to logger

source:
  name: noticeable
  stream: publications
  project_id: <YOUR_PROJECT_ID>
  authentication:
    type: api_key
    params:
      auth_method: Apikey
      token: <YOUR_API_KEY>

destination:
  name: logger
  config:
    dummy: dummy
```

## Notes

- Uses GraphQL API at `https://api.noticeable.io/graphql`
- Supports cursor-based pagination; on a persistent transient error the page size shrinks and the same cursor is retried
- API key authentication via the `Apikey` auth method (`Authorization: Apikey <token>`)
- `email_opened_events` has no date filter, so every run walks the full event history; on a large project this takes hours
- `email_subscriptions` records get a random id, since the API exposes no stable one
