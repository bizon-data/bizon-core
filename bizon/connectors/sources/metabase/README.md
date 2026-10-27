# Metabase Source

Bizon source connector for the Metabase API. Fetches users, cards (questions), dashboards, and permission groups.

## Streams

| Stream | Description |
|--------|-------------|
| `users` | User accounts (paginated) |
| `cards` | Saved questions/cards |
| `dashboards` | Dashboard definitions |
| `permissions_groups` | Permission group definitions |

## Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `base_url` | `str` | Yes | Base URL of your Metabase instance (e.g., `https://metabase.example.com`) |
| `page_size` | `int` | No | Records per page for users endpoint (default: 50, max: 100) |

## Example Pipeline Config

```yaml
name: metabase cards to logger

source:
  name: metabase
  stream: cards
  base_url: https://metabase.example.com
  authentication:
    type: api_key
    params:
      token: <YOUR_API_KEY>

destination:
  name: logger
  config:
    dummy: dummy
```

## Notes

- Uses the `x-api-key` header (not a Bearer token)
- Only the `users` stream is paginated; the others return all records in one response
