# Tropic Source

Bizon source connector for Tropic contracts. It uses the read-only contracts endpoint and paginates through `GET /api/v1/contracts`.

## Streams

| Stream | Description |
|--------|-------------|
| `contracts` | Contract records returned by Tropic's contracts API |

## Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `api_key` | `str` | Yes | Tropic API key |
| `base_url` | `str` | No | Defaults to `https://app.tropicapp.io` |
| `page_size` | `int` | No | Contracts per page, defaults to `50` |
| `request_timeout_seconds` | `int` | No | Read timeout for Tropic requests, defaults to `120` |

## Example Pipeline Config

```yaml
name: tropic contracts to logger

source:
  name: tropic
  stream: contracts
  api_key: <YOUR_API_KEY>
  page_size: 50

destination:
  name: logger
  config:
    dummy: dummy
```

## Notes

- Uses bearer token authentication.
- The working API host is `https://app.tropicapp.io`.
- Pagination follows Tropic's `pagination_metadata.next` link.
