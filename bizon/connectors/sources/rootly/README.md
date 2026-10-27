# Rootly Source

Bizon source connector for the Rootly API. Fetches incident management data including incidents, post-mortems, teams, and services.

## Streams

| Stream | Description |
|--------|-------------|
| `incidents` | All incidents with related data (statuses, causes, services, etc.) |
| `post_mortems` | Post-mortem reports with retrospective steps |
| `teams` | Team definitions |
| `services` | Service catalog |
| `users` | User accounts |
| `incident_action_items` | Action items from incidents |

## Configuration

No additional configuration fields required beyond authentication.

## Example Pipeline Config

```yaml
name: rootly incidents to logger

source:
  name: rootly
  stream: incidents
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

- Incidents include related data: sub_statuses, causes, subscribers, services, groups, action_items, incident_post_mortem, feedbacks
- Post-mortems fetch and embed incident retrospective steps, one request per step
- `incident_action_items` walks incidents one at a time and fetches each incident's action items
- Uses page-based pagination
