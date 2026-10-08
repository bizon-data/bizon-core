# AI Connector Generation Guide

This guide enables AI agents to create production-ready Bizon source connectors from arbitrary API documentation.

## Overview

A Bizon source connector extracts data from an API and produces `SourceRecord` objects. Each connector consists of:

```
bizon/connectors/sources/{source_name}/
├── config/
│   ├── {source_name}.example.yml      # Example config for testing
│   └── oauth.example.yml              # (Optional) Per auth-type examples
└── src/
    ├── __init__.py                    # Empty file
    ├── config.py                      # Configuration model (optional, can be in source.py)
    └── source.py                      # Main implementation
```

## Step 1: Extract Information from API Docs

Before writing code, identify these 7 pieces of information from the API documentation:

### 1.1 Base URL
- **What to find**: The API's base URL
- **Examples**: `https://api.notion.com/v1`, `https://api.hubspot.com/crm/v3`
- **Look for**: "Base URL", "API endpoint", "Host"

### 1.2 Authentication Type

| API Doc Says | Bizon Auth Type | Config |
|--------------|-----------------|--------|
| "API Key in header" | `API_KEY` or `BEARER` | `token`, `auth_header` |
| "Bearer token" | `API_KEY` or `BEARER` | `token` |
| "OAuth 2.0" | `OAUTH` | `client_id`, `client_secret`, `refresh_token`, `token_refresh_endpoint` |
| "Basic auth" | `BASIC` | `username`, `password` |
| "Cookie-based" | `COOKIES` | `cookies` dict |
| "No auth" / Public API | Return `None` | N/A |

**Decision tree:**
```
Does API require auth?
├── No → return None from get_authenticator()
└── Yes → What kind?
    ├── API Key/Bearer token → API_KEY or BEARER
    ├── OAuth with refresh tokens → OAUTH
    ├── Username/password → BASIC
    └── Session cookies → COOKIES
```

### 1.3 List Endpoint
- **What to find**: The endpoint that returns a list of records
- **Examples**: `GET /users`, `GET /contacts`, `POST /databases/{id}/query`
- **Look for**: "List all", "Get all", "Query", endpoints returning arrays

### 1.4 Pagination Pattern

| API Response Contains | Pattern | Implementation |
|----------------------|---------|----------------|
| `next_cursor` / `cursor` / `start_cursor` | **Cursor-based** | Pass cursor as param on next request |
| `next` URL | **URL-based** | Use URL directly for next request |
| `offset` + `limit` / `total` | **Offset-based** | Increment offset by limit each request |
| `page` number | **Page-based** | Increment page number |
| Nothing / single response | **None** | Return empty `next_pagination` |

**How to detect end of pagination:**
- `has_more: false`
- `next_cursor: null`
- `next: null`
- Empty `results` array
- `offset >= total`

### 1.5 Response Structure
- **What to find**: Where records are in the JSON response
- **Common keys**: `data`, `results`, `items`, `records`, `objects`
- **Example**: `{"results": [...], "has_more": true}` → records are in `results`

### 1.6 Record ID Field
- **What to find**: The unique identifier for each record
- **Common fields**: `id`, `uuid`, `_id`, `name`, `key`
- **Example**: If records have `{"id": "123", "name": "John"}` → use `id`

### 1.7 Rate Limits
- **What to find**: Requests per second/minute, retry behavior
- **Look for**: "Rate limits", "Throttling", "429 responses"
- **Default if not specified**: 10 retries, exponential backoff, retry on 429/500/502/503/504

## Step 2: Generate Connector Code

### Template: config.py (Optional - can be in source.py)

```python
from enum import Enum
from typing import List, Optional, Any, Dict
from pydantic import Field
from bizon.source.config import SourceConfig


# Define available streams (one per resource type)
class {{SOURCE_NAME}}Streams(str, Enum):
    {{STREAM_NAME}} = "{{stream_name}}"
    # Add more streams as needed


class {{SOURCE_NAME}}SourceConfig(SourceConfig):
    stream: {{SOURCE_NAME}}Streams

    # Add source-specific config options
    # Example: page_size for pagination
    page_size: int = Field(
        default={{DEFAULT_PAGE_SIZE}},  # Use API's default or max
        ge=1,
        le={{MAX_PAGE_SIZE}},
        description="Number of results per page"
    )

    # Example: additional IDs to filter
    # resource_ids: List[str] = Field(
    #     default_factory=list,
    #     description="List of resource IDs to fetch"
    # )
```

### Template: source.py

```python
from typing import Any, List, Optional, Tuple

from requests import Session
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from urllib3.util.retry import Retry

from bizon.source.auth.builder import AuthBuilder
from bizon.source.auth.config import AuthType
from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

# If config is in separate file:
# from .config import {{SOURCE_NAME}}SourceConfig, {{SOURCE_NAME}}Streams

# ============================================================
# CONFIGURATION (can be in separate config.py)
# ============================================================

from enum import Enum
from pydantic import Field

class {{SOURCE_NAME}}Streams(str, Enum):
    {{STREAM_NAME}} = "{{stream_name}}"

class {{SOURCE_NAME}}SourceConfig(SourceConfig):
    stream: {{SOURCE_NAME}}Streams
    page_size: int = Field(default={{DEFAULT_PAGE_SIZE}}, ge=1, le={{MAX_PAGE_SIZE}})

# ============================================================
# CONSTANTS
# ============================================================

BASE_URL = "{{BASE_URL}}"

# ============================================================
# SOURCE IMPLEMENTATION
# ============================================================

class {{SOURCE_NAME}}Source(AbstractSource):

    def __init__(self, config: {{SOURCE_NAME}}SourceConfig):
        super().__init__(config)
        self.config: {{SOURCE_NAME}}SourceConfig = config

    # ----------------------------------------------------------
    # SESSION CONFIGURATION (Rate limiting & retries)
    # ----------------------------------------------------------

    def get_session(self) -> Session:
        """Configure session with retry logic and custom headers."""
        session = Session()

        # Retry configuration
        retries = Retry(
            total={{RETRY_COUNT}},                    # Number of retries (10-50 typical)
            backoff_factor={{BACKOFF_FACTOR}},        # Exponential backoff (1-2 typical)
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            respect_retry_after_header=True,          # Honor Retry-After header
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))

        # Custom headers (if API requires)
        # session.headers.update({
        #     "{{CUSTOM_HEADER}}": "{{HEADER_VALUE}}",
        # })

        return session

    # ----------------------------------------------------------
    # AUTHENTICATION
    # ----------------------------------------------------------

    def get_authenticator(self) -> AuthBase:
        """Return the appropriate authenticator based on config."""
        # OPTION 1: No authentication (public API)
        # return None

        # OPTION 2: API Key / Bearer token. Sends `Authorization: Bearer <token>` by default; for a
        # bare key in a custom header, use TokenAuthParams(token=..., auth_header="x-api-key", auth_method="")
        if self.config.authentication.type in [AuthType.API_KEY, AuthType.BEARER]:
            return AuthBuilder.token(params=self.config.authentication.params)

        # OPTION 3: OAuth 2.0. Tokens are refreshed shortly before they expire (10% of their
        # lifetime, at most 60s; set `expiry_skew_seconds` to change it).
        # if self.config.authentication.type == AuthType.OAUTH:
        #     return AuthBuilder.oauth2(params=self.config.authentication.params)

        # OPTION 4: Basic auth
        # if self.config.authentication.type == AuthType.BASIC:
        #     return AuthBuilder.basic(params=self.config.authentication.params)

        return None

    # ----------------------------------------------------------
    # REQUIRED STATIC METHODS
    # ----------------------------------------------------------

    @staticmethod
    def streams() -> List[str]:
        """Return list of available stream names."""
        return [item.value for item in {{SOURCE_NAME}}Streams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        """Return the config class for this source."""
        return {{SOURCE_NAME}}SourceConfig

    # ----------------------------------------------------------
    # CONNECTION CHECK
    # ----------------------------------------------------------

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        """Test API connectivity. Return (success, error_message)."""
        try:
            # Make a simple API call to verify connectivity
            response = self.session.get(f"{BASE_URL}/{{TEST_ENDPOINT}}")
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        """Return total record count if API provides it, else None."""
        # If API returns total count in list response:
        # response = self.session.get(f"{BASE_URL}/{{STREAM_ENDPOINT}}")
        # return response.json().get("total")
        return None

    # ----------------------------------------------------------
    # DATA FETCHING
    # ----------------------------------------------------------

    def get_{{stream_name}}(self, pagination: dict = None) -> SourceIteration:
        """Fetch {{stream_name}} records with pagination."""

        # Build request URL/params
        url = f"{BASE_URL}/{{STREAM_ENDPOINT}}"
        params = {"{{PAGE_SIZE_PARAM}}": self.config.page_size}

        # PAGINATION PATTERN 1: Cursor-based
        if pagination and pagination.get("{{CURSOR_KEY}}"):
            params["{{CURSOR_PARAM}}"] = pagination["{{CURSOR_KEY}}"]

        # PAGINATION PATTERN 2: URL-based
        # if pagination and pagination.get("next"):
        #     url = pagination["next"]
        #     params = {}  # URL already has params

        # PAGINATION PATTERN 3: Offset-based
        # offset = pagination.get("offset", 0) if pagination else 0
        # params["offset"] = offset
        # params["limit"] = self.config.page_size

        # Make request
        response = self.session.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        # Extract records from response
        records = [
            SourceRecord(
                id=record["{{RECORD_ID_FIELD}}"],
                data=record,
            )
            for record in data.get("{{RESULTS_KEY}}", [])
        ]

        # Determine next pagination
        # CURSOR-BASED:
        if data.get("{{HAS_MORE_KEY}}"):
            next_pagination = {"{{CURSOR_KEY}}": data.get("{{NEXT_CURSOR_KEY}}")}
        else:
            next_pagination = {}

        # URL-BASED:
        # next_pagination = {"next": data.get("next")} if data.get("next") else {}

        # OFFSET-BASED:
        # if offset + len(records) < data.get("total", 0):
        #     next_pagination = {"offset": offset + self.config.page_size}
        # else:
        #     next_pagination = {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    # ----------------------------------------------------------
    # MAIN DISPATCH
    # ----------------------------------------------------------

    def get(self, pagination: dict = None) -> SourceIteration:
        """Main entry point - dispatch to stream-specific method."""
        if self.config.stream == {{SOURCE_NAME}}Streams.{{STREAM_NAME}}:
            return self.get_{{stream_name}}(pagination)

        raise NotImplementedError(f"Stream {self.config.stream} not implemented")
```

### Template: Example YAML Config

```yaml
name: {{source_name}}_to_destination

source:
  name: {{source_name}}
  stream: {{stream_name}}
  authentication:
    type: {{auth_type}}  # api_key, bearer, oauth, basic, or remove if no auth
    params:
      token: "YOUR_API_TOKEN"
      # For oauth:
      # client_id: "YOUR_CLIENT_ID"
      # client_secret: "YOUR_CLIENT_SECRET"
      # refresh_token: "YOUR_REFRESH_TOKEN"
      # token_refresh_endpoint: "https://api.example.com/oauth/token"

destination:
  name: logger  # For testing
  config:
    dummy: bizon

engine:
  backend:
    type: sqlite_in_memory
    config:
      database: not_used
      schema: not_used
```

## Step 3: Common Patterns Reference

### Pattern A: Cursor-based Pagination (Most Common)

```python
def get_records(self, pagination: dict = None) -> SourceIteration:
    params = {"page_size": self.config.page_size}

    if pagination and pagination.get("cursor"):
        params["cursor"] = pagination["cursor"]

    response = self.session.get(f"{BASE_URL}/records", params=params)
    data = response.json()

    records = [SourceRecord(id=r["id"], data=r) for r in data["results"]]

    next_pagination = {"cursor": data["next_cursor"]} if data.get("has_more") else {}

    return SourceIteration(records=records, next_pagination=next_pagination)
```

### Pattern B: URL-based Pagination

```python
def get_records(self, pagination: dict = None) -> SourceIteration:
    url = pagination.get("next") if pagination else f"{BASE_URL}/records"

    response = self.session.get(url)
    data = response.json()

    records = [SourceRecord(id=r["id"], data=r) for r in data["results"]]

    next_pagination = {"next": data["next"]} if data.get("next") else {}

    return SourceIteration(records=records, next_pagination=next_pagination)
```

### Pattern C: Offset-based Pagination

```python
def get_records(self, pagination: dict = None) -> SourceIteration:
    offset = pagination.get("offset", 0) if pagination else 0

    response = self.session.get(
        f"{BASE_URL}/records",
        params={"offset": offset, "limit": self.config.page_size}
    )
    data = response.json()

    records = [SourceRecord(id=r["id"], data=r) for r in data["results"]]

    if offset + len(records) < data.get("total", 0):
        next_pagination = {"offset": offset + self.config.page_size}
    else:
        next_pagination = {}

    return SourceIteration(records=records, next_pagination=next_pagination)
```

### Pattern D: No Pagination (Single Response)

```python
def get_records(self, pagination: dict = None) -> SourceIteration:
    response = self.session.get(f"{BASE_URL}/records")
    data = response.json()

    records = [SourceRecord(id=r["id"], data=r) for r in data["results"]]

    return SourceIteration(records=records, next_pagination={})
```

### Pattern E: POST-based Queries (e.g., GraphQL, Notion)

```python
def get_records(self, pagination: dict = None) -> SourceIteration:
    payload = {"page_size": self.config.page_size}

    if pagination and pagination.get("cursor"):
        payload["start_cursor"] = pagination["cursor"]

    response = self.session.post(f"{BASE_URL}/query", json=payload)
    data = response.json()

    records = [SourceRecord(id=r["id"], data=r) for r in data["results"]]

    next_pagination = {"cursor": data["next_cursor"]} if data.get("has_more") else {}

    return SourceIteration(records=records, next_pagination=next_pagination)
```

## Step 4: Validation Checklist

After generating the connector, verify:

### Code Structure
- [ ] `__init__.py` exists and is empty
- [ ] Config class extends `SourceConfig`
- [ ] Source class extends `AbstractSource`
- [ ] All required methods implemented: `streams()`, `get_config_class()`, `get_authenticator()`, `check_connection()`, `get_total_records_count()`, `get()`

### Pagination
- [ ] `next_pagination` returns empty dict `{}` when no more pages
- [ ] Pagination state is correctly passed and used
- [ ] End condition properly detected

### Records
- [ ] Each record has a unique `id` field
- [ ] `data` contains the full record as a dict
- [ ] Records are extracted from correct response key

### Error Handling
- [ ] `check_connection()` returns `(True, None)` on success, `(False, error_msg)` on failure
- [ ] HTTP errors are handled (via session retries or explicit try/catch)
- [ ] Rate limiting is configured with appropriate retries

### Testing
- [ ] `uv run pytest` passes
- [ ] Manual test with `uv run bizon run config.yml` works
- [ ] Pagination exhausts all records correctly

## Step 5: Generate Example Config Files

Create example YAML config files in the `config/` directory to help users get started quickly.

### File Naming Conventions

| Pattern | When to Use |
|---------|-------------|
| `{source_name}.example.yml` | Default example (most common auth type) |
| `api_key.example.yml` | When API Key auth is primary method |
| `oauth.example.yml` | When OAuth is available |
| `{source_name}_{stream}_to_logger.example.yml` | Stream-specific examples |

### Placeholder Conventions

| Placeholder Style | When to Use | Example |
|-------------------|-------------|---------|
| `<YOUR_API_KEY>` | Required user input | `<YOUR_API_KEY>` |
| `<MY_CLIENT_ID>` | Alternative style | `<MY_CLIENT_ID>` |
| `secret_xxx...xxx` | Token format hint | `secret_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx` |
| `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` | UUID format hint | For database IDs, page IDs |
| Actual example values | Non-sensitive defaults | `page_size: 100` |

### Template: API Key Authentication

```yaml
# {{SOURCE_DISPLAY_NAME}} Source Configuration
# Authentication: API Key
#
# To get your API key:
# 1. Go to {{API_KEY_INSTRUCTIONS_URL}}
# 2. {{STEP_BY_STEP_INSTRUCTIONS}}

name: {{source_name}}_{{stream_name}}_to_logger

source:
  name: {{source_name}}
  stream: {{stream_name}}  # Options: {{AVAILABLE_STREAMS}}
  authentication:
    type: api_key
    params:
      token: <YOUR_API_KEY>

  # Source-specific options (uncomment and modify as needed)
  # page_size: {{DEFAULT_PAGE_SIZE}}

destination:
  name: logger  # Use 'logger' for testing, prints records to console
  config:
    dummy: bizon

# Optional: Use in-memory backend for testing (no persistence)
# engine:
#   backend:
#     type: sqlite_in_memory
#     config:
#       database: not_used
#       schema: not_used
```

### Template: OAuth Authentication

```yaml
# {{SOURCE_DISPLAY_NAME}} Source Configuration
# Authentication: OAuth 2.0
#
# To set up OAuth:
# 1. Create an app at {{OAUTH_APP_URL}}
# 2. Configure redirect URI: {{REDIRECT_URI}}
# 3. Complete OAuth flow to get refresh token

name: {{source_name}}_{{stream_name}}_to_logger

source:
  name: {{source_name}}
  stream: {{stream_name}}  # Options: {{AVAILABLE_STREAMS}}
  authentication:
    type: oauth
    params:
      token_refresh_endpoint: {{TOKEN_REFRESH_ENDPOINT}}
      client_id: <YOUR_CLIENT_ID>
      client_secret: <YOUR_CLIENT_SECRET>
      refresh_token: <YOUR_REFRESH_TOKEN>

  # Source-specific options
  # page_size: {{DEFAULT_PAGE_SIZE}}

destination:
  name: logger
  config:
    dummy: bizon
```

### Template: No Authentication (Public API)

```yaml
# {{SOURCE_DISPLAY_NAME}} Source Configuration
# Public API - No authentication required

name: {{source_name}}_{{stream_name}}_to_logger

source:
  name: {{source_name}}
  stream: {{stream_name}}  # Options: {{AVAILABLE_STREAMS}}

  # Source-specific options
  # page_size: {{DEFAULT_PAGE_SIZE}}

destination:
  name: logger
  config:
    dummy: bizon
```

### Template: With Resource IDs (Notion-style)

```yaml
# {{SOURCE_DISPLAY_NAME}} Source Configuration
# Fetches data from specific resources

name: {{source_name}}_{{stream_name}}_to_logger

source:
  name: {{source_name}}
  stream: {{stream_name}}
  authentication:
    type: api_key
    params:
      token: <YOUR_API_KEY>

  # List of resource IDs to fetch
  # Find the ID in the URL: {{URL_PATTERN_TO_FIND_ID}}
  {{resource_ids_field}}:
    - "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    - "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"

  # Optional settings
  page_size: {{DEFAULT_PAGE_SIZE}}

destination:
  name: logger
  config:
    dummy: bizon
```

### Template: Production Config (BigQuery destination)

```yaml
# {{SOURCE_DISPLAY_NAME}} to BigQuery
# Production configuration example

name: {{source_name}}_{{stream_name}}_to_bigquery

source:
  name: {{source_name}}
  stream: {{stream_name}}
  authentication:
    type: api_key
    params:
      token: <YOUR_API_KEY>  # Or use env var: BIZON_ENV_{{SOURCE_NAME_UPPER}}_TOKEN

destination:
  name: bigquery
  config:
    project_id: <YOUR_GCP_PROJECT>
    dataset_id: {{source_name}}_data
    dataset_location: US

engine:
  backend:
    type: bigquery
    config:
      database: <YOUR_GCP_PROJECT>
      schema: bizon_state
      syncCursorInDBEvery: 10
```

### Config Placeholders Reference

| Placeholder | Description | Example |
|-------------|-------------|---------|
| `{{SOURCE_DISPLAY_NAME}}` | Human-readable name | `Notion`, `HubSpot`, `Stripe` |
| `{{source_name}}` | snake_case connector name | `notion`, `hubspot`, `stripe` |
| `{{stream_name}}` | Stream to sync | `users`, `contacts`, `pages` |
| `{{AVAILABLE_STREAMS}}` | Comma-separated list | `users, databases, pages, blocks` |
| `{{DEFAULT_PAGE_SIZE}}` | API's recommended page size | `100` |
| `{{API_KEY_INSTRUCTIONS_URL}}` | Where to get API key | `https://www.notion.so/my-integrations` |
| `{{TOKEN_REFRESH_ENDPOINT}}` | OAuth token endpoint | `https://api.hubspot.com/oauth/v1/token` |
| `{{resource_ids_field}}` | Field name for IDs | `database_ids`, `workspace_ids` |
| `{{SOURCE_NAME_UPPER}}` | UPPER_CASE for env vars | `NOTION`, `HUBSPOT` |

## Example: Minimal Connector (PokeAPI Style)

A complete minimal connector for a public API without authentication:

```python
# bizon/connectors/sources/example_api/src/source.py

from enum import Enum
from typing import Any, List, Optional, Tuple
from pydantic import Field
from requests.auth import AuthBase
from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.example.com"

class ExampleStreams(str, Enum):
    USERS = "users"

class ExampleSourceConfig(SourceConfig):
    stream: ExampleStreams

class ExampleSource(AbstractSource):
    def __init__(self, config: ExampleSourceConfig):
        super().__init__(config)
        self.config: ExampleSourceConfig = config

    @staticmethod
    def streams() -> List[str]:
        return [item.value for item in ExampleStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return ExampleSourceConfig

    def get_authenticator(self) -> AuthBase:
        return None  # Public API

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            self.session.get(f"{BASE_URL}/users").raise_for_status()
            return True, None
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def get(self, pagination: dict = None) -> SourceIteration:
        url = pagination.get("next") if pagination else f"{BASE_URL}/users"
        response = self.session.get(url)
        data = response.json()

        records = [SourceRecord(id=r["id"], data=r) for r in data["results"]]
        next_pagination = {"next": data["next"]} if data.get("next") else {}

        return SourceIteration(records=records, next_pagination=next_pagination)
```

## Step 6: Implement Incremental Sync Support

To support incremental sync, implement `get_records_after()` method that filters records by timestamp.

### 6.1 Determine API Timestamp Support

Check if the API supports server-side timestamp filtering:

| API Capability | Implementation Strategy |
|----------------|------------------------|
| Has `updated_after` / `modified_since` param | Server-side filtering (preferred) |
| Has `sort_by=updated_at` option | Fetch sorted, stop at cutoff |
| No timestamp support | Client-side filtering (less efficient) |

### 6.2 Template: Server-Side Filtering (Preferred)

```python
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord

def get_records_after(
    self, source_state: SourceIncrementalState, pagination: dict = None
) -> SourceIteration:
    """
    Fetch records updated after source_state.last_run.

    The producer calls this method when sync_mode=incremental and
    a previous successful job exists.

    Args:
        source_state.last_run: previous successful job's created_at (tz-aware UTC)
        source_state.run_started_at: this job's created_at, use it as the window's upper bound
        source_state.state: the previous successful run's SourceIteration.next_state
        source_state.cursor_field: field name from config (e.g., "updated_at")
        pagination: pagination state for multi-page results
    """
    # Convert datetime to API format
    last_run_iso = source_state.last_run.isoformat()

    # Build params with timestamp filter
    params = {
        "{{TIMESTAMP_PARAM}}": last_run_iso,  # e.g., "updated_after", "modified_since"
        "{{PAGE_SIZE_PARAM}}": self.config.page_size,
    }

    # Handle pagination
    if pagination and pagination.get("{{CURSOR_KEY}}"):
        params["{{CURSOR_PARAM}}"] = pagination["{{CURSOR_KEY}}"]

    response = self.session.get(f"{BASE_URL}/{{STREAM_ENDPOINT}}", params=params)
    response.raise_for_status()
    data = response.json()

    records = [
        SourceRecord(id=r["{{RECORD_ID_FIELD}}"], data=r)
        for r in data.get("{{RESULTS_KEY}}", [])
    ]

    # Same pagination pattern as get()
    next_pagination = {"{{CURSOR_KEY}}": data["{{NEXT_CURSOR_KEY}}"]} if data.get("{{HAS_MORE_KEY}}") else {}

    return SourceIteration(records=records, next_pagination=next_pagination)
```

### 6.3 Template: Client-Side Filtering (Fallback)

When API doesn't support timestamp filtering:

```python
def get_records_after(
    self, source_state: SourceIncrementalState, pagination: dict = None
) -> SourceIteration:
    """Fetch all records, filter client-side by timestamp."""

    # Use regular get() to fetch records
    iteration = self.get(pagination)

    # Filter records where timestamp > last_run
    cursor_field = source_state.cursor_field or "{{DEFAULT_TIMESTAMP_FIELD}}"
    last_run_iso = source_state.last_run.isoformat()

    filtered_records = [
        record for record in iteration.records
        if record.data.get(cursor_field, "") > last_run_iso
    ]

    return SourceIteration(
        records=filtered_records,
        next_pagination=iteration.next_pagination,
    )
```

### 6.4 Template: Multi-Stream Dispatch

For sources with multiple streams:

```python
def get_records_after(
    self, source_state: SourceIncrementalState, pagination: dict = None
) -> SourceIteration:
    """Dispatch to stream-specific incremental method."""
    stream = self.config.stream

    if stream == {{SOURCE_NAME}}Streams.{{STREAM_1}}:
        return self.get_{{stream_1}}_after(source_state, pagination)
    elif stream == {{SOURCE_NAME}}Streams.{{STREAM_2}}:
        return self.get_{{stream_2}}_after(source_state, pagination)
    else:
        # Fallback for streams without incremental support
        logger.warning(f"Stream {stream} doesn't support incremental, using full refresh")
        return self.get(pagination)

def get_{{stream_1}}_after(
    self, source_state: SourceIncrementalState, pagination: dict = None
) -> SourceIteration:
    """Stream-specific incremental implementation."""
    # Implement stream-specific logic here
    pass
```

### 6.5 Template: Incremental Example Config

```yaml
# {{SOURCE_DISPLAY_NAME}} Incremental Sync Configuration
#
# Incremental sync only fetches records updated since the last successful run.
# First run behaves like full_refresh, subsequent runs fetch only new/updated records.

name: {{source_name}}_{{stream_name}}_incremental

source:
  name: {{source_name}}
  stream: {{stream_name}}
  sync_mode: incremental
  cursor_field: {{CURSOR_FIELD}}  # e.g., updated_at, modified_at, last_edited_time
  authentication:
    type: api_key
    params:
      token: <YOUR_API_KEY>

  # Source-specific options
  page_size: {{DEFAULT_PAGE_SIZE}}

destination:
  name: bigquery
  config:
    project_id: <YOUR_GCP_PROJECT>
    dataset_id: {{source_name}}_data
    dataset_location: US

engine:
  backend:
    type: bigquery
    config:
      database: <YOUR_GCP_PROJECT>
      schema: bizon_backend
      syncCursorInDBEvery: 2
```

### 6.6 Incremental Sync Validation Checklist

- [ ] `get_records_after()` method implemented
- [ ] Handles `source_state.last_run` as datetime
- [ ] Returns `SourceIteration` with records and pagination
- [ ] Returns empty `next_pagination={}` when done
- [ ] Logging added for debugging (`logger.info("Incremental sync: found X records after Y")`)
- [ ] Example incremental config created
- [ ] Tested with two consecutive runs (first = full, second = incremental)

### 6.7 Common Incremental Patterns by API Type

| API Style | Implementation | Example APIs |
|-----------|---------------|--------------|
| REST with `updated_after` | Server-side filter | HubSpot, Salesforce |
| REST with sort only | Sort + client-side cutoff | Many REST APIs |
| GraphQL | Add `updatedAt_gt` to query | Shopify, GitHub |
| Search API | Client-side filter (inefficient) | Notion Search |
| Database query | Add timestamp to filter | Notion Database Query |

### 6.8 Reference Implementation

See `bizon/connectors/sources/notion/src/source.py` for a complete example:
- `get_pages_after()` - Search API with client-side filtering
- `get_blocks_markdown_after()` - Database query with combined timestamp + user filters
- `get_records_after()` - Main dispatch method

## Placeholders Reference

When generating code, replace these placeholders:

| Placeholder | Description | Example |
|-------------|-------------|---------|
| `{{SOURCE_NAME}}` | PascalCase source name | `HubSpot`, `Notion`, `Stripe` |
| `{{source_name}}` | snake_case source name | `hubspot`, `notion`, `stripe` |
| `{{STREAM_NAME}}` | UPPER_CASE stream name | `CONTACTS`, `USERS`, `ORDERS` |
| `{{stream_name}}` | snake_case stream name | `contacts`, `users`, `orders` |
| `{{BASE_URL}}` | API base URL | `https://api.notion.com/v1` |
| `{{TEST_ENDPOINT}}` | Endpoint for connection test | `users`, `me`, `ping` |
| `{{STREAM_ENDPOINT}}` | Endpoint for the stream | `users`, `contacts` |
| `{{RETRY_COUNT}}` | Number of retries | `10` |
| `{{BACKOFF_FACTOR}}` | Backoff multiplier | `2` |
| `{{DEFAULT_PAGE_SIZE}}` | Default page size | `100` |
| `{{MAX_PAGE_SIZE}}` | Max allowed page size | `100` |
| `{{PAGE_SIZE_PARAM}}` | Param name for page size | `page_size`, `limit`, `per_page` |
| `{{CURSOR_PARAM}}` | Param name for cursor | `cursor`, `start_cursor`, `after` |
| `{{CURSOR_KEY}}` | Key in pagination dict | `cursor`, `start_cursor` |
| `{{NEXT_CURSOR_KEY}}` | Response key for next cursor | `next_cursor`, `cursor` |
| `{{HAS_MORE_KEY}}` | Response key for has_more | `has_more`, `hasMore` |
| `{{RESULTS_KEY}}` | Response key for results array | `results`, `data`, `items` |
| `{{RECORD_ID_FIELD}}` | Field name for record ID | `id`, `uuid`, `name` |
| `{{CUSTOM_HEADER}}` | Custom header name | `X-API-Version`, `Notion-Version` |
| `{{HEADER_VALUE}}` | Custom header value | `2024-01-01` |
| `{{auth_type}}` | Auth type in YAML | `api_key`, `bearer`, `oauth` |
| `{{TIMESTAMP_PARAM}}` | API param for timestamp filter | `updated_after`, `modified_since` |
| `{{CURSOR_FIELD}}` | Cursor field for incremental | `updated_at`, `last_edited_time` |
| `{{DEFAULT_TIMESTAMP_FIELD}}` | Default timestamp field in data | `updated_at`, `modified_at` |
