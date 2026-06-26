# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Bizon is a Python-based data extraction and loading (EL) framework for processing large data streams with native fault tolerance, checkpointing, and high throughput (billions of records).

## Common Commands

```bash
# Install dependencies
make install                    # Full install with dev/test dependencies
uv sync --group test            # Install with test dependencies only
uv sync --all-extras            # Install with all extras (postgres, kafka, etc.)

# Run tests
uv run pytest                   # Run all tests
uv run pytest tests/path/to/test_file.py -k "test_name"  # Single test

# Format code
make format                     # Run Ruff formatter and linter
uv run ruff format .            # Format only
uv run ruff check --fix .       # Lint and auto-fix

# CLI commands
uv run bizon run config.yml     # Run a pipeline from YAML config
uv run bizon source list        # List available sources
uv run bizon stream list <source>  # List streams for a source
```

## Releasing

Releases are automated via GitHub Actions on tag push:

1. **Update CHANGELOG.md** - Add entry under `[Unreleased]` following [Keep a Changelog](https://keepachangelog.com/) format
2. **Bump version** in `pyproject.toml`
3. **Move changelog entries** from `[Unreleased]` to new version section with date
4. **Create and push tag**:
   ```bash
   git tag v0.X.Y
   git push origin v0.X.Y
   ```

This triggers `.github/workflows/publish.yml` which:
- Builds and publishes to PyPI
- Creates GitHub Release with changelog excerpt

### Changelog Format

```markdown
## [Unreleased]

## [0.X.Y] - YYYY-MM-DD

### Added
- New features

### Changed
- Changes to existing functionality

### Fixed
- Bug fixes

### Removed
- Removed features
```

## Code Style

- Ruff formatter with line length 120
- Ruff linter with isort rules for import sorting
- Configuration in `pyproject.toml` under `[tool.ruff]`

## Architecture

### Core Components

The framework uses a **producer-consumer pattern** with pluggable components:

```
YAML Config → RunnerFactory → Producer → Queue → Consumer → Destination
                                ↑                    ↓
                              Source              Backend (checkpoints)
```

### Key Abstractions

| Abstraction | Base Class | Location |
|-------------|------------|----------|
| Source | `AbstractSource` | `bizon/source/source.py` |
| Destination | `AbstractDestination` | `bizon/destination/destination.py` |
| Queue | `AbstractQueue` | `bizon/engine/queue/queue.py` |
| Backend | `AbstractBackend` | `bizon/engine/backend/backend.py` |
| Runner | `AbstractRunner` | `bizon/engine/runner/runner.py` |

### Directory Structure

- `bizon/cli/` - CLI entry points (`bizon run`, `bizon source list`)
- `bizon/source/` - Source abstraction, auth, cursor, discovery
- `bizon/destination/` - Destination abstraction, buffering
- `bizon/engine/` - Queue, backend, runner implementations
- `bizon/engine/pipeline/` - Producer and consumer logic
- `bizon/connectors/sources/` - Built-in source connectors
- `bizon/connectors/destinations/` - Built-in destination connectors
- `bizon/common/models.py` - `BizonConfig` main YAML schema
- `bizon/transform/` - Data transformation system

### Adding New Sources

Sources are auto-discovered via AST parsing. Create:

```
bizon/connectors/sources/{source_name}/src/
├── __init__.py
├── config.py    # SourceConfig subclass
└── source.py    # AbstractSource implementation
```

Required methods:
- `streams() -> List[str]` - Available streams
- `get_config_class()` - Return config class
- `get_authenticator()` - Return auth handler
- `check_connection()` - Test connectivity
- `get(pagination)` - Fetch records (returns `SourceIteration`)
- `get_records_after()` - For incremental sync support (optional)

### Claude Skills

Use these skills for common workflows:

| Skill | Description |
|-------|-------------|
| `/new-source` | Scaffold a new source connector |
| `/new-destination` | Scaffold a new destination connector |
| `/run-checks` | Run format, lint, and tests |

### AI-Assisted Connector Generation

**Source connectors** - Read these guides:
- `docs/ai-connector-guide.md` - Templates, decision trees, extraction checklists
- `docs/reference-connector.md` - Fully annotated production example

**Destination connectors** - Read:
- `docs/ai-destination-guide.md` - Templates with placeholders, registration steps

**Workflow for sources**:
```
API Docs URL → Extract info → Generate code → Validate
```
Sources are auto-discovered - no registration needed!

**Workflow for destinations**:
```
Generate code → Register in 3 places → Validate
```
Must register in: `DestinationTypes` enum, `BizonConfig.destination` Union, `DestinationFactory`

**Files to create for sources**:
```
bizon/connectors/sources/{source_name}/
├── config/
│   └── {source_name}.example.yml
└── src/
    ├── __init__.py
    ├── config.py
    └── source.py
```

**Files to create for destinations**:
```
bizon/connectors/destinations/{dest_name}/
└── src/
    ├── __init__.py
    ├── config.py
    └── destination.py
```

### Adding New Destinations

Create:

```
bizon/connectors/destinations/{dest_name}/src/
├── __init__.py
├── config.py      # DestinationConfig subclass with Literal name
└── destination.py # AbstractDestination implementation
```

Then register in:
1. `DestinationTypes` enum in `bizon/destination/config.py`
2. `BizonConfig.destination` Union in `bizon/common/models.py`
3. `DestinationFactory.get_destination()` in `bizon/destination/destination.py`

### Sync Modes

- `FULL_REFRESH` - Full dataset each run
- `INCREMENTAL` - Only new/updated records since last run (append-only)
- `STREAM` - Continuous streaming mode

### Implementing Incremental Sync

Incremental sync requires implementation in both **sources** and **destinations**.

#### Source: `get_records_after()` Method

Sources must implement `get_records_after(source_state, pagination)` to support incremental sync:

```python
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord

def get_records_after(
    self, source_state: SourceIncrementalState, pagination: dict = None
) -> SourceIteration:
    """
    Fetch records updated after source_state.last_run.

    Args:
        source_state: Contains:
            - last_run (datetime): Timestamp of last successful sync
            - cursor_field (str): Field name to filter by (e.g., "updated_at")
            - state (dict): Optional additional state
        pagination: Pagination state for multi-page results

    Returns:
        SourceIteration with records and next_pagination
    """
    # Convert last_run to API-compatible format
    last_edited_after = source_state.last_run.isoformat()

    # Query API with timestamp filter
    # Example: GET /records?updated_after={last_edited_after}
    response = self.session.get(
        f"{BASE_URL}/records",
        params={
            "updated_after": last_edited_after,
            "page_size": self.config.page_size,
            **({"cursor": pagination["cursor"]} if pagination else {}),
        }
    )
    data = response.json()

    records = [
        SourceRecord(id=r["id"], data=r)
        for r in data["results"]
    ]

    next_pagination = {"cursor": data["next_cursor"]} if data.get("has_more") else {}

    return SourceIteration(records=records, next_pagination=next_pagination)
```

**Key Implementation Notes:**
- `source_state.last_run` is a `datetime` from the previous successful job's `created_at`
- `source_state.cursor_field` tells you which field the user configured (e.g., "updated_at")
- Filter records server-side when possible (more efficient)
- For APIs without timestamp filters, filter client-side after fetching
- Handle pagination the same way as `get()` method

#### Destination: `finalize()` Method for Incremental

Destinations must implement `finalize()` to handle incremental data:

```python
from bizon.source.config import SourceSyncModes

def finalize(self) -> bool:
    """Finalize sync - handle temp table based on sync mode."""
    if self.sync_metadata.sync_mode == SourceSyncModes.FULL_REFRESH.value:
        # Replace main table with temp table
        self.client.query(f"CREATE OR REPLACE TABLE {self.table_id} AS SELECT * FROM {self.temp_table_id}")
        self.client.delete_table(self.temp_table_id, not_found_ok=True)
        return True

    elif self.sync_metadata.sync_mode == SourceSyncModes.INCREMENTAL.value:
        # Append temp table to main table
        self.client.query(f"INSERT INTO {self.table_id} SELECT * FROM {self.temp_table_id}")
        self.client.delete_table(self.temp_table_id, not_found_ok=True)
        return True

    elif self.sync_metadata.sync_mode == SourceSyncModes.STREAM.value:
        return True  # Direct writes, no finalization
```

**Temp Table Naming Convention:**
- Full refresh: `{table_id}_temp`
- Incremental: `{table_id}_incremental`
- Stream: `{table_id}` (direct writes)

#### Reference Implementation: Notion Source

See `bizon/connectors/sources/notion/src/source.py` for a complete incremental implementation:
- `get_pages_after()` - Uses Search API with client-side filtering
- `get_blocks_markdown_after()` - Queries databases with combined timestamp + user filters
- `get_records_after()` - Main dispatch method

### Queue Types

- `python_queue` - In-memory (dev/test)
- `kafka` - Apache Kafka/Redpanda (production)
- `rabbitmq` - RabbitMQ (production)

### Backend Types (state storage)

- `sqlite` / `sqlite_in_memory` - File/memory (dev/test)
- `postgres` - PostgreSQL (production)
- `bigquery` - Google BigQuery (production)

### Runner Types

- `thread` - ThreadPoolExecutor (default)
- `process` - ProcessPoolExecutor (true parallelism)
- `stream` - Synchronous single-thread

### Secret & Reference Resolution

Keep secrets out of YAML by referencing them with a URI scheme. Resolution runs once over
the raw config dict before Pydantic validation (`bizon/engine/resolvers/`), so **connectors
need no changes** — they always read plain strings.

- `gsm://<id>` → Google Secret Manager, latest version (ADC auth). Pin with
  `gsm://<id>/versions/<N>`, or pass a full `gsm://projects/<p>/secrets/<id>/versions/<N>` path.
- `env://<VAR>` → environment variable (also works **inline**).
- Inline form: embed in a larger string with `${...}`, e.g.
  `dsn: "postgres://u:${gsm://db-pw}@host/db"` (multiple tokens allowed).
- Optional `secrets:` block holds provider defaults (e.g. `secrets.gsm.project_id`).
- Legacy whole-value `BIZON_ENV_FOO` references still work unchanged.
- Install GSM support: `pip install 'bizon[secretmanager]'`.
- Validate before running: `bizon secrets check <config>` (dry-runs every reference, masked output).

Add a provider by dropping one adapter in `bizon/engine/resolvers/adapters/` and one entry in
`_SCHEME_FACTORIES` (`bizon/engine/resolvers/resolver.py`).

### Key Patterns

- **Factory Pattern**: `RunnerFactory`, `QueueFactory`, `BackendFactory`, `DestinationFactory`
- **Cursor-based Checkpointing**: Producer and destination cursors saved to backend for recovery
- **Pydantic Discriminators**: Union types route to correct implementation based on `type`/`name` field
- **Polars DataFrames**: Used for memory-efficient columnar data processing
- **Buffering**: Destinations buffer records before batch writes (configurable size/timeout)
