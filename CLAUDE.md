# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Bizon is a Python-based ETL framework (extract, optionally transform in-flight with Python, and load) for processing large data streams with native fault tolerance, checkpointing, and high throughput (billions of records).

> For user-facing docs (full feature list, config reference, connector catalog, examples), see [`README.md`](README.md). This file is the contributor/agent guide.

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
uv run bizon stream reset config.yml  # Queue a stream reset for the next run
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
- `bizon/monitoring/` - Datadog metrics & tracing
- `bizon/alerting/` - Slack alerting

### Built-in Connectors

**Sources** (`bizon/connectors/sources/`) — auto-discovered, run `bizon source list`:
`cycle`, `dummy`, `gbif`, `gsheets`, `hubspot`, `kafka`, `notion`, `periscope`, `pokeapi`, `sana_ai`.
`notion` is the reference incremental source (implements `get_records_after()`); `kafka` is the
reference streaming source.

**Destinations** (`bizon/connectors/destinations/`) — registered in 3 places (see below):
`bigquery` (GCS+Parquet batch loads, atomic copy-job swaps), `bigquery_streaming` (legacy streaming
insert API), `bigquery_streaming_v2` (Storage Write API), `file` (NDJSON), `logger` (stdout, testing).

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

### Stream Reset

One incremental run that ignores the watermark, re-fetches everything, and **replaces** the
destination table — then incremental resumes from that run. See `README.md#stream-reset` for the
user-facing docs.

The whole feature hangs off a single config field, `source.reset`, so it needs no new plumbing:
`init_job()` (`bizon/engine/runner/runner.py`) runs in the parent before the producer and consumer
are submitted, and both are handed the same `bizon_config` / `config` objects.

- **Trigger** — `bizon run --reset`, `source.reset: true`, or a pending row in `stream_resets`
  written by `bizon stream reset <config>` (the only form that reaches a run whose command line a
  scheduler owns; `--stream` overrides the config's stream). `AbstractRunner.resolve_reset()`
  collapses all three into one bool.
- **Granularity** — keyed on `(name, source_name, stream_name)`, the same triple as
  `get_last_successful_stream_job`, so a reset is exactly as scoped as the watermark it overrides.
  Multi-stream configs (the `streams:` block) can never be reset: they require `sync_mode: stream`.
- **Producer** (`pipeline/producer.py`) — skips the `get_last_successful_stream_job` lookup and falls
  through to `source.get()`.
- **Destination** — needs no reset-specific code. `SyncMetadata.from_bizon_config()` maps a reset onto
  `sync_mode: full_refresh`, so the existing full-refresh path is reused with no new finalize branch
  and every destination that can replace its table supports reset for free. Note this is the sync mode
  of the *materialization*, not of the job. Destinations that append even on a full refresh (only
  `bigquery_streaming`, which has no `finalize()`) are listed in `RESET_UNSUPPORTED_DESTINATIONS` in
  `bizon/common/models.py` and rejected at config validation.
- **Job row** — stays `incremental`, so `get_last_successful_stream_job` picks the reset run up as the
  next watermark automatically.
- **Crash safety** — every reset job has a consumed `stream_resets` row pointing at it
  (`bind_stream_reset_to_job`). That is how a retry knows the in-flight job is a reset instead of
  degrading into an append. Temp-table hygiene is *not* reset-specific — see
  [Job recovery semantics](#job-recovery-semantics).

### Job recovery semantics

A process killed from outside (Kubernetes `activeDeadlineSeconds`, OOM, preemption) never runs its
error path, so it always leaves its `stream_jobs` row in `running`. Every recovery decision hangs off
that row, and the rules differ by sync mode. Changing any of this without reading all three points
tends to reintroduce a silent data bug — see the `[0.5.2]` changelog entries.

- **Resuming is per sync mode.** `get_or_create_job()` resumes a `running` job for `incremental`
  (it appends, so completed work still counts) but cancels and recreates it for `full_refresh`
  (which republishes the whole table, so a stale item list is worthless and, because the run never
  reaches `finalize()`, keeps the job `running` for the *next* run to resume too). Resets are
  incremental jobs and are exempt: `resolve_reset()` returns `False` for any non-incremental mode.
- **Temp-table hygiene follows `WRITE_TRUNCATE`, not reset.** Loads always `WRITE_APPEND` into
  `temp_table_id`, so any run that publishes with a `WRITE_TRUNCATE` copy must first drop rows left
  by a crashed attempt. `BigQueryDestination._ensure_clean_temp_table()` therefore fires for
  `sync_mode == FULL_REFRESH` — which covers resets, since `SyncMetadata.from_bizon_config()` maps
  them onto `full_refresh`. It skips the drop when the job already wrote a destination cursor, since
  the resuming producer will not re-fetch those iterations. Plain incremental stages into
  `_incremental` and appends across runs, so it is left alone.
- **An empty pagination is terminal, not "no state".** `create_destination_cursor()` stores a falsy
  pagination as SQL `NULL` rather than `"{}"`, and `Cursor.update_state()` treats an empty pagination
  as "the source is exhausted". Such a job cannot be resumed: handing the source an empty pagination
  restarts it at page one and re-fetches the whole stream. The producer raises with the job id and
  stream instead.

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

The DML above is illustrative. The `bigquery` destination itself publishes with a **copy job**
(`copy_table`, `WRITE_TRUNCATE` for full refresh / `WRITE_APPEND` for incremental) — copy jobs are
metadata-only and free, whereas `CREATE OR REPLACE TABLE ... AS SELECT` / `INSERT INTO ... SELECT`
rescan and rewrite the temp table and are billed on bytes processed. Prefer the engine's native bulk
copy over query DML when one exists.

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

### Transforms

In-pipeline record mutation, applied by the consumer in order (`bizon/transform/`). A
`TransformModel` has two fields: `label` (display name) and `python` (code executed per record).
The record payload is exposed as a `data` dict; reassign `data` to change the output. There are no
built-in transforms — the logic is user-supplied.

```yaml
transforms:
  - label: redact
    python: |
      data.pop("ssn", None)
```

### Monitoring & Alerting

Both are optional top-level config blocks (`bizon/monitoring/config.py`, `bizon/alerting/models.py`).

- **Monitoring** — `type: datadog` with `config` (`enable_tracing`, `datadog_agent_host` or
  `datadog_host_env_var`, `datadog_agent_port` default `8125`, `tags`). Needs `bizon[datadog]`.
- **Alerting** — `type: slack`, `log_levels` (default `[ERROR]`), `config.webhook_url`.

### Multi-Stream Routing

The optional top-level `streams` block (`bizon/common/models.py`) maps multiple source streams to
their own destination tables/schemas in one run. Requires `source.sync_mode: stream` and the
`stream` runner. For Kafka, topics are auto-extracted from the `streams` block. Validation rejects
duplicate stream names and `table_id`s; `table_id` must be `project.dataset.table`. Reference:
`bizon/connectors/sources/kafka/config/kafka_streams.example.yml`.

### Config Defaults (gotchas)

- `engine.backend` defaults to file-based `sqlite` with `syncCursorInDBEvery: 2`.
- `engine.queue` defaults to `python_queue`; `engine.runner` defaults to `thread`.
- `source.sync_mode` defaults to `full_refresh`; `api_config.retry_limit` defaults to `10`.
- `BizonConfig` and `EngineConfig` are `extra="forbid"` — unknown keys raise validation errors.
- `pyproject.toml` declares `requires-python = ">=3.9,<3.13"`, but the code **does not actually run on
  3.9**: PEP 604 unions (`X | None`) are used in `def` signatures without
  `from __future__ import annotations`, and those are evaluated at import. The core SQLAlchemy backend
  is affected (`adapters/sqlalchemy/backend.py`), as are 8 of the bundled sources, so this is not a
  niche path. Nothing in CI runs 3.9 (`pytest.yml` and `kafka-e2e.yml` are 3.10, `publish.yml` is
  3.11), which is why it goes unnoticed. Treat **3.10** as the real minimum until either the
  annotations or the declared floor are fixed.

### Key Patterns

- **Factory Pattern**: `RunnerFactory`, `QueueFactory`, `BackendFactory`, `DestinationFactory`
- **Cursor-based Checkpointing**: Producer and destination cursors saved to backend for recovery
- **Pydantic Discriminators**: Union types route to correct implementation based on `type`/`name` field
- **Polars DataFrames**: Used for memory-efficient columnar data processing
- **Buffering**: Destinations buffer records before batch writes (configurable size/timeout)
