# bizon ⚡️

**Extract and load your largest data streams with a framework you can trust for billions of records.**

Bizon is a lightweight Python **ETL framework** built around a checkpointed
producer → queue → consumer pipeline. It extracts data from API clients, databases and event
streams, optionally **transforms** records in flight with Python, and loads them into your
warehouse — with native fault-tolerance, resumable checkpoints, and very high throughput, while
staying small enough to read end to end.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [CLI Reference](#cli-reference)
- [Configuration Reference](#configuration-reference)
- [Connectors](#connectors)
  - [Sources](#sources)
  - [Destinations](#destinations)
- [Sync Modes](#sync-modes)
  - [Incremental Sync](#incremental-sync)
  - [Stream Reset](#stream-reset)
- [Engine Configuration](#engine-configuration)
  - [Backends](#backends-state-storage)
  - [Queues](#queues)
  - [Runners](#runners)
- [Transforms](#transforms)
- [Secrets & References](#secrets--references)
- [Monitoring & Alerting](#monitoring--alerting)
- [Multi-Stream Routing](#multi-stream-routing)
- [Documentation & Contributing](#documentation--contributing)
- [License](#license)

---

## Features

- **Natively fault-tolerant** — a checkpointing mechanism tracks progress so a pipeline resumes
  from its last committed cursor after a crash or restart.
- **High throughput** — designed to process billions of records, using Polars DataFrames for
  memory-efficient, vectorized buffering and Parquet for batch loads.
- **Queue-system agnostic** — run on an in-process Python queue, RabbitMQ, or Kafka/Redpanda
  behind a single `Queue` interface; adapters can be written for any broker.
- **Pluggable connectors** — 10 built-in sources and 5 destinations, all behind clean
  `AbstractSource` / `AbstractDestination` interfaces. Sources are **auto-discovered** — no
  registration needed.
- **Multiple sync modes** — `full_refresh`, `incremental` (append-only), and continuous `stream`.
- **Secret resolution** — keep secrets out of YAML with `gsm://` (Google Secret Manager) and
  `env://` references, resolved before validation so connectors only ever see plain strings.
- **In-pipeline transforms** — apply user-defined Python to records in flight.
- **Pipeline metrics** — exhaustive metrics with Datadog & OpenTelemetry tracing: ETAs, records
  processed, completion %, and source ↔ destination latency.
- **Lightweight & lean** — a minimal codebase with few core dependencies (`requests`, `pyyaml`,
  `pydantic`, `sqlalchemy`, `polars`, `pyarrow`).

## Architecture

Bizon uses a **producer–consumer pattern** with pluggable components:

```
YAML Config → RunnerFactory → Producer → Queue → Consumer → Destination
                                  ↑                    ↓
                                Source              Backend (checkpoints)
```

- The **Producer** pulls records from the **Source** in iterations and pushes them onto the
  **Queue** as Polars DataFrames.
- The **Consumer** pulls from the queue, applies **Transforms**, and writes batches to the
  **Destination**.
- The **Backend** persists cursors so syncs are resumable.

**Checkpointing & recovery.** The producer writes its source cursor to the backend every
`syncCursorInDBEvery` iterations, and the consumer records a destination cursor after each
successful write. On restart, the pipeline reads the last destination cursor and resumes from the
next iteration. Bizon's delivery contract is **at-least-once** — on recovery a batch may be
re-written, so destinations are designed to tolerate duplicate writes.

| Abstraction | Base Class | Location |
|-------------|-----------|----------|
| Source | `AbstractSource` | `bizon/source/source.py` |
| Destination | `AbstractDestination` | `bizon/destination/destination.py` |
| Queue | `AbstractQueue` | `bizon/engine/queue/queue.py` |
| Backend | `AbstractBackend` | `bizon/engine/backend/backend.py` |
| Runner | `AbstractRunner` | `bizon/engine/runner/runner.py` |

## Installation

Requires **Python ≥ 3.9, < 3.13**.

```bash
pip install bizon
```

Optional features are installed via extras:

| Extra | `pip install` | Enables |
|-------|---------------|---------|
| `postgres` | `bizon[postgres]` | PostgreSQL backend |
| `bigquery` | `bizon[bigquery]` | BigQuery backend & destinations (incl. Storage Write API) |
| `kafka` | `bizon[kafka]` | Kafka/Redpanda queue **and** Kafka source (Avro/Schema Registry) |
| `rabbitmq` | `bizon[rabbitmq]` | RabbitMQ queue |
| `gsheets` | `bizon[gsheets]` | Google Sheets source |
| `datadog` | `bizon[datadog]` | Datadog metrics & tracing |
| `secretmanager` | `bizon[secretmanager]` | `gsm://` Google Secret Manager references |

Combine extras as needed, e.g. `pip install 'bizon[bigquery,kafka,secretmanager]'`.

### For Development

```bash
# Install uv (if not already installed)
pip install uv

# Clone and install with all extras and dependency groups
git clone https://github.com/bizon-data/bizon-core.git
cd bizon-core
uv sync --all-extras --all-groups

# Run tests
uv run pytest tests/

# Format & lint (Ruff, line length 120)
uv run ruff format .
uv run ruff check --fix .
```

## Quickstart

This example needs **zero external dependencies** — it uses the in-process Python queue and a
SQLite backend (both defaults), reading from the built-in `dummy` source and writing to `logger`.

Create `config.yml`:

```yaml
name: demo-creatures-pipeline

source:
  name: dummy
  stream: creatures
  authentication:
    type: api_key
    params:
      token: dummy_key

destination:
  name: logger
  config:
    dummy: dummy
```

Run it:

```bash
bizon run config.yml
```

## CLI Reference

The CLI entry point is `bizon` (`bizon.cli.main:cli`).

| Command | Description |
|---------|-------------|
| `bizon run <config.yml>` | Run a pipeline from a YAML config |
| `bizon source list` | List available sources and their streams |
| `bizon stream list <source>` | List a source's streams, flagged `[Supports incremental]` / `[Full refresh only]` |
| `bizon stream reset <config.yml>` | Queue a [stream reset](#stream-reset) for the next run of that pipeline |
| `bizon secrets check <config.yml>` | Dry-run every `gsm://` / `env://` reference and report (masked) results |
| `bizon destination` | Subcommand group (no subcommands yet) |

### `bizon run`

```bash
bizon run config.yml \
  --custom-source ./my_source.py \   # Custom Python file implementing a Bizon source
  --runner thread \                  # thread | process | stream (default: thread)
  --log-level INFO \                 # DEBUG | INFO | WARNING | ERROR | CRITICAL
  --env-file .env                    # Load env vars from a .env file (auto-detected if omitted)
```

The `--runner` flag overrides `engine.runner.type` in the config; `--log-level` overrides
`engine.runner.log_level`.

### `bizon secrets check`

```bash
bizon secrets check config.yml [--env-file .env]
```

Resolves every reference in the config (without running the pipeline) and prints each one with a
masked ✓/✗ status. Exits non-zero if any reference fails — handy as a pre-deploy gate.

## Configuration Reference

A pipeline is defined by a single YAML file validated against `BizonConfig`
(`bizon/common/models.py`). Unknown top-level keys are rejected.

| Key | Required | Description |
|-----|----------|-------------|
| `name` | ✅ | Unique name identifying this sync |
| `source` | ✅ | Source connector config (see below) |
| `destination` | ✅ | Destination connector config; routed by its `name` field |
| `transforms` | — | List of in-pipeline transforms (default `[]`) |
| `engine` | — | Backend, queue and runner config (sensible defaults if omitted) |
| `secrets` | — | Provider defaults for `gsm://` / `env://` resolution |
| `monitoring` | — | Datadog metrics & tracing |
| `alerting` | — | Slack alerting |
| `streams` | — | Multi-table routing (requires `source.sync_mode: stream`) |

### `source` keys

Common `SourceConfig` fields (`bizon/source/config.py`); each connector adds its own:

| Key | Default | Description |
|-----|---------|-------------|
| `name` | — | Connector name (e.g. `hubspot`, `notion`, `kafka`) |
| `stream` | — | Stream to sync |
| `sync_mode` | `full_refresh` | `full_refresh` \| `incremental` \| `stream` |
| `cursor_field` | `None` | Timestamp field for incremental filtering (e.g. `updated_at`) |
| `authentication` | `None` | Auth block (`type` + `params`); connector-specific |
| `force_ignore_checkpoint` | `false` | Ignore existing checkpoints and restart from iteration 0 |
| `reset` | `false` | Re-fetch the whole stream and replace the destination table, then resume incremental ([details](#stream-reset)) |
| `max_iterations` | `None` | Cap iterations per run (default: run until source is exhausted) |
| `api_config.retry_limit` | `10` | Retries before giving up on an API call |
| `source_file_path` | `None` | Path to a custom source file (same as `--custom-source`) |

### Annotated example

```yaml
name: hubspot contacts to bigquery

source:
  name: hubspot
  stream: contacts
  properties:
    strategy: all          # connector-specific: fetch all properties
  authentication:
    type: api_key
    api_key: ${env://HUBSPOT_API_KEY}   # inline secret reference

destination:
  name: bigquery
  config:
    project_id: my-gcp-project-id
    dataset_id: bizon_test
    dataset_location: US
    create_dataset: false
    gcs_buffer_bucket: bizon-buffer
    gcs_buffer_format: parquet
    buffer_size: 10            # in MB
    buffer_flush_timeout: 300  # in seconds

engine:
  backend:
    type: bigquery
    config:
      database: my-gcp-project
      schema_name: bizon_backend
    syncCursorInDBEvery: 10
```

Full, copy-pasteable examples live next to each connector under
`bizon/connectors/**/config/*.example.yml`.

## Connectors

### Sources

All sources are auto-discovered. Run `bizon source list` to see what's installed and
`bizon stream list <source>` for a source's streams.

| Source | Connects to | Example streams | Auth | Incremental | Extra |
|--------|-------------|-----------------|------|:-----------:|-------|
| `hubspot` | HubSpot CRM (v3) | `contacts`, `companies`, `deals` | api_key, oauth | — | — |
| `notion` | Notion API | `pages`, `databases`, `data_sources`, `blocks`, `blocks_markdown`, `users` (+ `all_*`) | api_key | ✅ | — |
| `kafka` | Kafka / Redpanda | `topic` (Avro + Schema Registry) | basic (SASL) | stream | `kafka` |
| `gsheets` | Google Sheets | `worksheet` | service account, ADC | — | `gsheets` |
| `cycle` | Cycle (GraphQL) | `customers` | api_key | — | — |
| `periscope` | Periscope / Sisense | `charts`, `dashboards`, `views`, `users`, `databases` | cookies | — | — |
| `sana_ai` | Sana AI Insight Reports | `insight_report` | oauth | — | — |
| `pokeapi` | PokéAPI (public) | `pokemon`, `berry`, `item` | none | — | — |
| `gbif` | GBIF biodiversity (public) | `occurrence` | none | — | — |
| `dummy` | Mock source (testing/demos) | `creatures`, `plants` | api_key, oauth | — | — |

**`notion` is the reference incremental source** — see
`bizon/connectors/sources/notion/src/source.py` for `get_records_after()`.

### Destinations

| Destination | Writes to | Sync modes | Notable features |
|-------------|-----------|------------|------------------|
| `bigquery` | BigQuery | full / incremental / stream | Batch loads via GCS + Parquet; atomic table swaps via free copy jobs; async/batched load jobs; partitioning; optional schema unnesting |
| `bigquery_streaming` | BigQuery | full / incremental / stream | Legacy streaming insert API; dynamic schema evolution; large-row fallback to load jobs |
| `bigquery_streaming_v2` | BigQuery | full / incremental / stream | Storage Write API (protobuf); higher throughput; multi-threaded appends; schema caching |
| `file` | Local NDJSON file | full / incremental / stream | Atomic finalize on full refresh; append on incremental; optional unnesting |
| `logger` | stdout (loguru) | full / incremental / stream | Logs records — for testing & debugging |

**Adding connectors.** Sources are auto-discovered — drop a connector under
`bizon/connectors/sources/{name}/src/` and it appears. Destinations register in three places
(see the guide). Use the `/new-source` and `/new-destination` Claude skills, or read
[`docs/contributing/adding-sources.md`](docs/contributing/adding-sources.md) and
[`docs/contributing/adding-destinations.md`](docs/contributing/adding-destinations.md).

## Sync Modes

| Mode | Behavior |
|------|----------|
| `full_refresh` | Re-syncs all data from scratch on each run (default) |
| `incremental` | Syncs only new/updated records since the last successful run (append-only) |
| `stream` | Continuous streaming for real-time data (e.g. Kafka) |

### Incremental Sync

Incremental sync fetches only new or updated records since the last successful run, using an
**append-only** strategy.

#### Configuration

```yaml
source:
  name: your_source
  stream: your_stream
  sync_mode: incremental
  cursor_field: updated_at  # The timestamp field to filter records by
```

#### How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│                        INCREMENTAL SYNC FLOW                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. Producer checks for last successful job                         │
│     └─> Backend.get_last_successful_stream_job()                    │
│                                                                     │
│  2. If found, creates SourceIncrementalState:                       │
│     └─> last_run = previous_job.created_at                          │
│     └─> cursor_field = config.cursor_field (e.g., "updated_at")     │
│                                                                     │
│  3. Calls source.get_records_after(source_state, pagination)        │
│     └─> Source filters: WHERE cursor_field > last_run               │
│                                                                     │
│  4. Records written to temp table: {table}_incremental              │
│                                                                     │
│  5. finalize() appends temp table to main table                     │
│     └─> BigQuery: free copy job (WRITE_APPEND), creating the         │
│         main table on first run; other destinations: INSERT INTO     │
│     └─> Deletes temp table                                          │
│                                                                     │
│  FIRST RUN: No previous job → falls back to get() (full refresh)    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

#### Supported Sources

Sources must implement `get_records_after()` to support incremental sync:

| Source | Cursor Field | Notes |
|--------|--------------|-------|
| `notion` | `last_edited_time` | Supports `pages`, `databases`, `blocks`, `blocks_markdown` streams |
| (others) | Varies | Check source docs or implement `get_records_after()` |

#### Supported Destinations

Destinations must implement `finalize()` with incremental logic:

| Destination | Support | Notes |
|-------------|---------|-------|
| `bigquery` | ✅ | Append-only via temp table |
| `bigquery_streaming_v2` | ✅ | Append-only via temp table |
| `file` | ✅ | Appends to existing file |
| `logger` | ✅ | Logs completion |

#### Example: Notion Incremental Sync

```yaml
name: notion pages incremental sync

source:
  name: notion
  stream: pages           # Options: databases, data_sources, pages, blocks, users
  sync_mode: incremental
  cursor_field: last_edited_time
  authentication:
    type: api_key
    params:
      token: secret_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  database_ids:
    - "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  page_size: 100

destination:
  name: bigquery
  config:
    project_id: my-gcp-project
    dataset_id: notion_data
    dataset_location: US
    gcs_buffer_bucket: my-gcs-bucket
    gcs_buffer_format: parquet

engine:
  backend:
    type: bigquery
    config:
      database: my-gcp-project
      schema_name: bizon_backend
    syncCursorInDBEvery: 2
```

#### First Run Behavior

On the first incremental run (no previous successful job):
- Falls back to the `get()` method (full-refresh behavior)
- All data is fetched and loaded
- The job is marked successful
- Subsequent runs use `get_records_after()` with the `last_run` timestamp

#### Stream Reset

A reset re-fetches the whole stream once and **replaces** the destination table, then resumes
incremental from that run. Use it when the table has drifted — a backfill, a bug in a transform, or
records the source changed without bumping its cursor field.

There are three ways to ask for one; all do the same thing:

```bash
# One-shot, for a run you launch yourself
bizon run config.yml --reset

# Queued in the backend, for a pipeline whose command line is fixed by a scheduler.
# The next `bizon run config.yml` picks it up — no change to the cron/Airflow job.
bizon stream reset config.yml
bizon stream reset config.yml --cancel   # changed your mind

# One config templated across streams? Pick which stream to reset.
bizon stream reset config.yml --stream deals
```

```yaml
source:
  sync_mode: incremental
  reset: true      # same effect, set in the config
```

What the run does differently:
- **Producer** ignores the watermark and calls `get()` instead of `get_records_after()`
- **Destination** materializes the run as a full refresh, replacing the table rather than appending to
  it (for `bigquery`: staging into `{table}_temp`, then a `WRITE_TRUNCATE` copy job)
- The job row stays `incremental`, so the next run uses *this* run as its new watermark

Notes:
- **Scoped to a single stream.** The request is keyed on `(name, source_name, stream_name)` — the same
  triple as the watermark it overrides — so resetting one stream never affects another, even under the
  same pipeline name. `bizon stream reset` takes the stream from the config; `--stream` overrides it.
- Only meaningful with `sync_mode: incremental`; ignored (with a warning) for the other modes.
- Supported by every destination with a working full-refresh path, since that is what a reset
  materializes as. The exception is `bigquery_streaming`, which has no staging table and appends even
  on a full refresh; a reset there is rejected at config validation rather than duplicating your data.
- A reset that crashes is retried as a reset — the request stays bound to its job, so a rerun cannot
  silently degrade into an append.
- `bizon stream reset` writes to the backend, so it must reach the same one the pipeline uses. With
  the default file-based `sqlite` backend that means the same machine and file.

## Engine Configuration

The `engine` block configures three pluggable subsystems. All have defaults, so `engine` is
optional for local runs.

### Backends (state storage)

The backend stores Bizon's state (jobs and cursors). Configured under `engine.backend`.

| Type | Use case |
|------|----------|
| `sqlite` | File-based SQLite — local development (default) |
| `sqlite_in_memory` | Memory-only SQLite — unit tests |
| `postgres` | PostgreSQL — production, frequent cursor updates (`bizon[postgres]`) |
| `bigquery` | BigQuery — lightweight production state storage (`bizon[bigquery]`) |

`syncCursorInDBEvery` (default `2`) controls how often the source cursor is flushed to the
backend — lower values mean finer-grained recovery, higher values mean less write overhead.

```yaml
engine:
  backend:
    type: postgres
    config:
      host: localhost
      port: 5432
      database: bizon
      schema_name: bizon
      username: ${env://PG_USER}
      password: ${env://PG_PASSWORD}
    syncCursorInDBEvery: 10
```

### Queues

The queue carries records from the producer to the consumer. Configured under `engine.queue`.

| Type | Use case |
|------|----------|
| `python_queue` | In-process — local development & testing (default) |
| `rabbitmq` | RabbitMQ — production, high throughput (`bizon[rabbitmq]`) |
| `kafka` | Kafka / Redpanda — production, high throughput, strong persistence (`bizon[kafka]`) |

Spin up a broker locally with the provided compose files:

```bash
docker compose --file ./scripts/queues/kafka-compose.yml up      # Kafka
docker compose --file ./scripts/queues/redpanda-compose.yml up   # Redpanda
docker compose --file ./scripts/queues/rabbitmq-compose.yml up    # RabbitMQ
```

```yaml
# Kafka
engine:
  queue:
    type: kafka
    config:
      queue:
        bootstrap_server: localhost:9092   # Kafka:9092 / Redpanda:19092

# RabbitMQ
engine:
  queue:
    type: rabbitmq
    config:
      queue:
        host: localhost
        queue_name: bizon
```

### Runners

The runner controls how the producer and consumer execute. Configured under `engine.runner`.

| Type | Description | Best for |
|------|-------------|----------|
| `thread` | `ThreadPoolExecutor` (default) | I/O-bound sources (APIs, DBs) |
| `process` | `ProcessPoolExecutor`, true parallelism | CPU-bound sources/transforms |
| `stream` | Single-threaded, inline | Real-time `stream` sync & multi-stream routing |

## Transforms

Transforms apply user-defined Python to each record as it flows through the pipeline. Each
transform receives the record's parsed payload as a `data` dict, mutates it, and the result is
re-serialized. Transforms are applied **sequentially**, in order.

```yaml
transforms:
  - label: debezium
    python: |
      from datetime import datetime

      cluster = data['value']['source']['name'].replace('_', '-')
      partition = data['partition']
      offset = data['offset']
      kafka_timestamp = datetime.utcfromtimestamp(
          data['value']['source']['ts_ms'] / 1000
      ).strftime('%Y-%m-%d %H:%M:%S.%f')

      deleted = False
      if data['value']['op'] == 'd':
          data = data['value']['before']
          deleted = True
```

A `TransformModel` has just two fields: `label` (a display name) and `python` (the code). There
are no built-in transforms — the logic is entirely yours.

## Secrets & References

Keep secrets out of YAML by referencing them with a URI scheme. Resolution runs once over the raw
config before validation (`bizon/engine/resolvers/`), so **connectors need no changes** — they
always read plain strings.

- `gsm://<id>` → Google Secret Manager, latest version (ADC auth). Pin with
  `gsm://<id>/versions/<N>`, or pass a full
  `gsm://projects/<p>/secrets/<id>/versions/<N>` path. Requires `bizon[secretmanager]`.
- `env://<VAR>` → environment variable.
- **Inline form** — embed in a larger string with `${...}`, e.g.
  `dsn: "postgres://u:${gsm://db-pw}@host/db"` (multiple tokens allowed).
- An optional `secrets:` block holds provider defaults (e.g. `secrets.gsm.project_id`).

Validate every reference before running:

```bash
bizon secrets check config.yml
```

Add a provider by dropping one adapter in `bizon/engine/resolvers/adapters/` and one entry in
`_SCHEME_FACTORIES` (`bizon/engine/resolvers/resolver.py`).

## Monitoring & Alerting

**Monitoring** (`bizon/monitoring/`) — emit pipeline metrics and traces to Datadog. Install
`bizon[datadog]`.

```yaml
monitoring:
  type: datadog
  config:
    enable_tracing: true
    datadog_agent_host: localhost   # or datadog_host_env_var: DD_AGENT_HOST
    datadog_agent_port: 8125
    tags:
      env: production
      team: data
```

**Alerting** (`bizon/alerting/`) — post alerts to Slack on configured log levels.

```yaml
alerting:
  type: slack
  log_levels: [ERROR]              # defaults to [ERROR]
  config:
    webhook_url: ${env://SLACK_WEBHOOK_URL}
```

## Multi-Stream Routing

For one-to-many pipelines (common with Kafka CDC), the `streams` block maps multiple source
streams to their own destination tables and schemas in a single run. It requires
`source.sync_mode: stream` and the `stream` runner.

```yaml
source:
  name: kafka
  stream: topic
  sync_mode: stream
  # topics are auto-extracted from the streams block below
  bootstrap_servers: your-kafka-broker:9092
  group_id: your-consumer-group

destination:
  name: bigquery_streaming_v2
  config:
    project_id: your-gcp-project
    dataset_id: your_dataset
    dataset_location: US
    unnest: true

streams:
  - name: users
    source:
      topic: cdc.public.users
    destination:
      table_id: your-gcp-project.your_dataset.users
      clustering_keys: [id]
      record_schema:
        - { name: id, type: INTEGER, mode: REQUIRED }
        - { name: email, type: STRING, mode: NULLABLE }
  - name: orders
    source:
      topic: cdc.public.orders
    destination:
      table_id: your-gcp-project.your_dataset.orders
      clustering_keys: [id, user_id]

engine:
  runner:
    type: stream
```

See `bizon/connectors/sources/kafka/config/kafka_streams.example.yml` for a complete example.

## Documentation & Contributing

- **Adding sources** — [`docs/contributing/adding-sources.md`](docs/contributing/adding-sources.md)
- **Adding destinations** — [`docs/contributing/adding-destinations.md`](docs/contributing/adding-destinations.md)
- **AI-assisted connector generation** — [`docs/ai-connector-guide.md`](docs/ai-connector-guide.md),
  [`docs/ai-destination-guide.md`](docs/ai-destination-guide.md)
- **Annotated reference connector** — [`docs/reference-connector.md`](docs/reference-connector.md)
- **Contribution guidelines** — [`CONTRIBUTING.md`](CONTRIBUTING.md)
- **Release notes** — [`CHANGELOG.md`](CHANGELOG.md)

Claude Code skills are available for common workflows: `/new-source`, `/new-destination`, and
`/run-checks`.

## License

Bizon is released under the **GNU General Public License v3.0**. See [`LICENSE`](LICENSE).
