# bizon ⚡️
Extract and load your largest data streams with a framework you can trust for billion records.

## Features
- **Natively fault-tolerant**: Bizon uses a checkpointing mechanism to keep track of the progress and recover from the last checkpoint.

- **High throughput**: Bizon is designed to handle high throughput and can process billions of records.

- **Queue system agnostic**: Bizon is agnostic of the queuing system, you can use any queuing system among Python Queue, RabbitMQ, Kafka or Redpanda. Thanks to the `bizon.engine.queue.Queue` interface, adapters can be written for any queuing system.

- **Pipeline metrics**: Bizon provides exhaustive pipeline metrics and implement Datadog & OpenTelemetry for tracing. You can monitor:
    - ETAs for completion
    - Number of records processed
    - Completion percentage
    - Latency Source <> Destination

- **Lightweight & lean**: Bizon is lightweight, minimal codebase and only uses few dependencies:
    - `requests` for HTTP requests
    - `pyyaml` for configuration
    - `sqlalchemy` for database / warehouse connections
    - `polars` for memory efficient data buffering and vectorized processing
    - `pyarrow` for Parquet file format

## Installation

### For Users
```bash
pip install bizon

# With optional dependencies
pip install bizon[postgres]    # PostgreSQL backend
pip install bizon[kafka]       # Kafka queue
pip install bizon[bigquery]    # BigQuery backend/destination
pip install bizon[rabbitmq]    # RabbitMQ queue
```

### For Development
```bash
# Install uv (if not already installed)
pip install uv

# Clone and install
git clone https://github.com/bizon-data/bizon-core.git
cd bizon-core
uv sync --all-extras --all-groups

# Run tests
uv run pytest tests/

# Format code
uv run ruff format .
uv run ruff check --fix .
```

## Usage

### List available sources and streams
```bash
bizon source list
bizon stream list <source_name>
```

### Create a pipeline

Create a file named `config.yml` in your working directory with the following content:

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

Run the pipeline with the following command:

```bash
bizon run config.yml
```
## Backend configuration

Backend is the interface used by Bizon to store its state. It can be configured in the `backend` section of the configuration file. The following backends are supported:
- `sqlite`: In-memory SQLite database, useful for testing and development.
- `bigquery`: Google BigQuery backend, perfect for light setup & production.
- `postgres`: PostgreSQL backend, for production use and frequent cursor updates.

## Queue configuration

Queue is the interface used by Bizon to exchange data between `Source` and `Destination`. It can be configured in the `queue` section of the configuration file. The following queues are supported:
- `python_queue`: Python Queue, useful for testing and development.
- `rabbitmq`: RabbitMQ, for production use and high throughput.
- `kafka`: Apache Kafka, for production use and high throughput and strong persistence.

## Runner configuration

Runner is the interface used by Bizon to run the pipeline. It can be configured in the `runner` section of the configuration file. The following runners are supported:
- `thread` (asynchronous)
- `process` (asynchronous)
- `stream` (synchronous)

## Sync Modes

Bizon supports three sync modes:
- `full_refresh`: Re-syncs all data from scratch on each run
- `incremental`: Syncs only new/updated data since the last successful run
- `stream`: Continuous streaming mode for real-time data (e.g., Kafka)

### Incremental Sync

Incremental sync fetches only new or updated records since the last successful run, using an **append-only** strategy.

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

#### Configuration Options

| Option | Required | Description | Example |
|--------|----------|-------------|---------|
| `sync_mode` | Yes | Set to `incremental` | `incremental` |
| `cursor_field` | Yes | Timestamp field to filter by | `updated_at`, `last_edited_time`, `modified_at` |

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
name: notion_incremental_sync

source:
  name: notion
  stream: blocks_markdown
  sync_mode: incremental
  cursor_field: last_edited_time
  authentication:
    type: api_key
    params:
      token: secret_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

  database_ids:
    - "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

  # Optional: filter which pages to sync
  database_filters:
    "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx":
      property: "Status"
      select:
        equals: "Published"

destination:
  name: bigquery
  config:
    project_id: my-gcp-project
    dataset_id: notion_data
    dataset_location: US

engine:
  backend:
    type: bigquery
    database: my-gcp-project
    schema: bizon_backend
    syncCursorInDBEvery: 2
```

#### First Run Behavior

On the first incremental run (no previous successful job):
- Falls back to `get()` method (full refresh behavior)
- All data is fetched and loaded
- Job is marked as successful
- Subsequent runs use `get_records_after()` with `last_run` timestamp

## Start syncing your data 🚀

### Quick setup without any dependencies ✌️

Queue configuration can be set to `python_queue` and backend configuration to `sqlite`.
This will allow you to test the pipeline without any external dependencies.


### Local Kafka setup

To test the pipeline with Kafka, you can use `docker compose` to setup Kafka or Redpanda locally.

**Kafka**
```bash
docker compose --file ./scripts/kafka-compose.yml up # Kafka
docker compose --file ./scripts/redpanda-compose.yml up # Redpanda
```

In your YAML configuration, set the `queue` configuration to Kafka under `engine`:
```yaml
engine:
  queue:
    type: kafka
    config:
      queue:
        bootstrap_server: localhost:9092 # Kafka:9092 & Redpanda: 19092
```

**RabbitMQ**
```bash
docker compose --file ./scripts/rabbitmq-compose.yml up
```

In your YAML configuration, set the `queue` configuration to Kafka under `engine`:

```yaml
engine:
  queue:
    type: rabbitmq
    config:
      queue:
        host: localhost
        queue_name: bizon
```
