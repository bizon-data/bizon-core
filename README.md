# bizon ⚡️
Extract and load your data reliably with native checkpointing mechanism. Sync your largest data streams with an ETL you can finally trust for millions records.

## Features
- **Natively fault-tolerant**: Bizon uses a checkpointing mechanism to keep track of the progress and recover from the last checkpoint.
- **Queue system agnostic**: Bizon is agnostic of the queuing system, you can use any queuing system like Python Queue, Kafka or Redpanda. Thanks to the `bizon.queue.Queue` interface, adapters can be written for any queuing system.
- **Exhaustive metrics**: Bizon provides exhaustive metrics for the pipeline and implement OpenTelemetry for tracing. You can monitor:
    - Completion percentage
    - Number of records processed
    - ETAs for completion
- **Lightweight**: Bizon is lightweight and has very minimal dependencies:
    - `requests` for HTTP requests
    - `pyyaml` for configuration
    - `sqlalchemy` for database / warehouse connections
    - `pyarrow` for Parquet file format

## Installation
```bash
pip install bizon
```

## Usage
```python
from yaml import safe_load
from bizon.pipeline.runner import RunnerFactory

yaml_config = """
source:
  source_name: dummy
  stream_name: creatures
  authentication:
    type: api_key
    params:
      token: dummy_key

backend:
  type: sqlite

consumer:
  enabled: true
  name: logger

pipeline:
  log_level: DEBUG
"""

config = safe_load(yaml_config)
runner = RunnerFactory.create_from_config_dict(config=config)
runner.run()
```

## Backend configuration

Backend is the interface used by Bizon to store its state. It can be configured in the `backend` section of the configuration file. The following backends are supported:
- `sqlite`: In-memory SQLite database, useful for testing and development.
- `biguquery`: Google BigQuery backend, perfect for light setup & production.
- `postgres`: PostgreSQL backend, for production use and frequent cursor updates.

## Queue configuration

Queue is the interface used by Bizon to exchange data between `Source` and `Destination`. It can be configured in the `queue` section of the configuration file. The following queues are supported:
- `python_queue`: Python Queue, useful for testing and development.
- `kafka`: Apache Kafka, for production use and high throughput.

## Start syncing your data 🚀

### Quick setup without any dependencies ✌️

Queue configuration can be set to `python_queue` and backend configuration  to `sqlite`.
This will allow you to test the pipeline without any external dependencies.


### Local Kafka setup

To test the pipeline with Kafka, you can use `docker compose` to setup Kafka or Redpanda locally.

**Kafka**

`bootstrap.servers: localhost:9092`

```bash
docker compose --file ./scripts/kafka-compose.yml up
```

```yaml
pipeline:
  runner: kafka
  runner_config:
    bootstrap_servers: localhost:9092
    topic: bizon
```

**Redpanda**
```bash
docker compose --file ./scripts/redpanda-compose.yml up
```

Setup your `pipeline`config:

```yaml
pipeline:
  runner: kafka
  runner_config:
    bootstrap_servers: localhost:19092
    topic: bizon
```
