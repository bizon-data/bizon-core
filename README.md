# bizon ⚡️
Extract and load your largest data streams with a framework you can trust for billion records.

## Features
- **Natively fault-tolerant**: Bizon uses a checkpointing mechanism to keep track of the progress and recover from the last checkpoint.
- **Queue system agnostic**: Bizon is agnostic of the queuing system, you can use any queuing system like Python Queue, Kafka or Redpanda. Thanks to the `bizon.queue.Queue` interface, adapters can be written for any queuing system.
- **Pipeline metrics**: Bizon provides exhaustive pipeline metrics and implement OpenTelemetry for tracing. You can monitor:
    - ETAs for completion
    - Number of records processed
    - Completion percentage
    - Latency Source <> Destination
- **Lightweight & lean**: Bizon is lightweight, minimal codebase and only uses few dependencies:
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
from bizon.engine.runner import RunnerFactory

yaml_config = """
source:
  source_name: dummy
  stream_name: creatures
  authentication:
    type: api_key
    params:
      token: dummy_key

destination:
  name: logger
  config:
    dummy: dummy
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
      queue: bootstrap_server: localhost:9092 # Kafka:9092 & Redpanda: 19092
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
