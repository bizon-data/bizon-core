# Contributing

## Local setup
- Install poetry: `pip install poetry`
- Activate the virtual environment: `poetry shell`
- Install dependencies: `poetry install --all-extras`

## Testing

### Message brokers

To test the pipeline with Kafka, you can use `docker compose` to setup Kafka, Redpanda or RabbitMQ locally.

**Kafka**
```bash
docker compose --file ./scripts/queues/kafka-compose.yml up # Kafka
```

In your YAML configuration, set the `queue` configuration to Kafka under `engine`:
```yaml
engine:
  queue:
    type: kafka
    config:
      queue: bootstrap_server: localhost:9092 # Kafka
```

**RedPanda**

```bash
docker compose --file ./scripts/queues/redpanda-compose.yml up # Redpanda
```

In your YAML configuration, set the `queue` configuration to Redpanda under `engine`:
```yaml
engine:
  queue:
    type: kafka
    config:
      queue: bootstrap_server: localhost:19092
```


**RabbitMQ**
```bash
docker compose --file ./scripts/queues/rabbitmq-compose.yml up
```

In your YAML configuration, set the `queue` configuration to Kafka under `engine`:

```yaml
engine:
  queue:
    type: rabbitmq
    config:
      queue:
        host: localhost

```

### Start the backends (Optional)
If you don't start the backend, the test fall back on the bizon default backend which is the destination warehouse.

Create a .env file living in the /tests folder:
```bash
BIGQUERY_PROJECT_ID=<YOUR_PROJECT_ID>
BIGQUERY_DATASET_ID=bizon_test
```