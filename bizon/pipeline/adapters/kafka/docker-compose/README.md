# Kafka Adapter

This adapter is used to connect to a Kafka cluster. It is used to read and write messages to and from Kafka topics.

## Local Development

### Kafka

`bootstrap.servers: localhost:9092`

```bash
docker compose --file kafka-compose.yml up
```

## Redpanda

`bootstrap.servers: localhost:19092`

```bash
docker compose --file redpanda-compose.yml up
```
