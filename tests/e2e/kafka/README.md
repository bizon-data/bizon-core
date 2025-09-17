# Kafka E2E Tests

This directory contains end-to-end tests for Kafka authentication failure scenarios.

## Files

- `docker-compose-auth.yml` - Docker compose setup with both plain and SASL-enabled Kafka
- `kafka_server_jaas.conf` - JAAS configuration for SASL authentication
- `test_e2e_kafka_auth_failures.py` - Authentication failure test suite
- `docker-compose.yml` - Basic Kafka setup (existing)

## Running Tests Locally

### Using pytest directly

```bash
# Start Kafka services
cd tests/e2e/kafka
docker compose -f docker-compose-auth.yml up -d

# Wait for services to be ready
sleep 15

# Run tests from project root (with required environment variable)
cd ../../..
export KAFKA_HOST=localhost:9092
export KAFKA_E2E_TESTS=1
poetry run pytest tests/e2e/kafka/test_e2e_kafka_auth_failures.py -v

# Cleanup
cd tests/e2e/kafka
docker compose -f docker-compose-auth.yml down -v
```

### Environment Variable Requirement

**Important**: The Kafka e2e tests will be **skipped** unless the `KAFKA_E2E_TESTS` environment variable is set:

```bash
# Tests will be skipped
pytest tests/e2e/kafka/test_e2e_kafka_auth_failures.py

# Tests will run
KAFKA_E2E_TESTS=1 pytest tests/e2e/kafka/test_e2e_kafka_auth_failures.py
```

This prevents accidental execution of integration tests that require external services.

## Test Scenarios

The authentication failure tests cover:

1. **Invalid Credentials** - Wrong username/password
2. **Unreachable Server** - Connection to non-existent host
3. **Wrong Port** - Connection to wrong port
4. **Timeout Handling** - Proper timeout behavior
5. **Malformed Configuration** - Invalid bootstrap server format

## Services

- **kafka** (port 9092) - Plain Kafka without authentication
- **kafka-with-auth** (port 9093) - SASL-enabled Kafka with authentication
- **test-runner** - Python container for running tests

## Authentication Details

Valid credentials for SASL Kafka (port 9093):
- admin/admin-secret
- valid_user/valid_password
- test_user/test_password

Invalid credentials should trigger authentication failures.

## CI Integration

These tests are integrated into the GitHub Actions workflow at `.github/workflows/kafka-e2e.yml`.