# Contributing to Bizon

## Quick Start

```bash
# 1. Clone and setup
git clone <repo> && cd bizon-core
pip install uv
make install  # or: uv sync --all-extras --all-groups

# 2. Make changes
# ...

# 3. Format and test
make format && uv run pytest

# 4. Submit PR
```

## Development Setup

### Prerequisites
- Python 3.10-3.14
- Use Python 3.11 or newer for the full development group (notebooks, profiling, and pre-commit).
  Python 3.10 can run the test suite with `uv sync --no-dev --all-extras --group test`
  and `uv run --no-dev --group test pytest`.
- uv (`pip install uv` or `curl -LsSf https://astral.sh/uv/install.sh | sh`)
- Docker (optional, for queue testing)

### Installation
```bash
make install                    # Full install with dev/test dependencies
uv sync --all-extras --all-groups  # Alternative
```

The virtual environment is created automatically in `.venv/`. Activate with `source .venv/bin/activate` or use `uv run <command>` directly.

## Code Style

- **Formatter**: Ruff (line length: 120)
- **Linter**: Ruff with isort rules
- **Config**: `pyproject.toml` under `[tool.ruff]`

```bash
make format                 # Format + lint with auto-fix
uv run ruff format .        # Format only
uv run ruff check --fix .   # Lint only
```

Pre-commit hooks run automatically on commit.

### Import Ordering
1. Standard library
2. Third-party packages
3. Local imports (`bizon.*`)

## Testing

```bash
uv run pytest                                    # All tests
uv run pytest tests/path/test_file.py           # Single file
uv run pytest tests/path/test_file.py -k "name" # Single test
uv run pytest --cov=bizon                       # With coverage
```

### Testing with Message Brokers

**Kafka**
```bash
docker compose --file ./scripts/queues/kafka-compose.yml up
```
```yaml
engine:
  queue:
    type: kafka
    config:
      queue:
        bootstrap_server: localhost:9092
```

**Redpanda**
```bash
docker compose --file ./scripts/queues/redpanda-compose.yml up
```
```yaml
engine:
  queue:
    type: kafka
    config:
      queue:
        bootstrap_server: localhost:19092
```

**RabbitMQ**
```bash
docker compose --file ./scripts/queues/rabbitmq-compose.yml up
```
```yaml
engine:
  queue:
    type: rabbitmq
    config:
      queue:
        host: localhost
```

### Backend Configuration (Optional)
Create `tests/.env`:
```bash
BIGQUERY_PROJECT_ID=<YOUR_PROJECT_ID>
BIGQUERY_DATASET_ID=bizon_test
```

## Commit Conventions

Format: `<type>(<scope>): <description>`

| Type | Description |
|------|-------------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation |
| `refactor` | Code refactoring |
| `test` | Tests |
| `chore` | Maintenance |

Examples:
- `feat(source): add Stripe connector`
- `fix(destination): handle BigQuery timeout`
- `docs: update contributing guide`

## Pull Request Process

1. Create branch: `git checkout -b feat/my-feature` or `fix/issue-description`
2. Make changes with tests
3. Run `make format && uv run pytest`
4. Push and create PR
5. Fill out PR template
6. Address review feedback

## Adding Connectors

### Adding a Source
Sources are **auto-discovered** - no registration needed!

1. Create directory: `bizon/connectors/sources/{name}/src/`
2. Implement `AbstractSource` with required methods
3. Add example config in `config/{name}.example.yml`

See [docs/contributing/adding-sources.md](docs/contributing/adding-sources.md) for detailed guide.

### Adding a Destination
Destinations require **manual registration** in 3 places.

1. Create directory: `bizon/connectors/destinations/{name}/src/`
2. Implement `AbstractDestination`
3. Register in: `DestinationTypes` enum, `BizonConfig.destination` Union, `DestinationFactory`

See [docs/contributing/adding-destinations.md](docs/contributing/adding-destinations.md) for detailed guide.

## AI Contributors

For AI-optimized documentation with templates and decision trees:
- [CLAUDE.md](CLAUDE.md) - Architecture and patterns
- [docs/ai-connector-guide.md](docs/ai-connector-guide.md) - Source connector generation
- [docs/ai-destination-guide.md](docs/ai-destination-guide.md) - Destination connector generation

### Claude Skills
- `/new-source` - Scaffold a new source connector
- `/new-destination` - Scaffold a new destination connector
- `/run-checks` - Run format, lint, and tests

## Implementing Incremental Sync for Sources

To support incremental sync in a source, implement the `get_records_after()` method:

```python
from bizon.source.models import SourceIncrementalState, SourceIteration

def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
    """
    Fetch records updated after source_state.last_run.

    Args:
        source_state: Contains:
            - last_run (datetime): Timestamp of the previous successful sync
            - cursor_field (str, optional): The field name to filter by (from config)
            - state (dict): Custom state from previous sync (usually empty)
        pagination: Pagination state for multi-page results

    Returns:
        SourceIteration with records and next_pagination
    """
    # Get the cursor field from state (configured by user in YAML)
    cursor_field = source_state.cursor_field or "updated_at"

    # Filter records by the cursor field > last_run
    records = self.fetch_records_after_timestamp(
        timestamp=source_state.last_run,
        cursor_field=cursor_field,
        pagination=pagination,
    )

    return SourceIteration(
        records=records,
        next_pagination=next_page_token,  # None if no more pages
    )
```

### Key Points:
- `source_state.last_run` is the `created_at` timestamp of the last successful job
- `source_state.cursor_field` is the field name configured in the source YAML (e.g., `updated_at`)
- The method should filter records where `cursor_field > last_run`
- Return `next_pagination={}` or `next_pagination=None` when there are no more records
- On the first incremental run (no previous job), Bizon falls back to calling `get()` instead
