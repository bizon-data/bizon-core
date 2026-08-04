import os
import tempfile

import pytest
from click.testing import CliRunner

from bizon.cli.main import cli
from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.adapters.sqlalchemy.config import (
    SQLiteConfigDetails,
    SQLiteSQLAlchemyConfig,
)
from bizon.engine.backend.config import BackendTypes

CONFIG_TEMPLATE = """
name: test_reset_pipeline

source:
  name: dummy
  stream: creatures
  sync_mode: {sync_mode}
  cursor_field: updated_at
  authentication:
    type: api_key
    params:
      token: dummy_key

destination:
  name: logger
  config:
    dummy: dummy

engine:
  backend:
    type: sqlite
    config:
      database: {database}
      schema: not_used
      syncCursorInDBEvery: 2
"""


@pytest.fixture
def sqlite_database(tmp_path):
    """A file-backed sqlite database: the marker must outlive the CLI process that wrote it."""
    return str(tmp_path / "bizon_reset_test")


@pytest.fixture
def backend(sqlite_database) -> SQLAlchemyBackend:
    return SQLAlchemyBackend(
        config=SQLiteSQLAlchemyConfig(
            type=BackendTypes.SQLITE,
            config=SQLiteConfigDetails(database=sqlite_database, schema="not_used", syncCursorInDBEvery=2),
        ).config,
        type=BackendTypes.SQLITE,
    )


def write_config(sqlite_database: str, sync_mode: str = "incremental") -> str:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as temp:
        temp.write(CONFIG_TEMPLATE.format(sync_mode=sync_mode, database=sqlite_database))
    return temp.name


def pending(backend: SQLAlchemyBackend, stream_name: str = "creatures"):
    return backend.get_pending_stream_reset(name="test_reset_pipeline", source_name="dummy", stream_name=stream_name)


def test_reset_records_a_pending_request(sqlite_database, backend):
    config_path = write_config(sqlite_database)

    result = CliRunner().invoke(cli, ["stream", "reset", config_path])

    assert result.exit_code == 0, result.output
    assert pending(backend) is not None

    os.unlink(config_path)


def test_reset_is_idempotent(sqlite_database, backend):
    """Asking twice must not queue two resets - the next run would reset, then reset again."""
    config_path = write_config(sqlite_database)
    runner = CliRunner()

    runner.invoke(cli, ["stream", "reset", config_path])
    result = runner.invoke(cli, ["stream", "reset", config_path])

    assert result.exit_code == 0, result.output
    assert "already pending" in result.output

    os.unlink(config_path)


def test_reset_can_be_cancelled(sqlite_database, backend):
    config_path = write_config(sqlite_database)
    runner = CliRunner()

    runner.invoke(cli, ["stream", "reset", config_path])
    result = runner.invoke(cli, ["stream", "reset", config_path, "--cancel"])

    assert result.exit_code == 0, result.output
    assert pending(backend) is None

    os.unlink(config_path)


def test_reset_is_rejected_for_non_incremental_streams(sqlite_database, backend):
    config_path = write_config(sqlite_database, sync_mode="full_refresh")

    result = CliRunner().invoke(cli, ["stream", "reset", config_path])

    assert result.exit_code != 0
    assert "Only incremental streams can be reset" in result.output

    os.unlink(config_path)


def test_stream_option_overrides_the_config(sqlite_database, backend):
    """One templated config, many streams: --stream picks which one to reset."""
    config_path = write_config(sqlite_database)

    result = CliRunner().invoke(cli, ["stream", "reset", config_path, "--stream", "pokemons"])

    assert result.exit_code == 0, result.output
    assert pending(backend, "pokemons") is not None
    # The config's own stream must not be touched.
    assert pending(backend, "creatures") is None

    os.unlink(config_path)


def test_stream_option_resets_are_independent(sqlite_database, backend):
    """Resetting one stream must never fan out to another under the same pipeline name."""
    config_path = write_config(sqlite_database)
    runner = CliRunner()

    runner.invoke(cli, ["stream", "reset", config_path, "--stream", "pokemons"])
    runner.invoke(cli, ["stream", "reset", config_path, "--stream", "pokemons", "--cancel"])

    runner.invoke(cli, ["stream", "reset", config_path, "--stream", "berries"])

    assert pending(backend, "pokemons") is None
    assert pending(backend, "berries") is not None

    os.unlink(config_path)


def test_never_run_stream_is_flagged(sqlite_database, backend):
    """A typo'd --stream would otherwise queue a reset that silently never fires."""
    config_path = write_config(sqlite_database)

    result = CliRunner().invoke(cli, ["stream", "reset", config_path, "--stream", "typoo"])

    assert result.exit_code == 0, result.output
    assert "no previous successful run found" in result.output

    os.unlink(config_path)
