import os

import pytest

from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.adapters.sqlalchemy.config import (
    BigQueryConfigDetails,
    BigQuerySQLAlchemyConfig,
    PostgresConfigDetails,
    PostgresSQLAlchemyConfig,
    SQLiteConfigDetails,
    SQLiteInMemoryConfig,
)
from bizon.engine.backend.backend import BackendFactory
from bizon.engine.backend.config import BackendTypes


def test_backend_factory_sqlite():

    config = SQLiteInMemoryConfig(
        type=BackendTypes.SQLITE_IN_MEMORY,
        config=SQLiteConfigDetails(
            database="NOT_DATABASE_WITH_SQLITE", schema="NOT_SCHEMA_IN_SQLITE", syncCursorInDBEvery=2
        ),
    )

    backend = BackendFactory().get_backend(config=config)
    assert isinstance(backend, SQLAlchemyBackend)


def test_backend_factory_postgres():

    config = PostgresSQLAlchemyConfig(
        type=BackendTypes.POSTGRES,
        config=PostgresConfigDetails(
            database="NOT_DATABASE_WITH_SQLITE",
            schema="NOT_SCHEMA_IN_SQLITE",
            syncCursorInDBEvery=2,
            host="localhost",
            port=5432,
            username="postgres",
            password="bizon",
        ),
    )

    backend = BackendFactory().get_backend(config=config)
    assert isinstance(backend, SQLAlchemyBackend)


@pytest.mark.skipif(
    os.getenv("POETRY_ENV_TEST") == "CI",
    reason="Skipping tests that require a BigQuery database",
)
def test_backend_factory_bigquery():

    config = BigQuerySQLAlchemyConfig(
        type=BackendTypes.BIGQUERY,
        config=BigQueryConfigDetails(
            database="project",
            schema="dataset",
            syncCursorInDBEvery=2,
        ),
    )

    backend = BackendFactory().get_backend(config=config)
    assert isinstance(backend, SQLAlchemyBackend)
