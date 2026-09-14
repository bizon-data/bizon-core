"""Tests for the backend's schema/dataset bootstrap (`create_schema`)."""

import uuid
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import inspect
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlalchemy_bigquery import BigQueryDialect

from bizon.engine.backend.adapters.sqlalchemy.backend import (
    SQLAlchemyBackend,
    build_bigquery_create_schema_sql,
)
from bizon.engine.backend.adapters.sqlalchemy.config import BigQueryConfigDetails
from bizon.engine.backend.backend import BackendSchemaMissingError
from bizon.engine.backend.config import BackendTypes


@pytest.fixture(scope="module")
def bq_preparer():
    return BigQueryDialect().identifier_preparer


@pytest.fixture
def make_bq_backend(bq_preparer):
    """Build a BigQuery-typed backend without a real engine.

    `create_engine("bigquery://...")` resolves Application Default Credentials and costs seconds per
    call, and `__init__` builds one eagerly. Nothing here connects; only the dialect's preparer is
    read, so hand it a stub carrying the real one.
    """

    def _make(**overrides) -> SQLAlchemyBackend:
        engine = MagicMock()
        engine.dialect.identifier_preparer = bq_preparer
        config = BigQueryConfigDetails(database="my-project", schema="bizon_state", **overrides)
        with patch.object(SQLAlchemyBackend, "_get_engine", return_value=engine):
            return SQLAlchemyBackend(config=config, type=BackendTypes.BIGQUERY)

    return _make


#### DDL ####


def test_create_schema_sql_without_location(bq_preparer):
    assert (
        build_bigquery_create_schema_sql("my-project", "bizon_state", None, bq_preparer)
        == "CREATE SCHEMA IF NOT EXISTS `my-project`.`bizon_state`"
    )


def test_create_schema_sql_with_location(bq_preparer):
    assert (
        build_bigquery_create_schema_sql("my-project", "bizon_state", "EU", bq_preparer)
        == "CREATE SCHEMA IF NOT EXISTS `my-project`.`bizon_state` OPTIONS(location='EU')"
    )


@pytest.mark.parametrize(
    "project,dataset,location",
    [
        ("my project", "bizon_state", None),
        ("my-project", "bizon`state", None),
        ("my-project", "bizon_state", "EU'); DROP SCHEMA x --"),
    ],
)
def test_create_schema_sql_rejects_injection(bq_preparer, project, dataset, location):
    """Identifiers and location are interpolated into DDL text, so they are re-validated here."""
    with pytest.raises(ValueError):
        build_bigquery_create_schema_sql(project, dataset, location, bq_preparer)


#### ensure ####


def test_missing_dataset_raises_when_create_schema_is_off(make_bq_backend):
    backend = make_bq_backend()

    with patch.object(SQLAlchemyBackend, "_schema_exists", return_value=False):
        with pytest.raises(BackendSchemaMissingError) as excinfo:
            backend.check_prerequisites()

    assert "create_schema" in str(excinfo.value)


def test_missing_dataset_is_created_when_create_schema_is_on(make_bq_backend):
    backend = make_bq_backend(create_schema=True, schema_location="EU")
    execute = MagicMock()

    with patch.object(SQLAlchemyBackend, "_schema_exists", return_value=False):
        with patch.object(SQLAlchemyBackend, "_execute_create_schema", execute):
            backend._ensure_schema_exists()

    assert (
        str(execute.call_args[0][0]) == "CREATE SCHEMA IF NOT EXISTS `my-project`.`bizon_state` OPTIONS(location='EU')"
    )


def test_existing_dataset_is_not_recreated(make_bq_backend):
    backend = make_bq_backend(create_schema=True)
    execute = MagicMock()

    with patch.object(SQLAlchemyBackend, "_schema_exists", return_value=True):
        with patch.object(SQLAlchemyBackend, "_execute_create_schema", execute):
            backend._ensure_schema_exists()

    execute.assert_not_called()


def test_dataset_creation_tolerates_losing_a_race(make_bq_backend):
    """Two pipelines sharing a dataset start on the same cron; losing is the outcome we wanted."""
    backend = make_bq_backend(create_schema=True)

    with patch.object(SQLAlchemyBackend, "_schema_exists", side_effect=[False, True]):
        with patch.object(
            SQLAlchemyBackend,
            "_execute_create_schema",
            side_effect=ProgrammingError("CREATE SCHEMA", {}, Exception("409 Already Exists")),
        ):
            backend._ensure_schema_exists()


def test_dataset_creation_still_raises_when_the_dataset_is_missing(make_bq_backend):
    backend = make_bq_backend(create_schema=True)

    with patch.object(SQLAlchemyBackend, "_schema_exists", side_effect=[False, False]):
        with patch.object(
            SQLAlchemyBackend,
            "_execute_create_schema",
            side_effect=OperationalError("CREATE SCHEMA", {}, Exception("permission denied")),
        ):
            with pytest.raises(OperationalError):
                backend._ensure_schema_exists()


def test_check_schema_exist_is_still_available(my_sqlite_backend: SQLAlchemyBackend):
    """Kept as an alias: a custom backend may call or override it."""
    my_sqlite_backend._check_schema_exist()


#### postgres ####


def test_postgres_keeps_raising_for_a_missing_schema(my_pg_backend_config):
    """`create_schema` is BigQuery-only - on Postgres the state tables land in the search_path."""
    config = my_pg_backend_config.config.model_copy(update={"schema_name": f"absent_{uuid.uuid4().hex}"})
    backend = SQLAlchemyBackend(config=config, type=BackendTypes.POSTGRES)

    with pytest.raises(BackendSchemaMissingError):
        backend.check_prerequisites()


def test_execute_create_schema_commits(my_pg_backend_config):
    """begin(), not connect(): on a bare connection the DDL would be rolled back on exit."""
    schema = f"bizon_{uuid.uuid4().hex[:8]}"
    config = my_pg_backend_config.config.model_copy(update={"schema_name": schema})
    backend = SQLAlchemyBackend(config=config, type=BackendTypes.POSTGRES)

    try:
        backend._execute_create_schema(CreateSchema(schema, if_not_exists=True))
        # A fresh engine, so this only passes if the DDL was committed.
        other = SQLAlchemyBackend(config=config, type=BackendTypes.POSTGRES)
        assert inspect(other.get_engine()).has_schema(schema)
    finally:
        with backend.get_engine().begin() as connection:
            connection.execute(DropSchema(schema, if_exists=True, cascade=True))
