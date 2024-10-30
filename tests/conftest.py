import os

import pytest
from dotenv import find_dotenv, load_dotenv
from sqlalchemy.orm import scoped_session, sessionmaker

from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.adapters.sqlalchemy.config import (
    PostgresConfigDetails,
    PostgresSQLAlchemyConfig,
    SQLiteConfigDetails,
    SQLiteInMemoryConfig,
)
from bizon.engine.backend.config import BackendTypes


@pytest.fixture(scope="session", autouse=True)
def load_env():
    env_file = find_dotenv(".env")
    load_dotenv(env_file)


@pytest.fixture(scope="function")
def my_backend_config():
    return SQLiteInMemoryConfig(
        type=BackendTypes.SQLITE_IN_MEMORY,
        config=SQLiteConfigDetails(
            database="NOT_DATABASE_WITH_SQLITE",
            schema="NOT_SCHEMA_IN_SQLITE",
            syncCursorInDBEvery=2,
        ),
    )


@pytest.fixture(scope="function")
def my_pg_backend_config():
    return PostgresSQLAlchemyConfig(
        type=BackendTypes.POSTGRES,
        config=PostgresConfigDetails(
            database="bizon_test",
            schema="public",
            syncCursorInDBEvery=2,
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=5432,
            username="postgres",
            password="bizon",
        ),
    )


@pytest.fixture(scope="function")
def my_pg_backend(my_pg_backend_config: PostgresSQLAlchemyConfig) -> SQLAlchemyBackend:
    return SQLAlchemyBackend(config=my_pg_backend_config.config, type=my_pg_backend_config.type)


@pytest.fixture(scope="function")
def pg_db_session(my_pg_backend: SQLAlchemyBackend):
    """yields a SQLAlchemy connection which is rollbacked after the test"""

    engine = my_pg_backend.get_engine()

    session_ = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))

    yield session_

    session_.rollback()
    session_.close()


@pytest.fixture(scope="function")
def my_sqlite_backend(my_backend_config: SQLiteInMemoryConfig) -> SQLAlchemyBackend:
    return SQLAlchemyBackend(config=my_backend_config.config, type=my_backend_config.type)


@pytest.fixture(scope="function")
def sqlite_db_session(my_sqlite_backend: SQLAlchemyBackend):
    """yields a SQLAlchemy connection which is rollbacked after the test"""

    engine = my_sqlite_backend.get_engine()

    session_ = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))

    yield session_

    session_.rollback()
    session_.close()
