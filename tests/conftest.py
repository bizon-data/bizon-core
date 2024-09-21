import pytest
from dotenv import find_dotenv, load_dotenv
from sqlalchemy.orm import scoped_session, sessionmaker

from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.adapters.sqlalchemy.config import (
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
def my_backend(my_backend_config: SQLiteInMemoryConfig) -> SQLAlchemyBackend:
    return SQLAlchemyBackend(config=my_backend_config.config, type=my_backend_config.type)


@pytest.fixture(scope="function")
def db_session(my_backend: SQLAlchemyBackend):
    """yields a SQLAlchemy connection which is rollbacked after the test"""

    engine = my_backend.get_engine()

    session_ = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))

    yield session_

    session_.rollback()
    session_.close()
