import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

from bizon.engine.backend.backend import Backend
from bizon.engine.backend.config import BackendConfig, BackendTypes


@pytest.fixture(scope="function")
def my_backend_config():
    return BackendConfig(
        type=BackendTypes.SQLITE,
        database="NOT_DATABASE_WITH_SQLITE",
        schema="NOT_SCHEMA_IN_SQLITE",
        syncCursorInDBEvery=2,
    )


@pytest.fixture(scope="function")
def my_backend_client(my_backend_config: BackendConfig) -> Backend:
    return Backend(config=my_backend_config)


@pytest.fixture(scope="function")
def db_session(my_backend_client: Backend):
    """yields a SQLAlchemy connection which is rollbacked after the test"""
    session_ = scoped_session(sessionmaker(bind=my_backend_client.engine, expire_on_commit=False))

    yield session_

    session_.rollback()
    session_.close()
