from typing import Literal, Union

from pydantic import Field

from bizon.engine.backend.config import (
    AbstractBackendConfig,
    AbstractBackendConfigDetails,
    BackendTypes,
)


class SQLAlchemyConfigDetails(AbstractBackendConfigDetails):
    echoEngine: bool = Field(False, description="Echo the engine in logs")


class SQLiteConfigDetails(SQLAlchemyConfigDetails):
    pass


class PostgresConfigDetails(SQLAlchemyConfigDetails):
    host: str = Field(
        description="Host of the database",
        default=...,
    )
    port: int = Field(
        description="Port of the database",
        default=...,
    )
    username: str = Field(
        description="Username to connect to the database",
        default=...,
    )
    password: str = Field(
        description="Password to connect to the database",
        default=...,
    )


class PostgresSQLAlchemyConfig(AbstractBackendConfig):
    type: Literal[BackendTypes.POSTGRES]
    config: Union[SQLiteConfigDetails, PostgresConfigDetails]


class SQLiteSQLAlchemyConfig(AbstractBackendConfig):
    type: Literal[BackendTypes.SQLITE]
    config: SQLiteConfigDetails


class SQLiteInMemoryConfig(AbstractBackendConfig):
    type: Literal[BackendTypes.SQLITE_IN_MEMORY]
    config: SQLiteConfigDetails
