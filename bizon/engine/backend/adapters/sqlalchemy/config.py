from typing import Literal

from pydantic import Field

from bizon.engine.backend.config import (
    AbstractBackendConfig,
    AbstractBackendConfigDetails,
    BackendTypes,
)


class SQLAlchemyConfigDetails(AbstractBackendConfigDetails):
    echoEngine: bool = Field(False, description="Echo the engine in logs")


## POSTGRES ##
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
    config: PostgresConfigDetails


## SQLITE ##
class SQLiteConfigDetails(SQLAlchemyConfigDetails):
    pass


class SQLiteSQLAlchemyConfig(AbstractBackendConfig):
    type: Literal[BackendTypes.SQLITE]
    config: SQLiteConfigDetails


class SQLiteInMemoryConfig(AbstractBackendConfig):
    type: Literal[BackendTypes.SQLITE_IN_MEMORY]
    config: SQLiteConfigDetails


## BIGQUERY ##
class BigQueryConfigDetails(SQLAlchemyConfigDetails):

    database: str = Field(
        description="GCP Project name",
        default=...,
    )

    schema_name: str = Field(
        description="BigQuery Dataset name",
        default=...,
        alias="schema",
    )

    service_account_key: str = Field(
        description="Service Account Key JSON string. If empty it will be infered",
        default="",
    )


class BigQuerySQLAlchemyConfig(AbstractBackendConfig):
    type: Literal[BackendTypes.BIGQUERY]
    config: SQLAlchemyConfigDetails
