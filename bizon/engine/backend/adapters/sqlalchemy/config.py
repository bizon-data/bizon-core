from typing import Literal, Optional

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
# Interpolated into CREATE SCHEMA DDL, so it is constrained here as well as at the call site.
BIGQUERY_LOCATION_PATTERN = r"[A-Za-z0-9][A-Za-z0-9-]*"


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

    create_schema: bool = Field(
        default=False,
        description="Create the backend dataset if it does not exist. When False, a missing dataset "
        "raises an error naming this flag.",
    )

    schema_location: Optional[str] = Field(
        default=None,
        pattern=rf"^{BIGQUERY_LOCATION_PATTERN}$",
        description="BigQuery location of the backend dataset (e.g. 'US', 'EU', 'europe-west1'). Used "
        "when creating it and as the location of the backend's query jobs. A dataset's location cannot "
        "be changed after creation. Unset leaves BigQuery's own default.",
    )


class BigQuerySQLAlchemyConfig(AbstractBackendConfig):
    type: Literal[BackendTypes.BIGQUERY]
    config: BigQueryConfigDetails
