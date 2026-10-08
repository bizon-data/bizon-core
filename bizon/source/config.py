from abc import ABC
from enum import Enum
from typing import List, Optional, Tuple, Union

from pydantic import BaseModel, Field

from .auth.config import AuthConfig


class SourceSyncModes(str, Enum):
    # Full refresh
    # - creates a new StreamJob
    # - start syncing data from scratch till pagination is empty
    FULL_REFRESH = "full_refresh"

    # Incremental syncs data
    # - get the last successful sync start_date from previous succeeded StreamJob
    # - create a new StreamJob
    # - sync data till pagination is empty
    INCREMENTAL = "incremental"

    # Stream mode
    # - get single RUNNING streamJob syncs data from the last successfuly committed offset in the source system
    STREAM = "stream"


class APIConfig(BaseModel):
    retry_limit: Optional[int] = Field(100, description="Number of retries before giving up", example=100)


class HttpRetryConfig(BaseModel):
    total: int = Field(10, description="Maximum number of retries, across all causes")
    backoff_factor: float = Field(2, description="Exponential backoff factor between retries, in seconds")
    status_forcelist: List[int] = Field(
        [429, 500, 502, 503, 504, 520, 522, 524], description="Status codes to retry, with or without a Retry-After"
    )
    allowed_methods: List[str] = Field(["GET", "POST"], description="HTTP methods that may be retried")
    retry_after_max: Optional[float] = Field(
        120, description="Upper bound in seconds on a Retry-After wait. None honours the header as sent."
    )


class HttpConfig(BaseModel):
    timeout: Optional[Union[float, Tuple[float, float]]] = Field(
        (10, 60), description="Default (connect, read) timeout in seconds for requests that do not set one"
    )
    retries: HttpRetryConfig = Field(default_factory=HttpRetryConfig)
    raise_for_status: bool = Field(
        True, description="Raise on 4xx/5xx responses. Disable for sources that inspect non-2xx responses."
    )


class SourceConfig(BaseModel, ABC):
    # Connector identifier to use, match a unique connector code
    name: str = Field(..., description="Name of the source to sync")
    stream: str = Field(..., description="Name of the stream to sync")

    source_file_path: Optional[str] = Field(
        default=None, description="Path to the source file, if not provided will look into bizon internal sources"
    )

    sync_mode: SourceSyncModes = Field(
        description="Sync mode to use",
        default=SourceSyncModes.FULL_REFRESH,
    )

    cursor_field: Optional[str] = Field(
        default=None,
        description="Field name to use for incremental filtering (e.g., 'updated_at', 'modified_at'). "
        "Source will fetch records where this field > last_run timestamp.",
    )

    force_ignore_checkpoint: bool = Field(
        description="Whether to force recreate the sync from iteration 0. Existing checkpoints will be ignored.",
        default=False,
    )

    reset: bool = Field(
        description="Re-fetch the whole stream and replace the destination table for this run, then resume "
        "incremental from it. Only meaningful with sync_mode: incremental.",
        default=False,
    )

    authentication: Optional[AuthConfig] = Field(
        description="Configuration for the authentication",
        default=None,
    )

    max_iterations: Optional[int] = Field(
        description="Maximum number of iterations for pulling from source, if None, run till all records are synced from source",  # noqa
        default=None,
    )

    api_config: APIConfig = Field(
        description="Configuration for the API client",
        default=APIConfig(retry_limit=10),
    )

    http: Optional[HttpConfig] = Field(
        description="HTTP policy for the default session. Unset keeps the legacy policy: no timeout, and retries "
        "only on 413/429/503 carrying a Retry-After header.",
        default=None,
    )

    init_pipeline: bool = Field(
        description="[Used for testing] Whether to initialize the source to run pipeline directly.",
        default=True,
    )
