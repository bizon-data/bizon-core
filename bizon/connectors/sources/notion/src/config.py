from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import ConfigDict, Field

from bizon.source.config import HttpConfig, SourceConfig


class NotionStreams(str, Enum):
    DATABASES = "databases"
    DATA_SOURCES = "data_sources"
    PAGES = "pages"
    BLOCKS = "blocks"
    BLOCKS_MARKDOWN = "blocks_markdown"
    USERS = "users"
    # Streams that fetch all accessible content (no database_ids/page_ids required)
    ALL_PAGES = "all_pages"
    ALL_DATABASES = "all_databases"
    ALL_DATA_SOURCES = "all_data_sources"
    ALL_BLOCKS_MARKDOWN = "all_blocks_markdown"


class NotionSourceConfig(SourceConfig):
    model_config = ConfigDict(populate_by_name=True)

    stream: NotionStreams

    database_ids: List[str] = Field(
        default_factory=list,
        validation_alias="notion_database_ids",
        description="List of Notion database IDs to fetch. Required for databases, data_sources, pages, and blocks streams.",
    )
    page_ids: List[str] = Field(
        default_factory=list,
        validation_alias="notion_page_ids",
        description="List of Notion page IDs to fetch. Used for pages and blocks streams.",
    )
    fetch_blocks_recursively: bool = Field(
        default=True,
        description="Whether to fetch nested blocks recursively. Only applies to blocks stream.",
    )
    max_recursion_depth: int = Field(
        default=5,
        ge=1,
        le=100,
        description="Maximum nesting depth for recursive block fetching. Prevents infinite loops.",
    )
    page_size: int = Field(
        default=100,
        ge=1,
        le=100,
        description="Number of results per page (max 100)",
    )
    max_workers: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Number of concurrent workers for fetching blocks. Keep low to respect rate limits.",
    )
    database_filters: Dict[str, Any] = Field(
        default_factory=dict,
        validation_alias="notion_database_filters",
        description="Map of database_id -> Notion filter object. Filters are passed directly to Notion API.",
    )
    start_date: Optional[str] = Field(
        default=None,
        description="Only sync pages created on or after this date (ISO format, e.g. 2024-01-01). "
        "Applied as a created_time filter, AND-combined with any database_filters entry.",
    )
    full_recursive_search: bool = Field(
        default=False,
        description="Discover child databases and child pages under page_ids, up to max_recursion_depth. "
        "Applies to the pages, databases and data_sources streams; blocks_markdown also follows child databases.",
    )
    http: Optional[HttpConfig] = Field(default_factory=HttpConfig, description="HTTP timeout/retry policy")
