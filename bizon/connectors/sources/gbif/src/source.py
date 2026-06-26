"""GBIF (Global Biodiversity Information Facility) source.

A no-auth, high-volume public API used mainly as a load generator for benchmarking
destinations. The `occurrence` stream exposes ~3.9 billion rich (~2 KB) records via
offset/limit pagination. GBIF caps deep paging at offset 100,000, so a single run
streams up to ~100k records (~200 MB) -- plenty to exercise size-based buffer flushing.

Docs: https://techdocs.gbif.org/en/openapi/v1/occurrence
"""

from enum import Enum
from typing import Any, List, Optional, Tuple

from requests.auth import AuthBase

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.gbif.org/v1"

# GBIF refuses deep paging beyond this offset on the search endpoint.
GBIF_MAX_OFFSET = 100_000
# Max page size accepted by the search endpoint.
GBIF_MAX_PAGE_LIMIT = 300


class GBIFStreams(str, Enum):
    OCCURRENCE = "occurrence"


class GBIFSourceConfig(SourceConfig):
    stream: GBIFStreams = GBIFStreams.OCCURRENCE
    page_limit: int = GBIF_MAX_PAGE_LIMIT
    # Optional cap to bound a benchmark run (defaults to GBIF's deep-paging limit).
    max_records: Optional[int] = None


class GBIFSource(AbstractSource):
    def __init__(self, config: GBIFSourceConfig):
        super().__init__(config)
        self.config: GBIFSourceConfig = config

    @property
    def url_search(self) -> str:
        return f"{BASE_URL}/{self.config.stream.value}/search"

    @staticmethod
    def streams() -> List[str]:
        return [item.value for item in GBIFStreams]

    @staticmethod
    def get_config_class() -> AbstractSource:
        return GBIFSourceConfig

    def get_authenticator(self) -> AuthBase:
        # Public API, no authentication required.
        return None

    def check_connection(self) -> Tuple[bool | Any | None]:
        response = self.session.get(self.url_search, params={"limit": 0})
        response.raise_for_status()
        return True, None

    def get_total_records_count(self) -> int | None:
        response = self.session.get(self.url_search, params={"limit": 0})
        return response.json().get("count")

    @property
    def _page_limit(self) -> int:
        return min(self.config.page_limit, GBIF_MAX_PAGE_LIMIT)

    @property
    def _max_records(self) -> int:
        return min(self.config.max_records or GBIF_MAX_OFFSET, GBIF_MAX_OFFSET)

    def get(self, pagination: dict = None) -> SourceIteration:
        offset = pagination.get("offset", 0) if pagination else 0
        limit = min(self._page_limit, self._max_records - offset)

        response = self.session.get(self.url_search, params={"limit": limit, "offset": offset})
        response.raise_for_status()
        data = response.json()

        next_offset = offset + limit
        has_more = not data.get("endOfRecords", True) and next_offset < self._max_records
        next_pagination = {"offset": next_offset} if has_more else {}

        return SourceIteration(
            next_pagination=next_pagination,
            records=[
                SourceRecord(
                    id=str(record.get("key") or record.get("gbifID") or f"{offset}_{i}"),
                    data=record,
                )
                for i, record in enumerate(data.get("results", []))
            ],
        )
