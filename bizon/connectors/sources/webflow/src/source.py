"""Webflow CMS source: the items of one collection (v1 API)."""

from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.webflow.com"
# Legacy (v1) site tokens are refused on /v2/* with a 401; v1 items come back flat.
ACCEPT_VERSION = "1.0.0"
PAGE_SIZE = 100
TIMEOUT = (10, 60)


class WebflowStreams(str, Enum):
    COLLECTION_ITEMS = "collection_items"


class WebflowSourceConfig(SourceConfig):
    stream: WebflowStreams
    api_token: str
    collection_id: str
    site_id: Optional[str] = Field(
        None, description="When set, check_connection verifies the collection is in this site"
    )
    # check_connection reports the status and body of a non-2xx response.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


class WebflowAuth(AuthBase):
    def __init__(self, api_token: str):
        self.api_token = api_token

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.api_token}"
        request.headers["accept-version"] = ACCEPT_VERSION
        return request


class WebflowSource(AbstractSource):
    def __init__(self, config: WebflowSourceConfig):
        super().__init__(config)
        self.config: WebflowSourceConfig = config

    def get_authenticator(self) -> AuthBase:
        return WebflowAuth(self.config.api_token)

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in WebflowStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return WebflowSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            if self.config.site_id:
                response = self.session.get(f"{BASE_URL}/sites/{self.config.site_id}/collections", timeout=TIMEOUT)
                if not response.ok:
                    return False, f"HTTP {response.status_code}: {response.text}"
                if self.config.collection_id not in {c["_id"] for c in response.json()}:
                    return False, f"collection {self.config.collection_id} is not in site {self.config.site_id}"
                return True, None
            response = self.session.get(
                f"{BASE_URL}/collections/{self.config.collection_id}/items", params={"limit": 1}, timeout=TIMEOUT
            )
            if response.ok:
                return True, None
            return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def get_collection_items(self, pagination: dict = None) -> SourceIteration:
        offset = (pagination or {}).get("offset", 0)
        response = self.session.get(
            f"{BASE_URL}/collections/{self.config.collection_id}/items",
            params={"limit": PAGE_SIZE, "offset": offset},
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        body = response.json()
        items = body.get("items", [])
        records = [SourceRecord(id=str(item["_id"]), data=item) for item in items]
        next_offset = offset + len(items)
        has_more = bool(items) and next_offset < body.get("total", 0)
        logger.info(
            f"Fetched {len(records)} items from collection {self.config.collection_id} "
            f"(offset={offset}, total={body.get('total')}, has_more={has_more})"
        )
        return SourceIteration(records=records, next_pagination={"offset": next_offset} if has_more else {})

    def get(self, pagination: dict = None) -> SourceIteration:
        if self.config.stream == WebflowStreams.COLLECTION_ITEMS:
            return self.get_collection_items(pagination)
        raise NotImplementedError(f"Stream '{self.config.stream}' is not implemented for Webflow source")
