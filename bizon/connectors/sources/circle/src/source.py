"""Circle.so community platform source (Admin API v2): members, spaces, posts and comments."""

from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://app.circle.so/api/admin/v2"
PER_PAGE = 100


class CircleStreams(str, Enum):
    MEMBERS = "members"
    SPACES = "spaces"
    POSTS = "posts"
    COMMENTS = "comments"


STREAM_ENDPOINTS = {
    CircleStreams.MEMBERS: "community_members",
    CircleStreams.SPACES: "spaces",
    CircleStreams.POSTS: "posts",
    CircleStreams.COMMENTS: "comments",
}


class CircleSourceConfig(SourceConfig):
    stream: CircleStreams
    api_token: str
    # Non-2xx bodies are reported before raising. The default forcelist already retries Cloudflare's 520/522/524.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


class CircleAuth(AuthBase):
    def __init__(self, api_token: str):
        self.api_token = api_token

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.api_token}"
        return request


class CircleSource(AbstractSource):
    def __init__(self, config: CircleSourceConfig):
        super().__init__(config)
        self.config: CircleSourceConfig = config

    def get_authenticator(self) -> AuthBase:
        return CircleAuth(self.config.api_token)

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in CircleStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return CircleSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response = self.session.get(f"{BASE_URL}/community_members", params={"per_page": 1, "page": 1})
            if response.ok:
                return True, None
            return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _get_page(self, endpoint: str, pagination: dict = None) -> SourceIteration:
        """Every v2 list endpoint shares the same envelope."""
        page = pagination.get("page", 1) if pagination else 1

        response = self.session.get(f"{BASE_URL}/{endpoint}", params={"page": page, "per_page": PER_PAGE})
        if not response.ok:
            logger.error(f"Circle API error {response.status_code} on {endpoint}: {response.text}")
            response.raise_for_status()

        data = response.json()
        records = [SourceRecord(id=str(rec["id"]), data=rec) for rec in data.get("records", [])]

        has_next = data.get("has_next_page", False)
        next_pagination = {"page": page + 1} if has_next else {}

        logger.info(f"Fetched {len(records)} {endpoint} (page={page}, has_next={has_next})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        endpoint = STREAM_ENDPOINTS.get(self.config.stream)
        if endpoint is None:
            raise NotImplementedError(f"Stream '{self.config.stream}' is not implemented for Circle source")
        return self._get_page(endpoint, pagination)
