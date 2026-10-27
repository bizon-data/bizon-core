from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.auth.config import AuthType
from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource


class MetabaseStreams(str, Enum):
    USERS = "users"
    CARDS = "cards"
    DASHBOARDS = "dashboards"
    PERMISSIONS_GROUPS = "permissions_groups"


class MetabaseSourceConfig(SourceConfig):
    stream: MetabaseStreams

    base_url: str = Field(
        ...,
        description="Base URL of your Metabase instance (e.g., https://metabase.example.com)",
    )

    page_size: int = Field(
        default=50,
        ge=1,
        le=100,
        description="Number of records per page for paginated endpoints (users). Default: 50",
    )

    # cards and dashboards come back unpaginated in one response, which can be slow on a large instance.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(timeout=(10, 300)), description="HTTP timeout/retry policy"
    )


class MetabaseApiKeyAuth(AuthBase):
    """Sends the key raw in `x-api-key`, with no Bearer prefix."""

    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["x-api-key"] = self.api_key
        return request


class MetabaseSource(AbstractSource):
    def __init__(self, config: MetabaseSourceConfig):
        super().__init__(config)
        self.config: MetabaseSourceConfig = config
        # Self-hosted instances are often served over plain http; give it the same retry policy.
        self.session.mount("http://", self.session.get_adapter("https://"))
        self.session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})

    @property
    def api_url(self) -> str:
        base = self.config.base_url.rstrip("/")
        return f"{base}/api"

    def get_authenticator(self) -> Optional[AuthBase]:
        if self.config.authentication is None:
            return None

        if self.config.authentication.type in [AuthType.API_KEY, AuthType.BEARER]:
            return MetabaseApiKeyAuth(api_key=self.config.authentication.params.token)

        logger.warning(f"Unknown auth type: {self.config.authentication.type}")
        return None

    @staticmethod
    def streams() -> List[str]:
        return [stream.value for stream in MetabaseStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return MetabaseSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response = self.session.get(f"{self.api_url}/user/current")
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        if self.config.stream == MetabaseStreams.USERS:
            try:
                response = self.session.get(f"{self.api_url}/user", params={"limit": 1, "offset": 0})
                response.raise_for_status()
                return response.json().get("total")
            except Exception:
                pass
        return None

    def get_users(self, pagination: dict = None) -> SourceIteration:
        offset = pagination.get("offset", 0) if pagination else 0

        response = self.session.get(
            f"{self.api_url}/user",
            params={
                "limit": self.config.page_size,
                "offset": offset,
            },
        )
        response.raise_for_status()
        data = response.json()

        records = [SourceRecord(id=str(user["id"]), data=user) for user in data.get("data", [])]

        total = data.get("total", 0)
        if offset + len(records) < total:
            next_pagination = {"offset": offset + self.config.page_size}
        else:
            next_pagination = {}

        logger.info(f"Fetched {len(records)} users (offset={offset}, total={total})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_cards(self, pagination: dict = None) -> SourceIteration:
        response = self.session.get(f"{self.api_url}/card")
        response.raise_for_status()
        data = response.json()

        records = [SourceRecord(id=str(card["id"]), data=card) for card in data]

        logger.info(f"Fetched {len(records)} cards")
        return SourceIteration(records=records, next_pagination={})

    def get_dashboards(self, pagination: dict = None) -> SourceIteration:
        response = self.session.get(f"{self.api_url}/dashboard")
        response.raise_for_status()
        data = response.json()

        records = [SourceRecord(id=str(dashboard["id"]), data=dashboard) for dashboard in data]

        logger.info(f"Fetched {len(records)} dashboards")
        return SourceIteration(records=records, next_pagination={})

    def get_permissions_groups(self, pagination: dict = None) -> SourceIteration:
        response = self.session.get(f"{self.api_url}/permissions/group")
        response.raise_for_status()
        data = response.json()

        records = [SourceRecord(id=str(group["id"]), data=group) for group in data]

        logger.info(f"Fetched {len(records)} permission groups")
        return SourceIteration(records=records, next_pagination={})

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream

        if stream == MetabaseStreams.USERS:
            return self.get_users(pagination)
        elif stream == MetabaseStreams.CARDS:
            return self.get_cards(pagination)
        elif stream == MetabaseStreams.DASHBOARDS:
            return self.get_dashboards(pagination)
        elif stream == MetabaseStreams.PERMISSIONS_GROUPS:
            return self.get_permissions_groups(pagination)

        raise NotImplementedError(f"Stream '{stream}' is not implemented for Metabase source")
