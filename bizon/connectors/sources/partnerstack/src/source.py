"""PartnerStack Vendor API (v2) source."""

from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase, HTTPBasicAuth

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.partnerstack.com/api/v2"
PAGE_SIZE = 250
REQUEST_TIMEOUT = (10, 60)


class PartnerstackStreams(str, Enum):
    CUSTOMERS = "customers"
    DEALS = "deals"
    GROUPS = "groups"
    LEADS = "leads"
    PARTNERSHIPS = "partnerships"
    REWARDS = "rewards"
    TRANSACTIONS = "transactions"


class PartnerstackSourceConfig(SourceConfig):
    stream: PartnerstackStreams
    public_key: str
    private_key: str
    leads_min_created: Optional[int] = Field(
        None, description="Only fetch leads created at or after this Unix timestamp in milliseconds. Unset fetches all"
    )
    http: Optional[HttpConfig] = Field(default_factory=HttpConfig, description="HTTP timeout/retry policy")


class PartnerstackAuth(AuthBase):
    """Basic auth: public key as username, private key as password."""

    def __init__(self, public_key: str, private_key: str):
        self._basic = HTTPBasicAuth(public_key, private_key)

    def __call__(self, request):
        return self._basic(request)


class PartnerstackSource(AbstractSource):
    def __init__(self, config: PartnerstackSourceConfig):
        super().__init__(config)
        self.config: PartnerstackSourceConfig = config

    def get_authenticator(self) -> AuthBase:
        return PartnerstackAuth(self.config.public_key, self.config.private_key)

    @staticmethod
    def streams() -> List[str]:
        return [stream.value for stream in PartnerstackStreams]

    @staticmethod
    def get_config_class() -> type:
        return PartnerstackSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            self._request(PartnerstackStreams.GROUPS.value, {"limit": 1})
            return True, None
        except Exception as exc:
            return False, str(exc)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _request(self, path: str, params: dict) -> dict:
        response = self.session.get(
            f"{BASE_URL}/{path}",
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()

        try:
            body = response.json()
        except ValueError as exc:
            raise RuntimeError(f"PartnerStack returned non-JSON response from {path}: {response.text[:500]}") from exc

        data = body.get("data") if isinstance(body, dict) else None
        if not isinstance(data, dict) or not isinstance(data.get("items"), list):
            raise RuntimeError(f"PartnerStack returned an unexpected response from {path}: {str(body)[:500]}")
        return data

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream
        cursor = pagination.get("starting_after") if pagination else None

        params: dict = {"limit": PAGE_SIZE}
        if cursor:
            params["starting_after"] = cursor
        if stream == PartnerstackStreams.LEADS and self.config.leads_min_created:
            params["min_created"] = self.config.leads_min_created

        data = self._request(stream.value, params)
        items = data["items"]

        records = []
        for item in items:
            key = item.get("key")
            if key is None:
                raise RuntimeError(f"PartnerStack {stream.value} record has no key")
            records.append(SourceRecord(id=str(key), data=item))

        next_pagination: dict = {}
        if data.get("has_more"):
            if not items:
                raise RuntimeError(f"PartnerStack {stream.value} reported has_more on an empty page")
            next_cursor = str(items[-1]["key"])
            if next_cursor == cursor:
                raise RuntimeError(f"PartnerStack {stream.value} returned a non-advancing cursor {cursor}")
            next_pagination = {"starting_after": next_cursor}

        logger.info(f"Fetched {len(records)} PartnerStack {stream.value} records (has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)
