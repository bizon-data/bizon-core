import time
from abc import ABC
from enum import Enum
from http import HTTPStatus
from typing import Any, Dict, List, Optional, Tuple

import pendulum
from loguru import logger
from pydantic import BaseModel, Field
from requests import HTTPError, Response
from requests.auth import AuthBase

from bizon.source.auth.authenticators.oauth import Oauth2AuthParams
from bizon.source.auth.authenticators.token import TokenAuthParams
from bizon.source.auth.builder import AuthBuilder
from bizon.source.auth.config import AuthType
from bizon.source.config import HttpConfig, HttpRetryConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

URL_BASE = "https://api.hubapi.com"
URL_GRANTED_SCOPES = f"{URL_BASE}/oauth/v1/access-tokens"
URL_TOKEN_REFRESH = f"{URL_BASE}/oauth/v1/token"

# (connect, read). Search and associations pages, and emails with HTML bodies, need the longer read.
DEFAULT_TIMEOUT = (10, 60)
LONG_TIMEOUT = (10, 120)

# A request prepared just before the 30-minute access token expires can reach HubSpot after it.
DEFAULT_EXPIRY_SKEW_SECONDS = 120

SEARCH_PAGE_SIZE_MAX = 200


def default_http_config() -> HttpConfig:
    return HttpConfig(retries=HttpRetryConfig(total=50, backoff_factor=1))


def to_ms(value) -> int:
    if isinstance(value, str):
        value = pendulum.parse(value)
    return int(value.timestamp() * 1000)


def last_modified_property(object_type: str) -> str:
    # Contacts keep their last-modified time in `lastmodifieddate`; their `hs_lastmodifieddate` is always null.
    return "lastmodifieddate" if object_type == "contacts" else "hs_lastmodifieddate"


def record_with_timestamp(record: dict, timestamp_field: str = "updatedAt") -> SourceRecord:
    # SourceRecord.timestamp is not Optional: omit it when absent so the extraction time applies.
    value = record.get(timestamp_field)
    if value:
        return SourceRecord(id=str(record["id"]), data=record, timestamp=pendulum.parse(value))
    return SourceRecord(id=str(record["id"]), data=record)


def id_paged_iteration(results: List[dict], records: List[SourceRecord]) -> SourceIteration:
    if not results:
        return SourceIteration(next_pagination={}, records=records)
    return SourceIteration(next_pagination={"last_id": str(max(int(r["id"]) for r in results))}, records=records)


class RateLimiter:
    def __init__(self, max_per_second: Optional[float]):
        self._min_interval = 1.0 / max_per_second if max_per_second else 0.0
        self._next_at = 0.0

    def wait(self) -> None:
        if not self._min_interval:
            return
        now = time.monotonic()
        if now < self._next_at:
            time.sleep(self._next_at - now)
            now = time.monotonic()
        self._next_at = now + self._min_interval


class PropertiesStrategy(str, Enum):
    ALL = "all"
    SELECTED = "selected"


class PropertiesConfig(BaseModel):
    strategy: PropertiesStrategy = Field(PropertiesStrategy.ALL, description="Properties strategy")
    selected_properties: Optional[List[str]] = Field([], description="List of selected properties")


class HubSpotBaseSourceConfig(SourceConfig):
    http: Optional[HttpConfig] = Field(default_factory=default_http_config, description="HTTP timeout/retry policy")
    max_requests_per_second: Optional[float] = Field(
        None,
        gt=0,
        description="Client-side pacing across every call of a run. Search endpoints are rate limited per account "
        "and send no rate-limit headers to back off on, so set this when other clients share the app's budget.",
    )


class HubSpotBaseSource(AbstractSource, ABC):
    def __init__(self, config: HubSpotBaseSourceConfig):
        super().__init__(config)
        self._rate_limiter = RateLimiter(getattr(config, "max_requests_per_second", None))
        self._properties_catalog_cache: Dict[str, List[str]] = {}
        self._run_start_ms: Optional[int] = None

    def get_authenticator(self) -> AuthBase:
        if self.config.authentication.type.value == AuthType.OAUTH.value:
            params = self.config.authentication.params
            skew = params.expiry_skew_seconds
            return AuthBuilder.oauth2(
                params=Oauth2AuthParams(
                    token_refresh_endpoint=URL_TOKEN_REFRESH,
                    client_id=params.client_id,
                    client_secret=params.client_secret,
                    refresh_token=params.refresh_token,
                    expiry_skew_seconds=DEFAULT_EXPIRY_SKEW_SECONDS if skew is None else skew,
                )
            )

        elif self.config.authentication.type.value == AuthType.API_KEY.value:
            return AuthBuilder.token(
                params=TokenAuthParams(
                    token=self.config.authentication.params.token,
                )
            )

        raise NotImplementedError(f"Auth type {self.config.authentication.type} not implemented for HubSpot")

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        """Check connection"""
        alive = True
        error_msg = None
        try:
            self.get()
        except HTTPError as error:
            alive = False
            error_msg = repr(error)
            if error.response is not None and error.response.status_code == HTTPStatus.BAD_REQUEST:
                response_json = error.response.json()
                error_msg = (
                    f"400 Bad Request: {response_json['message']}, please check if provided credentials are valid."
                )
        except Exception as e:
            alive = False
            error_msg = repr(e)
        return alive, error_msg

    def get_granted_scopes(self) -> List[str]:
        try:
            if self.config.authentication.type == AuthType.OAUTH.value:
                response = self._call(method="GET", url=f"{URL_GRANTED_SCOPES}/{self.session.auth.access_token}")
            else:
                raise NotImplementedError("Scope endpoint for API Key are not supported.")
            return response.json()["scopes"]
        except Exception as e:
            return False, repr(e)

    def _call(self, method: str, url: str, **kwargs) -> Response:
        """Refreshes the token and replays once on a 401 EXPIRED_AUTHENTICATION; a second one right after a forced
        refresh is not the expiry race, so it stays fatal."""
        kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
        try:
            return self._send(method, url, **kwargs)
        except HTTPError as error:
            if _auth_error_category(error) != "EXPIRED_AUTHENTICATION":
                raise
            logger.warning("HubSpot rejected the token as EXPIRED_AUTHENTICATION, refreshing and replaying once.")
            self.session.auth.set_token_expiry_date(-1)
            return self._send(method, url, **kwargs)

    def _send(self, method: str, url: str, **kwargs) -> Response:
        self._rate_limiter.wait()
        response = self.session.call(method=method, url=url, **kwargs)
        # The 401 replay and check_connection depend on HTTPError, whatever http.raise_for_status says.
        response.raise_for_status()
        return response

    def get_properties_catalog(self, object_type: str) -> List[str]:
        if object_type not in self._properties_catalog_cache:
            response = self._call(method="GET", url=f"{URL_BASE}/crm/v3/properties/{object_type}")
            self._properties_catalog_cache[object_type] = [p["name"] for p in response.json().get("results", [])]
        return self._properties_catalog_cache[object_type]

    def resolve_properties(self, object_type: str, properties: PropertiesConfig) -> List[str]:
        catalog = self.get_properties_catalog(object_type)
        if not catalog:
            raise ValueError(f"No properties found in HubSpot for {object_type}, which is likely an API error.")

        if properties.strategy == PropertiesStrategy.SELECTED:
            selected = properties.selected_properties or []
            missing = [p for p in selected if p not in catalog]
            if missing:
                raise ValueError(f"Properties {missing} are not present in HubSpot for {object_type}.")
        else:
            selected = catalog

        if not selected:
            raise ValueError("No properties selected to sync")
        logger.info(f"{len(selected)} selected properties for sync.")
        return selected

    def run_start_ms(self, source_state: Optional[SourceIncrementalState] = None) -> int:
        """Exclusive window end, fixed for the whole walk. The job's start time is the next run's `last_run`, so
        preferring it makes windows tile."""
        if source_state is not None and source_state.run_started_at is not None:
            return to_ms(source_state.run_started_at)
        if self._run_start_ms is None:
            self._run_start_ms = int(pendulum.now("UTC").timestamp() * 1000)
        return self._run_start_ms

    def search_page(
        self,
        object_type: str,
        properties: List[str],
        limit: int,
        last_id: Optional[str] = None,
        window: Optional[Tuple[str, int, int]] = None,
        filters: Optional[List[dict]] = None,
    ) -> List[dict]:
        """Pages on `hs_object_id > last_id` instead of `after`, so search's 10,000-result cap never applies."""
        filters = list(filters or [])
        if window is not None:
            cursor_property, start_ms, end_ms = window
            filters.append({"propertyName": cursor_property, "operator": "GTE", "value": start_ms})
            filters.append({"propertyName": cursor_property, "operator": "LT", "value": end_ms})
        if last_id:
            filters.append({"propertyName": "hs_object_id", "operator": "GT", "value": last_id})

        payload: Dict[str, Any] = {
            "properties": properties,
            "sorts": [{"propertyName": "hs_object_id", "direction": "ASCENDING"}],
            "limit": limit,
        }
        if filters:
            payload["filterGroups"] = [{"filters": filters}]

        response = self._call(
            method="POST", url=f"{URL_BASE}/crm/v3/objects/{object_type}/search", json=payload, timeout=LONG_TIMEOUT
        )
        return response.json().get("results", [])


def _auth_error_category(error: HTTPError) -> Optional[str]:
    response = error.response
    if response is None or response.status_code != HTTPStatus.UNAUTHORIZED:
        return None
    try:
        return response.json().get("category")
    except ValueError:
        return None
