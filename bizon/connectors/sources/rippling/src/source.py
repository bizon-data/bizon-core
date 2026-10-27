"""Rippling HR platform source (REST v2): workers, org structure, compensation and leave."""

from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://rest.ripplingapis.com"

# Without a read timeout a stalled socket never becomes a retryable event.
REQUEST_TIMEOUT = (10, 60)

# v2 caps `limit` at 100 (400 above).
PAGE_LIMIT = 100


class RipplingStreams(str, Enum):
    EMPLOYEES = "employees"
    LEVELS = "levels"
    LEAVE_REQUESTS = "leave_requests"
    LEAVE_BALANCES = "leave_balances"
    COMPANY_LEAVE_TYPES = "company_leave_types"
    LEAVE_ACCRUALS = "leave_accruals"
    DEPARTMENTS = "departments"
    TEAMS = "teams"
    COMPANIES = "companies"
    COMPENSATIONS = "compensations"
    USERS = "users"
    WORK_LOCATIONS = "work_locations"
    LEGAL_ENTITIES = "legal_entities"
    EMPLOYMENT_TYPES = "employment_types"
    TITLES = "titles"
    JOB_FUNCTIONS = "job_functions"
    TRACKS = "tracks"
    CUSTOM_FIELDS = "custom_fields"


@dataclass(frozen=True)
class StreamSpec:
    path: str  # dashes and a trailing slash
    expand: Optional[str] = None


STREAM_SPECS: dict[RipplingStreams, StreamSpec] = {
    RipplingStreams.EMPLOYEES: StreamSpec(
        path="/workers/",
        expand=(
            "user,manager,department,teams,level,employment_type,job_function,legal_entity,compensation,custom_fields"
        ),
    ),
    RipplingStreams.LEVELS: StreamSpec(path="/levels/", expand="parent,track"),
    RipplingStreams.LEAVE_REQUESTS: StreamSpec(path="/leave-requests/"),
    RipplingStreams.LEAVE_BALANCES: StreamSpec(path="/leave-balances/"),
    RipplingStreams.COMPANY_LEAVE_TYPES: StreamSpec(path="/leave-types/"),
    RipplingStreams.LEAVE_ACCRUALS: StreamSpec(path="/leave-accruals/"),
    RipplingStreams.DEPARTMENTS: StreamSpec(path="/departments/", expand="parent,department_hierarchy"),
    RipplingStreams.TEAMS: StreamSpec(path="/teams/", expand="parent"),
    RipplingStreams.COMPANIES: StreamSpec(path="/companies/", expand="parent_legal_entity,legal_entities"),
    RipplingStreams.COMPENSATIONS: StreamSpec(path="/compensations/", expand="worker"),
    RipplingStreams.USERS: StreamSpec(path="/users/"),
    RipplingStreams.WORK_LOCATIONS: StreamSpec(path="/work-locations/"),
    RipplingStreams.LEGAL_ENTITIES: StreamSpec(path="/legal-entities/"),
    RipplingStreams.EMPLOYMENT_TYPES: StreamSpec(path="/employment-types/"),
    RipplingStreams.TITLES: StreamSpec(path="/titles/"),
    RipplingStreams.JOB_FUNCTIONS: StreamSpec(path="/job-functions/"),
    RipplingStreams.TRACKS: StreamSpec(path="/tracks/"),
    RipplingStreams.CUSTOM_FIELDS: StreamSpec(path="/custom-fields/"),
}
assert set(STREAM_SPECS) == set(RipplingStreams), "every stream needs a StreamSpec"


class RipplingSourceConfig(SourceConfig):
    stream: RipplingStreams
    api_key: str
    expand: Optional[str] = Field(None, description="Override the stream's default `expand` parameter")
    # check_connection reports the status and body of a non-2xx response, and _fetch_page logs it before raising.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


class RipplingAuth(AuthBase):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.api_key}"
        return request


class RipplingSource(AbstractSource):
    def __init__(self, config: RipplingSourceConfig):
        super().__init__(config)
        self.config: RipplingSourceConfig = config

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in RipplingStreams]

    @staticmethod
    def get_config_class() -> type:
        return RipplingSourceConfig

    def get_authenticator(self) -> AuthBase:
        return RipplingAuth(self.config.api_key)

    def _url(self, spec: StreamSpec) -> str:
        return f"{BASE_URL}{spec.path}"

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        spec = STREAM_SPECS[self.config.stream]
        try:
            response = self.session.get(self._url(spec), params={"limit": 1}, timeout=REQUEST_TIMEOUT)
            if response.ok:
                return True, None
            return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _fetch_page(self, url: str, params: Optional[dict] = None) -> Tuple[list, Optional[str]]:
        response = (
            self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if params is not None
            else self.session.get(url, timeout=REQUEST_TIMEOUT)
        )
        if not response.ok:
            logger.error(f"Rippling API error {response.status_code}: {response.text}")
            response.raise_for_status()
        data = response.json()
        return data.get("results", []), data.get("next_link")

    def get(self, pagination: Optional[dict] = None) -> SourceIteration:
        stream = self.config.stream
        spec = STREAM_SPECS[stream]

        if pagination and pagination.get("next_link"):
            # next_link is absolute and already carries limit/expand, so it is followed verbatim.
            records_raw, next_link = self._fetch_page(pagination["next_link"])
        else:
            expand = self.config.expand if self.config.expand is not None else spec.expand
            params = {"limit": PAGE_LIMIT}
            if expand:
                params["expand"] = expand
            records_raw, next_link = self._fetch_page(self._url(spec), params)

        records = [SourceRecord(id=str(r["id"]), data=r) for r in records_raw]
        next_pagination = {"next_link": next_link} if next_link else {}

        logger.info(f"Fetched {len(records)} {stream.value} (has_more={bool(next_link)})")
        return SourceIteration(records=records, next_pagination=next_pagination)
