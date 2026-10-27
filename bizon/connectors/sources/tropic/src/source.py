from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests import Response
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

DEFAULT_BASE_URL = "https://app.tropicapp.io"
DEFAULT_PAGE_SIZE = 50
DEFAULT_REQUEST_TIMEOUT_SECONDS = 120
CONTRACTS_PATH = "/api/v1/contracts"


class TropicStreams(str, Enum):
    CONTRACTS = "contracts"


class TropicSourceConfig(SourceConfig):
    stream: TropicStreams
    api_key: str
    base_url: str = DEFAULT_BASE_URL
    page_size: int = Field(default=DEFAULT_PAGE_SIZE, ge=1, le=200)
    request_timeout_seconds: int = Field(default=DEFAULT_REQUEST_TIMEOUT_SECONDS, ge=10, le=600)
    # Error bodies are logged before raising, so the session must hand non-2xx responses back.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


class TropicAuth(AuthBase):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.api_key}"
        request.headers["Accept"] = "application/json"
        return request


class TropicSource(AbstractSource):
    def __init__(self, config: TropicSourceConfig):
        super().__init__(config)
        self.config: TropicSourceConfig = config
        self.base_url = self.config.base_url.rstrip("/")

    def get_authenticator(self) -> AuthBase:
        return TropicAuth(self.config.api_key)

    @staticmethod
    def streams() -> List[str]:
        return [stream.value for stream in TropicStreams]

    @staticmethod
    def get_config_class() -> type:
        return TropicSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response_json = self._request_json(
                CONTRACTS_PATH,
                params={"page[per_page]": 1},
            )
            if not isinstance(response_json.get("data"), list):
                return False, "Tropic contracts response did not include a data list"
            return True, None
        except Exception as exc:
            return False, str(exc)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _request_json(self, path_or_url: str, params: dict | None = None) -> dict:
        url = path_or_url if path_or_url.startswith("http") else f"{self.base_url}{path_or_url}"
        response = self.session.get(
            url,
            params=params,
            timeout=(30, self.config.request_timeout_seconds),
        )
        return self._decode_json_response(response)

    @staticmethod
    def _decode_json_response(response: Response) -> dict:
        try:
            payload = response.json()
        except Exception as exc:
            body_preview = response.text[:1_000]
            raise RuntimeError(f"Tropic response was not valid JSON: {body_preview}") from exc

        if not response.ok:
            body_preview = response.text[:1_000]
            logger.error(f"Tropic API error {response.status_code}: {body_preview}")
            response.raise_for_status()

        if not isinstance(payload, dict):
            raise RuntimeError(f"Tropic response must be a JSON object, got {type(payload).__name__}")

        return payload

    def get_contracts(self, pagination: dict | None = None) -> SourceIteration:
        next_path = pagination.get("next_path") if pagination else None

        if next_path:
            response_json = self._request_json(next_path)
        else:
            response_json = self._request_json(
                CONTRACTS_PATH,
                params={"page[per_page]": self.config.page_size},
            )

        records_json = response_json.get("data") or []
        pagination_metadata = response_json.get("pagination_metadata") or {}
        next_pagination = {}
        next_path = pagination_metadata.get("next")
        if next_path:
            next_pagination = {"next_path": next_path}

        records = [SourceRecord(id=str(record["id"]), data=record) for record in records_json]

        logger.info(
            f"Fetched {len(records)} Tropic contracts "
            f"(has_more={bool(next_pagination)}, total={pagination_metadata.get('total_record_count')})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        if self.config.stream == TropicStreams.CONTRACTS:
            return self.get_contracts(pagination)

        raise NotImplementedError(f"Stream '{self.config.stream}' is not implemented for Tropic")
