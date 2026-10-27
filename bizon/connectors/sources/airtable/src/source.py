"""Airtable source: one table per stream, flattened to sanitised column names with the full field set per record.

Column names, the `_airtable_*` metadata columns and the stringified field types follow a widely used Airtable
export layout, so existing downstream models can read the output unchanged.
"""

import re
from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests import Response
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

DEFAULT_BASE_URL = "https://api.airtable.com/v0"
DEFAULT_PAGE_SIZE = 100
DEFAULT_REQUEST_TIMEOUT_SECONDS = 120

# Types whose API value is a list/dict but which the export layout stores as a STRING holding the Python repr
# (single-quoted), not JSON. A downstream JSON_VALUE on a nested object yields NULL, so the repr has to be
# produced here or the column silently empties out. Every other type is left as the API returns it.
STRINGIFIED_FIELD_TYPES = frozenset(
    {"multipleAttachments", "button", "createdBy", "lastModifiedBy", "singleCollaborator"}
)

_INVALID_COLUMN_CHAR_RE = re.compile(r"[^0-9a-zA-Z_]")


def _sanitize_field_name(raw_name: str) -> str:
    """Lowercase, then replace every char outside [0-9a-zA-Z_] with `_` one-for-one: no run-collapsing, no
    camelCase splitting, so column names stay stable against the export layout."""
    name = _INVALID_COLUMN_CHAR_RE.sub("_", raw_name.lower())
    if name and name[0].isdigit():
        name = f"_{name}"
    return name


def _is_auth_error(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    return response is not None and getattr(response, "status_code", None) in (401, 403)


class AirtableStreams(str, Enum):
    TABLE = "table"


class AirtableSourceConfig(SourceConfig):
    stream: AirtableStreams
    api_token: str
    base_id: str
    table_id: str
    table_name: Optional[str] = Field(
        default=None, description="Overrides _airtable_table_name; defaults to the Meta API name"
    )
    view: Optional[str] = None
    # Non-2xx bodies are logged before raising, and a non-JSON body raises its own error.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )
    page_size: int = Field(default=DEFAULT_PAGE_SIZE, ge=1, le=100)
    request_timeout_seconds: int = Field(default=DEFAULT_REQUEST_TIMEOUT_SECONDS, ge=10, le=600)
    base_url: str = DEFAULT_BASE_URL
    ai_text_as_string: bool = Field(
        default=False,
        description="Emit aiText as its Python repr (a STRING column) instead of the raw {state, value, isStale} "
        "object (a JSON column)",
    )


class AirtableAuth(AuthBase):
    def __init__(self, api_token: str):
        self.api_token = api_token

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.api_token}"
        request.headers["Accept"] = "application/json"
        return request


class AirtableSource(AbstractSource):
    def __init__(self, config: AirtableSourceConfig):
        super().__init__(config)
        self.config: AirtableSourceConfig = config
        self.base_url = self.config.base_url.rstrip("/")
        self._schema_cache: Optional[dict] = None

    def get_authenticator(self) -> AuthBase:
        return AirtableAuth(self.config.api_token)

    @staticmethod
    def streams() -> List[str]:
        return [stream.value for stream in AirtableStreams]

    @staticmethod
    def get_config_class() -> type:
        return AirtableSourceConfig

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _request_json(self, path_or_url: str, params: dict | None = None) -> dict:
        url = path_or_url if path_or_url.startswith("http") else f"{self.base_url}{path_or_url}"
        response = self.session.get(
            url,
            params=params,
            timeout=(10, self.config.request_timeout_seconds),
        )
        return self._decode_json_response(response)

    @staticmethod
    def _decode_json_response(response: Response) -> dict:
        try:
            payload = response.json()
        except Exception as exc:
            body_preview = response.text[:1_000]
            raise RuntimeError(f"Airtable response was not valid JSON: {body_preview}") from exc

        if not response.ok:
            body_preview = response.text[:1_000]
            logger.error(f"Airtable API error {response.status_code}: {body_preview}")
            response.raise_for_status()

        if not isinstance(payload, dict):
            raise RuntimeError(f"Airtable response must be a JSON object, got {type(payload).__name__}")

        return payload

    def _schema(self) -> dict:
        """GET /meta/bases/{base_id}/tables, cached on the instance. Returns
        {"table_name": str, "fields": [(raw_name, airtable_type, column_name), ...]}
        in Meta-API field order for self.config.table_id -- this is what lets
        the source emit the full field set per record even for rows where
        Airtable omitted an empty field from `fields`."""
        if self._schema_cache is not None:
            return self._schema_cache

        payload = self._request_json(f"/meta/bases/{self.config.base_id}/tables")
        tables = payload.get("tables") or []
        table = next((t for t in tables if t.get("id") == self.config.table_id), None)
        if table is None:
            available = [f"{t.get('id')} ({t.get('name')})" for t in tables]
            raise ValueError(
                f"Airtable table {self.config.table_id!r} not found in base "
                f"{self.config.base_id!r}. Available tables: {available}"
            )

        fields: List[Tuple[str, str, str]] = []
        seen: dict[str, int] = {}
        for field in table.get("fields") or []:
            raw_name = field["name"]
            airtable_type = field["type"]
            column_name = _sanitize_field_name(raw_name)
            if column_name in seen:
                seen[column_name] += 1
                deduped = f"{column_name}_{seen[column_name]}"
                logger.warning(
                    f"airtable: duplicate column name {raw_name!r} (sanitised {column_name!r}) "
                    f"-- renamed to {deduped!r}"
                )
                column_name = deduped
            else:
                seen[column_name] = 0
            fields.append((raw_name, airtable_type, column_name))

        self._schema_cache = {
            "table_name": self.config.table_name or table.get("name"),
            "fields": fields,
        }
        return self._schema_cache

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            self._schema()
        except Exception as exc:
            if _is_auth_error(exc):
                return False, (
                    f"{type(exc).__name__}: {exc} -- check that the Airtable PAT holds the schema.bases:read scope."
                )
            return False, f"{type(exc).__name__}: {exc}"

        try:
            self._request_json(
                f"/{self.config.base_id}/{self.config.table_id}",
                params={"pageSize": 1},
            )
        except Exception as exc:
            if _is_auth_error(exc):
                return False, (
                    f"{type(exc).__name__}: {exc} -- check that the Airtable PAT holds the data.records:read scope."
                )
            return False, f"{type(exc).__name__}: {exc}"

        return True, None

    def _shape_record(self, rec: dict, fields: List[Tuple[str, str, str]], table_name: str) -> dict:
        raw_fields = rec.get("fields") or {}
        data: dict[str, Any] = {}
        for raw_name, airtable_type, column_name in fields:
            value = raw_fields.get(raw_name)
            stringify = airtable_type in STRINGIFIED_FIELD_TYPES or (
                airtable_type == "aiText" and self.config.ai_text_as_string
            )
            if stringify and value is not None:
                value = str(value)
            data[column_name] = value
        data["_airtable_id"] = rec["id"]
        data["_airtable_created_time"] = rec["createdTime"]
        data["_airtable_table_name"] = table_name
        return data

    def get(self, pagination: dict = None) -> SourceIteration:
        schema = self._schema()
        fields = schema["fields"]
        table_name = schema["table_name"]

        params: dict[str, Any] = {"pageSize": self.config.page_size}
        if self.config.view:
            params["view"] = self.config.view
        offset = (pagination or {}).get("offset")
        if offset:
            params["offset"] = offset

        payload = self._request_json(f"/{self.config.base_id}/{self.config.table_id}", params=params)
        records_json = payload.get("records") or []

        records = [
            SourceRecord(id=str(rec["id"]), data=self._shape_record(rec, fields, table_name)) for rec in records_json
        ]

        tok = payload.get("offset")
        next_pagination = {"offset": tok} if tok else {}

        logger.info(
            f"Fetched {len(records)} Airtable records from {self.config.base_id}/{self.config.table_id} "
            f"(has_more={bool(next_pagination)})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)
