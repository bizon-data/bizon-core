"""Vitally source: REST resources (accounts, users, notes, custom objects, ...) plus the app custom-fields API."""

from enum import Enum
from typing import Any, List, Optional, Tuple

from pydantic import Field
from requests.auth import AuthBase, HTTPBasicAuth

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

REST_BASE = "https://rest.vitally.io/resources/"
APP_BASE = "https://app.vitally.io/v1/"
TIMEOUT = (10, 60)
# The REST default sort is updatedAt desc, so a record updated mid-crawl jumps behind the
# cursor and is skipped. createdAt never changes; Vitally requires it on every page.
REST_SORT = {"sortBy": "createdAt"}


class VitallyStreams(str, Enum):
    ACCOUNTS = "accounts"
    ADMINS = "admins"
    USERS = "users"
    CUSTOM_FIELDS = "custom_fields"
    CUSTOM_OBJECTS = "custom_objects"
    CUSTOM_OBJECTS_INSTANCES = "custom_objects_instances"
    NOTES = "notes"
    PROJECTS = "projects"
    ACCOUNT_CUSTOM_FIELDS = "account_custom_fields"
    USER_CUSTOM_FIELDS = "user_custom_fields"


# The customFields metadata endpoint has no cursor, so each model is walked once.
_CUSTOM_FIELDS_MODELS = ["users", "accounts"]

_APP_MODEL_BY_STREAM = {
    VitallyStreams.ACCOUNT_CUSTOM_FIELDS.value: "Customer",
    VitallyStreams.USER_CUSTOM_FIELDS.value: "User",
}

_APP_STREAMS = {VitallyStreams.ACCOUNT_CUSTOM_FIELDS.value, VitallyStreams.USER_CUSTOM_FIELDS.value}


class VitallySourceConfig(SourceConfig):
    api_key: Optional[str] = Field(
        None, description="rest.vitally.io Basic-auth key, used by every stream except the *_custom_fields ones"
    )
    # app.vitally.io issues no API key for this endpoint: this is a short-lived browser session token.
    app_api_key: Optional[str] = Field(
        None, description="app.vitally.io Bearer token for account_custom_fields and user_custom_fields"
    )
    account_status: str = Field("activeOrChurned", description="`status` filter for the accounts stream")
    http: Optional[HttpConfig] = Field(default_factory=HttpConfig, description="HTTP timeout/retry policy")


class VitallySource(AbstractSource):
    def __init__(self, config: VitallySourceConfig):
        super().__init__(config)
        self.config: VitallySourceConfig = config

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in VitallyStreams]

    @staticmethod
    def get_config_class() -> type:
        return VitallySourceConfig

    def get_authenticator(self) -> Optional[AuthBase]:
        if self.config.stream in _APP_STREAMS:
            if not self.config.app_api_key:
                raise ValueError(f"stream {self.config.stream!r} requires app_api_key")
            self.session.headers.update({"Authorization": f"Bearer {self.config.app_api_key}"})
            return None
        if not self.config.api_key:
            raise ValueError(f"stream {self.config.stream!r} requires api_key")
        return HTTPBasicAuth(self.config.api_key, "")

    def check_connection(self) -> Tuple[bool, Optional[str]]:
        try:
            if self.config.stream in _APP_STREAMS:
                model = _APP_MODEL_BY_STREAM[self.config.stream]
                resp = self.session.get(
                    f"{APP_BASE}customFields/paginated",
                    params={"limit": 1, "includeHidden": "true", "model": model},
                    timeout=TIMEOUT,
                )
            else:
                resp = self.session.get(f"{REST_BASE}accounts", params={"limit": 1}, timeout=TIMEOUT)
            resp.raise_for_status()
            return True, None
        except Exception as e:  # noqa: BLE001 -- surfaced to the caller as (False, reason)
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def get(self, pagination: dict = None) -> SourceIteration:
        pagination = pagination or {}
        stream = self.config.stream

        if stream == VitallyStreams.ACCOUNTS.value:
            return self._get_rest_page("accounts", pagination, {"status": self.config.account_status})
        if stream == VitallyStreams.ADMINS.value:
            return self._get_rest_page("admins", pagination)
        if stream == VitallyStreams.USERS.value:
            return self._get_rest_page("users", pagination)
        if stream == VitallyStreams.NOTES.value:
            return self._get_rest_page("notes", pagination)
        if stream == VitallyStreams.PROJECTS.value:
            return self._get_rest_page("projects", pagination)
        if stream == VitallyStreams.CUSTOM_OBJECTS.value:
            return self._get_rest_page("customObjects", pagination)
        if stream == VitallyStreams.CUSTOM_OBJECTS_INSTANCES.value:
            return self._get_custom_objects_instances_page(pagination)
        if stream == VitallyStreams.CUSTOM_FIELDS.value:
            return self._get_custom_fields_page(pagination)
        if stream in _APP_STREAMS:
            return self._get_app_custom_fields_page(_APP_MODEL_BY_STREAM[stream], pagination)
        raise ValueError(f"unknown vitally stream {stream!r}")

    def _get_rest_page(self, path: str, pagination: dict, extra_params: Optional[dict] = None) -> SourceIteration:
        params: dict[str, Any] = {"limit": 100, **REST_SORT, **(extra_params or {})}
        cursor = pagination.get("from")
        if cursor:
            params["from"] = cursor
        resp = self.session.get(f"{REST_BASE}{path}", params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        records = [SourceRecord(id=str(r["id"]), data=r) for r in data.get("results", [])]
        next_cursor = data.get("next")
        next_pagination = {"from": next_cursor} if next_cursor else {}
        return SourceIteration(next_pagination=next_pagination, records=records)

    # custom_objects_instances fans out over every custom object id, each with its own cursor.
    def _list_custom_object_ids(self) -> List[str]:
        ids: List[str] = []
        cursor = None
        while True:
            params: dict[str, Any] = {"limit": 100, **REST_SORT}
            if cursor:
                params["from"] = cursor
            resp = self.session.get(f"{REST_BASE}customObjects", params=params, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            ids.extend(str(r["id"]) for r in data.get("results", []))
            cursor = data.get("next")
            if not cursor:
                return ids

    def _get_custom_objects_instances_page(self, pagination: dict) -> SourceIteration:
        if "current" not in pagination:
            ids = self._list_custom_object_ids()
            if not ids:
                return SourceIteration(next_pagination={}, records=[])
            queue, current, cursor = ids[1:], ids[0], None
        else:
            queue = list(pagination.get("queue") or [])
            current = pagination["current"]
            cursor = pagination.get("cursor")

        params: dict[str, Any] = {"limit": 100, **REST_SORT}
        if cursor:
            params["from"] = cursor
        resp = self.session.get(f"{REST_BASE}customObjects/{current}/instances", params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        records = [SourceRecord(id=str(r["id"]), data=r) for r in data.get("results", [])]

        next_cursor = data.get("next")
        if next_cursor:
            next_pagination = {"queue": queue, "current": current, "cursor": next_cursor}
        elif queue:
            next_pagination = {"queue": queue[1:], "current": queue[0], "cursor": None}
        else:
            next_pagination = {}
        return SourceIteration(next_pagination=next_pagination, records=records)

    def _get_custom_fields_page(self, pagination: dict) -> SourceIteration:
        index = pagination.get("model_index", 0)
        model = _CUSTOM_FIELDS_MODELS[index]
        resp = self.session.get(f"{REST_BASE}customFields", params={"model": model}, timeout=TIMEOUT)
        resp.raise_for_status()
        records = []
        for r in resp.json():
            r["object"] = model
            records.append(SourceRecord(id=str(r["id"]), data=r))
        next_index = index + 1
        next_pagination = {"model_index": next_index} if next_index < len(_CUSTOM_FIELDS_MODELS) else {}
        return SourceIteration(next_pagination=next_pagination, records=records)

    def _get_app_custom_fields_page(self, model: str, pagination: dict) -> SourceIteration:
        params: dict[str, Any] = {"limit": 100, "includeHidden": "true", "model": model}
        cursor = pagination.get("from")
        if cursor:
            params["from"] = cursor
        resp = self.session.get(f"{APP_BASE}customFields/paginated", params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        records = [SourceRecord(id=str(r["id"]), data=r) for r in data.get("results", [])]
        next_cursor = data.get("next")
        next_pagination = {"from": next_cursor} if next_cursor else {}
        return SourceIteration(next_pagination=next_pagination, records=records)
