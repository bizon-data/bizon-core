"""Harness FME (formerly Split.io) source: feature-flag configuration and segment membership from the
Admin API v2 (https://docs.split.io/reference). The API has no reliable updated-since filter, so every
stream is full refresh.
"""

import time
from enum import Enum
from typing import List, Optional, Tuple

import requests
from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, HttpRetryConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.split.io/internal/api/v2"

# Both list endpoints cap at 50 regardless of what's requested (passing 100
# silently returns 50) -- hardcode it rather than exposing a config knob that
# would silently do nothing above 50.
LIST_PAGE_SIZE = 50
# segment keys endpoint caps at 100.
KEYS_PAGE_SIZE = 100

DEFAULT_RATE_LIMIT_WAIT_SECONDS = 3.0
MAX_RATE_LIMIT_RETRIES = 6


class HarnessFmeStreams(str, Enum):
    FEATURE_FLAGS = "feature_flags"
    FEATURE_FLAG_DEFINITIONS = "feature_flag_definitions"
    SEGMENTS = "segments"
    SEGMENT_KEYS = "segment_keys"


class HarnessFmeSourceConfig(SourceConfig):
    """Configuration for the Harness FME (Split.io) source connector."""

    stream: HarnessFmeStreams

    api_key: str = Field(..., description="Harness service account token (SAT), sent as x-api-key")

    workspace_id: str = Field(..., description="FME project (workspace) id, e.g. the 'Default' project")

    environments: List[str] = Field(
        default_factory=lambda: ["Prod-Default"],
        description="Environment NAMES to sync (resolved to ids at runtime via GET /environments/ws/{ws}). "
        "Used by feature_flag_definitions and segment_keys.",
    )

    # 429 is left out of the retries: _request_json waits on the API's own rate-limit reset headers,
    # which urllib3 cannot read, so the session must hand the 429 back instead of raising.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(
            raise_for_status=False,
            retries=HttpRetryConfig(status_forcelist=[500, 502, 503, 504], allowed_methods=["GET"]),
        ),
        description="HTTP timeout/retry policy",
    )


class HarnessFmeAuth(AuthBase):
    """Harness service-account token auth: x-api-key header, NOT Authorization: Bearer.

    Bearer is only understood by the legacy Split Admin API; here it returns a misleading 404 instead of 401.
    """

    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["x-api-key"] = self.api_key
        return request


def _safe_float(value: Optional[str]) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


class HarnessFmeSource(AbstractSource):
    """Harness FME (Split.io) source connector."""

    def __init__(self, config: HarnessFmeSourceConfig):
        super().__init__(config)
        self.config: HarnessFmeSourceConfig = config
        self._environment_cache: Optional[List[Tuple[str, str]]] = None
        self.session.headers.update({"Accept": "application/json"})

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in HarnessFmeStreams]

    @staticmethod
    def get_config_class() -> type:
        return HarnessFmeSourceConfig

    def get_authenticator(self) -> Optional[AuthBase]:
        return HarnessFmeAuth(self.config.api_key)

    def check_connection(self) -> Tuple[bool, Optional[str]]:
        try:
            self._resolve_environment_ids()
            return True, None
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _request_json(self, path: str, params: Optional[dict] = None) -> dict:
        """GET against the Harness FME API using self.session, with explicit
        429 handling: sleep for max(x-ratelimit-reset-seconds-ip,
        x-ratelimit-reset-seconds-org, default 3s) and retry."""
        url = f"{BASE_URL}{path}"
        for attempt in range(MAX_RATE_LIMIT_RETRIES):
            response = self.session.get(url, params=params, timeout=30)
            if response.status_code == 429:
                wait_seconds = max(
                    _safe_float(response.headers.get("x-ratelimit-reset-seconds-ip"))
                    or DEFAULT_RATE_LIMIT_WAIT_SECONDS,
                    _safe_float(response.headers.get("x-ratelimit-reset-seconds-org"))
                    or DEFAULT_RATE_LIMIT_WAIT_SECONDS,
                    DEFAULT_RATE_LIMIT_WAIT_SECONDS,
                )
                logger.warning(
                    f"Harness FME rate limited on {path} (attempt {attempt + 1}/{MAX_RATE_LIMIT_RETRIES}); "
                    f"sleeping {wait_seconds}s"
                )
                time.sleep(wait_seconds)
                continue
            response.raise_for_status()
            return response.json()
        raise RuntimeError(f"Harness FME: exhausted rate-limit retries for {path}")

    def _resolve_environment_ids(self) -> List[Tuple[str, str]]:
        """Resolve configured environment NAMES to (name, id) pairs at
        runtime via GET /environments/ws/{ws}. Never hardcode environment
        ids in config -- they differ per workspace."""
        if self._environment_cache is not None:
            return self._environment_cache

        data = self._request_json(f"/environments/ws/{self.config.workspace_id}")
        objects = data.get("objects", data) if isinstance(data, dict) else data
        by_name = {obj["name"]: obj["id"] for obj in objects}

        resolved = []
        for name in self.config.environments:
            if name not in by_name:
                raise ValueError(
                    f"Harness FME: environment '{name}' not found in workspace {self.config.workspace_id}. "
                    f"Available: {sorted(by_name)}"
                )
            resolved.append((name, by_name[name]))

        self._environment_cache = resolved
        return resolved

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream
        if stream == HarnessFmeStreams.FEATURE_FLAGS:
            return self._get_feature_flags(pagination)
        elif stream == HarnessFmeStreams.FEATURE_FLAG_DEFINITIONS:
            return self._get_feature_flag_definitions(pagination)
        elif stream == HarnessFmeStreams.SEGMENTS:
            return self._get_segments(pagination)
        elif stream == HarnessFmeStreams.SEGMENT_KEYS:
            return self._get_segment_keys(pagination)
        raise ValueError(f"Unknown stream: {stream}")

    def _get_feature_flags(self, pagination: dict = None) -> SourceIteration:
        offset = (pagination or {}).get("offset", 0)
        data = self._request_json(
            f"/splits/ws/{self.config.workspace_id}",
            params={"limit": LIST_PAGE_SIZE, "offset": offset},
        )
        objects = data.get("objects", [])
        records = [SourceRecord(id=str(obj["id"]), data=obj) for obj in objects]

        total = data.get("totalCount", 0)
        new_offset = offset + len(objects)
        next_pagination = {"offset": new_offset} if objects and new_offset < total else {}

        logger.info(f"Harness FME feature_flags: fetched {len(records)} (offset={offset}, total={total})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _get_segments(self, pagination: dict = None) -> SourceIteration:
        offset = (pagination or {}).get("offset", 0)
        data = self._request_json(
            f"/segments/ws/{self.config.workspace_id}",
            params={"limit": LIST_PAGE_SIZE, "offset": offset},
        )
        objects = data.get("objects", [])
        records = [SourceRecord(id=str(obj["name"]), data=obj) for obj in objects]

        total = data.get("totalCount", 0)
        new_offset = offset + len(objects)
        next_pagination = {"offset": new_offset} if objects and new_offset < total else {}

        logger.info(f"Harness FME segments: fetched {len(records)} (offset={offset}, total={total})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _get_feature_flag_definitions(self, pagination: dict = None) -> SourceIteration:
        """The bulk per-environment endpoint: one paginated walk per environment instead of one call per flag."""
        state = pagination or {}
        env_idx = state.get("env_idx", 0)
        offset = state.get("offset", 0)

        envs = self._resolve_environment_ids()
        if env_idx >= len(envs):
            return SourceIteration(records=[], next_pagination={})

        env_name, env_id = envs[env_idx]
        data = self._request_json(
            f"/splits/ws/{self.config.workspace_id}/environments/{env_id}",
            params={"limit": LIST_PAGE_SIZE, "offset": offset},
        )
        objects = data.get("objects", [])
        records = [SourceRecord(id=f"{env_id}:{obj['name']}", data=obj) for obj in objects]

        total = data.get("totalCount", 0)
        new_offset = offset + len(objects)
        if objects and new_offset < total:
            next_pagination = {"env_idx": env_idx, "offset": new_offset}
        else:
            # Done with this environment -- advance. If this was the last
            # environment, the next get() call sees env_idx >= len(envs) and
            # terminates with {}.
            next_pagination = {"env_idx": env_idx + 1, "offset": 0}

        logger.info(
            f"Harness FME feature_flag_definitions: fetched {len(records)} for env '{env_name}' "
            f"(offset={offset}, total={total})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _get_segment_keys(self, pagination: dict = None) -> SourceIteration:
        """Driven from each environment's activated-segments list, not the project-wide `segments`
        list: a segment not activated in an environment 404s on the keys call. One iteration holds
        all keys of one segment, which keeps iterations bounded however many keys there are in total.
        """
        state = pagination or {}
        env_idx = state.get("env_idx", 0)
        segment_page_offset = state.get("segment_page_offset", 0)
        segment_names: List[str] = list(state.get("segment_names", []))

        envs = self._resolve_environment_ids()

        # Refill the segment_names queue, walking environments forward,
        # until we have a segment to process or have exhausted every
        # configured environment. Bounded by len(envs) -- never infinite.
        while env_idx < len(envs) and not segment_names:
            env_name, env_id = envs[env_idx]
            data = self._request_json(
                f"/segments/ws/{self.config.workspace_id}/environments/{env_id}",
                params={"limit": LIST_PAGE_SIZE, "offset": segment_page_offset},
            )
            objects = data.get("objects", [])
            if objects:
                segment_names = [obj["name"] for obj in objects]
                segment_page_offset += len(objects)
            else:
                env_idx += 1
                segment_page_offset = 0

        if env_idx >= len(envs):
            return SourceIteration(records=[], next_pagination={})

        env_name, env_id = envs[env_idx]
        segment = segment_names.pop(0)

        keys: List[str] = []
        key_offset = 0
        while True:
            try:
                data = self._request_json(
                    f"/segments/{env_id}/{segment}/keys",
                    params={"limit": KEYS_PAGE_SIZE, "offset": key_offset},
                )
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    logger.warning(
                        f"Harness FME: segment '{segment}' not found in environment '{env_name}' ({env_id}) "
                        "on the keys endpoint (404) -- skipping"
                    )
                    keys = []
                    break
                raise

            page_keys = [item["key"] for item in data.get("keys", [])]
            keys.extend(page_keys)
            key_offset += len(page_keys)
            if not page_keys or key_offset >= data.get("count", 0):
                break

        records = [
            SourceRecord(
                id=f"{env_id}:{segment}:{key}",
                data={
                    "environment_id": env_id,
                    "environment_name": env_name,
                    "segment_name": segment,
                    "key": key,
                },
            )
            for key in keys
        ]

        next_pagination = {
            "env_idx": env_idx,
            "segment_page_offset": segment_page_offset,
            "segment_names": segment_names,
        }

        logger.info(
            f"Harness FME segment_keys: fetched {len(records)} keys for segment '{segment}' in env '{env_name}' "
            f"({len(segment_names)} segments left in this page)"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)
