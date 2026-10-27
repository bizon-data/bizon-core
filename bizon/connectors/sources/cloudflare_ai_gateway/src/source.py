"""Incremental source for Cloudflare AI Gateway Logpush NDJSON files delivered to a GCS bucket."""

# No `from __future__ import annotations`: a custom source_file_path loads this file without registering it
# in sys.modules, so pydantic could not resolve stringified annotations naming module-local types.

import gzip
import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage
from loguru import logger
from pydantic import Field, model_validator

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

GZIP_MAGIC = b"\x1f\x8b"
CONNECT_TIMEOUT_SECONDS = 10
SOURCE_KEYS = ("bucket", "objectName", "generation", "rowNumber")
_FRACTION = re.compile(r"\.(\d+)")


class CloudflareAiGatewayStreams(str, Enum):
    LOGS = "logs"


class CloudflareAiGatewaySourceConfig(SourceConfig):
    stream: CloudflareAiGatewayStreams
    bucket_name: str = Field(min_length=1)
    prefix: str = ""
    page_size: int = Field(default=1000, ge=1, le=1000)
    lookback_seconds: int = Field(default=300, ge=0)
    request_timeout_seconds: int = Field(default=60, ge=1)
    list_fields: str | None = Field(
        default=None,
        description="objects.list partial-response projection. Needs items(name,generation,updated,size).",
    )
    download_threads: int = Field(
        default=8,
        ge=1,
        le=64,
        description="Threads fetching one iteration's objects. The storage client's "
        "HTTP pool holds 10 connections, so more than that only churns them.",
    )
    max_iteration_bytes: int = Field(
        default=64_000_000,
        ge=0,
        description="Stop taking objects once this many listed bytes are gathered. 0 "
        "disables. bizon needs one iteration's records to fit in buffer_size, and "
        "objects run to ~18MB, so the worst iteration is this budget plus one object.",
    )

    @model_validator(mode="after")
    def _list_fields_must_request_the_page_token(self):
        # Without nextPageToken the iterator can't say whether the bucket has more
        # objects, so the run would report success after the first page.
        if self.list_fields and "nextPageToken" not in self.list_fields:
            raise ValueError("list_fields must include nextPageToken")
        return self


class CloudflareAiGatewaySource(AbstractSource):
    def __init__(self, config: CloudflareAiGatewaySourceConfig):
        super().__init__(config)
        self.config: CloudflareAiGatewaySourceConfig = config
        self.client = storage.Client()

    @staticmethod
    def streams() -> list[str]:
        return [stream.value for stream in CloudflareAiGatewayStreams]

    @staticmethod
    def get_config_class() -> type[CloudflareAiGatewaySourceConfig]:
        return CloudflareAiGatewaySourceConfig

    def get_authenticator(self):
        return None

    def check_connection(self) -> tuple[bool, Any | None]:
        try:
            blobs = self.client.list_blobs(
                self.config.bucket_name,
                prefix=self.config.prefix or None,
                page_size=1,
                max_results=1,
                timeout=self._timeout,
                **self._list_options(),
            )
            next(iter(blobs), None)
            return True, None
        except Exception as error:
            message = (
                f"GCS listing failed for gs://{self.config.bucket_name}/{self.config.prefix} ({type(error).__name__})"
            )
            return False, message

    def get_total_records_count(self) -> int | None:
        return None

    def get(self, pagination: dict | None = None) -> SourceIteration:
        return self._get_page(pagination or {}, cutoff=None)

    def get_records_after(
        self,
        source_state: SourceIncrementalState,
        pagination: dict | None = None,
    ) -> SourceIteration:
        pagination = pagination or {}
        if "cutoff" in pagination:
            cutoff = self._parse_cutoff(pagination["cutoff"])
        else:
            last_run = self._as_utc(source_state.last_run)
            cutoff = last_run - timedelta(seconds=self.config.lookback_seconds)
        return self._get_page(pagination, cutoff=cutoff)

    def _get_page(self, pagination: dict, cutoff: datetime | None) -> SourceIteration:
        # Pagination is the last object name taken, not a page token: the byte budget
        # can end an iteration mid-page, and a name survives the 30-day lifecycle
        # deleting objects under us where a positional offset into the page would not.
        after = pagination.get("after")
        iterator = self.client.list_blobs(
            self.config.bucket_name,
            prefix=self.config.prefix or None,
            # start_offset is inclusive, so `after` comes back and takes one slot.
            page_size=self.config.page_size + (1 if after else 0),
            start_offset=after,
            timeout=self._timeout,
            **self._list_options(),
        )
        # .pages is a one-shot property; take the generator once and pull from it.
        page = next(iterator.pages, None)
        listed = list(page) if page is not None else []

        budget = self.config.max_iteration_bytes
        wanted = []
        gathered = 0
        taken = after
        stopped_early = False
        for blob in listed:
            if blob.name == after:
                continue
            if budget and gathered >= budget:
                stopped_early = True
                break
            taken = blob.name
            if blob.name.endswith("/") or blob.size == 0:
                continue
            if cutoff is not None:
                if blob.updated is None:
                    raise self._object_error(blob, "has no updated timestamp")
                if self._as_utc(blob.updated) < cutoff:
                    continue
            wanted.append(blob)
            gathered += blob.size or 0

        records: list[SourceRecord] = []
        if wanted:
            with ThreadPoolExecutor(max_workers=min(self.config.download_threads, len(wanted))) as pool:
                for blob_records in pool.map(self._read_blob, wanted):
                    records.extend(blob_records)

        if not (stopped_early or iterator.next_page_token):
            return SourceIteration(records=records, next_pagination={})
        next_pagination: dict[str, str] = {"after": taken}
        if cutoff is not None:
            next_pagination["cutoff"] = cutoff.isoformat()
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _read_blob(self, blob) -> list[SourceRecord]:
        if blob.generation is None:
            raise self._object_error(blob, "has no generation")
        try:
            body = blob.download_as_bytes(
                if_generation_match=blob.generation,
                timeout=self._timeout,
            )
        except (NotFound, PreconditionFailed):
            logger.warning(
                "Skipping GCS object that was deleted or replaced after listing: {}",
                self._gcs_uri(blob),
            )
            return []
        return self._records_for_body(blob, body)

    def _records_for_body(self, blob, body: bytes) -> list[SourceRecord]:
        uri = self._gcs_uri(blob)
        if body[:2] == GZIP_MAGIC:
            try:
                body = gzip.decompress(body)
            except (OSError, EOFError) as error:
                raise ValueError(f"{uri} is not a valid gzip payload") from error

        updated = self._required_updated(blob)
        object_meta = {
            "uri": uri,
            "generation": str(blob.generation),
            "updated": updated.isoformat(),
        }
        records = []
        for line_number, line in enumerate(body.split(b"\n"), start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except ValueError as error:
                raise ValueError(f"{uri}:{line_number} is not valid JSON") from error
            if not isinstance(row, dict):
                raise ValueError(f"{uri}:{line_number} is not a JSON object")
            records.append(
                SourceRecord(
                    id=self._row_id(row, uri, line_number),
                    data={**row, "gcs": {**object_meta, "line_number": line_number}},
                    timestamp=self._event_timestamp(row, updated),
                )
            )
        return records

    @staticmethod
    def _row_id(row: dict, uri: str, line_number: int) -> str:
        source = row.get("source")
        if not isinstance(source, dict) or any(source.get(key) is None for key in SOURCE_KEYS):
            raise ValueError(f"{uri}:{line_number} is missing source.{{{','.join(SOURCE_KEYS)}}}")
        return f"gs://{source['bucket']}/{source['objectName']}#{source['generation']}:{source['rowNumber']}"

    @classmethod
    def _event_timestamp(cls, row: dict, fallback: datetime) -> datetime:
        raw = row.get("eventTimestamp")
        if isinstance(raw, str):
            try:
                return cls._as_utc(cls._fromisoformat(raw))
            except ValueError:
                pass
        return fallback

    @property
    def _timeout(self) -> tuple[int, int]:
        return (CONNECT_TIMEOUT_SECONDS, self.config.request_timeout_seconds)

    def _list_options(self) -> dict[str, str]:
        options = {}
        if self.config.list_fields:
            options["fields"] = self.config.list_fields
        return options

    def _required_updated(self, blob) -> datetime:
        if blob.updated is None:
            raise self._object_error(blob, "has no updated timestamp")
        return self._as_utc(blob.updated)

    def _gcs_uri(self, blob) -> str:
        return f"gs://{self.config.bucket_name}/{blob.name}"

    def _object_error(self, blob, message: str) -> ValueError:
        return ValueError(f"GCS object {self._gcs_uri(blob)} {message}")

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _fromisoformat(value: str) -> datetime:
        # Before Python 3.11, fromisoformat rejects a trailing Z and fractions that are not 3 or 6 digits.
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        value = _FRACTION.sub(lambda m: "." + m.group(1)[:6].ljust(6, "0"), value, count=1)
        return datetime.fromisoformat(value)

    @classmethod
    def _parse_cutoff(cls, value: Any) -> datetime:
        if not isinstance(value, str):
            raise TypeError("incremental pagination cutoff must be an ISO-8601 string")
        try:
            return cls._as_utc(cls._fromisoformat(value))
        except ValueError as error:
            raise ValueError("incremental pagination cutoff must be an ISO-8601 datetime") from error
