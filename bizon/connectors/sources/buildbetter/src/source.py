"""BuildBetter source: calls, signals, people, companies, clusters and tags from the BuildBetter GraphQL API (Hasura).

Keyset pagination on id; `people` also supports incremental sync on (updated_at, id).
"""

import json
from datetime import timedelta, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.graphql import (
    DEFAULT_TRANSIENT_ERROR_TYPES,
    DEFAULT_TRANSIENT_MESSAGE_SIGNATURES,
    DEFAULT_TYPE_FIELDS,
    ErrorClass,
    errors_match,
    graphql_post,
)
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

GRAPHQL_URL = "https://api.buildbetter.app/v1/graphql"
PAGE_SIZE = 500
# A row committed after the last run's start can carry an earlier updated_at. Deduped downstream on id.
INCREMENTAL_LOOKBACK = timedelta(hours=1)

# Hasura auth-webhook / Postgres failures, on top of the shared generic signatures. Everything else stays
# FATAL so a genuine query bug fails fast.
_TRANSIENT_GRAPHQL_MESSAGE_SIGNATURES = (
    "webhook authentication request failed",
    "connection error",
    *DEFAULT_TRANSIENT_MESSAGE_SIGNATURES,
)
# Checked before the substring signatures: a query bug naming a `timeout` field must not retry.
_FATAL_HASURA_CODES = ("validation-failed", "parse-failed")


def _classify_buildbetter_graphql(response, body: dict) -> Optional[ErrorClass]:
    """Non-OK statuses are left unclaimed so the session adapter's 5xx retries aren't doubled."""
    if not response.ok:
        return None
    errors = body.get("errors") or []
    if not errors:
        return None
    if errors_match(errors, types=_FATAL_HASURA_CODES, type_fields=DEFAULT_TYPE_FIELDS):
        return ErrorClass.FATAL
    if errors_match(errors, types=("RATE_LIMITED", "RATELIMITED"), type_fields=DEFAULT_TYPE_FIELDS):
        return ErrorClass.RATE_LIMITED
    if errors_match(
        errors,
        types=DEFAULT_TRANSIENT_ERROR_TYPES,
        message_signatures=_TRANSIENT_GRAPHQL_MESSAGE_SIGNATURES,
        type_fields=DEFAULT_TYPE_FIELDS,
    ):
        return ErrorClass.TRANSIENT
    return ErrorClass.FATAL


CALLS_QUERY = """
query Calls($limit: Int!, $cursor: bigint!) {
  interview(
    limit: $limit,
    order_by: { id: asc },
    where: { id: { _gt: $cursor } }
  ) {
    id
    name
    original_name
    started_at
    completed_at
    recorded_at
    asset_duration_seconds
    asset_url
    interaction
    source
    summary
    short_summary
    organization_id
    type_id
    created_at
    updated_at
    attendees {
      person {
        id
        first_name
        last_name
        email
        title
        company_id
      }
      speaker
    }
    tags {
      tag {
        id
        name
        color
      }
    }
  }
}
"""

SIGNALS_QUERY = """
query Signals($limit: Int!, $cursor: bigint!) {
  extraction(
    limit: $limit,
    order_by: { id: asc },
    where: { id: { _gt: $cursor } }
  ) {
    id
    interview_id
    conversation_id
    feedback_id
    summary
    context
    sentiment
    severity
    bias
    speaker
    start_sec
    end_sec
    start_seq
    end_seq
    interaction_id
    user_updated
    created_at
    exact_quote { text start_sec end_sec }
    types { type { name } }
    topics { topic { text } }
    keywords { keyword { text } }
    emotions { emotion { name } }
    impacts { impact { name } }
    attendee {
      person {
        id
        first_name
        last_name
        email
        title
        company_id
      }
    }
  }
}
"""

_PERSON_FIELDS = """
    id
    external_id
    first_name
    last_name
    email
    title
    department
    avatar_url
    company_id
    persona_id
    organization_id
    source
    source_identifier
    created_at
    updated_at
    company {
      name
      domain
    }
    persona {
      name
    }
"""

PEOPLE_QUERY = (
    """
query People($limit: Int!, $cursor: bigint!) {
  person(
    limit: $limit,
    order_by: { id: asc },
    where: { id: { _gt: $cursor } }
  ) {"""
    + _PERSON_FIELDS
    + """  }
}
"""
)

# person.updated_at is a naive `timestamp`: `timestamptz!` fails Hasura validation. The (updated_at, id)
# tie-break keeps rows sharing an updated_at from being skipped at a page boundary.
PEOPLE_INCREMENTAL_QUERY = (
    """
query PeopleIncremental($limit: Int!, $updated_at: timestamp!, $id: bigint!) {
  person(
    limit: $limit,
    order_by: [{ updated_at: asc }, { id: asc }],
    where: {
      _or: [
        { updated_at: { _gt: $updated_at } },
        { updated_at: { _eq: $updated_at }, id: { _gt: $id } }
      ]
    }
  ) {"""
    + _PERSON_FIELDS
    + """  }
}
"""
)

COMPANIES_QUERY = """
query Companies($limit: Int!, $cursor: bigint!) {
  company(
    limit: $limit,
    order_by: { id: asc },
    where: { id: { _gt: $cursor } }
  ) {
    id
    name
    domain
    photo_url
    created_at
    updated_at
    metadata_records {
      record {
        values {
          value
          field {
            name
          }
        }
      }
    }
  }
}
"""

CLUSTERS_QUERY = """
query Clusters($limit: Int!, $cursor: bigint!) {
  folder(
    limit: $limit,
    order_by: { id: asc },
    where: { id: { _gt: $cursor }, name: { _neq: "" } }
  ) {
    id
    name
    creator_id
    organization_id
    is_subfolder
    from_project
    permission
    created_at
    updated_at
    items(limit: 1000) {
      id
      extraction_id
      interview_id
      created_at
    }
  }
}
"""

TAGS_QUERY = """
query Tags($limit: Int!, $cursor: bigint!) {
  tag(
    limit: $limit,
    order_by: { id: asc },
    where: { id: { _gt: $cursor } }
  ) {
    id
    name
    description
    color
    created_automatically
    organization_id
    created_at
  }
}
"""

CONNECTION_CHECK_QUERY = """
query { interview(limit: 1) { id } }
"""


class BuildBetterStreams(str, Enum):
    CALLS = "calls"
    SIGNALS = "signals"
    PEOPLE = "people"
    COMPANIES = "companies"
    CLUSTERS = "clusters"
    TAGS = "tags"


class BuildBetterSourceConfig(SourceConfig):
    stream: BuildBetterStreams
    api_key: str
    company_metadata_fields: Dict[str, str] = Field(
        default_factory=dict,
        description="Company custom fields to lift into top-level columns, as {column: BuildBetter field name}. "
        "Every custom field is also kept under `metadata`.",
    )
    # graphql_post logs a non-2xx body before raising, so the session must not raise first.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


class BuildBetterAuth(AuthBase):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["X-Buildbetter-API-Key"] = self.api_key
        request.headers["Content-Type"] = "application/json"
        return request


class BuildBetterSource(AbstractSource):
    def __init__(self, config: BuildBetterSourceConfig):
        super().__init__(config)
        self.config: BuildBetterSourceConfig = config

    def get_authenticator(self) -> AuthBase:
        return BuildBetterAuth(self.config.api_key)

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in BuildBetterStreams]

    @staticmethod
    def get_config_class() -> type:
        return BuildBetterSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[str]]:
        try:
            result = self._graphql_post(CONNECTION_CHECK_QUERY)
            if result.get("data", {}).get("interview") is not None:
                return True, None
            return False, "No data returned from connection check"
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _graphql_post(self, query: str, variables: dict = None) -> dict:
        return graphql_post(
            self.session,
            GRAPHQL_URL,
            query,
            variables,
            classify=_classify_buildbetter_graphql,
            log_prefix="BuildBetter",
        )

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream
        if stream == BuildBetterStreams.CALLS:
            return self._get_calls(pagination)
        if stream == BuildBetterStreams.SIGNALS:
            return self._get_signals(pagination)
        if stream == BuildBetterStreams.PEOPLE:
            return self._get_people(pagination)
        if stream == BuildBetterStreams.COMPANIES:
            return self._get_companies(pagination)
        if stream == BuildBetterStreams.CLUSTERS:
            return self._get_clusters(pagination)
        if stream == BuildBetterStreams.TAGS:
            return self._get_tags(pagination)
        raise NotImplementedError(f"Stream '{stream}' is not implemented")

    @staticmethod
    def _keyset_pagination(rows: list, page_size: int) -> dict:
        if len(rows) >= page_size:
            return {"cursor": rows[-1]["id"]}
        return {}

    @staticmethod
    def _cursor_from(pagination: dict | None) -> int:
        if pagination and pagination.get("cursor"):
            return pagination["cursor"]
        return 0

    def _get_calls(self, pagination: dict = None) -> SourceIteration:
        cursor = self._cursor_from(pagination)
        result = self._graphql_post(CALLS_QUERY, {"limit": PAGE_SIZE, "cursor": cursor})
        rows = result["data"]["interview"]

        records = [SourceRecord(id=str(row["id"]), data=self._transform_call(row)) for row in rows]

        next_pagination = self._keyset_pagination(rows, PAGE_SIZE)
        logger.info(f"Fetched {len(records)} calls (cursor={cursor}, has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    @staticmethod
    def _transform_call(row: dict) -> dict:
        attendees = [
            {
                "person_id": a["person"]["id"],
                "first_name": a["person"].get("first_name"),
                "last_name": a["person"].get("last_name"),
                "email": a["person"].get("email"),
                "title": a["person"].get("title"),
                "company_id": a["person"].get("company_id"),
                "speaker": a.get("speaker"),
            }
            for a in row.get("attendees", [])
            if a.get("person")
        ]
        tags = [
            {
                "id": t["tag"]["id"],
                "name": t["tag"].get("name"),
                "color": t["tag"].get("color"),
            }
            for t in row.get("tags", [])
            if t.get("tag")
        ]
        return {
            "id": row["id"],
            "name": row.get("name"),
            "original_name": row.get("original_name"),
            "started_at": row.get("started_at"),
            "completed_at": row.get("completed_at"),
            "recorded_at": row.get("recorded_at"),
            "asset_duration_seconds": row.get("asset_duration_seconds"),
            "asset_url": row.get("asset_url"),
            "interaction": row.get("interaction"),
            "source": row.get("source"),
            "summary": row.get("summary"),
            "short_summary": row.get("short_summary"),
            "organization_id": row.get("organization_id"),
            "type_id": row.get("type_id"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "attendees": json.dumps(attendees) if attendees else None,
            "tags": json.dumps(tags) if tags else None,
        }

    def _get_signals(self, pagination: dict = None) -> SourceIteration:
        cursor = self._cursor_from(pagination)
        result = self._graphql_post(SIGNALS_QUERY, {"limit": PAGE_SIZE, "cursor": cursor})
        rows = result["data"]["extraction"]

        records = [SourceRecord(id=str(row["id"]), data=self._transform_signal(row)) for row in rows]

        next_pagination = self._keyset_pagination(rows, PAGE_SIZE)
        logger.info(f"Fetched {len(records)} signals (cursor={cursor}, has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    @staticmethod
    def _transform_signal(row: dict) -> dict:
        types = [t["type"]["name"] for t in row.get("types", []) if t.get("type")]
        topics = [t["topic"]["text"] for t in row.get("topics", []) if t.get("topic")]
        keywords = [k["keyword"]["text"] for k in row.get("keywords", []) if k.get("keyword")]
        emotions = [e["emotion"]["name"] for e in row.get("emotions", []) if e.get("emotion")]
        impacts = [i["impact"]["name"] for i in row.get("impacts", []) if i.get("impact")]

        attendee = row.get("attendee") or {}
        person = attendee.get("person") or {}
        speaker_name = None
        if person.get("first_name") or person.get("last_name"):
            speaker_name = f"{person.get('first_name', '')} {person.get('last_name', '')}".strip()

        # exact_quote may be a JSONB array or object
        exact_quote = row.get("exact_quote")
        if exact_quote is not None and not isinstance(exact_quote, str):
            exact_quote = json.dumps(exact_quote)

        return {
            "id": row["id"],
            "interview_id": row.get("interview_id"),
            "conversation_id": row.get("conversation_id"),
            "feedback_id": row.get("feedback_id"),
            "summary": row.get("summary"),
            "context": row.get("context"),
            "sentiment": row.get("sentiment"),
            "severity": row.get("severity"),
            "bias": row.get("bias"),
            "speaker": row.get("speaker"),
            "start_sec": row.get("start_sec"),
            "end_sec": row.get("end_sec"),
            "start_seq": row.get("start_seq"),
            "end_seq": row.get("end_seq"),
            "interaction_id": row.get("interaction_id"),
            "user_updated": row.get("user_updated"),
            "created_at": row.get("created_at"),
            "exact_quote": exact_quote,
            "types": json.dumps(types) if types else None,
            "topics": json.dumps(topics) if topics else None,
            "keywords": json.dumps(keywords) if keywords else None,
            "emotions": json.dumps(emotions) if emotions else None,
            "impacts": json.dumps(impacts) if impacts else None,
            "speaker_person_id": person.get("id"),
            "speaker_name": speaker_name,
            "speaker_email": person.get("email"),
            "speaker_title": person.get("title"),
            "speaker_company_id": person.get("company_id"),
        }

    def _get_people(self, pagination: dict = None) -> SourceIteration:
        cursor = self._cursor_from(pagination)
        result = self._graphql_post(PEOPLE_QUERY, {"limit": PAGE_SIZE, "cursor": cursor})
        rows = result["data"]["person"]

        records = [SourceRecord(id=str(row["id"]), data=self._transform_person(row)) for row in rows]

        next_pagination = self._keyset_pagination(rows, PAGE_SIZE)
        logger.info(f"Fetched {len(records)} people (cursor={cursor}, has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        if self.config.stream != BuildBetterStreams.PEOPLE:
            raise NotImplementedError(f"Incremental sync not supported for stream '{self.config.stream.value}'")

        if pagination:
            updated_at, last_id = pagination["updated_at"], pagination["id"]
        else:
            last_run = source_state.last_run
            if last_run.tzinfo is not None:
                last_run = last_run.astimezone(timezone.utc).replace(tzinfo=None)
            updated_at, last_id = (last_run - INCREMENTAL_LOOKBACK).isoformat(), 0

        result = self._graphql_post(
            PEOPLE_INCREMENTAL_QUERY, {"limit": PAGE_SIZE, "updated_at": updated_at, "id": last_id}
        )
        rows = result["data"]["person"]

        records = [SourceRecord(id=str(row["id"]), data=self._transform_person(row)) for row in rows]

        next_pagination = {"updated_at": rows[-1]["updated_at"], "id": rows[-1]["id"]} if len(rows) >= PAGE_SIZE else {}
        logger.info(
            f"Fetched {len(records)} people (updated_at>={updated_at}, id>{last_id}, has_more={bool(next_pagination)})"
        )
        return SourceIteration(records=records, next_pagination=next_pagination)

    @staticmethod
    def _transform_person(row: dict) -> dict:
        company = row.get("company") or {}
        persona = row.get("persona") or {}
        return {
            "id": row["id"],
            "external_id": row.get("external_id"),
            "first_name": row.get("first_name"),
            "last_name": row.get("last_name"),
            "email": row.get("email"),
            "title": row.get("title"),
            "department": row.get("department"),
            "avatar_url": row.get("avatar_url"),
            "company_id": row.get("company_id"),
            "persona_id": row.get("persona_id"),
            "organization_id": row.get("organization_id"),
            "source": row.get("source"),
            "source_identifier": row.get("source_identifier"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "company_name": company.get("name"),
            "company_domain": company.get("domain"),
            "persona_name": persona.get("name"),
        }

    def _get_companies(self, pagination: dict = None) -> SourceIteration:
        cursor = self._cursor_from(pagination)
        result = self._graphql_post(COMPANIES_QUERY, {"limit": PAGE_SIZE, "cursor": cursor})
        rows = result["data"]["company"]

        records = [
            SourceRecord(id=str(row["id"]), data=self._transform_company(row, self.config.company_metadata_fields))
            for row in rows
        ]

        next_pagination = self._keyset_pagination(rows, PAGE_SIZE)
        logger.info(f"Fetched {len(records)} companies (cursor={cursor}, has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    @staticmethod
    def _transform_company(row: dict, metadata_fields: Dict[str, str]) -> dict:
        metadata = {}
        for mr in row.get("metadata_records", []):
            record = mr.get("record") or {}
            for v in record.get("values", []):
                field = v.get("field") or {}
                field_name = field.get("name")
                if field_name:
                    metadata[field_name] = v.get("value")

        company = {
            "id": row["id"],
            "name": row.get("name"),
            "domain": row.get("domain"),
            "photo_url": row.get("photo_url"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "metadata": metadata,
        }
        for column, field_name in metadata_fields.items():
            company[column] = metadata.get(field_name)
        return company

    def _get_clusters(self, pagination: dict = None) -> SourceIteration:
        cursor = self._cursor_from(pagination)
        result = self._graphql_post(CLUSTERS_QUERY, {"limit": PAGE_SIZE, "cursor": cursor})
        rows = result["data"]["folder"]

        records = [SourceRecord(id=str(row["id"]), data=self._transform_cluster(row)) for row in rows]

        next_pagination = self._keyset_pagination(rows, PAGE_SIZE)
        logger.info(f"Fetched {len(records)} clusters (cursor={cursor}, has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    @staticmethod
    def _transform_cluster(row: dict) -> dict:
        items = [
            {
                "id": item["id"],
                "extraction_id": item.get("extraction_id"),
                "interview_id": item.get("interview_id"),
                "created_at": item.get("created_at"),
            }
            for item in row.get("items", [])
        ]
        item_count = len(items)
        return {
            "id": row["id"],
            "name": row.get("name"),
            "creator_id": row.get("creator_id"),
            "organization_id": row.get("organization_id"),
            "is_subfolder": row.get("is_subfolder"),
            "from_project": row.get("from_project"),
            "permission": row.get("permission"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "items": json.dumps(items) if items else None,
            "item_count": item_count,
        }

    def _get_tags(self, pagination: dict = None) -> SourceIteration:
        cursor = self._cursor_from(pagination)
        result = self._graphql_post(TAGS_QUERY, {"limit": PAGE_SIZE, "cursor": cursor})
        rows = result["data"]["tag"]

        records = [SourceRecord(id=str(row["id"]), data=row) for row in rows]

        next_pagination = self._keyset_pagination(rows, PAGE_SIZE)
        logger.info(f"Fetched {len(records)} tags (cursor={cursor}, has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)
