"""Lattice source: users, departments, feedback, review cycles, reviewees, reviews and questions."""

import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.latticehq.com/v1"

REQUEST_TIMEOUT = (10, 60)

# Lattice rejects limit > 100 (400).
PAGE_LIMIT = 100

# 240 requests per rolling minute; pace proactively rather than lean on 429 retries.
RATE_LIMIT_FLOOR = 5
MAX_RETRY_AFTER_SECONDS = 120

# /users hides everything but ACTIVE unless `status` is passed, so walk every accepted status.
USER_STATUSES = ["ACTIVE", "DEACTIVATED", "CREATED", "INVITED"]


class LatticeStreams(str, Enum):
    DEPARTMENTS = "departments"
    FEEDBACKS = "feedbacks"
    REVIEW_CYCLES = "review_cycles"
    REVIEWEES = "reviewees"
    REVIEWS = "reviews"
    QUESTIONS = "questions"
    USERS = "users"


def _ref_id(value: Any) -> Optional[str]:
    return value.get("id") if isinstance(value, dict) else None


# Each projection lands a fixed column set. Free text (review comments) and personal attributes (birthDate,
# gender) are deliberately not carried.
def _project_department(r: dict) -> dict:
    return {k: r.get(k) for k in ("id", "name", "description", "createdAt")}


def _project_user(r: dict) -> dict:
    return {
        "id": r["id"],
        "name": r.get("name"),
        "preferredName": r.get("preferredName"),
        "email": r.get("email"),
        "title": r.get("title"),
        "status": r.get("status"),
        "startDate": r.get("startDate"),
        "timezone": r.get("timezone"),
        "isAdmin": r.get("isAdmin"),
        "createdAt": r.get("createdAt"),
        "updatedAt": r.get("updatedAt"),
        "manager_id": _ref_id(r.get("manager")),
        "department_id": _ref_id(r.get("department")),
    }


def _project_feedback(r: dict) -> dict:
    return {
        "id": r["id"],
        "author_id": _ref_id(r.get("author")),
        "body": r.get("body"),
        "visibility": r.get("visibility"),
        "isPublic": r.get("isPublic"),
        "targets": r.get("targets"),
        "competency": r.get("competency"),
        "feedbackRequest": r.get("feedbackRequest"),
        "associatedValues": r.get("associatedValues"),
        "createdAt": r.get("createdAt"),
    }


def _project_review_cycle(r: dict) -> dict:
    keys = (
        "id",
        "name",
        "stage",
        "createdAt",
        "updatedAt",
        "peerSelectionStartedAt",
        "reviewsStartedAt",
        "reviewsEndedAt",
    )
    return {k: r.get(k) for k in keys}


def _project_reviewee(r: dict, cycle_id: str) -> dict:
    return {
        "id": r["id"],
        "user_id": _ref_id(r.get("user")),
        "review_cycle_id": _ref_id(r.get("reviewCycle")) or cycle_id,
        "createdAt": r.get("createdAt"),
        "updatedAt": r.get("updatedAt"),
        "closedAt": r.get("closedAt"),
        "esignatureGivenAt": r.get("esignatureGivenAt"),
    }


def _project_review(r: dict) -> dict:
    response = r.get("response") or {}
    return {
        "id": r["id"],
        "reviewType": r.get("reviewType"),
        "question_id": _ref_id(r.get("question")),
        "reviewee_id": _ref_id(r.get("reviewee")),
        "reviewer_id": _ref_id(r.get("reviewer")),
        "rating": response.get("rating"),
        "choices": response.get("choices"),
        "submittedAt": r.get("submittedAt"),
        "calibrationEnded": r.get("calibrationEnded"),
    }


def _project_question(r: dict) -> dict:
    return {k: r.get(k) for k in ("id", "body", "description", "ratingScale", "hasFreeFormText")}


FLAT_STREAMS: Dict[LatticeStreams, Tuple[str, Callable[[dict], dict]]] = {
    LatticeStreams.DEPARTMENTS: ("/departments", _project_department),
    LatticeStreams.FEEDBACKS: ("/feedbacks", _project_feedback),
    LatticeStreams.REVIEW_CYCLES: ("/reviewCycles", _project_review_cycle),
}


class LatticeSourceConfig(SourceConfig):
    stream: LatticeStreams
    api_key: str
    # Server-side `onlyPublic=true` on /feedbacks, which returns fewer rows than filtering on `isPublic`.
    only_public_feedback: bool = True
    # Non-2xx bodies are reported before raising.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


class LatticeAuth(AuthBase):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.api_key}"
        return request


class LatticeSource(AbstractSource):
    def __init__(self, config: LatticeSourceConfig):
        super().__init__(config)
        self.config: LatticeSourceConfig = config
        self._cycle_ids: Optional[List[str]] = None
        # Lives as long as the run; a fresh instance would only re-emit a question row.
        self._seen_question_ids: set = set()

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in LatticeStreams]

    @staticmethod
    def get_config_class() -> type:
        return LatticeSourceConfig

    def get_authenticator(self) -> AuthBase:
        return LatticeAuth(self.config.api_key)

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response = self.session.get(f"{BASE_URL}/departments", params={"limit": 1}, timeout=REQUEST_TIMEOUT)
            if response.ok:
                return True, None
            return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _get_json(self, path: str, params: Optional[dict] = None) -> dict:
        response = self.session.get(f"{BASE_URL}{path}", params=params, timeout=REQUEST_TIMEOUT)
        if not response.ok:
            logger.error(f"Lattice API error {response.status_code} on {path}: {response.text}")
            response.raise_for_status()

        remaining = response.headers.get("x-ratelimit-remaining")
        reset = response.headers.get("x-ratelimit-reset")
        if remaining is not None and reset is not None and int(remaining) <= RATE_LIMIT_FLOOR:
            wait = min(max(int(reset) - time.time(), 0) + 1, MAX_RETRY_AFTER_SECONDS)
            logger.info(f"Lattice rate limit nearly spent ({remaining} left), sleeping {wait:.0f}s")
            time.sleep(wait)

        return response.json()

    def _page(self, path: str, after: Optional[str], extra: Optional[dict] = None) -> Tuple[List[dict], Optional[str]]:
        params: Dict[str, Any] = {"limit": PAGE_LIMIT}
        if extra:
            params.update(extra)
        if after:
            params["startingAfter"] = after
        body = self._get_json(path, params)
        items = body.get("data", [])
        # endingCursor is also populated on the final page; hasMore is the terminator.
        next_after = body.get("endingCursor") if body.get("hasMore") else None
        return items, next_after

    def _all_cycle_ids(self) -> List[str]:
        if self._cycle_ids is None:
            ids: List[str] = []
            after = None
            while True:
                items, after = self._page("/reviewCycles", after)
                ids.extend(str(i["id"]) for i in items)
                if not after:
                    break
            # Sorted so a cycle index in a cursor means the same cycle on any instance.
            self._cycle_ids = sorted(set(ids))
            logger.info(f"Loaded {len(self._cycle_ids)} review cycles")
        return self._cycle_ids

    def _records(self, rows: List[dict], project: Callable[[dict], dict]) -> List[SourceRecord]:
        out = []
        for row in rows:
            data = project(row)
            out.append(SourceRecord(id=str(data["id"]), data=data))
        return out

    def _get_flat(self, path: str, project: Callable[[dict], dict], pagination: Optional[dict]) -> SourceIteration:
        after = (pagination or {}).get("after")
        extra = (
            {"onlyPublic": "true"}
            if (self.config.stream == LatticeStreams.FEEDBACKS and self.config.only_public_feedback)
            else None
        )
        rows, next_after = self._page(path, after, extra)
        logger.info(f"Fetched {len(rows)} {self.config.stream.value} (has_more={bool(next_after)})")
        return SourceIteration(
            records=self._records(rows, project),
            next_pagination={"after": next_after} if next_after else {},
        )

    def _get_users(self, pagination: Optional[dict]) -> SourceIteration:
        pagination = pagination or {}
        status_index = pagination.get("status_index", 0)
        rows, next_after = self._page("/users", pagination.get("after"), {"status": USER_STATUSES[status_index]})
        logger.info(
            f"Fetched {len(rows)} users with status {USER_STATUSES[status_index]} (has_more={bool(next_after)})"
        )

        if next_after:
            next_pagination: dict = {"status_index": status_index, "after": next_after}
        elif status_index + 1 < len(USER_STATUSES):
            next_pagination = {"status_index": status_index + 1}
        else:
            next_pagination = {}
        return SourceIteration(records=self._records(rows, _project_user), next_pagination=next_pagination)

    def _cycle_page(self, pagination: Optional[dict], suffix: str) -> Tuple[str, List[dict], dict]:
        pagination = pagination or {}
        cycle_ids = self._all_cycle_ids()
        if not cycle_ids:
            return "", [], {}
        index = pagination.get("cycle", 0)
        cycle_id = cycle_ids[index]
        rows, next_after = self._page(f"/reviewCycle/{cycle_id}/{suffix}", pagination.get("after"))

        if next_after:
            next_pagination: dict = {"cycle": index, "after": next_after}
        elif index + 1 < len(cycle_ids):
            next_pagination = {"cycle": index + 1}
        else:
            next_pagination = {}
        logger.info(
            f"Fetched {len(rows)} {suffix} from cycle {index + 1}/{len(cycle_ids)} (has_more={bool(next_after)})"
        )
        return cycle_id, rows, next_pagination

    def _get_reviewees(self, pagination: Optional[dict]) -> SourceIteration:
        cycle_id, rows, next_pagination = self._cycle_page(pagination, "reviewees")
        records = self._records(rows, lambda r: _project_reviewee(r, cycle_id))
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _get_reviews(self, pagination: Optional[dict]) -> SourceIteration:
        _, rows, next_pagination = self._cycle_page(pagination, "reviews")
        return SourceIteration(records=self._records(rows, _project_review), next_pagination=next_pagination)

    def _get_questions(self, pagination: Optional[dict]) -> SourceIteration:
        # No list endpoint for questions: fetch each distinct one seen in reviews. Dedup on id downstream.
        _, rows, next_pagination = self._cycle_page(pagination, "reviews")
        new_ids = []
        for row in rows:
            question_id = _ref_id(row.get("question"))
            if question_id and question_id not in self._seen_question_ids:
                self._seen_question_ids.add(question_id)
                new_ids.append(question_id)

        questions = [self._get_json(f"/question/{qid}") for qid in new_ids]
        return SourceIteration(records=self._records(questions, _project_question), next_pagination=next_pagination)

    def get(self, pagination: Optional[dict] = None) -> SourceIteration:
        stream = self.config.stream
        if stream in FLAT_STREAMS:
            path, project = FLAT_STREAMS[stream]
            return self._get_flat(path, project, pagination)
        if stream == LatticeStreams.USERS:
            return self._get_users(pagination)
        if stream == LatticeStreams.REVIEWEES:
            return self._get_reviewees(pagination)
        if stream == LatticeStreams.REVIEWS:
            return self._get_reviews(pagination)
        if stream == LatticeStreams.QUESTIONS:
            return self._get_questions(pagination)
        raise ValueError(f"Unknown Lattice stream: {stream}")
