"""Wistia video hosting source: medias, viewing events and per-media daily stats."""

from datetime import date, timedelta
from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.wistia.com/v1"
PER_PAGE = 100
TIMEOUT = (10, 60)


class WistiaStreams(str, Enum):
    EVENTS = "events"
    MEDIAS = "medias"
    STATS_MEDIA = "stats_media"


class WistiaSourceConfig(SourceConfig):
    stream: WistiaStreams
    api_key: str
    start_date: str = Field(..., description="First day (YYYY-MM-DD) fetched for events and stats_media")
    # events.json filters start_date by day only, so a plain last_run watermark can drop events landing near a
    # day boundary between runs. Re-fetching a trailing window is safe: records are keyed on event_key.
    lookback_days: int = 2
    # check_connection reports a non-2xx response, and by_date stats answer a 400 for media without playback stats.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


class WistiaAuth(AuthBase):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self.api_key}"
        return request


class WistiaSource(AbstractSource):
    def __init__(self, config: WistiaSourceConfig):
        super().__init__(config)
        self.config: WistiaSourceConfig = config

    def get_authenticator(self) -> AuthBase:
        return WistiaAuth(self.config.api_key)

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in WistiaStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return WistiaSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response = self.session.get(f"{BASE_URL}/medias.json", params={"per_page": 1}, timeout=TIMEOUT)
            if response.ok:
                return True, None
            return False, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _get_json(self, path: str, params: dict) -> Any:
        response = self.session.get(f"{BASE_URL}{path}", params=params, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()

    def _get_by_date_stats(self, hashed_id: str, start_date: str, end_date: str) -> list:
        response = self.session.get(
            f"{BASE_URL}/stats/medias/{hashed_id}/by_date.json",
            params={"start_date": start_date, "end_date": end_date},
            timeout=TIMEOUT,
        )
        if response.status_code == 400:
            # Non-video media (e.g. type=Image) have no playback stats and always 400 here.
            logger.warning(f"Wistia by_date stats unavailable for media {hashed_id} (400) — skipping")
            return []
        response.raise_for_status()
        return response.json()

    def get_medias(self, pagination: dict = None) -> SourceIteration:
        page = (pagination or {}).get("page", 1)
        data = self._get_json("/medias.json", {"page": page, "per_page": PER_PAGE})
        records = [SourceRecord(id=str(m["hashed_id"]), data=m) for m in data]
        next_pagination = {"page": page + 1} if len(data) == PER_PAGE else {}
        logger.info(f"Fetched {len(records)} medias (page={page}, has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_events(self, pagination: dict = None, start_date: str = None, end_date: str = None) -> SourceIteration:
        page = (pagination or {}).get("page", 1)
        params = {"page": page, "per_page": PER_PAGE, "start_date": start_date}
        if end_date:
            params["end_date"] = end_date
        data = self._get_json("/stats/events.json", params)
        records = [SourceRecord(id=str(ev["event_key"]), data=ev) for ev in data]
        next_pagination = {"page": page + 1} if len(data) == PER_PAGE else {}
        logger.info(f"Fetched {len(records)} events (page={page}, has_more={bool(next_pagination)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _load_all_media_hashed_ids(self) -> list:
        hashed_ids = []
        page = 1
        while True:
            data = self._get_json("/medias.json", {"page": page, "per_page": PER_PAGE})
            hashed_ids.extend(m["hashed_id"] for m in data)
            if len(data) < PER_PAGE:
                break
            page += 1
        return hashed_ids

    def get_stats_media(self, pagination: dict = None) -> SourceIteration:
        if not pagination or "remaining_media_hashed_ids" not in pagination:
            hashed_ids = self._load_all_media_hashed_ids()
            logger.info(f"Loaded {len(hashed_ids)} media hashed_ids for stats_media")
            if not hashed_ids:
                return SourceIteration(records=[], next_pagination={})
            pagination = {"remaining_media_hashed_ids": hashed_ids}

        remaining = pagination["remaining_media_hashed_ids"]
        records = []

        # One media per call, skipping past media with no day rows so an iteration is never empty.
        while remaining:
            hashed_id = remaining.pop(0)
            day_rows = self._get_by_date_stats(hashed_id, self.config.start_date, date.today().isoformat())
            for row in day_rows:
                records.append(SourceRecord(id=f"{hashed_id}_{row['date']}", data={**row, "hashed_id": hashed_id}))
            if records:
                break

        next_pagination = {"remaining_media_hashed_ids": remaining} if remaining else {}
        logger.info(f"Fetched {len(records)} stats_media rows (remaining_media={len(remaining)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream
        if stream == WistiaStreams.MEDIAS:
            return self.get_medias(pagination)
        if stream == WistiaStreams.EVENTS:
            return self.get_events(pagination, start_date=self.config.start_date)
        if stream == WistiaStreams.STATS_MEDIA:
            return self.get_stats_media(pagination)

        raise NotImplementedError(f"Stream '{stream}' is not implemented for Wistia source")

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream
        if stream == WistiaStreams.EVENTS:
            start_date = (source_state.last_run.date() - timedelta(days=self.config.lookback_days)).isoformat()
            return self.get_events(pagination, start_date=start_date)

        raise NotImplementedError(f"Incremental sync not supported for stream '{stream}'")
