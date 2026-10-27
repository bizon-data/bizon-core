"""nOps cloud cost source: daily costs grouped by dimension, and cost summary totals. Docs: https://app.nops.io/swagger/"""

from datetime import datetime, timedelta
from enum import Enum
from typing import List, Optional, Tuple

from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://app.nops.io"

DIMENSION_STREAMS = {
    "dimension_daily_cost": "product_product_name",
    "dimension_daily_cost_by_account": "line_item_usage_account_id",
    "dimension_daily_cost_by_region": "product_region",
    "dimension_daily_cost_by_usagetype": "line_item_usage_type",
}


class NopsStreams(str, Enum):
    DIMENSION_DAILY_COST = "dimension_daily_cost"
    DIMENSION_DAILY_COST_BY_ACCOUNT = "dimension_daily_cost_by_account"
    DIMENSION_DAILY_COST_BY_REGION = "dimension_daily_cost_by_region"
    DIMENSION_DAILY_COST_BY_USAGETYPE = "dimension_daily_cost_by_usagetype"
    COST_SUMMARY = "cost_summary"


class NopsSourceConfig(SourceConfig):
    stream: NopsStreams
    api_key: str
    start_date: Optional[str] = Field(None, description="Start day (YYYY-MM-DD). Defaults to 30 days ago")
    # check_connection turns 401/403/other non-200 responses into readable messages.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


class NopsApiKeyAuth(AuthBase):
    def __init__(self, key: str):
        self.key = key

    def __call__(self, r):
        r.headers["X-Nops-Api-Key"] = self.key
        return r


class NopsSource(AbstractSource):
    def __init__(self, config: NopsSourceConfig):
        super().__init__(config)
        self.config: NopsSourceConfig = config

    def _get_date_start(self) -> str:
        if self.config.start_date:
            return self.config.start_date
        return (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in NopsStreams]

    @staticmethod
    def get_config_class() -> type:
        return NopsSourceConfig

    def get_authenticator(self) -> Optional[AuthBase]:
        return NopsApiKeyAuth(self.config.api_key)

    def check_connection(self) -> Tuple[bool, Optional[str]]:
        try:
            today = datetime.now()
            date_start = (today - timedelta(days=1)).strftime("%Y-%m-%d")
            date_end = today.strftime("%Y-%m-%d")

            response = self.session.get(
                f"{BASE_URL}/c/cost_page/dimension_daily_cost/",
                params={
                    "dimension": "product_product_name",
                    "granularity": "day",
                    "date_start": date_start,
                    "date_end": date_end,
                    "threshold": 1,
                },
                timeout=30,
            )

            if response.status_code == 401:
                return False, "Authentication failed. Check your API key."
            elif response.status_code == 403:
                return False, "Access forbidden. Check API key permissions."
            elif response.status_code != 200:
                return False, f"API returned status {response.status_code}: {response.text[:200]}"

            content_type = response.headers.get("content-type", "")
            if "application/json" not in content_type:
                return False, f"Expected JSON response but got {content_type}. Check API key."

            return True, None

        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream

        if stream in DIMENSION_STREAMS:
            return self._get_dimension_daily_cost(DIMENSION_STREAMS[stream], pagination)
        elif stream == NopsStreams.COST_SUMMARY:
            return self._get_cost_summary(pagination)
        else:
            raise ValueError(f"Unknown stream: {stream}")

    def _get_dimension_daily_cost(self, dimension: str, pagination: dict = None) -> SourceIteration:
        if pagination and pagination.get("done"):
            return SourceIteration(next_pagination={}, records=[])

        today = datetime.now()
        date_end = today.strftime("%Y-%m-%d")
        date_start = self._get_date_start()

        response = self.session.get(
            f"{BASE_URL}/c/cost_page/dimension_daily_cost/",
            params={
                "dimension": dimension,
                "granularity": "day",
                "date_start": date_start,
                "date_end": date_end,
                "threshold": 10000,
                "cost_type": "line_item_unblended_cost",
            },
            timeout=120,
        )

        response.raise_for_status()
        data = response.json()

        records = self._convert_dimension_response(data, date_start, date_end, dimension)

        return SourceIteration(next_pagination={"done": True}, records=records)

    def _get_cost_summary(self, pagination: dict = None) -> SourceIteration:
        if pagination and pagination.get("done"):
            return SourceIteration(next_pagination={}, records=[])

        today = datetime.now()
        date_end = today.strftime("%Y-%m-%d")
        date_start = self._get_date_start()

        response = self.session.get(
            f"{BASE_URL}/c/cost_page/cost_summary/",
            params={
                "date_start": date_start,
                "date_end": date_end,
                "cost_type": "line_item_unblended_cost",
            },
            timeout=120,
        )

        response.raise_for_status()
        data = response.json()

        record_data = {
            "date_start": date_start,
            "date_end": date_end,
        }
        if isinstance(data, dict):
            record_data.update(data)
        else:
            raise ValueError(f"Expected dict response from nOps cost_summary, got {type(data).__name__}")

        record_id = f"cost_summary_{date_start}_{date_end}"
        records = [SourceRecord(id=str(record_id), data=record_data)]

        return SourceIteration(next_pagination={"done": True}, records=records)

    def _convert_dimension_response(
        self,
        data: dict,
        date_start: str,
        date_end: str,
        dimension: str,
    ) -> List[SourceRecord]:
        records = []

        if not isinstance(data, dict):
            raise ValueError(f"Expected dict response from nOps API, got {type(data).__name__}")

        for dimension_value, cost_data in data.items():
            record_data = {
                "dimension_value": dimension_value,
                "dimension": dimension,
                "granularity": "day",
                "date_start": date_start,
                "date_end": date_end,
            }

            if isinstance(cost_data, dict):
                record_data.update(cost_data)
            else:
                record_data["value"] = cost_data

            records.append(SourceRecord(id=str(dimension_value), data=record_data))

        return records
