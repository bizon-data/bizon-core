"""apilayer Exchange Rates Data API source: one historical rate set per calendar day, walked day by day."""

from datetime import date, timedelta
from typing import List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.adapters import Retry
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

# apilayer's "Exchange Rates Data API" product with header auth; an access_key query param against the legacy
# api.exchangeratesapi.io host is rejected for keys provisioned for this product.
BASE_URL = "https://api.apilayer.com/exchangerates_data"

# Fetch through today. Before the day's ECB fix is published, apilayer answers the dated endpoint with the last
# published rates labelled with the requested date; a day later it serves the settled value for that date, which
# differs. Changing this lag switches a consumer from the provisional to the settled series.
DATA_LAG_DAYS = 0

# Status-code retries get their own small budget: on a plan with a few hundred requests a month, a deep retry
# storm against a 429 or a flapping 5xx spends a meaningful share of the quota. Connection failures cost nothing
# and keep the full total.
STATUS_RETRIES = 3

# Resume on the day after the watermark, so a daily run makes a single request. last_run is the last successful
# job's created_at, which with DATA_LAG_DAYS = 0 was fetched through. A failed run catches up on its own because
# last_run only advances on success.
RESUME_OFFSET_DAYS = 1


class ExchangeRatesSourceConfig(SourceConfig):
    access_key: str
    start_date: str = Field(..., description="First day (YYYY-MM-DD) fetched by a first or full sync")
    ignore_weekends: bool = False
    base: Optional[str] = Field(None, description="Base currency. Unset uses the account default (EUR)")
    # apilayer answers a spent monthly quota with a 429 whose Retry-After runs to the next month; the default
    # policy caps that wait at 120s so the run fails instead of sleeping.
    http: Optional[HttpConfig] = Field(default_factory=HttpConfig, description="HTTP timeout/retry policy")


class ExchangeRatesSource(AbstractSource):
    def __init__(self, config: ExchangeRatesSourceConfig):
        super().__init__(config)
        self.config: ExchangeRatesSourceConfig = config

    @staticmethod
    def streams() -> List[str]:
        return ["exchange_rates"]

    @staticmethod
    def get_config_class() -> type:
        return ExchangeRatesSourceConfig

    def get_retry_policy(self) -> Optional[Retry]:
        retry = super().get_retry_policy()
        return retry.new(status=STATUS_RETRIES) if retry is not None else None

    def get_authenticator(self) -> Optional[AuthBase]:
        # apilayer sends the key as the `apikey` header, not a query param.
        self.session.headers.update({"apikey": self.config.access_key})
        return None

    def _params(self) -> dict:
        params = {}
        if self.config.base:
            params["base"] = self.config.base
        return params

    def check_connection(self) -> Tuple[bool, Optional[str]]:
        try:
            response = self.session.get(f"{BASE_URL}/latest", params=self._params(), timeout=(10, 60))
            response.raise_for_status()
            body = response.json()
            if not body.get("success", True):
                return False, str(body.get("error"))
            return True, None
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _start_date(self) -> date:
        return date.fromisoformat(self.config.start_date)

    def _end_date(self) -> date:
        return date.today() - timedelta(days=DATA_LAG_DAYS)

    def _skip_weekends(self, day: date) -> date:
        while self.config.ignore_weekends and day.weekday() >= 5:
            day += timedelta(days=1)
        return day

    def _fetch(self, current_date: date, end_date: date) -> SourceIteration:
        response = self.session.get(
            f"{BASE_URL}/{current_date.isoformat()}",
            params=self._params(),
            timeout=(10, 60),
        )
        response.raise_for_status()
        body = response.json()

        if not body.get("success", True):
            raise RuntimeError(f"Exchange Rates API error for {current_date.isoformat()}: {body.get('error')}")

        records = [SourceRecord(id=current_date.isoformat(), data=body)]

        next_date = self._skip_weekends(current_date + timedelta(days=1))
        next_pagination = {"current_date": next_date.isoformat()} if next_date <= end_date else {}

        logger.info(f"Fetched exchange rates for {current_date.isoformat()}")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        end_date = self._end_date()

        if pagination and pagination.get("current_date"):
            current_date = date.fromisoformat(pagination["current_date"])
        else:
            current_date = self._skip_weekends(self._start_date())

        if current_date > end_date:
            return SourceIteration(records=[], next_pagination={})

        return self._fetch(current_date, end_date)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        end_date = self._end_date()

        if pagination and pagination.get("current_date"):
            current_date = date.fromisoformat(pagination["current_date"])
        else:
            current_date = source_state.last_run.date() + timedelta(days=RESUME_OFFSET_DAYS)
            current_date = max(current_date, self._start_date())
            current_date = self._skip_weekends(current_date)

        if current_date > end_date:
            logger.info(f"No new data: current_date={current_date} > end_date={end_date}. Skipping fetch.")
            return SourceIteration(records=[], next_pagination={})

        return self._fetch(current_date, end_date)
