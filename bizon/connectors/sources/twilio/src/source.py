"""Twilio source: subaccounts, messages, daily usage records, phone numbers and messaging services.

A master account can own thousands of subaccounts; narrow the fan-out with `subaccount_sids`. The master Auth
Token authorizes against every subaccount's endpoint by path substitution, so a single run iterates all
subaccounts internally. Subaccounts that return 404 (deprovisioned or unprovisioned for the resource) are
logged and skipped.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase, HTTPBasicAuth

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

API_HOST = "https://api.twilio.com"
API_PATH = "/2010-04-01"
MESSAGING_HOST = "https://messaging.twilio.com"

DEFAULT_BACKFILL_DAYS = 90
DEFAULT_PAGE_SIZE = 1000
DEFAULT_LAG_DAYS = 1
DEFAULT_CONCURRENCY = 10
# Each failure has already exhausted the session's own retries, so a worker that keeps failing is a persistent error.
MAX_WORKER_FAILURES = 5


class TwilioStreams(str, Enum):
    SUBACCOUNTS = "subaccounts"
    MESSAGES = "messages"
    MESSAGING_SERVICES = "messaging_services"
    # Concurrent per-subaccount fan-out for daily usage records.
    ALL_USAGE_RECORDS = "all_usage_records"
    # Concurrent per-subaccount fan-out for owned phone numbers (snapshot/full_refresh).
    INCOMING_PHONE_NUMBERS = "incoming_phone_numbers"
    # Concurrent per-(subaccount, messaging_service) fan-out for the phones attached
    # to each messaging service (snapshot/full_refresh — daily diff = pool churn).
    MESSAGING_SERVICE_PHONE_NUMBERS = "messaging_service_phone_numbers"


class TwilioSourceConfig(SourceConfig):
    stream: TwilioStreams
    account_sid: str
    auth_token: str
    starting_date: Optional[str] = None
    ending_date: Optional[str] = None
    page_size: int = DEFAULT_PAGE_SIZE
    lag_days: int = DEFAULT_LAG_DAYS
    include_message_body: bool = False
    # Restricts every per-subaccount stream to these SIDs, to narrow a large master account's fan-out.
    subaccount_sids: Optional[List[str]] = None
    # Worker count for the concurrent streams (all_usage_records,
    # incoming_phone_numbers, messaging_service_phone_numbers).
    concurrency: int = DEFAULT_CONCURRENCY
    # 404s (and 401/403 on subaccount-scoped endpoints) skip the subaccount instead of failing the run.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


class TwilioSource(AbstractSource):
    def __init__(self, config: TwilioSourceConfig):
        super().__init__(config)
        self.config: TwilioSourceConfig = config
        self._subaccounts_cache: Optional[List[dict]] = None
        self._ms_units_cache: Optional[List[dict]] = None
        self.session.headers.update({"Accept": "application/json"})

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in TwilioStreams]

    @staticmethod
    def get_config_class() -> type:
        return TwilioSourceConfig

    def get_authenticator(self) -> AuthBase:
        return HTTPBasicAuth(self.config.account_sid, self.config.auth_token)

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response = self.session.get(f"{API_HOST}{API_PATH}/Accounts/{self.config.account_sid}.json")
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _today(self) -> date:
        return datetime.now(timezone.utc).date()

    def _default_start(self) -> date:
        return self._today() - timedelta(days=DEFAULT_BACKFILL_DAYS)

    def _resolve_window(self, pagination: Optional[dict]) -> Tuple[date, date]:
        """Pull (start, end) from pagination if already frozen; else from config defaults."""
        if pagination and pagination.get("window_start") and pagination.get("window_end"):
            return (
                date.fromisoformat(pagination["window_start"]),
                date.fromisoformat(pagination["window_end"]),
            )
        start = date.fromisoformat(self.config.starting_date) if self.config.starting_date else self._default_start()
        end = (
            date.fromisoformat(self.config.ending_date)
            if self.config.ending_date
            else self._today() - timedelta(days=self.config.lag_days)
        )
        return start, end

    def _resolve_incremental_window(self, source_state: SourceIncrementalState) -> Tuple[date, date]:
        start = source_state.last_run.date() - timedelta(days=self.config.lag_days)
        end = self._today() - timedelta(days=self.config.lag_days)
        if self.config.ending_date:
            end = date.fromisoformat(self.config.ending_date)
        return start, end

    def _list_subaccounts(self) -> List[dict]:
        """Fetch and cache the active subaccount list (excludes the master account)."""
        if self._subaccounts_cache is not None:
            return self._subaccounts_cache

        accounts: List[dict] = []
        next_uri: Optional[str] = f"{API_PATH}/Accounts.json?Status=active&PageSize=1000"
        while next_uri:
            response = self.session.get(f"{API_HOST}{next_uri}")
            response.raise_for_status()
            data = response.json()
            accounts.extend(data.get("accounts", []))
            next_uri = data.get("next_page_uri")

        subaccounts = [a for a in accounts if a.get("sid") != self.config.account_sid]

        if self.config.subaccount_sids:
            allowlist = set(self.config.subaccount_sids)
            filtered = [a for a in subaccounts if a["sid"] in allowlist]
            missing = allowlist - {a["sid"] for a in filtered}
            if missing:
                logger.warning(
                    f"subaccount_sids filter: {len(missing)} requested SIDs not found: {sorted(missing)[:5]}..."
                )
            logger.info(
                f"Filtered {len(subaccounts)} active subaccounts down to {len(filtered)} via subaccount_sids allowlist"
            )
            subaccounts = filtered
        else:
            logger.info(
                f"Discovered {len(subaccounts)} active subaccounts (plus master account {self.config.account_sid})"
            )

        self._subaccounts_cache = subaccounts
        return subaccounts

    @staticmethod
    def _redact_subaccount(account: dict) -> dict:
        return {k: v for k, v in account.items() if k != "auth_token"}

    def _get_subaccounts(self, pagination: Optional[dict]) -> SourceIteration:
        next_uri = (pagination or {}).get("next_uri") or (
            f"{API_PATH}/Accounts.json?Status=active&PageSize={self.config.page_size}"
        )
        response = self.session.get(f"{API_HOST}{next_uri}")
        response.raise_for_status()
        data = response.json()

        records = [
            SourceRecord(id=str(item["sid"]), data=self._redact_subaccount(item)) for item in data.get("accounts", [])
        ]
        next_page_uri = data.get("next_page_uri")
        next_pagination = {"next_uri": next_page_uri} if next_page_uri else {}

        logger.info(f"Fetched {len(records)} accounts (has_more={bool(next_page_uri)})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _iter_subaccount_page(
        self,
        pagination: Optional[dict],
        window_start: date,
        window_end: date,
        build_initial_uri,
        records_key: str,
        record_id_fn,
        transform_fn,
    ) -> SourceIteration:
        """Generic page fetcher for the per-subaccount streams.

        Pagination state shape:
          {
            "subaccount_idx": int,
            "next_uri": str | None,   # None means "start the next subaccount"
            "window_start": iso-date,
            "window_end":   iso-date,
          }
        """
        subaccounts = self._list_subaccounts()
        if not subaccounts:
            logger.warning("No subaccounts found; nothing to fetch.")
            return SourceIteration(records=[], next_pagination={})

        sub_idx = (pagination or {}).get("subaccount_idx", 0)
        next_uri = (pagination or {}).get("next_uri")

        # Skip subaccounts whose index has overflowed (defensive).
        if sub_idx >= len(subaccounts):
            return SourceIteration(records=[], next_pagination={})

        current_sub = subaccounts[sub_idx]
        sub_sid = current_sub["sid"]

        request_url = (
            f"{API_HOST}{next_uri}"
            if next_uri
            else build_initial_uri(sub_sid, window_start, window_end, self.config.page_size)
        )

        response = self.session.get(request_url)
        if response.status_code == 404:
            logger.warning(
                f"[{records_key}] subaccount {sub_idx + 1}/{len(subaccounts)} "
                f"({sub_sid}): 404 — skipping (subaccount has no {records_key} endpoint)"
            )
            if sub_idx + 1 < len(subaccounts):
                return SourceIteration(
                    records=[],
                    next_pagination={
                        "subaccount_idx": sub_idx + 1,
                        "next_uri": None,
                        "window_start": window_start.isoformat(),
                        "window_end": window_end.isoformat(),
                    },
                )
            return SourceIteration(records=[], next_pagination={})
        response.raise_for_status()
        data = response.json()

        items = data.get(records_key, [])
        records = [SourceRecord(id=record_id_fn(item), data=transform_fn(item)) for item in items]

        api_next_uri = data.get("next_page_uri")

        if api_next_uri:
            new_pagination = {
                "subaccount_idx": sub_idx,
                "next_uri": api_next_uri,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
            }
        elif sub_idx + 1 < len(subaccounts):
            new_pagination = {
                "subaccount_idx": sub_idx + 1,
                "next_uri": None,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
            }
        else:
            new_pagination = {}

        logger.info(
            f"[{records_key}] subaccount {sub_idx + 1}/{len(subaccounts)} "
            f"({sub_sid}): fetched {len(records)} rows "
            f"(more_in_sub={bool(api_next_uri)})"
        )
        return SourceIteration(records=records, next_pagination=new_pagination)

    @staticmethod
    def _messages_initial_uri(sub_sid: str, start: date, end: date, page_size: int) -> str:
        return (
            f"{API_HOST}{API_PATH}/Accounts/{sub_sid}/Messages.json"
            f"?DateSent%3E={start.isoformat()}&DateSent%3C={end.isoformat()}"
            f"&PageSize={page_size}"
        )

    def _transform_message(self, msg: dict) -> dict:
        if not self.config.include_message_body:
            msg = {**msg, "body": None}
        return msg

    def _get_messages(self, pagination: Optional[dict], window: Tuple[date, date]) -> SourceIteration:
        start, end = window
        return self._iter_subaccount_page(
            pagination=pagination,
            window_start=start,
            window_end=end,
            build_initial_uri=self._messages_initial_uri,
            records_key="messages",
            record_id_fn=lambda m: str(m["sid"]),
            transform_fn=self._transform_message,
        )

    @staticmethod
    def _usage_initial_uri(sub_sid: str, start: date, end: date, page_size: int) -> str:
        return (
            f"{API_HOST}{API_PATH}/Accounts/{sub_sid}/Usage/Records/Daily.json"
            f"?StartDate={start.isoformat()}&EndDate={end.isoformat()}"
            f"&PageSize={page_size}"
        )

    @staticmethod
    def _usage_record_id(item: dict) -> str:
        return f"{item['account_sid']}_{item['start_date']}_{item['category']}"

    def _get_messaging_services(self, pagination: Optional[dict]) -> SourceIteration:
        """Per-subaccount snapshot of Messaging Services.

        Uses a different Twilio API surface: messaging.twilio.com/v1/Services.
        Auth must be the subaccount's own (sub_sid, sub_auth_token) — the master
        token does not grant access to subaccount-scoped messaging services.
        The per-subaccount auth_token comes from /Accounts.json (already
        cached in _list_subaccounts) and is passed on a per-request basis.

        Pagination state:
          { "subaccount_idx": int, "next_url": str | None }
        """
        subaccounts = self._list_subaccounts()
        if not subaccounts:
            return SourceIteration(records=[], next_pagination={})

        sub_idx = (pagination or {}).get("subaccount_idx", 0)
        next_url = (pagination or {}).get("next_url")

        if sub_idx >= len(subaccounts):
            return SourceIteration(records=[], next_pagination={})

        current_sub = subaccounts[sub_idx]
        sub_sid = current_sub["sid"]
        sub_auth_token = current_sub.get("auth_token")

        if not sub_auth_token:
            logger.warning(f"[messaging_services] {sub_sid}: no auth_token on /Accounts.json payload; skipping")
            return self._advance_or_finish_messaging_services(sub_idx, subaccounts)

        request_url = next_url or f"{MESSAGING_HOST}/v1/Services?PageSize={self.config.page_size}"

        response = self.session.get(request_url, auth=HTTPBasicAuth(sub_sid, sub_auth_token))
        if response.status_code in (401, 403, 404):
            logger.warning(
                f"[messaging_services] subaccount {sub_idx + 1}/{len(subaccounts)} "
                f"({sub_sid}): HTTP {response.status_code} — skipping"
            )
            return self._advance_or_finish_messaging_services(sub_idx, subaccounts)
        response.raise_for_status()
        data = response.json()

        services = data.get("services", [])
        records = [SourceRecord(id=str(s["sid"]), data=s) for s in services]

        api_next = data.get("meta", {}).get("next_page_url")

        if api_next:
            new_pagination = {"subaccount_idx": sub_idx, "next_url": api_next}
        elif sub_idx + 1 < len(subaccounts):
            new_pagination = {"subaccount_idx": sub_idx + 1, "next_url": None}
        else:
            new_pagination = {}

        logger.info(
            f"[messaging_services] subaccount {sub_idx + 1}/{len(subaccounts)} "
            f"({sub_sid}): fetched {len(records)} services (more_in_sub={bool(api_next)})"
        )
        return SourceIteration(records=records, next_pagination=new_pagination)

    @staticmethod
    def _advance_or_finish_messaging_services(sub_idx: int, subaccounts: List[dict]) -> SourceIteration:
        if sub_idx + 1 < len(subaccounts):
            return SourceIteration(
                records=[],
                next_pagination={"subaccount_idx": sub_idx + 1, "next_url": None},
            )
        return SourceIteration(records=[], next_pagination={})

    def _get_all_usage_records(self, pagination: Optional[dict], window: Tuple[date, date]) -> SourceIteration:
        """Concurrent per-subaccount fan-out for daily usage records."""
        start, end = window
        return self._concurrent_subaccount_fanout(
            pagination=pagination,
            initial_uri_fn=lambda sub_sid: self._usage_initial_uri(sub_sid, start, end, self.config.page_size),
            records_key="usage_records",
            record_id_fn=self._usage_record_id,
            transform_fn=lambda r: r,
            log_label="all_usage_records",
        )

    @staticmethod
    def _incoming_phone_numbers_initial_uri(sub_sid: str, page_size: int) -> str:
        return f"{API_HOST}{API_PATH}/Accounts/{sub_sid}/IncomingPhoneNumbers.json?PageSize={page_size}"

    def _get_incoming_phone_numbers(self, pagination: Optional[dict]) -> SourceIteration:
        """Concurrent per-subaccount fan-out for owned phone numbers.

        Same pagination contract as _get_all_usage_records — N workers, each
        on one subaccount, all fired in parallel each get() call. Pagination
        state shape:
          {"workers": [{"sub_idx": int, "next_uri": str | None}, ...],
           "next_unassigned_idx": int}
        """
        return self._concurrent_subaccount_fanout(
            pagination=pagination,
            initial_uri_fn=lambda sub_sid: self._incoming_phone_numbers_initial_uri(sub_sid, self.config.page_size),
            records_key="incoming_phone_numbers",
            record_id_fn=lambda item: str(item["sid"]),
            transform_fn=lambda r: r,
            log_label="incoming_phone_numbers",
        )

    def _concurrent_subaccount_fanout(
        self,
        pagination: Optional[dict],
        initial_uri_fn,
        records_key: str,
        record_id_fn,
        transform_fn,
        log_label: str,
    ) -> SourceIteration:
        """Reusable concurrent fan-out across subaccounts using master auth.

        Used by both all_usage_records and incoming_phone_numbers. Twilio's
        2010-04-01 envelope (next_page_uri as path) is assumed.
        """
        subaccounts = self._list_subaccounts()
        if not subaccounts:
            return SourceIteration(records=[], next_pagination={})

        n_workers_target = max(1, self.config.concurrency)
        workers = (pagination or {}).get("workers")
        next_idx = (pagination or {}).get("next_unassigned_idx", 0)

        if workers is None:
            workers = []
            while len(workers) < n_workers_target and next_idx < len(subaccounts):
                workers.append({"sub_idx": next_idx, "next_uri": None})
                next_idx += 1

        if not workers:
            return SourceIteration(records=[], next_pagination={})

        def fetch_one(worker: dict):
            sub_sid = subaccounts[worker["sub_idx"]]["sid"]
            url = f"{API_HOST}{worker['next_uri']}" if worker["next_uri"] else initial_uri_fn(sub_sid)
            try:
                resp = self.session.get(url, timeout=60)
                if resp.status_code == 404:
                    return worker, None, "404"
                resp.raise_for_status()
                return worker, resp.json(), None
            except Exception as e:
                return worker, None, f"error: {type(e).__name__}: {e}"

        records: List[SourceRecord] = []
        new_workers: List[dict] = []
        sub_done = 0
        skipped_404 = 0

        with ThreadPoolExecutor(max_workers=n_workers_target) as ex:
            for fut in as_completed([ex.submit(fetch_one, w) for w in workers]):
                worker, data, err = fut.result()
                sub_sid = subaccounts[worker["sub_idx"]]["sid"]

                if err == "404":
                    skipped_404 += 1
                    if next_idx < len(subaccounts):
                        new_workers.append({"sub_idx": next_idx, "next_uri": None})
                        next_idx += 1
                    continue
                if err is not None:
                    failures = worker.get("failures", 0) + 1
                    if failures >= MAX_WORKER_FAILURES:
                        raise RuntimeError(f"[{log_label}] {sub_sid}: {err}; giving up after {failures} attempts")
                    logger.warning(f"[{log_label}] {sub_sid}: {err}; will retry")
                    new_workers.append({**worker, "failures": failures})
                    continue

                items = (data or {}).get(records_key, [])
                records.extend(SourceRecord(id=record_id_fn(item), data=transform_fn(item)) for item in items)

                api_next = (data or {}).get("next_page_uri")
                if api_next:
                    new_workers.append({"sub_idx": worker["sub_idx"], "next_uri": api_next})
                else:
                    sub_done += 1
                    if next_idx < len(subaccounts):
                        new_workers.append({"sub_idx": next_idx, "next_uri": None})
                        next_idx += 1

        logger.info(
            f"[{log_label}] iter: workers={len(workers)} records={len(records)} "
            f"subs_completed_this_iter={sub_done} 404s={skipped_404} "
            f"progress={next_idx}/{len(subaccounts)} active_workers_next={len(new_workers)}"
        )

        if not new_workers:
            return SourceIteration(records=records, next_pagination={})

        return SourceIteration(
            records=records,
            next_pagination={
                "workers": new_workers,
                "next_unassigned_idx": next_idx,
            },
        )

    def _list_messaging_service_units(self) -> List[dict]:
        """Build the (sub_sid, sub_auth_token, ms_sid) work units, cached.

        Iterates subaccounts in parallel (concurrency from config) and for each,
        fetches its messaging services via the per-subaccount auth.
        """
        if hasattr(self, "_ms_units_cache") and self._ms_units_cache is not None:
            return self._ms_units_cache

        subaccounts = self._list_subaccounts()
        units: List[dict] = []
        n_workers = max(1, self.config.concurrency)

        def fetch_ms_for_sub(sub: dict) -> List[dict]:
            sub_sid = sub["sid"]
            sub_tok = sub.get("auth_token")
            if not sub_tok:
                return []
            results: List[dict] = []
            url: Optional[str] = f"{MESSAGING_HOST}/v1/Services?PageSize={self.config.page_size}"
            while url:
                try:
                    r = self.session.get(url, auth=HTTPBasicAuth(sub_sid, sub_tok), timeout=30)
                    if r.status_code in (401, 403, 404):
                        return results
                    r.raise_for_status()
                    d = r.json()
                    for s in d.get("services", []):
                        results.append(
                            {
                                "sub_sid": sub_sid,
                                "sub_auth_token": sub_tok,
                                "ms_sid": s["sid"],
                            }
                        )
                    url = d.get("meta", {}).get("next_page_url")
                except Exception as e:
                    logger.warning(f"[ms_units] {sub_sid}: {type(e).__name__}; partial result")
                    return results
            return results

        with ThreadPoolExecutor(max_workers=n_workers) as ex:
            for fut in as_completed([ex.submit(fetch_ms_for_sub, s) for s in subaccounts]):
                units.extend(fut.result())

        logger.info(
            f"[ms_units] discovered {len(units)} (subaccount, messaging_service) pairs "
            f"across {len(subaccounts)} subaccounts"
        )
        self._ms_units_cache = units
        return units

    def _get_messaging_service_phone_numbers(self, pagination: Optional[dict]) -> SourceIteration:
        """Concurrent per-(subaccount, messaging_service) fan-out for phone pool.

        Pagination state:
          {"workers": [{"unit_idx": int, "next_url": str | None}, ...],
           "next_unassigned_idx": int}
        """
        units = self._list_messaging_service_units()
        if not units:
            return SourceIteration(records=[], next_pagination={})

        n_workers_target = max(1, self.config.concurrency)
        workers = (pagination or {}).get("workers")
        next_idx = (pagination or {}).get("next_unassigned_idx", 0)

        if workers is None:
            workers = []
            while len(workers) < n_workers_target and next_idx < len(units):
                workers.append({"unit_idx": next_idx, "next_url": None})
                next_idx += 1

        if not workers:
            return SourceIteration(records=[], next_pagination={})

        def fetch_one(worker: dict):
            unit = units[worker["unit_idx"]]
            url = worker["next_url"] or (
                f"{MESSAGING_HOST}/v1/Services/{unit['ms_sid']}/PhoneNumbers?PageSize={self.config.page_size}"
            )
            try:
                resp = self.session.get(
                    url,
                    auth=HTTPBasicAuth(unit["sub_sid"], unit["sub_auth_token"]),
                    timeout=60,
                )
                if resp.status_code in (401, 403, 404):
                    return worker, None, str(resp.status_code)
                resp.raise_for_status()
                return worker, resp.json(), None
            except Exception as e:
                return worker, None, f"error: {type(e).__name__}: {e}"

        records: List[SourceRecord] = []
        new_workers: List[dict] = []
        unit_done = 0
        skipped = 0

        with ThreadPoolExecutor(max_workers=n_workers_target) as ex:
            for fut in as_completed([ex.submit(fetch_one, w) for w in workers]):
                worker, data, err = fut.result()
                unit = units[worker["unit_idx"]]

                if err in ("401", "403", "404"):
                    skipped += 1
                    if next_idx < len(units):
                        new_workers.append({"unit_idx": next_idx, "next_url": None})
                        next_idx += 1
                    continue
                if err is not None:
                    failures = worker.get("failures", 0) + 1
                    if failures >= MAX_WORKER_FAILURES:
                        raise RuntimeError(
                            f"[ms_phone_numbers] {unit['ms_sid']}: {err}; giving up after {failures} attempts"
                        )
                    logger.warning(f"[ms_phone_numbers] {unit['ms_sid']}: {err}; will retry")
                    new_workers.append({**worker, "failures": failures})
                    continue

                items = (data or {}).get("phone_numbers", [])
                for item in items:
                    # Stamp the subaccount sid alongside so the row joins back cleanly.
                    item = {"subaccount_sid": unit["sub_sid"], **item}
                    records.append(SourceRecord(id=str(item["sid"]), data=item))

                api_next = (data or {}).get("meta", {}).get("next_page_url")
                if api_next:
                    new_workers.append({"unit_idx": worker["unit_idx"], "next_url": api_next})
                else:
                    unit_done += 1
                    if next_idx < len(units):
                        new_workers.append({"unit_idx": next_idx, "next_url": None})
                        next_idx += 1

        logger.info(
            f"[ms_phone_numbers] iter: workers={len(workers)} records={len(records)} "
            f"units_completed_this_iter={unit_done} skipped={skipped} "
            f"progress={next_idx}/{len(units)} active_workers_next={len(new_workers)}"
        )

        if not new_workers:
            return SourceIteration(records=records, next_pagination={})

        return SourceIteration(
            records=records,
            next_pagination={
                "workers": new_workers,
                "next_unassigned_idx": next_idx,
            },
        )

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream

        if stream == TwilioStreams.SUBACCOUNTS:
            return self._get_subaccounts(pagination)
        if stream == TwilioStreams.MESSAGING_SERVICES:
            return self._get_messaging_services(pagination)
        if stream == TwilioStreams.INCOMING_PHONE_NUMBERS:
            return self._get_incoming_phone_numbers(pagination)
        if stream == TwilioStreams.MESSAGING_SERVICE_PHONE_NUMBERS:
            return self._get_messaging_service_phone_numbers(pagination)

        window = self._resolve_window(pagination)

        if stream == TwilioStreams.MESSAGES:
            return self._get_messages(pagination, window)
        if stream == TwilioStreams.ALL_USAGE_RECORDS:
            return self._get_all_usage_records(pagination, window)

        raise NotImplementedError(f"Stream '{stream}' is not implemented for Twilio source")

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream

        if stream == TwilioStreams.SUBACCOUNTS:
            # Snapshot stream — always do a full refresh.
            return self._get_subaccounts(pagination)
        if stream == TwilioStreams.MESSAGING_SERVICES:
            return self._get_messaging_services(pagination)
        if stream == TwilioStreams.INCOMING_PHONE_NUMBERS:
            return self._get_incoming_phone_numbers(pagination)
        if stream == TwilioStreams.MESSAGING_SERVICE_PHONE_NUMBERS:
            return self._get_messaging_service_phone_numbers(pagination)

        if pagination and pagination.get("window_start"):
            window = self._resolve_window(pagination)
        else:
            window = self._resolve_incremental_window(source_state)
            if window[0] > window[1]:
                logger.info(f"No new data: window_start={window[0]} > window_end={window[1]}. Skipping.")
                return SourceIteration(records=[], next_pagination={})

        if stream == TwilioStreams.MESSAGES:
            return self._get_messages(pagination, window)
        if stream == TwilioStreams.ALL_USAGE_RECORDS:
            return self._get_all_usage_records(pagination, window)

        raise NotImplementedError(f"Stream '{stream}' is not implemented for Twilio source")
