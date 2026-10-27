"""Pins LinkedIn request shaping (REST.li encoding, 20-field chunking, 15k-element window splitting, rejected-field
isolation) against the documented response shapes."""

import json
import time
from datetime import date
from unittest.mock import Mock
from urllib.parse import parse_qs, unquote, urlparse

import pytest

from bizon.connectors.sources.linkedin_ads.src import source as li


def _response(status, payload):
    resp = Mock(status_code=status)
    resp.json.return_value = payload
    resp.text = json.dumps(payload)
    resp.content = resp.text.encode()
    return resp


class FakeLinkedin:
    """adAnalytics stand-in: returns one element per (pivot value, day) with the requested fields."""

    def __init__(self, pivot_values=("urn:li:sponsoredCampaign:1",), reject=(), cap_over_days=None):
        self.pivot_values = list(pivot_values)
        self.reject = set(reject)
        self.cap_over_days = cap_over_days
        self.calls = []

    def close(self):
        pass

    def get(self, url, headers=None, timeout=None):
        parsed = urlparse(url)
        query = {k: v[0] for k, v in parse_qs(parsed.query, keep_blank_values=True).items()}
        self.calls.append(query)
        fields = query.get("fields", "").split(",")
        if self.reject & set(fields):
            return _response(400, {"message": "invalid projection"})
        start, end = _date_range(query["dateRange"])
        days = (end - start).days + 1
        if self.cap_over_days and days > self.cap_over_days:
            return _response(200, {"elements": [{} for _ in range(li.MAX_ELEMENTS)]})
        elements = []
        for offset in range(days):
            d = date.fromordinal(start.toordinal() + offset)
            for value in self.pivot_values:
                element = {
                    "pivotValues": [value],
                    "dateRange": {"start": {"year": d.year, "month": d.month, "day": d.day}},
                }
                element.update({f: 1 for f in fields if f not in ("pivotValues", "dateRange")})
                elements.append(element)
        return _response(200, {"elements": elements})


def _date_range(literal):
    nums = [int(x) for x in __import__("re").findall(r"\d+", unquote(literal))]
    return date(nums[0], nums[1], nums[2]), date(nums[3], nums[4], nums[5])


def _linkedin(fake, stream="campaign_daily", **config):
    config.setdefault("start_date", "2024-01-01")
    src = li.LinkedinAdsSource(
        li.LinkedinAdsSourceConfig(
            name="linkedin_ads",
            stream=stream,
            client_id="x",
            client_secret="x",
            refresh_token="x",
            account_ids=["7"],
            **config,
        )
    )
    src._token, src._token_expires_at = "t", time.time() + 3600
    src.session = fake
    return src


def test_linkedin_query_string_matches_restli_syntax():
    qs = li.build_query_string(
        {
            "q": "analytics",
            "pivot": "MEMBER_COMPANY",
            "campaigns": ["urn:li:sponsoredCampaign:1"],
            "dateRange": li.date_range_literal(date(2026, 1, 1), date(2026, 1, 31)),
            "fields": "impressions,pivotValues",
        }
    )
    assert qs == (
        "?q=analytics&pivot=MEMBER_COMPANY&campaigns=List(urn%3Ali%3AsponsoredCampaign%3A1)"
        "&dateRange=(start:(year:2026,month:1,day:1),end:(year:2026,month:1,day:31))&fields=impressions,pivotValues"
    )


def test_linkedin_chunks_metrics_to_20_fields_and_merges_rows():
    fake = FakeLinkedin()
    src = _linkedin(fake)
    rows = src._analytics(
        {"q": "analytics", "pivot": "CAMPAIGN"}, li.ALL_METRICS, date(2026, 10, 1), date(2026, 10, 2), "DAILY"
    )
    assert len(rows) == 2
    assert all(len(c["fields"].split(",")) <= li.MAX_FIELDS_PER_REQUEST for c in fake.calls)
    assert set(li.ALL_METRICS) <= set(rows[0])


def test_linkedin_drops_only_the_rejected_metric():
    fake = FakeLinkedin(reject={"audiencePenetration"})
    src = _linkedin(fake)
    rows = src._analytics(
        {"q": "analytics", "pivot": "CREATIVE"}, li.ALL_METRICS, date(2026, 10, 1), date(2026, 10, 1), "DAILY"
    )
    assert "audiencePenetration" not in rows[0]
    assert set(li.ALL_METRICS) - {"audiencePenetration"} <= set(rows[0])


def test_linkedin_raises_when_the_query_itself_is_bad():
    fake = FakeLinkedin(reject={"impressions", "clicks"})
    src = _linkedin(fake)
    with pytest.raises(li.LinkedinApiError):
        src._analytics(
            {"q": "analytics", "pivot": "CAMPAIGN"}, ["clicks"], date(2026, 10, 1), date(2026, 10, 1), "DAILY"
        )


def test_linkedin_splits_a_window_that_hits_the_element_cap():
    fake = FakeLinkedin(cap_over_days=4)
    src = _linkedin(fake)
    rows = src._request_chunk(
        {"q": "analytics", "pivot": "CAMPAIGN"}, ["impressions"], date(2026, 10, 1), date(2026, 10, 10), "DAILY"
    )
    assert len(rows) == 10


def test_linkedin_member_company_runs_one_query_per_served_campaign_then_finishes():
    fake = FakeLinkedin(pivot_values=("urn:li:sponsoredCampaign:1", "urn:li:sponsoredCampaign:2"))
    src = _linkedin(
        fake, stream="member_company_campaign_daily", start_date="2026-10-01", end_date="2026-10-02", window_days=2
    )
    first = src.get()
    assert first.next_pagination["unit_idx"] == 1
    assert first.next_pagination["units"][0] == {
        "q": "analytics",
        "pivot": "MEMBER_COMPANY",
        "campaigns": ["urn:li:sponsoredCampaign:1"],
    }
    second = src.get(first.next_pagination)
    assert second.next_pagination == {}
    assert {r.data["campaign_facet"] for r in first.records + second.records} == {
        "urn:li:sponsoredCampaign:1",
        "urn:li:sponsoredCampaign:2",
    }


def test_linkedin_cursor_stores_window_dates_not_indexes():
    # A resumed run recomputes `end` from today; the cursor must name the next
    # window by its first day so a resume continues where the last run stopped.
    fake = FakeLinkedin()
    src = _linkedin(fake, start_date="2026-10-01", end_date="2026-10-03", window_days=1)
    first = src.get()
    assert first.next_pagination == {"account_idx": 0, "window_start": "2026-10-02"}
    resumed = src.get({"account_idx": 0, "window_start": "2026-10-03"})
    assert {r.data["date"] for r in resumed.records} == {"2026-10-03"}
    assert resumed.next_pagination == {}


def test_linkedin_organizations_split_a_window_that_hits_the_cap():
    # Each day has 10,000 distinct companies, so any multi-day ALL request is "full"
    # (14,999 rows) and must be split until single days fit.
    class FakeCompanies(FakeLinkedin):
        def get(self, url, headers=None, timeout=None):
            query = {k: v[0] for k, v in parse_qs(urlparse(url).query, keep_blank_values=True).items()}
            self.calls.append(query)
            if "adTargetingEntities" in url:
                return _response(200, {"elements": []})
            start, end = _date_range(query["dateRange"])
            if start != end:
                return _response(
                    200,
                    {"elements": [{"pivotValues": [f"urn:li:organization:x{i}"]} for i in range(li.MAX_ELEMENTS - 1)]},
                )
            return _response(
                200, {"elements": [{"pivotValues": [f"urn:li:organization:{start.day}-{i}"]} for i in range(10000)]}
            )

    fake = FakeCompanies()
    src = _linkedin(fake, stream="organizations", end_date="2026-10-04", organization_lookback_days=3)
    urns = src._member_company_urns("urn:li:sponsoredAccount:7", date(2026, 10, 1), date(2026, 10, 4))
    assert len(urns) == 40000
