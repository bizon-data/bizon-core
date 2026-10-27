"""Pins OpenAI Ads request shaping (rejected-field isolation, 413 range splitting) against the documented response
shapes."""

import json
from datetime import date
from unittest.mock import Mock

from bizon.connectors.sources.openai_ads.src import source as oai


def _response(status, payload):
    resp = Mock(status_code=status)
    resp.json.return_value = payload
    resp.text = json.dumps(payload)
    resp.content = resp.text.encode()
    return resp


class FakeOpenai:
    def __init__(self, reject=(), max_days=None):
        self.reject = set(reject)
        self.max_days = max_days
        self.calls = []

    def close(self):
        pass

    def request(self, method, url, params=None, json=None, timeout=None):
        self.calls.append((method, url, params, json))
        if url.endswith("/ad_account/insights"):
            fields = [v for k, v in params if k == "fields[]"]
            if self.reject & set(fields):
                return _response(400, {"error": {"message": "invalid field"}})
            return _response(200, {"data": [{"id": "r1", **{f: 1 for f in fields}}], "has_more": False})
        if url.endswith("/conversions/insights"):
            rng = __import__("json").loads(json["time_ranges"][0])
            days = (date.fromisoformat(rng["until"]) - date.fromisoformat(rng["since"])).days + 1
            if self.max_days and days > self.max_days:
                return _response(413, {"error": {"message": "too many rows"}})
            return _response(200, {"data": [{"entity_id": "cmpn_1", "date": rng["since"]}], "account_currency": "USD"})
        if url.endswith("/campaigns"):
            after = dict(params).get("after")
            if after is None:
                return _response(200, {"data": [{"id": "cmpn_1"}], "has_more": True, "last_id": "cmpn_1"})
            return _response(200, {"data": [{"id": "cmpn_2"}], "has_more": False, "last_id": "cmpn_2"})
        raise AssertionError(url)


def _openai(fake, stream="ad_daily", **config):
    src = oai.OpenaiAdsSource(oai.OpenaiAdsSourceConfig(name="openai_ads", stream=stream, api_key="k", **config))
    src.session = fake
    return src


def test_openai_insights_drops_a_rejected_field_and_keeps_the_rest():
    fake = FakeOpenai(reject={"ad.order_created_roas"})
    src = _openai(fake, start_date="2026-10-01", end_date="2026-10-01")
    result = src.get()
    row = result.records[0].data
    assert "ad.order_created_roas" not in row and row["ad.spend"] == 1
    assert result.next_pagination == {}


def test_openai_conversion_insights_split_the_range_on_413():
    fake = FakeOpenai(max_days=3)
    src = _openai(fake, stream="conversion_events_daily", start_date="2026-10-01", end_date="2026-10-08", window_days=8)
    result = src.get()
    assert len(result.records) >= 3
    assert all(
        json_body["entity_ids"] == ["cmpn_1", "cmpn_2"]
        for _, url, _, json_body in fake.calls
        if url.endswith("/conversions/insights")
    )
