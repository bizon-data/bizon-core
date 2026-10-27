from unittest.mock import Mock

import pytest

from bizon.connectors.sources.storeleads.src import source as storeleads

PAGE = storeleads.APPS_TECH_PAGE_SIZE


def _catalog(n=300):
    platforms = ("shopify", "shopify", "shopify", "wix")
    categories = (["analytics"], ["seo", "chat"], ["other"], [])
    return [
        {
            "id": f"{i}.app{i}",
            "token": f"app{i}",
            "platform": platforms[i % 4],
            "categories": categories[i % 4],
        }
        for i in range(n)
    ]


def _matches(app, params):
    ok = app["platform"] == "shopify"
    if params.get("f:p:op") == "not":
        ok = not ok
    elif "f:p" not in params:
        ok = True
    if ok and "f:categories" in params:
        wanted = set(params["f:categories"].split(","))
        in_set = bool(wanted & set(app["categories"]))
        ok = in_set if params["f:categories:op"] == "or" else not in_set
    return ok


class FakeSession:
    """Serves all/app like StoreLeads: 50 per page, f:p / f:categories filters, 422 past MAX_PAGE_INDEX."""

    def __init__(self, apps):
        self.apps = apps
        self.calls = []

    def close(self):
        pass

    def get(self, url, params=None, timeout=None):
        self.calls.append(dict(params))
        rows = [a for a in self.apps if _matches(a, params)]
        resp = Mock(status_code=200)
        if params["page"] > storeleads.MAX_PAGE_INDEX:
            resp.raise_for_status.side_effect = RuntimeError("422 invalid page value")
        resp.json.return_value = {"apps": rows[params["page"] * PAGE : (params["page"] + 1) * PAGE], "total": len(rows)}
        return resp


def _source(apps, monkeypatch, max_page_index, stream="apps", **config):
    monkeypatch.setattr(storeleads, "MAX_PAGE_INDEX", max_page_index)
    src = storeleads.StoreleadsSource(
        storeleads.StoreleadsSourceConfig(name="storeleads", stream=stream, api_token="x", **config)
    )
    src.session = FakeSession(apps)
    src._throttle_list = lambda: None
    return src


def _walk_apps(src):
    records, pagination = [], {}
    for _ in range(100):
        it = src._get_apps_page(pagination)
        records += it.records
        pagination = it.next_pagination
        if pagination == {}:
            return records
    raise AssertionError("walk did not terminate")


def test_every_app_is_emitted_exactly_once_across_partitions(monkeypatch):
    apps = _catalog()
    src = _source(apps, monkeypatch, max_page_index=5)

    records = _walk_apps(src)

    assert sorted(r.id for r in records) == sorted(a["token"] for a in apps)
    assert all(isinstance(r.id, str) for r in records)
    assert src._apps_duplicates == 0
    assert sum(src._apps_part_totals.values()) == len(apps)
    assert not any("sort" in c for c in src.session.calls)


def test_pagination_walks_partitions_in_order_and_ends_empty(monkeypatch):
    src = _source(_catalog(), monkeypatch, max_page_index=5)
    seen, pagination = [], {}
    while True:
        it = src._get_apps_page(pagination)
        pagination = it.next_pagination
        if pagination == {}:
            break
        seen.append(pagination)

    assert seen[0] == {"part": 0, "page": 1}
    assert [p["part"] for p in seen] == sorted(p["part"] for p in seen)
    assert {p["part"] for p in seen} == {0, 1, 2}
    assert pagination == {}


def test_oversized_partition_raises(monkeypatch):
    src = _source(_catalog(n=600), monkeypatch, max_page_index=1)

    with pytest.raises(RuntimeError, match=r"shopify_in_categories.*rebalance the category set"):
        _walk_apps(src)


def test_raises_on_empty_page_before_total_is_reached(monkeypatch):
    src = _source(_catalog(), monkeypatch, max_page_index=5)
    src.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value={"apps": [], "total": 120})))

    with pytest.raises(RuntimeError, match="was empty"):
        src._get_apps_page({})


@pytest.mark.parametrize("body", [{"apps": []}, {"apps": [], "total": 0}])
def test_raises_when_total_is_missing_or_zero(monkeypatch, body):
    src = _source(_catalog(), monkeypatch, max_page_index=5)
    src.session.get = Mock(return_value=Mock(status_code=200, json=Mock(return_value=body)))

    with pytest.raises(RuntimeError, match="no `total`"):
        src._get_apps_page({})


def test_zero_total_mid_walk_does_not_end_the_partition_quietly(monkeypatch):
    src = _source(_catalog(), monkeypatch, max_page_index=5)
    real_get = src.session.get
    pages = iter(range(100))
    src.session.get = lambda url, params=None, timeout=None: (
        Mock(status_code=200, json=Mock(return_value={"apps": [], "total": 0}))
        if next(pages) == 2
        else real_get(url, params=params, timeout=timeout)
    )

    with pytest.raises(RuntimeError, match="no `total`"):
        _walk_apps(src)


@pytest.mark.parametrize("pagination", [{"part": 1, "page": 0}, {"part": 0, "page": 2}])
def test_raises_on_resume_without_walk_state(monkeypatch, pagination):
    src = _source(_catalog(), monkeypatch, max_page_index=5)

    with pytest.raises(RuntimeError, match="no in-memory walk state"):
        src._get_apps_page(pagination)


def test_app_reviews_scan_walks_only_shopify_partitions(monkeypatch):
    apps = _catalog()
    src = _source(apps, monkeypatch, max_page_index=5, stream="app_reviews")

    for _ in range(100):
        src._scan_apps_page({})
        if src._scan_done:
            break

    assert src._scan_done
    assert sorted(src._app_tokens) == sorted(a["token"] for a in apps if a["platform"] == "shopify")
    assert {c["f:p"] for c in src.session.calls} == {"shopify"}
    assert {c.get("f:p:op") for c in src.session.calls} == {None}
    assert not any("sort" in c for c in src.session.calls)


def test_app_reviews_scan_honours_max_review_scan_pages(monkeypatch):
    src = _source(_catalog(), monkeypatch, max_page_index=5, stream="app_reviews", max_review_scan_pages=2)

    for _ in range(10):
        src._scan_apps_page({})
        if src._scan_done:
            break

    assert src._scan_done
    assert len(src.session.calls) == 2
