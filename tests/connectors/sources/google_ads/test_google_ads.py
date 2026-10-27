"""Pins the Google Ads error parsers on payloads captured from the API."""

from bizon.connectors.sources.google_ads.src import source as gads


def test_google_ads_prunes_metrics_not_the_grain_segment():
    # Captured from the API: selecting all campaign metrics with segments.date.
    payload = {
        "error": {
            "details": [
                {
                    "errors": [
                        {
                            "errorCode": {"queryError": "PROHIBITED_SEGMENT_WITH_METRIC_IN_SELECT_OR_WHERE_CLAUSE"},
                            "message": "Cannot select the following segments because at least one unsupported metric "
                            "is found in SELECT or WHERE clause: 'segments.date'(unsupported metrics: "
                            "'optimization_score_uplift', 'optimization_score_url').",
                        }
                    ]
                }
            ]
        }
    }
    selected = [
        "campaign.id",
        "segments.date",
        "metrics.optimization_score_uplift",
        "metrics.optimization_score_url",
        "metrics.clicks",
    ]
    rejected = gads.GoogleAdsSource._rejected_fields(payload, selected, ["campaign.id", "segments.date"])
    assert rejected == ["metrics.optimization_score_uplift", "metrics.optimization_score_url"]


def test_google_ads_prunes_field_path_and_access_denied_metrics():
    not_allowlisted = {
        "error": {
            "details": [
                {
                    "errors": [
                        {
                            "errorCode": {"notAllowlistedError": "CUSTOMER_NOT_ALLOWLISTED_FOR_THIS_FEATURE"},
                            "location": {"fieldPathElements": [{"fieldName": "metrics"}, {"fieldName": "svr"}]},
                        }
                    ]
                }
            ]
        }
    }
    access_denied = {
        "error": {
            "details": [
                {
                    "errors": [
                        {
                            "errorCode": {"authorizationError": "METRIC_ACCESS_DENIED"},
                            "message": "The developer doesn't have access to metrics: "
                            "''auction_insight_search_impression_share', 'auction_insight_search_overlap_rate''.",
                        }
                    ]
                }
            ]
        }
    }
    selected = [
        "campaign.id",
        "metrics.svr",
        "metrics.auction_insight_search_impression_share",
        "metrics.auction_insight_search_overlap_rate",
        "metrics.clicks",
    ]
    assert gads.GoogleAdsSource._rejected_fields(not_allowlisted, selected, ["campaign.id"]) == ["metrics.svr"]
    assert gads.GoogleAdsSource._rejected_fields(access_denied, selected, ["campaign.id"]) == [
        "metrics.auction_insight_search_impression_share",
        "metrics.auction_insight_search_overlap_rate",
    ]


def test_google_ads_geo_target_constants_read_once_without_customer_id(monkeypatch):
    src = gads.GoogleAdsSource(
        gads.GoogleAdsSourceConfig(
            name="google_ads",
            stream="geo_target_constants",
            client_id="c",
            client_secret="s",
            refresh_token="r",
            login_customer_id="123-456-7890",
        )
    )
    monkeypatch.setattr(
        src, "_resource_attributes", lambda resource: ["geo_target_constant.id", "geo_target_constant.name"]
    )
    queries = []
    pages = {
        None: {
            "results": [{"geoTargetConstant": {"resourceName": "geoTargetConstants/200743", "name": "A"}}],
            "nextPageToken": "p2",
        },
        "p2": {"results": [{"geoTargetConstant": {"resourceName": "geoTargetConstants/1012908", "name": "B"}}]},
    }

    def search(customer_id, query, page_token=None):
        queries.append((customer_id, query))
        return 200, pages[page_token]

    monkeypatch.setattr(src, "_search", search)
    first = src.get()
    second = src.get(first.next_pagination)
    assert second.next_pagination == {}
    assert [q[0] for q in queries] == ["1234567890", "1234567890"]
    assert all("customer.id" not in q[1] and "FROM geo_target_constant" in q[1] for q in queries)
    assert len({r.id for r in first.records + second.records}) == 2
