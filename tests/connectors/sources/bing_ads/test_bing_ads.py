"""Pins the Microsoft Advertising error parser on payloads captured from the API."""

from bizon.connectors.sources.bing_ads.src import source as bing


def test_bing_invalid_column_index_from_error_path():
    # Captured from the API: an unknown report column name.
    error = bing.BingApiError(
        400,
        {
            "OperationErrors": [
                {
                    "Code": 100,
                    "ErrorCode": "NullRequest",
                    "Details": "Invalid JSON at line 0 position 230. Path: $.Columns[1]",
                }
            ]
        },
    )
    assert error.invalid_column_index() == 1
    restricted = bing.BingApiError(
        400, {"OperationErrors": [{"ErrorCode": "RestrictedColumnCombinations", "Details": None}]}
    )
    assert restricted.invalid_column_index() is None


def test_bing_impression_share_columns_live_in_their_own_stream():
    performance = set(bing.REPORT_SPECS["campaign_performance"][1])
    share = set(bing.REPORT_SPECS["campaign_impression_share"][1])
    assert "ImpressionSharePercent" not in performance and "ImpressionSharePercent" in share
    assert "BidMatchType" in performance and "BidMatchType" not in share
