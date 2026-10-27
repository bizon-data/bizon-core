from unittest.mock import Mock

import pytest

from bizon.connectors.sources.twilio.src.source import MAX_WORKER_FAILURES, TwilioSource, TwilioSourceConfig


def _source() -> TwilioSource:
    config = TwilioSourceConfig(name="twilio", stream="incoming_phone_numbers", account_sid="AC0", auth_token="x")
    source = TwilioSource(config)
    source._subaccounts_cache = [{"sid": "AC1"}]
    source.session.get = Mock(side_effect=ConnectionError("boom"))
    return source


def test_fanout_requeues_a_failing_worker_with_a_failure_count():
    iteration = _source().get(pagination=None)

    assert iteration.records == []
    assert iteration.next_pagination["workers"] == [{"sub_idx": 0, "next_uri": None, "failures": 1}]


def test_fanout_gives_up_on_a_persistent_error():
    source = _source()
    pagination = None
    with pytest.raises(RuntimeError, match="giving up"):
        for _ in range(MAX_WORKER_FAILURES):
            pagination = source.get(pagination=pagination).next_pagination
