import gzip
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from google.api_core.exceptions import NotFound, PreconditionFailed
from pydantic import ValidationError

from bizon.connectors.sources.cloudflare_ai_gateway.src.source import (
    CloudflareAiGatewaySource,
    CloudflareAiGatewaySourceConfig,
    CloudflareAiGatewayStreams,
)
from bizon.source.models import SourceIncrementalState

UTC = timezone.utc
NOW = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)
BUCKET = "analytics-bucket"


def row(n, object_name="20261008/raw.log.gz", generation=111, **overrides):
    payload = {
        "source": {
            "bucket": "raw-bucket",
            "objectName": object_name,
            "generation": generation,
            "rowNumber": n,
        },
        "eventTimestamp": "2026-10-08T12:05:19Z",
        "gateway": "gw",
        "metadata": {"domain": "acme", "ticket_id": 7},
        "responseBody": {"choices": [{"text": "hi"}]},
    }
    payload.update(overrides)
    return payload


def ndjson(*rows):
    return ("\n".join(json.dumps(r) for r in rows) + "\n").encode()


class FakeBlob:
    def __init__(
        self,
        name,
        body=b"",
        *,
        generation=123,
        updated=NOW,
        size=None,
        download_error=None,
    ):
        self.name = name
        self.generation = generation
        self.updated = updated
        self.size = len(body) if size is None else size
        self._body = body
        self._download_error = download_error
        self.download_calls = []

    def download_as_bytes(self, **kwargs):
        self.download_calls.append(kwargs)
        if self._download_error:
            raise self._download_error
        return self._body


class FakePageIterator:
    def __init__(self, blobs, next_page_token):
        self._blobs = blobs
        self.next_page_token = next_page_token

    @property
    def pages(self):
        yield iter(self._blobs)

    def __iter__(self):
        return iter(self._blobs)


class FakeClient:
    """Lists a mutable, name-ordered bucket the way GCS does: start_offset is
    inclusive and a page holds at most page_size objects."""

    def __init__(self, blobs):
        self.blobs = sorted(blobs, key=lambda blob: blob.name)
        self.calls = []

    def list_blobs(self, bucket_name, **kwargs):
        self.calls.append((bucket_name, kwargs))
        start = kwargs.get("start_offset")
        remaining = [b for b in self.blobs if start is None or b.name >= start]
        size = kwargs["page_size"]
        token = "more" if len(remaining) > size else None
        return FakePageIterator(remaining[:size], token)


def make_source(blobs, **overrides):
    config = CloudflareAiGatewaySourceConfig(
        name="cloudflare_ai_gateway",
        stream="logs",
        bucket_name=BUCKET,
        **overrides,
    )
    with patch("bizon.connectors.sources.cloudflare_ai_gateway.src.source.storage.Client") as client_cls:
        client = FakeClient(blobs)
        client_cls.return_value = client
        source = CloudflareAiGatewaySource(config)
    return source, client


def state(last_run):
    return SourceIncrementalState(last_run=last_run, state={}, cursor_field="updated")


def drain(source):
    """Follow next_pagination to the end the way the bizon producer does."""
    pagination, records, iterations = {}, [], 0
    while True:
        iteration = source.get(pagination)
        records.extend(iteration.records)
        iterations += 1
        pagination = iteration.next_pagination
        if pagination == {}:
            return records, iterations
        assert iterations < 100, "pagination never terminated"


def test_one_record_per_line_with_stable_row_id_and_gcs_metadata():
    blob = FakeBlob("20261008/a.json", ndjson(row(0), row(1)), generation=777)
    source, _ = make_source([blob])

    iteration = source.get()

    assert iteration.next_pagination == {}
    first, second = iteration.records
    assert first.id == "gs://raw-bucket/20261008/raw.log.gz#111:0"
    assert second.id == "gs://raw-bucket/20261008/raw.log.gz#111:1"
    assert first.timestamp == datetime(2026, 10, 8, 12, 5, 19, tzinfo=UTC)
    assert first.data["metadata"] == {"domain": "acme", "ticket_id": 7}
    assert first.data["gcs"] == {
        "uri": f"gs://{BUCKET}/20261008/a.json",
        "generation": "777",
        "updated": "2026-10-08T12:00:00+00:00",
        "line_number": 1,
    }
    assert second.data["gcs"]["line_number"] == 2


def test_blank_lines_are_skipped_but_still_count_toward_line_number():
    body = json.dumps(row(0)).encode() + b"\n\n  \n" + json.dumps(row(1)).encode()
    source, _ = make_source([FakeBlob("20261008/a.json", body)])

    records = source.get().records

    assert [r.data["gcs"]["line_number"] for r in records] == [1, 4]


def test_gzip_payload_is_decompressed():
    body = gzip.compress(ndjson(row(0), row(1)))
    source, _ = make_source([FakeBlob("20261008/a.json", body)])

    assert len(source.get().records) == 2


def test_corrupt_gzip_raises_naming_the_object():
    source, _ = make_source([FakeBlob("20261008/a.json", b"\x1f\x8bnot gzip")])

    with pytest.raises(ValueError, match=rf"gs://{BUCKET}/20261008/a.json"):
        source.get()


def test_invalid_json_raises_with_uri_and_line():
    body = ndjson(row(0)) + b"{not json\n"
    source, _ = make_source([FakeBlob("20261008/a.json", body)])

    with pytest.raises(ValueError, match=rf"gs://{BUCKET}/20261008/a.json:2 "):
        source.get()


def test_non_object_line_raises():
    source, _ = make_source([FakeBlob("20261008/a.json", b"[1, 2]\n")])

    with pytest.raises(ValueError, match=r"a.json:1 is not a JSON object"):
        source.get()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda r: r.pop("source"),
        lambda r: r.update(source="gs://nope"),
        lambda r: r["source"].pop("rowNumber"),
        lambda r: r["source"].update(generation=None),
    ],
)
def test_missing_or_incomplete_source_raises_with_uri_and_line(mutate):
    bad = row(0)
    mutate(bad)
    source, _ = make_source([FakeBlob("20261008/a.json", ndjson(row(1), bad))])

    with pytest.raises(ValueError, match=r"a.json:2 is missing source"):
        source.get()


def test_row_number_zero_is_a_valid_identity():
    source, _ = make_source([FakeBlob("20261008/a.json", ndjson(row(0)))])

    assert source.get().records[0].id.endswith("#111:0")


@pytest.mark.parametrize("value", [None, "not a timestamp", 12])
def test_timestamp_falls_back_to_blob_updated(value):
    updated = NOW - timedelta(hours=1)
    bad = row(0, eventTimestamp=value)
    source, _ = make_source([FakeBlob("20261008/a.json", ndjson(bad), updated=updated)])

    assert source.get().records[0].timestamp == updated


def test_incremental_cutoff_is_last_run_minus_lookback_and_filters_on_updated():
    old = FakeBlob("20261008/old.json", ndjson(row(0, "old")), updated=NOW - timedelta(minutes=10))
    edge = FakeBlob("20261008/edge.json", ndjson(row(0, "edge")), updated=NOW - timedelta(minutes=4))
    new = FakeBlob("20261008/new.json", ndjson(row(0, "new")), updated=NOW)
    source, _ = make_source([old, edge, new], lookback_seconds=300)

    iteration = source.get_records_after(state(NOW - timedelta(minutes=0)))

    assert [r.data["gcs"]["uri"] for r in iteration.records] == [
        f"gs://{BUCKET}/20261008/edge.json",
        f"gs://{BUCKET}/20261008/new.json",
    ]
    assert old.download_calls == []
    assert iteration.next_pagination == {}


def test_cutoff_is_pinned_in_pagination_so_it_does_not_drift_across_iterations():
    blobs = [FakeBlob(f"20261008/{n}.json", ndjson(row(0, str(n))), size=60) for n in range(3)]
    source, _ = make_source(blobs, lookback_seconds=0, max_iteration_bytes=100)

    first = source.get_records_after(state(NOW - timedelta(hours=1)))
    pinned = first.next_pagination["cutoff"]
    second = source.get_records_after(state(NOW + timedelta(hours=5)), first.next_pagination)

    assert pinned == (NOW - timedelta(hours=1)).isoformat()
    assert second.next_pagination == {}


def test_full_refresh_pagination_has_no_cutoff():
    blobs = [FakeBlob(f"20261008/{n}.json", ndjson(row(0, str(n))), size=60) for n in range(3)]
    source, _ = make_source(blobs, max_iteration_bytes=100)

    assert source.get().next_pagination == {"after": "20261008/1.json"}


def test_notfound_and_precondition_failed_are_skipped_not_fatal():
    gone = FakeBlob("20261008/a.json", b"x", download_error=NotFound("gone"))
    replaced = FakeBlob("20261008/b.json", b"x", download_error=PreconditionFailed("412"))
    kept = FakeBlob("20261008/c.json", ndjson(row(0)))
    source, _ = make_source([gone, replaced, kept])

    records = source.get().records

    assert [r.data["gcs"]["uri"] for r in records] == [f"gs://{BUCKET}/20261008/c.json"]


def test_download_is_pinned_to_the_listed_generation_with_explicit_timeout():
    blob = FakeBlob("20261008/a.json", ndjson(row(0)), generation=42)
    source, _ = make_source([blob], request_timeout_seconds=30)

    source.get()

    assert blob.download_calls == [{"if_generation_match": 42, "timeout": (10, 30)}]


def test_unexpected_download_error_propagates():
    blob = FakeBlob("20261008/a.json", b"x", download_error=RuntimeError("boom"))
    source, _ = make_source([blob])

    with pytest.raises(RuntimeError, match="boom"):
        source.get()


def test_directory_placeholders_and_empty_objects_are_not_downloaded():
    placeholder = FakeBlob("20261008/", b"")
    empty = FakeBlob("20261008/empty.json", b"")
    real = FakeBlob("20261008/real.json", ndjson(row(0)))
    source, _ = make_source([placeholder, empty, real])

    records = source.get().records

    assert len(records) == 1
    assert placeholder.download_calls == [] and empty.download_calls == []


def test_byte_budget_splits_a_page_at_object_granularity_and_loses_nothing():
    blobs = [FakeBlob(f"20261008/{n:02d}.json", ndjson(row(0, f"o{n}")), size=40) for n in range(7)]
    source, _ = make_source(blobs, max_iteration_bytes=100)

    first = source.get()
    assert len(first.records) == 3  # 40+40+40 crosses 100, the stop is after the object
    assert first.next_pagination == {"after": "20261008/02.json"}

    records, iterations = drain_from(source, first.next_pagination)

    assert iterations == 2
    assert [r.id for r in first.records + records] == [f"gs://raw-bucket/o{n}#111:0" for n in range(7)]


def test_one_object_over_budget_is_still_taken_alone():
    big = FakeBlob("20261008/a.json", ndjson(row(0, "big")), size=500)
    small = FakeBlob("20261008/b.json", ndjson(row(0, "small")), size=10)
    source, _ = make_source([big, small], max_iteration_bytes=100)

    first = source.get()

    assert [r.data["gcs"]["uri"] for r in first.records] == [f"gs://{BUCKET}/20261008/a.json"]
    assert first.next_pagination == {"after": "20261008/a.json"}


def test_resume_survives_the_lifecycle_deleting_objects_between_iterations():
    blobs = [FakeBlob(f"20261008/{n:02d}.json", ndjson(row(0, f"o{n}")), size=60) for n in range(6)]
    source, client = make_source(blobs, max_iteration_bytes=100)

    first = source.get()
    taken = {r.id for r in first.records}
    client.blobs = [b for b in client.blobs if b.name not in ("20261008/00.json", "20261008/01.json")]
    rest, _ = drain_from(source, first.next_pagination)

    ids = taken | {r.id for r in rest}
    assert ids == {f"gs://raw-bucket/o{n}#111:0" for n in range(6)}


def drain_from(source, pagination):
    records, iterations = [], 0
    while pagination != {}:
        iteration = source.get(pagination)
        records.extend(iteration.records)
        pagination = iteration.next_pagination
        iterations += 1
        assert iterations < 100
    return records, iterations


def test_page_size_one_still_advances_past_the_inclusive_start_offset():
    blobs = [FakeBlob(f"20261008/{n}.json", ndjson(row(0, str(n)))) for n in range(4)]
    source, _ = make_source(blobs, page_size=1, max_iteration_bytes=0)

    records, iterations = drain(source)

    assert len(records) == 4
    assert iterations == 4


def test_no_budget_takes_the_whole_page_and_pages_through_the_bucket():
    blobs = [FakeBlob(f"20261008/{n}.json", ndjson(row(0, str(n)))) for n in range(5)]
    source, _ = make_source(blobs, page_size=2, max_iteration_bytes=0)

    records, iterations = drain(source)

    assert len(records) == 5
    assert iterations == 3


def test_empty_bucket_ends_immediately_with_an_empty_dict():
    source, _ = make_source([])

    iteration = source.get()

    assert iteration.records == []
    assert iteration.next_pagination == {}


def test_next_pagination_is_literal_safe():
    blobs = [FakeBlob(f"20261008/{n}.json", ndjson(row(0, str(n))), size=60) for n in range(3)]
    source, _ = make_source(blobs, max_iteration_bytes=100)

    pagination = source.get_records_after(state(NOW)).next_pagination

    assert json.loads(json.dumps(pagination)) == pagination
    assert all(isinstance(v, str) for v in pagination.values())


def test_listing_options_reach_list_blobs():
    fields = "items(name,generation,updated,size),nextPageToken"
    source, client = make_source([], list_fields=fields, prefix="20261008/")

    source.get()

    ((bucket, kwargs),) = client.calls
    assert bucket == BUCKET
    assert kwargs["fields"] == fields
    assert "match_glob" not in kwargs
    assert kwargs["prefix"] == "20261008/"
    assert kwargs["timeout"] == (10, 60)


def test_empty_prefix_lists_the_whole_bucket():
    source, client = make_source([])

    source.get()

    assert client.calls[0][1]["prefix"] is None


def test_check_connection_reports_listing_failures():
    ok_source, _ = make_source([FakeBlob("20261008/a.json", ndjson(row(0)))])
    assert ok_source.check_connection() == (True, None)

    source, client = make_source([])
    client.list_blobs = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("403"))
    ok, message = source.check_connection()
    assert ok is False
    assert BUCKET in message


def test_list_fields_must_request_the_page_token():
    with pytest.raises(ValidationError, match="nextPageToken"):
        CloudflareAiGatewaySourceConfig(
            name="cloudflare_ai_gateway",
            stream="logs",
            bucket_name="bucket",
            list_fields="items(name,generation)",
        )


def test_config_builds_from_a_plain_dict():
    config = CloudflareAiGatewaySource.get_config_class().model_validate(
        {"name": "cloudflare_ai_gateway", "stream": "logs", "bucket_name": "bucket"}
    )
    assert config.stream == CloudflareAiGatewayStreams.LOGS
    assert CloudflareAiGatewaySource.streams() == ["logs"]
