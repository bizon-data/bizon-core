import pytest

from bizon.connectors.sources.sana_ai.src import source as sana
from bizon.connectors.sources.sana_ai.src.source import SanaReportJobError, SanaSource, SanaSourceConfig


@pytest.fixture
def make_source():
    def _make(**overrides) -> SanaSource:
        config = SanaSourceConfig.model_validate(
            {
                "name": "sana_ai",
                "stream": "insight_report",
                "domain": "example",
                "query": "SELECT 1",
                "init_pipeline": False,
                "authentication": {
                    "type": "oauth",
                    "params": {
                        "token_refresh_endpoint": "https://example.sana.ai/api/token",
                        "client_id": "id",
                        "client_secret": "secret",
                        "grant_type": "client_credentials",
                    },
                },
                **overrides,
            }
        )
        return SanaSource(config)

    return _make


@pytest.fixture
def clock(monkeypatch):
    now = [0.0]

    def sleep(seconds):
        now[0] += seconds

    monkeypatch.setattr(sana.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(sana.time, "sleep", sleep)
    return now


def stub_job(monkeypatch, statuses):
    statuses = iter(statuses)
    monkeypatch.setattr(SanaSource, "create_insight_report_job", lambda self, query: "job-1")
    monkeypatch.setattr(
        SanaSource,
        "get_insight_report_job",
        lambda self, job_id: {"data": {"status": next(statuses), "link": {"url": "https://example/report.csv"}}},
    )


def test_default_http_policy_retries_post(make_source):
    source = make_source()

    retries = source.session.get_adapter("https://example.sana.ai").max_retries
    assert "POST" in retries.allowed_methods
    assert 429 in retries.status_forcelist and 503 in retries.status_forcelist
    assert source.session.timeout == (10, 60)


def test_failed_job_raises(make_source, monkeypatch, clock):
    stub_job(monkeypatch, ["pending", "failed"])

    with pytest.raises(SanaReportJobError, match="job-1 failed"):
        make_source().get()


def test_stuck_job_gives_up_after_max_wait(make_source, monkeypatch, clock):
    stub_job(monkeypatch, ["pending"] * 1000)

    with pytest.raises(SanaReportJobError, match="still 'pending' after 30s"):
        make_source(report_job_max_wait_seconds=30).get()

    assert 30 < clock[0] <= 30 + sana.REPORT_JOB_POLL_INTERVAL_SECONDS


def test_successful_job_reads_csv(make_source, monkeypatch, clock):
    stub_job(monkeypatch, ["pending", "successful"])
    source = make_source()

    class CsvResponse:
        content = b"user,status\na,active\nb,disabled\n"

        def raise_for_status(self):
            pass

    monkeypatch.setattr(source.session, "get", lambda url, timeout: CsvResponse())

    iteration = source.get()

    assert [(r.id, r.data) for r in iteration.records] == [
        ("0", {"user": "a", "status": "active"}),
        ("1", {"user": "b", "status": "disabled"}),
    ]
    assert iteration.next_pagination == {}
