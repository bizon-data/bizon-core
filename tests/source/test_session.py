import pickle
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

from bizon.source.config import SourceConfig
from bizon.source.session import CappedRetry, Session
from bizon.source.source import AbstractSource


class HttpSourceConfig(SourceConfig):
    pass


class HttpSource(AbstractSource):
    @staticmethod
    def streams():
        return ["items"]

    @staticmethod
    def get_config_class():
        return HttpSourceConfig

    def get_authenticator(self):
        return None

    def check_connection(self):
        return True, None

    def get_total_records_count(self):
        return None

    def get(self, pagination=None):
        raise NotImplementedError


def make_source(**http) -> HttpSource:
    config = HttpSourceConfig(name="http", stream="items", **({"http": http} if http else {}))
    source = HttpSource(config)
    # The default session only mounts https://, the test server is plain http.
    source.session.mount("http://", source.session.get_adapter("https://"))
    return source


@pytest.fixture
def server():
    """Serves the queued (status, headers, delay) responses in order, repeating the last one."""
    responses = []
    hits = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            hits.append(self.path)
            status, headers, delay = responses[min(len(hits), len(responses)) - 1]
            time.sleep(delay)
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{httpd.server_port}/", responses, hits
    httpd.shutdown()


def test_without_http_block_the_legacy_policy_is_kept():
    source = make_source()
    retries = source.session.get_adapter("https://").max_retries

    assert type(retries) is not CappedRetry
    assert retries.total == 20
    assert not retries.status_forcelist
    assert source.session.timeout is None


def test_http_block_retries_5xx_without_retry_after(server):
    url, responses, hits = server
    responses += [(502, {}, 0), (500, {}, 0), (200, {}, 0)]
    source = make_source(retries={"backoff_factor": 0})

    assert source.session.get(url).status_code == 200
    assert len(hits) == 3


def test_exhausted_retries_raise_http_error_with_the_last_response(server):
    url, responses, hits = server
    responses += [(503, {}, 0)]
    source = make_source(retries={"total": 2, "backoff_factor": 0})

    with pytest.raises(requests.HTTPError) as error:
        source.session.get(url)

    assert error.value.response.status_code == 503
    assert len(hits) == 3


def test_retry_after_is_capped(server):
    url, responses, hits = server
    responses += [(429, {"Retry-After": "812122"}, 0), (200, {}, 0)]
    source = make_source(retries={"backoff_factor": 0, "retry_after_max": 0.05})

    started = time.monotonic()
    assert source.session.get(url).status_code == 200
    assert time.monotonic() - started < 5
    assert len(hits) == 2


def test_cap_survives_urllib3_rebuilding_the_retry():
    retry = CappedRetry(total=5, max_retry_after=3)

    assert retry.increment(method="GET", url="/").max_retry_after == 3


def test_default_timeout_applies_and_an_explicit_one_wins(server):
    url, responses, hits = server
    responses += [(200, {}, 0.5)]
    source = make_source(timeout=0.1, retries={"total": 0})

    with pytest.raises(requests.exceptions.ConnectionError):
        source.session.get(url)
    assert source.session.get(url, timeout=5).status_code == 200


def test_raise_for_status_can_be_disabled(server):
    url, responses, hits = server
    responses += [(404, {}, 0)]
    source = make_source(raise_for_status=False)

    assert source.session.get(url).status_code == 404


def test_get_retry_policy_override_keeps_the_default_session():
    class TunedSource(HttpSource):
        def get_retry_policy(self):
            return CappedRetry(total=3, status_forcelist=[500], max_retry_after=1)

    source = TunedSource(HttpSourceConfig(name="http", stream="items"))
    retries = source.session.get_adapter("https://").max_retries

    assert retries.total == 3
    assert source.session.hooks["response"]


def test_session_timeout_survives_pickling():
    session = Session(timeout=(10, 60))

    assert pickle.loads(pickle.dumps(session)).timeout == (10, 60)


def test_http_block_warns_when_get_session_is_overridden():
    from loguru import logger

    class OwnSessionSource(HttpSource):
        def get_session(self):
            return requests.Session()

    messages = []
    sink = logger.add(messages.append, level="WARNING")
    try:
        OwnSessionSource(HttpSourceConfig(name="http", stream="items", http={}))
    finally:
        logger.remove(sink)

    assert any("overrides get_session()" in m for m in messages)
