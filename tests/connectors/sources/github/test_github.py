import os
import shutil
import tarfile
import unittest
from unittest.mock import Mock, patch

from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import RetryError
from tenacity import stop_after_attempt, wait_none

from bizon.connectors.sources.github.src import source as github_source

REAL_QUERY = "REAL_QUERY"
PROBE_QUERY = "PROBE_QUERY"


def _bare_source(max_phantom_skips_per_page: int = 50, max_phantom_skips_per_run: int = 200):
    """A GitHubSource with just enough state for _graphql_post_adaptive: bypasses
    __init__ (which needs a real GitHubSourceConfig + auth), stubs config with only
    the two knobs the skip path reads, and seeds the run-wide skip counter."""
    source = object.__new__(github_source.GitHubSource)
    source.config = Mock(
        max_phantom_skips_per_page=max_phantom_skips_per_page,
        max_phantom_skips_per_run=max_phantom_skips_per_run,
    )
    source._phantom_skips = github_source.MutableCounter()
    # AbstractSource.__del__ closes self.session; give it a stub so bypassing
    # __init__ here doesn't emit a spurious AttributeError on teardown.
    source.session = Mock()
    return source


def _probe_response(end_cursor, has_next_page=True):
    return {
        "data": {"repository": {"pullRequests": {"pageInfo": {"endCursor": end_cursor, "hasNextPage": has_next_page}}}}
    }


def _real_success_response():
    return {
        "data": {
            "repository": {
                "pullRequests": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        }
    }


class GitHubRetryTests(unittest.TestCase):
    def test_rest_session_raises_retry_error_and_leaves_status_handling_to_the_source(self) -> None:
        config = github_source.GitHubSourceConfig(name="github", stream="commits", app_id="1", private_key="k")
        source = github_source.GitHubSource(config)
        retry = source.session.get_adapter("https://api.github.com").max_retries
        self.assertTrue(retry.raise_on_status)
        self.assertEqual(retry.status_forcelist, [429, 500, 502, 503, 504])
        self.assertEqual(source.session.hooks["response"], [])

    def test_http_adapter_retry_exhaustion_is_retryable_by_outer_backoff(self) -> None:
        """An adapter RetryError must reach tenacity, not fail after one cycle."""
        source = object.__new__(github_source.GitHubSource)
        source._wait_for_rate_limit = Mock()
        source.auth = Mock()
        source.auth.get_installation_token_threadsafe.return_value = "token"
        source.session = Mock()
        successful_response = Mock(status_code=200, headers={})
        source.session.get.side_effect = [RetryError("500 responses"), successful_response]

        retrying_get = github_source.GitHubSource._api_get_with_token.retry_with(
            wait=wait_none(),
            stop=stop_after_attempt(2),
        )
        result = retrying_get(source, "https://api.github.com/test", 123)

        self.assertIs(result, successful_response)
        self.assertEqual(source.session.get.call_count, 2)


class GraphQLTokenMintingTests(unittest.TestCase):
    """The installation token is minted per attempt INSIDE the transport's retry
    ladder. Minting it outside would leave POST /app/installations/{id}/access_tokens
    -- a real network call, made roughly hourly on a long backfill -- unprotected,
    so one blip there would fail the whole stream with a RepoFetchError."""

    @staticmethod
    def _source(token_side_effect, post_body=None):
        source = object.__new__(github_source.GitHubSource)
        source.session = Mock()
        response = Mock(status_code=200, ok=True, headers={}, text="")
        response.json.return_value = post_body or {"data": {"ok": True}}
        source.session.post.return_value = response
        # The GraphQL POST and the token mint share one mock here so the
        # assertions on session.post below cover both.
        source._graphql_session = source.session
        source.auth = Mock()
        source.auth.get_installation_token_threadsafe.side_effect = token_side_effect
        return source

    def test_transient_error_minting_the_token_is_retried(self) -> None:
        source = self._source([RequestsConnectionError("connection reset"), "tok"])

        with patch.object(github_source.time, "sleep") as sleep:
            result = source._graphql_post(REAL_QUERY, {"first": 25}, 123)

        self.assertEqual(result, {"data": {"ok": True}})
        self.assertEqual(source.auth.get_installation_token_threadsafe.call_count, 2)
        self.assertEqual(source.session.post.call_count, 1, "the failed attempt never got to POST")
        sleep.assert_called_once_with(60)

    def test_token_minting_retries_share_the_rate_limit_budget(self) -> None:
        """Exhausting the ladder on the token call re-raises the network error
        rather than hanging or swallowing it."""
        source = self._source(RequestsConnectionError("connection reset"))

        with patch.object(github_source.time, "sleep"):
            with self.assertRaises(RequestsConnectionError):
                source._graphql_post(REAL_QUERY, {"first": 25}, 123)

        self.assertEqual(source.auth.get_installation_token_threadsafe.call_count, 6)

    def test_adapter_retry_error_minting_the_token_is_retried(self) -> None:
        """An exhausted HTTPAdapter raises RetryError, not HTTPError -- it must
        still be caught by the ladder."""
        source = self._source([RetryError("500 responses"), "tok"])

        with patch.object(github_source.time, "sleep"):
            source._graphql_post(REAL_QUERY, {"first": 25}, 123)

        self.assertEqual(source.auth.get_installation_token_threadsafe.call_count, 2)


class TransformGraphqlPrTests(unittest.TestCase):
    """_transform_graphql_pr_to_rest must emit the REST assignees shape on every fetch."""

    def test_assignees_mapped_to_rest_shape(self) -> None:
        node = {
            "databaseId": 27123,
            "number": 27123,
            "state": "MERGED",
            "assignees": {
                "totalCount": 2,
                "nodes": [
                    {"login": "hubot", "avatarUrl": "https://a", "databaseId": 101},
                    {"login": "octocat", "avatarUrl": "https://b", "databaseId": 102},
                ],
            },
        }
        pr = github_source.GitHubSource._transform_graphql_pr_to_rest(node)
        self.assertEqual(
            pr["assignees"],
            [
                {"login": "hubot", "id": 101, "avatar_url": "https://a"},
                {"login": "octocat", "id": 102, "avatar_url": "https://b"},
            ],
        )

    def test_assignees_empty_list_when_none(self) -> None:
        """No assignees connection (or empty nodes) must emit [], not None -- an
        explicit empty array is a real value that overwrites stale rows downstream,
        whereas None would leave json_query returning NULL."""
        node = {"databaseId": 1, "number": 1, "state": "OPEN"}
        pr = github_source.GitHubSource._transform_graphql_pr_to_rest(node)
        self.assertEqual(pr["assignees"], [])


class GraphQLPostAdaptivePhantomSkipTests(unittest.TestCase):
    """Coverage for the phantom-index skip in _graphql_post_adaptive.

    The shrink ladder for start_size=25, min_size=1 is [25, 10, 5, 1] (4 calls) --
    see _build_page_shrink_sequence. All fixtures below build on that.
    """

    def test_phantom_run_recovers_and_returns_start_size(self) -> None:
        """N contiguous phantom entries: each needs its own probe-skip-retry cycle;
        once past all N, the real query succeeds. The returned page size must be
        start_size, not min_size -- the shrink ladder is not the remedy here."""
        source = _bare_source()
        phantom_count = 3
        # 4 ladder attempts + phantom_count post-skip retries, the last of which succeeds.
        total_real_calls_before_success = 4 + phantom_count
        real_calls = {"n": 0}
        probe_calls = {"n": 0}

        def side_effect(query, variables, installation_id):
            if query == PROBE_QUERY:
                probe_calls["n"] += 1
                return _probe_response(f"cursor-{probe_calls['n']}")
            real_calls["n"] += 1
            if real_calls["n"] < total_real_calls_before_success:
                raise github_source.TransientGraphQLError("something went wrong")
            return _real_success_response()

        source._graphql_post = Mock(side_effect=side_effect)

        variables = {"owner": "acme", "name": "widgets", "first": 25}
        result, page_size = source._graphql_post_adaptive(
            REAL_QUERY,
            variables,
            installation_id=1,
            start_size=25,
            min_size=1,
            repo="acme/widgets",
            cursor_probe_query=PROBE_QUERY,
        )

        self.assertEqual(result, _real_success_response())
        self.assertEqual(page_size, 25, "must resume at start_size, not min_size")
        self.assertEqual(source._phantom_skips.value, phantom_count)
        self.assertEqual(probe_calls["n"], phantom_count)
        self.assertEqual(variables["after"], f"cursor-{phantom_count}")

    def test_probe_also_fails_propagates_transient_error(self) -> None:
        """If the pageInfo-only probe fails too, it's a real outage, not a phantom
        entry -- the fail-closed behaviour must still fire."""
        source = _bare_source()

        def side_effect(query, variables, installation_id):
            raise github_source.TransientGraphQLError("service unavailable")

        source._graphql_post = Mock(side_effect=side_effect)
        variables = {"owner": "acme", "name": "widgets", "first": 25}

        with self.assertRaises(github_source.TransientGraphQLError):
            source._graphql_post_adaptive(
                REAL_QUERY,
                variables,
                installation_id=1,
                start_size=25,
                min_size=1,
                repo="acme/widgets",
                cursor_probe_query=PROBE_QUERY,
            )
        self.assertEqual(source._phantom_skips.value, 0)

    def test_max_phantom_skips_per_page_exhausted_raises(self) -> None:
        """A pathologically long phantom run must still fail closed rather than
        skip forever -- bounded by max_phantom_skips_per_page."""
        source = _bare_source(max_phantom_skips_per_page=2)
        probe_calls = {"n": 0}

        def side_effect(query, variables, installation_id):
            if query == PROBE_QUERY:
                probe_calls["n"] += 1
                return _probe_response(f"cursor-{probe_calls['n']}")
            # The real query never recovers within the page-cap window.
            raise github_source.TransientGraphQLError("something went wrong")

        source._graphql_post = Mock(side_effect=side_effect)
        variables = {"owner": "acme", "name": "widgets", "first": 25}

        with self.assertRaises(github_source.TransientGraphQLError):
            source._graphql_post_adaptive(
                REAL_QUERY,
                variables,
                installation_id=1,
                start_size=25,
                min_size=1,
                repo="acme/widgets",
                cursor_probe_query=PROBE_QUERY,
            )
        self.assertEqual(source._phantom_skips.value, 2)
        self.assertEqual(probe_calls["n"], 2)

    def test_max_phantom_skips_per_run_exceeded_raises_systemic_runtime_error(self) -> None:
        """The run-wide ceiling must raise RuntimeError (not the transient error) so
        a systemically broken run doesn't silently report a large gap as success."""
        source = _bare_source(max_phantom_skips_per_page=50, max_phantom_skips_per_run=2)
        source._phantom_skips = github_source.MutableCounter(2)  # at the ceiling from earlier pages

        def side_effect(query, variables, installation_id):
            if query == PROBE_QUERY:
                return _probe_response("cursor-1")
            raise github_source.TransientGraphQLError("something went wrong")

        source._graphql_post = Mock(side_effect=side_effect)
        variables = {"owner": "acme", "name": "widgets", "first": 25}

        with self.assertRaises(RuntimeError) as ctx:
            source._graphql_post_adaptive(
                REAL_QUERY,
                variables,
                installation_id=1,
                start_size=25,
                min_size=1,
                repo="acme/widgets",
                cursor_probe_query=PROBE_QUERY,
            )
        self.assertNotIsInstance(ctx.exception, github_source.TransientGraphQLError)
        self.assertIn("stream reset", str(ctx.exception))
        self.assertEqual(source._phantom_skips.value, 3)

    def test_cursor_probe_query_none_behaves_like_today(self) -> None:
        """The deployments call site (and any future one) passes no probe query --
        behaviour must be identical to before the skip fix: raise once the shrink
        ladder is exhausted, no probing, no skip counter movement."""
        source = _bare_source()
        source._graphql_post = Mock(side_effect=github_source.TransientGraphQLError("boom"))
        variables = {"owner": "acme", "name": "widgets", "first": 25}

        with self.assertRaises(github_source.TransientGraphQLError):
            source._graphql_post_adaptive(
                REAL_QUERY,
                variables,
                installation_id=1,
                start_size=25,
                min_size=1,
                repo="acme/widgets",
            )

        # Only the 4-step shrink ladder should have run -- no probe calls at all.
        self.assertEqual(source._graphql_post.call_count, 4)
        self.assertEqual(source._phantom_skips.value, 0)

    def test_phantom_recovery_restores_configured_max_size_not_start_size(self) -> None:
        """By the time pagination reaches a phantom run, the shrink ladder
        has already walked page_size down to the floor, so the `start_size` this
        call receives is already `min_size` -- mimicked here with
        start_size=min_size=1. Recovery must restore the stream's configured
        page size (`max_size`), not the already-shrunk `start_size`/1, or every
        subsequent page in the repo is fetched one row at a time."""
        source = _bare_source()
        phantom_count = 2
        # 1 ladder attempt (start_size == min_size == 1, so the ladder is a single
        # step) + phantom_count post-skip retries, the last of which succeeds.
        total_real_calls_before_success = 1 + phantom_count
        real_calls = {"n": 0}
        probe_calls = {"n": 0}

        def side_effect(query, variables, installation_id):
            if query == PROBE_QUERY:
                probe_calls["n"] += 1
                return _probe_response(f"cursor-{probe_calls['n']}")
            real_calls["n"] += 1
            if real_calls["n"] < total_real_calls_before_success:
                raise github_source.TransientGraphQLError("something went wrong")
            return _real_success_response()

        source._graphql_post = Mock(side_effect=side_effect)

        variables = {"owner": "acme", "name": "widgets", "first": 1}
        result, page_size = source._graphql_post_adaptive(
            REAL_QUERY,
            variables,
            installation_id=1,
            start_size=1,
            min_size=1,
            repo="acme/widgets",
            cursor_probe_query=PROBE_QUERY,
            max_size=25,
        )

        self.assertEqual(result, _real_success_response())
        self.assertEqual(
            page_size,
            25,
            "must restore the configured max_size, not the already-shrunk start_size (this "
            "is the exact defect: returning start_size here would return 1, a no-op)",
        )
        self.assertEqual(source._phantom_skips.value, phantom_count)

    def test_phantom_recovery_without_max_size_returns_start_size(self) -> None:
        """Back-compat: a call site that doesn't pass `max_size` (the default)
        must keep the pre-existing behaviour of resuming at `start_size`."""
        source = _bare_source()
        phantom_count = 2
        total_real_calls_before_success = 4 + phantom_count  # ladder for 10->5->1 is 3 steps + start_size itself
        real_calls = {"n": 0}
        probe_calls = {"n": 0}

        def side_effect(query, variables, installation_id):
            if query == PROBE_QUERY:
                probe_calls["n"] += 1
                return _probe_response(f"cursor-{probe_calls['n']}")
            real_calls["n"] += 1
            if real_calls["n"] < total_real_calls_before_success:
                raise github_source.TransientGraphQLError("something went wrong")
            return _real_success_response()

        source._graphql_post = Mock(side_effect=side_effect)

        variables = {"owner": "acme", "name": "widgets", "first": 10}
        result, page_size = source._graphql_post_adaptive(
            REAL_QUERY,
            variables,
            installation_id=1,
            start_size=10,
            min_size=1,
            repo="acme/widgets",
            cursor_probe_query=PROBE_QUERY,
            # max_size intentionally omitted
        )

        self.assertEqual(result, _real_success_response())
        self.assertEqual(page_size, 10, "with max_size omitted, must default to start_size")

    def test_normal_shrink_path_unaffected_by_max_size(self) -> None:
        """A plain (non-phantom) shrink -- success at a smaller ladder size, no
        cursor probe ever invoked -- must still return the size that actually
        succeeded. `max_size` only changes behaviour on the phantom-recovery
        path; it must not leak into the ordinary shrink-and-retry return value."""
        source = _bare_source()
        call_sizes = []

        def side_effect(query, variables, installation_id):
            call_sizes.append(variables["first"])
            if variables["first"] == 25:
                raise github_source.TransientGraphQLError("something went wrong")
            return _real_success_response()

        source._graphql_post = Mock(side_effect=side_effect)
        variables = {"owner": "acme", "name": "widgets", "first": 25}

        result, page_size = source._graphql_post_adaptive(
            REAL_QUERY,
            variables,
            installation_id=1,
            start_size=25,
            min_size=1,
            repo="acme/widgets",
            max_size=99,
        )

        self.assertEqual(result, _real_success_response())
        self.assertEqual(call_sizes, [25, 10])
        self.assertEqual(page_size, 10, "must return the size that actually succeeded, not max_size")

    def test_probe_no_progress_raises_instead_of_spinning(self) -> None:
        """If the probe ever returns the same endCursor as the current position
        (no progress -- e.g. it echoes back the just-skipped-to cursor), it must
        raise rather than loop forever re-probing the same dead end."""
        source = _bare_source()
        probe_calls = {"n": 0}

        def side_effect(query, variables, installation_id):
            if query == PROBE_QUERY:
                probe_calls["n"] += 1
                # First probe makes progress to cursor-1; second probe stalls there.
                return _probe_response("cursor-1")
            raise github_source.TransientGraphQLError("something went wrong")

        source._graphql_post = Mock(side_effect=side_effect)
        variables = {"owner": "acme", "name": "widgets", "first": 25}

        with self.assertRaises(github_source.TransientGraphQLError):
            source._graphql_post_adaptive(
                REAL_QUERY,
                variables,
                installation_id=1,
                start_size=25,
                min_size=1,
                repo="acme/widgets",
                cursor_probe_query=PROBE_QUERY,
            )

        # One successful skip to cursor-1, then the second probe echoes cursor-1
        # back (no progress) and we bail instead of skipping again.
        self.assertEqual(probe_calls["n"], 2)
        self.assertEqual(source._phantom_skips.value, 1)


class GitHubTarballExtractionTests(unittest.TestCase):
    """_fetch_repo_tarball extracts only tree-filtered regular files -- a symlink or
    a path-traversal member must never land outside the per-repo temp dir."""

    @staticmethod
    def _fake_tarball_bytes() -> bytes:
        import io

        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:

            def add_file(name: str, content: bytes):
                info = tarfile.TarInfo(name=name)
                info.size = len(content)
                tar.addfile(info, io.BytesIO(content))

            add_file("owner-repo-abc123/good.txt", b"hello")
            add_file("owner-repo-abc123/../evil.txt", b"pwned")

            link_info = tarfile.TarInfo(name="owner-repo-abc123/link.txt")
            link_info.type = tarfile.SYMTYPE
            link_info.linkname = "good.txt"
            tar.addfile(link_info)
        return buf.getvalue()

    def test_extraction_rejects_path_traversal_and_symlinks(self) -> None:
        source = object.__new__(github_source.GitHubSource)
        # AbstractSource.__del__ closes self.session; stub it so bypassing __init__
        # here doesn't emit a spurious AttributeError on teardown (see _bare_source).
        source.session = Mock()
        tar_bytes = self._fake_tarball_bytes()

        fake_response = Mock()
        fake_response.status_code = 200
        fake_response.raise_for_status = Mock()
        fake_response.iter_content = Mock(return_value=iter([tar_bytes]))
        fake_response.close = Mock()
        source._api_get_with_token = Mock(return_value=fake_response)

        wanted_paths = {"good.txt", "../evil.txt", "link.txt"}
        result = source._fetch_repo_tarball("owner/repo", "main", 123, wanted_paths)

        self.assertIsNotNone(result)
        extract_dir, missing = result
        try:
            self.assertTrue(os.path.isfile(os.path.join(extract_dir, "good.txt")))
            # Neither the traversal member nor the symlink is extracted at all --
            # both stay in `missing` for the per-blob fallback to pick up.
            self.assertFalse(os.path.exists(os.path.join(os.path.dirname(extract_dir), "evil.txt")))
            self.assertFalse(os.path.exists(os.path.join(extract_dir, "link.txt")))
            self.assertEqual(missing, {"../evil.txt", "link.txt"})
        finally:
            shutil.rmtree(extract_dir, ignore_errors=True)

        fake_response.close.assert_called_once()


class GraphQLGatewayTimeoutShrinkTests(unittest.TestCase):
    """GitHub answers an over-heavy /graphql page with a persistent 502. That
    must reach the adaptive page-size shrink quickly, not sit on the 60-900s
    network backoff at the same page size, which can spend over an hour on one page.

    Uses a real local HTTP server so the urllib3 Retry config in
    _build_graphql_session is actually exercised, not mocked away."""

    def _serve(self, handler_cls):
        import threading
        from http.server import ThreadingHTTPServer

        server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_address[1]}/graphql"

    def _source(self):
        source = object.__new__(github_source.GitHubSource)
        source.session = Mock()
        source.config = Mock(max_phantom_skips_per_page=0, max_phantom_skips_per_run=0)
        source._phantom_skips = github_source.MutableCounter()
        source.auth = Mock()
        source.auth.get_installation_token_threadsafe.return_value = "tok"
        graphql_session = github_source.GitHubSource._build_graphql_session()
        graphql_session.mount("http://", graphql_session.get_adapter("https://"))
        source._graphql_session = graphql_session
        return source

    def test_classifier_treats_5xx_as_transient(self) -> None:
        for status in (500, 502, 503, 504):
            response = Mock(status_code=status, ok=False)
            self.assertIs(
                github_source._classify_github_graphql(response, {}),
                github_source.ErrorClass.TRANSIENT,
            )

    def test_persistent_502_shrinks_page_instead_of_long_backoff(self) -> None:
        import json
        from http.server import BaseHTTPRequestHandler

        sizes_seen = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # keep test output quiet
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                first = body["variables"]["first"]
                sizes_seen.append(first)
                if first > 10:
                    self.send_response(502)
                    self.send_header("Content-Type", "text/html")
                    self.end_headers()
                    self.wfile.write(b"<html>502 Bad Gateway</html>")
                    return
                payload = json.dumps(_real_success_response()).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        url = self._serve(Handler)
        source = self._source()

        with patch.object(github_source, "GRAPHQL_URL", url), patch.object(github_source.time, "sleep") as sleep:
            result, page_size = source._graphql_post_adaptive(
                REAL_QUERY,
                {"owner": "acme", "name": "big-repo", "first": 25},
                installation_id=1,
                start_size=25,
                repo="acme/big-repo",
            )

        self.assertEqual(result, _real_success_response())
        self.assertEqual(page_size, 10)
        # 3 transient attempts at size 25, each = 1 request + GRAPHQL_ADAPTER_RETRIES
        # quick adapter retries; then size 10 succeeds first time.
        per_attempt = 1 + github_source.GRAPHQL_ADAPTER_RETRIES
        self.assertEqual(sizes_seen, [25] * (3 * per_attempt) + [10])
        # Nothing reached the 60-900s rate-limit backoff.
        self.assertTrue(all(call.args[0] < 60 for call in sleep.call_args_list), sleep.call_args_list)

    def test_429_is_not_retried_by_the_graphql_adapter(self) -> None:
        """429 belongs to the classifier's reset-hint path, not to the adapter."""
        session = github_source.GitHubSource._build_graphql_session()
        retry = session.get_adapter("https://api.github.com").max_retries
        self.assertNotIn(429, retry.status_forcelist)
        self.assertFalse(retry.raise_on_status)


class WorkflowJobsPageShrinkTests(unittest.TestCase):
    """GitHub can return a deterministic 502 for a 100-job page of a big run
    (here 225 jobs) while 50/25 render fine.
    `_fetch_jobs_for_runs` must restart the run at a smaller page size instead of
    retrying the same page, and must stay fail-closed if no size works."""

    RUN_ID = 1234567890
    TOTAL_JOBS = 225

    def _source(self, status_for):
        """`status_for(per_page, page)` -> HTTP status; 200 pages are served from a
        225-job run by offset, like the real endpoint."""
        source = object.__new__(github_source.GitHubSource)
        source.session = Mock()
        source.config = Mock(per_page=100)
        source._jobs_session = Mock(name="jobs_session")
        self.calls = []

        def fake_get(url, installation_id, session=None, **kwargs):
            params = kwargs["params"]
            per_page, page = params["per_page"], params["page"]
            self.calls.append((per_page, page, session, kwargs.get("timeout")))
            status = status_for(per_page, page)
            response = Mock(status_code=status)
            if status == 200:
                start = (page - 1) * per_page
                ids = range(start, min(start + per_page, self.TOTAL_JOBS))
                response.json.return_value = {"jobs": [{"id": i} for i in ids]}
            return response

        source._api_get_with_token = fake_get
        return source

    def test_502_on_page_two_restarts_at_smaller_size_without_duplicates(self) -> None:
        source = self._source(lambda per_page, page: 502 if per_page == 100 and page == 2 else 200)

        records = source._fetch_jobs_for_runs([{"id": self.RUN_ID}], "acme/ml", 1)

        ids = [r.id for r in records]
        self.assertEqual(len(ids), self.TOTAL_JOBS)
        self.assertEqual(len(set(ids)), self.TOTAL_JOBS)
        # page 1 and the failing page 2 at 100, then a fresh pass at 50 (5 pages)
        self.assertEqual(
            [(per_page, page) for per_page, page, _, _ in self.calls],
            [(100, 1), (100, 2), (50, 1), (50, 2), (50, 3), (50, 4), (50, 5)],
        )
        for _, _, session, timeout in self.calls:
            self.assertIs(session, source._jobs_session)
            self.assertEqual(timeout, (10, 60))

    def test_persistent_5xx_at_every_size_raises_repo_fetch_error(self) -> None:
        source = self._source(lambda per_page, page: 502)

        with self.assertRaises(github_source.RepoFetchError) as ctx:
            source._fetch_jobs_for_runs([{"id": self.RUN_ID}], "acme/ml", 1)

        self.assertIn(str(self.RUN_ID), str(ctx.exception))
        self.assertEqual([c[0] for c in self.calls], [100, 50, 25, 10])

    def test_healthy_run_makes_only_per_page_100_requests(self) -> None:
        source = self._source(lambda per_page, page: 200)

        records = source._fetch_jobs_for_runs([{"id": self.RUN_ID}], "acme/ml", 1)

        self.assertEqual(len({r.id for r in records}), self.TOTAL_JOBS)
        self.assertEqual([(c[0], c[1]) for c in self.calls], [(100, 1), (100, 2), (100, 3)])

    def test_404_still_skips_run(self) -> None:
        source = self._source(lambda per_page, page: 404)

        self.assertEqual(source._fetch_jobs_for_runs([{"id": self.RUN_ID}], "acme/ml", 1), [])
        self.assertEqual(len(self.calls), 1)


if __name__ == "__main__":
    unittest.main()
