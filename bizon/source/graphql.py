"""Shared GraphQL transport for bizon sources.

Bounded-retry POST, a same-cursor page-size shrink ladder, and an optional
phantom-entry cursor probe, shared by the GraphQL sources. Deliberately
API-agnostic: every API-specific decision (which `errors[]` shapes are
transient, where `pageInfo` lives, how to read a rate-limit reset header, how
to acquire a token) is injected by the caller.

The helper never creates a session and never calls `requests.*` directly; it
posts through the session it is handed, which carries the source's auth and
`Retry` adapter.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Iterable, Optional, Sequence, Tuple

import requests
from loguru import logger

# Explicit (connect, read) on every POST: without a read timeout a stalled
# socket never becomes a retryable event and the run hangs to the pod deadline.
GRAPHQL_TIMEOUT = (10, 120)

DEFAULT_SHRINK_STEPS = (25, 10, 5, 1)

DEFAULT_TRANSIENT_ERROR_TYPES = ("INTERNAL", "SERVICE_UNAVAILABLE", "RATE_LIMITED")
DEFAULT_TRANSIENT_MESSAGE_SIGNATURES = (
    "internal server error",
    "something went wrong",
    "service unavailable",
    "timeout",
    "timed out",
)
DEFAULT_TYPE_FIELDS = ("type", "extensions.type", "extensions.code")


class ErrorClass(Enum):
    """How a caller's classifier wants a response handled."""

    TRANSIENT = "transient"
    RATE_LIMITED = "rate_limited"
    FATAL = "fatal"


class GraphQLError(RuntimeError):
    """A GraphQL `errors[]` response that is not retry-worthy.

    Subclasses RuntimeError so existing `except RuntimeError` fail-closed call
    sites keep working unchanged.
    """


class TransientGraphQLError(GraphQLError):
    """Raised when a response's `errors[]` matched a caller-supplied transient
    signature and the transient attempt budget is exhausted.

    "Transient" means "retry-worthy", not strictly "flaky": a deterministic
    query-cost ceiling belongs here too, because its remedy has the same shape
    as a timeout's -- shrink the page and retry the same cursor, which is what
    `graphql_post_adaptive` does.
    """


class GraphQLRateLimited(GraphQLError):
    """Raised when the rate-limit attempt budget is exhausted.

    `wait_seconds` is first and positional so sources can keep raising it from
    non-GraphQL code paths that only have a wait to report.
    """

    def __init__(self, wait_seconds: float | None = None, message: str = ""):
        self.wait_seconds = wait_seconds
        super().__init__(message or f"Rate limited (last wait {wait_seconds}s)")


Classifier = Callable[[requests.Response, dict], Optional[ErrorClass]]
RateLimitWaitHint = Callable[[requests.Response], Optional[float]]


def _type_field_value(error: dict, field: str) -> str:
    if field.startswith("extensions."):
        extensions = error.get("extensions") or {}
        if not isinstance(extensions, dict):
            return ""
        return str(extensions.get(field.split(".", 1)[1]) or "").upper()
    return str(error.get(field) or "").upper()


def errors_match(
    errors: Iterable[Any],
    *,
    types: Iterable[str] = (),
    message_signatures: Iterable[str] = (),
    type_fields: Sequence[str] = ("type",),
) -> bool:
    """True if any `errors[]` entry carries one of `types` in one of
    `type_fields`, or one of `message_signatures` as a lowercased substring of
    its `message`.

    `type_fields` is explicit rather than "check everywhere" because the two
    APIs put the discriminator in different places -- top-level `type` vs
    `extensions.type`/`extensions.code` -- and widening a source's match set is
    a silent behaviour change, not a cleanup.
    """
    wanted_types = {t.upper() for t in types}
    signatures = tuple(message_signatures)
    for error in errors:
        if not isinstance(error, dict):
            continue
        if wanted_types and any(_type_field_value(error, f) in wanted_types for f in type_fields):
            return True
        message = error.get("message", "")
        message_lower = message.lower() if isinstance(message, str) else ""
        if any(signature in message_lower for signature in signatures):
            return True
    return False


def default_classifier(response: requests.Response, body: dict) -> ErrorClass | None:
    """Generic classifier: the cases every GraphQL API shares.

    Sources with their own error vocabulary pass their own `classify`.
    """
    if response.status_code == 429:
        return ErrorClass.RATE_LIMITED
    if response.status_code >= 500:
        return ErrorClass.TRANSIENT

    errors = body.get("errors") if isinstance(body, dict) else None
    if not errors:
        return None
    if errors_match(errors, types=("RATE_LIMITED", "RATELIMITED"), type_fields=DEFAULT_TYPE_FIELDS):
        return ErrorClass.RATE_LIMITED
    if errors_match(
        errors,
        types=DEFAULT_TRANSIENT_ERROR_TYPES,
        message_signatures=DEFAULT_TRANSIENT_MESSAGE_SIGNATURES,
        type_fields=DEFAULT_TYPE_FIELDS,
    ):
        return ErrorClass.TRANSIENT
    return ErrorClass.FATAL


def _parse_body(response: requests.Response) -> dict | None:
    """None means the body isn't JSON at all -- distinct from valid JSON that
    isn't a dict, which still folds to {} as before."""
    try:
        body = response.json()
    except ValueError:
        return None
    return body if isinstance(body, dict) else {}


def _error_message(body: dict) -> str:
    errors = body.get("errors") or []
    if not errors:
        return "GraphQL errors: <no errors[] in body>"
    joined = "; ".join(e.get("message", str(e)) if isinstance(e, dict) else str(e) for e in errors)
    return f"GraphQL errors: {joined}"


def graphql_post(
    session,
    url: str,
    query: str,
    variables: dict | None = None,
    *,
    classify: Classifier = default_classifier,
    timeout=GRAPHQL_TIMEOUT,
    transient_attempts: int = 3,
    transient_wait: Tuple[float, float] = (15, 60),
    rate_limit_attempts: int = 6,
    rate_limit_wait: Tuple[float, float] = (60, 900),
    rate_limit_wait_hint: RateLimitWaitHint | None = None,
    retry_exceptions: Tuple[type, ...] = (),
    headers: dict | None = None,
    auth=None,
    auth_factory: Callable[[], Any] | None = None,
    log_prefix: str = "",
) -> dict:
    """POST a GraphQL query with bounded retries; return the parsed JSON body.

    `classify` is called for every response with `(response, parsed_body)` and
    returns None for a success, else the class to act on.

    Both `*_attempts` values are TOTAL attempts, and both `*_wait` values are
    `(initial, max)` seconds doubling per retry -- so the defaults sleep
    15, 30 across 3 transient attempts and 60, 120, 240, 480, 900 across 6
    rate-limited ones. The rate-limit budget resets on each transient attempt
    (the two failure modes must not share a counter).

    `retry_exceptions` (typically the source's network-error tuple, including
    the adapter's `RetryError` -- exhausting the adapter raises that, not
    `HTTPError`) are retried on the rate-limit ladder and share its budget.

    `auth_factory` mints the request's `auth` per attempt, INSIDE the retried
    region, for sources whose credential is a short-lived token fetched over
    the network -- minting it outside would leave that fetch unprotected by the
    ladder. It takes precedence over `auth`.

    Raises `TransientGraphQLError` / `GraphQLRateLimited` once the matching
    budget is spent, `GraphQLError` immediately on FATAL, and lets a non-OK
    response the classifier did not claim surface as `HTTPError`. An OK
    response with a non-JSON body is treated as transient too, not returned
    as a silent `{}` success.
    """
    payload: dict = {"query": query}
    if variables:
        payload["variables"] = variables
    prefix = f"{log_prefix} " if log_prefix else ""

    transient_sleep = transient_wait[0]
    transient_left = transient_attempts

    while True:
        rate_limit_sleep = rate_limit_wait[0]
        rate_limit_left = rate_limit_attempts
        while True:
            try:
                request_auth = auth_factory() if auth_factory else auth
                response = session.post(url, json=payload, timeout=timeout, headers=headers, auth=request_auth)
            except retry_exceptions as exc:
                attempt = rate_limit_attempts - rate_limit_left + 1
                rate_limit_left -= 1
                if rate_limit_left <= 0:
                    raise
                logger.warning(
                    f"{prefix}GraphQL request failed ({type(exc).__name__}: {exc}), "
                    f"sleeping {rate_limit_sleep:.0f}s and retrying "
                    f"(attempt {attempt}/{rate_limit_attempts})"
                )
                time.sleep(rate_limit_sleep)
                rate_limit_sleep = min(rate_limit_sleep * 2, rate_limit_wait[1])
                continue

            parsed = _parse_body(response)
            parse_failed = parsed is None
            body = {} if parse_failed else parsed
            if parse_failed and response.ok:
                # Non-JSON body must not read as a falsy {} success.
                error_class = ErrorClass.TRANSIENT
            else:
                error_class = classify(response, body)
            if error_class is not ErrorClass.RATE_LIMITED:
                break

            attempt = rate_limit_attempts - rate_limit_left + 1
            rate_limit_left -= 1
            if rate_limit_left <= 0:
                logger.error(f"{prefix}Rate limit not cleared after {rate_limit_attempts} attempts")
                raise GraphQLRateLimited(rate_limit_sleep, f"{prefix}{_error_message(body)}".strip())

            hint = rate_limit_wait_hint(response) if rate_limit_wait_hint else None
            if hint is not None:
                # Small pad so we don't race the exact reset instant, capped at
                # the same ceiling as the blind ladder so a reset far in the
                # future can't stall the stream on a single event.
                sleep_for = min(hint + 1 + random.random(), rate_limit_wait[1])
                logger.warning(
                    f"{prefix}Rate limited, sleeping {sleep_for:.1f}s per reset hint "
                    f"(attempt {attempt}/{rate_limit_attempts})"
                )
            else:
                sleep_for = rate_limit_sleep
                logger.warning(
                    f"{prefix}Rate limited, sleeping {sleep_for:.0f}s -- no reset hint, "
                    f"using fallback backoff (attempt {attempt}/{rate_limit_attempts})"
                )
            time.sleep(sleep_for)
            rate_limit_sleep = min(rate_limit_sleep * 2, rate_limit_wait[1])

        if error_class is None:
            if not response.ok:
                logger.error(f"{prefix}GraphQL HTTP {response.status_code}: {response.text[:1000]}")
                response.raise_for_status()
            return body

        if parse_failed:
            message = f"Non-JSON response body (HTTP {response.status_code}): {response.text[:200]}"
        else:
            message = _error_message(body)
        if error_class is ErrorClass.FATAL:
            raise GraphQLError(message)

        attempt = transient_attempts - transient_left + 1
        transient_left -= 1
        if transient_left <= 0:
            logger.error(f"{prefix}Transient GraphQL error not cleared after {transient_attempts} attempts: {message}")
            raise TransientGraphQLError(message)
        logger.warning(
            f"{prefix}Transient GraphQL error, sleeping {transient_sleep:.0f}s and "
            f"retrying: {message} (attempt {attempt}/{transient_attempts})"
        )
        time.sleep(transient_sleep)
        transient_sleep = min(transient_sleep * 2, transient_wait[1])


def build_page_shrink_sequence(start_size: int, floor: int = 1, *, steps: Sequence[int] = DEFAULT_SHRINK_STEPS) -> list:
    """Strictly-decreasing page sizes from `start_size` down to `floor`, inclusive.

    `steps` intersected with [floor, start_size], plus the two endpoints -- so a
    full-size page walks the canonical ladder while a start_size already below
    the first step shrinks directly instead of inventing an intermediate size.
    `steps` is per-source: the useful ladder depends on what the API charges for
    a page.
    """
    sizes = {size for size in steps if floor <= size <= start_size}
    sizes.add(start_size)
    sizes.add(floor)
    return sorted(sizes, reverse=True)


class MutableCounter:
    """A shared integer, so a per-run ceiling survives across calls and threads."""

    def __init__(self, value: int = 0):
        self.value = value
        self._lock = threading.Lock()

    def increment(self) -> int:
        with self._lock:
            self.value += 1
            return self.value


@dataclass
class PhantomProbe:
    """A node-free sibling of the real query, used to tell a broken node
    resolver apart from a broken connection.

    Some GraphQL connections contain contiguous index entries whose node data
    the server can never load: page size is irrelevant, but a `pageInfo`-only
    probe of the exact same cursor succeeds. That asymmetry is the
    discriminator -- if the probe fails too it is a real outage and the caller
    must still fail closed.

    `query` must select only `pageInfo { hasNextPage endCursor }` off the same
    connection, taking the same variables. `page_info` digs that block out of a
    probe response body (the path is API-specific). `describe_cursor` is an
    optional log-only decoder that turns an opaque cursor into something
    human-readable.
    """

    query: str
    cursor_var: str
    max_skips_per_page: int
    max_skips_per_run: int
    run_counter: MutableCounter
    page_info: Callable[[dict], dict]
    describe_cursor: Callable[[str], str | None] | None = None


def graphql_post_adaptive(
    session,
    url: str,
    query: str,
    variables: dict,
    *,
    page_size_var: str,
    start_size: int,
    floor: int = 1,
    max_size: int | None = None,
    shrink_steps: Sequence[int] = DEFAULT_SHRINK_STEPS,
    cursor_var: str = "after",
    probe: PhantomProbe | None = None,
    label: str = "",
    post: Callable[[str, dict], dict] | None = None,
    **graphql_post_kwargs,
) -> Tuple[dict, int]:
    """POST, shrinking `variables[page_size_var]` on a persistent
    TransientGraphQLError instead of retrying the same page size.

    Relay cursors are page-size-independent, so stepping the page size down and
    retrying the SAME cursor is correct and lets pagination continue once a size
    succeeds. Returns `(body, size_that_worked)`; callers carry that size
    forward, since a page that shrank once usually needs to again.

    Once the ladder is exhausted at `floor`, re-raises the last transient error
    (fail closed, watermark does not advance) unless a `probe` is supplied and
    recovers -- see `PhantomProbe`.

    `post` overrides the default `graphql_post(session, url, ...)` call, for
    sources whose transport needs per-request auth or token refresh; `session`
    and `url` are then unused.
    """
    if max_size is None:
        max_size = start_size
    if post is not None and graphql_post_kwargs:
        raise TypeError("post= and graphql_post kwargs are mutually exclusive")
    if post is None:

        def post(q: str, v: dict) -> dict:
            return graphql_post(session, url, q, v, **graphql_post_kwargs)

    if probe is not None:
        cursor_var = probe.cursor_var

    sizes = build_page_shrink_sequence(start_size, floor, steps=shrink_steps)
    last_error: TransientGraphQLError | None = None

    for i, size in enumerate(sizes):
        if i > 0:
            logger.warning(
                f"GraphQL page still failing for {label or 'page'} "
                f"({cursor_var}={variables.get(cursor_var)}) -- shrinking page size "
                f"{sizes[i - 1]} -> {size} and retrying same cursor"
            )
        variables[page_size_var] = size
        try:
            return post(query, variables), size
        except TransientGraphQLError as exc:
            last_error = exc
            if size == floor:
                logger.error(
                    f"GraphQL page fetch for {label or 'page'} failed even at the "
                    f"minimum page size ({page_size_var}={floor}) -- cursor "
                    f"{cursor_var}={variables.get(cursor_var)}. Giving up on this window; "
                    f"it will fail-closed and be retried next run."
                )
            continue

    if probe is None or probe.max_skips_per_page == 0:
        raise last_error

    starting_cursor = variables.get(cursor_var)
    skipped_this_page = 0
    while skipped_this_page < probe.max_skips_per_page:
        try:
            probe_body = post(probe.query, {**variables, page_size_var: 1})
        except TransientGraphQLError:
            raise last_error

        page_info = probe.page_info(probe_body)
        end_cursor = page_info.get("endCursor")
        has_next_page = page_info.get("hasNextPage")
        if not end_cursor or not has_next_page or end_cursor == variables.get(cursor_var):
            # End of connection, or the probe echoed the same cursor back: no
            # progress is possible, so this can't be an entry we step over.
            raise last_error

        described = probe.describe_cursor(end_cursor) if probe.describe_cursor else None
        logger.warning(
            f"GraphQL node resolution broken for {label or 'page'} at cursor "
            f"{cursor_var}={variables.get(cursor_var)} but a pageInfo-only probe of the "
            f"same cursor succeeded -- stepping past an unresolvable index entry "
            f"(endCursor={end_cursor}"
            + (f", {described}" if described else "")
            + "). Logging the raw cursor so this position stays recoverable if it ever "
            "turns out to be real data."
        )
        variables[cursor_var] = end_cursor
        skipped_this_run = probe.run_counter.increment()
        skipped_this_page += 1

        if skipped_this_run > probe.max_skips_per_run:
            raise RuntimeError(
                f"GraphQL index entries skipped this run ({skipped_this_run}) exceeds the "
                f"per-run ceiling of {probe.max_skips_per_run} (most recently for "
                f"{label or 'page'}). This looks like systemic breakage rather than a "
                "handful of isolated unresolvable entries -- aborting rather than silently "
                "reporting a large gap as success. If this fires mid-run (after some rows "
                "already landed), the remedy is a stream reset, not just a re-run."
            )

        variables[page_size_var] = floor
        try:
            result = post(query, variables)
        except TransientGraphQLError as exc:
            # Still broken past this cursor -- likely another contiguous entry.
            last_error = exc
            continue

        logger.warning(
            f"Recovered from unresolvable GraphQL index entries for {label or 'page'}: "
            f"stepped over {skipped_this_page} "
            f"entr{'y' if skipped_this_page == 1 else 'ies'} starting at cursor "
            f"{cursor_var}={starting_cursor}, resuming pagination at page size {max_size}."
        )
        # max_size, NOT start_size: by the time pagination reaches an
        # unresolvable run the ladder has already walked page_size down to the
        # floor, so start_size here IS floor -- returning it leaves the rest of
        # the connection fetched one row at a time. Restoring the configured
        # size is safe because the next window no longer contains the entries
        # just stepped over, and the ladder will kick in again if it does.
        return result, max_size

    raise last_error
