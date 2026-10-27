"""Shared GraphQL transport: bounded-retry POST, a same-cursor page-size shrink ladder and an optional
phantom-entry cursor probe.

API-agnostic: error classification, `pageInfo` location, rate-limit hints and auth are injected by the caller, and
requests go through the caller's session.
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

# Without a read timeout a stalled socket never becomes retryable and hangs to the pod deadline.
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
    """A GraphQL `errors[]` response that is not retry-worthy."""


class TransientGraphQLError(GraphQLError):
    """Transient `errors[]` persisted past the attempt budget.

    "Transient" means retry-worthy: a query-cost ceiling belongs here too, since shrinking the page fixes it.
    """


class GraphQLRateLimited(GraphQLError):
    """Rate limit persisted past the attempt budget; also raised from non-GraphQL paths with only a wait."""

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
    """True if any `errors[]` entry has one of `types` in a `type_fields` field, or a `message_signatures`
    substring in its lowercased message.

    `type_fields` is explicit because APIs put the discriminator in different places (`type` vs `extensions.*`).
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
    """The cases every GraphQL API shares; sources with their own error vocabulary pass their own `classify`."""
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
    """None for a non-JSON body; valid JSON that isn't a dict folds to {}."""
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

    `classify(response, body)` returns None for a success, else the class to act on. `*_attempts` are total
    attempts and `*_wait` are `(initial, max)` seconds doubling per retry; the rate-limit budget resets on each
    transient attempt. `retry_exceptions` (include the adapter's `RetryError`) share the rate-limit budget.
    `auth_factory` mints `auth` per attempt, inside the retried region, so a short-lived token fetch is retried too.

    Raises `TransientGraphQLError` / `GraphQLRateLimited` once a budget is spent and `GraphQLError` on FATAL; a
    non-OK response the classifier did not claim surfaces as `HTTPError`.
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
                # Padded past the reset instant, and capped so a far-future reset can't stall the stream.
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
    """Strictly-decreasing page sizes: `steps` within [floor, start_size], plus both endpoints."""
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
    """A node-free sibling of the real query, telling a broken node resolver apart from a broken connection.

    Some connections hold index entries whose nodes never load at any page size, while a `pageInfo`-only probe
    of the same cursor succeeds; if the probe fails too, it is a real outage. `query` must select only
    `pageInfo { hasNextPage endCursor }` off the same connection with the same variables; `page_info` extracts
    that block from the response, and `describe_cursor` optionally decodes a cursor for logs.
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
    """POST, shrinking `variables[page_size_var]` and retrying the same cursor on a persistent
    TransientGraphQLError (Relay cursors are page-size-independent).

    Returns `(body, size_that_worked)` for the caller to carry forward. At `floor` it re-raises unless `probe`
    recovers. `post` replaces the default `graphql_post(session, url, ...)` call, leaving `session`/`url` unused.
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
            # End of connection, or the probe echoed the cursor back: nothing to step over.
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
            # Likely another contiguous broken entry.
            last_error = exc
            continue

        logger.warning(
            f"Recovered from unresolvable GraphQL index entries for {label or 'page'}: "
            f"stepped over {skipped_this_page} "
            f"entr{'y' if skipped_this_page == 1 else 'ies'} starting at cursor "
            f"{cursor_var}={starting_cursor}, resuming pagination at page size {max_size}."
        )
        # max_size, not start_size: start_size is floor by now, and the rest of the connection would be fetched
        # one row at a time.
        return result, max_size

    raise last_error
