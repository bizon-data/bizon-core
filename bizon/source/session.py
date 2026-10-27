import time
from typing import Optional, Tuple, Union

import requests
from loguru import logger
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import ChunkedEncodingError, HTTPError
from requests.models import Response
from urllib3.exceptions import ProtocolError

# The read timeout bounds an idle socket, not the whole request: it exists so a stalled connection fails
# and becomes retryable instead of hanging until the pod deadline.
DEFAULT_TIMEOUT = (10, 300)
TRUNCATED_BODY_RETRIES = 3
# For sessions whose adapter carries no retry policy, which is what a plain requests.Session mounts.
TRUNCATED_BODY_METHODS = frozenset({"GET", "POST"})


# Define a named function instead of using a lambda to be able to pickle it
def raise_for_status_hook(response: Response, *args, **kwargs):
    response.raise_for_status()


class CappedRetry(Retry):
    """A Retry that bounds how long a Retry-After header can make it sleep.

    urllib3 only gained `retry_after_max` in 2.7, and some APIs answer a spent quota with a Retry-After of
    days, which would sleep the run into its deadline instead of failing it.
    """

    def __init__(self, *args, max_retry_after: Optional[float] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_retry_after = max_retry_after

    # urllib3 rebuilds the Retry after every attempt from the parameters it knows about.
    def new(self, **kwargs) -> "CappedRetry":
        retry = super().new(**kwargs)
        retry.max_retry_after = self.max_retry_after
        return retry

    def get_retry_after(self, response) -> Optional[float]:
        seconds = super().get_retry_after(response)
        if seconds is not None and self.max_retry_after is not None:
            return min(seconds, self.max_retry_after)
        return seconds


def legacy_retry_policy() -> Retry:
    # Without a status_forcelist urllib3 only retries 413/429/503, and only when they carry a Retry-After.
    return Retry(
        total=20,
        backoff_factor=1,
        raise_on_status=True,
        status=30,
        allowed_methods=["GET", "POST"],
    )


def _truncated_body_policy(session: requests.Session, url) -> Tuple[Optional[frozenset], float]:
    retry = session.get_adapter(url).max_retries
    if not isinstance(retry, Retry) or retry.total == 0:
        return TRUNCATED_BODY_METHODS, 1
    return retry.allowed_methods, retry.backoff_factor


def request_retrying_truncated_body(session: requests.Session, send, method, url, *args, **kwargs):
    """Call `send`, retrying a response whose body was cut off mid-read.

    urllib3's Retry never sees the body: a connection dropped mid-body only surfaces when requests reads
    .content, after every retry layer has already returned.
    """
    attempt = 0
    while True:
        try:
            return send(method, url, *args, **kwargs)
        except (ChunkedEncodingError, ProtocolError) as e:
            if kwargs.get("stream") or attempt >= TRUNCATED_BODY_RETRIES:
                raise
            allowed_methods, backoff_factor = _truncated_body_policy(session, url)
            if allowed_methods is not None and method.upper() not in allowed_methods:
                raise
            delay = min(backoff_factor * (2**attempt), 120)
            attempt += 1
            logger.warning(f"Truncated response body for {method} {url}, retry {attempt} in {delay}s: {e}")
            time.sleep(delay)


def harden_session(session):
    """Give a session that is not bizon's own the default timeout and the truncated-body retry.

    Only `request` is wrapped: the adapters, retry policy and hooks a source chose are left as they are.
    """
    if not isinstance(session, requests.Session) or isinstance(session, Session):
        return session
    if getattr(session, "_bizon_hardened", False):
        return session

    send = session.request

    def request(method, url, *args, **kwargs):
        kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
        return request_retrying_truncated_body(session, send, method, url, *args, **kwargs)

    session.request = request
    session._bizon_hardened = True
    return session


class Session(requests.Session):
    # requests.Session pickles only the attributes listed here.
    __attrs__ = requests.Session.__attrs__ + ["timeout"]

    def __init__(
        self,
        retries: Optional[Retry] = None,
        timeout: Optional[Union[float, Tuple[float, float]]] = None,
        raise_for_status: bool = True,
    ):
        super().__init__()
        self.timeout = timeout if timeout is not None else DEFAULT_TIMEOUT

        if raise_for_status:
            self.hooks["response"] = [raise_for_status_hook]

        self.mount(
            "https://",
            HTTPAdapter(
                max_retries=retries or legacy_retry_policy(),
                pool_maxsize=64,
            ),
        )
        self._method_mapping = {
            "POST": self.post,
            "GET": self.get,
        }

    def request(self, method, url, *args, **kwargs) -> requests.Response:
        if self.timeout is not None:
            kwargs.setdefault("timeout", self.timeout)

        return request_retrying_truncated_body(self, super().request, method, url, *args, **kwargs)

    def call(
        self,
        method: str,
        url: str,
        content_type: str = "application/json",
        *args,
        **kwargs,
    ) -> requests.Response:
        self.headers.update({"content-type": content_type})

        try:
            response = self._method_mapping[method](url=url, *args, **kwargs)
        except HTTPError as e:
            logger.error(f"Error {e}")
            try:
                detail = e.response.json()
            except ValueError:
                detail = e.response.text[:500]
            logger.error(f"detailed error response: {detail}")
            logger.error(f"for request body: {e.request.body}")
            raise e
        except Exception as e:
            logger.error(f"Error {e}")
            logger.error(
                f"""for request '{method}' on url='{url}'
                              with args={args}
                              and kwargs={kwargs}
                          """
            )
            raise e
        else:
            return response
