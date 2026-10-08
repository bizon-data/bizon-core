from typing import Optional, Tuple, Union

import requests
from loguru import logger
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import HTTPError
from requests.models import Response


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
        self.timeout = timeout

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
        return super().request(method, url, *args, **kwargs)

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
            logger.error(f"detailed error response: {e.response.json()}")
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
