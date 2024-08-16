import requests
from loguru import logger
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import HTTPError


class Session(requests.Session):
    def __init__(self):
        super().__init__()

        # call response.raise_for_status() after every response
        self.hooks["response"] = [lambda response, *args, **kwargs: response.raise_for_status()]

        # Implement basic rety policy if rate-limited
        retries = Retry(
            total=20,
            backoff_factor=1,
            raise_on_status=True,
            status=30,
            allowed_methods=["GET", "POST"],
        )

        self.mount(
            "https://",
            HTTPAdapter(
                max_retries=retries,
                pool_maxsize=64,
            ),
        )
        self._method_mapping = {
            "POST": self.post,
            "GET": self.get,
        }

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
