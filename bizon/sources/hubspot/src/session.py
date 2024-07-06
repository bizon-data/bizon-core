from requests.adapters import HTTPAdapter, Retry

from bizon.api.session import Session


class HubspotSession(Session):
    def __init__(self):
        super().__init__()

        # Override rety policy if rate-limited by HubSpot
        retries = Retry(
            total=50,
            backoff_factor=1,
            raise_on_status=True,
            status_forcelist=[429, 500, 502, 503, 504],
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
