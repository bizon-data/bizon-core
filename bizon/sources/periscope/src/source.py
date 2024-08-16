import itertools
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any, List, Tuple

from loguru import logger
from requests.auth import AuthBase

from bizon.source.auth.authenticators.cookies import CookiesAuthParams
from bizon.source.auth.builder import AuthBuilder
from bizon.source.auth.config import AuthType
from bizon.source.source import Source

from .config import PeriscopeSourceConfig, PeriscopeStreams

BASE_URL = "https://app.periscopedata.com"

URL_OWNERS = f"{BASE_URL}/users/owners"
URL_VIEWS = f"{BASE_URL}/login_state/sql_views"
URL_DATABASES = f"{BASE_URL}/welcome/remaining_state/site_models"


class PeriscopeSource(Source):

    def __init__(self, config: PeriscopeSourceConfig):
        super().__init__(config)
        self.config: PeriscopeSourceConfig = config

    @property
    def cookies(self) -> dict:
        return {
            "__cf_bm": self.config.authentication.params.cookies.cf_bm,
            "periscope_session": self.config.authentication.params.cookies.periscope_session,
        }

    @property
    def http_params(self) -> dict:
        return {
            "client_site_id": self.config.client_site_id,
        }

    def check_connection(self) -> Tuple[bool | Any | None]:
        return True, None

    def get_authenticator(self) -> AuthBase:
        if self.config.authentication.type == AuthType.COOKIES:
            return AuthBuilder.cookies(
                params=CookiesAuthParams(
                    cookies=self.cookies,
                    headers={
                        "authority": "app.periscopedata.com",
                        "accept": "application/json, text/javascript, */*; q=0.01",
                        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
                        "sec-ch-ua": '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
                        "sec-ch-ua-mobile": "?0",
                        "sec-ch-ua-platform": '"macOS"',
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors",
                        "sec-fetch-site": "same-origin",
                        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
                        "x-requested-with": "XMLHttpRequest",
                    },
                )
            )

        raise NotImplementedError(f"Auth type {self.config.authentication.type} not implemented for Periscope")

    def get_total_records_count(self) -> int | None:
        return None

    def get_dashboards(self, pagination: dict = None) -> Tuple[dict, List[dict]]:
        response = self.session.call(method="GET", url=f"{BASE_URL}/login_state/dashboards", params=self.http_params)
        return dict(), response.json()["Dashboard"]

    def get_dashboard_ids(self) -> List[int]:
        _, records = self.get_dashboards()
        return [record["id"] for record in records]

    def _get_charts(self, dashboard_id: int) -> List[dict]:
        MAXIMUM_ITERATION: int = 1000
        iter_count: int = 0
        window_start: int = 0
        iter_charts: List[dict] = []
        charts_list: set = set()

        dashboard_charts: List[dict] = []

        for iter_count in range(MAXIMUM_ITERATION):

            # Break the loop if no more charts are available
            if iter_count > 0 and len(iter_charts) == 0:
                break

            # Prepare params for iteration
            params = {
                "current_dashboard": str(dashboard_id),
                "minimum_dashboard_fluid_row": window_start,
                "fluid_column_for_last_widget_in_row": 0,
            }
            params.update(self.http_params)

            try:
                response = self.session.call(
                    method="GET",
                    url=f"{BASE_URL}/welcome/remaining_widgets",
                    params=params,
                )

                if not response.ok:
                    print(f"Failed to fetch the dashboard with id: {dashboard_id}")
                    continue

                window_start += 50
                iter_count += 1
                iter_charts = response.json().get("Widget")

                for chart in iter_charts:
                    # Only fetch charts connected to my-gcp-project-id
                    if str(chart.get("database_id")) == str(self.config.database_id):
                        if chart.get("id") not in charts_list:
                            charts_list.add(chart.get("id"))
                            dashboard_charts.append(chart)
            except Exception:
                continue

        return dashboard_charts

    def get_charts(self, pagination: dict = None) -> Tuple[dict, List[dict]]:
        MAX_CONCURRENT_REQUESTS = 10

        if not pagination:
            dashboard_ids = self.get_dashboard_ids()

            if not dashboard_ids:
                logger.warning("No dashboard found")
                return dict(), []

            pagination = {"dashboard_ids": dashboard_ids}

        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
            result = list(executor.map(self._get_charts, pagination["dashboard_ids"][:10]))

        # Remove the first 10 dashboard ids if there are more
        if len(pagination["dashboard_ids"]) > 10:
            pagination["dashboard_ids"] = pagination["dashboard_ids"][10:]
        else:
            pagination["dashboard_ids"] = []

        records = list(itertools.chain.from_iterable(result))
        return pagination, records

    def get(self, pagination: dict = None) -> Tuple[dict | List[dict]]:
        if self.config.stream_name == PeriscopeStreams.DASHBOARDS:
            return self.get_dashboards(pagination)

        if self.config.stream_name == PeriscopeStreams.CHARTS:
            return self.get_charts(pagination)

        raise NotImplementedError(f"Stream {self.config.stream_name} not implemented for Periscope")
