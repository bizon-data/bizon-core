from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.auth.builder import AuthBuilder
from bizon.source.auth.config import AuthType
from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

REQUEST_TIMEOUT = (10, 60)


class RootlySourceConfig(SourceConfig):
    http: Optional[HttpConfig] = Field(default_factory=HttpConfig, description="HTTP timeout/retry policy")


class RootlySource(AbstractSource):
    def __init__(self, config: RootlySourceConfig):
        super().__init__(config)
        self.base_url = "https://api.rootly.com/v1"

    def get_authenticator(self) -> AuthBase:
        if self.config.authentication.type.value == AuthType.API_KEY:
            return AuthBuilder.token(params=self.config.authentication.params)

        raise NotImplementedError(
            f"Authentication type {self.config.authentication.type.value} not implemented for Rootly"
        )

    @staticmethod
    def streams() -> List[str]:
        return [
            "post_mortems",
            "incidents",
            "teams",
            "services",
            "users",
            "incident_action_items",
        ]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return RootlySourceConfig

    def check_connection(self) -> Tuple[bool | Any | None]:
        # Always probes /services, whichever stream runs: a /services-only failure fails every stream.
        try:
            self.get_services(pagination={})
        except Exception as e:
            return False, str(e)
        return True, None

    def get_total_records_count(self) -> int | None:
        return None

    def get_post_mortems(self, pagination: dict) -> SourceIteration:
        """Return all post mortems"""

        if not pagination:
            response = self.session.get(
                f"{self.base_url}/post_mortems", params={"page[size]": 100}, timeout=REQUEST_TIMEOUT
            )
        else:
            if pagination.get("next_page"):
                response = self.session.get(
                    f"{self.base_url}/post_mortems",
                    params={"page[number]": pagination.get("next_page"), "page[size]": 100},
                    timeout=REQUEST_TIMEOUT,
                )
            else:
                return SourceIteration(
                    next_pagination={},
                    records=[],
                )

        records_json = response.json()

        for i, record in enumerate(records_json["data"]):
            logger.info(
                f"Retrieving {len(record['relationships']['incident_retrospective_steps']['data'])} incident retrospective steps for post mortem {record['id']}"
            )

            for j, incident_retrospective_step in enumerate(
                record["relationships"]["incident_retrospective_steps"]["data"]
            ):
                incident_retrospective_step_id = incident_retrospective_step["id"]
                incident_retrospective_step_data = self.session.get(
                    f"{self.base_url}/incident_retrospective_steps/{incident_retrospective_step_id}",
                    timeout=REQUEST_TIMEOUT,
                ).json()
                records_json["data"][i]["relationships"]["incident_retrospective_steps"]["data"][j]["data"] = (
                    incident_retrospective_step_data["data"]
                )

        return SourceIteration(
            next_pagination=records_json["meta"],
            records=[
                SourceRecord(
                    id=record["id"],
                    data=record,
                )
                for record in records_json["data"]
            ],
        )

    def get_incidents(self, pagination: dict, page_size: int = 200) -> SourceIteration:
        """Return all incidents"""

        include = "sub_statuses,causes,subscribers,services,groups,action_items,incident_post_mortem,feedbacks"

        if not pagination:
            response = self.session.get(
                f"{self.base_url}/incidents",
                params={"page[size]": page_size, "include": include},
                timeout=REQUEST_TIMEOUT,
            )
        else:
            if pagination.get("next_page"):
                response = self.session.get(
                    f"{self.base_url}/incidents",
                    params={"page[number]": pagination.get("next_page"), "page[size]": page_size, "include": include},
                    timeout=REQUEST_TIMEOUT,
                )
            else:
                return SourceIteration(
                    next_pagination={},
                    records=[],
                )

        records_json = response.json()

        return SourceIteration(
            next_pagination=records_json["meta"],
            records=[SourceRecord(id=record["id"], data=record) for record in records_json["data"]],
        )

    def get_teams(self, pagination: dict) -> SourceIteration:
        """Return all teams"""
        if not pagination:
            response = self.session.get(f"{self.base_url}/teams", params={"page[size]": 100}, timeout=REQUEST_TIMEOUT)
        else:
            if pagination.get("next_page"):
                response = self.session.get(
                    f"{self.base_url}/teams",
                    params={"page[number]": pagination.get("next_page"), "page[size]": 100},
                    timeout=REQUEST_TIMEOUT,
                )
            else:
                return SourceIteration(
                    next_pagination={},
                    records=[],
                )
        records_json = response.json()
        return SourceIteration(
            next_pagination=records_json["meta"],
            records=[SourceRecord(id=record["id"], data=record) for record in records_json["data"]],
        )

    def get_services(self, pagination: dict) -> SourceIteration:
        """Return all services"""
        if not pagination:
            response = self.session.get(
                f"{self.base_url}/services", params={"page[size]": 100}, timeout=REQUEST_TIMEOUT
            )
        else:
            if pagination.get("next_page"):
                response = self.session.get(
                    f"{self.base_url}/services",
                    params={"page[number]": pagination.get("next_page"), "page[size]": 100},
                    timeout=REQUEST_TIMEOUT,
                )
            else:
                return SourceIteration(
                    next_pagination={},
                    records=[],
                )
        records_json = response.json()
        return SourceIteration(
            next_pagination=records_json["meta"],
            records=[SourceRecord(id=record["id"], data=record) for record in records_json["data"]],
        )

    def get_users(self, pagination: dict) -> SourceIteration:
        """Return all users"""
        if not pagination:
            response = self.session.get(f"{self.base_url}/users", params={"page[size]": 100}, timeout=REQUEST_TIMEOUT)
        else:
            if pagination.get("next_page"):
                response = self.session.get(
                    f"{self.base_url}/users",
                    params={"page[number]": pagination.get("next_page"), "page[size]": 100},
                    timeout=REQUEST_TIMEOUT,
                )
            else:
                return SourceIteration(
                    next_pagination={},
                    records=[],
                )
        records_json = response.json()
        return SourceIteration(
            next_pagination=records_json["meta"],
            records=[SourceRecord(id=record["id"], data=record) for record in records_json["data"]],
        )

    def get_incident_action_items(self, pagination: dict) -> SourceIteration:
        """Return all incident action items"""
        incident_iteration = self.get_incidents(pagination=pagination, page_size=1)

        if len(incident_iteration.records) == 0:
            return SourceIteration(
                next_pagination={},
                records=[],
            )

        assert len(incident_iteration.records) < 2, "Only one incident should be returned"

        incident = incident_iteration.records[0]

        records = []

        iterate_action_items = True

        params = {
            "page[size]": 100,
        }

        logger.info(f"Iterating over action items for incident {incident.data['id']}")

        while iterate_action_items:
            response = self.session.get(
                f"{self.base_url}/incidents/{incident.data['id']}/action_items",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            records_json = response.json()
            records.extend(records_json["data"])

            if records_json["meta"].get("next_page", None) is None:
                iterate_action_items = False
            else:
                params["page[number]"] = records_json["meta"].get("next_page")

        logger.info(f"Found {len(records)} action items for incident {incident.data['id']}")

        if not records:
            return self.get_incident_action_items(pagination=incident_iteration.next_pagination)

        return SourceIteration(
            next_pagination=incident_iteration.next_pagination,
            records=[SourceRecord(id=record["id"], data=record) for record in records],
        )

    def get(self, pagination: dict = None) -> SourceIteration:
        if self.config.stream == "post_mortems":
            return self.get_post_mortems(pagination)

        if self.config.stream == "incidents":
            return self.get_incidents(pagination)

        if self.config.stream == "teams":
            return self.get_teams(pagination)

        if self.config.stream == "services":
            return self.get_services(pagination)

        if self.config.stream == "users":
            return self.get_users(pagination)

        if self.config.stream == "incident_action_items":
            return self.get_incident_action_items(pagination)

        raise NotImplementedError(f"Stream {self.config.stream} not implemented for Rootly")
