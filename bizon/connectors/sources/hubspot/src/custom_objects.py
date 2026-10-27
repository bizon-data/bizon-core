from typing import List

from pydantic import Field

from bizon.source.config import SourceConfig

from .hubspot_objects import HubSpotObjectsSource, HubSpotSourceConfig


class HubSpotCustomObjectSourceConfig(HubSpotSourceConfig):
    object_type: str = Field(
        ...,
        description="Custom object to sync: its objectTypeId (e.g. 2-1234567) or its fully qualified name",
    )


class HubSpotCustomObjectSource(HubSpotObjectsSource):
    """Any CRM object, custom ones included, picked by `object_type`. Same list and search paths as the
    contacts/companies/deals streams."""

    def __init__(self, config: HubSpotCustomObjectSourceConfig):
        super().__init__(config)
        self.config: HubSpotCustomObjectSourceConfig = config

    @staticmethod
    def streams() -> List[str]:
        return ["custom_object"]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return HubSpotCustomObjectSourceConfig

    def get_object_type(self) -> str:
        return self.config.object_type
