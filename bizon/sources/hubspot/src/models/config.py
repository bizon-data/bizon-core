from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field

from bizon.source.config import SourceConfig


class HubSpotObjects(str, Enum):
    CONTACTS = "contacts"
    COMPANIES = "companies"
    DEALS = "deals"


class PropertiesStrategy(str, Enum):
    ALL = "all"
    SELECTED = "selected"


class PropertiesConfig(BaseModel):
    strategy: PropertiesStrategy = Field(PropertiesStrategy.ALL, description="Properties strategy")
    selected_properties: Optional[List[str]] = Field([], description="List of selected properties")


class HubSpotSourceConfig(SourceConfig):
    name: str = "hubspot"
    stream_name: HubSpotObjects
    properties: PropertiesConfig = PropertiesConfig(strategy=PropertiesStrategy.ALL, selected_properties=None)
