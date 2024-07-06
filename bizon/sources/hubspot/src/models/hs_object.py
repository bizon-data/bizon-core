from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class HubSpotProperty(BaseModel):
    name: str
    label: str
    field_type: str
    type: str
    description: Optional[str]


class HubSpotObjectProperty(BaseModel):
    value: Any


class AllObjectProperties(BaseModel):
    properties: List[HubSpotProperty]

    @property
    def names(self) -> List[str]:
        return [hs_property.name for hs_property in self.properties]

    def property_names(
        self,
    ) -> List[str]:
        return [hs_property.name for hs_property in self.properties]


class HubSpotObject(BaseModel):
    id: int
    properties: Dict[str, HubSpotObjectProperty]
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_raw_obj_response(
        cls,
        raw_obj: dict,
    ):

        properties = {}

        for property_name, property_value in raw_obj.get("properties", {}).items():
            properties[property_name] = HubSpotObjectProperty(value=property_value)

        return cls(
            id=raw_obj["id"],
            properties=properties,
            created_at=raw_obj["createdAt"],
            updated_at=raw_obj["updatedAt"],
        )
