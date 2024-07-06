from typing import Dict, Optional, Any, List
from pydantic import BaseModel
from datetime import datetime


class HSProperty(BaseModel):
    name: str
    label: str
    field_type: str
    type: str
    description: Optional[str]


class HSObjectProperty(BaseModel):
    value: Any


class AllObjectProperties(BaseModel):
    properties: List[HSProperty]

    @property
    def names(self) -> List[str]:
        return [hs_property.name for hs_property in self.properties]


class HSObject(BaseModel):
    id: int
    properties: Dict[str, HSObjectProperty]
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_raw_obj_response(
        cls,
        raw_obj: dict,
    ):

        properties = {}

        for property_name, property_value in raw_obj.get("properties", {}).items():
            properties[property_name] = HSObjectProperty(value=property_value)

        return cls(
            id=raw_obj["id"],
            properties=properties,
            created_at=raw_obj["createdAt"],
            updated_at=raw_obj["updatedAt"],
        )