from typing import List, Union

from pydantic import BaseModel, BeforeValidator, Field, field_validator


### /!\ These models Source* will be used in all sources so we better never have to change them !!!
class SourceRecord(BaseModel):
    id: str = Field(..., description="Unique identifier of the record in the source")
    data: dict = Field(..., description="JSON payload of the record")

    @field_validator("id", mode="before")
    def coerce_int_to_str(value: Union[int, str]) -> str:
        # Coerce int to str in case Source return id as int
        if isinstance(value, int):
            return str(value)
        return value


class SourceIteration(BaseModel):
    next_pagination: dict = Field(..., description="Next pagination to be used in the next iteration")
    records: List[SourceRecord] = Field(..., description="List of records retrieved in the current iteration")
