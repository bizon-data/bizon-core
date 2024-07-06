from typing import Optional

from pydantic import BaseModel

from bizon.api.models.state import AbstractState


class HubSpotPaging(BaseModel):
    link: Optional[str]
    after: Optional[str]


class HubSpotSyncState(AbstractState):
    paging: Optional[HubSpotPaging] = None
