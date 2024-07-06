from typing import List
from .generic_client import HubSpotClient
from .models.config import HubSpotSourceConfig, HubSpotObjects


class HubSpotContacts(HubSpotClient):
    def __init__(self, config: HubSpotSourceConfig):
        super().__init__(config=config, object=HubSpotObjects.CONTACTS)
