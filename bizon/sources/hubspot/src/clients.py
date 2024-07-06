from typing import List
from .generic_client import HubSpotClient
from .models.config import HubSpotConfig, H


class HubSpotContacts(HubSpotClient):

    def __init__(self, config: HubSpotConfig, auth: HubSpotAuthConfig):
        super().__init__(object="contacts")

    @property
    def 
