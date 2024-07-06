from .generic_client import HubSpotClient
from .models.config import HubSpotSourceConfig, HubSpotObjects


class HubSpotContacts(HubSpotClient):
    def __init__(self, config: HubSpotSourceConfig):
        super().__init__(config=config, object=HubSpotObjects.CONTACTS)

class HubSpotCompanies(HubSpotClient):
    def __init__(self, config: HubSpotSourceConfig):
        super().__init__(config=config, object=HubSpotObjects.COMPANIES)

class HubSpotDeals(HubSpotClient):
    def __init__(self, config: HubSpotSourceConfig):
        super().__init__(config=config, object=HubSpotObjects.DEALS)
