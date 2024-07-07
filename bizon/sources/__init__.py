from .hubspot.src.clients import HubSpotCompanies, HubSpotContacts, HubSpotDeals
from .hubspot.src.models.config import HubSpotSourceConfig

MAP_SOURCES = {
    "hubspot": {
        "contacts": {"client": HubSpotContacts, "config": HubSpotSourceConfig},
        "companies": {"client": HubSpotCompanies, "config": HubSpotSourceConfig},
        "deals": {"client": HubSpotDeals, "config": HubSpotSourceConfig},
    }
}

__all__ = [
    "__version__",
    "MAP_SOURCES",
]
