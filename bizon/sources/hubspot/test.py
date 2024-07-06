from bizon.sources.hubspot.src.clients import HubSpotContacts
from bizon.sources.hubspot.src.models.config import HubSpotSourceConfig
from bizon.cli.parser import parse_from_yaml
import os

raw_config = parse_from_yaml(os.path.abspath("bizon/sources/hubspot/config/hubspot.yml"))

HubSpotSourceConfig = HubSpotSourceConfig.parse_obj(raw_config["source"])

client = HubSpotContacts(
    config=HubSpotSourceConfig
)
result = client.get()
print(result)

properties = client.list_properties()
print(properties)