from bizon.sources.hubspot.src.clients import HubSpotContacts
from bizon.sources.hubspot.src.models.config import HubSpotSourceConfig
from bizon.cli.parser import parse_from_yaml
import os

raw_config = parse_from_yaml(os.path.abspath("bizon/sources/hubspot/config/hubspot.yml"))

HubSpotSourceConfig = HubSpotSourceConfig.parse_obj(raw_config["source"])

client = HubSpotContacts(
    config=HubSpotSourceConfig
)

is_connected, error = client.check_connection()
assert is_connected

granted_scopes = client.get_granted_scopes()
print(granted_scopes)

properties = client.list_properties()
print(properties)

result = client.get()
print(result)
