import os

from bizon.cli.parser import parse_from_yaml
from bizon.sources.hubspot.src.models.config import HubSpotSourceConfig
from bizon.sources.hubspot.src.source import HubSpotSource

raw_config = parse_from_yaml(os.path.abspath("bizon/sources/hubspot/config/hubspot.yml"))

HubSpotSourceConfig = HubSpotSourceConfig.model_validate(raw_config["source"])

client = HubSpotSource(config=HubSpotSourceConfig)

is_connected, error = client.check_connection()
assert is_connected

total_count = client.get_total_records_count()

granted_scopes = client.get_granted_scopes()
print(granted_scopes)

properties = client.list_properties()
print(properties)

next_pagination, data = client.get()
print(data)
