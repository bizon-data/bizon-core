import os
from threading import Thread

from bizon.api.cursor import Cursor
from bizon.cli.parser import parse_from_yaml
from bizon.sources.hubspot.src.clients import HubSpotContacts
from bizon.sources.hubspot.src.models.config import HubSpotSourceConfig

raw_config = parse_from_yaml(os.path.abspath("bizon/sources/hubspot/config/hubspot.yml"))

HubSpotSourceConfig = HubSpotSourceConfig.parse_obj(raw_config["source"])
client = HubSpotContacts(config=HubSpotSourceConfig)

# First we check if the connection is successful and initialize the cursor
data, next_state = client.get(state=None)
cursor = Cursor(state=next_state)

total_records = client.get_total_records_count()
assert total_records > 0

while cursor.state.is_finished is False:
    result, next_state = client.get(state=cursor.state)
    cursor.update_state(next_state)
    print(result)
