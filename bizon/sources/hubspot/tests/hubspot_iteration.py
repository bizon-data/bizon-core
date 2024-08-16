import os
from threading import Thread

from bizon.cli.parser import parse_from_yaml
from bizon.source.cursor import Cursor
from bizon.sources.hubspot.src.models.config import HubSpotSourceConfig
from bizon.sources.hubspot.src.source import HubSpotSource

raw_config = parse_from_yaml(os.path.abspath("bizon/sources/hubspot/config/hubspot.yml"))

HubSpotSourceConfig = HubSpotSourceConfig.model_validate(raw_config["source"])
client = HubSpotSource(config=HubSpotSourceConfig)

# First we check if the connection is successful and initialize the cursor
is_success_connection, connection_error = client.check_connection()

assert is_success_connection


total_records = client.get_total_records_count()
assert total_records > 0

cursor = Cursor(source_name="hubspot", stream_name="contacts", job_id=11112, total_records=total_records)

next_pagination, data = client.get()

while not cursor.is_finished:
    next_pagination, data = client.get(pagination=next_pagination)
    cursor.update_state(pagination_dict=next_pagination, nb_records_fetched=len(data))
    print(data)
