import json
from typing import Tuple

import polars as pl
from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.destination.destination import AbstractDestination
from bizon.engine.backend.backend import AbstractBackend

from .config import FileDestinationDetailsConfig


class FileDestination(AbstractDestination):

    def __init__(self, sync_metadata: SyncMetadata, config: FileDestinationDetailsConfig, backend: AbstractBackend):
        super().__init__(sync_metadata, config, backend)
        self.config: FileDestinationDetailsConfig = config

    def check_connection(self) -> bool:
        return True

    def delete_table(self) -> bool:
        return True

    def write_records(self, df_destination_records: pl.DataFrame) -> Tuple[bool, str]:

        if self.config.unnest:

            schema_keys = set([column.name for column in self.config.record_schema])

            with open(self.config.filepath, "a") as f:
                for value in df_destination_records["source_data"].str.json_decode().to_list():
                    assert set(value.keys()) == schema_keys, "Keys do not match the schema"
                    f.write(f"{json.dumps(value)}\n")

        else:
            df_destination_records.write_ndjson(self.config.filepath)

        return True, ""
