from typing import List, Tuple

from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.destinations.destination import AbstractDestination
from bizon.destinations.models import DestinationRecord
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

    def write_records(self, destination_records: List[DestinationRecord]) -> Tuple[bool, str]:
        with open(self.config.filepath, "a") as f:
            for record in destination_records:
                f.write(record.model_dump_json() + "\n")
        return True, ""
