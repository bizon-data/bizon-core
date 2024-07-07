from threading import Thread

from loguru import logger

from bizon.api.client import APIClient
from bizon.api.cursor import Cursor
from bizon.api.models.config import SourceConfig
from bizon.sources import MAP_SOURCES

from .constants import QUEUE_TERMINATION
from .queue import PipelineQueue


class SourceThread(Thread):
    def __init__(self, queue: PipelineQueue, source_config: dict):
        super(SourceThread, self).__init__()
        self.name = "source"
        self.queue = queue
        self.raw_source_config = source_config
        self.parsed_sync_config = SourceConfig.model_validate(source_config)

    def get_client_from_config(self, raw_source_config: dict) -> APIClient:
        if raw_source_config.get("name") not in MAP_SOURCES:
            raise ValueError(
                f"Connector {raw_source_config['name']} is not supported. Supported sources are {MAP_SOURCES.keys()}"
            )

        if raw_source_config.get("stream_name") not in MAP_SOURCES[raw_source_config["name"]]:
            raise ValueError(
                (
                    f"Stream {raw_source_config['stream_name']} is not supported for connector {raw_source_config['name']}."
                    f"Supported streams are {MAP_SOURCES[raw_source_config['name']].keys()}"
                )
            )

        config_client_class: SourceConfig = MAP_SOURCES[raw_source_config["name"]][raw_source_config["stream_name"]][
            "config"
        ]
        client_class: APIClient = MAP_SOURCES[raw_source_config["name"]][raw_source_config["stream_name"]]["client"]

        config_client = config_client_class.model_validate(raw_source_config)
        connector = client_class(config=config_client)
        return connector

    def run(self):
        client = self.get_client_from_config(self.raw_source_config)

        # Get the total number of records
        total_records = client.get_total_records_count()
        assert total_records > 0

        # First we check if the connection is successful and initialize the cursor
        data, next_state = client.get(state=None)
        cursor = Cursor(state=next_state, total_records=total_records)
        self.queue.put(data, index=cursor.iteration)

        while cursor.state.is_finished is False:

            if (
                self.parsed_sync_config.max_iterations is not None
                and cursor.iteration >= self.parsed_sync_config.max_iterations
            ):
                break

            result, next_state = client.get(state=cursor.state)
            cursor.update_state(next_state)
            self.queue.put(result, index=cursor.iteration)
            print(len(result))

        logger.info("Terminating destination thread ...")
        self.queue.put(QUEUE_TERMINATION, index=cursor.iteration)
        logger.info("Sent termination signal to destination thread ...")
