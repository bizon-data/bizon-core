import os
from contextlib import contextmanager
from typing import Dict, List, Union

from datadog import initialize, statsd
from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.monitoring.config import MonitoringConfig
from bizon.monitoring.monitor import AbstractMonitor
from bizon.source.models import SourceRecord


class DatadogMonitor(AbstractMonitor):
    def __init__(self, sync_metadata: SyncMetadata, monitoring_config: MonitoringConfig):
        super().__init__(sync_metadata, monitoring_config)

        # In Kubernetes, set the host dynamically
        try:
            datadog_host_from_env_var = os.getenv(monitoring_config.config.datadog_host_env_var)
            if datadog_host_from_env_var:
                initialize(
                    statsd_host=datadog_host_from_env_var,
                    statsd_port=monitoring_config.config.datadog_agent_port,
                )
            else:
                initialize(
                    statsd_host=monitoring_config.config.datadog_agent_host,
                    statsd_port=monitoring_config.config.datadog_agent_port,
                )
        except Exception as e:
            logger.info(f"Failed to initialize Datadog agent: {e}")

        self.pipeline_monitor_status = "bizon_pipeline.status"
        self.tags = [
            f"pipeline_name:{self.sync_metadata.name}",
            f"pipeline_stream:{self.sync_metadata.stream_name}",
            f"pipeline_source:{self.sync_metadata.source_name}",
            f"pipeline_destination:{self.sync_metadata.destination_name}",
        ] + [f"{key}:{value}" for key, value in self.monitoring_config.config.tags.items()]

        self.pipeline_active_pipelines = "bizon_pipeline.active_pipelines"
        self.pipeline_records_synced = "bizon_pipeline.records_synced"
        self.pipeline_large_records = "bizon_pipeline.large_records"

    def track_pipeline_status(self, pipeline_status: PipelineReturnStatus, extra_tags: Dict[str, str] = {}) -> None:
        """
        Track the status of the pipeline.

        Args:
            status (str): The current status of the pipeline (e.g., 'running', 'failed', 'completed').
        """

        statsd.increment(
            self.pipeline_monitor_status,
            tags=self.tags
            + [f"pipeline_status:{pipeline_status}"]
            + [f"{key}:{value}" for key, value in extra_tags.items()],
        )

    def track_records_synced(
        self, num_records: int, destination_id: str, extra_tags: Dict[str, str] = {}, headers: List[Dict[str, str]] = []
    ) -> Union[List[Dict[str, str]], None]:
        """
        Track the number of records synced in the pipeline.

        Args:
            num_records (int): Number of records synced in this batch
        """
        statsd.increment(
            self.pipeline_records_synced,
            value=num_records,
            tags=self.tags + [f"{key}:{value}" for key, value in extra_tags.items()],
        )
        if os.getenv("DD_DATA_STREAMS_ENABLED") == "true":
            from ddtrace.data_streams import set_produce_checkpoint

            destination_type = self.sync_metadata.destination_alias

            for header in headers:
                if "x-datadog-sampling-priority" in header:
                    del header["x-datadog-sampling-priority"]
                if "dd-pathway-ctx-base64" in header:
                    del header["dd-pathway-ctx-base64"]
                set_produce_checkpoint(destination_type, destination_id, header.setdefault)
            return headers

    def track_large_records_synced(self, num_records: int, extra_tags: Dict[str, str] = {}) -> None:
        statsd.increment(
            self.pipeline_large_records,
            value=num_records,
            tags=self.tags + [f"{key}:{value}" for key, value in extra_tags.items()],
        )

    def track_source_iteration(self, records: List[SourceRecord]) -> Union[List[Dict[str, str]], None]:
        """
        Track the number of records consumed from a Kafka topic.

        Args:
            kafka_topic (str): The Kafka topic name
        """

        if os.getenv("DD_DATA_STREAMS_ENABLED") == "true":
            from ddtrace.data_streams import set_consume_checkpoint

            headers_list = []
            for record in records:
                headers = record.data.get("headers", {})
                set_consume_checkpoint("kafka", record.data["topic"], headers.get)
                headers_list.append(headers)
            return headers_list

    @contextmanager
    def trace(self, operation_name: str, resource: str = None, extra_tags: Dict[str, str] = None):
        """
        Create a trace span for monitoring using Datadog APM.

        Args:
            operation_name (str): The name of the operation being traced
            resource (str): The resource being operated on (e.g., topic name, table name)
            extra_tags (Dict[str, str]): Additional tags for the trace

        Yields:
            A span object that can be used to add additional metadata
        """
        if not self.monitoring_config.config.enable_tracing:
            yield None
            return

        try:
            from ddtrace import tracer
        except ImportError:
            logger.warning("ddtrace not available, skipping tracing")
            yield None
            return

        try:
            # Combine tags
            all_tags = self.tags.copy()
            if extra_tags:
                all_tags.extend([f"{key}:{value}" for key, value in extra_tags.items()])

            # Create the span
            with tracer.trace(operation_name, resource=resource) as span:
                # Add tags to the span
                for tag in all_tags:
                    if ":" in tag:
                        key, value = tag.split(":", 1)
                        span.set_tag(key, value)
                span.set_tag("_sampling_priority_v1", 1)
                yield span
        except Exception as e:
            logger.warning(f"Failed to create trace: {e}")
            yield None
