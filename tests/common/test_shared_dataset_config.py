"""Tests for the backend/destination guard on a shared BigQuery dataset."""

from unittest.mock import MagicMock, patch

import pytest
import yaml
from pydantic import ValidationError

from bizon.common.models import BizonConfig

CONFIG_TEMPLATE = """
name: test_shared_dataset

source:
  name: dummy
  stream: creatures
  authentication:
    type: api_key
    params:
      token: dummy_key

destination:
  name: bigquery
  config:
    project_id: my-project
    dataset_id: {dataset_id}
    dataset_location: {dataset_location}
    create_dataset: {create_dataset}
    gcs_buffer_bucket: my-bucket

engine:
  backend:
    type: bigquery
    config:
      database: my-project
      schema: bizon_state
      create_schema: {create_schema}
"""


def build_config(
    dataset_id: str = "bizon_state",
    dataset_location: str = "US",
    create_dataset: bool = False,
    create_schema: bool = False,
    schema_location: str = None,
) -> BizonConfig:
    config = yaml.safe_load(
        CONFIG_TEMPLATE.format(
            dataset_id=dataset_id,
            dataset_location=dataset_location,
            create_dataset=str(create_dataset).lower(),
            create_schema=str(create_schema).lower(),
        )
    )
    if schema_location:
        config["engine"]["backend"]["config"]["schema_location"] = schema_location
    return BizonConfig.model_validate(config)


def test_create_dataset_without_create_schema_is_rejected():
    """The backend checks the shared dataset in init_job, before any destination code runs, so this
    combination cannot bootstrap anything - it just fails on the first run."""
    with pytest.raises(ValidationError) as excinfo:
        build_config(create_dataset=True)

    assert "create_schema" in str(excinfo.value)


def test_a_dataset_the_backend_does_not_share_is_left_alone():
    config = build_config(dataset_id="bizon_data", create_dataset=True)
    assert config.engine.backend.config.create_schema is False


def test_unset_schema_location_is_inherited_from_the_destination():
    config = build_config(dataset_location="EU", create_dataset=True, create_schema=True)
    assert config.engine.backend.config.schema_location == "EU"


def test_conflicting_locations_warn_but_do_not_reject():
    """The dataset is usually already there, in which case both settings are inert and the pipeline
    has been working - rejecting would break it on upgrade."""
    warning = MagicMock()

    with patch("bizon.common.models.logger.warning", warning):
        config = build_config(dataset_location="EU", create_dataset=True, create_schema=True, schema_location="US")

    assert config.engine.backend.config.schema_location == "US"
    assert "schema_location" in warning.call_args[0][0]


def test_a_config_using_neither_flag_is_untouched():
    config = build_config()
    assert config.engine.backend.config.create_schema is False
    assert config.engine.backend.config.schema_location is None
