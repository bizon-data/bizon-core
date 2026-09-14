"""Tests for the backend config models, and for what the discriminated union preserves."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.adapters.sqlalchemy.config import BigQueryConfigDetails
from bizon.engine.backend.config import BackendTypes
from bizon.engine.config import EngineConfig


def bigquery_engine_config(**config) -> EngineConfig:
    return EngineConfig.model_validate(
        {"backend": {"type": "bigquery", "config": {"database": "my-project", "schema": "bizon_state", **config}}}
    )


def test_bigquery_backend_keeps_its_own_fields():
    """`BigQuerySQLAlchemyConfig.config` used to be annotated `SQLAlchemyConfigDetails`, which meant
    pydantic validated every BigQuery-only field away without an error."""
    config = bigquery_engine_config(service_account_key="{}", create_schema=True, schema_location="EU").backend.config

    assert isinstance(config, BigQueryConfigDetails)
    assert config.service_account_key == "{}"
    assert config.create_schema is True
    assert config.schema_location == "EU"


def test_new_fields_default_off():
    config = bigquery_engine_config().backend.config
    assert config.create_schema is False
    assert config.schema_location is None


@pytest.mark.parametrize("location", ["EU'); DROP SCHEMA x --", "US\nEU", ""])
def test_schema_location_is_constrained(location):
    with pytest.raises(ValidationError):
        bigquery_engine_config(schema_location=location)


def test_engine_is_built_unchanged_without_the_new_fields():
    """A config that predates `schema_location` must produce the call it produces today."""
    config = bigquery_engine_config().backend.config

    with patch("bizon.engine.backend.adapters.sqlalchemy.backend.create_engine") as create_engine:
        SQLAlchemyBackend(config=config, type=BackendTypes.BIGQUERY)

    assert create_engine.call_args.kwargs == {"echo": False}


def test_schema_location_pins_the_job_location():
    config = bigquery_engine_config(schema_location="EU").backend.config

    with patch("bizon.engine.backend.adapters.sqlalchemy.backend.create_engine") as create_engine:
        SQLAlchemyBackend(config=config, type=BackendTypes.BIGQUERY)

    assert create_engine.call_args.kwargs["location"] == "EU"


def test_service_account_key_is_still_ignored():
    """Now that the field survives validation, honouring it would move pipelines off the ADC they
    have actually been authenticating with. It warns instead."""
    config = bigquery_engine_config(service_account_key='{"client_email": "x"}').backend.config
    warning = MagicMock()

    with patch("bizon.engine.backend.adapters.sqlalchemy.backend.create_engine") as create_engine:
        with patch("bizon.engine.backend.adapters.sqlalchemy.backend.logger.warning", warning):
            SQLAlchemyBackend(config=config, type=BackendTypes.BIGQUERY)

    assert "credentials_info" not in create_engine.call_args.kwargs
    assert "service_account_key" in warning.call_args[0][0]
