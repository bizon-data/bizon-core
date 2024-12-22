import pytest
from pydantic import ValidationError

from bizon.destination.config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
)


def test_config():
    config = AbstractDestinationConfig(
        name="file",
        config=AbstractDestinationDetailsConfig(
            unnest=False,
        ),
    )
    assert config


def test_config_no_record_schema_provided():

    with pytest.raises(ValidationError) as e:
        AbstractDestinationConfig(
            name="file",
            config=AbstractDestinationDetailsConfig(
                unnest=True,
            ),
        )


def test_config_with_unnest_provided_schema():
    config = AbstractDestinationConfig(
        name="file",
        config=AbstractDestinationDetailsConfig(
            unnest=True,
            record_schema=[
                {"name": "name", "type": "string", "description": "Name of the user"},
                {"name": "age", "type": "int", "description": "Age of the user"},
            ],
        ),
    )
    assert config
