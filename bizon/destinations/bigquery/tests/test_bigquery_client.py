import logging
import os
import random
from random import randint

import pytest
from faker import Faker

from bizon.cli.parser import parse_from_yaml
from bizon.destinations.bigquery.src.config import BigQueryConfig
from bizon.destinations.bigquery.src.destination import BigQueryDestination
from bizon.source.config import SourceConfig

logger = logging.getLogger(__name__)

fake = Faker("en_US")


@pytest.mark.skipif(
    os.getenv("POETRY_ENV_TEST") == "CI",
    reason="Skipping tests that require a BigQuery connexion",
)
def test_config_models():
    raw_config = parse_from_yaml(os.path.abspath("bizon/destinations/bigquery/config/bigquery.test.yml"))
    assert bool(SourceConfig.model_validate(raw_config["source"])) is True
    logger.info("source validated...")

    assert bool(BigQueryConfig.model_validate(raw_config["destination"])) is True
    logger.info("destination validated...")


# def test_load_records_to_bigquery():
#     bigquery_config = BigQueryConfig.model_validate(raw_config["destination"])
#     source_config = SourceConfig.model_validate(raw_config["source"])

#     fake_records = [
#         {"foo": randint(0, 100), "bar": {"baz": fake.name(), "poo": float(random.randrange(155, 389)) / 100}}
#         for _ in range(100)
#     ]
#     client = BigQueryClient(config=bigquery_config.config, source_config=source_config)

#     success = client.load_records_to_bigquery(json_records=fake_records)

#     assert success is True
