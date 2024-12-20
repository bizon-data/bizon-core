from bizon.connectors.sources.hubspot.src.hubspot_objects import (
    URL_TOKEN_REFRESH,
    HubSpotObjectsSource,
    HubSpotSourceConfig,
)
from bizon.source.auth.config import AuthType


def test_source_config_valid():
    raw_config = {
        "source": {
            "name": "hubspot",
            "stream": "contacts",
            "authentication": {
                "type": "oauth",
                "params": {
                    "token_refresh_endpoint": "https://api.hubapi.com/oauth/v1/token",
                    "client_id": "id-12345abcd",
                    "client_secret": "cli-12345abcd",
                    "refresh_token": "fresh-12345abcd",
                },
            },
        }
    }
    source_config = HubSpotSourceConfig.model_validate(raw_config["source"])
    assert source_config.authentication.type == AuthType.OAUTH
    assert source_config.authentication.params.client_id == "id-12345abcd"
    assert source_config.authentication.params.client_secret == "cli-12345abcd"
    assert source_config.authentication.params.refresh_token == "fresh-12345abcd"
    assert source_config.authentication.params.token_refresh_endpoint == URL_TOKEN_REFRESH


def test_source_instanciation_contacts():
    raw_config = {
        "source": {
            "name": "hubspot",
            "stream": "contacts",
            "init_pipeline": False,
            "authentication": {
                "type": "oauth",
                "params": {
                    "token_refresh_endpoint": "https://api.hubapi.com/oauth/v1/token",
                    "client_id": "id-12345abcd",
                    "client_secret": "cli-12345abcd",
                    "refresh_token": "fresh-12345abcd",
                },
            },
        }
    }
    client = HubSpotObjectsSource(config=HubSpotSourceConfig.model_validate(raw_config["source"]))
    assert client.object == "contacts"


def test_source_instanciation_companies():
    raw_config = {
        "source": {
            "name": "hubspot",
            "stream": "companies",
            "init_pipeline": False,
            "authentication": {
                "type": "oauth",
                "params": {
                    "token_refresh_endpoint": "https://api.hubapi.com/oauth/v1/token",
                    "client_id": "id-12345abcd",
                    "client_secret": "cli-12345abcd",
                    "refresh_token": "fresh-12345abcd",
                },
            },
        }
    }
    client = HubSpotObjectsSource(config=HubSpotSourceConfig.model_validate(raw_config["source"]))
    assert client.object == "companies"
