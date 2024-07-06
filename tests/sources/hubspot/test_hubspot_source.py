from bizon.sources.hubspot.src.models.config import HubSpotSourceConfig, HubSpotAuthType
from bizon.sources.hubspot.src.clients import HubSpotContacts, HubSpotCompanies
from bizon.sources.hubspot.src.constants import URL_TOKEN_REFRESH

def test_source_config_valid():
    raw_config = {
        "source": {
            "name": "hubspot",
            "stream_name": "contacts",
            "authentication": {
                "type": "oauth",
                "oauth": {
                    "client_id": "id-12345abcd",
                    "client_secret": "cli-12345abcd",
                    "refresh_token": "fresh-12345abcd"
                }
            }
        }
    }
    source_config = HubSpotSourceConfig.parse_obj(raw_config["source"])
    assert source_config.authentication.type == HubSpotAuthType.OAUTH
    assert source_config.authentication.oauth.client_id == "id-12345abcd"
    assert source_config.authentication.oauth.client_secret == "cli-12345abcd"
    assert source_config.authentication.oauth.refresh_token == "fresh-12345abcd"
    assert source_config.authentication.oauth.token_refresh_endpoint == URL_TOKEN_REFRESH


def test_source_instanciation_contacts():
    raw_config = {
        "source": {
            "name": "hubspot",
            "stream_name": "contacts",
            "authentication": {
                "type": "oauth",
                "oauth": {
                    "client_id": "id-12345abcd",
                    "client_secret": "cli-12345abcd",
                    "refresh_token": "fresh-12345abcd"
                }
            }
        }
    }
    client = HubSpotContacts(
        config=HubSpotSourceConfig.parse_obj(raw_config["source"])
    )
    assert client.object == "contacts"


def test_source_instanciation_companies():
    raw_config = {
        "source": {
            "name": "hubspot",
            "stream_name": "companies",
            "authentication": {
                "type": "oauth",
                "oauth": {
                    "client_id": "id-12345abcd",
                    "client_secret": "cli-12345abcd",
                    "refresh_token": "fresh-12345abcd"
                }
            }
        }
    }
    client = HubSpotCompanies(
        config=HubSpotSourceConfig.parse_obj(raw_config["source"])
    )
    assert client.object == "companies"
