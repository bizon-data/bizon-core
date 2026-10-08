import pendulum
import pytest
import requests

from bizon.source.auth.authenticators.oauth import Oauth2AuthParams
from bizon.source.auth.authenticators.token import TokenAuthParams
from bizon.source.auth.builder import AuthBuilder


def _header(auth) -> dict:
    return dict(auth(requests.Request("GET", "https://api.example.com").prepare()).headers)


def test_token_defaults_to_bearer():
    assert _header(AuthBuilder.token(TokenAuthParams(token="abc")))["Authorization"] == "Bearer abc"


def test_token_without_prefix_sends_the_bare_token():
    auth = AuthBuilder.token(TokenAuthParams(token="abc", auth_header="x-api-key", auth_method=""))

    assert _header(auth)["x-api-key"] == "abc"


def test_token_without_prefix_is_accepted_by_requests():
    session = requests.Session()
    session.auth = AuthBuilder.token(TokenAuthParams(token="abc", auth_header="x-api-key", auth_method=""))

    prepared = session.prepare_request(requests.Request("GET", "https://api.example.com"))

    assert prepared.headers["x-api-key"] == "abc"


def _oauth(**params):
    return AuthBuilder.oauth2(
        Oauth2AuthParams(token_refresh_endpoint="https://auth.example.com", client_id="id", client_secret="s", **params)
    )


@pytest.mark.parametrize(
    "lifetime, expected_skew",
    [(1800, 60), (300, 30), (10, 1)],
)
def test_default_skew_is_ten_percent_of_lifetime_capped_at_a_minute(lifetime, expected_skew):
    auth = _oauth()
    auth.set_token_expiry_date(lifetime)

    assert auth.get_expiry_skew_seconds() == pytest.approx(expected_skew, abs=0.01)


def test_token_is_refreshed_within_the_skew():
    auth = _oauth()
    auth.set_token_expiry_date(1800)
    auth._token_expiry_date = pendulum.now().add(seconds=30)

    assert auth.token_has_expired()


def test_token_is_kept_outside_the_skew():
    auth = _oauth()
    auth.set_token_expiry_date(1800)

    assert not auth.token_has_expired()


def test_configured_skew_wins():
    auth = _oauth(expiry_skew_seconds=0)
    auth.set_token_expiry_date(1800)
    auth._token_expiry_date = pendulum.now().add(seconds=30)

    assert not auth.token_has_expired()


def test_refresh_happens_once_per_lifetime(monkeypatch):
    auth = _oauth()
    refreshes = []
    monkeypatch.setattr(auth, "refresh_access_token", lambda: refreshes.append(1) or (f"t{len(refreshes)}", 1800))

    assert auth.get_access_token() == "t1"
    assert auth.get_access_token() == "t1"
    assert len(refreshes) == 1
