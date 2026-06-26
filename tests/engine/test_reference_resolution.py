import sys
import types

import pytest

from bizon.engine.resolvers import resolver as resolver_module
from bizon.engine.resolvers.adapters.env import EnvResolver
from bizon.engine.resolvers.resolver import (
    AbstractReferenceResolver,
    ReferenceResolutionError,
    collect_references_in_config,
    resolve_references_in_config,
)

# ---------------------------------------------------------------------------
# Fake gsm resolver so the walker/registry can be tested without real GCP.
# ---------------------------------------------------------------------------


class _FakeGSMResolver(AbstractReferenceResolver):
    scheme = "gsm"

    def __init__(self, settings=None):
        self.settings = settings or {}
        self.calls = []

    def resolve(self, path: str) -> str:
        self.calls.append(path)
        return f"val::{path}"


@pytest.fixture
def fake_gsm(monkeypatch):
    state = {}

    def factory(settings):
        resolver = _FakeGSMResolver(settings)
        state["resolver"] = resolver
        return resolver

    monkeypatch.setitem(resolver_module._SCHEME_FACTORIES, "gsm", factory)
    return state


# ---------------------------------------------------------------------------
# Walker / registry behaviour (scheme-agnostic logic)
# ---------------------------------------------------------------------------


def test_whole_value_reference_resolved(fake_gsm):
    config = {"source": {"token": "gsm://my-secret"}}
    out = resolve_references_in_config(config)
    assert out["source"]["token"] == "val::my-secret"


def test_version_pinned_reference(fake_gsm):
    out = resolve_references_in_config({"token": "gsm://my-secret/versions/3"})
    assert out["token"] == "val::my-secret/versions/3"


def test_inline_reference_inside_larger_string(fake_gsm):
    out = resolve_references_in_config({"dsn": "postgres://u:${gsm://db-pw}@host/db"})
    assert out["dsn"] == "postgres://u:val::db-pw@host/db"


def test_multiple_inline_references_in_one_string(fake_gsm):
    out = resolve_references_in_config({"dsn": "${gsm://user}:${gsm://pw}@host"})
    assert out["dsn"] == "val::user:val::pw@host"


def test_reference_in_list_field_is_resolved(fake_gsm):
    out = resolve_references_in_config({"ids": ["plain", "gsm://secret-id"]})
    assert out["ids"] == ["plain", "val::secret-id"]


def test_repeated_reference_hits_resolver_once(fake_gsm):
    config = {"a": "gsm://same", "b": "gsm://same", "c": "${gsm://same}"}
    resolve_references_in_config(config)
    assert fake_gsm["resolver"].calls == ["same"]


def test_plain_uri_with_unknown_scheme_left_untouched(fake_gsm):
    config = {"url": "postgresql://user:pw@host:5432/db", "web": "https://example.com"}
    out = resolve_references_in_config(config)
    assert out == config


def test_unknown_scheme_inline_raises(fake_gsm):
    with pytest.raises(ReferenceResolutionError, match="Unknown reference scheme 'nope'"):
        resolve_references_in_config({"x": "pre-${nope://thing}-post"})


def test_malformed_inline_token_raises(fake_gsm):
    with pytest.raises(ReferenceResolutionError, match="Malformed reference token"):
        resolve_references_in_config({"x": "${gsm:missing-slashes}"})


def test_error_names_the_config_path(fake_gsm):
    with pytest.raises(ReferenceResolutionError, match=r"engine\.backend\.password"):
        resolve_references_in_config({"engine": {"backend": {"password": "${bad://x}"}}})


def test_secrets_block_settings_passed_to_provider(fake_gsm):
    config = {"secrets": {"gsm": {"project_id": "my-proj"}}, "token": "gsm://s"}
    resolve_references_in_config(config)
    assert fake_gsm["resolver"].settings == {"project_id": "my-proj"}


# ---------------------------------------------------------------------------
# env:// resolver
# ---------------------------------------------------------------------------


def test_env_whole_value(monkeypatch):
    monkeypatch.setenv("MY_TOKEN", "abc123")
    out = resolve_references_in_config({"token": "env://MY_TOKEN"})
    assert out["token"] == "abc123"


def test_env_inline(monkeypatch):
    monkeypatch.setenv("PG_PW", "s3cr3t")
    out = resolve_references_in_config({"dsn": "postgres://u:${env://PG_PW}@host/db"})
    assert out["dsn"] == "postgres://u:s3cr3t@host/db"


def test_env_missing_variable_raises(monkeypatch):
    monkeypatch.delenv("NOPE_VAR", raising=False)
    with pytest.raises(ReferenceResolutionError, match="NOPE_VAR"):
        resolve_references_in_config({"token": "env://NOPE_VAR"})


def test_env_resolver_direct(monkeypatch):
    monkeypatch.setenv("FOO", "bar")
    assert EnvResolver().resolve("FOO") == "bar"


# ---------------------------------------------------------------------------
# collect_references_in_config (CLI dry-run helper)
# ---------------------------------------------------------------------------


def test_collect_references(fake_gsm):
    config = {
        "source": {"token": "gsm://api-key"},
        "engine": {"backend": {"dsn": "postgres://${gsm://pw}@h/d"}},
        "ids": ["gsm://id-a"],
        "plain": "not-a-ref",
    }
    found = collect_references_in_config(config)
    assert ("source.token", "gsm://api-key") in found
    assert ("engine.backend.dsn", "gsm://pw") in found
    assert ("ids[0]", "gsm://id-a") in found
    assert all(ref != "not-a-ref" for _, ref in found)


# ---------------------------------------------------------------------------
# GSMResolver path building + payload decoding (fake secretmanager client)
# ---------------------------------------------------------------------------


class _FakePayload:
    def __init__(self, data):
        self.data = data


class _FakeResponse:
    def __init__(self, data):
        self.payload = _FakePayload(data)


class _FakeClient:
    def __init__(self, secrets):
        self._secrets = secrets
        self.requested = []

    def access_secret_version(self, name):
        self.requested.append(name)
        if name not in self._secrets:
            raise RuntimeError("404 Secret not found")
        return _FakeResponse(self._secrets[name].encode("UTF-8"))


@pytest.fixture
def fake_secretmanager(monkeypatch):
    secrets = {
        "projects/my-proj/secrets/db-pw/versions/latest": "from-latest",
        "projects/my-proj/secrets/db-pw/versions/3": "from-v3",
        "projects/other/secrets/full/versions/9": "from-full-path",
    }
    client = _FakeClient(secrets)

    fake_module = types.ModuleType("google.cloud.secretmanager")
    fake_module.SecretManagerServiceClient = lambda: client
    monkeypatch.setitem(sys.modules, "google.cloud.secretmanager", fake_module)
    return client


def _make_gsm(project_id="my-proj"):
    from bizon.engine.resolvers.adapters.gcp.gsm import GSMResolver

    return GSMResolver(project_id=project_id)


def test_gsm_short_name_uses_latest(fake_secretmanager):
    assert _make_gsm().resolve("db-pw") == "from-latest"
    assert fake_secretmanager.requested == ["projects/my-proj/secrets/db-pw/versions/latest"]


def test_gsm_version_pinned(fake_secretmanager):
    assert _make_gsm().resolve("db-pw/versions/3") == "from-v3"


def test_gsm_full_resource_path_passthrough(fake_secretmanager):
    assert _make_gsm().resolve("projects/other/secrets/full/versions/9") == "from-full-path"


def test_gsm_missing_secret_raises(fake_secretmanager):
    with pytest.raises(ReferenceResolutionError, match="could not access"):
        _make_gsm().resolve("does-not-exist")


def test_gsm_no_project_raises(fake_secretmanager, monkeypatch):
    from bizon.engine.resolvers.adapters.gcp import gsm as gsm_module

    monkeypatch.setattr(gsm_module.GSMResolver, "_default_project", staticmethod(lambda: None))
    resolver = _make_gsm(project_id=None)
    with pytest.raises(ReferenceResolutionError, match="no GCP project"):
        resolver.resolve("db-pw")
