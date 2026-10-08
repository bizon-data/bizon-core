import pytest
from click.testing import CliRunner

from bizon.cli.main import cli

BASE_SOURCE = """
  name: dummy
  stream: creatures
  authentication:
    type: api_key
    params:
      token: dummy_key
"""

DESTINATION = """
destination:
  name: logger
  config:
    dummy: dummy
"""


@pytest.fixture
def write_config(tmp_path):
    def write(source_block: str = BASE_SOURCE, extra: str = "") -> str:
        path = tmp_path / "config.yml"
        path.write_text("name: validate_test\n\nsource:" + source_block + extra + DESTINATION)
        return str(path)

    return write


def validate(*args):
    return CliRunner().invoke(cli, ["config", "validate", *args])


def test_valid_config(write_config):
    result = validate(write_config())

    assert result.exit_code == 0, result.output
    assert "is valid: dummy.creatures (full_refresh) -> logger" in result.output
    assert "Warning" not in result.output


def test_unknown_stream_is_an_error(write_config):
    result = validate(write_config(BASE_SOURCE.replace("creatures", "dragons")))

    assert result.exit_code == 1
    assert "Stream dragons not found" in result.output


def test_source_config_class_is_validated(write_config):
    result = validate(write_config(BASE_SOURCE + "  sleep: not_an_int\n"))

    assert result.exit_code == 1
    assert "sleep" in result.output


def test_engine_schema_is_validated(write_config):
    result = validate(write_config(extra="\nengine:\n  runner:\n    type: spaceship\n"))

    assert result.exit_code == 1
    assert "is invalid" in result.output


def test_unknown_source_key_warns(write_config):
    path = write_config(BASE_SOURCE + "  buffer_size: 50\n")

    result = validate(path)
    assert result.exit_code == 0, result.output
    assert "not declared by DummySourceConfig are ignored: buffer_size" in result.output

    strict = validate(path, "--strict")
    assert strict.exit_code == 1


def test_reset_in_config_is_deprecated(write_config):
    result = validate(write_config(BASE_SOURCE + "  reset: true\n"))

    assert result.exit_code == 0, result.output
    assert "`source.reset: true` in a config" in result.output


def test_unresolvable_reference_is_an_error(write_config, monkeypatch):
    monkeypatch.delenv("BIZON_VALIDATE_MISSING", raising=False)
    source = BASE_SOURCE.replace("dummy_key", "env://BIZON_VALIDATE_MISSING")

    result = validate(write_config(source))

    assert result.exit_code == 1
    assert "BIZON_VALIDATE_MISSING" in result.output


def test_skip_references_validates_without_resolving(write_config, monkeypatch):
    monkeypatch.delenv("BIZON_VALIDATE_MISSING", raising=False)
    source = BASE_SOURCE.replace("dummy_key", "env://BIZON_VALIDATE_MISSING")

    result = validate(write_config(source), "--skip-references")

    assert result.exit_code == 0, result.output


def test_skip_references_tolerates_a_reference_in_a_typed_field(write_config):
    result = validate(write_config(BASE_SOURCE + "  sleep: BIZON_ENV_SLEEP\n"), "--skip-references")

    assert result.exit_code == 0, result.output
    assert "1 field(s) hold an unresolved reference" in result.output


def test_skip_references_still_reports_real_errors(write_config):
    result = validate(write_config(BASE_SOURCE + "  sleep: not_an_int\n"), "--skip-references")

    assert result.exit_code == 1
