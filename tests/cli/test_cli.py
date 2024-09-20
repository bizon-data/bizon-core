import os

from click.testing import CliRunner

from bizon.cli.main import cli


def test_run_command():
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["run", os.path.abspath("tests/cli/test_pg_config.yml")])
    assert result.exit_code == 0
