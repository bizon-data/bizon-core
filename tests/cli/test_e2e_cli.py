import os

from click.testing import CliRunner

from bizon.cli.main import cli

BIZON_CONFIG_DUMMY_TO_FILE = f"""
    name: test_job

    source:
      name: dummy
      stream: creatures
      authentication:
        type: api_key
        params:
          token: dummy_key

    destination:
      name: file
      config:
        filepath: test_e2e_run__dummy_to_file.jsonl

    engine:
      backend:
        type: postgres
        config:
          database: bizon_test
          schema: public
          syncCursorInDBEvery: 2
          host: {os.environ.get("POSTGRES_HOST", "localhost")}
          port: 5432
          username: postgres
          password: bizon

      runner:
        type: thread
"""


def test_e2e_run_command_dummy_to_file():

    runner = CliRunner(mix_stderr=False)

    with runner.isolated_filesystem():

        with open("config.yml", "w") as file:
            file.write(BIZON_CONFIG_DUMMY_TO_FILE)

        result = runner.invoke(cli, ["run", "config.yml"], catch_exceptions=True)
        assert result.exit_code == 0
        assert result.output == "Pipeline finished successfully.\n"
