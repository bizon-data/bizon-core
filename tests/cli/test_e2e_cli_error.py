import tempfile

from click.testing import CliRunner

from bizon.cli.main import cli

BIZON_CONFIG_DUMMY_TO_FILE = f"""
name: test_job_3

source:
    name: dummy
    stream: creatures

    authentication:
        type: api_key
        params:
            token: dummy_key
    sleep: 2

destination:
    name: file
    config:
        filepath: test.jsonl

transforms:
- label: transform_data
  python: |
    if 'name' in data:
        data['name'] = data['this_key_doesnt_exist'].upper()
"""


def test_e2e_run_command_dummy_to_file():

    runner = CliRunner(mix_stderr=False)

    with tempfile.NamedTemporaryFile(delete=False) as temp:
        # Write config in temp file
        with open(temp.name, "w") as f:
            f.write(BIZON_CONFIG_DUMMY_TO_FILE)

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(cli, ["run", temp.name])

        assert result.exit_code == 1
        assert (
            "Pipeline finished with status Failure (Producer: killed_by_runner, Consumer: transform_error)"
            in result.stderr
        )
