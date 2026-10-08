import sqlite3
import textwrap

from click.testing import CliRunner

from bizon.cli.main import cli
from bizon.common.models import BizonConfig

SOURCE = """
import os

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource


class IdConfig(SourceConfig):
    pass


class IdSource(AbstractSource):
    @staticmethod
    def streams():
        return ["items"]

    @staticmethod
    def get_config_class():
        return IdConfig

    def get_authenticator(self):
        return None

    def check_connection(self):
        return True, None

    def get_total_records_count(self):
        return None

    def get(self, pagination=None):
        with open(os.environ["ID_TEST_LOG"], "a") as f:
            f.write("get\\n")
        return SourceIteration(records=[SourceRecord(id="1", data={})], next_pagination={})

    def get_records_after(self, source_state, pagination=None):
        with open(os.environ["ID_TEST_LOG"], "a") as f:
            f.write("get_records_after\\n")
        return SourceIteration(records=[SourceRecord(id="1", data={})], next_pagination={})
"""


def _config(name: str, id_line: str = "") -> dict:
    return {
        "name": name,
        **({"id": id_line} if id_line else {}),
        "source": {"name": "x", "stream": "y"},
        "destination": {"name": "logger", "config": {}},
    }


def test_job_name_defaults_to_name():
    assert BizonConfig.model_validate(_config("orders to bigquery")).job_name == "orders to bigquery"
    assert BizonConfig.model_validate(_config("orders to bigquery", "orders")).job_name == "orders"


def test_renaming_a_pipeline_keeps_its_state_when_id_is_set(tmp_path, monkeypatch):
    (tmp_path / "source.py").write_text(SOURCE)
    log = tmp_path / "calls.log"
    monkeypatch.setenv("ID_TEST_LOG", str(log))
    db = tmp_path / "state.db"

    def run(name: str, id_line: str):
        config = tmp_path / "config.yml"
        config.write_text(
            textwrap.dedent(
                f"""
                name: {name}
                {id_line}
                source:
                  source_file_path: {tmp_path / "source.py"}
                  name: idsource
                  stream: items
                  sync_mode: incremental
                destination:
                  name: logger
                  config: {{}}
                engine:
                  backend:
                    type: sqlite
                    config:
                      database: {db}
                      schema: main
                """
            )
        )
        outcome = CliRunner().invoke(cli, ["run", str(config)])
        assert outcome.exit_code == 0, outcome.output

    run("items to logger", "id: items-stable")
    run("items to logger, renamed", "id: items-stable")
    run("items to logger, renamed again", "")

    assert log.read_text().splitlines() == ["get", "get_records_after", "get"]
    # The sqlite backend appends its own extension to `database`.
    with sqlite3.connect(f"{db}.sqlite3") as conn:
        names = [row[0] for row in conn.execute("SELECT name FROM stream_jobs ORDER BY created_at")]
        cursor_names = {row[0] for row in conn.execute("SELECT name FROM destination_cursors")}
    assert names == ["items-stable", "items-stable", "items to logger, renamed again"]
    assert "items-stable" in cursor_names
