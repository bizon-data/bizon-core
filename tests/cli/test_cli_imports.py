"""Guard the CLI against optional dependencies leaking into module-level imports.

`bizon.common.models` imports every destination config unconditionally (pydantic needs the classes
to build the discriminated union), so a single module-level `from google.cloud import bigquery`
anywhere in that chain makes *every* command fail on an install without the matching extra —
`bizon --help` included.

This asserts on `sys.modules` in a subprocess rather than on an ImportError, so it catches the
regression even in a dev environment where all the extras happen to be installed.
"""

import subprocess
import sys

# Top-level packages that are only present with an extra (see [project.optional-dependencies]).
OPTIONAL_DEPENDENCIES = [
    "google.cloud.bigquery",
    "google.cloud.bigquery_storage",
    "google.cloud.secretmanager",
    "confluent_kafka",
    "psycopg2",
    "pika",
    "gspread",
    "datadog",
    "ddtrace",
]

PROBE = """
import sys
import bizon.cli.main  # noqa: F401
leaked = [m for m in {optional!r} if m in sys.modules]
print(",".join(leaked))
"""


def _modules_pulled_in_by(statement: str) -> list:
    result = subprocess.run(
        [sys.executable, "-c", statement],
        capture_output=True,
        text=True,
        check=True,
    )
    return [m for m in result.stdout.strip().split(",") if m]


def test_cli_import_pulls_in_no_optional_dependency():
    """Importing the CLI must not import anything that lives behind an extra."""
    leaked = _modules_pulled_in_by(PROBE.format(optional=OPTIONAL_DEPENDENCIES))

    assert leaked == [], (
        f"Importing bizon.cli.main pulled in optional dependencies: {leaked}. "
        f"Every CLI command will fail on an install without the matching extra. "
        f"Import these lazily, inside the function that needs them."
    )


def test_cli_help_runs_without_touching_optional_dependencies():
    """The end result the guard above protects: the CLI is usable on a bare install."""
    result = subprocess.run(
        [sys.executable, "-m", "bizon", "--help"],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "Bizon CLI" in result.stdout
