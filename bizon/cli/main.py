import click
from dotenv import find_dotenv, load_dotenv

from bizon.common.models import BizonConfig
from bizon.engine.backend.backend import BackendFactory
from bizon.engine.backend.config import BackendTypes
from bizon.engine.engine import (
    RunnerFactory,
    replace_env_variables_in_config,
    resolve_config,
)
from bizon.engine.resolvers import (
    ReferenceResolutionError,
    ResolverRegistry,
    collect_references_in_config,
)
from bizon.engine.runner.config import LoggerLevel
from bizon.source.config import SourceSyncModes
from bizon.source.discover import discover_all_sources

from .utils import (
    parse_from_yaml,
    set_custom_source_path_in_config,
    set_log_level,
    set_reset_in_config,
    set_runner_in_config,
)


@click.group()
def cli():
    """Bizon CLI."""
    pass


# Create a 'destination' group under 'bizon'
@cli.group()
def source():
    """Subcommands for handling sources."""
    pass


@source.command()
def list():
    """List available sources."""

    click.echo("Retrieving available sources...")
    sources = discover_all_sources()

    click.echo("Available sources:")
    for source_name, source_model in sources.items():
        if not source_model.available_streams:
            click.echo(
                f"{source_name} - NOT AVAILABLE, run 'pip install bizon[{source_name}]' to install missing dependencies."
            )
        else:
            click.echo(f"{source_name} - {source_model.available_streams}")


# Create a 'destination' group under 'bizon'
@cli.group()
def stream():
    """Subcommands for handling streams."""
    pass


@stream.command()
@click.argument("source_name", type=click.STRING)
def list(source_name: str):  # noqa
    """List available streams for a source."""
    sources = discover_all_sources()
    source_model = sources.get(source_name)
    if not source_model:
        click.echo(f"Source {source_name} not found.")
        return

    click.echo(f"Available streams for {source_name}:")
    for stream in source_model.streams:
        stream_mode = "[Supports incremental]" if stream.supports_incremental else "[Full refresh only]"
        click.echo(f"{stream_mode} - {stream.name}")


@stream.command()
@click.argument("filename", type=click.Path(exists=True))
@click.option(
    "--env-file",
    required=False,
    type=click.Path(exists=True),
    help="Path to .env file to load environment variables from.",
)
@click.option("--cancel", is_flag=True, default=False, help="Cancel pending reset requests instead of adding one.")
@click.option(
    "--stream",
    "stream_name",
    required=False,
    help="Reset this stream instead of the one named in the config, for configs templated across streams.",
)
def reset(filename: str, env_file: str, cancel: bool, stream_name: str):
    """Request a reset of the incremental stream defined by a config file.

    The request is stored in the backend and consumed by the next run of that pipeline, so scheduled
    pipelines pick it up without any change to their command line. It is scoped to a single stream:
    resetting one stream never affects another, even under the same pipeline name.
    """

    if env_file:
        load_dotenv(env_file)
    else:
        load_dotenv(find_dotenv(".env"))

    config = resolve_config(parse_from_yaml(filename))

    if stream_name:
        config["source"]["stream"] = stream_name

    bizon_config = BizonConfig.model_validate(obj=config)

    if bizon_config.source.sync_mode != SourceSyncModes.INCREMENTAL:
        raise click.exceptions.ClickException(
            f"Only incremental streams can be reset, but sync_mode is '{bizon_config.source.sync_mode.value}'. "
            f"A '{bizon_config.source.sync_mode.value}' stream already rebuilds its destination on every run."
        )

    backend = BackendFactory.get_backend(config=bizon_config.engine.backend)
    backend.check_prerequisites()
    backend.create_all_tables()

    if bizon_config.engine.backend.type == BackendTypes.SQLITE:
        click.secho(
            "Warning: the sqlite backend is a local file, so this request is only visible to runs using the same file.",
            fg="yellow",
        )

    stream_label = f"{bizon_config.source.name} - {bizon_config.source.stream}"

    if cancel:
        cancelled = backend.cancel_pending_stream_resets(
            name=bizon_config.name,
            source_name=bizon_config.source.name,
            stream_name=bizon_config.source.stream,
        )

        if cancelled:
            click.secho(f"Cancelled {cancelled} pending reset request(s) for {stream_label}.", fg="green")
        else:
            click.echo(f"No pending reset request for {stream_label}.")
        return

    if backend.get_pending_stream_reset(
        name=bizon_config.name,
        source_name=bizon_config.source.name,
        stream_name=bizon_config.source.stream,
    ):
        click.echo(f"A reset is already pending for {stream_label}, nothing to do.")
        return

    # Nothing validates the stream name here (the source is never instantiated), so a typo — most
    # likely via --stream — would otherwise queue a reset that silently never fires.
    if not backend.get_last_successful_stream_job(
        name=bizon_config.name,
        source_name=bizon_config.source.name,
        stream_name=bizon_config.source.stream,
    ):
        click.secho(
            f"Warning: no previous successful run found for {stream_label}. Check the stream name — "
            f"a stream that has never run already fetches everything on its next run.",
            fg="yellow",
        )

    backend.create_stream_reset(
        name=bizon_config.name,
        source_name=bizon_config.source.name,
        stream_name=bizon_config.source.stream,
    )
    click.secho(
        f"Reset requested for {stream_label}. The next run will re-fetch the full stream, replace the "
        f"destination table, and resume incremental from there.",
        fg="green",
    )


# Create a 'destination' group under 'bizon'
@cli.group()
def destination():
    """Subcommands for handling destinations."""
    pass


# Create a 'secrets' group under 'bizon'
@cli.group()
def secrets():
    """Subcommands for handling secret/reference resolution."""
    pass


@secrets.command()
@click.argument("filename", type=click.Path(exists=True))
@click.option(
    "--env-file",
    required=False,
    type=click.Path(exists=True),
    help="Path to .env file to load environment variables from.",
)
def check(filename: str, env_file: str):
    """Dry-run all gsm:// / env:// references in a config and report (masked) results."""

    # Load environment variables from .env file (same as `run`)
    if env_file:
        load_dotenv(env_file)
    else:
        load_dotenv(find_dotenv(".env"))

    config = parse_from_yaml(filename)
    # Resolve legacy BIZON_ENV_ whole-value references first, like the real run does
    config = replace_env_variables_in_config(config=config)

    references = collect_references_in_config(config)
    if not references:
        click.echo("No gsm:// / env:// references found in config.")
        return

    registry = ResolverRegistry(settings=config.get("secrets") or {})

    path_width = max(len(path) for path, _ in references)
    ref_width = max(len(reference) for _, reference in references)
    failures = 0

    for path, reference in references:
        try:
            value = registry.resolve_reference(reference)
            status = click.style(f"✓  ({len(value)} chars)", fg="green")
        except ReferenceResolutionError as error:
            failures += 1
            status = click.style(f"✗  {error}", fg="red")
        click.echo(f"{path.ljust(path_width)}   {reference.ljust(ref_width)}   {status}")

    if failures:
        raise click.exceptions.ClickException(f"{failures} reference(s) failed to resolve.")
    click.secho(f"All {len(references)} reference(s) resolved.", fg="green")


@cli.command()
@click.argument("filename", type=click.Path(exists=True))
@click.option(
    "--custom-source",
    required=False,
    type=click.Path(exists=True),
    help="Custom Python file implementing a Bizon source.",
)
@click.option(
    "--runner",
    required=False,
    type=click.Choice(["thread", "process", "stream"]),
    default="thread",
    show_default=True,
    help="Runner type to use. Thread or Process.",
)
@click.option(
    "--log-level",
    required=False,
    type=click.Choice([level.name for level in LoggerLevel]),
    show_default=True,
    help="Log level to use.",
)
@click.option(
    "--env-file",
    required=False,
    type=click.Path(exists=True),
    help="Path to .env file to load environment variables from.",
)
@click.option(
    "--reset",
    is_flag=True,
    default=False,
    help="Reset the incremental stream: re-fetch it in full and replace the destination table, "
    "then resume incremental from this run.",
)
def run(
    filename: str,
    custom_source: str,
    runner: str,
    log_level: LoggerLevel,
    env_file: str,
    reset: bool,
    help="Run a bizon pipeline from a YAML file.",
):
    """Run a bizon pipeline from a YAML file."""

    # Load environment variables from .env file
    if env_file:
        load_dotenv(env_file)
    else:
        load_dotenv(find_dotenv(".env"))

    # Parse config from YAML file as a dictionary
    config = parse_from_yaml(filename)

    # Set debug mode
    set_log_level(config=config, level=log_level)

    # Override source_file_path param in config
    set_custom_source_path_in_config(config=config, custom_source=custom_source)

    # Override runner param in config
    set_runner_in_config(config=config, runner=runner)

    # Override reset param in config
    set_reset_in_config(config=config, reset=reset)

    runner = RunnerFactory.create_from_config_dict(config=config)
    result = runner.run()

    if result.is_success:
        click.secho("Pipeline finished successfully.", fg="green")

    else:
        raise click.exceptions.ClickException(result.to_string())


if __name__ == "__main__":
    cli()
