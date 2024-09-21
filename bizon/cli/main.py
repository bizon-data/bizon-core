import os

import click

from bizon.cli.util import parse_from_yaml
from bizon.engine.runner import RunnerFactory
from bizon.source.discover import discover_all_sources


@click.group()
def cli():
    """Bizon CLI."""
    pass


# Create a 'destination' group under 'bizon'
@cli.group()
def source():
    """Subcommands for handling destinations."""
    pass


@source.command()
def list():
    """List available sources."""

    click.echo("Retrieving available sources...")
    sources = discover_all_sources()

    click.echo("Available sources:")
    for source_name, source_class in sources.items():
        if not source_class.available_streams:
            click.echo(
                f"{source_name} - NOT AVAILABLE, run 'pip install bizon[{source_name}]' to install missing dependencies."
            )
        else:
            click.echo(f"{source_name} - {source_class.available_streams}")


# Create a 'destination' group under 'bizon'
@cli.group()
def destination():
    """Subcommands for handling destinations."""
    pass


@cli.command()
@click.argument("filename", type=click.Path(exists=True))
@click.option(
    "--custom-source",
    required=False,
    type=click.Path(exists=True),
    help="Custom Python file implementing a Bizon source.",
)
@click.option(
    "--debug",
    required=False,
    is_flag=True,
    show_default=True,
    default=False,
    help="Enable debug mode.",
)
def run(filename: str, custom_source: str, debug, help="Run a bizon pipeline from a YAML file."):
    """Run a bizon pipeline from a YAML file."""
    ctx = click.get_current_context()

    # Set Log Level
    if debug:
        os.environ["LOGURU_LEVEL"] = "DEBUG"

    # Parse config from YAML file as a dictionary
    config = parse_from_yaml(filename)

    # Override source_file_path param in config
    custom_source = ctx.get_parameter_source("custom-source")
    if custom_source:
        config["source"]["source_file_path"] = custom_source

    runner = RunnerFactory.create_from_config_dict(config=config)
    runner.run()


if __name__ == "__main__":
    cli()
