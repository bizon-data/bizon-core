import click

from bizon.cli.util import parse_from_yaml
from bizon.engine.runner import RunnerFactory
from bizon.sources import discover_all_sources


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
                f"{source_name} - NOT AVAILABLE, run 'pip install bizon[gsheets]' to install missing dependencies."
            )
        else:
            click.echo(f"{source_name} - {source_class.available_streams}")


# Create a 'destination' group under 'bizon'
# @cli.group()
# def destination():
#     """Subcommands for handling destinations."""
#     pass


@cli.command()
@click.argument("filename", type=click.Path(exists=True))
@click.option(
    "--custom-source",
    required=False,
    type=click.Path(exists=True),
    help="Custom Python file implementing a Bizon source.",
)
def run(filename: str, custom_source: str, help="Run a bizon pipeline from a YAML file."):
    """Run a bizon pipeline from a YAML file."""
    ctx = click.get_current_context()
    custom_source = ctx.get_parameter_source("custom-source")
    if custom_source:
        # TODO Load the custom source
        pass
    config = parse_from_yaml(filename)
    runner = RunnerFactory.create_from_config_dict(config=config)
    runner.run()


if __name__ == "__main__":
    cli()
