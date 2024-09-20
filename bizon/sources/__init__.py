import ast
import importlib
import importlib.util
import os
import traceback
from typing import Any, List, Mapping, Type

from loguru import logger
from pydantic import BaseModel

from bizon.source.source import Source


class Stream(BaseModel):
    name: str
    source_class: Type[Source]


class SourceModel(BaseModel):
    name: str
    streams: List[Stream]

    @property
    def available_streams(self) -> List[str]:
        return [stream.name for stream in self.streams]

    def get_stream_by_name(self, name: str) -> Stream:
        for stream in self.streams:
            if stream.name == name:
                return stream
        raise ValueError(f"Stream {name} not found. Available streams are {self.available_streams}")


def find_inherited_classes(file_path, parent_class_name):
    # Open the file and parse its content using ast
    with open(file_path, "r") as file:
        tree = ast.parse(file.read())

    # List to store classes that inherit from the given class name
    inherited_classes = []

    # Walk through the AST nodes
    for node in ast.walk(tree):
        # Check if the node is a class definition
        if isinstance(node, ast.ClassDef):
            # Check if the class inherits from the parent_class_name
            for base in node.bases:
                if isinstance(base, ast.Name) and base.id == parent_class_name:
                    inherited_classes.append(node.name)

    return inherited_classes


def find_all_source_paths(source_dir_name: str = "src"):
    """Find all source code paths in the sources directory
    Return a dict mapping source_name to source_code_path
    Like {'gsheets': '/path/to/bizon/sources/gsheets/src'}
    """

    discovered_sources_paths = {}
    base_dir = os.path.abspath(os.path.dirname(__file__))

    for source_name in os.listdir(base_dir):

        # First check that it's a dir and contains a dir called src
        if os.path.isdir(os.path.join(base_dir, source_name)) and os.path.exists(
            os.path.join(base_dir, source_name, source_dir_name)
        ):

            # Store path to source code
            source_code_path = os.path.join(base_dir, source_name, source_dir_name)
            discovered_sources_paths[source_name] = source_code_path

    return discovered_sources_paths


def get_sources_in_path(source_name: str, source_code_path: str) -> SourceModel:
    """Get all sources in a given path
    Return a SourceModel object with the source name and its streams
    """

    streams = []

    # Iterate all files in the source code path recursively
    for root, _, files in os.walk(source_code_path):

        for file in files:

            # Check if the file is a python file
            if file.endswith(".py"):

                # Find all classes that inherit from Source
                source_classes_name = find_inherited_classes(
                    file_path=os.path.join(root, file), parent_class_name="Source"
                )

                # If classes are found
                if len(source_classes_name) > 0:

                    # Get the relative path of the file within bizon module
                    relative_path: str = os.path.join(os.path.relpath(root), file)

                    logger.debug(
                        f"Found class {','.join(source_classes_name)} for {source_name} with path: {relative_path}"
                    )

                    for source_class_name in source_classes_name:

                        # Transform the relative path to a python import path and import the module
                        python_import_path = relative_path.replace("/", ".").replace(".py", "")
                        source_module = importlib.import_module(python_import_path)
                        source_class: Type = getattr(source_module, source_class_name)

                        # Check if the class has a streams method
                        if hasattr(source_class, "streams") and callable(source_class.streams):
                            for stream in source_class.streams():
                                streams.append(
                                    Stream(
                                        name=stream,
                                        source_class=source_class,
                                    )
                                )
                        else:
                            logger.warning(
                                f"Class {source_class_name} in {relative_path} does not have a streams method."
                                "Please add a streams method to the class to be able to use it."
                            )

    if len(streams) == 0:
        logger.error(f"No streams found for {source_name}. Please add a stream to the source to be able to use it.")
        raise ValueError(f"No streams found for {source_name}")

    return SourceModel(name=source_name, streams=streams)


def get_internal_source_class_by_source_and_stream(source_name: str, stream_name: str) -> Type[Source]:
    """Get the source class by source and stream name"""

    discovered_sources_paths_dict = find_all_source_paths()

    if source_name not in discovered_sources_paths_dict.keys():
        raise ValueError(
            f"Source {source_name} not found. Available sources are {discovered_sources_paths_dict.keys()}"
        )

    source_code_path = discovered_sources_paths_dict[source_name]

    source_model = get_sources_in_path(source_name=source_name, source_code_path=source_code_path)

    if stream_name not in source_model.available_streams:
        raise ValueError(f"Stream {stream_name} not found. Available streams are {source_model.available_streams}")

    stream = source_model.get_stream_by_name(name=stream_name)

    return stream.source_class


def discover_all_sources() -> Mapping[str, SourceModel]:
    """Discover all sources in the sources directory
    Return a dict mapping source_name to SourceModel object
    """

    discovered_sources = {}

    discovered_sources_paths = find_all_source_paths()

    # Iterate all source code paths found
    for source_name, source_code_path in discovered_sources_paths.items():
        try:
            source_model = get_sources_in_path(source_name=source_name, source_code_path=source_code_path)
            discovered_sources[source_name] = source_model
        except Exception as e:
            logger.error(f"Error while discovering source {source_name}: {e}")
            logger.error(traceback.format_exc())

    return discovered_sources


def get_source_instance_by_source_and_stream(
    source_name: str, stream_name: str, source_config: Mapping[str, Any]
) -> Source:
    """Get an instance of the source by source and stream name"""

    source_class: Source = get_internal_source_class_by_source_and_stream(
        source_name=source_name, stream_name=stream_name
    )

    config_class = source_class.get_config_class()
    config_parsed = config_class.model_validate(source_config)

    return source_class(config=config_parsed)


__all__ = [
    "get_internal_source_class_by_source_and_stream",
    "get_source_instance_by_source_and_stream",
    "discover_all_sources",
]
