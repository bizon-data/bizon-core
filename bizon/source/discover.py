import ast
import importlib
import importlib.util
import inspect
import os
import traceback
from typing import Any, List, Mapping, Type

from loguru import logger
from pydantic import BaseModel

from bizon.source.source import AbstractSource
from bizon.utils import BIZON_ABSOLUTE_PATH


class Stream(BaseModel):
    name: str
    source_class: Type[AbstractSource]
    supports_incremental: bool


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


def find_inherited_classes(file_path):
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
                if isinstance(base, ast.Name):
                    inherited_classes.append(node.name)

    return inherited_classes


def get_python_import_path(relative_path: str) -> str:
    """Transform a relative path to a python import path"""
    python_path = relative_path.replace("/", ".").replace(".py", "")
    # Find the index where the desired substring starts
    start_index = python_path.find("bizon.sources.")

    # Extract the substring from that point onwards
    if start_index != -1:
        return python_path[start_index:]
    raise ValueError("Substring not found.")


def find_all_source_paths(source_dir_name: str = "src") -> Mapping[str, str]:
    """Find all source code paths in the sources directory
    Return a dict mapping source_name to source_code_path
    Like {'gsheets': '/path/to/bizon/sources/gsheets/src'}
    """

    discovered_sources_paths = {}

    # Leading to the bizon internal sources directory
    base_dir = os.path.join(BIZON_ABSOLUTE_PATH, "sources")

    for source_name in os.listdir(base_dir):

        # First check that it's a dir and contains a dir called src
        if os.path.isdir(os.path.join(base_dir, source_name)) and os.path.exists(
            os.path.join(base_dir, source_name, source_dir_name)
        ):
            # Store path to source code
            source_code_path = os.path.join(base_dir, source_name, source_dir_name)
            discovered_sources_paths[source_name] = source_code_path

    return discovered_sources_paths


def is_class_a_source(source_class: Type, path: str) -> bool:
    """Check if a class is a source"""

    source_class_name = source_class.__name__

    # Get the source class from the module
    if inspect.isabstract(source_class):
        logger.debug(f"Class {source_class_name} in {path} is abstract. Skipping.")
        return False

    if not issubclass(source_class, AbstractSource):
        logger.debug(f"Class {source_class_name} in {path} does not inherit from AbstractSource. Skipping.")
        return False

    # Check if the class has a streams method
    if not hasattr(source_class, "streams") or not callable(source_class.streams):
        logger.warning(
            f"Class {source_class_name} in {path} does not have a streams method."
            "Please add a streams method to the class to be able to use it."
        )
        return False

    if len(source_class.streams()) == 0:
        logger.warning(
            f"Class {source_class_name} in {path} has no streams defined."
            "Please add at least one stream to the class to be able to use it."
        )
        return False

    return True


def is_source_class_implementing_incremental(source_class: Type[AbstractSource]) -> bool:
    """Check if a source class implements incremental"""
    source_code = inspect.getsource(source_class.get_records_updated_after).strip()
    supports_incremental = not source_code.endswith("pass")
    return supports_incremental


def parse_streams_from_filepath(source_name: str, filepath: str, skip_unavailable_sources: bool) -> List[Stream]:

    streams = []

    # Find all classes that inherit from AbstractSource
    source_classes_name = find_inherited_classes(file_path=filepath)

    relative_path: str = os.path.relpath(filepath)

    # If classes are found
    for source_class_name in source_classes_name:

        # Transform the relative path to a python import path and import the module
        python_import_path = get_python_import_path(relative_path)
        logger.info(f"Importing {python_import_path}")

        try:
            source_module = importlib.import_module(python_import_path, package="sources")
        except ImportError as e:
            if not skip_unavailable_sources:
                raise e
            else:
                logger.warning(
                    f"{source_name} is not available, run 'pip install bizon[{source_name}]' to install missing dependencies."
                )
            break
        except Exception as e:
            logger.error(f"Error while importing {python_import_path}: {e}")
            logger.error(traceback.format_exc())
            break

        source_class = getattr(source_module, source_class_name)

        # Get the source class from the module
        if is_class_a_source(source_class=source_class, path=relative_path):
            for stream in source_class.streams():
                streams.append(
                    Stream(
                        name=stream,
                        source_class=source_class,
                        supports_incremental=is_source_class_implementing_incremental(source_class),
                    )
                )

    return streams


def get_sources_in_path(source_name: str, source_code_path: str, skip_unavailable_sources: bool = True) -> SourceModel:
    """Get all sources in a given path
    Return a SourceModel object with the source name and its streams
    """

    streams = []

    # Iterate all files in the source code path recursively
    for root, _, files in os.walk(source_code_path):
        for file in files:
            if file.endswith(".py"):
                streams.extend(
                    parse_streams_from_filepath(
                        source_name=source_name,
                        filepath=os.path.join(root, file),
                        skip_unavailable_sources=skip_unavailable_sources,
                    )
                )

    return SourceModel(name=source_name, streams=streams)


def get_internal_source_class_by_source_and_stream(source_name: str, stream_name: str) -> Type[AbstractSource]:
    """Get the source class by source and stream name"""

    discovered_sources_paths_dict = find_all_source_paths()

    if source_name not in discovered_sources_paths_dict.keys():
        raise ValueError(
            f"Source {source_name} not found. Available sources are {discovered_sources_paths_dict.keys()}"
        )

    source_code_path = discovered_sources_paths_dict[source_name]

    source_model = get_sources_in_path(
        source_name=source_name, source_code_path=source_code_path, skip_unavailable_sources=False
    )

    if len(source_model.streams) == 0:
        raise ValueError(
            f"No streams found for source {source_name}, ensure you installed required dependencies with pip install bizon[{source_name}] if needed."
            "You should not have any warning related to your source when running 'bizon source list'."
        )

    if stream_name not in source_model.available_streams:
        raise ValueError(f"Stream {stream_name} not found. Available streams are {source_model.available_streams}")

    stream = source_model.get_stream_by_name(name=stream_name)

    return stream.source_class


def get_external_source_class_by_source_and_stream(
    source_name: str, stream_name: str, filepath: str
) -> Type[AbstractSource]:
    """Get the source class by source and stream name"""

    if not os.path.exists(filepath):
        raise ValueError(f"File {filepath} not found.")

    if not filepath.endswith(".py"):
        raise ValueError(f"File {filepath} is not a python file. File extension should be '.py'")

    # Find  class that inherit from AbstractSource
    class_names = find_inherited_classes(file_path=filepath)

    stream_names = []

    for class_name in class_names:

        # Extract the module name from the file (remove directory and extension)
        module_name = os.path.splitext(os.path.basename(filepath))[0]

        # Create module spec from the file
        spec = importlib.util.spec_from_file_location(module_name, filepath)

        # Create a module from the spec
        module = importlib.util.module_from_spec(spec)

        # Execute the module (this loads the module into memory)
        spec.loader.exec_module(module)

        class_to_test = getattr(module, class_name)

        if is_class_a_source(class_to_test, filepath):
            if stream_name in class_to_test.streams():
                return class_to_test

            stream_names.extend(class_to_test.streams())

    raise ValueError(f"Stream {stream_name} not found. Available streams are {stream_names}")


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
) -> AbstractSource:
    """Get an instance of the source by source and stream name"""

    if source_config.get("source_file_path"):
        source_class: AbstractSource = get_external_source_class_by_source_and_stream(
            source_name=source_name,
            stream_name=stream_name,
            filepath=source_config["source_file_path"],
        )
    else:
        source_class: AbstractSource = get_internal_source_class_by_source_and_stream(
            source_name=source_name, stream_name=stream_name
        )

    config_class = source_class.get_config_class()
    config_parsed = config_class.model_validate(source_config)

    return source_class(config=config_parsed)
