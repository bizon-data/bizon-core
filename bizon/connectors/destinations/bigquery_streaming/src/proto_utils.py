from typing import List, Tuple, Type

from google.cloud.bigquery import SchemaField
from google.cloud.bigquery_storage_v1.types import ProtoSchema
from google.protobuf.descriptor_pb2 import (
    DescriptorProto,
    FieldDescriptorProto,
    FileDescriptorProto,
)
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.message import Message
from google.protobuf.message_factory import GetMessageClassesForFiles


def map_bq_type_to_field_descriptor(bq_type: str) -> int:
    """Map BigQuery type to Protobuf FieldDescriptorProto type."""
    type_map = {
        "STRING": FieldDescriptorProto.TYPE_STRING,  # STRING -> TYPE_STRING
        "BYTES": FieldDescriptorProto.TYPE_BYTES,  # BYTES -> TYPE_BYTES
        "INTEGER": FieldDescriptorProto.TYPE_INT64,  # INTEGER -> TYPE_INT64
        "FLOAT": FieldDescriptorProto.TYPE_DOUBLE,  # FLOAT -> TYPE_DOUBLE
        "NUMERIC": FieldDescriptorProto.TYPE_STRING,  # NUMERIC -> TYPE_STRING (use string to handle precision)
        "BIGNUMERIC": FieldDescriptorProto.TYPE_STRING,  # BIGNUMERIC -> TYPE_STRING
        "BOOLEAN": FieldDescriptorProto.TYPE_BOOL,  # BOOLEAN -> TYPE_BOOL
        "DATE": FieldDescriptorProto.TYPE_STRING,  # DATE -> TYPE_STRING
        "DATETIME": FieldDescriptorProto.TYPE_STRING,  # DATETIME -> TYPE_STRING
        "TIME": FieldDescriptorProto.TYPE_STRING,  # TIME -> TYPE_STRING
        "TIMESTAMP": FieldDescriptorProto.TYPE_STRING,  # TIMESTAMP -> TYPE_INT64 (Unix epoch time)
        "RECORD": FieldDescriptorProto.TYPE_MESSAGE,  # RECORD -> TYPE_MESSAGE (nested message)
    }

    return type_map.get(bq_type, FieldDescriptorProto.TYPE_STRING)  # Default to TYPE_STRING


def get_proto_schema_and_class(
    bq_schema: List[SchemaField], clustering_keys: List[str] = None
) -> Tuple[ProtoSchema, Type[Message]]:
    """Generate a ProtoSchema and a TableRow class for unnested BigQuery schema."""
    # Define the FileDescriptorProto
    file_descriptor_proto = FileDescriptorProto()
    file_descriptor_proto.name = "dynamic.proto"
    file_descriptor_proto.package = "dynamic_package"

    # Define the TableRow message schema
    message_descriptor = DescriptorProto()
    message_descriptor.name = "TableRow"

    # Add fields to the message, only use TYPE_STRING, BigQuery does not support other types
    # It does not imapact data types in final table

    # https://stackoverflow.com/questions/70489919/protobuf-type-for-bigquery-timestamp-field
    fields = [
        {
            "name": col.name,
            "type": map_bq_type_to_field_descriptor(col.field_type),
            "label": (
                FieldDescriptorProto.LABEL_REQUIRED if col.mode == "REQUIRED" else FieldDescriptorProto.LABEL_OPTIONAL
            ),
        }
        for col in bq_schema
    ]

    if clustering_keys:
        for key in clustering_keys:
            fields.append(
                {
                    "name": key,
                    "type": FieldDescriptorProto.TYPE_STRING,
                    "label": FieldDescriptorProto.LABEL_OPTIONAL,
                }
            )

    for i, field in enumerate(fields, start=1):
        field_descriptor = message_descriptor.field.add()
        field_descriptor.name = field["name"]
        field_descriptor.number = i
        field_descriptor.type = field["type"]
        field_descriptor.label = field["label"]

    # Add the message to the file descriptor
    file_descriptor_proto.message_type.add().CopyFrom(message_descriptor)

    # Create a DescriptorPool and register the FileDescriptorProto
    pool = DescriptorPool()
    pool.Add(file_descriptor_proto)

    # Use the registered file name to fetch the message classes
    message_classes = GetMessageClassesForFiles(["dynamic.proto"], pool=pool)

    # Fetch the TableRow class
    table_row_class = message_classes["dynamic_package.TableRow"]

    # Create the ProtoSchema
    proto_schema = ProtoSchema()
    proto_schema.proto_descriptor.CopyFrom(message_descriptor)

    return proto_schema, table_row_class
