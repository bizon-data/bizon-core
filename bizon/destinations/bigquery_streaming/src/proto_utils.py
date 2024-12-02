from typing import List, Tuple, Type

from google.cloud.bigquery_storage_v1.types import ProtoSchema
from google.protobuf.descriptor_pb2 import (
    DescriptorProto,
    FieldDescriptorProto,
    FileDescriptorProto,
)
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.message import Message
from google.protobuf.message_factory import GetMessageClassesForFiles


def get_proto_schema_and_class(clustering_keys: List[str] = None) -> Tuple[ProtoSchema, Type[Message]]:
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
        {"name": "_bizon_id", "type": FieldDescriptorProto.TYPE_STRING, "label": FieldDescriptorProto.LABEL_REQUIRED},
        {
            "name": "_bizon_extracted_at",
            "type": FieldDescriptorProto.TYPE_STRING,
            "label": FieldDescriptorProto.LABEL_REQUIRED,
        },
        {
            "name": "_bizon_loaded_at",
            "type": FieldDescriptorProto.TYPE_STRING,
            "label": FieldDescriptorProto.LABEL_REQUIRED,
        },
        {
            "name": "_source_record_id",
            "type": FieldDescriptorProto.TYPE_STRING,
            "label": FieldDescriptorProto.LABEL_REQUIRED,
        },
        {
            "name": "_source_timestamp",
            "type": FieldDescriptorProto.TYPE_STRING,
            "label": FieldDescriptorProto.LABEL_REQUIRED,
        },
        {
            "name": "_source_data",
            "type": FieldDescriptorProto.TYPE_STRING,
            "label": FieldDescriptorProto.LABEL_OPTIONAL,
        },
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
