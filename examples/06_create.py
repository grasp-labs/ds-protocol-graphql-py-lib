"""
Example demonstrating REAL GraphQL create() mutation execution.

This example uses GraphQL Zero API (https://graphqlzero.almansi.me/api)
which supports create mutations.

Based on the working pattern from the attached example.
"""

import uuid

import pandas as pd

from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ds_protocol_graphql_py_lib.dataset.graphql import (
    GraphqlDataset,
    GraphqlDatasetSettings,
    GraphqlCreateSettings,
)

linked_service = HttpLinkedService(
    settings=HttpLinkedServiceSettings(
        host="https://graphqlzero.almansi.me/",
        auth_type=AuthType.NO_AUTH,
    ),
    id=uuid.uuid4(),
    name="example::graphql_zero_api",
    version="1.0.0",
)

dataset = GraphqlDataset(
    deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
    serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
    settings=GraphqlDatasetSettings(
        url="https://graphqlzero.almansi.me/api",
        create=GraphqlCreateSettings(
            # Real mutation from GraphQL Zero API (single line)
            mutation="mutation CreatePost($input: CreatePostInput!) { createPost(input: $input) { id title body } }",
            # The field name for the input variable
            input_field="input",
            operation_name="CreatePost",
        ),
    ),
    linked_service=linked_service,
    id=uuid.uuid4(),
    name="example::create_posts_dataset",
    version="1.0.0",
)

# Prepare rows to create
dataset.input = pd.DataFrame(
    {
        "title": ["My Test Post 1"],
        "body": [
            "This is my first post created via GraphQL",
        ],
    }
)

print("\nInput rows to create:")
print(dataset.input)

# Connect and execute the create operation
print("\nConnecting to GraphQL Zero API...")
linked_service.connect()

print("Executing create()...")
dataset.create()

print("\nCreated rows (from self.output):")
print(dataset.output)
