"""
Example demonstrating REAL GraphQL delete() mutation execution.

This example uses a REAL public GraphQL API that supports mutations:
- GraphQL Zero API (https://graphqlzero.almansi.me/api)
- Free, no authentication required
- Supports deletePost mutation
- Actually works with real data!

This is a real working example!
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
    GraphqlDeleteSettings,
)


# Create a linked service for the REAL GraphQL Zero API
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
        delete=GraphqlDeleteSettings(
            # Real mutation from GraphQL Zero API
            mutation="mutation DeletePost($id: ID!) { deletePost(id: $id) }",
            # Column name matches GraphQL variable name
            identity_columns=["id"],
            operation_name="DeletePost",
        ),
    ),
    linked_service=linked_service,
    id=uuid.uuid4(),
    name="example::delete_posts_dataset",
    version="1.0.0",
)

dataset.input = pd.DataFrame(
    {
        "id": ["1"],
        "title": ["Post 1"],
    }
)

print("\nInput rows to delete:")
print(dataset.input)

# Connect and execute the delete operation
print("\nConnecting to GraphQL Zero API...")
linked_service.connect()

print("Executing delete()...")
dataset.delete()

print("\nDeleted rows (from self.output):")
print(dataset.output)
