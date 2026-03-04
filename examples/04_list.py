import uuid

from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ds_protocol_graphql_py_lib.dataset.graphql import (
    GraphqlDataset,
    GraphqlDatasetSettings,
)

# Create a linked service for the graphql.country API
linked_service = HttpLinkedService(
    settings=HttpLinkedServiceSettings(
        host="https://graphql.country/",
        auth_type=AuthType.NO_AUTH,
    ),
    id=uuid.uuid4(),
    name="example::countries_linked_service",
    version="1.0.0",
)

# Create a GraphQL dataset and list available queries
dataset = GraphqlDataset(
    settings=GraphqlDatasetSettings(
        url="https://graphql.country/graphql",
    ),
    linked_service=linked_service,
    id=uuid.uuid4(),
    name="example::list_queries_dataset",
    version="1.0.0",
)

# Connect and list available queries via introspection
dataset.linked_service.connect()
dataset.list()

print("Available GraphQL Queries in graphql.country API:")
print(dataset.output)
print("\nDataFrame info:")
print(dataset.output.info())
