import uuid

from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ds_protocol_graphql_py_lib.dataset.graphql import (
    GraphqlDataset,
    GraphqlDatasetSettings,
    GraphqlReadSettings,
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

# GraphQL query with variables - Query countries by region using variables
dataset = GraphqlDataset(
    deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
    serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
    settings=GraphqlDatasetSettings(
        url="https://graphql.country/graphql",
        read=GraphqlReadSettings(
            query="""query GetCountriesByRegion($regionName: String!) {
                      countries(region: $regionName, first: 10) {
                        edges {
                          node {
                            name
                            alpha2Code
                            capital
                            area
                            population
                            region
                          }
                        }
                      }
                    }""",
            variables={"regionName": "Europe"},  # Filter by region using variable
            operation_name="GetCountriesByRegion",
        ),
    ),
    linked_service=linked_service,
    id=uuid.uuid4(),
    name="example::countries_dataset",
    version="1.0.0",
)

dataset.linked_service.connect()
result = dataset.read()
print("Example with graphql.country API - Countries in Europe region:")
print(dataset.output)
