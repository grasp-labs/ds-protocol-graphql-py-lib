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

linked_service = HttpLinkedService(
    settings=HttpLinkedServiceSettings(
        host="https://graphql.org/graphql/",
        auth_type=AuthType.NO_AUTH,
    ),
    id=uuid.uuid4(),
    name="example::linked_service",
    version="1.0.0",
)

# Simple GraphQL introspection query
dataset = GraphqlDataset(
    deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
    serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
    settings=GraphqlDatasetSettings(
        url="https://graphql.org/graphql/",
        read=GraphqlReadSettings(
            query='{ __type(name: "Root") { fields { name } } }',
        ),
    ),
    linked_service=linked_service,
    id=uuid.uuid4(),
    name="example::dataset",
    version="1.0.0",
)

dataset.linked_service.connect()
dataset.read()
print(dataset.output)
