"""
**File:** ``__init__.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset``

Package initialization for GraphQL dataset module.

Example:
    >>> from ds_protocol_graphql_py_lib.dataset import GraphqlDataset, GraphqlDatasetSettings
    >>> from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
    >>> linked_service = HttpLinkedService(
    ...     settings=HttpLinkedServiceSettings(host="https://api.example.graphql/graphql"),
    ...     id="service-id",
    ...     name="graphql_service",
    ...     version="1.0.0",
    ... )
    >>> dataset = GraphqlDataset(
    ...     linked_service=linked_service,
    ...     settings=GraphqlDatasetSettings(url="https://api.example.graphql/graphql"),
    ...     id="dataset-id",
    ...     name="graphql_dataset",
    ...     version="1.0.0",
    ... )
"""

from .graphql import GraphqlDataset, GraphqlDatasetSettings, GraphqlReadSettings

__all__ = [
    "GraphqlDataset",
    "GraphqlDatasetSettings",
    "GraphqlReadSettings",
]
