"""
**File:** ``test_graphql_list.py``
**Region:** ``tests/dataset``

Test GraphQL Dataset list() method implementation.

Covers:
    GraphQL dataset listing functionality including query execution,
    variable handling, operation names, batch mode operations, pagination
    support, and error handling for connection failures and invalid queries.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, PropertyMock, patch

import pandas as pd
import pytest
from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType
from ds_resource_plugin_py_lib.common.resource.dataset.errors import ListError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    ConnectionError,
)
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ds_protocol_graphql_py_lib.dataset.graphql import (
    GraphqlDataset,
    GraphqlDatasetSettings,
)


def test_list_returns_none():
    """Test that list() returns None per DATASET_CONTRACT."""
    linked_service = HttpLinkedService(
        settings=HttpLinkedServiceSettings(
            host="https://example.graphql.api/",
            auth_type=AuthType.NO_AUTH,
        ),
        id=uuid.uuid4(),
        name="test_linked_service",
        version="1.0.0",
    )

    dataset = GraphqlDataset(
        deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
        serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"__schema": {"queryType": {"fields": []}}}}
    mock_connection.session.post.return_value = mock_response

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        result = dataset.list()
        assert result is None


def test_list_populates_output():
    """Test that list() populates self.output as a DataFrame."""
    linked_service = HttpLinkedService(
        settings=HttpLinkedServiceSettings(
            host="https://example.graphql.api/",
            auth_type=AuthType.NO_AUTH,
        ),
        id=uuid.uuid4(),
        name="test_linked_service",
        version="1.0.0",
    )

    dataset = GraphqlDataset(
        deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
        serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    introspection_response = {
        "data": {
            "__schema": {
                "queryType": {
                    "fields": [
                        {
                            "name": "countries",
                            "description": "Query countries",
                            "args": [
                                {
                                    "name": "region",
                                    "type": {"name": "String", "kind": "SCALAR"},
                                },
                                {
                                    "name": "first",
                                    "type": {"name": "Int", "kind": "SCALAR"},
                                },
                            ],
                        },
                    ]
                }
            }
        }
    }

    mock_response = MagicMock()
    mock_response.json.return_value = introspection_response
    mock_connection.session.post.return_value = mock_response

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.list()

    assert isinstance(dataset.output, pd.DataFrame)
    assert len(dataset.output) == 1
    assert dataset.output.iloc[0]["name"] == "countries"


def test_list_no_connection_raises_error():
    """Test that list() raises ConnectionError if connection is not initialized."""
    linked_service = HttpLinkedService(
        settings=HttpLinkedServiceSettings(
            host="https://example.graphql.api/",
            auth_type=AuthType.NO_AUTH,
        ),
        id=uuid.uuid4(),
        name="test_linked_service",
        version="1.0.0",
    )

    dataset = GraphqlDataset(
        deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
        serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    with pytest.raises(ConnectionError):
        dataset.list()


def test_list_graphql_error_raises_list_error():
    """Test that GraphQL errors in introspection response raise ListError."""
    linked_service = HttpLinkedService(
        settings=HttpLinkedServiceSettings(
            host="https://example.graphql.api/",
            auth_type=AuthType.NO_AUTH,
        ),
        id=uuid.uuid4(),
        name="test_linked_service",
        version="1.0.0",
    )

    dataset = GraphqlDataset(
        deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
        serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {"errors": [{"message": "Schema unavailable"}]}
    mock_connection.session.post.return_value = mock_response

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        with pytest.raises(ListError, match="GraphQL introspection query failed"):
            dataset.list()


def test_list_exception_wrapped_in_list_error():
    """Test that generic exceptions are wrapped in ListError."""
    linked_service = HttpLinkedService(
        settings=HttpLinkedServiceSettings(
            host="https://example.graphql.api/",
            auth_type=AuthType.NO_AUTH,
        ),
        id=uuid.uuid4(),
        name="test_linked_service",
        version="1.0.0",
    )

    dataset = GraphqlDataset(
        deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
        serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_connection.session.post.side_effect = ValueError("Network error")

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        with pytest.raises(ListError, match="Failed to list GraphQL schema resources"):
            dataset.list()


def test_list_multiple_fields():
    """Test that list() handles multiple schema fields."""
    linked_service = HttpLinkedService(
        settings=HttpLinkedServiceSettings(
            host="https://example.graphql.api/",
            auth_type=AuthType.NO_AUTH,
        ),
        id=uuid.uuid4(),
        name="test_linked_service",
        version="1.0.0",
    )

    dataset = GraphqlDataset(
        deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
        serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    introspection_response = {
        "data": {
            "__schema": {
                "queryType": {
                    "fields": [
                        {
                            "name": "users",
                            "description": "Get all users",
                            "args": [{"name": "limit", "type": {"name": "Int", "kind": "SCALAR"}}],
                        },
                        {
                            "name": "posts",
                            "description": None,
                            "args": [],
                        },
                        {
                            "name": "comments",
                            "description": "Get comments",
                            "args": [
                                {"name": "postId", "type": {"name": "ID", "kind": "SCALAR"}},
                                {"name": "limit", "type": {"name": "Int", "kind": "SCALAR"}},
                            ],
                        },
                    ]
                }
            }
        }
    }

    mock_response = MagicMock()
    mock_response.json.return_value = introspection_response
    mock_connection.session.post.return_value = mock_response

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.list()

    assert isinstance(dataset.output, pd.DataFrame)
    assert len(dataset.output) == 3
    assert list(dataset.output["name"]) == ["users", "posts", "comments"]
    assert dataset.output.iloc[0]["arg_count"] == 1
    assert dataset.output.iloc[1]["arg_count"] == 0
    assert dataset.output.iloc[2]["arg_count"] == 2


def test_list_fields_with_no_args():
    """Test that list() handles fields with no arguments."""
    linked_service = HttpLinkedService(
        settings=HttpLinkedServiceSettings(
            host="https://example.graphql.api/",
            auth_type=AuthType.NO_AUTH,
        ),
        id=uuid.uuid4(),
        name="test_linked_service",
        version="1.0.0",
    )

    dataset = GraphqlDataset(
        deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
        serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    introspection_response = {
        "data": {
            "__schema": {
                "queryType": {
                    "fields": [
                        {
                            "name": "allPosts",
                            "description": "Get all posts",
                            "args": [],
                        },
                    ]
                }
            }
        }
    }

    mock_response = MagicMock()
    mock_response.json.return_value = introspection_response
    mock_connection.session.post.return_value = mock_response

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.list()

    assert dataset.output.iloc[0]["arguments"] is None
    assert dataset.output.iloc[0]["arg_count"] == 0
