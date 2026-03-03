"""Test GraphQL Dataset list() method implementation."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, PropertyMock, patch

import pandas as pd
import pytest
from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType
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
