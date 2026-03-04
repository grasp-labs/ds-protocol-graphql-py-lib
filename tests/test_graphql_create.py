"""Test GraphQL Dataset create() method implementation."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, PropertyMock, patch

import pandas as pd
import pytest
from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType
from ds_resource_plugin_py_lib.common.resource.dataset.errors import CreateError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    ConnectionError,
)
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ds_protocol_graphql_py_lib.dataset.graphql import (
    GraphqlCreateSettings,
    GraphqlDataset,
    GraphqlDatasetSettings,
)


def test_create_returns_none():
    """Test that create() returns None per DATASET_CONTRACT."""
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
            create=GraphqlCreateSettings(
                mutation="mutation CreatePost($input: CreatePostInput!) { createPost(input: $input) { id } }",
                input_field="input",
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"createPost": {"id": "1"}}}
    mock_connection.session.post.return_value = mock_response

    dataset.input = pd.DataFrame({"title": ["Test"], "body": ["Test body"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        result = dataset.create()
        assert result is None


def test_create_populates_output():
    """Test that create() populates self.output with created rows."""
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
            create=GraphqlCreateSettings(
                mutation="mutation CreatePost($input: CreatePostInput!) { createPost(input: $input) { id title body } }",
                input_field="input",
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "data": {
            "createPost": [
                {"id": "1", "title": "Post 1", "body": "Body 1"},
                {"id": "2", "title": "Post 2", "body": "Body 2"},
            ]
        }
    }
    mock_connection.session.post.return_value = mock_response

    dataset.input = pd.DataFrame({"title": ["Post 1", "Post 2"], "body": ["Body 1", "Body 2"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.create()

    assert isinstance(dataset.output, pd.DataFrame)
    assert len(dataset.output) == 2
    assert list(dataset.output["title"]) == ["Post 1", "Post 2"]


def test_create_empty_input_is_noop():
    """Test that create() with empty input is a no-op."""
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
            create=GraphqlCreateSettings(
                mutation="mutation CreatePost($input: CreatePostInput!) { createPost(input: $input) { id } }",
                input_field="input",
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()

    dataset.input = pd.DataFrame()

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.create()

    mock_connection.session.post.assert_not_called()
    assert len(dataset.output) == 0


def test_create_no_connection_raises_error():
    """Test that create() raises ConnectionError if connection is not initialized."""
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
            create=GraphqlCreateSettings(
                mutation="mutation CreatePost($input: CreatePostInput!) { createPost(input: $input) { id } }",
                input_field="input",
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    dataset.input = pd.DataFrame({"title": ["Test"]})

    with pytest.raises(ConnectionError):
        dataset.create()


def test_create_missing_settings_raises_error():
    """Test that create() raises error when create settings are not configured."""
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

    dataset.input = pd.DataFrame({"title": ["Test"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        with pytest.raises(CreateError, match="create settings must be provided"):
            dataset.create()


def test_create_graphql_error_raises_create_error():
    """Test that GraphQL errors in response raise CreateError."""
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
            create=GraphqlCreateSettings(
                mutation="mutation CreatePost($input: CreatePostInput!) { createPost(input: $input) { id } }",
                input_field="input",
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()

    error_response = {"errors": [{"message": "Invalid input"}]}

    mock_response = MagicMock()
    mock_response.json.return_value = error_response
    mock_connection.session.post.return_value = mock_response

    dataset.input = pd.DataFrame({"title": ["Test"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        with pytest.raises(CreateError, match="GraphQL create mutation failed"):
            dataset.create()
