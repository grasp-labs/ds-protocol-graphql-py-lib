"""
**File:** ``test_graphql_delete.py``
**Region:** ``tests/dataset``

Test GraphQL Dataset delete() method implementation.

Covers:
    GraphQL dataset deletion functionality including mutation execution,
    identity column handling, batch mode operations, variable passing,
    operation names, and error handling for connection failures and
    invalid mutations.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, PropertyMock, patch

import pandas as pd
import pytest
from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType
from ds_resource_plugin_py_lib.common.resource.dataset.errors import DeleteError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    ConnectionError,
)
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ds_protocol_graphql_py_lib.dataset.graphql import (
    GraphqlDataset,
    GraphqlDatasetSettings,
    GraphqlDeleteSettings,
)


def test_delete_returns_none():
    """Test that delete() returns None per DATASET_CONTRACT."""
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
            delete=GraphqlDeleteSettings(
                mutation="query DeletePost($id: ID!) { deletePost(id: $id) }",
                identity_columns=["id"],
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"deletePost": True}}
    mock_connection.session.post.return_value = mock_response

    dataset.input = pd.DataFrame({"id": [1], "name": ["Test"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        result = dataset.delete()
        assert result is None


def test_delete_populates_output():
    """Test that delete() populates self.output with deleted rows."""
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
            delete=GraphqlDeleteSettings(
                mutation="query DeletePost($id: ID!) { deletePost(id: $id) { id name } }",
                identity_columns=["id"],
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
            "deletePost": [
                {"id": 1, "name": "A"},
                {"id": 2, "name": "B"},
            ]
        }
    }
    mock_connection.session.post.return_value = mock_response

    dataset.input = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.delete()

    assert isinstance(dataset.output, pd.DataFrame)
    assert len(dataset.output) == 2


def test_delete_empty_input_is_noop():
    """Test that delete() with empty input is a no-op."""
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
            delete=GraphqlDeleteSettings(
                mutation="query DeletePost($id: ID!) { deletePost(id: $id) }",
                identity_columns=["id"],
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
        dataset.delete()

    mock_connection.session.post.assert_not_called()
    assert len(dataset.output) == 0


def test_delete_no_connection_raises_error():
    """Test that delete() raises ConnectionError if connection is not initialized."""
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
            delete=GraphqlDeleteSettings(
                mutation="query DeletePost($id: ID!) { deletePost(id: $id) }",
                identity_columns=["id"],
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    dataset.input = pd.DataFrame({"id": [1]})

    with pytest.raises(ConnectionError):
        dataset.delete()


def test_delete_no_settings_raises_error():
    """Test that delete() raises error when delete settings are not provided."""
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
    dataset.input = pd.DataFrame({"id": [1]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        with pytest.raises(DeleteError, match="delete settings must be provided"):
            dataset.delete()


def test_delete_missing_identity_columns_raises_error():
    """Test that delete() raises error when identity columns are not provided."""
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
            delete=GraphqlDeleteSettings(
                mutation="mutation DeletePost($id: ID!) { deletePost(id: $id) }",
                identity_columns=[],
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    dataset.input = pd.DataFrame({"id": [1]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        with pytest.raises(DeleteError, match="Identity columns must be provided"):
            dataset.delete()


def test_delete_identity_column_not_in_input_raises_error():
    """Test that delete() raises error when identity column not in input."""
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
            delete=GraphqlDeleteSettings(
                mutation="mutation DeletePost($id: ID!) { deletePost(id: $id) }",
                identity_columns=["missing_column"],
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    dataset.input = pd.DataFrame({"id": [1], "name": ["Test"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        with pytest.raises(DeleteError, match="Identity column 'missing_column' not found"):
            dataset.delete()


def test_delete_single_row_identity_variables():
    """Test that delete() with single row extracts identity values as object."""
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
            delete=GraphqlDeleteSettings(
                mutation="mutation DeletePost($id: ID!) { deletePost(id: $id) { id } }",
                identity_columns=["id"],
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"deletePost": {"id": 1}}}
    mock_connection.session.post.return_value = mock_response

    dataset.input = pd.DataFrame({"id": [1], "name": ["Test"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.delete()

    # Verify variables were extracted correctly
    call_kwargs = mock_connection.session.post.call_args[1]
    variables = call_kwargs["json"]["variables"]
    assert variables["id"] == 1


def test_delete_with_operation_name():
    """Test that delete() includes operation name in payload."""
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
            delete=GraphqlDeleteSettings(
                mutation="mutation DeletePost($id: ID!) { deletePost(id: $id) }",
                identity_columns=["id"],
                operation_name="DeletePost",
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"deletePost": True}}
    mock_connection.session.post.return_value = mock_response

    dataset.input = pd.DataFrame({"id": [1]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.delete()

    # Verify operation name was included
    call_kwargs = mock_connection.session.post.call_args[1]
    assert call_kwargs["json"]["operationName"] == "DeletePost"


def test_delete_graphql_error_raises_delete_error():
    """Test that GraphQL errors in response raise DeleteError."""
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
            delete=GraphqlDeleteSettings(
                mutation="mutation DeletePost($id: ID!) { deletePost(id: $id) }",
                identity_columns=["id"],
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {"errors": [{"message": "Post not found"}]}
    mock_connection.session.post.return_value = mock_response

    dataset.input = pd.DataFrame({"id": [1]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        with pytest.raises(DeleteError, match="GraphQL delete mutation failed"):
            dataset.delete()


def test_delete_exception_wrapped_in_delete_error():
    """Test that generic exceptions are wrapped in DeleteError."""
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
            delete=GraphqlDeleteSettings(
                mutation="mutation DeletePost($id: ID!) { deletePost(id: $id) }",
                identity_columns=["id"],
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_connection.session.post.side_effect = ValueError("Network error")

    dataset.input = pd.DataFrame({"id": [1]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        with pytest.raises(DeleteError, match="Failed to delete rows via GraphQL"):
            dataset.delete()


def test_delete_no_data_in_response_uses_input():
    """Test that delete() uses input when response has no data field."""
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
            delete=GraphqlDeleteSettings(
                mutation="mutation DeletePost($id: ID!) { deletePost(id: $id) }",
                identity_columns=["id"],
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    # Response without data field - just success
    mock_response.json.return_value = {}
    mock_connection.session.post.return_value = mock_response

    dataset.input = pd.DataFrame({"id": [1], "name": ["Test"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.delete()

    # Output should use input data
    assert isinstance(dataset.output, pd.DataFrame)
    assert len(dataset.output) == 1


def test_delete_with_additional_variables():
    """Test that delete() merges additional variables from settings."""
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
            delete=GraphqlDeleteSettings(
                mutation="mutation DeletePost($id: ID!, $force: Boolean) { deletePost(id: $id, force: $force) }",
                identity_columns=["id"],
                variables={"force": True},
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"deletePost": True}}
    mock_connection.session.post.return_value = mock_response

    dataset.input = pd.DataFrame({"id": [1]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.delete()

    # Verify additional variables were merged
    call_kwargs = mock_connection.session.post.call_args[1]
    variables = call_kwargs["json"]["variables"]
    assert variables["id"] == 1
    assert variables["force"] is True
