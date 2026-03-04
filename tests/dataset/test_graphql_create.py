"""
**File:** ``test_graphql_create.py``
**Region:** ``tests/dataset``

Test GraphQL Dataset create() method implementation.

Covers:
    GraphQL dataset creation functionality including mutation execution,
    batch mode operations, variable handling, operation names, and error
    handling for connection failures and invalid mutations.
"""

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
        with pytest.raises(CreateError, match="Create settings must be provided"):
            dataset.create()


def test_create_with_operation_name():
    """Test that create() includes operation name in payload."""
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
                operation_name="CreatePost",
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

    dataset.input = pd.DataFrame({"title": ["Test"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.create()

    # Verify operation name was included in payload
    call_kwargs = mock_connection.session.post.call_args[1]
    assert call_kwargs["json"]["operationName"] == "CreatePost"


def test_create_single_row_sends_single_object():
    """Test that create() with single row sends input as single object."""
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
    mock_response.json.return_value = {"data": {"createPost": {"id": "1", "title": "Test"}}}
    mock_connection.session.post.return_value = mock_response

    dataset.input = pd.DataFrame({"title": ["Test"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.create()

    # Verify input was sent as a single object, not a list
    call_kwargs = mock_connection.session.post.call_args[1]
    input_value = call_kwargs["json"]["variables"]["input"]
    assert isinstance(input_value, dict)
    assert input_value["title"] == "Test"


def test_create_multiple_rows_sends_array():
    """Test that create() with multiple rows sends input as array."""
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
                mutation="mutation CreatePosts($input: [CreatePostInput!]!) { createPosts(input: $input) { id } }",
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
    mock_response.json.return_value = {"data": {"createPosts": [{"id": "1"}, {"id": "2"}]}}
    mock_connection.session.post.return_value = mock_response

    dataset.input = pd.DataFrame({"title": ["Test1", "Test2"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.create()

    # Verify input was sent as an array
    call_kwargs = mock_connection.session.post.call_args[1]
    input_value = call_kwargs["json"]["variables"]["input"]
    assert isinstance(input_value, list)
    assert len(input_value) == 2


def test_create_exception_wrapped_in_create_error():
    """Test that generic exceptions are wrapped in CreateError."""
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
    mock_connection.session.post.side_effect = ValueError("Network error")

    dataset.input = pd.DataFrame({"title": ["Test"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        with pytest.raises(CreateError, match="Failed to create rows via GraphQL"):
            dataset.create()


def test_create_empty_response_data_uses_input():
    """Test that create() uses input when response has no data."""
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
    # Response with empty data field
    mock_response.json.return_value = {"data": None}
    mock_connection.session.post.return_value = mock_response

    dataset.input = pd.DataFrame({"title": ["Test"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.create()

    assert isinstance(dataset.output, pd.DataFrame)


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


def test_create_missing_mutation_raises_error():
    """Test that create() raises error when mutation is not provided."""
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
                mutation="",  # Empty mutation
                input_field="input",
            ),
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
        with pytest.raises(CreateError, match="GraphQL mutation must be provided"):
            dataset.create()


def test_create_missing_input_field_raises_error():
    """Test that create() raises error when input field is not provided."""
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
                input_field="",  # Empty input field
            ),
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
        with pytest.raises(CreateError, match="Input field name must be provided"):
            dataset.create()


def test_create_unwraps_nested_mutation_response():
    """Test that create() unwraps nested mutation responses."""
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
    # Response with nested mutation response
    mock_response.json.return_value = {"data": {"createPost": {"id": "1", "title": "Test"}}}
    mock_connection.session.post.return_value = mock_response

    dataset.input = pd.DataFrame({"title": ["Test"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.create()

    # Verify the single object was properly extracted
    assert isinstance(dataset.output, pd.DataFrame)
    assert len(dataset.output) == 1


def test_create_response_without_data_field_uses_input():
    """Test that create() uses input when response lacks data field."""
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
    # Response without data key
    mock_response.json.return_value = {}
    mock_connection.session.post.return_value = mock_response

    dataset.input = pd.DataFrame({"title": ["Test"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.create()

    # Output should use input data
    assert isinstance(dataset.output, pd.DataFrame)


def test_validate_create_settings_no_connection_raises_error():
    """Test that _validate_create_settings raises error when connection is None."""
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

    mock_connection = None
    dataset.input = pd.DataFrame({"title": ["Test"]})

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        with pytest.raises(ConnectionError):
            dataset.create()
