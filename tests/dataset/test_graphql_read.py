"""
**File:** ``test_graphql_read.py``
**Region:** ``tests/dataset``

Test GraphQL Dataset read() method implementation.

Covers:
    GraphQL dataset read functionality including query execution,
    variable handling, operation names, response deserialization,
    and error handling for connection failures and invalid queries.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, PropertyMock, patch

import pandas as pd
import pytest
from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType
from ds_resource_plugin_py_lib.common.resource.dataset.errors import ReadError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    ConnectionError,
)
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ds_protocol_graphql_py_lib import GraphqlDeserializer
from ds_protocol_graphql_py_lib.dataset.graphql import (
    GraphqlDataset,
    GraphqlDatasetSettings,
    GraphqlReadSettings,
)


def test_read_returns_none():
    """Test that read() returns None per DATASET_CONTRACT."""
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
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
            read=GraphqlReadSettings(query="{ allPosts { id title } }"),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"allPosts": [{"id": "1", "title": "Test"}]}}
    mock_connection.session.post.return_value = mock_response

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        result = dataset.read()
        assert result is None


def test_read_populates_output():
    """Test that read() populates self.output with response."""
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
        deserializer=GraphqlDeserializer(format=DatasetStorageFormatType.JSON),
        serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
            read=GraphqlReadSettings(query="{ allPosts { id title } }"),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    response_data = {"data": {"allPosts": [{"id": "1", "title": "Post 1"}]}}
    mock_response.json.return_value = response_data
    mock_connection.session.post.return_value = mock_response

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.read()

    # read() should populate output with a DataFrame
    assert isinstance(dataset.output, pd.DataFrame)
    assert len(dataset.output) == 1
    assert list(dataset.output.columns) == ["id", "title"]
    assert dataset.output.iloc[0]["id"] == "1"
    assert dataset.output.iloc[0]["title"] == "Post 1"


def test_read_with_variables():
    """Test that read() includes variables in payload."""
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
        deserializer=GraphqlDeserializer(format=DatasetStorageFormatType.JSON),
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
            read=GraphqlReadSettings(
                query="query GetPost($id: ID!) { post(id: $id) { id title } }",
                variables={"id": "123"},
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"post": {"id": "123", "title": "Test"}}}
    mock_connection.session.post.return_value = mock_response

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.read()

    # Verify the POST was called with variables in payload
    call_kwargs = mock_connection.session.post.call_args[1]
    assert "variables" in call_kwargs["json"]
    assert call_kwargs["json"]["variables"] == {"id": "123"}


def test_read_with_operation_name():
    """Test that read() includes operation name in payload."""
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
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
            read=GraphqlReadSettings(
                query="query GetAllPosts { allPosts { id title } }",
                operation_name="GetAllPosts",
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"allPosts": []}}
    mock_connection.session.post.return_value = mock_response

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.read()

    # Verify the POST was called with operation name in payload
    call_kwargs = mock_connection.session.post.call_args[1]
    assert "operationName" in call_kwargs["json"]
    assert call_kwargs["json"]["operationName"] == "GetAllPosts"


def test_read_with_variables_and_operation_name():
    """Test that read() includes both variables and operation name."""
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
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
            read=GraphqlReadSettings(
                query="query GetPost($id: ID!) { post(id: $id) { id title } }",
                variables={"id": "123"},
                operation_name="GetPost",
            ),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"post": {"id": "123", "title": "Test"}}}
    mock_connection.session.post.return_value = mock_response

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.read()

    # Verify the POST was called with both variables and operation name
    call_kwargs = mock_connection.session.post.call_args[1]
    assert call_kwargs["json"]["variables"] == {"id": "123"}
    assert call_kwargs["json"]["operationName"] == "GetPost"


def test_read_no_connection_raises_error():
    """Test that read() raises ConnectionError if connection is not initialized."""
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
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
            read=GraphqlReadSettings(query="{ allPosts { id } }"),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    with pytest.raises(ConnectionError):
        dataset.read()


def test_read_no_settings_raises_error():
    """Test that read() raises ReadError if read settings are not provided."""
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
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        with pytest.raises(ReadError, match="GraphQL read settings must be provided"):
            dataset.read()


def test_read_relay_style_pagination():
    """Test that read() handles Relay-style pagination with edges/node structure."""
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
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
            read=GraphqlReadSettings(query="{ countries { edges { node { name alpha2Code } } } }"),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    # Relay-style pagination response with edges/node
    mock_response.json.return_value = {
        "data": {
            "countries": {
                "edges": [
                    {"node": {"name": "Albania", "alpha2Code": "AL"}},
                    {"node": {"name": "Andorra", "alpha2Code": "AD"}},
                    {"node": {"name": "Austria", "alpha2Code": "AT"}},
                ]
            }
        }
    }
    mock_connection.session.post.return_value = mock_response

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.read()

    # Should extract nodes from edges and create DataFrame
    assert isinstance(dataset.output, pd.DataFrame)
    assert len(dataset.output) == 3
    assert list(dataset.output.columns) == ["name", "alpha2Code"]
    assert dataset.output.iloc[0]["name"] == "Albania"
    assert dataset.output.iloc[0]["alpha2Code"] == "AL"
    assert dataset.output.iloc[1]["name"] == "Andorra"
    assert dataset.output.iloc[2]["name"] == "Austria"


def test_read_nested_dict_with_array_field():
    """Test that read() handles nested dict containing a single array field."""
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
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
            read=GraphqlReadSettings(query='{ __type(name: "Root") { fields { name } } }'),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    # Nested dict with array field (GraphQL introspection pattern)
    mock_response.json.return_value = {
        "data": {
            "__type": {
                "fields": [
                    {"name": "allFilms"},
                    {"name": "film"},
                    {"name": "allPeople"},
                    {"name": "person"},
                ]
            }
        }
    }
    mock_connection.session.post.return_value = mock_response

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        dataset.read()

    # Should extract the array from nested dict and create DataFrame
    assert isinstance(dataset.output, pd.DataFrame)
    assert len(dataset.output) == 4
    assert list(dataset.output.columns) == ["name"]
    assert dataset.output.iloc[0]["name"] == "allFilms"
    assert dataset.output.iloc[1]["name"] == "film"
    assert dataset.output.iloc[2]["name"] == "allPeople"
    assert dataset.output.iloc[3]["name"] == "person"


def test_read_no_deserializer_raises_error():
    """Test that read() raises ReadError if deserializer is not configured."""
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
        deserializer=None,
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
            read=GraphqlReadSettings(query="{ allPosts { id } }"),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        with pytest.raises(ReadError, match="Deserializer is not configured"):
            dataset.read()


def test_read_json_parse_error_wrapped():
    """Test that JSON parse errors are wrapped in ReadError."""
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
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
            read=GraphqlReadSettings(query="{ allPosts { id } }"),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.side_effect = ValueError("Invalid JSON")
    mock_connection.session.post.return_value = mock_response

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        with pytest.raises(ReadError, match="Failed to read from GraphQL"):
            dataset.read()


def test_read_graphql_errors_response_raises_read_error_with_details() -> None:
    """GraphQL errors from API response should be raised as ReadError without wrapping."""
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
        settings=GraphqlDatasetSettings(
            url="https://example.graphql.api/graphql",
            read=GraphqlReadSettings(query="{ countries { name } }"),
        ),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )

    mock_connection = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = {"errors": [{"message": "boom"}]}
    mock_connection.session.post.return_value = mock_response

    with patch.object(type(linked_service), "connection", new_callable=PropertyMock) as mock_prop:
        mock_prop.return_value = mock_connection
        with pytest.raises(ReadError, match="GraphQL query failed") as exc_info:
            dataset.read()

    assert exc_info.value.details == {"errors": [{"message": "boom"}]}


def test_check_for_graphql_read_error_no_errors_does_not_raise() -> None:
    """No `errors` key should pass validation."""
    GraphqlDataset._check_for_graphql_read_error({"data": {"countries": []}})


@pytest.mark.parametrize(
    ("response_data", "expected_message"),
    [
        (
            {"errors": [{"message": "boom"}]},
            "GraphQL query failed: boom",
        ),
        (
            {"errors": {"message": "boom"}},
            "GraphQL query failed: boom",
        ),
        (
            {"errors": []},
            "GraphQL query failed",
        ),
        (
            {"errors": [{"code": "E123"}]},
            "GraphQL query failed",
        ),
        (
            {"errors": {"message": ""}},
            "GraphQL query failed",
        ),
    ],
)
def test_check_for_graphql_read_error_raises_read_error_with_expected_message(
    response_data: dict[str, object], expected_message: str
) -> None:
    """Validation should raise ReadError and preserve original errors payload in details."""
    with pytest.raises(ReadError, match=expected_message) as exc_info:
        GraphqlDataset._check_for_graphql_read_error(response_data)

    assert exc_info.value.details == {"errors": response_data["errors"]}
