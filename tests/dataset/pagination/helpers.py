"""
**File:** ``helpers.py``
**Region:** ``tests/dataset/pagination``

Shared builders for pagination tests: Visma BNXT / Xledger style page bodies,
pagination settings, a ``GraphqlDataset`` with a mocked connection, and
helpers to run ``read()`` against a scripted list of responses.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType
from ds_resource_plugin_py_lib.common.resource.dataset.errors import ReadError

from ds_protocol_graphql_py_lib import GraphqlDeserializer
from ds_protocol_graphql_py_lib.dataset.graphql import (
    GraphqlDataset,
    GraphqlDatasetSettings,
    GraphqlReadSettings,
)
from ds_protocol_graphql_py_lib.dataset.pagination import (
    CursorPaginationSettings,
    PaginationSettings,
    PaginationStrategy,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

URL = "https://example.graphql.api/graphql"

VISMA_QUERY = """
query read($cid: Int!, $first: Int!, $after: String, $filter: FilterExpression_Associate!) {
  useCompany(no: $cid) {
    associate(first: $first, after: $after, filter: $filter) {
      items { associateNo name }
      totalCount
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

XLEDGER_QUERY = """
query read($first: Int, $after: String) {
  customers(first: $first, after: $after) {
    pageInfo { hasNextPage }
    edges { cursor node { dbId description } }
  }
}
"""


# --------------------------------------------------------------------------- page bodies


def visma_page(rows: list[dict[str, Any]], *, has_next: bool, end_cursor: Any) -> dict[str, Any]:
    """Visma Business NXT style: ``items`` + ``pageInfo.endCursor``."""
    return {
        "data": {
            "useCompany": {
                "associate": {
                    "items": rows,
                    "totalCount": 99,
                    "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
                }
            }
        }
    }


def xledger_page(nodes: list[tuple[str, dict[str, Any]]], *, has_next: bool) -> dict[str, Any]:
    """Xledger style: ``edges`` with a cursor per edge, no ``endCursor``."""
    return {
        "data": {
            "customers": {
                "pageInfo": {"hasNextPage": has_next},
                "edges": [{"cursor": cursor, "node": node} for cursor, node in nodes],
            }
        }
    }


# --------------------------------------------------------------------------- settings


def visma_pagination(**overrides: Any) -> PaginationSettings:
    cursor_kwargs: dict[str, Any] = {
        "cursor_path": "data.useCompany.associate.pageInfo.endCursor",
        "has_next_path": "data.useCompany.associate.pageInfo.hasNextPage",
        "page_size": 2,
    }
    cursor_kwargs.update(overrides.pop("cursor", {}))
    return PaginationSettings(
        strategy=PaginationStrategy.CURSOR,
        items_path="data.useCompany.associate.items",
        cursor=CursorPaginationSettings(**cursor_kwargs),
        **overrides,
    )


def xledger_pagination(**overrides: Any) -> PaginationSettings:
    cursor_kwargs: dict[str, Any] = {
        "cursor_path": "data.customers.edges.-1.cursor",
        "has_next_path": "data.customers.pageInfo.hasNextPage",
        "page_size": 2,
    }
    cursor_kwargs.update(overrides.pop("cursor", {}))
    return PaginationSettings(
        strategy=PaginationStrategy.CURSOR,
        items_path="data.customers.edges",
        cursor=CursorPaginationSettings(**cursor_kwargs),
        **overrides,
    )


# --------------------------------------------------------------------------- dataset + transport


def make_dataset(read: GraphqlReadSettings, headers: dict[str, str] | None = None) -> GraphqlDataset[Any, Any]:
    linked_service = HttpLinkedService(
        settings=HttpLinkedServiceSettings(host="https://example.graphql.api/", auth_type=AuthType.NO_AUTH),
        id=uuid.uuid4(),
        name="test_linked_service",
        version="1.0.0",
    )
    return GraphqlDataset(
        deserializer=GraphqlDeserializer(format=DatasetStorageFormatType.JSON),
        settings=GraphqlDatasetSettings(url=URL, read=read, headers=headers),
        linked_service=linked_service,
        id=uuid.uuid4(),
        name="test_dataset",
        version="1.0.0",
    )


def response(body: dict[str, Any]) -> MagicMock:
    mock = MagicMock()
    mock.json.return_value = body
    return mock


@contextmanager
def mocked_session(dataset: GraphqlDataset[Any, Any], bodies: list[Any]) -> Iterator[MagicMock]:
    """
    Patch ``linked_service.connection`` so ``session.post`` returns ``bodies`` in order.

    An ``Exception`` instance in ``bodies`` is raised by that call instead.
    """
    connection = MagicMock()
    connection.session.post.side_effect = [b if isinstance(b, Exception) else response(b) for b in bodies]
    with patch.object(type(dataset.linked_service), "connection", new_callable=PropertyMock) as prop:
        prop.return_value = connection
        yield connection.session


def run_read(dataset: GraphqlDataset[Any, Any], bodies: list[Any]) -> MagicMock:
    """Run ``read()`` with the given page bodies; return the session mock."""
    with mocked_session(dataset, bodies) as session:
        dataset.read()
    return session


def run_read_expect_error(
    dataset: GraphqlDataset[Any, Any], bodies: list[Any]
) -> tuple[pytest.ExceptionInfo[ReadError], MagicMock]:
    """Run ``read()`` expecting ``ReadError``; return the error info and the session mock."""
    with mocked_session(dataset, bodies) as session, pytest.raises(ReadError) as exc_info:
        dataset.read()
    return exc_info, session


def sent_variables(session: MagicMock) -> list[dict[str, Any]]:
    """GraphQL variables sent on each ``session.post`` call, in order."""
    return [call.kwargs["json"].get("variables", {}) for call in session.post.call_args_list]
