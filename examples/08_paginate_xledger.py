"""
Cursor pagination against Xledger.

Xledger style: Relay ``edges { cursor node }`` + ``pageInfo { hasNextPage }``
and **no** ``endCursor`` — the next cursor is read from the last edge.

Env:
    XLEDGER_TOKEN   API token (sent as ``Authorization: token <XLEDGER_TOKEN>``)
    XLEDGER_URL     optional, default https://demo.xledger.net/graphql

Run:
    XLEDGER_TOKEN=... uv run python examples/08_paginate_xledger.py
"""

import os
import uuid

from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_protocol_http_py_lib.linked_service.http import ApiKeyAuthSettings
from ds_resource_plugin_py_lib.common.resource.dataset.errors import ReadError

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

URL = os.environ.get("XLEDGER_URL", "https://demo.xledger.net/graphql")

# The query declares $first / $after; the provider fills them per page.
QUERY = """
query read($first: Int!, $after: String) {
  customers(first: $first, after: $after) {
    pageInfo { hasNextPage }
    edges {
      cursor
      node { dbId code description modifiedAt }
    }
  }
}
"""

linked_service = HttpLinkedService(
    settings=HttpLinkedServiceSettings(
        host=URL,
        auth_type=AuthType.API_KEY,
        api_key=ApiKeyAuthSettings(name="Authorization", value=f"token {os.environ['XLEDGER_TOKEN']}"),
    ),
    id=uuid.uuid4(),
    name="example::xledger",
    version="1.0.0",
)

dataset = GraphqlDataset(
    settings=GraphqlDatasetSettings(
        url=URL,
        read=GraphqlReadSettings(
            query=QUERY,
            pagination=PaginationSettings(
                strategy=PaginationStrategy.CURSOR,
                items_path="data.customers.edges",  # edges: paging control + output (unwrapped to node)
                max_pages=3,  # safety ceiling for the example; None = read everything
                cursor=CursorPaginationSettings(
                    cursor_path="data.customers.edges.-1.cursor",  # no endCursor: cursor on the last edge (-1)
                    has_next_path="data.customers.pageInfo.hasNextPage",
                    cursor_variable="after",
                    page_size_variable="first",
                    page_size=100,
                ),
            ),
        ),
    ),
    linked_service=linked_service,
    id=uuid.uuid4(),
    name="example::xledger_customers",
    version="1.0.0",
)

dataset.linked_service.connect()

try:
    dataset.read()
except ReadError as exc:
    # Partial output is kept and checkpoint["pagination"] points at the next page.
    print(f"Read stopped: {exc.message}")

print(dataset.output)
print(f"rows: {len(dataset.output)}")
print(f"checkpoint: {dataset.checkpoint}")  # empty after a complete read
