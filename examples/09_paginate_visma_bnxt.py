"""
Cursor pagination against Visma Business NXT.

Visma style: ``items`` + ``totalCount`` + ``pageInfo { hasNextPage endCursor }``.
Declare ``$after: String`` (nullable): Visma's ``after`` argument accepts a
missing cursor, so the first page simply omits it. (Older query files use
``$after: String!``, which forces sending ``after: ""``.)

Env:
    VISMA_BNXT_CLIENT_ID       OAuth2 client id
    VISMA_BNXT_CLIENT_SECRET   OAuth2 client secret
    VISMA_BNXT_COMPANY_NO      company number (cid)

Run:
    VISMA_BNXT_CLIENT_ID=... VISMA_BNXT_CLIENT_SECRET=... VISMA_BNXT_COMPANY_NO=... \
        uv run python examples/09_paginate_visma_bnxt.py
"""

import os
import uuid

from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_protocol_http_py_lib.linked_service.http import OAuth2AuthSettings
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

API_ENDPOINT = "https://business.visma.net/api/graphql-service"
TOKEN_ENDPOINT = "https://connect.visma.com/connect/token"

# The query declares $first / $after / $filter; the provider fills $first and
# $after per page. $cid and $filter come from static variables.
QUERY = """
query read($cid: Int!, $first: Int!, $after: String, $filter: FilterExpression_Associate!) {
  useCompany(no: $cid) {
    associate(first: $first, after: $after, filter: $filter) {
      totalCount
      pageInfo { hasNextPage endCursor }
      items { associateNo name customerNo supplierNo changedDateTime }
    }
  }
}
"""

linked_service = HttpLinkedService(
    settings=HttpLinkedServiceSettings(
        host=API_ENDPOINT,
        auth_type=AuthType.OAUTH2,
        oauth2=OAuth2AuthSettings(
            token_endpoint=TOKEN_ENDPOINT,
            client_id=os.environ["VISMA_BNXT_CLIENT_ID"],
            client_secret=os.environ["VISMA_BNXT_CLIENT_SECRET"],
            scope="business-graphql-service-api:access-group-based",
        ),
    ),
    id=uuid.uuid4(),
    name="example::visma_bnxt",
    version="1.0.0",
)

dataset = GraphqlDataset(
    settings=GraphqlDatasetSettings(
        url=API_ENDPOINT,
        read=GraphqlReadSettings(
            query=QUERY,
            variables={
                "cid": int(os.environ["VISMA_BNXT_COMPANY_NO"]),
                "filter": {},  # required by the query; put real filters here
            },
            pagination=PaginationSettings(
                strategy=PaginationStrategy.CURSOR,
                items_path="data.useCompany.associate.items",  # paging control + output rows
                max_pages=3,  # safety ceiling for the example; None = read everything
                cursor=CursorPaginationSettings(
                    cursor_path="data.useCompany.associate.pageInfo.endCursor",  # cursor is in pageInfo
                    has_next_path="data.useCompany.associate.pageInfo.hasNextPage",
                    cursor_variable="after",
                    page_size_variable="first",
                    page_size=100,
                ),
            ),
        ),
    ),
    linked_service=linked_service,
    id=uuid.uuid4(),
    name="example::visma_bnxt_associates",
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
