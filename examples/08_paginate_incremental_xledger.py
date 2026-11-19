"""
Cursor pagination with incremental (watermark) reads against Xledger.

Pagination, Xledger style: Relay ``edges { cursor node }`` + ``pageInfo
{ hasNextPage }`` and **no** ``endCursor``, so the next cursor is read from the
last edge (``cursor_path="data.customers.edges.-1.cursor"``). All pages are
fetched inside one ``read()``. Leave out ``incremental=`` for a plain paginated
full read.

Incremental: each run reads only customers added since the previous run:

1. The query filters on a lower bound variable: ``dbId_gt: $since``.
2. ``IncrementalSettings(variable="since", watermark_path="dbId")`` tells the
   provider to put the stored watermark into ``$since`` before the first page.
3. After a fully successful read, the watermark becomes the highest ``dbId``
   in the output and is stored in ``checkpoint["incremental"]``.

The checkpoint is kept in a JSON file between runs (``CHECKPOINT_FILE``). In a
real pipeline the orchestrator stores and restores ``dataset.checkpoint``.

Run it twice: the first run loads every customer and sets the watermark, the
second run sends ``$since`` and returns only customers created in between
(usually none).

Notes:
    - ``$since: Int = 0``: on the first run there is no watermark, so ``$since``
      is omitted. Xledger treats ``dbId_gt: null`` as "match nothing", so the
      query declares a default. APIs that ignore a null bound don't need it.
    - ``dbId`` only catches *new* rows. To also catch *changed* rows, use a
      change timestamp (e.g. ``modifiedAt``) as the watermark with a ``>=``
      filter, and expect the boundary rows to be read again (dedupe on the key).
    - Incremental is applied only when ``pagination`` is set.
    - On failure the watermark does not move; ``checkpoint["pagination"]``
      lets the next run resume the same incremental run where it stopped, and
      ``checkpoint["incremental"]["pending"]`` keeps the max already read, so
      the resumed run commits the max over both attempts.

Env:
    XLEDGER_TOKEN   API token (sent as ``Authorization: token <XLEDGER_TOKEN>``)
    XLEDGER_URL     optional, default https://demo.xledger.net/graphql

Run:
    XLEDGER_TOKEN=... uv run python examples/08_paginate_incremental_xledger.py
    XLEDGER_TOKEN=... uv run python examples/08_paginate_incremental_xledger.py   # again: only new rows
    rm .xledger_checkpoint.json                                                   # start over
"""

import json
import os
import uuid
from pathlib import Path

from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_protocol_http_py_lib.linked_service.http import ApiKeyAuthSettings
from ds_resource_plugin_py_lib.common.resource.dataset.errors import ReadError

from ds_protocol_graphql_py_lib.dataset.graphql import (
    GraphqlDataset,
    GraphqlDatasetSettings,
    GraphqlReadSettings,
)
from ds_protocol_graphql_py_lib.dataset.incremental import IncrementalSettings
from ds_protocol_graphql_py_lib.dataset.pagination import (
    CursorPaginationSettings,
    PaginationSettings,
    PaginationStrategy,
)

URL = os.environ.get("XLEDGER_URL", "https://demo.xledger.net/graphql")
CHECKPOINT_FILE = Path(".xledger_checkpoint.json")

# $first / $after are filled per page by pagination; $since by incremental.
QUERY = """
query read($first: Int!, $after: String, $since: Int = 0) {
  customers(first: $first, after: $after, filter: { dbId_gt: $since }) {
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
                items_path="data.customers.edges",
                max_pages=None,  # the watermark only commits after a full drain
                cursor=CursorPaginationSettings(
                    cursor_path="data.customers.edges.-1.cursor",
                    has_next_path="data.customers.pageInfo.hasNextPage",
                    cursor_variable="after",
                    page_size_variable="first",
                    page_size=100,
                ),
            ),
            incremental=IncrementalSettings(
                variable="since",  # query variable that receives the watermark
                watermark_path="dbId",  # field on each output row; the max becomes the next watermark
            ),
        ),
    ),
    linked_service=linked_service,
    id=uuid.uuid4(),
    name="example::xledger_customers_incremental",
    version="1.0.0",
)

# Restore the checkpoint from the previous run (watermark and, after a failure, pagination).
if CHECKPOINT_FILE.exists():
    dataset.checkpoint = json.loads(CHECKPOINT_FILE.read_text())
print(f"checkpoint before: {dataset.checkpoint}")

dataset.linked_service.connect()

try:
    dataset.read()
except ReadError as exc:
    # Watermark unchanged; checkpoint["pagination"] points at the next page, so the next run resumes.
    print(f"Read stopped: {exc.message}")

# Persist whatever the read left behind: a new watermark on success, a resume point on failure.
CHECKPOINT_FILE.write_text(json.dumps(dataset.checkpoint))

print(dataset.output)
print(f"rows: {len(dataset.output)}")
print(f"checkpoint after: {dataset.checkpoint}")  # e.g. {"incremental": {"watermark": 6820999}}
