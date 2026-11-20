"""
Cursor pagination with incremental (watermark) reads against Visma Business NXT.

Pagination, Visma style: ``items`` + ``pageInfo { hasNextPage endCursor }``.
Declare ``$after: String`` (nullable): Visma's ``after`` argument accepts a
missing cursor, so the first page simply omits it. (Older query files use
``$after: String!``, which forces sending ``after: ""``.) All pages are fetched
inside one ``read()``. Leave out ``incremental=`` for a plain paginated full read.

Incremental: each run reads only associates added since the previous run:

1. The query filters on a lower bound variable: ``associateNo: { _gt: $since }``.
2. ``IncrementalSettings(variable="since", watermark_path="associateNo")`` tells
   the provider to put the stored watermark into ``$since`` before the first page.
3. After a fully successful read, the watermark becomes the highest
   ``associateNo`` in the output and is stored in ``checkpoint["incremental"]``.

The checkpoint is kept in a JSON file between runs (``CHECKPOINT_FILE``). In a
real pipeline the orchestrator stores and restores ``dataset.checkpoint``.

Run it twice: the first run loads every associate and sets the watermark, the
second run sends ``$since`` and returns only associates created in between
(usually none).

Notes:
    - ``$since: Int = 0``: on the first run ``$since`` is omitted. Visma treats
      ``associateNo: { _gt: null }`` on its own as "match nothing" (seen live: 0
      rows), so the query declares a default lower bound. Same as Xledger; see
      ``08_paginate_incremental_xledger.py``.
    - The filter is written in the query document, not passed as ``$filter``:
      incremental injects a variable's *value*, it cannot edit a filter object.
    - When nothing matches, Visma returns ``items: null``; that is read as an
      empty page, the run succeeds and the watermark stays where it was.
    - ``associateNo`` only catches *new* rows. To also catch *changed* rows, use
      ``changedDateTime`` as the watermark with a ``_gte`` filter, and expect the
      boundary rows to be read again (dedupe on the key).
    - Incremental requires ``pagination``: ``GraphqlReadSettings`` raises otherwise.
    - On failure the watermark does not move; ``checkpoint["pagination"]``
      lets the next run resume the same incremental run where it stopped, and
      ``checkpoint["incremental"]["pending"]`` keeps the max already read, so
      the resumed run commits the max over both attempts.

Env:
    VISMA_BNXT_CLIENT_ID       OAuth2 client id
    VISMA_BNXT_CLIENT_SECRET   OAuth2 client secret
    VISMA_BNXT_COMPANY_NO      company number (cid)

Run:
    VISMA_BNXT_CLIENT_ID=... VISMA_BNXT_CLIENT_SECRET=... VISMA_BNXT_COMPANY_NO=... \
        uv run python examples/09_paginate_incremental_visma_bnxt.py   # run again: only new rows
    rm .visma_bnxt_checkpoint.json                                     # start over
"""

import json
import os
import uuid
from pathlib import Path

from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_protocol_http_py_lib.linked_service.http import OAuth2AuthSettings
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

API_ENDPOINT = "https://business.visma.net/api/graphql-service"
TOKEN_ENDPOINT = "https://connect.visma.com/connect/token"
CHECKPOINT_FILE = Path(".visma_bnxt_checkpoint.json")

# $first / $after are filled per page by pagination; $since by incremental; $cid is static.
QUERY = """
query read($cid: Int!, $first: Int!, $after: String, $since: Int = 0) {
  useCompany(no: $cid) {
    associate(first: $first, after: $after, filter: { associateNo: { _gt: $since } }) {
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
            variables={"cid": int(os.environ["VISMA_BNXT_COMPANY_NO"])},
            pagination=PaginationSettings(
                strategy=PaginationStrategy.CURSOR,
                items_path="data.useCompany.associate.items",
                max_pages=None,  # the watermark only commits after a full drain
                cursor=CursorPaginationSettings(
                    cursor_path="data.useCompany.associate.pageInfo.endCursor",
                    has_next_path="data.useCompany.associate.pageInfo.hasNextPage",
                    cursor_variable="after",
                    page_size_variable="first",
                    page_size=100,
                ),
            ),
            incremental=IncrementalSettings(
                variable="since",  # query variable that receives the watermark
                watermark_path="associateNo",  # field on each output row; the max becomes the next watermark
            ),
        ),
    ),
    linked_service=linked_service,
    id=uuid.uuid4(),
    name="example::visma_bnxt_associates_incremental",
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
print(f"checkpoint after: {dataset.checkpoint}")  # e.g. {"incremental": {"watermark": 18}}
