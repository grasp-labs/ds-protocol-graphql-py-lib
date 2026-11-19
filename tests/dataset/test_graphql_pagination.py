"""
**File:** ``test_graphql_pagination.py``
**Region:** ``tests/dataset``

End-to-end cursor pagination through ``GraphqlDataset.read()``.

Unit tests per module live in ``tests/dataset/pagination/`` (``Paginate``,
settings, registry, cursor strategy). This file covers the dataset-level
behaviour:

Covers:
    - Read routing: paginated vs. single request, ``supports_checkpoint``.
    - Visma Business NXT style (``items`` + ``pageInfo.endCursor``).
    - Xledger style (``edges`` + cursor on the last edge).
    - Failure through ``read()``: ``ReadError``, partial output, checkpoint,
      then resume from that checkpoint.
    - Incremental watermark across two ``read()`` runs (Xledger style).
    - ``GraphqlReadSettings`` with pagination round-trips through serde.
"""

from __future__ import annotations

from typing import Any

import pytest
import requests
from ds_common_serde_py_lib.errors import DeserializationError

from ds_protocol_graphql_py_lib.dataset.graphql import GraphqlReadSettings
from ds_protocol_graphql_py_lib.dataset.incremental import IncrementalSettings

from .pagination.helpers import (
    VISMA_QUERY,
    XLEDGER_QUERY,
    make_dataset,
    run_read,
    run_read_expect_error,
    sent_variables,
    visma_page,
    visma_pagination,
    xledger_page,
    xledger_pagination,
)

# --------------------------------------------------------------------------- read routing


def test_supports_checkpoint_only_when_paginated() -> None:
    assert make_dataset(GraphqlReadSettings(query="{ a }", pagination=visma_pagination())).supports_checkpoint is True
    assert make_dataset(GraphqlReadSettings(query="{ a }")).supports_checkpoint is False


def test_supports_checkpoint_without_read_settings() -> None:
    dataset = make_dataset(GraphqlReadSettings(query="{ a }"))
    dataset.settings.read = None
    assert dataset.supports_checkpoint is False


def test_single_request_path_without_pagination() -> None:
    """No pagination: one request, records_path honoured, checkpoint untouched."""
    dataset = make_dataset(GraphqlReadSettings(query=VISMA_QUERY, records_path="data.useCompany.associate.items"))
    session = run_read(dataset, [visma_page([{"associateNo": 1}, {"associateNo": 2}], has_next=True, end_cursor="c1")])
    assert session.post.call_count == 1
    assert dataset.output["associateNo"].tolist() == [1, 2]
    assert dataset.checkpoint == {}


# --------------------------------------------------------------------------- Visma BNXT


def test_visma_drains_all_pages() -> None:
    dataset = make_dataset(
        GraphqlReadSettings(query=VISMA_QUERY, variables={"cid": 123, "filter": {}}, pagination=visma_pagination())
    )
    session = run_read(
        dataset,
        [
            visma_page([{"associateNo": 1, "name": "a"}, {"associateNo": 2, "name": "b"}], has_next=True, end_cursor="c1"),
            visma_page([{"associateNo": 3, "name": "c"}, {"associateNo": 4, "name": "d"}], has_next=True, end_cursor="c2"),
            visma_page([{"associateNo": 5, "name": "e"}], has_next=False, end_cursor="c3"),
        ],
    )

    assert sent_variables(session) == [
        {"cid": 123, "filter": {}, "first": 2},
        {"cid": 123, "filter": {}, "first": 2, "after": "c1"},
        {"cid": 123, "filter": {}, "first": 2, "after": "c2"},
    ]
    assert list(dataset.output.columns) == ["associateNo", "name"]
    assert dataset.output["associateNo"].tolist() == [1, 2, 3, 4, 5]
    assert "pagination" not in dataset.checkpoint


def test_visma_static_variables_are_not_mutated() -> None:
    variables: dict[str, Any] = {"cid": 1, "filter": {"a": 1}}
    dataset = make_dataset(GraphqlReadSettings(query=VISMA_QUERY, variables=variables, pagination=visma_pagination()))
    run_read(
        dataset,
        [
            visma_page([{"associateNo": 1}, {"associateNo": 2}], has_next=True, end_cursor="c1"),
            visma_page([], has_next=False, end_cursor=None),
        ],
    )
    assert variables == {"cid": 1, "filter": {"a": 1}}
    assert dataset.settings.read is not None
    assert dataset.settings.read.variables == {"cid": 1, "filter": {"a": 1}}


def test_visma_required_cursor_start_value_from_static_variables() -> None:
    """For APIs requiring the cursor (``after: String!``), "" in static variables is sent on page 1, then overridden."""
    dataset = make_dataset(GraphqlReadSettings(query=VISMA_QUERY, variables={"after": ""}, pagination=visma_pagination()))
    session = run_read(
        dataset,
        [
            visma_page([{"associateNo": 1}], has_next=True, end_cursor="c1"),
            visma_page([{"associateNo": 2}], has_next=False, end_cursor=None),
        ],
    )
    assert sent_variables(session) == [{"after": "", "first": 2}, {"after": "c1", "first": 2}]


def test_visma_without_has_next_stops_on_empty_cursor() -> None:
    dataset = make_dataset(GraphqlReadSettings(query=VISMA_QUERY, pagination=visma_pagination(cursor={"has_next_path": None})))
    session = run_read(
        dataset,
        [
            visma_page([{"associateNo": 1}, {"associateNo": 2}], has_next=True, end_cursor="c1"),
            visma_page([{"associateNo": 3}], has_next=True, end_cursor=""),
        ],
    )
    assert session.post.call_count == 2
    assert dataset.output["associateNo"].tolist() == [1, 2, 3]


# --------------------------------------------------------------------------- Xledger


def test_xledger_uses_last_edge_cursor_and_unwraps_nodes() -> None:
    dataset = make_dataset(GraphqlReadSettings(query=XLEDGER_QUERY, pagination=xledger_pagination()))
    session = run_read(
        dataset,
        [
            xledger_page([("e1", {"dbId": 1, "description": "a"}), ("e2", {"dbId": 2, "description": "b"})], has_next=True),
            xledger_page([("e3", {"dbId": 3, "description": "c"})], has_next=False),
        ],
    )

    assert sent_variables(session) == [{"first": 2}, {"first": 2, "after": "e2"}]
    assert list(dataset.output.columns) == ["dbId", "description"]
    assert dataset.output["dbId"].tolist() == [1, 2, 3]
    assert "pagination" not in dataset.checkpoint


def test_xledger_empty_last_page() -> None:
    dataset = make_dataset(GraphqlReadSettings(query=XLEDGER_QUERY, pagination=xledger_pagination()))
    run_read(
        dataset,
        [
            xledger_page([("e1", {"dbId": 1}), ("e2", {"dbId": 2})], has_next=True),
            xledger_page([], has_next=False),
        ],
    )
    assert dataset.output["dbId"].tolist() == [1, 2]


def test_xledger_empty_page_with_has_next_true_raises() -> None:
    dataset = make_dataset(GraphqlReadSettings(query=XLEDGER_QUERY, pagination=xledger_pagination()))
    exc_info, _ = run_read_expect_error(dataset, [xledger_page([], has_next=True)])
    assert "no next cursor" in exc_info.value.message


# --------------------------------------------------------------------------- failure, then resume


def test_failure_then_resume_continues_where_it_stopped() -> None:
    dataset = make_dataset(GraphqlReadSettings(query=XLEDGER_QUERY, pagination=xledger_pagination()))

    # Run 1: page 1 ok, page 2 times out.
    run_read_expect_error(
        dataset, [xledger_page([("e1", {"dbId": 1}), ("e2", {"dbId": 2})], has_next=True), requests.Timeout("slow")]
    )
    assert dataset.output["dbId"].tolist() == [1, 2]
    assert dataset.checkpoint["pagination"] == {"strategy": "cursor", "cursor": "e2", "page_size": 2, "page_index": 1}

    # Run 2: resumes at the checkpoint cursor and finishes.
    session = run_read(dataset, [xledger_page([("e3", {"dbId": 3})], has_next=False)])
    assert sent_variables(session) == [{"first": 2, "after": "e2"}]
    assert dataset.output["dbId"].tolist() == [3]
    assert "pagination" not in dataset.checkpoint


def test_graphql_errors_through_read_raise_read_error() -> None:
    body = visma_page([{"associateNo": 1}], has_next=True, end_cursor="c1")
    body["errors"] = [{"message": "boom"}]
    dataset = make_dataset(GraphqlReadSettings(query=VISMA_QUERY, pagination=visma_pagination()))
    exc_info, _ = run_read_expect_error(dataset, [body])
    assert exc_info.value.message == "GraphQL query failed: boom"


# --------------------------------------------------------------------------- incremental watermark


XLEDGER_INCREMENTAL_QUERY = """
query read($first: Int, $after: String, $since: DateTime) {
  customers(first: $first, after: $after, filter: { modifiedAt_gte: $since }) {
    pageInfo { hasNextPage }
    edges { cursor node { dbId modifiedAt } }
  }
}
"""


def test_incremental_watermark_across_runs() -> None:
    dataset = make_dataset(
        GraphqlReadSettings(
            query=XLEDGER_INCREMENTAL_QUERY,
            pagination=xledger_pagination(),
            incremental=IncrementalSettings(variable="since", watermark_path="modifiedAt"),
        )
    )

    # Run 1: full load, no lower bound; watermark = max modifiedAt.
    session = run_read(
        dataset,
        [
            xledger_page(
                [("e1", {"dbId": 1, "modifiedAt": "2026-01-03"}), ("e2", {"dbId": 2, "modifiedAt": "2026-01-01"})], has_next=True
            ),
            xledger_page([("e3", {"dbId": 3, "modifiedAt": "2026-01-02"})], has_next=False),
        ],
    )
    assert sent_variables(session) == [{"first": 2}, {"first": 2, "after": "e2"}]
    assert dataset.checkpoint == {"incremental": {"watermark": "2026-01-03"}}

    # Run 2: lower bound injected; only changed rows come back.
    session = run_read(dataset, [xledger_page([("e9", {"dbId": 1, "modifiedAt": "2026-01-10"})], has_next=False)])
    assert sent_variables(session) == [{"since": "2026-01-03", "first": 2}]
    assert dataset.output["dbId"].tolist() == [1]
    assert dataset.checkpoint == {"incremental": {"watermark": "2026-01-10"}}


def test_incremental_without_pagination_is_rejected() -> None:
    """A watermark on a single-request read would be ignored, so it fails at config time."""
    with pytest.raises(ValueError, match="incremental requires pagination"):
        GraphqlReadSettings(query=XLEDGER_QUERY, incremental=IncrementalSettings(variable="since", watermark_path="modifiedAt"))


def test_incremental_without_pagination_is_rejected_on_deserialize() -> None:
    """A stored config fails to load too (serde wraps the ValueError, message kept)."""
    data = {"query": XLEDGER_QUERY, "incremental": {"variable": "since", "watermark_path": "modifiedAt"}}
    with pytest.raises(DeserializationError, match="incremental requires pagination"):
        GraphqlReadSettings.deserialize(data)


# --------------------------------------------------------------------------- settings


def test_read_settings_with_pagination_roundtrip_serialization() -> None:
    settings = GraphqlReadSettings(
        query=VISMA_QUERY,
        variables={"cid": 1, "filter": {}},
        records_path="data.useCompany.associate.items",
        pagination=visma_pagination(),
        incremental=IncrementalSettings(variable="since", watermark_path="changedDateTime"),
    )
    assert GraphqlReadSettings.deserialize(settings.serialize()) == settings
