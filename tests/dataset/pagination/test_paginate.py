"""
**File:** ``test_paginate.py``
**Region:** ``tests/dataset/pagination``

Unit tests for ``Paginate`` (``pagination/paginate.py``), called directly.
One section per method.

Covers:
    - ``from_dataset``: requires pagination, resolves the strategy.
    - ``records_path``: ``read.records_path`` falling back to ``items_path``.
    - ``_snapshot``: request built from settings, variables / headers deep-copied.
    - ``_validate_checkpoint_strategy``: accepts none / matching, rejects others.
    - ``_concat_frames``: page frames joined, empty pages dropped, empty list.
    - ``drain``: page loop, variable injection, frame collection, mid-run
      checkpoint, GraphQL errors, ``items_path`` checks, ``max_pages``.
    - ``run``: output + checkpoint on success, error mapping and partial
      output on failure.
    - ``run`` + incremental: watermark injected before the first page and
      kept on every page; committed only on full success.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest
import requests
from ds_resource_plugin_py_lib.common.resource.dataset.errors import ReadError
from ds_resource_plugin_py_lib.common.resource.errors import ResourceException
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    AuthenticationError,
    AuthorizationError,
    ConnectionError,
)

from ds_protocol_graphql_py_lib.dataset.graphql import GraphqlDataset, GraphqlReadSettings
from ds_protocol_graphql_py_lib.dataset.incremental import IncrementalSettings, NoIncrementalRules, WatermarkRules
from ds_protocol_graphql_py_lib.dataset.pagination import Paginate
from ds_protocol_graphql_py_lib.dataset.pagination.strategies import CursorPaginationStrategy

from .helpers import (
    URL,
    VISMA_QUERY,
    XLEDGER_QUERY,
    make_dataset,
    mocked_session,
    sent_variables,
    visma_page,
    visma_pagination,
    xledger_page,
    xledger_pagination,
)


def visma_dataset(incremental: IncrementalSettings | None = None, **pagination_overrides: Any) -> GraphqlDataset[Any, Any]:
    return make_dataset(
        GraphqlReadSettings(
            query=VISMA_QUERY,
            variables={"cid": 1, "filter": {}},
            pagination=visma_pagination(**pagination_overrides),
            incremental=incremental,
        )
    )


def page(*associate_nos: int, has_next: bool, end_cursor: Any) -> dict[str, Any]:
    return visma_page([{"associateNo": n} for n in associate_nos], has_next=has_next, end_cursor=end_cursor)


def run(dataset: GraphqlDataset[Any, Any], bodies: list[Any]) -> Any:
    with mocked_session(dataset, bodies) as session:
        Paginate.from_dataset(dataset).run(dataset)
    return session


def run_expect(
    error: type[Exception],
    dataset: GraphqlDataset[Any, Any],
    bodies: list[Any],
) -> tuple[pytest.ExceptionInfo[Any], Any]:
    with mocked_session(dataset, bodies) as session, pytest.raises(error) as exc_info:
        Paginate.from_dataset(dataset).run(dataset)
    return exc_info, session


def drain(
    dataset: GraphqlDataset[Any, Any], bodies: list[Any], frames: list[pd.DataFrame] | None = None
) -> tuple[list[pd.DataFrame], Any]:
    paginate = Paginate.from_dataset(dataset)
    with mocked_session(dataset, bodies) as session:
        result = paginate.drain(dataset, paginate._snapshot(dataset), frames)
    return result, session


# --------------------------------------------------------------------------- from_dataset


def test_from_dataset_builds_from_read_settings() -> None:
    dataset = visma_dataset()
    paginate = Paginate.from_dataset(dataset)
    assert isinstance(paginate.strategy, CursorPaginationStrategy)
    assert paginate.read_settings is dataset.settings.read
    assert paginate.pagination is dataset.settings.read.pagination


def test_from_dataset_without_incremental_uses_no_op_rules() -> None:
    paginate = Paginate.from_dataset(visma_dataset())
    assert isinstance(paginate.incremental_rules, NoIncrementalRules)
    assert paginate.incremental is None


def test_from_dataset_resolves_watermark_rules() -> None:
    settings = IncrementalSettings(variable="since", watermark_path="changed")
    paginate = Paginate.from_dataset(visma_dataset(incremental=settings))
    assert isinstance(paginate.incremental_rules, WatermarkRules)
    assert paginate.incremental is settings


def test_constructor_defaults_to_no_op_rules() -> None:
    dataset = visma_dataset()
    assert dataset.settings.read is not None
    assert dataset.settings.read.pagination is not None
    paginate = Paginate(
        strategy=CursorPaginationStrategy(),
        read_settings=dataset.settings.read,
        pagination=dataset.settings.read.pagination,
    )
    assert isinstance(paginate.incremental_rules, NoIncrementalRules)


def test_from_dataset_requires_pagination() -> None:
    dataset = make_dataset(GraphqlReadSettings(query="{ a }"))
    with pytest.raises(ValueError, match=r"requires settings\.read\.pagination"):
        Paginate.from_dataset(dataset)


def test_from_dataset_requires_read_settings() -> None:
    dataset = make_dataset(GraphqlReadSettings(query="{ a }"))
    dataset.settings.read = None
    with pytest.raises(ValueError, match=r"requires settings\.read\.pagination"):
        Paginate.from_dataset(dataset)


# --------------------------------------------------------------------------- records_path


def test_records_path_defaults_to_items_path() -> None:
    assert Paginate.from_dataset(visma_dataset()).records_path == "data.useCompany.associate.items"


def test_records_path_prefers_read_records_path() -> None:
    dataset = make_dataset(GraphqlReadSettings(query=VISMA_QUERY, records_path="data.other", pagination=visma_pagination()))
    assert Paginate.from_dataset(dataset).records_path == "data.other"


# --------------------------------------------------------------------------- _snapshot


def test_snapshot_built_from_settings() -> None:
    dataset = make_dataset(
        GraphqlReadSettings(query=VISMA_QUERY, variables={"cid": 1}, operation_name="read", pagination=visma_pagination()),
        headers={"X-Test": "1"},
    )
    snapshot = Paginate.from_dataset(dataset)._snapshot(dataset)
    assert (snapshot.url, snapshot.query, snapshot.operation_name) == (URL, VISMA_QUERY, "read")
    assert snapshot.variables == {"cid": 1}
    assert snapshot.headers == {"X-Test": "1"}


def test_snapshot_without_variables_or_headers() -> None:
    dataset = make_dataset(GraphqlReadSettings(query=VISMA_QUERY, pagination=visma_pagination()))
    snapshot = Paginate.from_dataset(dataset)._snapshot(dataset)
    assert snapshot.variables == {}
    assert snapshot.headers is None


def test_snapshot_deep_copies_variables_and_headers() -> None:
    variables = {"cid": 1, "filter": {"a": [1]}}
    headers = {"X-Test": "1"}
    dataset = make_dataset(
        GraphqlReadSettings(query=VISMA_QUERY, variables=variables, pagination=visma_pagination()), headers=headers
    )
    snapshot = Paginate.from_dataset(dataset)._snapshot(dataset)

    snapshot.variables["filter"]["a"].append(2)
    assert snapshot.headers is not None
    snapshot.headers["X-Test"] = "2"

    assert variables == {"cid": 1, "filter": {"a": [1]}}
    assert headers == {"X-Test": "1"}


# --------------------------------------------------------------------------- _validate_checkpoint_strategy


@pytest.mark.parametrize(
    "checkpoint_slice",
    [
        pytest.param(None, id="no checkpoint"),
        pytest.param({"strategy": "cursor", "cursor": "c1"}, id="matching strategy"),
        pytest.param({"cursor": "c1"}, id="no strategy key"),
    ],
)
def test_validate_checkpoint_strategy_accepts(checkpoint_slice: Any) -> None:
    Paginate._validate_checkpoint_strategy(checkpoint_slice, "cursor")


def test_validate_checkpoint_strategy_mismatch_raises() -> None:
    with pytest.raises(ValueError, match="Checkpoint pagination strategy 'offset' does not match configured strategy 'cursor'"):
        Paginate._validate_checkpoint_strategy({"strategy": "offset"}, "cursor")


@pytest.mark.parametrize(("value", "type_name"), [("bad", "str"), (["c1"], "list"), (1, "int")])
def test_validate_checkpoint_strategy_not_a_dict_raises(value: Any, type_name: str) -> None:
    with pytest.raises(TypeError, match=rf"must be a dict \(got {type_name}\)"):
        Paginate._validate_checkpoint_strategy(value, "cursor")


# --------------------------------------------------------------------------- _concat_frames


def test_concat_frames_joins_pages_with_continuous_index() -> None:
    result = Paginate._concat_frames([pd.DataFrame([{"id": 1}, {"id": 2}]), pd.DataFrame([{"id": 3}])])
    assert result["id"].tolist() == [1, 2, 3]
    assert result.index.tolist() == [0, 1, 2]


def test_concat_frames_drops_empty_pages() -> None:
    result = Paginate._concat_frames([pd.DataFrame([{"id": 1}]), pd.DataFrame(), pd.DataFrame([{"id": 2}])])
    assert result["id"].tolist() == [1, 2]


def test_concat_frames_empty_page_with_columns_does_not_change_dtypes() -> None:
    empty_object_columns = pd.DataFrame(columns=["id"])
    result = Paginate._concat_frames([pd.DataFrame([{"id": 1}]), empty_object_columns])
    assert result["id"].dtype == "int64"


def test_concat_frames_union_of_columns() -> None:
    result = Paginate._concat_frames([pd.DataFrame([{"id": 1}]), pd.DataFrame([{"id": 2, "email": "e"}])])
    assert list(result.columns) == ["id", "email"]
    assert result["email"].isna().tolist() == [True, False]


@pytest.mark.parametrize("frames", [[], [pd.DataFrame(), pd.DataFrame()]], ids=["no pages", "only empty pages"])
def test_concat_frames_without_rows_is_empty(frames: list[pd.DataFrame]) -> None:
    result = Paginate._concat_frames(frames)
    assert isinstance(result, pd.DataFrame)
    assert result.empty


# --------------------------------------------------------------------------- drain


def test_drain_injects_variables_per_page() -> None:
    _, session = drain(
        visma_dataset(),
        [page(1, 2, has_next=True, end_cursor="c1"), page(3, has_next=False, end_cursor=None)],
    )
    assert sent_variables(session) == [
        {"cid": 1, "filter": {}, "first": 2},
        {"cid": 1, "filter": {}, "first": 2, "after": "c1"},
    ]


def test_drain_posts_url_headers_and_payload() -> None:
    dataset = make_dataset(
        GraphqlReadSettings(query=VISMA_QUERY, operation_name="read", pagination=visma_pagination()),
        headers={"X-Test": "1"},
    )
    _, session = drain(dataset, [page(1, has_next=False, end_cursor=None)])
    call = session.post.call_args
    assert call.kwargs["url"] == URL
    assert call.kwargs["headers"] == {"X-Test": "1"}
    assert call.kwargs["json"] == {"query": VISMA_QUERY, "variables": {"first": 2}, "operationName": "read"}


def test_drain_returns_one_frame_per_page() -> None:
    frames, _ = drain(visma_dataset(), [page(1, 2, has_next=True, end_cursor="c1"), page(3, has_next=False, end_cursor=None)])
    assert [f["associateNo"].tolist() for f in frames] == [[1, 2], [3]]


def test_drain_appends_to_given_frames_list() -> None:
    frames: list[pd.DataFrame] = [pd.DataFrame([{"associateNo": 0}])]
    result, _ = drain(visma_dataset(), [page(1, has_next=False, end_cursor=None)], frames)
    assert result is frames
    assert len(frames) == 2


def test_drain_creates_frames_list_when_omitted() -> None:
    frames, _ = drain(visma_dataset(), [page(1, has_next=False, end_cursor=None)])
    assert len(frames) == 1


def test_drain_uses_records_path_for_frames() -> None:
    dataset = make_dataset(
        GraphqlReadSettings(query=XLEDGER_QUERY, pagination=xledger_pagination()),
    )
    frames, _ = drain(dataset, [xledger_page([("e1", {"dbId": 1})], has_next=False)])
    assert list(frames[0].columns) == ["dbId"]


def test_drain_stops_when_strategy_says_stop() -> None:
    dataset = visma_dataset(cursor={"has_next_path": None})
    _, session = drain(dataset, [page(has_next=True, end_cursor="c1")])
    assert session.post.call_count == 1


def test_drain_writes_checkpoint_after_each_page() -> None:
    dataset = visma_dataset()
    drain(dataset, [page(1, has_next=True, end_cursor="c1"), page(2, has_next=False, end_cursor=None)])
    # Written after page 1, not after the last page (drain does not clear it; run does).
    assert dataset.checkpoint["pagination"] == {"strategy": "cursor", "cursor": "c1", "page_size": 2, "page_index": 1}


def test_drain_resumes_from_checkpoint() -> None:
    dataset = visma_dataset()
    dataset.checkpoint["pagination"] = {"strategy": "cursor", "cursor": "c5", "page_size": 50, "page_index": 5}
    _, session = drain(dataset, [page(6, has_next=False, end_cursor=None)])
    assert sent_variables(session) == [{"cid": 1, "filter": {}, "first": 50, "after": "c5"}]


def test_drain_requires_deserializer() -> None:
    dataset = visma_dataset()
    dataset.deserializer = None
    with pytest.raises(ValueError, match=r"requires dataset\.deserializer"):
        drain(dataset, [])


def test_drain_raises_on_graphql_errors() -> None:
    body = page(1, has_next=True, end_cursor="c1")
    body["errors"] = [{"message": "boom"}]
    with pytest.raises(ReadError, match="GraphQL query failed: boom"):
        drain(visma_dataset(), [body])


def test_drain_raises_on_http_error_status() -> None:
    dataset = visma_dataset()
    paginate = Paginate.from_dataset(dataset)
    with mocked_session(dataset, [page(1, has_next=False, end_cursor=None)]) as session:
        session.post.side_effect = None
        session.post.return_value.raise_for_status.side_effect = requests.HTTPError("401 Client Error")
        with pytest.raises(requests.HTTPError):
            paginate.drain(dataset, paginate._snapshot(dataset))


def test_drain_items_path_parent_null_raises() -> None:
    with pytest.raises(ValueError, match="missing in response"):
        drain(visma_dataset(), [{"data": {"useCompany": None}}])


def test_drain_items_key_missing_raises() -> None:
    body = page(has_next=False, end_cursor=None)
    del body["data"]["useCompany"]["associate"]["items"]
    with pytest.raises(ValueError, match="missing in response"):
        drain(visma_dataset(), [body])


def test_drain_null_items_is_an_empty_page() -> None:
    """Visma returns ``items: null`` (not ``[]``) when nothing matches."""
    body = page(has_next=False, end_cursor=None)
    body["data"]["useCompany"]["associate"]["items"] = None
    frames, session = drain(visma_dataset(), [body])
    assert session.post.call_count == 1
    assert all(frame.empty for frame in frames)


def test_drain_items_path_not_a_list_raises() -> None:
    body = page(has_next=False, end_cursor=None)
    body["data"]["useCompany"]["associate"]["items"] = {"not": "a list"}
    with pytest.raises(TypeError, match="did not resolve to a list"):
        drain(visma_dataset(), [body])


def test_drain_validates_checkpoint_before_first_request() -> None:
    dataset = visma_dataset()
    dataset.checkpoint["pagination"] = {"strategy": "offset"}
    with pytest.raises(ValueError, match="does not match configured strategy"):
        drain(dataset, [])


def test_drain_max_pages_exceeded_raises() -> None:
    dataset = visma_dataset(max_pages=2)
    frames: list[pd.DataFrame] = []
    with pytest.raises(ReadError, match="exceeded max_pages=2") as exc_info:
        drain(dataset, [page(1, has_next=True, end_cursor="c1"), page(2, has_next=True, end_cursor="c2")], frames)
    assert exc_info.value.status_code == 400
    assert exc_info.value.details == {"type": dataset.type.value, "strategy": "cursor", "max_pages": 2}
    assert len(frames) == 2


def test_drain_max_pages_reached_with_natural_stop_succeeds() -> None:
    frames, _ = drain(
        visma_dataset(max_pages=2), [page(1, has_next=True, end_cursor="c1"), page(2, has_next=False, end_cursor=None)]
    )
    assert len(frames) == 2


def test_drain_max_pages_counts_pages_from_checkpoint() -> None:
    dataset = visma_dataset(max_pages=2)
    dataset.checkpoint["pagination"] = {"strategy": "cursor", "cursor": "c1", "page_size": 2, "page_index": 1}
    with (
        mocked_session(dataset, [page(2, has_next=True, end_cursor="c2")]) as session,
        pytest.raises(ReadError, match="exceeded max_pages=2"),
    ):
        paginate = Paginate.from_dataset(dataset)
        paginate.drain(dataset, paginate._snapshot(dataset))
    assert session.post.call_count == 1


def test_drain_max_pages_already_reached_sends_nothing() -> None:
    dataset = visma_dataset(max_pages=2)
    dataset.checkpoint["pagination"] = {"strategy": "cursor", "cursor": "c2", "page_size": 2, "page_index": 2}
    with mocked_session(dataset, []) as session, pytest.raises(ReadError, match="exceeded max_pages=2"):
        paginate = Paginate.from_dataset(dataset)
        paginate.drain(dataset, paginate._snapshot(dataset))
    assert session.post.call_count == 0


# --------------------------------------------------------------------------- run: success


def test_run_sets_output_and_clears_checkpoint() -> None:
    dataset = visma_dataset()
    run(dataset, [page(1, 2, has_next=True, end_cursor="c1"), page(3, has_next=False, end_cursor=None)])
    assert dataset.output["associateNo"].tolist() == [1, 2, 3]
    assert "pagination" not in dataset.checkpoint


def test_run_keeps_other_checkpoint_keys() -> None:
    dataset = visma_dataset()
    dataset.checkpoint["incremental"] = {"watermark": "2026-01-01"}
    run(dataset, [page(1, has_next=False, end_cursor=None)])
    assert dataset.checkpoint == {"incremental": {"watermark": "2026-01-01"}}


def test_run_without_rows_sets_empty_output() -> None:
    dataset = visma_dataset()
    run(dataset, [page(has_next=False, end_cursor=None)])
    assert dataset.output.empty


# --------------------------------------------------------------------------- run: failure keeps partial output + checkpoint


def test_run_failure_keeps_partial_output_and_checkpoint() -> None:
    dataset = visma_dataset()
    run_expect(
        ReadError,
        dataset,
        [page(1, 2, has_next=True, end_cursor="c1"), page(3, 4, has_next=True, end_cursor="c2"), requests.Timeout("slow")],
    )
    assert dataset.output["associateNo"].tolist() == [1, 2, 3, 4]
    assert dataset.checkpoint["pagination"] == {"strategy": "cursor", "cursor": "c2", "page_size": 2, "page_index": 2}


def test_run_failure_on_first_page_sets_empty_output() -> None:
    dataset = visma_dataset()
    run_expect(ReadError, dataset, [requests.Timeout("slow")])
    assert dataset.output.empty
    assert "pagination" not in dataset.checkpoint


# --------------------------------------------------------------------------- run: error mapping


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(AuthenticationError(message="bad token"), id="AuthenticationError"),
        pytest.param(AuthorizationError(message="forbidden"), id="AuthorizationError"),
        pytest.param(ConnectionError(message="lost"), id="ConnectionError"),
        pytest.param(ReadError(message="already a read error", status_code=418), id="ReadError"),
    ],
)
def test_run_reraises_linked_service_and_read_errors(error: Exception) -> None:
    dataset = visma_dataset()
    exc_info, _ = run_expect(type(error), dataset, [page(1, has_next=True, end_cursor="c1"), error])
    assert exc_info.value is error
    assert dataset.output["associateNo"].tolist() == [1]


def test_run_wraps_resource_exception_keeping_status_and_details() -> None:
    dataset = visma_dataset()
    error = ResourceException(message="upstream", status_code=503, details={"a": 1})
    exc_info, _ = run_expect(ReadError, dataset, [page(1, has_next=True, end_cursor="c1"), error])
    assert exc_info.value.message == "upstream"
    assert exc_info.value.status_code == 503
    assert exc_info.value.details == {"a": 1, "type": dataset.type.value}
    assert exc_info.value.__cause__ is error
    assert dataset.output["associateNo"].tolist() == [1]


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(ValueError("bad value"), id="ValueError"),
        pytest.param(TypeError("bad type"), id="TypeError"),
        pytest.param(KeyError("bad key"), id="KeyError"),
        pytest.param(AttributeError("bad attr"), id="AttributeError"),
    ],
)
def test_run_maps_value_errors_to_400(error: Exception) -> None:
    dataset = visma_dataset()
    exc_info, _ = run_expect(ReadError, dataset, [page(1, has_next=True, end_cursor="c1"), error])
    assert exc_info.value.message == str(error)
    assert exc_info.value.status_code == 400
    assert exc_info.value.details == {"type": dataset.type.value, "url": URL}
    assert exc_info.value.__cause__ is error
    assert dataset.output["associateNo"].tolist() == [1]


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(requests.ConnectionError("network down"), id="requests.ConnectionError"),
        pytest.param(requests.HTTPError("500 Server Error"), id="requests.HTTPError"),
        pytest.param(RuntimeError("unexpected"), id="RuntimeError"),
    ],
)
def test_run_wraps_other_exceptions_as_500(error: Exception) -> None:
    dataset = visma_dataset()
    exc_info, _ = run_expect(ReadError, dataset, [page(1, has_next=True, end_cursor="c1"), error])
    assert exc_info.value.message == f"Failed to read from GraphQL: {error}"
    assert exc_info.value.status_code == 500
    assert exc_info.value.details == {"type": dataset.type.value, "url": URL}
    assert exc_info.value.__cause__ is error
    assert dataset.output["associateNo"].tolist() == [1]


def test_run_strategy_errors_become_read_error_400() -> None:
    """Cursor strategy ValueError (repeated cursor) surfaces as ReadError 400 with partial output."""
    dataset = visma_dataset()
    exc_info, _ = run_expect(
        ReadError, dataset, [page(1, has_next=True, end_cursor="c1"), page(2, has_next=True, end_cursor="c1")]
    )
    assert "made no progress" in exc_info.value.message
    assert exc_info.value.status_code == 400
    assert dataset.output["associateNo"].tolist() == [1, 2]


def test_run_missing_deserializer_becomes_read_error() -> None:
    dataset = visma_dataset()
    paginate = Paginate.from_dataset(dataset)
    dataset.deserializer = None
    with pytest.raises(ReadError, match=r"requires dataset\.deserializer") as exc_info:
        paginate.run(dataset)
    assert exc_info.value.status_code == 400
    assert dataset.output.empty


# --------------------------------------------------------------------------- run: incremental watermark

INCREMENTAL = IncrementalSettings(variable="since", watermark_path="changed")


def changed_page(*changed: str, has_next: bool, end_cursor: Any) -> dict[str, Any]:
    rows = [{"associateNo": i, "changed": c} for i, c in enumerate(changed, start=1)]
    return visma_page(rows, has_next=has_next, end_cursor=end_cursor)


def test_run_first_run_sends_no_watermark_and_commits_max() -> None:
    dataset = visma_dataset(incremental=INCREMENTAL)
    session = run(
        dataset,
        [
            changed_page("2026-01-02", "2026-01-05", has_next=True, end_cursor="c1"),
            changed_page("2026-01-03", has_next=False, end_cursor=None),
        ],
    )
    assert all("since" not in v for v in sent_variables(session))
    assert dataset.checkpoint == {"incremental": {"watermark": "2026-01-05"}}


def test_run_injects_watermark_on_every_page() -> None:
    dataset = visma_dataset(incremental=INCREMENTAL)
    dataset.checkpoint["incremental"] = {"watermark": "2026-01-01"}
    session = run(
        dataset,
        [
            changed_page("2026-01-02", has_next=True, end_cursor="c1"),
            changed_page("2026-01-03", has_next=False, end_cursor=None),
        ],
    )
    assert sent_variables(session) == [
        {"cid": 1, "filter": {}, "since": "2026-01-01", "first": 2},
        {"cid": 1, "filter": {}, "since": "2026-01-01", "first": 2, "after": "c1"},
    ]
    assert dataset.checkpoint == {"incremental": {"watermark": "2026-01-03"}}


def test_run_without_new_rows_keeps_watermark() -> None:
    dataset = visma_dataset(incremental=INCREMENTAL)
    dataset.checkpoint["incremental"] = {"watermark": "2026-01-01"}
    run(dataset, [changed_page(has_next=False, end_cursor=None)])
    assert dataset.checkpoint == {"incremental": {"watermark": "2026-01-01"}}


def test_run_null_items_keeps_watermark() -> None:
    """Nothing after the watermark: Visma sends ``items: null``; the run succeeds and keeps the watermark."""
    dataset = visma_dataset(incremental=INCREMENTAL)
    dataset.checkpoint["incremental"] = {"watermark": "2026-01-01"}
    body = changed_page(has_next=False, end_cursor=None)
    body["data"]["useCompany"]["associate"]["items"] = None
    run(dataset, [body])
    assert dataset.output.empty
    assert dataset.checkpoint == {"incremental": {"watermark": "2026-01-01"}}


def test_run_commit_failure_keeps_pagination_checkpoint() -> None:
    """Non-comparable watermark values: commit raises after a full drain; the resume point is kept."""
    dataset = visma_dataset(incremental=IncrementalSettings(variable="since", watermark_path="changed"))
    dataset.checkpoint["incremental"] = {"watermark": "2026-01-01"}
    bodies = [
        visma_page([{"associateNo": 1, "changed": "2026-01-05"}], has_next=True, end_cursor="c1"),
        visma_page([{"associateNo": 2, "changed": 7}], has_next=False, end_cursor=None),
    ]
    exc_info, _ = run_expect(ReadError, dataset, bodies)
    assert exc_info.value.status_code == 400
    assert "not comparable" in exc_info.value.message
    assert dataset.output["associateNo"].tolist() == [1, 2]
    assert dataset.checkpoint == {
        "incremental": {"watermark": "2026-01-01"},
        "pagination": {"strategy": "cursor", "cursor": "c1", "page_size": 2, "page_index": 1},
    }


def test_run_failure_does_not_advance_watermark() -> None:
    dataset = visma_dataset(incremental=INCREMENTAL)
    dataset.checkpoint["incremental"] = {"watermark": "2026-01-01"}
    run_expect(ReadError, dataset, [changed_page("2026-01-09", has_next=True, end_cursor="c1"), requests.Timeout("slow")])
    assert dataset.output["changed"].tolist() == ["2026-01-09"]
    assert dataset.checkpoint == {
        "incremental": {"watermark": "2026-01-01"},
        "pagination": {"strategy": "cursor", "cursor": "c1", "page_size": 2, "page_index": 1},
    }


def test_run_resume_keeps_old_watermark_then_commits() -> None:
    """A resumed run uses the same lower bound as the failed run; watermark advances only at the end."""
    dataset = visma_dataset(incremental=INCREMENTAL)
    dataset.checkpoint["incremental"] = {"watermark": "2026-01-01"}
    dataset.checkpoint["pagination"] = {"strategy": "cursor", "cursor": "c1", "page_size": 2, "page_index": 1}
    session = run(dataset, [changed_page("2026-01-07", has_next=False, end_cursor=None)])
    assert sent_variables(session) == [{"cid": 1, "filter": {}, "since": "2026-01-01", "first": 2, "after": "c1"}]
    assert dataset.checkpoint == {"incremental": {"watermark": "2026-01-07"}}


def test_run_watermark_values_not_comparable_raise_read_error() -> None:
    dataset = make_dataset(
        GraphqlReadSettings(query=VISMA_QUERY, pagination=visma_pagination(), incremental=INCREMENTAL),
    )
    body = visma_page(
        [{"associateNo": 1, "changed": "2026-01-01"}, {"associateNo": 2, "changed": 5}], has_next=False, end_cursor=None
    )
    exc_info, _ = run_expect(ReadError, dataset, [body])
    assert "not comparable" in exc_info.value.message
    assert exc_info.value.status_code == 400
    assert "incremental" not in dataset.checkpoint
    assert len(dataset.output) == 2
