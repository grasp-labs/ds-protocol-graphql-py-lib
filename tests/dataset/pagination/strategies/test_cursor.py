"""
**File:** ``test_cursor.py``
**Region:** ``tests/dataset/pagination/strategies``

Unit tests for ``CursorPaginationStrategy`` (``strategies/cursor.py``),
called directly (no dataset, no HTTP). One section per hook.

Covers:
    - ``_read_next_cursor``: missing / ``null`` / ``""`` are "no cursor";
      other values are stringified.
    - ``_read_has_next``: ``None`` when unset; non-boolean raises.
    - ``initial_state``: fresh run vs. resume from checkpoint.
    - ``inject``: page size + cursor variables, custom names, disabled page
      size, injected values win, request not mutated.
    - ``advance``: next cursor, page index, page size kept; missing or
      repeated cursor raises.
    - ``should_stop``: rules with and without ``has_next_path``, last-edge
      ``-1`` paths, ``state`` ignored.
    - ``to_checkpoint`` / ``from_checkpoint`` round trip.
"""

from __future__ import annotations

from typing import Any

import pytest

from ds_protocol_graphql_py_lib.dataset.pagination import CursorPaginationSettings
from ds_protocol_graphql_py_lib.dataset.pagination.strategies import CursorPaginationStrategy, PageState
from ds_protocol_graphql_py_lib.dataset.pagination.strategies.cursor import _read_has_next, _read_next_cursor
from ds_protocol_graphql_py_lib.dataset.request import GraphqlRequestSnapshot

CURSOR_PATH = "data.x.pageInfo.endCursor"
HAS_NEXT_PATH = "data.x.pageInfo.hasNextPage"

# Sentinel: leave the key out of the response entirely.
MISSING = object()

ROWS = [{"id": 1}, {"id": 2}]


def body(*, cursor: Any = MISSING, has_next: Any = MISSING, items: list[Any] | None = None) -> dict[str, Any]:
    """Build a Relay-style response; MISSING leaves the field out."""
    page_info: dict[str, Any] = {}
    if cursor is not MISSING:
        page_info["endCursor"] = cursor
    if has_next is not MISSING:
        page_info["hasNextPage"] = has_next
    return {"data": {"x": {"pageInfo": page_info, "items": items or []}}}


def cfg(
    *,
    has_next_path: str | None = HAS_NEXT_PATH,
    cursor_path: str = CURSOR_PATH,
    **kwargs: Any,
) -> CursorPaginationSettings:
    return CursorPaginationSettings(cursor_path=cursor_path, has_next_path=has_next_path, **kwargs)


def request(variables: dict[str, Any] | None = None) -> GraphqlRequestSnapshot:
    return GraphqlRequestSnapshot(url="https://api.example/graphql", query="{ a }", variables=variables or {})


def should_stop(response: dict[str, Any], items: list[Any], settings: CursorPaginationSettings) -> bool:
    return CursorPaginationStrategy().should_stop(body=response, items=items, state=PageState(), cfg=settings)


@pytest.fixture
def strategy() -> CursorPaginationStrategy:
    return CursorPaginationStrategy()


# --------------------------------------------------------------------------- _read_next_cursor


@pytest.mark.parametrize(
    ("cursor", "expected"),
    [
        pytest.param("c1", "c1", id="string"),
        pytest.param('{"v":2,"k":["5"]}', '{"v":2,"k":["5"]}', id="opaque token"),
        pytest.param(42, "42", id="int stringified"),
        pytest.param(0, "0", id="zero is a cursor"),
        pytest.param(MISSING, None, id="missing"),
        pytest.param(None, None, id="null"),
        pytest.param("", None, id="empty string"),
    ],
)
def test_read_next_cursor(cursor: Any, expected: str | None) -> None:
    assert _read_next_cursor(body(cursor=cursor), cfg()) == expected


def test_read_next_cursor_wrong_path_is_none() -> None:
    assert _read_next_cursor(body(cursor="c1"), cfg(cursor_path="data.nope")) is None


# --------------------------------------------------------------------------- _read_has_next


def test_read_has_next_unset_is_none() -> None:
    assert _read_has_next(body(has_next=True), cfg(has_next_path=None)) is None


@pytest.mark.parametrize("value", [True, False])
def test_read_has_next_bool(value: bool) -> None:
    assert _read_has_next(body(has_next=value), cfg()) is value


@pytest.mark.parametrize(
    ("value", "type_name"),
    [
        pytest.param("true", "str", id="string"),
        pytest.param(1, "int", id="int"),
        pytest.param(None, "NoneType", id="null"),
        pytest.param(MISSING, "NoneType", id="missing"),
        pytest.param({"v": True}, "dict", id="object"),
    ],
)
def test_read_has_next_non_boolean_raises(value: Any, type_name: str) -> None:
    with pytest.raises(TypeError, match=rf"must resolve to a boolean \(got {type_name}\)"):
        _read_has_next(body(has_next=value), cfg())


# --------------------------------------------------------------------------- initial_state


def test_initial_state_fresh_run(strategy: CursorPaginationStrategy) -> None:
    state = strategy.initial_state(cfg(page_size=50), None)
    assert state == PageState(values={"cursor": None, "page_size": 50}, page_index=0)


def test_initial_state_resumes_from_checkpoint(strategy: CursorPaginationStrategy) -> None:
    checkpoint = {"strategy": "cursor", "cursor": "c7", "page_size": 25, "page_index": 7}
    state = strategy.initial_state(cfg(page_size=50), checkpoint)
    assert state == PageState(values={"cursor": "c7", "page_size": 25}, page_index=7)


# --------------------------------------------------------------------------- inject


def test_inject_first_page_sends_page_size_only(strategy: CursorPaginationStrategy) -> None:
    state = strategy.initial_state(cfg(page_size=10), None)
    assert strategy.inject(request(), state, cfg(page_size=10)).variables == {"first": 10}


def test_inject_sends_cursor_after_first_page(strategy: CursorPaginationStrategy) -> None:
    state = PageState(values={"cursor": "c1", "page_size": 10}, page_index=1)
    assert strategy.inject(request(), state, cfg()).variables == {"first": 10, "after": "c1"}


def test_inject_custom_variable_names(strategy: CursorPaginationStrategy) -> None:
    settings = cfg(cursor_variable="cursor", page_size_variable="limit")
    state = PageState(values={"cursor": "c1", "page_size": 10}, page_index=1)
    assert strategy.inject(request(), state, settings).variables == {"limit": 10, "cursor": "c1"}


def test_inject_page_size_variable_disabled(strategy: CursorPaginationStrategy) -> None:
    state = PageState(values={"cursor": "c1", "page_size": 10}, page_index=1)
    assert strategy.inject(request(), state, cfg(page_size_variable=None)).variables == {"after": "c1"}


def test_inject_page_size_none(strategy: CursorPaginationStrategy) -> None:
    settings = cfg(page_size=None)
    state = strategy.initial_state(settings, None)
    assert strategy.inject(request(), state, settings).variables == {}


def test_inject_state_page_size_wins_over_settings(strategy: CursorPaginationStrategy) -> None:
    """A resumed run keeps the page size it started with."""
    state = PageState(values={"cursor": "c1", "page_size": 25}, page_index=3)
    assert strategy.inject(request(), state, cfg(page_size=100)).variables["first"] == 25


def test_inject_falls_back_to_settings_page_size(strategy: CursorPaginationStrategy) -> None:
    state = PageState(values={"cursor": None}, page_index=0)
    assert strategy.inject(request(), state, cfg(page_size=100)).variables == {"first": 100}


def test_inject_overrides_static_variables(strategy: CursorPaginationStrategy) -> None:
    """``after: ""`` in static variables (required cursor) is sent on page 1, then replaced."""
    base = request({"cid": 1, "after": "", "first": 999})
    first = strategy.inject(base, strategy.initial_state(cfg(page_size=10), None), cfg(page_size=10))
    later = strategy.inject(base, PageState(values={"cursor": "c1", "page_size": 10}, page_index=1), cfg())
    assert first.variables == {"cid": 1, "after": "", "first": 10}
    assert later.variables == {"cid": 1, "after": "c1", "first": 10}


def test_inject_does_not_mutate_request(strategy: CursorPaginationStrategy) -> None:
    base = request({"cid": 1})
    strategy.inject(base, PageState(values={"cursor": "c1", "page_size": 10}), cfg())
    assert base.variables == {"cid": 1}


# --------------------------------------------------------------------------- advance


def test_advance_moves_to_next_cursor(strategy: CursorPaginationStrategy) -> None:
    state = PageState(values={"cursor": None, "page_size": 10}, page_index=0)
    nxt = strategy.advance(body=body(cursor="c1"), items=ROWS, state=state, cfg=cfg())
    assert nxt == PageState(values={"cursor": "c1", "page_size": 10}, page_index=1)


def test_advance_does_not_mutate_state(strategy: CursorPaginationStrategy) -> None:
    state = PageState(values={"cursor": "c1", "page_size": 10}, page_index=1)
    strategy.advance(body=body(cursor="c2"), items=ROWS, state=state, cfg=cfg())
    assert state == PageState(values={"cursor": "c1", "page_size": 10}, page_index=1)


def test_advance_keeps_page_size_from_state(strategy: CursorPaginationStrategy) -> None:
    state = PageState(values={"cursor": "c1", "page_size": 25}, page_index=1)
    nxt = strategy.advance(body=body(cursor="c2"), items=ROWS, state=state, cfg=cfg(page_size=100))
    assert nxt.values["page_size"] == 25


def test_advance_stringifies_numeric_cursor(strategy: CursorPaginationStrategy) -> None:
    state = strategy.initial_state(cfg(), None)
    assert strategy.advance(body=body(cursor=42), items=ROWS, state=state, cfg=cfg()).values["cursor"] == "42"


@pytest.mark.parametrize("cursor", [MISSING, None, ""], ids=["missing", "null", "empty string"])
def test_advance_without_cursor_raises(strategy: CursorPaginationStrategy, cursor: Any) -> None:
    state = strategy.initial_state(cfg(), None)
    with pytest.raises(ValueError, match="expected a next cursor but none was found"):
        strategy.advance(body=body(cursor=cursor), items=ROWS, state=state, cfg=cfg())


def test_advance_identical_cursor_raises(strategy: CursorPaginationStrategy) -> None:
    state = PageState(values={"cursor": "c1", "page_size": 10}, page_index=1)
    with pytest.raises(ValueError, match="made no progress"):
        strategy.advance(body=body(cursor="c1"), items=ROWS, state=state, cfg=cfg())


# --------------------------------------------------------------------------- should_stop: has_next_path unset


@pytest.mark.parametrize(
    ("items", "cursor", "expected"),
    [
        pytest.param(ROWS, "c1", False, id="rows + cursor -> continue"),
        pytest.param(ROWS, MISSING, True, id="rows + cursor missing -> stop"),
        pytest.param(ROWS, None, True, id="rows + cursor null -> stop"),
        pytest.param(ROWS, "", True, id="rows + cursor empty string -> stop"),
        pytest.param([], "c1", True, id="empty page + cursor -> stop"),
        pytest.param([], MISSING, True, id="empty page + cursor missing -> stop"),
        pytest.param([], None, True, id="empty page + cursor null -> stop"),
    ],
)
def test_should_stop_without_has_next_path(items: list[Any], cursor: Any, expected: bool) -> None:
    response = body(cursor=cursor, items=items)
    assert should_stop(response, items, cfg(has_next_path=None)) is expected


def test_should_stop_without_has_next_path_ignores_has_next_in_response() -> None:
    """hasNextPage in the body is not read when has_next_path is not configured."""
    response = body(cursor="c1", has_next=False, items=ROWS)
    assert should_stop(response, ROWS, cfg(has_next_path=None)) is False


def test_should_stop_without_has_next_path_numeric_cursor_continues() -> None:
    response = body(cursor=42, items=ROWS)
    assert should_stop(response, ROWS, cfg(has_next_path=None)) is False


# --------------------------------------------------------------------------- should_stop: has_next_path set


@pytest.mark.parametrize(
    ("items", "cursor"),
    [
        pytest.param(ROWS, "c1", id="rows + cursor"),
        pytest.param(ROWS, None, id="rows + cursor null"),
        pytest.param(ROWS, MISSING, id="rows + cursor missing"),
        pytest.param([], "c1", id="empty page + cursor"),
        pytest.param([], None, id="empty page + cursor null"),
    ],
)
def test_should_stop_has_next_false_always_stops(items: list[Any], cursor: Any) -> None:
    response = body(cursor=cursor, has_next=False, items=items)
    assert should_stop(response, items, cfg()) is True


@pytest.mark.parametrize("items", [pytest.param(ROWS, id="rows"), pytest.param([], id="empty page")])
def test_should_stop_has_next_true_with_cursor_continues(items: list[Any]) -> None:
    """has_next_path is authoritative: an empty page with hasNextPage=true and a cursor continues."""
    response = body(cursor="c1", has_next=True, items=items)
    assert should_stop(response, items, cfg()) is False


@pytest.mark.parametrize("cursor", [MISSING, None, ""], ids=["cursor missing", "cursor null", "cursor empty string"])
def test_should_stop_has_next_true_without_cursor_raises(cursor: Any) -> None:
    response = body(cursor=cursor, has_next=True, items=ROWS)
    with pytest.raises(ValueError, match=r"is true but no next cursor was found") as exc_info:
        should_stop(response, ROWS, cfg())
    message = str(exc_info.value)
    assert f"has_next_path '{HAS_NEXT_PATH}'" in message
    assert f"cursor_path='{CURSOR_PATH}'" in message
    assert "page_rows=2" in message


def test_should_stop_has_next_true_with_wrong_cursor_path_raises() -> None:
    """A typo in cursor_path surfaces on the first page instead of silently ending after one page."""
    response = body(cursor="c1", has_next=True, items=ROWS)
    with pytest.raises(ValueError, match=r"no next cursor"):
        should_stop(response, ROWS, cfg(cursor_path="data.x.pageInfo.endCursr"))


@pytest.mark.parametrize("value", ["true", 1, None, MISSING, {"v": True}], ids=["string", "int", "null", "missing", "object"])
def test_should_stop_has_next_non_boolean_raises(value: Any) -> None:
    response = body(cursor="c1", has_next=value, items=ROWS)
    with pytest.raises(TypeError, match=r"must resolve to a boolean"):
        should_stop(response, ROWS, cfg())


# --------------------------------------------------------------------------- should_stop: last-edge (-1) cursor paths (Xledger)


EDGES_CURSOR_PATH = "data.customers.edges.-1.cursor"
EDGES_HAS_NEXT_PATH = "data.customers.pageInfo.hasNextPage"


def edges_body(edges: list[dict[str, Any]], *, has_next: bool) -> dict[str, Any]:
    return {"data": {"customers": {"pageInfo": {"hasNextPage": has_next}, "edges": edges}}}


def test_should_stop_last_edge_cursor_continues() -> None:
    edges = [{"cursor": "6820487", "node": {"dbId": 1}}, {"cursor": "6820488", "node": {"dbId": 2}}]
    settings = cfg(cursor_path=EDGES_CURSOR_PATH, has_next_path=EDGES_HAS_NEXT_PATH)
    assert should_stop(edges_body(edges, has_next=True), edges, settings) is False


def test_should_stop_last_edge_empty_page_has_next_true_raises() -> None:
    settings = cfg(cursor_path=EDGES_CURSOR_PATH, has_next_path=EDGES_HAS_NEXT_PATH)
    with pytest.raises(ValueError, match=r"page_rows=0"):
        should_stop(edges_body([], has_next=True), [], settings)


def test_should_stop_last_edge_empty_page_has_next_false_stops() -> None:
    settings = cfg(cursor_path=EDGES_CURSOR_PATH, has_next_path=EDGES_HAS_NEXT_PATH)
    assert should_stop(edges_body([], has_next=False), [], settings) is True


def test_should_stop_last_edge_empty_page_without_has_next_stops() -> None:
    settings = cfg(cursor_path=EDGES_CURSOR_PATH, has_next_path=None)
    assert should_stop(edges_body([], has_next=True), [], settings) is True


def test_advance_uses_last_edge_cursor(strategy: CursorPaginationStrategy) -> None:
    edges = [{"cursor": "6820487", "node": {"dbId": 1}}, {"cursor": "6820488", "node": {"dbId": 2}}]
    settings = cfg(cursor_path=EDGES_CURSOR_PATH, has_next_path=EDGES_HAS_NEXT_PATH)
    state = strategy.advance(body=edges_body(edges, has_next=True), items=edges, state=PageState(), cfg=settings)
    assert state.values["cursor"] == "6820488"


# --------------------------------------------------------------------------- should_stop: state is ignored


@pytest.mark.parametrize(
    "state",
    [
        pytest.param(PageState(), id="fresh"),
        pytest.param(PageState(values={"cursor": "c1", "page_size": 10}, page_index=5), id="resumed"),
        pytest.param(PageState(values={"cursor": "c1"}, page_index=999), id="same cursor as response"),
    ],
)
def test_should_stop_state_does_not_affect_result(strategy: CursorPaginationStrategy, state: PageState) -> None:
    response = body(cursor="c1", has_next=True, items=ROWS)
    assert strategy.should_stop(body=response, items=ROWS, state=state, cfg=cfg()) is False


# --------------------------------------------------------------------------- to_checkpoint / from_checkpoint


def test_to_checkpoint(strategy: CursorPaginationStrategy) -> None:
    state = PageState(values={"cursor": "c2", "page_size": 10}, page_index=2)
    assert strategy.to_checkpoint(state) == {"strategy": "cursor", "cursor": "c2", "page_size": 10, "page_index": 2}


def test_checkpoint_roundtrip(strategy: CursorPaginationStrategy) -> None:
    state = PageState(values={"cursor": "c2", "page_size": None}, page_index=2)
    assert strategy.from_checkpoint(strategy.to_checkpoint(state)) == state


@pytest.mark.parametrize("missing", ["cursor", "page_size", "page_index"])
def test_from_checkpoint_missing_key_raises(strategy: CursorPaginationStrategy, missing: str) -> None:
    data = {"strategy": "cursor", "cursor": "c2", "page_size": 10, "page_index": 2}
    del data[missing]
    with pytest.raises(KeyError, match=missing):
        strategy.from_checkpoint(data)
