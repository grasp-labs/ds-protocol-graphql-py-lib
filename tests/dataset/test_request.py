"""
**File:** ``test_request.py``
**Region:** ``tests/dataset``

Unit tests for ``GraphqlRequestSnapshot`` (``dataset/request.py``).

Covers:
    - ``with_variables``: merge, injected values win, original untouched.
    - ``to_payload``: ``variables`` / ``operationName`` only when set.
    - The snapshot is frozen.
"""

from __future__ import annotations

import dataclasses

import pytest

from ds_protocol_graphql_py_lib.dataset.request import GraphqlRequestSnapshot

URL = "https://api.example/graphql"


def snapshot(**kwargs: object) -> GraphqlRequestSnapshot:
    return GraphqlRequestSnapshot(url=URL, query="{ a }", **kwargs)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- with_variables


def test_with_variables_adds_new_variables() -> None:
    result = snapshot(variables={"cid": 1}).with_variables({"first": 100, "after": "c1"})
    assert result.variables == {"cid": 1, "first": 100, "after": "c1"}


def test_with_variables_injected_values_win() -> None:
    result = snapshot(variables={"after": "", "first": 10}).with_variables({"after": "c1"})
    assert result.variables == {"after": "c1", "first": 10}


def test_with_variables_does_not_modify_original() -> None:
    original = snapshot(variables={"cid": 1})
    original.with_variables({"after": "c1"})
    assert original.variables == {"cid": 1}


def test_with_variables_empty_values_keeps_variables() -> None:
    original = snapshot(variables={"cid": 1})
    assert original.with_variables({}).variables == {"cid": 1}


def test_with_variables_keeps_other_fields() -> None:
    original = snapshot(operation_name="read", headers={"X": "1"})
    result = original.with_variables({"after": "c1"})
    assert (result.url, result.query, result.operation_name, result.headers) == (URL, "{ a }", "read", {"X": "1"})


def test_with_variables_on_default_variables() -> None:
    assert snapshot().with_variables({"first": 5}).variables == {"first": 5}


# --------------------------------------------------------------------------- to_payload


def test_to_payload_query_only() -> None:
    assert snapshot().to_payload() == {"query": "{ a }"}


def test_to_payload_with_variables_and_operation_name() -> None:
    payload = snapshot(variables={"first": 1}, operation_name="read").to_payload()
    assert payload == {"query": "{ a }", "variables": {"first": 1}, "operationName": "read"}


@pytest.mark.parametrize("operation_name", [None, ""])
def test_to_payload_omits_empty_operation_name(operation_name: str | None) -> None:
    assert "operationName" not in snapshot(operation_name=operation_name).to_payload()


def test_to_payload_omits_empty_variables() -> None:
    assert "variables" not in snapshot(variables={}).to_payload()


def test_to_payload_does_not_include_headers() -> None:
    assert "headers" not in snapshot(headers={"Authorization": "x"}).to_payload()


# --------------------------------------------------------------------------- immutability


def test_snapshot_is_frozen() -> None:
    with pytest.raises(dataclasses.FrozenInstanceError):
        snapshot().variables = {"x": 1}  # type: ignore[misc]
