"""
**File:** ``test_json_utils.py``
**Region:** ``tests/utils``

Test restricted dotted-path JSON helpers.
"""

from __future__ import annotations

import pytest

from ds_protocol_graphql_py_lib.utils.json_utils import get_list, get_path

BODY = {"data": {"x": {"items": [{"id": 1}, {"id": 2}], "count": 2, "none": None}}}


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("$", BODY),
        ("", BODY),
        ("data.x.count", 2),
        ("data.x.items.1.id", 2),
        ("data.x.items.-1.id", 2),
        ("data.x.items.-2.id", 1),
        ("data.x.items.-3.id", None),
        ("data.x.missing", None),
        ("data.x.items.9", None),
        ("data.x.items.notint", None),
        ("data.x.count.deeper", None),
    ],
)
def test_get_path(path: str, expected: object) -> None:
    assert get_path(BODY, path) == expected


def test_get_list_returns_list() -> None:
    assert get_list(BODY, "data.x.items") == [{"id": 1}, {"id": 2}]


def test_get_list_root_list() -> None:
    assert get_list([1, 2], "$") == [1, 2]


@pytest.mark.parametrize("path", ["data.x.missing", "data.x.none"])
def test_get_list_missing_or_null_raises(path: str) -> None:
    with pytest.raises(ValueError, match="missing or null"):
        get_list(BODY, path)


def test_get_list_non_list_raises() -> None:
    with pytest.raises(TypeError, match="did not resolve to a list"):
        get_list(BODY, "data.x.count")


def test_get_path_negative_index_on_empty_list_is_none() -> None:
    assert get_path({"data": {"edges": []}}, "data.edges.-1.cursor") is None
