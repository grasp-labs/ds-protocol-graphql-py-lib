"""
**File:** ``test_enums.py``
**Region:** ``tests/dataset/pagination``

Unit tests for ``PaginationStrategy`` (``pagination/enums.py``).
"""

from __future__ import annotations

from ds_protocol_graphql_py_lib.dataset.pagination import PaginationStrategy


def test_cursor_value() -> None:
    assert PaginationStrategy.CURSOR.value == "cursor"


def test_only_cursor_is_supported() -> None:
    assert [s.value for s in PaginationStrategy] == ["cursor"]


def test_constructed_from_string() -> None:
    assert PaginationStrategy("cursor") is PaginationStrategy.CURSOR
