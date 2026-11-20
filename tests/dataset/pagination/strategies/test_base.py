"""
**File:** ``test_base.py``
**Region:** ``tests/dataset/pagination/strategies``

Unit tests for ``PageState`` and ``PaginationStrategyHandler``
(``strategies/base.py``).
"""

from __future__ import annotations

import pytest

from ds_protocol_graphql_py_lib.dataset.pagination.strategies import PageState, PaginationStrategyHandler


def test_page_state_defaults() -> None:
    state = PageState()
    assert state.values == {}
    assert state.page_index == 0


def test_page_state_values_are_not_shared() -> None:
    first, second = PageState(), PageState()
    first.values["cursor"] = "c1"
    assert second.values == {}


def test_handler_is_abstract() -> None:
    with pytest.raises(TypeError, match="abstract"):
        PaginationStrategyHandler()  # type: ignore[abstract]


def test_handler_requires_all_hooks() -> None:
    assert PaginationStrategyHandler.__abstractmethods__ == {
        "initial_state",
        "inject",
        "advance",
        "should_stop",
        "to_checkpoint",
        "from_checkpoint",
    }
