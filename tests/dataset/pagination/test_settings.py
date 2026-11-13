"""
**File:** ``test_settings.py``
**Region:** ``tests/dataset/pagination``

Unit tests for ``CursorPaginationSettings`` and ``PaginationSettings``
(``pagination/settings.py``).

Covers:
    - Cursor defaults (``after`` / ``first`` / 100, no ``has_next_path``).
    - The nest matching ``strategy`` is required.
    - ``strategy_config`` returns that nest.
    - Serialize / deserialize round trip.
"""

from __future__ import annotations

import pytest

from ds_protocol_graphql_py_lib.dataset.pagination import (
    CursorPaginationSettings,
    PaginationSettings,
    PaginationStrategy,
)

from .helpers import visma_pagination, xledger_pagination

# --------------------------------------------------------------------------- CursorPaginationSettings


def test_cursor_settings_defaults() -> None:
    settings = CursorPaginationSettings(cursor_path="data.x.pageInfo.endCursor")
    assert settings.cursor_variable == "after"
    assert settings.page_size_variable == "first"
    assert settings.page_size == 100
    assert settings.has_next_path is None


def test_cursor_settings_requires_cursor_path() -> None:
    with pytest.raises(TypeError):
        CursorPaginationSettings()  # type: ignore[call-arg]


def test_cursor_settings_roundtrip_serialization() -> None:
    settings = CursorPaginationSettings(
        cursor_path="data.x.edges.-1.cursor",
        cursor_variable="cursor",
        page_size_variable=None,
        page_size=None,
        has_next_path="data.x.pageInfo.hasNextPage",
    )
    assert CursorPaginationSettings.deserialize(settings.serialize()) == settings


# --------------------------------------------------------------------------- PaginationSettings


def test_pagination_settings_defaults() -> None:
    settings = PaginationSettings(
        strategy=PaginationStrategy.CURSOR,
        items_path="data.x.items",
        cursor=CursorPaginationSettings(cursor_path="data.x.pageInfo.endCursor"),
    )
    assert settings.max_pages is None


def test_pagination_settings_require_matching_nest() -> None:
    with pytest.raises(ValueError, match=r"PaginationSettings\.cursor is required when strategy is 'cursor'"):
        PaginationSettings(strategy=PaginationStrategy.CURSOR, items_path="data.x")


def test_strategy_config_returns_cursor_nest() -> None:
    cursor = CursorPaginationSettings(cursor_path="data.x.pageInfo.endCursor")
    settings = PaginationSettings(strategy=PaginationStrategy.CURSOR, items_path="data.x.items", cursor=cursor)
    assert settings.strategy_config is cursor


@pytest.mark.parametrize("build", [visma_pagination, xledger_pagination], ids=["visma", "xledger"])
def test_pagination_settings_roundtrip_serialization(build: object) -> None:
    settings = build(max_pages=5)  # type: ignore[operator]
    restored = PaginationSettings.deserialize(settings.serialize())
    assert restored == settings
    assert restored.strategy is PaginationStrategy.CURSOR
    assert isinstance(restored.strategy_config, CursorPaginationSettings)


def test_pagination_settings_deserialize_from_plain_dict() -> None:
    restored = PaginationSettings.deserialize(
        {
            "strategy": "cursor",
            "items_path": "data.customers.edges",
            "cursor": {
                "cursor_path": "data.customers.edges.-1.cursor",
                "has_next_path": "data.customers.pageInfo.hasNextPage",
            },
        }
    )
    assert restored.strategy_config.cursor_path == "data.customers.edges.-1.cursor"
    assert restored.strategy_config.page_size == 100
