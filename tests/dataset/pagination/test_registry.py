"""
**File:** ``test_registry.py``
**Region:** ``tests/dataset/pagination``

Unit tests for the strategy registry (``pagination/registry.py``).

Covers:
    - ``register`` stores one handler instance and returns the class unchanged.
    - Duplicate registration raises.
    - ``get_strategy`` resolves registered handlers and raises for unknown ones.
    - The cursor strategy is registered on import.
"""

from __future__ import annotations

from typing import Any

import pytest

from ds_protocol_graphql_py_lib.dataset.pagination import PaginationStrategy, get_strategy, register
from ds_protocol_graphql_py_lib.dataset.pagination import registry as registry_module
from ds_protocol_graphql_py_lib.dataset.pagination.strategies import CursorPaginationStrategy


@pytest.fixture
def empty_registry(monkeypatch: pytest.MonkeyPatch) -> dict[Any, Any]:
    """Swap in an empty registry so tests can register without touching the real one."""
    registry: dict[Any, Any] = {}
    monkeypatch.setattr(registry_module, "_STRATEGY_REGISTRY", registry)
    return registry


class FakeHandler:
    pass


def test_register_stores_an_instance(empty_registry: dict[Any, Any]) -> None:
    register(PaginationStrategy.CURSOR)(FakeHandler)
    assert isinstance(empty_registry[PaginationStrategy.CURSOR], FakeHandler)


def test_register_returns_class_unchanged(empty_registry: dict[Any, Any]) -> None:
    assert register(PaginationStrategy.CURSOR)(FakeHandler) is FakeHandler


def test_get_strategy_returns_the_same_instance(empty_registry: dict[Any, Any]) -> None:
    register(PaginationStrategy.CURSOR)(FakeHandler)
    assert get_strategy(PaginationStrategy.CURSOR) is get_strategy(PaginationStrategy.CURSOR)


def test_register_duplicate_raises(empty_registry: dict[Any, Any]) -> None:
    register(PaginationStrategy.CURSOR)(FakeHandler)
    with pytest.raises(ValueError, match="already registered: cursor"):
        register(PaginationStrategy.CURSOR)(type("Dup", (), {}))
    assert isinstance(empty_registry[PaginationStrategy.CURSOR], FakeHandler)


def test_get_strategy_unknown_raises(empty_registry: dict[Any, Any]) -> None:
    with pytest.raises(KeyError, match="No pagination strategy registered for 'cursor'"):
        get_strategy(PaginationStrategy.CURSOR)


def test_cursor_strategy_is_registered_on_import() -> None:
    assert isinstance(get_strategy(PaginationStrategy.CURSOR), CursorPaginationStrategy)


def test_real_registry_rejects_duplicate_cursor() -> None:
    with pytest.raises(ValueError, match="already registered"):
        register(PaginationStrategy.CURSOR)(type("Dup", (), {}))
