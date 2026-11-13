"""
**File:** ``test_registry.py``
**Region:** ``tests/dataset/incremental``

Unit tests for the incremental rules registry (``incremental/registry.py``).
"""

from __future__ import annotations

from typing import Any

import pytest

from ds_protocol_graphql_py_lib.dataset.incremental import (
    IncrementalStrategy,
    NoIncrementalRules,
    WatermarkRules,
    get_rules,
    register,
)
from ds_protocol_graphql_py_lib.dataset.incremental import registry as registry_module


@pytest.fixture
def empty_registry(monkeypatch: pytest.MonkeyPatch) -> dict[Any, Any]:
    """Swap in an empty registry so tests can register without touching the real one."""
    registry: dict[Any, Any] = {}
    monkeypatch.setattr(registry_module, "_RULES_REGISTRY", registry)
    return registry


class FakeRules:
    pass


def test_register_stores_an_instance_and_returns_class(empty_registry: dict[Any, Any]) -> None:
    assert register(IncrementalStrategy.WATERMARK)(FakeRules) is FakeRules
    assert isinstance(empty_registry[IncrementalStrategy.WATERMARK], FakeRules)


def test_register_duplicate_raises(empty_registry: dict[Any, Any]) -> None:
    register(IncrementalStrategy.WATERMARK)(FakeRules)
    with pytest.raises(ValueError, match="already registered: watermark"):
        register(IncrementalStrategy.WATERMARK)(type("Dup", (), {}))


def test_get_rules_unknown_raises(empty_registry: dict[Any, Any]) -> None:
    with pytest.raises(KeyError, match="No incremental strategy registered for 'watermark'"):
        get_rules(IncrementalStrategy.WATERMARK)


def test_watermark_rules_registered_on_import() -> None:
    assert isinstance(get_rules(IncrementalStrategy.WATERMARK), WatermarkRules)


def test_none_rules_are_not_registered() -> None:
    """NoIncrementalRules is Paginate's fallback, not a selectable strategy."""
    assert not isinstance(get_rules(IncrementalStrategy.WATERMARK), NoIncrementalRules)
