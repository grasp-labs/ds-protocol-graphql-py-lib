"""
**File:** ``test_rules.py``
**Region:** ``tests/dataset/incremental``

Unit tests for the ``IncrementalRules`` ABC (``incremental/rules.py``).
"""

from __future__ import annotations

import pytest

from ds_protocol_graphql_py_lib.dataset.incremental import IncrementalRules


def test_rules_are_abstract() -> None:
    with pytest.raises(TypeError, match="abstract"):
        IncrementalRules()  # type: ignore[abstract]


def test_rules_require_prepare_observe_and_commit() -> None:
    assert IncrementalRules.__abstractmethods__ == {"prepare", "observe", "commit"}


def test_rules_without_observe_cannot_be_instantiated() -> None:
    class NoObserve(IncrementalRules):
        def prepare(self, request, checkpoint, settings):  # type: ignore[no-untyped-def]
            return request

        def commit(self, checkpoint, output, settings):  # type: ignore[no-untyped-def]
            return None

    with pytest.raises(TypeError, match="observe"):
        NoObserve()  # type: ignore[abstract]
