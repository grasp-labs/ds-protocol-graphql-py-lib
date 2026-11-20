"""
**File:** ``test_none.py``
**Region:** ``tests/dataset/incremental/strategies``

Unit tests for ``NoIncrementalRules`` (``strategies/none.py``).
"""

from __future__ import annotations

import pandas as pd

from ds_protocol_graphql_py_lib.dataset.incremental import IncrementalSettings, NoIncrementalRules
from ds_protocol_graphql_py_lib.dataset.request import GraphqlRequestSnapshot

SETTINGS = IncrementalSettings(variable="since", watermark_path="updatedAt")


def test_prepare_returns_request_unchanged() -> None:
    request = GraphqlRequestSnapshot(url="u", query="{ a }", variables={"x": 1})
    checkpoint = {"incremental": {"watermark": "2026-01-01"}}
    assert NoIncrementalRules().prepare(request, checkpoint, SETTINGS) is request


def test_commit_does_not_touch_checkpoint() -> None:
    checkpoint = {"incremental": {"watermark": "2026-01-01"}}
    NoIncrementalRules().commit(checkpoint, pd.DataFrame([{"updatedAt": "2027-01-01"}]), SETTINGS)
    assert checkpoint == {"incremental": {"watermark": "2026-01-01"}}


def test_observe_does_not_touch_checkpoint() -> None:
    checkpoint = {"incremental": {"watermark": "2026-01-01"}}
    NoIncrementalRules().observe(checkpoint, pd.DataFrame([{"updatedAt": "2027-01-01"}]), SETTINGS)
    assert checkpoint == {"incremental": {"watermark": "2026-01-01"}}
