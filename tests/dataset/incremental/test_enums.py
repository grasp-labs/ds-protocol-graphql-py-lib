"""
**File:** ``test_enums.py``
**Region:** ``tests/dataset/incremental``

Unit tests for ``IncrementalStrategy`` (``incremental/enums.py``).
"""

from __future__ import annotations

from ds_protocol_graphql_py_lib.dataset.incremental import IncrementalStrategy


def test_watermark_value() -> None:
    assert IncrementalStrategy.WATERMARK.value == "watermark"


def test_only_watermark_is_supported() -> None:
    assert [s.value for s in IncrementalStrategy] == ["watermark"]
