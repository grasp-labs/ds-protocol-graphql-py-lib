"""
**File:** ``test_settings.py``
**Region:** ``tests/dataset/incremental``

Unit tests for ``IncrementalSettings`` (``incremental/settings.py``).
"""

from __future__ import annotations

import pytest

from ds_protocol_graphql_py_lib.dataset.incremental import IncrementalSettings, IncrementalStrategy


def test_defaults_to_watermark_strategy() -> None:
    settings = IncrementalSettings(variable="updatedSince", watermark_path="updatedAt")
    assert settings.strategy is IncrementalStrategy.WATERMARK


@pytest.mark.parametrize("missing", ["variable", "watermark_path"])
def test_required_fields(missing: str) -> None:
    kwargs = {"variable": "updatedSince", "watermark_path": "updatedAt"}
    del kwargs[missing]
    with pytest.raises(TypeError, match=missing):
        IncrementalSettings(**kwargs)  # type: ignore[arg-type]


def test_roundtrip_serialization() -> None:
    settings = IncrementalSettings(variable="updatedSince", watermark_path="meta.updatedAt")
    assert IncrementalSettings.deserialize(settings.serialize()) == settings


def test_deserialize_from_plain_dict() -> None:
    restored = IncrementalSettings.deserialize({"variable": "since", "watermark_path": "modifiedAt"})
    assert restored == IncrementalSettings(variable="since", watermark_path="modifiedAt")
