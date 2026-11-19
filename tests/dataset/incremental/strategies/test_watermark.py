"""
**File:** ``test_watermark.py``
**Region:** ``tests/dataset/incremental/strategies``

Unit tests for ``WatermarkRules`` (``strategies/watermark.py``), called
directly (no dataset, no HTTP).

Covers:
    - ``prepare``: injects the stored watermark into ``settings.variable``;
      no-op without settings, slice, or watermark; injected value wins over
      a static start value; request not mutated.
    - ``commit``: max over output rows, nested ``watermark_path``, missing
      values ignored, ``Timestamp`` → ISO string, empty output / no values
      leave the watermark unchanged, mixed types raise, other checkpoint keys
      untouched.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from ds_protocol_graphql_py_lib.dataset.incremental import IncrementalSettings, WatermarkRules
from ds_protocol_graphql_py_lib.dataset.request import GraphqlRequestSnapshot

SETTINGS = IncrementalSettings(variable="since", watermark_path="updatedAt")


def request(variables: dict[str, Any] | None = None) -> GraphqlRequestSnapshot:
    return GraphqlRequestSnapshot(url="https://api.example/graphql", query="{ a }", variables=variables or {})


@pytest.fixture
def rules() -> WatermarkRules:
    return WatermarkRules()


# --------------------------------------------------------------------------- prepare


def test_prepare_injects_stored_watermark(rules: WatermarkRules) -> None:
    checkpoint = {"incremental": {"watermark": "2026-01-01T00:00:00"}}
    prepared = rules.prepare(request({"cid": 1}), checkpoint, SETTINGS)
    assert prepared.variables == {"cid": 1, "since": "2026-01-01T00:00:00"}


def test_prepare_keeps_watermark_type(rules: WatermarkRules) -> None:
    prepared = rules.prepare(request(), {"incremental": {"watermark": 6820488}}, SETTINGS)
    assert prepared.variables == {"since": 6820488}


def test_prepare_injected_watermark_wins_over_static_start_value(rules: WatermarkRules) -> None:
    prepared = rules.prepare(request({"since": "1900-01-01"}), {"incremental": {"watermark": "2026-01-01"}}, SETTINGS)
    assert prepared.variables == {"since": "2026-01-01"}


@pytest.mark.parametrize(
    "checkpoint",
    [
        pytest.param({}, id="no incremental slice"),
        pytest.param({"incremental": {}}, id="no watermark key"),
        pytest.param({"incremental": {"watermark": None}}, id="watermark null"),
        pytest.param({"incremental": "bad"}, id="slice not a dict"),
        pytest.param({"pagination": {"cursor": "c1"}}, id="only pagination slice"),
    ],
)
def test_prepare_without_watermark_returns_request_unchanged(rules: WatermarkRules, checkpoint: dict[str, Any]) -> None:
    base = request({"since": "1900-01-01"})
    assert rules.prepare(base, checkpoint, SETTINGS) is base


def test_prepare_without_settings_returns_request_unchanged(rules: WatermarkRules) -> None:
    base = request()
    assert rules.prepare(base, {"incremental": {"watermark": "2026-01-01"}}, None) is base


def test_prepare_does_not_mutate_request_or_checkpoint(rules: WatermarkRules) -> None:
    base = request({"cid": 1})
    checkpoint = {"incremental": {"watermark": "2026-01-01"}}
    rules.prepare(base, checkpoint, SETTINGS)
    assert base.variables == {"cid": 1}
    assert checkpoint == {"incremental": {"watermark": "2026-01-01"}}


# --------------------------------------------------------------------------- commit


def commit(
    rules: WatermarkRules,
    rows: list[dict[str, Any]],
    checkpoint: dict[str, Any] | None = None,
    settings: IncrementalSettings = SETTINGS,
) -> dict[str, Any]:
    checkpoint = {} if checkpoint is None else checkpoint
    rules.commit(checkpoint, pd.DataFrame(rows), settings)
    return checkpoint


def test_commit_sets_max_value(rules: WatermarkRules) -> None:
    rows = [{"updatedAt": "2026-01-02"}, {"updatedAt": "2026-01-03"}, {"updatedAt": "2026-01-01"}]
    assert commit(rules, rows) == {"incremental": {"watermark": "2026-01-03"}}


def test_commit_numeric_values(rules: WatermarkRules) -> None:
    assert commit(rules, [{"updatedAt": 5}, {"updatedAt": 42}, {"updatedAt": 7}])["incremental"]["watermark"] == 42


def test_commit_replaces_previous_watermark(rules: WatermarkRules) -> None:
    checkpoint = {"incremental": {"watermark": "2026-01-01"}}
    assert commit(rules, [{"updatedAt": "2026-02-01"}], checkpoint) == {"incremental": {"watermark": "2026-02-01"}}


def test_commit_nested_watermark_path(rules: WatermarkRules) -> None:
    settings = IncrementalSettings(variable="since", watermark_path="meta.updatedAt")
    rows = [{"id": 1, "meta": {"updatedAt": "2026-01-01"}}, {"id": 2, "meta": {"updatedAt": "2026-03-01"}}]
    assert commit(rules, rows, settings=settings)["incremental"]["watermark"] == "2026-03-01"


@pytest.mark.parametrize(
    "missing",
    [None, float("nan"), pd.NaT, pd.NA],
    ids=["None", "NaN", "NaT", "NA"],
)
def test_commit_ignores_missing_values(rules: WatermarkRules, missing: Any) -> None:
    rows = [{"updatedAt": "2026-01-01"}, {"updatedAt": missing}, {"updatedAt": "2026-01-02"}]
    assert commit(rules, rows)["incremental"]["watermark"] == "2026-01-02"


def test_commit_ignores_rows_without_the_field(rules: WatermarkRules) -> None:
    rows = [{"id": 1, "updatedAt": "2026-01-01"}, {"id": 2}]
    assert commit(rules, rows)["incremental"]["watermark"] == "2026-01-01"


def test_commit_timestamp_stored_as_iso_string(rules: WatermarkRules) -> None:
    rows = [{"updatedAt": pd.Timestamp("2026-01-01T10:00:00Z")}, {"updatedAt": pd.Timestamp("2026-01-02T10:00:00Z")}]
    assert commit(rules, rows)["incremental"]["watermark"] == "2026-01-02T10:00:00+00:00"


def test_commit_empty_output_leaves_watermark_unchanged(rules: WatermarkRules) -> None:
    checkpoint = {"incremental": {"watermark": "2026-01-01"}}
    assert commit(rules, [], checkpoint) == {"incremental": {"watermark": "2026-01-01"}}


def test_commit_no_values_leaves_watermark_unchanged(rules: WatermarkRules) -> None:
    checkpoint = {"incremental": {"watermark": "2026-01-01"}}
    assert commit(rules, [{"updatedAt": None}, {"id": 2}], checkpoint) == {"incremental": {"watermark": "2026-01-01"}}


def test_commit_without_settings_does_nothing(rules: WatermarkRules) -> None:
    checkpoint: dict[str, Any] = {}
    rules.commit(checkpoint, pd.DataFrame([{"updatedAt": "2026-01-01"}]), None)
    assert checkpoint == {}


def test_commit_mixed_types_raise(rules: WatermarkRules) -> None:
    with pytest.raises(TypeError, match=r"values are not comparable across types \['int', 'str'\]"):
        commit(rules, [{"updatedAt": "2026-01-01"}, {"updatedAt": 5}])


def test_commit_keeps_other_checkpoint_keys(rules: WatermarkRules) -> None:
    checkpoint = {"pagination": {"cursor": "c1"}, "other": 1}
    commit(rules, [{"updatedAt": "2026-01-01"}], checkpoint)
    assert checkpoint == {"pagination": {"cursor": "c1"}, "other": 1, "incremental": {"watermark": "2026-01-01"}}


# --------------------------------------------------------------------------- commit with pending / previous


def test_commit_uses_pending_when_higher_than_output(rules: WatermarkRules) -> None:
    checkpoint = {"incremental": {"watermark": "2026-01-01", "pending": "2026-01-09"}}
    assert commit(rules, [{"updatedAt": "2026-01-07"}], checkpoint) == {"incremental": {"watermark": "2026-01-09"}}


def test_commit_uses_output_when_higher_than_pending(rules: WatermarkRules) -> None:
    checkpoint = {"incremental": {"watermark": "2026-01-01", "pending": "2026-01-03"}}
    assert commit(rules, [{"updatedAt": "2026-01-07"}], checkpoint) == {"incremental": {"watermark": "2026-01-07"}}


def test_commit_empty_output_promotes_pending(rules: WatermarkRules) -> None:
    checkpoint = {"incremental": {"watermark": "2026-01-01", "pending": "2026-01-09"}}
    assert commit(rules, [], checkpoint) == {"incremental": {"watermark": "2026-01-09"}}


def test_commit_first_run_without_rows_writes_nothing(rules: WatermarkRules) -> None:
    assert commit(rules, []) == {}


def test_commit_pending_without_previous_watermark(rules: WatermarkRules) -> None:
    assert commit(rules, [], {"incremental": {"pending": 18}}) == {"incremental": {"watermark": 18}}


def test_commit_never_moves_watermark_backwards(rules: WatermarkRules) -> None:
    checkpoint = {"incremental": {"watermark": "2026-02-01"}}
    assert commit(rules, [{"updatedAt": "2026-01-15"}], checkpoint) == {"incremental": {"watermark": "2026-02-01"}}


def test_commit_compares_stored_iso_with_timestamps_as_instants(rules: WatermarkRules) -> None:
    """'2026-01-01T12:00:00+02:00' sorts after '...T11:00:00+00:00' as text, but is the earlier instant."""
    checkpoint = {"incremental": {"pending": "2026-01-01T12:00:00+02:00"}}
    rows = [{"updatedAt": pd.Timestamp("2026-01-01T11:00:00Z")}]
    assert commit(rules, rows, checkpoint) == {"incremental": {"watermark": "2026-01-01T11:00:00+00:00"}}


def test_commit_pending_of_other_type_raises(rules: WatermarkRules) -> None:
    with pytest.raises(TypeError, match="not comparable"):
        commit(rules, [{"updatedAt": "2026-01-07"}], {"incremental": {"pending": 5}})


# --------------------------------------------------------------------------- observe


def observe(
    rules: WatermarkRules,
    rows: list[dict[str, Any]],
    checkpoint: dict[str, Any] | None = None,
    settings: IncrementalSettings | None = SETTINGS,
) -> dict[str, Any]:
    checkpoint = {} if checkpoint is None else checkpoint
    rules.observe(checkpoint, pd.DataFrame(rows), settings)
    return checkpoint


def test_observe_records_page_max_as_pending_without_moving_watermark(rules: WatermarkRules) -> None:
    checkpoint = {"incremental": {"watermark": "2026-01-01"}}
    observe(rules, [{"updatedAt": "2026-01-04"}, {"updatedAt": "2026-01-09"}], checkpoint)
    assert checkpoint == {"incremental": {"watermark": "2026-01-01", "pending": "2026-01-09"}}


def test_observe_keeps_higher_pending(rules: WatermarkRules) -> None:
    checkpoint = {"incremental": {"watermark": "2026-01-01", "pending": "2026-01-09"}}
    observe(rules, [{"updatedAt": "2026-01-07"}], checkpoint)
    assert checkpoint["incremental"]["pending"] == "2026-01-09"


def test_observe_increases_pending(rules: WatermarkRules) -> None:
    checkpoint = {"incremental": {"pending": "2026-01-03"}}
    observe(rules, [{"updatedAt": "2026-01-07"}], checkpoint)
    assert checkpoint == {"incremental": {"pending": "2026-01-07"}}


def test_observe_timestamp_stored_as_iso_string(rules: WatermarkRules) -> None:
    checkpoint = observe(rules, [{"updatedAt": pd.Timestamp("2026-01-02T10:00:00Z")}])
    assert checkpoint == {"incremental": {"pending": "2026-01-02T10:00:00+00:00"}}


@pytest.mark.parametrize(
    "rows",
    [pytest.param([], id="empty page"), pytest.param([{"updatedAt": None}, {"id": 2}], id="no values")],
)
def test_observe_page_without_values_changes_nothing(rules: WatermarkRules, rows: list[dict[str, Any]]) -> None:
    checkpoint = {"incremental": {"watermark": "2026-01-01"}}
    observe(rules, rows, checkpoint)
    assert checkpoint == {"incremental": {"watermark": "2026-01-01"}}


def test_observe_without_settings_does_nothing(rules: WatermarkRules) -> None:
    assert observe(rules, [{"updatedAt": "2026-01-07"}], settings=None) == {}


def test_observe_mixed_types_raise(rules: WatermarkRules) -> None:
    with pytest.raises(TypeError, match="not comparable"):
        observe(rules, [{"updatedAt": "2026-01-07"}], {"incremental": {"pending": 5}})


def test_prepare_ignores_pending(rules: WatermarkRules) -> None:
    """A resumed drain keeps the failed attempt's lower bound."""
    checkpoint = {"incremental": {"watermark": "2026-01-01", "pending": "2026-01-09"}}
    assert rules.prepare(request(), checkpoint, SETTINGS).variables == {"since": "2026-01-01"}


def test_prepare_with_only_pending_sends_no_watermark(rules: WatermarkRules) -> None:
    base = request()
    assert rules.prepare(base, {"incremental": {"pending": "2026-01-09"}}, SETTINGS) is base


# --------------------------------------------------------------------------- prepare after commit


def test_commit_then_prepare_round_trip(rules: WatermarkRules) -> None:
    checkpoint = commit(rules, [{"updatedAt": "2026-01-01"}, {"updatedAt": "2026-01-05"}])
    assert rules.prepare(request(), checkpoint, SETTINGS).variables == {"since": "2026-01-05"}
