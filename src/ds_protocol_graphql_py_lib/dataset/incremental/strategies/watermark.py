"""
**File:** ``watermark.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/incremental/strategies/watermark``

High-watermark incremental ruleset.

Prepare
    If ``checkpoint["incremental"]["watermark"]`` is set, inject it into the
    GraphQL variable ``settings.variable``. Otherwise send the request
    unchanged (first run / full load). ``pending`` is never sent.

Observe (every fetched page)
    Set ``checkpoint["incremental"]["pending"]`` to the max of its current
    value and the page's values at ``watermark_path``, so it only ever goes
    up. The committed watermark does not move. A drain that
    fails after some pages leaves ``pending`` behind, so the resumed drain,
    which starts after the saved cursor and never sees those pages, still
    commits their max.

Commit (full success only)
    Set the watermark to the max of the previous watermark, ``pending`` and
    the values in this call's output, and drop ``pending``. The watermark
    never moves backwards, and an empty resumed tail still promotes
    ``pending``.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import pandas as pd  # type: ignore[import-untyped]

from ....utils.json_utils import get_path
from ..enums import IncrementalStrategy
from ..registry import register
from ..rules import IncrementalRules

if TYPE_CHECKING:
    from ...request import GraphqlRequestSnapshot
    from ..settings import IncrementalSettings

_CHECKPOINT_KEY = "incremental"
_WATERMARK_KEY = "watermark"
_PENDING_KEY = "pending"

# Strings starting like an ISO date (YYYY-MM-DD) are compared as timestamps.
_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _comparable(value: Any) -> Any:
    """Sort key: ISO date strings become ``pd.Timestamp``; anything else is compared as is."""
    if isinstance(value, str) and _ISO_DATE.match(value):
        try:
            return pd.Timestamp(value)
        except ValueError:
            return value
    return value


@register(IncrementalStrategy.WATERMARK)
class WatermarkRules(IncrementalRules):
    """
    Resume on a caller-persisted high-watermark.

    Reads ``checkpoint["incremental"]["watermark"]`` on prepare. Tracks the
    max value seen so far in ``checkpoint["incremental"]["pending"]`` per page,
    and on commit advances the watermark to the max of the previous watermark,
    ``pending`` and the output rows. Pandas-missing scalars (``NaN``, ``NaT``,
    ``NA``) are ignored. Does not clear pagination state — Paginate owns that.
    """

    def prepare(
        self,
        request: GraphqlRequestSnapshot,
        checkpoint: dict[str, Any],
        settings: IncrementalSettings | None,
    ) -> GraphqlRequestSnapshot:
        if settings is None:
            return request

        slice_ = checkpoint.get(_CHECKPOINT_KEY)
        if not isinstance(slice_, dict):
            return request

        watermark = slice_.get(_WATERMARK_KEY)
        if watermark is None:
            return request

        return request.with_variables({settings.variable: watermark})

    def observe(
        self,
        checkpoint: dict[str, Any],
        page: pd.DataFrame,
        settings: IncrementalSettings | None,
    ) -> None:
        if settings is None:
            return

        page_values = self._values(page, settings)
        if not page_values:
            return

        slice_ = self._slice(checkpoint)
        pending = self._max([*self._stored(slice_, _PENDING_KEY), *page_values], settings)
        checkpoint[_CHECKPOINT_KEY] = {**slice_, _PENDING_KEY: self._to_stored(pending)}

    def commit(
        self,
        checkpoint: dict[str, Any],
        output: pd.DataFrame,
        settings: IncrementalSettings | None,
    ) -> None:
        if settings is None:
            return

        slice_ = self._slice(checkpoint)
        candidates = [
            *self._stored(slice_, _WATERMARK_KEY),
            *self._stored(slice_, _PENDING_KEY),
            *self._values(output, settings),
        ]
        if not candidates:
            return

        watermark = self._max(candidates, settings)
        checkpoint[_CHECKPOINT_KEY] = {_WATERMARK_KEY: self._to_stored(watermark)}

    @staticmethod
    def _slice(checkpoint: dict[str, Any]) -> dict[str, Any]:
        slice_ = checkpoint.get(_CHECKPOINT_KEY)
        return slice_ if isinstance(slice_, dict) else {}

    @staticmethod
    def _stored(slice_: dict[str, Any], key: str) -> list[Any]:
        value = slice_.get(key)
        return [] if value is None else [value]

    @classmethod
    def _values(cls, frame: pd.DataFrame, settings: IncrementalSettings) -> list[Any]:
        if frame.empty:
            return []
        return [
            value
            for record in frame.to_dict(orient="records")
            if not cls._is_missing_scalar(value := get_path(record, settings.watermark_path))
        ]

    @staticmethod
    def _max(values: list[Any], settings: IncrementalSettings) -> Any:
        """
        Max of ``values``, comparing ISO date strings as instants.
        """
        try:
            return max(values, key=_comparable)
        except TypeError as exc:
            types = sorted({type(value).__name__ for value in values})
            raise TypeError(
                f"Cannot compute watermark at '{settings.watermark_path}': values are not comparable across types {types}",
            ) from exc

    @staticmethod
    def _to_stored(value: Any) -> Any:
        """``pd.Timestamp`` → ISO string so the checkpoint stays JSON-serialisable."""
        return value.isoformat() if isinstance(value, pd.Timestamp) else value

    @staticmethod
    def _is_missing_scalar(value: Any) -> bool:
        """True for ``None`` and pandas-missing scalars (``NaN``, ``NaT``, ``NA``)."""
        return bool(pd.api.types.is_scalar(value) and pd.isna(value))
