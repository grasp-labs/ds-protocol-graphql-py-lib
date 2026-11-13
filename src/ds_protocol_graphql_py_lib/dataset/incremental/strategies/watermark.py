"""
**File:** ``watermark.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/incremental/strategies/watermark``

High-watermark incremental ruleset.

Prepare
    If ``checkpoint["incremental"]["watermark"]`` is set, inject it into the
    GraphQL variable ``settings.variable``. Otherwise send the request
    unchanged (first run / full load).

Commit (full success only)
    Set the watermark to the max non-missing value at ``watermark_path``
    across output rows. ``pd.Timestamp`` is stored as an ISO string. No rows
    or no values: the watermark is left unchanged.
"""

from __future__ import annotations

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


@register(IncrementalStrategy.WATERMARK)
class WatermarkRules(IncrementalRules):
    """
    Resume on a caller-persisted high-watermark.

    Reads ``checkpoint["incremental"]["watermark"]`` on prepare. On commit,
    advances it to the max present value at ``settings.watermark_path`` across
    rows. Pandas-missing scalars (``NaN``, ``NaT``, ``NA``) are ignored.
    Does not clear pagination state — Paginate owns that.
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

    def commit(
        self,
        checkpoint: dict[str, Any],
        output: pd.DataFrame,
        settings: IncrementalSettings | None,
    ) -> None:
        if settings is None or output.empty:
            return

        values = [
            value
            for record in output.to_dict(orient="records")
            if not self._is_missing_scalar(value := get_path(record, settings.watermark_path))
        ]
        if not values:
            return

        try:
            watermark: Any = max(values)
        except TypeError as exc:
            types = sorted({type(value).__name__ for value in values})
            raise TypeError(
                f"Cannot compute watermark at '{settings.watermark_path}': values are not comparable across types {types}",
            ) from exc

        if isinstance(watermark, pd.Timestamp):
            watermark = watermark.isoformat()

        checkpoint[_CHECKPOINT_KEY] = {_WATERMARK_KEY: watermark}

    @staticmethod
    def _is_missing_scalar(value: Any) -> bool:
        """True for ``None`` and pandas-missing scalars (``NaN``, ``NaT``, ``NA``)."""
        return bool(pd.api.types.is_scalar(value) and pd.isna(value))
