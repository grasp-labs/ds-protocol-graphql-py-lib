"""
**File:** ``none.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/incremental/strategies/none``

No-op incremental rules (full load; checkpoint watermark untouched).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..rules import IncrementalRules

if TYPE_CHECKING:
    import pandas as pd  # type: ignore[import-untyped]

    from ...request import GraphqlRequestSnapshot
    from ..settings import IncrementalSettings


class NoIncrementalRules(IncrementalRules):
    """Used by Paginate when ``read.incremental`` is unset: prepare and commit do nothing."""

    def prepare(
        self,
        request: GraphqlRequestSnapshot,
        checkpoint: dict[str, Any],
        settings: IncrementalSettings | None,
    ) -> GraphqlRequestSnapshot:
        del checkpoint, settings
        return request

    def commit(
        self,
        checkpoint: dict[str, Any],
        output: pd.DataFrame,
        settings: IncrementalSettings | None,
    ) -> None:
        del checkpoint, output, settings
