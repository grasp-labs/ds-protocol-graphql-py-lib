"""
**File:** ``rules.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/incremental/rules``

Incremental rulesets consulted by :class:`~dataset.pagination.paginate.Paginate`.

Rules prepare a request from ``checkpoint`` and commit after a successful
drain. They never send requests.
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd  # type: ignore[import-untyped]

    from ..request import GraphqlRequestSnapshot
    from .settings import IncrementalSettings


class IncrementalRules(abc.ABC):
    """
    Shifting-scope rules for a paginated read.

    Instruction lives in ``IncrementalSettings``; state lives in ``checkpoint``.
    """

    @abc.abstractmethod
    def prepare(
        self,
        request: GraphqlRequestSnapshot,
        checkpoint: dict[str, Any],
        settings: IncrementalSettings | None,
    ) -> GraphqlRequestSnapshot:
        """Apply persisted state (the stored watermark) to the base request."""

    @abc.abstractmethod
    def observe(
        self,
        checkpoint: dict[str, Any],
        page: pd.DataFrame,
        settings: IncrementalSettings | None,
    ) -> None:
        """
        Record state from each fetched page for a later ``commit``.

        Never write the watermark ``prepare`` sends; use a separate checkpoint
        key (e.g. ``pending``), so a resumed drain keeps the same lower bound.
        """

    @abc.abstractmethod
    def commit(
        self,
        checkpoint: dict[str, Any],
        output: pd.DataFrame,
        settings: IncrementalSettings | None,
    ) -> None:
        """
        Advance persisted state after a fully successful drain.

        Does not clear ``checkpoint["pagination"]`` — that is owned by Paginate.
        """
