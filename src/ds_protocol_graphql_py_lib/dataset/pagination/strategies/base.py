"""
**File:** ``base.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/pagination/strategies/base``

Base ABC for registered pagination strategies (page-variation hooks only).

The request loop lives on :class:`~dataset.pagination.paginate.Paginate`,
which *uses* a strategy — strategies do not send requests.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from ...request import GraphqlRequestSnapshot

ConfigT = TypeVar("ConfigT")


@dataclass
class PageState:
    """Strategy-owned traversal state for one paginated read."""

    values: dict[str, Any] = field(default_factory=dict)
    """Opaque strategy values (cursor, page size, …)."""

    page_index: int = 0
    """Zero-based count of pages fetched so far."""


class PaginationStrategyHandler(abc.ABC, Generic[ConfigT]):
    """Page-variation contract parameterized by the strategy settings nest."""

    @abc.abstractmethod
    def initial_state(self, cfg: ConfigT, checkpoint_slice: dict[str, Any] | None) -> PageState:
        """Build the starting page state, optionally restoring from checkpoint."""

    @abc.abstractmethod
    def inject(self, request: GraphqlRequestSnapshot, state: PageState, cfg: ConfigT) -> GraphqlRequestSnapshot:
        """Return a request snapshot with pagination variables applied."""

    @abc.abstractmethod
    def advance(self, *, body: Any, items: list[Any], state: PageState, cfg: ConfigT) -> PageState:
        """Advance state after a successful page response."""

    @abc.abstractmethod
    def should_stop(self, *, body: Any, items: list[Any], state: PageState, cfg: ConfigT) -> bool:
        """Return True when traversal should terminate after this page."""

    @abc.abstractmethod
    def to_checkpoint(self, state: PageState) -> dict[str, Any]:
        """Serialize traversal state into the checkpoint ``pagination`` slice."""

    @abc.abstractmethod
    def from_checkpoint(self, data: dict[str, Any]) -> PageState:
        """Restore traversal state from a checkpoint ``pagination`` slice."""
