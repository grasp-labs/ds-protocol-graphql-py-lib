"""
**File:** ``settings.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/pagination/settings``

Pagination settings nests for GraphqlDataset read configuration.

Example (Visma Business NXT style, ``pageInfo.endCursor``):
    >>> PaginationSettings(
    ...     strategy=PaginationStrategy.CURSOR,
    ...     items_path="data.useCompany.associate.items",
    ...     cursor=CursorPaginationSettings(
    ...         cursor_path="data.useCompany.associate.pageInfo.endCursor",
    ...         has_next_path="data.useCompany.associate.pageInfo.hasNextPage",
    ...     ),
    ... )

Example (Xledger style, cursor on last edge):
    >>> PaginationSettings(
    ...     strategy=PaginationStrategy.CURSOR,
    ...     items_path="data.customers.edges",
    ...     cursor=CursorPaginationSettings(
    ...         cursor_path="data.customers.edges.-1.cursor",
    ...         has_next_path="data.customers.pageInfo.hasNextPage",
    ...     ),
    ... )
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from ds_common_serde_py_lib import Serializable

from .enums import PaginationStrategy


@dataclass(kw_only=True)
class CursorPaginationSettings(Serializable):
    """Configuration for cursor pagination via GraphQL variables."""

    cursor_path: str
    """
    Dotted path on the full response body to the next cursor.

    Examples: ``data.x.pageInfo.endCursor`` (Relay ``endCursor``), or
    ``data.x.edges.-1.cursor`` for the cursor on the last edge when the API
    has no ``endCursor`` (e.g. Xledger). ``-1`` is the last list element; on
    an empty page it resolves to no cursor.
    """

    cursor_variable: str = "after"
    """GraphQL variable that receives the cursor."""

    page_size_variable: str | None = "first"
    """GraphQL variable that receives the page size. ``None`` to not send one."""

    page_size: int | None = 100
    """Requested page size. Never clamped; server rejections surface as errors."""

    has_next_path: str | None = None
    """
    Optional path on the full response to a boolean "more pages" flag
    (e.g. ``data.x.pageInfo.hasNextPage``).

    When set, traversal stops when it is ``false``. When unset, traversal
    stops on an empty page or a missing / empty next cursor.
    """


@dataclass(kw_only=True)
class PaginationSettings(Serializable):
    """
    Declared pagination mechanism and strategy-specific nest.

    Provide the nest matching ``strategy``. Absence of ``PaginationSettings``
    on ``GraphqlReadSettings`` means a single request.
    """

    strategy: PaginationStrategy
    """Pagination mechanism. Declared explicitly; never inferred."""

    items_path: str
    """
    Path on the full response to the page's record list, used for paging
    control (empty-page checks). Missing or null is
    an error. Also the default ``records_path`` for output when that is unset.
    """

    max_pages: int | None = None
    """Optional safety ceiling on pages fetched per ``read()``. Exceeding it raises."""

    cursor: CursorPaginationSettings | None = None
    """Required when ``strategy`` is ``cursor``."""

    def __post_init__(self) -> None:
        if self._selected_nest() is None:
            raise ValueError(
                f"PaginationSettings.{self.strategy.value} is required when strategy is '{self.strategy.value}'",
            )

    def _selected_nest(self) -> CursorPaginationSettings | None:
        return {
            PaginationStrategy.CURSOR: self.cursor,
        }[self.strategy]

    @property
    def strategy_config(self) -> CursorPaginationSettings:
        """Strategy-specific nest for ``strategy`` (guaranteed by ``__post_init__``)."""
        return cast("CursorPaginationSettings", self._selected_nest())
