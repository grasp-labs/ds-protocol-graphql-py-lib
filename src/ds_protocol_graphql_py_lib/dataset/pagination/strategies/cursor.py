"""
**File:** ``cursor.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/pagination/strategies/cursor``

Cursor page strategy for GraphQL connections.

Intent
    Server-driven continuation via a cursor variable (Relay ``after`` and
    similar). The next cursor is read from ``cursor_path`` on the response,
    e.g. ``pageInfo.endCursor``, or ``edges.-1.cursor`` (last edge) for APIs
    that do not expose ``endCursor`` (e.g. Xledger).

Stop rules
    - ``has_next_path`` set: stop when it is ``false``.
    - ``has_next_path`` unset: stop on an empty page or missing next cursor.

Failure rules (raise, mapped to ``ReadError`` by Paginate)
    - ``has_next_path`` resolves to a non-boolean.
    - ``has_next_path`` is ``true`` but the next cursor is missing
      (including a last-edge ``-1`` path on an empty page).
    - The next cursor equals the current cursor (no progress).

Boundaries
    Owns cursor / page-size variables, stop / progress checks, and the
    ``checkpoint["pagination"]`` shape. Does not send requests or clear the
    checkpoint — :class:`Paginate` does.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ....utils.json_utils import get_path
from ..enums import PaginationStrategy
from ..registry import register
from ..settings import CursorPaginationSettings
from .base import PageState, PaginationStrategyHandler

if TYPE_CHECKING:
    from ...request import GraphqlRequestSnapshot


def _read_next_cursor(body: Any, cfg: CursorPaginationSettings) -> str | None:
    value = get_path(body, cfg.cursor_path)
    if value is None or value == "":
        return None
    return str(value)


def _read_has_next(body: Any, cfg: CursorPaginationSettings) -> bool | None:
    if cfg.has_next_path is None:
        return None
    value = get_path(body, cfg.has_next_path)
    if not isinstance(value, bool):
        raise TypeError(
            f"has_next_path '{cfg.has_next_path}' must resolve to a boolean (got {type(value).__name__})",
        )
    return value


@register(PaginationStrategy.CURSOR)
class CursorPaginationStrategy(PaginationStrategyHandler[CursorPaginationSettings]):
    """Cursor pagination: echo the server cursor until there are no more pages."""

    def initial_state(self, cfg: CursorPaginationSettings, checkpoint_slice: dict[str, Any] | None) -> PageState:
        if checkpoint_slice is not None:
            return self.from_checkpoint(checkpoint_slice)
        return PageState(values={"cursor": None, "page_size": cfg.page_size}, page_index=0)

    def inject(
        self,
        request: GraphqlRequestSnapshot,
        state: PageState,
        cfg: CursorPaginationSettings,
    ) -> GraphqlRequestSnapshot:
        values: dict[str, Any] = {}
        # Prefer page size from state so a resumed run keeps the same page size.
        page_size = state.values.get("page_size", cfg.page_size)
        if cfg.page_size_variable is not None and page_size is not None:
            values[cfg.page_size_variable] = page_size
        cursor = state.values.get("cursor")
        if cursor is not None:
            values[cfg.cursor_variable] = cursor
        return request.with_variables(values)

    def advance(self, *, body: Any, items: list[Any], state: PageState, cfg: CursorPaginationSettings) -> PageState:
        del items
        previous = state.values.get("cursor")
        nxt = _read_next_cursor(body, cfg)
        if nxt is None:
            raise ValueError("Cursor pagination expected a next cursor but none was found")
        if nxt == previous:
            raise ValueError("Cursor pagination made no progress (identical next cursor)")
        return PageState(
            values={"cursor": nxt, "page_size": state.values.get("page_size", cfg.page_size)},
            page_index=state.page_index + 1,
        )

    def should_stop(self, *, body: Any, items: list[Any], state: PageState, cfg: CursorPaginationSettings) -> bool:
        del state
        has_next = _read_has_next(body, cfg)  # None when has_next_path is not configured
        nxt = _read_next_cursor(body, cfg)  # None when the cursor is missing, null or ""

        # Stop on an empty page, or when the server returned no next cursor.
        if has_next is None:
            return not items or nxt is None

        # has_next_path is authoritative: stop when the server says there are no more pages.
        if not has_next:
            return True

        # Server says more pages exist but gave no cursor to fetch them with
        if nxt is None:
            raise ValueError(
                f"has_next_path '{cfg.has_next_path}' is true but no next cursor was found "
                f"(cursor_path='{cfg.cursor_path}', page_rows={len(items)})",
            )

        # More pages and a cursor to continue with: keep going.
        return False

    def to_checkpoint(self, state: PageState) -> dict[str, Any]:
        return {
            "strategy": PaginationStrategy.CURSOR.value,
            "cursor": state.values["cursor"],
            "page_size": state.values["page_size"],
            "page_index": state.page_index,
        }

    def from_checkpoint(self, data: dict[str, Any]) -> PageState:
        return PageState(
            values={"cursor": data["cursor"], "page_size": data["page_size"]},
            page_index=data["page_index"],
        )
