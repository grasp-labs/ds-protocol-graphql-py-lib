"""
**File:** ``__init__.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/pagination/strategies``

Importing this package registers all built-in strategy handlers.
"""

from .base import PageState, PaginationStrategyHandler
from .cursor import CursorPaginationStrategy

__all__ = [
    "CursorPaginationStrategy",
    "PageState",
    "PaginationStrategyHandler",
]
