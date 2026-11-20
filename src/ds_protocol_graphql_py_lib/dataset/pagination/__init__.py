"""
**File:** ``__init__.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/pagination``

Pagination for GraphqlDataset reads. Importing this package registers the
built-in strategies.

Example:
    >>> from ds_protocol_graphql_py_lib.dataset.pagination import (
    ...     CursorPaginationSettings,
    ...     PaginationSettings,
    ...     PaginationStrategy,
    ... )
"""

from . import strategies  # noqa: F401  (registers strategy handlers)
from .enums import PaginationStrategy
from .paginate import Paginate
from .registry import get_strategy, register
from .settings import CursorPaginationSettings, PaginationSettings

__all__ = [
    "CursorPaginationSettings",
    "Paginate",
    "PaginationSettings",
    "PaginationStrategy",
    "get_strategy",
    "register",
]
