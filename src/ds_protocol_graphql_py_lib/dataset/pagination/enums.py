"""
**File:** ``enums.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/pagination/enums``

Pagination strategy enum.
"""

from enum import StrEnum


class PaginationStrategy(StrEnum):
    """Declared pagination mechanisms supported by GraphqlDataset."""

    CURSOR = "cursor"
    """Opaque cursor pagination (Relay ``first`` / ``after`` and similar)."""
