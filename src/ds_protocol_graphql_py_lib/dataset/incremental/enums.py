"""
**File:** ``enums.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/incremental/enums``

Incremental ruleset enum.
"""

from enum import StrEnum


class IncrementalStrategy(StrEnum):
    """Declared incremental rulesets supported by Paginate."""

    WATERMARK = "watermark"
    """Variable-injected high-watermark across runs."""
