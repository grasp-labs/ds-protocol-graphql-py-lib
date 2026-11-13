"""
**File:** ``settings.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/incremental/settings``

Incremental (shifting-scope) settings for GraphqlDataset reads.

The query document owns the filter shape; incremental only supplies the
value of one GraphQL variable.

Example:
    >>> IncrementalSettings(variable="updatedSince", watermark_path="updatedAt")

    With a query such as::

        query($first: Int, $after: String, $updatedSince: String) {
          customers(first: $first, after: $after, filter: { updatedAt_gte: $updatedSince }) { ... }
        }
"""

from dataclasses import dataclass

from ds_common_serde_py_lib import Serializable

from .enums import IncrementalStrategy


@dataclass(kw_only=True)
class IncrementalSettings(Serializable):
    """
    Instruction nest for incremental rules used by Paginate.

    Instruction (stable): which GraphQL variable carries the lower bound and
    which output-row field advances the high-watermark.

    State (runtime): the watermark value itself lives in
    ``checkpoint["incremental"]``. Paginate reads it on prepare and writes it
    on commit.
    """

    strategy: IncrementalStrategy = IncrementalStrategy.WATERMARK
    """Incremental ruleset. Declared explicitly; never inferred."""

    variable: str
    """
    GraphQL variable that receives the stored watermark on every page.

    Not sent when no watermark is stored yet (first run). Declare it nullable
    in the query, or put a start value in ``read.variables``; the injected
    watermark wins over it once one exists.
    """

    watermark_path: str
    """Dotted path on each **output row** (after deserialize) used to compute the new high-watermark."""
