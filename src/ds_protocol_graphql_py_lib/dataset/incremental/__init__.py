"""
**File:** ``__init__.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/incremental``

Incremental (shifting-scope) rules consulted by
:class:`~ds_protocol_graphql_py_lib.dataset.pagination.Paginate`. Importing
this package registers the built-in rulesets.

Example:
    >>> from ds_protocol_graphql_py_lib.dataset.incremental import IncrementalSettings
    >>> IncrementalSettings(variable="updatedSince", watermark_path="updatedAt")
"""

from .enums import IncrementalStrategy
from .registry import get_rules, register
from .rules import IncrementalRules
from .settings import IncrementalSettings
from .strategies import NoIncrementalRules, WatermarkRules

__all__ = [
    "IncrementalRules",
    "IncrementalSettings",
    "IncrementalStrategy",
    "NoIncrementalRules",
    "WatermarkRules",
    "get_rules",
    "register",
]
