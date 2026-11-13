"""
**File:** ``__init__.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/incremental/strategies``

Importing this package registers all built-in incremental rulesets.
"""

from .none import NoIncrementalRules
from .watermark import WatermarkRules

__all__ = [
    "NoIncrementalRules",
    "WatermarkRules",
]
