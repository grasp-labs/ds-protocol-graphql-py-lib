"""
**File:** ``enums.py``
**Region:** ``ds_protocol_graphql_py_lib/enums``

Constants for GRAPHQL protocol.

Example:
    >>> ResourceType.DATASET
    'DS.RESOURCE.DATASET.GRAPHQL'
"""

from enum import StrEnum


class ResourceType(StrEnum):
    """
    Constants for GRAPHQL protocol.
    """

    DATASET = "DS.RESOURCE.DATASET.GRAPHQL"
