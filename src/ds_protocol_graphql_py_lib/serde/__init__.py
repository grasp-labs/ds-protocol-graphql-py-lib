"""
**File:** ``__init__.py``
**Region:** ``ds_protocol_graphql_py_lib/serde``

Serialization and deserialization utilities for GraphQL protocol.

Example:
    >>> from ds_protocol_graphql_py_lib.serde import GraphqlDeserializer
    >>> deserializer = GraphqlDeserializer()
"""

from .deserializer import GraphqlDeserializer

__all__ = [
    "GraphqlDeserializer",
]
