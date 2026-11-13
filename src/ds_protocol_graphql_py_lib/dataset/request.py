"""
**File:** ``request.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/request``

Immutable GraphQL-over-HTTP request snapshot used by pagination.

Pagination only ever changes **variables**; the query document is never
rewritten.

Example:
    >>> snapshot = GraphqlRequestSnapshot(url="https://api/graphql", query="{ a }", variables={"x": 1})
    >>> snapshot.with_variables({"after": "c1"}).to_payload()
    {'query': '{ a }', 'variables': {'x': 1, 'after': 'c1'}}
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any


@dataclass(frozen=True, kw_only=True)
class GraphqlRequestSnapshot:
    """One GraphQL POST: endpoint, document, variables, operation name, headers."""

    url: str
    query: str
    variables: dict[str, Any] = field(default_factory=dict)
    operation_name: str | None = None
    headers: dict[str, str] | None = None

    def with_variables(self, values: dict[str, Any]) -> GraphqlRequestSnapshot:
        """
        Return a copy with ``values`` merged into variables.

        Injected values win over existing variables with the same name.
        The original snapshot is not modified.
        """
        return replace(self, variables={**self.variables, **values})

    def to_payload(self) -> dict[str, Any]:
        """Build the JSON body for the GraphQL POST."""
        payload: dict[str, Any] = {"query": self.query}
        if self.variables:
            payload["variables"] = self.variables
        if self.operation_name:
            payload["operationName"] = self.operation_name
        return payload
