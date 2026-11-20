"""
**File:** ``deserializer.py``
**Region:** ``ds_protocol_graphql_py_lib/serde``

GraphQL response deserializer for converting GraphQL responses to DataFrames.

Example:
    >>> from ds_protocol_graphql_py_lib import GraphqlDeserializer
    >>> deserializer = GraphqlDeserializer(format=DatasetStorageFormatType.JSON)
    >>> response = {"data": {"users": [{"id": "1", "name": "John"}]}}
    >>> df = deserializer.deserialize_graphql(response)
    >>> page = {"data": {"x": {"items": [{"id": 1}], "pageInfo": {"hasNextPage": False}}}}
    >>> df = deserializer.deserialize_graphql(page, records_path="data.x.items")
"""

from typing import Any

import pandas as pd  # type: ignore[import-untyped]
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer

from ..utils.json_utils import get_path

# Where deserialization starts when no ``records_path`` is given: the GraphQL ``data`` object.
DEFAULT_RECORDS_PATH = "data"

# Key of a Relay connection's edge list. Elements of a list under this key are edges.
_EDGES_KEY = "edges"

# Edge fields that describe the edge itself, not the record: dropped when an edge becomes a row.
_EDGE_META_KEYS = frozenset({"cursor", "__typename"})


class GraphqlDeserializer(PandasDeserializer):
    """
    Deserializer for GraphQL API responses.

    Starts at ``records_path`` (default ``"data"``) and turns the value there
    into rows:
    - Lists: one row per element
    - Relay edges (a list under ``edges``, reached by the default path or a
      ``records_path`` ending in ``.edges``): see :meth:`_edge_row`
    - Direct arrays: {"data": {"users": [...]}}
    - Relay connections: {"data": {"users": {"edges": [{"node": {...}}]}}}
    - Single objects: {"data": {"user": {...}}}
    - Introspection queries: {"data": {"__type": {"fields": [...]}}}
    """

    def deserialize_graphql(self, data: Any, records_path: str = DEFAULT_RECORDS_PATH) -> pd.DataFrame:
        """
        Deserialize GraphQL response to pandas DataFrame.

        Args:
            data: GraphQL response dict.
            records_path: Dotted path on the full response where deserialization
                starts (default ``"data"``), e.g. ``"data.useCompany.associate.items"``.
                The value there is parsed with the same rules as the default: lists
                become rows (Relay edges unwrapped to ``node``), connections are
                unwrapped, single objects become one row. Missing or null yields an
                empty DataFrame. With the default path, a payload without a
                top-level ``"data"`` key is parsed as a whole (already unwrapped).

        Returns:
            DataFrame containing the extracted data
        """
        if not isinstance(data, dict):
            return pd.DataFrame()

        if records_path == DEFAULT_RECORDS_PATH and DEFAULT_RECORDS_PATH not in data:
            return self._parse_graphql_data(data)

        extracted_data = get_path(data, records_path)
        if isinstance(extracted_data, list):
            return self._frame_from_list(extracted_data, key=records_path.rsplit(".", 1)[-1])

        return self._parse_graphql_data(extracted_data)

    @staticmethod
    def _edge_row(edge: Any) -> Any:
        """
        One Relay edge → one row, or ``None`` to skip it. Same rule on every path.

        - ``node`` is a dict: the row is the node's fields plus the edge's own
          fields (e.g. ``role`` on a membership edge). ``cursor`` and
          ``__typename`` describe the edge and are dropped; on a name clash the
          node's value wins.
        - ``node`` is ``null``: no record (e.g. deleted or hidden), skipped even
          when the edge has other fields.
        - ``null`` element: skipped.
        - Anything else (no ``node`` selected, or a non-dict node): kept as is.
        """
        if edge is None:
            return None
        if not isinstance(edge, dict) or "node" not in edge:
            return edge
        node = edge["node"]
        if node is None:
            return None
        if not isinstance(node, dict):
            return edge
        extra = {key: value for key, value in edge.items() if key != "node" and key not in _EDGE_META_KEYS}
        return {**extra, **node}

    def _frame_from_edges(self, edges: list[Any]) -> pd.DataFrame:
        """Rows from a Relay ``edges`` list (see :meth:`_edge_row`)."""
        return pd.DataFrame([row for edge in edges if (row := self._edge_row(edge)) is not None])

    def _frame_from_list(self, records: list[Any], *, key: str | None = None) -> pd.DataFrame:
        """
        One row per element. A list under ``edges`` is a Relay edge list; any
        other list is taken as rows unchanged (a ``node`` field there is data).
        """
        if key == _EDGES_KEY:
            return self._frame_from_edges(records)
        return pd.DataFrame(records)

    def _parse_graphql_data(self, data: Any) -> pd.DataFrame:
        """
        Parse GraphQL data structure into DataFrame.

        Handles nested structures and various GraphQL response patterns.

        Args:
            data: The data portion of the GraphQL response

        Returns:
            DataFrame with the extracted data
        """
        if data is None:
            return pd.DataFrame()

        if isinstance(data, list):
            return self._frame_from_list(data)

        if not isinstance(data, dict):
            return pd.DataFrame([data])

        if len(data) == 1:
            key, nested_value = next(iter(data.items()))

            # Nested value is a list - use it
            if isinstance(nested_value, list):
                return self._frame_from_list(nested_value, key=key)

            # Nested value has edges (Relay pagination)
            if isinstance(nested_value, dict) and "edges" in nested_value:
                return self._extract_relay_nodes(nested_value)

            # Check if it's a pure container with only array fields
            if isinstance(nested_value, dict):
                return self._handle_nested_dict(nested_value)

        # Handle Relay pagination at top level
        if "edges" in data:
            return self._extract_relay_nodes(data)

        return pd.DataFrame([data])

    def _handle_nested_dict(self, nested_value: dict[str, Any]) -> pd.DataFrame:
        """
        Handle nested dict - determine if it's a pure array container or single object.

        Args:
            nested_value: The nested dictionary to analyze

        Returns:
            DataFrame with appropriate data extraction
        """
        all_values = list(nested_value.values())
        list_fields = [v for v in all_values if isinstance(v, list)]
        non_list_fields = [v for v in all_values if not isinstance(v, list) and v is not None]

        # Pure container with single array field (e.g., GraphQL introspection)
        # Example: {"fields": [...]} where ALL non-null values are the single list
        if len(list_fields) == 1 and len(non_list_fields) == 0:
            return pd.DataFrame(list_fields[0])

        # Single object with mixed fields - keep as single row
        return pd.DataFrame([nested_value])

    def _extract_relay_nodes(self, data: dict[str, Any]) -> pd.DataFrame:
        """
        Extract nodes from Relay-style pagination structure.

        Args:
            data: Dict containing "edges" key with Relay pagination structure

        Returns:
            DataFrame with one row per edge (see :meth:`_edge_row`).
        """
        edges = data.get(_EDGES_KEY, [])
        if not isinstance(edges, list):
            return pd.DataFrame()

        return self._frame_from_edges(edges)
