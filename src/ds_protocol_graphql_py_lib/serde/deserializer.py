"""
**File:** ``deserializer.py``
**Region:** ``ds_protocol_graphql_py_lib/serde``

GraphQL response deserializer for converting GraphQL responses to DataFrames.

Example:
    >>> from ds_protocol_graphql_py_lib import GraphqlDeserializer
    >>> deserializer = GraphqlDeserializer(format=DatasetStorageFormatType.JSON)
    >>> response = {"data": {"users": [{"id": "1", "name": "John"}]}}
    >>> df = deserializer.deserialize(response)
"""

from typing import Any

import pandas as pd  # type: ignore[import-untyped]
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer


class GraphqlDeserializer(PandasDeserializer):
    """
    Deserializer for GraphQL API responses.

    Handles various GraphQL response patterns:
    - Direct arrays: {"data": {"users": [...]}}
    - Relay connections: {"data": {"users": {"edges": [{"node": {...}}]}}}
    - Single objects: {"data": {"user": {...}}}
    - Introspection queries: {"data": {"__type": {"fields": [...]}}}
    """

    def deserialize_graphql(self, data: Any) -> pd.DataFrame:
        """
        Deserialize GraphQL response to pandas DataFrame.

        Args:
            data: GraphQL response dict or raw data

        Returns:
            DataFrame containing the extracted data
        """
        if not isinstance(data, dict):
            return pd.DataFrame()

        extracted_data = data.get("data", data)

        return self._parse_graphql_data(extracted_data)

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
            return pd.DataFrame(data)

        if not isinstance(data, dict):
            return pd.DataFrame([data])

        if len(data) == 1:
            nested_value = next(iter(data.values()))

            # Nested value is a list - use it
            if isinstance(nested_value, list):
                return pd.DataFrame(nested_value)

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
            DataFrame with nodes extracted from edges
        """
        edges = data.get("edges", [])
        if not isinstance(edges, list):
            return pd.DataFrame()

        nodes = [edge.get("node", edge) for edge in edges if edge]
        return pd.DataFrame(nodes)
