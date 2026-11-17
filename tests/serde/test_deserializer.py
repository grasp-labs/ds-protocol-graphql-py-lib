"""
**File:** ``test_deserializer.py``
**Region:** ``tests``

Unit tests for GraphqlDeserializer response parsing.

Covers:
    Edge-case branches for non-dict input, list/scalar parsing,
    top-level Relay edges extraction, and invalid Relay edges payloads.
"""

from __future__ import annotations

import pandas as pd
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType

from ds_protocol_graphql_py_lib.serde.deserializer import GraphqlDeserializer


def _deserializer() -> GraphqlDeserializer:
    return GraphqlDeserializer(format=DatasetStorageFormatType.JSON)


def test_deserialize_graphql_non_dict_returns_empty_dataframe() -> None:
    deserializer = _deserializer()

    result = deserializer.deserialize_graphql("not-a-dict")

    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_parse_graphql_data_list_returns_dataframe_rows() -> None:
    deserializer = _deserializer()

    result = deserializer._parse_graphql_data([{"id": 1}, {"id": 2}])

    assert list(result["id"]) == [1, 2]


def test_parse_graphql_data_scalar_wraps_into_single_row() -> None:
    deserializer = _deserializer()

    result = deserializer._parse_graphql_data("value")

    assert result.shape == (1, 1)
    assert result.iloc[0, 0] == "value"


def test_parse_graphql_data_top_level_edges_extracts_nodes() -> None:
    deserializer = _deserializer()

    data = {
        "edges": [
            {"node": {"name": "Albania", "alpha2Code": "AL"}},
            {"node": {"name": "Andorra", "alpha2Code": "AD"}},
        ]
    }

    result = deserializer._parse_graphql_data(data)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2

    # Accept either direct node flattening or fallback edge rows.
    if "name" in result.columns:
        assert list(result["name"]) == ["Albania", "Andorra"]
        assert list(result["alpha2Code"]) == ["AL", "AD"]
    else:
        assert "node" in result.columns
        assert result.iloc[0]["node"]["name"] == "Albania"
        assert result.iloc[1]["node"]["name"] == "Andorra"


def test_extract_relay_nodes_non_list_edges_returns_empty_dataframe() -> None:
    deserializer = _deserializer()

    result = deserializer._extract_relay_nodes({"edges": {"node": {"id": 1}}})

    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_records_path_extracts_list_next_to_page_info() -> None:
    """Visma-style: items next to pageInfo / totalCount become rows."""
    deserializer = _deserializer()
    response = {
        "data": {
            "useCompany": {
                "associate": {
                    "items": [{"associateNo": 1}, {"associateNo": 2}],
                    "totalCount": 2,
                    "pageInfo": {"hasNextPage": False, "endCursor": "x"},
                }
            }
        }
    }
    df = deserializer.deserialize_graphql(response, records_path="data.useCompany.associate.items")
    assert df["associateNo"].tolist() == [1, 2]
    assert list(df.columns) == ["associateNo"]


def test_records_path_unwraps_relay_edges() -> None:
    """Xledger-style: edges are unwrapped to node; cursor is not a column."""
    deserializer = _deserializer()
    response = {"data": {"customers": {"edges": [{"cursor": "a", "node": {"dbId": 1}}, {"cursor": "b", "node": {"dbId": 2}}]}}}
    df = deserializer.deserialize_graphql(response, records_path="data.customers.edges")
    assert list(df.columns) == ["dbId"]
    assert df["dbId"].tolist() == [1, 2]


def test_records_path_missing_or_null_returns_empty() -> None:
    deserializer = _deserializer()
    assert deserializer.deserialize_graphql({"data": {"x": None}}, records_path="data.x.items").empty
    assert deserializer.deserialize_graphql({"data": {"x": {"items": None}}}, records_path="data.x.items").empty


def test_records_path_to_single_object_gives_one_row() -> None:
    """The value at records_path goes through the same rules as the default: an object is one row."""
    deserializer = _deserializer()
    df = deserializer.deserialize_graphql({"data": {"x": {"id": 1, "name": "a"}}}, records_path="data.x")
    assert df.to_dict(orient="records") == [{"id": 1, "name": "a"}]


def test_records_path_to_connection_unwraps_edges() -> None:
    deserializer = _deserializer()
    response = {"data": {"customers": {"pageInfo": {"hasNextPage": False}, "edges": [{"cursor": "a", "node": {"dbId": 1}}]}}}
    df = deserializer.deserialize_graphql(response, records_path="data.customers")
    assert df.to_dict(orient="records") == [{"dbId": 1}]


def test_default_records_path_is_data() -> None:
    deserializer = _deserializer()
    assert deserializer.deserialize_graphql({"data": {"users": [{"id": 1}]}})["id"].tolist() == [1]
    assert deserializer.deserialize_graphql({"users": [{"id": 1}]}).empty  # no "data" key → nothing to read


def test_top_level_edges_are_unwrapped() -> None:
    """{"data": {"edges": [...]}} → node fields (used to give a 'node' column of dicts)."""
    deserializer = _deserializer()
    df = deserializer.deserialize_graphql({"data": {"edges": [{"node": {"id": 1}}, {"node": {"id": 2}}]}})
    assert df.to_dict(orient="records") == [{"id": 1}, {"id": 2}]


def test_edges_with_typename_are_unwrapped() -> None:
    """``__typename`` on the edge is metadata: selecting it must not change the output schema."""
    deserializer = _deserializer()
    edges = [
        {"__typename": "CustomerEdge", "cursor": "a", "node": {"dbId": 1}},
        {"__typename": "CustomerEdge", "node": {"dbId": 2}},
    ]
    df = deserializer.deserialize_graphql({"data": {"customers": {"edges": edges}}}, records_path="data.customers.edges")
    assert df.to_dict(orient="records") == [{"dbId": 1}, {"dbId": 2}]


def test_node_typename_is_kept_as_a_column() -> None:
    """``__typename`` selected on the node is record data and stays."""
    deserializer = _deserializer()
    edges = [{"__typename": "CustomerEdge", "cursor": "a", "node": {"__typename": "Customer", "dbId": 1}}]
    df = deserializer.deserialize_graphql({"data": {"customers": {"edges": edges}}}, records_path="data.customers.edges")
    assert df.to_dict(orient="records") == [{"__typename": "Customer", "dbId": 1}]


def test_edge_with_other_extra_fields_is_not_unwrapped() -> None:
    """Edge fields other than cursor/__typename may be data (e.g. a score), so the edge stays as is."""
    deserializer = _deserializer()
    edges = [{"cursor": "a", "score": 0.9, "node": {"dbId": 1}}]
    df = deserializer.deserialize_graphql({"data": {"customers": {"edges": edges}}}, records_path="data.customers.edges")
    assert set(df.columns) == {"cursor", "score", "node"}


def test_row_with_node_field_is_not_unwrapped() -> None:
    """Only edge-shaped dicts (node + optional cursor / __typename) are unwrapped."""
    deserializer = _deserializer()
    rows = [{"id": 1, "node": {"x": 1}}, {"id": 2, "node": {"x": 2}}]
    df = deserializer.deserialize_graphql({"data": {"things": rows}})
    assert df["id"].tolist() == [1, 2]
    assert df["node"].tolist() == [{"x": 1}, {"x": 2}]
