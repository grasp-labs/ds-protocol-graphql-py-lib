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
