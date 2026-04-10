"""
**File:** ``test_graphql_read_settings.py``
**Region:** ``tests/dataset``

Tests to verify GraphQL read functionality works correctly.

Covers:
    GraphqlReadSettings dataclass instantiation and field validation
    including query, variables, and operation_name parameters.
"""

from ds_protocol_graphql_py_lib.dataset.graphql import GraphqlReadSettings


def test_graphql_read_settings_has_expected_fields():
    """
    Ensure that the GraphqlReadSettings dataclass exists and exposes
    the expected annotated fields.
    """
    assert GraphqlReadSettings is not None
    annotations = getattr(GraphqlReadSettings, "__annotations__", {})

    # Required fields that should be present on the dataclass
    expected_fields = {"query", "variables", "operation_name"}
    missing_fields = expected_fields.difference(annotations.keys())

    assert not missing_fields, f"Missing expected fields: {missing_fields}"


def test_graphql_read_settings_instance_attributes():
    """
    Verify that an instance of GraphqlReadSettings can be constructed
    with the expected parameters and that its attributes are set correctly.
    """
    query = "{ test }"
    variables = {"key": "value"}
    operation_name = "TestOp"

    settings = GraphqlReadSettings(
        query=query,
        variables=variables,
        operation_name=operation_name,
    )

    assert settings.query == query
    assert settings.variables == variables
    assert settings.operation_name == operation_name
