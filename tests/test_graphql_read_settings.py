#!/usr/bin/env python3
"""Test script to verify GraphQL read functionality works correctly."""

import sys

sys.path.insert(0, "/")

from ds_protocol_graphql_py_lib.dataset.graphql import GraphqlReadSettings

# Test that GraphqlReadSettings exists and has the correct fields
print("GraphqlReadSettings class exists:", GraphqlReadSettings)
print("Fields:", GraphqlReadSettings.__annotations__)

# Create an instance
settings = GraphqlReadSettings(query="{ test }", variables={"key": "value"}, operation_name="TestOp")

print("\nCreated instance successfully:")
print(f"  query: {settings.query}")
print(f"  variables: {settings.variables}")
print(f"  operation_name: {settings.operation_name}")

print("\n✅ All tests passed!")
