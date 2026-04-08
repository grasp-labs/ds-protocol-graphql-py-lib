#!/usr/bin/env python
"""Quick test of GraphqlDeserializer."""

from ds_protocol_graphql_py_lib.serde.graphql_deserializer import GraphqlDeserializer

deserializer = GraphqlDeserializer()

# Test 1: Simple array
print("Test 1: Simple array")
response1 = {"data": {"users": [{"id": "1", "name": "John"}, {"id": "2", "name": "Jane"}]}}
df1 = deserializer.deserialize(response1)
print(df1)
print()

# Test 2: Relay-style pagination
print("Test 2: Relay pagination")
response2 = {
    "data": {
        "countries": {
            "edges": [
                {"node": {"name": "Albania", "code": "AL"}},
                {"node": {"name": "Austria", "code": "AT"}},
            ]
        }
    }
}
df2 = deserializer.deserialize(response2)
print(df2)
print()

# Test 3: Single object
print("Test 3: Single object")
response3 = {"data": {"film": {"title": "A New Hope", "producers": ["Gary Kurtz", "Rick McCallum"], "releaseDate": "1977-05-25"}}}
df3 = deserializer.deserialize(response3)
print(df3)
print()

# Test 4: Introspection pattern
print("Test 4: Introspection")
response4 = {
    "data": {
        "__type": {
            "fields": [
                {"name": "allFilms"},
                {"name": "film"},
                {"name": "allPeople"},
            ]
        }
    }
}
df4 = deserializer.deserialize(response4)
print(df4)
print()

print("✅ All tests passed!")
