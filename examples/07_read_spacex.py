import uuid

from ds_protocol_http_py_lib import HttpLinkedService, HttpLinkedServiceSettings
from ds_protocol_http_py_lib.enums import AuthType
from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType

from ds_protocol_graphql_py_lib import GraphqlDeserializer
from ds_protocol_graphql_py_lib.dataset.graphql import (
    GraphqlDataset,
    GraphqlDatasetSettings,
    GraphqlReadSettings,
)

QUERY = """
query GetLaunches($limit: Int!, $mission: String!) {
  launchesPast(limit: $limit, find: { mission_name: $mission }) {
    mission_name
    launch_date_utc
    launch_site {
      site_name_long
    }
    rocket {
      rocket_name
      rocket_type
      first_stage {
        cores {
          flight
          reused
        }
      }
      second_stage {
        payloads {
          payload_type
          payload_mass_kg
          orbit
        }
      }
    }
    ships {
      name
      home_port
      image
    }
    links {
      article_link
      video_link
    }
  }
}
"""
linked_service = HttpLinkedService(
    settings=HttpLinkedServiceSettings(
        host="https://spacex-production.up.railway.app/",
        auth_type=AuthType.NO_AUTH,
    ),
    id=uuid.uuid4(),
    name="example::linked_service",
    version="1.0.0",
)

# Simple GraphQL introspection query
dataset = GraphqlDataset(
    deserializer=GraphqlDeserializer(format=DatasetStorageFormatType.JSON),
    settings=GraphqlDatasetSettings(
        url="https://spacex-production.up.railway.app/",
        read=GraphqlReadSettings(query=QUERY, variables={"limit": 3, "mission": "Starlink"}),
    ),
    linked_service=linked_service,
    id=uuid.uuid4(),
    name="example::dataset",
    version="1.0.0",
)

dataset.linked_service.connect()
dataset.read()
# print(dataset.output)
print(dataset.output.columns)
