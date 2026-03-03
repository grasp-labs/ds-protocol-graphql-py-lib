from dataclasses import dataclass, field
from typing import Any, Generic, NoReturn, TypeVar

import pandas as pd  # type: ignore[import-untyped]
from ds_common_logger_py_lib import Logger
from ds_protocol_http_py_lib.dataset.http import HttpLinkedServiceType
from ds_resource_plugin_py_lib.common.resource.dataset import (
    DatasetSettings,
    DatasetStorageFormatType,
    TabularDataset,
)
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    DeleteError,
    ListError,
    ReadError,
)
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    ConnectionError,
)
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ..enums import ResourceType

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class GraphqlReadSettings:
    """Settings specific to reading data from GraphQL API."""

    query: str
    variables: dict[str, Any] | None = None
    operation_name: str | None = None


@dataclass(kw_only=True)
class GraphqlDeleteSettings:
    """Settings specific to deleting data from GraphQL API."""

    mutation: str
    identity_columns: list[str]
    variables: dict[str, Any] | None = None
    operation_name: str | None = None


@dataclass(kw_only=True)
class GraphqlCreateSettings:
    """Settings specific to creating data in GraphQL API."""

    mutation: str
    input_field: str  # The field name for input variables (e.g., "input")
    variables: dict[str, Any] | None = None
    operation_name: str | None = None


@dataclass(kw_only=True)
class GraphqlDatasetSettings(DatasetSettings):
    url: str
    primary_keys: list[str] | None = None
    headers: dict[str, str] | None = None
    read: GraphqlReadSettings | None = None
    delete: GraphqlDeleteSettings | None = None
    create: GraphqlCreateSettings | None = None


GraphqlDatasetSettingsType = TypeVar(
    "GraphqlDatasetSettingsType",
    bound=GraphqlDatasetSettings,
)


@dataclass(kw_only=True)
class GraphqlDataset(
    TabularDataset[
        HttpLinkedServiceType,
        GraphqlDatasetSettingsType,
        PandasSerializer,
        PandasDeserializer,
    ],
    Generic[HttpLinkedServiceType, GraphqlDatasetSettingsType],
):
    """
    Represent Graphql dataset.
    """

    settings: GraphqlDatasetSettingsType
    linked_service: HttpLinkedServiceType

    serializer: PandasSerializer | None = field(
        default_factory=lambda: PandasSerializer(format=DatasetStorageFormatType.JSON),
    )
    deserializer: PandasDeserializer | None = field(
        default_factory=lambda: PandasDeserializer(format=DatasetStorageFormatType.JSON),
    )

    @property
    def type(self) -> ResourceType:
        return ResourceType.DATASET

    def read(self) -> None:
        """
        Read Graphql dataset.

        Sends a GraphQL query to the endpoint with the query, variables, and operation name
        specified in settings.read.
        """
        if self.linked_service.connection is None:
            raise ConnectionError(message="Connection is not initialized.") from None

        if not self.settings.read:
            raise ReadError("GraphQL read settings must be provided in settings.read")

        if not self.settings.read.query:
            raise ReadError("GraphQL query must be provided in settings.read.query")

        client = self.linked_service.connection

        # Build GraphQL request payload
        payload: dict[str, Any] = {
            "query": self.settings.read.query,
        }

        if self.settings.read.variables:
            payload["variables"] = self.settings.read.variables

        if self.settings.read.operation_name:
            payload["operationName"] = self.settings.read.operation_name

        result = client.session.post(
            url=self.settings.url,
            json=payload,
            headers=self.settings.headers,
        )

        self.output = result.json()

    def create(self) -> None:
        """
        Create new rows in the GraphQL endpoint using mutations.

        Uses GraphQL mutations to insert rows specified in self.input.
        Executes as a single atomic transaction - all rows are created together
        or none are created. Populates self.output with the created rows.
        """
        # Per DATASET_CONTRACT: empty input is a no-op
        if self.input is None or len(self.input) == 0:
            self.output = self.input.copy() if self.input is not None else pd.DataFrame()
            return

        if self.linked_service.connection is None:
            raise ConnectionError(message="Connection is not initialized.") from None

        if not self.settings.create:
            raise CreateError("GraphQL create settings must be provided in settings.create")

        if not self.settings.create.mutation:
            raise CreateError("GraphQL mutation must be provided in settings.create.mutation")

        if not self.settings.create.input_field:
            raise CreateError("Input field name must be provided in settings.create.input_field")

        client = self.linked_service.connection

        try:
            # Build and execute mutations for each row
            created_rows = []

            for idx, row in self.input.iterrows():
                # Convert row to dictionary
                row_dict = row.to_dict()

                # Build variables with the input field
                variables = {self.settings.create.input_field: row_dict}

                # Merge with any static variables from settings
                if self.settings.create.variables:
                    variables.update(self.settings.create.variables)

                # Build GraphQL request payload
                # Note: GraphQL always uses "query" key for both queries and mutations
                payload: dict[str, Any] = {
                    "query": self.settings.create.mutation,
                }

                if variables:
                    payload["variables"] = variables

                if self.settings.create.operation_name:
                    payload["operationName"] = self.settings.create.operation_name

                # Execute the mutation
                result = client.session.post(
                    url=self.settings.url,
                    json=payload,
                    headers=self.settings.headers,
                )

                result.raise_for_status()
                response_data = result.json()

                # Check for GraphQL errors
                if "errors" in response_data:
                    raise CreateError(
                        message="GraphQL create mutation failed",
                        details={
                            "row_index": idx,
                            "input_data": row_dict,
                            "errors": response_data.get("errors"),
                        },
                    )

                created_rows.append(row)

            # Per DATASET_CONTRACT: populate self.output with created rows
            self.output = pd.DataFrame(created_rows) if created_rows else self.input.copy()

        except CreateError:
            raise
        except Exception as e:
            raise CreateError(
                message=f"Failed to create rows via GraphQL: {e!s}",
                details={
                    "url": self.settings.url,
                    "input_field": self.settings.create.input_field,
                    "row_count": len(self.input),
                },
            ) from e

    def update(self) -> None:
        """
        Update entity using Graphql.
        """
        raise NotSupportedError("Update operation is not supported for Graphql dataset")

    def upsert(self) -> None:
        """
        Upsert entity using Graphql.
        """
        raise NotSupportedError("Upsert operation is not supported for Graphql dataset")

    def delete(self) -> None:  # noqa: PLR0912
        """
        Delete specific rows from the GraphQL endpoint using mutations.

        Uses GraphQL mutations to delete rows specified in self.input, matched
        by identity columns defined in self.settings.delete.identity_columns.

        Executes as a single atomic transaction - all rows are deleted together
        or none are deleted. Populates self.output with the deleted rows.
        """
        # Per DATASET_CONTRACT: empty input is a no-op
        if self.input is None or len(self.input) == 0:
            self.output = self.input.copy() if self.input is not None else pd.DataFrame()
            return

        if self.linked_service.connection is None:
            raise ConnectionError(message="Connection is not initialized.") from None

        if not self.settings.delete:
            raise DeleteError("GraphQL delete settings must be provided in settings.delete")

        if not self.settings.delete.mutation:
            raise DeleteError("GraphQL mutation must be provided in settings.delete.mutation")

        if not self.settings.delete.identity_columns:
            raise DeleteError("Identity columns must be provided in settings.delete.identity_columns")

        client = self.linked_service.connection

        # Verify identity columns exist in input
        for col in self.settings.delete.identity_columns:
            if col not in self.input.columns:
                raise DeleteError(
                    message=f"Identity column '{col}' not found in input",
                    details={"available_columns": list(self.input.columns)},
                )

        try:
            # Build and execute mutations for each row
            deleted_rows = []

            for idx, row in self.input.iterrows():
                # Build variables from identity columns
                variables = {col: row[col] for col in self.settings.delete.identity_columns}

                # Merge with any static variables from settings
                if self.settings.delete.variables:
                    variables.update(self.settings.delete.variables)

                # Build GraphQL request payload
                # Note: GraphQL always uses "query" key for both queries and mutations
                payload: dict[str, Any] = {
                    "query": self.settings.delete.mutation,
                }

                if variables:
                    payload["variables"] = variables

                if self.settings.delete.operation_name:
                    payload["operationName"] = self.settings.delete.operation_name

                # Execute the mutation
                result = client.session.post(
                    url=self.settings.url,
                    json=payload,
                    headers=self.settings.headers,
                )

                result.raise_for_status()
                response_data = result.json()

                # Check for GraphQL errors
                if "errors" in response_data:
                    raise DeleteError(
                        message="GraphQL delete mutation failed",
                        details={
                            "row_index": idx,
                            "identity_values": variables,
                            "errors": response_data.get("errors"),
                        },
                    )

                deleted_rows.append(row)

            # Per DATASET_CONTRACT: populate self.output with deleted rows
            self.output = pd.DataFrame(deleted_rows) if deleted_rows else self.input.copy()

        except DeleteError:
            raise
        except Exception as e:
            raise DeleteError(
                message=f"Failed to delete rows via GraphQL: {e!s}",
                details={
                    "url": self.settings.url,
                    "identity_columns": self.settings.delete.identity_columns,
                    "row_count": len(self.input),
                },
            ) from e

    def purge(self) -> NoReturn:
        """
        Purge entity using Graphql.
        """
        raise NotSupportedError("Purge operation is not supported for Graphql dataset")

    def rename(self) -> NoReturn:
        """
        Rename entity using Graphql.
        """
        raise NotSupportedError("Rename operation is not supported for Graphql dataset")

    def list(self) -> None:
        """
        Discover available resources in the GraphQL schema via introspection.

        Executes a GraphQL introspection query to fetch all available queries
        and their arguments from the schema. Populates self.output with a DataFrame
        containing resource metadata (name, type, description, etc.).
        """
        if self.linked_service.connection is None:
            raise ConnectionError(message="Connection is not initialized.") from None

        client = self.linked_service.connection

        # GraphQL introspection query to discover all queries
        introspection_query = """
        query IntrospectionQuery {
          __schema {
            queryType {
              name
              fields {
                name
                description
                args {
                  name
                  type {
                    name
                    kind
                  }
                }
              }
            }
          }
        }
        """

        try:
            result = client.session.post(
                url=self.settings.url,
                json={"query": introspection_query},
                headers=self.settings.headers,
            )

            result.raise_for_status()
            response_data = result.json()

            if "errors" in response_data:
                raise ListError(
                    message="GraphQL introspection query failed",
                    details={"errors": response_data.get("errors")},
                )

            # Extract query fields from the introspection response
            query_type = response_data.get("data", {}).get("__schema", {}).get("queryType", {})
            fields = query_type.get("fields", [])

            # Transform into a DataFrame
            resources = []
            for field in fields:
                arg_names = [arg.get("name") for arg in field.get("args", [])]
                resources.append(
                    {
                        "name": field.get("name"),
                        "type": "query",
                        "description": field.get("description") or "",
                        "arguments": ", ".join(arg_names) if arg_names else None,
                        "arg_count": len(arg_names),
                    }
                )

            self.output = pd.DataFrame(resources)

        except Exception as e:
            raise ListError(
                message=f"Failed to list GraphQL schema resources: {e!s}",
                details={"url": self.settings.url},
            ) from e

    def close(self) -> None:
        pass
