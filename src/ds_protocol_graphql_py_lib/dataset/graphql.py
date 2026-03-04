from dataclasses import dataclass, field
from typing import Any, Generic, NoReturn, TypeVar

import pandas as pd  # type: ignore[import-untyped]
from ds_common_logger_py_lib import Logger
from ds_common_serde_py_lib import Serializable
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
class GraphqlReadSettings(Serializable):
    """Settings specific to reading data from GraphQL API."""

    query: str = ""
    variables: dict[str, Any] | None = None
    operation_name: str | None = None


@dataclass(kw_only=True)
class GraphqlDeleteSettings(Serializable):
    """Settings specific to deleting data from GraphQL API."""

    mutation: str = ""
    identity_columns: list[str] | None = None
    variables: dict[str, Any] | None = None
    operation_name: str | None = None


@dataclass(kw_only=True)
class GraphqlCreateSettings(Serializable):
    """Settings specific to creating data in GraphQL API."""

    mutation: str = ""
    input_field: str = ""  # The field name for input variables (e.g., "input")
    operation_name: str | None = None


@dataclass(kw_only=True)
class GraphqlDatasetSettings(DatasetSettings):
    url: str
    primary_keys: list[str] | None = None
    headers: dict[str, str] | None = None
    read: GraphqlReadSettings = field(default_factory=GraphqlReadSettings)
    delete: GraphqlDeleteSettings = field(default_factory=GraphqlDeleteSettings)
    create: GraphqlCreateSettings = field(default_factory=GraphqlCreateSettings)


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

        # Build GraphQL request payload
        payload: dict[str, Any] = {
            "query": self.settings.read.query,
        }

        if self.settings.read.variables:
            payload["variables"] = self.settings.read.variables

        if self.settings.read.operation_name:
            payload["operationName"] = self.settings.read.operation_name

        result = self.linked_service.connection.session.post(
            url=self.settings.url,
            json=payload,
            headers=self.settings.headers,
        )

        self.output = result.json()

    def _validate_create_settings(self) -> None:
        """Validate create settings are properly configured."""
        if self.linked_service.connection is None:
            raise ConnectionError(message="Connection is not initialized.") from None
        if not self.settings.create:
            raise CreateError("GraphQL create settings must be provided in settings.create")
        if not self.settings.create.mutation:
            raise CreateError("GraphQL mutation must be provided in settings.create.mutation")
        if not self.settings.create.input_field:
            raise CreateError("Input field name must be provided in settings.create.input_field")

    def _extract_created_rows(self, response_data: dict[str, Any], rows_data: list[dict[str, Any]]) -> list[Any]:
        """Extract created rows from GraphQL response."""
        if "data" not in response_data:
            return rows_data

        response_payload = response_data["data"]
        if isinstance(response_payload, dict):
            mutation_keys = [k for k in response_payload if k not in ["__typename"]]
            if len(mutation_keys) == 1:
                response_payload = response_payload[mutation_keys[0]]

        return response_payload if isinstance(response_payload, list) else [response_payload]

    def create(self) -> None:
        """
        Create new rows in the GraphQL endpoint using mutations.

        Sends all rows in a single atomic GraphQL mutation request.
        Populates self.output with the created rows.
        """
        # Per DATASET_CONTRACT: empty input is a no-op
        if self.input is None or len(self.input) == 0:
            self.output = self.input.copy() if self.input is not None else pd.DataFrame()
            return

        self._validate_create_settings()

        # After validation, we know these are not None
        create_settings = self.settings.create

        try:
            # Convert all rows to list of dictionaries
            rows_data = self.input.to_dict(orient="records")

            # Build variables - send as single object if one row, array if multiple
            input_value = rows_data[0] if len(rows_data) == 1 else rows_data

            variables = {create_settings.input_field: input_value}

            # Build GraphQL request payload
            payload: dict[str, Any] = {"query": create_settings.mutation}
            if variables:
                payload["variables"] = variables
            if create_settings.operation_name:
                payload["operationName"] = create_settings.operation_name

            # Execute the mutation (single atomic request)
            result = self.linked_service.connection.session.post(
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
                        "input_data": rows_data,
                        "errors": response_data.get("errors"),
                    },
                )

            # Extract created rows from response
            created_rows = self._extract_created_rows(response_data, rows_data)

            # Per DATASET_CONTRACT: populate self.output with created rows
            self.output = pd.DataFrame(created_rows) if created_rows else self.input.copy()

        except CreateError:
            raise
        except Exception as e:
            raise CreateError(
                message=f"Failed to create rows via GraphQL: {e!s}",
                details={
                    "url": self.settings.url,
                    "input_field": create_settings.input_field,
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

        Sends all rows in a single atomic GraphQL mutation request.
        Populates self.output with the deleted rows.
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

        # Verify identity columns exist in input
        for col in self.settings.delete.identity_columns:
            if col not in self.input.columns:
                raise DeleteError(
                    message=f"Identity column '{col}' not found in input",
                    details={"available_columns": list(self.input.columns)},
                )

        try:
            # Convert all rows to list of dictionaries
            rows_data = self.input.to_dict(orient="records")

            # Build variables using identity column values
            # Identity columns become GraphQL variable names
            variables = {}

            if len(rows_data) == 1:
                # Single row: extract identity values
                row = rows_data[0]
                for col in self.settings.delete.identity_columns:
                    variables[col] = row[col]
            else:
                # Multiple rows: build array for each identity column
                for col in self.settings.delete.identity_columns:
                    variables[col] = [row[col] for row in rows_data]

            if self.settings.delete.variables:
                variables.update(self.settings.delete.variables)

            # Build GraphQL request payload
            payload: dict[str, Any] = {
                "query": self.settings.delete.mutation,
            }

            if variables:
                payload["variables"] = variables

            if self.settings.delete.operation_name:
                payload["operationName"] = self.settings.delete.operation_name

            # Execute the mutation (single atomic request)
            result = self.linked_service.connection.session.post(
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
                        "input_data": rows_data,
                        "errors": response_data.get("errors"),
                    },
                )

            # Extract deleted rows from response
            if "data" in response_data:
                response_payload = response_data["data"]
                # Unwrap nested mutation response
                if isinstance(response_payload, dict):
                    mutation_keys = [k for k in response_payload if k not in ["__typename"]]
                    if len(mutation_keys) == 1:
                        response_payload = response_payload[mutation_keys[0]]

                # Convert to list if needed
                deleted_rows = response_payload if isinstance(response_payload, list) else [response_payload]
            else:
                deleted_rows = rows_data

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
