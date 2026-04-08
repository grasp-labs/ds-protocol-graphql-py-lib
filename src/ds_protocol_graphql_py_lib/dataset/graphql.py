"""
**File:** ``graphql.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset``

GraphQL dataset implementation for CRUD operations via GraphQL API.

Example:
    >>> linked_service = HttpLinkedService(
    ...     settings=HttpLinkedServiceSettings(
    ...         host="https://api.example.graphql/graphql",
    ...         auth_type=AuthType.NO_AUTH,
    ...     ),
    ...     id="service-id",
    ...     name="graphql_service",
    ...     version="1.0.0",
    ... )
    >>> dataset = GraphqlDataset(
    ...     linked_service=linked_service,
    ...     settings=GraphqlDatasetSettings(
    ...         url="https://api.example.graphql/graphql",
    ...         read=GraphqlReadSettings(
    ...             query="{ users { id name email } }"
    ...         ),
    ...     ),
    ...     id="dataset-id",
    ...     name="graphql_dataset",
    ...     version="1.0.0",
    ... )
    >>> dataset.read()
"""

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
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer

from ..enums import ResourceType
from ..serde.deserializer import GraphqlDeserializer

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class GraphqlReadSettings(Serializable):
    """Settings specific to reading data from GraphQL API."""

    query: str
    """ The GraphQL query string to execute for reading data.
    This should be a valid GraphQL query that the endpoint can execute to return the desired data.
    For example: "{ users { id name email } }"
    """
    variables: dict[str, Any] | None = None
    """ Optional variables to include with the GraphQL query."""
    operation_name: str | None = None
    """ Optional operation name for the GraphQL query, used when the query contains multiple operations."""


@dataclass(kw_only=True)
class GraphqlDeleteSettings(Serializable):
    """Settings specific to deleting data from GraphQL API."""

    mutation: str
    """ The GraphQL mutation string to execute for deleting data."""
    identity_columns: list[str]
    """ The list of column names in the input DataFrame that uniquely identify the rows to delete."""
    variables: dict[str, Any] | None = None
    """ Optional variables to include with the GraphQL mutation."""
    operation_name: str | None = None
    """ Optional operation name for the GraphQL mutation, used when the mutation contains multiple operations."""


@dataclass(kw_only=True)
class GraphqlCreateSettings(Serializable):
    """Settings specific to creating data in GraphQL API."""

    mutation: str
    """ The GraphQL mutation string to execute for creating data.
    This should be a valid GraphQL mutation that the endpoint can execute to create new records based on the input data.
    For example: "mutation CreateUser($input: CreateUserInput!) { createUser(input: $input) { id name email } }"
    """
    input_field: str  # The field name for input variables (e.g., "input")
    """ The name of the variable in the GraphQL mutation that will receive the input data."""
    operation_name: str | None = None
    """ Optional operation name for the GraphQL mutation, used when the mutation contains multiple operations."""


@dataclass(kw_only=True)
class GraphqlDatasetSettings(DatasetSettings):
    url: str
    """The URL of the GraphQL endpoint to connect to. This is the base URL where the GraphQL API is hosted."""
    primary_keys: list[str] | None = None
    """Optional list of column names that serve as primary keys for the dataset.
    This can be used for operations that require unique identification of rows."""
    headers: dict[str, str] | None = None
    """Optional HTTP headers to include in requests to the GraphQL endpoint, such as authentication tokens or content type."""
    read: GraphqlReadSettings | None = None
    """Settings for read operations."""
    delete: GraphqlDeleteSettings | None = None
    """Settings for delete operations."""
    create: GraphqlCreateSettings | None = None
    """Settings for create operations."""


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
        GraphqlDeserializer,
    ],
    Generic[HttpLinkedServiceType, GraphqlDatasetSettingsType],
):
    """
    Represent Graphql dataset.
    """

    settings: GraphqlDatasetSettingsType
    linked_service: HttpLinkedServiceType

    deserializer: GraphqlDeserializer | None = field(
        default_factory=lambda: GraphqlDeserializer(format=DatasetStorageFormatType.JSON),
    )

    @property
    def type(self) -> ResourceType:
        return ResourceType.DATASET

    @property
    def supports_checkpoint(self) -> bool:
        """
        Indicate whether this provider supports incremental loads via checkpointing.

        GraphQL provider does not yet support checkpoint-based incremental loads.
        All reads are full loads.

        Returns:
            False, indicating checkpointing is not supported.
        """
        return False

    def read(self) -> None:
        """
        Read Graphql dataset.

        Sends a GraphQL query to the endpoint with the query, variables, and operation name
        specified in settings.read. Populates self.output with the result as a DataFrame.

        Handles various GraphQL response patterns via GraphqlDeserializer:
        - Direct arrays: {"data": {"users": [...]}}
        - Relay connections: {"data": {"users": {"edges": [{"node": {...}}]}}}
        - Single objects: {"data": {"user": {...}}}

        Returns:
            None. The result is stored in self.output as a DataFrame.

        Raises:
            ConnectionError: If the linked service connection is not initialized.
            ReadError: If read settings are not provided or if the GraphQL query fails.
        """
        if self.linked_service.connection is None:
            raise ConnectionError(message="Connection is not initialized.") from None

        if not self.settings.read:
            raise ReadError("GraphQL read settings must be provided in settings.read")

        if not self.deserializer:
            raise ReadError("Deserializer is not configured for GraphQL dataset")

        try:
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

            result.raise_for_status()
            response_data = result.json()

            self._check_for_graphql_read_error(response_data)
            self.output = self.deserializer.deserialize_graphql(response_data)

        except ReadError:
            raise
        except Exception as e:
            raise ReadError(
                message=f"Failed to read from GraphQL: {e!s}",
                details={"url": self.settings.url},
            ) from e

    @staticmethod
    def _check_for_graphql_read_error(response_data: dict[str, Any]) -> None:
        if "errors" in response_data:
            base_message = "GraphQL query failed"

            errors = response_data["errors"]

            # Safely extract a human-readable message from the first error, if available.
            if isinstance(errors, list) and errors:
                first_error = errors[0]
                if isinstance(first_error, dict):
                    error_message = first_error.get("message")
                    if isinstance(error_message, str) and error_message:
                        base_message = f"{base_message}: {error_message}"
            elif isinstance(errors, dict):
                error_message = errors.get("message")
                if isinstance(error_message, str) and error_message:
                    base_message = f"{base_message}: {error_message}"

            raise ReadError(
                message=base_message,
                details={"errors": errors},
            )

    def create(self) -> None:
        """
        Create new rows in the GraphQL endpoint using mutations.

        Sends all rows in a single atomic GraphQL mutation request.
        Populates self.output with the created rows.

        Returns:
            None. The result is stored in self.output as a DataFrame.

        Raises:
            ConnectionError: If the linked service connection is not initialized.
            CreateError: If create settings are not provided or if the GraphQL mutation fails.
        """
        # Per DATASET_CONTRACT: empty input is a no-op
        if self.input is None or len(self.input) == 0:
            self.output = self.input.copy() if self.input is not None else pd.DataFrame()
            return

        if self.settings.create is None:
            raise CreateError("Create settings must be provided in settings.create")

        if not self.deserializer:
            raise CreateError("Deserializer is not configured for GraphQL dataset")

        self._validate_create_settings()

        try:
            rows_data = self.input.to_dict(orient="records")
            input_value = rows_data[0] if len(rows_data) == 1 else rows_data

            variables = {self.settings.create.input_field: input_value}

            payload: dict[str, Any] = {"query": self.settings.create.mutation}
            if variables:
                payload["variables"] = variables
            if self.settings.create.operation_name:
                payload["operationName"] = self.settings.create.operation_name

            result = self.linked_service.connection.session.post(
                url=self.settings.url,
                json=payload,
                headers=self.settings.headers,
            )

            result.raise_for_status()
            response_data = result.json()

            if "errors" in response_data:
                raise CreateError(
                    message="GraphQL create mutation failed",
                    details={
                        "input_data": rows_data,
                        "errors": response_data.get("errors"),
                    },
                )

            if "data" in response_data:
                df_result = self.deserializer.deserialize_graphql(response_data)
                self.output = df_result if not df_result.empty else self.input.copy()
            else:
                self.output = self.input.copy()

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

        Sends all rows in a single atomic GraphQL mutation request.
        Populates self.output with the deleted rows.

        Returns:
            None. The result is stored in self.output as a DataFrame.

        Raises:
            ConnectionError: If the linked service connection is not initialized.
            DeleteError: If delete settings are not provided, if identity columns are missing, or if the GraphQL mutation fails.
        """
        # Per DATASET_CONTRACT: empty input is a no-op
        if self.input is None or len(self.input) == 0:
            self.output = self.input.copy() if self.input is not None else pd.DataFrame()
            return

        if self.linked_service.connection is None:
            raise ConnectionError(message="Connection is not initialized.") from None

        if not self.settings.delete:
            raise DeleteError("GraphQL delete settings must be provided in settings.delete")

        if not self.deserializer:
            raise DeleteError("Deserializer is not configured for GraphQL dataset")

        for col in self.settings.delete.identity_columns:
            if col not in self.input.columns:
                raise DeleteError(
                    message=f"Identity column '{col}' not found in input",
                    details={"available_columns": list(self.input.columns)},
                )

        try:
            rows_data = self.input.to_dict(orient="records")
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

            if "errors" in response_data:
                raise DeleteError(
                    message="GraphQL delete mutation failed",
                    details={
                        "input_data": rows_data,
                        "errors": response_data.get("errors"),
                    },
                )

            if "data" in response_data:
                df_result = self.deserializer.deserialize_graphql(response_data)
                self.output = df_result if not df_result.empty else self.input.copy()
            else:
                self.output = self.input.copy()

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

        Returns:
            None. The result is stored in self.output as a DataFrame.

        Raises:
            ConnectionError: If the linked service connection is not initialized.
            ListError: If the GraphQL introspection query fails.
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
        """
        Just to satisfy the contract - GraphQL dataset does not maintain persistent connections that require cleanup.

        Returns:
            None
        """
        pass

    def _validate_create_settings(self) -> None:
        """
        Validate create settings are properly configured.

        Returns:
            None: if settings are valid.

        Raises:
            CreateError: If any required create settings are missing or invalid.
        """
        if self.linked_service.connection is None:
            raise ConnectionError(message="Connection is not initialized.") from None
        if not self.settings.create:
            raise CreateError("GraphQL create settings must be provided in settings.create")
        if not self.settings.create.mutation:
            raise CreateError("GraphQL mutation must be provided in settings.create.mutation")
        if not self.settings.create.input_field:
            raise CreateError("Input field name must be provided in settings.create.input_field")
