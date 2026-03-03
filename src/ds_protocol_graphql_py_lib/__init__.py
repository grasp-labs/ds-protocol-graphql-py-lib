"""
**File:** ``__init__.py``
**Region:** ``ds-protocol-graphql-py-lib``

Description
-----------
A Python package from the ds-protocol-graphql-py-lib library.

Example
-------
.. code-block:: python

    from ds_provider_graphql_py_lib import __version__

    print(f"Package version: {__version__}")
"""

from importlib.metadata import version

from .dataset import GraphqlDataset, GraphqlDatasetSettings

PACKAGE_NAME = "ds-protocol-graphql-py-lib"
__version__ = version(PACKAGE_NAME)

__all__ = [
    "GraphqlDataset",
    "GraphqlDatasetSettings",
    "__version__",
]
