"""
**File:** ``json_utils.py``
**Region:** ``ds_protocol_graphql_py_lib/utils/json_utils``

Restricted dotted-path helpers for GraphQL response JSON.

Paths are object members and list indexes separated by dots
(``data.customers.edges``, ``data.items.0.id``). Negative indexes count from
the end (``data.customers.edges.-1.cursor`` is the last edge's cursor).
``$`` or ``""`` is the root. No wildcards, filters, slices, or recursive descent.

Example:
    >>> body = {"data": {"customers": {"edges": [{"cursor": "a"}]}}}
    >>> get_path(body, "data.customers.edges.0.cursor")
    'a'
    >>> get_path(body, "data.customers.edges.-1.cursor")
    'a'
    >>> get_list(body, "data.customers.edges")
    [{'cursor': 'a'}]
"""

from __future__ import annotations

from typing import Any

_MISSING: Any = object()


def _resolve(data: Any, path: str) -> Any:
    """Like :func:`get_path`, but returns ``_MISSING`` (not ``None``) when the path does not exist."""
    if path in ("$", ""):
        return data

    current = data
    for segment in path.split("."):
        if isinstance(current, dict):
            if segment not in current:
                return _MISSING
            current = current[segment]
            continue
        if isinstance(current, list):
            try:
                current = current[int(segment)]
            except (ValueError, IndexError):
                return _MISSING
            continue
        return _MISSING
    return current


def get_path(data: Any, path: str) -> Any:
    """
    Read a value via dotted path.

    Args:
        data: Parsed JSON value.
        path: Dotted path; ``$`` or ``""`` returns ``data`` itself. List
            segments are integer indexes; negative indexes count from the end.

    Returns:
        The value at ``path``, or ``None`` when any segment is missing or a
        list index is out of range (e.g. ``-1`` on an empty list).
    """
    value = _resolve(data, path)
    return None if value is _MISSING else value


def get_list(data: Any, path: str) -> list[Any]:
    """
    Read a list at ``path``.

    A ``null`` list is an empty list: GraphQL APIs commonly return ``null``
    instead of ``[]`` for an empty nullable list (Visma Business NXT does).
    A missing path, including one whose parent is ``null``, is an error, so a
    typo in the path fails loudly instead of reading as "no rows".

    Args:
        data: Parsed JSON value.
        path: Dotted path to a list.

    Returns:
        The list at ``path``, or ``[]`` when the value there is ``null``.

    Raises:
        ValueError: If the path is missing.
        TypeError: If the path resolves to a non-list value.
    """
    value = _resolve(data, path)
    if value is _MISSING:
        raise ValueError(f"path '{path}' is missing in response")
    if value is None:
        return []
    if not isinstance(value, list):
        raise TypeError(f"path '{path}' did not resolve to a list (got {type(value).__name__})")
    return value
