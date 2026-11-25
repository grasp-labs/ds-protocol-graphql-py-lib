"""
**File:** ``paginate.py``
**Region:** ``ds_protocol_graphql_py_lib/dataset/pagination/paginate``

Paginate composer: page strategy + incremental rules + checkpoint + GraphQL transport.

Example:
    >>> dataset.settings.read.pagination = PaginationSettings(
    ...     strategy=PaginationStrategy.CURSOR,
    ...     items_path="data.customers.edges",
    ...     cursor=CursorPaginationSettings(
    ...         cursor_path="data.customers.edges.-1.cursor",
    ...         has_next_path="data.customers.pageInfo.hasNextPage",
    ...     ),
    ... )
    >>> Paginate.from_dataset(dataset).run(dataset)
"""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Any

import pandas as pd  # type: ignore[import-untyped]
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.dataset.errors import ReadError
from ds_resource_plugin_py_lib.common.resource.errors import ResourceException
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    AuthenticationError,
    AuthorizationError,
    ConnectionError,
)

from ...utils.json_utils import get_list
from ..incremental import NoIncrementalRules, get_rules
from ..request import GraphqlRequestSnapshot
from .registry import get_strategy

if TYPE_CHECKING:
    from ..graphql import GraphqlDataset, GraphqlReadSettings
    from ..incremental.rules import IncrementalRules
    from ..incremental.settings import IncrementalSettings
    from .settings import PaginationSettings
    from .strategies.base import PaginationStrategyHandler

logger = Logger.get_logger(__name__, package=True)

_CHECKPOINT_KEY = "pagination"


class Paginate:
    """
    Composer for a paginated ``GraphqlDataset.read``.

    Resolves a registered page strategy and optional incremental ruleset, then
    drains pages. Strategy hooks supply page variation only; incremental rules
    supply the lower bound and advance the watermark. This class owns
    transport, GraphQL error checks, deserialization, the
    ``checkpoint["pagination"]`` slice, and error mapping.
    """

    def __init__(
        self,
        strategy: PaginationStrategyHandler[Any],
        read_settings: GraphqlReadSettings,
        pagination: PaginationSettings,
        incremental_rules: IncrementalRules | None = None,
        incremental: IncrementalSettings | None = None,
    ) -> None:
        self.strategy = strategy
        self.read_settings = read_settings
        self.pagination = pagination
        self.incremental_rules: IncrementalRules = incremental_rules or NoIncrementalRules()
        self.incremental = incremental

    @classmethod
    def from_dataset(cls, dataset: GraphqlDataset[Any, Any]) -> Paginate:
        """
        Build from ``dataset.settings.read`` and the strategy / rules registries.

        Raises:
            ValueError: If ``settings.read.pagination`` is not configured.
        """
        read_settings = dataset.settings.read
        if read_settings is None or read_settings.pagination is None:
            raise ValueError("Paginate requires settings.read.pagination")
        incremental = read_settings.incremental
        return cls(
            strategy=get_strategy(read_settings.pagination.strategy),
            read_settings=read_settings,
            pagination=read_settings.pagination,
            incremental_rules=get_rules(incremental.strategy) if incremental is not None else NoIncrementalRules(),
            incremental=incremental,
        )

    @property
    def records_path(self) -> str:
        """Output record path: ``read.records_path``, falling back to ``items_path``."""
        return self.read_settings.records_path or self.pagination.items_path

    def run(self, dataset: GraphqlDataset[Any, Any]) -> None:
        """
        Prepare → drain → assign ``output`` / checkpoint.

        Incremental ``prepare`` injects the stored watermark (if any) into the
        base request before the first page; ``observe`` records each fetched
        page (the pending watermark max) as the drain goes.

        On full success: ``output`` is all pages of this call, incremental
        ``commit`` advances ``checkpoint["incremental"]`` (including what earlier
        failed attempts observed), then ``checkpoint["pagination"]`` is cleared.
        On failure (including a failing ``commit``): ``output`` holds the pages
        fetched so far, ``checkpoint["pagination"]`` is kept, and the watermark
        is unchanged.

        Raises:
            ReadError: On any GraphQL, transport, paging-control, or parse failure.
            ConnectionError / AuthenticationError / AuthorizationError: Re-raised as-is.
        """
        frames: list[pd.DataFrame] = []
        try:
            request = self.incremental_rules.prepare(self._snapshot(dataset), dataset.checkpoint, self.incremental)
            self.drain(dataset, request, frames)
            dataset.output = self._concat_frames(frames)
            self.incremental_rules.commit(dataset.checkpoint, dataset.output, self.incremental)
            dataset.checkpoint.pop(_CHECKPOINT_KEY, None)
        except (AuthenticationError, AuthorizationError, ConnectionError, ReadError):
            dataset.output = self._concat_frames(frames)
            raise
        except ResourceException as exc:
            dataset.output = self._concat_frames(frames)
            raise ReadError(
                message=exc.message,
                status_code=exc.status_code,
                details={**exc.details, "type": dataset.type.value},
            ) from exc
        except (AttributeError, KeyError, TypeError, ValueError) as exc:
            dataset.output = self._concat_frames(frames)
            raise ReadError(
                message=str(exc),
                status_code=400,
                details={"type": dataset.type.value, "url": dataset.settings.url},
            ) from exc
        except Exception as exc:
            dataset.output = self._concat_frames(frames)
            raise ReadError(
                message=f"Failed to read from GraphQL: {exc!s}",
                details={"type": dataset.type.value, "url": dataset.settings.url},
            ) from exc

    def drain(
        self,
        dataset: GraphqlDataset[Any, Any],
        request: GraphqlRequestSnapshot,
        frames: list[pd.DataFrame] | None = None,
    ) -> list[pd.DataFrame]:
        """
        Page loop via strategy hooks; write mid-run ``checkpoint["pagination"]``.

        Appends page frames to ``frames`` so callers keep partial output when
        this method raises mid-drain.

        Raises:
            ValueError: If the checkpoint strategy mismatches.
            ReadError: If GraphQL returns errors or ``max_pages`` is exceeded.
        """
        if frames is None:
            frames = []

        deserializer = dataset.deserializer

        pagination = self.pagination
        strategy = self.strategy
        cfg = pagination.strategy_config
        checkpoint_slice = dataset.checkpoint.get(_CHECKPOINT_KEY)
        self._validate_checkpoint_strategy(checkpoint_slice, pagination.strategy.value)
        state = strategy.initial_state(cfg, checkpoint_slice)

        fetched = 0
        while pagination.max_pages is None or fetched < pagination.max_pages:
            fetched += 1
            page_request = strategy.inject(request, state, cfg)
            logger.debug(f"Fetching GraphQL page {state.page_index} from {page_request.url}")
            response = dataset.linked_service.connection.post(
                url=page_request.url,
                json=page_request.to_payload(),
                headers=page_request.headers,
            )
            body = response.json()
            dataset._check_for_graphql_read_error(body)

            items = get_list(body, pagination.items_path)
            frame = deserializer.deserialize_graphql(body, records_path=self.records_path)
            frames.append(frame)
            self.incremental_rules.observe(dataset.checkpoint, frame, self.incremental)

            if strategy.should_stop(body=body, items=items, state=state, cfg=cfg):
                break

            state = strategy.advance(body=body, items=items, state=state, cfg=cfg)
            dataset.checkpoint[_CHECKPOINT_KEY] = strategy.to_checkpoint(state)
        else:
            raise ReadError(
                message=f"Pagination exceeded max_pages={pagination.max_pages} for strategy '{pagination.strategy}'",
                status_code=400,
                details={
                    "type": dataset.type.value,
                    "strategy": pagination.strategy.value,
                    "max_pages": pagination.max_pages,
                },
            )

        return frames

    def _snapshot(self, dataset: GraphqlDataset[Any, Any]) -> GraphqlRequestSnapshot:
        """Build an isolated request from settings; caller variables are deep-copied."""
        return GraphqlRequestSnapshot(
            url=dataset.settings.url,
            query=self.read_settings.query,
            variables=deepcopy(self.read_settings.variables or {}),
            operation_name=self.read_settings.operation_name,
            headers=deepcopy(dataset.settings.headers),
        )

    @staticmethod
    def _validate_checkpoint_strategy(checkpoint_slice: Any, expected: str) -> None:
        if checkpoint_slice is None:
            return
        if not isinstance(checkpoint_slice, dict):
            raise TypeError(f"checkpoint['pagination'] must be a dict (got {type(checkpoint_slice).__name__})")
        actual = checkpoint_slice.get("strategy")
        if actual is not None and actual != expected:
            raise ValueError(
                f"Checkpoint pagination strategy '{actual}' does not match configured strategy '{expected}'",
            )

    @staticmethod
    def _concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
        """Concatenate page frames, or return an empty frame when none were fetched."""
        non_empty = [frame for frame in frames if not frame.empty]
        return pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame()
