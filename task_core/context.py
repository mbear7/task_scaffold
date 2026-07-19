# -*- coding: utf-8 -*-
"""
Level 2: task_context. Depends on source_tracking.py (dispatch types,
SourceFingerprint, make_source_signature, _json_safe_scalar) and
types.py (SourceCheckError). Does NOT depend on resources/ -- loaders
are injected by the caller (ops_task.py-style build_context()
functions), never imported here.
"""

from task_core.types import SourceCheckError
from task_core.source_tracking import (
    TrackedResourceSource,
    TrackedDbQuerySource,
    SourceFingerprint,
    make_source_signature,
    _json_safe_scalar,
)


class task_context:
    def __init__(self, task_name, loaders, tracked_sources=None, resource_keys_by_spec_id=None):
        self.task_name = task_name
        self._loaders = loaders
        self._cache = {}
        self._results = {}
        self._shared = {}
        self.tracked_sources = list(tracked_sources) if tracked_sources else []
        # Populated by build_resource_context() (task_core/binding.py) for
        # tasks using the RESOURCES/bind() model -- maps a ResourceSpec's
        # id() back to its string key in RESOURCES, so run_pipelines() can
        # resolve a PipelineBinding's kwargs via the same cached
        # get_resource() a hand-rolled build_context() already used. Stays
        # empty for tasks that don't use the new binding model at all.
        self.resource_keys_by_spec_id = dict(resource_keys_by_spec_id) if resource_keys_by_spec_id else {}

    def collect_source_fingerprints(self):
        # Fingerprints are collected through get_resource() (cached), never
        # through a throwaway resource instance: this way, if the task ends
        # up running for real, the same resource object gets reused instead
        # of being built twice (e.g. re-opening an Excel workbook, or
        # re-connecting to a DB).
        return [self._fingerprint_for(source) for source in self.tracked_sources]

    def _fingerprint_for(self, source):
        if isinstance(source, TrackedResourceSource):
            return self._collect_resource_fingerprint(source)
        if isinstance(source, TrackedDbQuerySource):
            return self._collect_db_query_fingerprint(source)
        raise SourceCheckError(
            f'{self.task_name}: unsupported tracked source type {type(source).__name__!r}'
        )

    def _collect_resource_fingerprint(self, source):
        if source.resource_key not in self._loaders:
            raise SourceCheckError(
                f'{self.task_name}: tracked source {source.resource_key!r} has no matching loader'
            )

        resource = self.get_resource(source.resource_key)
        fingerprint_fn = getattr(resource, 'source_fingerprint', None)
        if not callable(fingerprint_fn):
            raise SourceCheckError(
                f'{self.task_name}: resource {source.resource_key!r} does not support source_fingerprint()'
            )

        return fingerprint_fn(source.source_key)

    def _collect_db_query_fingerprint(self, source):
        if source.resource_key not in self._loaders:
            raise SourceCheckError(
                f'{self.task_name}: tracked source {source.source_key!r} references resource '
                f'{source.resource_key!r}, which has no matching loader'
            )

        resource = self.get_resource(source.resource_key)
        get_table_fn = getattr(resource, 'get_table', None)
        if not callable(get_table_fn):
            raise SourceCheckError(
                f'{self.task_name}: resource {source.resource_key!r} does not support get_table(query=...); '
                'TrackedDbQuerySource requires a db_resource-like object'
            )

        tbl = get_table_fn(query=source.query)

        iterator = iter(tbl)
        try:
            header = next(iterator)
        except StopIteration:
            raise SourceCheckError(
                f'{self.task_name}: TrackedDbQuerySource {source.source_key!r} query returned no rows '
                '(no header); the query contract requires exactly one row'
            )

        columns = [str(c) for c in header]
        if len(set(columns)) != len(columns):
            raise SourceCheckError(
                f'{self.task_name}: TrackedDbQuerySource {source.source_key!r} query returned duplicate '
                f'column names {columns!r}; the query contract requires stable, unique column names'
            )

        rows = list(iterator)
        if len(rows) != 1:
            raise SourceCheckError(
                f'{self.task_name}: TrackedDbQuerySource {source.source_key!r} query returned '
                f'{len(rows)} row(s); the query contract requires exactly one row'
            )

        if not columns:
            raise SourceCheckError(
                f'{self.task_name}: TrackedDbQuerySource {source.source_key!r} query returned no '
                'columns; the query contract requires one or more scalar columns'
            )

        error_context = f'{self.task_name}: TrackedDbQuerySource {source.source_key!r}'
        row_dict = {
            col: _json_safe_scalar(value, context=error_context)
            for col, value in zip(columns, rows[0], strict=True)
        }

        # Signature is over the row result only (per the query contract), not
        # the query text -- so if you edit the query's filters without the
        # underlying data changing, nothing forces a rerun. If you change
        # what a source_key means, use force_run once or change the key.
        return SourceFingerprint(
            source_key=source.source_key,
            source_kind='db_query',
            root_path=None,
            include_mask=None,
            recursive=False,
            file_count=0,
            total_size_bytes=0,
            max_modified_at_utc=None,
            source_signature=make_source_signature(row_dict),
            source_snapshot={'query': source.query, 'row': row_dict},
            store_snapshot=bool(source.store_snapshot),
        )

    def get_resource(self, name):
        if name not in self._cache:
            if name not in self._loaders:
                raise KeyError(f'no loader registered for resource: {name!r}')
            self._cache[name] = self._loaders[name]()
        return self._cache[name]

    def set_result(self, name, tbl):
        self._results[name] = tbl

    def get_result(self, name):
        try:
            return self._results[name]
        except KeyError as e:
            raise KeyError(f'published result not found: {name!r}') from e

    def has_result(self, name):
        return name in self._results

    def set_shared(self, name, value):
        self._shared[name] = value

    def get_shared(self, name, default=None):
        return self._shared.get(name, default)

    def require_shared(self, name):
        if name not in self._shared:
            raise KeyError(f'shared runtime artifact not found: {name!r}')
        return self._shared[name]

    def has_shared(self, name):
        return name in self._shared

    def push_shared(self, name, value):
        if name not in self._shared:
            self._shared[name] = []
        self._shared[name].append(value)

    def close(self):
        for obj in self._cache.values():
            close_fn = getattr(obj, 'close', None)
            if close_fn is not None:
                close_fn()


# Example:
#
# return task_context(
#     task_name='ops_support_etl',
#     loaders={
#         'ops_xlsx': lambda: build_latest_xlsx_resource(...),
#         'strat_db': lambda: build_db_resource(...),
#     },
#     tracked_sources=[
#         TrackedResourceSource('ops_xlsx'),
#
#         # Not enabled yet -- pending an agreed fingerprint query (and
#         # ideally a supporting index) with the strat_db view owner:
#         # TrackedDbQuerySource(
#         #     source_key='strat35_2025',
#         #     resource_key='strat_db',
#         #     query='''
#         #         select max(updated_at) as max_updated_at
#         #         from strat35_view
#         #         where year = 2025
#         #     ''',
#         # ),
#     ],
# )
