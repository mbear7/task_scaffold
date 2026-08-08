"""
Level 2: resource binding. Depends on context.py (task_context),
source_tracking.py (TrackedResourceSource) and types.py.

The context.py import was previously deferred inside
build_resource_context() with no comment explaining why. Confirmed
directly that hoisting it creates no cycle -- context.py imports cleanup,
types and source_tracking, none of which reach back here -- and the
function runs once per run, so there was no import-cost argument either.
An unexplained deferred import invites the wrong guess about what it
protects. Promoted from a sandboxed prototype
after it was proven against every real resource shape in hr_task.py and
ops_task.py: latest_xlsx (single file), xlsx_file_set (multi-file), and
generic resource() (DB-shaped).

This module implements the binding slice of a broader resource-model
design -- required direct resources only, resolved through keyword-only
run() parameters. It is not the complete design. Not yet implemented,
deferred until a real pipeline needs one: required=False, missing-resource
acceptance under keyword injection, and zero-match file sets as a missing
optional resource.

This deferral is about this module specifically -- the low-level
build_file_set_resource() still genuinely supports on_empty='raise'|'empty',
predating this module entirely, and build_csv_file_set_resource() passes it
through because zero matching files is a selection question. None of the
factories in resources/factories.py exposes on_empty, and nothing here
gives an injected resource's "empty" state any defined meaning -- that gap
is deliberate, not an oversight.

ResourceSpec.tracker is a bool, not a callable -- found, while building
this for real, that the real task_core doesn't attach a tracker callable
to a resource at all. TrackedResourceSource is a separate, string-keyed
marker (source_tracking.py), and the resource object a loader returns is
itself responsible for .source_fingerprint(source_key). This field just
says "does this resource participate in tracking"; build_resource_context()
below translates a tracker=True entry into a real TrackedResourceSource,
using the RESOURCES dict's own key as the source_key -- matching
TrackedResourceSource.source_key's existing behavior of doubling as
resource_key. This bool is the current tracking bridge for self-
fingerprinting file resources specifically, not a general or final tracking
model -- it does not express a TrackedDbQuerySource (source_tracking.py),
which has genuinely different fields (query, store_snapshot) a bool cannot
carry.

Binding convention: every keyword-only parameter a bound pipeline's run()
declares after ctx is treated as an injected resource role, and bind() must
supply exactly that set of names, no more, no fewer (validate_bindings()
below). A keyword-only parameter that is not meant to be a resource --
`def run(cls, ctx, *, debug=False):` -- will still be required from bind()
under this convention; keyword-only parameters after ctx are reserved for
resource roles for this reason, not general pipeline configuration.
"""

import inspect
import ntpath
import os
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Callable, Mapping

from task_core.context import task_context
from task_core.source_tracking import TrackedResourceSource
from task_core.types import PipelineContractError, find_duplicates

# === Resource environment ===

@dataclass(kw_only=True)
class ResourceEnvironment:
    base_path: str | None = None
    file_access: object | None = None
    credentials: Mapping[str, object] = field(default_factory=dict)
    # A previous revision also carried a generic `config` mapping here,
    # annotated as unused-decide-or-remove. Removed (v0.2.0): nothing in
    # this project ever read it, credentials already covers the
    # custom-loader need, and removal was free precisely while unused.
    # Re-add it the day a real loader needs arbitrary configuration --
    # with that loader as its documented use case.

    def resolve_path(self, path):
        path = os.fspath(path)
        base_path = None if self.base_path is None else os.fspath(self.base_path)

        if path.startswith('\\\\') or path.startswith('/') or (len(path) > 1 and path[1] == ':'):
            resolved = path
        else:
            if base_path is None:
                raise ValueError(
                    f"Resource uses relative path {path!r}, but this task has no base_path. "
                    f"Use an absolute path or provide base_path."
                )
            if base_path.startswith('\\\\'):
                resolved = ntpath.join(base_path, path)
            else:
                resolved = os.path.join(base_path, path)

        # Normalize regardless of which branch produced it -- a relative
        # path joined against a UNC base_path (e.g. latest_xlsx('.')) would
        # otherwise carry a literal trailing '\.' all the way to smbclient
        # over the real SMB connection. Locally harmless (filesystem calls
        # resolve '.' transparently); not guaranteed to be, remotely, over
        # the wire. ntpath.normpath preserves the UNC prefix correctly
        # (verified directly: '\\\\srv\\share\\X\\.' -> '\\\\srv\\share\\X').
        if resolved.startswith('\\\\'):
            return ntpath.normpath(resolved)
        return os.path.normpath(resolved)

    def require_file_access(self):
        if self.file_access is None:
            raise ValueError('This resource needs file_access, but none was provided.')
        return self.file_access


# === Generic resource core ===

@dataclass(frozen=True)
class ResourceSpec:
    loader: Callable[[ResourceEnvironment], object]
    tracker: bool = field(default=False, kw_only=True)


# === Pipeline-resource binding ===

@dataclass(frozen=True)
class PipelineBinding:
    pipeline: type
    resources: Mapping[str, ResourceSpec] = field(default_factory=dict)


def bind(pipeline_cls, **resource_bindings: ResourceSpec):
    # MappingProxyType, not a plain dict: PipelineBinding is frozen=True,
    # but that only blocks reassigning the resources field itself -- a
    # plain dict inside it could still be mutated after validation, after
    # resource wiring, after resource_keys_by_spec_id is computed, making
    # the "frozen" guarantee shallow rather than real.
    return PipelineBinding(pipeline=pipeline_cls, resources=MappingProxyType(dict(resource_bindings)))


# === Structural validation ===

def _validate_run_signature(pipeline_name, pipeline_cls):
    """Validates the complete bound-pipeline run() shape, not just its
    keyword-only parameters: after classmethod binding, run() must accept
    exactly one positional parameter (ctx) plus zero or more keyword-only
    ones. A signature like run(cls, ctx, extra, *, source) would otherwise
    pass the old keyword-only-only check and still fail at actual
    invocation time, with a TypeError far from the real cause. Returns the
    keyword-only parameter names -- the set bind() must match exactly."""
    run_func = getattr(pipeline_cls, 'run', None)
    if not callable(run_func):
        raise PipelineContractError(
            f"Pipeline '{pipeline_name}' ({pipeline_cls.__name__}): has no callable run()"
        )

    sig = inspect.signature(run_func)
    positional = [
        param for param in sig.parameters.values()
        if param.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if len(positional) != 1 or positional[0].name != 'ctx':
        raise PipelineContractError(
            f"Pipeline '{pipeline_name}' ({pipeline_cls.__name__}): a bound pipeline's "
            f"run() must accept exactly run(ctx, *, ...resource roles), got run{sig}"
        )

    defaulted = [
        name for name, param in sig.parameters.items()
        if param.kind == inspect.Parameter.KEYWORD_ONLY and param.default is not inspect.Parameter.empty
    ]
    if defaulted:
        raise PipelineContractError(
            f"Pipeline '{pipeline_name}' ({pipeline_cls.__name__}): keyword-only parameter(s) "
            f"{defaulted} have a default value. Every keyword-only parameter after ctx is "
            f"reserved for an injected resource role, and bind() always supplies a value for "
            f"each one -- a default can never actually be used under the current, required-only "
            f"model, so having one can only mean this wasn't meant to be a resource role at all. "
            f"Rename or restructure rather than default it."
        )

    return {
        name for name, param in sig.parameters.items()
        if param.kind == inspect.Parameter.KEYWORD_ONLY
    }


def validate_bindings(resources, pipelines):
    """Structural validation only -- no file is opened, no credential
    required, no resource constructed here. Covers every declared binding,
    regardless of RUN_SEQUENCE, and RESOURCES itself:

    1. every RESOURCES entry is actually a ResourceSpec with a callable
       loader and a bool tracker;
    2. no ResourceSpec is registered under more than one RESOURCES key
       (silent last-key-wins would otherwise make tracking/injection
       depend on dict ordering);
    3. every bind()-referenced value is a ResourceSpec that is actually
       registered in RESOURCES (an unregistered spec would otherwise only
       fail later, as an obscure KeyError deep inside the runner, not a
       clear task-composition error);
    4. every bound pipeline's run() has the complete required shape --
       exactly one positional parameter (ctx), any number of keyword-only
       ones -- not just that its keyword-only names match bind();
    5. every bind()'s keyword names match run()'s keyword-only parameter
       names exactly."""
    keys_by_spec_id = {}
    for key, spec in resources.items():
        if not isinstance(spec, ResourceSpec):
            raise PipelineContractError(
                f"RESOURCES[{key!r}] must be a ResourceSpec, got {type(spec).__name__}"
            )
        if not callable(spec.loader):
            raise PipelineContractError(f"RESOURCES[{key!r}].loader must be callable")
        if not isinstance(spec.tracker, bool):
            raise PipelineContractError(f"RESOURCES[{key!r}].tracker must be bool")
        keys_by_spec_id.setdefault(id(spec), []).append(key)

    duplicates = {tuple(keys) for keys in keys_by_spec_id.values() if len(keys) > 1}
    if duplicates:
        raise PipelineContractError(
            f'Same ResourceSpec registered under multiple RESOURCES keys: {sorted(duplicates)}. '
            f'Register it once and share that one binding across pipelines instead.'
        )

    valid_spec_ids = set(keys_by_spec_id)

    for name, entry in pipelines.items():
        if not isinstance(entry, PipelineBinding):
            continue
        expected = _validate_run_signature(name, entry.pipeline)
        supplied = set(entry.resources.keys())
        if expected != supplied:
            missing = expected - supplied
            extra = supplied - expected
            detail = []
            if missing:
                detail.append(f'run() expects {sorted(missing)}, bind() did not supply')
            if extra:
                detail.append(f'bind() supplied {sorted(extra)}, run() does not declare')
            raise PipelineContractError(
                f"Pipeline '{name}' ({entry.pipeline.__name__}): binding mismatch. " + '; '.join(detail)
            )

        for alias, spec in entry.resources.items():
            if not isinstance(spec, ResourceSpec):
                raise PipelineContractError(
                    f"Pipeline '{name}' binding {alias!r} must be a ResourceSpec, got {type(spec).__name__}"
                )
            if id(spec) not in valid_spec_ids:
                raise PipelineContractError(
                    f"Pipeline '{name}' binding {alias!r} references a ResourceSpec "
                    f"that is not registered in RESOURCES"
                )


# === Task composition: RESOURCES/PIPELINES/RUN_SEQUENCE -> task_context ===

def compute_resource_wiring(resources, pipelines, run_sequence, env):
    """The lower-level half of build_resource_context() -- just the
    loaders/tracked_sources/key-map computation, without constructing a
    task_context. Exists so a task file migrating pipelines one at a time
    can merge this with its own remaining old-style resource_builder/
    source_folder wiring into a single task_context, rather than needing
    every pipeline migrated before any of them can be."""
    validate_bindings(resources, pipelines)

    active_names = list(run_sequence)
    missing = [name for name in active_names if name not in pipelines]
    if missing:
        raise PipelineContractError(f'run_sequence contains unknown pipeline(s): {missing}')

    duplicates = find_duplicates(active_names)
    if duplicates:
        raise PipelineContractError(f'run_sequence contains duplicate pipeline(s): {duplicates}')

    active_spec_ids = set()
    for name in active_names:
        entry = pipelines[name]
        if isinstance(entry, PipelineBinding):
            active_spec_ids.update(id(spec) for spec in entry.resources.values())

    key_by_spec_id = {id(spec): key for key, spec in resources.items()}

    loaders = {}
    tracked_sources = []
    for key, spec in resources.items():
        if id(spec) not in active_spec_ids:
            continue
        loaders[key] = (lambda spec=spec: spec.loader(env))
        if spec.tracker:
            tracked_sources.append(TrackedResourceSource(resource_key=key))

    return loaders, tracked_sources, key_by_spec_id


def build_resource_context(task_name, resources, pipelines, run_sequence, env):
    """Replaces a task file's hand-rolled build_context() when every
    pipeline uses the RESOURCES/bind() model. Validates every declared
    binding structurally (regardless of RUN_SEQUENCE), derives the active
    resource set from RUN_SEQUENCE, and builds a real task_context with
    loaders/tracked_sources for active resources only -- same lazy,
    cached construction task_context.get_resource() already provides, so
    a resource fingerprinted during source-state evaluation is the same
    object later injected into its pipeline, not reconstructed."""
    loaders, tracked_sources, key_by_spec_id = compute_resource_wiring(
        resources, pipelines, run_sequence, env,
    )

    return task_context(
        task_name=task_name,
        loaders=loaders,
        tracked_sources=tracked_sources,
        resource_keys_by_spec_id=key_by_spec_id,
    )
