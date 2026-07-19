# -*- coding: utf-8 -*-
"""
Level 2: convenience factories on top of ResourceSpec (task_core/binding.py)
and the existing build_latest_xlsx_resource/build_file_set_resource/
build_db_resource. "Recipes" built on the generic core -- not a new
resource-construction mechanism, just named shortcuts for the constructor
calls a RESOURCES entry would otherwise write out by hand.
"""

from task_core.binding import ResourceSpec
from task_core.resources.excel import build_latest_xlsx_resource
from task_core.resources.file_set import build_file_set_resource


def resource(loader, tracker=False):
    """The generic entry point. Equivalent to constructing ResourceSpec
    directly; exists so a RESOURCES entry with a fully custom loader
    (e.g. a DB resource) reads the same way as the convenience factories
    below, rather than switching vocabulary. Untracked by default -- an
    arbitrary loader isn't reliably fingerprintable without knowing what
    it constructs."""
    return ResourceSpec(loader=loader, tracker=tracker)


def latest_xlsx(path='.', pattern='*.xlsx', tracker=True):
    # Tracked by default: file resources are normally fingerprintable, and
    # were tracked automatically under the previous resource_builder
    # convention. Forgetting to opt in on a migrated pipeline wouldn't
    # fail loudly -- a task with other tracked resources would just
    # silently under-track this one and could skip a run where this file
    # actually changed. Explicit tracker=False remains available for a
    # deliberately untracked case.
    def _load(env):
        return build_latest_xlsx_resource(
            env.resolve_path(path),
            pattern=pattern,
            source_access=env.require_file_access(),
        )
    return ResourceSpec(loader=_load, tracker=tracker)


def xlsx_file_set(path='.', pattern='*.xlsx', tracker=True):
    # Same reasoning as latest_xlsx above.
    def _load(env):
        return build_file_set_resource(
            env.resolve_path(path),
            pattern=pattern,
            source_access=env.require_file_access(),
        )
    return ResourceSpec(loader=_load, tracker=tracker)
