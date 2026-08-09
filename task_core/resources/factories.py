"""
Level 2: convenience factories on top of ResourceSpec (task_core/binding.py)
and the existing build_latest_xlsx_resource/build_file_set_resource/
build_db_resource. "Recipes" built on the generic core -- not a new
resource-construction mechanism, just named shortcuts for the constructor
calls a RESOURCES entry would otherwise write out by hand.
"""

from task_core.binding import ResourceSpec
from task_core.resources.csv import (
    build_csv_file_resource,
    build_csv_file_set_resource,
    build_latest_csv_resource,
)
from task_core.resources.excel import (
    build_latest_xlsx_resource,
    build_xlsx_file_resource,
)
from task_core.resources.file_set import build_file_set_resource


def resource(loader, tracker=False):
    """The generic entry point. Equivalent to constructing ResourceSpec
    directly; exists so a RESOURCES entry with a fully custom loader
    (e.g. a DB resource) reads the same way as the convenience factories
    below, rather than switching vocabulary. Untracked by default -- an
    arbitrary loader isn't reliably fingerprintable without knowing what
    it constructs."""
    return ResourceSpec(loader=loader, tracker=tracker)


def xlsx_file(path, tracker=True):
    """One named workbook, by exact path.

    Exact selection is a generic file-resource concept, not a CSV one --
    decisions/0015 adds it for both formats at once rather than letting
    csv_file() be the only way to name a file directly. `path` has no
    default: there is no sensible "the file" the way '.' is a sensible
    folder.

    Tracked by default, like the other file factories. That is only
    possible because the builder captures selection metadata; a resource
    built from a bare path cannot be fingerprinted at all.
    """
    def _load(env):
        return build_xlsx_file_resource(
            env.resolve_path(path),
            source_access=env.require_file_access(),
        )
    return ResourceSpec(loader=_load, tracker=tracker)


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


def csv_file(path, tracker=True, *, options=None):
    """One named CSV file, by exact path.

    `options` is the only parser control. Individual parser arguments are
    deliberately not mirrored here (decisions/0015 section 3): with both
    forms available there would have to be a precedence rule between
    options=CsvReadOptions(delimiter=',') and delimiter=',', and every
    such rule is a thing to get wrong. options=None means CsvReadOptions(),
    whose delimiter is ';'.
    """
    def _load(env):
        return build_csv_file_resource(
            env.resolve_path(path),
            options,
            source_access=env.require_file_access(),
        )
    return ResourceSpec(loader=_load, tracker=tracker)


def latest_csv(path='.', pattern='*.csv', tracker=True, *, options=None):
    # Same tracking reasoning as latest_xlsx below.
    def _load(env):
        return build_latest_csv_resource(
            env.resolve_path(path),
            pattern,
            options,
            source_access=env.require_file_access(),
        )
    return ResourceSpec(loader=_load, tracker=tracker)


def csv_file_set(path='.', pattern='*.csv', tracker=True, *, options=None):
    """A folder of CSV files read as one logical table.

    One parser configuration applies to every member -- the set is one
    logical source, not a bag of independently-configured files. With an
    inferred header, every usable member must carry exactly the same
    header text; with `columns=` declared, member headers are consumed and
    ignored and rows are positional, which is the supported way to read a
    feed whose header spelling moves between deliveries.
    """
    def _load(env):
        return build_csv_file_set_resource(
            env.resolve_path(path),
            pattern,
            options,
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
