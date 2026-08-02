# -*- coding: utf-8 -*-
"""
task_core and tests/ must be standalone -- neither may ever import
anything from tasks/ (hr_task.py, ops_task.py, hr_petl_task.py, or any
future task added there), at any point. Tasks utilize task_core; they
are never part of it.

Statically parses every import statement in every .py file under
task_core/ and tests/ (via ast, not by trying to import and catching
failures -- a task-level import that happens to succeed because some
task file is coincidentally present on sys.path would not be caught by
an import-and-see-if-it-fails approach, but is exactly what this needs
to catch). An import is allowed only if it's task_core itself (internal),
Python's own standard library (sys.stdlib_module_names, not a hand-
maintained list that could silently go stale), or one of the specific,
genuine third-party packages task_core actually depends on per
requirements.txt. Anything else -- in particular any of the task-level
module names above -- fails the test.

Verified this test has teeth, not just that it passes: temporarily added
`import hr_task` to a task_core file during development and confirmed
this test caught it before reverting.
"""

import ast
import os
from pathlib import Path
import sys
import unittest

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_THIS_DIR)
_TASK_CORE_DIR = os.path.join(_PROJECT_ROOT, 'task_core')

# The genuine, real third-party packages task_core depends on -- see
# requirements.txt. Not task-level modules, not standard library.
_ALLOWED_THIRD_PARTY = {
    'petl', 'pandas', 'numpy', 'openpyxl', 'sqlalchemy', 'lxml',
    'psycopg2', 'smbclient', 'cryptography',
}


def _iter_py_files(root):
    for dirpath, _dirnames, filenames in os.walk(root):
        if '__pycache__' in dirpath:
            continue
        for name in filenames:
            if name.endswith('.py'):
                yield os.path.join(dirpath, name)


def _top_level_import_names(filepath):
    """Every top-level module name a file imports -- import x.y.z and
    from x.y import z both yield 'x'. Relative imports (from . import x,
    level > 0) are always internal to whatever package contains them, so
    they're skipped here rather than needing special-casing per caller."""
    with open(filepath, encoding='utf-8') as f:
        tree = ast.parse(f.read(), filename=filepath)

    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.level == 0:
                names.add(node.module.split('.')[0])
    return names


def _find_disallowed_imports(root, *, extra_allowed=()):
    stdlib = sys.stdlib_module_names
    allowed = _ALLOWED_THIRD_PARTY | set(extra_allowed) | {'task_core'}
    violations = {}
    for filepath in _iter_py_files(root):
        for name in _top_level_import_names(filepath):
            if name in stdlib or name in allowed:
                continue
            violations.setdefault(os.path.relpath(filepath, _PROJECT_ROOT), set()).add(name)
    return violations


class TestTaskCoreIsStandalone(unittest.TestCase):
    def test_task_core_imports_nothing_task_level(self):
        violations = _find_disallowed_imports(_TASK_CORE_DIR)
        self.assertEqual(
            violations, {},
            f"task_core imports something outside its own package, the standard "
            f"library, or {sorted(_ALLOWED_THIRD_PARTY)}: {violations}"
        )


class TestTestSuiteIsStandalone(unittest.TestCase):
    def test_test_suite_imports_nothing_task_level(self):
        # 'examples' is allowed alongside 'tests': tests/test_docs.py runs
        # examples/local_task.py to prove the README's quick start actually
        # works, which is the only guard against shipping a documented
        # example nobody can execute -- the previous documentation's
        # "minimal task" called an undefined function and pointed at an SMB
        # path.
        #
        # This does not weaken the standalone guarantee. examples/ is held
        # to the same rule below, and tests/test_docs.py separately asserts
        # by AST that the example imports nothing the quick start
        # disclaims. Allowing it here is transitive, not an exemption.
        # tasks/ remains excluded -- see
        # docs/decisions/0002-keep-core-tests-independent-of-tasks.md.
        violations = _find_disallowed_imports(
            _THIS_DIR, extra_allowed=('unittest', 'tests', 'examples'),
        )
        self.assertEqual(
            violations, {},
            f"tests/ imports something outside task_core, the standard library, or "
            f"{sorted(_ALLOWED_THIRD_PARTY)}: {violations}"
        )


class TestExamplesAreStandalone(unittest.TestCase):
    """The quick start promises examples/ needs no share, no database and
    nothing this project does not ship. That promise is only as good as
    what enforces it."""

    def test_examples_import_nothing_task_level(self):
        examples_dir = os.path.join(_PROJECT_ROOT, 'examples')
        if not os.path.isdir(examples_dir):
            self.skipTest('no examples/ directory')

        violations = _find_disallowed_imports(examples_dir)
        self.assertEqual(
            violations, {},
            f'examples/ imports something outside task_core, the standard library, '
            f'or {sorted(_ALLOWED_THIRD_PARTY)}: {violations}'
        )


class TestShippedFilesDoNotNameExternalModules(unittest.TestCase):
    """The scaffold does not document modules it neither ships nor depends
    on.

    Task files legitimately import shared in-house helpers; task_core, its
    documentation, its tests and its examples must not name them. Doing so
    couples the scaffold's documentation to something outside its control,
    and makes it read as though the dependency were the scaffold's.

    The names are read from .gitignore's ignored root-level modules rather
    than written here, so this test does not itself become the last place
    naming them.
    """

    def _externally_supplied_module_names(self):
        gitignore = os.path.join(_PROJECT_ROOT, '.gitignore')
        if not os.path.exists(gitignore):
            return []
        names = []
        for line in Path(gitignore).read_text(encoding='utf-8').splitlines():
            entry = line.strip()
            # Root-level ignored .py files: modules expected to exist at
            # runtime but supplied from outside the repository.
            if entry.startswith('/') and entry.endswith('.py'):
                names.append(os.path.basename(entry)[:-3])
        return names

    def test_no_shipped_file_names_an_externally_supplied_module(self):
        names = self._externally_supplied_module_names()
        if not names:
            self.skipTest('no externally supplied modules declared in .gitignore')

        roots = ['task_core', 'docs', 'examples', 'tests', 'README.md', 'CHANGELOG.md']
        offenders = {}

        for root in roots:
            path = os.path.join(_PROJECT_ROOT, root)
            if os.path.isfile(path):
                candidates = [path]
            elif os.path.isdir(path):
                candidates = [
                    os.path.join(dirpath, filename)
                    for dirpath, _dirs, filenames in os.walk(path)
                    if '__pycache__' not in dirpath
                    for filename in filenames
                    if filename.endswith(('.py', '.md'))
                ]
            else:
                continue

            for candidate in candidates:
                if os.path.abspath(candidate) == os.path.abspath(__file__):
                    continue
                text = Path(candidate).read_text(encoding='utf-8')
                for name in names:
                    if name in text:
                        offenders.setdefault(
                            os.path.relpath(candidate, _PROJECT_ROOT), set()
                        ).add(name)

        self.assertEqual(
            offenders, {},
            f'shipped files name externally supplied module(s): {offenders}'
        )


if __name__ == '__main__':
    unittest.main()
