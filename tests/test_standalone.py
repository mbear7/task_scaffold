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
import sys
import unittest

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_THIS_DIR)
_TASK_CORE_DIR = os.path.join(_PROJECT_ROOT, 'task_core')

# The genuine, real third-party packages task_core depends on -- see
# requirements.txt. Not task-level modules, not standard library.
_ALLOWED_THIRD_PARTY = {'petl', 'pandas', 'numpy', 'openpyxl', 'sqlalchemy', 'lxml', 'psycopg2', 'smbclient'}


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
        violations = _find_disallowed_imports(_THIS_DIR, extra_allowed=('unittest', 'tests'))
        self.assertEqual(
            violations, {},
            f"tests/ imports something outside task_core, the standard library, or "
            f"{sorted(_ALLOWED_THIRD_PARTY)}: {violations}"
        )


if __name__ == '__main__':
    unittest.main()
