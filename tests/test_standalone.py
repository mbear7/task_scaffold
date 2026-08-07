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
import builtins
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

        # `tools/` postdates this test and shipped ~1500 lines outside its
        # scope. Confirmed by injecting a forbidden name there and watching
        # the tripwire pass.
        roots = [
            'task_core', 'docs', 'examples', 'tests', 'tools',
            'README.md', 'CHANGELOG.md',
        ]
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


class TestImportsNameTheModuleThatDefinesTheSymbol(unittest.TestCase):
    """`from task_core.X import Y` requires that X *defines* Y.

    Python is happy to let a module import a name through any other module
    that happens to have imported it. That is how a package stays coupled
    while looking split: the definitions move, the dependency edges do not,
    the tests pass, and the architecture diagram is describing a structure
    the imports no longer follow.

    It is invisible to the layering and subsystem-order checks, because both
    spellings are legal edges. After the 0.7.4 split, sixteen imports across
    six files still reached through `db/publish.py` and `db/copy.py` --
    `source_state.py` taking identifier rules via the publisher,
    `publish.py` taking `SpoolIdentity` via `copy.py`. All were legal; all
    were wrong.

    The duplicate check is here for the same reason and from the same
    release: cleaning those up added a second import of
    `cleanup_predecessor_spools` beside one that already existed, which
    nothing noticed -- both named the correct module, and the name was used,
    so neither a wrong-owner audit nor an unused-import pass could see it.
    """

    @staticmethod
    def _module_file(dotted):
        rel = dotted[len('task_core.'):].replace('.', os.sep)
        for candidate in (
            os.path.join(_PROJECT_ROOT, 'task_core', rel + '.py'),
            os.path.join(_PROJECT_ROOT, 'task_core', rel, '__init__.py'),
        ):
            if os.path.exists(candidate):
                return candidate
        return None

    @classmethod
    def _defined_names(cls, dotted):
        """Top-level names a module defines. Its own imports do not count."""
        path = cls._module_file(dotted)
        if path is None:
            return None
        names = set()
        for node in ast.parse(Path(path).read_text(encoding='utf-8')).body:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef,
                                 ast.AsyncFunctionDef)):
                names.add(node.name)
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        names.add(target.id)
            elif isinstance(node, ast.AnnAssign):
                if isinstance(node.target, ast.Name):
                    names.add(node.target.id)
        return names

    def test_no_module_imports_a_symbol_through_a_third_module(self):
        offenders = []
        for path in _iter_py_files(_TASK_CORE_DIR):
            tree = ast.parse(Path(path).read_text(encoding='utf-8'))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ImportFrom) or not node.module:
                    continue
                if not node.module.startswith('task_core.'):
                    continue
                defined = self._defined_names(node.module)
                if defined is None:
                    continue
                for alias in node.names:
                    if alias.name == '*' or alias.name in defined:
                        continue
                    offenders.append(
                        f'{os.path.relpath(path, _PROJECT_ROOT)}:{node.lineno} '
                        f'imports {alias.name} from {node.module}, '
                        f'which does not define it'
                    )

        self.assertEqual(
            sorted(set(offenders)), [],
            'import each name from the module that defines it, not through '
            'one that merely re-imports it'
        )

    def test_no_module_imports_the_same_name_twice(self):
        offenders = []
        for path in _iter_py_files(_TASK_CORE_DIR):
            tree = ast.parse(Path(path).read_text(encoding='utf-8'))
            seen = {}
            for node in tree.body:
                if not isinstance(node, (ast.Import, ast.ImportFrom)):
                    continue
                for alias in node.names:
                    bound = alias.asname or alias.name.split('.')[0]
                    if bound in seen:
                        offenders.append(
                            f'{os.path.relpath(path, _PROJECT_ROOT)}: {bound} '
                            f'imported at lines {seen[bound]} and {node.lineno}'
                        )
                    seen[bound] = node.lineno

        self.assertEqual(
            sorted(set(offenders)), [],
            'the same name is imported more than once at module level'
        )


class TestFilesystemFailuresKeepTheirOwnType(unittest.TestCase):
    """ADR 0011 §Filesystem failures keep their own type, enforced.

    A filesystem exception caught in `task_core/` may be re-raised as itself,
    optionally with a better message, but must not be converted into a
    task_core exception type. Those are reserved for framework validation,
    ownership, serialization and spool-format failures.

    The rule exists because one such conversion produced a per-errno contract
    nobody could hold in their head: the same `EACCES` raised `PermissionError`
    on a first attempt and `DbPublishError` on a retry.

    The check is structural rather than a list of task_core exception classes,
    which a first version tried and got wrong three ways: it matched only the
    substring `DbPublish`, so `SpoolFormatError` -- itself a `DbPublishError`
    subclass -- passed; it read only bare `ast.Name` nodes, so
    `errors.DbPublishError` and `except builtins.OSError` both passed. Asking
    "is the raised type a builtin OSError subclass?" needs no such list and
    cannot drift as task_core gains exception classes.

    ADR 0011 permits a deliberate higher-level policy to inspect a filesystem
    failure and raise its own semantic error -- predecessor cleanup does
    exactly that. Such policies act on a *returned* residual path rather than
    inside an `except` block, so they do not appear here.
    """

    # Every builtin OSError subclass, taken from builtins rather than written
    # out, so a future Python that adds one is covered automatically.
    FILESYSTEM = frozenset(
        name for name in dir(builtins)
        if isinstance(getattr(builtins, name), type)
        and issubclass(getattr(builtins, name), OSError)
    )

    @staticmethod
    def _handler_names(handler):
        """Names in an except clause, tolerating tuples and qualified forms."""
        names = []
        target = handler.type
        elements = target.elts if isinstance(target, ast.Tuple) else [target]
        for element in elements:
            if isinstance(element, ast.Name):
                names.append(element.id)
            elif isinstance(element, ast.Attribute):
                names.append(element.attr)
        return names

    @staticmethod
    def _raised_name(node):
        call = node.exc.func if isinstance(node.exc, ast.Call) else node.exc
        if isinstance(call, ast.Name):
            return call.id
        if isinstance(call, ast.Attribute):
            return call.attr
        return None

    @classmethod
    def _conversions(cls, source, label='<source>'):
        """Filesystem handlers that raise anything but the type they caught.

        Allowed, because each provably preserves the exception type:

        - bare `raise`;
        - `raise exc`, where `exc` is the handler's own bound name -- the
          identical object, so the type cannot change;
        - constructing the caught class, but only when the handler caught
          exactly one. `except (FileNotFoundError, PermissionError) as exc:`
          followed by `raise FileNotFoundError(...)` converts a caught
          `PermissionError`, so a tuple handler may only bare-raise or
          re-raise its bound name.

        An earlier version asked only whether the raised name was *some*
        builtin OSError subclass, which allowed `FileNotFoundError` to become
        `PermissionError` -- "filesystem exception to some filesystem
        exception" rather than "to the same one" -- while flagging the
        type-preserving `raise exc` as a violation.

        Deliberately conservative about nesting: a raise inside a nested
        handler within a filesystem handler is still reported. No such shape
        exists in task_core, and for a tripwire an unnecessary review beats a
        silent miss.
        """
        offenders = []
        for node in ast.walk(ast.parse(source)):
            if not isinstance(node, ast.ExceptHandler) or node.type is None:
                continue
            caught = cls._handler_names(node)
            if not set(caught) & cls.FILESYSTEM:
                continue
            bound = node.name
            for inner in ast.walk(node):
                if not isinstance(inner, ast.Raise) or inner.exc is None:
                    continue
                # `raise exc` -- the caught object itself.
                if (
                    bound is not None
                    and isinstance(inner.exc, ast.Name)
                    and inner.exc.id == bound
                ):
                    continue
                raised = cls._raised_name(inner)
                # Reconstructing the one class this handler caught.
                if len(caught) == 1 and raised == caught[0]:
                    continue
                offenders.append(
                    f'{label}:{inner.lineno} catches '
                    f'{"/".join(caught)} and raises {raised}'
                )
        return offenders

    def test_the_scanner_detects_every_conversion_shape(self):
        """Synthetic proof, because the tree contains no violation.

        The repository scan below cannot protect this scanner -- with nothing
        to find, weakening it leaves the suite green. That is the same gap the
        main-guard tripwire had.
        """
        allowed = (
            ('bare re-raise', 'try:\n    pass\nexcept OSError:\n    raise\n'),
            (
                'same class reconstructed with added context',
                'try:\n    pass\nexcept FileNotFoundError as e:\n'
                "    raise FileNotFoundError('better message') from e\n",
            ),
            (
                're-raise of the bound name',
                'try:\n    pass\nexcept FileNotFoundError as exc:\n'
                '    raise exc\n',
            ),
            (
                're-raise of the bound name from a tuple handler',
                'try:\n    pass\nexcept (FileNotFoundError, PermissionError) as exc:\n'
                '    raise exc\n',
            ),
        )
        for label, source in allowed:
            with self.subTest(allowed=label):
                self.assertEqual(
                    self._conversions(source), [],
                    f'{label} must be allowed: re-raising the native type is '
                    f'how context is added'
                )

        forbidden = (
            ('task_core base type',
             "try:\n    pass\nexcept OSError:\n    raise DbPublishError('x')\n"),
            ('task_core subclass',
             "try:\n    pass\nexcept OSError:\n    raise SpoolFormatError('x')\n"),
            ('qualified raised name',
             "try:\n    pass\nexcept OSError:\n    raise errors.DbPublishError('x')\n"),
            ('qualified handler name',
             "try:\n    pass\nexcept builtins.OSError:\n    raise DbPublishError('x')\n"),
            ('tuple handler',
             'try:\n    pass\nexcept (OSError, ValueError):\n'
             "    raise DbPublishError('x')\n"),
            ('nested inside the handler',
             'try:\n    pass\nexcept OSError:\n    if True:\n'
             "        raise DbPublishError('x')\n"),
            # Native-to-native conversion is still a conversion: the caller
            # is told a different thing went wrong than actually did.
            ('one native type into another',
             'try:\n    pass\nexcept FileNotFoundError as exc:\n'
             "    raise PermissionError('x') from exc\n"),
            # A tuple handler cannot prove which class it caught, so
            # constructing one of them may convert the other.
            ('reconstructed class from a tuple handler',
             'try:\n    pass\nexcept (FileNotFoundError, PermissionError) as exc:\n'
             "    raise FileNotFoundError('x') from exc\n"),
        )
        for label, source in forbidden:
            with self.subTest(forbidden=label):
                self.assertEqual(
                    len(self._conversions(source)), 1,
                    f'{label} was not detected; the scanner claims to reject '
                    f'every conversion of a filesystem exception'
                )

    def test_no_filesystem_handler_converts_to_a_task_core_exception(self):
        offenders = []
        for path in _iter_py_files(_TASK_CORE_DIR):
            offenders += self._conversions(
                Path(path).read_text(encoding='utf-8'),
                os.path.relpath(path, _PROJECT_ROOT),
            )

        self.assertEqual(
            sorted(set(offenders)), [],
            'filesystem exceptions must keep their native type; task_core '
            'exception types are for validation, ownership, serialization '
            'and spool-format failures (ADR 0011)'
        )



class TestNoTestModuleHidesCasesBehindItsMainBlock(unittest.TestCase):
    """`unittest.main()` must be the last statement in a test module.

    A test class defined *after* `if __name__ == '__main__': unittest.main()`
    is invisible when the file is run directly: at the moment main() executes,
    that class does not exist yet. Discovery still finds it, because discovery
    imports the module under its real name and never runs the block -- so the
    two invocations disagree and the direct run still prints `OK`.

    This happened in two files at once. `tests/test_db_copy.py` ran 206 of 211
    cases and `tests/test_types.py` ran 10 of 32, both reporting success. The
    documented commands all use discovery, which is why it survived: nothing
    that anyone was told to run could observe it.
    """

    def _module_paths(self):
        # Both suites, not just this directory. `tools/tests/` is exactly the
        # kind of latecomer that drifted outside the external-module tripwire
        # above; a new tripwire should not repeat that.
        for directory in (
            _THIS_DIR,
            os.path.join(_PROJECT_ROOT, 'tools', 'tests'),
        ):
            if not os.path.isdir(directory):
                continue
            for name in sorted(os.listdir(directory)):
                if name.startswith('test_') and name.endswith('.py'):
                    yield os.path.join(directory, name)

    @staticmethod
    def _is_main_guard(node):
        if not isinstance(node, ast.If):
            return False
        test = node.test
        return (
            isinstance(test, ast.Compare)
            and isinstance(test.left, ast.Name)
            and test.left.id == '__name__'
            and len(test.ops) == 1
            and isinstance(test.ops[0], ast.Eq)
            # The operator matters. Checking only the operands accepted
            # `!=`, `is` and even `<` as main guards, so `if __name__ !=
            # '__main__':` would have been treated as the guard and every
            # statement after it reported.
            and len(test.comparators) == 1
            and isinstance(test.comparators[0], ast.Constant)
            and test.comparators[0].value == '__main__'
        )

    @classmethod
    def _statements_after_main_guard(cls, source):
        """Names (or node types) of every top-level statement after the guard.

        Every statement, not just ClassDef/FunctionDef. A test case built by
        assignment -- `T = type('T', (unittest.TestCase,), {...})` -- is a
        plain Assign node, and the first version of this scanner ignored it:
        discovery saw 33 cases, a direct run saw 32, and the tripwire stayed
        green. Comments and blank lines leave no AST node, so this cleanly
        means "the guard is the last statement".
        """
        body = ast.parse(source).body
        guard = next(
            (i for i, node in enumerate(body) if cls._is_main_guard(node)),
            None,
        )
        if guard is None:
            return []
        return [
            getattr(node, 'name', type(node).__name__)
            for node in body[guard + 1:]
        ]

    def test_assignment_after_main_guard_is_detected(self):
        """Protects the scanner itself, against synthetic source.

        The repository scan below cannot protect it: every real guard is
        already last, so reverting the scanner to its ClassDef/FunctionDef
        filter leaves the whole suite green. Only synthetic source that
        actually contains the defect can fail when the fix is reverted.
        """
        source = (
            "import unittest\n"
            "\n"
            "if __name__ == '__main__':\n"
            "    unittest.main()\n"
            "\n"
            "TestGenerated = type(\n"
            "    'TestGenerated',\n"
            "    (unittest.TestCase,),\n"
            "    {'test_generated': lambda self: None},\n"
            ")\n"
        )
        self.assertEqual(
            self._statements_after_main_guard(source), ['Assign'],
            'a TestCase built by assignment after the main guard went '
            'unnoticed; discovery would run it and a direct run would not'
        )

    def test_only_an_equality_test_counts_as_the_main_guard(self):
        """`__name__` on the left is not enough -- the operator decides.

        Checking operands alone accepted `!=`, `is` and `<`. A module opening
        with `if __name__ != '__main__':` would then have had that treated as
        its guard, and every statement below it reported as hidden.
        """
        trailing = "\n\nclass TestAfter(unittest.TestCase):\n    pass\n"
        guard = "if __name__ {} '__main__':\n    unittest.main()\n"

        with self.subTest(operator='=='):
            self.assertEqual(
                self._statements_after_main_guard(
                    guard.format('==') + trailing
                ),
                ['TestAfter'],
            )
        for operator in ('!=', 'is', 'is not', '<'):
            with self.subTest(operator=operator):
                self.assertEqual(
                    self._statements_after_main_guard(
                        guard.format(operator) + trailing
                    ),
                    [],
                    f'`__name__ {operator} "__main__"` was treated as the '
                    f'main guard; only equality is one'
                )

    def test_a_clean_module_reports_nothing_after_the_guard(self):
        source = (
            "import unittest\n"
            "\n"
            "class TestOne(unittest.TestCase):\n"
            "    def test_x(self):\n"
            "        pass\n"
            "\n"
            "if __name__ == '__main__':\n"
            "    unittest.main()\n"
        )
        self.assertEqual(self._statements_after_main_guard(source), [])

    def test_nothing_at_all_follows_the_main_guard(self):
        offenders = {}
        for path in self._module_paths():
            hidden = self._statements_after_main_guard(
                Path(path).read_text(encoding='utf-8')
            )
            if hidden:
                offenders[os.path.relpath(path, _PROJECT_ROOT)] = hidden

        self.assertEqual(
            offenders, {},
            'these statements follow `if __name__ == "__main__"` and are '
            f'silently skipped when the file is run directly: {offenders}'
        )


if __name__ == '__main__':
    unittest.main()
