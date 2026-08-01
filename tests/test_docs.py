# -*- coding: utf-8 -*-
"""
Verifies that documented claims still match the code.

The documentation has no test suite of its own, and the previous README
accumulated at least three claims that did not survive contact with the
code -- a dependency described as a real package when it was a stub, a
rationale about identifier case folding that was false in this codebase,
and a no-orphan-residue guarantee stated as measurement when it was
upstream documentation. A file organised around findings makes that easy
to hide; a file organised around how the system works does not, but only
if something checks it.

These are deliberately claims a reader would ACT on -- signatures,
defaults, field sets, the adapter interface, layering, which module
imports what -- not prose. A claim that cannot be expressed as an
assertion belongs in the text with its uncertainty stated, not here.
"""

import ast
import contextlib
import io
import dataclasses
import importlib
import inspect
import os
from pathlib import Path
import re
import unittest

import petl as etl

import task_core as tc
from task_core.db_publish import (
    MAX_IDENTIFIER_BYTES,
    STAGING_NAME_KIND,
    DbPublisher,
    from_petl,
)
from task_core.table_adapters import _ADAPTERS, VALID_TABLE_ADAPTERS
from task_core.types import PORTABLE_IDENTIFIER_RE


class Test1DocumentedApiMatchesTheCode(unittest.TestCase):
    """docs/task-authoring.md tabulates these; a reader copies them."""

    def test_run_pipelines_defaults_are_as_documented(self):
        params = inspect.signature(tc.run_pipelines).parameters
        for name, default in (('output_excel', False), ('output_db', False),
                              ('pg_schema', 'bsr'), ('force_run', False),
                              ('publisher_config', None)):
            with self.subTest(parameter=name):
                self.assertEqual(params[name].default, default)

    def test_both_outputs_default_off(self):
        """Excel defaulted on until 0.3.6, which contradicted
        decisions/0007: an aid you opt into should not be produced by a
        task that never asked for it.

        Asserted as a pair, because the point is the consistency -- a task
        that declares no outputs produces none, and each is switched on
        deliberately.
        """
        params = inspect.signature(tc.run_pipelines).parameters
        self.assertFalse(params['output_excel'].default)
        self.assertFalse(params['output_db'].default)

    def test_required_run_pipelines_parameters_are_as_documented(self):
        params = inspect.signature(tc.run_pipelines).parameters
        required = {n for n, p in params.items() if p.default is inspect.Parameter.empty}
        self.assertEqual(required, {'task_name', 'build_context', 'pipelines', 'run_sequence'})

    def test_pipeline_spec_fields_are_fully_documented(self):
        # A field absent from the table is a field nobody knows exists.
        self.assertEqual(
            {f.name for f in dataclasses.fields(tc.PipelineSpec)},
            {'excel_name', 'db_table', 'db_output', 'db_contract', 'db_type_overrides',
             'db_not_null_columns', 'output_schema', 'db_table_id_pix',
             'db_updated_at', 'publish_result', 'debug_display', 'table_adapter',
             'db_publication_strategy', 'db_loader'},
        )

    def test_result_shapes_are_as_documented(self):
        self.assertEqual(
            {f.name for f in dataclasses.fields(tc.RunResult)},
            {'task_name', 'pipeline_rows', 'excel_outputs', 'db', 'skipped', 'skip_reason',
             'source_check_enabled', 'source_changed', 'source_fingerprints'},
        )
        self.assertEqual(
            {f.name for f in dataclasses.fields(tc.DbRunResult)},
            {'requested', 'had_outputs', 'committed', 'committed_tables',
             'published_tables', 'row_counts'},
        )

    def test_the_adapter_interface_is_the_documented_five_plus_validate(self):
        # to_row_source is the COPY-path counterpart to to_db_payload,
        # added in 0.6.2 for ADR 0011 §Row-source contract. It is
        # exercised only when db_loader='copy' (currently rejected at
        # every public boundary) but is part of the adapter interface
        # rather than a hidden helper because the runner reaches for it
        # by name; keeping it in this documented surface makes any
        # future removal or rename fail visibly here.
        expected = {'validate', 'nrows', 'display', 'to_excel', 'to_db_payload',
                    'stabilize', 'to_row_source'}
        for key, adapter in _ADAPTERS.items():
            with self.subTest(adapter=key):
                self.assertEqual({m for m in dir(adapter) if not m.startswith('_')}, expected)
        self.assertEqual(VALID_TABLE_ADAPTERS, frozenset({'petl', 'pandas', None}))

    def test_the_publisher_protocol_is_as_documented(self):
        # docs/architecture.md lists these as what an alternate publisher
        # must provide. Removing finalize_published_tables() from the
        # protocol was a deliberate correction; it must not creep back.
        self.assertLessEqual(
            {'publish', 'commit', 'rollback', 'close',
             'begin_run', 'ensure_connection'},
            set(dir(DbPublisher)),
        )
        # discard_pending_read() managed a lifecycle state the staged model
        # makes impossible: the source-state read is a bounded phase that
        # commits, so there is no pending read to discard. Removed rather
        # than left as unreachable code.
        self.assertFalse(hasattr(DbPublisher, 'discard_pending_read'))
        self.assertLessEqual(
            {'committed', 'committed_tables', 'written_tables', 'table_rows'},
            set(dir(DbPublisher)),
        )
        self.assertFalse(hasattr(DbPublisher, 'finalize_published_tables'))
        self.assertIsInstance(inspect.getattr_static(DbPublisher, 'preflight'), classmethod)

    def test_identifier_constants_are_as_documented(self):
        self.assertEqual(PORTABLE_IDENTIFIER_RE.pattern, r'^[a-z_][a-z0-9_]*$')
        self.assertEqual(MAX_IDENTIFIER_BYTES, 63)
        self.assertEqual(STAGING_NAME_KIND, 'stg')
        self.assertIs(
            importlib.import_module('task_core.source_state')._SAFE_IDENTIFIER_RE,
            PORTABLE_IDENTIFIER_RE,
            'decisions/0004 says the pattern is shared, not duplicated',
        )


class Test2DocumentedLayeringHolds(unittest.TestCase):
    """docs/architecture.md opens with a layering diagram. It is only
    worth printing if it is true."""

    def _modules(self):
        for root, _, files in os.walk('task_core'):
            for name in sorted(files):
                if name.endswith('.py'):
                    yield name, os.path.join(root, name)

    def test_nothing_below_the_runner_imports_the_runner(self):
        for name, path in self._modules():
            if name in ('runner.py', '__init__.py'):
                continue
            with self.subTest(module=name):
                self.assertNotIn('task_core.runner', Path(path).read_text(encoding='utf-8'))

    def test_the_runner_imports_no_table_engine(self):
        # The engine-neutrality claim: every engine difference is reached
        # through the adapter interface.
        source = Path('task_core/runner.py').read_text(encoding='utf-8')
        self.assertIsNone(re.search(r'^(import|from) (petl|pandas)', source, re.M))

    def test_only_the_documented_modules_import_an_engine(self):
        # architecture.md names these and says why each needs one.
        allowed = {'table_adapters.py', 'db_publish.py', 'db_values.py', 'excel.py', 'db.py'}
        for name, path in self._modules():
            if re.search(r'^(import|from) (petl|pandas)', Path(path).read_text(encoding='utf-8'), re.M):
                with self.subTest(module=name):
                    self.assertIn(name, allowed)

    def test_the_runner_reaches_context_only_under_type_checking(self):
        source = Path('task_core/runner.py').read_text(encoding='utf-8')
        self.assertIn('TYPE_CHECKING', source)
        module_level = [
            node for node in ast.parse(source).body
            if isinstance(node, ast.ImportFrom) and node.module == 'task_core.context'
        ]
        self.assertEqual(module_level, [])


class Test3DocumentedBehaviourOfDeclaredSpecFields(unittest.TestCase):
    """The distinction that a previous version of the documentation got
    wrong, and that a task author will get wrong the same way."""

    def test_db_output_is_declarative_and_not_applied(self):
        # The scaffold validates db_output and reads it during preflight,
        # but the pipeline does its own projection. Declaring it without
        # cutting is not an error -- you get the pipeline's columns.
        payload = from_petl(etl.wrap([['b', 'a'], [1, 2]]), table_name='t', schema=None)
        self.assertEqual(payload.columns, ['b', 'a'])

    def test_db_contract_is_applied_by_the_scaffold(self):
        payload = from_petl(
            etl.wrap([['Блок'], ['x']]), table_name='t', schema=None,
            db_contract={'Блок': 'block'},
        )
        self.assertEqual(payload.columns, ['block'])

    def test_resources_return_petl_tables_whatever_the_pipeline_uses(self):
        # A pandas pipeline reading an Excel or DB resource converts them
        # itself; task-authoring.md says so because it is not obvious.
        import tempfile
        from openpyxl import Workbook

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'x.xlsx')
            workbook = Workbook()
            workbook.active['A1'] = 'h'
            workbook.active['A2'] = 'v'
            workbook.save(path)

            resource = tc.build_excel_resource(path)
            self.addCleanup(resource.close)
            rows = resource.get_sheet_rows(resource.sheets[0])
            self.assertTrue(type(rows).__module__.startswith('petl'))


class Test4DocumentationFilesExistWhereLinkedFrom(unittest.TestCase):
    """A broken link in the front door is worse than no front door."""

    def test_no_exported_name_shadows_its_own_submodule(self):
        """A facade export named after its own module makes the module
        unreachable by attribute -- `task_core.file_access` resolved to the
        class, not the module, until 0.3.1.

        Checks the whole facade rather than that one name, because the
        trap is structural: any future `from task_core.foo import foo`
        recreates it. types.py shadowing the stdlib is the same class of
        problem one level up.
        """
        import task_core
        package_dir = os.path.dirname(task_core.__file__)
        submodules = {
            name[:-3] for name in os.listdir(package_dir)
            if name.endswith('.py') and name != '__init__.py'
        }

        shadowed = sorted(
            name for name in submodules
            if not isinstance(getattr(task_core, name, None), type(task_core))
            and hasattr(task_core, name)
        )
        self.assertEqual(
            shadowed, [],
            f'facade export(s) shadow their own submodule: {shadowed}',
        )

    def test_the_architecture_document_states_the_current_version(self):
        # It says "as of X" in two places, and both silently went stale
        # one release after being written.
        import task_core
        text = Path('docs/architecture.md').read_text(encoding='utf-8')
        claimed = set(re.findall(r'as of (\d+\.\d+\.\d+)', text))
        claimed |= set(re.findall(r'describes (\d+\.\d+\.\d+)', text))
        self.assertTrue(claimed, 'no version claim found to check')
        self.assertEqual(
            claimed, {task_core.__version__},
            f'architecture.md claims {sorted(claimed)}, package is {task_core.__version__}',
        )

    def test_documented_pipeline_examples_use_keyword_only_resources(self):
        # Bound resources are keyword-only. Two documented examples showed
        # the positional form, which fails at validation -- one of them
        # survived a correction pass because only the other was flagged.
        for path in ('docs/architecture.md', 'docs/task-authoring.md'):
            text = Path(path).read_text(encoding='utf-8')
            for match in re.findall(r'def run\(cls, ctx([^)]*)\)', text):
                with self.subTest(document=path, signature=match):
                    if match.strip():
                        self.assertTrue(
                            match.lstrip().startswith(', *'),
                            f'{path}: bound resources must be keyword-only, got "def run(cls, ctx{match})"',
                        )

    def test_the_excel_debugging_only_rule_is_stated_where_it_is_needed(self):
        """A decision only prevents a question from recurring if it is
        findable from wherever the question gets asked.

        Excel output not being a publication target is stated in the ADR,
        in the README's limitations, in the architecture's lifecycle, and
        in task-authoring where an author decides what to depend on. This
        asserts all four, because the ADR alone did not stop the topic
        coming back.
        """
        expected = {
            'docs/decisions/0007-excel-output-is-a-debugging-aid.md': 'debugging aid',
            'README.md': 'debugging aid',
            'docs/architecture.md': 'debugging aid',
            'docs/task-authoring.md': 'for debugging',
        }
        for path, phrase in expected.items():
            with self.subTest(document=path):
                text = Path(path).read_text(encoding='utf-8').lower()
                self.assertIn(phrase, text)
                self.assertIn('0007', text, f'{path} does not link the decision')

    def test_every_documented_path_exists(self):
        for path in ('docs/architecture.md', 'docs/task-authoring.md',
                     'docs/decisions/README.md', 'CHANGELOG.md',
                     'docs/decisions/0001-replace-tables-instead-of-truncating.md',
                     'docs/decisions/0002-keep-core-tests-independent-of-tasks.md',
                     'docs/decisions/0003-gc-collect-for-remote-workbook-handles.md',
                     'docs/decisions/0004-lowercase-portable-identifiers.md'):
            with self.subTest(path=path):
                self.assertTrue(os.path.exists(path), f'{path} is linked but missing')

    def test_internal_markdown_links_resolve(self):
        pattern = re.compile(r'\]\((?!https?://)([^)#]+)')
        for root, _, files in os.walk('docs'):
            for name in files:
                if not name.endswith('.md'):
                    continue
                source_path = os.path.join(root, name)
                for target in pattern.findall(Path(source_path).read_text(encoding='utf-8')):
                    resolved = os.path.normpath(os.path.join(root, target))
                    with self.subTest(source=source_path, target=target):
                        self.assertTrue(os.path.exists(resolved), f'{source_path} -> {target}')

        for target in pattern.findall(Path('README.md').read_text(encoding='utf-8')):
            with self.subTest(source='README.md', target=target):
                self.assertTrue(os.path.exists(os.path.normpath(target)))


class Test5TheDocumentedLevelMapMatchesRealImports(unittest.TestCase):
    """The layering diagram in docs/architecture.md is parsed out of the
    document and checked against actual imports.

    The earlier version of these tests verified selected dependency claims
    ('nothing imports the runner', 'the runner imports no engine') but not
    the level map itself, and missed that the diagram placed
    table_adapters.py at level 1 while it imports from db_publish.py at
    level 2. A diagram is worth printing only if something checks it.
    """

    LEVEL_BLOCK = re.compile(r'^level (\d)\s+(\S+\.py|\S+/)', re.M)
    CONTINUATION = re.compile(r'^\s{10,}(\S+\.py|\S+/)', re.M)

    def _documented_levels(self):
        """{module filename: level} as the architecture diagram states it."""
        text = Path('docs/architecture.md').read_text(encoding='utf-8')
        start = text.index('```\nlevel 0')
        block = text[start:text.index('```', start + 3)]

        levels, current = {}, None
        for line in block.splitlines():
            header = re.match(r'^level (\d)\s+(\S+)', line)
            if header:
                current = int(header.group(1))
                levels[header.group(2)] = current
                continue
            item = re.match(r'^\s{6,}(\S+\.py|\S+/)', line)
            if item and current is not None:
                levels[item.group(1)] = current
        return levels

    def _module_path(self, entry):
        if entry.endswith('/'):
            return None
        for candidate in (os.path.join('task_core', entry),
                          os.path.join('task_core', 'resources', entry)):
            if os.path.exists(candidate):
                return candidate
        return None

    def test_the_diagram_parses_and_covers_the_package(self):
        levels = self._documented_levels()
        self.assertGreater(len(levels), 8, 'level diagram did not parse')
        # Every non-package module in task_core/ is either in the diagram
        # or deliberately outside it.
        outside = {'__init__.py'}
        actual = {f for f in os.listdir('task_core') if f.endswith('.py')} - outside
        self.assertLessEqual(actual, set(levels), 'module missing from the level diagram')

    def test_no_module_imports_from_a_higher_level(self):
        levels = self._documented_levels()
        offenders = []

        for entry, level in sorted(levels.items()):
            path = self._module_path(entry)
            if path is None:
                continue
            for node in ast.walk(ast.parse(Path(path).read_text(encoding='utf-8'))):
                if not isinstance(node, ast.ImportFrom) or not node.module:
                    continue
                if not node.module.startswith('task_core.'):
                    continue
                imported = node.module.split('.')[-1] + '.py'
                imported_level = levels.get(imported)
                if imported_level is None:
                    continue
                if imported_level > level:
                    offenders.append(f'{entry} (level {level}) imports {imported} (level {imported_level})')

        self.assertEqual(offenders, [], 'level diagram contradicts real imports')


class Test6TheQuickStartActuallyRuns(unittest.TestCase):
    """README's quick start promises `python -m examples.local_task` works
    with no share, no database, and nothing outside this project's own
    declared requirements.

    The previous documentation's 'minimal task' called an undefined
    read_sheet(), pointed at an SMB path, needed database credentials, and
    could not be run by anyone who had merely cloned the repository. An example nobody can execute is
    worse than no example, because it looks like it should work. This test
    is the only thing standing between that and happening again.
    """

    def test_the_example_task_runs_and_produces_both_workbooks(self):
        from examples import local_task

        # The example prints its results, which is the point of it as an
        # example and noise inside a test run. Captured rather than
        # silenced, so a failure still has the output to look at.
        captured = io.StringIO()
        with contextlib.redirect_stdout(captured):
            result = local_task.main()

        self.assertIn('by_region.xlsx', captured.getvalue())

        self.assertFalse(result.skipped)
        self.assertEqual(sorted(result.excel_outputs), ['by_region.xlsx', 'deals.xlsx'])
        self.assertEqual(result.pipeline_rows['deals'], 4)
        self.assertEqual(result.pipeline_rows['by_region'], 2)

    def test_the_example_imports_nothing_the_quick_start_disclaims(self):
        # Actual imports, via AST -- not a text search, which would match
        # the module docstring's own promise that it needs none of these.
        tree = ast.parse(Path('examples/local_task.py').read_text(encoding='utf-8'))
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split('.')[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split('.')[0])

        # Named literally rather than derived, because the quick start
        # makes a promise about these specifically. 'in_house_helpers'
        # stands for any shared module a real task imports that this
        # project does not ship.
        for forbidden in ('in_house_helpers', 'smbclient', 'psycopg2', 'sqlalchemy'):
            with self.subTest(module=forbidden):
                self.assertNotIn(forbidden, imported)

    def test_bound_pipelines_use_keyword_only_resources(self):
        # The example got this wrong first and failed at validation, which
        # is how the documentation's own example was found to be wrong too.
        from examples import local_task
        signature = inspect.signature(local_task.deals.run)
        parameters = list(signature.parameters.values())
        self.assertEqual([p.name for p in parameters if p.kind is p.POSITIONAL_OR_KEYWORD], ['ctx'])
        self.assertEqual([p.name for p in parameters if p.kind is p.KEYWORD_ONLY], ['source'])


class Test7DbInsertBoundary(unittest.TestCase):
    """db_insert.py is the INSERT loader factored out of db_publish.py so
    it holds exactly one responsibility -- filling a staging table -- and
    none of the publisher's lifecycle. ADR 0011 §Tests names three
    structural properties that make that separation real rather than
    stylistic: db_insert does not know about DbPublisher, does not manage
    transactions, and does not build engines or open connections. AST
    checks rather than call-count mocks because the check is on what the
    file IS, not on what it happens to do this month.

    Scope: these are architectural tripwires, not exhaustive enforcement.
    They catch the obvious regressions (direct `create_engine`, `.begin`,
    `from task_core.db_publish import DbPublisher`) that would show up
    literally in the source; they do not catch `engine.raw_connection()`,
    `engine_from_config()`, or a transitive import that reaches the
    publisher through an intermediate helper. The current loader is 44
    lines and clean, so nothing more is warranted today. A stronger form
    -- import allowlist plus a check that the connection is only reached
    through a parameter -- is fair to add if the loader grows.
    """

    _SOURCE = Path('task_core/db_insert.py').read_text(encoding='utf-8')
    _TREE = ast.parse(_SOURCE)

    def _all_attribute_and_name_calls(self):
        # Every Call node with either a bare Name callee (create_engine,
        # connect) or an Attribute callee (.begin, .commit, .rollback).
        # Yields the leaf name in either case, which is what these tests
        # actually want to check membership on.
        for node in ast.walk(self._TREE):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if isinstance(func, ast.Name):
                yield func.id
            elif isinstance(func, ast.Attribute):
                yield func.attr

    def test_db_insert_does_not_import_db_publisher(self):
        for node in ast.walk(self._TREE):
            if isinstance(node, ast.ImportFrom):
                imported = [alias.name for alias in (node.names or [])]
                with self.subTest(module=node.module, names=imported):
                    self.assertNotIn('DbPublisher', imported)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    self.assertFalse(
                        alias.name.endswith('db_publish'),
                        f'db_insert imports {alias.name} -- the loader must not '
                        f'know the publisher exists',
                    )

    def test_db_insert_never_begins_commits_or_rolls_back_transactions(self):
        forbidden = {'begin', 'commit', 'rollback',
                     '_ensure_transaction', '_commit_transaction'}
        calls = set(self._all_attribute_and_name_calls())
        offenders = sorted(forbidden & calls)
        self.assertEqual(
            offenders, [],
            f'db_insert calls {offenders}: the publisher owns the transaction, '
            f'per db_insert\'s own module docstring and ADR 0011',
        )

    def test_db_insert_never_creates_an_engine_or_second_connection(self):
        forbidden = {'create_engine', 'connect', 'URL', 'create'}
        # 'create' is broad on purpose -- create_engine, URL.create, and
        # engine.connect all show up under it or under connect. False
        # positives would come from calls named 'create' that are unrelated
        # (e.g. sa.Table.create), and there are none in db_insert today; a
        # future one would need justification here rather than a silent
        # pass.
        calls = set(self._all_attribute_and_name_calls())
        offenders = sorted(forbidden & calls)
        self.assertEqual(
            offenders, [],
            f'db_insert calls {offenders}: it must run against the connection '
            f'the publisher passed in, not construct its own',
        )


if __name__ == '__main__':
    unittest.main()
