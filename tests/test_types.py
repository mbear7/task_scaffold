# -*- coding: utf-8 -*-
"""
PipelineSpec validation and immutability. Both test classes here exist
because of the same underlying gap, found by external review:
frozen=True on a dataclass only ever blocks reassigning a field itself,
never mutating (or, for a generator, consuming) whatever the field
points to. __post_init__ validated db_output/db_contract/
db_type_overrides but never normalized them into genuinely immutable
forms, so a generator passed as db_output would validate successfully
and then be silently exhausted, and a plain dict passed as db_contract
remained mutable in place despite the dataclass being frozen --
contradicting export.py's own stated guarantee that publish
configuration is captured before run() and cannot change during
execution.
"""

import unittest

import task_core as tc
from task_core.types import find_duplicates


class Test1DbOutputRejectsNonListTuple(unittest.TestCase):
    def test_generator_is_rejected_not_silently_consumed(self):
        # Confirmed directly before fixing: this used to validate
        # successfully, then list(spec.db_output) == [] on every later
        # read, with no error anywhere -- the generator had already been
        # exhausted by the validation loop itself.
        values = (x for x in ['a', 'b'])
        with self.assertRaises(TypeError):
            tc.PipelineSpec(db_output=values)

    def test_set_is_rejected(self):
        # Iterable and non-string/Mapping, so the old check accepted it --
        # but db_output's order is meaningful (a column projection/order),
        # and a set doesn't preserve one.
        with self.assertRaises(TypeError):
            tc.PipelineSpec(db_output={'a', 'b'})

    def test_list_is_normalized_to_tuple(self):
        spec = tc.PipelineSpec(db_output=['a', 'b'])
        self.assertIsInstance(spec.db_output, tuple)
        self.assertEqual(spec.db_output, ('a', 'b'))

    def test_tuple_still_works(self):
        spec = tc.PipelineSpec(db_output=('a', 'b'))
        self.assertEqual(spec.db_output, ('a', 'b'))

    def test_none_still_works(self):
        spec = tc.PipelineSpec(db_output=None)
        self.assertIsNone(spec.db_output)


class Test2DbContractAndTypeOverridesGenuinelyImmutable(unittest.TestCase):
    def test_db_contract_mutation_is_blocked(self):
        # Confirmed directly before fixing: spec.db_contract['a'] = 'y'
        # succeeded silently despite frozen=True on the dataclass itself.
        spec = tc.PipelineSpec(db_contract={'a': 'x'})
        with self.assertRaises(TypeError):
            spec.db_contract['a'] = 'y'
        self.assertEqual(dict(spec.db_contract), {'a': 'x'})

    def test_db_type_overrides_mutation_is_blocked(self):
        spec = tc.PipelineSpec(db_type_overrides={'a': 'INT'})
        with self.assertRaises(TypeError):
            spec.db_type_overrides['a'] = 'TEXT'

    def test_external_dict_mutation_after_construction_does_not_leak_in(self):
        # A different angle on the same guarantee: mutating the caller's
        # own dict *after* passing it to PipelineSpec must not affect the
        # already-constructed spec either -- __post_init__ must copy, not
        # alias.
        source = {'a': 'x'}
        spec = tc.PipelineSpec(db_contract=source)
        source['a'] = 'y'
        self.assertEqual(dict(spec.db_contract), {'a': 'x'})

    def test_none_still_works_for_both(self):
        spec = tc.PipelineSpec(db_contract=None, db_type_overrides=None)
        self.assertIsNone(spec.db_contract)
        self.assertIsNone(spec.db_type_overrides)

    def test_downstream_still_reads_a_real_db_contract_correctly(self):
        # Confirms the immutable form doesn't break real, downstream
        # usage in db_publish.py's _apply_db_contract_columns, which
        # does list(db_contract)/dict(db_contract) on it, not
        # isinstance(x, dict) -- verified this doesn't break by actually
        # running a real pipeline (hr_task.py's staff) with a genuine
        # db_contract end to end, not just this isolated check, before
        # trusting the fix.
        spec = tc.PipelineSpec(db_contract={'source_col': 'target_col'})
        from task_core.db_publish import from_petl
        import petl as etl
        tbl = etl.wrap([('source_col',), ('value',)])
        payload = from_petl(tbl, table_name='t', schema='s', db_contract=spec.db_contract)
        self.assertEqual(payload.columns, ['target_col'])


if __name__ == '__main__':
    unittest.main()


class Test3DbLoaderVocabulary(unittest.TestCase):
    """PipelineSpec exposes exactly the two implemented staging loaders."""

    def test_the_vocabulary_lists_both_implemented_loaders(self):
        from task_core.types import DB_LOADERS
        self.assertEqual(DB_LOADERS, ('insert', 'copy'))

    def test_default_is_insert(self):
        spec = tc.PipelineSpec(db_table='t')
        self.assertEqual(spec.db_loader, 'insert')
        self.assertIsNone(spec.db_copy_spool_encryption)

    def test_explicit_insert_and_copy_are_accepted(self):
        for value in ('insert', 'copy'):
            with self.subTest(value=value):
                self.assertEqual(
                    tc.PipelineSpec(db_table='t', db_loader=value).db_loader,
                    value,
                )

    def test_an_unknown_value_gets_the_generic_message(self):
        with self.assertRaises(ValueError) as caught:
            tc.PipelineSpec(db_table='t', db_loader='banana')
        self.assertIn("'banana'", str(caught.exception))

    def test_a_non_string_is_rejected(self):
        with self.assertRaises(ValueError):
            tc.PipelineSpec(db_table='t', db_loader=None)


class Test3bCopySpoolEncryptionOverride(unittest.TestCase):
    def test_accepts_none_true_and_false(self):
        for value in (None, True, False):
            with self.subTest(value=value):
                self.assertIs(
                    tc.PipelineSpec(db_copy_spool_encryption=value).db_copy_spool_encryption,
                    value,
                )

    def test_rejects_non_bool_values(self):
        for value in (0, 1, 'false', [], {}):
            with self.subTest(value=value):
                with self.assertRaises(TypeError):
                    tc.PipelineSpec(db_copy_spool_encryption=value)


class Test4OldPositionalPipelineSpecConstructionKeepsItsMeaning(unittest.TestCase):
    """PipelineSpec has grown fields across 0.4, 0.5 and 0.6. Every one was
    added at the end deliberately, so the same positional call written in
    0.4.1 still means what it meant in 0.4.1 -- the source comments at
    types.py:208, :213 and :221 spell out that this is the API-hygiene
    rule the additions must not break. CHANGELOG's promise for 0.6.0
    that 'keyword construction of PipelineSpec(...) needs no source
    change' is only half the story: the other half is that positional
    construction also survives. This test pins both halves so a future
    field insertion in the middle of the sequence fails here rather than
    silently reinterpreting every task file that used positional args.
    """

    def test_the_0_5_2_positional_sequence_still_binds_field_for_field(self):
        # Constructed as the 0.5.2 caller would have -- 13 positional
        # arguments, no db_loader. The 14th field (db_loader) must default
        # to 'insert'; every one of the 13 named values must land on the
        # field name the 0.5.2 caller intended.
        spec = tc.PipelineSpec(
            'sheet.xlsx',                 # excel_name
            'my_table',                   # db_table
            ('a', 'b'),                   # db_output
            {'src': 'tgt'},               # db_contract
            {'a': 'INT'},                 # db_type_overrides
            'idpix',                      # db_table_id_pix
            'updated',                    # db_updated_at (str truthy)
            True,                         # publish_result
            True,                         # debug_display
            'petl',                       # table_adapter
            ('a', 'b'),                   # db_not_null_columns
            None,                         # output_schema (None keeps db_type_overrides valid)
            'replace',                    # db_publication_strategy
        )

        self.assertEqual(spec.excel_name, 'sheet.xlsx')
        self.assertEqual(spec.db_table, 'my_table')
        self.assertEqual(spec.db_output, ('a', 'b'))
        self.assertEqual(dict(spec.db_contract), {'src': 'tgt'})
        self.assertEqual(dict(spec.db_type_overrides), {'a': 'INT'})
        self.assertEqual(spec.db_table_id_pix, 'idpix')
        self.assertEqual(spec.db_updated_at, 'updated')
        self.assertTrue(spec.publish_result)
        self.assertTrue(spec.debug_display)
        self.assertEqual(spec.table_adapter, 'petl')
        self.assertEqual(spec.db_not_null_columns, ('a', 'b'))
        self.assertIsNone(spec.output_schema)
        self.assertEqual(spec.db_publication_strategy, 'replace')
        # db_loader is the 0.6.0 addition; positional callers from 0.5.2
        # did not pass it, so it must default to 'insert'.
        self.assertEqual(spec.db_loader, 'insert')


class Test5DbRowSourceProtocolShape(unittest.TestCase):
    """DbRowSource (ADR 0011 §Row-source contract) is the shape the COPY
    transport reads from. It lives in types.py because it is level 0
    engine-neutral vocabulary -- adapters, publisher, and future
    db_copy.py all consult it. runtime_checkable is deliberate so a
    hand-written test double with an ``iter_rows()`` method can be
    isinstance-checked without a formal subclass declaration."""

    def test_the_protocol_is_declared_and_runtime_checkable(self):
        from task_core.types import DbRowSource
        # A plain object with the right method satisfies the protocol
        # under runtime_checkable, without inheriting from anything.
        class _Duck:
            def iter_rows(self):
                yield (1, 2)
        self.assertIsInstance(_Duck(), DbRowSource)

    def test_missing_iter_rows_fails_the_protocol_check(self):
        from task_core.types import DbRowSource
        class _NotASource:
            pass
        self.assertNotIsInstance(_NotASource(), DbRowSource)


class Test6PayloadSourceStateMatrix(unittest.TestCase):
    """validate_payload_source_state (ADR 0011 §Row-source contract)
    enforces the exact (loader, rows, row_source) legal states. The
    matrix has four cells; two are valid, two are configuration errors.

    Both loader legs are public in 0.6.6. These tests call the validator
    directly so the exact rows/row_source state matrix stays independent of
    adapter and publisher behavior."""

    def test_insert_with_rows_and_no_row_source_is_valid(self):
        from task_core.types import validate_payload_source_state
        # No return value; no exception either.
        validate_payload_source_state('insert', [{'a': 1}], None)

    def test_insert_with_missing_rows_is_a_configuration_error(self):
        from task_core.types import validate_payload_source_state
        with self.assertRaises(ValueError) as caught:
            validate_payload_source_state('insert', None, None)
        self.assertIn("'insert'", str(caught.exception))

    def test_insert_with_extra_row_source_is_a_configuration_error(self):
        from task_core.types import validate_payload_source_state
        class _S:
            def iter_rows(self): return iter(())
        with self.assertRaises(ValueError) as caught:
            validate_payload_source_state('insert', [{'a': 1}], _S())
        self.assertIn('row_source', str(caught.exception))

    def test_copy_with_row_source_and_no_rows_is_valid(self):
        from task_core.types import validate_payload_source_state
        class _S:
            def iter_rows(self): return iter(())
        validate_payload_source_state('copy', None, _S())

    def test_copy_with_missing_row_source_is_a_configuration_error(self):
        from task_core.types import validate_payload_source_state
        with self.assertRaises(ValueError) as caught:
            validate_payload_source_state('copy', None, None)
        self.assertIn("'copy'", str(caught.exception))

    def test_copy_with_extra_rows_is_a_configuration_error(self):
        from task_core.types import validate_payload_source_state
        class _S:
            def iter_rows(self): return iter(())
        with self.assertRaises(ValueError) as caught:
            validate_payload_source_state('copy', [{'a': 1}], _S())
        self.assertIn('materialized rows', str(caught.exception))

    def test_unknown_loader_is_an_invariant_violation(self):
        # validate_db_loader is supposed to reject unknown loaders first,
        # so reaching this function with an unknown value means the two
        # have drifted. Verified as an internal invariant, not a task-
        # author error, per the docstring in types.py.
        from task_core.types import validate_payload_source_state
        with self.assertRaises(ValueError) as caught:
            validate_payload_source_state('banana', [{'a': 1}], None)
        self.assertIn('invariant', str(caught.exception))

    def test_error_type_argument_is_honored(self):
        # validate_payload_source_state accepts error_type= for the same
        # reason validate_db_loader does -- DbPayload calls it with
        # DbPublishError so failures surface with the correct exception
        # class for downstream handlers.
        from task_core.types import validate_payload_source_state
        class _Custom(Exception): pass
        with self.assertRaises(_Custom):
            validate_payload_source_state('insert', None, None, error_type=_Custom)


class TestFindDuplicates(unittest.TestCase):
    """find_duplicates() is the one shared implementation of the
    order-preserving duplicate-finder previously hand-rolled in
    runner.py, binding.py, and db_publish.py -- these tests pin the
    properties those three call sites rely on for their error messages."""

    def test_no_duplicates_returns_empty_list(self):
        self.assertEqual(find_duplicates(['a', 'b', 'c']), [])
        self.assertEqual(find_duplicates([]), [])

    def test_first_occurrence_order_each_duplicate_once(self):
        # 'b' duplicates before 'a' does -- reported in that order, and a
        # triple still appears exactly once.
        self.assertEqual(find_duplicates(['a', 'b', 'b', 'a', 'b']), ['b', 'a'])

    def test_works_on_any_iterable_of_hashables(self):
        self.assertEqual(find_duplicates(iter((1, 2, 1))), [1])
