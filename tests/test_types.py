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
