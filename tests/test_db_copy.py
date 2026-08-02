# -*- coding: utf-8 -*-
"""Tests for task_core.db_copy: the COPY-loader spool subsystem primitives.

Phase 5.b of ADR 0011 introduces four independent primitives, each
tested in its own TestN class:

- ownership token (deterministic 40-char SHA-1 digest of five
  ingredients)
- filename grammar (compose/parse round-trip, boundary cases)
- internal header (write/read round-trip, rejection modes)
- directory resolution (best-effort mkdir, tempdir fallback)

None of these open a database connection or begin a transaction. The
whole module is filesystem + bytes work; the tests match.
"""

import io
import json
import os
import struct
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timedelta, timezone
from pathlib import Path

import task_core.db_copy as db_copy_module

from task_core.db_copy import (
    CopyLoadPolicy,
    DEFAULT_SPOOL_SUBDIR,
    FORMAT_VERSION,
    MAGIC,
    PROTECTION_AES256_GCM,
    PROTECTION_NONE,
    SPOOL_FILENAME_RE,
    SPOOL_STAGES,
    SpoolFormatError,
    SpoolIdentity,
    cleanup_predecessor_spools,
    cleanup_spool_paths,
    load_copy_into_staging,
    compose_ownership_token,
    compose_spool_filename,
    open_spool_for_read,
    open_spool_for_write,
    parse_spool_filename,
    prepare_copy_source,
    read_neutral_preamble,
    read_neutral_row,
    read_spool_header,
    resolve_spool_directory,
    serialize_row_to_copytext,
    write_neutral_preamble,
    write_neutral_row,
    write_neutral_terminator,
    write_spool_header,
)
from task_core.db_values import DbPublishError, ResolvedColumn

import math
from datetime import date
from decimal import Decimal

import sqlalchemy as sa


# A ready-made valid ingredient tuple used by several tests.
_RUN_START = datetime(2026, 8, 1, 14, 23, 11, 123456, tzinfo=timezone.utc)
_INGREDIENTS = dict(
    task='hr_task',
    target_schema='public',
    target_table='employees',
    run_start_utc=_RUN_START,
    pid=12345,
)


class Test1OwnershipTokenIsDeterministicAndDistinguishing(unittest.TestCase):
    """The token is the file's link between the current run and any
    stale predecessor spool. Two runs with different ingredients must
    produce different tokens, or the ownership check that predecessor
    cleanup depends on collapses. Two calls with the same ingredients
    must produce the same token, or the same run cannot recognize its
    own file after restart.
    """

    def test_same_ingredients_yield_same_token(self):
        a = compose_ownership_token(**_INGREDIENTS)
        b = compose_ownership_token(**_INGREDIENTS)
        self.assertEqual(a, b)
        # SHA-1 hex length; guards against an accidental algorithm swap
        # that would break every serialized filename in the wild.
        self.assertEqual(len(a), 40)
        self.assertRegex(a, r'^[0-9a-f]{40}$')

    def test_different_task_yields_different_token(self):
        a = compose_ownership_token(**_INGREDIENTS)
        b = compose_ownership_token(**{**_INGREDIENTS, 'task': 'ops_task'})
        self.assertNotEqual(a, b)

    def test_different_schema_yields_different_token(self):
        a = compose_ownership_token(**_INGREDIENTS)
        b = compose_ownership_token(**{**_INGREDIENTS, 'target_schema': 'staging'})
        self.assertNotEqual(a, b)

    def test_different_table_yields_different_token(self):
        a = compose_ownership_token(**_INGREDIENTS)
        b = compose_ownership_token(**{**_INGREDIENTS, 'target_table': 'contractors'})
        self.assertNotEqual(a, b)

    def test_different_run_start_yields_different_token(self):
        a = compose_ownership_token(**_INGREDIENTS)
        later = _RUN_START + timedelta(microseconds=1)
        b = compose_ownership_token(**{**_INGREDIENTS, 'run_start_utc': later})
        self.assertNotEqual(a, b)

    def test_different_pid_yields_different_token(self):
        a = compose_ownership_token(**_INGREDIENTS)
        b = compose_ownership_token(**{**_INGREDIENTS, 'pid': 12346})
        self.assertNotEqual(a, b)

    def test_delimiter_swap_cannot_forge_the_same_token(self):
        # A delimiter that can appear inside an ingredient invites
        # collision by boundary migration. If the delimiter were `|`,
        # these two ingredient tuples would join to the identical
        # `foo|bar|baz|qux|...` string and digest identically:
        #
        #   a: table='baz|qux'  -> ...|bar|baz|qux|...
        #   b: schema='bar|baz' -> ...|bar|baz|qux|...
        #
        # With `\x1f` -- a control byte neither identifiers nor
        # ISO-8601 timestamps contain -- the collision is impossible.
        a = compose_ownership_token(
            task='foo', target_schema='bar', target_table='baz|qux',
            run_start_utc=_RUN_START, pid=1,
        )
        b = compose_ownership_token(
            task='foo', target_schema='bar|baz', target_table='qux',
            run_start_utc=_RUN_START, pid=1,
        )
        self.assertNotEqual(a, b)

    def test_rejects_empty_task_schema_or_table(self):
        for key in ('task', 'target_schema', 'target_table'):
            with self.subTest(field=key):
                with self.assertRaises(DbPublishError):
                    compose_ownership_token(**{**_INGREDIENTS, key: ''})

    def test_rejects_non_utc_aware_datetime(self):
        # A naive datetime would digest one way on machine A and another
        # on machine B interpreting the same wall clock -- the whole
        # point of requiring UTC.
        naive = datetime(2026, 8, 1, 14, 23, 11, 123456)
        with self.assertRaises(DbPublishError):
            compose_ownership_token(**{**_INGREDIENTS, 'run_start_utc': naive})
        # A non-UTC aware datetime is refused for the same reason: the
        # same instant expressed in different offsets isoformats
        # differently.
        tokyo = datetime(2026, 8, 1, 23, 23, 11, 123456,
                         tzinfo=timezone(timedelta(hours=9)))
        with self.assertRaises(DbPublishError):
            compose_ownership_token(**{**_INGREDIENTS, 'run_start_utc': tokyo})

    def test_rejects_bool_or_zero_pid(self):
        # bool subclasses int; True as pid would silently digest the
        # string '1' -- almost certainly not what the caller meant.
        for bad in (True, False, 0, -1):
            with self.subTest(value=bad):
                with self.assertRaises(DbPublishError):
                    compose_ownership_token(**{**_INGREDIENTS, 'pid': bad})


class Test2FilenameGrammarRoundTripsAndRejectsDeviations(unittest.TestCase):
    """Filenames are the pre-filter for predecessor cleanup. The parser
    must accept everything compose_spool_filename emits and reject
    everything else, without regard to whether the on-disk file exists.
    """

    def test_compose_and_parse_round_trip(self):
        token = compose_ownership_token(**_INGREDIENTS)
        for stage in SPOOL_STAGES:
            with self.subTest(stage=stage):
                name = compose_spool_filename(token=token, stage=stage)
                parsed = parse_spool_filename(name)
                self.assertEqual(parsed, {'token': token, 'stage': stage})

    def test_regex_is_anchored_both_ends(self):
        # A well-formed name embedded in a longer string is not our
        # file; anchoring keeps predecessor cleanup from matching
        # substrings inside somebody else's filename.
        token = compose_ownership_token(**_INGREDIENTS)
        name = compose_spool_filename(token=token, stage='neutral')
        self.assertIsNotNone(SPOOL_FILENAME_RE.match(name))
        self.assertIsNone(SPOOL_FILENAME_RE.match(name + '.bak'))
        self.assertIsNone(SPOOL_FILENAME_RE.match('_' + name))

    def test_parse_rejects_uppercase_hex_in_token(self):
        # Filenames are lowercase-normalized by convention here.
        # Accepting mixed case would double the set of "valid" filenames
        # for the same underlying token and split ownership.
        malformed = 'task_core-copy-' + ('A' * 40) + '-neutral.spool'
        self.assertIsNone(parse_spool_filename(malformed))

    def test_parse_rejects_wrong_stage(self):
        token = 'a' * 40
        malformed = f'task_core-copy-{token}-final.spool'
        self.assertIsNone(parse_spool_filename(malformed))

    def test_parse_rejects_wrong_prefix(self):
        token = 'a' * 40
        self.assertIsNone(parse_spool_filename(
            f'taskcore-copy-{token}-neutral.spool'
        ))
        self.assertIsNone(parse_spool_filename(
            f'task_core-COPY-{token}-neutral.spool'
        ))

    def test_parse_rejects_wrong_extension(self):
        token = 'a' * 40
        self.assertIsNone(parse_spool_filename(
            f'task_core-copy-{token}-neutral.tmp'
        ))
        self.assertIsNone(parse_spool_filename(
            f'task_core-copy-{token}-neutral.spool.gz'
        ))

    def test_parse_rejects_short_or_long_token(self):
        for length in (39, 41):
            token = 'a' * length
            with self.subTest(length=length):
                self.assertIsNone(parse_spool_filename(
                    f'task_core-copy-{token}-neutral.spool'
                ))

    def test_parse_returns_none_for_non_string_input(self):
        # bytes, Path, None; predecessor cleanup iterates os.listdir()
        # which yields strings, but a defensive None keeps the failure
        # mode consistent instead of TypeErroring.
        self.assertIsNone(parse_spool_filename(None))
        self.assertIsNone(parse_spool_filename(123))

    def test_compose_rejects_a_malformed_token(self):
        for bad in ('short', 'A' * 40, 'g' * 40, 123, None):
            with self.subTest(token=bad):
                with self.assertRaises(DbPublishError):
                    compose_spool_filename(token=bad, stage='neutral')

    def test_compose_rejects_unknown_stage(self):
        token = compose_ownership_token(**_INGREDIENTS)
        for bad in ('final', 'raw', '', 'NEUTRAL', None):
            with self.subTest(stage=bad):
                with self.assertRaises(DbPublishError):
                    compose_spool_filename(token=token, stage=bad)


class Test3InternalHeaderRoundTripsAndRejectsMalformedFiles(unittest.TestCase):
    """The header is the second half of positive ownership. Predecessor
    cleanup will read it and compare its payload to the current run's
    ingredients. Every rejection path must raise SpoolFormatError -- the
    exception type cleanup treats as "not ours, leave alone".
    """

    def _write_and_read(self, buf=None, **overrides):
        buf = buf if buf is not None else io.BytesIO()
        ingredients = {**_INGREDIENTS, **overrides}
        token = compose_ownership_token(
            task=ingredients['task'],
            target_schema=ingredients['target_schema'],
            target_table=ingredients['target_table'],
            run_start_utc=ingredients['run_start_utc'],
            pid=ingredients['pid'],
        )
        write_spool_header(
            buf,
            task=ingredients['task'],
            target_schema=ingredients['target_schema'],
            target_table=ingredients['target_table'],
            run_start_utc=ingredients['run_start_utc'],
            pid=ingredients['pid'],
            token=token,
            stage=ingredients.get('stage', 'neutral'),
        )
        buf.seek(0)
        return read_spool_header(buf), token

    def test_round_trip_preserves_all_ingredients(self):
        payload, token = self._write_and_read()
        self.assertEqual(payload['task'], _INGREDIENTS['task'])
        self.assertEqual(payload['target_schema'], _INGREDIENTS['target_schema'])
        self.assertEqual(payload['target_table'], _INGREDIENTS['target_table'])
        self.assertEqual(payload['run_start_utc'], _RUN_START.isoformat())
        self.assertEqual(payload['pid'], _INGREDIENTS['pid'])
        self.assertEqual(payload['token'], token)
        self.assertEqual(payload['stage'], 'neutral')

    def test_round_trip_covers_both_stages(self):
        for stage in SPOOL_STAGES:
            payload, _ = self._write_and_read(stage=stage)
            self.assertEqual(payload['stage'], stage)

    def test_write_refuses_a_token_that_disagrees_with_ingredients(self):
        # A self-inconsistent header would defeat predecessor cleanup:
        # the reader compares filename-token to header-token to
        # ingredients. Refuse at write time so this class of corruption
        # never reaches disk.
        buf = io.BytesIO()
        bogus_token = 'f' * 40
        with self.assertRaises(DbPublishError):
            write_spool_header(
                buf,
                **_INGREDIENTS,
                token=bogus_token,
                stage='neutral',
            )

    def test_write_refuses_unknown_stage(self):
        buf = io.BytesIO()
        token = compose_ownership_token(**_INGREDIENTS)
        with self.assertRaises(DbPublishError):
            write_spool_header(
                buf, **_INGREDIENTS, token=token, stage='final'
            )

    def test_reader_rejects_wrong_magic(self):
        # Truncate the header, replace the magic with a plausible
        # imposter (PostgreSQL COPY binary starts with "PGCOPY"), and
        # confirm the reader refuses.
        buf = io.BytesIO()
        token = compose_ownership_token(**_INGREDIENTS)
        write_spool_header(buf, **_INGREDIENTS, token=token, stage='neutral')
        contents = buf.getvalue()
        forged = b'PGCOPY' + contents[6:]
        with self.assertRaises(SpoolFormatError) as cm:
            read_spool_header(io.BytesIO(forged))
        self.assertIn('magic', str(cm.exception).lower())

    def test_reader_rejects_unknown_version(self):
        # Old readers must refuse a header written by a future writer
        # (higher version) rather than guess at the payload shape.
        buf = io.BytesIO()
        token = compose_ownership_token(**_INGREDIENTS)
        write_spool_header(buf, **_INGREDIENTS, token=token, stage='neutral')
        contents = bytearray(buf.getvalue())
        # Replace uint16 version at offset 6 with a value != FORMAT_VERSION.
        struct.pack_into('>H', contents, 6, FORMAT_VERSION + 1)
        with self.assertRaises(SpoolFormatError) as cm:
            read_spool_header(io.BytesIO(bytes(contents)))
        self.assertIn('version', str(cm.exception).lower())

    def test_reader_rejects_short_prefix(self):
        # Empty file, truncated file: both mean "not enough bytes to
        # even identify the format". Both raise SpoolFormatError.
        with self.assertRaises(SpoolFormatError):
            read_spool_header(io.BytesIO(b''))
        with self.assertRaises(SpoolFormatError):
            read_spool_header(io.BytesIO(MAGIC[:3]))

    def test_reader_rejects_truncated_payload(self):
        # Prefix claims a payload length, but the file ends short. This
        # is the classic corruption case; must raise, not hang or return
        # partial data.
        prefix = struct.pack('>6sHI', MAGIC, FORMAT_VERSION, 100)
        with self.assertRaises(SpoolFormatError) as cm:
            read_spool_header(io.BytesIO(prefix + b'{}'))  # only 2 payload bytes
        self.assertIn('payload', str(cm.exception).lower())

    def test_reader_rejects_oversized_payload_length(self):
        # A corrupted length field claiming gigabytes of header would
        # invite a runaway allocation. The cap is what defends against
        # that; test that the cap is enforced.
        huge = 1 << 30
        prefix = struct.pack('>6sHI', MAGIC, FORMAT_VERSION, huge)
        with self.assertRaises(SpoolFormatError) as cm:
            read_spool_header(io.BytesIO(prefix))
        self.assertIn('cap', str(cm.exception).lower())

    def test_reader_rejects_malformed_json_payload(self):
        garbage = b'not json at all}'
        prefix = struct.pack('>6sHI', MAGIC, FORMAT_VERSION, len(garbage))
        with self.assertRaises(SpoolFormatError):
            read_spool_header(io.BytesIO(prefix + garbage))

    def test_reader_rejects_json_that_is_not_a_mapping(self):
        payload = b'["task", "schema"]'
        prefix = struct.pack('>6sHI', MAGIC, FORMAT_VERSION, len(payload))
        with self.assertRaises(SpoolFormatError):
            read_spool_header(io.BytesIO(prefix + payload))

    def test_reader_rejects_payload_missing_a_required_key(self):
        skeleton = {
            'task': 'x', 'target_schema': 'y', 'target_table': 'z',
            'run_start_utc': _RUN_START.isoformat(), 'pid': 1,
            'token': 'a' * 40, 'stage': 'neutral',
        }
        for key in list(skeleton):
            partial = {k: v for k, v in skeleton.items() if k != key}
            payload = json.dumps(partial, sort_keys=True).encode('ascii')
            prefix = struct.pack('>6sHI', MAGIC, FORMAT_VERSION, len(payload))
            with self.subTest(missing=key):
                with self.assertRaises(SpoolFormatError):
                    read_spool_header(io.BytesIO(prefix + payload))

    def test_reader_rejects_payload_with_unknown_stage(self):
        payload_dict = {
            'task': 'x', 'target_schema': 'y', 'target_table': 'z',
            'run_start_utc': _RUN_START.isoformat(), 'pid': 1,
            'token': 'a' * 40, 'stage': 'raw',
        }
        payload = json.dumps(payload_dict, sort_keys=True).encode('ascii')
        prefix = struct.pack('>6sHI', MAGIC, FORMAT_VERSION, len(payload))
        with self.assertRaises(SpoolFormatError):
            read_spool_header(io.BytesIO(prefix + payload))

    def test_spool_format_error_is_a_dbpublisherror(self):
        # Callers that already catch the publication exception
        # hierarchy pick up SpoolFormatError without a new except.
        # Assert the inheritance directly so a future flattening of the
        # hierarchy shows up as a broken test rather than a silent
        # widening.
        self.assertTrue(issubclass(SpoolFormatError, DbPublishError))


class Test4SpoolDirectoryResolvesAndCreatesBestEffort(unittest.TestCase):
    """resolve_spool_directory returns an existing directory. It creates
    what does not exist yet (0o700 where honored), accepts what already
    does, and never re-applies permissions to a directory it did not
    create.
    """

    def test_policy_supplied_path_is_created_and_returned(self):
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / 'my-spool'
            self.assertFalse(target.exists())
            resolved = resolve_spool_directory(CopyLoadPolicy(spool_directory=target))
            self.assertTrue(resolved.is_dir())
            self.assertEqual(resolved, target.resolve(strict=False))

    def test_none_policy_falls_back_to_tempdir_subdir(self):
        resolved = resolve_spool_directory(None)
        expected = (Path(tempfile.gettempdir()) / DEFAULT_SPOOL_SUBDIR).resolve(strict=False)
        self.assertEqual(resolved, expected)
        self.assertTrue(resolved.is_dir())

    def test_default_policy_falls_back_to_tempdir_subdir(self):
        # Same behavior as None -- an unset policy is functionally
        # equivalent to a defaulted CopyLoadPolicy.
        resolved = resolve_spool_directory(CopyLoadPolicy())
        expected = (Path(tempfile.gettempdir()) / DEFAULT_SPOOL_SUBDIR).resolve(strict=False)
        self.assertEqual(resolved, expected)

    def test_existing_directory_is_accepted(self):
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / 'existing-spool'
            target.mkdir()
            resolved = resolve_spool_directory(CopyLoadPolicy(spool_directory=target))
            self.assertEqual(resolved, target.resolve(strict=False))

    def test_deeply_nested_target_creates_parents(self):
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / 'a' / 'b' / 'c'
            resolved = resolve_spool_directory(CopyLoadPolicy(spool_directory=target))
            self.assertTrue(resolved.is_dir())

    def test_returns_an_absolute_path(self):
        # Callers -- especially predecessor cleanup -- reason about
        # containment; a relative path would depend on the current
        # working directory.
        with tempfile.TemporaryDirectory() as td:
            target = Path(td) / 'my-spool'
            resolved = resolve_spool_directory(CopyLoadPolicy(spool_directory=target))
            self.assertTrue(resolved.is_absolute())

    def test_rejects_a_non_policy(self):
        for bad in ('/tmp', object(), 5):
            with self.subTest(value=bad):
                with self.assertRaises(DbPublishError):
                    resolve_spool_directory(bad)


class Test5ConstantsAreStable(unittest.TestCase):
    """The three constants are on-disk facts: any change would silently
    orphan every spool ever written under the old value. Assert them
    literally so a change bumps this test as a load-bearing warning
    that a format-version bump is due.
    """

    def test_magic_bytes(self):
        self.assertEqual(MAGIC, b'TCCPY\x00')
        self.assertEqual(len(MAGIC), 6)

    def test_format_version(self):
        self.assertEqual(FORMAT_VERSION, 2)

    def test_stages(self):
        self.assertEqual(SPOOL_STAGES, ('neutral', 'copytext'))


class Test6NeutralSpoolRoundTripsEveryScalarFamily(unittest.TestCase):
    """The type-neutral spool must preserve every scalar family ADR 0011
    §Local spool design lists (NULL, bool, int, float, Decimal, str,
    bytes, date, naive datetime, aware datetime) with byte-for-byte
    equality. Any loss (str(Decimal), tz normalization, silent int
    truncation) would surface at pass 2 as a wrong-value bug that looks
    like a serializer defect but is actually a spool defect.
    """

    def _round_trip(self, columns, rows):
        buf = io.BytesIO()
        write_neutral_preamble(buf, columns=columns)
        for row in rows:
            write_neutral_row(buf, row, expected_width=len(columns))
        write_neutral_terminator(buf)
        buf.seek(0)
        read_names = read_neutral_preamble(buf)
        read_rows = []
        while True:
            r = read_neutral_row(buf, len(columns))
            if r is None:
                break
            read_rows.append(r)
        return read_names, read_rows

    def test_null_round_trips(self):
        names, rows = self._round_trip(['x'], [[None]])
        self.assertEqual(names, ('x',))
        self.assertEqual(rows, [(None,)])

    def test_bool_round_trips_both_values(self):
        _, rows = self._round_trip(['x'], [[True], [False]])
        # `is` not ==: 1 == True and 0 == False in Python, so equality
        # alone would not distinguish int-encoded-as-bool from real bool.
        self.assertIs(rows[0][0], True)
        self.assertIs(rows[1][0], False)

    def test_int_round_trips_across_widths(self):
        # 0, ±1, byte boundaries, int64 boundaries, arbitrarily large --
        # the variable-length encoding must handle all of them.
        cases = [0, 1, -1, 127, 128, -128, -129,
                 32767, -32768, 32768, -32769,
                 2**31 - 1, -(2**31), 2**31,
                 2**63 - 1, -(2**63), 2**63,
                 2**200, -(2**200)]
        _, rows = self._round_trip(['x'], [[v] for v in cases])
        for expected, (actual,) in zip(cases, rows):
            with self.subTest(value=expected):
                self.assertEqual(actual, expected)
                self.assertIs(type(actual), int)

    def test_float_round_trips_regular_and_special_values(self):
        # Floats are stored bit-exact as IEEE-754 doubles; NaN cannot be
        # compared with ==, so use math.isnan for that case.
        cases = [0.0, -0.0, 1.5, -1.5, 3.141592653589793,
                 1e300, -1e300, 1e-300]
        _, rows = self._round_trip(['x'], [[v] for v in cases])
        for expected, (actual,) in zip(cases, rows):
            with self.subTest(value=expected):
                # Note: -0.0 == 0.0 is True; check the sign bit via repr
                self.assertEqual(actual, expected)
        # Special: inf, -inf, NaN
        _, rows = self._round_trip(['x'], [[float('inf')], [float('-inf')], [float('nan')]])
        self.assertEqual(rows[0][0], float('inf'))
        self.assertEqual(rows[1][0], float('-inf'))
        self.assertTrue(math.isnan(rows[2][0]))

    def test_negative_zero_float_preserves_sign_bit(self):
        _, rows = self._round_trip(['x'], [[-0.0]])
        # struct '>d' is bit-preserving; math.copysign detects the sign
        # even though -0.0 == 0.0.
        self.assertEqual(math.copysign(1.0, rows[0][0]), -1.0)

    def test_decimal_round_trips_finite_and_special(self):
        cases = [Decimal('0'), Decimal('123.456'), Decimal('-0.000001'),
                 Decimal('1E+100'), Decimal('NaN'), Decimal('Infinity'),
                 Decimal('-Infinity')]
        _, rows = self._round_trip(['x'], [[v] for v in cases])
        for expected, (actual,) in zip(cases, rows):
            with self.subTest(value=str(expected)):
                # NaN != NaN, so compare via is_nan on both sides
                if expected.is_nan():
                    self.assertTrue(actual.is_nan())
                else:
                    self.assertEqual(actual, expected)

    def test_str_round_trips_empty_ascii_and_non_bmp(self):
        # Empty string, plain ASCII, BMP unicode, and a non-BMP codepoint
        # (`U+1F600` is a supplementary plane character; UTF-16 pairs it,
        # UTF-8 uses four bytes). All must round-trip identically.
        cases = ['', 'hello', 'käse', '\U0001F600']
        _, rows = self._round_trip(['x'], [[v] for v in cases])
        for expected, (actual,) in zip(cases, rows):
            with self.subTest(value=repr(expected)):
                self.assertEqual(actual, expected)

    def test_bytes_round_trips_empty_and_binary_including_nulls(self):
        # NUL bytes inside a payload must not be treated as terminators;
        # the length prefix is the only framing signal that matters.
        cases = [b'', b'\x00\x01\x02', b'\xff' * 10, b'row\x00mid\x00end']
        _, rows = self._round_trip(['x'], [[v] for v in cases])
        for expected, (actual,) in zip(cases, rows):
            with self.subTest(value=expected):
                self.assertEqual(actual, expected)

    def test_bytes_accepts_bytearray_and_memoryview(self):
        # The pass 1 source might hand us a bytearray or memoryview
        # (e.g. from a driver's binary type). Both must serialize to the
        # same on-disk representation as bytes and read back as bytes.
        for original in (bytearray(b'abc'), memoryview(b'abc')):
            with self.subTest(kind=type(original).__name__):
                _, rows = self._round_trip(['x'], [[original]])
                self.assertEqual(rows[0][0], b'abc')
                self.assertIs(type(rows[0][0]), bytes)

    def test_date_round_trips_including_year_boundaries(self):
        cases = [date(1, 1, 1), date(9999, 12, 31), date(2026, 8, 1)]
        _, rows = self._round_trip(['x'], [[v] for v in cases])
        for expected, (actual,) in zip(cases, rows):
            with self.subTest(value=expected):
                self.assertEqual(actual, expected)
                self.assertIs(type(actual), date)

    def test_datetime_naive_round_trips_including_year_boundaries(self):
        cases = [datetime(1, 1, 1),
                 datetime(9999, 12, 31, 23, 59, 59, 999999),
                 datetime(2026, 8, 1, 14, 23, 11, 123456)]
        _, rows = self._round_trip(['x'], [[v] for v in cases])
        for expected, (actual,) in zip(cases, rows):
            with self.subTest(value=expected):
                self.assertEqual(actual, expected)
                self.assertIsNone(actual.tzinfo)

    def test_datetime_aware_round_trips_utc_and_nonzero_offsets(self):
        utc_dt = datetime(2026, 8, 1, 14, 23, 11, 123456, tzinfo=timezone.utc)
        tokyo = datetime(2026, 8, 1, 23, 23, 11, 123456,
                         tzinfo=timezone(timedelta(hours=9)))
        indian = datetime(2026, 8, 1, 20, 8, 11, 123456,
                          tzinfo=timezone(timedelta(hours=5, minutes=45)))
        negative = datetime(2026, 8, 1, 6, 23, 11, 123456,
                            tzinfo=timezone(timedelta(hours=-8)))
        cases = [utc_dt, tokyo, indian, negative]
        _, rows = self._round_trip(['x'], [[v] for v in cases])
        for expected, (actual,) in zip(cases, rows):
            with self.subTest(value=expected):
                self.assertEqual(actual, expected)
                self.assertIsNotNone(actual.tzinfo)
                self.assertEqual(actual.utcoffset(), expected.utcoffset())

    def test_datetime_subclass_of_date_encodes_as_datetime_not_date(self):
        # datetime is a subclass of date. A dispatch that tested date
        # first would encode datetimes as dates and silently drop the
        # time component -- a bug the type_check below catches.
        dt = datetime(2026, 8, 1, 14, 23, 11)
        _, rows = self._round_trip(['x'], [[dt]])
        self.assertIs(type(rows[0][0]), datetime)
        self.assertEqual(rows[0][0], dt)

    def test_bool_encoded_distinctly_from_int_zero_and_one(self):
        # Same trap in the other direction: without the bool-before-int
        # check, True/False would encode as int 1/0 and pass 2 would
        # route them to Integer instead of Boolean.
        _, rows = self._round_trip(['x'], [[True], [1], [False], [0]])
        self.assertIs(type(rows[0][0]), bool)
        self.assertIs(type(rows[1][0]), int)
        self.assertIs(type(rows[2][0]), bool)
        self.assertIs(type(rows[3][0]), int)

    def test_multi_column_multi_row_round_trip(self):
        columns = ['id', 'name', 'amount', 'active', 'when']
        rows = [
            [1, 'alice', Decimal('10.50'), True,
             datetime(2026, 8, 1, 12, 0, 0)],
            [2, None, None, False, None],
            [3, 'carol', Decimal('0'), None,
             datetime(2026, 8, 1, 12, 30, 0)],
        ]
        _, read_rows = self._round_trip(columns, rows)
        self.assertEqual(len(read_rows), 3)
        for expected, actual in zip(rows, read_rows):
            self.assertEqual(tuple(expected), actual)

    def test_zero_row_spool_terminates_immediately(self):
        buf = io.BytesIO()
        write_neutral_preamble(buf, columns=['x'])
        write_neutral_terminator(buf)
        buf.seek(0)
        self.assertEqual(read_neutral_preamble(buf), ('x',))
        self.assertIsNone(read_neutral_row(buf, 1))


class Test7NeutralSpoolRejectsCorruptionAndInvalidInputs(unittest.TestCase):
    """Every framing violation surfaces as SpoolFormatError, and every
    caller-side type violation surfaces as DbPublishError. The two
    exception types have different semantics for the lifecycle in
    Phase 5.f: SpoolFormatError means "not ours, leave the file alone";
    DbPublishError means "the row we tried to spool was malformed".
    """

    def test_writer_rejects_unsupported_type(self):
        buf = io.BytesIO()
        write_neutral_preamble(buf, columns=['x'])
        with self.assertRaises(DbPublishError):
            write_neutral_row(buf, [object()], expected_width=1)

    def test_writer_rejects_row_width_mismatch_before_marker_lands(self):
        # A width mismatch is a caller bug; the marker byte must not be
        # written or the reader would advance past a legitimate marker
        # into garbage. Assert directly on buf.tell().
        buf = io.BytesIO()
        write_neutral_preamble(buf, columns=['a', 'b'])
        checkpoint = buf.tell()
        with self.assertRaises(DbPublishError):
            write_neutral_row(buf, [1], expected_width=2)
        self.assertEqual(buf.tell(), checkpoint,
                         'row-start byte leaked despite width mismatch')
        with self.assertRaises(DbPublishError):
            write_neutral_row(buf, [1, 2, 3], expected_width=2)
        self.assertEqual(buf.tell(), checkpoint)

    def test_writer_rejects_bad_expected_width(self):
        buf = io.BytesIO()
        for bad in (-1, 1.5, '1', True):
            with self.subTest(value=bad):
                with self.assertRaises(DbPublishError):
                    write_neutral_row(buf, [1], expected_width=bad)

    def test_writer_rejects_non_sequence_row(self):
        buf = io.BytesIO()
        for bad in ('abc', b'abc', 5, None):
            with self.subTest(value=bad):
                with self.assertRaises(DbPublishError):
                    write_neutral_row(buf, bad, expected_width=1)

    def test_writer_rejects_non_sequence_columns(self):
        buf = io.BytesIO()
        for bad in ('abc', b'abc', 5, None):
            with self.subTest(value=bad):
                with self.assertRaises(DbPublishError):
                    write_neutral_preamble(buf, columns=bad)

    def test_writer_rejects_subsecond_tz_offset(self):
        buf = io.BytesIO()
        weird = datetime(2026, 8, 1, tzinfo=timezone(timedelta(seconds=3600, microseconds=1)))
        with self.assertRaises(DbPublishError):
            write_neutral_row(buf, [weird], expected_width=1)

    def test_reader_rejects_unknown_tag(self):
        # Handcraft a body with an out-of-range tag after a valid marker.
        body = bytes([254, 0x7F])  # ROW_START, then tag 0x7F (unknown)
        with self.assertRaises(SpoolFormatError) as cm:
            read_neutral_row(io.BytesIO(body), 1)
        self.assertIn('tag', str(cm.exception).lower())

    def test_reader_rejects_wrong_row_marker(self):
        body = bytes([0x42])  # neither ROW_START nor TERMINATOR
        with self.assertRaises(SpoolFormatError) as cm:
            read_neutral_row(io.BytesIO(body), 1)
        self.assertIn('row-start', str(cm.exception).lower())

    def test_reader_rejects_missing_terminator(self):
        # Empty file: no marker at all. That is what a truncated spool
        # would look like, and the reader must not silently return None.
        with self.assertRaises(SpoolFormatError):
            read_neutral_row(io.BytesIO(b''), 1)

    def test_reader_rejects_truncated_value(self):
        # ROW_START, TAG_STR, length prefix claiming 10 bytes, only 2.
        body = bytes([254, 0x06]) + struct.pack('>I', 10) + b'ab'
        with self.assertRaises(SpoolFormatError):
            read_neutral_row(io.BytesIO(body), 1)

    def test_reader_rejects_oversized_length(self):
        # Corrupted 4-byte length claiming 1 GB for a str field.
        body = bytes([254, 0x06]) + struct.pack('>I', 1 << 30)
        with self.assertRaises(SpoolFormatError) as cm:
            read_neutral_row(io.BytesIO(body), 1)
        self.assertIn('cap', str(cm.exception).lower())

    def test_reader_rejects_int_with_length_zero(self):
        # Length 0 is an impossible encoding under the writer's rules
        # (minimum is 1 byte for value 0). Reject rather than return 0
        # via a bogus code path.
        body = bytes([254, 0x03, 0])
        with self.assertRaises(SpoolFormatError):
            read_neutral_row(io.BytesIO(body), 1)

    def test_reader_rejects_invalid_utf8_in_str(self):
        # 0xFF is not a legal start byte in UTF-8; the decode must fail
        # cleanly rather than substitute a replacement character.
        body = bytes([254, 0x06]) + struct.pack('>I', 2) + b'\xff\xfe'
        with self.assertRaises(SpoolFormatError):
            read_neutral_row(io.BytesIO(body), 1)

    def test_reader_rejects_invalid_date_components(self):
        # February 30th is invalid; the date() constructor rejects it,
        # which the reader must surface as SpoolFormatError.
        body = bytes([254, 0x08]) + struct.pack('>HBB', 2026, 2, 30)
        with self.assertRaises(SpoolFormatError):
            read_neutral_row(io.BytesIO(body), 1)

    def test_reader_rejects_bad_column_count_type(self):
        with self.assertRaises(DbPublishError):
            read_neutral_row(io.BytesIO(b''), -1)
        with self.assertRaises(DbPublishError):
            read_neutral_row(io.BytesIO(b''), 'one')


class Test8NeutralSpoolRowAtomicity(unittest.TestCase):
    """Row atomicity is not decoration -- a mid-row failure that landed
    partial bytes would corrupt the spool in a way the reader cannot
    distinguish from a legitimate short read. The writer buffers the
    entire row in memory and issues a single fp.write, so either the
    whole row lands or none of it does.
    """

    def test_row_is_written_with_a_single_underlying_write(self):
        # Wrap a BytesIO in a recording adapter and count write() calls
        # per row. If the writer streams bytes to fp piecewise, this
        # count is >1 and the atomicity guarantee is a lie.
        class CountingBuf:
            def __init__(self):
                self.underlying = io.BytesIO()
                self.write_calls = 0

            def write(self, data):
                self.write_calls += 1
                return self.underlying.write(data)

        buf = CountingBuf()
        # Preamble writes are not row writes; measure only row writes.
        write_neutral_preamble(buf, columns=['a', 'b', 'c'])
        before = buf.write_calls
        write_neutral_row(buf, [1, 'two', None], expected_width=3)
        after = buf.write_calls
        self.assertEqual(after - before, 1,
                         f'row write took {after - before} fp.write calls, '
                         'expected 1 for atomicity')


def _copytext_unescape_field(field: bytes) -> object:
    """Reverse COPY-text escaping for a single field's raw wire bytes.

    Deliberately written from PostgreSQL's COPY documentation rather than
    from the task_core implementation, so a round-trip through this
    decoder exercises the encoder against an independent reference. The
    only escape sequences the encoder produces are `\\\\`, `\\t`, `\\n`,
    `\\r`; every other byte passes through unchanged. `\\N` as an entire
    field means SQL NULL.
    """
    if field == b'\\N':
        return None
    result = bytearray()
    i = 0
    while i < len(field):
        b = field[i]
        if b == 0x5c:  # backslash
            i += 1
            if i >= len(field):
                raise ValueError('trailing unpaired backslash')
            nxt = field[i]
            if nxt == 0x5c:
                result.append(0x5c)
            elif nxt == ord('t'):
                result.append(0x09)
            elif nxt == ord('n'):
                result.append(0x0a)
            elif nxt == ord('r'):
                result.append(0x0d)
            else:
                raise ValueError(f'unknown COPY escape \\{chr(nxt)}')
        else:
            result.append(b)
        i += 1
    return bytes(result)


def _split_copy_line(line: bytes) -> list[bytes]:
    """Split one wire line into raw (still-escaped) field bytes on TAB.

    COPY's TAB is a hard separator: an in-value TAB is always encoded as
    the two-byte sequence `\\t` by the writer, so plain `bytes.split` on
    0x09 is exactly the right inverse.
    """
    if line.endswith(b'\n'):
        line = line[:-1]
    return line.split(b'\t')


class Test9CopyTextSerializerProducesExactWireBytes(unittest.TestCase):
    """Every declared family has one canonical wire form; each cell in
    this test asserts the exact bytes that must appear on the wire.
    These are the golden reference for the encoder: a change here is a
    protocol change and should be visible in review.
    """

    def _serialize_one(self, column, value):
        row = {column.name: value}
        line = serialize_row_to_copytext(row, [column], 'tbl', 1)
        self.assertTrue(line.endswith(b'\n'), 'row missing LF terminator')
        return line[:-1]

    def test_bool_true_is_lowercase_t(self):
        col = ResolvedColumn('b', sa.Boolean(), True)
        self.assertEqual(self._serialize_one(col, True), b't')

    def test_bool_false_is_lowercase_f(self):
        col = ResolvedColumn('b', sa.Boolean(), True)
        self.assertEqual(self._serialize_one(col, False), b'f')

    def test_smallint_serializes_as_decimal_ascii(self):
        col = ResolvedColumn('n', sa.SmallInteger(), True)
        self.assertEqual(self._serialize_one(col, -32000), b'-32000')

    def test_integer_serializes_as_decimal_ascii(self):
        col = ResolvedColumn('n', sa.Integer(), True)
        self.assertEqual(self._serialize_one(col, 2_000_000_000), b'2000000000')

    def test_bigint_serializes_as_decimal_ascii(self):
        col = ResolvedColumn('n', sa.BigInteger(), True)
        self.assertEqual(
            self._serialize_one(col, 9_223_372_036_854_775_807),
            b'9223372036854775807',
        )

    def test_numeric_preserves_exact_decimal_digits(self):
        col = ResolvedColumn('n', sa.Numeric(precision=10, scale=4), True)
        self.assertEqual(self._serialize_one(col, Decimal('1.2300')), b'1.2300')

    def test_numeric_accepts_python_int_and_writes_integer_form(self):
        col = ResolvedColumn('n', sa.Numeric(precision=10, scale=0), True)
        self.assertEqual(self._serialize_one(col, 42), b'42')

    def test_float_finite_uses_repr_shortest_round_trip(self):
        col = ResolvedColumn('f', sa.Float(), True)
        self.assertEqual(self._serialize_one(col, 3.14), b'3.14')

    def test_float_nan_is_capitalized_NaN(self):
        col = ResolvedColumn('f', sa.Float(), True)
        # str(float('nan')) is 'nan' -- PostgreSQL rejects that spelling.
        self.assertEqual(self._serialize_one(col, float('nan')), b'NaN')

    def test_float_positive_infinity_is_Infinity(self):
        col = ResolvedColumn('f', sa.Float(), True)
        self.assertEqual(self._serialize_one(col, float('inf')), b'Infinity')

    def test_float_negative_infinity_is_minus_Infinity(self):
        col = ResolvedColumn('f', sa.Float(), True)
        self.assertEqual(self._serialize_one(col, float('-inf')), b'-Infinity')

    def test_text_ascii_passes_through_unescaped(self):
        col = ResolvedColumn('s', sa.Text(), True)
        self.assertEqual(self._serialize_one(col, 'hello world'), b'hello world')

    def test_text_utf8_passes_through_encoded(self):
        col = ResolvedColumn('s', sa.Text(), True)
        # A four-byte codepoint (emoji) plus Cyrillic to prove UTF-8 is
        # never touched by the escaper.
        self.assertEqual(
            self._serialize_one(col, 'привет \U0001F600'),
            'привет \U0001F600'.encode('utf-8'),
        )

    def test_text_empty_string_is_zero_bytes_not_null(self):
        col = ResolvedColumn('s', sa.Text(), True)
        # Empty field != NULL. This is the entire reason COPY has a
        # distinct \N marker. A test that conflates the two would let a
        # regression through silently.
        self.assertEqual(self._serialize_one(col, ''), b'')

    def test_null_in_nullable_column_is_the_two_byte_marker(self):
        col = ResolvedColumn('s', sa.Text(), True)
        self.assertEqual(self._serialize_one(col, None), b'\\N')

    def test_bytes_uses_backslash_x_hex_form_on_wire(self):
        col = ResolvedColumn('b', sa.LargeBinary(), True)
        # bytea in COPY text: field on wire is `\\x<hex>` so COPY's
        # unescape yields `\x<hex>` for the bytea input parser.
        self.assertEqual(
            self._serialize_one(col, b'\x00\x01\xff'),
            b'\\\\x0001ff',
        )

    def test_bytes_empty_is_backslash_x_with_no_hex(self):
        col = ResolvedColumn('b', sa.LargeBinary(), True)
        self.assertEqual(self._serialize_one(col, b''), b'\\\\x')

    def test_bytes_accepts_memoryview_and_bytearray(self):
        col = ResolvedColumn('b', sa.LargeBinary(), True)
        self.assertEqual(
            self._serialize_one(col, bytearray(b'\xab\xcd')),
            b'\\\\xabcd',
        )
        self.assertEqual(
            self._serialize_one(col, memoryview(b'\xab\xcd')),
            b'\\\\xabcd',
        )

    def test_date_serializes_as_isoformat(self):
        col = ResolvedColumn('d', sa.Date(), True)
        self.assertEqual(self._serialize_one(col, date(2024, 1, 15)), b'2024-01-15')

    def test_naive_datetime_uses_space_separator(self):
        col = ResolvedColumn('t', sa.DateTime(timezone=False), True)
        self.assertEqual(
            self._serialize_one(col, datetime(2024, 1, 15, 10, 30, 45, 123456)),
            b'2024-01-15 10:30:45.123456',
        )

    def test_aware_datetime_carries_utc_offset(self):
        col = ResolvedColumn('t', sa.DateTime(timezone=True), True)
        self.assertEqual(
            self._serialize_one(
                col,
                datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc),
            ),
            b'2024-01-15 10:30:45+00:00',
        )


class Test10CopyTextEscapingHandlesAdversarialTextCorpus(unittest.TestCase):
    """The escaper is the single place in the codebase where COPY-text
    escaping exists. Two ways it can go wrong silently: (1) escape in the
    wrong order so backslash + n in user data collapses to a newline;
    (2) forget one of the four structural bytes so a tab or LF slips
    through as a separator. Test 11's round-trip proves those and more.
    """

    def _round_trip(self, text: str) -> str:
        col = ResolvedColumn('s', sa.Text(), True)
        line = serialize_row_to_copytext({'s': text}, [col], 'tbl', 1)
        fields = _split_copy_line(line)
        self.assertEqual(len(fields), 1, 'single-field row split into multiple')
        recovered = _copytext_unescape_field(fields[0])
        self.assertIsInstance(recovered, bytes)
        return recovered.decode('utf-8')

    def test_backslash_is_escaped_first(self):
        # The critical ordering test. If the encoder replaced `\n` before
        # `\`, a genuine backslash in front of a legitimate `n` would
        # decode as newline. Assert the raw wire bytes directly.
        col = ResolvedColumn('s', sa.Text(), True)
        line = serialize_row_to_copytext({'s': '\\n'}, [col], 'tbl', 1)
        self.assertEqual(line, b'\\\\n\n')

    def test_literal_backslash_N_survives_unchanged(self):
        # User data of `\N` must round-trip -- otherwise it would be
        # indistinguishable from NULL on the wire.
        self.assertEqual(self._round_trip('\\N'), '\\N')

    def test_double_backslash_survives_unchanged(self):
        self.assertEqual(self._round_trip('\\\\'), '\\\\')

    def test_tab_survives_and_does_not_split_the_row(self):
        self.assertEqual(self._round_trip('col1\tcol2'), 'col1\tcol2')

    def test_newline_survives_and_does_not_terminate_the_row(self):
        self.assertEqual(self._round_trip('line1\nline2'), 'line1\nline2')

    def test_carriage_return_survives(self):
        self.assertEqual(self._round_trip('line1\r\nline2'), 'line1\r\nline2')

    def test_empty_string_survives(self):
        self.assertEqual(self._round_trip(''), '')

    def test_unicode_survives(self):
        text = 'привет \U0001F600 mixing \tsymbols\n and \\backslashes'
        self.assertEqual(self._round_trip(text), text)


class Test11CopyTextRowSerializerJoinsAndValidatesEveryCell(unittest.TestCase):
    """The row-level function's job is composition + validation, not
    escaping. These tests exercise multi-column rows, wire ordering,
    per-cell validation dispatch, and the NULL-vs-empty distinction at
    the row level.
    """

    def test_multi_column_row_joins_fields_with_tab_and_ends_with_lf(self):
        cols = [
            ResolvedColumn('id', sa.Integer(), False),
            ResolvedColumn('name', sa.Text(), True),
            ResolvedColumn('active', sa.Boolean(), False),
        ]
        row = {'id': 7, 'name': 'alice', 'active': True}
        line = serialize_row_to_copytext(row, cols, 'employees', 1)
        self.assertEqual(line, b'7\talice\tt\n')

    def test_wire_order_follows_columns_not_row_dict_iteration(self):
        # Build the row dict with keys in the opposite order to verify
        # the wire order is column-driven, not dict-driven.
        cols = [
            ResolvedColumn('id', sa.Integer(), False),
            ResolvedColumn('name', sa.Text(), True),
        ]
        row = {'name': 'bob', 'id': 3}
        line = serialize_row_to_copytext(row, cols, 't', 1)
        self.assertEqual(line, b'3\tbob\n')

    def test_null_in_nullable_column_becomes_the_marker(self):
        cols = [
            ResolvedColumn('id', sa.Integer(), False),
            ResolvedColumn('note', sa.Text(), True),
        ]
        line = serialize_row_to_copytext({'id': 1, 'note': None}, cols, 't', 1)
        self.assertEqual(line, b'1\t\\N\n')

    def test_empty_text_field_is_distinguishable_from_null(self):
        # This is the second half of the NULL-vs-empty invariant: an
        # empty text value must land as zero bytes between separators,
        # never as `\N`. Test 9 checks the encoder branch; this test
        # checks the row-level composition.
        cols = [
            ResolvedColumn('id', sa.Integer(), False),
            ResolvedColumn('note', sa.Text(), True),
        ]
        line = serialize_row_to_copytext({'id': 1, 'note': ''}, cols, 't', 1)
        self.assertEqual(line, b'1\t\n')
        # And the two must differ on the wire.
        null_line = serialize_row_to_copytext(
            {'id': 1, 'note': None}, cols, 't', 1,
        )
        self.assertNotEqual(line, null_line)

    def test_null_in_non_nullable_column_raises(self):
        cols = [ResolvedColumn('id', sa.Integer(), False)]
        with self.assertRaises(DbPublishError) as ctx:
            serialize_row_to_copytext({'id': None}, cols, 't', 5)
        self.assertIn('non-nullable column', str(ctx.exception))
        self.assertIn("'id'", str(ctx.exception))
        self.assertIn('row 5', str(ctx.exception))

    def test_wrong_python_type_raises_via_validate_declared_value(self):
        # Str where int declared: this must go through the shared
        # validator, not through a duplicate check inside db_copy. If
        # this ever starts passing, some path inside db_copy has grown
        # its own type check that will drift from db_publish.
        cols = [ResolvedColumn('n', sa.Integer(), False)]
        with self.assertRaises(DbPublishError):
            serialize_row_to_copytext({'n': 'seven'}, cols, 't', 1)

    def test_bool_is_not_accepted_as_integer(self):
        cols = [ResolvedColumn('n', sa.Integer(), False)]
        with self.assertRaises(DbPublishError):
            serialize_row_to_copytext({'n': True}, cols, 't', 1)

    def test_nul_in_text_is_rejected_before_serialization(self):
        # `_validate_declared_value` rejects NUL, so the serializer never
        # has to escape it. Confirm the boundary is enforced here too.
        cols = [ResolvedColumn('s', sa.Text(), True)]
        with self.assertRaises(DbPublishError) as ctx:
            serialize_row_to_copytext({'s': 'a\x00b'}, cols, 't', 1)
        self.assertIn('NUL', str(ctx.exception))

    def test_naive_datetime_rejected_for_timezone_true_column(self):
        cols = [ResolvedColumn('t', sa.DateTime(timezone=True), False)]
        with self.assertRaises(DbPublishError):
            serialize_row_to_copytext(
                {'t': datetime(2024, 1, 1, 0, 0, 0)}, cols, 't', 1,
            )

    def test_aware_datetime_rejected_for_timezone_false_column(self):
        cols = [ResolvedColumn('t', sa.DateTime(timezone=False), False)]
        with self.assertRaises(DbPublishError):
            serialize_row_to_copytext(
                {'t': datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)},
                cols, 't', 1,
            )


class Test12CopyTextRoundTripsWithIndependentDecoderForEveryFamily(unittest.TestCase):
    """A last-mile check: every family the encoder produces must be
    correctly parsed by an independent decoder written from the COPY
    docs. This catches encoder mistakes that produce syntactically
    valid but semantically wrong output.
    """

    def test_mixed_row_round_trips_through_independent_decoder(self):
        cols = [
            ResolvedColumn('id', sa.Integer(), False),
            ResolvedColumn('name', sa.Text(), True),
            ResolvedColumn('note', sa.Text(), True),
            ResolvedColumn('active', sa.Boolean(), False),
            ResolvedColumn('score', sa.Numeric(10, 2), True),
        ]
        row = {
            'id': 42,
            'name': 'weird\tvalue\nwith \\backslashes',
            'note': None,
            'active': False,
            'score': Decimal('99.50'),
        }
        line = serialize_row_to_copytext(row, cols, 't', 1)
        fields = _split_copy_line(line)
        self.assertEqual(len(fields), 5)
        self.assertEqual(_copytext_unescape_field(fields[0]), b'42')
        self.assertEqual(
            _copytext_unescape_field(fields[1]).decode('utf-8'),
            'weird\tvalue\nwith \\backslashes',
        )
        self.assertIsNone(_copytext_unescape_field(fields[2]))
        self.assertEqual(_copytext_unescape_field(fields[3]), b'f')
        self.assertEqual(_copytext_unescape_field(fields[4]), b'99.50')


def _make_identity(**overrides):
    """Build a SpoolIdentity with the canonical ingredient tuple, overriding
    named fields for tests that vary one dimension at a time."""
    args = dict(_INGREDIENTS)
    args.update(overrides)
    return SpoolIdentity(**args)


def _open_plain_spool(directory, *, stage, identity, **kwargs):
    """Low-level framing tests opt out explicitly; secure defaults are
    exercised separately by Test17EncryptedSpoolContainer."""
    handle = open_spool_for_write(
        directory, stage=stage, identity=identity, encrypt=False, **kwargs,
    )
    return handle.stream, handle.path


class Test13SpoolIdentityBundlesTheFiveOwnershipIngredientsWithDerivedToken(unittest.TestCase):
    """SpoolIdentity should freeze the five ingredients + derived token
    together so that no caller can pass a token that disagrees with its
    ingredients. Prior versions of the phase design threaded five keyword
    arguments through four different primitives; the dataclass exists to
    replace that.
    """

    def test_token_matches_compose_ownership_token(self):
        ident = _make_identity()
        expected = compose_ownership_token(**_INGREDIENTS)
        self.assertEqual(ident.token, expected)

    def test_identity_is_frozen(self):
        ident = _make_identity()
        with self.assertRaises(Exception):
            ident.task = 'other'

    def test_construction_validates_ingredients(self):
        # Empty task -> DbPublishError from compose_ownership_token,
        # surfaced at construction time not at first use.
        with self.assertRaises(DbPublishError):
            SpoolIdentity(**{**_INGREDIENTS, 'task': ''})

    def test_naive_timestamp_rejected(self):
        with self.assertRaises(DbPublishError):
            SpoolIdentity(**{
                **_INGREDIENTS,
                'run_start_utc': datetime(2026, 8, 1, 14, 23, 11),
            })

    def test_two_identities_with_same_ingredients_produce_same_token(self):
        a = _make_identity()
        b = _make_identity()
        self.assertEqual(a.token, b.token)
        self.assertEqual(a, b)


class Test14SpoolFilesAreCreatedAtomicallyUnderExclusiveOpen(unittest.TestCase):
    """open_spool_for_write must create the file with O_EXCL and write
    the ownership header before returning. Loss of either property lets a
    silent overwrite or a header-less file survive the call.
    """

    def test_file_created_with_header_and_expected_filename(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            ident = _make_identity()
            fp, path = _open_plain_spool(
                directory, stage='neutral', identity=ident,
            )
            try:
                self.assertTrue(path.exists())
                self.assertEqual(
                    path.name,
                    f'task_core-copy-{ident.token}-neutral.spool',
                )
            finally:
                fp.close()

    def test_header_round_trips_via_read_spool_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            ident = _make_identity()
            fp, path = _open_plain_spool(
                directory, stage='copytext', identity=ident,
            )
            fp.close()
            with path.open('rb') as rp:
                header = read_spool_header(rp)
            self.assertEqual(header['task'], ident.task)
            self.assertEqual(header['target_schema'], ident.target_schema)
            self.assertEqual(header['target_table'], ident.target_table)
            self.assertEqual(header['token'], ident.token)
            self.assertEqual(header['stage'], 'copytext')
            self.assertEqual(header['pid'], ident.pid)
            self.assertEqual(header['protection'], PROTECTION_NONE)

    def test_o_excl_blocks_silent_overwrite_of_existing_file(self):
        # This is the teeth test for O_EXCL. Predecessor cleanup runs
        # first; if a file survives that pass and open_spool_for_write
        # then overwrites it silently, we destroy evidence.
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            ident = _make_identity()
            filename = f'task_core-copy-{ident.token}-neutral.spool'
            (directory / filename).write_bytes(b'preexisting bytes')
            with self.assertRaises(FileExistsError):
                _open_plain_spool(
                    directory, stage='neutral', identity=ident,
                )
            # And the original bytes must still be there -- no truncate.
            self.assertEqual(
                (directory / filename).read_bytes(), b'preexisting bytes'
            )

    def test_permissions_are_owner_only_on_posix(self):
        # On Windows the mode is silently ignored; we just check that
        # the call doesn't raise there. On POSIX we check 0o600.
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            ident = _make_identity()
            fp, path = _open_plain_spool(
                directory, stage='neutral', identity=ident,
            )
            try:
                if os.name == 'posix':
                    mode = path.stat().st_mode & 0o777
                    self.assertEqual(mode, 0o600)
            finally:
                fp.close()

    def test_rejects_bad_stage(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(DbPublishError):
                _open_plain_spool(
                    Path(tmp), stage='bogus', identity=_make_identity(),
                )

    def test_rejects_non_path_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(DbPublishError):
                _open_plain_spool(
                    tmp, stage='neutral', identity=_make_identity(),  # type: ignore[arg-type]
                )

    def test_rejects_non_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(DbPublishError):
                _open_plain_spool(
                    Path(tmp), stage='neutral',
                    identity=dict(_INGREDIENTS),  # type: ignore[arg-type]
                )

    def test_rejects_non_positive_buffer(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(DbPublishError):
                _open_plain_spool(
                    Path(tmp), stage='neutral',
                    identity=_make_identity(), buffer_bytes=0,
                )


class Test15SpoolReadValidatesHeaderAgainstIdentity(unittest.TestCase):
    """open_spool_for_read exists to verify a spool the same process wrote
    is still the one it thinks. Token or stage disagreement is a bug --
    DbPublishError -- distinct from framing corruption -- SpoolFormatError.
    """

    def test_reads_back_own_spool_and_leaves_body_position_at_first_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            ident = _make_identity()
            handle = open_spool_for_write(
                directory, stage='neutral', identity=ident,
            )
            handle.stream.write(b'BODY-BYTES')
            handle.stream.close()
            rp = open_spool_for_read(
                handle.path, identity=ident, stage='neutral', key=handle.key,
            )
            try:
                self.assertEqual(rp.read(), b'BODY-BYTES')
            finally:
                rp.close()

    def test_wrong_stage_raises_publish_error_not_format_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            ident = _make_identity()
            wp, path = _open_plain_spool(
                directory, stage='neutral', identity=ident,
            )
            wp.close()
            with self.assertRaises(DbPublishError) as ctx:
                open_spool_for_read(path, identity=ident, stage='copytext')
            # Not SpoolFormatError (framing is fine, ownership is wrong).
            self.assertNotIsInstance(ctx.exception.__cause__, SpoolFormatError)

    def test_foreign_identity_raises_publish_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            ours = _make_identity()
            wp, path = _open_plain_spool(
                directory, stage='neutral', identity=ours,
            )
            wp.close()
            theirs = _make_identity(pid=999999)
            with self.assertRaises(DbPublishError):
                open_spool_for_read(path, identity=theirs, stage='neutral')

    def test_truncated_header_raises_format_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            ident = _make_identity()
            filename = f'task_core-copy-{ident.token}-neutral.spool'
            path = directory / filename
            path.write_bytes(b'\x00\x01')  # too short for even the prefix
            with self.assertRaises(SpoolFormatError):
                open_spool_for_read(path, identity=ident, stage='neutral')


class Test16CleanupSpoolPathsIsBestEffortAndNeverRaises(unittest.TestCase):
    """cleanup_spool_paths runs inside `finally` blocks in the runner; it
    must not raise, even when handed a missing or unremovable path.
    """

    def test_removes_present_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = Path(tmp) / 'a'
            b = Path(tmp) / 'b'
            a.write_bytes(b'')
            b.write_bytes(b'')
            failed = cleanup_spool_paths([a, b])
            self.assertEqual(failed, [])
            self.assertFalse(a.exists())
            self.assertFalse(b.exists())

    def test_missing_paths_are_success(self):
        failed = cleanup_spool_paths([Path('/no/such/path/xyz')])
        self.assertEqual(failed, [])

    def test_empty_input_is_success(self):
        self.assertEqual(cleanup_spool_paths([]), [])

    def test_transient_unlink_failure_is_retried(self):
        path = Path('/tmp/retry-me')
        with patch.object(
            Path, 'unlink', side_effect=[OSError('busy'), OSError('busy'), None],
        ) as unlink:
            failed = cleanup_spool_paths(
                [path], attempts=3, retry_delay_seconds=0,
            )
        self.assertEqual(failed, [])
        self.assertEqual(unlink.call_count, 3)

    def test_residual_path_is_returned_and_logged_exactly(self):
        path = Path('/tmp/cannot-remove-this-spool')
        with patch.object(Path, 'unlink', side_effect=OSError('still open')):
            with self.assertLogs('task_core.db_copy', level='WARNING') as captured:
                failed = cleanup_spool_paths(
                    [path], attempts=2, retry_delay_seconds=0,
                )
        self.assertEqual(failed, [path])
        self.assertIn(str(path), '\n'.join(captured.output))
        self.assertIn('still open', '\n'.join(captured.output))


class Test17PredecessorCleanupDeletesOwnSpoolsPreservesEverythingElse(unittest.TestCase):
    """cleanup_predecessor_spools is the reap-under-lock pass. It must
    delete files that positively belong to this task (grammar-conforming
    filename AND header task-field match) and preserve everything else.
    """

    def test_returns_empty_lists_when_directory_missing(self):
        deleted, preserved = cleanup_predecessor_spools(
            Path('/no/such/path/definitely_missing'),
            task='hr_task',
        )
        self.assertEqual(deleted, [])
        self.assertEqual(preserved, [])

    def test_returns_empty_lists_when_directory_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            deleted, preserved = cleanup_predecessor_spools(
                Path(tmp), task='hr_task',
            )
            self.assertEqual(deleted, [])
            self.assertEqual(preserved, [])

    def test_deletes_own_task_spool_from_prior_pid(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            prior = _make_identity(pid=1)
            fp, path = _open_plain_spool(
                directory, stage='neutral', identity=prior,
            )
            fp.close()
            deleted, preserved = cleanup_predecessor_spools(
                directory, task='hr_task',
            )
            self.assertEqual(deleted, [path])
            self.assertEqual(preserved, [])
            self.assertFalse(path.exists())

    def test_preserves_foreign_task_spool(self):
        # Teeth test: another task's runner might be holding a spool
        # in the shared directory. We must not touch it, even though
        # its filename matches our grammar.
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            theirs = _make_identity(task='ops_task')
            fp, path = _open_plain_spool(
                directory, stage='neutral', identity=theirs,
            )
            fp.close()
            deleted, preserved = cleanup_predecessor_spools(
                directory, task='hr_task',
            )
            self.assertEqual(deleted, [])
            self.assertEqual(preserved, [path])
            self.assertTrue(path.exists())

    def test_preserves_malformed_header_even_with_matching_filename(self):
        # Teeth test: a garbled file that just happens to share our
        # filename prefix. Header check catches it; must not be deleted.
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            ident = _make_identity()
            filename = f'task_core-copy-{ident.token}-neutral.spool'
            path = directory / filename
            path.write_bytes(b'not a valid header')
            deleted, preserved = cleanup_predecessor_spools(
                directory, task='hr_task',
            )
            self.assertEqual(deleted, [])
            self.assertEqual(preserved, [path])
            self.assertTrue(path.exists())

    def test_preserves_valid_header_under_mismatched_filename_token(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            original = _make_identity(pid=1)
            handle = open_spool_for_write(
                directory, stage='neutral', identity=original,
            )
            handle.stream.close()
            other = _make_identity(pid=2)
            renamed = directory / compose_spool_filename(
                token=other.token, stage='neutral',
            )
            handle.path.rename(renamed)

            deleted, preserved = cleanup_predecessor_spools(
                directory, task='hr_task',
            )
            self.assertEqual(deleted, [])
            self.assertEqual(preserved, [renamed])
            self.assertTrue(renamed.exists())

    def test_preserves_valid_header_under_mismatched_filename_stage(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            ident = _make_identity(pid=1)
            handle = open_spool_for_write(
                directory, stage='neutral', identity=ident,
            )
            handle.stream.close()
            renamed = directory / compose_spool_filename(
                token=ident.token, stage='copytext',
            )
            handle.path.rename(renamed)

            deleted, preserved = cleanup_predecessor_spools(
                directory, task='hr_task',
            )
            self.assertEqual(deleted, [])
            self.assertEqual(preserved, [renamed])
            self.assertTrue(renamed.exists())

    def test_preserves_non_spool_filenames(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            other = directory / 'not-a-spool.txt'
            other.write_bytes(b'contents')
            deleted, preserved = cleanup_predecessor_spools(
                directory, task='hr_task',
            )
            self.assertEqual(deleted, [])
            self.assertEqual(preserved, [other])
            self.assertTrue(other.exists())

    def test_preserves_subdirectories(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            sub = directory / 'nested'
            sub.mkdir()
            deleted, preserved = cleanup_predecessor_spools(
                directory, task='hr_task',
            )
            self.assertEqual(deleted, [])
            self.assertEqual(preserved, [sub])
            self.assertTrue(sub.is_dir())

    def test_mixed_directory_partitions_correctly(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            ours = _make_identity(pid=1)
            theirs = _make_identity(task='ops_task')
            fp_ours, path_ours = _open_plain_spool(
                directory, stage='neutral', identity=ours,
            )
            fp_ours.close()
            fp_theirs, path_theirs = _open_plain_spool(
                directory, stage='neutral', identity=theirs,
            )
            fp_theirs.close()
            other_file = directory / 'stray.txt'
            other_file.write_bytes(b'x')
            deleted, preserved = cleanup_predecessor_spools(
                directory, task='hr_task',
            )
            self.assertEqual(deleted, [path_ours])
            self.assertIn(path_theirs, preserved)
            self.assertIn(other_file, preserved)
            self.assertFalse(path_ours.exists())
            self.assertTrue(path_theirs.exists())
            self.assertTrue(other_file.exists())

    def test_owned_spool_that_cannot_be_deleted_is_fatal(self):
        """Known task data must not be silently left beside a new run.

        Before Phase 7, bounded unlink failure reclassified an owned spool
        as merely preserved. That made startup continue and allowed residue
        to accumulate after crashes.
        """
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            ident = _make_identity(pid=1)
            fp, path = _open_plain_spool(
                directory, stage='neutral', identity=ident,
            )
            fp.close()

            with patch.object(Path, 'unlink', side_effect=OSError('still busy')):
                with self.assertRaisesRegex(
                    DbPublishError,
                    'could not remove predecessor COPY spool',
                ):
                    cleanup_predecessor_spools(
                        directory, task='hr_task',
                    )
            self.assertTrue(path.exists())

    def test_non_directory_path_raises_publish_error(self):
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tmp_file = Path(tf.name)
        try:
            with self.assertRaises(DbPublishError):
                cleanup_predecessor_spools(tmp_file, task='hr_task')
        finally:
            tmp_file.unlink()

    def test_empty_task_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(DbPublishError):
                cleanup_predecessor_spools(Path(tmp), task='')


class Test17EncryptedSpoolContainer(unittest.TestCase):
    def test_prepare_encrypts_both_spool_stages_by_default(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            prepared = prepare_copy_source(
                row_source=[(1, 'secret-value')],
                columns=['id', 'name'],
                declared_schema=None,
                identity=ident,
                directory=Path(tmp),
            )
            self.assertEqual(prepared.protection, PROTECTION_AES256_GCM)
            raw = prepared.path.read_bytes()
            self.assertNotIn(b'secret-value', raw)
            with prepared.path.open('rb') as fp:
                header = read_spool_header(fp)
            self.assertEqual(header['protection'], PROTECTION_AES256_GCM)
            with prepared.open_reader() as reader:
                self.assertEqual(reader.read(), b'1\tsecret-value\n')

    def test_task_policy_can_disable_encryption_without_changing_reader_contract(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            prepared = prepare_copy_source(
                row_source=[(1, 'visible')],
                columns=['id', 'name'],
                declared_schema=None,
                identity=ident,
                directory=Path(tmp),
                policy=CopyLoadPolicy(encrypt_spools=False),
            )
            self.assertEqual(prepared.protection, PROTECTION_NONE)
            raw = prepared.path.read_bytes()
            self.assertIn(b'visible', raw)
            with prepared.open_reader() as reader:
                self.assertEqual(reader.read(), b'1\tvisible\n')

    def test_wrong_key_is_detected_at_authentication(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            prepared = prepare_copy_source(
                row_source=[(1,)], columns=['id'], declared_schema=None,
                identity=ident, directory=Path(tmp),
            )
            with self.assertRaisesRegex(SpoolFormatError, 'authentication failed'):
                with open_spool_for_read(
                    prepared.path,
                    identity=ident,
                    stage='copytext',
                    key=b'x' * 32,
                ) as reader:
                    reader.read()

    def test_ciphertext_corruption_is_detected(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            prepared = prepare_copy_source(
                row_source=[(1, 'secret')], columns=['id', 'name'],
                declared_schema=None, identity=ident, directory=Path(tmp),
            )
            with prepared.path.open('r+b') as fp:
                read_spool_header(fp)
                body_pos = fp.tell()
                byte = fp.read(1)
                self.assertTrue(byte)
                fp.seek(body_pos)
                fp.write(bytes([byte[0] ^ 0x01]))
            with self.assertRaisesRegex(SpoolFormatError, 'authentication failed'):
                with prepared.open_reader() as reader:
                    reader.read()

    def test_truncated_footer_is_rejected(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            prepared = prepare_copy_source(
                row_source=[(1,)], columns=['id'], declared_schema=None,
                identity=ident, directory=Path(tmp),
            )
            size = prepared.path.stat().st_size
            with prepared.path.open('r+b') as fp:
                fp.truncate(size - 5)
            with self.assertRaises(SpoolFormatError):
                prepared.open_reader()

    def test_fdopen_failure_reaps_the_created_path(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            path = directory / compose_spool_filename(
                token=ident.token, stage='neutral',
            )
            with patch('task_core.db_copy.os.fdopen', side_effect=RuntimeError('boom')):
                with self.assertRaisesRegex(RuntimeError, 'boom'):
                    open_spool_for_write(
                        directory, stage='neutral', identity=ident,
                    )
            self.assertFalse(path.exists())


# --- prepare_copy_source orchestrator ---------------------------------

def _read_copytext_body(prepared):
    """Return the decrypted/plain COPY-text body from a preparation result."""
    with prepared.open_reader() as f:
        return f.read()


def _list_spools(directory):
    """Return the sorted spool basenames in `directory`. Non-spool files
    (present in some tests) are filtered out so the assertion is on our
    files only."""
    return sorted(
        p.name for p in directory.iterdir()
        if parse_spool_filename(p.name) is not None
    )


class Test18PrepareCopySourceRoundTripsFromRowSourceToCopyTextSpool(unittest.TestCase):
    """The orchestrator drives pass 1 (neutral spool + inference) and
    pass 2 (copytext), reaps the neutral on success, and returns an immutable
    preparation result with the final path, resolved columns, exact count and
    bounded reader. Phase 6 hands this shape to the database transport, so
    the round-trip contract is pinned here independently of PostgreSQL.
    """

    def test_success_path_returns_copytext_path_and_resolved_columns(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            rows = [(1, 'alice', True), (2, 'bob', False), (3, None, True)]
            prepared = prepare_copy_source(
                row_source=iter(rows),
                columns=['id', 'name', 'active'],
                declared_schema=None,
                identity=ident,
                directory=d,
            )
            self.assertTrue(prepared.path.exists())
            self.assertEqual(prepared.path.name, compose_spool_filename(
                token=ident.token, stage='copytext',
            ))
            self.assertEqual(
                [c.name for c in prepared.columns], ['id', 'name', 'active'],
            )
            # Inference default for a column with no not-null constraint
            # is nullable=True (ADR 0011 -- prepare_copy_source has no
            # not_null concept; the higher-level publisher owns that).
            self.assertTrue(all(c.nullable for c in prepared.columns))

    def test_neutral_spool_is_reaped_on_success(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            prepared = prepare_copy_source(
                row_source=[('x',)],
                columns=['name'],
                declared_schema=None,
                identity=ident,
                directory=d,
            )
            remaining = _list_spools(d)
            self.assertEqual(remaining, [prepared.path.name])
            self.assertNotIn(compose_spool_filename(
                token=ident.token, stage='neutral',
            ), remaining)

    def test_copytext_bytes_match_serializer_output(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            rows = [(1, 'alice'), (2, 'bob\twith\ttabs')]
            prepared = prepare_copy_source(
                row_source=rows,
                columns=['id', 'name'],
                declared_schema=None,
                identity=ident,
                directory=d,
            )
            body = _read_copytext_body(prepared)
            expected = (
                serialize_row_to_copytext(
                    {'id': 1, 'name': 'alice'}, prepared.columns,
                    ident.target_table, 1,
                )
                + serialize_row_to_copytext(
                    {'id': 2, 'name': 'bob\twith\ttabs'}, prepared.columns,
                    ident.target_table, 2,
                )
            )
            self.assertEqual(body, expected)

    def test_empty_row_source_produces_header_only_copytext(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            prepared = prepare_copy_source(
                row_source=iter(()),
                columns=['id'],
                declared_schema=None,
                identity=ident,
                directory=d,
            )
            self.assertTrue(prepared.path.exists())
            body = _read_copytext_body(prepared)
            self.assertEqual(body, b'')
            # With no rows, inference sees nothing and resolves to Text
            # per _resolve_families(empty_set) -> Text. Locking that in
            # here so a future accumulator change surfaces at the
            # orchestrator boundary too.
            self.assertEqual([c.name for c in prepared.columns], ['id'])


class Test19PrepareCopySourceUsesDeclaredSchemaWhenProvided(unittest.TestCase):
    """A declared schema replaces inference and enables direct one-pass
    validation into the final COPY-text spool. The result retains the exact
    declared columns and wire order.
    """

    def test_declared_schema_is_returned_verbatim(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            declared = (
                ResolvedColumn('id', sa.Integer(), False),
                ResolvedColumn('name', sa.Text(), True),
            )
            prepared = prepare_copy_source(
                row_source=[(1, 'alice')],
                columns=['id', 'name'],
                declared_schema=declared,
                identity=ident,
                directory=d,
            )
            self.assertEqual(prepared.columns, declared)

    def test_value_type_mismatch_against_declared_schema_raises(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            declared = (ResolvedColumn('n', sa.Integer(), False),)
            with self.assertRaises(DbPublishError):
                prepare_copy_source(
                    row_source=[('not an int',)],
                    columns=['n'],
                    declared_schema=declared,
                    identity=ident,
                    directory=d,
                )
            # The partially written final spool is reaped on validation failure.
            self.assertEqual(_list_spools(d), [])

    def test_null_in_non_nullable_declared_column_raises(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            declared = (ResolvedColumn('n', sa.Integer(), False),)
            with self.assertRaises(DbPublishError):
                prepare_copy_source(
                    row_source=[(None,)],
                    columns=['n'],
                    declared_schema=declared,
                    identity=ident,
                    directory=d,
                )
            self.assertEqual(_list_spools(d), [])


class Test19bDeclaredPreparationIsOnePassAndSerializationIsCompiled(unittest.TestCase):
    """Performance fixes must remove work rather than only preserve output.

    The declared path has all target types before traversal, so opening a
    neutral spool is unnecessary. Both declared and inferred preparation use
    the positional compiled serializer instead of rebuilding a row mapping
    and rediscovering scalar families for every output row.
    """

    def test_declared_path_opens_only_the_final_copytext_spool(self):
        ident = _make_identity()
        declared = (
            ResolvedColumn('name', sa.Text(), False),
            ResolvedColumn('id', sa.Integer(), False),
        )
        stages = []
        original = db_copy_module.open_spool_for_write

        def recording_open(*args, **kwargs):
            stages.append(kwargs['stage'])
            return original(*args, **kwargs)

        with tempfile.TemporaryDirectory() as tmp:
            with patch(
                'task_core.db_copy.open_spool_for_write',
                side_effect=recording_open,
            ):
                prepared = prepare_copy_source(
                    row_source=[(1, 'alice'), (2, 'bob')],
                    columns=['id', 'name'],
                    declared_schema=declared,
                    identity=ident,
                    directory=Path(tmp),
                )
            self.assertEqual(
                stages,
                ['copytext'],
                'declared COPY must not create a type-neutral predecessor spool',
            )
            self.assertEqual(
                _read_copytext_body(prepared),
                b'alice\t1\nbob\t2\n',
            )

    def test_inferred_path_does_not_call_mapping_serializer_per_row(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            with patch(
                'task_core.db_copy.serialize_row_to_copytext',
                side_effect=AssertionError(
                    'prepare_copy_source rebuilt a mapping serializer per row'
                ),
            ):
                prepared = prepare_copy_source(
                    row_source=[(1, 'alice'), (2, 'bob')],
                    columns=['id', 'name'],
                    declared_schema=None,
                    identity=ident,
                    directory=Path(tmp),
                )
            self.assertEqual(
                _read_copytext_body(prepared),
                b'1\talice\n2\tbob\n',
            )


class Test19cDeclaredDirectSerializerHotPath(unittest.TestCase):
    """Declared COPY must fuse normalization, validation, and encoding.

    The 0.6.9 one-pass path still sent every ordinary Python cell through
    `_normalize_value()`, the generic declared validator, and the generic
    family serializer. A one-million-row, five-column profile measured five
    million calls through each layer. These tests protect the direct compiled
    path and its scalar-wrapper fallback separately.
    """

    def test_native_values_bypass_generic_value_pipeline(self):
        ident = _make_identity()
        declared = (
            ResolvedColumn('flag', sa.Boolean(), False),
            ResolvedColumn('small', sa.SmallInteger(), False),
            ResolvedColumn('integer', sa.Integer(), False),
            ResolvedColumn('big', sa.BigInteger(), False),
            ResolvedColumn('amount', sa.Numeric(10, 2), False),
            ResolvedColumn('ratio', sa.Float(), False),
            ResolvedColumn('label', sa.String(20), False),
            ResolvedColumn('payload', sa.LargeBinary(), False),
            ResolvedColumn('day', sa.Date(), False),
            ResolvedColumn('local_at', sa.DateTime(), False),
            ResolvedColumn('utc_at', sa.DateTime(timezone=True), False),
        )
        row = (
            True,
            -7,
            8,
            9,
            Decimal('12.30'),
            3.5,
            'x\ty',
            b'\x00\xff',
            date(2026, 8, 2),
            datetime(2026, 8, 2, 10, 11, 12),
            datetime(2026, 8, 2, 10, 11, 12, tzinfo=timezone.utc),
        )
        with tempfile.TemporaryDirectory() as tmp:
            with (
                patch(
                    'task_core.db_copy._normalize_value',
                    side_effect=AssertionError(
                        'native declared COPY called generic normalization'
                    ),
                ),
                patch(
                    'task_core.db_copy._validate_declared_value_family',
                    side_effect=AssertionError(
                        'native declared COPY called generic validation'
                    ),
                    create=True,
                ),
                patch(
                    'task_core.db_copy._serialize_value_copytext_family',
                    side_effect=AssertionError(
                        'native declared COPY called generic serialization'
                    ),
                ),
            ):
                prepared = prepare_copy_source(
                    row_source=[row],
                    columns=[column.name for column in declared],
                    declared_schema=declared,
                    identity=ident,
                    directory=Path(tmp),
                )
            self.assertEqual(
                _read_copytext_body(prepared),
                (
                    b't\t-7\t8\t9\t12.30\t3.5\tx\\ty\t'
                    b'\\\\x00ff\t2026-08-02\t2026-08-02 10:11:12\t'
                    b'2026-08-02 10:11:12+00:00\n'
                ),
            )

    def test_non_native_scalars_use_exact_normalization_fallback(self):
        import numpy as np
        import pandas as pd

        ident = _make_identity()
        declared = (
            ResolvedColumn('id', sa.BigInteger(), False),
            ResolvedColumn('flag', sa.Boolean(), False),
            ResolvedColumn('at', sa.DateTime(), False),
            ResolvedColumn('missing', sa.Text(), True),
        )
        original = db_copy_module._normalize_value
        with tempfile.TemporaryDirectory() as tmp:
            with patch(
                'task_core.db_copy._normalize_value',
                wraps=original,
            ) as normalize:
                prepared = prepare_copy_source(
                    row_source=[(
                        np.int64(7),
                        np.bool_(True),
                        pd.Timestamp('2026-08-02 10:11:12'),
                        pd.NA,
                    )],
                    columns=[column.name for column in declared],
                    declared_schema=declared,
                    identity=ident,
                    directory=Path(tmp),
                )
            self.assertEqual(normalize.call_count, 4)
            self.assertEqual(
                _read_copytext_body(prepared),
                b'7\tt\t2026-08-02 10:11:12\t\\N\n',
            )

    def test_native_nan_markers_keep_declared_null_semantics(self):
        ident = _make_identity()
        declared = (
            ResolvedColumn('ratio', sa.Float(), True),
            ResolvedColumn('amount', sa.Numeric(), True),
        )
        with tempfile.TemporaryDirectory() as tmp:
            with patch(
                'task_core.db_copy._normalize_value',
                side_effect=AssertionError(
                    'native NaN markers called generic normalization'
                ),
            ):
                prepared = prepare_copy_source(
                    row_source=[(float('nan'), Decimal('NaN'))],
                    columns=['ratio', 'amount'],
                    declared_schema=declared,
                    identity=ident,
                    directory=Path(tmp),
                )
            self.assertEqual(_read_copytext_body(prepared), b'\\N\t\\N\n')


class Test20PrepareCopySourceRejectsSchemaAndInputMismatch(unittest.TestCase):
    """Configuration errors are caught at the orchestrator boundary --
    before any file is created where possible, and before pass 2 in the
    two cases where they must reach mid-pipeline (row width, invalid
    scalar type in pass 1).
    """

    def test_declared_schema_name_mismatch_raises(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            declared = (
                ResolvedColumn('id', sa.Integer(), False),
                ResolvedColumn('other', sa.Text(), True),
            )
            with self.assertRaises(DbPublishError) as cm:
                prepare_copy_source(
                    row_source=[(1, 'alice')],
                    columns=['id', 'name'],
                    declared_schema=declared,
                    identity=ident,
                    directory=d,
                )
            self.assertIn('do not match', str(cm.exception))
            # Rejected before pass 1: no spool files touched.
            self.assertEqual(list(d.iterdir()), [])

    def test_declared_schema_reorders_wire_columns(self):
        # Declared INSERT permits source order to differ from declaration
        # order. COPY must emit fields in declared order too.
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            declared = (
                ResolvedColumn('name', sa.Text(), True),
                ResolvedColumn('id', sa.Integer(), False),
            )
            prepared = prepare_copy_source(
                row_source=[(1, 'alice')],
                columns=['id', 'name'],
                declared_schema=declared,
                identity=ident,
                directory=d,
            )
            self.assertEqual(prepared.columns, declared)
            self.assertEqual(_read_copytext_body(prepared), b'alice\t1\n')

    def test_row_width_mismatch_raises_and_reaps(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            with self.assertRaises(DbPublishError):
                prepare_copy_source(
                    row_source=[(1, 'alice', 'extra')],
                    columns=['id', 'name'],
                    declared_schema=None,
                    identity=ident,
                    directory=d,
                )
            self.assertEqual(_list_spools(d), [])

    def test_empty_columns_rejected(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            with self.assertRaises(DbPublishError):
                prepare_copy_source(
                    row_source=[],
                    columns=[],
                    declared_schema=None,
                    identity=ident,
                    directory=d,
                )
            self.assertEqual(list(d.iterdir()), [])


class Test20bPrepareCopySourceParityCorrections(unittest.TestCase):
    def test_normalizes_numpy_and_pandas_scalars_once(self):
        import numpy as np
        import pandas as pd

        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            prepared = prepare_copy_source(
                row_source=[(np.int64(7), pd.NA, np.nan)],
                columns=['id', 'missing_a', 'missing_b'],
                declared_schema=None,
                identity=ident,
                directory=Path(tmp),
            )
            self.assertEqual(prepared.row_count, 1)
            self.assertEqual(_read_copytext_body(prepared), b'7\t\\N\t\\N\n')

    def test_inferred_numeric_widening_accepts_float_and_decimal(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            prepared = prepare_copy_source(
                row_source=[(1,), (3.5,), (Decimal('2.30'),)],
                columns=['amount'],
                declared_schema=None,
                identity=ident,
                directory=Path(tmp),
            )
            self.assertIsInstance(prepared.columns[0].type, sa.Numeric)
            self.assertEqual(_read_copytext_body(prepared), b'1\n3.5\n2.30\n')

    def test_inferred_date_datetime_widening_accepts_both(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            prepared = prepare_copy_source(
                row_source=[(date(2024, 1, 1),), (datetime(2024, 1, 2, 3, 4),)],
                columns=['at'],
                declared_schema=None,
                identity=ident,
                directory=Path(tmp),
            )
            self.assertIsInstance(prepared.columns[0].type, sa.DateTime)
            self.assertEqual(
                _read_copytext_body(prepared),
                b'2024-01-01\n2024-01-02 03:04:00\n',
            )

    def test_inferred_aware_datetime_resolves_to_timestamptz(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            prepared = prepare_copy_source(
                row_source=[
                    (datetime(2026, 8, 2, 10, 11, 12, tzinfo=timezone.utc),),
                    (
                        datetime(
                            2026, 8, 2, 15, 41, 12,
                            tzinfo=timezone(timedelta(hours=5, minutes=30)),
                        ),
                    ),
                ],
                columns=['at'],
                declared_schema=None,
                identity=ident,
                directory=Path(tmp),
            )
            self.assertIsInstance(prepared.columns[0].type, sa.DateTime)
            self.assertTrue(
                prepared.columns[0].type.timezone,
                'aware COPY inference must create TIMESTAMPTZ',
            )
            self.assertEqual(
                _read_copytext_body(prepared),
                (
                    b'2026-08-02 10:11:12+00:00\n'
                    b'2026-08-02 15:41:12+05:30\n'
                ),
            )

    def test_inferred_mixed_datetime_awareness_rejects_and_reaps(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            with self.assertRaisesRegex(
                DbPublishError,
                'timezone-aware.*naive',
            ):
                prepare_copy_source(
                    row_source=[
                        (datetime(2026, 8, 2, 10, 11, 12),),
                        (
                            datetime(
                                2026, 8, 2, 10, 11, 12,
                                tzinfo=timezone.utc,
                            ),
                        ),
                    ],
                    columns=['at'],
                    declared_schema=None,
                    identity=ident,
                    directory=directory,
                )
            self.assertEqual(_list_spools(directory), [])

    def test_type_override_and_not_null_are_applied(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            prepared = prepare_copy_source(
                row_source=[(1,)],
                columns=['value'],
                declared_schema=None,
                identity=ident,
                directory=Path(tmp),
                type_overrides={'value': 'TEXT'},
                not_null_columns=('value',),
            )
            self.assertIsInstance(prepared.columns[0].type, sa.Text)
            self.assertFalse(prepared.columns[0].nullable)
            self.assertEqual(_read_copytext_body(prepared), b'1\n')

    def test_not_null_rejects_normalized_missing_value(self):
        import pandas as pd

        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(DbPublishError, 'non-nullable'):
                prepare_copy_source(
                    row_source=[(pd.NA,)],
                    columns=['value'],
                    declared_schema=None,
                    identity=ident,
                    directory=Path(tmp),
                    not_null_columns=('value',),
                )

    def test_prepared_result_carries_exact_count_and_size(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            prepared = prepare_copy_source(
                row_source=[(1,), (2,)],
                columns=['id'],
                declared_schema=None,
                identity=ident,
                directory=Path(tmp),
            )
            self.assertEqual(prepared.row_count, 2)
            self.assertEqual(prepared.spool_bytes, prepared.path.stat().st_size)


class Test21PrepareCopySourceReapsSpoolsOnEveryFailurePath(unittest.TestCase):
    """The `finally`-like cleanup path is the invariant Phase 5.h leans
    on: when unlink succeeds, prepare_copy_source() removes its current-run
    spools before propagating an error. Both the mid-pass-1 (source raises)
    and mid-pass-2 (validation raises) cases must satisfy it. Separate fault
    injection covers the residual-path behavior when unlink itself fails.

    Teeth for both: remove the cleanup, confirm files survive; restore.
    Recorded here rather than left as an inline comment because the
    revert-observe-restore ritual is what CLAUDE.md requires for every
    invariant we depend on.
    """

    def test_row_source_exception_reaps_neutral_spool(self):
        ident = _make_identity()

        def bad_source():
            yield (1, 'alice')
            raise RuntimeError('source blew up mid-stream')

        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            with self.assertRaises(RuntimeError):
                prepare_copy_source(
                    row_source=bad_source(),
                    columns=['id', 'name'],
                    declared_schema=None,
                    identity=ident,
                    directory=d,
                )
            # Neutral spool was in the process of being written. It must
            # be gone -- otherwise a future run's predecessor cleanup is
            # the only reaper, and this run's failure path has silently
            # relied on it.
            self.assertEqual(_list_spools(d), [])

    def test_declared_value_validation_reaps_the_final_spool(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            declared = (ResolvedColumn('n', sa.Integer(), False),)
            with self.assertRaises(DbPublishError):
                prepare_copy_source(
                    row_source=[('not an int',)],
                    columns=['n'],
                    declared_schema=declared,
                    identity=ident,
                    directory=d,
                )
            # Declared mode writes only the final spool; it must be reaped
            # when the first row fails validation.
            self.assertEqual(_list_spools(d), [])

    def test_unsupported_scalar_type_in_pass_1_reaps_neutral_spool(self):
        # _write_value rejects e.g. `object()` outright. That path
        # unwinds through pass 1 with the neutral spool open. Verifies
        # the "pass 1 failure" branch independently of the row-source
        # exception path above.
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            with self.assertRaises(DbPublishError):
                prepare_copy_source(
                    row_source=[(object(),)],
                    columns=['x'],
                    declared_schema=None,
                    identity=ident,
                    directory=d,
                )
            self.assertEqual(_list_spools(d), [])


class Test22PrepareCopySourceValidatesInputsBeforeAnyFileIsCreated(unittest.TestCase):
    """Input validation is at the orchestrator boundary and must never
    leave a spool file. Anything that fails these guards is a caller
    bug, not a run failure -- distinguishing the two is what lets
    `finally: cleanup_spool_paths(...)` in the runner stay tight.
    """

    def test_identity_type_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            with self.assertRaises(DbPublishError):
                prepare_copy_source(
                    row_source=[(1,)],
                    columns=['id'],
                    declared_schema=None,
                    identity='not an identity',  # type: ignore[arg-type]
                    directory=d,
                )
            self.assertEqual(list(d.iterdir()), [])

    def test_directory_type_rejected(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(DbPublishError):
                prepare_copy_source(
                    row_source=[(1,)],
                    columns=['id'],
                    declared_schema=None,
                    identity=ident,
                    directory=tmp,  # str not Path; type: ignore[arg-type]
                )

    def test_columns_must_be_sequence_of_nonempty_str(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            with self.assertRaises(DbPublishError):
                prepare_copy_source(
                    row_source=[(1,)],
                    columns='id',  # string is not a valid column sequence
                    declared_schema=None,
                    identity=ident,
                    directory=d,
                )
            with self.assertRaises(DbPublishError):
                prepare_copy_source(
                    row_source=[(1,)],
                    columns=[''],
                    declared_schema=None,
                    identity=ident,
                    directory=d,
                )
            self.assertEqual(list(d.iterdir()), [])

    def test_declared_schema_entry_type_rejected(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            with self.assertRaises(DbPublishError):
                prepare_copy_source(
                    row_source=[(1,)],
                    columns=['id'],
                    declared_schema=[{'name': 'id'}],  # not a ResolvedColumn
                    identity=ident,
                    directory=d,
                )
            self.assertEqual(list(d.iterdir()), [])

    def test_policy_type_rejected(self):
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            with self.assertRaises(DbPublishError):
                prepare_copy_source(
                    row_source=[(1,)],
                    columns=['id'],
                    declared_schema=None,
                    identity=ident,
                    directory=d,
                    policy='not a policy',  # type: ignore[arg-type]
                )

    def test_default_policy_is_used_when_omitted(self):
        # Sanity: omitting policy should not raise; the default is
        # CopyLoadPolicy() with buffer_bytes=1_048_576.
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            prepared = prepare_copy_source(
                row_source=[(1,)],
                columns=['id'],
                declared_schema=None,
                identity=ident,
                directory=d,
            )
            self.assertTrue(prepared.path.exists())

    def test_duplicate_column_names_are_rejected_before_any_file_is_created(self):
        # Compiled positional lookup requires one unambiguous source index
        # per name. RowProjection blocks duplicates upstream --
        # this guard is defensive symmetry at the orchestrator boundary.
        ident = _make_identity()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            with self.assertRaises(DbPublishError) as ctx:
                prepare_copy_source(
                    row_source=[(1, 'a', 'b')],
                    columns=['id', 'name', 'name'],
                    declared_schema=None,
                    identity=ident,
                    directory=d,
                )
            self.assertIn('duplicate', str(ctx.exception).lower())
            self.assertEqual(
                list(d.iterdir()), [],
                'duplicate-name rejection must happen before any spool is created',
            )


class Test24CloseAndWriteFailureSemantics(unittest.TestCase):
    """Low-level spool I/O must preserve the primary failure."""

    class _CloseFails:
        def close(self):
            raise OSError('close failed')

    class _ShortWriter:
        def __init__(self):
            self.data = bytearray()

        def write(self, data):
            chunk = bytes(data[:1])
            self.data.extend(chunk)
            return len(chunk)

    def test_close_failure_surfaces_on_success_path(self):
        stream = self._CloseFails()
        with self.assertRaisesRegex(OSError, 'close failed'):
            with db_copy_module._close_preserving_primary(
                stream, description='test spool',
            ):
                pass

    def test_close_failure_does_not_replace_primary_failure(self):
        stream = self._CloseFails()
        with self.assertLogs('task_core.db_copy', level='ERROR') as logs:
            with self.assertRaisesRegex(RuntimeError, 'primary failed'):
                with db_copy_module._close_preserving_primary(
                    stream, description='test spool',
                ):
                    raise RuntimeError('primary failed')
        self.assertIn('secondary error while closing test spool', '\n'.join(logs.output))

    def test_write_all_retries_short_writes(self):
        writer = self._ShortWriter()
        db_copy_module._write_all(writer, b'abcdef')
        self.assertEqual(bytes(writer.data), b'abcdef')

    def test_write_all_rejects_zero_progress(self):
        class ZeroWriter:
            def write(self, data):
                return 0

        with self.assertRaisesRegex(OSError, 'short write'):
            db_copy_module._write_all(ZeroWriter(), b'x')


if __name__ == '__main__':
    unittest.main()


class Test19DbapiCopyTransport(unittest.TestCase):
    class _Cursor:
        def __init__(self, owner, *, error=None, close_error=None):
            self.owner = owner
            self.error = error
            self.close_error = close_error
            self.closed = False

        def copy_expert(self, sql, reader, size):
            self.owner.copy_sql = sql
            self.owner.copy_size = size
            if self.error is not None:
                raise self.error
            self.owner.copy_body = reader.read()

        def close(self):
            self.closed = True
            if self.close_error is not None:
                raise self.close_error

    class _Raw:
        def __init__(self, *, error=None, close_error=None):
            self.error = error
            self.close_error = close_error
            self.closed = 0
            self.cursor_instance = None
            self.copy_sql = None
            self.copy_size = None
            self.copy_body = None

        def cursor(self):
            self.cursor_instance = Test19DbapiCopyTransport._Cursor(
                self, error=self.error, close_error=self.close_error,
            )
            return self.cursor_instance

    class _Proxy:
        def __init__(self, raw):
            self.driver_connection = raw

    class _Conn:
        def __init__(
            self, raw, *, postgres=True, driver='psycopg2', disconnect=False,
        ):
            from sqlalchemy.dialects.postgresql.base import PGDialect
            dialect = PGDialect()
            dialect.name = 'postgresql' if postgres else 'sqlite'
            dialect.driver = driver
            dialect.is_disconnect = lambda exc, connection, cursor: disconnect
            self.dialect = dialect
            self.connection = Test19DbapiCopyTransport._Proxy(raw)
            self.invalidate_calls = []

        def invalidate(self, exc=None):
            self.invalidate_calls.append(exc)

    def _prepared(self, directory, *, encrypt=True):
        identity = SpoolIdentity(
            task='copy_transport', target_schema='bsr', target_table='target',
            run_start_utc=_RUN_START, pid=123,
        )
        return prepare_copy_source(
            row_source=[(1, 'x'), (2, None)],
            columns=('id', 'name'),
            declared_schema=(
                ResolvedColumn('id', sa.BigInteger(), nullable=False),
                ResolvedColumn('name', sa.Text(), nullable=True),
            ),
            identity=identity,
            directory=Path(directory),
            policy=CopyLoadPolicy(
                spool_directory=Path(directory),
                buffer_bytes=4096,
                encrypt_spools=encrypt,
            ),
        )

    def _table(self):
        return sa.Table(
            'stage_table', sa.MetaData(),
            sa.Column('id', sa.BigInteger(), nullable=False),
            sa.Column('name', sa.Text()),
            schema='bsr',
        )

    def test_streams_authenticated_reader_through_existing_connection(self):
        with tempfile.TemporaryDirectory() as tmp:
            prepared = self._prepared(tmp, encrypt=True)
            raw = self._Raw()
            conn = self._Conn(raw)
            try:
                loaded = load_copy_into_staging(
                    conn, self._table(), prepared, 999,
                )
                self.assertEqual(loaded, 2)
                self.assertEqual(raw.copy_body, b'1\tx\n2\t\\N\n')
                self.assertEqual(raw.copy_size, 4096)
                self.assertIn('COPY bsr.stage_table (id, name) FROM STDIN', raw.copy_sql)
                self.assertIn("DELIMITER E'\\t'", raw.copy_sql)
                self.assertIn("NULL E'\\\\N'", raw.copy_sql)
                self.assertIn("ENCODING 'UTF8'", raw.copy_sql)
                self.assertTrue(raw.cursor_instance.closed)
                self.assertEqual(conn.invalidate_calls, [])
            finally:
                cleanup_spool_paths([prepared.path])

    def test_rejects_non_postgresql_or_non_psycopg2_connection(self):
        with tempfile.TemporaryDirectory() as tmp:
            prepared = self._prepared(tmp, encrypt=False)
            try:
                with self.assertRaisesRegex(DbPublishError, 'PostgreSQL'):
                    load_copy_into_staging(
                        self._Conn(self._Raw(), postgres=False),
                        self._table(), prepared,
                    )
                with self.assertRaisesRegex(DbPublishError, 'psycopg2'):
                    load_copy_into_staging(
                        self._Conn(self._Raw(), driver='psycopg'),
                        self._table(), prepared,
                    )
            finally:
                cleanup_spool_paths([prepared.path])

    def test_closed_driver_connection_invalidates_sqlalchemy_connection(self):
        with tempfile.TemporaryDirectory() as tmp:
            prepared = self._prepared(tmp, encrypt=True)
            failure = RuntimeError('connection terminated')
            raw = self._Raw(error=failure)
            raw.closed = 2
            conn = self._Conn(raw)
            try:
                with self.assertRaisesRegex(RuntimeError, 'terminated'):
                    load_copy_into_staging(conn, self._table(), prepared)
                self.assertEqual(conn.invalidate_calls, [failure])
                self.assertTrue(raw.cursor_instance.closed)
            finally:
                cleanup_spool_paths([prepared.path])

    def test_dialect_disconnect_detection_invalidates_when_closed_flag_is_clear(self):
        with tempfile.TemporaryDirectory() as tmp:
            prepared = self._prepared(tmp, encrypt=True)
            failure = RuntimeError('server closed the connection unexpectedly')
            raw = self._Raw(error=failure)
            self.assertEqual(raw.closed, 0)
            conn = self._Conn(raw, disconnect=True)
            try:
                with self.assertRaisesRegex(RuntimeError, 'unexpectedly'):
                    load_copy_into_staging(conn, self._table(), prepared)
                self.assertEqual(conn.invalidate_calls, [failure])
            finally:
                cleanup_spool_paths([prepared.path])

    def test_cursor_close_failure_does_not_replace_copy_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            prepared = self._prepared(tmp, encrypt=False)
            primary = RuntimeError('copy failed')
            raw = self._Raw(error=primary, close_error=OSError('close failed'))
            conn = self._Conn(raw)
            try:
                with self.assertRaisesRegex(RuntimeError, 'copy failed'):
                    load_copy_into_staging(conn, self._table(), prepared)
            finally:
                cleanup_spool_paths([prepared.path])
