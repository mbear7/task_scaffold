"""Stateless value and schema kernel for db publication.

Everything here is independent of the connection, transaction, and
publication policy that live in publish.py -- exception classes,
value normalization, declared/inferred schema resolution, and the
family classification used by inference. Public publication exceptions
are imported by publish.py; private kernel helpers remain owned here.

Import direction is one way: publish depends on values. Nothing
here imports publish, insert or copy -- keeping that arrow
straight is the whole reason the split exists.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from itertools import islice
from typing import Any

import pandas as pd
import sqlalchemy as sa

from task_core.types import OutputColumn, find_duplicates


class DbPublishError(RuntimeError):
    pass


class DbPublishInvariantError(DbPublishError):
    """An internal guarantee of this module failed -- not something a task
    author did wrong, and not something they can fix. Raised only where
    the code is supposed to have made a failure impossible by
    construction: a generated staging identifier over the byte limit, or
    two generated names colliding after preflight already proved they
    could not.

    Subclasses DbPublishError deliberately, so existing cleanup paths that
    catch DbPublishError still catch it, while isinstance() still tells the
    two apart. The distinction is for diagnostics: the message says the
    invariant was violated rather than implying a bad declaration.
    """


_TYPE_OVERRIDES = {
    'TEXT': sa.Text,
    'STRING': sa.Text,
    'VARCHAR': sa.Text,
    'CHAR': sa.Text,
    'SMALLINT': sa.SmallInteger,
    'INT2': sa.SmallInteger,
    'BIGINT': sa.BigInteger,
    'INT8': sa.BigInteger,
    'INTEGER': sa.Integer,
    'INT4': sa.Integer,
    'INT': sa.Integer,
    'NUMERIC': sa.Numeric,
    'DECIMAL': sa.Numeric,
    'FLOAT': sa.Float,
    'REAL': sa.REAL,
    'FLOAT4': sa.REAL,
    'DOUBLE': sa.Double,
    'DOUBLE PRECISION': sa.Double,
    'FLOAT8': sa.Double,
    'BOOLEAN': sa.Boolean,
    'BOOL': sa.Boolean,
    'DATE': sa.Date,
    'TIMESTAMP': sa.DateTime,
    'DATETIME': sa.DateTime,
    'TIMESTAMPTZ': lambda: sa.DateTime(timezone=True),
}


_INTEGER_RANGES = {
    sa.SmallInteger: (-(2 ** 15), 2 ** 15 - 1),
    sa.Integer: (-(2 ** 31), 2 ** 31 - 1),
    sa.BigInteger: (-(2 ** 63), 2 ** 63 - 1),
}


# The two inferred types whose unsampled compatibility can be checked by one
# exact Python type. PostgreSQL silently accepts the wider value instead of
# rejecting it -- confirmed directly against a real PostgreSQL instance by
# the project owner, not assumed from the documentation:
#
#   create temp table t (v bigint);
#   insert into t values (3.5);      -- succeeds, stores 4 (assignment
#                                    -- cast rounds; NO error)
#   create temp table d (v date);
#   insert into d values (timestamp '2024-01-01 13:30');
#                                    -- succeeds, stores 2024-01-01
#                                    -- (the time is silently dropped)
#
# Timestamp awareness is another silent semantic boundary, but it cannot be
# represented by one exact Python type because both aware and naive values are
# datetime instances. `_infer_column_type()` verifies that branch separately.
# Other narrowings fail loudly at insert time ('N/A' or True into bigint both
# error), so they do not need a remainder sweep.
#
# The paired Python type is the EXACT type a value must have to be
# consistent with the inferred column type -- `type(value) is int`, not
# isinstance(). Exactness matters in both directions here: bool is a
# subclass of int (so isinstance would wave True through into a bigint
# column, which PostgreSQL then rejects loudly anyway), and datetime is a
# subclass of date (so isinstance would wave a datetime through into a
# date column, which is precisely the silent truncation above).
_SILENTLY_WIDENABLE = (
    (sa.BigInteger, int),
    (sa.Date, date),
)


@dataclass(frozen=True)
class ResolvedColumn:
    name: str
    type: sa.types.TypeEngine
    nullable: bool = True


@dataclass(frozen=True)
class ResolvedSchema:
    columns: tuple[ResolvedColumn, ...]
    source: str


def _is_scalar_like(value):
    """True for a genuine scalar, and also for a zero-dimensional numpy
    array, which wraps exactly one scalar and has no container semantics
    to preserve.

    `pd.api.types.is_scalar()` alone says False for `np.array(5)` -- it is
    an ndarray, so it is not a scalar -- which is correct as far as it
    goes and wrong for what both callers below actually need to know.
    Confirmed directly: `np.array(5)` reached the DB driver as an array
    rather than the int 5, while `np.array([5])` is correctly preserved as
    a real, one-element container. `.ndim == 0` is the exact line between
    those two, and it is the same line for both callers, which is why this
    is one shared predicate rather than a check duplicated in each.

    This also closes an asymmetry that predates the zero-dimensional
    question and is easy to miss, confirmed directly across every dtype
    tried: `pd.isna()` returns a plain numpy bool for a *typed* zero-dim
    array (float64, datetime64) but a zero-dim *array* for an object one.
    So `np.array(np.nan)` already normalized to None, while
    `np.array(pd.NaT)`, `np.array(None)` and `np.array(pd.NA)` did not --
    four values that all hold nothing, behaving two different ways
    depending only on the dtype numpy happened to pick. Applying this
    predicate to `pd.isna()`'s own result in is_missing() makes all four
    agree.

    numpy's own scalar types (np.int64, np.float64, ...) also report
    `.ndim == 0`, but they are already `is_scalar()`, so the first branch
    short-circuits and this changes nothing for them -- confirmed
    directly, not assumed.
    """
    if pd.api.types.is_scalar(value):
        return True
    return getattr(value, 'ndim', None) == 0


def is_missing(value):
    """True if value is a missing-value marker (NaN, pd.NA, pd.NaT, or
    any other value pd.isna() recognizes) that should normalize to None.
    Shared between this module's own _normalize_value() and
    table_adapters.py's normalize_for_excel() -- found independently
    broken in both, from the same underlying cause: a bare
    `value != value` check, which correctly detects a plain NaN but
    genuinely raises TypeError for pd.NA specifically ("boolean value
    of NA is ambiguous"), confirmed directly, not assumed. Both callers
    wrapped that check in a broad except Exception: pass, which silently
    swallowed the TypeError and let a raw pd.NA fall through unconverted
    instead of becoming None -- a real, live gap, not just an inaccurate
    comment, confirmed directly against both functions in isolation
    before this fix (each returned pd.NA itself, untouched).

    pd.isna() isn't a fully safe drop-in on its own, either: for a
    multi-element list/array-like value it returns an array rather than
    a scalar, which itself raises inside a plain `if`/bool() for
    anything but a single-element array. Still safely handled here by
    the same broad except Exception both callers already had -- if
    pd.isna()'s result can't be evaluated as a single, unambiguous truth
    value, this treats the original value as not missing, the same
    pre-existing fallback behavior either caller already had for any
    value pd.isna() itself doesn't like. Verified directly against
    every case this needs to distinguish: pd.NA, np.nan, None, pd.NaT,
    ordinary scalars, and multi-/empty-element lists.

    That verification was itself incomplete, found by a further review:
    a ONE-element container (list, array, Index) makes pd.isna() return
    a one-element array, and unlike a multi-element array, bool() on a
    single-element array succeeds rather than raising -- so the
    except Exception: pass fallback above never triggered for exactly
    this shape, and is_missing([None]) genuinely, silently returned
    True, corrupting a real, non-missing container value (a list
    containing one None) into a bare None. pd.api.types.is_scalar()
    checks pd.isna()'s own result is genuinely a single boolean before
    trusting it at all -- a container is never itself "the missing
    marker" regardless of its own size, only ever something that might
    hold missing values inside it, a separate question this function
    was never meant to answer. Verified directly against every case
    this needs to distinguish, this time including the one-element
    shape that was missed before: pd.NA, np.nan, None, pd.NaT, ordinary
    scalars, and empty/single-/multi-element lists, arrays, and a
    pd.Index, each holding either a missing or a non-missing value."""
    try:
        result = pd.isna(value)
        # _is_scalar_like(), not is_scalar(): pd.isna() hands back a
        # zero-dim ARRAY for an object-dtype zero-dim input and a plain
        # numpy bool for a typed one, so is_scalar() alone made
        # np.array(np.nan) missing and np.array(pd.NaT) not -- see
        # _is_scalar_like()'s own docstring.
        return bool(result) if _is_scalar_like(result) else False
    except Exception:
        return False


def _normalize_value(value):
    if value is None:
        return None

    if is_missing(value):
        return None

    # Found by a further review: is_missing() itself was already
    # correct at this point, but the duck-typed conversions below --
    # completely separate logic, a different mechanism entirely -- were
    # still silently collapsing a one-element container down to its
    # sole element. numpy's own .item() is genuinely designed to do
    # exactly that for an array of size 1 (raising for anything larger,
    # which the except Exception: pass below was already catching) --
    # confirmed directly, not assumed: np.array([5]).item() succeeds
    # and returns the plain int 5, silently discarding the array itself.
    # pd.api.types.is_scalar() stops this before it starts: a genuine
    # numpy/pandas scalar (np.int64, pd.Timestamp, ...) still reaches
    # to_pydatetime()/.item() below and normalizes correctly, since
    # is_scalar() is True for those -- confirmed directly. An array,
    # Index, Series, or any other container now returns unchanged
    # instead, reaching the DB driver intact or failing honestly there
    # rather than silently changing shape here.
    if not _is_scalar_like(value):
        return value

    to_pydatetime = getattr(value, 'to_pydatetime', None)
    if callable(to_pydatetime):
        value = to_pydatetime()
        if value is None:
            return None

    item = getattr(value, 'item', None)
    if callable(item) and not isinstance(value, (str, bytes, bytearray)):
        try:
            value = item()
        except Exception:
            pass

    return value


def _validate_unique_columns(columns, *, table_name):
    # Must run before row dicts are built, not after -- once a row is
    # {col: value for col, value in zip(columns, row)}, a duplicate
    # column name has already silently collapsed to whichever value came
    # last, with no trace of the collision left to detect. columns would
    # still list the duplicate twice while rows lost a value entirely --
    # an internally inconsistent DbPayload that fails much later, inside
    # SQLAlchemy's CREATE TABLE with two same-named columns, rather than
    # clearly here. Distinct original labels that stringify to the same
    # value (1 and '1') hit this identically, since columns is already
    # the stringified list by the time this runs.
    duplicates = find_duplicates(columns)
    if duplicates:
        raise DbPublishError(f'{table_name!r}: duplicate output column names: {duplicates!r}')


def _apply_db_contract_columns(columns, rows, db_contract, *, table_name):
    if not db_contract:
        return list(columns), rows

    source_cols = list(db_contract)
    missing = [col for col in source_cols if col not in columns]
    if missing:
        raise DbPublishError(f'{table_name!r}: missing columns for db_contract: {missing!r}')

    rename = dict(db_contract)
    target_cols = [rename[col] for col in source_cols]

    # Two source columns mapping to the same target name would silently
    # collapse rows (Python dict literals with repeated keys keep only
    # the last one) while columns keeps a duplicate entry -- an
    # internally inconsistent DbPayload (columns says 2, rows has 1 key)
    # that fails cryptically much later, inside SQLAlchemy's CREATE
    # TABLE with two same-named columns, rather than clearly here. A
    # hand-written, static db_contract is reviewed once and stays fixed;
    # a dynamically-computed one (get_dynamic_db_contract) is built from
    # runtime data and can accidentally collide if its own logic has a
    # bug -- this isn't specific to that hook, but it makes hitting this
    # case easier, so it's worth closing at the shared boundary both
    # engines (and funnel_pandas's direct callers) go through.
    seen = {}
    duplicates = {}
    for source, target in zip(source_cols, target_cols):
        if target in seen:
            duplicates.setdefault(target, [seen[target]]).append(source)
        else:
            seen[target] = source
    if duplicates:
        raise DbPublishError(
            f'{table_name!r}: db_contract maps multiple source columns to the same '
            f'target name: {duplicates!r}'
        )

    return target_cols, [
        {rename[col]: row.get(col) for col in source_cols}
        for row in rows
    ]


def _resolve_override(value):
    match value:
        case None:
            return None
        case sa.types.TypeEngine():
            return value
        case type() as cls if issubclass(cls, sa.types.TypeEngine):
            return cls()
        case str():
            key = value.strip().upper()
            try:
                return _TYPE_OVERRIDES[key]()
            except KeyError as e:
                raise DbPublishError(f'unsupported db type override: {value!r}') from e
        case _:
            raise DbPublishError(f'unsupported db type override: {value!r}')


def _declared_int_parameter(value, *, name, type_obj):
    """Validate an integer SQL type parameter without accepting bool or float."""
    if value is None:
        return None
    if type(value) is not int:
        raise DbPublishError(
            f'{name} must be an integer in output_schema: {type_obj}'
        )
    return value


def _declared_type_family(type_obj):
    """Return the supported declared scalar family, or raise clearly.

    The PostgreSQL dialect silently drops several ambiguous SQLAlchemy type
    parameters (for example ``Float(0)``, ``String(0)`` and
    ``Numeric(scale=2)``). Declared schemas are a physical contract, so reject
    shapes whose rendered PostgreSQL type would not preserve the declaration.
    """
    if isinstance(type_obj, sa.DateTime):
        return 'datetime'
    if isinstance(type_obj, sa.Date):
        return 'date'
    if isinstance(type_obj, sa.Boolean):
        return 'bool'
    if isinstance(type_obj, sa.SmallInteger):
        return 'smallint'
    if isinstance(type_obj, sa.BigInteger):
        return 'bigint'
    if isinstance(type_obj, sa.Integer):
        return 'integer'
    if isinstance(type_obj, sa.Float):
        precision = _declared_int_parameter(
            type_obj.precision, name='FLOAT precision', type_obj=type_obj,
        )
        if precision is not None and not 1 <= precision <= 53:
            raise DbPublishError(
                f'FLOAT precision must be between 1 and 53 in output_schema: {type_obj}'
            )
        return 'float'
    if isinstance(type_obj, sa.Numeric):
        precision = _declared_int_parameter(
            type_obj.precision, name='NUMERIC precision', type_obj=type_obj,
        )
        scale = _declared_int_parameter(
            type_obj.scale, name='NUMERIC scale', type_obj=type_obj,
        )
        if scale is not None and precision is None:
            raise DbPublishError(
                f'NUMERIC scale requires precision in output_schema: {type_obj}'
            )
        if precision is not None and not 1 <= precision <= 1000:
            raise DbPublishError(
                f'NUMERIC precision must be between 1 and 1000 in output_schema: {type_obj}'
            )
        if scale is not None and scale < 0:
            raise DbPublishError(
                f'negative NUMERIC scale is not supported in output_schema: {type_obj}'
            )
        if precision is not None and scale is not None and scale > precision:
            raise DbPublishError(
                f'NUMERIC scale greater than precision is outside the supported '
                f'output_schema subset: {type_obj}'
            )
        return 'numeric'
    if isinstance(type_obj, sa.LargeBinary):
        length = _declared_int_parameter(
            type_obj.length, name='LargeBinary length', type_obj=type_obj,
        )
        if length is not None:
            raise DbPublishError(
                f'bounded LargeBinary is not supported in output_schema because '
                f'PostgreSQL BYTEA does not preserve its length: {type_obj}'
            )
        return 'bytes'
    if isinstance(type_obj, sa.Enum):
        raise DbPublishError(
            f'Enum is not supported in output_schema: {type_obj!r}'
        )
    if isinstance(type_obj, sa.CHAR):
        raise DbPublishError(
            f'fixed-length CHAR is not supported in output_schema: {type_obj!r}'
        )
    if isinstance(type_obj, sa.String):
        length = _declared_int_parameter(
            type_obj.length, name='VARCHAR length', type_obj=type_obj,
        )
        if length is not None and length < 1:
            raise DbPublishError(
                f'VARCHAR length must be positive in output_schema: {type_obj}'
            )
        if type_obj.collation is not None:
            raise DbPublishError(
                f'text collation is not supported in output_schema: {type_obj}'
            )
        return 'text'
    raise DbPublishError(
        f'unsupported output_schema type {type_obj!r}; supported families are '
        'boolean, integer, floating point, numeric, text, binary, date, and timestamp'
    )


def _resolve_declared_type(value):
    resolved = _resolve_override(value)
    if resolved is None:
        raise DbPublishError('output_schema column type must not be None')
    _declared_type_family(resolved)
    return resolved


def _is_aware_datetime(value):
    return value.tzinfo is not None and value.utcoffset() is not None


def _declared_value_error(table_name, column, row_number, detail):
    raise DbPublishError(
        f'{table_name!r}: output row {row_number} column {column.name!r} '
        f'is incompatible with declared type {column.type}: {detail}'
    )


def _validate_numeric_value(table_name, column, row_number, value):
    if type(value) is int:
        decimal_value = Decimal(value)
    elif isinstance(value, Decimal):
        decimal_value = value
    else:
        _declared_value_error(
            table_name, column, row_number,
            'expected int or Decimal; float-to-NUMERIC conversion is not implicit',
        )

    if not decimal_value.is_finite():
        _declared_value_error(table_name, column, row_number, 'non-finite Decimal is not supported')

    precision = column.type.precision
    declared_scale = column.type.scale
    effective_scale = declared_scale if declared_scale is not None else (0 if precision is not None else None)

    if decimal_value.is_zero():
        fractional_digits = 0
        integer_digits = 0
    else:
        parts = decimal_value.as_tuple()
        digits = list(parts.digits)
        exponent = parts.exponent

        # Trailing fractional zeroes do not require rounding. For example,
        # Decimal('1.2300') is exactly representable at scale 2.
        while exponent < 0 and digits and digits[-1] == 0:
            digits.pop()
            exponent += 1

        fractional_digits = max(-exponent, 0)
        integer_digits = max(len(digits) + exponent, 0)

    if effective_scale is not None and fractional_digits > effective_scale:
        _declared_value_error(
            table_name, column, row_number,
            f'value requires rounding to fit scale {effective_scale}',
        )

    if precision is not None:
        scale = effective_scale or 0
        max_integer_digits = precision - scale
        if integer_digits > max_integer_digits:
            _declared_value_error(
                table_name, column, row_number,
                f'value exceeds NUMERIC({precision}, {scale}) integer-digit capacity',
            )


def _validate_declared_value_family(
    table_name,
    column,
    row_number,
    value,
    family,
):
    """Validate one declared value when its scalar family is already known.

    COPY compiles the family once per column before entering its row loop.
    INSERT and other callers continue to use `_validate_declared_value`,
    which resolves the family and delegates here. Keeping the actual rules in
    one kernel prevents the optimized COPY path from drifting from INSERT.
    """
    if value is None:
        if column.nullable:
            return
        raise DbPublishError(
            f'{table_name!r}: output row {row_number} contains NULL in '
            f'non-nullable column {column.name!r}'
        )

    if family == 'bool':
        if type(value) is not bool:
            _declared_value_error(table_name, column, row_number, 'expected bool')
        return

    if family in {'smallint', 'integer', 'bigint'}:
        if type(value) is not int:
            _declared_value_error(table_name, column, row_number, 'expected int, not bool or another numeric family')
        range_type = {
            'smallint': sa.SmallInteger,
            'integer': sa.Integer,
            'bigint': sa.BigInteger,
        }[family]
        lower, upper = _INTEGER_RANGES[range_type]
        if not lower <= value <= upper:
            _declared_value_error(table_name, column, row_number, f'value is outside {family} range')
        return

    if family == 'numeric':
        _validate_numeric_value(table_name, column, row_number, value)
        return

    if family == 'float':
        if type(value) is not float:
            _declared_value_error(table_name, column, row_number, 'expected float')
        return

    if family == 'text':
        if not isinstance(value, str):
            _declared_value_error(table_name, column, row_number, 'expected str')
        if '\x00' in value:
            _declared_value_error(
                table_name, column, row_number,
                'NUL character is not supported in PostgreSQL text',
            )
        if column.type.length is not None and len(value) > column.type.length:
            _declared_value_error(
                table_name, column, row_number,
                f'text length exceeds VARCHAR({column.type.length})',
            )
        return

    if family == 'bytes':
        if not isinstance(value, (bytes, bytearray, memoryview)):
            _declared_value_error(table_name, column, row_number, 'expected bytes-like value')
        return

    if family == 'date':
        if type(value) is not date:
            _declared_value_error(table_name, column, row_number, 'expected date; datetime-to-DATE conversion is not implicit')
        return

    if family == 'datetime':
        if not isinstance(value, datetime):
            _declared_value_error(table_name, column, row_number, 'expected datetime')
        aware = _is_aware_datetime(value)
        wants_timezone = bool(column.type.timezone)
        if wants_timezone and not aware:
            _declared_value_error(table_name, column, row_number, 'timezone-aware datetime required')
        if not wants_timezone and aware:
            _declared_value_error(
                table_name, column, row_number,
                'timezone-aware datetime cannot be published to timestamp without time zone',
            )
        return

    raise DbPublishInvariantError(
        f'internal invariant violated -- unsupported declared family {family!r}'
    )


def _validate_declared_value(table_name, column, row_number, value):
    """Enforce declared nullability and type rules for one value.

    Takes `table_name` (not a full payload) so the same kernel serves both
    the INSERT path and the COPY path. The COPY preparer can resolve a
    column's family once and call `_validate_declared_value_family` directly;
    all other callers retain this stateless convenience boundary.
    """
    family = _declared_type_family(column.type)
    _validate_declared_value_family(
        table_name,
        column,
        row_number,
        value,
        family,
    )


def _resolve_payload_schema(payload, *, sample_size):
    """Resolve one schema model and validate all schema-owned row rules."""
    _validate_unique_columns(payload.columns, table_name=payload.table_name)

    if payload.output_schema is not None:
        if not isinstance(payload.output_schema, (list, tuple)):
            raise DbPublishError(
                f'{payload.table_name!r}: output_schema must be a list or tuple of OutputColumn values'
            )
        if not payload.output_schema:
            raise DbPublishError(
                f'{payload.table_name!r}: output_schema must contain at least one column'
            )
        if not all(isinstance(column, OutputColumn) for column in payload.output_schema):
            raise DbPublishError(
                f'{payload.table_name!r}: output_schema must contain only OutputColumn values'
            )
        duplicates = find_duplicates(column.name for column in payload.output_schema)
        if duplicates:
            raise DbPublishError(
                f'{payload.table_name!r}: output_schema contains duplicate column name(s): {duplicates}'
            )
        incompatible = []
        if payload.type_overrides is not None:
            incompatible.append('type_overrides')
        if payload.not_null_columns:
            incompatible.append('not_null_columns')
        if incompatible:
            raise DbPublishError(
                f'{payload.table_name!r}: output_schema cannot be combined with '
                + ', '.join(incompatible)
            )

    if not isinstance(payload.not_null_columns, (list, tuple)):
        raise DbPublishError(
            f'{payload.table_name!r}: not_null_columns must be a list or tuple of strings'
        )
    if not all(isinstance(name, str) for name in payload.not_null_columns):
        raise DbPublishError(
            f'{payload.table_name!r}: not_null_columns must contain only strings'
        )
    not_null_duplicates = find_duplicates(payload.not_null_columns)
    if not_null_duplicates:
        raise DbPublishError(
            f'{payload.table_name!r}: not_null_columns contains duplicate column(s): '
            f'{not_null_duplicates}'
        )

    framework_names = [column.name for column in payload.framework_columns]
    if len(framework_names) != len(set(framework_names)):
        raise DbPublishInvariantError(
            f'internal invariant violated -- duplicate framework columns for {payload.table_name!r}'
        )

    if payload.output_schema is not None:
        declared_names = [column.name for column in payload.output_schema]
        collisions = [name for name in framework_names if name in set(declared_names)]
        if collisions:
            raise DbPublishError(
                f'{payload.table_name!r}: output_schema must not declare framework '
                f'column(s): {collisions!r}'
            )
        expected_names = declared_names + framework_names
        actual_names = list(payload.columns)
        missing = [name for name in expected_names if name not in actual_names]
        unexpected = [name for name in actual_names if name not in expected_names]
        if missing or unexpected:
            parts = []
            if missing:
                parts.append(f'missing columns: {missing!r}')
            if unexpected:
                parts.append(f'unexpected columns: {unexpected!r}')
            raise DbPublishError(
                f'{payload.table_name!r}: output columns do not match output_schema; '
                + '; '.join(parts)
            )

        resolved_columns = [
            ResolvedColumn(column.name, _resolve_declared_type(column.type), column.nullable)
            for column in payload.output_schema
        ]
        resolved_columns.extend(
            ResolvedColumn(column.name, _resolve_declared_type(column.type), column.nullable)
            for column in payload.framework_columns
        )
        resolved = ResolvedSchema(tuple(resolved_columns), 'declared')
        expected_set = set(expected_names)

        # from_petl()/from_pandas() normalized every cell while building
        # the mappings, because inference needs Python-native values to
        # classify at all. Repeating it here was 41% of this function's
        # cost on a wide declared payload, for values that cannot change
        # -- _normalize_value() is idempotent. A payload built by hand
        # still gets normalized: the flag defaults False.
        needs_normalize = not payload.rows_normalized

        for row_number, row in enumerate(payload.rows, start=1):
            if not isinstance(row, Mapping):
                raise DbPublishError(
                    f'{payload.table_name!r}: output row {row_number} is not a mapping'
                )
            # A keys view compares against a set without building one per
            # row, which the previous set(row) did for every row in the
            # payload purely to throw it away.
            if row.keys() != expected_set:
                missing_row = [name for name in expected_names if name not in row]
                unexpected_row = [name for name in row if name not in expected_set]
                raise DbPublishError(
                    f'{payload.table_name!r}: output row {row_number} does not match '
                    f'output_schema; missing={missing_row!r}, unexpected={unexpected_row!r}'
                )
            for column in resolved.columns:
                value = row[column.name]
                if needs_normalize:
                    value = _normalize_value(value)
                    row[column.name] = value
                _validate_declared_value(payload.table_name, column, row_number, value)

        payload.columns = expected_names
        return resolved

    not_null = tuple(payload.not_null_columns or ())
    missing_constraints = [name for name in not_null if name not in payload.columns]
    if missing_constraints:
        raise DbPublishError(
            f'{payload.table_name!r}: db_not_null_columns contains column(s) not present '
            f'in the output: {missing_constraints!r}'
        )

    framework_by_name = {column.name: column for column in payload.framework_columns}
    overrides = payload.type_overrides or {}
    resolved_overrides = {name: _resolve_override(overrides.get(name)) for name in payload.columns}

    # Gathered before anything is resolved so the columns that genuinely
    # need inference can be inferred together. A framework column carries
    # its own declared type and an overridden one is pinned, so neither
    # reaches the scan at all -- excluding them here keeps the width that
    # picks the traversal order the real width, not the nominal one.
    to_infer = [
        name for name in payload.columns
        if name not in framework_by_name and resolved_overrides[name] is None
    ]
    inferred_types = _infer_column_types(payload.rows, to_infer, sample_size=sample_size)

    resolved_columns = []
    for name in payload.columns:
        framework = framework_by_name.get(name)
        if framework is not None:
            type_obj = _resolve_declared_type(framework.type)
            nullable = framework.nullable
        else:
            type_obj = resolved_overrides[name]
            if type_obj is None:
                type_obj = inferred_types[name]
            nullable = name not in not_null
        resolved_columns.append(ResolvedColumn(name, type_obj, nullable))

    resolved = ResolvedSchema(tuple(resolved_columns), 'inferred')
    constrained = {column.name: column for column in resolved.columns if not column.nullable}
    if constrained:
        needs_normalize = not payload.rows_normalized
        for row_number, row in enumerate(payload.rows, start=1):
            for name, column in constrained.items():
                normalized = row.get(name)
                if needs_normalize:
                    normalized = _normalize_value(normalized)
                    row[name] = normalized
                if normalized is None:
                    raise DbPublishError(
                        f'{payload.table_name!r}: output row {row_number} contains NULL in '
                        f'non-nullable column {column.name!r}'
                    )
    return resolved


def _scan_families(rows, col_name, *, sample_size):
    """The value families present in the scanned rows. Returned as a set
    rather than folded straight into a type so _infer_column_type() can
    tell 'this column is text' apart from 'this sample saw nothing at all'
    -- both of which resolve to Text, and only one of which is an
    observation. Distinguishing them without a second pass over the sample
    is the whole reason this is split out.
    """
    families = set()
    rows_to_scan = rows if sample_size is None else rows[:sample_size]

    for row in rows_to_scan:
        value = row.get(col_name)
        if value is None:
            continue
        families.add(_value_family(value))
        if len(families) > 2:
            # Three families can only ever resolve to Text; the exact set
            # stops mattering, so stop reading.
            return families

    return families


def _infer_from_scan(
    rows: list[dict[str, Any]],
    col_name: str,
    *,
    sample_size: int | None,
):
    """The raw family scan. Split out of _infer_column_type() (below) so
    that function can re-run it over the full rows list after its own
    sampled answer is found to be narrower than the real data -- see
    _infer_column_type()'s own docstring for why that verification pass
    exists at all."""
    return _resolve_families(_scan_families(rows, col_name, sample_size=sample_size))


def _resolve_families(families):
    if not families:
        return sa.Text()

    if families == {'bool'}:
        return sa.Boolean()

    if families == {'int'}:
        return sa.BigInteger()

    if families <= {'int', 'numeric'} and families:
        return sa.Numeric()

    temporal_families = {'date', 'datetime_naive', 'datetime_aware'}
    if families <= temporal_families:
        if families == {'date'}:
            return sa.Date()
        if 'datetime_aware' in families:
            if families != {'datetime_aware'}:
                raise DbPublishError(
                    'cannot infer one timestamp type from timezone-aware '
                    'datetime and naive datetime or date values; normalize '
                    'the column or declare output_schema explicitly'
                )
            return sa.DateTime(timezone=True)
        return sa.DateTime(timezone=False)

    if families == {'text'}:
        return sa.Text()

    if families == {'bytes'}:
        return sa.LargeBinary()

    return sa.Text()


def _silently_widenable_exact_type(inferred):
    for sa_type, exact_type in _SILENTLY_WIDENABLE:
        if type(inferred) is sa_type:
            return exact_type
    return None


def _infer_column_type(
    rows: list[dict[str, Any]],
    col_name: str,
    *,
    sample_size: int | None = 5000,
):
    """Column type inferred from the first `sample_size` rows, then
    verified against the remainder when -- and only when -- a later value
    could silently widen the sampled answer.

    The sample alone was genuinely wrong, not merely imprecise: confirmed
    directly that a column whose first 5000 rows are ints and whose row
    5001 is 3.5 infers BigInteger, and that PostgreSQL then rounds 3.5 to
    4 on insert without error (see _SILENTLY_WIDENABLE above for the exact
    reproduction). Same shape for a date column meeting a later datetime,
    which loses the time. Both are data-dependent, appear only once a
    table grows past the sample, and are invisible in the log -- the task
    reports success either way.

    Scanning every row instead was measured before being rejected: on a
    1,000,000-row, 20-column table it cost ~7.2s against the sample's
    ~35ms, a ~200x penalty paid on every publish to correct something the
    sample already gets right in the overwhelming majority of cases. The
    verification pass below costs ~1.0s on that same table (~30x, and only
    ~3-5% on top of that table's own ~18s payload construction, before the
    network insert that dwarfs both), because it runs only for the two
    narrowable types and uses a bare exact-type check per row rather than
    the full _value_family() match statement. Confirmed directly that it
    returns the identical answer to a full scan for every case tried: a
    late float, Decimal, datetime, text, and bool, plus clean int and
    clean date columns with no late value at all.

    A third approach -- one pass over rows accumulating families for every
    column at once, retiring each column as soon as its answer can no
    longer change -- was also built and measured: ~2.5s on the same table,
    still worse than verification and a substantially larger change to a
    function every publish depends on. Not pursued.

    sample_size=None still means "scan every row", exactly as before; the
    verification pass is skipped in that case because a full scan has
    nothing left to verify against.
    """
    families = _scan_families(rows, col_name, sample_size=sample_size)

    # A sample that saw no non-null value at all tells us nothing, and
    # _infer_from_scan()'s Text fallback for that case is a guess, not an
    # observation. Confirmed directly: 5000 nulls followed by an int
    # inferred Text where a full scan gives BigInteger, and likewise for a
    # late float, date or datetime. Sparse business columns routinely have
    # long leading null runs after sorting or monthly expansion, so this is
    # a realistic shape rather than a contrived one. The rows are already
    # materialized, so re-scanning costs CPU and no additional source I/O.
    if sample_size is not None and len(rows) > sample_size and not families:
        return _infer_from_scan(rows, col_name, sample_size=None)

    inferred = _resolve_families(families)

    if sample_size is None or len(rows) <= sample_size:
        return inferred

    if isinstance(inferred, sa.DateTime):
        wants_timezone = bool(inferred.timezone)
        for row in islice(rows, sample_size, None):
            value = row.get(col_name)
            if value is None:
                continue
            if type(value) is date and not wants_timezone:
                continue
            if (
                isinstance(value, datetime)
                and _is_aware_datetime(value) is wants_timezone
            ):
                continue
            return _infer_from_scan(rows, col_name, sample_size=None)
        return inferred

    exact_type = _silently_widenable_exact_type(inferred)
    if exact_type is None:
        return inferred

    for row in islice(rows, sample_size, None):
        # islice, not rows[sample_size:] -- the slice would copy the
        # entire unsampled remainder into a second list (995,000 dicts on
        # the table measured above) purely to iterate it once.
        value = row.get(col_name)
        if value is None or type(value) is exact_type:
            continue
        # Something outside the sample genuinely doesn't fit. Re-infer
        # across every row rather than trying to widen the sampled answer
        # in place: the correct result depends on the full set of families
        # present (a late float widens int to Numeric, but a late string
        # collapses the whole column to Text), and _infer_from_scan()
        # already encodes that resolution exactly once.
        return _infer_from_scan(rows, col_name, sample_size=None)

    return inferred


# Above this many columns, infer them all in one walk instead of one walk
# each. Measured, both directions: row-major loses on a narrow table
# because it pays per-row bookkeeping (iterating the live set, a dict
# lookup per column, retirement) that the per-column loop does not --
# 0.64x at 3 columns, 0.80x at 10, 0.93x at 20 -- and wins once the
# traversals it saves dominate that: 1.09x at 30, 1.26x at 80, and
# 2.3x at 200 columns x 100,000 rows. The crossover sat at 25-30 on every
# shape tried, so the threshold is the measurement rather than a guess.
#
# Dispatched on column count rather than configured. The deciding
# dimension is len(columns), which this function already has and a task
# author often does not -- in inferred mode the column set comes from the
# data and can change between runs. See decisions/0001 on why the
# neighbouring sample size is not exposed either.
_ROW_MAJOR_MIN_COLUMNS = 30


def _scan_families_row_major(rows, col_names, *, start=0, stop=None):
    """Families for several columns in a single walk over the rows.

    Same cell visits as calling _scan_families() per column -- the same
    row.get() happens for the same (row, column) pairs. What this saves
    is len(col_names) - 1 traversals of the row list and that many
    re-fetches of each row object, which is the whole reason it is
    faster on a wide table and slower on a narrow one.

    Retires a column as soon as a third family makes its answer Text
    regardless, exactly as the per-column scan's early return does.
    """
    families = {name: set() for name in col_names}
    live = set(col_names)

    for row in islice(rows, start, stop):
        if not live:
            break
        retired = None
        for name in live:
            value = row.get(name)
            if value is None:
                continue
            found = families[name]
            found.add(_value_family(value))
            if len(found) > 2:
                if retired is None:
                    retired = []
                retired.append(name)
        if retired:
            live.difference_update(retired)

    return families


def _infer_column_types_row_major(rows, col_names, *, sample_size):
    """_infer_column_type() for several columns, one walk per phase.

    Every branch mirrors the per-column function deliberately: the
    all-null sample falling back to a full scan, the DateTime awareness
    check, the silently-widenable exact-type check, and the full re-infer
    when the remainder contradicts the sample. Any divergence here is a
    bug, not a variation -- tests/test_declared_schema.py asserts the two
    agree column by column on the shapes that distinguish them.

    Rejected, after building and measuring it: accumulating families
    during the verification walk so a contradicted column never needs the
    fallback rescan at all. It looks like the obvious next step and it is
    correct -- 300 randomised differential trials against the per-column
    path found no disagreement, including the columns that raise on an
    aware/naive mix. It is simply not faster: 1.02x on the 200-column x
    100,000-row shape built to favour it, and 0.88x on a shape with no
    contradictions, where its bookkeeping is pure overhead.

    The reason is worth keeping, because the idea will occur again. The
    dominant cost is _value_family() per non-null cell, and both shapes
    call it the same number of times -- fusing the rescan into the
    verification walk moves that work rather than removing it, and the
    remainder is ~95% of a tall table, so the "extra" pass was never the
    expensive part. Loop structure is exhausted as a lever here; the
    floor is now the per-cell classification itself.
    """
    row_count = len(rows)
    families = _scan_families_row_major(rows, col_names, stop=sample_size)
    sampled_short = sample_size is not None and row_count > sample_size

    resolved = {}
    needs_full_scan = []
    for name in col_names:
        if sampled_short and not families[name]:
            # Saw no non-null value at all: Text here would be a guess
            # rather than an observation, same as the per-column path.
            needs_full_scan.append(name)
        else:
            resolved[name] = _resolve_families(families[name])

    if not sampled_short:
        return resolved

    skip = set(needs_full_scan)
    to_verify = {}
    for name in col_names:
        if name in skip:
            continue
        inferred = resolved[name]
        if isinstance(inferred, sa.DateTime):
            to_verify[name] = (True, bool(inferred.timezone))
            continue
        exact_type = _silently_widenable_exact_type(inferred)
        if exact_type is not None:
            to_verify[name] = (False, exact_type)

    contradicted = []
    if to_verify:
        for row in islice(rows, sample_size, None):
            if not to_verify:
                break
            failed = None
            for name, (is_datetime, expected) in to_verify.items():
                value = row.get(name)
                if value is None:
                    continue
                if is_datetime:
                    wants_timezone = expected
                    if type(value) is date and not wants_timezone:
                        continue
                    if (
                        isinstance(value, datetime)
                        and _is_aware_datetime(value) is wants_timezone
                    ):
                        continue
                elif type(value) is expected:
                    continue
                if failed is None:
                    failed = []
                failed.append(name)
            if failed:
                for name in failed:
                    contradicted.append(name)
                    del to_verify[name]

    # One walk for every column that needs the full set of families,
    # rather than one walk each. On a wide table with many contradicted
    # columns this is the difference the batching exists for: the
    # per-column path re-scanned every row once per such column.
    rescan = needs_full_scan + contradicted
    if rescan:
        full = _scan_families_row_major(rows, rescan)
        for name in rescan:
            resolved[name] = _resolve_families(full[name])

    return resolved


def _infer_column_types(rows, col_names, *, sample_size):
    """Inferred types for `col_names`, keyed by name.

    Dispatches on width alone -- see _ROW_MAJOR_MIN_COLUMNS. Both paths
    must return identical answers for every input; only their cost
    differs.
    """
    names = list(col_names)
    if len(names) < _ROW_MAJOR_MIN_COLUMNS:
        return {
            name: _infer_column_type(rows, name, sample_size=sample_size)
            for name in names
        }
    return _infer_column_types_row_major(rows, names, sample_size=sample_size)


def _value_family(value):
    match value:
        case bool():
            return 'bool'
        case datetime():
            return 'datetime_aware' if _is_aware_datetime(value) else 'datetime_naive'
        case date():
            return 'date'
        case int():
            return 'int'
        case float() | Decimal():
            return 'numeric'
        case bytes() | bytearray() | memoryview():
            return 'bytes'
        case _:
            return 'text'


class _InferenceStreamState:
    """Per-column family accumulator for one-pass row-source inference.

    The COPY path (ADR 0011 §Preparation flows) cannot rewind its input,
    so the sample-then-verify shape _infer_column_type() uses for the
    insert path is not available. This class replaces it with a single
    pass that observes every row: for each column, it grows a family set
    exactly the way _scan_families() does for the insert path, then
    resolves via the shared _resolve_families() kernel at EOF.

    Streaming = full-scan semantics: given the same normalized values in
    the same order, .resolve()[i] returns a type equal to
    _infer_column_type(rows_as_dicts, columns[i], sample_size=None) for
    every column. The parity test in tests/test_db_publish.py enforces
    this over the insert-path corpus.

    Input contract: values must already be normalized to Python-native
    types (via _normalize_value or an equivalent). Normalization is not
    layered here so that the same normalized value can also be written
    to the spool once, without a second pass -- see ADR 0011 §Local
    spool design. Feeding raw source values (pd.NA, np.int64, ...)
    would misclassify them the same way _scan_families() would if
    from_petl/from_pandas skipped their normalization step.

    Bounded memory: one set per column, each at most three entries
    (the shared _resolve_families() cannot distinguish anything larger,
    so growth stops once size crosses two). Independent of row count.
    """

    def __init__(self, column_count: int):
        if column_count < 1:
            raise DbPublishError(
                'inference stream requires at least one column, '
                f'got {column_count}'
            )
        self._column_count = column_count
        self._family_sets: list[set[str]] = [set() for _ in range(column_count)]

    def feed_row(self, row: Sequence[Any]) -> None:
        if len(row) != self._column_count:
            raise DbPublishError(
                f'row width {len(row)} does not match expected column '
                f'count {self._column_count}'
            )
        for index, value in enumerate(row):
            families = self._family_sets[index]
            # Same short-circuit _scan_families uses: three families
            # can only ever resolve to Text, so any further value in
            # this column changes nothing.
            if len(families) > 2:
                continue
            if value is None:
                continue
            families.add(_value_family(value))

    def resolve(self) -> tuple[sa.types.TypeEngine, ...]:
        return tuple(_resolve_families(families) for families in self._family_sets)
