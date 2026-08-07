"""Serialization to PostgreSQL COPY text.

Split out of copy.py in 0.7.4. Escaping exists exactly once in this
project, in `_escape_copytext_text`, and ADR 0011 §Final serialization
requires adapters never implement their own.

Two paths converge here. Declared schemas compile one direct writer per
column, fusing missing handling, validation and encoding; inferred schemas
resolve a family per column first and preserve widening semantics. A parity
test asserts both produce byte-identical output to the generic serializer
over the ADR's corpus, because the compiled path bypasses it entirely.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any, BinaryIO, Callable

from task_core.db.values import (
    DbPublishError,
    ResolvedColumn,
    _declared_type_family,
    _declared_value_error,
    _is_aware_datetime,
    _normalize_value,
    _validate_declared_value,
    _validate_numeric_value,
)

# --- COPY text serialization ------------------------------------------

# PostgreSQL COPY text format wire rules used here:
#   - field separator: TAB (0x09)
#   - row terminator: LF (0x0A)
#   - NULL marker: the two bytes 0x5C 0x4E  (`\N`)
#   - escape character: 0x5C (backslash)
#
# We escape only the four bytes COPY treats as structural: backslash, tab,
# newline, carriage return. Everything else -- including UTF-8 continuation
# bytes and any control character other than TAB/LF/CR -- passes through
# unchanged. `_validate_declared_value` has already rejected NUL (0x00) in
# text columns, so we never need to escape it here.
#
# Ordering matters: backslash must be escaped FIRST. Otherwise a genuine
# `\` in user data would combine with a following `n` we just inserted
# and be read back as a newline by COPY. Test 10 covers the ordering.


def _escape_copytext_text(text: str) -> bytes:
    """Encode `text` as UTF-8 with COPY text escaping applied."""
    escaped = (
        text.replace('\\', '\\\\')
            .replace('\t', '\\t')
            .replace('\n', '\\n')
            .replace('\r', '\\r')
    )
    return escaped.encode('utf-8')


def _serialize_float_text(value: float) -> bytes:
    if value != value:
        return b'NaN'
    if value == float('inf'):
        return b'Infinity'
    if value == float('-inf'):
        return b'-Infinity'
    return repr(value).encode('ascii')


def _serialize_value_copytext_family(value: Any, column, family: str) -> bytes:
    """Serialize one non-NULL declared value for a pre-resolved family."""
    if family == 'bool':
        return b't' if value else b'f'
    if family in {'smallint', 'integer', 'bigint'}:
        return str(value).encode('ascii')
    if family == 'numeric':
        return str(value).encode('ascii')
    if family == 'float':
        return _serialize_float_text(value)
    if family == 'text':
        return _escape_copytext_text(value)
    if family == 'bytes':
        payload = bytes(value) if isinstance(value, (bytearray, memoryview)) else value
        return b'\\\\x' + payload.hex().encode('ascii')
    if family == 'date':
        return value.isoformat().encode('ascii')
    if family == 'datetime':
        return value.isoformat(sep=' ').encode('ascii')
    raise DbPublishError(
        f'internal invariant violated -- unsupported family {family!r} in copy serializer'
    )


def _serialize_value_copytext(value: Any, column) -> bytes:
    """Serialize one value already validated against a declared schema."""
    family = _declared_type_family(column.type)
    return _serialize_value_copytext_family(value, column, family)


def _serialize_inferred_value_copytext_family(
    value: Any,
    column: ResolvedColumn,
    table_name: str,
    row_number: int,
    family: str,
) -> bytes:
    """Serialize one inferred/override value using PostgreSQL input syntax.

    Declared mode deliberately refuses semantic coercion. Inferred mode is
    different: its resolved type can represent several observed Python
    families (int+float -> NUMERIC, date+datetime -> TIMESTAMP, mixed scalar
    families -> TEXT). COPY must therefore render those observed values in
    the resolved target type instead of applying the stricter declared-mode
    validator that INSERT never applies to inferred payloads.
    """
    if value is None:
        if column.nullable:
            return b'\\N'
        raise DbPublishError(
            f'{table_name!r}: output row {row_number} contains NULL in '
            f'non-nullable column {column.name!r}'
        )

    if family == 'bool':
        if type(value) is not bool:
            raise DbPublishError(
                f'{table_name!r}: output row {row_number} column {column.name!r} '
                f'is incompatible with inferred/overridden type {column.type}: expected bool'
            )
        return b't' if value else b'f'

    if family in {'smallint', 'integer', 'bigint'}:
        if type(value) is not int:
            raise DbPublishError(
                f'{table_name!r}: output row {row_number} column {column.name!r} '
                f'is incompatible with inferred/overridden type {column.type}: expected int'
            )
        # Reuse the declared range check; it is not coercion and catches a
        # value PostgreSQL would reject after the expensive spool pass.
        _validate_declared_value(table_name, column, row_number, value)
        return str(value).encode('ascii')

    if family == 'numeric':
        if type(value) is int or isinstance(value, Decimal):
            return str(value).encode('ascii')
        if type(value) is float:
            return _serialize_float_text(value)
        raise DbPublishError(
            f'{table_name!r}: output row {row_number} column {column.name!r} '
            f'is incompatible with inferred/overridden type {column.type}: '
            'expected int, float, or Decimal'
        )

    if family == 'float':
        if type(value) is float:
            return _serialize_float_text(value)
        if type(value) is int or isinstance(value, Decimal):
            return str(value).encode('ascii')
        raise DbPublishError(
            f'{table_name!r}: output row {row_number} column {column.name!r} '
            f'is incompatible with inferred/overridden type {column.type}: '
            'expected int, float, or Decimal'
        )

    if family == 'text':
        if isinstance(value, str):
            text = value
        elif type(value) is bool:
            text = 'true' if value else 'false'
        elif type(value) in {int, float} or isinstance(value, Decimal):
            if type(value) is float and not (value == value and abs(value) != float('inf')):
                text = _serialize_float_text(value).decode('ascii')
            else:
                text = str(value)
        elif isinstance(value, datetime):
            text = value.isoformat(sep=' ')
        elif type(value) is date:
            text = value.isoformat()
        elif isinstance(value, (bytes, bytearray, memoryview)):
            # There is no unambiguous bytea-to-text coercion. Failing here is
            # safer than inventing a representation that INSERT may not use.
            raise DbPublishError(
                f'{table_name!r}: output row {row_number} column {column.name!r} '
                'cannot render bytes-like data into an inferred TEXT column'
            )
        else:
            text = str(value)
        if '\x00' in text:
            raise DbPublishError(
                f'{table_name!r}: output row {row_number} column {column.name!r} '
                'contains NUL, which PostgreSQL text does not support'
            )
        return _escape_copytext_text(text)

    if family == 'bytes':
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise DbPublishError(
                f'{table_name!r}: output row {row_number} column {column.name!r} '
                f'is incompatible with inferred/overridden type {column.type}: '
                'expected bytes-like value'
            )
        payload = bytes(value)
        return b'\\\\x' + payload.hex().encode('ascii')

    if family == 'date':
        if type(value) is not date:
            raise DbPublishError(
                f'{table_name!r}: output row {row_number} column {column.name!r} '
                f'is incompatible with inferred/overridden type {column.type}: expected date'
            )
        return value.isoformat().encode('ascii')

    if family == 'datetime':
        if isinstance(value, datetime):
            return value.isoformat(sep=' ').encode('ascii')
        if type(value) is date:
            # PostgreSQL TIMESTAMP input accepts a date as midnight. This is
            # the widening represented by the shared date+datetime inference
            # rule, not a declared-mode convenience conversion.
            return value.isoformat().encode('ascii')
        raise DbPublishError(
            f'{table_name!r}: output row {row_number} column {column.name!r} '
            f'is incompatible with inferred/overridden type {column.type}: '
            'expected date or datetime'
        )

    raise DbPublishError(
        f'internal invariant violated -- unsupported family {family!r} in inferred copy serializer'
    )


def _serialize_inferred_value_copytext(
    value: Any,
    column: ResolvedColumn,
    table_name: str,
    row_number: int,
) -> bytes:
    family = _declared_type_family(column.type)
    return _serialize_inferred_value_copytext_family(
        value,
        column,
        table_name,
        row_number,
        family,
    )


_CompiledInferredFieldSerializer = tuple[int, Callable[[Any, int], bytes]]
_CompiledDeclaredFieldWriter = tuple[int, Callable[[Any, int, bytearray], None]]


_DECLARED_INTEGER_RANGES = {
    'smallint': (-(2 ** 15), (2 ** 15) - 1),
    'integer': (-(2 ** 31), (2 ** 31) - 1),
    'bigint': (-(2 ** 63), (2 ** 63) - 1),
}


def _write_declared_null(
    *,
    table_name: str,
    column: ResolvedColumn,
    row_number: int,
    buffer: bytearray,
) -> None:
    if not column.nullable:
        raise DbPublishError(
            f'{table_name!r}: output row {row_number} contains NULL in '
            f'non-nullable column {column.name!r}'
        )
    buffer.extend(b'\\N')


def _compile_declared_copy_field_writers(
    source_columns: Sequence[str],
    resolved_columns: Sequence[ResolvedColumn],
    table_name: str,
) -> tuple[_CompiledDeclaredFieldWriter, ...]:
    """Compile direct declared-value writers once per COPY spool.

    Common native Python values stay entirely on a family-specific hot path:
    missing handling, type validation, declared constraints, and COPY-text
    encoding happen in one callable. `_normalize_value()` remains the exact
    compatibility fallback for pandas, NumPy, and other scalar wrappers, but
    ordinary task rows no longer pay its generic `pd.isna()` and duck-typing
    cost for every cell.
    """
    source_index = {name: index for index, name in enumerate(source_columns)}
    compiled: list[_CompiledDeclaredFieldWriter] = []

    for column in resolved_columns:
        index = source_index[column.name]
        family = _declared_type_family(column.type)

        if family == 'bool':
            def write_bool(
                value,
                row_number,
                buffer,
                *,
                column=column,
            ):
                if type(value) is bool:
                    buffer.append(0x74 if value else 0x66)
                    return
                if value is not None:
                    value = _normalize_value(value)
                if value is None:
                    _write_declared_null(
                        table_name=table_name,
                        column=column,
                        row_number=row_number,
                        buffer=buffer,
                    )
                    return
                if type(value) is not bool:
                    _declared_value_error(
                        table_name, column, row_number, 'expected bool',
                    )
                buffer.append(0x74 if value else 0x66)

            writer = write_bool

        elif family in _DECLARED_INTEGER_RANGES:
            lower, upper = _DECLARED_INTEGER_RANGES[family]

            def write_integer(
                value,
                row_number,
                buffer,
                *,
                column=column,
                family=family,
                lower=lower,
                upper=upper,
            ):
                if type(value) is not int:
                    if value is not None:
                        value = _normalize_value(value)
                    if value is None:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                    if type(value) is not int:
                        _declared_value_error(
                            table_name,
                            column,
                            row_number,
                            'expected int, not bool or another numeric family',
                        )
                if not lower <= value <= upper:
                    _declared_value_error(
                        table_name,
                        column,
                        row_number,
                        f'value is outside {family} range',
                    )
                buffer.extend(str(value).encode('ascii'))

            writer = write_integer

        elif family == 'numeric':
            def write_numeric(
                value,
                row_number,
                buffer,
                *,
                column=column,
            ):
                native = type(value) is int or isinstance(value, Decimal)
                if not native:
                    if value is not None:
                        value = _normalize_value(value)
                    if value is None:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                elif isinstance(value, Decimal) and value.is_nan():
                    value = None
                    _write_declared_null(
                        table_name=table_name,
                        column=column,
                        row_number=row_number,
                        buffer=buffer,
                    )
                    return
                _validate_numeric_value(
                    table_name,
                    column,
                    row_number,
                    value,
                )
                buffer.extend(str(value).encode('ascii'))

            writer = write_numeric

        elif family == 'float':
            def write_float(
                value,
                row_number,
                buffer,
                *,
                column=column,
            ):
                if type(value) is float:
                    if value != value:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                else:
                    if value is not None:
                        value = _normalize_value(value)
                    if value is None:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                    if type(value) is not float:
                        _declared_value_error(
                            table_name, column, row_number, 'expected float',
                        )
                buffer.extend(_serialize_float_text(value))

            writer = write_float

        elif family == 'text':
            max_length = column.type.length

            def write_text(
                value,
                row_number,
                buffer,
                *,
                column=column,
                max_length=max_length,
            ):
                if type(value) is not str:
                    if value is not None:
                        value = _normalize_value(value)
                    if value is None:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                    if not isinstance(value, str):
                        _declared_value_error(
                            table_name, column, row_number, 'expected str',
                        )
                if '\x00' in value:
                    _declared_value_error(
                        table_name,
                        column,
                        row_number,
                        'NUL character is not supported in PostgreSQL text',
                    )
                if max_length is not None and len(value) > max_length:
                    _declared_value_error(
                        table_name,
                        column,
                        row_number,
                        f'text length exceeds VARCHAR({max_length})',
                    )
                buffer.extend(_escape_copytext_text(value))

            writer = write_text

        elif family == 'bytes':
            def write_bytes(
                value,
                row_number,
                buffer,
                *,
                column=column,
            ):
                if not isinstance(value, (bytes, bytearray, memoryview)):
                    if value is not None:
                        value = _normalize_value(value)
                    if value is None:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                    if not isinstance(value, (bytes, bytearray, memoryview)):
                        _declared_value_error(
                            table_name,
                            column,
                            row_number,
                            'expected bytes-like value',
                        )
                payload = (
                    bytes(value)
                    if isinstance(value, (bytearray, memoryview))
                    else value
                )
                buffer.extend(b'\\\\x')
                buffer.extend(payload.hex().encode('ascii'))

            writer = write_bytes

        elif family == 'date':
            def write_date(
                value,
                row_number,
                buffer,
                *,
                column=column,
            ):
                if type(value) is not date:
                    if value is not None:
                        value = _normalize_value(value)
                    if value is None:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                    if type(value) is not date:
                        _declared_value_error(
                            table_name,
                            column,
                            row_number,
                            'expected date; datetime-to-DATE conversion is not implicit',
                        )
                buffer.extend(value.isoformat().encode('ascii'))

            writer = write_date

        elif family == 'datetime':
            wants_timezone = bool(column.type.timezone)

            def write_datetime(
                value,
                row_number,
                buffer,
                *,
                column=column,
                wants_timezone=wants_timezone,
            ):
                if type(value) is not datetime:
                    if value is not None:
                        value = _normalize_value(value)
                    if value is None:
                        _write_declared_null(
                            table_name=table_name,
                            column=column,
                            row_number=row_number,
                            buffer=buffer,
                        )
                        return
                    if not isinstance(value, datetime):
                        _declared_value_error(
                            table_name,
                            column,
                            row_number,
                            'expected datetime',
                        )
                aware = _is_aware_datetime(value)
                if wants_timezone and not aware:
                    _declared_value_error(
                        table_name,
                        column,
                        row_number,
                        'timezone-aware datetime required',
                    )
                if not wants_timezone and aware:
                    _declared_value_error(
                        table_name,
                        column,
                        row_number,
                        'timezone-aware datetime cannot be published to timestamp without time zone',
                    )
                buffer.extend(value.isoformat(sep=' ').encode('ascii'))

            writer = write_datetime

        else:
            raise DbPublishError(
                f'internal invariant violated -- unsupported family '
                f'{family!r} in declared COPY compiler'
            )

        compiled.append((index, writer))

    return tuple(compiled)


def _compile_inferred_copy_field_serializers(
    source_columns: Sequence[str],
    resolved_columns: Sequence[ResolvedColumn],
    table_name: str,
) -> tuple[_CompiledInferredFieldSerializer, ...]:
    """Compile inferred source positions and scalar families once per spool."""
    source_index = {name: index for index, name in enumerate(source_columns)}
    compiled: list[_CompiledInferredFieldSerializer] = []
    for column in resolved_columns:
        index = source_index[column.name]
        family = _declared_type_family(column.type)

        def render_inferred(
            value,
            row_number,
            *,
            column=column,
            family=family,
        ):
            return _serialize_inferred_value_copytext_family(
                value,
                column,
                table_name,
                row_number,
                family,
            )

        compiled.append((index, render_inferred))
    return tuple(compiled)


def _write_compiled_declared_copytext_row(
    fp: BinaryIO,
    row: Sequence[Any],
    serializers: Sequence[_CompiledDeclaredFieldWriter],
    row_number: int,
    buffer: bytearray,
    *,
    expected_width: int,
) -> None:
    """Validate and serialize one raw declared row without a normalized tuple."""
    if type(row) not in (tuple, list):
        if not isinstance(row, Sequence) or isinstance(row, (str, bytes)):
            raise DbPublishError(
                f'row source yielded non-sequence {type(row).__name__}'
            )
    if len(row) != expected_width:
        raise DbPublishError(
            f'row width {len(row)} does not match column count {expected_width}'
        )

    buffer.clear()
    for field_number, (source_index, write_field) in enumerate(serializers):
        if field_number:
            buffer.append(0x09)
        write_field(row[source_index], row_number, buffer)
    buffer.append(0x0A)
    fp.write(buffer)


def _write_compiled_inferred_copytext_row(
    fp: BinaryIO,
    values: Sequence[Any],
    serializers: Sequence[_CompiledInferredFieldSerializer],
    row_number: int,
    buffer: bytearray,
) -> None:
    """Serialize one normalized inferred row into a reusable output buffer."""
    buffer.clear()
    for field_number, (source_index, render) in enumerate(serializers):
        if field_number:
            buffer.append(0x09)
        buffer.extend(render(values[source_index], row_number))
    buffer.append(0x0A)
    fp.write(buffer)

def serialize_row_to_copytext(
    row: Mapping[str, Any],
    columns: Sequence,
    table_name: str,
    row_number: int,
    *,
    declared: bool = True,
) -> bytes:
    """Convert one normalized row to its COPY text wire representation.

    Declared mode applies `_validate_declared_value`, the same strict kernel
    used by INSERT. Inferred mode instead renders values through the resolved
    widening family because INSERT does not apply declared coercion rules to
    inferred payloads. Both paths reject unexpected NULLs. Escaping exists
    exactly once in the codebase, inside `_escape_copytext_text`.

    Returns the wire bytes for the row: fields joined by TAB, terminated
    by LF. `columns` is the resolved schema in wire order (framework
    columns already appended); `row` must supply exactly those keys, but
    iteration order is taken from `columns` so the wire order is stable.
    """
    fields: list[bytes] = []
    for column in columns:
        value = row[column.name]
        if declared:
            _validate_declared_value(table_name, column, row_number, value)
            if value is None:
                fields.append(b'\\N')
            else:
                fields.append(_serialize_value_copytext(value, column))
        else:
            fields.append(_serialize_inferred_value_copytext(
                value, column, table_name, row_number,
            ))
    return b'\t'.join(fields) + b'\n'
