from dataclasses import dataclass
import struct
from typing import Any, Optional, Union
from collections.abc import Callable
from collections import namedtuple


CELL_SCHEMA = namedtuple('CellSchema', 'mark size')


@dataclass
class BaseCellType:
    name: str
    schema: list[CELL_SCHEMA]
    equal_: Optional[Callable[[Any, Any], bool]] = lambda x, y: x == y
    less_: Optional[Callable[[Any, Any], bool]] = lambda x, y: x < y
    greater_: Optional[Callable[[Any, Any], bool]] = lambda x, y: x > y

    def __repr__(self) -> str:
        return f'Cell type is "{self.name}". Schema is {self.schema}.'

    def __eq__(self, x: 'BaseCellType') -> bool:
        return self.schema == x.schema

    def less(self, x: Any, y: Any) -> Optional[bool]:
        if self.less_ is None:
            raise Exception(f'For cell type "{self.name}" method less is not implemented.')
        return None if x is None or y is None else self.less_(x, y)

    def greater(self, x: Any, y: Any) -> Optional[bool]:
        if self.greater_ is None:
            raise Exception(f'For cell type "{self.name}" method greater is not implemented.')
        return None if x is None or y is None else self.greater_(x, y)

    def equal(self, x: Any, y: Any) -> Optional[bool]:
        if self.equal_ is None:
            raise Exception(f'For cell type "{self.name}" method equal is not implemented.')
        return None if x is None or y is None else self.equal_(x, y)


class StructMark:
    CHAR = 'c'
    SIGNED_CHAR = 'b'
    UNSIGNED_CHAR = 'B'
    BOOL = '?'
    SHORT = 'h'
    UNSIGNED_SHORT = 'H'
    INT = 'i'
    UNSIGNED_INT = 'I'
    LONG = 'l'
    UNSIGNED_LONG = 'L'
    LONG_LONG = 'q'
    UNSIGNED_LONG_LONG = 'Q'
    HALF_FLOAT = 'e'
    FLOAT = 'f'
    DOUBLE = 'd'
    STRING = 's'
    BYTES = 'p'


SIZE_BY_STRUCT_MARK = {
    StructMark.CHAR: 1, StructMark.SIGNED_CHAR: 1,
    StructMark.UNSIGNED_CHAR: 1, StructMark.BOOL: 1,

    StructMark.SHORT: 2, StructMark.UNSIGNED_SHORT: 2,
    StructMark.HALF_FLOAT: 2,

    StructMark.INT: 4, StructMark.UNSIGNED_INT: 4,
    StructMark.LONG: 4, StructMark.UNSIGNED_LONG: 4,
    StructMark.FLOAT: 4,

    StructMark.LONG_LONG: 8, StructMark.UNSIGNED_LONG_LONG: 8,
    StructMark.DOUBLE: 8
}


def _build_schema(marks: tuple[str, ...]) -> list[tuple[str, int]]:
    return [
        CELL_SCHEMA(
            mark,
            SIZE_BY_STRUCT_MARK[mark] if mark in SIZE_BY_STRUCT_MARK else None
        )
        for mark in marks
    ]


def _float_equal(epsilon: float) -> Callable[(float, float), bool]:
    return lambda x, y: abs(x - y) < epsilon


def _float_less(epsilon: float) -> Callable[(float, float), bool]:
    return lambda x, y: x < y - epsilon


def _float_greater(epsilon: float) -> Callable[(float, float), bool]:
    return lambda x, y: x > y + epsilon


class CellType:
    """Type for cell in the row."""
    CHAR = BaseCellType('Char', _build_schema((StructMark.CHAR,)))
    SIGNED_CHAR = BaseCellType('Signed char', _build_schema((StructMark.SIGNED_CHAR,)))
    UNSIGNED_CHAR = BaseCellType('Unsigned char', _build_schema((StructMark.UNSIGNED_CHAR,)))
    BOOL = BaseCellType('Bool', _build_schema((StructMark.BOOL,)))
    SHORT = BaseCellType('Short', _build_schema((StructMark.SHORT,)))
    UNSIGNED_SHORT = BaseCellType('Unsigned short', _build_schema((StructMark.UNSIGNED_SHORT,)))
    INT = BaseCellType('Int', _build_schema((StructMark.INT,)))
    UNSIGNED_INT = BaseCellType('Unsigned int', _build_schema((StructMark.UNSIGNED_INT,)))
    LONG = BaseCellType('Long', _build_schema((StructMark.LONG,)))
    UNSIGNED_LONG = BaseCellType('Unsigned long', _build_schema((StructMark.UNSIGNED_LONG,)))
    LONG_LONG = BaseCellType('Long long', _build_schema((StructMark.LONG_LONG,)))
    UNSIGNED_LONG_LONG = BaseCellType('Unsigned long long', _build_schema((StructMark.UNSIGNED_LONG_LONG,)))
    HALF_FLOAT = BaseCellType(
        'Half float',
        _build_schema((StructMark.HALF_FLOAT,)),
        _float_equal(1e-3), _float_less(1e-3), _float_greater(1e-3)
    )
    FLOAT = BaseCellType(
        'Float',
        _build_schema((StructMark.FLOAT,)),
        _float_equal(1e-3), _float_less(1e-3), _float_greater(1e-3)
    )
    DOUBLE = BaseCellType(
        'Double',
        _build_schema((StructMark.DOUBLE,)),
        _float_equal(1e-3), _float_less(1e-3), _float_greater(1e-3)
    )
    STRING = BaseCellType('String', _build_schema((StructMark.UNSIGNED_INT, StructMark.STRING)))
    BYTES = BaseCellType(
        'Double',
        _build_schema((StructMark.UNSIGNED_INT, StructMark.BYTES)),
        None, None, None
    )


LENGTH_ROW_TYPE = CellType.UNSIGNED_INT
NULL_FLAG_TYPE = CellType.BOOL
COMPOSITE_TYPES = (CellType.STRING, CellType.BYTES)

SchemaType = Union[list[BaseCellType], tuple[BaseCellType, ...]]


def _build_template_struct_pattern(schema: SchemaType, with_null: bool) -> list[str]:
    composite_indexes = [i for i, x in enumerate(schema) if x in COMPOSITE_TYPES]

    result = []
    for index, cell_type in enumerate(schema):
        if with_null:
            result.append(NULL_FLAG_TYPE.schema[0].mark)

        value = cell_type.schema[0].mark
        if index in composite_indexes:
            value += '{}' + cell_type.schema[1].mark

        result.append(value)

    return result


def serialize(schema: SchemaType, rows: list[list[Any]]) -> bytes:
    """Serialize data to bytes.

    :param schema: row schema by cell types.
    :param rows: list of rows.
    :return:
    """
    composite_indexes = {i for i, x in enumerate(schema) if x in COMPOSITE_TYPES}

    result = []
    for row in rows:
        template_struct_pattern = []
        for index, cell_type in enumerate(schema):
            template_struct_pattern.append(NULL_FLAG_TYPE.schema[0].mark)
            if row[index] is not None:
                value = cell_type.schema[0].mark
                if index in composite_indexes:
                    value += '{}' + cell_type.schema[1].mark

                template_struct_pattern.append(value)
        template_struct_pattern = ''.join(template_struct_pattern)

        composite_element_sizes = []
        for index in composite_indexes:
            if row[index] is not None:
                composite_element_sizes.append(len(row[index]))
        pattern = template_struct_pattern.format(*composite_element_sizes)

        new_row = []
        length_bytes = LENGTH_ROW_TYPE.schema[0].size
        for index, value in enumerate(row):
            length_bytes += NULL_FLAG_TYPE.schema[0].size

            if value is not None:
                new_row.append(False)

                length_bytes += schema[index].schema[0].size
                if index in composite_indexes:
                    if schema[index] == CellType.STRING:
                        value = value.encode('utf8')
                    length = len(value)

                    new_row.append(length)
                    new_row.append(value)

                    length_bytes += length
                else:
                    if schema[index] == CellType.CHAR:
                        value = value.encode('utf8')
                        assert len(value) == 1, \
                            'Type CHAR used only for 1 byte characters.\n' \
                            f'Now value = {value.decode("utf8")}, length bytes = {len(value)}'

                    new_row.append(value)
            else:
                new_row.append(True)

        result.append(struct.pack(f'={LENGTH_ROW_TYPE.schema[0].mark}{pattern}', length_bytes, *new_row))

    return b''.join(result)


def deserialize(schema: SchemaType, block: bytes) -> tuple[list[list[Any]], bytes]:
    """Deserialize bytes to data.

    :param schema: row schema by cell types.
    :param block: bytes.
    :return:
    """
    rows = []
    composite_indexes = {i for i, x in enumerate(schema) if x in COMPOSITE_TYPES}

    block_size = len(block)
    row_length_size = LENGTH_ROW_TYPE.schema[0].size
    row_size = struct.unpack('=' + LENGTH_ROW_TYPE.schema[0].mark, block[:row_length_size])[0]
    null_flag_pattern, null_flag_size = NULL_FLAG_TYPE.schema[0].mark, NULL_FLAG_TYPE.schema[0].size

    byte_index = 0
    while block_size - byte_index >= row_size:
        row_size -= row_length_size
        byte_index += row_length_size

        row = []
        cell_index = 0
        is_second_composite_variable = False
        while row_size > 0:
            if not is_second_composite_variable:
                is_null = struct.unpack('=' + null_flag_pattern, block[byte_index:byte_index + null_flag_size])[0]
                row_size -= null_flag_size
                byte_index += null_flag_size
            else:
                is_null = False

            if is_null:
                value = None
                cell_index += 1
            else:
                if cell_index in composite_indexes and is_second_composite_variable:
                    value_size = row.pop()

                    if value_size > 0:
                        value = struct.unpack(
                            f'={value_size}{schema[cell_index].schema[1].mark}',
                            block[byte_index:byte_index + value_size]
                        )[0]

                        row_size -= value_size
                        byte_index += value_size
                    else:
                        value = b''

                    if schema[cell_index] == CellType.STRING:
                        value = value.decode('utf8')

                    cell_index += 1
                    is_second_composite_variable = False
                else:
                    value_size = schema[cell_index].schema[0].size
                    value = struct.unpack(
                        '=' + schema[cell_index].schema[0].mark,
                        block[byte_index:byte_index + value_size]
                    )[0]

                    if schema[cell_index] == CellType.CHAR:
                        value = value.decode('utf8')

                    row_size -= value_size
                    byte_index += value_size

                    if cell_index in composite_indexes:
                        is_second_composite_variable = True
                    else:
                        cell_index += 1

            if row_size == 0 and is_second_composite_variable and value is not None:
                if schema[-1] == CellType.STRING:
                    value = ''
                else:
                    value = b''

            row.append(value)

        rows.append(row)

        if block_size - byte_index >= row_length_size:
            row_size = struct.unpack(
                '=' + LENGTH_ROW_TYPE.schema[0].mark,
                block[byte_index:byte_index + row_length_size]
            )[0]
        else:
            break

    bytes_tail = b''
    if byte_index < block_size:
        bytes_tail = block[byte_index:]

    return rows, bytes_tail
