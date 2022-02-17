import struct
from typing import Any, Union


class CellType:
    """Type for cell in the row."""
    CHAR = ('c',)
    SIGNED_CHAR = ('b',)
    UNSIGNED_CHAR = ('B',)
    BOOL = ('?',)
    SHORT = ('h',)
    UNSIGNED_SHORT = ('H',)
    INT = ('i',)
    UNSIGNED_INT = ('I',)
    LONG = ('l',)
    UNSIGNED_LONG = ('L',)
    LONG_LONG = ('q',)
    UNSIGNED_LONG_LONG = ('Q',)
    SSIZE_T = ('n',)
    SIZE_T = ('N',)
    HALF_FLOAT = ('e',)
    FLOAT = ('f',)
    DOUBLE = ('d',)
    STRING = ('I', 's')
    BYTES = ('I', 'p')


SIZE_BY_MARK = {
    CellType.CHAR[0]: 1, CellType.SIGNED_CHAR[0]: 1, CellType.UNSIGNED_CHAR[0]: 1,
    CellType.BOOL[0]: 1,

    CellType.SHORT[0]: 2, CellType.UNSIGNED_SHORT[0]: 2,
    CellType.HALF_FLOAT[0]: 2,

    CellType.INT[0]: 4, CellType.UNSIGNED_INT[0]: 4,
    CellType.LONG[0]: 4, CellType.UNSIGNED_LONG[0]: 4,
    CellType.FLOAT[0]: 4,

    CellType.LONG_LONG[0]: 8, CellType.UNSIGNED_LONG_LONG[0]: 8,
    CellType.DOUBLE[0]: 8
}
ROW_SIZE_TYPE = CellType.UNSIGNED_INT
SchemaType = list[Union[tuple[str], tuple[str, str]]]


def serialize(schema: SchemaType, rows: list[list[Any]]) -> bytes:
    """Serialize data to bytes.

    :param schema: row schema by cell types.
    :param rows: list of rows.
    :return:
    """
    pair_indexes = [i for i, x in enumerate(schema) if len(x) == 2]

    struct_pattern = []
    for cell_type in schema:
        value = cell_type[0]
        if len(cell_type) == 2:
            value += '{}' + cell_type[1]
        struct_pattern.append(value)
    struct_pattern = ''.join(struct_pattern)

    result = []
    for row in rows:
        string_sizes = []
        for string_index in pair_indexes:
            string_sizes.append(len(row[string_index]))
        pattern = struct_pattern.format(*string_sizes)

        new_row = []
        length_bytes = SIZE_BY_MARK[ROW_SIZE_TYPE[0]]
        for i, cell in enumerate(row):
            length_bytes += SIZE_BY_MARK[schema[i][0]]
            if len(schema[i]) == 2:
                if schema[i] == CellType.STRING:
                    cell = cell.encode('utf8')
                length = len(cell)

                new_row.append(length)
                new_row.append(cell)

                length_bytes += length
            else:
                if schema[i] == CellType.CHAR:
                    cell = cell.encode('utf8')
                    assert len(cell) == 1, \
                        'Type CHAR used only for 1 byte characters.\n' \
                        f'Now value = {cell.decode("utf8")}, length bytes = {len(cell)}'

                new_row.append(cell)

        result.append(struct.pack(f'={ROW_SIZE_TYPE[0]}{pattern}', length_bytes, *new_row))

    return b''.join(result)


def deserialize(schema: SchemaType, block: bytes) -> tuple[list[list[Any]], bytes]:
    """Deserialize bytes to data.

    :param schema: row schema by cell types.
    :param block: bytes.
    :return:
    """
    rows = []

    struct_pattern = []
    for cell_type in schema:
        struct_pattern.append(cell_type[0])
        if len(cell_type) == 2:
            struct_pattern.append('{}' + cell_type[1])

    block_size = len(block)
    row_length_size = SIZE_BY_MARK[ROW_SIZE_TYPE[0]]
    byte_index = 0
    row_size = struct.unpack('=' + ROW_SIZE_TYPE[0], block[:row_length_size])[0]
    while block_size - byte_index >= row_size:
        row_size -= row_length_size
        byte_index += row_length_size

        row = []
        pattern_index = 0
        while row_size > 0:
            pattern = struct_pattern[pattern_index]

            if pattern.startswith('{'):
                value_size = row.pop()
                value = struct.unpack('=' + pattern.format(value_size), block[byte_index:byte_index + value_size])[0]

                if pattern.endswith('s'):
                    value = value.decode('utf8')

                row_size -= value_size
                byte_index += value_size
            else:
                value_size = SIZE_BY_MARK[pattern]
                value = struct.unpack('=' + pattern, block[byte_index:byte_index + value_size])[0]
                if pattern == 'c':
                    value = value.decode('utf8')
                row_size -= value_size
                byte_index += value_size

            pattern_index += 1

            if row_size == 0 and pattern_index < len(struct_pattern):
                if struct_pattern[-1].endswith('s'):
                    value = ''
                else:
                    value = b''

            row.append(value)

        rows.append(row)
        if block_size - byte_index >= row_length_size:
            row_size = struct.unpack('=' + ROW_SIZE_TYPE[0], block[byte_index:byte_index + row_length_size])[0]
        else:
            break

    bytes_tail = b''
    if byte_index < block_size:
        bytes_tail = block[byte_index:]

    return rows, bytes_tail
