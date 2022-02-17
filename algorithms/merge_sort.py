from os import path, remove
import struct
from dataclasses import dataclass

from .serialize import CellType, serialize, deserialize, SIZE_BY_MARK, ROW_SIZE_TYPE, SchemaType


@dataclass
class SortInfo:
    """All parameters for sort."""
    schema: SchemaType
    schema_sort_index: int
    tmp_directory: str
    block_size: int
    is_ascending_order: bool = True
    input_file_name: str = ''


class GeneratorID:
    """Generator string id."""
    _instance = None
    last_id = 0

    def __new__(cls, *args, **kwargs):
        """Use singleton pattern.

        :param args: arguments.
        :param kwargs: dict arguments.
        """
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls, *args, **kwargs)
        return cls._instance

    def next_id(self) -> str:
        """Generate next id.

        :return:
        """
        result = self.last_id
        self.last_id += 1
        return f'{result:015d}'


GENERATOR_ID = GeneratorID()


def split_file(file_name: str, info: SortInfo) -> tuple[str, str]:
    """Split file by two files.

    :param file_name: original file name.
    :param info: info object.
    :return:
    """
    file_size = path.getsize(file_name)

    with open(file_name, 'rb') as file:
        row_length_size = SIZE_BY_MARK[ROW_SIZE_TYPE[0]]

        index = 0
        while index < file_size // 2:
            row_size = struct.unpack('=' + ROW_SIZE_TYPE[0], file.read(row_length_size))[0]
            index += row_size
            file.seek(index)

        left_file_end = index
        right_file_end = file_size

        left_file_name = path.join(info.tmp_directory, GENERATOR_ID.next_id())
        right_file_name = path.join(info.tmp_directory, GENERATOR_ID.next_id())

        with open(left_file_name, 'wb') as left_file, \
                open(right_file_name, 'wb') as right_file:
            file.seek(0)

            index = 0
            while index < left_file_end:
                read_size = min(info.block_size, left_file_end - index)
                left_file.write(file.read(read_size))
                index += read_size
            while index < right_file_end:
                read_size = min(info.block_size, right_file_end - index)
                right_file.write(file.read(read_size))
                index += read_size

    if info.input_file_name != file_name:
        remove(file_name)

    return left_file_name, right_file_name


def merge_files(left_file_name: str, right_file_name: str, info: SortInfo) -> str:
    """Merge two files into one file.

    :param left_file_name: first file name.
    :param right_file_name: second file name.
    :param info: info object.
    :return:
    """
    result_file_name = path.join(info.tmp_directory, GENERATOR_ID.next_id())
    left_size, right_size = path.getsize(left_file_name), path.getsize(right_file_name)
    with open(left_file_name, 'rb') as left_file, \
            open(right_file_name, 'rb') as right_file, \
            open(result_file_name, 'wb') as result_file:
        left_rows, left_head = [], b''
        right_rows, right_head = [], b''
        left_index, right_index = 0, 0
        left_row_index, right_row_index = 0, 0

        while left_index < left_size or right_index < right_size:
            if left_row_index == len(left_rows):
                left_read_size = min(info.block_size, left_size - left_index)
                if left_read_size == 0:
                    break
                left_block = left_head + left_file.read(left_read_size)
                left_rows, left_head = deserialize(info.schema, left_block)
                left_index += left_read_size
                left_row_index = 0
            if right_row_index == len(right_rows):
                right_read_size = min(info.block_size, right_size - right_index)
                if right_read_size == 0:
                    break
                right_block = right_head + right_file.read(right_read_size)
                right_rows, right_head = deserialize(info.schema, right_block)
                right_index += right_read_size
                right_row_index = 0

            result_rows = []
            while left_row_index < len(left_rows) and right_row_index < len(right_rows):
                x, y = left_rows[left_row_index], right_rows[right_row_index]
                x_key, y_key = x[info.schema_sort_index], y[info.schema_sort_index]
                if (x_key < y_key and info.is_ascending_order) or (x_key > y_key and not info.is_ascending_order):
                    result_rows.append(x)
                    left_row_index += 1
                elif (x_key > y_key and info.is_ascending_order) or (x_key < y_key and not info.is_ascending_order):
                    result_rows.append(y)
                    right_row_index += 1
                else:
                    result_rows.append(x)
                    left_row_index += 1

            result_file.write(serialize(info.schema, result_rows))

        result_rows = []
        if left_row_index < len(left_rows):
            result_rows = left_rows[left_row_index:]
        if right_row_index < len(right_rows):
            result_rows = right_rows[right_row_index:]

        if len(result_rows) > 0:
            result_file.write(serialize(info.schema, result_rows))

        while left_index < left_size:
            left_read_size = min(info.block_size, left_size - left_index)
            left_block = left_head + left_file.read(left_read_size)
            result_file.write(left_block)
            left_index += left_read_size
            left_head = b''
        while right_index < right_size:
            right_read_size = min(info.block_size, right_size - right_index)
            right_block = right_head + right_file.read(right_read_size)
            result_file.write(right_block)
            right_index += right_read_size
            right_head = b''

    if info.input_file_name not in (left_file_name, right_file_name):
        remove(left_file_name)
        remove(right_file_name)

    return result_file_name


def _merge_sort(file_name: str, info: SortInfo) -> str:
    """Merge sort for file by one key.

    :param file_name: original file name.
    :param info: info object.
    :return:
    """
    if path.getsize(file_name) > info.block_size:
        left_file_name, right_file_name = split_file(file_name, info)
        left_file_name = _merge_sort(left_file_name, info)
        right_file_name = _merge_sort(right_file_name, info)
    else:
        with open(file_name, 'rb') as file:
            rows, tail = deserialize(info.schema, file.read())
            assert len(tail) == 0, f'Error deserialize: bad format file {file_name}.'

            rows.sort(key=lambda x: x[info.schema_sort_index])
            new_file_name = path.join(info.tmp_directory, GENERATOR_ID.next_id())
            with open(new_file_name, 'wb') as new_file:
                new_file.write(serialize(info.schema, rows))

        if info.input_file_name != file_name:
            remove(file_name)

        return new_file_name

    return merge_files(left_file_name, right_file_name, info)


def merge_sort(
    file_name: str, schema: SchemaType, schema_sort_indexes: list[int],
    tmp_directory: str, block_size: int, is_ascending_order: bool = True
) -> str:
    """Merge sort for file.

    :param file_name: original file name.
    :param schema: row schema.
    :param schema_sort_indexes: sort indexes from high to low power.
    :param tmp_directory: temporary directory.
    :param block_size: block size.
    :param is_ascending_order: ascending order or not.
    :return:
    """
    assert len([
        x
        for i, x in enumerate(schema)
        if i in schema_sort_indexes and x == CellType.BYTES
    ]) == 0, 'Selected for sort columns have type BYTES'

    info = SortInfo(schema, -1, tmp_directory, block_size, is_ascending_order, file_name)
    result = file_name
    for schema_sort_index in reversed(schema_sort_indexes):
        info.schema_sort_index = schema_sort_index
        result = _merge_sort(result, info)

    return result
