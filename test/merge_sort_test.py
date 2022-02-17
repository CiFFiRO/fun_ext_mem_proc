from os import path

from algorithms import serialize, deserialize, SortInfo, merge_sort, split_file, merge_files
from util import generate_ordered_data, check_equal_data
import pytest


@pytest.fixture
def ordered_data():
    return generate_ordered_data()


def test_split_merge(ordered_data):
    file_name = path.join('.', 'test', 'data', 'test_split')
    schema, data = ordered_data

    with open(file_name, 'wb') as file:
        raw_data = serialize(schema, data)
        file.write(raw_data)

    info = SortInfo(schema, 16, path.join('.', 'test', 'data'), 2 ** 23, True, file_name)
    first_file_name, second_file_name = split_file(file_name, info)
    with open(first_file_name, 'rb') as first_file, \
            open(second_file_name, 'rb') as second_file:
        first_data, byte_tail = deserialize(schema, first_file.read())
        assert len(byte_tail) == 0
        second_data, byte_tail = deserialize(schema, second_file.read())
        assert len(byte_tail) == 0

    new_data = first_data + second_data
    assert len(data) == len(new_data)
    assert check_equal_data(data, new_data)

    merged_file_name = merge_files(first_file_name, second_file_name, info)
    with open(merged_file_name, 'rb') as merged_file:
        new_data, byte_tail = deserialize(schema, merged_file.read())
        assert len(byte_tail) == 0

    assert len(data) == len(new_data)
    assert check_equal_data(data, new_data)


def test_merge_sort(ordered_data):
    file_name = path.join('.', 'test', 'data', 'test_merge_sort')
    schema, data = ordered_data
    data.reverse()

    with open(file_name, 'wb') as file:
        raw_data = serialize(schema, data)
        file.write(raw_data)

    schema_sort_indexes = [0, 4, 12, 15, 16]
    sorted_file_name = merge_sort(file_name, schema, schema_sort_indexes, path.join('.', 'test', 'data'), 2 ** 23)
    with open(sorted_file_name, 'rb') as sorted_file:
        new_data, byte_tail = deserialize(schema, sorted_file.read())
        assert len(byte_tail) == 0
        assert len(new_data) == len(data)

        for schema_sort_index in reversed(schema_sort_indexes):
            data.sort(key=lambda x: x[schema_sort_index])
        assert check_equal_data(data, new_data)
