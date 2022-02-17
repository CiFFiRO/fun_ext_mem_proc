from os import path

from algorithms import serialize, deserialize
from util import generate_ordered_data, check_equal_data


def test_serialize():
    """Test case for serialize and deserialize generated data.

    :return:
    """
    schema, data = generate_ordered_data()
    raw_data = serialize(schema, data)

    block_size = 2**20
    byte_index = 0
    bytes_tail = b''
    new_data = []
    while byte_index < len(raw_data):
        new_rows, bytes_tail = deserialize(schema, bytes_tail + raw_data[byte_index:byte_index + block_size])
        byte_index += block_size
        new_data.extend(new_rows)

    assert len(data) == len(new_data)
    assert check_equal_data(data, new_data)

    file_name = path.join('.', 'test', 'data', 'test_serialize')

    with open(file_name, 'wb') as file:
        raw_data = serialize(schema, data)
        file.write(raw_data)

    with open(file_name, 'rb') as file:
        read_data = file.read()
        new_data, byte_tail = deserialize(schema, read_data)
        assert len(byte_tail) == 0

    assert len(data) == len(new_data)
    assert check_equal_data(data, new_data)
