import string

from algorithms import CellType, serialize, deserialize


def test_serialize():
    """Test case for serialize and deserialize generated data.

    :return:
    """
    max_strings = 100
    max_string_length = 3
    string_number = 0

    def create_strings(string_='', number=0):
        """Generator for strings.

        :param string_: current string.
        :param number: string index.
        :return:
        """
        nonlocal string_number

        if string_number >= max_strings:
            return

        yield string_
        string_number += 1

        if len(string_) < max_string_length:
            for character in string.ascii_lowercase:
                yield from create_strings(string_ + character)

    def create_integers(number=0):
        """Generator for integers.

        :param number: current integer.
        :return:
        """
        max_int = 5
        while number < max_int:
            yield number
            number += 1

    def create_float(number=0.0):
        """Generator for float.

        :param number: current float.
        :return:
        """
        max_float = 5
        while number < max_float:
            yield number
            number += 0.1

    schema = [
        CellType.CHAR, CellType.SIGNED_CHAR, CellType.UNSIGNED_CHAR,

        CellType.SHORT, CellType.UNSIGNED_SHORT,
        CellType.INT, CellType.UNSIGNED_INT,
        CellType.LONG, CellType.UNSIGNED_LONG,
        CellType.LONG_LONG, CellType.UNSIGNED_LONG_LONG,

        CellType.HALF_FLOAT, CellType.FLOAT, CellType.DOUBLE,

        CellType.STRING,

        CellType.BOOL,

        CellType.STRING
    ]

    data = []
    boolean_value = False
    for string_ in create_strings():
        for character in string.ascii_lowercase[:12]:
            for number in create_integers():
                for float_number in create_float():
                    boolean_value = not boolean_value

                    row = [None] * len(schema)
                    row[-1] = row[-3] = string_
                    row[-2] = boolean_value
                    row[0] = character

                    for index in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10):
                        row[index] = number
                    for index in (11, 12, 13):
                        row[index] = float_number

                    data.append(row)

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

    eps = 0.01
    for row, new_row in zip(data, new_data):
        assert len(row) == len(new_row), f'Rows length is not equal ({len(row)} != {len(new_row)})\n{row}\n{new_row}'
        for x, y in zip(row, new_row):
            assert type(x) == type(y), \
                f'Types of parallels elements is not equal ({type(x).__name__} != {type(y).__name__})'

            type_ = type(x)
            message = f'Values of parallels elements is not equal ({x} != {y})'
            if type_ == float:
                assert abs(x - y) < eps, message
            else:
                assert x == y, message
