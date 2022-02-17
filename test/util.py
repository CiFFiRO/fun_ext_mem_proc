import string
from typing import Any

from algorithms import CellType, SchemaType


def generate_ordered_data() -> tuple[SchemaType, list[list[Any]]]:
    max_strings = 100
    max_string_length = 3
    string_number = 0

    def create_strings(string_=''):
        """Generator for strings.

        :param string_: current string.
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
        CellType.CHAR,

        CellType.SIGNED_CHAR, CellType.UNSIGNED_CHAR,

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

    return schema, data


def check_equal(x: Any, y: Any, eps: float = 0.01) -> bool:
    assert type(x) == type(y), \
        f'Types of parallels elements is not equal ({type(x).__name__} != {type(y).__name__})'

    type_ = type(x)
    if type_ == float:
        return abs(x - y) < eps
    else:
        return x == y


def check_equal_data(data, new_data) -> bool:
    for row_index, (row, new_row) in enumerate(zip(data, new_data)):
        assert len(row) == len(new_row), f'Rows length is not equal ({len(row)} != {len(new_row)})\n{row}\n{new_row}'
        for i, (x, y) in enumerate(zip(row, new_row)):
            assert check_equal(x, y), f'Value of parallels elements by index {i} is not equal {x} != {y}\n' \
                                      f'Row index {row_index}\n' \
                                      f'Old rows: {data[max(row_index - 1, 0):row_index + 2]}\n' \
                                      f'New row: {new_data[max(row_index - 1, 0):row_index + 2]}'
    return True
