#!/usr/bin/python
# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest


class Arrays(object):
    """
    Represents an array object inside the extensions section

    Attributes:
            - values : array values
    """

    def __init__(self, values=None):
        self.values = values

    def get_values(self):
        return self.values

    @staticmethod
    def read_os(content, index):
        # Skip header and any annotation
        while content[index].startswith('#') or content[index] == '\n' or content[index] == '<arrays>\n':
            index = index + 1

        # Process mandatory field: num_arrays
        num_arrays = int(content[index])
        index = index + 1

        # Skip empty lines and any annotation
        while content[index].startswith('#') or content[index] == '\n':
            index = index + 1

        # Process values (Can be one per line with index or all in one line)
        input_values = content[index].split()

        if len(input_values) == num_arrays:
            # All values in a single line
            values = input_values
        else:
            # Values one per line with index
            values = [None] * num_arrays
            num_values = 0
            while num_values < num_arrays and index < len(content):
                input_values = content[index].split()
                value_index = int(input_values[0]) - 1
                value_value = input_values[1]
                values[value_index] = value_value
                index = index + 1
                num_values = num_values + 1

        # Skip empty lines, any annotation, and footer
        while index < len(content) and (
                content[index].startswith('#') or content[index] == '\n' or content[index] == '</arrays>\n'):
            index = index + 1

        # Build Arrays
        arrays = Arrays(values)

        # Return structure
        return arrays, index

    def write_os(self, f):
        # Print header
        print("<arrays>", file=f)

        if self.values is not None:
            # Print number of arrays
            print("# Number of arrays", file=f)
            print(str(len(self.values)), file=f)

            # Print arrays
            print("# Mapping array-identifiers/array-names", file=f)
            index = 1
            for value in self.values:
                print(str(index) + " " + str(value), file=f)
                index = index + 1

        # Print footer
        print("</arrays>", file=f)
        print("", file=f)


#
# UNIT TESTS
#

class TestArrays(unittest.TestCase):

    def test_empty(self):
        a = Arrays()

        self.assertEqual(a.get_values(), None)

    def test_full(self):
        values = ["i", "mSize", "j", "kSize", "k", "nSize", "c", "a", "b"]
        a = Arrays(values)

        self.assertEqual(a.get_values(), values)

    def test_write_os(self):
        values = ["i", "mSize", "j", "kSize", "k", "nSize", "c", "a", "b"]
        a = Arrays(values)

        file_name = "arrays_test.out"
        try:
            # Generate file
            with open(file_name, 'w') as f:
                a.write_os(f)

            # Check file content
            expected = "<arrays>\n# Number of arrays\n9\n# Mapping array-identifiers/array-names\n1 i\n2 mSize\n3 " \
                       "j\n4 kSize\n5 k\n6 nSize\n7 c\n8 a\n9 b\n</arrays>\n\n"
            with open(file_name, 'r') as f:
                content = f.read()
            self.assertEqual(content, expected)
        except Exception:
            raise
        finally:
            # Erase file
            import os
            os.remove(file_name)

    def test_read_os(self):
        # Store all file content
        import os
        dir_path = os.path.dirname(os.path.realpath(__file__))
        arrays_file = dir_path + "/tests/arrays_test.expected.scop"
        with open(arrays_file, 'r') as f:
            content = f.readlines()

        # Read from file
        arrays, index = Arrays.read_os(content, 0)

        # Check index value
        self.assertEqual(index, len(content))

        # Check Arrays object content
        output_file = dir_path + "/tests/arrays_test.out.scop"
        try:
            # Write to file
            with open(output_file, 'w') as f:
                arrays.write_os(f)

            # Check file content
            with open(arrays_file, 'r') as f:
                expected_content = f.read()
            with open(output_file, 'r') as f:
                output_content = f.read()
            self.assertEqual(output_content, expected_content)
        except Exception:
            raise
        finally:
            # Remove test file
            os.remove(output_file)


#
# MAIN
#

if __name__ == '__main__':
    unittest.main()
