#!/usr/bin/python
# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest


class Coordinates(object):
    """
    Represents a coordinates object inside the extensions section

    Attributes:
            - file_name : Name of the original source file
            - start_line : Start line of the scop tag in the original source file
            - start_col : Start column of the scop tag in the original source file
            - end_line : End line of the scop tag in the original source file
            - end_col : End column of the scop tag in the original source file
            - indent : Indentation value in the original source file
    """

    def __init__(self, file_name=None, start_line=-1, start_col=-1, end_line=-1, end_col=-1, indent=-1):
        self.file_name = file_name
        self.start_line = start_line
        self.start_col = start_col
        self.end_line = end_line
        self.end_col = end_col
        self.indent = indent

    def get_file_name(self):
        return self.file_name

    def get_start_line(self):
        return self.start_line

    def get_start_column(self):
        return self.start_col

    def get_end_line(self):
        return self.end_line

    def get_end_column(self):
        return self.end_col

    def get_indentation(self):
        return self.indent

    @staticmethod
    def read_os(content, index):
        # Skip header and any annotation
        while content[index].startswith('#') or content[index] == '\n' or content[index] == '<coordinates>\n':
            index = index + 1

        # Process mandatory field: file_name
        file_name = content[index].strip()
        index = index + 1

        # Skip empty lines and any annotation
        while content[index].startswith('#') or content[index] == '\n':
            index = index + 1

        # Process mandatory field: start
        start_line, start_col = content[index].split()
        index = index + 1

        # Skip empty lines and any annotation
        while content[index].startswith('#') or content[index] == '\n':
            index = index + 1

        # Process mandatory field: end
        end_line, end_col = content[index].split()
        index = index + 1

        # Skip empty lines and any annotation
        while content[index].startswith('#') or content[index] == '\n':
            index = index + 1

        # Process mandatory field: indentation
        indentation = content[index].strip()
        index = index + 1

        # Skip empty lines, any annotation, and footer
        while index < len(content) and (
                content[index].startswith('#') or content[index] == '\n' or content[index] == '</coordinates>\n'):
            index = index + 1

        # Build Coordinates
        c = Coordinates(file_name, start_line, start_col, end_line, end_col, indentation)

        # Return structure
        return c, index

    def write_os(self, f):
        # Print header
        print("<coordinates>", file=f)

        # Print file name
        print("# File name", file=f)
        print(str(self.file_name), file=f)

        # Print starting line and column
        print("# Starting line and column", file=f)
        print(str(self.start_line) + " " + str(self.start_col), file=f)

        # Print ending line and column
        print("# Ending line and column", file=f)
        print(str(self.end_line) + " " + str(self.end_col), file=f)

        # Print indentation
        print("# Indentation", file=f)
        print(str(self.indent), file=f)

        # Print footer
        print("</coordinates>", file=f)
        print("", file=f)


#
# UNIT TESTS
#

class TestCoordinates(unittest.TestCase):

    def test_empty(self):
        c = Coordinates()

        self.assertEqual(c.get_file_name(), None)
        self.assertEqual(c.get_start_line(), -1)
        self.assertEqual(c.get_start_column(), -1)
        self.assertEqual(c.get_end_line(), -1)
        self.assertEqual(c.get_end_column(), -1)
        self.assertEqual(c.get_indentation(), -1)

    def test_full(self):
        file_name = "example2_src_matmul.cc"
        sl = 72
        sc = 0
        el = 80
        ec = 0
        indent = 4
        c = Coordinates(file_name, sl, sc, el, ec, indent)

        self.assertEqual(c.get_file_name(), file_name)
        self.assertEqual(c.get_start_line(), sl)
        self.assertEqual(c.get_start_column(), sc)
        self.assertEqual(c.get_end_line(), el)
        self.assertEqual(c.get_end_column(), ec)
        self.assertEqual(c.get_indentation(), indent)

    def test_write_os(self):
        fn = "example2_src_matmul.cc"
        sl = 72
        sc = 0
        el = 80
        ec = 0
        indent = 4
        c = Coordinates(fn, sl, sc, el, ec, indent)

        file_name = "coordinates_test.out"
        try:
            # Generate file
            with open(file_name, 'w') as f:
                c.write_os(f)

            # Check file content
            expected = "<coordinates>\n# File name\nexample2_src_matmul.cc\n# Starting line and column\n72 0\n# " \
                       "Ending line and column\n80 0\n# Indentation\n4\n</coordinates>\n\n"
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
        coordinates_file = dir_path + "/tests/coordinates_test.expected.scop"
        with open(coordinates_file, 'r') as f:
            content = f.readlines()

        # Read from file
        coordinates, index = Coordinates.read_os(content, 0)

        # Check index value
        self.assertEqual(index, len(content))

        # Check Scatnames object content
        output_file = dir_path + "/tests/coordinates_test.out.scop"
        try:
            # Write to file
            with open(output_file, 'w') as f:
                coordinates.write_os(f)

            # Check file content
            with open(coordinates_file, 'r') as f:
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
