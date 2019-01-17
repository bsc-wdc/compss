#!/usr/bin/python
# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest


class StatementExtension(object):
    """
    Represents an Extension within a Statement

    Attributes:
            - original_iterators : List of original iterators
            - expr : Statement body expression
    """

    def __init__(self, original_iterators=None, expr=None):
        self.original_iterators = original_iterators
        self.expr = expr

    def get_number_original_iterators(self):
        if self.original_iterators is not None:
            return len(self.original_iterators)
        else:
            return -1

    def get_original_iterators(self):
        return self.original_iterators

    def get_expr(self):
        return self.expr

    @staticmethod
    def read_os(content, index):
        # Skip header and any annotation
        while content[index].startswith('#') or content[index] == '\n':
            index = index + 1

        # Skip body header, empty lines and annotations
        while index < len(content) and (
                content[index].startswith('<') or content[index].startswith('#') or content[index] == '\n'):
            index = index + 1

        # Skip iterators size
        # iterators_size = int(content[index].strip())
        index = index + 1

        # Skip empty lines and annotations
        while index < len(content) and (content[index].startswith('#') or content[index] == '\n'):
            index = index + 1

        # Process iterators
        iters = content[index].split()
        index = index + 1

        # Skip empty lines and annotations
        while index < len(content) and (content[index].startswith('#') or content[index] == '\n'):
            index = index + 1

        # Process expression
        expr = content[index].strip()
        index = index + 1

        # Skip footer, empty lines, and any annotation
        while index < len(content) and (
                content[index].startswith('</') or content[index].startswith('#') or content[index] == '\n'):
            index = index + 1

        # Build statement extension
        se = StatementExtension(iters, expr)

        # Return structure
        return se, index

    def write_os(self, f):
        # Print header
        print("<body>", file=f)

        # Print number of original iterators
        print("# Number of original iterators", file=f)
        if self.original_iterators is not None:
            print(str(len(self.original_iterators)), file=f)
        else:
            print("0", file=f)

        # Print original iterators
        print("# List of original iterators", file=f)
        line = ""
        if self.original_iterators is not None:
            for elem in self.original_iterators:
                line = line + str(elem) + " "
        print(line, file=f)

        # Print statement expression
        print("# Statement body expression", file=f)
        print(self.expr, file=f)

        # Print footer
        print("</body>", file=f)


#
# UNIT TESTS
#

class TestStatementExtension(unittest.TestCase):

    def test_empty(self):
        extension = StatementExtension()
        self.assertEqual(extension.get_number_original_iterators(), -1)
        self.assertEqual(extension.get_original_iterators(), None)
        self.assertEqual(extension.get_expr(), None)

    def test_full(self):
        iterators = ["i", "j", "k"]
        expr = "c[i][j] += a[i][k]*b[k][j];"
        extension = StatementExtension(iterators, expr)
        self.assertEqual(extension.get_number_original_iterators(), 3)
        self.assertEqual(extension.get_original_iterators(), iterators)
        self.assertEqual(extension.get_expr(), expr)

    def test_write_os(self):
        iterators = ["i", "j", "k"]
        expr = "c[i][j] += a[i][k]*b[k][j];"
        extension = StatementExtension(iterators, expr)

        file_name = "extension_test.out"
        try:
            # Generate file
            with open(file_name, 'w') as f:
                extension.write_os(f)

            # Check file content
            expected = "<body>\n# Number of original iterators\n3\n# List of original iterators\ni j k \n# Statement " \
                       "body expression\nc[i][j] += a[i][k]*b[k][j];\n</body>\n"
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
        s_ext_file = dir_path + "/tests/statement_extension_test.expected.scop"
        with open(s_ext_file, 'r') as f:
            content = f.readlines()

        # Read from file
        ext, index = StatementExtension.read_os(content, 0)

        # Check index value
        self.assertEqual(index, len(content))

        # Check StatementExtension object content
        output_file = dir_path + "/tests/statement_extension_test.out.scop"
        try:
            # Write to file
            with open(output_file, 'w') as f:
                ext.write_os(f)

            # Check file content
            with open(s_ext_file, 'r') as f:
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
