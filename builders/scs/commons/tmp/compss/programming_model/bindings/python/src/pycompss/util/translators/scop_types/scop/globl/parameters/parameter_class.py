#!/usr/bin/python
# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest


class Parameter(object):
    """
    Represents a global parameter

    Attributes:
            - ptype : Type of the parameter
            - pvalue : Value of the parameter
    """

    def __init__(self, ptype=None, values_list=None):
        self.ptype = ptype
        if values_list is None:
            self.pvalue = None
        else:
            self.pvalue = " ".join(values_list)

    def get_type(self):
        return self.ptype

    def get_value(self):
        return self.pvalue

    @staticmethod
    def read_os(content, index):
        # Skip header and any annotation
        while content[index].startswith('#') or content[index] == '\n':
            index = index + 1

        # Process optional header
        ptype = None
        if content[index].startswith('<'):
            ptype = content[index][1:-2]
            index = index + 1

        # Process mandatory field: value
        values_list = content[index].split()
        index = index + 1

        # Process optional footer
        if content[index].startswith('</'):
            index = index + 1

        # Skip empty lines and any annotation
        while index < len(content) and (content[index].startswith('#') or content[index] == '\n'):
            index = index + 1

        # Build Parameter
        p = Parameter(ptype, values_list)

        # Return structure
        return p, index

    def write_os(self, f):
        # Print header
        print("<" + str(self.ptype) + ">", file=f)
        print(str(self.pvalue), file=f)
        print("</" + str(self.ptype) + ">", file=f)


#
# UNIT TESTS
#

class TestParameter(unittest.TestCase):

    def test_empty(self):
        param = Parameter()
        self.assertEqual(param.get_type(), None)
        self.assertEqual(param.get_value(), None)

    def test_full(self):
        t = "strings"
        val_list = ["mSize", "kSize", "nSize"]
        param = Parameter(t, val_list)

        val = "mSize kSize nSize"
        self.assertEqual(param.get_type(), t)
        self.assertEqual(param.get_value(), val)

    def test_write_os(self):
        t = "strings"
        val_list = ["mSize", "kSize", "nSize"]
        param = Parameter(t, val_list)

        file_name = "parameter_test.out"
        try:
            # Generate file
            with open(file_name, 'w') as f:
                param.write_os(f)

            # Check file content
            expected = "<strings>\nmSize kSize nSize\n</strings>\n"
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
        parameter_file = dir_path + "/tests/parameter_test.expected.scop"
        with open(parameter_file, 'r') as f:
            content = f.readlines()

        # Read from file
        p, index = Parameter.read_os(content, 0)

        # Check index value
        self.assertEqual(index, len(content))

        # Check Parameter object content
        output_file = dir_path + "/tests/parameter_test.out.scop"
        try:
            # Write to file
            with open(output_file, 'w') as f:
                p.write_os(f)

            # Check file content
            with open(parameter_file, 'r') as f:
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
