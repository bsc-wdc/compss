#!/usr/bin/python
# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest
from enum import Enum


#
# CONTEXT TYPE ENUMERATION
#

class ContextType(Enum):
    UNDEFINED = -1
    CONTEXT = 2


#
# CONTEXT CLASS
#

class Context(object):
    """
    Represents a global context

    Attributes:
            - context_type : The context type (CONTEXT or UNDEFINED)
            - rows : Number of rows
            - columns : Number of columns
            - output_dims : Number of output dimensions
            - input_dims : Number of input dimensions
            - local_dims : Number of local dimensions
            - params : Number of parameters
    """

    def __init__(self, context_type=ContextType.UNDEFINED, rows=-1, columns=-1, output_dims=-1, input_dims=-1,
                 local_dims=-1, params=-1):
        self.context_type = context_type
        self.rows = rows
        self.columns = columns
        self.output_dims = output_dims
        self.input_dims = input_dims
        self.local_dims = local_dims
        self.params = params

    def get_context_type(self):
        return self.context_type

    def get_rows(self):
        return self.rows

    def get_columns(self):
        return self.columns

    def get_output_dims(self):
        return self.output_dims

    def get_input_dims(self):
        return self.input_dims

    def get_local_dims(self):
        return self.local_dims

    def get_params(self):
        return self.params

    @staticmethod
    def read_os(content, index):
        # Skip header and any annotation
        while content[index].startswith('#') or content[index] == '\n' or content[index] == 'CONTEXT\n':
            index = index + 1

        # Process mandatory field: type rows columns outputDims inputDims localDims params
        line = content[index]
        index = index + 1

        fields = line.split()

        # Skip empty lines, and any annotation
        while index < len(content) and (content[index].startswith('#') or content[index] == '\n'):
            index = index + 1

        # Build Context
        context = Context(ContextType.CONTEXT, *fields)

        # Return structure
        return context, index

    def write_os(self, f):
        # Print type
        print(self.context_type.name, file=f)

        # Print value attributes
        print(str(self.rows) + " " + str(self.columns) + " " + str(self.output_dims) + " " + str(
            self.input_dims) + " " + str(self.local_dims) + " " + str(self.params), file=f)

        # Separator
        print("", file=f)


#
# UNIT TESTS
#

class TestContext(unittest.TestCase):

    def test_empty(self):
        context = Context()

        self.assertEqual(context.get_context_type().name, ContextType.UNDEFINED.name)
        self.assertEqual(context.get_rows(), -1)
        self.assertEqual(context.get_columns(), -1)
        self.assertEqual(context.get_output_dims(), -1)
        self.assertEqual(context.get_input_dims(), -1)
        self.assertEqual(context.get_local_dims(), -1)
        self.assertEqual(context.get_params(), -1)

    def test_full(self):
        context_type = ContextType.CONTEXT
        rows = 0
        cols = 5
        od = 0
        ind = 0
        ld = 0
        params = 3
        context = Context(context_type, rows, cols, od, ind, ld, params)

        self.assertEqual(context.get_context_type().name, context_type.name)
        self.assertEqual(context.get_rows(), rows)
        self.assertEqual(context.get_columns(), cols)
        self.assertEqual(context.get_output_dims(), od)
        self.assertEqual(context.get_input_dims(), ind)
        self.assertEqual(context.get_local_dims(), ld)
        self.assertEqual(context.get_params(), params)

    def test_write_os(self):
        context_type = ContextType.CONTEXT
        rows = 0
        cols = 5
        od = 0
        ind = 0
        ld = 0
        params = 3
        context = Context(context_type, rows, cols, od, ind, ld, params)

        file_name = "context_test.out"
        try:
            # Generate file
            with open(file_name, 'w') as f:
                context.write_os(f)

            # Check file content
            expected = "CONTEXT\n0 5 0 0 0 3\n\n"
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
        context_file = dir_path + "/tests/context_test.expected.scop"
        with open(context_file, 'r') as f:
            content = f.readlines()

        # Read from file
        c, index = Context.read_os(content, 0)

        # Check index value
        self.assertEqual(index, len(content))

        # Check Context object content
        output_file = dir_path + "/tests/context_test.out.scop"
        try:
            # Write to file
            with open(output_file, 'w') as f:
                c.write_os(f)

            # Check file content
            with open(context_file, 'r') as f:
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
