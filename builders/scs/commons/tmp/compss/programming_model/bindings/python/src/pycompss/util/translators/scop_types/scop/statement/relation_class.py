#!/usr/bin/python
# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest
from enum import Enum


#
# RELATION TYPE ENUMERATION
#

class RelationType(Enum):
    UNDEFINED = -1
    CONTEXT = 2
    DOMAIN = 3
    SCATTERING = 4
    READ = 6
    WRITE = 7
    MAY_WRITE = 8


#
# RELATION CLASS
#

class Relation(object):
    """
    Represents a Relation within a statement

    Attributes:
            - relation_type : The relation type (DOMAIN, SCATTERING, READ, WRITE)
            - rows : Number of rows
            - columns : Number of columns
            - output_dims : Number of output dimensions
            - input_dims : Number of input dimensions
            - local_dims : Number of local dimensions
            - params : Number of parameters
            - constraint_matrix : Matrix of constraints
    """

    def __init__(self, relation_type=RelationType.UNDEFINED, rows=-1, columns=-1, output_dims=-1, input_dims=-1,
                 local_dims=-1, params=-1, constraint_matrix=None):
        self.relation_type = relation_type
        self.rows = rows
        self.columns = columns
        self.output_dims = output_dims
        self.input_dims = input_dims
        self.local_dims = local_dims
        self.params = params
        self.constraint_matrix = constraint_matrix

    def get_relation_type(self):
        return self.relation_type

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

    def get_constraint_matrix(self):
        return self.constraint_matrix

    @staticmethod
    def read_os(content, index):
        # Skip header and any annotation
        while content[index].startswith('#') or content[index] == '\n':
            index = index + 1

        # Process mandatory field: type
        type_ind = content[index].strip()
        index = index + 1
        rel_type = RelationType[type_ind]

        # Process mandatory fields: dimensions
        dims = content[index].split()
        index = index + 1
        rows = int(dims[0])
        cols = int(dims[1])
        o_dims = int(dims[2])
        i_dims = int(dims[3])
        l_dims = int(dims[4])
        params = int(dims[5])

        # Process constraint matrix
        c_matrix = []
        for _ in range(rows):
            row_vals = content[index].split()
            index = index + 1
            c_matrix.append(row_vals)

        # Skip empty lines and any annotation
        while index < len(content) and (content[index].startswith('#') or content[index] == '\n'):
            index = index + 1

        # Build relation
        rel = Relation(rel_type, rows, cols, o_dims, i_dims, l_dims, params, c_matrix)

        # Return structure
        return rel, index

    def write_os(self, f):
        # Print type
        print(self.relation_type.name, file=f)

        # Print value attributes
        print(str(self.rows) + " " + str(self.columns) + " " + str(self.output_dims) + " " + str(
            self.input_dims) + " " + str(self.local_dims) + " " + str(self.params), file=f)

        # Print constraint matrix
        if self.constraint_matrix is not None:
            for constraintRow in self.constraint_matrix:
                line = ""
                for value in constraintRow:
                    line = line + str(value) + "\t"
                print(line, file=f)
        print("", file=f)


#
# UNIT TESTS
#

class TestRelation(unittest.TestCase):

    def test_empty(self):
        relation = Relation()

        self.assertEqual(relation.get_relation_type().name, RelationType.UNDEFINED.name)
        self.assertEqual(relation.get_rows(), -1)
        self.assertEqual(relation.get_columns(), -1)
        self.assertEqual(relation.get_output_dims(), -1)
        self.assertEqual(relation.get_input_dims(), -1)
        self.assertEqual(relation.get_local_dims(), -1)
        self.assertEqual(relation.get_params(), -1)
        self.assertEqual(relation.get_constraint_matrix(), None)

    def test_full(self):
        rel_type = RelationType.DOMAIN
        rows = 9
        cols = 8
        od = 3
        ind = 2
        ld = 0
        params = 1
        matrix = [[1, -1], [1, -1]]
        relation = Relation(rel_type, rows, cols, od, ind, ld, params, matrix)

        self.assertEqual(relation.get_relation_type().name, rel_type.name)
        self.assertEqual(relation.get_rows(), rows)
        self.assertEqual(relation.get_columns(), cols)
        self.assertEqual(relation.get_output_dims(), od)
        self.assertEqual(relation.get_input_dims(), ind)
        self.assertEqual(relation.get_local_dims(), ld)
        self.assertEqual(relation.get_params(), params)
        self.assertEqual(relation.get_constraint_matrix(), matrix)

    def test_write_os(self):
        rel_type = RelationType.DOMAIN
        rows = 9
        cols = 8
        od = 3
        ind = 2
        ld = 0
        params = 1
        matrix = [[1, -1], [1, -1]]
        relation = Relation(rel_type, rows, cols, od, ind, ld, params, matrix)

        file_name = "relation_test.out"
        try:
            # Generate file

            with open(file_name, 'w') as f:
                relation.write_os(f)

            # Check file content
            expected = "DOMAIN\n9 8 3 2 0 1\n1\t-1\t\n1\t-1\t\n\n"
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
        relation_file = dir_path + "/tests/relation_test.expected.scop"
        with open(relation_file, 'r') as f:
            content = f.readlines()

        # Read from file
        rel, index = Relation.read_os(content, 0)

        # Check index value
        self.assertEqual(index, len(content))

        # Check Relation object content
        output_file = dir_path + "/tests/relation_test.out.scop"
        try:
            # Write to file
            with open(output_file, 'w') as f:
                rel.write_os(f)

            # Check file content
            with open(relation_file, 'r') as f:
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
