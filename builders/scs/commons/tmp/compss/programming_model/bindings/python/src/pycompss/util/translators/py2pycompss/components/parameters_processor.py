#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import logging
import unittest
import ast

#
# Logger definition
#

logger = logging.getLogger(__name__)


#
# ParametersProcessor class
#

class ParametersProcessor(object):

    @staticmethod
    def process_parameters(statement):
        """
        Processes all the directions of the parameters found in the given statement

        :param statement: AST node representing the head of the statement
        :return in_vars: List of names of IN variables
        :return out_vars: List of names of OUT variables
        :return inout_vars: List of names of INOUT variables
        :return return_vars: List of names of RETURN variables
        :raise Py2PyCOMPSsParametersException: For unrecognised target types
        """

        in_vars = ParametersProcessor._get_access_vars(statement)

        out_vars = []
        inout_vars = []
        return_vars = []

        target_vars = ParametersProcessor._get_target_vars(statement)
        if isinstance(statement, ast.Assign) and isinstance(statement.value, ast.Call):
            # Target vars are the return of a function
            return_vars = target_vars
        else:
            # Target vars are the result of an expression
            out_vars = target_vars

        # Fix duplicate variables and directions
        fixed_in_vars = []
        fixed_out_vars = []
        fixed_inout_vars = []
        fixed_return_vars = return_vars
        for iv in in_vars:
            if iv in out_vars or iv in inout_vars:
                if iv not in fixed_inout_vars:
                    fixed_inout_vars.append(iv)
            else:
                if iv not in fixed_in_vars:
                    fixed_in_vars.append(iv)
        for ov in out_vars:
            if ov in in_vars or ov in inout_vars:
                if ov not in fixed_inout_vars:
                    fixed_inout_vars.append(ov)
            else:
                if ov not in fixed_out_vars:
                    fixed_out_vars.append(ov)
        for iov in inout_vars:
            if iov not in fixed_inout_vars:
                fixed_inout_vars.append(iov)

        # Return variables
        return fixed_in_vars, fixed_out_vars, fixed_inout_vars, fixed_return_vars

    @staticmethod
    def _get_access_vars(statement, is_target=False):
        """
        Returns the accessed variable names within the given expression

        :param statement: AST node representing the head of the statement
        :param is_target: Boolean to indicate if we are inside a target node or not
        :return in_vars: List of names of accessed variables
        """

        # Direct case
        if isinstance(statement, ast.Name):
            if is_target:
                return []
            else:
                return [statement.id]

        # Child recursion
        in_vars = []
        for field, value in ast.iter_fields(statement):
            if field == "func" or field == "keywords":
                # Skip function names and var_args keywords
                pass
            else:
                children_are_target = is_target or (field == "targets")
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, ast.AST):
                            in_vars.extend(ParametersProcessor._get_access_vars(item, children_are_target))
                elif isinstance(value, ast.AST):
                    in_vars.extend(ParametersProcessor._get_access_vars(value, children_are_target))
        return in_vars

    @staticmethod
    def _get_target_vars(statement):
        """
        Returns the target variables within given the expression

        :param statement: AST node representing the head of the statement
        :return target_vars: List of names of target variables
        :raise Py2PyCOMPSsParametersException: For unrecognised target types
        """

        if isinstance(statement, ast.Assign):
            # Assign can have more than one target var, process all
            target_vars = []
            for t in statement.targets:
                target_vars.extend(ParametersProcessor._get_target_vars(t))
            return target_vars
        elif isinstance(statement, ast.AugAssign):
            # Operations on assign have a single target var
            return ParametersProcessor._get_target_vars(statement.target)
        elif isinstance(statement, ast.Name):
            # Add Id of used variable
            return [statement.id]
        elif isinstance(statement, ast.Subscript):
            # On array access process value (not indexes)
            return ParametersProcessor._get_target_vars(statement.value)
        elif isinstance(statement, ast.Expr):
            # No target on void method call
            return []
        elif isinstance(statement, ast.List):
            # Process all the elements of the list
            target_vars = []
            for list_fields in statement.elts:
                target_vars.extend(ParametersProcessor._get_target_vars(list_fields))
            return target_vars
        elif isinstance(statement, ast.Tuple):
            # Process all the fields of the tuple
            target_vars = []
            for tuple_field in statement.elts:
                target_vars.extend(ParametersProcessor._get_target_vars(tuple_field))
            return target_vars
        else:
            # Unrecognised statement expression
            raise Py2PyCOMPSsParametersException(
                "[ERROR] Unrecognised expression on write operation " + str(type(statement)))


#
# Exception Class
#

class Py2PyCOMPSsParametersException(Exception):

    def __init__(self, msg=None, nested_exception=None):
        self.msg = msg
        self.nested_exception = nested_exception

    def __str__(self):
        s = "Exception on Py2PyCOMPSs.translate.process_parameters method.\n"
        if self.msg is not None:
            s = s + "Message: " + str(self.msg) + "\n"
        if self.nested_exception is not None:
            s = s + "Nested Exception: " + str(self.nested_exception) + "\n"
        return s


#
# UNIT TESTS
#

class TestParametersProcessor(unittest.TestCase):
    pass


#
# MAIN
#

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(name)s - %(message)s')
    unittest.main()
