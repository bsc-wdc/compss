#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import logging
import unittest

#
# Logger definition
#

logger = logging.getLogger(__name__)


#
# CodeCleaner class
#

class CodeCleaner(object):

    @staticmethod
    def sort_task_names(task_names_list):
        """
        Sorts the given list of tasks

        :param task_names_list: List of task names to sort
        :return: Sorted list of task names
        """

        return sorted(task_names_list, key=CodeCleaner._task_name_eval)

    @staticmethod
    def contains_import_statement(import_statement, list_of_imports):
        """
        Function to determine if an import already exists in the given list of imports.
        The import must exactly match to another one on the list (if the import is contained in a multi-import
        it will not be detected)

        :param import_statement: Import to evaluate
        :param list_of_imports: List of saved imports
        :return: True if the import already exists, False otherwise
        """

        for i in list_of_imports:
            if CodeCleaner._equal_imports(import_statement, i):
                return True
        return False

    @staticmethod
    def _task_name_eval(task_name):
        """
        Function used to sort the entries of a list of the form [a-zA-Z]*[0-9]*

        :param task_name: name to evaluate
        :return result: Compare value
        """

        import re
        res = []
        for c in re.split('(\d+)', task_name):
            if c.isdigit():
                res.append(int(c))
            else:
                res.append(c)
        return res

    @staticmethod
    def _equal_imports(import_statement1, import_statement2):
        """
        Determines whether two import statements are equal or not

        :param import_statement1: Import statement 1
        :param import_statement2: Import statement 2
        :return: True if import_statement1 is equal to import_statement2, False otherwise
        """

        if len(import_statement1.names) != len(import_statement2.names):
            return False
        for i in range(len(import_statement1.names)):
            import1 = import_statement1.names[i]
            import2 = import_statement2.names[i]
            if import1.name != import2.name or import1.asname != import2.asname:
                return False
        return True


#
# UNIT TESTS
#

class TestCodeCleaner(unittest.TestCase):

    def test_sort_task_names(self):
        tasks = ["S3", "LT5", "S1", "S2", "LT4", "LT7", "S6"]

        new_tasks = CodeCleaner.sort_task_names(tasks)
        expected_tasks = ["LT4", "LT5", "LT7", "S1", "S2", "S3", "S6"]
        self.assertEqual(new_tasks, expected_tasks)

    def test_imports(self):
        # Create many imports
        import ast
        imp1 = ast.parse("import my_module1").body[0]
        imp2 = ast.parse("import my_module2, my_module3").body[0]
        imp3 = ast.parse("from my_module import my_module4").body[0]
        imp4 = ast.parse("from my_module import my_module5, my_module6").body[0]

        imports_list = [imp1, imp2, imp3, imp4]

        # Check existing import
        exist = CodeCleaner.contains_import_statement(imp1, imports_list)
        self.assertTrue(exist)

        new_imp = ast.parse("import my_module4").body[0]
        exist = CodeCleaner.contains_import_statement(new_imp, imports_list)
        self.assertTrue(exist)

        # Check un-existing import
        new_imp = ast.parse("import my_module_false").body[0]
        exist = CodeCleaner.contains_import_statement(new_imp, imports_list)
        self.assertFalse(exist)

        new_imp = ast.parse("import my_module3").body[0]
        exist = CodeCleaner.contains_import_statement(new_imp, imports_list)
        self.assertFalse(exist)


#
# MAIN
#

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(name)s - %(message)s')
    unittest.main()
