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
# Header Builder class
#

class HeaderBuilder(object):

    @staticmethod
    def build_task_header(in_vars, out_vars, inout_vars, return_vars, task_star_args, len_var):
        """
        Constructs the task header corresponding to the given IN, OUT, and INOUT variables

        :param in_vars: List of names of IN variables
        :param out_vars: List of names of OUT variables
        :param inout_vars: List of names of INOUT variables
        :param return_vars: List of names of RETURN variables
        :param task_star_args: List of variables that will be passed as star arguments
        :param len_var: Variable containing the name of the global variable used for star_args length
        :return task_header: String representing the PyCOMPSs task header
        """

        # Construct task header
        task_header = "@task("

        # Add parameters information
        first = True
        for iv in in_vars:
            if iv not in task_star_args:
                if not first:
                    task_header += ", "
                else:
                    first = False
                task_header += iv + "=IN"
        for ov in out_vars:
            if ov not in task_star_args:
                if not first:
                    task_header += ", "
                else:
                    first = False
                task_header += ov + "=OUT"
        for iov in inout_vars:
            if iov not in task_star_args:
                if not first:
                    task_header += ", "
                else:
                    first = False
                task_header += iov + "=INOUT"

        # Add return information
        if len(task_star_args) > 0:
            if not first:
                task_header += ", "
            task_header += "returns=\"" + len_var + "\""
        elif len(return_vars) > 0:
            if not first:
                task_header += ", "
            task_header += "returns=" + str(len(return_vars))

        # Close task header
        task_header += ")"

        return task_header

    @staticmethod
    def split_task_header(header):
        """
        Constructs a map containing all the variables of the task header and its directionality (IN, OUT, INOUT)

        :param header: String containing the task header
        :return args2dirs: Map containing the task variables and its directionality
        """

        header = header.replace("@task(", "")
        header = header.replace(")", "")

        args2dirs = {}
        arguments = header.split(", ")
        for entry in arguments:
            argument, direction = entry.split("=")
            args2dirs[argument] = direction

        return args2dirs


#
# UNIT TESTS
#

class TestHeaderBuilder(unittest.TestCase):

    def test_regular_header(self):
        in_vars = ["in1", "in2"]
        out_vars = ["out1", "out2"]
        inout_vars = ["inout1", "inout2"]
        return_vars = ["r1", "r2", "r3"]
        task_star_args = []
        len_var = None

        header_got = HeaderBuilder.build_task_header(in_vars, out_vars, inout_vars, return_vars, task_star_args,
                                                     len_var)
        header_expected = "@task(in1=IN, in2=IN, out1=OUT, out2=OUT, inout1=INOUT, inout2=INOUT, returns=3)"
        self.assertEqual(header_got, header_expected)

    def test_starargs_header(self):
        in_vars = ["in1", "in2", "var1"]
        out_vars = []
        inout_vars = ["inout1", "inout2", "var2"]
        return_vars = []
        task_star_args = ["var1", "var2", "var3"]
        len_var = "task_len_var"

        header_got = HeaderBuilder.build_task_header(in_vars, out_vars, inout_vars, return_vars, task_star_args,
                                                     len_var)
        header_expected = "@task(in1=IN, in2=IN, inout1=INOUT, inout2=INOUT, returns=\"task_len_var\")"
        self.assertEqual(header_got, header_expected)


#
# MAIN
#

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(name)s - %(message)s')
    unittest.main()
