#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest

from modules.utils import verify_line

from pycompss.api.api import compss_open
from pycompss.api.api import compss_delete_file
from pycompss.api.api import compss_barrier

from pycompss.api.task import task
from pycompss.api.parameter import FILE_OUT


@task(filepath=FILE_OUT)
def test_basic_types(filepath, b_arg, c_arg, s_arg, by_arg, sh_arg, i_arg, l_arg, f_arg, d_arg):
    """
    Prints into a file the values received as a parameter.

    * @param file filepath where to print all the values
    * @param b boolean value to be printed on the file
    * @param c char value to be printed on the file
    * @param s string value to be printed on the file
    * @param by byte value to be printed on the file
    * @param sh short value to be printed on the file
    * @param i integer value to be printed on the file
    * @param l long value to be printed on the file
    * @param f float value to be printed on the file
    * @param d double value to be printed on the file
    """
    with open(filepath, "w") as f_channel:
        f_channel.write("TEST BASIC TYPES\n")
        f_channel.write("- boolean: " + str(b_arg)+"\n")
        f_channel.write("- char: " + str(c_arg)+"\n")
        f_channel.write("- String: " + s_arg+"\n")
        f_channel.write("- byte: " + str(by_arg)+"\n")
        f_channel.write("- short: " + str(sh_arg)+"\n")
        f_channel.write("- int: " + str(i_arg)+"\n")
        f_channel.write("- long: " + str(l_arg)+"\n")
        f_channel.write("- float: " + str(f_arg)+"\n")
        f_channel.write("- double: " + str(d_arg)+"\n")


class TestBasicTypes(unittest.TestCase):
    """
    Unit Test verifying the execution of a task passing in primitive-type parameters.
    """

    def test_basic_types(self):
        """
        Tries primitive types parameter passing.
        """
        print("Running basic types task")
        filename = "basic_types_file"
        b_val = True
        c_val = 'E'
        s_val = "My Test"
        by_val = 7
        sh_val = 77
        i_val = 777
        l_val = 7777
        f_val = 7.7
        d_val = 7.77777
        test_basic_types(filename, b_val, c_val, s_val, by_val, sh_val, i_val, l_val, f_val, d_val)

        with compss_open(filename, "r") as f_channel:
            line = f_channel.readline()
            verify_line(line, "TEST BASIC TYPES\n")
            line = f_channel.readline()
            verify_line(line, "- boolean: " + str(b_val)+"\n")
            line = f_channel.readline()
            verify_line(line, "- char: " + str(c_val)+"\n")
            line = f_channel.readline()
            verify_line(line, "- String: " + str(s_val)+"\n")
            line = f_channel.readline()
            verify_line(line, "- byte: " + str(by_val)+"\n")
            line = f_channel.readline()
            verify_line(line, "- short: " + str(sh_val)+"\n")
            line = f_channel.readline()
            verify_line(line, "- int: " + str(i_val)+"\n")
            line = f_channel.readline()
            verify_line(line, "- long: " + str(l_val)+"\n")
            line = f_channel.readline()
            verify_line(line, "- float: " + str(f_val)+"\n")
            line = f_channel.readline()
            verify_line(line, "- double: " + str(d_val)+"\n")
            line = f_channel.readline()
            verify_line(line, None)

        compss_delete_file(filename)
        compss_barrier()
        print("\t OK")
