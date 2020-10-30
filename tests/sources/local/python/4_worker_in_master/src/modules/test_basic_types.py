#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest
import numpy as np

from modules.utils import verify_line
from modules.utils import wrapper

from pycompss.api.api import compss_open
from pycompss.api.api import compss_delete_file
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on

from pycompss.api.task import task
from pycompss.api.parameter import FILE_OUT
from pycompss.api.parameter import COLLECTION_IN
from pycompss.api.parameter import COLLECTION_INOUT
from pycompss.api.parameter import DICTIONARY_IN
from pycompss.api.parameter import DICTIONARY_INOUT


@task(filepath=FILE_OUT)
def test_basic_types(filepath, b_arg, c_arg, s_arg, by_arg, sh_arg, i_arg,
                     l_arg, f_arg, d_arg):
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


@task(coll_arg=COLLECTION_IN)
def test_collection_in(coll_arg):
    assert np.array_equal(coll_arg, np.zeros(4))
    return coll_arg


@task(c=COLLECTION_INOUT)
def test_collection_inout(c):
    for elem in c:
        elem += 1.0


@task(dict_coll_arg=DICTIONARY_IN, returns=1)
def test_dict_collection_in(dict_coll_arg):
    assert dict_coll_arg == {1:2, 3:4, 5:6}
    return dict_coll_arg


@task(d=DICTIONARY_INOUT)
def test_dict_collection_inout(d):
    for i in d.keys():
        result = []
        for j in d[i]:
            result.append(j + 1.0)
        d[i] = result


class TestBasicTypes(unittest.TestCase):
    """
    Unit Test verifying the execution of a task passing in primitive-type
    parameters.
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
        test_basic_types(filename, b_val, c_val, s_val, by_val, sh_val,
                         i_val, l_val, f_val, d_val)

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

    def test_collection_in(self):
        """
        Tries COLLECTION_IN parameter passing.
        """
        print("Running collection in task")
        c_arg = np.zeros(4)
        result = test_collection_in(c_arg)
        result = compss_wait_on(result)
        assert np.array_equal(result, c_arg)
        print("\t OK")

    def test_collection_inout(self):
        """
        Tries COLLECTION_INOUT parameter passing.
        """
        print("Running collection inout task")
        c_arg = np.zeros(4)
        test_collection_inout(c_arg)
        c_arg = compss_wait_on(c_arg)
        expected = np.zeros(4)
        for elem in expected:
            elem += 1.0
        assert np.array_equal(c_arg, expected)
        print("\t OK")

    def test_dict_collection_in(self):
        """
        Tries DICTIONARY_IN parameter passing.
        """
        print("Running dictionary collection in task")
        d_arg = {1: 2, 3: 4, 5: 6}
        result = test_dict_collection_in(d_arg)
        result = compss_wait_on(result)
        assert result == d_arg
        print("\t OK")

    def test_dict_collection_inout(self):
        """
        Tries DICTIONARY_INOUT parameter passing.
        """
        print("Running dictionary collection inout task")
        d_arg = {wrapper(1): np.zeros(1), wrapper(3): np.zeros(3), wrapper(5): np.zeros(5)}
        test_dict_collection_inout(d_arg)
        d_arg = compss_wait_on(d_arg)
        # not exactly the expected since we will check the key from the
        # d_arg[].content
        expected = {1: np.zeros(1), 3: np.zeros(3), 5: np.zeros(5)}
        for i in expected.keys():
            result = []
            for j in expected[i]:
                result.append(j + 1.0)
            expected[i] = result
        # Check keys
        dkeys = []
        for i in d_arg.keys():
            dkeys.append(i.content)
        ekeys = expected.keys()
        assert sorted(dkeys) == sorted(ekeys)
        # Check contents
        print(str(d_arg.items()))
        for k, v in d_arg.items():
            assert expected[k.content] == v
        print("\t OK")
