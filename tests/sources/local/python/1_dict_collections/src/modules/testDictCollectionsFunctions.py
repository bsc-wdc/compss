#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest

from pycompss.api.api import compss_wait_on
from pycompss.api.parameter import *
from pycompss.api.task import task

from my_class import wrapper
import numpy as np


@task(dict_coll_arg=DICT_COLLECTION_IN, returns=1)
def test_dict_collection_in(dict_coll_arg):
    assert dict_coll_arg == {1:2, 3:4, 5:6}
    return dict_coll_arg


@task(dict_coll_arg={Type: DICT_COLLECTION_IN, Depth: 2}, returns=1)
def test_dict_collection_in_deeper(dict_coll_arg):
    assert dict_coll_arg == {1:{2:'a'}, 3:{ 4:'b'}, 5:{6:'c'}}
    return dict_coll_arg


@task(d=DICT_COLLECTION_INOUT)
def test_dict_collection_inout(d):
    for i in d.keys():
        result = []
        for j in d[i]:
            result.append(j + 1.0)
        d[i] = result


@task(d=DICT_COLLECTION_OUT)
def test_dict_collection_out(d):
    d[1] = np.zeros(1)
    d[3] = np.zeros(3)
    d[5] = np.zeros(5)



class testDictCollectionFunctions(unittest.TestCase):

    def test_dict_collection_in(self):
        """
        Tries DICT_COLLECTION_IN parameter passing.
        """
        print("Running dictionary collection in task")
        d_arg = {1: 2, 3: 4, 5: 6}
        result = test_dict_collection_in(d_arg)
        result = compss_wait_on(result)
        assert result == d_arg
        print("\t OK")

    def test_dict_collection_in_deeper(self):
        """
        Tries DICT_COLLECTION_IN parameter passing depth 2.
        """
        print("Running dictionary collection in task depth 2")
        d_arg = {1:{2:'a'}, 3:{4:'b'}, 5:{6:'c'}}
        result = test_dict_collection_in_deeper(d_arg)
        result = compss_wait_on(result)
        assert result == d_arg
        print("\t OK")

    def test_dict_collection_inout(self):
        """
        Tries DICT_COLLECTION_INOUT parameter passing.
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

    @unittest.skip("DICT_COLLECTION_OUT NOT SUPPORTED")
    def test_dict_collection_out(self):
        """
        Tries DICT_COLLECTION_OUT parameter passing
        """
        print("Running dictionary collection out task")
        d_arg = {}
        test_dict_collection_out(d_arg)
        d_arg = compss_wait_on(d_arg)
        print(d_arg)
        # TODO: add d_arg check
