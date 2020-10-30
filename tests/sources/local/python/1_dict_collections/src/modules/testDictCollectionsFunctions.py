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
from pycompss.runtime.management.classes import Future

from my_class import wrapper
import numpy as np


@task(dict_coll_arg=DICTIONARY_IN, returns=1)
def test_dict_collection_in(dict_coll_arg):
    assert dict_coll_arg == {1:2, 3:4, 5:6}
    return dict_coll_arg


@task(dict_coll_arg={Type: DICTIONARY_IN, Depth: 2}, returns=1)
def test_dict_collection_in_deeper(dict_coll_arg):
    assert dict_coll_arg == {1:{2:'a'}, 3:{ 4:'b'}, 5:{6:'c'}}
    return dict_coll_arg


@task(d=DICTIONARY_INOUT)
def test_dict_collection_inout(d):
    for i in d.keys():
        result = []
        for j in d[i]:
            result.append(j + 1.0)
        d[i] = result


@task(d=DICTIONARY_OUT)
def test_dict_collection_out(d):
    d[1] = np.zeros(1)
    d[3] = np.zeros(3)
    d[5] = np.zeros(5)


@task(returns=1)
def increment(v):
    return v + 1

@task(returns=1)
def increment_wrapper(v):
    return wrapper(v.content + 1)

@task(returns=1, d=DICTIONARY_IN)
def check_collection(d):
    result = True
    if d['a'] != 2:
        result = False
    if d['b'] != 3:
        result = False
    if d['c'] != 4:
        result = False
    return result


@task(d=DICTIONARY_INOUT)
def increment_collection(d):
    for k in d.keys():
        d[k].content += 1


class testDictCollectionFunctions(unittest.TestCase):

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

    def test_dict_collection_in_deeper(self):
        """
        Tries DICTIONARY_IN parameter passing depth 2.
        """
        print("Running dictionary collection in task depth 2")
        d_arg = {1:{2:'a'}, 3:{4:'b'}, 5:{6:'c'}}
        result = test_dict_collection_in_deeper(d_arg)
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

    @unittest.skip("DICTIONARY_OUT NOT SUPPORTED")
    def test_dict_collection_out(self):
        """
        Tries DICTIONARY_OUT parameter passing
        """
        print("Running dictionary collection out task")
        d_arg = {}
        test_dict_collection_out(d_arg)
        d_arg = compss_wait_on(d_arg)
        print(d_arg)
        # TODO: add d_arg check

    def test_dependencies_1(self):
        data = {'a': 1, 'b': 2, 'c': 3}
        for k, v in data.items():
            data[k] = increment(v)
            assert isinstance(data[k], Future)
        result = check_collection(data)
        assert result, "Received unexpected parameters checking the collection."

    def test_dependencies_2(self):
        data1 = {'a': wrapper(1), 'b': wrapper(2), 'c': wrapper(3)}
        for k, v in data1.items():
            data1[k] = increment_wrapper(v)
        data2 = {'d': wrapper(4), 'a': wrapper(5), 'b': wrapper(6)}  # reuses a and  b
        for k, v in data2.items():
            data2[k] = increment_wrapper(v)
        data1.update(data2)  # join two dicts
        increment_collection(data1)
        result = compss_wait_on(data1)
        processed_result = {}
        for k, v in result.items():
            processed_result[k] = v.content
        assert processed_result == {'a': 7, 'b': 8, 'c': 5, 'd': 6}, "Unexpected result."
