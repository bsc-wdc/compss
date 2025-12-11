#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest
import time

from pycompss.api.task import task

# from decorator import decorator

from pycompss.functions.elapsed_time import timeit

@timeit()
@task(returns=1)
def function_time_decorated_master(x):
    return x**3

@task(returns=1)
@timeit()
def function_time_decorated_worker(x):
    return x**3


class testDecoratorRetInt(unittest.TestCase):

    def setUp(self):
        self.x = 2

    def test_decorator_master(self):
        """ Test time decorator master"""
        from pycompss.api.api import compss_wait_on
        o = function_time_decorated_master(self.x)
        o = compss_wait_on(o)
        res = o[0]
        print('RES IS EQUAL TO %s' % str(res))
        time = o[1]
        #print '(master time) {}'.format(time)
        self.assertEqual(res, self.x**3)

    def test_decorator_worker(self):
        """ Test time decorator worker"""
        from pycompss.api.api import compss_wait_on
        o = function_time_decorated_worker(self.x)
        o = compss_wait_on(o)
        res = o[0]
        time = o[1]
        #print '(worker time) {}'.format(time)
        self.assertEqual(res, self.x**3)

    def test_decorator_worker_list(self):
        """ Test time decorator with list worker"""
        from pycompss.api.api import compss_wait_on
        tasks = [function_time_decorated_worker(self.x) for _ in range(5)]
        tasks = compss_wait_on(tasks)
        values = [t[0] for t in tasks]
        times = [t[1] for t in tasks]
        #print '(master times) {}'.format(times)
        self.assertSequenceEqual(values, [self.x**3 for _ in range(5)])

    def test_decorator_master_list(self):
        """ Test time decorator with list master"""
        from pycompss.api.api import compss_wait_on
        tasks = [function_time_decorated_master(self.x) for _ in range(5)]
        values = [t[0] for t in tasks]
        times = [t[1] for t in tasks]
        values = compss_wait_on(values)
        #print '(worker times) {}'.format(times)
        self.assertSequenceEqual(values, [self.x**3 for _ in range(5)])

    def tearDown(self):
        self.x = None
