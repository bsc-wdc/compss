#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
==================
"""

# Imports
import unittest
import numpy as np
from pycompss.api.task import task
from pycompss.api.api import compss_wait_on
from pycompss.api.parameter import *
from pycompss.api.on_failure import on_failure


@on_failure(management ='FAIL')
@task(returns=1, cache_returns=True)
def producer():
    obj = np.random.random(1000)
    return obj # This will be put into cache


class testEviction(unittest.TestCase):

    def testCacheNumpyArray(self):
        objs = []
        # It will only fit 125 into cache
        for _ in range(200):
            objs.append(producer())
        objs = compss_wait_on(objs)
        # print("Results: " + str(objs))
