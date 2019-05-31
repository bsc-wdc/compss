#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task
from pycompss.api.parameter import *

from pycompss.api.constraint import constraint
from pycompss.api.implement import implement
from pycompss.api.multinode import multinode


@task(returns=1)
def level1(value):
    return value + level2(value)

@task(returns=1)
def level2(value):
    return value + level3(value)

@task(returns=1)
def level3(value):
    return value * 2

@constraint(computing_units="2")
@task(returns=1)
def level1c(value):
    return level2c(value) / value

@constraint(computing_units="2")
@task(returns=1)
def level2c(value):
    return value + level3c(value)

@constraint(computing_units="2")
@task(returns=1)
def level3c(value):
    return value * 2

# force not to run this task, just the implementation
@constraint(computing_units="100")
@task(returns=int)
def slow_task(value):
    # 80 + 60 - 40 = 100
    return level1(value) + level2c(value) - level3(value)

@implement(source_class="modules.testTaskInTask", method="slow_task")
@constraint(computing_units="1")
@task(returns=list)
def fast_task(value):
    # 3 + 60 + 40 = 103
    return level1c(value) + level2(value) + level3c(value)

@constraint(computing_units="2")
@multinode(computing_nodes="2")
@task(returns=1)
def multi_node_task(value):
    # 80 * 60 * 40 = 192000
    return level1(value) * level2c(value) * level3(value)


class testTaskInTask(unittest.TestCase):

    def testTaskInTask(self):
        result = level1(1)
        result = compss_wait_on(result)
        self.assertEqual(result, 4)

    def testTaskInTaskConstraints(self):
        result = level1c(2)
        result = compss_wait_on(result)
        self.assertEqual(result, 3)

    def testImplementArgWarn(self):
        result = slow_task(20)
        result = compss_wait_on(result)
        self.assertEqual(result, 3 + 60 + 40)

    def testMultinodeArgWarn(self):
        result = multi_node_task(20)
        result = compss_wait_on(result)
        self.assertEqual(result, 80 * 60 * 40)
