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
from pycompss.api.constraint import constraint
from pycompss.api.implement import implement
from pycompss.api.multinode import multinode
from .external import external
from .external import externalc
from .external import example


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


@task(returns=1)
def ext_task(value):
    return value / external(value)


@task(returns=1)
def ext_task_c(value):
    return value / externalc(value)


@task(returns=1)
def class_task_inc(value):
    o = example(value)
    o.increment(value)
    return o.get_v()


@task(returns=1)
def class_task_sub(value):
    o = example(value)
    o.subtract(value)
    return o.get_v()


class testTaskInTask(unittest.TestCase):

    def testTaskInTask(self):
        result = level1(1)
        result = compss_wait_on(result)
        self.assertEqual(result, 4)

    def testTaskInTaskConstraints(self):
        result = level1c(2)
        result = compss_wait_on(result)
        self.assertEqual(result, 3)

    def testImplementTaskInTask(self):
        result = slow_task(20)
        # the runtime must choose the fast_tas (@implement)
        result = compss_wait_on(result)
        self.assertEqual(result, 3 + 60 + 40)

    def testMultinodeTaskInTask(self):
        result = multi_node_task(20)
        result = compss_wait_on(result)
        self.assertEqual(result, 80 * 60 * 40)

    def testTaskInTaskExt(self):
        result = ext_task(20)
        result = compss_wait_on(result)
        self.assertEqual(result, 2)

    def testTaskInTaskExtC(self):
        result = ext_task_c(20)
        result = compss_wait_on(result)
        self.assertEqual(result, 4)

    def testTaskInTaskClass(self):
        o = example(4)
        o.increment(20)
        o = compss_wait_on(o)
        self.assertEqual(o.get_v(), 15)

    def testTaskInTaskClassConstraint(self):
        o = example(8)
        o.subtract(20)
        o = compss_wait_on(o)
        self.assertEqual(o.get_v(), 2)

    def testTaskInTaskClass2(self):
        result = class_task_inc(10)
        result = compss_wait_on(result)
        self.assertEqual(result, 16)

    def testTaskInTaskClass2Constraint(self):
        result = class_task_sub(40)
        result = compss_wait_on(result)
        self.assertEqual(result, 24)
