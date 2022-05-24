#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import os
import unittest

from pycompss.api.task import task
from pycompss.api.compss import compss
from pycompss.api.constraint import constraint
from pycompss.api.software import software
from pycompss.api.api import compss_wait_on


@compss(runcompss="${RUNCOMPSS}", flags="-d", app_name="${APP_DIR}/src/simple_compss_nested.py",
        worker_in_master="false", computing_nodes="2", fail_by_exit_value=False)
@constraint(computing_units="2")
@task(returns=int)
def simple(value):
    pass


@compss(runcompss="${RUNCOMPSS}", flags="-d", app_name="${APP_DIR}/src/bad_simple_compss_nested.py",
        worker_in_master="false", computing_nodes="2", fail_by_exit_value=False)
@constraint(computing_units="2")
@task(returns=int)
def bad_simple(value):
    pass


@compss(runcompss="${RUNCOMPSS}", flags="-d", app_name="${APP_DIR}/src/exit_compss_nested.py",
        worker_in_master="false", computing_nodes="2", fail_by_exit_value=False)
@constraint(computing_units="2")
@task(returns=int)
def exit_code_test(value):
    pass


@software(config_file=os.getcwd() + "/src/compss.json")
@task(returns=int)
def simple_w_software(value):
    pass


class TestCOMPSsDecorator(unittest.TestCase):

    def testNestedSingleNode(self):
        ev = simple(1)
        ev = compss_wait_on(ev)
        self.assertEqual(ev, 0)

    def testNestedSingleNodeFailure(self):
        ev = bad_simple(1)
        ev = compss_wait_on(ev)
        self.assertEqual(ev, 1)

    def testNestedExitCode(self):
        exit_code = 123
        ev = exit_code_test(exit_code)
        ev = compss_wait_on(ev)
        self.assertEqual(ev, exit_code)

    def testWithSoftware(self):
        ev = simple_w_software(1)
        ev = compss_wait_on(ev)
        self.assertEqual(ev, 0)
