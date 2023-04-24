#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest
import os

from pycompss.api.api import compss_barrier, compss_open, compss_wait_on
from pycompss.api.mpi import mpi
from pycompss.api.task import task
from pycompss.api.compss import compss
from pycompss.api.constraint import constraint



@constraint(computing_units="8")
@mpi(binary="hostname", working_dir="/tmp", runner="mpirun", processes=4, processes_per_node=2)
@task()
def proccesPerNode():
    pass

@constraint(computing_units="2")
@mpi(binary="hostname", working_dir="/tmp", runner="mpirun", processes=2, scale_by_cu=True)
@task()
def scaleBYCU():
    pass

@compss(runcompss="${RUNCOMPSS}", flags="-d", app_name="${APP_DIR}/src/testMpiDecorator.py",
        worker_in_master="false", computing_nodes="2", fail_by_exit_value=False)
@constraint(computing_units="2")
@task(returns=int)
def simple(value):
    pass


class TestCOMPSsDecorator(unittest.TestCase):
    def test(self):
        ev = simple(1)
        ev = compss_wait_on(ev)
        self.assertEqual(ev, 0)


