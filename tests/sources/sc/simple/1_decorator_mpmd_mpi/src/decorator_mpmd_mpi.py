#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Simple
========================
"""

# Imports
from pycompss.api.api import compss_barrier as cb, compss_wait_on as cwo
from pycompss.api.mpmd_mpi import mpmd_mpi
from pycompss.api.task import task

import unittest


@mpmd_mpi(runner="mpirun",
          programs=[
               dict(binary="date", processes=2, params="-d {{first}}"),
               dict(binary="date", processes=2, params="-d {{second}}")
          ])
@task()
def params(first, second):
    pass


class MpmdMPITest(unittest.TestCase):

    def testParams(self):
        params("next+monday", "next+friday")
        cb()


if __name__ == '__main__':
    unittest.main()
