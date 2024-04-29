#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest

from pycompss.api.task import task
from pycompss.api.multinode import multinode
from pycompss.api.constraint import constraint

from pycompss.api.api import compss_wait_on


@constraint(computingUnits="2")
@multinode(computing_nodes="2", processes_per_node="2")
@task()
def multi_node_task():
    # Expected values
    expected_num_nodes = 2
    expected_num_procs = 4
    expected_num_threads = 2
    expected_hostnames = ["COMPSsWorker01", "COMPSsWorker02"]

    # Check the environment variables
    import os

    num_nodes = int(os.environ["COMPSS_NUM_NODES"])
    if num_nodes != expected_num_nodes:
        print("ERROR: Incorrect number of nodes")
        print("  - Expected: " + str(expected_num_nodes))
        print("  - Got: " + str(num_nodes))
        return 1

    num_threads = int(os.environ["COMPSS_NUM_THREADS"])
    if num_threads != expected_num_threads:
        print("ERROR: Incorrect number of threads")
        print("  - Expected: " + str(expected_num_threads))
        print("  - Got: " + str(num_threads))
        return 2

    hostnames = sorted(os.environ["COMPSS_HOSTNAMES"].split(","))
    if hostnames != expected_hostnames:
        print("ERROR: Incorrect hostnames")
        print("  - Expected: " + expected_hostnames)
        print("  - Got: " + hostnames)
        return 3

    omp_num_threads = int(os.environ["OMP_NUM_THREADS"])
    if omp_num_threads != expected_num_threads:
        print("ERROR: Incorrect number of OMP threads")
        print("  - Expected: " + str(expected_num_threads))
        print("  - Got: " + str(omp_num_threads))
        return 4
    procs = int(os.environ["COMPSS_NUM_PROCS"])
    if procs != expected_num_procs:
        print("ERROR: Incorrect number of proceses_per_node")
        print("  - Expected: " + str(expected_num_procs))
        print("  - Got: " + str(procs))
        return 4

    # All ok
    return 0


class TestMultinodeDecorator(unittest.TestCase):

    def testFunctionalUsage(self):
        ev = multi_node_task()
        ev = compss_wait_on(ev)
        self.assertEqual(ev, 0)
