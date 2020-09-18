#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest


from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_wait_on

from pycompss.api.task import task

PARALLEL_TEST_COUNT = 20
PARALLEL_TEST_MAX_COUNT = 4


class Report(object):
    """
    Object class shared among tasks.
    """

    def __init__(self):
        self.start = None
        self.end = None



@task(returns=1)
def sleep_task():
    """
    Sleeps for a second and returns a time report of the execution.
    """
    report = Report()
    import time
    report.start = int(round(time.time() * 1000))
    time.sleep(0.5)
    report.end = int(round(time.time() * 1000))
    return report


class WorkerModification(object):
    """
    Class representing a worker load modification.
    """

    def __init__(self, time, mod):
        self.moment = time
        self.modification = mod



class TestConcurrency(unittest.TestCase):
    """
    Unit Test verifying the execution of a task passing in object parameters
    """


    def test_concurrency(self):
        """
        Launches PARALLEL_TEST_COUNT independent tasks and validates that the runtime executes
        them in parallel without overloading the resources.
        """
        print("Testing concurrent executions")
        reports = []
        for i in range(0, PARALLEL_TEST_COUNT):
            reports.append(sleep_task())
        compss_barrier()

        # Verify the number of tasks able to run in parallel in the process
        modifications = []
        for report in reports:
            report = compss_wait_on(report)
            modifications.append(WorkerModification(report.start, 1))
            modifications.append(WorkerModification(report.end, -1))
        import operator
        sorted_modifications = sorted(modifications, key=operator.attrgetter('moment'))

        current_count = 0
        max_count = 0
        for mod in sorted_modifications:
            current_count = current_count + mod.modification
            max_count = max(max_count, current_count)
            if current_count > PARALLEL_TEST_MAX_COUNT:
                raise Exception("Scheduling does not properly manage the maximum number of tasks")

        if max_count != PARALLEL_TEST_MAX_COUNT:
            raise Exception("Worker in master does not hold as many task as possible")

        print("\tOK")

