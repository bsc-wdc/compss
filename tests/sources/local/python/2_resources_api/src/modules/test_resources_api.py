#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# For better print formatting
from __future__ import print_function

# Imports
import unittest
import time

from pycompss.api.task import task
from pycompss.api.on_failure import on_failure
from pycompss.api.api import TaskGroup
from pycompss.api.api import compss_get_number_of_resources
from pycompss.api.api import compss_request_resources
from pycompss.api.api import compss_free_resources

TIME_RUNTIME_INITIAL_VMS = 30  # s
TIME_PETITION = 30  # s
NUM_SLEEPS = 10
TIME_FOREVER = 60  # s


# Tasks definition
@task()
def dummy_task():
    print("Dummy task executed")


@on_failure(management="IGNORE")
@task()
def waiter_task():
    print("Starting task")

    for i in range(NUM_SLEEPS):
        print("Sleeping...")
        time.sleep(TIME_FOREVER)

    print("ERROR: Notification was not received")


# Test
class TestResourcesApi(unittest.TestCase):

    def _scale_up(self, num_resources):
        print("- Scaling up " + str(num_resources))

        # Retrieve number of resources
        cn1 = compss_get_number_of_resources()

        # Request resources
        compss_request_resources(num_resources, None)

        # Wait for completion
        time.sleep(TIME_PETITION*num_resources)

        # Retrieve number of resources and check the increase
        cn2 = compss_get_number_of_resources()
        print("CN before = " + str(cn1) + " , CN after = " + str(cn2))
        self.assertEquals(cn1 + num_resources, cn2)
        time.sleep(TIME_PETITION)

    def _scale_down(self, num_resources):
        print("- Scaling down " + str(num_resources))

        # Retrieve number of resources
        cn1 = compss_get_number_of_resources()

        # Free resources
        compss_free_resources(num_resources, None)

        # Wait for completion
        time.sleep(TIME_PETITION*num_resources)

        # Retrieve number of resources and check the decrease
        cn2 = compss_get_number_of_resources()
        print("CN before = " + str(cn1) + " , CN after = " + str(cn2))
        self.assertEquals(cn1 - num_resources, cn2)

    @classmethod
    def setUpClass(cls):
        # Wait for runtime to create initial VMs
        print("Waiting for runtime to create the initial VMs...")
        time.sleep(TIME_RUNTIME_INITIAL_VMS)
        print("Done")

    def test_1_get(self):
        cn = compss_get_number_of_resources()
        print("Num resources = " + str(cn))
        self.assertEquals(cn, 1)

    def test_2_scale_up(self):
        self._scale_up(1)

    def test_3_multi_scale_up(self):
        self._scale_up(2)

    def test_4_scale_down(self):
        self._scale_down(1)

    def test_5_multi_scale_down(self):
        self._scale_down(2)

    def test_6_scale_up_and_notify(self):
        # Request resources
        num_resources = 1
        group_name = "increase"
        with TaskGroup(group_name):
            waiter_task()
            time.sleep(1)
            compss_request_resources(num_resources, group_name)

        # The implicit barrier of the group will wait until my_task is cancelled by the resources notification

        # Resource mechanisms are already checked by other methods (the runtime here can destroy machines
        # because a core element is created so we don't really know how many resources there will be.

        # Result script must check that task was cancelled
        print("Result script must check that task was cancelled")

    def test_7_scale_down_and_notify(self):
        # Launch task and decrease resources
        num_resources = 1
        group_name = "decrease"
        with TaskGroup(group_name):
            waiter_task()
            time.sleep(1)
            compss_free_resources(num_resources, group_name)

        # The implicit barrier of the group will wait until my_task is cancelled by the resources notification

        # Resource mechanisms are already checked by other methods (the runtime here can destroy machines
        # because a core element is created so we don't really know how many resources there will be.

        # Result script must check that task was cancelled
        print("Result script must check that task was cancelled")
