#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest

from modules.utils import verify_line

from pycompss.api.api import compss_barrier

from pycompss.api.task import task
from pycompss.api.parameter import INOUT

from pycompss.api.constraint import constraint

PARALLEL_TEST_COUNT = 20

INITIAL_CONTENT = "This is the initial content of the file"
UPDATED_CONTENT_1 = "This is the updated content 1 of the file"


class StringWrapper(object):
    """
    Object class shared among tasks.
    """

    def __init__(self):
        self.value = None



@task(returns=1)
def create_object_with_content(content):
    """
    Creates a new StringWrapper with the content passed in.
    """
    return_sw = StringWrapper()
    return_sw.value = content
    return return_sw

@constraint(processor_architecture="master")
@task(returns=1)
def create_object_with_content_master(content):
    """
    Creates a new StringWrapper with the content passed in.
    """
    return_sw = StringWrapper()
    return_sw.value = content
    return return_sw

@constraint(processor_architecture="worker")
@task(returns=1)
def create_object_with_content_worker(content):
    """
    Creates a new StringWrapper with the content passed in.
    """
    return_sw = StringWrapper()
    return_sw.value = content
    return return_sw

@constraint(processor_architecture="worker", processor_name="MainProcessor01")
@task(returns=1)
def create_object_with_content_worker01(content):
    """
    Creates a new StringWrapper with the content passed in.
    """
    return_sw = StringWrapper()
    return_sw.value = content
    return return_sw

@constraint(processor_architecture="worker", processor_name="MainProcessor02")
@task(returns=1)
def create_object_with_content_worker02(content):
    """
    Creates a new StringWrapper with the content passed in.
    """
    return_sw = StringWrapper()
    return_sw.value = content
    return return_sw

@task()
def check_object_with_content(content, input_sw):
    """
    Verifies that the content of the StringWrapper on the path matches
    the expected value.
    """
    line = input_sw.value
    verify_line(line, content)

@constraint(processor_architecture="master")
@task()
def check_object_with_content_master(content, input_sw):
    """
    Verifies that the content of the StringWrapper on the path matches
    the expected value.
    """
    line = input_sw.value
    verify_line(line, content)

@constraint(processor_architecture="worker")
@task()
def check_object_with_content_worker(content, input_sw):
    """
    Verifies that the content of the StringWrapper on the path matches
    the expected value.
    """
    line = input_sw.value
    verify_line(line, content)

@constraint(processor_architecture="worker", processor_name="MainProcessor01")
@task()
def check_object_with_content_worker01(content, input_sw):
    """
    Verifies that the content of the StringWrapper on the path matches
    the expected value.
    """
    line = input_sw.value
    verify_line(line, content)

@constraint(processor_architecture="worker", processor_name="MainProcessor02")
@task()
def check_object_with_content_worker02(content, input_sw):
    """
    Verifies that the content of the StringWrapper on the path matches
    the expected value.
    """
    line = input_sw.value
    verify_line(line, content)


@task(inout_sw=INOUT)
def check_and_update_object_with_content(content, new_content, inout_sw):
    """
    Verifies that the content of the StringWrapper on the path matches
    the expected value and updates its value.
    """
    line = inout_sw.value
    verify_line(line, content)
    inout_sw.value = new_content

@constraint(processor_architecture="master")
@task(inout_sw=INOUT)
def check_and_update_object_with_content_master(content, new_content, inout_sw):
    """
    Verifies that the content of the StringWrapper on the path matches
    the expected value and updates its value.
    """
    line = inout_sw.value
    verify_line(line, content)
    inout_sw.value = new_content

@constraint(processor_architecture="worker")
@task(inout_sw=INOUT)
def check_and_update_object_with_content_worker(content, new_content, inout_sw):
    """
    Verifies that the content of the StringWrapper on the path matches
    the expected value and updates its value.
    """
    line = inout_sw.value
    verify_line(line, content)
    inout_sw.value = new_content

@constraint(processor_architecture="worker", processor_name="MainProcessor01")
@task(inout_sw=INOUT)
def check_and_update_object_with_content_worker01(content, new_content, inout_sw):
    """
    Verifies that the content of the StringWrapper on the path matches
    the expected value and updates its value.
    """
    line = inout_sw.value
    verify_line(line, content)
    inout_sw.value = new_content

@constraint(processor_architecture="worker", processor_name="MainProcessor02")
@task(inout_sw=INOUT)
def check_and_update_object_with_content_worker02(content, new_content, inout_sw):
    """
    Verifies that the content of the StringWrapper on the path matches
    the expected value and updates its value.
    """
    line = inout_sw.value
    verify_line(line, content)
    inout_sw.value = new_content


class TestObjects(unittest.TestCase):
    """
    Unit Test verifying the execution of a task passing in object parameters
    """

    def test_master_producer_worker_consumer_object(self):
        print("Master produces object, worker consumes")
        stringwrapper = create_object_with_content_master(INITIAL_CONTENT)
        check_object_with_content_worker(INITIAL_CONTENT, stringwrapper)
        compss_barrier()
        print("\t OK")


    def test_worker_producer_master_consumer_object(self):
        print("Worker produces object, master consumes")
        stringwrapper = create_object_with_content_worker(INITIAL_CONTENT)
        check_object_with_content_master(INITIAL_CONTENT, stringwrapper)
        compss_barrier()
        print("\t OK")


    def test_master_producer_worker_consumer_master_updates_object(self):
        print("Master produces object, several workers consume, master updates, worker reads")
        stringwrapper = create_object_with_content_master(INITIAL_CONTENT)
        for i in range(0, PARALLEL_TEST_COUNT):
            check_object_with_content_worker(INITIAL_CONTENT, stringwrapper)
        check_and_update_object_with_content(INITIAL_CONTENT, UPDATED_CONTENT_1, stringwrapper)
        check_object_with_content_worker(UPDATED_CONTENT_1, stringwrapper)
        compss_barrier()
        print("\t OK")
