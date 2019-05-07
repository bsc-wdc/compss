#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest

from modules.utils import verify_line

from pycompss.api.api import compss_open
from pycompss.api.api import compss_delete_file
from pycompss.api.api import compss_barrier

from pycompss.api.task import task
from pycompss.api.parameter import FILE_IN
from pycompss.api.parameter import FILE_INOUT
from pycompss.api.parameter import FILE_OUT

from pycompss.api.constraint import constraint

PARALLEL_TEST_COUNT = 20
PARALLEL_TEST_MAX_COUNT = 4

INITIAL_CONTENT = "This is the initial content of the file"
UPDATED_CONTENT_1 = "This is the updated content 1 of the file"


def create_file(content, file_name):
    with open(file_name, "w") as f_channel:
        f_channel.write(content)

@task(file_name=FILE_OUT)
def create_file_with_content(content, file_name):
    """
    Creates a file with content on the path.
    """
    create_file(content, file_name)

@constraint(processor_architecture="master")
@task(file_name=FILE_OUT)
def create_file_with_content_master(content, file_name):
    """
    Creates a file with content on the path.
    """
    create_file(content, file_name)

@constraint(processor_architecture="worker")
@task(file_name=FILE_OUT)
def create_file_with_content_worker(content, file_name):
    """
    Creates a file with content on the path.
    """
    create_file(content, file_name)

@constraint(processor_architecture="worker", processor_name="MainProcessor01")
@task(file_name=FILE_OUT)
def create_file_with_content_worker01(content, file_name):
    """
    Creates a file with content on the path.
    """
    create_file(content, file_name)

@constraint(processor_architecture="worker", processor_name="MainProcessor02")
@task(file_name=FILE_OUT)
def create_file_with_content_worker02(content, file_name):
    """
    Creates a file with content on the path.
    """
    create_file(content, file_name)

def check_file(content, file_name):
    """
    Verifies that the content of the file on the path matches
    the expected value.
    """
    with open(file_name, "r") as f_channel:
        line = f_channel.readline()
        verify_line(line, content)

@task(file_name=FILE_IN)
def check_file_with_content(content, file_name):
    """
    Verifies that the content of the file on the path matches
    the expected value.
    """
    check_file(content, file_name)

@constraint(processor_architecture="master")
@task(file_name=FILE_IN)
def check_file_with_content_master(content, file_name):
    """
    Verifies that the content of the file on the path matches
    the expected value.
    """
    check_file(content, file_name)

@constraint(processor_architecture="worker")
@task(file_name=FILE_IN)
def check_file_with_content_worker(content, file_name):
    """
    Verifies that the content of the file on the path matches
    the expected value.
    """
    check_file(content, file_name)

@constraint(processor_architecture="worker",processor_name="MainProcessor01")
@task(file_name=FILE_IN)
def check_file_with_content_worker01(content, file_name):
    """
    Verifies that the content of the file on the path matches
    the expected value.
    """
    check_file(content, file_name)

@constraint(processor_architecture="worker",processor_name="MainProcessor02")
@task(file_name=FILE_IN)
def check_file_with_content_worker02(content, file_name):
    """
    Verifies that the content of the file on the path matches
    the expected value.
    """
    check_file(content, file_name)


def check_and_update_file(content, new_content, file_name):
    """
    Verifies that the content of the file on the path matches
    the expected value and updates its value.
    """
    with open(file_name, "r") as f_channel:
        line = f_channel.readline()
        verify_line(line, content)

    with open(file_name, "w") as f_channel:
        f_channel.write(new_content)

@task(file_name=FILE_INOUT)
def check_and_update_file_with_content(content, new_content, file_name):
    """
    Verifies that the content of the file on the path matches
    the expected value and updates its value.
    """
    check_and_update_file(content, new_content, file_name)

@constraint(processor_architecture="master")
@task(file_name=FILE_INOUT)
def check_and_update_file_with_content_master(content, new_content, file_name):
    """
    Verifies that the content of the file on the path matches
    the expected value and updates its value.
    """
    check_and_update_file(content, new_content, file_name)

@constraint(processor_architecture="worker")
@task(file_name=FILE_INOUT)
def check_and_update_file_with_content_worker(content, new_content, file_name):
    """
    Verifies that the content of the file on the path matches
    the expected value and updates its value.
    """
    check_and_update_file(content, new_content, file_name)

@constraint(processor_architecture="worker",processor_name="MainProcessor01")
@task(file_name=FILE_INOUT)
def check_and_update_file_with_content_worker01(content, new_content, file_name):
    """
    Verifies that the content of the file on the path matches
    the expected value and updates its value.
    """
    check_and_update_file(content, new_content, file_name)

@constraint(processor_architecture="worker",processor_name="MainProcessor02")
@task(file_name=FILE_INOUT)
def check_and_update_file_with_content_worker02(content, new_content, file_name):
    """
    Verifies that the content of the file on the path matches
    the expected value and updates its value.
    """
    check_and_update_file(content, new_content, file_name)

class TestFiles(unittest.TestCase):
    """
    Unit Test verifying the execution of a task passing in file parameters.
    """

    def test_master_producer_worker_consumer_file(self):
        """
        Creates a file and passes it as an input parameter to a task.
        """
        print("Master produces file, worker consumes")
        file_name = "master_producer_worker_consumer"
        create_file_with_content_master(INITIAL_CONTENT, file_name)
        check_file_with_content_worker(INITIAL_CONTENT, file_name)
        compss_barrier()
        compss_delete_file(file_name)
        print("\t OK")


    def test_worker_producer_master_consumer_file(self):
        """
        """
        print("Worker produces file, master consumes")
        file_name = "worker_producer_master_consumer"
        create_file_with_content_worker(INITIAL_CONTENT, file_name)
        check_file_with_content_master(INITIAL_CONTENT, file_name)
        compss_barrier()
        compss_delete_file(file_name)
        print("\t OK")

    def test_master_producer_worker_consumer_master_updates_file(self):
        """
        """
        print("Master produces file, several workers consume, master updates, worker reads")
        file_name = "produce_consume_update"
        create_file_with_content_master(INITIAL_CONTENT, file_name)
        for i in range(0, PARALLEL_TEST_COUNT):
            check_file_with_content_worker(INITIAL_CONTENT, file_name)

        check_and_update_file_with_content(INITIAL_CONTENT, UPDATED_CONTENT_1, file_name)
        check_file_with_content_worker(UPDATED_CONTENT_1, file_name)
        compss_barrier()
        print("\t OK")
