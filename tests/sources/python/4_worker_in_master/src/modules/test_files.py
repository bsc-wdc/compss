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



PARALLEL_TEST_COUNT = 20
PARALLEL_TEST_MAX_COUNT = 4

INITIAL_CONTENT = "This is the initial content of the file"
UPDATED_CONTENT_1 = "This is the updated content 1 of the file"
UPDATED_CONTENT_2 = "This is the updated content 2 of the file"
UPDATED_CONTENT_3 = "This is the updated content 3 of the file"
UPDATED_CONTENT_4 = "This is the updated content 4 of the file"
UPDATED_CONTENT_5 = "This is the updated content 5 of the file"


@task(file_name=FILE_OUT)
def create_file_with_content(content, file_name):
    """
    Creates a file with content on the path.
    """
    with open(file_name, "w") as f_channel:
        f_channel.write(content)

@task(file_name=FILE_IN)
def check_file_with_content(content, file_name):
    """
    Verifies that the content of the file on the path matches
    the expected value.
    """
    with open(file_name, "r") as f_channel:
        line = f_channel.readline()
        verify_line(line, content)

@task(file_name=FILE_INOUT)
def check_and_update_file_with_content(content, new_content, file_name):
    """
    Verifies that the content of the file on the path matches
    the expected value and updates its value.
    """
    with open(file_name, "r") as f_channel:
        line = f_channel.readline()
        verify_line(line, content)

    with open(file_name, "w") as f_channel:
        f_channel.write(new_content)

class TestFiles(unittest.TestCase):
    """
    Unit Test verifying the execution of a task passing in primitive-type parameters.
    """

    def test_main_to_task(self):
        """
        Creates a file and passes it as an input parameter to a task.
        """
        print("Creating file on main and using it on task")
        file_name = "main_to_task_file"
        with open(file_name, "w") as f_channel:
            f_channel.write(INITIAL_CONTENT)

        check_file_with_content(INITIAL_CONTENT, file_name)
        compss_barrier()
        compss_delete_file(file_name)
        print("\t OK")



    def test_task_to_main(self):
        """
        Creates a file on a task and reads it on the master.
        """
        print("Creating file on task and using it on main")
        file_name = "task_to_main_file"
        create_file_with_content(INITIAL_CONTENT, file_name)
        with compss_open(file_name, "r") as f_channel:
            line = f_channel.readline()
            verify_line(line, INITIAL_CONTENT)
            line = f_channel.readline()
            verify_line(line, None)
        compss_barrier()
        compss_delete_file(file_name)
        print("\t OK")


    def test_file_dependencies(self):
        """
        Creates a file on a task, verifies its content in a second one,
        a third task updates its value, and, finally, the master checks
        the value.
        """
        file_name = "dependencies_file_1"

        create_file_with_content(INITIAL_CONTENT, file_name)
        check_file_with_content(INITIAL_CONTENT, file_name)
        check_and_update_file_with_content(INITIAL_CONTENT, UPDATED_CONTENT_1, file_name)
        with compss_open(file_name, "r") as f_channel:
            line = f_channel.readline()
            verify_line(line, UPDATED_CONTENT_1)

        compss_barrier()
        compss_delete_file(file_name)
        print("\t OK")


    def test_file_dependencies_complex(self):
        """
        Creates a file on a task, verifies its content in a second one,
        a third task updates its value, and, then, the master checks
        the value. After that, the master updates the value, verifies its
        content locally and on a task.

        Later, it updates the value on a task, checks the value on another
        task, and updates twice the value on using two tasks. Finally, the
        value returns to the master so it checks it.
        """
        print("Testing file dependencies - Complex Version")
        file_name = "dependencies_file_2"

        create_file_with_content(INITIAL_CONTENT, file_name)
        check_file_with_content(INITIAL_CONTENT, file_name)
        check_and_update_file_with_content(INITIAL_CONTENT, UPDATED_CONTENT_1, file_name)
        with compss_open(file_name, "r") as f_channel:
            line = f_channel.readline()
            verify_line(line, UPDATED_CONTENT_1)

        # Update File Content on Main
        with compss_open(file_name, "w") as f_channel:
            f_channel.write(UPDATED_CONTENT_2)

        # Verify File update on Main
        with compss_open(file_name, "r") as f_channel:
            line = f_channel.readline()
            verify_line(line, UPDATED_CONTENT_2)
        check_file_with_content(UPDATED_CONTENT_2, file_name)

        check_and_update_file_with_content(UPDATED_CONTENT_2, UPDATED_CONTENT_3, file_name)
        check_file_with_content(UPDATED_CONTENT_3, file_name)
        check_and_update_file_with_content(UPDATED_CONTENT_3, UPDATED_CONTENT_4, file_name)
        check_and_update_file_with_content(UPDATED_CONTENT_4, UPDATED_CONTENT_5, file_name)
        with compss_open(file_name, "r") as f_channel:
            line = f_channel.readline()
            verify_line(line, UPDATED_CONTENT_5)

        compss_barrier()
        compss_delete_file(file_name)
        print("\t OK")


    