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
from pycompss.api.api import compss_wait_on


from pycompss.api.task import task
from pycompss.api.parameter import INOUT

PARALLEL_TEST_COUNT = 20
PARALLEL_TEST_MAX_COUNT = 4

INITIAL_CONTENT = "This is the initial content of the file"
UPDATED_CONTENT_1 = "This is the updated content 1 of the file"
UPDATED_CONTENT_2 = "This is the updated content 2 of the file"
UPDATED_CONTENT_3 = "This is the updated content 3 of the file"
UPDATED_CONTENT_4 = "This is the updated content 4 of the file"
UPDATED_CONTENT_5 = "This is the updated content 5 of the file"


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

@task()
def check_object_with_content(content, input_sw):
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


class TestObjects(unittest.TestCase):
    """
    Unit Test verifying the execution of a task passing in object parameters
    """


    def test_object_dependencies(self):
        """
        Creates a Stringwrapper on a task, verifies its content in a second one,
        a third task updates its value, and, finally, the master checks
        the value.
        """
        print("Testing object dependencies")
        stringwrapper = create_object_with_content(INITIAL_CONTENT)
        check_object_with_content(INITIAL_CONTENT, stringwrapper)
        check_and_update_object_with_content(INITIAL_CONTENT, UPDATED_CONTENT_1, stringwrapper)
        stringwrapper = compss_wait_on(stringwrapper)
        line = stringwrapper.value
        verify_line(line, UPDATED_CONTENT_1)
        compss_barrier()
        print("\t OK")


    def test_file_dependencies_complex(self):
        """
        Creates a Stringwrapper on a task, verifies its content in a second one,
        a third task updates its value, and, then, the master checks
        the value. After that, the master updates the value, verifies its
        content locally and on a task.

        Later, it updates the value on a task, checks the value on another
        task, and updates twice the value on using two tasks. Finally, the
        value returns to the master so it checks it.
        """
        print("Testing object dependencies - Complex Version")

        stringwrapper = create_object_with_content(INITIAL_CONTENT)
        check_object_with_content(INITIAL_CONTENT, stringwrapper)
        check_and_update_object_with_content(INITIAL_CONTENT, UPDATED_CONTENT_1, stringwrapper)

        stringwrapper = compss_wait_on(stringwrapper)
        line = stringwrapper.value
        verify_line(line, UPDATED_CONTENT_1)

        # Update object Content on Main
        stringwrapper.value = UPDATED_CONTENT_2

        # Verify object update on Main
        line = stringwrapper.value
        verify_line(line, UPDATED_CONTENT_2)

        # Verify Object content on task
        check_object_with_content(UPDATED_CONTENT_2, stringwrapper)

        # Update value on task
        check_and_update_object_with_content(UPDATED_CONTENT_2, UPDATED_CONTENT_3, stringwrapper)
        # Check proper value on task
        check_object_with_content(UPDATED_CONTENT_3, stringwrapper)
        # Update twice on tasks
        check_and_update_object_with_content(UPDATED_CONTENT_3, UPDATED_CONTENT_4, stringwrapper)
        check_and_update_object_with_content(UPDATED_CONTENT_4, UPDATED_CONTENT_5, stringwrapper)

        # Verify object update on Main
        stringwrapper = compss_wait_on(stringwrapper)
        line = stringwrapper.value
        verify_line(line, UPDATED_CONTENT_5)

        compss_barrier()
        print("\t OK")

