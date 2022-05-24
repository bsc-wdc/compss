#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Delete File test
=========================
    This file represents PyCOMPSs Testbench.
    Checks the delete file functionality.
"""

# Imports
import unittest
from pycompss.api.api import compss_open, compss_delete_file
from .tasks import increment, increment2


class testDeleteFile(unittest.TestCase):

    def testDeleteFile(self):
        # Check and get parameters
        initial_value = '1'
        counter_name = 'counter_INOUT'  # check that this file does not exist after the execution
        counter_name_IN = 'counter_IN'  # check that this file does not exist after the execution
        counter_name_OUT = 'counter_OUT'  # check that this file does not exist after the execution

        for i in range(3):
            # Write value
            if i <= 1:
                fos = open(counter_name, 'w')
                fos.write(initial_value)
                fos.close()
            fos2 = open(counter_name_IN, 'w')
            fos2.write(initial_value)
            fos2.close()
            print(('Initial counter value is %s' % initial_value))
            # Execute increment
            increment(counter_name)
            increment2(counter_name_IN, counter_name_OUT)
            # Read new value
            print('After sending task')
            if i == 0:
                compss_delete_file(counter_name)
            compss_delete_file(counter_name_IN)
            compss_delete_file(counter_name_OUT)

        fis = compss_open(counter_name, 'r+')
        final_value = fis.read()
        fis.close()
        print(('Final counter value is %s' % final_value))
        self.assertEqual(final_value, '3')
        compss_delete_file(counter_name)

    def testDeleteMultipleFiles(self):
        # Check and get parameters
        initial_value = '1'
        counter_name = 'multi_counter_INOUT'  # check that this file does not exist after the execution
        counter_name_IN = 'multi_counter_IN'  # check that this file does not exist after the execution
        counter_name_OUT = 'multi_counter_OUT'  # check that this file does not exist after the execution

        to_remove = []
        for i in range(3):
            # Write value
            if i <= 1:
                fos = open(counter_name, 'w')
                fos.write(initial_value)
                fos.close()
            fos2 = open(counter_name_IN, 'w')
            fos2.write(initial_value)
            fos2.close()
            print(('Initial counter value is %s' % initial_value))
            # Execute increment
            increment(counter_name)
            increment2(counter_name_IN, counter_name_OUT)
            # Read new value
            print('After sending task')
            partial_to_remove = []
            if i == 0:
                partial_to_remove.append(counter_name)
            partial_to_remove.append(counter_name_IN)
            partial_to_remove.append(counter_name_OUT)
            to_remove.append(tuple(partial_to_remove))
        to_remove.append(counter_name)
        removed = compss_delete_file(to_remove)
        self.assertEqual(removed, [(False, False, False), (False, False), (False, False), False])
