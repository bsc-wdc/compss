#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench PSCO Models
========================
"""

# Imports
import time

from pycompss.api.task import task
from pycompss.api.parameter import *

from storage.Object import SCO


class MyFile(SCO):

    def __init__(self, path):
        self.path = path

    @task(targetDirection=CONCURRENT)
    def write_three(self):
        # Write value
        with open(self.path, 'a') as f:
            f.write("3")
        time.sleep(2)

    @task(targetDirection=INOUT)
    def write_four(self):
        # Write value
        with open(self.path, 'a') as f:
            f.write("4")
        time.sleep(2)

    def count_threes(self):
        # Read final value
        with open(self.path) as f:
            final_value = f.read()

        total = final_value.count('3')
        return total

    def count_fours(self):
        # Read final value
        with open(self.path) as f:
            final_value = f.read()

        total = final_value.count('4')
        return total

    def get(self):
        return self.path
