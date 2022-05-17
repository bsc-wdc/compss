#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench object
========================
"""

from pycompss.api.task import task
from pycompss.api.parameter import *

class Shape(object):
    def __init__(self,x,y):
        self.x = x
        self.y = y
        description = "This shape has not been described yet"

    @task(returns=int, target_direction=IN)
    def area(self):
        return self.x * self.y