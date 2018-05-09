#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
from pycompss.api.task import task
import time


@task(returns=int)
def get_hero():
    print("Hero working.")
    time.sleep(1)
    return 1
