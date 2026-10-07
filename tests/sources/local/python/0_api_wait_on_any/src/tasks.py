#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks - wait_on_any
"""

import time

from pycompss.api.task import task


@task(returns=1)
def sleep_task(duration):
    time.sleep(duration)
    return duration
