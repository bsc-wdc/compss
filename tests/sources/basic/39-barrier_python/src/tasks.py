"""
PyCOMPSs Testbench Tasks
========================
"""

from pycompss.api.task import task
from pycompss.api.parameter import *
import time

@task(returns = int)
def get_hero():
    print "Hero working."
    time.sleep(1)
    return 1
