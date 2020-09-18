#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest

from pycompss.api.task import task
from pycompss.api.parameter import *

from pycompss.api.binary import binary
from pycompss.api.compss import compss
from pycompss.api.constraint import constraint
from pycompss.api.decaf import decaf
from pycompss.api.implement import implement
from pycompss.api.mpi import mpi
from pycompss.api.ompss import ompss
# from pycompss.api.opencl import opencl

# All of the following tasks do not include an mandatory argument or
# include it wrong.
# An exception has to be raised by the binding in this case.
# If not found the exception, then raise an exception to the test.

try:
    # Missing binary
    @binary(working_dir="/tmp")
    @task()
    def binary_task(dprefix, param):
        pass
except Exception:
    print("Exception thrown by @binary SUCCESSFULLY detected")
else:
    raise Exception("ERROR: Exception expected from @binary not found")

try:
    # Wrong app_name name (without underscore) - it should behave as if it is missing
    @compss(runcompss="${RUNCOMPSS}", flags="-d", appname="${APP_DIR}/src/simple_compss_nested.py", worker_in_master="false", processes="2")
    @constraint(computing_units="2")
    @task(returns=int)
    def compss_task(value):
        pass
except Exception:
    print("Exception thrown by @compss SUCCESSFULLY detected")
else:
    raise Exception("ERROR: Exception expected from @compss not found")

try:
    # Wrong df_script name (without underscore) - it should behave as if it is missing
    @decaf(working_dir=".", Runner="mpirun", dfscript="${APP_DIR}/src/test_decaf.py", df_executor="test.sh", df_lib="lib")
    @task(param=FILE_OUT)
    def my_decaf_task(param):
        pass
except Exception:
    print("Exception thrown by @decaf SUCCESSFULLY detected")
else:
    raise Exception("ERROR: Exception expected from @decaf not found")

@task(returns=int)
def slow_task(value):
    return value * value * value

try:
    # Missing method argument
    @implement(source_class="modules.testArgumentError")
    @constraint(computing_units="1")
    @task(returns=list)
    def better_task(value):
        return value ** 3
except Exception:
    print("Exception thrown by @implement SUCCESSFULLY detected")
else:
    raise Exception("ERROR: Exception expected from @implement not found")

try:
    # Missing runner
    @mpi(binary="date", workingDir="/tmp")
    @task()
    def mpi_task(dprefix, param):
        pass
except Exception:
    print("Exception thrown by @mpi SUCCESSFULLY detected")
else:
    raise Exception("ERROR: Exception expected from @mpi not found")

try:
    # Missing binary
    @ompss(workingDir="/tmp")
    @task()
    def ompss_task(dprefix, param):
        pass
except Exception:
    print("Exception thrown by @ompss SUCCESSFULLY detected")
else:
    raise Exception("ERROR: Exception expected from @ompss not found")

# try:
#     # Missing kernel
#     @opencl(workingDir="/tmp")
#     @task()
#     def opencl_task(dprefix, param):
#         pass
# except Exception:
#     print("Exception thrown by @opencl SUCCESSFULLY detected")
# else:
#     raise Exception("ERROR: Exception expected from @opencl not found")

class testArgumentError(unittest.TestCase):

    # This test will never reach the execution of the tasks:
    #    - if the exception is caught: the call to a task will raise a
    #      NameError exception since it will not be defined (previous try
    #      except)
    #    - if the exception is not caught: the exception thrown will
    #      stop the test

    def testErrors(self):
        # Symbolic test. Everything is tested when loading the test
        pass
