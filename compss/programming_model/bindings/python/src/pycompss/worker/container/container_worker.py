#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

"""
PyCOMPSs Worker for Containers
=======================
    This file contains the code of a fake worker to execute Python tasks
inside containers.
"""

# Fix PYTHONPATH setup
import pycompss.worker.container.pythonpath_fixer  # noqa

# Regular imports
import sys
import logging

# PyCOMPSs imports
import pycompss.util.context as context
from pycompss.worker.commons.worker import execute_task
from pycompss.worker.commons.executor import build_return_params_message

# Define static logger
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # NOSONAR
LOGGER = logging.getLogger()


#
# Main method for Python task execution inside a Container
#
def main():
    # type: (...) -> int
    """ Main method to process the task execution.

    :return: Exit value
    """

    # Log initialisation
    if __debug__:
        LOGGER.debug("Initialising Python worker inside the container...")

    # Parse arguments
    if __debug__:
        LOGGER.debug("Parsing Python function and arguments...")

    # TODO: Enhance the received parameters from ContainerInvoker.java
    func_file_path = str(sys.argv[1])
    func_name = str(sys.argv[2])
    num_slaves = 0
    timeout = 0
    cus = 1
    has_target = str(sys.argv[3]).lower() == "true"
    return_type = str(sys.argv[4])
    return_length = int(sys.argv[5])
    num_params = int(sys.argv[6])
    func_params = sys.argv[7:]

    execute_task_params = [func_file_path, func_name, num_slaves,
                           timeout, cus, has_target, return_type,
                           return_length, num_params] + func_params

    if __debug__:
        LOGGER.debug("- File: " + str(func_file_path))
        LOGGER.debug("- Function: " + str(func_name))
        LOGGER.debug("- HasTarget: " + str(has_target))
        LOGGER.debug("- ReturnType: " + str(return_type))
        LOGGER.debug("- Num Returns: " + str(return_length))
        LOGGER.debug("- Num Parameters: " + str(num_params))
        LOGGER.debug("- Parameters: " + str(func_params))
        LOGGER.debug("DONE Parsing Python function and arguments")

    # Process task
    if __debug__:
        LOGGER.debug("Processing task...")

    process_name = "ContainerInvoker"
    storage_conf = "null"
    tracing = False
    log_files = None
    python_mpi = False
    collections_layouts = None
    context.set_pycompss_context(context.WORKER)
    result = execute_task(process_name,
                          storage_conf,
                          execute_task_params,
                          tracing,
                          LOGGER,
                          None,
                          log_files,           # noqa
                          python_mpi,
                          collections_layouts  # noqa
                          )
    # The ignored result is time out
    exit_value, new_types, new_values, _, except_msg = result

    if __debug__:
        LOGGER.debug("DONE Processing task")

    # Process results
    if __debug__:
        LOGGER.debug("Processing results...")
        LOGGER.debug("Task exit value = " + str(exit_value))

    if exit_value == 0:
        # Task has finished without exceptions
        if __debug__:
            LOGGER.debug("Building return parameters...")
            LOGGER.debug("New Types: " + str(new_types))
            LOGGER.debug("New Values: " + str(new_values))
        build_return_params_message(new_types, new_values)
        if __debug__:
            LOGGER.debug("DONE Building return parameters")
    elif exit_value == 2:
        # Task has finished with a COMPSs Exception
        if __debug__:
            except_msg = except_msg.replace(" ", "_")
            LOGGER.debug("Registered COMPSs Exception: %s" %
                         str(except_msg))
    else:
        # An exception has been raised in task
        if __debug__:
            except_msg = except_msg.replace(" ", "_")
            LOGGER.debug("Registered Exception in task execution %s" %
                         str(except_msg))

    # Return
    if exit_value != 0:
        LOGGER.debug("ERROR: Task execution finished with non-zero exit value (%s != 0)" % str(exit_value))  # noqa: E501
    else:
        LOGGER.debug("Task execution finished SUCCESSFULLY!")
    return exit_value


#
# Entry point
#
if __name__ == "__main__":
    main()
