#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Worker - Container - Worker.

This file contains the code of a fake worker to execute Python tasks
inside containers.
"""

import logging
import os
import sys

from pycompss.util.context import CONTEXT
from pycompss.worker.container.pythonpath_fixer import fix_pythonpath
from pycompss.util.logger.helpers import init_logging_worker
from pycompss.util.typing_helper import typing
from pycompss.worker.commons.executor import build_return_params_message
from pycompss.worker.commons.worker import execute_task


def main() -> int:
    """Process python task inside a container main method.

    Received parameters from ContainerInvoker.java.

    :return: Exit value.
    """
    # Fix PYTHONPATH setup
    fix_pythonpath()

    # Parse arguments
    func_file_path = str(sys.argv[1])
    func_name = str(sys.argv[2])
    num_slaves = 0
    timeout = 0
    cus = 1
    log_level = sys.argv[3]
    tracing = sys.argv[4] == "true"
    has_target = str(sys.argv[5]).lower() == "true"
    return_type = str(sys.argv[6])
    return_length = int(sys.argv[7])
    num_params = int(sys.argv[8])
    func_params = sys.argv[9:]

    # Log initialisation
    # Load log level configuration file
    worker_path = os.path.dirname(os.path.realpath(__file__))
    if log_level in ("true", "debug"):
        # Debug
        log_json = "".join((worker_path, "/log/logging_container_worker_debug.json"))
    elif log_level in ("info", "off"):
        # Info or no debug
        log_json = "".join((worker_path, "/log/logging_container_worker_off.json"))
    else:
        # Default
        log_json = "".join((worker_path, "/log/logging_container_worker.json"))
    init_logging_worker(log_json, tracing)
    if __debug__:
        logger = logging.getLogger("pycompss.worker.container.container_worker")
        logger.debug("Initialising Python worker inside the container...")

    task_params = [
        func_file_path,
        func_name,
        num_slaves,
        timeout,
        cus,
        has_target,
        return_type,
        return_length,
        num_params,
    ]  # type: typing.List[typing.Any]
    execute_task_params = task_params + func_params

    if __debug__:
        logger.debug("- File: %s", str(func_file_path))
        logger.debug("- Function: %s", str(func_name))
        logger.debug("- HasTarget: %s", str(has_target))
        logger.debug("- ReturnType: %s", str(return_type))
        logger.debug("- Num Returns: %s", str(return_length))
        logger.debug("- Num Parameters: %s", str(num_params))
        logger.debug("- Parameters: %s", str(func_params))
        logger.debug("DONE Parsing Python function and arguments")

    # Process task
    if __debug__:
        logger.debug("Processing task...")

    process_name = "ContainerInvoker"
    storage_conf = "null"
    tracing = False
    log_files = ()
    python_mpi = False
    collections_layouts = {}  # type: typing.Dict[str, typing.Tuple[int, int, int]]
    CONTEXT.set_worker()
    result = execute_task(
        process_name,
        storage_conf,
        execute_task_params,
        tracing,
        logger,
        "None",
        log_files,  # noqa
        python_mpi,
        collections_layouts,
    )
    # The ignored result is time out
    exit_value, new_types, new_values, _, except_msg = result

    if __debug__:
        logger.debug("DONE Processing task")

    # Process results
    if __debug__:
        logger.debug("Processing results...")
        logger.debug("Task exit value = %s", str(exit_value))

    if exit_value == 0:
        # Task has finished without exceptions
        if __debug__:
            logger.debug("Building return parameters...")
            logger.debug("New Types: %s", str(new_types))
            logger.debug("New Values: %s", str(new_values))
        build_return_params_message(new_types, new_values)
        if __debug__:
            logger.debug("DONE Building return parameters")
    elif exit_value == 2:
        # Task has finished with a COMPSs Exception
        if __debug__:
            except_msg = except_msg.replace(" ", "_")
            logger.debug("Registered COMPSs Exception: %s", str(except_msg))
    else:
        # An exception has been raised in task
        if __debug__:
            except_msg = except_msg.replace(" ", "_")
            logger.debug("Registered Exception in task execution %s", str(except_msg))

    # Return
    if exit_value != 0:
        logger.debug(
            "ERROR: Task execution finished with non-zero exit value (%s != 0)",
            str(exit_value),
        )
    else:
        logger.debug("Task execution finished SUCCESSFULLY!")
    return exit_value


#
# Entry point
#
if __name__ == "__main__":
    main()
