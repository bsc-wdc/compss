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

# -*- coding: utf-8 -*-

"""
PyCOMPSs Worker for GAT
=======================
    This file contains the worker code for GAT.
    Args: debug full_path (method_class)
    method_name has_target num_params par_type_1 par_1 ... par_type_n par_n
"""

import logging
import os
import sys

from pycompss.runtime.commons import IS_PYTHON3
from pycompss.util.logger.helpers import init_logging_worker
from pycompss.worker.commons.worker import execute_task
from pycompss.util.tracing.helpers import trace_multiprocessing_worker
from pycompss.util.tracing.helpers import dummy_context
from pycompss.util.tracing.helpers import event
from pycompss.worker.commons.constants import INIT_STORAGE_AT_WORKER_EVENT
from pycompss.worker.commons.constants import FINISH_STORAGE_AT_WORKER_EVENT
from pycompss.worker.gat.commons.constants import PROCESS_CREATION
from pycompss.worker.gat.commons.constants import PARAMETER_PROCESSING
from pycompss.worker.gat.commons.constants import LOGGING
from pycompss.worker.gat.commons.constants import MODULES_IMPORT
from pycompss.worker.gat.commons.constants import WORKER_END
from pycompss.worker.gat.commons.constants import PROCESS_DESTRUCTION

from pycompss.streams.components.distro_stream_client import DistroStreamClientHandler  # noqa: E501

if IS_PYTHON3:
    long = int
else:
    # Exception moved to built-in
    str_escape = 'string_escape'

# Uncomment the next line if you do not want to reuse pyc files.
# sys.dont_write_bytecode = True


def compss_worker(tracing, task_id, storage_conf, params):
    # type: (bool, str, str, list) -> str
    """ Worker main method (invoked from __main__).

    :param tracing: Tracing boolean
    :param task_id: Task identifier
    :param storage_conf: Storage configuration file
    :param params: Parameters following the common order of the workers
    :return: Exit code
    """

    if __debug__:
        logger = logging.getLogger('pycompss.worker.gat.worker')
        logger.debug("Starting Worker")

    # Set the binding in worker mode
    import pycompss.util.context as context
    context.set_pycompss_context(context.WORKER)

    result = execute_task("".join(("Task ", task_id)),
                          storage_conf,
                          params,
                          tracing,
                          logger,
                          (),
                          False)
    # Result contains:
    # exit_code, new_types, new_values, timed_out, except_msg = result
    exit_code, _, _, _, _ = result

    if __debug__:
        logger.debug("Finishing Worker")

    return exit_code


def main():
    # type: () -> None
    """ GAT worker main code.

    Executes the task provided by parameters.

    :return: None
    """
    # Emit sync event if tracing is enabled
    tracing = sys.argv[1] == 'true'
    task_id = int(sys.argv[2])
    log_level = sys.argv[3]
    storage_conf = sys.argv[4]
    stream_backend = sys.argv[5]
    stream_master_name = sys.argv[6]
    stream_master_port = sys.argv[7]
    # Next: method_type = sys.argv[8]
    params = sys.argv[9:]
    # Next parameters:
    # class_name = sys.argv[10]
    # method_name = sys.argv[11]
    # num_slaves = sys.argv[12]
    # i = 13 + num_slaves
    # slaves = sys.argv[12..i]
    # numCus = sys.argv[i+1]
    # has_target = sys.argv[i+2] == 'true'
    # num_params = int(sys.argv[i+3])
    # params = sys.argv[i+4..]

    if log_level == "true" or log_level == "debug":
        print("Tracing = " + str(tracing))
        print("Task id = " + str(task_id))
        print("Log level = " + str(log_level))
        print("Storage conf = " + str(storage_conf))

    persistent_storage = False
    if storage_conf != "null":
        persistent_storage = True

    streaming = False
    if stream_backend not in [None, "null", "NONE"]:
        streaming = True

    with trace_multiprocessing_worker() if tracing else dummy_context():

        if streaming:
            # Start streaming
            DistroStreamClientHandler.init_and_start(
                master_ip=stream_master_name,
                master_port=stream_master_port)

        # Load log level configuration file
        worker_path = os.path.dirname(os.path.realpath(__file__))
        if log_level == "true" or log_level == "debug":
            # Debug
            log_json = "".join((worker_path,
                                "/../../../log/logging_gat_worker_debug.json"))
        elif log_level == "info" or log_level == "off":
            # Info or no debug
            log_json = "".join((worker_path,
                                "/../../../log/logging_gat_worker_off.json"))
        else:
            # Default
            log_json = "".join((worker_path,
                                "/../../../log/logging_gat_worker.json"))
        init_logging_worker(log_json, tracing)

        if persistent_storage:
            # Initialize storage
            with event(INIT_STORAGE_AT_WORKER_EVENT):
                from storage.api import initWorker as initStorageAtWorker  # noqa
                initStorageAtWorker(config_file_path=storage_conf)

        # Init worker
        exit_code = compss_worker(tracing, str(task_id), storage_conf, params)

        if streaming:
            # Finish streaming
            DistroStreamClientHandler.set_stop()

        if persistent_storage:
            # Finish storage
            with event(FINISH_STORAGE_AT_WORKER_EVENT):
                from storage.api import finishWorker as finishStorageAtWorker  # noqa
                finishStorageAtWorker()

    if exit_code == 1:
        exit(1)


if __name__ == '__main__':
    main()
