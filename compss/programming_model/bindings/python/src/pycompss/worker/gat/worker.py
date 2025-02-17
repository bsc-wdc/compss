#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Worker - GAT - Worker.

This file contains the worker code for GAT.
Args: debug full_path (method_class)
method_name has_target num_params par_type_1 par_1 ... par_type_n par_n
"""

import logging
import sys

from pycompss.streams.components.distro_stream_client import (
    DistroStreamClientHandler,
)
from pycompss.util.context import CONTEXT
from pycompss.util.logger.helpers import init_logging_worker
from pycompss.util.logger.remittent import LOG_REMITTENT
from pycompss.util.logger.level import LOG_LEVEL
from pycompss.util.tracing.helpers import dummy_context
from pycompss.util.tracing.helpers import EventWorker
from pycompss.util.tracing.helpers import trace_multiprocessing_worker
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.worker.commons.worker import execute_task


# Uncomment the next line if you do not want to reuse pyc files.
# sys.dont_write_bytecode = True


def compss_worker(
    tracing: bool,
    task_id: str,
    storage_conf: str,
    params: list,
) -> int:
    """Worker main method (invoked from __main__).

    :param tracing: Tracing boolean.
    :param task_id: Task identifier.
    :param storage_conf: Storage configuration file.
    :param params: Parameters following the common order of the workers.
    :return: Exit code.
    """
    if __debug__:
        logger = logging.getLogger("pycompss.worker.gat.worker")
        logger.debug("Starting Worker")

    # Set the binding in worker mode
    CONTEXT.set_worker()

    result = execute_task(
        "".join(("Task ", task_id)),
        storage_conf,
        params,
        tracing,
        logger,
        (),
        False,
        {},
        None,
        None,
    )
    # Result contains:
    # exit_code, new_types, new_values, timed_out, except_msg = result
    exit_code, _, _, _, _ = result

    if __debug__:
        logger.debug("Finishing Worker")

    return exit_code


def main() -> None:
    """GAT worker main code.

    Executes the task provided by parameters.

    :return: None.
    """
    # Emit sync event if tracing is enabled
    tracing = sys.argv[1] == "true"
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

    if log_level in ("true", "debug"):
        print(f"Tracing = {str(tracing)}")
        print(f"Task id = {str(task_id)}")
        print(f"Log level = {str(log_level)}")
        print(f"Storage conf = {str(storage_conf)}")

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
                master_ip=stream_master_name, master_port=stream_master_port
            )

        # Load log level configuration file
        if log_level in ("true", "debug"):
            # Debug
            init_logging_worker(
                LOG_REMITTENT.GAT_WORKER, LOG_LEVEL.DEBUG, tracing
            )
        elif log_level == "info":
            # Info
            init_logging_worker(
                LOG_REMITTENT.GAT_WORKER, LOG_LEVEL.INFO, tracing
            )
        else:
            # Default (off)
            init_logging_worker(
                LOG_REMITTENT.GAT_WORKER, LOG_LEVEL.OFF, tracing
            )

        if persistent_storage:
            # Initialize storage
            with EventWorker(TRACING_WORKER.init_storage_at_worker_event):
                from storage.api import (  # pylint: disable=E0401, C0415
                    # disable=import-error, import-outside-toplevel
                    initWorker as initStorageAtWorker,
                )

                initStorageAtWorker(config_file_path=storage_conf)

        # Init worker
        exit_code = compss_worker(tracing, str(task_id), storage_conf, params)

        if streaming:
            # Finish streaming
            DistroStreamClientHandler.set_stop()

        if persistent_storage:
            # Finish storage
            with EventWorker(TRACING_WORKER.finish_storage_at_worker_event):
                from storage.api import (  # pylint: disable=E0401, C0415
                    # disable=import-error, import-outside-toplevel
                    finishWorker as finishStorageAtWorker,
                )

                finishStorageAtWorker()

    if exit_code == 1:
        sys.exit(1)


if __name__ == "__main__":
    main()
