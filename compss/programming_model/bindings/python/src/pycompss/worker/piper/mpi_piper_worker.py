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
PyCOMPSs Worker - Piper - MPI Worker.

This file contains the mpi piper worker code.
"""

import os
import signal
import sys

from mpi4py import MPI
from pycompss.runtime.commons import GLOBALS
from pycompss.util.context import CONTEXT
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.process.manager import Queue
from pycompss.util.tracing.helpers import dummy_context
from pycompss.util.tracing.helpers import EventWorker
from pycompss.util.tracing.helpers import trace_mpi_executor
from pycompss.util.tracing.helpers import trace_mpi_worker
from pycompss.util.tracing.helpers import emit_manual_event_explicit
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.util.typing_helper import typing
from pycompss.worker.piper.cache.setup import is_cache_enabled
from pycompss.worker.piper.cache.setup import start_cache
from pycompss.worker.piper.cache.setup import stop_cache
from pycompss.worker.piper.commons.constants import HEADER
from pycompss.worker.piper.commons.constants import TAGS
from pycompss.worker.piper.commons.executor import ExecutorConf
from pycompss.worker.piper.commons.executor import executor
from pycompss.worker.piper.commons.utils import PiperWorkerConfiguration
from pycompss.worker.piper.commons.utils_logger import load_loggers

# Persistent worker global variables
COMM = MPI.COMM_WORLD
SIZE = COMM.Get_size()
RANK = COMM.Get_rank()
PROCESSES = {}  # IN_PIPE -> PROCESS ID


def is_worker() -> bool:
    """Return whether the process should act as a worker.

    :return: The process should act as a worker.
    """
    return RANK == 0


def shutdown_handler(
    signal: int,  # pylint: disable=redefined-outer-name, unused-argument
    frame: typing.Any,  # pylint: disable=unused-argument
) -> None:
    """Handle shutdown - Shutdown handler.

    CAUTION! Do not remove the parameters.

    :param signal: Shutdown signal.
    :param frame: Frame.
    :return: None.
    """
    if is_worker():
        print(f"{HEADER}Shutdown signal handler")
    else:
        print(f"[PYTHON EXECUTOR {RANK}] Shutdown signal handler")


def user_signal_handler(
    signal: int,  # pylint: disable=redefined-outer-name, unused-argument
    frame: typing.Any,  # pylint: disable=unused-argument
) -> None:
    """Handle user signal - User signal handler.

    CAUTION! Do not remove the parameters.

    :param signal: Shutdown signal.
    :param frame: Frame.
    :return: None.
    """
    if is_worker():
        print(f"{HEADER}Default user signal handler")
    else:
        print(f"[PYTHON EXECUTOR {RANK}] Default user signal handler")


######################
# Main method
######################


def compss_persistent_worker(config: PiperWorkerConfiguration) -> None:
    """Create persistent worker main function.

    Retrieve the initial configuration and represents the main worker process.

    :param config: Piper Worker Configuration description.
    :return: None.
    """
    # First thing to do is to emit the process identifier event
    emit_manual_event_explicit(
        TRACING_WORKER.process_identifier, TRACING_WORKER.process_worker_event
    )

    pids = COMM.gather(str(os.getpid()), root=0)
    if not pids:
        raise PyCOMPSsException("Could not gather MPI COMM.")

    # Catch SIGTERM sent by bindings_piper
    signal.signal(signal.SIGTERM, shutdown_handler)
    # Catch SIGUSER2 to solve strange behaviour with mpi4py
    signal.signal(signal.SIGUSR2, user_signal_handler)

    # Set the binding in worker mode
    CONTEXT.set_worker()

    persistent_storage = config.storage_conf != "null"

    logger, _, _ = load_loggers(config.debug, persistent_storage)

    if __debug__:
        logger.debug(
            "%s[mpi_piper_worker.py] rank: %s wake up", HEADER, str(RANK)
        )
        config.print_on_logger(logger)

    # Start storage
    if persistent_storage:
        # Initialize storage
        if __debug__:
            logger.debug("%sStarting persistent storage", HEADER)
        from storage.api import (  # pylint: disable=E0401, C0415
            # disable=import-error, import-outside-toplevel
            initWorker as initStorageAtWorker,
        )

        initStorageAtWorker(config_file_path=config.storage_conf)

    for i in range(0, config.tasks_x_node):
        child_in_pipe = config.pipes[i].input_pipe
        try:
            child_pid = pids[i + 1]
        except IndexError:
            child_pid = pids[i]
        PROCESSES[child_in_pipe] = child_pid

    if __debug__:
        logger.debug("%sStarting alive", HEADER)
        logger.debug("%sControl pipe: %s", HEADER, str(config.control_pipe))
    # Read command from control pipe
    alive = True
    control_pipe = config.control_pipe
    while alive:
        command = control_pipe.read_command()
        if command != "":
            line = command.split()
            if line[0] == TAGS.add_executor:
                in_pipe = line[1]
                out_pipe = line[2]
                control_pipe.write(
                    " ".join(
                        (TAGS.add_executor_failed, out_pipe, in_pipe, str(0))
                    )
                )

            elif line[0] == TAGS.remove_executor:
                in_pipe = line[1]
                out_pipe = line[2]
                PROCESSES.pop(in_pipe, None)
                control_pipe.write(
                    " ".join((TAGS.removed_executor, out_pipe, in_pipe))
                )

            elif line[0] == TAGS.query_executor_id:
                in_pipe = line[1]
                out_pipe = line[2]
                pid = PROCESSES.get(in_pipe)
                control_pipe.write(
                    " ".join(
                        (TAGS.reply_executor_id, out_pipe, in_pipe, str(pid))
                    )
                )

            elif line[0] == TAGS.cancel_task:
                in_pipe = line[1]
                cancel_pid = str(PROCESSES.get(in_pipe))
                if __debug__:
                    logger.debug(
                        "%sSignaling process with PID %s to cancel a task",
                        HEADER,
                        cancel_pid,
                    )
                # Cancellation produced by COMPSs
                os.kill(int(cancel_pid), signal.SIGUSR2)

            elif line[0] == TAGS.ping:
                control_pipe.write(TAGS.pong)

            elif line[0] == TAGS.quit:
                alive = False
            else:
                if __debug__:
                    logger.debug(
                        "%sERROR: UNKNOWN COMMAND: %s", HEADER, command
                    )
                alive = False

    # Stop storage
    if persistent_storage:
        # Finish storage
        if __debug__:
            logger.debug("%sStopping persistent storage", HEADER)
        from storage.api import (  # pylint: disable=E0401, C0415
            # disable=import-error, import-outside-toplevel
            finishWorker as finishStorageAtWorker,
        )

        finishStorageAtWorker()

    if __debug__:
        logger.debug("%sFinished", HEADER)

    control_pipe.write(TAGS.quit)
    control_pipe.close()


def compss_persistent_executor(
    config: PiperWorkerConfiguration,
    tracing: bool,
    in_cache_queue: typing.Optional[Queue],
    out_cache_queue: typing.Optional[Queue],
    cache_ids: typing.Any,
) -> None:
    """Create persistent MPI executor main function.

    Retrieve the initial configuration and performs executor process
    functionality.

    :param config: Piper Worker Configuration description.
    :param tracing: If tracing is activated.
    :param in_cache_queue: Cache input queue.
    :param out_cache_queue: Cache output queue.
    :param cache_ids: Cache identifiers.
    :return: None.
    """
    COMM.gather(str(os.getpid()), root=0)

    # Catch SIGTERM sent by bindings_piper
    signal.signal(signal.SIGTERM, shutdown_handler)
    # Catch SIGUSER2 to solve strange behaviour with mpi4py
    signal.signal(signal.SIGUSR2, user_signal_handler)

    # Set the binding in worker mode
    CONTEXT.set_worker()

    persistent_storage = config.storage_conf != "null"

    logger, storage_loggers, _ = load_loggers(config.debug, persistent_storage)

    cache_profiler = False
    if config.cache_profiler.lower() == "true":
        cache_profiler = True

    if persistent_storage:
        # Initialize storage
        with EventWorker(TRACING_WORKER.init_storage_at_worker_event):
            from storage.api import (  # pylint: disable=E0401, C0415
                # disable=import-error, import-outside-toplevel
                initWorker as initStorageAtWorker,
            )

            initStorageAtWorker(config_file_path=config.storage_conf)

    executor_id = config.exec_ids[RANK - 1]
    executor_name = "".join(("Rank-", str(RANK)))
    conf = ExecutorConf(
        config.debug,
        GLOBALS.get_temporary_directory(),
        tracing,
        config.storage_conf,
        logger,
        persistent_storage,
        storage_loggers,
        config.stream_backend,
        config.stream_master_name,
        config.stream_master_port,
        cache_ids,
        in_cache_queue,
        out_cache_queue,
        cache_profiler,
    )
    executor(
        None,
        None,
        None,
        executor_id,
        executor_name,
        config.pipes[RANK - 1],
        conf,
    )

    if persistent_storage:
        # Finish storage
        if __debug__:
            logger.debug("%sStopping persistent storage", HEADER)
        with EventWorker(TRACING_WORKER.finish_storage_at_worker_event):
            from storage.api import (  # pylint: disable=E0401, C0415
                # disable=import-error, import-outside-toplevel
                finishWorker as finishStorageAtWorker,
            )

            finishStorageAtWorker()


############################
# Main -> Calls main method
############################


def main() -> None:
    """Start the MPI piper worker.

    :return: None.
    """
    tracing = sys.argv[6] == "true"

    # Enable coverage if performed
    if "COVERAGE_PROCESS_START" in os.environ:
        import coverage  # pylint: disable=import-outside-toplevel

        coverage.process_startup()

    # Configure the piper worker with the arguments
    worker_conf = PiperWorkerConfiguration()
    worker_conf.update_params(sys.argv)

    persistent_storage = worker_conf.storage_conf != "null"
    logger, _, log_dir = load_loggers(worker_conf.debug, persistent_storage)
    analysis_dir = GLOBALS.get_analysis_directory()

    cache_profiler = False
    if worker_conf.cache_profiler.lower() == "true":
        cache_profiler = True

    # No cache or it is an executor
    cache = False
    in_cache_queue = None  # type: typing.Any
    out_cache_queue = None  # type: typing.Any
    cache_ids = None
    if is_worker():
        # Setup cache if enabled
        if is_cache_enabled(str(worker_conf.cache)):
            # Deploy the necessary processes
            cache = True
            cache_params = start_cache(
                logger, str(worker_conf.cache), cache_profiler, analysis_dir
            )
            (
                smm,
                cache_process,
                in_cache_queue,
                out_cache_queue,
                cache_ids,
            ) = cache_params

    if is_worker():
        with trace_mpi_worker() if tracing else dummy_context():
            compss_persistent_worker(worker_conf)
    else:
        with trace_mpi_executor() if tracing else dummy_context():
            compss_persistent_executor(
                worker_conf,
                tracing,
                in_cache_queue,
                out_cache_queue,
                cache_ids,
            )

    if cache and is_worker():
        # Beware of smm, in_cache_queue, out_cache_queue and cache_process
        # variables, since they are only initialized when is_worker() and
        # cache is enabled.# Reason for noqa.
        stop_cache(
            smm, in_cache_queue, out_cache_queue, cache_profiler, cache_process
        )


if __name__ == "__main__":
    main()
