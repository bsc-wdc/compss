#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Persistent Worker
===========================
    This file contains the worker code.
"""

import os
import sys
import signal
from pycompss.runtime.commons import range
from pycompss.util.tracing.helpers import trace_mpi_worker
from pycompss.util.tracing.helpers import trace_mpi_executor
from pycompss.util.tracing.helpers import dummy_context
from pycompss.util.tracing.helpers import event
from pycompss.worker.commons.constants import INIT_STORAGE_AT_WORKER_EVENT
from pycompss.worker.commons.constants import FINISH_STORAGE_AT_WORKER_EVENT
from pycompss.worker.piper.commons.executor import ExecutorConf
from pycompss.worker.piper.commons.executor import executor
from pycompss.worker.piper.commons.utils import load_loggers
from pycompss.worker.piper.commons.utils import PiperWorkerConfiguration
from pycompss.worker.piper.commons.constants import CANCEL_TASK_TAG
from pycompss.worker.piper.commons.constants import PING_TAG
from pycompss.worker.piper.commons.constants import PONG_TAG
from pycompss.worker.piper.commons.constants import ADD_EXECUTOR_TAG
from pycompss.worker.piper.commons.constants import ADD_EXECUTOR_FAILED_TAG
from pycompss.worker.piper.commons.constants import QUERY_EXECUTOR_ID_TAG
from pycompss.worker.piper.commons.constants import REPLY_EXECUTOR_ID_TAG
from pycompss.worker.piper.commons.constants import REMOVE_EXECUTOR_TAG
from pycompss.worker.piper.commons.constants import REMOVED_EXECUTOR_TAG
from pycompss.worker.piper.commons.constants import QUIT_TAG
from pycompss.worker.piper.commons.constants import HEADER
from pycompss.worker.piper.cache.setup import is_cache_enabled
from pycompss.worker.piper.cache.setup import start_cache
from pycompss.worker.piper.cache.setup import stop_cache

from mpi4py import MPI

# Persistent worker global variables
COMM = MPI.COMM_WORLD
SIZE = COMM.Get_size()
RANK = COMM.Get_rank()
PROCESSES = {}  # IN_PIPE -> PROCESS ID
TRACING = False
WORKER_CONF = None
CACHE_IDS = None
CACHE_QUEUE = None


def is_worker():
    # type: () -> bool
    """ Returns whether the process should act as a worker.

    :return: the process should act as a worker.
    """
    return RANK == 0


def shutdown_handler(signal, frame):  # noqa
    """ Shutdown handler.

    Do not remove the parameters.

    :param signal: shutdown signal.
    :param frame: Frame.
    :return: None
    """
    if is_worker():
        print(HEADER + "Shutdown signal handler")
    else:
        print("[PYTHON EXECUTOR %s] Shutdown signal handler" % RANK)


def user_signal_handler(signal, frame):  # noqa
    """ User signal handler.

    Do not remove the parameters.

    :param signal: shutdown signal.
    :param frame: Frame.
    :return: None
    """
    if is_worker():
        print(HEADER + "Default user signal handler")
    else:
        print("[PYTHON EXECUTOR %s] Default user signal handler" % RANK)


######################
# Main method
######################

def compss_persistent_worker(config):
    # type: (PiperWorkerConfiguration) -> None
    """ Persistent worker main function.

    Retrieves the initial configuration and spawns the worker processes.

    :param config: Piper Worker Configuration description.
    :return: None
    """
    pids = COMM.gather(str(os.getpid()), root=0)

    # Catch SIGTERM sent by bindings_piper
    signal.signal(signal.SIGTERM, shutdown_handler)
    # Catch SIGUSER2 to solve strange behaviour with mpi4py
    signal.signal(signal.SIGUSR2, user_signal_handler)

    # Set the binding in worker mode
    import pycompss.util.context as context
    context.set_pycompss_context(context.WORKER)

    persistent_storage = (config.storage_conf != 'null')

    logger, _ = load_loggers(config.debug,
                             persistent_storage,
                             config.tracing)

    if __debug__:
        logger.debug(HEADER + "mpi_piper_worker.py rank: " + str(RANK) +
                     " wake up")
        config.print_on_logger(logger)

    # Start storage
    if persistent_storage:
        # Initialize storage
        if __debug__:
            logger.debug(HEADER + "Starting persistent storage")
        from storage.api import initWorker as initStorageAtWorker  # noqa
        initStorageAtWorker(config_file_path=config.storage_conf)

    for i in range(0, config.tasks_x_node):
        child_in_pipe = config.pipes[i].input_pipe
        try:
            child_pid = pids[i + 1]
        except IndexError:
            child_pid = pids[i]
        PROCESSES[child_in_pipe] = child_pid

    if __debug__:
        logger.debug(HEADER + "Starting alive")
        logger.debug(HEADER + "Control pipe: " + str(config.control_pipe))
    # Read command from control pipe
    alive = True
    control_pipe = config.control_pipe
    while alive:
        command = control_pipe.read_command()
        if command != "":
            line = command.split()
            if line[0] == ADD_EXECUTOR_TAG:
                in_pipe = line[1]
                out_pipe = line[2]
                control_pipe.write(" ".join((ADD_EXECUTOR_FAILED_TAG,
                                             out_pipe,
                                             in_pipe,
                                             str(0))))

            elif line[0] == REMOVE_EXECUTOR_TAG:
                in_pipe = line[1]
                out_pipe = line[2]
                PROCESSES.pop(in_pipe, None)
                control_pipe.write(" ".join((REMOVED_EXECUTOR_TAG,
                                             out_pipe,
                                             in_pipe)))

            elif line[0] == QUERY_EXECUTOR_ID_TAG:
                in_pipe = line[1]
                out_pipe = line[2]
                pid = PROCESSES.get(in_pipe)
                control_pipe.write(" ".join((REPLY_EXECUTOR_ID_TAG,
                                             out_pipe,
                                             in_pipe,
                                             str(pid))))

            elif line[0] == CANCEL_TASK_TAG:
                in_pipe = line[1]
                pid = PROCESSES.get(in_pipe)
                logger.debug(HEADER + "Signaling process with PID " +
                             pid + " to cancel a task")
                os.kill(int(pid), signal.SIGUSR2)  # NOSONAR cancellation produced by COMPSs

            elif line[0] == PING_TAG:
                control_pipe.write(PONG_TAG)

            elif line[0] == QUIT_TAG:
                alive = False
            else:
                logger.debug(HEADER + "ERROR: UNKNOWN COMMAND: " + command)
                alive = False

    # Stop storage
    if persistent_storage:
        # Finish storage
        logger.debug(HEADER + "Stopping persistent storage")
        from storage.api import finishWorker as finishStorageAtWorker  # noqa
        finishStorageAtWorker()

    if __debug__:
        logger.debug(HEADER + "Finished")

    control_pipe.write(QUIT_TAG)
    control_pipe.close()


def compss_persistent_executor(config):
    # type: (PiperWorkerConfiguration) -> None
    """ Persistent executor main function.

    Retrieves the initial configuration and spawns the worker processes.

    :param config: Piper Worker Configuration description.
    :return: None
    """
    COMM.gather(str(os.getpid()), root=0)

    # Catch SIGTERM sent by bindings_piper
    signal.signal(signal.SIGTERM, shutdown_handler)
    # Catch SIGUSER2 to solve strange behaviour with mpi4py
    signal.signal(signal.SIGUSR2, user_signal_handler)

    # Set the binding in worker mode
    import pycompss.util.context as context
    context.set_pycompss_context(context.WORKER)

    persistent_storage = (config.storage_conf != 'null')

    logger, storage_loggers = load_loggers(config.debug,
                                           persistent_storage,
                                           config.tracing)

    if persistent_storage:
        # Initialize storage
        with event(INIT_STORAGE_AT_WORKER_EVENT):
            from storage.api import initWorker as initStorageAtWorker  # noqa
            initStorageAtWorker(config_file_path=config.storage_conf)

    process_name = "".join(("Rank-", str(RANK)))
    conf = ExecutorConf(TRACING,
                        config.storage_conf,
                        logger,
                        storage_loggers,
                        config.stream_backend,
                        config.stream_master_name,
                        config.stream_master_port,
                        CACHE_IDS,
                        CACHE_QUEUE)
    executor(None, process_name, config.pipes[RANK - 1], conf)

    if persistent_storage:
        # Finish storage
        if __debug__:
            logger.debug(HEADER + "Stopping persistent storage")
        with event(FINISH_STORAGE_AT_WORKER_EVENT):
            from storage.api import finishWorker as finishStorageAtWorker  # noqa
            finishStorageAtWorker()


############################
# Main -> Calls main method
############################

def main():
    # Configure the global tracing variable from the argument
    global TRACING
    global WORKER_CONF
    global CACHE_IDS
    global CACHE_QUEUE

    TRACING = (int(sys.argv[4]) > 0)

    # Enable coverage if performed
    if "COVERAGE_PROCESS_START" in os.environ:
        import coverage
        coverage.process_startup()

    # Configure the piper worker with the arguments
    WORKER_CONF = PiperWorkerConfiguration()
    WORKER_CONF.update_params(sys.argv)

    if is_worker():
        # Setup cache
        if is_cache_enabled(WORKER_CONF.cache):
            # Deploy the necessary processes
            cache = True
            cache_params = start_cache(None, WORKER_CONF.cache)
        else:
            # No cache
            cache = False
            cache_params = (None, None, None, None)
    else:
        # Otherwise it is an executor
        cache = False  # to stop only the cache from the main process
        cache_params = (None, None, None, None)
    smm, cache_process, CACHE_QUEUE, CACHE_IDS = cache_params

    if is_worker():
        with trace_mpi_worker() if TRACING else dummy_context():
            compss_persistent_worker(WORKER_CONF)
    else:
        with trace_mpi_executor() if TRACING else dummy_context():
            compss_persistent_executor(WORKER_CONF)

    if cache and is_worker():
        stop_cache(smm, CACHE_QUEUE, cache_process)  # noqa


if __name__ == '__main__':
    main()
