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
from multiprocessing import Process
from multiprocessing import Queue

from pycompss.runtime.commons import range
from pycompss.util.exceptions import NotImplementedException
from pycompss.util.tracing.helpers import trace_multiprocessing_worker
from pycompss.util.tracing.helpers import dummy_context
from pycompss.util.tracing.helpers import event
from pycompss.worker.commons.constants import INIT_STORAGE_AT_WORKER_EVENT
from pycompss.worker.commons.constants import FINISH_STORAGE_AT_WORKER_EVENT
from pycompss.worker.piper.commons.constants import CANCEL_TASK_TAG
from pycompss.worker.piper.commons.constants import PING_TAG
from pycompss.worker.piper.commons.constants import PONG_TAG
from pycompss.worker.piper.commons.constants import ADD_EXECUTOR_TAG
from pycompss.worker.piper.commons.constants import ADDED_EXECUTOR_TAG
from pycompss.worker.piper.commons.constants import QUERY_EXECUTOR_ID_TAG
from pycompss.worker.piper.commons.constants import REPLY_EXECUTOR_ID_TAG
from pycompss.worker.piper.commons.constants import REMOVE_EXECUTOR_TAG
from pycompss.worker.piper.commons.constants import REMOVED_EXECUTOR_TAG
from pycompss.worker.piper.commons.constants import QUIT_TAG
from pycompss.worker.piper.commons.constants import HEADER
from pycompss.worker.piper.commons.executor import Pipe
from pycompss.worker.piper.commons.executor import ExecutorConf
from pycompss.worker.piper.commons.executor import executor
from pycompss.worker.piper.commons.utils import load_loggers
from pycompss.worker.piper.commons.utils import PiperWorkerConfiguration
import pycompss.util.context as context

# Persistent worker global variables
PROCESSES = {}  # IN_PIPE -> PROCESS
TRACING = False
WORKER_CONF = None


def shutdown_handler(signal, frame):  # noqa
    """ Shutdown handler.

    Do not remove the parameters.

    :param signal: shutdown signal.
    :param frame: Frame.
    :return: None
    """
    for proc in PROCESSES.values():
        if proc.is_alive():
            proc.terminate()


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
    # Catch SIGTERM sent by bindings_piper
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Set the binding in worker mode
    context.set_pycompss_context(context.WORKER)

    persistent_storage = (config.storage_conf != 'null')

    logger, storage_loggers = load_loggers(config.debug,
                                           persistent_storage,
                                           config.tracing)

    if __debug__:
        logger.debug(HEADER + "piper_worker.py wake up")
        config.print_on_logger(logger)

    if persistent_storage:
        # Initialize storage
        logger.debug(HEADER + "Starting persistent storage")
        with event(INIT_STORAGE_AT_WORKER_EVENT):
            from storage.api import initWorker as initStorageAtWorker  # noqa
            initStorageAtWorker(config_file_path=config.storage_conf)

    # Create new threads
    queues = []

    conf = ExecutorConf(TRACING,
                        config.storage_conf,
                        logger,
                        storage_loggers,
                        config.stream_backend,
                        config.stream_master_name,
                        config.stream_master_port)

    for i in range(0, config.tasks_x_node):
        if __debug__:
            logger.debug(HEADER + "Launching process " + str(i))
        process_name = "".join(("Process-", str(i)))
        pid, queue = create_executor_process(process_name, conf, config.pipes[i])
        queues.append(queue)

    # Read command from control pipe
    alive = True
    process_counter = config.tasks_x_node
    control_pipe = config.control_pipe
    while alive:
        command = control_pipe.read_command(retry_period=1)
        if command != "":
            line = command.split()

            if line[0] == ADD_EXECUTOR_TAG:
                process_name = "".join(("Process-", str(process_counter)))
                process_counter = process_counter + 1
                in_pipe = line[1]
                out_pipe = line[2]
                pipe = Pipe(in_pipe, out_pipe)
                pid, queue = create_executor_process(process_name, conf, pipe)
                queues.append(queue)
                control_pipe.write(" ".join((ADDED_EXECUTOR_TAG,
                                             out_pipe,
                                             in_pipe,
                                             str(pid))))

            elif line[0] == QUERY_EXECUTOR_ID_TAG:
                in_pipe = line[1]
                out_pipe = line[2]
                proc = PROCESSES.get(in_pipe)
                pid = proc.pid
                control_pipe.write(" ".join((REPLY_EXECUTOR_ID_TAG,
                                             out_pipe,
                                             in_pipe,
                                             str(pid))))

            elif line[0] == CANCEL_TASK_TAG:
                in_pipe = line[1]
                proc = PROCESSES.get(in_pipe)
                pid = proc.pid
                if __debug__:
                    logger.debug(HEADER + "Signaling process with PID " +
                                 str(pid) + " to cancel a task")
                os.kill(pid, signal.SIGUSR2)  # NOSONAR cancellation produced by COMPSs

            elif line[0] == REMOVE_EXECUTOR_TAG:
                in_pipe = line[1]
                out_pipe = line[2]
                proc = PROCESSES.pop(in_pipe, None)
                if proc:
                    if proc.is_alive():
                        logger.warn(HEADER + "Forcing terminate on : " +
                                    proc.name)
                        proc.terminate()
                    proc.join()
                control_pipe.write(" ".join((REMOVED_EXECUTOR_TAG,
                                             out_pipe,
                                             in_pipe)))

            elif line[0] == PING_TAG:
                control_pipe.write(PONG_TAG)

            elif line[0] == QUIT_TAG:
                alive = False

    # Wait for all threads
    for proc in PROCESSES.values():
        proc.join()

    # Check if there is any exception message from the threads
    for i in range(0, config.tasks_x_node):
        if not queues[i].empty:
            logger.error(HEADER + "Exception in threads queue: " +
                         str(queues[i].get()))

    for queue in queues:
        queue.close()
        queue.join_thread()

    if persistent_storage:
        # Finish storage
        if __debug__:
            logger.debug(HEADER + "Stopping persistent storage")
        with event(FINISH_STORAGE_AT_WORKER_EVENT):
            from storage.api import finishWorker as finishStorageAtWorker  # noqa
            finishStorageAtWorker()

    if __debug__:
        logger.debug(HEADER + "Finished")

    control_pipe.write(QUIT_TAG)
    control_pipe.close()


def create_executor_process(process_name, conf, pipe):
    # type: (str, dict, ...) -> (int, queue)
    """ Starts a new executor.

    :param process_name: Process name.
    :param conf: executor config.
    :param pipe: Communication pipes (in, out).
    :return: Process identifier and queue used by the process
    """
    queue = Queue()
    process = Process(target=executor, args=(queue,
                                             process_name,
                                             pipe,
                                             conf))
    PROCESSES[pipe.input_pipe] = process
    process.start()
    return process.pid, queue


############################
# Main -> Calls main method
############################

def main():
    global TRACING
    global WORKER_CONF
    # Configure the global tracing variable from the argument
    TRACING = (int(sys.argv[4]) > 0)

    with trace_multiprocessing_worker() if TRACING else dummy_context():
        # Configure the piper worker with the arguments
        WORKER_CONF = PiperWorkerConfiguration()
        WORKER_CONF.update_params(sys.argv)

        compss_persistent_worker(WORKER_CONF)


if __name__ == '__main__':
    main()
