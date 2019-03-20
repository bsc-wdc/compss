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
import signal

from pycompss.worker.pipe_constants import *
from pycompss.worker.piper_executor import Pipe
from pycompss.worker.piper_executor import ExecutorConf
from pycompss.worker.piper_executor import executor
from pycompss.worker.piper_worker import load_loggers
from pycompss.worker.piper_worker import PiperWorkerConfiguration
from mpi4py import MPI


# Persistent worker global variables
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

PROCESSES = {} # IN_PIPE -> PROCESS ID

def is_worker():
    """
    Returns whether the process should act as a worker

    :return: the process should act as a worker
    """
    return rank == 0

def shutdown_handler(signal, frame):
    """
    Shutdown handler (do not remove the parameters).

    :param signal: shutdown signal
    :param frame: Frame
    :return: None
    """
    if is_worker():
        print("[PYTHON WORKER] Shutdown signal handler")
    else:
        print("[PYTHON EXECUTOR %s] Shutdown signal handler" % rank)


######################
# Main method
######################
def compss_persistent_worker(config):
    """
    Persistent worker main function.
    Retrieves the initial configuration and spawns the worker processes.

    :param config: Piper Worker Configuration description
    :return: None
    """
    import os
    pids = comm.gather(str(os.getpid()), root=0)

    # Catch SIGTERM sent by bindings_piper
    signal.signal(signal.SIGTERM, shutdown_handler)


    # Set the binding in worker mode
    import pycompss.util.context as context
    context.set_pycompss_context(context.WORKER)

    if TRACING:
        import pyextrae.mpi as pyextrae
        pyextrae.eventandcounters(SYNC_EVENTS, 1)
        pyextrae.eventandcounters(TASK_EVENTS, WORKER_RUNNING_EVENT)

    persistent_storage = (config.storage_conf != 'null')

    logger, storage_loggers = load_loggers(config.debug, persistent_storage)

    if __debug__:
        logger.debug("[PYTHON WORKER] piper_worker.py wake up")
        config.print_on_logger(logger)

    if persistent_storage:
        # Initialize storage
        from storage.api import initWorker as initStorageAtWorker
        initStorageAtWorker(config_file_path=config.storage_conf)

    for i in range(0, config.tasks_x_node):
        child_in_pipe = config.pipes[i].input_pipe
        child_pid = pids[i+1]
        PROCESSES[child_in_pipe] = child_pid

    # Read command from control pipe
    alive = True
    control_pipe = config.control_pipe
    while alive:
        command = control_pipe.read_command()
        if command != "":
            line = command.split()
            if line[0] == REMOVE_EXECUTOR_TAG:
                in_pipe = line[1]
                out_pipe = line[2]

                pid = PROCESSES.pop(in_pipe, None)
                control_pipe.write(REMOVED_EXECUTOR_TAG +" "+out_pipe+" "+in_pipe)

            elif line[0] == QUERY_EXECUTOR_ID_TAG:
                in_pipe = line[1]
                out_pipe = line[2]
                pid = PROCESSES.get(in_pipe)
                control_pipe.write(REPLY_EXECUTOR_ID_TAG+" "+out_pipe+" "+in_pipe+" "+str(pid))

            elif line[0] == PING_TAG:
                control_pipe.write(PONG_TAG)

            elif line[0] == QUIT_TAG:
                alive = False
            else:
                logger.debug("UNKNOWN COMMAND "+ command)

    if persistent_storage:
        # Finish storage
        from storage.api import finishWorker as finishStorageAtWorker
        finishStorageAtWorker()

    if __debug__:
        logger.debug("[PYTHON WORKER] Finished")

    if TRACING:
        pyextrae.eventandcounters(TASK_EVENTS, 0)
        pyextrae.eventandcounters(SYNC_EVENTS, 0)

    control_pipe.write(QUIT_TAG)
    control_pipe.close()


def compss_persistent_executor(config):
    """
    Persistent worker main function.
    Retrieves the initial configuration and spawns the worker processes.

    :param config: Piper Worker Configuration description
    :return: None
    """
    import os
    comm.gather(str(os.getpid()), root=0)

    # Catch SIGTERM sent by bindings_piper
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Set the binding in worker mode
    import pycompss.util.context as context
    context.set_pycompss_context(context.WORKER)

    persistent_storage = (config.storage_conf != 'null')

    logger, storage_loggers = load_loggers(config.debug, persistent_storage)

    if persistent_storage:
        # Initialize storage
        from storage.api import initWorker as initStorageAtWorker
        initStorageAtWorker(config_file_path=config.storage_conf)

    process_name = 'Rank-' + str(rank)
    conf = ExecutorConf(TRACING, config.storage_conf, logger, storage_loggers)
    executor(None, process_name, config.pipes[rank-1], conf)

############################
# Main -> Calls main method
############################

if __name__ == '__main__':
    import sys
    # Get args
    global TRACING
    TRACING = (sys.argv[2] == 'true')
    WORKER_CONF = PiperWorkerConfiguration()
    WORKER_CONF.update_params(sys.argv)

    if is_worker():
        compss_persistent_worker(WORKER_CONF)
    else:
        compss_persistent_executor(WORKER_CONF)

