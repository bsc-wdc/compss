#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import logging
import os
import signal
from multiprocessing import Process
from multiprocessing import Queue
from pycompss.util.logs import init_logging_worker
from pycompss.worker.pipe_constants import *
from pycompss.worker.piper_executor import Pipe
from pycompss.worker.piper_executor import ExecutorConf
from pycompss.worker.piper_executor import executor

# Persistent worker global variables
PROCESSES = {} # IN_PIPE -> PROCES

class PiperWorkerConfiguration(object):
    """
    Description of the configuration parameters for the Piper Worker
    """

    def __init__(self):
        """
        Constructs an empty configuration description for the piper worker
        """
        self.debug = False
        self.tracing = False
        self.storage_conf = None
        self.tasks_x_node = 0
        self.pipes = []
        self.control_pipe = None

    def update_params(self, argv):
        """
        Constructs a configuration description for the piper worker using the
        arguments

        :param argv: arguments from the command line
        """
        self.debug = argv[1] == 'true'
        self.tracing = argv[2] == 'true'
        self.storage_conf = argv[3]
        self.tasks_x_node = int(argv[4])
        in_pipes = argv[5:5 + self.tasks_x_node]
        out_pipes = argv[5 + self.tasks_x_node:-2]
        if self.debug:
            assert self.tasks_x_node == len(in_pipes)
            assert self.tasks_x_node == len(out_pipes)
        self.pipes = []
        for i in range(0, self.tasks_x_node):
            self.pipes.append(Pipe(in_pipes[i], out_pipes[i]))
        self.control_pipe = Pipe(argv[-2], argv[-1])

    def print_on_logger(self, logger):
        """
        Prints the configuration through the logger

        :param logger: logger to output the configuration
        """
        logger.debug("[PYTHON WORKER] -----------------------------")
        logger.debug("[PYTHON WORKER] Persistent worker parameters:")
        logger.debug("[PYTHON WORKER] -----------------------------")
        logger.debug("[PYTHON WORKER] Debug          : " + str(self.debug))
        logger.debug("[PYTHON WORKER] Tracing        : " + str(self.tracing))
        logger.debug("[PYTHON WORKER] Tasks per node : " + str(self.tasks_x_node))
        logger.debug("[PYTHON WORKER] Pipe Pairs     : ")
        for pipe in self.pipes:
            logger.debug("[PYTHON WORKER]                  * "+str(pipe))
        logger.debug("[PYTHON WORKER] Storage conf.  : " + str(self.storage_conf))
        logger.debug("[PYTHON WORKER] -----------------------------")

def shutdown_handler(signal, frame):
    """
    Shutdown handler (do not remove the parameters).

    :param signal: shutdown signal
    :param frame: Frame
    :return: None
    """
    for proc in PROCESSES.values():
        if proc.is_alive():
            proc.terminate()

def load_loggers(debug, persistent_storage):
    """
    Loads all the loggers

    :param debug: is Debug enabled
    :param persistent_storage: is persistent storage enabled

    :return logger: main logger of the application
    :return storage_loggers: loggers for the persistent data engine
    """
    # Load log level configuration file
    worker_path = os.path.dirname(os.path.realpath(__file__))
    if debug:
        # Debug
        init_logging_worker(worker_path + '/../../log/logging_debug.json')
    else:
        # Default
        init_logging_worker(worker_path + '/../../log/logging_off.json')

    # Define logger facilities
    logger = logging.getLogger('pycompss.worker.piper_worker')
    storage_loggers = []
    if persistent_storage:
        storage_loggers.append(logging.getLogger('dataclay'))
        storage_loggers.append(logging.getLogger('hecuba'))
        storage_loggers.append(logging.getLogger('redis'))
        storage_loggers.append(logging.getLogger('storage'))
    return logger, storage_loggers

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

    # Catch SIGTERM sent by bindings_piper
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Set the binding in worker mode
    import pycompss.util.context as context
    context.set_pycompss_context(context.WORKER)

    if TRACING:
        import os
        try:
            user_paths = os.environ['PYTHONPATH']
        except KeyError:
            user_paths = ""
        print("PYTHON PATH = "+user_paths)
        import pyextrae.multiprocessing as pyextrae
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

    def create_threads(process_name, pipe):
        """
        Creates and starts a new process where to run tasks and updates
        the corresponding variables (PROCESSES, queues).

        :param i: index of the process
        :param process_name: name of the process
        :return pid: id of process running the new executor
        """
        queue = Queue()
        queues.append(queue)
        conf = ExecutorConf(TRACING, config.storage_conf, logger, storage_loggers)
        process = Process(target=executor, args=(queue,
                                                 process_name,
                                                 pipe,
                                                 conf))
        PROCESSES[pipe.input_pipe] = process
        process.start()
        return process.pid

    # Create new threads
    queues = []
    for i in range(0, config.tasks_x_node):
        if __debug__:
            logger.debug("[PYTHON WORKER] Launching process " + str(i))
        process_name = 'Process-' + str(i)
        create_threads(process_name, config.pipes[i])

    # Read command from control pipe
    alive = True
    process_counter = config.tasks_x_node
    control_pipe = config.control_pipe
    while alive:
        command = control_pipe.read_command(retry_period=1)
        if command != "":
            line = command.split()
            if line[0] == ADD_EXECUTOR_TAG:

                process_name = 'Process-' + str(process_counter)
                process_counter = process_counter + 1
                in_pipe = line[1]
                out_pipe = line[2]
                pipe = Pipe(in_pipe, out_pipe)
                pid = create_threads(process_name, pipe)
                control_pipe.write(ADDED_EXECUTOR_TAG+" "+out_pipe+" "+in_pipe+" "+str(pid))

            elif line[0] == QUERY_EXECUTOR_ID_TAG:
                in_pipe = line[1]
                out_pipe = line[2]
                proc = PROCESSES.get(in_pipe)
                pid = proc.pid
                control_pipe.write(REPLY_EXECUTOR_ID_TAG+" "+out_pipe+" "+in_pipe+" "+str(pid))

            elif line[0] == REMOVE_EXECUTOR_TAG:

                in_pipe = line[1]
                out_pipe = line[2]

                proc = PROCESSES.pop(in_pipe, None)

                if proc:
                    if proc.is_alive():
                        logger.warn("[PYTHON WORKER] Forcing terminate on : " + proc.name)
                        proc.terminate()
                    proc.join()
                control_pipe.write(REMOVED_EXECUTOR_TAG +" "+out_pipe+" "+in_pipe)
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
            logger.error("[PYTHON WORKER] Exception in threads queue: " + str(queues[i].get()))

    for queue in queues:
        queue.close()
        queue.join_thread()

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

############################
# Main -> Calls main method
############################

if __name__ == '__main__':
    import sys
    # Get args
    print(str(sys.argv))
    global TRACING
    TRACING = (sys.argv[2] == 'true')
    print("Tracing:"+str(TRACING))
    WORKER_CONF = PiperWorkerConfiguration()
    WORKER_CONF.update_params(sys.argv)

    compss_persistent_worker(WORKER_CONF)

