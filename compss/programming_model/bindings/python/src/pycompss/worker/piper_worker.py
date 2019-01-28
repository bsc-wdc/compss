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
import sys
import copy
from multiprocessing import Process
from multiprocessing import Queue
import thread_affinity
from pycompss.util.logs import init_logging_worker

SYNC_EVENTS = 8000666

# Should be equal to Tracer.java definitions (but only worker running all other are trace through
# with function-list
TASK_EVENTS = 60000100
WORKER_RUNNING = 102

# Persistent worker global variables
TRACING = False
PROCESSES = []

# if sys.version_info >= (2, 7):
#     import importlib


#####################
#  Tag variables
#####################
EXECUTE_TASK_TAG = "EXECUTE_TASK"  # -- "task" taskId jobOut jobErr task_params
END_TASK_TAG = "END_TASK"  # -- "endTask" taskId endStatus
ERROR_TASK_TAG = "ERROR_TASK"
QUIT_TAG = "QUIT"  # -- "quit"
REMOVE_TAG = "REMOVE"
SERIALIZE_TAG = "SERIALIZE"


######################
#  Processes body
######################
def worker(queue, process_name, input_pipe, output_pipe, storage_conf, logger):
    """
    Thread main body - Overrides Threading run method.
    Iterates over the input pipe in order to receive tasks (with their
    parameters) and process them.
    Notifies the runtime when each task  has finished with the
    corresponding output value.
    Finishes when the "quit" message is received.

    :param queue: Queue where to put exception messages
    :param process_name: Process name (Thread-X, where X is the thread id).
    :param input_pipe: Input pipe for the thread. To receive messages from the runtime.
    :param output_pipe: Output pipe for the thread. To send messages to the runtime.
    :param storage_conf: Storage configuration file
    :param logger: Logger
    :return: None
    """

    # Get a copy of the necessary information from the logger to re-establish after each task
    logger_handlers = copy.copy(logger.handlers)
    logger_level = logger.getEffectiveLevel()
    logger_formatter = logging.Formatter(logger_handlers[0].formatter._fmt)

    if storage_conf != 'null':
        try:
            from storage.api import initWorkerPostFork as initStorageAtWorkerPostFork
            initStorageAtWorkerPostFork()
        except ImportError:
            if __debug__:
                logger.info("[PYTHON WORKER] Could not find initWorkerPostFork storage call. Ignoring it.")

    alive = True
    stdout = sys.stdout
    stderr = sys.stderr

    if __debug__:
        logger.debug("[PYTHON WORKER] Starting process " + str(process_name))

    while alive:
        in_pipe = open(input_pipe, 'r')  # , 0) # 0 just for python 2

        affinity_ok = True

        def process_task(current_line, pipe):
            if __debug__:
                logger.debug("[PYTHON WORKER] Received message: %s" % str(current_line))
            current_line = current_line.split()
            pipe.close()
            if current_line[0] == EXECUTE_TASK_TAG:
                # CPU binding
                binded_cpus = current_line[-3]

                def bind_cpus(binded_cpus):
                    if binded_cpus != "-":
                        os.environ['COMPSS_BINDED_CPUS'] = binded_cpus
                        if __debug__:
                            logger.debug("[PYTHON WORKER] Assigning affinity %s" % str(binded_cpus))
                        binded_cpus = list(map(int, binded_cpus.split(",")))
                        try:
                            thread_affinity.setaffinity(binded_cpus)
                        except Exception:
                            if __debug__:
                                logger.error("[PYTHON WORKER] Warning: could not assign affinity %s" % str(binded_cpus))
                            affinity_ok = False

                bind_cpus(binded_cpus)

                # GPU binding
                binded_gpus = current_line[-2]

                def bind_gpus(current_binded_gpus):
                    if current_binded_gpus != "-":
                        os.environ['COMPSS_BINDED_GPUS'] = current_binded_gpus
                        os.environ['CUDA_VISIBLE_DEVICES'] = current_binded_gpus
                        os.environ['GPU_DEVICE_ORDINAL'] = current_binded_gpus

                bind_gpus(binded_gpus)

                # Remove the last elements: cpu and gpu bindings
                current_line = current_line[0:-3]

                # task jobId command
                job_id = current_line[1]
                job_out = current_line[2]
                job_err = current_line[3]
                # current_line[4] = <boolean> = tracing
                # current_line[5] = <integer> = task id
                # current_line[6] = <boolean> = debug
                # current_line[7] = <string>  = storage conf.
                # current_line[8] = <string>  = operation type (e.g. METHOD)
                # current_line[9] = <string>  = module
                # current_line[10]= <string>  = method
                # current_line[11]= <integer> = Number of slaves (worker nodes) == #nodes
                # <<list of slave nodes>>
                # current_line[11 + #nodes] = <integer> = computing units
                # current_line[12 + #nodes] = <boolean> = has target
                # current_line[13 + #nodes] = <string>  = has return (always 'null')
                # current_line[14 + #nodes] = <integer> = Number of parameters
                # <<list of parameters>>
                #       !---> type, stream, prefix , value

                if __debug__:
                    logger.debug("[PYTHON WORKER %s] Received task." % str(process_name))
                    logger.debug("[PYTHON WORKER %s] - TASK CMD: %s" % (str(process_name), str(current_line)))

                # Swap logger from stream handler to file handler - All task output will be redirected to job.out/err
                for log_handler in logger_handlers:
                    logger.removeHandler(log_handler)
                out_file_handler = logging.FileHandler(job_out)
                out_file_handler.setLevel(logger_level)
                out_file_handler.setFormatter(logger_formatter)
                logger.addHandler(out_file_handler)
                err_file_handler = logging.FileHandler(job_err)
                err_file_handler.setLevel(logging.ERROR)
                err_file_handler.setFormatter(logger_formatter)
                logger.addHandler(err_file_handler)

                if __debug__:
                    logger.debug("[PYTHON WORKER %s] Received task." % str(process_name))
                    logger.debug("[PYTHON WORKER %s] - TASK CMD: %s" % (str(process_name), str(current_line)))

                try:
                    # Setup out/err wrappers
                    out = open(job_out, 'a')
                    err = open(job_err, 'a')
                    sys.stdout = out
                    sys.stderr = err

                    # Check thread affinity
                    if not affinity_ok:
                        err.write(
                            "WARNING: This task is going to be executed with default thread affinity %s" % thread_affinity.getaffinity())

                    # Setup process environment
                    cn = int(current_line[11])
                    cn_names = ','.join(current_line[12:12 + cn])
                    cu = current_line[12 + cn]
                    os.environ["COMPSS_NUM_NODES"] = str(cn)
                    os.environ["COMPSS_HOSTNAMES"] = cn_names
                    os.environ["COMPSS_NUM_THREADS"] = cu
                    os.environ["OMP_NUM_THREADS"] = cu

                    # Execute task
                    from pycompss.worker.worker_commons import execute_task
                    exit_value, new_types, new_values = execute_task(process_name, storage_conf, current_line[9:],
                                                                     TRACING)

                    # Restore out/err wrappers
                    sys.stdout = stdout
                    sys.stderr = stderr
                    sys.stdout.flush()
                    sys.stderr.flush()
                    out.close()
                    err.close()

                    if exit_value == 0:
                        # Task has finished without exceptions
                        # endTask jobId exitValue message
                        params = build_return_params_message(new_types, new_values)
                        message = END_TASK_TAG + " " + str(job_id) \
                                  + " " + str(exit_value) \
                                  + " " + str(params) + "\n"
                    else:
                        # An exception has been raised in task
                        message = END_TASK_TAG + " " + str(job_id) + " " + str(exit_value) + "\n"

                    if __debug__:
                        logger.debug("[PYTHON WORKER %s] - Pipe %s END TASK MESSAGE: %s" % (str(process_name),
                                                                                            str(output_pipe),
                                                                                            str(message)))
                    # The return message is:
                    #
                    # TaskResult ==> jobId exitValue D List<Object>
                    #
                    # Where List<Object> has D * 2 length:
                    # D = #parameters == #task_parameters + (has_target ? 1 : 0) + #returns
                    # And contains a pair of elements per parameter:
                    #     - Parameter new type.
                    #     - Parameter new value:
                    #         - 'null' if it is NOT a PSCO
                    #         - PSCOId (String) if is a PSCO
                    # Example:
                    #     4 null 9 null 12 <pscoid>
                    #
                    # The order of the elements is: parameters + self + returns
                    #
                    # This is sent through the pipe with the END_TASK message.
                    # If the task had an object or file as parameter and the worker returns the id,
                    # the runtime can change the type (and locations) to a EXTERNAL_OBJ_T.

                    with open(output_pipe, 'w') as out_pipe:
                        out_pipe.write(message)
                except Exception as e:
                    logger.exception("[PYTHON WORKER %s] Exception %s" % (str(process_name), str(e)))
                    queue.put("EXCEPTION")

                if binded_cpus != "-":
                    del os.environ['COMPSS_BINDED_CPUS']
                if binded_gpus != "-":
                    del os.environ['COMPSS_BINDED_GPUS']
                    del os.environ['CUDA_VISIBLE_DEVICES']
                    del os.environ['GPU_DEVICE_ORDINAL']
                del os.environ['COMPSS_HOSTNAMES']

                # Restore logger
                logger.removeHandler(out_file_handler)
                logger.removeHandler(err_file_handler)
                for handler in logger_handlers:
                    logger.addHandler(handler)

            elif current_line[0] == QUIT_TAG:
                # Received quit message -> Suicide
                if __debug__:
                    logger.debug("[PYTHON WORKER %s] Received quit." % str(process_name))
                return False
            return True

        for line in in_pipe:
            if line != "":
                alive = process_task(line, in_pipe)
                break

    if storage_conf != 'null':
        try:
            from storage.api import finishWorkerPostFork as finishStorageAtWorkerPostFork
            finishStorageAtWorkerPostFork()
        except ImportError:
            if __debug__:
                logger.info("[PYTHON WORKER] Could not find finishWorkerPostFork storage call. Ignoring it.")

    sys.stdout.flush()
    sys.stderr.flush()
    print("[PYTHON WORKER] Exiting process ", process_name)


def build_return_params_message(types, values):
    """
    Build the return message with the parameters output.

    :param types: List of the parameter's types
    :param values: List of the parameter's values
    :return: Message as string
    """

    assert len(types) == len(values), 'Inconsistent state: return type-value length mismatch for return message.'
    pairs = list(zip(types, values))
    num_params = len(pairs)
    params = ''
    for p in pairs:
        params = params + str(p[0]) + ' ' + str(p[1]) + ' '
    message = str(num_params) + ' ' + params
    return message


def shutdown_handler(signal, frame):
    """
    Shutdown handler (do not remove the parameters).

    :param signal: shutdown signal
    :param frame: Frame
    :return: None
    """

    for proc in PROCESSES:
        if proc.is_alive():
            proc.terminate()


######################
# Main method
######################


def compss_persistent_worker():
    """
    Persistent worker main function.
    Retrieves the initial configuration and spawns the worker processes.

    :return: None
    """

    # Set the binding in worker mode
    import pycompss.util.context as context
    context.set_pycompss_context(context.WORKER)

    # Get args
    debug = (sys.argv[1] == 'true')
    global TRACING
    TRACING = (sys.argv[2] == 'true')
    storage_conf = sys.argv[3]
    tasks_x_node = int(sys.argv[4])
    in_pipes = sys.argv[5:5 + tasks_x_node]
    out_pipes = sys.argv[5 + tasks_x_node:]

    if TRACING:
        import pyextrae.multiprocessing as pyextrae
        pyextrae.eventandcounters(SYNC_EVENTS, 1)
        pyextrae.eventandcounters(TASK_EVENTS, WORKER_RUNNING)

    if debug:
        assert tasks_x_node == len(in_pipes)
        assert tasks_x_node == len(out_pipes)

    persistent_storage = False
    if storage_conf != 'null':
        persistent_storage = True
        from storage.api import initWorker as initStorageAtWorker
        from storage.api import finishWorker as finishStorageAtWorker

    # Load log level configuration file
    worker_path = os.path.dirname(os.path.realpath(__file__))
    if debug:
        # Debug
        init_logging_worker(worker_path + '/../../log/logging_debug.json')
    else:
        # Default
        init_logging_worker(worker_path + '/../../log/logging_off.json')

    if __debug__:
        logger = logging.getLogger('pycompss.worker.worker')
        logger.debug("[PYTHON WORKER] piper_worker.py wake up")
        logger.debug("[PYTHON WORKER] -----------------------------")
        logger.debug("[PYTHON WORKER] Persistent worker parameters:")
        logger.debug("[PYTHON WORKER] -----------------------------")
        logger.debug("[PYTHON WORKER] Debug          : " + str(debug))
        logger.debug("[PYTHON WORKER] Tracing        : " + str(TRACING))
        logger.debug("[PYTHON WORKER] Tasks per node : " + str(tasks_x_node))
        logger.debug("[PYTHON WORKER] In Pipes       : " + str(in_pipes))
        logger.debug("[PYTHON WORKER] Out Pipes      : " + str(out_pipes))
        logger.debug("[PYTHON WORKER] Storage conf.  : " + str(storage_conf))
        logger.debug("[PYTHON WORKER] -----------------------------")

    if persistent_storage:
        # Initialize storage
        initStorageAtWorker(config_file_path=storage_conf)

    # Create new threads
    queues = []
    for i in range(0, tasks_x_node):
        if __debug__:
            logger.debug("[PYTHON WORKER] Launching process " + str(i))
        process_name = 'Process-' + str(i)
        queues.append(Queue())

        def create_threads():
            PROCESSES.append(Process(target=worker, args=(queues[i],
                                                          process_name,
                                                          in_pipes[i],
                                                          out_pipes[i],
                                                          storage_conf,
                                                          logger)))
            PROCESSES[i].start()

        create_threads()

    # Catch SIGTERM send by bindings_piper to exit all PROCESSES
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Wait for all threads
    for i in range(0, tasks_x_node):
        PROCESSES[i].join()

    # Check if there is any exception message from the threads
    for i in range(0, tasks_x_node):
        if not queues[i].empty:
            logger.error("[PYTHON WORKER] Exception in threads queue: " + str(queues[i].get()))

    for q in queues:
        q.close()
        q.join_thread()

    if persistent_storage:
        # Finish storage
        finishStorageAtWorker()

    if __debug__:
        logger.debug("[PYTHON WORKER] Finished")

    if TRACING:
        pyextrae.eventandcounters(TASK_EVENTS, 0)
        pyextrae.eventandcounters(SYNC_EVENTS, 0)


############################
# Main -> Calls main method
############################

if __name__ == '__main__':
    compss_persistent_worker()
