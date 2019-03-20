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
    This file contains the code of an executor running the commands that it reads from a pipe.
"""
import copy
import signal
import logging
import os
import sys
import time
import thread_affinity

from pycompss.worker.pipe_constants import EXECUTE_TASK_TAG
from pycompss.worker.pipe_constants import END_TASK_TAG
from pycompss.worker.pipe_constants import PING_TAG
from pycompss.worker.pipe_constants import PONG_TAG
from pycompss.worker.pipe_constants import QUIT_TAG

def shutdown_handler(signal, frame):
    """
    Shutdown handler (do not remove the parameters).

    :param signal: shutdown signal
    :param frame: Frame
    :return: None
    """


class Pipe(object):
    """
    Bi-directional communication channel
    """

    def __init__(self, input_pipe, output_pipe):
        """
        Constructs a new Pipe

        :param input_pipe: Input pipe for the thread. To receive messages from the runtime.
        :param output_pipe: Output pipe for the thread. To send messages to the runtime.
        """
        self.input_pipe = input_pipe
        self.input_pipe_open = None
        self.output_pipe = output_pipe

    def read_command(self, retry_period=0):
        """
        Returns the first command on the pipe

        :param retry_period: time (ms) that the thread sleeps if EOF is read from pipe

        :return: the first command available on the pipe
        """
        if self.input_pipe_open is None:
            self.input_pipe_open = open(self.input_pipe, 'r')

        line = self.input_pipe_open.readline()
        if line == "":
            time.sleep(0.001*retry_period)
            line = self.input_pipe_open.readline()

        return line

    def write(self, message):
        """
        Writes a message through the pipe

        :param message: message sent through the pipe
        """
        with open(self.output_pipe, 'w') as out_pipe:
            out_pipe.write(message+"\n")

    def close(self):
        """
        Closes the pipe, if open
        """
        if self.input_pipe_open:
            self.input_pipe_open.close()
            self.input_pipe_open = None

    def __str__(self):
        return "PIPE IN "+self.input_pipe+" OUT "+self.output_pipe

class ExecutorConf(object):
    """
    Executor configuration
    """

    def __init__(self, tracing, storage_conf, logger, storage_loggers):
        """
        Constructs a new executor configuration

        :param tracing: Enable tracing for the executor
        :param storage_conf: Storage configuration file
        :param logger: Main logger
        :param storage_loggers: List of supported storage loggers - empty if running w/o storage
        """
        self.tracing = tracing
        self.storage_conf = storage_conf
        self.logger = logger
        self.storage_loggers = storage_loggers


######################
#  Processes body
######################
def executor(queue, process_name, pipe, conf):
    """
    Thread main body - Overrides Threading run method.
    Iterates over the input pipe in order to receive tasks (with their
    parameters) and process them.
    Notifies the runtime when each task  has finished with the
    corresponding output value.
    Finishes when the "quit" message is received.

    :param queue: Queue where to put exception messages
    :param process_name: Process name (Thread-X, where X is the thread id).
    :param pipe: Pipe to receive and send messages from/to the runtime.
    :param conf: configuration of the executor
    :return: None
    """
    # Replace Python Worker's SIGTERM handler.
    signal.signal(signal.SIGTERM, shutdown_handler)

    
    tracing = conf.tracing
    storage_conf = conf.storage_conf
    logger = conf.logger
    storage_loggers = conf.storage_loggers

    # Get a copy of the necessary information from the logger to re-establish after each task
    logger_handlers = copy.copy(logger.handlers)
    logger_level = logger.getEffectiveLevel()
    logger_formatter = logging.Formatter(logger_handlers[0].formatter._fmt)
    storage_loggers_handlers = []
    for storage_logger in storage_loggers:
        storage_loggers_handlers.append(copy.copy(storage_logger.handlers))

    if storage_conf != 'null':
        try:
            from storage.api import initWorkerPostFork as initStorageAtWorkerPostFork
            initStorageAtWorkerPostFork()
        except ImportError:
            if __debug__:
                logger.info("[PYTHON EXECUTOR] [%s] Could not find initWorkerPostFork storage call. Ignoring it." % str(process_name))

    alive = True
    stdout = sys.stdout
    stderr = sys.stderr

    if __debug__:
        logger.debug("[PYTHON EXECUTOR] [%s] Starting process" % str(process_name))

    while alive:
        affinity_ok = True

        def process_task(current_line):
            if __debug__:
                logger.debug("[PYTHON EXECUTOR] [%s] Received message: %s"
                             % (str(process_name), str(current_line)))
            current_line = current_line.split()
            if current_line[0] == EXECUTE_TASK_TAG:
                # CPU binding
                binded_cpus = current_line[-3]

                def bind_cpus(binded_cpus):
                    if binded_cpus != "-":
                        os.environ['COMPSS_BINDED_CPUS'] = binded_cpus
                        if __debug__:
                            logger.debug("[PYTHON EXECUTOR] [%s] Assigning affinity %s"
                                         % (str(process_name), str(binded_cpus)))
                        binded_cpus = list(map(int, binded_cpus.split(",")))
                        try:
                            thread_affinity.setaffinity(binded_cpus)
                        except Exception:
                            if __debug__:
                                logger.error("[PYTHON EXECUTOR] [%s] Warning: could not assign affinity %s" % (str(process_name), str(binded_cpus)))
                            affinity_ok = False

                bind_cpus(binded_cpus)

                # GPU binding
                binded_gpus = current_line[-2]

                def bind_gpus(current_binded_gpus):
                    if current_binded_gpus != "-":
                        os.environ['COMPSS_BINDED_GPUS'] = current_binded_gpus
                        os.environ['CUDA_VISIBLE_DEVICES'] = current_binded_gpus
                        os.environ['GPU_DEVICE_ORDINAL'] = current_binded_gpus
                        if __debug__:
                            logger.debug("[PYTHON EXECUTOR] [%s] Assigning GPU %s" % (str(process_name), str(current_binded_gpus)))

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
                    logger.debug("[PYTHON EXECUTOR] [%s] Received task with id: %s" % (str(process_name), str(job_id)))
                    logger.debug("[PYTHON EXECUTOR] [%s] - TASK CMD: %s" % (str(process_name), str(current_line)))

                # Swap logger from stream handler to file handler - All task output will be redirected to job.out/err
                for log_handler in logger_handlers:
                    logger.removeHandler(log_handler)
                for storage_logger in storage_loggers:
                    for log_handler in storage_logger.handlers:
                        storage_logger.removeHandler(log_handler)
                out_file_handler = logging.FileHandler(job_out)
                out_file_handler.setLevel(logger_level)
                out_file_handler.setFormatter(logger_formatter)
                err_file_handler = logging.FileHandler(job_err)
                err_file_handler.setLevel("ERROR")
                err_file_handler.setFormatter(logger_formatter)
                logger.addHandler(out_file_handler)
                logger.addHandler(err_file_handler)
                for storage_logger in storage_loggers:
                    storage_logger.addHandler(out_file_handler)
                    storage_logger.addHandler(err_file_handler)

                if __debug__:
                    logger.debug("Received task in process: %s" % str(process_name))
                    logger.debug(" - TASK CMD: %s" % str(current_line))

                try:
                    # Setup out/err wrappers
                    out = open(job_out, 'a')
                    err = open(job_err, 'a')
                    sys.stdout = out
                    sys.stderr = err

                    # Check thread affinity
                    if not affinity_ok:
                        err.write("WARNING: This task is going to be executed with default thread affinity %s"
                                  % thread_affinity.getaffinity())

                    # Setup process environment
                    cn = int(current_line[11])
                    cn_names = ','.join(current_line[12:12 + cn])
                    cu = current_line[12 + cn]
                    os.environ["COMPSS_NUM_NODES"] = str(cn)
                    os.environ["COMPSS_HOSTNAMES"] = cn_names
                    os.environ["COMPSS_NUM_THREADS"] = cu
                    os.environ["OMP_NUM_THREADS"] = cu
                    if __debug__:
                        logger.debug("Process environment:")
                        logger.debug("\t - Number of nodes: %s" % (str(cn)))
                        logger.debug("\t - Hostnames: %s" % str(cn_names))
                        logger.debug("\t - Number of threads: %s" % (str(cu)))

                    # Execute task
                    from pycompss.worker.worker_commons import execute_task
                    exit_value, new_types, new_values = execute_task(process_name,
                                                                     storage_conf,
                                                                     current_line[9:],
                                                                     tracing,
                                                                     logger)

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
                        params = _build_return_params_message(new_types, new_values)
                        message = END_TASK_TAG + " " + str(job_id) \
                                               + " " + str(exit_value) \
                                               + " " + str(params) + "\n"
                    else:
                        # An exception has been raised in task
                        message = END_TASK_TAG + " " + str(job_id) + " " + str(exit_value) + "\n"

                    if __debug__:
                        logger.debug("%s - Pipe %s END TASK MESSAGE: %s" % (str(process_name),
                                                                            str(pipe.output_pipe),
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
                    pipe.write(message)

                except Exception as e:
                    logger.exception("%s - Exception %s" % (str(process_name), str(e)))
                    if queue:
                        queue.put("EXCEPTION")

                # Clean environment variables
                if __debug__:
                    logger.debug("Cleaning environment.")
                if binded_cpus != "-":
                    del os.environ['COMPSS_BINDED_CPUS']
                if binded_gpus != "-":
                    del os.environ['COMPSS_BINDED_GPUS']
                    del os.environ['CUDA_VISIBLE_DEVICES']
                    del os.environ['GPU_DEVICE_ORDINAL']
                del os.environ['COMPSS_HOSTNAMES']

                # Restore loggers
                if __debug__:
                    logger.debug("Restoring loggers.")
                logger.removeHandler(out_file_handler)
                logger.removeHandler(err_file_handler)
                for handler in logger_handlers:
                    logger.addHandler(handler)
                i = 0
                for storage_logger in storage_loggers:
                    storage_logger.removeHandler(out_file_handler)
                    storage_logger.removeHandler(err_file_handler)
                    for handler in storage_loggers_handlers[i]:
                        storage_logger.addHandler(handler)
                    i += 1
                if __debug__:
                    logger.debug("[PYTHON EXECUTOR] [%s] Finished task with id: %s"
                                 % (str(process_name), str(job_id)))

            elif current_line[0] == PING_TAG:
                pipe.write(PONG_TAG)

            elif current_line[0] == QUIT_TAG:
                # Received quit message -> Suicide
                if __debug__:
                    logger.debug("[PYTHON EXECUTOR] [%s] Received quit." % str(process_name))
                return False
            return True

        command = pipe.read_command(retry_period=0.5)
        if command != "":
            logger.debug("[PYTHON EXECUTOR] Received %s" % command)
            alive = process_task(command)

    if storage_conf != 'null':
        try:
            from storage.api import finishWorkerPostFork as finishStorageAtWorkerPostFork
            finishStorageAtWorkerPostFork()
        except ImportError:
            if __debug__:
                logger.info("[PYTHON EXECUTOR] [%s] Could not find finishWorkerPostFork storage call. Ignoring it." % (str(process_name)))

    sys.stdout.flush()
    sys.stderr.flush()
    if __debug__:
        logger.debug("[PYTHON EXECUTOR] [%s] Exiting process " % str(process_name))

    pipe.write(QUIT_TAG)
    pipe.close()

def _build_return_params_message(types, values):
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
    for pair in pairs:
        params = params + str(pair[0]) + ' ' + str(pair[1]) + ' '
    message = str(num_params) + ' ' + params
    return message

