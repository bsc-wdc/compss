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
PyCOMPSs PYTHON MPI Executor
===========================
    This file contains the code of an executor running Python MPI execution
    command that is passed from the runtime worker.
"""

import copy
import signal
import logging
import os
import sys
from mpi4py import MPI

import pycompss.util.context as context
from pycompss.util.logger.helpers import init_logging_worker
from pycompss.util.tracing.helpers import emit_event
from pycompss.worker.commons.constants import PROCESS_TASK_EVENT
from pycompss.worker.piper.commons.constants import EXECUTE_TASK_TAG
from pycompss.worker.piper.commons.constants import END_TASK_TAG
from pycompss.worker.commons.executor import build_return_params_message
from pycompss.worker.commons.worker import execute_task

# TODO: Comments about exit value and return following values was
# in another branch need to be reviewed if it works in trunk
# SUCCESS_SIG = 0
# FAILURE_SIG = 1
# UNEXPECTED_SIG = 2


def shutdown_handler(signal, frame):  # noqa
    """ MPI exception signal handler

    Do not remove the parameters.

    :param signal: shutdown signal
    :param frame: Frame
    :return: None
    """
    raise Exception("Received SIGTERM")


######################
#  Processes body
######################

def executor(process_name, command):
    # type: (str, str) -> None
    """ Execution main method.

    Iterates over the input pipe in order to receive tasks (with their
    parameters) and process them.

    :param process_name: Process name (MPI Process-X, where X is the MPI rank).
    :param command: Command to execute.
    :return: None
    """
    # Replace Python Worker's SIGTERM handler.
    signal.signal(signal.SIGTERM, shutdown_handler)

    log_level = command.split()[6]
    tracing = command.split()[7] == "true"

    # Load log level configuration file
    worker_path = os.path.dirname(os.path.realpath(__file__))
    if log_level == 'true' or log_level == "debug":
        # Debug
        log_json = "".join((worker_path,
                            "/../../../log/logging_mpi_worker_debug.json"))
    elif log_level == "info" or log_level == "off":
        log_json = "".join((worker_path,
                            "/../../../log/logging_mpi_worker_off.json"))
    else:
        # Default
        log_json = "".join((worker_path,
                            "/../../../log/logging_mpi_worker.json"))
    init_logging_worker(log_json, tracing)

    logger = logging.getLogger("pycompss.worker.external.mpi_worker")
    logger_handlers = copy.copy(logger.handlers)
    logger_level = logger.getEffectiveLevel()
    try:
        logger_formatter = logging.Formatter(logger_handlers[0].formatter._fmt)  # noqa
    except IndexError:
        logger_formatter = None

    if __debug__:
        logger.debug("[PYTHON EXECUTOR] [%s] Starting process" %
                     str(process_name))

    sig, msg = process_task(command,
                            process_name,
                            logger,
                            logger_handlers,
                            logger_level,
                            logger_formatter)
    # if sig == FAILURE_SIG:
    #     raise Exception("Task execution failed!", msg)
    # elif sig == UNEXPECTED_SIG:
    #     raise Exception("Unexpected message!", msg)

    sys.stdout.flush()
    sys.stderr.flush()
    if __debug__:
        logger.debug("[PYTHON EXECUTOR] [%s] Exiting process " %
                     str(process_name))
    if sig != 0:
        sys.exit(sig)


@emit_event(PROCESS_TASK_EVENT)
def process_task(current_line,     # type: str
                 process_name,     # type: str
                 logger,           # type: ...
                 logger_handlers,  # type: ...
                 logger_level,     # type: int
                 logger_formatter  # type: ...
                 ):
    # type: (...) -> (str, str)
    """ Process command received from the current_line.

    :param current_line: Current command (line) to process.
    :param process_name: Process name for logger messages.
    :param logger: Logger.
    :param logger_handlers: Logger handlers.
    :param logger_level: Logger level.
    :param logger_formatter: Logger formatter.
    :return: exit_value and message.
    """
    # Process properties
    stdout = sys.stdout
    stderr = sys.stderr
    job_id = None

    if __debug__:
        logger.debug("[PYTHON EXECUTOR] [%s] Received message: %s"
                     % (str(process_name), str(current_line)))

    current_line = current_line.split()
    if current_line[0] == EXECUTE_TASK_TAG:
        num_collection_params = int(current_line[-1])
        if num_collection_params > 0:
            collections_layouts = dict()
            raw_layouts = current_line[((num_collection_params * -4) - 1):-1]
            for i in range(num_collection_params):
                param = raw_layouts[i * 4]
                layout = [int(raw_layouts[(i * 4) + 1]),
                          int(raw_layouts[(i * 4) + 2]),
                          int(raw_layouts[(i * 4) + 3])]
                collections_layouts[param] = layout
        else:
            collections_layouts = None

        # Remove the last elements: cpu and gpu bindings and collection params
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
        # current_line[11]= <string>  = time out
        # current_line[12]= <integer> = Number of slaves (worker nodes)==#nodes
        # <<list of slave nodes>>
        # current_line[12 + #nodes] = <integer> = computing units
        # current_line[13 + #nodes] = <boolean> = has target
        # current_line[14 + #nodes] = <string>  = has return (always 'null')
        # current_line[15 + #nodes] = <integer> = Number of parameters
        # <<list of parameters>>
        #       !---> type, stream, prefix , value

        if __debug__:
            logger.debug("[PYTHON EXECUTOR] [%s] Received task with id: %s" %
                         (str(process_name), str(job_id)))
            logger.debug("[PYTHON EXECUTOR] [%s] - TASK CMD: %s" %
                         (str(process_name), str(current_line)))

        # Swap logger from stream handler to file handler
        # All task output will be redirected to job.out/err
        for log_handler in logger_handlers:
            logger.removeHandler(log_handler)

        out_file_handler = logging.FileHandler(job_out)
        out_file_handler.setLevel(logger_level)
        out_file_handler.setFormatter(logger_formatter)
        err_file_handler = logging.FileHandler(job_err)
        err_file_handler.setLevel("ERROR")
        err_file_handler.setFormatter(logger_formatter)
        logger.addHandler(out_file_handler)
        logger.addHandler(err_file_handler)

        if __debug__:
            logger.debug("Received task in process: %s" %
                         str(process_name))
            logger.debug(" - TASK CMD: %s" %
                         str(current_line))

        try:
            # Setup out/err wrappers
            out = open(job_out, 'a')
            err = open(job_err, 'a')
            sys.stdout = out
            sys.stderr = err

            # Setup process environment
            cn = int(current_line[11])
            cn_names = ','.join(current_line[12:12 + cn])
            os.environ["COMPSS_NUM_NODES"] = str(cn)
            os.environ["COMPSS_HOSTNAMES"] = cn_names
            if __debug__:
                logger.debug("Process environment:")
                logger.debug("\t - Number of nodes: %s" % (str(cn)))
                logger.debug("\t - Hostnames: %s" % str(cn_names))

            # Execute task
            storage_conf = "null"
            tracing = False
            python_mpi = True
            result = execute_task(process_name,
                                  storage_conf,
                                  current_line[9:],
                                  tracing,
                                  logger,
                                  (job_out, job_err),
                                  python_mpi,
                                  collections_layouts)
            exit_value, new_types, new_values, time_out, except_msg = result

            # Restore out/err wrappers
            sys.stdout = stdout
            sys.stderr = stderr
            sys.stdout.flush()
            sys.stderr.flush()
            out.close()
            err.close()

            # global_exit_value = MPI.COMM_WORLD.reduce(exit_value, op=MPI.SUM, root=0)
            # message = ""

            # if MPI.COMM_WORLD.rank == 0 and global_exit_value == 0:
            if exit_value == 0:
                # Task has finished without exceptions
                # endTask jobId exitValue message
                params = build_return_params_message(new_types, new_values)
                message = " ".join((END_TASK_TAG,
                                    str(job_id),
                                    str(exit_value),
                                    str(params) + "\n"))
            elif exit_value == 2:
                # Task has finished with a COMPSs Exception
                # compssExceptionTask jobId exitValue message
                except_msg = except_msg.replace(" ", "_")
                message = " ".join((COMPSS_EXCEPTION_TAG,
                                    str(job_id),
                                    str(except_msg) + "\n"))
                if __debug__:
                    logger.debug("%s - COMPSS EXCEPTION TASK MESSAGE: %s" %
                                 (str(process_name), str(except_msg)))
            else:
                # elif MPI.COMM_WORLD.rank == 0 and global_exit_value != 0:
                # An exception has been raised in task
                message = " ".join((END_TASK_TAG,
                                    str(job_id),
                                    str(exit_value) + "\n"))
                # return FAILURE_SIG, except_msg

            if __debug__:
                logger.debug("%s - END TASK MESSAGE: %s" % (str(process_name),
                                                            str(message)))
            # The return message is:
            #
            # TaskResult ==> jobId exitValue D List<Object>
            #
            # Where List<Object> has D * 2 length:
            # D = #parameters == #task_parameters +
            #                    (has_target ? 1 : 0) +
            #                    #returns
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
            # If the task had an object or file as parameter and the worker
            # returns the id, the runtime can change the type (and locations)
            # to a EXTERNAL_OBJ_T.

        except Exception as e:
            logger.exception("%s - Exception %s" % (str(process_name),
                                                    str(e)))
            exit_value = 7
            message = " ".join((END_TASK_TAG,
                                str(job_id),
                                str(exit_value) + "\n"))
            # return FAILURE_SIG, e

        # Clean environment variables
        if __debug__:
            logger.debug("Cleaning environment.")

        del os.environ['COMPSS_HOSTNAMES']

        # Restore loggers
        if __debug__:
            logger.debug("Restoring loggers.")
        logger.removeHandler(out_file_handler)
        logger.removeHandler(err_file_handler)
        for handler in logger_handlers:
            logger.addHandler(handler)

        if __debug__:
            logger.debug("[PYTHON EXECUTOR] [%s] Finished task with id: %s" %
                         (str(process_name), str(job_id)))
        # return SUCCESS_SIG, "{0} -- Task Ended Successfully!".format(str(process_name))

    else:
        if __debug__:
            logger.debug("[PYTHON EXECUTOR] [%s] Unexpected message: %s" %
                         (str(process_name), str(current_line)))
        exit_value = 7
        message = " ".join((END_TASK_TAG,
                            str(job_id),
                            str(exit_value) + "\n"))

    return exit_value, message
    #    return UNEXPECTED_SIG, "Unexpected message: %s" % str(current_line)


def main():
    # Set the binding in worker mode
    context.set_pycompss_context(context.WORKER)

    executor("MPI Process-{0}".format(MPI.COMM_WORLD.rank), sys.argv[1])


if __name__ == '__main__':
    main()
