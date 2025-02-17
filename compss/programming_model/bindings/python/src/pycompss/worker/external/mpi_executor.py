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
PyCOMPSs Worker - External - MPI Executor.

This file contains the code of an executor running Python MPI execution
command that is passed from the runtime worker.
"""

import copy
import logging
import os
import signal
import sys

from pycompss.util.context import CONTEXT
from mpi4py import MPI
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.logger.helpers import init_logging_worker
from pycompss.util.logger.remittent import LOG_REMITTENT
from pycompss.util.logger.level import LOG_LEVEL
from pycompss.util.tracing.helpers import EventWorker
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.util.typing_helper import typing
from pycompss.worker.commons.executor import build_return_params_message
from pycompss.worker.commons.worker import execute_task
from pycompss.worker.piper.commons.constants import TAGS


# TODO: Comments about exit value and return following values was in another
#       branch need to be reviewed if it works in trunk
# SUCCESS_SIG = 0
# FAILURE_SIG = 1
# UNEXPECTED_SIG = 2


def shutdown_handler(
    signal: int, frame: typing.Any
) -> None:  # pylint: disable=redefined-outer-name
    """Handle shutdown - MPI exception signal handler.

    CAUTION! Do not remove the parameters.

    :param signal: shutdown signal
    :param frame: Frame
    :return: None
    """
    raise PyCOMPSsException("Received SIGTERM")


######################
#  Processes body
######################


def executor(process_name: str, command: str) -> None:
    """Executor main method.

    Iterates over the input pipe in order to receive tasks (with their
    parameters) and process them.

    :param process_name: Process name (MPI Process-X, where X is the MPI rank).
    :param command: Command to execute.
    :return: None
    """
    # Replace Python Worker's SIGTERM handler.
    signal.signal(signal.SIGTERM, shutdown_handler)

    log_level = command.split()[7]
    tracing = command.split()[8] == "true"

    # Load log level configuration file
    if log_level in ("true", "debug"):
        # Debug
        init_logging_worker(LOG_REMITTENT.MPI_WORKER, LOG_LEVEL.DEBUG, tracing)
    elif log_level == "info":
        # Info
        init_logging_worker(LOG_REMITTENT.MPI_WORKER, LOG_LEVEL.INFO, tracing)
    else:
        # Default (off)
        init_logging_worker(LOG_REMITTENT.MPI_WORKER, LOG_LEVEL.OFF, tracing)

    logger = logging.getLogger("pycompss.worker.external.mpi_executor")
    logger_handlers = copy.copy(logger.handlers)
    logger_level = logger.getEffectiveLevel()
    try:
        lh0_formatter = logger_handlers[0].formatter  # type: typing.Any
        logger_formatter = logging.Formatter(
            lh0_formatter._fmt
        )  # type: typing.Any
    except IndexError:
        logger_formatter = None

    if __debug__:
        logger.debug(
            "[PYTHON EXECUTOR] [%s] Starting process", str(process_name)
        )

    sig, _ = process_task(
        command,
        process_name,
        logger,
        logger_handlers,
        logger_level,
        logger_formatter,
    )
    # Signal expected management:
    # if sig == FAILURE_SIG:
    #     raise Exception("Task execution failed!", msg)
    # elif sig == UNEXPECTED_SIG:
    #     raise Exception("Unexpected message!", msg)

    sys.stdout.flush()
    sys.stderr.flush()
    if __debug__:
        logger.debug(
            "[PYTHON EXECUTOR] [%s] Exiting process ", str(process_name)
        )
    if sig != 0:
        sys.exit(sig)


def process_task(
    current_line: str,
    process_name: str,
    logger: logging.Logger,
    logger_handlers: typing.Any,
    logger_level: int,
    logger_formatter: typing.Any,
) -> typing.Tuple[int, str]:
    """Process command received from the current_line.

    :param current_line: Current command (line) to process.
    :param process_name: Process name for logger messages.
    :param logger: Logger.
    :param logger_handlers: Logger handlers.
    :param logger_level: Logger level.
    :param logger_formatter: Logger formatter.
    :return: exit_value and message.
    """
    with EventWorker(TRACING_WORKER.process_task_event):
        # Process properties
        stdout = sys.stdout
        stderr = sys.stderr
        job_id = None
        current_working_dir = os.getcwd()

        if __debug__:
            logger.debug(
                "[PYTHON EXECUTOR] [%s] Received message: %s",
                str(process_name),
                str(current_line),
            )

        splitted_current_line = current_line.split()
        if splitted_current_line[0] == TAGS.execute_task:
            num_collection_params = int(splitted_current_line[-1])
            collections_layouts = (
                {}
            )  # type: typing.Dict[str, typing.Tuple[int, int, int]]
            if num_collection_params > 0:
                raw_layouts = splitted_current_line[
                    ((num_collection_params * -4) - 1) : -1  # noqa: E203
                ]
                for i in range(num_collection_params):
                    param = raw_layouts[i * 4]
                    layout = (
                        int(raw_layouts[(i * 4) + 1]),
                        int(raw_layouts[(i * 4) + 2]),
                        int(raw_layouts[(i * 4) + 3]),
                    )
                    collections_layouts[param] = layout

            # Remove the last elements: cpu and gpu bindings and
            # collection params
            current_line_filtered = splitted_current_line[0:-3]

            # task jobId command
            job_id = current_line_filtered[1]
            working_dir = current_line_filtered[2]
            job_out = current_line_filtered[3]
            job_err = current_line_filtered[4]
            # current_line_filtered[5] = <boolean> = tracing
            # current_line_filtered[6] = <integer> = task id
            # current_line_filtered[7] = <boolean> = debug
            # current_line_filtered[8] = <string>  = storage conf.
            # current_line_filtered[9] = <string>  = operation type
            #                                        (e.g. METHOD)
            # current_line_filtered[10] = <string> = module
            # current_line_filtered[11]= <string>  = method
            # current_line_filtered[12]= <string>  = time out
            # current_line_filteres[13]= <integer> = ppn
            # current_line_filtered[14]= <integer> = Number of slaves
            #                                        (worker nodes)==#nodes
            # <<list of slave nodes>>
            # current_line_filtered[14 + #nodes] = <integer> = computing units
            # current_line_filtered[15 + #nodes] = <boolean> = has target
            # current_line_filtered[16 + #nodes] = <string>  = has return
            #                                                  (always "null")
            # current_line_filtered[17 + #nodes] = <integer> = Number of
            #                                                  parameters
            # <<list of parameters>>
            #       !---> type, stream, prefix , value

            # Setting working directory
            os.chdir(working_dir)

            if __debug__:
                logger.debug(
                    "[PYTHON EXECUTOR] [%s] Received task with id: %s",
                    str(process_name),
                    str(job_id),
                )
                logger.debug(
                    "[PYTHON EXECUTOR] [%s] Setting working directory: %s",
                    str(process_name),
                    str(working_dir),
                )
                logger.debug(
                    "[PYTHON EXECUTOR] [%s] - TASK CMD: %s",
                    str(process_name),
                    str(current_line_filtered),
                )

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
                logger.debug("Received task in process: %s", str(process_name))
                logger.debug(" - TASK CMD: %s", str(current_line_filtered))

            try:
                # Setup out/err wrappers
                out = open(job_out, "a")  # pylint: disable=consider-using-with
                err = open(job_err, "a")  # pylint: disable=consider-using-with
                sys.stdout = out
                sys.stderr = err

                # Setup process environment
                compss_ppn = int(current_line_filtered[13])
                compss_nodes = int(current_line_filtered[14])
                compss_nodes_names = ",".join(
                    current_line_filtered[15 : 15 + compss_nodes]  # noqa: E203
                )
                computing_units = int(current_line_filtered[15 + compss_nodes])
                os.environ["COMPSS_NUM_NODES"] = str(compss_nodes)
                os.environ["COMPSS_NUM_PROCS"] = str(compss_ppn * compss_nodes)
                os.environ["COMPSS_HOSTNAMES"] = compss_nodes_names
                os.environ["COMPSS_NUM_THREADS"] = str(computing_units)
                os.environ["OMP_NUM_THREADS"] = str(computing_units)
                if __debug__:
                    logger.debug("Process environment:")
                    logger.debug(
                        "\t - Number of nodes: %s", (str(compss_nodes))
                    )
                    logger.debug("\t - Hostnames: %s", str(compss_nodes_names))
                    logger.debug(
                        "\t - Number of threads: %s", (str(computing_units))
                    )

                # Execute task
                storage_conf = "null"
                tracing = False
                python_mpi = True
                result = execute_task(
                    process_name,
                    storage_conf,
                    current_line_filtered[10:],
                    tracing,
                    logger,
                    (job_out, job_err),
                    python_mpi,
                    collections_layouts,
                    None,
                    None,
                )
                exit_value, new_types, new_values, _, except_msg = result

                # Restore out/err wrappers
                sys.stdout = stdout
                sys.stderr = stderr
                sys.stdout.flush()
                sys.stderr.flush()
                out.close()
                err.close()

                # To reduce if necessary:
                # global_exit_value = MPI.COMM_WORLD.reduce(exit_value,
                #                                           op=MPI.SUM,
                #                                           root=0)
                # message = ""

                # if MPI.COMM_WORLD.rank == 0 and global_exit_value == 0:
                if exit_value == 0:
                    # Task has finished without exceptions
                    # endTask jobId exitValue message
                    params = build_return_params_message(new_types, new_values)
                    message = " ".join(
                        (
                            TAGS.end_task,
                            str(job_id),
                            str(exit_value),
                            str(params) + "\n",
                        )
                    )
                elif exit_value == 2:
                    # Task has finished with a COMPSs Exception
                    # compssExceptionTask jobId exitValue message
                    except_msg = except_msg.replace(" ", "_")
                    message = " ".join(
                        (
                            TAGS.compss_exception,
                            str(job_id),
                            str(except_msg) + "\n",
                        )
                    )
                    if __debug__:
                        logger.debug(
                            "%s - COMPSS EXCEPTION TASK MESSAGE: %s",
                            str(process_name),
                            str(except_msg),
                        )
                else:
                    # elif MPI.COMM_WORLD.rank == 0 and global_exit_value != 0:
                    # An exception has been raised in task
                    message = " ".join(
                        (TAGS.end_task, str(job_id), str(exit_value) + "\n")
                    )

                if __debug__:
                    logger.debug(
                        "%s - END TASK MESSAGE: %s",
                        str(process_name),
                        str(message),
                    )
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
                #         - "null" if it is NOT a PSCO
                #         - PSCOId (String) if is a PSCO
                # Example:
                #     4 null 9 null 12 <pscoid>
                #
                # The order of the elements is: parameters + self + returns
                #
                # This is sent through the pipe with the END_TASK message.
                # If the task had an object or file as parameter and the worker
                # returns the id, the runtime can change the type (and
                # locations) to a EXTERNAL_OBJ_T.

            except (
                Exception
            ) as general_exception:  # pylint: disable=broad-except
                logger.exception(
                    "%s - Exception %s",
                    str(process_name),
                    str(general_exception),
                )
                exit_value = 7
                message = " ".join(
                    (TAGS.end_task, str(job_id), str(exit_value) + "\n")
                )

            # Clean environment variables
            if __debug__:
                logger.debug("Cleaning environment.")

            del os.environ["COMPSS_HOSTNAMES"]

            # Restore loggers
            if __debug__:
                logger.debug("Restoring loggers.")
            logger.removeHandler(out_file_handler)
            logger.removeHandler(err_file_handler)
            for handler in logger_handlers:
                logger.addHandler(handler)

            if __debug__:
                logger.debug(
                    "[PYTHON EXECUTOR] [%s] Finished task with id: %s",
                    str(process_name),
                    str(job_id),
                )
            # return SUCCESS_SIG,
            #        f"{str(process_name)} -- Task Ended Successfully!"

        else:
            if __debug__:
                logger.debug(
                    "[PYTHON EXECUTOR] [%s] Unexpected message: %s",
                    str(process_name),
                    str(current_line),
                )
            exit_value = 7
            message = " ".join(
                (TAGS.end_task, str(job_id), str(exit_value) + "\n")
            )

        # Go back to initial current working directory
        os.chdir(current_working_dir)

        return exit_value, message


def main() -> None:
    """MPI executor main method.

    :returns: None.
    """
    # Set the binding in worker mode
    CONTEXT.set_worker()

    executor(f"MPI Process-{MPI.COMM_WORLD.rank}", sys.argv[1])


if __name__ == "__main__":
    main()
