#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Persistent Worker Pipe Executor
========================================
    This file contains the code of an executor running the commands that it
    reads from a pipe.
"""

import copy
import logging
import os
import signal
import sys
import time
from multiprocessing import Queue

from pycompss.util.typing_helper import typing

try:
    THREAD_AFFINITY = True
    import thread_affinity  # noqa
except ImportError:
    from pycompss.worker.piper.commons.constants import HEADER as MAIN_HEADER

    print(
        "".join(
            (
                MAIN_HEADER,
                "WARNING: Could not import process affinity library: ",
                "CPU AFFINITY NOT SUPPORTED!",
            )
        )
    )
    THREAD_AFFINITY = False

import pycompss.runtime.management.COMPSs as COMPSs
import pycompss.util.context as context
from pycompss.runtime.commons import GLOBALS
from pycompss.worker.piper.commons.constants import EXECUTE_TASK_TAG
from pycompss.worker.piper.commons.constants import END_TASK_TAG
from pycompss.worker.piper.commons.constants import COMPSS_EXCEPTION_TAG
from pycompss.worker.piper.commons.constants import PING_TAG
from pycompss.worker.piper.commons.constants import PONG_TAG
from pycompss.worker.piper.commons.constants import QUIT_TAG
from pycompss.worker.piper.commons.utils_logger import load_loggers
from pycompss.worker.commons.executor import build_return_params_message
from pycompss.worker.commons.worker import execute_task
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.tracing.helpers import emit_manual_event
from pycompss.util.tracing.helpers import event_worker
from pycompss.util.tracing.helpers import event_inside_worker
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.worker.piper.cache.tracker import load_shared_memory_manager

# Streaming imports
from pycompss.streams.components.distro_stream_client import (
    DistroStreamClientHandler,
)

HEADER = "*[PYTHON EXECUTOR] "


def shutdown_handler(signal: int, frame: typing.Any) -> None:
    """Shutdown handler

    Do not remove the parameters.

    :param signal: shutdown signal.
    :param frame: Frame.
    :return: None
    :raises PyCOMPSsException: Received signal.
    """
    raise PyCOMPSsException("Received SIGTERM")


class Pipe(object):
    """
    Bi-directional communication channel
    """

    __slots__ = ["input_pipe", "input_pipe_open", "output_pipe"]

    def __init__(self, input_pipe: str, output_pipe: str) -> None:
        """Constructs a new Pipe.

        :param input_pipe: Input pipe for the thread. To receive messages from
                           the runtime.
        :param output_pipe: Output pipe for the thread. To send messages to
                            the runtime.
        """
        self.input_pipe = input_pipe
        self.input_pipe_open = None  # type: typing.Any
        self.output_pipe = output_pipe

    def read_command(self, retry_period: float = 0.5) -> str:
        """Returns the first command on the pipe.

        :param retry_period: time (ms) that the thread sleeps if EOF is read
                             from pipe.
        :return: the first command available on the pipe.
        """
        if self.input_pipe_open is None:
            self.input_pipe_open = open(self.input_pipe, "r")
            # Non blocking open:
            # fd = os.open(self.input_pipe, os.O_RDWR)
            # self.input_pipe_open = os.fdopen(fd, "r")

        line = self.input_pipe_open.readline()
        if line == "":
            time.sleep(0.001 * retry_period)
            line = self.input_pipe_open.readline()

        return line

    def write(self, message: str) -> None:
        """Writes a message through the pipe.

        :param message: message sent through the pipe
        :return: None
        """
        with open(self.output_pipe, "w") as out_pipe:
            out_pipe.write("".join((message, "\n")))

    def close(self) -> None:
        """Closes the pipe, if open.

        :return: None
        """
        if self.input_pipe_open:
            self.input_pipe_open.close()
            self.input_pipe_open = None

    def __str__(self) -> str:
        """Representation of the Pipe.

        :return: String representing the Pipe object.
        """
        return " ".join(("PIPE IN", self.input_pipe, "PIPE OUT", self.output_pipe))


class ExecutorConf(object):
    """
    Executor configuration
    """

    __slots__ = [
        "debug",
        "tmp_dir",
        "tracing",
        "storage_conf",
        "logger",
        "logger_cfg",
        "persistent_storage",
        "storage_loggers",
        "stream_backend",
        "stream_master_ip",
        "stream_master_port",
        "cache_ids",
        "cache_queue",
        "cache_profiler",
    ]

    def __init__(
        self,
        debug: bool,
        tmp_dir: str,
        tracing: bool,
        storage_conf: str,
        logger: typing.Any,
        logger_cfg: str,
        persistent_storage: bool,
        storage_loggers: typing.Any,
        stream_backend: str,
        stream_master_ip: str,
        stream_master_port: str,
        cache_ids: typing.Any = None,
        cache_queue: Queue = None,
        cache_profiler: bool = False,
    ) -> None:
        """
        Constructs a new executor configuration.

        :param debug: If debug is enabled.
        :param tmp_dir: Temporary directory for logging purposes.
        :param tracing: Enable tracing for the executor.
        :param storage_conf: Storage configuration file.
        :param logger: Main logger.
        :param logger_cfg: Logger configuration file.
        :param persistent_storage: If persistent storage is enabled
        :param storage_loggers: List of supported storage loggers
                                (empty if running w/o storage).
        :param stream_backend: Streaming backend type.
        :param stream_master_ip: Streaming master IP.
        :param stream_master_port: Streaming master port.
        :param cache_ids: Proxy cache dictionary.
        :param cache_queue: Cache queue where to submit to add new entries to
                            cache_ids.
        """
        self.debug = debug
        self.tmp_dir = tmp_dir
        self.tracing = tracing
        self.storage_conf = storage_conf
        self.logger = logger
        self.logger_cfg = logger_cfg
        self.persistent_storage = persistent_storage
        self.storage_loggers = storage_loggers
        self.stream_backend = stream_backend
        self.stream_master_ip = stream_master_ip
        self.stream_master_port = stream_master_port
        self.cache_ids = cache_ids  # Read-only
        self.cache_queue = cache_queue
        self.cache_profiler = cache_profiler


######################
#  Processes body
######################


def executor(
    queue: typing.Union[None, Queue], process_name: str, pipe: Pipe, conf: typing.Any
) -> None:
    """Thread main body - Overrides Threading run method.

    Iterates over the input pipe in order to receive tasks (with their
    parameters) and process them.
    Notifies the runtime when each task  has finished with the
    corresponding output value.
    Finishes when the "quit" message is received.

    :param queue: Queue where to put exception messages.
    :param process_name: Process name (Thread-X, where X is the thread id).
    :param pipe: Pipe to receive and send messages from/to the runtime.
    :param conf: configuration of the executor.
    :return: None
    """
    try:
        # Replace Python Worker's SIGTERM handler.
        signal.signal(signal.SIGTERM, shutdown_handler)

        if len(conf.logger.handlers) == 0:
            # Logger has not been inherited correctly. Happens in MacOS.
            GLOBALS.set_temporary_directory(conf.tmp_dir, create_tmpdir=False)
            # Reload logger
            (
                conf.logger,
                conf.logger_cfg,
                conf.storage_loggers,
                _,
            ) = load_loggers(  # noqa: E501
                conf.debug, conf.persistent_storage
            )
            # Set the binding in worker mode too
            context.set_pycompss_context(context.WORKER)
        logger = conf.logger

        tracing = conf.tracing
        storage_conf = conf.storage_conf
        storage_loggers = conf.storage_loggers

        # Get a copy of the necessary information from the logger to
        # re-establish after each task
        logger_handlers = copy.copy(logger.handlers)
        logger_level = logger.getEffectiveLevel()
        logger_formatter = logging.Formatter(logger_handlers[0].formatter._fmt)  # noqa
        storage_loggers_handlers = []
        for storage_logger in storage_loggers:
            storage_loggers_handlers.append(copy.copy(storage_logger.handlers))

        # Establish link with the binding-commons to enable task nesting
        if __debug__:
            logger.debug(
                HEADER
                + "Establishing link with runtime in process "
                + str(process_name)
            )
        COMPSs.load_runtime(external_process=False, _logger=logger)
        COMPSs.set_pipes(pipe.output_pipe, pipe.input_pipe)

        if storage_conf != "null":
            try:
                from storage.api import initWorkerPostFork  # noqa

                with event_worker(TRACING_WORKER.init_worker_postfork_event):
                    initWorkerPostFork()
            except (ImportError, AttributeError):
                if __debug__:
                    logger.info(
                        HEADER
                        + "[%s] Could not find initWorkerPostFork storage call. Ignoring it."  # noqa: E501
                        % str(process_name)
                    )

        # Start the streaming backend if necessary
        streaming = False
        if conf.stream_backend not in [None, "null", "NONE"]:
            streaming = True

        if streaming:
            # Initialize streaming
            logger.debug(
                HEADER + "Starting streaming for process " + str(process_name)
            )  # noqa: E501
            try:
                DistroStreamClientHandler.init_and_start(
                    master_ip=conf.stream_master_ip,
                    master_port=conf.stream_master_port,  # noqa: E501
                )
            except Exception as e:
                logger.error(e)
                raise e

        # Connect to Shared memory manager
        if conf.cache_queue:
            load_shared_memory_manager()

        # Process properties
        alive = True

        if __debug__:
            logger.debug(HEADER + "[%s] Starting process" % str(process_name))

        # MAIN EXECUTOR LOOP
        while alive:
            # Runtime -> pipe - Read command from pipe
            command = COMPSs.read_pipes()
            if command != "":
                if __debug__:
                    logger.debug(
                        HEADER
                        + "[%s] Received command %s"
                        % (str(process_name), str(command))  # noqa: E501
                    )
                # Process the command
                alive = process_message(
                    command,
                    process_name,
                    pipe,
                    queue,
                    tracing,
                    logger,
                    conf.logger_cfg,
                    logger_handlers,
                    logger_level,
                    logger_formatter,
                    storage_conf,
                    storage_loggers,
                    storage_loggers_handlers,
                    conf.cache_queue,
                    conf.cache_ids,
                    conf.cache_profiler,
                )
        # Stop storage
        if storage_conf != "null":
            try:
                from storage.api import finishWorkerPostFork  # noqa

                with event_worker(TRACING_WORKER.finish_worker_postfork_event):
                    finishWorkerPostFork()
            except (ImportError, AttributeError):
                if __debug__:
                    logger.info(
                        HEADER
                        + "[%s] Could not find finishWorkerPostFork storage call. Ignoring it."  # noqa: E501
                        % str(process_name)
                    )

        # Stop streaming
        if streaming:
            logger.debug(
                HEADER + "Stopping streaming for process " + str(process_name)
            )  # noqa: E501
            DistroStreamClientHandler.set_stop()

        sys.stdout.flush()
        sys.stderr.flush()
        if __debug__:
            logger.debug(HEADER + "[%s] Exiting process " % str(process_name))
        pipe.write(QUIT_TAG)
        pipe.close()
    except Exception as e:
        logger.error(e)
        raise e


def process_message(
    current_line: str,
    process_name: str,
    pipe: Pipe,
    queue: typing.Optional[Queue],
    tracing: bool,
    logger: typing.Any,
    logger_cfg: str,
    logger_handlers: list,
    logger_level: int,
    logger_formatter: typing.Any,
    storage_conf: str,
    storage_loggers: list,
    storage_loggers_handlers: list,
    cache_queue: typing.Optional[Queue] = None,
    cache_ids: typing.Any = None,
    cache_profiler: bool = False,
) -> bool:
    """Process command received from the runtime through a pipe.

    :param current_line: Current command (line) to process
    :param process_name: Process name for logger messages
    :param pipe: Pipe where to write the result
    :param queue: Queue where to drop the process exceptions
    :param tracing: Tracing
    :param logger: Logger
    :param logger_cfg: Logger configuration file
    :param logger_handlers: Logger handlers
    :param logger_level: Logger level
    :param logger_formatter: Logger formatter
    :param storage_conf: Storage configuration
    :param storage_loggers: Storage loggers
    :param storage_loggers_handlers: Storage loggers handlers
    :param cache_queue: Cache tracker communication queue
    :param cache_ids: Cache proxy dictionary (read-only)
    :param cache_profiler: Cache profiler
    :return: <Boolean> True if processed successfully, False otherwise.
    """
    if __debug__:
        logger.debug(
            HEADER
            + "[%s] Processing message: %s"
            % (str(process_name), str(current_line))  # noqa: E501
        )

    current_line_split = current_line.split()
    if current_line_split[0] == EXECUTE_TASK_TAG:
        # Process task
        return process_task(
            current_line_split,
            process_name,
            pipe,
            queue,
            tracing,
            logger,
            logger_cfg,
            logger_handlers,
            logger_level,
            logger_formatter,
            storage_conf,
            storage_loggers,
            storage_loggers_handlers,
            cache_queue,
            cache_ids,
            cache_profiler,
        )
    elif current_line_split[0] == PING_TAG:
        # Response -> Pong
        return process_ping(pipe, logger, process_name)
    elif current_line_split[0] == QUIT_TAG:
        # Received quit message -> Suicide
        return process_quit(logger, process_name)
    else:
        if __debug__:
            logger.debug(
                HEADER
                + "[%s] Unexpected message: %s"
                % (str(process_name), str(current_line_split))
            )
        raise PyCOMPSsException(
            "Unexpected message: %s" % str(current_line_split)
        )  # noqa: E501


def process_task(
    current_line: list,
    process_name: str,
    pipe: Pipe,
    queue: typing.Optional[Queue],
    tracing: bool,
    logger: typing.Any,
    logger_cfg: str,
    logger_handlers: list,
    logger_level: int,
    logger_formatter: typing.Any,
    storage_conf: str,
    storage_loggers: list,
    storage_loggers_handlers: list,
    cache_queue: typing.Optional[Queue],
    cache_ids: typing.Any,
    cache_profiler: bool,
) -> bool:
    """Process command received from the runtime through a pipe.

    :param current_line: Current command (line) to process.
    :param process_name: Process name for logger messages.
    :param pipe: Pipe where to write the result.
    :param queue: Queue where to drop the process exceptions.
    :param tracing: Tracing.
    :param logger: Logger.
    :param logger_cfg: Logger configuration file
    :param logger_handlers: Logger handlers.
    :param logger_level: Logger level.
    :param logger_formatter: Logger formatter.
    :param storage_conf: Storage configuration.
    :param storage_loggers: Storage loggers.
    :param storage_loggers_handlers: Storage loggers handlers.
    :param cache_queue: Cache tracker communication queue.
    :param cache_ids: Cache proxy dictionary (read-only).
    :param cache_profiler: Cache profiler
    :return: True if processed successfully, False otherwise.
    """
    with event_worker(TRACING_WORKER.process_task_event):
        affinity_event_emit = False
        binded_cpus = False
        binded_gpus = False

        # CPU binding
        cpus = current_line[-3]
        if cpus != "-" and THREAD_AFFINITY:
            # The cpu affinity event is already emitted in Java.
            # Instead of emitting what we receive, we are emitting what we
            # check after setting the affinity.
            binded_cpus = bind_cpus(cpus, process_name, logger)

        # GPU binding
        gpus = current_line[-2]
        if gpus != "-":
            emit_manual_event(int(gpus) + 1, inside=True, gpu_affinity=True)
            bind_gpus(gpus, process_name, logger)
            binded_gpus = True

        # Remove the last elements: cpu and gpu bindings
        current_line = current_line[0:-3]

        # task jobId command
        job_id, job_out, job_err = current_line[1:4]  # 4th is not taken
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
        # current_line[14 + #nodes] = <string>  = has return (always "null")
        # current_line[15 + #nodes] = <integer> = Number of parameters
        # <<list of parameters>>
        #       !---> type, stream, prefix , value

        if __debug__:
            logger.debug(
                HEADER
                + "[%s] Received task with id: %s"
                % (str(process_name), str(job_id))  # noqa: E501
            )
            logger.debug(
                HEADER
                + "[%s] - TASK CMD: %s"
                % (str(process_name), str(current_line))  # noqa: E501
            )

        # Swap logger from stream handler to file handler
        # All task output will be redirected to job.out/err
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
            # From now onwards the log is in the job out and err files
            logger.debug("-" * 100)
            logger.debug("Received task in process: %s" % str(process_name))
            logger.debug("TASK CMD: %s" % str(current_line))

        try:
            # Check thread affinity
            if THREAD_AFFINITY:
                # The cpu affinity can be long if multiple cores have been
                # assigned. To avoid issues, we get just the first id.
                real_affinity = thread_affinity.getaffinity()
                cpus = str(real_affinity[0])
                num_cpus = len(real_affinity)
                emit_manual_event(int(cpus) + 1, inside=True, cpu_affinity=True)
                emit_manual_event(int(num_cpus), inside=True, cpu_number=True)
                affinity_event_emit = True
                if not binded_cpus:
                    logger.warning(
                        "This task is going to be executed with default thread affinity %s"  # noqa: E501
                        % str(real_affinity)
                    )

            # Setup process environment
            cn = int(current_line[12])
            cn_names = ",".join(current_line[13 : 13 + cn])
            cu = current_line[13 + cn]
            if __debug__:
                logger.debug("Process environment:")
                logger.debug("\t - Number of nodes: %s" % (str(cn)))
                logger.debug("\t - Hostnames: %s" % str(cn_names))
                logger.debug("\t - Number of threads: %s" % (str(cu)))
            setup_environment(cn, cn_names, cu)

            # Execute task
            result = execute_task(
                process_name,
                storage_conf,
                current_line[9:],
                tracing,
                logger,
                logger_cfg,
                (job_out, job_err),
                False,
                None,
                cache_queue,
                cache_ids,
                cache_profiler,
            )
            # The ignored variable is timed_out
            exit_value, new_types, new_values, _, except_msg = result

            if exit_value == 0:
                # Task has finished without exceptions
                # endTask jobId exitValue message
                message = build_successful_message(
                    new_types, new_values, job_id, exit_value
                )
                if __debug__:
                    logger.debug(
                        "%s - Pipe %s END TASK MESSAGE: %s"
                        % (
                            str(process_name),
                            str(pipe.output_pipe),
                            str(message),
                        )  # noqa: E501
                    )
            elif exit_value == 2:
                # Task has finished with a COMPSs Exception
                # compssExceptionTask jobId exitValue message
                except_msg, message = build_compss_exception_message(
                    except_msg, job_id
                )  # noqa: E501
                if __debug__:
                    logger.debug(
                        "%s - Pipe %s COMPSS EXCEPTION TASK MESSAGE: %s"
                        % (
                            str(process_name),
                            str(pipe.output_pipe),
                            str(except_msg),
                        )  # noqa: E501
                    )
            else:
                # An exception other than COMPSsException has been raised
                # within the task
                message = build_exception_message(job_id, exit_value)
                if __debug__:
                    logger.debug(
                        "%s - Pipe %s END TASK MESSAGE: %s"
                        % (
                            str(process_name),
                            str(pipe.output_pipe),
                            str(message),
                        )  # noqa: E501
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
            # returns the id, the runtime can change the type (and locations)
            # to a EXTERNAL_OBJ_T.

        except Exception as e:
            logger.exception("%s - Exception %s" % (str(process_name), str(e)))
            if queue:
                queue.put("EXCEPTION")

            # Stop the worker process
            return False

        # Clean environment variables
        if __debug__:
            logger.debug("Cleaning environment.")
        clean_environment(binded_cpus, binded_gpus)
        if affinity_event_emit:
            emit_manual_event(0, inside=True, cpu_affinity=True)
            emit_manual_event(0, inside=True, cpu_number=True)
        if binded_gpus:
            emit_manual_event(0, inside=True, gpu_affinity=True)

        # Restore loggers
        if __debug__:
            logger.debug("Restoring loggers.")
            logger.debug("-" * 100)
            # No more logs in job out and err files
        # Restore worker log
        logger.removeHandler(out_file_handler)
        logger.removeHandler(err_file_handler)
        logger.handlers = []
        for handler in logger_handlers:
            logger.addHandler(handler)
        i = 0
        for storage_logger in storage_loggers:
            storage_logger.removeHandler(out_file_handler)
            storage_logger.removeHandler(err_file_handler)
            storage_logger.handlers = []
            for handler in storage_loggers_handlers[i]:
                storage_logger.addHandler(handler)
            i += 1
        if __debug__:
            logger.debug(
                HEADER
                + "[%s] Finished task with id: %s"
                % (str(process_name), str(job_id))  # noqa: E501
            )

        # Notify the runtime that the task has finished
        pipe.write(message)

        return True


def process_ping(pipe: Pipe, logger: typing.Any, process_name: str) -> bool:
    """Process ping message.

    Response: Pong.

    :param pipe: Where to write the ping response.
    :param logger: Logger.
    :param process_name: Process name.
    :return: True if success. False otherwise.
    """
    with event_worker(TRACING_WORKER.process_ping_event):
        if __debug__:
            logger.debug(HEADER + "[%s] Received ping." % str(process_name))
        try:
            pipe.write(PONG_TAG)
        except Exception:  # noqa
            return False
        return True


def process_quit(logger: typing.Any, process_name: str) -> bool:
    """Process quit message.

    Response: False.

    :param logger: Logger.
    :param process_name: Process name.
    :return: Always false.
    """
    with event_worker(TRACING_WORKER.process_quit_event):
        if __debug__:
            logger.debug(HEADER + "[%s] Received quit." % str(process_name))
        return False


def bind_cpus(cpus: str, process_name: str, logger: typing.Any) -> bool:
    """Bind the given CPUs for core affinity to this process.

    :param cpus: Target CPUs.
    :param process_name: Process name for logger messages.
    :param logger: Logger.
    :return: True if success, False otherwise.
    """
    with event_inside_worker(TRACING_WORKER.bind_cpus_event):
        if __debug__:
            logger.debug(
                HEADER
                + "[%s] Assigning affinity %s"
                % (str(process_name), str(cpus))  # noqa: E501
            )
        cpus_list = cpus.split(",")
        cpus_map = list(map(int, cpus_list))
        try:
            thread_affinity.setaffinity(cpus_map)
        except Exception:  # noqa
            if __debug__:
                logger.error(
                    HEADER
                    + "[%s] WARNING: could not assign affinity %s"
                    % (str(process_name), str(cpus_map))
                )
            return False
        # Export only if success
        os.environ["COMPSS_BINDED_CPUS"] = cpus
        return True


def bind_gpus(gpus: str, process_name: str, logger: typing.Any) -> None:
    """Bind the given GPUs to this process.

    :param gpus: Target GPUs.
    :param process_name: Process name for logger messages.
    :param logger: Logger.
    :return: None
    """
    with event_inside_worker(TRACING_WORKER.bind_gpus_event):
        os.environ["COMPSS_BINDED_GPUS"] = gpus
        os.environ["CUDA_VISIBLE_DEVICES"] = gpus
        os.environ["GPU_DEVICE_ORDINAL"] = gpus
        if __debug__:
            logger.debug(
                HEADER + "[%s] Assigning GPU %s" % (str(process_name), str(gpus))
            )


def setup_environment(cn: int, cn_names: str, cu: str) -> None:
    """Sets the environment (mainly environment variables).

    :param cn: Number of COMPSs nodes.
    :param cn_names: COMPSs hostnames.
    :param cu: Number of COMPSs threads.
    :return: None
    """
    with event_inside_worker(TRACING_WORKER.setup_environment_event):
        os.environ["COMPSS_NUM_NODES"] = str(cn)
        os.environ["COMPSS_HOSTNAMES"] = cn_names
        os.environ["COMPSS_NUM_THREADS"] = cu
        os.environ["OMP_NUM_THREADS"] = cu


def build_successful_message(
    new_types: list, new_values: list, job_id: str, exit_value: int
) -> str:
    """Generate a successful message.

    :param new_types: New types (can change if INOUT).
    :param new_values: New values (can change if INOUT).
    :param job_id: Job identifier.
    :param exit_value: Exit value.
    :return: Successful message.
    """
    with event_inside_worker(TRACING_WORKER.build_successful_message_event):
        # Task has finished without exceptions
        # endTask jobId exitValue message
        params = build_return_params_message(new_types, new_values)
        message = " ".join(
            (END_TASK_TAG, str(job_id), str(exit_value), str(params) + "\n")
        )
        return message


def build_compss_exception_message(
    except_msg: str, job_id: str
) -> typing.Tuple[str, str]:
    """Generate a COMPSs exception message.

    :param except_msg: Exception stacktrace.
    :param job_id: Job identifier.
    :return: Exception message and message.
    """
    with event_inside_worker(TRACING_WORKER.build_compss_exception_message_event):
        except_msg = except_msg.replace(" ", "_")
        message = " ".join(
            (COMPSS_EXCEPTION_TAG, str(job_id), str(except_msg) + "\n")
        )  # noqa: E501
        return except_msg, message


def build_exception_message(job_id: str, exit_value: int) -> str:
    """Generate an exception message.

    :param job_id: Job identifier.
    :param exit_value: Exit value.
    :return: Exception message.
    """
    with event_inside_worker(TRACING_WORKER.build_exception_message_event):
        message = " ".join((END_TASK_TAG, str(job_id), str(exit_value) + "\n"))
        return message


def clean_environment(cpus: bool, gpus: bool) -> None:
    """Clean the environment

    Mainly unset environment variables.

    :param cpus: If binded cpus.
    :param gpus: If binded gpus.
    :return: None
    """
    with event_inside_worker(TRACING_WORKER.clean_environment_event):
        if cpus:
            del os.environ["COMPSS_BINDED_CPUS"]
        if gpus:
            del os.environ["COMPSS_BINDED_GPUS"]
            del os.environ["CUDA_VISIBLE_DEVICES"]
            del os.environ["GPU_DEVICE_ORDINAL"]
        del os.environ["COMPSS_HOSTNAMES"]
