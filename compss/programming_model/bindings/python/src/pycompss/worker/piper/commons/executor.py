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
PyCOMPSs Worker - Piper - Commons - Executor.

This file contains the code of an executor running the commands that it
reads from a pipe (for mpi and multiprocessing piped workers).
"""

import copy
import logging
import os
import signal
import sys
import time
import traceback
import gc
import contextlib
from pycompss.util.process.manager import Queue
from pycompss.util.process.manager import DictProxy
from pycompss.runtime.management.object_tracker import OT

from pycompss.util.typing_helper import typing

try:
    THREAD_AFFINITY = True
    import process_affinity  # noqa
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

from pycompss.runtime.management.COMPSs import COMPSs
from pycompss.util.context import CONTEXT
from pycompss.runtime.commons import GLOBALS
from pycompss.worker.piper.commons.constants import TAGS
from pycompss.worker.piper.commons.utils_logger import load_loggers
from pycompss.worker.commons.executor import build_return_params_message
from pycompss.worker.commons.worker import execute_task
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.tracing.helpers import emit_manual_event
from pycompss.util.tracing.helpers import emit_manual_event_explicit
from pycompss.util.tracing.helpers import EventWorker
from pycompss.util.tracing.helpers import EventInsideWorker
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.worker.piper.cache.tracker import CACHE_TRACKER

# Streaming imports
from pycompss.streams.components.distro_stream_client import (
    DistroStreamClientHandler,
)

try:
    from threadpoolctl import threadpool_limits

    THREADPOOLCTL_AVAILABLE = True
except ImportError:
    THREADPOOLCTL_AVAILABLE = False

COMPSS_WITH_DLB = False
if int(os.getenv("COMPSS_WITH_DLB", 0)) >= 1:
    COMPSS_WITH_DLB = True
    import dlb_affinity


HEADER = "*[PYTHON EXECUTOR] "
EARING = False
if "EAR_INITIALIZATION_TIME" in os.environ:
    EAR_INITIALIZATION = int(os.environ["EAR_INITIALIZATION_TIME"])
else:
    EAR_INITIALIZATION = 40  # seconds minimum before doing any finalize.


def shutdown_handler(
    signal: int, frame: typing.Any  # pylint: disable=redefined-outer-name
) -> None:
    """Handle shutdown - Shutdown handler.

    CAUTION! Do not remove the parameters.

    :param signal: shutdown signal.
    :param frame: Frame.
    :return: None
    :raises PyCOMPSsException: Received signal.
    """
    process_id = os.getpid()
    process_name = os.environ["EAR_APP_NAME"]
    sys.stderr.write(
        f"[shutdown_handler] executor.py Received SIGTERM - "
        f"PID: {process_id} "
        f"PROCESS NAME: {process_name} "
        f"TIMESTAMP: {time.time()}\n"
    )
    sys.stderr.write(f"SIGNAL: {signal}\n")
    sys.stderr.write(f"FRAME: %{str(frame)}\n")
    traceback.print_stack(frame)
    sys.stderr.flush()
    sys.stdout.flush()
    raise PyCOMPSsException("Received SIGTERM")


class Pipe:
    """Bi-directional communication channel class."""

    __slots__ = ["input_pipe", "input_pipe_open", "output_pipe"]

    def __init__(self, input_pipe: str = "", output_pipe: str = "") -> None:
        """Construct a new Pipe.

        :param input_pipe: Input pipe for the thread. To receive messages from
                           the runtime.
        :param output_pipe: Output pipe for the thread. To send messages to
                            the runtime.
        """
        self.input_pipe = input_pipe
        self.input_pipe_open = None  # type: typing.Optional[typing.TextIO]
        self.output_pipe = output_pipe

    def read_command(self, retry_period: float = 0.5) -> str:
        """Return the first command on the pipe.

        :param retry_period: Time (ms) that the thread sleeps if EOF is read
                             from pipe.
        :return: The first command available on the pipe.
        """
        if self.input_pipe == "":
            raise PyCOMPSsException("Undefined input pipe in Pipe object")
        if self.input_pipe_open is None:
            self.input_pipe_open = open(  # pylint: disable=consider-using-with
                self.input_pipe, "r"
            )
            # Non blocking open:
            # fd = os.open(self.input_pipe, os.O_RDWR)
            # self.input_pipe_open = os.fdopen(fd, "r")

        line = self.input_pipe_open.readline()
        if line == "":
            time.sleep(0.001 * retry_period)
            line = self.input_pipe_open.readline()

        return line

    def write(self, message: str) -> None:
        """Write a message through the pipe.

        :param message: Message sent through the pipe.
        :return: None.
        """
        if self.output_pipe == "":
            raise PyCOMPSsException("Undefined output pipe in Pipe object")
        with open(self.output_pipe, "w") as out_pipe:
            out_pipe.write("".join((message, "\n")))

    def close(self) -> None:
        """Close the pipe, if open.

        :return: None.
        """
        if self.input_pipe_open:
            self.input_pipe_open.close()
            self.input_pipe_open = None

    def __str__(self) -> str:
        """Representation of the Pipe object.

        :return: String representing the Pipe object.
        """
        return " ".join(
            ("PIPE IN", self.input_pipe, "PIPE OUT", self.output_pipe)
        )


class ExecutorConf:
    """Executor configuration class."""

    __slots__ = [
        "debug",
        "tmp_dir",
        "tracing",
        "storage_conf",
        "logger",
        "persistent_storage",
        "storage_loggers",
        "stream_backend",
        "stream_master_ip",
        "stream_master_port",
        "cache_ids",
        "in_cache_queue",
        "out_cache_queue",
        "cache_profiler",
        "ear",
        "dp_enabled",
    ]

    def __init__(
        self,
        debug: bool,
        tmp_dir: str,
        tracing: bool,
        storage_conf: str,
        logger: logging.Logger,
        persistent_storage: bool,
        storage_loggers: typing.List[logging.Logger],
        stream_backend: str,
        stream_master_ip: str,
        stream_master_port: str,
        cache_ids: typing.Optional[DictProxy] = None,
        in_cache_queue: typing.Optional[Queue] = None,
        out_cache_queue: typing.Optional[Queue] = None,
        cache_profiler: bool = False,
        ear: bool = False,
        dp_enabled: bool = False,
    ) -> None:
        """Construct a new executor configuration.

        :param debug: If debug is enabled.
        :param tmp_dir: Temporary directory for logging purposes.
        :param tracing: Enable tracing for the executor.
        :param storage_conf: Storage configuration file.
        :param logger: Main logger.
        :param persistent_storage: If persistent storage is enabled
        :param storage_loggers: List of supported storage loggers
                                (empty if running w/o storage).
        :param stream_backend: Streaming backend type.
        :param stream_master_ip: Streaming master IP.
        :param stream_master_port: Streaming master port.
        :param cache_ids: Proxy cache dictionary.
        :param in_cache_queue: Cache queue where to submit to add new entries
                               to cache_ids.
        :param out_cache_queue: Cache queue where to the cache returns info.
        :param ear: Ear energy metering.
        :param dp_enabled: Check if Provenance is enabled or not.
        """
        self.debug = debug
        self.tmp_dir = tmp_dir
        self.tracing = tracing
        self.storage_conf = storage_conf
        self.logger = logger
        self.persistent_storage = persistent_storage
        self.storage_loggers = storage_loggers
        self.stream_backend = stream_backend
        self.stream_master_ip = stream_master_ip
        self.stream_master_port = stream_master_port
        self.cache_ids = cache_ids  # Read-only
        self.in_cache_queue = in_cache_queue
        self.out_cache_queue = out_cache_queue
        self.cache_profiler = cache_profiler
        self.ear = ear
        self.dp_enabled = dp_enabled


######################
#  Processes body
######################


def executor(
    lock: typing.Any,
    queue: typing.Union[None, Queue],
    event: typing.Any,
    process_id: int,
    process_name: str,
    pipe: Pipe,
    conf: ExecutorConf,
) -> None:
    """Thread main body - Overrides Threading run method.

    Iterates over the input pipe in order to receive tasks (with their
    parameters) and process them.
    Notifies the runtime when each task  has finished with the
    corresponding output value.
    Finishes when the "quit" message is received.

    :param lock: Lock to ensure mutual exclusion.
    :param queue: Queue where to put exception messages.
    :param process_id: Process identifier (number that matches the java
                       processes).
    :param process_name: Process name (Thread-X, where X is the thread id).
    :param pipe: Pipe to receive and send messages from/to the runtime.
    :param conf: Executor configuration.
    :return: None.
    """
    global EARING
    start_time = time.time()

    try:
        # First thing to do is to emit the process identifier event
        emit_manual_event_explicit(
            TRACING_WORKER.process_identifier,
            TRACING_WORKER.process_worker_executor_event,
        )
        # Second thing to do is to emit the executor process identifier event
        emit_manual_event_explicit(
            TRACING_WORKER.executor_identifier, process_id
        )

        if COMPSS_WITH_DLB:
            dlb_affinity.init()
            dlb_affinity.setaffinity([], os.getpid())
            dlb_affinity.lend()

        # Replace Python Worker's SIGTERM handler.
        signal.signal(signal.SIGTERM, shutdown_handler)

        if len(conf.logger.handlers) == 0:
            # Logger has not been inherited correctly. Happens in MacOS.
            tmp_dir = os.path.join(conf.tmp_dir, "..")
            GLOBALS.set_temporary_directory(tmp_dir)
            # Reload logger
            (
                conf.logger,
                conf.storage_loggers,
                _,
            ) = load_loggers(conf.debug, conf.persistent_storage)
            # Set the binding in worker mode too
            CONTEXT.set_worker()
        logger = conf.logger

        tracing = conf.tracing
        storage_conf = conf.storage_conf
        storage_loggers = conf.storage_loggers

        # Get a copy of the necessary information from the logger to
        # re-establish after each task
        logger_handlers = copy.copy(logger.handlers)
        logger_level = logger.getEffectiveLevel()
        _fmt = logger_handlers[0].formatter._fmt  # type: ignore # pylint:W0212
        # disable=protected-access
        logger_formatter = logging.Formatter(_fmt)
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
                from storage.api import (  # pylint: disable=E0401, C0415
                    # disable=import-error, import-outside-toplevel
                    initWorkerPostFork,
                )

                with EventWorker(TRACING_WORKER.init_worker_postfork_event):
                    initWorkerPostFork()
            except (ImportError, AttributeError):
                if __debug__:
                    logger.info(
                        "%s[%s] Could not find initWorkerPostFork "
                        "storage call. Ignoring it.",
                        HEADER,
                        str(process_name),
                    )

        # Start the streaming backend if necessary
        streaming = False
        if conf.stream_backend not in [None, "null", "NONE"]:
            streaming = True

        if streaming:
            # Initialize streaming
            if __debug__:
                logger.debug(
                    "%s[%s] Starting streaming for process",
                    HEADER,
                    str(process_name),
                )
            try:
                DistroStreamClientHandler.init_and_start(
                    master_ip=conf.stream_master_ip,
                    master_port=conf.stream_master_port,
                )
            except (
                Exception
            ) as general_exception:  # pylint: disable=broad-except
                logger.error(general_exception)
                raise general_exception from general_exception

        # Load ear if necessary
        if conf.ear:
            # Initialize ear
            if __debug__:
                logger.debug(
                    "%s[%s] Loading ear",
                    HEADER,
                    str(process_name),
                )
            with EventWorker(TRACING_WORKER.executor_load_ear_event):
                import ear
            EARING = True

        # Connect to Shared memory manager
        if conf.in_cache_queue and conf.out_cache_queue:
            CACHE_TRACKER.set_lock(lock)
            CACHE_TRACKER.connect_to_shared_memory_manager()

        # Process properties
        alive = True

        if __debug__:
            logger.debug("%s[%s] Starting process", HEADER, str(process_name))

        # MAIN EXECUTOR LOOP
        while alive and not event.is_set():
            # Runtime -> pipe - Read command from pipe
            command = COMPSs.read_pipes()
            if command != "":
                if __debug__:
                    logger.debug(
                        "%s[%s] Received command %s",
                        HEADER,
                        str(process_name),
                        str(command),
                    )
                # Process the command
                alive = process_message(
                    command,
                    process_name,
                    pipe,
                    queue,
                    tracing,
                    logger,
                    logger_handlers,
                    logger_level,
                    logger_formatter,
                    storage_conf,
                    storage_loggers,
                    storage_loggers_handlers,
                    conf.in_cache_queue,
                    conf.out_cache_queue,
                    conf.cache_ids,
                    conf.cache_profiler,
                )
        if __debug__:
            logger.debug(
                "%s[%s] Ending process (alive: %s - event: %s)",
                HEADER,
                str(process_name),
                str(alive),
                str(event.is_set()),
            )
        # Stop storage
        if storage_conf != "null":
            try:
                from storage.api import (  # pylint: disable=E0401, C0415
                    # disable=import-error, import-outside-toplevel
                    finishWorkerPostFork,
                )

                with EventWorker(TRACING_WORKER.finish_worker_postfork_event):
                    finishWorkerPostFork()
            except (ImportError, AttributeError):
                if __debug__:
                    logger.info(
                        "%s[%s] Could not find finishWorkerPostFork "
                        "storage call. Ignoring it.",
                        HEADER,
                        str(process_name),
                    )

        # Stop streaming
        if streaming:
            if __debug__:
                logger.debug(
                    "%s Stopping streaming for process ",
                    HEADER,
                    str(process_name),
                )
            DistroStreamClientHandler.set_stop()

        sys.stdout.flush()
        sys.stderr.flush()
        if EARING:
            with EventWorker(TRACING_WORKER.executor_finalize_ear_event):
                elapsed_time = time.time() - start_time
                if __debug__:
                    logger.debug(
                        "%s[%s] Stopping EAR (elapsed %s)",
                        HEADER,
                        str(process_name),
                        str(elapsed_time),
                    )
                if elapsed_time < EAR_INITIALIZATION:
                    logger.debug(
                        "%s[%s] Waiting to finalize EAR: %s seconds",
                        HEADER,
                        str(process_name),
                        str(elapsed_time),
                    )
                    time.sleep(elapsed_time)
                ear.finalize()
            EARING = False
            if __debug__:
                logger.debug(
                    "%s[%s] Stopped EAR at %s",
                    HEADER,
                    str(process_name),
                    str(time.time()),
                )
        if __debug__:
            logger.debug("%s[%s] Exiting process ", HEADER, str(process_name))
        # Send quit message back to the runtime
        pipe.write(TAGS.quit)
        pipe.close()
    except Exception as general_exception:  # pylint: disable=broad-except
        sys.stderr.write("\nGENERAL EXCEPTION:\n")
        sys.stderr.write(f"\n{str(general_exception)}\n")
        sys.stderr.flush()
        raise general_exception from general_exception


def process_message(
    current_line: str,
    process_name: str,
    pipe: Pipe,
    queue: typing.Optional[Queue],
    tracing: bool,
    logger: logging.Logger,
    logger_handlers: list,
    logger_level: int,
    logger_formatter: typing.Any,
    storage_conf: str,
    storage_loggers: typing.List[logging.Logger],
    storage_loggers_handlers: list,
    in_cache_queue: typing.Optional[Queue] = None,
    out_cache_queue: typing.Optional[Queue] = None,
    cache_ids: typing.Any = None,
    cache_profiler: bool = False,
) -> bool:
    """Process command received from the runtime through a pipe.

    :param current_line: Current command (line) to process.
    :param process_name: Process name for logger messages.
    :param pipe: Pipe where to write the result.
    :param queue: Queue where to drop the process exceptions.
    :param tracing: Tracing.
    :param logger: Logger.
    :param logger_handlers: Logger handlers.
    :param logger_level: Logger level.
    :param logger_formatter: Logger formatter.
    :param storage_conf: Storage configuration.
    :param storage_loggers: Storage loggers.
    :param storage_loggers_handlers: Storage loggers handlers.
    :param in_cache_queue: Cache tracker input communication queue.
    :param out_cache_queue: Cache tracker output communication queue.
    :param cache_ids: Cache proxy dictionary (read-only).
    :param cache_profiler: Cache profiler.
    :return: True if processed successfully, False otherwise.
    """
    if __debug__:
        logger.debug(
            "%s[%s] Processing message: %s",
            HEADER,
            str(process_name),
            str(current_line),
        )

    current_line_split = current_line.split()
    if current_line_split[0] == TAGS.execute_task:
        # Process task
        return process_task(
            current_line_split,
            process_name,
            pipe,
            queue,
            tracing,
            logger,
            logger_handlers,
            logger_level,
            logger_formatter,
            storage_conf,
            storage_loggers,
            storage_loggers_handlers,
            in_cache_queue,
            out_cache_queue,
            cache_ids,
            cache_profiler,
        )

    if current_line_split[0] == TAGS.ping:
        # Response -> Pong
        return process_ping(pipe, logger, process_name)

    if current_line_split[0] == TAGS.quit:
        # Received quit message -> Suicide
        return process_quit(logger, process_name)

    if __debug__:
        logger.debug(
            "%s[%s] Unexpected message: %s",
            HEADER,
            str(process_name),
            str(current_line_split),
        )
    raise PyCOMPSsException(f"Unexpected message: {str(current_line_split)}")


def process_task(
    current_line: typing.List[str],
    process_name: str,
    pipe: Pipe,
    queue: typing.Optional[Queue],
    tracing: bool,
    logger: logging.Logger,
    logger_handlers: list,
    logger_level: int,
    logger_formatter: typing.Any,
    storage_conf: str,
    storage_loggers: typing.List[logging.Logger],
    storage_loggers_handlers: list,
    in_cache_queue: typing.Optional[Queue],
    out_cache_queue: typing.Optional[Queue],
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
    :param logger_handlers: Logger handlers.
    :param logger_level: Logger level.
    :param logger_formatter: Logger formatter.
    :param storage_conf: Storage configuration.
    :param storage_loggers: Storage loggers.
    :param storage_loggers_handlers: Storage loggers handlers.
    :param in_cache_queue: Cache tracker input communication queue.
    :param out_cache_queue: Cache tracker output communication queue.
    :param cache_ids: Cache proxy dictionary (read-only).
    :param cache_profiler: Cache profiler.
    :return: True if processed successfully, False otherwise.
    """
    with EventWorker(TRACING_WORKER.process_task_event):
        affinity_event_emit = False
        binded_cpus = False
        binded_gpus = False
        current_working_dir = os.getcwd()

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
            # Emit a mask event to identify which gpu has been assigned
            # (add always 1 since 0 can not be emitted).
            # Single gpu assigned:
            #   - 0 = gpu 1 assigned => 0001 == 1 (decimal)
            #   - 1 = gpu 2 assigned => 0010 == 2 (decimal)
            #   - 2 = gpu 3 assigned => 0100 == 4 (decimal)
            #   - 3 = gpu 4 assigned => 1000 == 8 (decimal)
            # Multiple gpu assigned:
            #   - 0,1 = gpu 1 and 2 assigned => 0011 == 3 (decimal)
            #   - 2,3 = gpu 3 and 4 assigned => 1100 == 12 (decimal)
            #   - 0,1,3,4 = gpu 1, 2,3 and 4 assigned => 1111 == 15 (decimal)
            gpu_mask_list = [0] * 4
            if "," in gpus:
                # There are more than one gpus assigned.
                gpus_list = gpus.split(",")
                for i in range(len(gpus_list)):
                    gpu_mask_list[int(i)] = 1
            else:
                # It is a single gpu where gpus identify which gpu has been
                # assigned (0, 1, 2, or 3).
                gpu_mask_list[int(gpus)] = 1
            gpu_mask_list.reverse()
            gpu_mask = "".join(map(str, gpu_mask_list))
            assigned_gpus = int(gpu_mask, 2)  # convert base 2 to decimal
            emit_manual_event(assigned_gpus, inside=True, gpu_affinity=True)
            bind_gpus(gpus, process_name, logger)
            binded_gpus = True

        # Remove the last elements: cpu and gpu bindings
        current_line = current_line[0:-3]

        # task jobId command
        job_id, working_dir, job_out, job_err = current_line[
            1:5
        ]  # 5th is not taken
        # current_line[5] = <boolean> = tracing
        # current_line[6] = <integer> = task id
        # current_line[7] = <boolean> = debug
        # current_line[8] = <string>  = storage conf.
        # current_line[9] = <string>  = operation type (e.g. METHOD)
        # current_line[10] = <string>  = module
        # current_line[11]= <string>  = method
        # current_line[12]= <string>  = time out
        # current_line[13]= <integer> = Number of slaves (worker nodes)==#nodes
        # <<list of slave nodes>>
        # current_line[14 + #nodes] = <integer> = computing units
        # current_line[15 + #nodes] = <boolean> = has target
        # current_line[16 + #nodes] = <string>  = has return (always "null")
        # current_line[17 + #nodes] = <integer> = Number of parameters
        # <<list of parameters>>
        #       !---> type, stream, prefix , value

        # Setting working directory
        os.chdir(working_dir)
        GLOBALS.set_temporary_directory(working_dir)

        if __debug__:
            logger.debug(
                "%s[%s] Received task with id: %s",
                HEADER,
                str(process_name),
                str(job_id),
            )
            logger.debug(
                "%s[%s] Setting working directory: %s",
                HEADER,
                str(process_name),
                str(working_dir),
            )
            logger.debug(
                "%s[%s] - TASK CMD: %s",
                HEADER,
                str(process_name),
                str(current_line),
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
            logger.debug("Received task in process: %s", str(process_name))
            logger.debug("TASK CMD: %s", str(current_line))

        try:
            # Check thread affinity
            if THREAD_AFFINITY:
                # The cpu affinity can be long if multiple cores have been
                # assigned. To avoid issues, we get just the first id.
                real_affinity = process_affinity.getaffinity()
                cpus = str(real_affinity[0])
                num_cpus = len(real_affinity)
                emit_manual_event(
                    int(cpus) + 1, inside=True, cpu_affinity=True
                )
                emit_manual_event(int(num_cpus), inside=True, cpu_number=True)
                affinity_event_emit = True
                if not binded_cpus:
                    logger.warning(
                        "This task is going to be executed with default "
                        "thread affinity %s",
                        str(real_affinity),
                    )

            # Setup process environment
            compss_procs = int(current_line[13])
            compss_nodes = int(current_line[14])
            compss_nodes_names = ",".join(
                current_line[15 : 15 + compss_nodes]  # noqa: E203
            )
            computing_units = current_line[15 + compss_nodes]
            if __debug__:
                logger.debug("Process environment:")
                logger.debug("\t - Number of nodes: %s", (str(compss_nodes)))
                logger.debug("\t - Hostnames: %s", str(compss_nodes_names))
                logger.debug(
                    "\t - Number of threads: %s", (str(computing_units))
                )
            setup_environment(
                compss_nodes, compss_procs, compss_nodes_names, computing_units
            )

            # Clean object tracker
            OT.clean_object_tracker(hard_stop=False)

            if not COMPSS_WITH_DLB and THREADPOOLCTL_AVAILABLE:
                thread_context = threadpool_limits(limits=int(computing_units))
            else:
                thread_context = contextlib.nullcontext()

            with thread_context:
                # Execute task
                result = execute_task(
                    process_name,
                    storage_conf,
                    current_line[10:],
                    tracing,
                    logger,
                    (job_out, job_err),
                    False,
                    {},
                    in_cache_queue,
                    out_cache_queue,
                    cache_ids,
                    cache_profiler,
                )

            # The ignored variable is timed_out
            exit_value, new_types, new_values, _, except_msg = result

            if COMPSS_WITH_DLB:
                dlb_affinity.setaffinity([], os.getpid())
                dlb_affinity.lend()

            if exit_value == 0:
                # Task has finished without exceptions
                # endTask jobId exitValue message
                message = build_successful_message(
                    new_types, new_values, job_id, exit_value
                )
                if __debug__:
                    logger.debug(
                        "%s - Pipe %s END TASK MESSAGE: %s",
                        str(process_name),
                        str(pipe.output_pipe),
                        str(message),
                    )
            elif exit_value == 2:
                # Task has finished with a COMPSs Exception
                # compssExceptionTask jobId exitValue message
                except_msg, message = build_compss_exception_message(
                    except_msg, job_id
                )
                if __debug__:
                    logger.debug(
                        "%s - Pipe %s COMPSS EXCEPTION TASK MESSAGE: %s",
                        str(process_name),
                        str(pipe.output_pipe),
                        str(except_msg),
                    )
            else:
                # An exception other than COMPSsException has been raised
                # within the task
                message = build_exception_message(job_id, exit_value)
                if __debug__:
                    logger.debug(
                        "%s - Pipe %s END TASK MESSAGE: %s",
                        str(process_name),
                        str(pipe.output_pipe),
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
            # returns the id, the runtime can change the type (and locations)
            # to a EXTERNAL_OBJ_T.

        except Exception as general_exception:  # pylint: disable=broad-except
            logger.exception(
                "%s - Exception %s", str(process_name), str(general_exception)
            )
            if queue:
                queue.put("EXCEPTION")
            # Clean object tracker
            OT.clean_object_tracker(hard_stop=True)
            # Go back to initial current working directory
            os.chdir(current_working_dir)
            GLOBALS.set_temporary_directory(current_working_dir)
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

        with EventInsideWorker(TRACING_WORKER.cleanup_task_event):
            gc.collect()

        if __debug__:
            logger.debug(
                "%s[%s] Finished task with id: %s",
                HEADER,
                str(process_name),
                str(job_id),
            )

        # Clean object tracker
        OT.clean_object_tracker(hard_stop=True)
        # Notify the runtime that the task has finished
        pipe.write(message)
        # Go back to original working directory
        os.chdir(current_working_dir)
        GLOBALS.set_temporary_directory(current_working_dir)
        return True


def process_ping(
    pipe: Pipe, logger: logging.Logger, process_name: str
) -> bool:
    """Process ping message.

    Response: Pong.

    :param pipe: Where to write the ping response.
    :param logger: Logger.
    :param process_name: Process name.
    :return: True if success. False otherwise.
    """
    with EventWorker(TRACING_WORKER.process_ping_event):
        if __debug__:
            logger.debug("%s[%s] Received ping.", HEADER, str(process_name))
        try:
            pipe.write(TAGS.pong)
        except Exception:  # pylint: disable=broad-except
            return False
        return True


def process_quit(logger: logging.Logger, process_name: str) -> bool:
    """Process quit message.

    Response: False.

    :param logger: Logger.
    :param process_name: Process name.
    :return: Always false.
    """
    with EventWorker(TRACING_WORKER.process_quit_event):
        if __debug__:
            logger.debug("%s[%s] Received quit.", HEADER, str(process_name))
        return False


def bind_cpus(cpus: str, process_name: str, logger: logging.Logger) -> bool:
    """Bind the given CPUs for core affinity to this process.

    :param cpus: Target CPUs.
    :param process_name: Process name for logger messages.
    :param logger: Logger.
    :return: True if success, False otherwise.
    """
    import traceback

    with EventInsideWorker(TRACING_WORKER.bind_cpus_event):
        if __debug__:
            logger.debug(
                "%s[%s] Assigning affinity %s",
                HEADER,
                str(process_name),
                str(cpus),
            )
        cpus_list = cpus.split(",")
        cpus_map = list(map(int, cpus_list))
        try:
            if COMPSS_WITH_DLB:
                dlb_affinity.setaffinity(cpus_map, os.getpid())
            else:
                process_affinity.setaffinity(cpus_map)
        except Exception as e:  # pylint: disable=broad-except
            if __debug__:
                logger.error(
                    "%s[%s] WARNING: could not assign affinity %s",
                    HEADER,
                    str(process_name),
                    str(cpus_map),
                )
                traceback.print_exc()
                logger.error(str(e))
            return False
        # Export only if success
        os.environ["COMPSS_BINDED_CPUS"] = cpus
        os.environ["COMPSS_NUM_CPUS"] = str(len(cpus_list))
        return True


def bind_gpus(gpus: str, process_name: str, logger: logging.Logger) -> None:
    """Bind the given GPUs to this process.

    :param gpus: Target GPUs.
    :param process_name: Process name for logger messages.
    :param logger: Logger.
    :return: None.
    """
    with EventInsideWorker(TRACING_WORKER.bind_gpus_event):
        os.environ["COMPSS_BINDED_GPUS"] = gpus
        os.environ["CUDA_VISIBLE_DEVICES"] = gpus
        os.environ["GPU_DEVICE_ORDINAL"] = gpus
        if __debug__:
            logger.debug(
                "%s[%s] Assigning GPU %s", HEADER, str(process_name), str(gpus)
            )


def setup_environment(
    compss_nodes: int,
    compss_ppn: int,
    compss_nodes_names: str,
    computing_units: str,
) -> None:
    """Set the environment (mainly environment variables).

    :param compss_nodes: Number of COMPSs nodes.
    :param compss_nodes_names: COMPSs hostnames.
    :param computing_units: Number of COMPSs threads.
    :return: None.
    """
    with EventInsideWorker(TRACING_WORKER.setup_environment_event):
        os.environ["COMPSS_NUM_NODES"] = str(compss_nodes)
        os.environ["COMPSS_NUM_PROCS"] = str(compss_ppn * compss_nodes)
        os.environ["COMPSS_HOSTNAMES"] = compss_nodes_names
        os.environ["COMPSS_NUM_THREADS"] = computing_units
        os.environ["OMP_NUM_THREADS"] = computing_units


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
    with EventInsideWorker(TRACING_WORKER.build_successful_message_event):
        # Task has finished without exceptions
        # endTask jobId exitValue message
        params = build_return_params_message(new_types, new_values)
        message = " ".join(
            (TAGS.end_task, str(job_id), str(exit_value), str(params) + "\n")
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
    with EventInsideWorker(
        TRACING_WORKER.build_compss_exception_message_event
    ):
        except_msg = except_msg.replace(" ", "_")
        message = " ".join(
            (TAGS.compss_exception, str(job_id), str(except_msg) + "\n")
        )
        return except_msg, message


def build_exception_message(job_id: str, exit_value: int) -> str:
    """Generate an exception message.

    :param job_id: Job identifier.
    :param exit_value: Exit value.
    :return: Exception message.
    """
    with EventInsideWorker(TRACING_WORKER.build_exception_message_event):
        message = " ".join(
            (TAGS.end_task, str(job_id), str(exit_value) + "\n")
        )
        return message


def clean_environment(cpus: bool, gpus: bool) -> None:
    """Clean the environment.

    Mainly unset environment variables.

    :param cpus: If binded cpus.
    :param gpus: If binded gpus.
    :return: None
    """
    with EventInsideWorker(TRACING_WORKER.clean_environment_event):
        if cpus:
            del os.environ["COMPSS_BINDED_CPUS"]
        if gpus:
            del os.environ["COMPSS_BINDED_GPUS"]
            del os.environ["CUDA_VISIBLE_DEVICES"]
            del os.environ["GPU_DEVICE_ORDINAL"]
        del os.environ["COMPSS_HOSTNAMES"]
