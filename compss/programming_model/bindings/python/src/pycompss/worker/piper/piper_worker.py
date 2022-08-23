#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Worker - Piper - Multiprocessing worker.

This file contains the multiprocessing piper worker code.
"""

import os
import signal
import sys
from multiprocessing import Process  # Used only for typing

from pycompss.util.context import CONTEXT
from pycompss.runtime.commons import GLOBALS
from pycompss.util.process.manager import Queue  # just typing
from pycompss.util.process.manager import create_process
from pycompss.util.process.manager import initialize_multiprocessing
from pycompss.util.process.manager import new_queue
from pycompss.util.tracing.helpers import dummy_context
from pycompss.util.tracing.helpers import EventWorker
from pycompss.util.tracing.helpers import trace_multiprocessing_worker
from pycompss.util.tracing.helpers import emit_manual_event_explicit
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.util.typing_helper import typing
from pycompss.worker.piper.cache.setup import is_cache_enabled
from pycompss.worker.piper.cache.setup import start_cache
from pycompss.worker.piper.cache.setup import stop_cache
from pycompss.worker.piper.commons.constants import HEADER
from pycompss.worker.piper.commons.constants import TAGS
from pycompss.worker.piper.commons.executor import ExecutorConf
from pycompss.worker.piper.commons.executor import Pipe
from pycompss.worker.piper.commons.executor import executor
from pycompss.worker.piper.commons.utils import PiperWorkerConfiguration
from pycompss.worker.piper.commons.utils_logger import load_loggers

# Persistent worker global variables
# PROCESSES = IN_PIPE -> PROCESS
PROCESSES = {}  # type: typing.Dict[str, Process]
CACHE = None
CACHE_PROCESS = None


def shutdown_handler(
    signal: int,  # pylint: disable=redefined-outer-name, unused-argument
    frame: typing.Any,  # pylint: disable=unused-argument
) -> None:
    """Handle shutdown - Shutdown handler.

    CAUTION! Do not remove the parameters.

    :param signal: Shutdown signal.
    :param frame: Frame.
    :return: None
    """
    for proc in PROCESSES.values():
        if proc.is_alive():
            proc.terminate()
    if CACHE and CACHE_PROCESS.is_alive():  # noqa
        CACHE_PROCESS.terminate()  # noqa


######################
# Main method
######################


def compss_persistent_worker(config: PiperWorkerConfiguration, tracing: bool) -> None:
    """Retrieve the initial configuration and spawns the worker processes.

    Persistent worker main function.

    :param config: Piper Worker Configuration description.
    :param tracing: If tracing is enabled.
    :return: None.
    """
    global CACHE
    global CACHE_PROCESS

    # Catch SIGTERM sent by bindings_piper
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Set the binding in worker mode
    CONTEXT.set_worker()

    persistent_storage = config.storage_conf != "null"

    logger, logger_cfg, storage_loggers, log_dir = load_loggers(
        config.debug, persistent_storage
    )

    if __debug__:
        logger.debug("%s[piper_worker.py] wake up", HEADER)
        config.print_on_logger(logger)

    if persistent_storage:
        # Initialize storage
        logger.debug("%sStarting persistent storage", HEADER)
        with EventWorker(TRACING_WORKER.init_storage_at_worker_event):
            from storage.api import (  # pylint: disable=import-error, import-outside-toplevel
                initWorker as initStorageAtWorker,
            )

            initStorageAtWorker(config_file_path=config.storage_conf)

    # Create new processes
    queues = []

    cache_profiler = False
    if config.cache_profiler.lower() == "true":
        cache_profiler = True

    # Setup cache
    CACHE = False
    cache_ids, in_cache_queue, out_cache_queue = None, None, None
    if is_cache_enabled(str(config.cache)):
        # Deploy the necessary processes
        CACHE = True
        cache_params = start_cache(logger, str(config.cache), cache_profiler, log_dir)
        (
            smm,
            cache_process,
            in_cache_queue_act,
            out_cache_queue_act,
            cache_ids,
        ) = cache_params
        in_cache_queue = in_cache_queue_act
        out_cache_queue = out_cache_queue_act
        CACHE_PROCESS = cache_process

    # Create new executor processes
    conf = ExecutorConf(
        config.debug,
        GLOBALS.get_temporary_directory(),
        tracing,
        config.storage_conf,
        logger,
        logger_cfg,
        persistent_storage,
        storage_loggers,
        config.stream_backend,
        config.stream_master_name,
        config.stream_master_port,
        cache_ids,
        in_cache_queue,
        out_cache_queue,
        cache_profiler,
    )

    for i in range(0, config.tasks_x_node):
        exec_id = config.exec_ids[i]
        if __debug__:
            logger.debug("%sLaunching process %s", HEADER, str(exec_id))
        process_name = "".join(("Process-", str(exec_id)))
        pid, queue = create_executor_process(
            exec_id, process_name, conf, config.pipes[i]
        )
        queues.append(queue)

    # Read command from control pipe
    alive = True
    error_msgs = []  # type: typing.List[str]
    process_counter = config.tasks_x_node
    control_pipe = config.control_pipe
    while alive:
        command = control_pipe.read_command(retry_period=1)
        if command != "":
            line = command.split()

            if line[0] == TAGS.add_executor:
                process_name = "".join(("Process-", str(process_counter)))
                process_counter = process_counter + 1
                exec_id = int(line[1])
                in_pipe = line[2]
                out_pipe = line[3]
                pipe = Pipe(in_pipe, out_pipe)
                pid, queue = create_executor_process(exec_id, process_name, conf, pipe)
                queues.append(queue)
                control_pipe.write(
                    " ".join((TAGS.added_executor, out_pipe, in_pipe, str(pid)))
                )

            elif line[0] == TAGS.query_executor_id:
                in_pipe = line[1]
                out_pipe = line[2]
                query_proc = PROCESSES[in_pipe]
                query_pid = query_proc.pid
                control_pipe.write(
                    " ".join(
                        (TAGS.reply_executor_id, out_pipe, in_pipe, str(query_pid))
                    )
                )

            elif line[0] == TAGS.cancel_task:
                in_pipe = line[1]
                cancel_proc = PROCESSES[in_pipe]
                cancel_pid = cancel_proc.pid
                if cancel_pid is None:
                    alive = False
                    error_msgs.append("Cancel pid is None")
                else:
                    cancel_pid_int = int(cancel_pid)
                if __debug__:
                    logger.debug(
                        "%sSignaling process with PID %s to cancel a task",
                        HEADER,
                        str(cancel_pid),
                    )
                # Cancellation produced by COMPSs
                os.kill(cancel_pid_int, signal.SIGUSR2)

            elif line[0] == TAGS.remove_executor:
                in_pipe = line[1]
                out_pipe = line[2]
                proc = PROCESSES.pop(in_pipe, None)
                if proc:
                    if proc.is_alive():
                        logger.warning("%sForcing terminate on : %s", HEADER, proc.name)
                        proc.terminate()
                    proc.join()
                control_pipe.write(" ".join((TAGS.removed_executor, out_pipe, in_pipe)))

            elif line[0] == TAGS.ping:
                control_pipe.write(TAGS.pong)

            elif line[0] == TAGS.quit:
                alive = False

    # Wait for all threads
    for proc in PROCESSES.values():
        proc.join()

    # Check if there is any exception message from the threads
    for i in range(0, config.tasks_x_node):
        if not queues[i].empty:
            logger.error(
                "%sException in threads queue: %s", HEADER, str(queues[i].get())
            )

    # Check if there is any exception from the messages
    for msg in error_msgs:
        logger.error("%sException in piper worker message: %s", HEADER, msg)

    for queue in queues:
        queue.close()
        queue.join_thread()

    if CACHE:
        # Beware of smm, in_cache_queue_act, out_cache_queue_act and
        # cache_process variables, since they are only initialized when
        # cache is enabled. Reason for noqa.
        stop_cache(
            smm, in_cache_queue_act, out_cache_queue_act, cache_profiler, cache_process
        )

    if persistent_storage:
        # Finish storage
        if __debug__:
            logger.debug("%sStopping persistent storage", HEADER)
        with EventWorker(TRACING_WORKER.finish_storage_at_worker_event):
            from storage.api import (  # pylint: disable=import-error, import-outside-toplevel
                finishWorker as finishStorageAtWorker,
            )

            finishStorageAtWorker()

    if __debug__:
        logger.debug("%sFinished", HEADER)

    control_pipe.write(TAGS.quit)
    control_pipe.close()


def create_executor_process(
    executor_id: int, executor_name: str, conf: ExecutorConf, pipe: Pipe
) -> typing.Tuple[int, Queue]:
    """Start a new executor.

    :param executor_id: Executor process identifier.
    :param executor_name: Executor process name.
    :param conf: executor config.
    :param pipe: Communication pipes (in, out).
    :return: Process identifier and queue used by the process.
    """
    queue = new_queue()
    process = create_process(
        target=executor,
        args=(queue, executor_id, executor_name, pipe, conf),
        prepend_lock=True,
    )
    PROCESSES[pipe.input_pipe] = process
    process.start()
    return int(str(process.pid)), queue


############################
# Main -> Calls main method
############################


def main() -> None:
    """Start the multiprocessing worker.

    :return: None.
    """
    # Configure the global tracing variable from the argument
    tracing = sys.argv[4] == "true"
    with trace_multiprocessing_worker() if tracing else dummy_context():
        # First thing to do is to emit the process identifier event
        emit_manual_event_explicit(
            TRACING_WORKER.process_identifier, TRACING_WORKER.process_worker_event
        )
        # Configure the piper worker with the arguments
        worker_conf = PiperWorkerConfiguration()
        worker_conf.update_params(sys.argv)
        compss_persistent_worker(worker_conf, tracing)


if __name__ == "__main__":
    # Initialize multiprocessing
    initialize_multiprocessing()
    # Then start the main function
    main()
