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
PyCOMPSs Binding - Management - Link.

This file contains the functions to link with the binding-commons.
In particular, manages a separate process which handles the compss
extension, so that the process can be removed when shutting off
and restarted (interactive usage of PyCOMPSs - ipython and jupyter).
"""

import os

from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.process.manager import Queue
from pycompss.util.process.manager import create_process
from pycompss.util.process.manager import new_process
from pycompss.util.process.manager import new_queue
from pycompss.util.std.redirects import ipython_std_redirector
from pycompss.util.std.redirects import not_std_redirector
from pycompss.util.typing_helper import typing

# Global variables
LINK_PROCESS = new_process()
IN_QUEUE = new_queue()
OUT_QUEUE = new_queue()
RELOAD = False

# Queue messages
START = "START"
SET_DEBUG = "SET_DEBUG"
STOP = "STOP"
CANCEL_TASKS = "CANCEL_TASKS"
ACCESSED_FILE = "ACCESSED_FILE"
OPEN_FILE = "OPEN_FILE"
CLOSE_FILE = "CLOSE_FILE"
DELETE_FILE = "DELETE_FILE"
GET_FILE = "GET_FILE"
GET_DIRECTORY = "GET_DIRECTORY"
BARRIER = "BARRIER"
BARRIER_GROUP = "BARRIER_GROUP"
OPEN_TASK_GROUP = "OPEN_TASK_GROUP"
CLOSE_TASK_GROUP = "CLOSE_TASK_GROUP"
GET_LOGGING_PATH = "GET_LOGGING_PATH"
GET_NUMBER_OF_RESOURCES = "GET_NUMBER_OF_RESOURCES"
REQUEST_RESOURCES = "REQUEST_RESOURCES"
FREE_RESOURCES = "FREE_RESOURCES"
REGISTER_CORE_ELEMENT = "REGISTER_CORE_ELEMENT"
PROCESS_HTTP_TASK = "PROCESS_HTTP_TASK"
PROCESS_TASK = "PROCESS_TASK"
SET_PIPES = "SET_PIPES"
READ_PIPES = "READ_PIPES"
SET_WALL_CLOCK = "SET_WALL_CLOCK"

if __debug__:
    import logging

    link_logger = logging.getLogger(__name__)


def shutdown_handler(signal: int, frame: typing.Any) -> None:
    """Shutdown handler.

    Do not remove the parameters.

    :param signal: shutdown signal.
    :param frame: Frame.
    :return: None
    """
    if LINK_PROCESS.is_alive():
        LINK_PROCESS.terminate()


def c_extension_link(
    in_queue: Queue,
    out_queue: Queue,
    redirect_std: bool,
    out_file_name: str,
    err_file_name: str,
) -> None:
    """Establish C extension within an external process and communicates through queues.

    :param in_queue: Queue to receive messages.
    :param out_queue: Queue to send messages.
    :param redirect_std: Decide whether to store the stdout and stderr into
                         files or not.
    :param out_file_name: File where to store the stdout (only required if
                          redirect_std is True).
    :param err_file_name: File where to store the stderr (only required if
                          redirect_std is True).
    :return: None
    """
    # Import C extension within the external process
    import compss

    with ipython_std_redirector(
        out_file_name, err_file_name
    ) if redirect_std else not_std_redirector():
        alive = True
        while alive:
            message = in_queue.get()
            command = message[0]
            parameters = []  # type: list
            if len(message) > 0:
                parameters = list(message[1:])
            if command == START:
                compss.start_runtime()
            elif command == SET_DEBUG:
                compss.set_debug(*parameters)
            elif command == STOP:
                compss.stop_runtime(*parameters)
                alive = False
            elif command == CANCEL_TASKS:
                compss.cancel_application_tasks(*parameters)
            elif command == ACCESSED_FILE:
                accessed = compss.accessed_file(*parameters)
                out_queue.put(accessed)
            elif command == OPEN_FILE:
                compss_name = compss.open_file(*parameters)
                out_queue.put(compss_name)
            elif command == CLOSE_FILE:
                compss.close_file(*parameters)
            elif command == DELETE_FILE:
                result = compss.delete_file(*parameters)
                out_queue.put(result)
            elif command == GET_FILE:
                compss.get_file(*parameters)
            elif command == GET_DIRECTORY:
                compss.get_directory(*parameters)
            elif command == BARRIER:
                compss.barrier(*parameters)
            elif command == BARRIER_GROUP:
                exception_message = compss.barrier_group(*parameters)
                out_queue.put(exception_message)
            elif command == OPEN_TASK_GROUP:
                compss.open_task_group(*parameters)
            elif command == CLOSE_TASK_GROUP:
                compss.close_task_group(*parameters)
            elif command == GET_LOGGING_PATH:
                log_path = compss.get_logging_path()
                out_queue.put(log_path)
            elif command == GET_NUMBER_OF_RESOURCES:
                num_resources = compss.get_number_of_resources(*parameters)
                out_queue.put(num_resources)
            elif command == REQUEST_RESOURCES:
                compss.request_resources(*parameters)
            elif command == FREE_RESOURCES:
                compss.free_resources(*parameters)
            elif command == REGISTER_CORE_ELEMENT:
                compss.register_core_element(*parameters)
            elif command == PROCESS_TASK:
                compss.process_task(*parameters)
            elif command == PROCESS_HTTP_TASK:
                compss.process_http_task(*parameters)
            elif command == SET_PIPES:
                compss.set_pipes(*parameters)
            elif command == READ_PIPES:
                compss.read_pipes(*parameters)
            elif command == SET_WALL_CLOCK:
                compss.set_wall_clock(*parameters)
            else:
                raise PyCOMPSsException("Unknown link command")


def establish_link(logger: typing.Any = None) -> typing.Any:
    """Load the compss C extension within the same process.

    Does not implement support for stdout and stderr redirecting as the
    establish_interactive_link.

    :param logger: Use this logger instead of the module logger.
    :return: The COMPSs C extension link.
    """
    if __debug__:
        message = "Loading compss extension"
        if logger:
            logger.debug(message)
        else:
            link_logger.debug(message)
    import compss

    if __debug__:
        message = "Loaded compss extension"
        if logger:
            logger.debug(message)
        else:
            link_logger.debug(message)
    return compss


def establish_interactive_link(
    logger: typing.Any = None, redirect_std: bool = False
) -> typing.Tuple[typing.Any, str, str]:
    """Start a new process which will be in charge of communicating with the C-extension.

    It will return stdout file name and stderr file name as None if
    redirect_std is False. Otherwise, returns the names which are the
    current process pid followed by the out/err extension.

    :param logger: Use this logger instead of the module logger.
    :param redirect_std: Decide whether to store the stdout and stderr into
                         files or not.
    :return: The COMPSs C extension link, stdout file name and stderr file
             name.
    """
    global LINK_PROCESS
    global IN_QUEUE
    global OUT_QUEUE
    global RELOAD

    out_file_name = ""
    err_file_name = ""
    if redirect_std:
        pid = str(os.getpid())
        out_file_name = "compss-" + pid + ".out"
        err_file_name = "compss-" + pid + ".err"

    if RELOAD:
        IN_QUEUE = new_queue()
        OUT_QUEUE = new_queue()
        RELOAD = False

    if __debug__:
        message = "Starting new process linking with the C-extension"
        if logger:
            logger.debug(message)
        else:
            link_logger.debug(message)

    LINK_PROCESS = create_process(
        target=c_extension_link,
        args=(IN_QUEUE, OUT_QUEUE, redirect_std, out_file_name, err_file_name),
    )
    LINK_PROCESS.start()

    if __debug__:
        message = "Established link with C-extension"
        if logger:
            logger.debug(message)
        else:
            link_logger.debug(message)

    compss_link = _COMPSs  # object that mimics compss library
    return compss_link, out_file_name, err_file_name


def wait_for_interactive_link() -> None:
    """Wait for interactive link finalization.

    :return: None
    """
    global RELOAD

    # Wait for the link to finish
    IN_QUEUE.close()
    OUT_QUEUE.close()
    IN_QUEUE.join_thread()
    OUT_QUEUE.join_thread()
    LINK_PROCESS.join()

    # Notify that if terminated, the queues must be reopened to start again.
    RELOAD = True


def terminate_interactive_link() -> None:
    """Terminate the compss C extension process.

    :return: None
    """
    LINK_PROCESS.terminate()


class _COMPSs(object):
    """Class that mimics the compss extension library.

    Each function puts into the queue a list or set composed by:
         (COMMAND_TAG, parameter1, parameter2, ...)

    IMPORTANT: methods must be exactly the same.
    """

    @staticmethod
    def start_runtime() -> None:
        """Call to start_runtime.

        :return: None
        """
        IN_QUEUE.put([START])

    @staticmethod
    def set_debug(mode: bool) -> None:
        """Call to set_debug.

        :param mode: Debug mode ( True | False ).
        :return: None.
        """
        IN_QUEUE.put((SET_DEBUG, mode))

    @staticmethod
    def stop_runtime(code: int) -> None:
        """Call to stop_runtime.

        :param code: Stopping code.
        :return: None.
        """
        IN_QUEUE.put([STOP, code])
        wait_for_interactive_link()
        # terminate_interactive_link()

    @staticmethod
    def cancel_application_tasks(app_id: int, value: int) -> None:
        """Call to cancel_application_tasks.

        :param app_id: Application identifier.
        :param value:  Task identifier.
        :return: None.
        """
        IN_QUEUE.put((CANCEL_TASKS, app_id, value))

    @staticmethod
    def accessed_file(app_id: int, file_name: str) -> bool:
        """Call to accessed_file.

        :param app_id: Application identifier.
        :param file_name: File name to check if accessed.
        :return: If the file has been accessed.
        """
        IN_QUEUE.put((ACCESSED_FILE, app_id, file_name))
        accessed = OUT_QUEUE.get(block=True)
        return accessed

    @staticmethod
    def open_file(app_id: int, file_name: str, mode: int) -> str:
        """Call to open_file.

        Synchronizes if necessary.

        :param app_id: Application identifier.
        :param file_name: File name to open.
        :param mode: Open mode.
        :return: The real file name.
        """
        IN_QUEUE.put((OPEN_FILE, app_id, file_name, mode))
        compss_name = OUT_QUEUE.get(block=True)
        return compss_name

    @staticmethod
    def close_file(app_id: int, file_name: str, mode: int) -> None:
        """Call to close_file.

        :param app_id: Application identifier.
        :param file_name: File name reference to close.
        :param mode: Close mode.
        :return: None.
        """
        IN_QUEUE.put((CLOSE_FILE, app_id, file_name, mode))

    @staticmethod
    def delete_file(app_id: int, file_name: str, mode: bool) -> bool:
        """Call to delete_file.

        :param app_id: Application identifier.
        :param file_name: File name reference to delete.
        :param mode: Delete mode.
        :return: The deletion result.
        """
        IN_QUEUE.put((DELETE_FILE, app_id, file_name, mode))
        result = OUT_QUEUE.get(block=True)
        if result is None:
            return False
        else:
            return result

    @staticmethod
    def get_file(app_id: int, file_name: str) -> None:
        """Call to (synchronize file) get_file.

        :param app_id: Application identifier.
        :param file_name: File name reference to get.
        :return: None.
        """
        IN_QUEUE.put((GET_FILE, app_id, file_name))

    @staticmethod
    def get_directory(app_id: int, directory_name: str) -> None:
        """Call to (synchronize directory) get_directory.

        :param app_id: Application identifier.
        :param directory_name: Directory name reference to get.
        :return: None.
        """
        IN_QUEUE.put((GET_DIRECTORY, app_id, directory_name))

    @staticmethod
    def barrier(app_id: int, no_more_tasks: bool) -> None:
        """Call to barrier.

        :param app_id: Application identifier.
        :param no_more_tasks: No more tasks boolean.
        :return: None
        """
        IN_QUEUE.put((BARRIER, app_id, no_more_tasks))

    @staticmethod
    def barrier_group(app_id: int, group_name: str) -> str:
        """Call to barrier_group.

        :param app_id: Application identifier.
        :param group_name: Group name.
        :return: Exception message.
        """
        IN_QUEUE.put((BARRIER_GROUP, app_id, group_name))
        exception_message = OUT_QUEUE.get(block=True)
        return exception_message

    @staticmethod
    def open_task_group(group_name: str, implicit_barrier: bool, app_id: int) -> None:
        """Call to open_task_group.

        :param group_name: Group name.
        :param implicit_barrier: Implicit barrier boolean.
        :param app_id: Application identifier.
        :return: None.
        """
        IN_QUEUE.put((OPEN_TASK_GROUP, group_name, implicit_barrier, app_id))

    @staticmethod
    def close_task_group(group_name: str, app_id: int) -> None:
        """Call to close_task_group.

        :param group_name: Group name.
        :param app_id: Application identifier.
        :return: None.
        """
        IN_QUEUE.put((CLOSE_TASK_GROUP, group_name, app_id))

    @staticmethod
    def get_logging_path() -> str:
        """Call to get_logging_path.

        :return: The COMPSs log path.
        """
        IN_QUEUE.put([GET_LOGGING_PATH])
        log_path = OUT_QUEUE.get(block=True)
        return log_path

    @staticmethod
    def get_number_of_resources(app_id: int) -> int:
        """Call to number_of_resources.

        :param app_id: Application identifier.
        :return: Number of resources.
        """
        IN_QUEUE.put((GET_NUMBER_OF_RESOURCES, app_id))
        num_resources = OUT_QUEUE.get(block=True)
        return num_resources

    @staticmethod
    def request_resources(app_id: int, num_resources: int, group_name: str) -> None:
        """Call to request_resources.

        :param app_id: Application identifier.
        :param num_resources: Number of resources.
        :param group_name: Group name.
        :return: None.
        """
        IN_QUEUE.put((REQUEST_RESOURCES, app_id, num_resources, group_name))

    @staticmethod
    def free_resources(app_id: int, num_resources: int, group_name: str) -> None:
        """Call to free_resources.

        :param app_id: Application identifier.
        :param num_resources: Number of resources.
        :param group_name: Group name.
        :return: None.
        """
        IN_QUEUE.put((FREE_RESOURCES, app_id, num_resources, group_name))

    @staticmethod
    def set_wall_clock(app_id: int, wcl: int) -> None:
        """Call to set_wall_clock.

        :param app_id: Application identifier.
        :param wcl: Wall Clock limit in seconds.
        :return: None.
        """
        IN_QUEUE.put((SET_WALL_CLOCK, app_id, wcl))

    @staticmethod
    def register_core_element(
        ce_signature: str,
        impl_signature: typing.Optional[str],
        impl_constraints: typing.Optional[str],
        impl_type: typing.Optional[str],
        impl_io: str,
        prolog: typing.List[str],
        epilog: typing.List[str],
        impl_type_args: typing.List[str],
    ) -> None:
        """Call to register_core_element.

        :param ce_signature: Core element signature.
        :param impl_signature: Implementation signature.
        :param impl_constraints: Implementation constraints.
        :param impl_type: Implementation type.
        :param impl_io: Implementation IO.
        :param prolog: [binary, params, fail_by_exit_value] of the prolog.
        :param epilog: [binary, params, fail_by_exit_value] of the epilog.
        :param impl_type_args: Implementation type arguments.
        :return: None.
        """
        IN_QUEUE.put(
            (
                REGISTER_CORE_ELEMENT,
                ce_signature,
                impl_signature,
                impl_constraints,
                impl_type,
                impl_io,
                prolog,
                epilog,
                impl_type_args,
            )
        )

    @staticmethod
    def process_task(
        app_id: int,
        signature: str,
        on_failure: str,
        time_out: int,
        has_priority: bool,
        num_nodes: int,
        reduction: bool,
        chunk_size: int,
        replicated: bool,
        distributed: bool,
        has_target: bool,
        num_returns: int,
        values: list,
        names: list,
        compss_types: list,
        compss_directions: list,
        compss_streams: list,
        compss_prefixes: list,
        content_types: list,
        weights: list,
        keep_renames: list,
    ) -> None:
        """Call to process_task.

        :param app_id: Application identifier.
        :param signature: Task signature.
        :param on_failure: On failure action.
        :param time_out: Task time out.
        :param has_priority: Boolean has priority.
        :param num_nodes: Number of nodes.
        :param reduction: Boolean indicating if the task is of type reduce.
        :param chunk_size: Size of chunks for executing the reduce operation.
        :param replicated: Boolean is replicated.
        :param distributed: Boolean is distributed.
        :param has_target: Boolean has target.
        :param num_returns: Number of returns.
        :param values: Values.
        :param names: Names.
        :param compss_types: COMPSs types.
        :param compss_directions: COMPSs directions.
        :param compss_streams: COMPSs streams.
        :param compss_prefixes: COMPSs prefixes.
        :param content_types: COMPSs types.
        :param weights: Parameter weights.
        :param keep_renames: Boolean keep renames.
        :return: None.
        """
        IN_QUEUE.put(
            (
                PROCESS_TASK,
                app_id,
                signature,
                on_failure,
                time_out,
                has_priority,
                num_nodes,
                reduction,
                chunk_size,
                replicated,
                distributed,
                has_target,
                num_returns,
                values,
                names,
                compss_types,
                compss_directions,
                compss_streams,
                compss_prefixes,
                content_types,
                weights,
                keep_renames,
            )
        )

    @staticmethod
    def process_http_task(
        app_id: int,
        signature: str,
        on_failure: str,
        time_out: int,
        has_priority: bool,
        num_nodes: int,
        reduction: bool,
        chunk_size: int,
        replicated: bool,
        distributed: bool,
        has_target: bool,
        num_returns: int,
        values: list,
        names: list,
        compss_types: list,
        compss_directions: list,
        compss_streams: list,
        compss_prefixes: list,
        content_types: list,
        weights: list,
        keep_renames: list,
    ) -> None:
        """Call to process_http_task.

        :param app_id: Application identifier.
        :param signature: Task signature.
        :param on_failure: On failure action.
        :param time_out: Task time out.
        :param has_priority: Boolean has priority.
        :param num_nodes: Number of nodes.
        :param reduction: Boolean indicating if the task is of type reduce.
        :param chunk_size: Size of chunks for executing the reduce operation.
        :param replicated: Boolean is replicated.
        :param distributed: Boolean is distributed.
        :param has_target: Boolean has target.
        :param num_returns: Number of returns.
        :param values: Values.
        :param names: Names.
        :param compss_types: COMPSs types.
        :param compss_directions: COMPSs directions.
        :param compss_streams: COMPSs streams.
        :param compss_prefixes: COMPSs prefixes.
        :param content_types: COMPSs types.
        :param weights: Parameter weights.
        :param keep_renames: Boolean keep renames.
        :return: None.
        """
        IN_QUEUE.put(
            (
                PROCESS_HTTP_TASK,
                app_id,
                signature,
                on_failure,
                time_out,
                has_priority,
                num_nodes,
                reduction,
                chunk_size,
                replicated,
                distributed,
                has_target,
                num_returns,
                values,
                names,
                compss_types,
                compss_directions,
                compss_streams,
                compss_prefixes,
                content_types,
                weights,
                keep_renames,
            )
        )

    @staticmethod
    def set_pipes(pipe_in: str, pipe_out: str) -> None:
        """Set nesting pipes.

        :param pipe_in: Input pipe.
        :param pipe_out: Output pipe.
        :return: None.
        """
        IN_QUEUE.put((SET_PIPES, pipe_in, pipe_out))

    @staticmethod
    def read_pipes() -> str:
        """Call to read_pipes.

        :return: The command read from the pipe.
        """
        IN_QUEUE.put([READ_PIPES])
        command = OUT_QUEUE.get(block=True)
        return command
