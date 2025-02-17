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
PyCOMPSs Binding - Management - Link.

This file contains the functions to link with the binding-commons.
In particular, manages a separate process which handles the compss
extension, so that the process can be removed when shutting off
and restarted (interactive usage of PyCOMPSs - ipython and jupyter).
"""

import os
import logging
import signal

from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.process.manager import Queue
from pycompss.util.process.manager import create_process
from pycompss.util.process.manager import new_process
from pycompss.util.process.manager import new_queue
from pycompss.util.std.redirects import ipython_std_redirector
from pycompss.util.std.redirects import not_std_redirector
from pycompss.util.typing_helper import typing
from pycompss.runtime.management.link.messages import LINK_MESSAGES

if __debug__:
    link_logger = logging.getLogger(__name__)


def shutdown_handler(
    signal: int,  # pylint: disable=unused-argument, redefined-outer-name
    frame: typing.Any,  # pylint: disable=unused-argument
) -> None:
    """Shutdown handler.

    Do not remove the parameters.

    :param signal: shutdown signal.
    :param frame: Frame.
    :return: None
    """
    if EXTERNAL_LINK.link_process.is_alive():
        EXTERNAL_LINK.terminate_interactive_link()


def establish_interactive_link(
    logger: typing.Optional[logging.Logger] = None, redirect_std: bool = False
) -> typing.Tuple[typing.Any, str, str]:
    """Start a process that communicates with the C-extension.

    Starts a new process which will be in charge of communicating with
    the C-extension.

    It will return stdout file name and stderr file name as None if
    redirect_std is False. Otherwise, returns the names which are the
    current process pid followed by the out/err extension.

    :param logger: Use this logger instead of the module logger.
    :param redirect_std: Decide whether to store the stdout and stderr into
                         files or not.
    :return: The COMPSs C extension link, stdout file name and stderr file
             name.
    """
    return EXTERNAL_LINK.establish_interactive_link(logger, redirect_std)


class ExternalLink:
    """External link class."""

    __slots__ = ["link_process", "in_queue", "out_queue", "reload"]

    def __init__(self) -> None:
        """Instantiate a new ExternalLink class."""
        self.link_process = new_process()
        self.in_queue = new_queue()
        self.out_queue = new_queue()
        self.reload = False

    def establish_interactive_link(
        self,
        logger: typing.Optional[logging.Logger] = None,
        redirect_std: bool = False,
    ) -> typing.Tuple[typing.Any, str, str]:
        """Start a process that communicates with the C-extension.

        Start a new process which will be in charge of communicating with
        the C-extension.

        It will return stdout file name and stderr file name as None if
        redirect_std is False. Otherwise, returns the names which are the
        current process pid followed by the out/err extension.

        :param logger: Use this logger instead of the module logger.
        :param redirect_std: Decide whether to store the stdout and stderr into
                             files or not.
        :return: The COMPSs C extension link, stdout file name and stderr file
                 name.
        """
        out_file_name = ""
        err_file_name = ""
        if redirect_std:
            pid = str(os.getpid())
            out_file_name = "compss-" + pid + ".out"
            err_file_name = "compss-" + pid + ".err"

        if self.reload:
            self.in_queue = new_queue()
            self.out_queue = new_queue()
            self.reload = False

        if __debug__:
            message = "Starting new process linking with the C-extension"
            if logger:
                logger.debug(message)
            else:
                link_logger.debug(message)

        self.link_process = create_process(
            target=c_extension_link,
            args=(
                self.in_queue,
                self.out_queue,
                redirect_std,
                out_file_name,
                err_file_name,
            ),
        )
        signal.signal(signal.SIGTERM, shutdown_handler)
        self.link_process.start()

        if __debug__:
            message = "Established link with C-extension"
            if logger:
                logger.debug(message)
            else:
                link_logger.debug(message)

        # Create object that mimics compss library
        compss_link = _COMPSs(self.in_queue, self.out_queue)
        return compss_link, out_file_name, err_file_name

    def wait_for_interactive_link(self) -> None:
        """Wait for interactive link finalization.

        :return: None
        """
        # Wait for the link to finish
        self.in_queue.close()
        self.out_queue.close()
        self.in_queue.join_thread()
        self.out_queue.join_thread()
        self.link_process.join()
        # Notify that if terminated, the queues must be reopened to start again
        self.reload = True

    def terminate_interactive_link(self) -> None:
        """Terminate the compss C extension process.

        :return: None
        """
        self.link_process.terminate()


# ############################################################# #
# #################### EXTERNAL LINK CLASS #################### #
# ############################################################# #

EXTERNAL_LINK = ExternalLink()


def c_extension_link(  # pylint: disable=too-many-locals
    in_queue: Queue,
    out_queue: Queue,
    redirect_std: bool,
    out_file_name: str,
    err_file_name: str,
) -> None:
    """Establish C extension within external process.

    Establish C extension within an external process and communicates
    through queues.

    :param lock: Global lock for
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
    import compss  # pylint: disable=import-outside-toplevel

    command_done = LINK_MESSAGES.command_done

    with (
        ipython_std_redirector(out_file_name, err_file_name)
        if redirect_std
        else not_std_redirector()
    ):
        alive = True
        while alive:
            message = list(in_queue.get())
            command = str(message[0])
            parameters = []  # type: list
            if len(message) > 0:
                parameters = list(message[1:])
            if command == LINK_MESSAGES.start:
                compss.start_runtime()
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.set_debug:
                compss.set_debug(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.stop:
                compss.stop_runtime(*parameters)
                alive = False
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.cancel_tasks:
                compss.cancel_application_tasks(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.accessed_file:
                accessed = compss.accessed_file(*parameters)
                out_queue.put(accessed)
            elif command == LINK_MESSAGES.open_file:
                compss_name = compss.open_file(*parameters)
                out_queue.put(compss_name)
            elif command == LINK_MESSAGES.close_file:
                compss.close_file(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.delete_file:
                result = compss.delete_file(*parameters)
                out_queue.put(result)
            elif command == LINK_MESSAGES.get_file:
                compss.get_file(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.get_directory:
                compss.get_directory(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.barrier:
                compss.barrier(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.barrier_group:
                exception_message = compss.barrier_group(*parameters)
                out_queue.put(exception_message)
            elif command == LINK_MESSAGES.open_task_group:
                compss.open_task_group(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.close_task_group:
                compss.close_task_group(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.cancel_task_group:
                compss.cancel_task_group(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.snapshot:
                compss.snapshot(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.get_logging_path:
                log_path = compss.get_logging_path()
                out_queue.put(log_path)
            elif command == LINK_MESSAGES.get_master_working_path:
                master_working_path = compss.get_master_working_path()
                out_queue.put(master_working_path)
            elif command == LINK_MESSAGES.get_number_of_resources:
                num_resources = compss.get_number_of_resources(*parameters)
                out_queue.put(num_resources)
            elif command == LINK_MESSAGES.request_resources:
                compss.request_resources(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.free_resources:
                compss.free_resources(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.register_core_element:
                compss.register_core_element(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.process_task:
                compss.process_task(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.process_http_task:
                compss.process_http_task(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.set_pipes:
                compss.set_pipes(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.read_pipes:
                compss.read_pipes(*parameters)
                out_queue.put(command_done)
            elif command == LINK_MESSAGES.set_wall_clock:
                compss.set_wall_clock(*parameters)
                out_queue.put(command_done)
            else:
                raise PyCOMPSsException("Unknown link command")


class _COMPSs:
    """Class that mimics the compss extension library.

    Each function puts into the queue a list or set composed by:
         (COMMAND_TAG, parameter1, parameter2, ...)

    IMPORTANT: methods must be exactly the same.
    """

    __slots__ = ["in_queue", "out_queue"]

    def __init__(self, in_queue: Queue, out_queue: Queue) -> None:
        """Instantiate a new _COMPSs object."""
        self.in_queue = in_queue
        self.out_queue = out_queue

    def start_runtime(self) -> None:
        """Call to start_runtime.

        :return: None
        """
        self.in_queue.put([LINK_MESSAGES.start])
        _ = self.out_queue.get(block=True)

    def set_debug(self, mode: bool) -> None:
        """Call to set_debug.

        :param mode: Debug mode ( True | False ).
        :return: None.
        """
        self.in_queue.put((LINK_MESSAGES.set_debug, mode))
        _ = self.out_queue.get(block=True)

    def stop_runtime(self, code: int) -> None:
        """Call to stop_runtime.

        :param code: Stopping code.
        :return: None.
        """
        self.in_queue.put((LINK_MESSAGES.stop, code))
        _ = self.out_queue.get(block=True)
        EXTERNAL_LINK.wait_for_interactive_link()
        # EXTERNAL_LINK.terminate_interactive_link()

    def cancel_application_tasks(self, app_id: int, value: int) -> None:
        """Call to cancel_application_tasks.

        :param app_id: Application identifier.
        :param value:  Task identifier.
        :return: None.
        """
        self.in_queue.put((LINK_MESSAGES.cancel_tasks, app_id, value))
        _ = self.out_queue.get(block=True)

    def accessed_file(self, app_id: int, file_name: str) -> bool:
        """Call to accessed_file.

        :param app_id: Application identifier.
        :param file_name: File name to check if accessed.
        :return: If the file has been accessed.
        """
        self.in_queue.put((LINK_MESSAGES.accessed_file, app_id, file_name))
        accessed = self.out_queue.get(block=True)
        return accessed

    def open_file(self, app_id: int, file_name: str, mode: int) -> str:
        """Call to open_file.

        Synchronizes if necessary.

        :param app_id: Application identifier.
        :param file_name: File name to open.
        :param mode: Open mode.
        :return: The real file name.
        """
        self.in_queue.put((LINK_MESSAGES.open_file, app_id, file_name, mode))
        compss_name = self.out_queue.get(block=True)
        return compss_name

    def close_file(self, app_id: int, file_name: str, mode: int) -> None:
        """Call to close_file.

        :param app_id: Application identifier.
        :param file_name: File name reference to close.
        :param mode: Close mode.
        :return: None.
        """
        self.in_queue.put((LINK_MESSAGES.close_file, app_id, file_name, mode))
        _ = self.out_queue.get(block=True)

    def delete_file(
        self, app_id: int, file_name: str, mode: bool, application_delete=True
    ) -> bool:
        """Call to delete_file.

        :param app_id: Application identifier.
        :param file_name: File name reference to delete.
        :param mode: Delete mode.
        :param application_delete: Application delete.
        :return: The deletion result.
        """
        self.in_queue.put(
            (
                LINK_MESSAGES.delete_file,
                app_id,
                file_name,
                mode,
                application_delete,
            )
        )
        result = self.out_queue.get(block=True)
        if result is None:
            return False
        return result

    def get_file(self, app_id: int, file_name: str) -> None:
        """Call to (synchronize file) get_file.

        :param app_id: Application identifier.
        :param file_name: File name reference to get.
        :return: None.
        """
        self.in_queue.put((LINK_MESSAGES.get_file, app_id, file_name))
        _ = self.out_queue.get(block=True)

    def get_directory(self, app_id: int, directory_name: str) -> None:
        """Call to (synchronize directory) get_directory.

        :param app_id: Application identifier.
        :param directory_name: Directory name reference to get.
        :return: None.
        """
        self.in_queue.put(
            (LINK_MESSAGES.get_directory, app_id, directory_name)
        )
        _ = self.out_queue.get(block=True)

    def barrier(self, app_id: int, no_more_tasks: bool) -> None:
        """Call to barrier.

        :param app_id: Application identifier.
        :param no_more_tasks: No more tasks boolean.
        :return: None
        """
        self.in_queue.put((LINK_MESSAGES.barrier, app_id, no_more_tasks))
        _ = self.out_queue.get(block=True)

    def barrier_group(
        self, app_id: int, group_name: str
    ) -> typing.Optional[str]:
        """Call to barrier_group.

        :param app_id: Application identifier.
        :param group_name: Group name.
        :return: Exception message.
        """
        self.in_queue.put((LINK_MESSAGES.barrier_group, app_id, group_name))
        exception_message = self.out_queue.get(block=True)
        return exception_message

    def open_task_group(
        self, group_name: str, implicit_barrier: bool, app_id: int
    ) -> None:
        """Call to open_task_group.

        :param group_name: Group name.
        :param implicit_barrier: Implicit barrier boolean.
        :param app_id: Application identifier.
        :return: None.
        """
        self.in_queue.put(
            (
                LINK_MESSAGES.open_task_group,
                group_name,
                implicit_barrier,
                app_id,
            )
        )
        _ = self.out_queue.get(block=True)

    def close_task_group(self, group_name: str, app_id: int) -> None:
        """Call to close_task_group.

        :param group_name: Group name.
        :param app_id: Application identifier.
        :return: None.
        """
        self.in_queue.put((LINK_MESSAGES.close_task_group, group_name, app_id))
        _ = self.out_queue.get(block=True)

    def cancel_task_group(self, group_name: str, app_id: int) -> None:
        """Call to cancel_task_group.

        :param group_name: Group name.
        :param app_id: Application identifier.
        :return: None.
        """
        self.in_queue.put(
            (LINK_MESSAGES.cancel_task_group, group_name, app_id)
        )
        _ = self.out_queue.get(block=True)

    def snapshot(self, code: int) -> None:
        """Call to snapshot.

        :param code: Stopping code.
        :return: None.
        """
        self.in_queue.put((LINK_MESSAGES.snapshot, code))
        _ = self.out_queue.get(block=True)

    def get_logging_path(self) -> str:
        """Call to get_logging_path.

        :return: The COMPSs log path.
        """
        self.in_queue.put([LINK_MESSAGES.get_logging_path])
        log_path = self.out_queue.get(block=True)
        return log_path

    def get_master_working_path(self) -> str:
        """Call to master_working_path.

        :return: The COMPSs master working path.
        """
        self.in_queue.put([LINK_MESSAGES.get_master_working_path])
        master_working_path = self.out_queue.get(block=True)
        return master_working_path

    def get_number_of_resources(self, app_id: int) -> int:
        """Call to number_of_resources.

        :param app_id: Application identifier.
        :return: Number of resources.
        """
        self.in_queue.put((LINK_MESSAGES.get_number_of_resources, app_id))
        num_resources = self.out_queue.get(block=True)
        return num_resources

    def request_resources(
        self, app_id: int, num_resources: int, group_name: str
    ) -> None:
        """Call to request_resources.

        :param app_id: Application identifier.
        :param num_resources: Number of resources.
        :param group_name: Group name.
        :return: None.
        """
        self.in_queue.put(
            (
                LINK_MESSAGES.request_resources,
                app_id,
                num_resources,
                group_name,
            )
        )
        _ = self.out_queue.get(block=True)

    def free_resources(
        self, app_id: int, num_resources: int, group_name: str
    ) -> None:
        """Call to free_resources.

        :param app_id: Application identifier.
        :param num_resources: Number of resources.
        :param group_name: Group name.
        :return: None.
        """
        self.in_queue.put(
            (LINK_MESSAGES.free_resources, app_id, num_resources, group_name)
        )
        _ = self.out_queue.get(block=True)

    def set_wall_clock(self, app_id: int, wcl: int) -> None:
        """Call to set_wall_clock.

        :param app_id: Application identifier.
        :param wcl: Wall Clock limit in seconds.
        :return: None.
        """
        self.in_queue.put((LINK_MESSAGES.set_wall_clock, app_id, wcl))
        _ = self.out_queue.get(block=True)

    def register_core_element(
        self,
        ce_signature: str,
        impl_signature: typing.Optional[str],
        impl_constraints: typing.Optional[str],
        impl_type: typing.Optional[str],
        impl_local: str,
        impl_io: str,
        impl_prolog: typing.List[str],
        impl_epilog: typing.List[str],
        impl_container: typing.List[str],
        impl_type_args: typing.List[str],
    ) -> None:
        """Call to register_core_element.

        :param ce_signature: Core element signature.
        :param impl_signature: Implementation signature.
        :param impl_constraints: Implementation constraints.
        :param impl_type: Implementation type.
        :param impl_local: Implementation Local.
        :param impl_io: Implementation IO.
        :param impl_prolog: [binary, params, fail_by_exit_value] of the prolog.
        :param impl_epilog: [binary, params, fail_by_exit_value] of the epilog.
        :param impl_container: [engine, image, options] of the container.
        :param impl_type_args: Implementation type arguments.
        :return: None.
        """
        self.in_queue.put(
            (
                LINK_MESSAGES.register_core_element,
                ce_signature,
                impl_signature,
                impl_constraints,
                impl_type,
                impl_local,
                impl_io,
                impl_prolog,
                impl_epilog,
                impl_container,
                impl_type_args,
            )
        )
        _ = self.out_queue.get(block=True)

    def process_task(
        self,
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
        self.in_queue.put(
            (
                LINK_MESSAGES.process_task,
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
        _ = self.out_queue.get(block=True)

    def process_http_task(
        self,
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
        self.in_queue.put(
            (
                LINK_MESSAGES.process_http_task,
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
        _ = self.out_queue.get(block=True)

    def set_pipes(self, pipe_in: str, pipe_out: str) -> None:
        """Set nesting pipes.

        :param pipe_in: Input pipe.
        :param pipe_out: Output pipe.
        :return: None.
        """
        self.in_queue.put((LINK_MESSAGES.set_pipes, pipe_in, pipe_out))
        _ = self.out_queue.get(block=True)

    def read_pipes(self) -> str:
        """Call to read_pipes.

        :return: The command read from the pipe.
        """
        self.in_queue.put([LINK_MESSAGES.read_pipes])
        command = self.out_queue.get(block=True)
        return command
