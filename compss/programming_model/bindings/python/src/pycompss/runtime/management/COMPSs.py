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
PyCOMPSs Binding - Management - COMPSs Runtime.

This file contains the COMPSs runtime connection.
Loads the external C module.
"""

from pycompss.runtime.management.link import establish_interactive_link
from pycompss.runtime.management.link import establish_link
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.typing_helper import typing

if __debug__:
    import logging

    LOGGER = logging.getLogger(__name__)

# C module extension for the communication with the runtime
# See ext/compssmodule.cc
# Keep the COMPSs runtime link in this module so that any module can access
# it through the module methods.
COMPSS = None  # type: typing.Any
# Files where the std may be redirected with interactive
STDOUT = ""
STDERR = ""


######################################################
#             INTERNAL FUNCTIONS                     #
######################################################


def load_runtime(external_process: bool = False, _logger: typing.Any = None) -> None:
    """Load the external C extension module.

    :param external_process: Loads the runtime in an external process if true.
                             Within this python process if false.
    :param _logger: Use this logger instead of the module LOGGER.
    :return: None
    """
    global COMPSS
    global STDOUT
    global STDERR

    if external_process:
        # For interactive python environments
        COMPSS, STDOUT, STDERR = establish_interactive_link(_logger, True)
    else:
        # Normal python environments
        COMPSS = establish_link(_logger)


def is_redirected() -> bool:
    """Check if the stdout and stderr are being redirected.

    :return: If stdout/stderr are being redirected.
    """
    if STDOUT == "" and STDERR == "":
        return False
    if STDOUT != "" and STDERR != "":
        return True
    message = "Inconsistent status of STDOUT and STDERR"
    raise PyCOMPSsException(message)


def get_redirection_file_names() -> typing.Tuple[str, str]:
    """Retrieve the stdout and stderr file names.

    :return: The stdout and stderr file names.
    """
    if is_redirected():
        return STDOUT, STDERR
    message = "The runtime STDOUT and STDERR are not being redirected."
    raise PyCOMPSsException(message)


######################################################
#          COMPSs API EXPOSED FUNCTIONS              #
######################################################


def start_runtime() -> None:
    """Call to start_runtime.

    :return: None
    """
    COMPSS.start_runtime()  # noqa


def set_debug(mode: bool) -> None:
    """Call to set_debug.

    :param mode: Debug mode ( True | False ).
    :return: None.
    """
    COMPSS.set_debug(mode)  # noqa


def stop_runtime(code: int) -> None:
    """Call to stop_runtime.

    :param code: Stopping code.
    :return: None.
    """
    COMPSS.stop_runtime(code)  # noqa


def cancel_application_tasks(app_id: int, value: int) -> None:
    """Call to cancel_application_tasks.

    :param app_id: Application identifier.
    :param value:  Task identifier.
    :return: None.
    """
    COMPSS.cancel_application_tasks(app_id, value)  # noqa


def accessed_file(app_id: int, file_name: str) -> bool:
    """Call to accessed_file.

    :param app_id: Application identifier.
    :param file_name: File name to check if accessed.
    :return: If the file has been accessed.
    """
    return COMPSS.accessed_file(app_id, file_name)  # noqa


def open_file(app_id: int, file_name: str, mode: int) -> str:
    """Call to open_file.

    Synchronizes if necessary.

    :param app_id: Application identifier.
    :param file_name: File name reference to open.
    :param mode: Open mode.
    :return: The real file name.
    """
    return COMPSS.open_file(app_id, file_name, mode)  # noqa


def close_file(app_id: int, file_name: str, mode: int) -> None:
    """Call to close_file.

    :param app_id: Application identifier.
    :param file_name: File name reference to close.
    :param mode: Close mode.
    :return: None
    """
    COMPSS.close_file(app_id, file_name, mode)  # noqa


def delete_file(app_id: int, file_name: str, mode: bool) -> bool:
    """Call to delete_file.

    :param app_id: Application identifier.
    :param file_name: File name reference to delete.
    :param mode: Delete mode.
    :return: The deletion result.
    """
    result = COMPSS.delete_file(app_id, file_name, mode)  # noqa
    if result is None:
        return False
    return result


def get_file(app_id: int, file_name: str) -> None:
    """Call to (synchronize) get_file.

    :param app_id: Application identifier.
    :param file_name: File name reference to get.
    :return: None.
    """
    COMPSS.get_file(app_id, file_name)  # noqa


def get_directory(app_id: int, directory_name: str) -> None:
    """Call to (synchronize) get_directory.

    :param app_id: Application identifier.
    :param directory_name: Directory name reference to get.
    :return: None.
    """
    COMPSS.get_directory(app_id, directory_name)  # noqa


def barrier(app_id: int, no_more_tasks: bool) -> None:
    """Call to barrier.

    :param app_id: Application identifier.
    :param no_more_tasks: No more tasks boolean.
    :return: None
    """
    COMPSS.barrier(app_id, no_more_tasks)  # noqa


def barrier_group(app_id: int, group_name: str) -> str:
    """Call to barrier_group.

    :param app_id: Application identifier.
    :param group_name: Group name.
    :return: Exception message.
    """
    return str(COMPSS.barrier_group(app_id, group_name))  # noqa


def open_task_group(group_name: str, implicit_barrier: bool, app_id: int) -> None:
    """Call to open_task_group.

    :param group_name: Group name.
    :param implicit_barrier: Implicit barrier boolean.
    :param app_id: Application identifier.
    :return: None.
    """
    COMPSS.open_task_group(group_name, implicit_barrier, app_id)  # noqa


def close_task_group(group_name: str, app_id: int) -> None:
    """Call to close_task_group.

    :param group_name: Group name.
    :param app_id: Application identifier.
    :return: None.
    """
    COMPSS.close_task_group(group_name, app_id)  # noqa


def get_logging_path() -> str:
    """Call to get_logging_path.

    :return: The COMPSs log path.
    """
    return COMPSS.get_logging_path()  # noqa


def get_number_of_resources(app_id: int) -> int:
    """Call to number_of_resources.

    :param app_id: Application identifier.
    :return: Number of resources.
    """
    return COMPSS.get_number_of_resources(app_id)  # noqa


def request_resources(app_id: int, num_resources: int, group_name: str) -> None:
    """Call to request_resources.

    :param app_id: Application identifier.
    :param num_resources: Number of resources.
    :param group_name: Group name.
    :return: None.
    """
    COMPSS.request_resources(app_id, num_resources, group_name)  # noqa


def free_resources(app_id: int, num_resources: int, group_name: str) -> None:
    """Call to free_resources.

    :param app_id: Application identifier.
    :param num_resources: Number of resources.
    :param group_name: Group name.
    :return: None.
    """
    COMPSS.free_resources(app_id, num_resources, group_name)  # noqa


def set_wall_clock(app_id: float, wcl: float) -> None:
    """Call to set_wall_clock.

    :param app_id: Application identifier.
    :param wcl: Wall Clock limit in seconds.
    :return: None.
    """
    COMPSS.set_wall_clock(app_id, wcl)  # noqa


def register_core_element(
    ce_signature: str,
    impl_signature: typing.Optional[str],
    impl_constraints: typing.Optional[str],
    impl_type: typing.Optional[str],
    impl_io: str,
    impl_prolog: typing.List[str],
    impl_epilog: typing.List[str],
    impl_type_args: typing.List[str],
) -> None:
    """Call to register_core_element.

    :param ce_signature: Core element signature.
    :param impl_signature: Implementation signature.
    :param impl_constraints: Implementation constraints.
    :param impl_type: Implementation type.
    :param impl_io: Implementation IO.
    :param impl_prolog: [binary, params, fail_by_exit_value] of the prolog.
    :param impl_epilog: [binary, params, fail_by_exit_value] of the epilog.
    :param impl_type_args: Implementation type arguments.
    :return: None.
    """
    COMPSS.register_core_element(
        ce_signature,  # noqa
        impl_signature,
        impl_constraints,
        impl_type,
        impl_io,
        impl_prolog,
        impl_epilog,
        impl_type_args,
    )


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
    COMPSS.process_task(
        app_id,  # noqa
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
    COMPSS.process_http_task(
        app_id,  # noqa
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


def set_pipes(pipe_in: str, pipe_out: str) -> None:
    """Set nesting pipes.

    :param pipe_in: Input pipe.
    :param pipe_out: Output pipe.
    :return: None.
    """
    COMPSS.set_pipes(pipe_in, pipe_out)  # noqa


def read_pipes() -> str:
    """Call to read_pipes.

    :return: The command read from the pipe.
    """
    command = COMPSS.read_pipes()  # noqa
    return command
