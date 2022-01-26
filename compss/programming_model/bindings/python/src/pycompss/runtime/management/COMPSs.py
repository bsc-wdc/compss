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
PyCOMPSs Binding - Management - Runtime
=======================================
    This file contains the COMPSs runtime connection.
    Loads the external C module.
"""

from pycompss.util.typing_helper import typing

from pycompss.runtime.management.link import establish_interactive_link
from pycompss.runtime.management.link import establish_link
from pycompss.util.exceptions import PyCOMPSsException

if __debug__:
    import logging

    logger = logging.getLogger(__name__)

# C module extension for the communication with the runtime
# See ext/compssmodule.cc
# Keep the COMPSs runtime link in this module so that any module can access
# it through the module methods.
_COMPSs = None  # type: typing.Any
# Files where the std may be redirected with interactive
_STDOUT = ""
_STDERR = ""


######################################################
#             INTERNAL FUNCTIONS                     #
######################################################


def load_runtime(external_process: bool = False, _logger: typing.Any = None) -> None:
    """Loads the external C extension module.

    :param external_process: Loads the runtime in an external process if true.
                             Within this python process if false.
    :param _logger: Use this logger instead of the module logger.
    :return: None
    """
    global _COMPSs
    global _STDOUT
    global _STDERR

    if external_process:
        # For interactive python environments
        _COMPSs, _STDOUT, _STDERR = establish_interactive_link(_logger, True)
    else:
        # Normal python environments
        _COMPSs = establish_link(_logger)


def is_redirected() -> bool:
    """Check if the stdout and stderr are being redirected.

    :return: If stdout/stderr are being redirected.
    """
    if _STDOUT == "" and _STDERR == "":
        return False
    elif _STDOUT != "" and _STDERR != "":
        return True
    else:
        raise PyCOMPSsException("Inconsistent status of _STDOUT and _STDERR")


def get_redirection_file_names() -> typing.Tuple[str, str]:
    """Retrieves the stdout and stderr file names.

    :return: The stdout and stderr file names.
    """
    if is_redirected():
        return _STDOUT, _STDERR
    else:
        message = "The runtime stdout and stderr are  not being redirected."
        raise PyCOMPSsException(message)


######################################################
#          COMPSs API EXPOSED FUNCTIONS              #
######################################################


def start_runtime() -> None:
    """Call to start_runtime.

    :return: None
    """
    _COMPSs.start_runtime()  # noqa


def set_debug(mode: bool) -> None:
    """Call to set_debug.

    :param mode: Debug mode
    :return: None
    """
    _COMPSs.set_debug(mode)  # noqa


def stop_runtime(code: int) -> None:
    """Call to stop_runtime.

    :param code: Code
    :return: None
    """
    _COMPSs.stop_runtime(code)  # noqa


def cancel_application_tasks(app_id: int, value: int) -> None:
    """Call to cancel_application_tasks.

    :param app_id: Application identifier.
    :param value: Value
    :return: None
    """
    _COMPSs.cancel_application_tasks(app_id, value)  # noqa


def accessed_file(app_id: int, file_name: str) -> bool:
    """Call to accessed_file.

    :param app_id: Application identifier.
    :param file_name: File name.
    :return: If the file has been accessed.
    """
    return _COMPSs.accessed_file(app_id, file_name)  # noqa


def open_file(app_id: int, file_name: str, mode: int) -> str:
    """Call to open_file.

    :param app_id: Application identifier.
    :param file_name: File name reference.
    :param mode: Open mode.
    :return: The real file name.
    """
    return _COMPSs.open_file(app_id, file_name, mode)  # noqa


def close_file(app_id: int, file_name: str, mode: int) -> None:
    """Call to close_file.

    :param app_id: Application identifier.
    :param file_name: File name reference.
    :param mode: Close mode.
    :return: None
    """
    _COMPSs.close_file(app_id, file_name, mode)  # noqa


def delete_file(app_id: int, file_name: str, mode: bool) -> bool:
    """Call to delete_file.

    :param app_id: Application identifier.
    :param file_name: File name reference.
    :param mode: Delete mode.
    :return: The deletion result.
    """
    result = _COMPSs.delete_file(app_id, file_name, mode)  # noqa
    if result is None:
        return False
    else:
        return result


def get_file(app_id: int, file_name: str) -> None:
    """Call to get_file.

    :param app_id: Application identifier.
    :param file_name: File name.
    :return: None
    """
    _COMPSs.get_file(app_id, file_name)  # noqa


def get_directory(app_id: int, file_name: str) -> None:
    """Call to get_directory.

    :param app_id: Application identifier.
    :param file_name: File name.
    :return: None
    """
    _COMPSs.get_directory(app_id, file_name)  # noqa


def barrier(app_id: int, no_more_tasks: bool) -> None:
    """Call to barrier.

    :param app_id: Application identifier.
    :param no_more_tasks: No more tasks boolean.
    :return: None
    """
    _COMPSs.barrier(app_id, no_more_tasks)  # noqa


def barrier_group(app_id: int, group_name: str) -> str:
    """Call barrier_group.

    :param app_id: Application identifier.
    :param group_name: Group name.
    :return: Exception message.
    """
    return str(_COMPSs.barrier_group(app_id, group_name))  # noqa


def open_task_group(group_name: str, implicit_barrier: bool, app_id: int) -> None:
    """Call to open_task_group.

    :param group_name: Group name.
    :param implicit_barrier: Implicit barrier boolean.
    :param app_id: Application identifier.
    :return: None
    """
    _COMPSs.open_task_group(group_name, implicit_barrier, app_id)  # noqa


def close_task_group(group_name: str, app_id: int) -> None:
    """Call to close_task_group.

    :param group_name: Group name.
    :param app_id: Application identifier.
    :return: None
    """
    _COMPSs.close_task_group(group_name, app_id)  # noqa


def get_logging_path() -> str:
    """Call to get_logging_path.

    :return: The COMPSs log path
    """
    return _COMPSs.get_logging_path()  # noqa


def get_number_of_resources(app_id: int) -> int:
    """Call to number_of_resources.

    :param app_id: Application identifier
    :return: Number of resources
    """
    return _COMPSs.get_number_of_resources(app_id)  # noqa


def request_resources(app_id: int, num_resources: int, group_name: str) -> None:
    """Call to request_resources.

    :param app_id: Application identifier.
    :param num_resources: Number of resources.
    :param group_name: Group name.
    :return: None
    """
    _COMPSs.request_resources(app_id, num_resources, group_name)  # noqa


def free_resources(app_id: int, num_resources: int, group_name: str) -> None:
    """Call to free_resources.

    :param app_id: Application identifier.
    :param num_resources: Number of resources.
    :param group_name: Group name.
    :return: None
    """
    _COMPSs.free_resources(app_id, num_resources, group_name)  # noqa


def set_wall_clock(app_id: float, wcl: float) -> None:
    """Call to set_wall_clock.

    :param app_id: Application identifier.
    :param wcl: Wall Clock limit in seconds.
    :return: None
    """
    _COMPSs.set_wall_clock(app_id, wcl)  # noqa


def register_core_element(
    ce_signature: str,
    impl_signature: typing.Optional[str],
    impl_constraints: typing.Optional[str],
    impl_type: typing.Optional[str],
    impl_io: str,
    prolog: typing.List[str],
    epilog: typing.List[str],
    impl_type_args: typing.List[str]
) -> None:
    """Call to register_core_element.

    :param ce_signature: Core element signature
    :param impl_signature: Implementation signature
    :param impl_constraints: Implementation constraints
    :param impl_type: Implementation type
    :param impl_io: Implementation IO
    :param prolog: Prolog; a list containing the binary and params
    :param epilog: epilog; a list containing the binary and params
    :param impl_type_args: Implementation type arguments
    :return: None
    """
    _COMPSs.register_core_element(
        ce_signature,  # noqa
        impl_signature,
        impl_constraints,
        impl_type,
        impl_io,
        prolog,
        epilog,
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

    :param app_id: Application identifier
    :param signature: Task signature
    :param on_failure: On failure action
    :param time_out: Task time out
    :param has_priority: Boolean has priority
    :param num_nodes: Number of nodes
    :param reduction: Boolean indicating if the task is of type reduce
    :param chunk_size: Size of chunks for executing the reduce operation
    :param replicated: Boolean is replicated
    :param distributed: Boolean is distributed
    :param has_target: Boolean has target
    :param num_returns: Number of returns
    :param values: Values
    :param names: Names
    :param compss_types: COMPSs types
    :param compss_directions: COMPSs directions
    :param compss_streams: COMPSs streams
    :param compss_prefixes: COMPSs prefixes
    :param content_types: COMPSs types
    :param weights: Parameter weights
    :param keep_renames: Boolean keep renames
    :return: None
    """
    _COMPSs.process_task(
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

    :param app_id: Application identifier
    :param signature: Task signature
    :param on_failure: On failure action
    :param time_out: Task time out
    :param has_priority: Boolean has priority
    :param num_nodes: Number of nodes
    :param reduction: Boolean indicating if the task is of type reduce
    :param chunk_size: Size of chunks for executing the reduce operation
    :param replicated: Boolean is replicated
    :param distributed: Boolean is distributed
    :param has_target: Boolean has target
    :param num_returns: Number of returns
    :param values: Values
    :param names: Names
    :param compss_types: COMPSs types
    :param compss_directions: COMPSs directions
    :param compss_streams: COMPSs streams
    :param compss_prefixes: COMPSs prefixes
    :param content_types: COMPSs types
    :param weights: Parameter weights
    :param keep_renames: Boolean keep renames
    :return: None
    """
    _COMPSs.process_http_task(
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
    :return: None
    """
    _COMPSs.set_pipes(pipe_in, pipe_out)  # noqa


def read_pipes() -> str:
    """Call to read_pipes.

    :return: The command read from the pipe
    """
    o = _COMPSs.read_pipes()  # noqa
    return o
