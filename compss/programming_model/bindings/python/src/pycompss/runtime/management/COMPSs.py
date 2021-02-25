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
PyCOMPSs Binding - Management - Runtime
=======================================
    This file contains the COMPSs runtime connection.
    Loads the external C module.
"""

from pycompss.runtime.management.link import establish_interactive_link
from pycompss.runtime.management.link import establish_link

if __debug__:
    import logging
    logger = logging.getLogger(__name__)

# C module extension for the communication with the runtime
# See ext/compssmodule.cc
# Keep the COMPSs runtime link in this module so that any module can access
# it through the module methods.
_COMPSs = None
# Files where the std may be redirected with interactive
_STDOUT = None
_STDERR = None


######################################################
#             INTERNAL FUNCTIONS                     #
######################################################

def load_runtime(external_process=False, _logger=None):
    # type: (bool, ...) -> None
    """ Loads the external C extension module.

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


def is_redirected():
    """ Check if the stdout and stderr are being redirected.

    :return: If stdout/stderr are being redirected.
    """
    if _STDOUT is None and _STDERR is None:
        return False
    elif _STDOUT is not None and _STDERR is not None:
        return True
    else:
        raise Exception("Inconsistent status of _STDOUT and _STDERR")


def get_redirection_file_names():
    """ Retrieves the stdout and stderr file names.

    :return: The stdout and stderr file names.
    """
    if is_redirected():
        return _STDOUT, _STDERR
    else:
        raise Exception("The runtime stdout and stderr are  not being redirected.")  # noqa: E501


######################################################
#          COMPSs API EXPOSED FUNCTIONS              #
######################################################

def start_runtime():
    # type: () -> None
    """ Call to start_runtime.

    :return: None
    """
    _COMPSs.start_runtime()  # noqa


def set_debug(mode):
    # type: (bool) -> None
    """ Call to set_debug.

    :param mode: Debug mode
    :return: None
    """
    _COMPSs.set_debug(mode)  # noqa


def stop_runtime(code):
    # type: (int) -> None
    """ Call to stop_runtime.

    :param code: Code
    :return: None
    """
    _COMPSs.stop_runtime(code)  # noqa


def cancel_application_tasks(app_id, value):
    # type: (int, int) -> None
    """ Call to cancel_application_tasks.

    :param app_id: Application identifier.
    :param value: Value
    :return: None
    """
    _COMPSs.cancel_application_tasks(app_id, value)  # noqa


def accessed_file(app_id, file_name):
    # type: (int, str) -> bool
    """ Call to accessed_file.

    :param app_id: Application identifier.
    :param file_name: File name.
    :return: If the file has been accessed.
    """
    return _COMPSs.accessed_file(app_id, file_name)  # noqa


def open_file(app_id, file_name, mode):
    # type: (int, str, int) -> str
    """ Call to open_file.

    :param app_id: Application identifier.
    :param file_name: File name reference.
    :param mode: Open mode.
    :return: The real file name.
    """
    return _COMPSs.open_file(app_id, file_name, mode)  # noqa


def close_file(app_id, file_name, mode):
    # type: (int, str, int) -> None
    """ Call to close_file.

    :param app_id: Application identifier.
    :param file_name: File name reference.
    :param mode: Close mode.
    :return: None
    """
    _COMPSs.close_file(app_id, file_name, mode)  # noqa


def delete_file(app_id, file_name, mode):
    # type: (int, str, bool) -> bool
    """ Call to delete_file.

    :param app_id: Application identifier.
    :param file_name: File name reference.
    :param mode: Delete mode.
    :return: The deletion result.
    """
    return _COMPSs.delete_file(app_id, file_name, mode)  # noqa


def get_file(app_id, file_name):
    # type: (int, str) -> None
    """ Call to get_file.

    :param app_id: Application identifier.
    :param file_name: File name.
    :return: None
    """
    _COMPSs.get_file(app_id, file_name)  # noqa


def get_directory(app_id, file_name):
    # type: (int, str) -> None
    """ Call to get_directory.

    :param app_id: Application identifier.
    :param file_name: File name.
    :return: None
    """
    _COMPSs.get_directory(app_id, file_name)  # noqa


def barrier(app_id, no_more_tasks):
    # type: (int, bool) -> None
    """ Call to barrier.

    :param app_id: Application identifier.
    :param no_more_tasks: No more tasks boolean.
    :return: None
    """
    _COMPSs.barrier(app_id, no_more_tasks)  # noqa


def barrier_group(app_id, group_name):
    # type: (int, str) -> str
    """ Call barrier_group.

    :param app_id: Application identifier.
    :param group_name: Group name.
    :return: Exception message.
    """
    return _COMPSs.barrier_group(app_id, group_name)  # noqa


def open_task_group(group_name, implicit_barrier, app_id):
    # type: (str, bool, int) -> None
    """ Call to open_task_group.

    :param group_name: Group name.
    :param implicit_barrier: Implicit barrier boolean.
    :param app_id: Application identifier.
    :return: None
    """
    _COMPSs.open_task_group(group_name, implicit_barrier, app_id)  # noqa


def close_task_group(group_name, app_id):
    # type: (str, int) -> None
    """ Call to close_task_group.

    :param group_name: Group name.
    :param app_id: Application identifier.
    :return: None
    """
    _COMPSs.close_task_group(group_name, app_id)  # noqa


def get_logging_path():
    # type: () -> str
    """ Call to get_logging_path.

    :return: The COMPSs log path
    """
    return _COMPSs.get_logging_path()  # noqa


def get_number_of_resources(app_id):
    # type: (int) -> int
    """ Call to number_of_resources.

    :param app_id: Application identifier
    :return: Number of resources
    """
    return _COMPSs.get_number_of_resources(app_id)  # noqa


def request_resources(app_id, num_resources, group_name):
    # type: (int, int, str) -> None
    """ Call to request_resources.

    :param app_id: Application identifier.
    :param num_resources: Number of resources.
    :param group_name: Group name.
    :return: None
    """
    _COMPSs.request_resources(app_id, num_resources, group_name)  # noqa


def free_resources(app_id, num_resources, group_name):
    # type: (int, int, str) -> None
    """ Call to free_resources.

    :param app_id: Application identifier.
    :param num_resources: Number of resources.
    :param group_name: Group name.
    :return: None
    """
    _COMPSs.free_resources(app_id, num_resources, group_name)  # noqa


def set_wall_clock(app_id, wcl):
    # type: (long, long) -> None
    """ Call to set_wall_clock.

    :param app_id: Application identifier.
    :param wcl: Wall Clock limit in seconds.
    :return: None
    """
    _COMPSs.set_wall_clock(app_id, wcl)  # noqa


def register_core_element(ce_signature,      # type: str
                          impl_signature,    # type: str
                          impl_constraints,  # type: str
                          impl_type,         # type: str
                          impl_io,           # type: str
                          impl_type_args     # type: list
                          ):
    # type: (...) -> None
    """ Call to register_core_element.

    :param ce_signature: Core element signature
    :param impl_signature: Implementation signature
    :param impl_constraints: Implementation constraints
    :param impl_type: Implementation type
    :param impl_io: Implementation IO
    :param impl_type_args: Implementation type arguments
    :return: None
    """
    _COMPSs.register_core_element(ce_signature,    # noqa
                                  impl_signature,
                                  impl_constraints,
                                  impl_type,
                                  impl_io,
                                  impl_type_args)


def process_task(app_id,             # type: int
                 signature,          # type: str
                 on_failure,         # type: str
                 time_out,           # type: int
                 has_priority,       # type: bool
                 num_nodes,          # type: int
                 reduction,          # type: bool
                 chunk_size,         # type: int
                 replicated,         # type: bool
                 distributed,        # type: bool
                 has_target,         # type: bool
                 num_returns,        # type: int
                 values,             # type: list
                 names,              # type: list
                 compss_types,       # type: list
                 compss_directions,  # type: list
                 compss_streams,     # type: list
                 compss_prefixes,    # type: list
                 content_types,      # type: list
                 weights,            # type: list
                 keep_renames        # type: list
                 ):  # NOSONAR
    # type: (...) -> None
    """ Call to process_task.

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
    _COMPSs.process_task(app_id,    # noqa
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
                         keep_renames)


def set_pipes(pipe_in, pipe_out):
    # type: (str, str) -> None
    """ Set nesting pipes.

    :param pipe_in: Input pipe.
    :param pipe_out: Output pipe.
    :return: None
    """
    _COMPSs.set_pipes(pipe_in, pipe_out)  # noqa
