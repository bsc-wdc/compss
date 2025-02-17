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
PyCOMPSs Binding - Management - COMPSs Runtime.

This file contains the COMPSs runtime connection.
Loads the external C module.
"""

import logging
from pycompss.runtime.management.link.separate import (
    establish_interactive_link,
)
from pycompss.runtime.management.link.direct import establish_link
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.typing_helper import typing

if __debug__:
    LOGGER = logging.getLogger(__name__)


class COMPSsModule:  # pylint: disable=invalid-name, too-many-public-methods
    """C module extension for the communication with the runtime.

    See ext/compssmodule.cc

    Keep the COMPSs runtime link in this module so that any module can access
    it through the module methods.
    """

    __slots__ = ["compss", "stdout", "stderr"]

    def __init__(self) -> None:
        """Create a new COMPSs object."""
        # COMPSs connection
        self.compss = None  # type: typing.Any
        # Files where the std may be redirected with interactive
        self.stdout = ""
        self.stderr = ""

    ######################################################
    #             INTERNAL FUNCTIONS                     #
    ######################################################

    def load_runtime(
        self,
        external_process: bool = False,
        _logger: typing.Optional[logging.Logger] = None,
    ) -> None:
        """Load the external C extension module.

        :param external_process: Loads the runtime in an external process
                                 if true.
                                 Within this python process if false.
        :param _logger: Use this logger instead of the module LOGGER.
        :return: None
        """
        if external_process:
            # For interactive python environments
            self.compss, self.stdout, self.stderr = establish_interactive_link(
                _logger, True
            )
        else:
            # Normal python environments
            self.compss = establish_link(_logger)

    def is_redirected(self) -> bool:
        """Check if the stdout and stderr are being redirected.

        :return: If stdout/stderr are being redirected.
        """
        if self.stdout == "" and self.stderr == "":
            return False
        if self.stdout != "" and self.stderr != "":
            return True
        message = "Inconsistent status of STDOUT and STDERR"
        raise PyCOMPSsException(message)

    def get_redirection_file_names(self) -> typing.Tuple[str, str]:
        """Retrieve the stdout and stderr file names.

        :return: The stdout and stderr file names.
        """
        if self.is_redirected():
            return self.stdout, self.stderr
        message = "The runtime STDOUT and STDERR are not being redirected."
        raise PyCOMPSsException(message)

    ######################################################
    #          COMPSs API EXPOSED FUNCTIONS              #
    ######################################################

    def start_runtime(self) -> None:
        """Call to start_runtime.

        :return: None
        """
        self.compss.start_runtime()

    def set_debug(self, mode: bool) -> None:
        """Call to set_debug.

        :param mode: Debug mode ( True | False ).
        :return: None.
        """
        self.compss.set_debug(mode)

    def stop_runtime(self, code: int) -> None:
        """Call to stop_runtime.

        :param code: Stopping code.
        :return: None.
        """
        self.compss.stop_runtime(code)

    def cancel_application_tasks(self, app_id: int, value: int) -> None:
        """Call to cancel_application_tasks.

        :param app_id: Application identifier.
        :param value:  Task identifier.
        :return: None.
        """
        self.compss.cancel_application_tasks(app_id, value)

    def accessed_file(self, app_id: int, file_name: str) -> bool:
        """Call to accessed_file.

        :param app_id: Application identifier.
        :param file_name: File name to check if accessed.
        :return: If the file has been accessed.
        """
        return self.compss.accessed_file(app_id, file_name)

    def open_file(self, app_id: int, file_name: str, mode: int) -> str:
        """Call to open_file.

        Synchronizes if necessary.

        :param app_id: Application identifier.
        :param file_name: File name reference to open.
        :param mode: Open mode.
        :return: The real file name.
        """
        return self.compss.open_file(app_id, file_name, mode)

    def close_file(self, app_id: int, file_name: str, mode: int) -> None:
        """Call to close_file.

        :param app_id: Application identifier.
        :param file_name: File name reference to close.
        :param mode: Close mode.
        :return: None
        """
        self.compss.close_file(app_id, file_name, mode)

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
        result = self.compss.delete_file(
            app_id, file_name, mode, application_delete
        )
        if result is None:
            return False
        return result

    def get_file(self, app_id: int, file_name: str) -> None:
        """Call to (synchronize) get_file.

        :param app_id: Application identifier.
        :param file_name: File name reference to get.
        :return: None.
        """
        self.compss.get_file(app_id, file_name)

    def get_directory(self, app_id: int, directory_name: str) -> None:
        """Call to (synchronize) get_directory.

        :param app_id: Application identifier.
        :param directory_name: Directory name reference to get.
        :return: None.
        """
        self.compss.get_directory(app_id, directory_name)

    def barrier(self, app_id: int, no_more_tasks: bool) -> None:
        """Call to barrier.

        :param app_id: Application identifier.
        :param no_more_tasks: No more tasks boolean.
        :return: None
        """
        self.compss.barrier(app_id, no_more_tasks)

    def barrier_group(self, app_id: int, group_name: str) -> str:
        """Call to barrier_group.

        :param app_id: Application identifier.
        :param group_name: Group name.
        :return: Exception message.
        """
        return str(self.compss.barrier_group(app_id, group_name))

    def open_task_group(
        self, group_name: str, implicit_barrier: bool, app_id: int
    ) -> None:
        """Call to open_task_group.

        :param group_name: Group name.
        :param implicit_barrier: Implicit barrier boolean.
        :param app_id: Application identifier.
        :return: None.
        """
        self.compss.open_task_group(group_name, implicit_barrier, app_id)

    def close_task_group(self, group_name: str, app_id: int) -> None:
        """Call to close_task_group.

        :param group_name: Group name.
        :param app_id: Application identifier.
        :return: None.
        """
        self.compss.close_task_group(group_name, app_id)

    def cancel_task_group(self, group_name: str, app_id: int) -> str:
        """Call to cancel_task_group.

        :param group_name: Group name.
        :param app_id: Application identifier.
        :return: Exception message.
        """
        return str(self.compss.cancel_task_group(group_name, app_id))

    def snapshot(self, app_id) -> None:
        """Call to snapshot.

        :param app_id: Application identifier.
        :return: None
        """
        self.compss.snapshot(app_id)

    def get_logging_path(self) -> str:
        """Call to get_logging_path.

        :return: The COMPSs log path.
        """
        return self.compss.get_logging_path()

    def get_master_working_path(self) -> str:
        """Call to get_master_working_path.

        :return: The COMPSs log path.
        """
        return self.compss.get_master_working_path()

    def get_number_of_resources(self, app_id: int) -> int:
        """Call to number_of_resources.

        :param app_id: Application identifier.
        :return: Number of resources.
        """
        return self.compss.get_number_of_resources(app_id)

    def request_resources(
        self, app_id: int, num_resources: int, group_name: str
    ) -> None:
        """Call to request_resources.

        :param app_id: Application identifier.
        :param num_resources: Number of resources.
        :param group_name: Group name.
        :return: None.
        """
        self.compss.request_resources(app_id, num_resources, group_name)

    def free_resources(
        self, app_id: int, num_resources: int, group_name: str
    ) -> None:
        """Call to free_resources.

        :param app_id: Application identifier.
        :param num_resources: Number of resources.
        :param group_name: Group name.
        :return: None.
        """
        self.compss.free_resources(app_id, num_resources, group_name)

    def set_wall_clock(self, app_id: float, wcl: float) -> None:
        """Call to set_wall_clock.

        :param app_id: Application identifier.
        :param wcl: Wall Clock limit in seconds.
        :return: None.
        """
        self.compss.set_wall_clock(app_id, wcl)

    def register_core_element(  # pylint: disable=too-many-arguments
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
        self.compss.register_core_element(
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

    def process_task(  # pylint: disable=too-many-arguments, too-many-locals
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
        self.compss.process_task(
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

    def process_http_task(  # pylint: disable=too-many-arguments, R0914
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
        self.compss.process_http_task(
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

    def set_pipes(self, pipe_in: str, pipe_out: str) -> None:
        """Set nesting pipes.

        :param pipe_in: Input pipe.
        :param pipe_out: Output pipe.
        :return: None.
        """
        self.compss.set_pipes(pipe_in, pipe_out)

    def read_pipes(self) -> str:
        """Call to read_pipes.

        :return: The command read from the pipe.
        """
        command = self.compss.read_pipes()
        return command


# ################################################# #
# ########## MAIN EXTERNAL COMPSS MODULE ########## #
# ################################################# #


COMPSs = COMPSsModule()
