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
PyCOMPSs API.

This file defines the public PyCOMPSs API functions.
It implements the:
    - start
    - stop
    - open
    - delete file
    - wait on file
    - delete object
    - barrier
    - barrier group
    - get_number_of_resources
    - request_resources_creation
    - request_resources_destruction
    - set_wall_clock
    - wait_on
    - TaskGroup (class)
functions.
Also includes the redirection to the dummy API.

CAUTION: If the context has not been defined, it will load the dummy API
         automatically.
"""

import pycompss.util.context as context

# Dummy imports
from pycompss.api.dummy.api import (
    compss_start as __dummy_compss_start__,
    compss_stop as __dummy_compss_stop__,
    compss_file_exists as __dummy_compss_file_exists__,
    compss_open as __dummy_compss_open__,
    compss_delete_file as __dummy_compss_delete_file__,
    compss_wait_on_file as __dummy_compss_wait_on_file__,
    compss_wait_on_directory as __dummy_compss_wait_on_directory__,
    compss_delete_object as __dummy_compss_delete_object__,
    compss_barrier as __dummy_compss_barrier__,
    compss_barrier_group as __dummy_compss_barrier_group__,
    compss_wait_on as __dummy_compss_wait_on__,
    compss_get_number_of_resources as __dummy_compss_get_number_of_resources__,
    compss_request_resources as __dummy_compss_request_resources__,
    compss_free_resources as __dummy_compss_free_resources__,
    compss_set_wall_clock as __dummy_compss_set_wall_clock__,
)
from pycompss.util.typing_helper import typing

if context.in_pycompss():
    # ################################################################# #
    #                PyCOMPSs API definitions                           #
    # Any change on this API must be considered within the dummy API.   #
    # ################################################################# #

    from pycompss.runtime.binding import (
        start_runtime as __start_runtime__,
        stop_runtime as __stop_runtime__,
        accessed_file as __accessed_file__,
        open_file as __open_file__,
        delete_file as __delete_file__,
        get_file as __get_file__,
        get_directory as __get_directory__,
        delete_object as __delete_object__,
        barrier as __barrier__,
        barrier_group as __barrier_group__,
        open_task_group as __open_task_group__,
        close_task_group as __close_task_group__,
        get_number_of_resources as __get_number_of_resources__,
        request_resources as __request_resources__,
        free_resources as __free_resources__,
        set_wall_clock as __set_wall_clock__,
        wait_on as __wait_on__,
    )
    from pycompss.api.exceptions import COMPSsException as __COMPSsException__


def compss_start(
    log_level: str = "off",
    tracing: bool = False,
    interactive: bool = False,
    disable_external: bool = False,
) -> None:
    """Start the COMPSs runtime.

    :param log_level: Log level [ True | False ].
    :param tracing: Activate or disable tracing.
    :param interactive: Boolean if interactive (ipython or jupyter).
    :param disable_external: To avoid to load compss in external process.
    :return: None
    """
    if context.in_pycompss():
        __start_runtime__(log_level, tracing, interactive, disable_external)
    else:
        __dummy_compss_start__(log_level, tracing, interactive, disable_external)


def compss_stop(code: int = 0, _hard_stop: bool = False) -> None:
    """Stop the COMPSs runtime.

    :param code: Stop code.
    :param _hard_stop: Stop compss when runtime has died.
    :return: None
    """
    if context.in_pycompss():
        __stop_runtime__(code, _hard_stop)
    else:
        __dummy_compss_stop__(code, _hard_stop)


def compss_file_exists(*file_name: str) -> typing.Union[bool, typing.List[bool]]:
    """Check if a file exists.

    If it does not exist, it checks if the given file name has been
    accessed before by calling the runtime.

    :param file_name: The file/s name to check.
    :return: True either the file exists or has been accessed by the
             runtime. False otherwise.
    """
    if context.in_pycompss():
        if len(file_name) == 1:
            return __accessed_file__(file_name[0])
        else:
            return [__accessed_file__(f_name) for f_name in file_name]
    else:
        return __dummy_compss_file_exists__(*file_name)


def compss_open(file_name: str, mode: str = "r") -> typing.Any:
    """Open a remotely produced file.

    Calls the runtime to bring the file to the master and opens it.
    It will wait for the file to be produced.
    CAUTION: Remember to close the file after using it with builtin close
             function.

    :param file_name: File name.
    :param mode: Open mode. Options = [w, r+ or a, r or empty].
                 Default = "r"
    :return: An object of "file" type.
    :raise IOError: If the file can not be opened.
    """
    if context.in_pycompss():
        compss_name = __open_file__(file_name, mode)
        return open(compss_name, mode)
    else:
        return __dummy_compss_open__(file_name, mode)


def compss_delete_file(*file_name: str) -> typing.Union[bool, typing.List[bool]]:
    """Delete a file.

    Calls the runtime to delete the file everywhere in the infrastructure.
    Deletion is asynchronous and will be performed when the file is not
    necessary anymore.

    :param file_name: File/s name.
    :return: True if success. False otherwise.
    """
    if context.in_pycompss():
        if len(file_name) == 1:
            return __delete_file__(file_name[0])
        else:
            return [__delete_file__(f_name) for f_name in file_name]
    else:
        return __dummy_compss_delete_file__(*file_name)


def compss_wait_on_file(*file_name: str) -> None:
    """Wait and get a file.

    Calls the runtime to bring the file to the master when possible
    and waits until produced.

    :param file_name: File/s name.
    :return: None
    """
    if context.in_pycompss():
        if len(file_name) == 1:
            __get_file__(file_name[0])
        else:
            for f_name in file_name:
                __get_file__(f_name)
    else:
        __dummy_compss_wait_on_file__(*file_name)


def compss_wait_on_directory(*directory_name: str) -> None:
    """Wait and get a directory.

    Calls the runtime to bring the directory to the master when possible
    and waits until produced.

    :param directory_name: Directory/ies name.
    :return: None
    """
    if context.in_pycompss():
        if len(directory_name) == 1:
            __get_directory__(directory_name[0])
        else:
            for d_name in directory_name:
                __get_directory__(d_name)
    else:
        __dummy_compss_wait_on_directory__(*directory_name)


def compss_delete_object(*obj: typing.Any) -> typing.Union[bool, typing.List[bool]]:
    """Delete object.

    Removes a used object from the internal structures and calls the
    external python library (that calls the bindings-common)
    in order to request its corresponding file removal.

    :param obj: Object/s to delete.
    :return: True if success. False otherwise.
    """
    if context.in_pycompss():
        if len(obj) == 1:
            return __delete_object__(obj[0])
        else:
            return [__delete_object__(i_obj) for i_obj in obj]
    else:
        return __dummy_compss_delete_object__(*obj)


def compss_barrier(no_more_tasks: bool = False) -> None:
    """Wait for all tasks.

    Perform a barrier waiting until all the submitted tasks have finished.

    :param no_more_tasks: No more tasks boolean.
    :return: None.
    """
    if context.in_pycompss():
        __barrier__(no_more_tasks)
    else:
        __dummy_compss_barrier__(no_more_tasks)


def compss_barrier_group(group_name: str) -> None:
    """Perform a barrier to a group.

    Stop until all the tasks of a group have finished.

    :param group_name: Name of the group to wait.
    :return: None.
    """
    if context.in_pycompss():
        exception_message = __barrier_group__(group_name)
        if exception_message != "None":
            raise __COMPSsException__(exception_message)
    else:
        __dummy_compss_barrier_group__(group_name)


def compss_wait_on(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
    """Wait for objects.

    Waits on a set of objects defined in args with the options defined in
    kwargs.
    Kwargs options:
        - "mode" Write enable? [ 'r' | 'rw' ] Default = 'rw'

    :param args: Objects to wait on.
    :param kwargs: Options dictionary.
    :return: List with the final values (or a single element if only one).
    """
    if context.in_pycompss():
        return __wait_on__(*args, **kwargs)
    else:
        return __dummy_compss_wait_on__(*args, **kwargs)


def compss_get_number_of_resources() -> int:
    """Request for the number of active resources.

    :return: The number of active resources.
    """
    if context.in_pycompss():
        return __get_number_of_resources__()
    else:
        return __dummy_compss_get_number_of_resources__()


def compss_request_resources(
    num_resources: int, group_name: typing.Optional[str]
) -> None:
    """Request the creation of num_resources resources.

    :param num_resources: Number of resources to create.
    :param group_name: Task group to notify upon resource creation.
                       (it can be None)
    :return: None
    """
    if context.in_pycompss():
        __request_resources__(num_resources, group_name)
    else:
        __dummy_compss_request_resources__(num_resources, group_name)


def compss_free_resources(num_resources: int, group_name: typing.Optional[str]) -> None:
    """Request the destruction of num_resources resources.

    :param num_resources: Number of resources to destroy.
    :param group_name: Task group to notify upon resource creation
    :return: None
    """
    if context.in_pycompss():
        __free_resources__(num_resources, group_name)
    else:
        __dummy_compss_free_resources__(num_resources, group_name)


def compss_set_wall_clock(wall_clock_limit: int) -> None:
    """Set the application wall clock limit.

    :param wall_clock_limit: Wall clock limit in seconds.
    :return: None
    """
    if context.in_pycompss():
        __set_wall_clock__(wall_clock_limit)
    else:
        __dummy_compss_set_wall_clock__(wall_clock_limit)


class TaskGroup(object):
    """
    A context-like class used to represent a group of tasks.

    This context is aimed at enabling to define groups of tasks
    using the "with" statement.

    For example:
        ...
        with TaskGroup("my_group", False):
            # call to tasks
            # they will be considered within my_group group.
            ...
        ...
    """

    __slots__ = ["group_name", "implicit_barrier"]

    def __init__(self, group_name: str, implicit_barrier: bool = True) -> None:
        """Define a new group of tasks.

        :param group_name: Group name.
        :param implicit_barrier: Perform implicit barrier.

        :attr str group_name: Group name.
        :attr bool implicit_barrier: Perform implicit barrier.
        """
        if context.in_pycompss():
            self.group_name = group_name
            self.implicit_barrier = implicit_barrier
        else:
            pass

    def __enter__(self) -> None:
        """Group creation."""
        if context.in_pycompss():
            __open_task_group__(self.group_name, self.implicit_barrier)
        else:
            pass

    def __exit__(
        self, type: typing.Any, value: typing.Any, traceback: typing.Any
    ) -> None:
        """Group closing."""
        if context.in_pycompss():
            __close_task_group__(self.group_name)
            if self.implicit_barrier:
                compss_barrier_group(self.group_name)
        else:
            pass
