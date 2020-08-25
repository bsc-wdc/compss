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
PyCOMPSs API
============
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
        - wait_on
        - TaskGroup (class)
    functions.
    Also includes the redirection to the dummy API.

    CAUTION: If the context has not been defined, it will load the dummy API
             automatically.
"""

import pycompss.util.context as context

if context.in_pycompss():
    # ################################################################# #
    #                PyCOMPSs API definitions                           #
    # Any change on this API must be considered within the dummy API.   #
    # ################################################################# #

    from pycompss.runtime.binding import start_runtime as __start_runtime__
    from pycompss.runtime.binding import stop_runtime as __stop_runtime__
    from pycompss.runtime.binding import accessed_file as __accessed_file__
    from pycompss.runtime.binding import open_file as __open_file__
    from pycompss.runtime.binding import delete_file as __delete_file__
    from pycompss.runtime.binding import get_file as __get_file__
    from pycompss.runtime.binding import get_directory as __get_directory__
    from pycompss.runtime.binding import delete_object as __delete_object__
    from pycompss.runtime.binding import barrier as __barrier__
    from pycompss.runtime.binding import barrier_group as __barrier_group__
    from pycompss.runtime.binding import open_task_group as __open_task_group__
    from pycompss.runtime.binding import close_task_group as __close_task_group__                # noqa: E501
    from pycompss.runtime.binding import get_number_of_resources as __get_number_of_resources__  # noqa: E501
    from pycompss.runtime.binding import request_resources as __request_resources__              # noqa: E501
    from pycompss.runtime.binding import free_resources as __free_resources__
    from pycompss.runtime.binding import wait_on as __wait_on__
    from pycompss.api.exceptions import COMPSsException as __COMPSsException__

    def compss_start(log_level='off', tracing=0, interactive=False):
        # type: (str, int, bool) -> None
        """ Starts the runtime.

        :param log_level: Log level ['trace'|'debug'|'info'|'api'|'off'].
        :param tracing: Tracing level [0 (deactivated)|1 (basic)|2 (advanced)].
        :param interactive: Boolean if interactive (ipython or jupyter).
        :return: None
        """
        __start_runtime__(log_level, tracing, interactive)

    def compss_stop(code=0):
        # type: (int) -> None
        """ Stops the runtime.

        :param code: Stop code.
        :return: None
        """
        __stop_runtime__(code)

    def compss_file_exists(file_name):
        # type: (str) -> bool
        """ Check if a file exists.

        If it does not exist, it checks if the given file name has been
        accessed before by calling the runtime.

        :param file_name: The file name to check.
        :return: True either the file exists or has been accessed by the
                 runtime. False otherwise.
        """
        from os import path
        if not path.exists(file_name):
            return __accessed_file__(file_name)
        else:
            return True

    def compss_open(file_name, mode='r'):
        # type: (str, str) -> object
        """ Open a remotely produced file.

        Calls the runtime to bring the file to the master and opens it.
        It will wait for the file to be produced.
        CAUTION: Remember to close the file after using it with builtin close
                 function.

        :param file_name: File name.
        :param mode: Open mode. Options = [w, r+ or a , r or empty].
                     Default = 'r'
        :return: An object of 'file' type.
        :raise IOError: If the file can not be opened.
        """
        compss_name = __open_file__(file_name, mode)
        return open(compss_name, mode)

    def compss_delete_file(file_name):
        # type: (str) -> bool
        """ Delete a file.

        Calls the runtime to delete the file everywhere in the infrastructure.
        The delete is asynchronous and will be performed when the file is not
        necessary anymore.

        :param file_name: File name.
        :return: True if success. False otherwise.
        """
        return __delete_file__(file_name)

    def compss_wait_on_file(file_name):
        # type: (str) -> None
        """ Wait and get a file.

        Calls the runtime to bring the file to the master when possible
        and waits until produced.

        :param file_name: File name.
        :return: None
        """
        __get_file__(file_name)

    def compss_wait_on_directory(directory_name):
        # type: (str) -> None
        """ Wait and get a directory.

        Calls the runtime to bring the directory to the master when possible
        and waits until produced.

        :param directory_name: Directory name.
        :return: None
        """
        __get_directory__(directory_name)

    def compss_delete_object(obj):
        # type: (...) -> bool
        """ Delete object.

        Removes a used object from the internal structures and calls the
        external python library (that calls the bindings-common)
        in order to request a its corresponding file removal.

        :param obj: Object to delete.
        :return: True if success. False otherwise.
        """
        return __delete_object__(obj)

    def compss_barrier(no_more_tasks=False):
        # type: (bool) -> None
        """ Wait for all tasks.

        Perform a barrier waiting until all the submitted tasks have finished.

        :param no_more_tasks: No more tasks boolean.
        :return: None.
        """
        __barrier__(no_more_tasks)

    def compss_barrier_group(group_name):
        # type: (str) -> None
        """ Perform a barrier to a group.

        Stop until all the tasks of a group have finished.

        :param group_name: Name of the group to wait.
        :return: None.
        """
        exception_message = __barrier_group__(group_name)
        if exception_message is not None:
            raise __COMPSsException__(exception_message)

    def compss_wait_on(*args, **kwargs):
        # type: (*object, dict) -> object
        """ Wait for objects.

        Waits on a set of objects defined in args with the options defined in
        kwargs.
        Kwargs options:
            - 'mode' Write enable? [ 'r' | 'rw' ] Default = 'rw'

        :param args: Objects to wait on.
        :param kwargs: Options dictionary.
        :return: List with the final values (or a single element if only one).
        """
        return __wait_on__(*args, **kwargs)

    def compss_get_number_of_resources():
        # type: () -> int
        """ Request for the number of active resources.

        :return: The number of active resources.
        """
        return __get_number_of_resources__()

    def compss_request_resources(num_resources, group_name):
        # type: (int, str) -> None
        """ Requests the creation of num_resources resources.

        :param num_resources: Number of resources to create.
        :param group_name: Task group to notify upon resource creation.
                           (it can be None)
        :return: None
        """
        __request_resources__(num_resources, group_name)

    def compss_free_resources(num_resources, group_name):
        # type: (int, str) -> None
        """ Requests the destruction of num_resources resources.

        :param num_resources: Number of resources to destroy.
        :param group_name: Task group to notify upon resource creation
        :return: None
        """
        __free_resources__(num_resources, group_name)

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

        def __init__(self, group_name, implicit_barrier=True):
            # type: (str, bool) -> None
            """ Define a new group of tasks.

            :param group_name: Group name.
            :param implicit_barrier: Perform implicit barrier.
            """
            self.group_name = group_name
            self.implicit_barrier = implicit_barrier

        def __enter__(self):
            # Group creation
            __open_task_group__(self.group_name, self.implicit_barrier)

        def __exit__(self, type, value, traceback):
            # Group closing
            __close_task_group__(self.group_name)
            if self.implicit_barrier:
                compss_barrier_group(self.group_name)

else:
    # ################################################################# #
    #                    Dummmy API redirections                        #
    # ################################################################# #

    # Hidden imports
    from pycompss.api.dummy.api import compss_start as \
        __dummy_compss_start__
    from pycompss.api.dummy.api import compss_stop as \
        __dummy_compss_stop__
    from pycompss.api.dummy.api import compss_file_exists as \
        __dummy_compss_file_exists__
    from pycompss.api.dummy.api import compss_open as \
        __dummy_compss_open__
    from pycompss.api.dummy.api import compss_delete_file as \
        __dummy_compss_delete_file__
    from pycompss.api.dummy.api import compss_wait_on_file as \
        __dummy_compss_wait_on_file__
    from pycompss.api.dummy.api import compss_wait_on_directory as \
        __dummy_compss_wait_on_directory__
    from pycompss.api.dummy.api import compss_delete_object as \
        __dummy_compss_delete_object__
    from pycompss.api.dummy.api import compss_barrier as \
        __dummy_compss_barrier__
    from pycompss.api.dummy.api import compss_barrier_group as \
        __dummy_compss_barrier_group__
    from pycompss.api.dummy.api import compss_wait_on as \
        __dummy_compss_wait_on__
    from pycompss.api.dummy.api import compss_get_number_of_resources as \
        __dummy_compss_get_number_of_resources__
    from pycompss.api.dummy.api import compss_request_resources as \
        __dummy_compss_request_resources__
    from pycompss.api.dummy.api import compss_free_resources as \
        __dummy_compss_free_resources__
    # Hidden TaskGroup context manager
    from pycompss.api.dummy.api import TaskGroup  # noqa


    def compss_start(log_level='off', tracing=0, interactive=False):
        # type: (str, int, bool) -> None
        __dummy_compss_start__(log_level, tracing, interactive)

    def compss_stop(code=0):
        # type: (int) -> None
        __dummy_compss_stop__(code)

    def compss_file_exists(file_name):
        # type: (str) -> bool
        return __dummy_compss_file_exists__(file_name)

    def compss_open(file_name, mode='r'):
        # type: (str, str) -> object
        return __dummy_compss_open__(file_name, mode)

    def compss_delete_file(file_name):
        # type: (str) -> bool
        return __dummy_compss_delete_file__(file_name)

    def compss_wait_on_file(file_name):
        # type: (str) -> None
        __dummy_compss_wait_on_file__(file_name)

    def compss_wait_on_directory(directory_name):
        # type: (str) -> None
        __dummy_compss_wait_on_directory__(directory_name)

    def compss_delete_object(obj):
        # type: (...) -> bool
        return __dummy_compss_delete_object__(obj)

    def compss_barrier(no_more_tasks=False):
        # type: (bool) -> None
        __dummy_compss_barrier__(no_more_tasks)

    def compss_barrier_group(group_name):
        # type: (str) -> None
        __dummy_compss_barrier_group__(group_name)

    def compss_wait_on(*args):
        # type: (str) -> object
        return __dummy_compss_wait_on__(*args)

    def compss_get_number_of_resources():
        # type: () -> int
        return __dummy_compss_get_number_of_resources__()

    def compss_request_resources(num_resources, group_name):
        # type: (int, str) -> None
        __dummy_compss_request_resources__(num_resources, group_name)

    def compss_free_resources(num_resources, group_name):
        # type: (int, str) -> None
        __dummy_compss_free_resources__(num_resources, group_name)
