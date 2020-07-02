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
    from pycompss.runtime.commons import IS_PYTHON3 as __IS_PYTHON3__
    from pycompss.api.exceptions import COMPSsException as __COMPSsException__

    if __IS_PYTHON3__:
        listType = list
        dictType = dict
    else:
        import types

        listType = types.ListType
        dictType = types.DictType

    def compss_start(log_level='off', interactive=False):
        """
        Starts the runtime.

        :return: None
        """
        __start_runtime__(log_level, interactive)

    def compss_stop(code=0):
        """
        Stops the runtime.

        :return: None
        """
        __stop_runtime__(code)

    def compss_file_exists(file_name):
        """
        Check if a file exists. If it does not exist, it check
        if file has been accessed before by calling the runtime.

        :param file_name: File name.
        :return: True, either the file exists or has been accessed by the
                 runtime.
        """
        from os import path
        if not path.exists(file_name):
            return __accessed_file__(file_name)
        else:
            return True

    def compss_open(file_name, mode='r'):
        """
        Open a file -> Calls runtime.

        :param file_name: File name.
        :param mode: Open mode. Options = [w, r+ or a , r or empty]. Default=r
        :return: An object of 'file' type.
        :raise IOError: If the file can not be opened.
        """
        compss_name = __open_file__(file_name, mode)
        return open(compss_name, mode)

    def compss_delete_file(file_name):
        """
        Delete a file -> Calls runtime.

        :param file_name: File name.
        :return: True if success. False otherwise.
        """
        return __delete_file__(file_name)

    def compss_wait_on_file(file_name):
        """
        Gets a file -> Calls runtime.

        :param file_name: File name.
        :return: True if success. False otherwise.
        """
        return __get_file__(file_name)

    def compss_wait_on_directory(directory_name):
        """
        Gets a directory -> Calls runtime.

        :param directory_name: Directory name.
        :return: True if success. False otherwise.
        """
        return __get_directory__(directory_name)

    def compss_delete_object(obj):
        """
        Delete object used within COMPSs,

        :param obj: Object to delete.
        :return: True if success. False otherwise.
        """
        return __delete_object__(obj)

    def compss_barrier(no_more_tasks=False):
        """
        Perform a barrier when called.
        Stop until all the submitted tasks have finished.

        :param no_more_tasks: No more tasks boolean
        """
        __barrier__(no_more_tasks)

    def compss_barrier_group(group_name):
        """
        Perform a barrier to a group when called.
        Stop until all the tasks of a group have finished.

        :param group_name: Name of the group to wait
        """

        exception_message = __barrier_group__(group_name)
        if exception_message is not None:
            raise __COMPSsException__(exception_message)

    def compss_wait_on(*args, **kwargs):
        """
        Wait for objects.

        :param args: Objects to wait on
        :return: List with the final values.
        """
        return __wait_on__(*args, **kwargs)

    def compss_get_number_of_resources():
        """
        Request for the number of active resources.

        :return: The number of active resources
            +type: <int>
        """
        return __get_number_of_resources__()

    def compss_request_resources(num_resources, group_name):
        """
        Requests the creation of num_resources resources.

        :param num_resources: Number of resources to create.
            +type: <int>
        :param group_name: Task group to notify upon resource creation
            +type: <str> or None
        :return: None
        """
        __request_resources__(num_resources, group_name)

    def compss_free_resources(num_resources, group_name):
        """
        Requests the destruction of num_resources resources.

        :param num_resources: Number of resources to destroy.
            +type: <int>
        :param group_name: Task group to notify upon resource creation
            +type: <str> or None
        :return: None
        """
        __free_resources__(num_resources, group_name)

    class TaskGroup(object):
        def __init__(self, group_name, implicit_barrier=True):
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

    def compss_start(log_level='off', interactive=False):
        __dummy_compss_start__(log_level, interactive)

    def compss_stop():
        __dummy_compss_stop__()

    def compss_file_exists(file_name):
        return __dummy_compss_file_exists__(file_name)

    def compss_open(file_name, mode='r'):
        return __dummy_compss_open__(file_name, mode)

    def compss_delete_file(file_name):
        return __dummy_compss_delete_file__(file_name)

    def compss_wait_on_file(file_name):
        return __dummy_compss_wait_on_file__(file_name)

    def compss_delete_object(obj):
        return __dummy_compss_delete_object__(obj)

    def compss_barrier(no_more_tasks=False):
        __dummy_compss_barrier__(no_more_tasks)

    def compss_barrier_group(group_name):
        __dummy_compss_barrier_group__(group_name)

    def compss_wait_on(*args):
        return __dummy_compss_wait_on__(*args)

    def compss_get_number_of_resources():
        return __dummy_compss_get_number_of_resources__()

    def compss_request_resources(num_resources, group_name):
        __dummy_compss_request_resources__(num_resources, group_name)

    def compss_free_resources(num_resources, group_name):
        __dummy_compss_free_resources__(num_resources, group_name)
