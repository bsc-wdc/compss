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

    from pycompss.runtime.binding import start_runtime
    from pycompss.runtime.binding import stop_runtime
    from pycompss.runtime.binding import accessed_file
    from pycompss.runtime.binding import open_file
    from pycompss.runtime.binding import delete_file
    from pycompss.runtime.binding import get_file
    from pycompss.runtime.binding import get_directory
    from pycompss.runtime.binding import delete_object
    from pycompss.runtime.binding import barrier
    from pycompss.runtime.binding import barrier_group
    from pycompss.runtime.binding import open_task_group
    from pycompss.runtime.binding import close_task_group
    from pycompss.runtime.binding import wait_on
    from pycompss.runtime.binding import get_compss_mode
    from pycompss.runtime.commons import IS_PYTHON3
    from pycompss.api.exceptions import COMPSsException

    if IS_PYTHON3:
        listType = list
        dictType = dict
    else:
        import types
        listType = types.ListType
        dictType = types.DictType

    def compss_start():
        """
        Starts the runtime.

        :return: None
        """
        start_runtime()

    def compss_stop(code=0):
        """
        Stops the runtime.

        :return: None
        """
        stop_runtime(code)

    def compss_file_exists(file_name):
        """
        Check if a file exists. If it does not exist, it check 
        if file has been accessed before by calling the runtime.

        :param file_name: File name.
        :return: True, either the file exists or has been accessed by the runtime.
        """
        from os import path
        if (not path.exists(file_name)):
            return accessed_file(file_name)
        else :
            return True
    
    def compss_open(file_name, mode='r'):
        """
        Open a file -> Calls runtime.

        :param file_name: File name.
        :param mode: Open mode. Options = [w, r+ or a , r or empty]. Default=r
        :return: An object of 'file' type.
        :raise IOError: If the file can not be opened.
        """
        compss_mode = get_compss_mode(mode)
        compss_name = open_file(file_name, compss_mode)
        return open(compss_name, mode)

    def compss_delete_file(file_name):
        """
        Delete a file -> Calls runtime.

        :param file_name: File name.
        :return: True if success. False otherwise.
        """
        return delete_file(file_name)

    def compss_wait_on_file(file_name):
        """
        Gets a file -> Calls runtime.

        :param file_name: File name.
        :return: True if success. False otherwise.
        """
        return get_file(file_name)

    def compss_wait_on_directory(dir_name):
        """

        :param dir_name: Directory name.
        :return: True if success. False otherwise.
        """
        return get_directory(dir_name)

    def compss_delete_object(obj):
        """
        Delete object used within COMPSs,

        :param obj: Object to delete.
        :return: True if success. False otherwise.
        """
        return delete_object(obj)

    def compss_barrier(no_more_tasks=False):
        """
        Perform a barrier when called.
        Stop until all the submitted tasks have finished.

        :param no_more_tasks: No more tasks boolean
        """
        barrier(no_more_tasks)

    def compss_barrier_group(group_name):
        """
        Perform a barrier to a group when called.
        Stop until all the tasks of a group have finished.

        :param group_name: Name of the group to wait
        """

        exception_message = barrier_group(group_name)
        if exception_message is not None:
            raise COMPSsException(exception_message)

    def compss_wait_on(*args, **kwargs):
        """
        Wait for objects.

        :param args: Objects to wait on
        :return: List with the final values.
        """
        return wait_on(*args, **kwargs)

    class TaskGroup(object):
        def __init__(self, group_name, implicit_barrier=True):
            self.group_name = group_name
            self.implicit_barrier = implicit_barrier

        def __enter__(self):
            # Group creation
            open_task_group(self.group_name, self.implicit_barrier)

        def __exit__(self, type, value, traceback):
            # Group closing
            close_task_group(self.group_name)
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
    from pycompss.api.dummy.api import compss_open_task_group as \
        __dummy_compss_open_task_group__
    from pycompss.api.dummy.api import compss_close_task_group as \
        __dummy_compss_close_task_group__

    def compss_start():
        __dummy_compss_start__()

    def compss_stop():
        __dummy_compss_stop__()

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

    def compss_open_task_group(group_name, implicit_barrier):
        return __dummy_compss_open_task_group__(group_name, implicit_barrier)

    def compss_close_task_group(group_name):
        return __dummy_compss_close_task_group__(group_name)
