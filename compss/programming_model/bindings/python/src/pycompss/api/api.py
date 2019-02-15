#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
        - delete object
        - barrier
        - wait_on
    functions.
    Also includes the redirection to the dummy API.

    CAUTION: If the context has not been defined, it will load the dummy API.
"""

import pycompss.util.context as context

if context.in_pycompss():
    # ################################################################# #
    #                PyCOMPSs API definitions                           #
    # Any change on this API must be considered within the dummy API.   #
    # ################################################################# #

    from pycompss.runtime.binding import start_runtime
    from pycompss.runtime.binding import stop_runtime
    from pycompss.runtime.binding import get_file
    from pycompss.runtime.binding import delete_file
    from pycompss.runtime.binding import delete_object
    from pycompss.runtime.binding import barrier
    from pycompss.runtime.binding import synchronize
    from pycompss.runtime.binding import get_compss_mode
    from pycompss.runtime.binding import pending_to_synchronize
    from pycompss.runtime.binding import Future
    from pycompss.runtime.binding import EmptyReturn
    from pycompss.runtime.commons import IS_PYTHON3

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


    def compss_stop():
        """
        Stops the runtime.

        :return: None
        """
        stop_runtime()


    def compss_open(file_name, mode='r'):
        """
        Open a file -> Calls runtime.

        :param file_name: File name.
        :param mode: Open mode. Options = [w, r+ or a , r or empty]. Default = r
        :return: An object of 'file' type.
        :raise IOError: If the file can not be opened.
        """

        compss_mode = get_compss_mode(mode)
        compss_name = get_file(file_name, compss_mode)
        return open(compss_name, mode)


    def compss_delete_file(file_name):
        """
        Delete a file -> Calls runtime.

        :param file_name: File name.
        :return: True if success. False otherwise.
        """

        return delete_file(file_name)


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


    def compss_wait_on(*args):
        """
        Wait for objects.

        :param args: Objects to wait on
        :return: List with the final values.
        """

        def _compss_wait_on(obj, to_write=False):
            """
            Waits on an object.

            :param obj: Object to wait on.
            :param to_write: Write enable?. Options = [True, False]. Default = True
            :return: An object of 'file' type.
            """

            # print("Waiting on", obj)
            if to_write:
                mode = 'r+'
            else:
                mode = 'r'
            compss_mode = get_compss_mode(mode)

            # Private function used below (recursively)
            def wait_on_iterable(iter_obj):
                """
                Wait on an iterable object.
                Currently supports lists and dictionaries (syncs the values).
                :param iter_obj: iterable object
                :return: synchronized object
                """
                # check if the object is in our pending_to_synchronize dictionary
                from pycompss.runtime.binding import get_object_id
                obj_id = get_object_id(iter_obj)
                if obj_id in pending_to_synchronize:
                    return synchronize(iter_obj, compss_mode)
                else:
                    if type(iter_obj) == list:
                        return [wait_on_iterable(x) for x in iter_obj]
                    elif type(iter_obj) == dict:
                        return {k: wait_on_iterable(v) for k, v in iter_obj.items()}
                    else:
                        return synchronize(iter_obj, compss_mode)

            if isinstance(obj, Future) or not (isinstance(obj, listType) or isinstance(obj, dictType)):
                return synchronize(obj, compss_mode)
            else:
                if len(obj) == 0:  # FUTURE OBJECT
                    return synchronize(obj, compss_mode)
                else:
                    # Will be a iterable object
                    res = wait_on_iterable(obj)
                    return res

        ret = list(map(_compss_wait_on, args))
        ret = ret[0] if len(ret) == 1 else ret
        # Check if there are empty elements return elements that need to be removed.
        if isinstance(ret, listType):
            # Look backwards the list removing the first EmptyReturn elements.
            for elem in reversed(ret):
                if isinstance(elem, EmptyReturn):
                    ret.remove(elem)
        return ret

else:
    # ################################################################# #
    #                    Dummmy API redirections                        #
    # ################################################################# #

    # Hidden imports
    from pycompss.api.dummy.api import compss_start as __dummy_compss_start__
    from pycompss.api.dummy.api import compss_stop as __dummy_compss_stop__
    from pycompss.api.dummy.api import compss_open as __dummy_compss_open__
    from pycompss.api.dummy.api import compss_delete_file as __dummy_compss_delete_file__
    from pycompss.api.dummy.api import compss_delete_object as __dummy_compss_delete_object__
    from pycompss.api.dummy.api import compss_barrier as __dummy_compss_barrier__
    from pycompss.api.dummy.api import compss_wait_on as __dummy_compss_wait_on__


    def compss_start():
        __dummy_compss_start__()


    def compss_stop():
        __dummy_compss_stop__()


    def compss_open(file_name, mode='r'):
        return __dummy_compss_open__(file_name, mode)


    def compss_delete_file(file_name):
        return __dummy_compss_delete_file__(file_name)


    def compss_delete_object(obj):
        return __dummy_compss_delete_object__(obj)


    def compss_barrier(no_more_tasks=False):
        __dummy_compss_barrier__(no_more_tasks)


    def compss_wait_on(*args):
        return __dummy_compss_wait_on__(*args)
