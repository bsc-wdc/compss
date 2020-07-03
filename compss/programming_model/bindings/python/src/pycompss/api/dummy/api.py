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
PyCOMPSs Dummy API
==================
    This file defines the public PyCOMPSs API functions without functionality.
    It implements a dummy compss_open and compss_wait_on functions.
"""
from os import path


def compss_start(log_level='off', interactive=False):  # noqa
    """
    Dummy runtime starter.

    :param log_level: Log level [ 'trace' | 'debug' | 'info' | 'api' | 'off' ]
    :param interactive: Boolean if interactive (ipython or jupyter)
    :return: None
    """
    pass


def compss_stop():
    """
    Dummy runtime stopper.

    :return: None
    """
    pass


def compss_file_exists(file_name):
    """
    Dummy compss_file_exists.

    :param file_name: The file name to check
    :return: True if exists
    """
    return path.exists(file_name)


def compss_open(file_name, mode='r'):
    """
    Dummy compss_open.

    :param file_name: The file name to open
    :param mode: Open mode. Options = [w, r+ or a , r or empty]. Default=r
    :return: An object of 'file' type.
    :raise IOError: If the file can not be opened.
    """
    return open(file_name, mode)


def compss_delete_file(file_name):  # noqa
    """
    Dummy compss_delete.

    :param file_name: File name.
    :return: Always True.
    """
    return True


def compss_delete_object(obj):  # noqa
    """
    Dummy compss_delete_object.

    :param obj: Object.
    :return: Always True.
    """
    return True


def compss_wait_on_file(file_name):  # noqa
    """
    Delete a file -> Calls runtime.

    :param file_name: File name.
    :return: True if success. False otherwise.
    """
    return True


def compss_barrier(no_more_tasks=False):  # noqa
    """
    Dummy barrier.

    :param no_more_tasks: No more tasks boolean
    :return: None
    """
    pass


def compss_barrier_group(group_name):  # noqa
    """
    Dummy barrier for groups.

    :param group_name: Name of the group.
    :return: None
    """
    pass


def compss_wait_on(*args):
    """
    Dummy compss_wait_on

    :param args: Objects to wait on
    :return: The same objects defined as parameter
    """
    ret = list(map(lambda o: o, args))
    ret = ret[0] if len(ret) == 1 else ret
    return ret


def compss_get_number_of_resources():
    """
    Request for the number of active resources.

    :return: The number of active resources
        +type: <int>
    """
    return 1


def compss_request_resources(num_resources, group_name):  # noqa
    """
    Requests the creation of num_resources resources.

    :param num_resources: Number of resources to create.
        +type: <int>
    :param group_name: Task group to notify upon resource creation
        +type: <str> or None
    :return: None
    """
    pass


def compss_free_resources(num_resources, group_name):  # noqa
    """
    Requests the destruction of num_resources resources.

    :param num_resources: Number of resources to destroy.
        +type: <int>
    :param group_name: Task group to notify upon resource creation
        +type: <str> or None
    :return: None
    """
    pass


class TaskGroup(object):
    """
    Dummy TaskGroup context manager.
    """
    def __init__(self, group_name, implicit_barrier=True):  # noqa
        pass

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):  # noqa
        pass
