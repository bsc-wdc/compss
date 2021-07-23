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
PyCOMPSs Dummy API
==================
    This file defines the public PyCOMPSs API functions without functionality.
    It implements a dummy compss_open and compss_wait_on functions.
"""

import os
import typing


def compss_start(log_level="off", tracing=0, interactive=False):  # noqa
    # type: (str, int, bool) -> None
    """ Dummy runtime start.

    Does nothing.

    :param log_level: Log level ["trace"|"debug"|"info"|"api"|"off"].
    :param interactive: Boolean if interactive (ipython or jupyter).
    :return: None
    """
    pass


def compss_stop(code=0, _hard_stop=False):  # noqa
    # type: (int, bool) -> None
    """ Dummy runtime stop.

    Does nothing.

    :param code: Stop code.
    :param _hard_stop: Stop compss when runtime has died.
    :return: None
    """
    pass


def compss_file_exists(*file_name):
    # type: (*str) -> typing.Union[bool, typing.List[bool]]
    """ Dummy compss_file_exists.

    Check if the file exists.

    :param file_name: The file/s name to check.
    :return: True if exists. False otherwise.
    """
    if len(file_name) == 1:
        return os.path.exists(file_name[0])
    else:
        return [os.path.exists(f_name) for f_name in file_name]


def compss_open(file_name, mode="r"):
    # type: (str, str) -> typing.Any
    """ Dummy compss_open.

    Open the given file with the defined mode (see builtin open).

    :param file_name: The file name to open.
    :param mode: Open mode. Options = [w, r+ or a , r or empty]. Default=r.
    :return: An object of "file" type.
    :raise IOError: If the file can not be opened.
    """
    return open(file_name, mode)


def compss_delete_file(*file_name):  # noqa
    # type: (*str) -> typing.Union[bool, typing.List[bool]]
    """ Dummy compss_delete.

    Does nothing and always return True.

    :param file_name: File/s name.
    :return: Always True.
    """
    if len(file_name) == 1:
        return True
    else:
        return [True] * len(file_name)


def compss_wait_on_file(*file_name):  # noqa
    # type: (*str) -> None
    """ Dummy compss_wait_on_file.

    Does nothing.

    :param file_name: File/s name.
    :return: None
    """
    return None


def compss_wait_on_directory(*directory_name):  # noqa
    # type: (*str) -> None
    """ Dummy compss_wait_on_directory.

    Does nothing.

    :param directory_name: Directory/ies name.
    :return: None
    """
    return None


def compss_delete_object(*obj):  # noqa
    # type: (*typing.Any) -> typing.Union[bool, typing.List[bool]]
    """ Dummy compss_delete_object.

    Does nothing and always return True.

    :param obj: Object/s to delete.
    :return: Always True.
    """
    if len(obj) == 1:
        return True
    else:
        return [True] * len(obj)


def compss_barrier(no_more_tasks=False):  # noqa
    # type: (bool) -> None
    """ Dummy barrier.

    Does nothing.

    :param no_more_tasks: No more tasks boolean.
    :return: None
    """
    pass


def compss_barrier_group(group_name):  # noqa
    # type: (str) -> None
    """ Dummy barrier for groups.

    Does nothing.

    :param group_name: Name of the group.
    :return: None
    """
    pass


def compss_wait_on(*args, **kwargs):  # noqa
    # type: (*typing.Any, **typing.Any) -> typing.Any
    """ Dummy compss_wait_on.

    Does nothing.

    :param args: Objects to wait on.
    :param kwargs: Options dictionary.
    :return: The same objects defined as parameter.
    """
    ret = list(map(lambda o: o, args))
    ret = ret[0] if len(ret) == 1 else ret
    return ret


def compss_get_number_of_resources():
    # type: () -> int
    """ Request for the number of active resources.

    Does nothing.

    :return: The number of active resources
    """
    return 1


def compss_request_resources(num_resources, group_name):  # noqa
    # type: (int, str) -> None
    """ Requests the creation of num_resources resources.

    Does nothing.

    :param num_resources: Number of resources to create.
    :param group_name: Task group to notify upon resource creation
    :return: None
    """
    pass


def compss_free_resources(num_resources, group_name):  # noqa
    # type: (int, str) -> None
    """ Requests the destruction of num_resources resources.

    Does nothing.

    :param num_resources: Number of resources to destroy.
    :param group_name: Task group to notify upon resource creation
    :return: None
    """
    pass


def compss_set_wall_clock(wall_clock_limit):  # noqa
    # type: (int) -> None
    """ Sets the application wall_clock_limit.

    Does nothing.

    :param wall_clock_limit: Wall clock limit in seconds.
    :return: None
    """
    pass


class TaskGroup(object):
    """
    Dummy TaskGroup context manager.
    """
    def __init__(self, group_name, implicit_barrier=True):  # noqa
        # type: (str, bool) -> None
        """ Define a new group of tasks.

        :param group_name: Group name.
        :param implicit_barrier: Perform implicit barrier.
        """
        pass

    def __enter__(self):
        # Dummy: do nothing
        pass

    def __exit__(self, type, value, traceback):  # noqa
        # Dummy: do nothing
        pass
