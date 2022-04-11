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
PyCOMPSs - dummy - api.

This file defines the public PyCOMPSs API functions without functionality.
"""

import os

from pycompss.util.typing_helper import typing


def compss_start(
    log_level: str = "off",
    tracing: bool = False,
    interactive: bool = False,
    disable_external: bool = False,
) -> None:
    """Start runtime dummy.

    Does nothing.

    :param log_level: Log level [ True | False ].
    :param tracing: Activate or disable tracing.
    :param interactive: Boolean if interactive (ipython or jupyter).
    :param disable_external: To avoid to load compss in external process.
    :return: None
    """
    pass


def compss_stop(code: int = 0, _hard_stop: bool = False) -> None:
    """Stop runtime dummy.

    Does nothing.

    :param code: Stop code.
    :param _hard_stop: Stop COMPSs when runtime has died.
    :return: None
    """
    pass


def compss_file_exists(*file_name: str) -> typing.Union[bool, typing.List[bool]]:
    """Check if file used in task exists dummy.

    Check if the file exists.

    :param file_name: The file/s name to check.
    :return: True if exists. False otherwise.
    """
    if len(file_name) == 1:
        return os.path.exists(file_name[0])
    else:
        return [os.path.exists(f_name) for f_name in file_name]


def compss_open(file_name: str, mode: str = "r") -> typing.Any:
    """Open a file used in task dummy.

    Open the given file with the defined mode (see builtin open).

    :param file_name: The file name to open.
    :param mode: Open mode. Options = [w, r+ or a, r or empty]. Default=r.
    :return: An object of "file" type.
    :raise IOError: If the file can not be opened.
    """
    return open(file_name, mode)


def compss_delete_file(*file_name: str) -> typing.Union[bool, typing.List[bool]]:
    """Delete a file used in task dummy.

    Does nothing and always return True.

    :param file_name: File/s name.
    :return: Always True.
    """
    if len(file_name) == 1:
        return True
    else:
        return [True] * len(file_name)


def compss_wait_on_file(*file_name: str) -> None:
    """Wait on file used in task dummy.

    Does nothing.

    :param file_name: File/s name.
    :return: None
    """
    return None


def compss_wait_on_directory(*directory_name: str) -> None:
    """Wait on directory used in task dummy.

    Does nothing.

    :param directory_name: Directory/ies name.
    :return: None
    """
    return None


def compss_delete_object(*obj: typing.Any) -> typing.Union[bool, typing.List[bool]]:
    """Delete object used in task dummy.

    Does nothing and always return True.

    :param obj: Object/s to delete.
    :return: Always True.
    """
    if len(obj) == 1:
        return True
    else:
        return [True] * len(obj)


def compss_barrier(no_more_tasks: bool = False) -> None:
    """Wait for all submitted tasks dummy.

    Does nothing.

    :param no_more_tasks: No more tasks boolean.
    :return: None
    """
    pass


def compss_barrier_group(group_name: str) -> None:
    """Wait for all submitted tasks of a group dummy.

    Does nothing.

    :param group_name: Name of the group.
    :return: None
    """
    pass


def compss_wait_on(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
    """Synchronize an object used in task dummy.

    Does nothing.

    :param args: Objects to wait on.
    :param kwargs: Options dictionary.
    :return: The same objects defined as parameter.
    """
    ret = list(map(lambda o: o, args))
    if len(ret) == 1:
        return ret[0]
    else:
        return ret


def compss_get_number_of_resources() -> int:
    """Request for the number of active resources dummy.

    Does nothing.

    :return: The number of active resources
    """
    return 1


def compss_request_resources(
    num_resources: int, group_name: typing.Optional[str]
) -> None:
    """Request the creation of num_resources resources dummy.

    Does nothing.

    :param num_resources: Number of resources to create.
    :param group_name: Task group to notify upon resource creation
    :return: None
    """
    pass


def compss_free_resources(num_resources: int, group_name: typing.Optional[str]) -> None:
    """Request the destruction of num_resources resources dummy.

    Does nothing.

    :param num_resources: Number of resources to destroy.
    :param group_name: Task group to notify upon resource creation
    :return: None
    """
    pass


def compss_set_wall_clock(wall_clock_limit: int) -> None:
    """Set the application wall_clock_limit dummy.

    Does nothing.

    :param wall_clock_limit: Wall clock limit in seconds.
    :return: None
    """
    pass


class TaskGroup(object):
    """Dummy TaskGroup context manager."""

    def __init__(self, group_name: str, implicit_barrier: bool = True):
        """Define a new group of tasks.

        :param group_name: Group name.
        :param implicit_barrier: Perform implicit barrier.
        """
        pass

    def __enter__(self) -> None:
        """Do nothing.

        :return: None
        """
        pass

    def __exit__(
        self, type: typing.Any, value: typing.Any, traceback: typing.Any
    ) -> None:
        """Do nothing.

        :return: None
        """
        pass
