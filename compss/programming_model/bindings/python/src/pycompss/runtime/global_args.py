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
PyCOMPSs Binding - Globals
==========================
    This file contains the global variables used in the Python binding.
"""

from pycompss.runtime.task.parameter import Parameter  # noqa # typing purposes
from pycompss.util.typing_helper import typing

# GLOBALS
# Worker arguments received on the task call
__WORKER_ARGS__ = tuple()  # type: tuple


def set_worker_args(worker_args: tuple) -> None:
    """Worker arguments to save in WORKER_ARGS.

    :param worker_args: Worker arguments
    :return: None
    """
    global __WORKER_ARGS__
    __WORKER_ARGS__ = worker_args


def get_worker_args() -> tuple:
    """Retrieve the worker arguments.

    :return: Worker arguments
    """
    return __WORKER_ARGS__


def update_worker_argument_parameter_content(
    name: typing.Optional[str], content: typing.Any
) -> None:
    """Update the Parameter's content for the given name.

    :param name: Parameter name
    :param content: New content
    :return: None
    """
    if name:
        for param in __WORKER_ARGS__:
            if (
                not param.is_collection()
                and not param.is_dict_collection()
                and param.name == name
            ):
                param.content = content
                return


def delete_worker_args() -> None:
    """Remove the worker args global variable.

    :return: None
    """
    global __WORKER_ARGS__
    del __WORKER_ARGS__
