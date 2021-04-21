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
PyCOMPSs Util - Context
=======================
    This file contains the methods to detect the origin of the call stack.
    Useful to detect if we are in the master or in the worker.
"""

import inspect
import typing
from contextlib import contextmanager

MASTER = "MASTER"
WORKER = "WORKER"
OUT_OF_SCOPE = "OUT_OF_SCOPE"

_WHO = OUT_OF_SCOPE    # type: typing.Any
_WHERE = OUT_OF_SCOPE  # type: typing.Any

NESTING = False
LOADING = False
TO_REGISTER = []


def in_master():
    # type: () -> bool
    """
    Determine if the execution is being performed in the master node

    :return:  <Boolean> - True if in master. False on the contrary.
    """
    return _WHERE == MASTER


def in_worker():
    # type: () -> bool
    """
    Determine if the execution is being performed in a worker node.

    :return:  <Boolean> - True if in worker. False on the contrary.
    """
    return _WHERE == WORKER


def in_pycompss():
    # type: () -> bool
    """
    Determine if the execution is being performed within the PyCOMPSs scope.

    :return:  <Boolean> - True if under scope. False on the contrary.
    """
    return _WHERE != OUT_OF_SCOPE


def set_pycompss_context(where):
    # type: (str) -> None
    """
    Set the Python Binding context (MASTER or WORKER or OUT_OF_SCOPE)

    :param where: New context (MASTER or WORKER or OUT_OF_SCOPE)
    :return: None
    """
    assert where in [MASTER, WORKER, OUT_OF_SCOPE], \
        "PyCOMPSs context must be %s, %s or %s" % \
        (MASTER, WORKER, OUT_OF_SCOPE)
    global _WHERE
    _WHERE = where
    global _WHO
    caller_stack = inspect.stack()[1]
    caller_module = inspect.getmodule(caller_stack[0])
    _WHO = caller_module


def get_pycompss_context():
    # type: () -> str
    """
    Returns PyCOMPSs context name.
    * For debugging purposes.

    :return: PyCOMPSs context name
    """
    return _WHERE


def get_who_contextualized():
    # type: () -> str
    """
    Returns PyCOMPSs contextualization caller.
    * For debugging purposes.

    :return: PyCOMPSs contextualization caller name
    """
    return _WHO


def is_nesting_enabled():
    # type: () -> bool
    """ Check if nesting is enabled.

    :returns: None
    """
    return NESTING is True


def enable_nesting():
    # type: () -> None
    """ Enable nesting.

    :returns: None
    """
    global NESTING
    NESTING = True


def disable_nesting():
    # type: () -> None
    """ Disable nesting.

    :returns: None
    """
    global NESTING
    NESTING = False


@contextmanager
def loading_context():
    # type: () -> typing.Iterator[None]
    """ Context which sets the loading mode (intended to be used only with
    the @implements decorators, since they try to register on loading).

    :return: None
    """
    __enable_loading__()
    yield
    __disable_loading__()


def is_loading():
    # type: () -> bool
    """ Check if is loading is enabled.

    :returns: None
    """
    return LOADING is True


def __enable_loading__():
    # type: () -> None
    """ Enable loading.

    :returns: None
    """
    global LOADING
    LOADING = True


def add_to_register_later(core_element):
    # type: (typing.Any) -> None
    """ Accumulate core elements to be registered later.

    :param core_element: Core element to be registered
    :return: None
    """
    global TO_REGISTER
    TO_REGISTER.append(core_element)


def get_to_register():
    # type: () -> list
    """ Retrieve the to register list.

    :return: To register list
    """
    return TO_REGISTER


def __disable_loading__():
    # type: () -> None
    """ Enable loading.

    :returns: None
    """
    global LOADING
    LOADING = False
