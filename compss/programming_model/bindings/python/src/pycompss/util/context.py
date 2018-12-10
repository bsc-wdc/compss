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
PyCOMPSs Utils - Context
===================
    This file contains the methods to detect the origin of the call stack.
    Useful to detect if we are in the master or in the worker.
"""

import inspect

MASTER = 'MASTER'
WORKER = 'WORKER'
OUTOFSCOPE = 'OUTOFSCOPE'

_WHO = OUTOFSCOPE
_WHERE = OUTOFSCOPE


def in_master():
    """
    Determine if the execution is being performed in the master node
    :return:  <Boolean> - True if in master. False on the contrary.
    """
    return _WHERE == MASTER


def in_worker():
    """
    Determine if the execution is being performed in a worker node.
    :return:  <Boolean> - True if in worker. False on the contrary.
    """
    return _WHERE == WORKER


def in_pycompss():
    """
    Determine if the execution is being performed within the PyCOMPSs scope.
    :return:  <Boolean> - True if under scope. False on the contrary.
    """
    return _WHERE != OUTOFSCOPE


def set_pycompss_context(where):
    """
    Set the Python Binding context (MASTER or WORKER or INITIALIZATION)
    :param where: New context (MASTER or WORKER or INITIALIZATION)
    :return: None
    """
    assert where in [MASTER, WORKER], 'PyCOMPSs context must be %s or %s' % (MASTER, WORKER)
    global _WHERE
    _WHERE = where
    global _WHO
    caller_stack = inspect.stack()[1]
    caller_module = inspect.getmodule(caller_stack[0])
    _WHO = caller_module


def get_pycompss_context():
    """
    Returns PyCOMPSs context name.
    * For debugging purposes.
    :return: PyCOMPSs context name
    """
    return _WHERE


def get_who_contextualized():
    """
    Returns PyCOMPSs contextualization caller.
    * For debugging purposes.
    :return: PyCOMPSs contextualization caller name
    """
    return _WHO