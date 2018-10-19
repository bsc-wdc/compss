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
from pycompss.runtime.commons import IS_PYTHON3
from pycompss.runtime.commons import IS_INTERACTIVE

# wtf is this
DECORATORS_TO_CHECK = ['pycompss/api/binary.py',
                       'pycompss/api/constraint.py',
                       'pycompss/api/decaf.py',
                       'pycompss/api/implement.py',
                       'pycompss/api/mpi.py',
                       'pycompss/api/ompss.py',
                       'pycompss/api/opencl.py',
                       'pycompss/api/parallel.py',
                       'pycompss/api/task.py']

MASTER = 'MASTER'
WORKER = 'WORKER'
OUTOFSCOPE = 'OUTOFSCOPE'

_WHERE = OUTOFSCOPE

def in_master():
    '''Determine if the execution is being performed in the master node
    '''
    return _WHERE == MASTER

def in_worker():
    '''Determine if the execution is being performed in a worker node.
    '''
    return _WHERE == WORKER

def in_pycompss():
    '''Determine if the execution is being performed within the PyCOMPSs scope.
    :return:  <Boolean> - True if under scope. False on the contrary.
    '''
    return _WHERE != OUTOFSCOPE

def set_pycompss_context(where):
    '''Set the Python Binding context (MASTER OR WORKER)
    :param where: New context (MASTER or WORKER)
    :return: None
    '''
    assert where in [MASTER, WORKER], 'PyCOMPSs context should be %s or %s' % (MASTER, WORKER)
    global _WHERE
    _WHERE = where

def get_pycompss_context():
    '''Returns PyCOMPSs context name.
    For debugging purposes.
    :return: PyCOMPSs context name
    '''
    return _WHERE