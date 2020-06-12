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
PyCOMPSs Tracing helpers
=========================
    This file contains a set of context managers and decorators to ease the
    tracing events emission.
"""
import time
from functools import wraps
from contextlib import contextmanager
from pycompss.worker.commons.constants import SYNC_EVENTS
from pycompss.worker.commons.constants import WORKER_EVENTS
from pycompss.worker.commons.constants import WORKER_RUNNING_EVENT

PYEXTRAE = None
TRACING = False


@contextmanager
def dummy_context():
    """
    Context which deactivates and nothing else.

    :return: None
    """
    global TRACING
    TRACING = False
    yield


@contextmanager
def trace_multiprocessing_worker():
    """
    Sets up the tracing for the multiprocessing worker.

    :return: None
    """
    global PYEXTRAE
    global TRACING
    import pyextrae.multiprocessing as pyextrae  # noqa
    PYEXTRAE = pyextrae
    TRACING = True
    pyextrae.eventandcounters(SYNC_EVENTS, 1)
    pyextrae.eventandcounters(WORKER_EVENTS, WORKER_RUNNING_EVENT)
    yield  # here the worker runs
    pyextrae.eventandcounters(WORKER_EVENTS, 0)
    pyextrae.eventandcounters(SYNC_EVENTS, 0)
    pyextrae.eventandcounters(SYNC_EVENTS, int(time.time()))
    pyextrae.eventandcounters(SYNC_EVENTS, 0)


@contextmanager
def trace_mpi_worker():
    """
    Sets up the tracing for the mpi worker.

    :return: None
    """
    global PYEXTRAE
    global TRACING
    import pyextrae.mpi as pyextrae  # noqa
    PYEXTRAE = pyextrae
    TRACING = True
    pyextrae.eventandcounters(SYNC_EVENTS, 1)
    pyextrae.eventandcounters(WORKER_EVENTS, WORKER_RUNNING_EVENT)
    yield  # here the worker runs
    pyextrae.eventandcounters(WORKER_EVENTS, 0)
    pyextrae.eventandcounters(SYNC_EVENTS, 0)
    pyextrae.eventandcounters(SYNC_EVENTS, int(time.time()))
    pyextrae.eventandcounters(SYNC_EVENTS, 0)


@contextmanager
def trace_mpi_executor():
    """
    Sets up the tracing for each mpi executor.

    :return: None
    """
    global PYEXTRAE
    global TRACING
    import pyextrae.mpi as pyextrae  # noqa
    PYEXTRAE = pyextrae
    TRACING = True
    yield  # here the mpi executor runs


def emit_event(event_id):
    """
    Simple decorator to emit events

    :param event_id: Event identifier to emit
    :return: Wraps the function with eventandcounter if tracing is active.
    """
    def actual_decorator(function):
        @wraps(function)
        def wrapped(*args, **kwargs):
            result = None
            if TRACING:
                PYEXTRAE.eventandcounters(WORKER_EVENTS, event_id)
                result = function(*args, **kwargs)
                PYEXTRAE.eventandcounters(WORKER_EVENTS, 0)
            else:
                result = function(*args, **kwargs)
            return result
        return wrapped
    return actual_decorator


@contextmanager
def event(event_id):
    """
    Emits an event wrapping the desired code.
    Does nothing if tracing is disabled

    :return: None
    """
    if TRACING:
        PYEXTRAE.eventandcounters(WORKER_EVENTS, event_id)
    yield  # here the code runs
    if TRACING:
        PYEXTRAE.eventandcounters(WORKER_EVENTS, 0)
