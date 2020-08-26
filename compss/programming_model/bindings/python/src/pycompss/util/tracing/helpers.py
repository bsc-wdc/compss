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
from pycompss.worker.commons.constants import TASK_EVENTS
from pycompss.worker.commons.constants import WORKER_EVENTS
from pycompss.worker.commons.constants import WORKER_RUNNING_EVENT
from pycompss.runtime.constants import MASTER_EVENTS

PYEXTRAE = None
TRACING = False


@contextmanager
def dummy_context():
    # type: () -> None
    """ Context which deactivates the tracing flag and nothing else.

    :return: None
    """
    global TRACING
    TRACING = False
    yield


@contextmanager
def trace_multiprocessing_worker():
    # type: () -> None
    """ Sets up the tracing for the multiprocessing worker.

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
    # type: () -> None
    """ Sets up the tracing for the mpi worker.

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
    # type: () -> None
    """ Sets up the tracing for each mpi executor.

    :return: None
    """
    global PYEXTRAE
    global TRACING
    import pyextrae.mpi as pyextrae  # noqa
    PYEXTRAE = pyextrae
    TRACING = True
    yield  # here the mpi executor runs


def emit_event(event_id, master=False):
    """
    Simple decorator to emit worker events.

    :param event_id: Event identifier to emit.
    :param master: If the event is emitted as master.
    :return: Wraps the function with eventandcounter if tracing is active.
    """
    event_group = MASTER_EVENTS if master else WORKER_EVENTS

    def worker_tracing_decorator(function):
        @wraps(function)
        def worker_tracing_wrapper(*args, **kwargs):
            if TRACING:
                PYEXTRAE.eventandcounters(event_group, event_id)  # noqa
                result = function(*args, **kwargs)
                PYEXTRAE.eventandcounters(event_group, 0)         # noqa
            else:
                result = function(*args, **kwargs)
            return result
        return worker_tracing_wrapper
    return worker_tracing_decorator


@contextmanager
def event(event_id, master=False, inside=False):
    # type: (int, bool, bool) -> None
    """ Emits an event wrapping the desired code.

    Does nothing if tracing is disabled.

    :param event_id: Event identifier to emit.
    :param master: If the event is emitted as master.
    :param inside: If the event is produced inside the worker.
    :return: None
    """
    if master:
        event_group = MASTER_EVENTS
    else:
        if inside:
            event_group = TASK_EVENTS
        else:
            event_group = WORKER_EVENTS
    if TRACING:
        PYEXTRAE.eventandcounters(event_group, event_id)  # noqa
    yield  # here the code runs
    if TRACING:
        PYEXTRAE.eventandcounters(event_group, 0)         # noqa


def emit_manual_event(event_id, master=False):
    # type: (int, bool) -> None
    """ Emits a single event with the desired code.

    Does nothing if tracing is disabled.

    :param event_id: Event identifier to emit.
    :param master: If the event is emitted as master.
    :return: None
    """
    event_group = MASTER_EVENTS if master else WORKER_EVENTS
    if TRACING:
        PYEXTRAE.eventandcounters(event_group, event_id)  # noqa


def enable_trace_master():
    # type: () -> None
    """ Enables tracing for the master process.

    :return: None
    """
    global PYEXTRAE
    global TRACING
    import pyextrae.sequential as pyextrae  # noqa
    PYEXTRAE = pyextrae
    TRACING = True
