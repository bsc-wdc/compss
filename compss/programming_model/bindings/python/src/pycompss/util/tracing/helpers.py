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
PyCOMPSs Util - Tracing - Helpers.

This file contains a set of context managers and decorators to ease the
tracing events emission.
"""

import time
from contextlib import contextmanager

from pycompss.util.context import in_master
from pycompss.util.context import in_worker
from pycompss.util.tracing.types_events_master import TRACING_MASTER
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.util.typing_helper import typing

PYEXTRAE = None  # type: typing.Any
TRACING = False  # type: bool


@contextmanager
def dummy_context() -> typing.Iterator[None]:
    """Context which deactivates the tracing flag and nothing else.

    :return: None.
    """
    global TRACING
    TRACING = False
    yield


@contextmanager
def trace_multiprocessing_worker() -> typing.Iterator[None]:
    """Set up the tracing for the multiprocessing worker.

    :return: None.
    """
    global PYEXTRAE
    global TRACING
    import pyextrae.multiprocessing as pyextrae  # noqa

    PYEXTRAE = pyextrae
    TRACING = True
    pyextrae.eventandcounters(TRACING_WORKER.sync_type, 1)
    pyextrae.eventandcounters(
        TRACING_WORKER.inside_worker_type, TRACING_WORKER.worker_running_event
    )
    yield  # here the worker runs
    pyextrae.eventandcounters(TRACING_WORKER.inside_worker_type, 0)
    pyextrae.eventandcounters(TRACING_WORKER.sync_type, 0)
    pyextrae.eventandcounters(TRACING_WORKER.sync_type, int(time.time()))
    pyextrae.eventandcounters(TRACING_WORKER.sync_type, 0)


@contextmanager
def trace_mpi_worker() -> typing.Iterator[None]:
    """Set up the tracing for the mpi worker.

    :return: None.
    """
    global PYEXTRAE
    global TRACING
    import pyextrae.mpi as pyextrae  # noqa

    PYEXTRAE = pyextrae
    TRACING = True
    pyextrae.eventandcounters(TRACING_WORKER.sync_type, 1)
    pyextrae.eventandcounters(
        TRACING_WORKER.inside_worker_type, TRACING_WORKER.worker_running_event
    )
    yield  # here the worker runs
    pyextrae.eventandcounters(TRACING_WORKER.inside_worker_type, 0)
    pyextrae.eventandcounters(TRACING_WORKER.sync_type, 0)
    pyextrae.eventandcounters(TRACING_WORKER.sync_type, int(time.time()))
    pyextrae.eventandcounters(TRACING_WORKER.sync_type, 0)


@contextmanager
def trace_mpi_executor() -> typing.Iterator[None]:
    """Set up the tracing for each mpi executor.

    :return: None.
    """
    global PYEXTRAE
    global TRACING
    import pyextrae.mpi as pyextrae  # noqa

    PYEXTRAE = pyextrae
    TRACING = True
    yield  # here the mpi executor runs


# class EmitEvent(object):  # noqa
#
#     def __init__(self,
#                  event_id: int,
#                  master: bool = False,
#                  inside: bool = False,
#                  cpu_affinity: bool = False,
#                  gpu_affinity: bool = False
#                  ) -> None:
#         self.event_id = event_id
#         self.master = master
#         self.inside = inside
#         self.cpu_affinity = cpu_affinity
#         self.gpu_affinity = gpu_affinity
#
#     def __call__(self, f):
#         # type: (typing.Any) -> typing.Any
#         def wrapped_f(*args, **kwargs):
#             # type: (*typing.Any, **typing.Any) -> typing.Any
#             if TRACING:
#                 with event(self.event_id, self.master,
#                            self.inside, self.cpu_affinity, self.gpu_affinity):
#                     result = f(*args, **kwargs)
#             else:
#                 result = f(*args, **kwargs)
#             return result
#
#         return wrapped_f


# @contextmanager
# def event(event_id: int,
#           master: bool = False,
#           inside: bool = False,
#           cpu_affinity: bool = False,
#           gpu_affinity: bool = False
#           ) -> typing.Iterator[None]:
#     """ Emits an event wrapping the desired code.
#
#     Does nothing if tracing is disabled.
#
#     :param event_id: Event identifier to emit.
#     :param master: If the event is emitted as master.
#     :param inside: If the event is produced inside the worker.
#     :param cpu_affinity: If the event is produced inside the worker for
#                          cpu affinity.
#     :param gpu_affinity: If the event is produced inside the worker for
#                          gpu affinity.
#     :return: None
#     """
#     emit = False
#     if TRACING and in_master() and master:
#         emit = True
#     if TRACING and in_worker() and not master:
#         emit = True
#     if emit:
#         event_group, event_id = __get_proper_type_event__(event_id,
#                                                           master,
#                                                           inside,
#                                                           cpu_affinity,
#                                                           gpu_affinity)
#         PYEXTRAE.eventandcounters(event_group, event_id)  # noqa
#     yield  # here the code runs
#     if emit:
#         PYEXTRAE.eventandcounters(event_group, 0)         # noqa


class event_master:
    """Decorator that emits an event at master wrapping the desired code.

    Does nothing if tracing is disabled.

    :param event_id: Event identifier to emit.
    :return: None.
    """

    __slots__ = ["emitted"]

    def __init__(self, event_id: int) -> None:
        """Emit the given event identifier in the master group.

        :param event_id: Event identifier.
        :returns: None.
        """
        self.emitted = False
        if TRACING and in_master():
            PYEXTRAE.eventandcounters(TRACING_MASTER.binding_master_type, event_id)
            self.emitted = True

    def __enter__(self) -> None:
        """Do nothing.

        :returns: None.
        """
        pass

    def __exit__(
        self, type: typing.Any, value: typing.Any, traceback: typing.Any
    ) -> None:
        """Emit the 0 event in the master group when the context is finished.

        * Signature from context structure.

        :param type: Type.
        :param value: Value.
        :param traceback: Traceback.
        :returns: None.
        """
        if TRACING and self.emitted:
            PYEXTRAE.eventandcounters(TRACING_MASTER.binding_master_type, 0)


class event_worker:
    """Decorator that emits an event at worker wrapping the desired code.

    Does nothing if tracing is disabled.

    :param event_id: Event identifier to emit.
    :return: None.
    """

    __slots__ = ["emitted"]

    def __init__(self, event_id: int) -> None:
        """Emit the given event identifier in the worker group.

        :param event_id: Event identifier.
        :returns: None.
        """
        self.emitted = False
        if TRACING and in_worker():
            PYEXTRAE.eventandcounters(
                TRACING_WORKER.inside_worker_type, event_id
            )  # noqa
            self.emitted = True

    def __enter__(self) -> None:
        """Do nothing.

        :returns: None.
        """
        pass

    def __exit__(
        self, type: typing.Any, value: typing.Any, traceback: typing.Any
    ) -> None:
        """Emit the 0 event in the worker group when the context is finished.

        * Signature from context structure.

        :param type: Type.
        :param value: Value.
        :param traceback: Traceback.
        :returns: None.
        """
        if TRACING and self.emitted:
            PYEXTRAE.eventandcounters(TRACING_WORKER.inside_worker_type, 0)  # noqa


class event_inside_worker:
    """Decorator that emits an event at worker (inside task) wrapping the desired code.

    Does nothing if tracing is disabled.

    :param event_id: Event identifier to emit.
    :return: None.
    """

    __slots__ = ["emitted"]

    def __init__(self, event_id: int) -> None:
        """Emit the given event identifier in the inside worker group.

        :param event_id: Event identifier.
        :returns: None.
        """
        self.emitted = False
        if TRACING and in_worker():
            PYEXTRAE.eventandcounters(
                TRACING_WORKER.inside_tasks_type, event_id
            )  # noqa
            self.emitted = True

    def __enter__(self) -> None:
        """Do nothing.

        :returns: None.
        """
        pass

    def __exit__(
        self, type: typing.Any, value: typing.Any, traceback: typing.Any
    ) -> None:
        """Emit the 0 event in the inside worker group when the context is finished.

        * Signature from context structure.

        :param type: Type.
        :param value: Value.
        :param traceback: Traceback.
        :returns: None.
        """
        if TRACING and self.emitted:
            PYEXTRAE.eventandcounters(TRACING_WORKER.inside_tasks_type, 0)  # noqa


def emit_manual_event(
    event_id: int,
    master: bool = False,
    inside: bool = False,
    cpu_affinity: bool = False,
    gpu_affinity: bool = False,
    cpu_number: bool = False,
) -> None:
    """Emit a single event with the desired code.

    Does nothing if tracing is disabled.

    :param event_id: Event identifier to emit.
    :param master: If the event is emitted as master.
    :param inside: If the event is produced inside the worker.
    :param cpu_affinity: If the event is produced inside the worker for
                         cpu affinity.
    :param gpu_affinity: If the event is produced inside the worker for
                         gpu affinity.
    :param cpu_number: If the event is produced inside the worker for
                       cpu number.
    :return: None.
    """
    if TRACING:
        event_group, event_id = __get_proper_type_event__(
            event_id, master, inside, cpu_affinity, gpu_affinity, cpu_number
        )
        PYEXTRAE.eventandcounters(event_group, event_id)  # noqa


def emit_manual_event_explicit(event_group: int, event_id: int) -> None:
    """Emit a single event for a group.

    Does nothing if tracing is disabled.

    :param event_group: Event group to emit.
    :param event_id: Event identifier to emit.
    :return: None.
    """
    if TRACING:
        PYEXTRAE.eventandcounters(event_group, event_id)  # noqa


def __get_proper_type_event__(
    event_id: int,
    master: bool,
    inside: bool,
    cpu_affinity: bool,
    gpu_affinity: bool,
    cpu_number: bool,
) -> typing.Tuple[int, int]:
    """Parse the flags to retrieve the appropriate event_group.

    It also parses the event_id in case of affinity since it is received
    as string.

    :param event_id: Event identifier to emit.
    :param master: If the event is emitted as master.
    :param inside: If the event is produced inside the worker.
    :param cpu_affinity: If the event is produced inside the worker for
                         cpu affinity.
    :param gpu_affinity: If the event is produced inside the worker for
                         gpu affinity.
    :param cpu_number: If the event is produced inside the worker for
                       cpu number.
    :return: Retrieves the appropriate event_group and event_id.
    """
    if master:
        event_group = TRACING_MASTER.binding_master_type
    else:
        if inside:
            if cpu_affinity:
                event_group = TRACING_WORKER.inside_tasks_cpu_affinity_type
                event_id = __parse_affinity_event_id__(event_id)
            elif gpu_affinity:
                event_group = TRACING_WORKER.inside_tasks_gpu_affinity_type
                event_id = __parse_affinity_event_id__(event_id)
            elif cpu_number:
                event_group = TRACING_WORKER.inside_tasks_cpu_count_type
                event_id = int(event_id)
            else:
                event_group = TRACING_WORKER.inside_tasks_type
        else:
            event_group = TRACING_WORKER.inside_worker_type
    return event_group, event_id


def __parse_affinity_event_id__(event_id: typing.Any) -> int:
    """Parse the affinity event identifier.

    :param event_id: Event identifier.
    :return: The parsed event identifier as integer.
    """
    if isinstance(event_id, str):
        try:
            event_id = int(event_id)
        except ValueError:
            # The event_id is a string with multiple cores
            # Get only the first core
            event_id = int(event_id.split(",")[0].split("-")[0])
        event_id += 1  # since it starts with 0
    return event_id


def enable_trace_master() -> None:
    """Enable tracing for the master process.

    :return: None.
    """
    global PYEXTRAE
    global TRACING
    import pyextrae.sequential as pyextrae  # noqa

    PYEXTRAE = pyextrae
    TRACING = True
