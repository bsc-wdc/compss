#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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

from pycompss.util.context import CONTEXT
from pycompss.util.tracing.types_events_master import TRACING_MASTER
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.util.tracing.types_events_worker import TRACING_WORKER_CACHE
from pycompss.util.typing_helper import typing


class Tracing:
    """Keep the tracing status."""

    __slots__ = ["pyextrae", "tracing"]

    def __init__(
        self,
        py_extrae: typing.Any = None,
        tracing: bool = False,
    ):
        """Instantiate a new Tracing class."""
        self.pyextrae = py_extrae
        self.tracing = tracing

    def get_pyextrae(self) -> typing.Any:
        """Get PYEXTRAE value.

        :return: PyExtrae value.
        """
        return self.pyextrae

    def set_pyextrae(self, pyextrae: typing.Any) -> None:
        """Set PYEXTRAE value.

        :param: pyextrae: New pyextrae value.
        :return: None
        """
        self.pyextrae = pyextrae

    def is_tracing(self) -> bool:
        """Get Tracing value.

        :return: If tracing is enabled.
        """
        return self.tracing

    def enable_tracing(self) -> None:
        """Enable tracing value.

        :return: None
        """
        self.tracing = True

    def disable_tracing(self) -> None:
        """Disable tracing value.

        :return: None
        """
        self.tracing = False


# Keep tracing library and status
TRACING = Tracing()


@contextmanager
def dummy_context() -> typing.Iterator[None]:
    """Context which deactivates the tracing flag and nothing else.

    :return: None.
    """
    TRACING.disable_tracing()
    yield


@contextmanager
def trace_multiprocessing_worker() -> typing.Iterator[None]:
    """Set up the tracing for the multiprocessing worker.

    :return: None.
    """
    import pyextrae.multiprocessing as pyextrae  # pylint: disable=C0415, E0401

    # disable=import-outside-toplevel, import-error

    TRACING.set_pyextrae(pyextrae)
    TRACING.enable_tracing()
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
    import pyextrae.mpi as pyextrae  # pylint: disable=C0415, E0401

    # disable=import-outside-toplevel, import-error

    TRACING.set_pyextrae(pyextrae)
    TRACING.enable_tracing()
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
    import pyextrae.mpi as pyextrae  # pylint: disable=C0415, E0401

    # disable=import-outside-toplevel, import-error

    TRACING.set_pyextrae(pyextrae)
    TRACING.enable_tracing()
    yield  # here the mpi executor runs


class EventMaster:
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
        if TRACING.is_tracing() and CONTEXT.in_master():
            TRACING.get_pyextrae().eventandcounters(
                TRACING_MASTER.binding_master_type, event_id
            )
            self.emitted = True

    def __enter__(self) -> None:
        """Do nothing.

        :returns: None.
        """

    def __exit__(
        self,
        type: typing.Any,  # pylint: disable=redefined-builtin
        value: typing.Any,
        traceback: typing.Any,
    ) -> None:
        """Emit the 0 event in the master group when the context is finished.

        * Signature from context structure.

        :param type: Type.
        :param value: Value.
        :param traceback: Traceback.
        :returns: None.
        """
        if TRACING.is_tracing() and self.emitted:
            TRACING.get_pyextrae().eventandcounters(
                TRACING_MASTER.binding_master_type, 0
            )


class EventWorker:
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
        if TRACING.is_tracing() and CONTEXT.in_worker():
            TRACING.get_pyextrae().eventandcounters(
                TRACING_WORKER.inside_worker_type, event_id
            )  # noqa
            self.emitted = True

    def __enter__(self) -> None:
        """Do nothing.

        :returns: None.
        """

    def __exit__(
        self,
        type: typing.Any,  # pylint: disable=redefined-builtin
        value: typing.Any,
        traceback: typing.Any,
    ) -> None:
        """Emit the 0 event in the worker group when the context is finished.

        * Signature from context structure.

        :param type: Type.
        :param value: Value.
        :param traceback: Traceback.
        :returns: None.
        """
        if TRACING.is_tracing() and self.emitted:
            TRACING.get_pyextrae().eventandcounters(
                TRACING_WORKER.inside_worker_type, 0
            )  # noqa


class EventInsideWorker:
    """Decorator that emits an event at worker (inside task).

    Wraps the desired function code.
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
        if TRACING.is_tracing() and CONTEXT.in_worker():
            TRACING.get_pyextrae().eventandcounters(
                TRACING_WORKER.inside_tasks_type, event_id
            )  # noqa
            self.emitted = True

    def __enter__(self) -> None:
        """Do nothing.

        :returns: None.
        """

    def __exit__(
        self,
        type: typing.Any,  # pylint: disable=redefined-builtin
        value: typing.Any,
        traceback: typing.Any,
    ) -> None:
        """Emit the 0 event in the inside worker group when context finishes.

        * Signature from context structure.

        :param type: Type.
        :param value: Value.
        :param traceback: Traceback.
        :returns: None.
        """
        if TRACING.is_tracing() and self.emitted:
            TRACING.get_pyextrae().eventandcounters(
                TRACING_WORKER.inside_tasks_type, 0
            )


class EventWorkerCache:
    """Decorator that emits an event at worker cache wrapping the desired code.

    Does nothing if tracing is disabled.

    :param event_id: Event identifier to emit.
    :return: None.
    """

    __slots__ = ["emitted"]

    def __init__(self, event_id: int) -> None:
        """Emit the given event identifier in the inside worker cache group.

        :param event_id: Event identifier.
        :returns: None.
        """
        self.emitted = False
        if TRACING.is_tracing() and CONTEXT.in_worker():
            TRACING.get_pyextrae().eventandcounters(
                TRACING_WORKER_CACHE.worker_cache_type, event_id
            )
            self.emitted = True

    def __enter__(self) -> None:
        """Do nothing.

        :returns: None.
        """

    def __exit__(
        self,
        type: typing.Any,  # pylint: disable=redefined-builtin
        value: typing.Any,
        traceback: typing.Any,
    ) -> None:
        """Emit the 0 event in the worker cache group when context finishes.

        * Signature from context structure.

        :param type: Type.
        :param value: Value.
        :param traceback: Traceback.
        :returns: None.
        """
        if TRACING.is_tracing() and self.emitted:
            TRACING.get_pyextrae().eventandcounters(
                TRACING_WORKER_CACHE.worker_cache_type, 0
            )


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
    if TRACING.is_tracing():
        event_group, event_id = __get_proper_type_event(
            event_id, master, inside, cpu_affinity, gpu_affinity, cpu_number
        )
        TRACING.get_pyextrae().eventandcounters(event_group, event_id)  # noqa


def emit_manual_event_explicit(event_group: int, event_id: int) -> None:
    """Emit a single event for a group.

    Does nothing if tracing is disabled.

    :param event_group: Event group to emit.
    :param event_id: Event identifier to emit.
    :return: None.
    """
    if TRACING.is_tracing():
        TRACING.get_pyextrae().eventandcounters(event_group, event_id)  # noqa


def __get_proper_type_event(
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
                event_id = __parse_affinity_event_id(event_id)
            elif gpu_affinity:
                event_group = TRACING_WORKER.inside_tasks_gpu_affinity_type
                event_id = __parse_affinity_event_id(event_id)
            elif cpu_number:
                event_group = TRACING_WORKER.inside_tasks_cpu_count_type
                event_id = int(event_id)
            else:
                event_group = TRACING_WORKER.inside_tasks_type
        else:
            event_group = TRACING_WORKER.inside_worker_type
    return event_group, event_id


def __parse_affinity_event_id(event_id: typing.Any) -> int:
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
    import pyextrae.sequential as pyextrae  # pylint: disable=C0415, E0401

    # disable=import-outside-toplevel, import-error

    TRACING.set_pyextrae(pyextrae)
    TRACING.enable_tracing()
