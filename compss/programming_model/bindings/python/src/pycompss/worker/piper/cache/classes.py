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
PyCOMPSs Worker - Piper - Cache classes.

This file contains the cache required classes.
"""

from pycompss.util.process.manager import Queue

# Used only for typing
from pycompss.util.process.manager import DictProxy  # noqa: F401
from pycompss.util.typing_helper import typing


class TaskWorkerCache:
    """Class that represents the cache attributes in the worker."""

    __slots__ = [
        "ids",
        "in_queue",
        "out_queue",
        "references",
        "profiler",
    ]

    def __init__(self) -> None:
        """Worker cache placeholder constructor."""
        # These variables are initialized on call since they are only for
        # the worker
        self.ids = None  # type: typing.Optional[DictProxy]
        self.in_queue = Queue()  # type: Queue
        self.out_queue = Queue()  # type: Queue
        # Placeholder to keep the object references and avoid garbage collector
        self.references = []  # type: typing.List[typing.Any]
        # If profiling cache
        self.profiler = False


class CacheQueueMessage:
    """Class that represents a message to the cache."""

    __slots__ = [
        "action",
        "messages",  # list of strings
        "size",  # size for put messages
        "d_type",  # d_type for put messages
        "shape",  # shape for put messages
    ]

    def __init__(
        self,
        action: str = "undefined",
        messages: typing.Sequence[str] = (),
        size: int = 0,
        d_type: type = type(None),
        shape: typing.Sequence[int] = (),
    ) -> None:
        """Cache message placeholder constructor."""
        self.action = action
        # Store any kind of string messages
        self.messages = messages
        # Store specific parameters
        self.size = size
        self.d_type = d_type
        self.shape = shape

    def __repr__(self) -> str:
        """Message representation."""
        message = f"Action: {self.action}"
        message += f" Messages: {self.messages}"
        message += f" Size: {self.size}"
        message += f" d_type: {self.d_type}"
        message += f" Shape: {self.shape}"
        return message
