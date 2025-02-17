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
PyCOMPSs Util - Process - Manager.

This file centralizes the multiprocessing management.
It helps to homogenize the behaviour between linux and mac.
"""

import multiprocessing
from multiprocessing import Manager
from multiprocessing.managers import SyncManager  # Used only for typing
from multiprocessing import Process  # Used only for typing
from multiprocessing import Queue  # Used only for typing

from pycompss.util.typing_helper import typing

try:
    from multiprocessing.shared_memory import SharedMemory  # noqa
    from multiprocessing.shared_memory import ShareableList  # noqa
    from multiprocessing.managers import SharedMemoryManager  # noqa
except ImportError:
    # Unsupported in python < 3.8
    SharedMemory = None  # type: ignore
    ShareableList = None  # type: ignore
    SharedMemoryManager = None  # type: ignore

# Next import can be used only for typing with Python >= 3.8
# from multiprocessing.managers import DictProxy
DictProxy = typing.Any  # type: ignore


# Global variables
LOCK = None


def initialize_multiprocessing() -> None:
    """Set global mechanism to start multiprocessing processes.

    https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods

    CAUTION: Using fork even in MacOS.
    WARNING: This method must be called only once and at the very beginning.

    :return: None
    """
    global LOCK
    try:
        multiprocessing.set_start_method("fork")
    except AttributeError:
        # Unsupported set_start_method (python 2 mainly).
        # Use default start method.
        pass
    except RuntimeError:
        # Already initialized
        pass
    manager = multiprocessing.Manager()
    LOCK = manager.RLock()


def new_process() -> Process:
    """Instantiate a new empty process.

    :return: Empty process.
    """
    return multiprocessing.Process()


def new_queue() -> Queue:
    """Instantiate a new queue.

    :return: New queue
    """
    return multiprocessing.Queue()


def new_event() -> typing.Any:
    """Instantiate a new event.

    :return: New event
    """
    return multiprocessing.Event()


def new_manager() -> SyncManager:
    """Instantiate a new empty multiprocessing manager.

    :return: Empty multiprocessing manager.
    """
    return Manager()


def create_process(
    target: typing.Callable, args: tuple = (), prepend_lock: bool = False
) -> Process:
    """Create a new process for the given target with the provided arguments.

    :param target: Function to execute in a multiprocessing process.
    :param args: function arguments.
    :param prepend_lock: Include a lock for mutex purposes.
    :return: New process.
    """
    if prepend_lock:
        args = (LOCK,) + tuple(args)
    process = multiprocessing.Process(target=target, args=args)
    return process


def create_shared_memory_manager(
    address: typing.Tuple[str, int], authkey: typing.Optional[bytes]
) -> SharedMemoryManager:
    """Create a new shared memory manager process.

    At the given address with the provided authkey.

    :param address: Shared memory manager address (IP, PORT).
    :param authkey: Shared memory manager authentication key.
    :return: New process.
    """
    smm = SharedMemoryManager(address=address, authkey=authkey)
    return smm


def create_proxy_dict() -> DictProxy:
    """Create a proxy dictionary.

    Aimed at sharing the information across workers within the same node.

    :return: Proxy dictionary.
    """
    manager = new_manager()
    cache_ids = manager.dict()  # type: DictProxy
    return cache_ids
