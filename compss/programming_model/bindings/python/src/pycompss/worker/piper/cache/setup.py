#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Worker - Piper - Cache Setup.

This file contains the cache setup and instantiation.
IMPORTANT: Only used with python >= 3.8.
"""
import logging

from pycompss.util.process.manager import Process
from pycompss.util.process.manager import Queue
from pycompss.util.process.manager import create_process
from pycompss.util.process.manager import create_proxy_dict
from pycompss.util.process.manager import DictProxy  # typing only
from pycompss.util.process.manager import new_queue
from pycompss.util.typing_helper import typing
from pycompss.worker.piper.cache.classes import CacheQueueMessage
from pycompss.worker.piper.cache.tracker import CacheTrackerConf
from pycompss.worker.piper.cache.tracker import CACHE_TRACKER
from pycompss.worker.piper.cache.manager import cache_tracker


def is_cache_enabled(cache_config: str) -> bool:
    """Check if the cache is enabled.

    :param cache_config: Cache configuration defined on startup.
    :return: True if enabled, False otherwise. And size if enabled.
    """
    if ":" in cache_config:
        cache, _ = cache_config.split(":")
        cache_status = cache.lower() == "true"
    else:
        cache_status = cache_config.lower() == "true"
    return cache_status


def start_cache(
    logger: logging.Logger,
    cache_config: str,
    cache_profiler: bool,
    log_dir: str,
) -> typing.Tuple[typing.Any, Process, Queue, Queue, DictProxy]:
    """Set up the cache process which keeps the consistency of the cache.

    :param logger: Logger.
    :param cache_config: Cache configuration defined on startup.
    :param cache_profiler: If cache profiling is enabled or not.
    :param log_dir: Log directory where to store the profiling.
    :return: Shared memory manager, cache process, cache message queue and
             cache ids dictionary.
    """
    cache_size = __get_cache_size__(cache_config)
    # Cache can be used - Create proxy dict
    cache_ids = create_proxy_dict()
    cache_hits = {}  # type: typing.Dict[int, typing.Dict[str, int]]
    profiler_dict = (
        {}
    )  # type: typing.Dict[str, typing.Dict[str, typing.Dict[str, typing.Dict[str, int]]]]
    profiler_get_struct = [[], [], []]  # type: typing.List[typing.List[str]]
    # profiler_get_struct structure: Filename, Parameter, Function
    smm = CACHE_TRACKER.start_shared_memory_manager()
    conf = CacheTrackerConf(
        logger,
        cache_size,
        "default",
        cache_ids,
        cache_hits,
        profiler_dict,
        profiler_get_struct,
        log_dir,
        cache_profiler,
    )
    cache_process, in_cache_queue, out_cache_queue = __create_cache_tracker_process__(
        "cache_tracker", conf
    )
    return smm, cache_process, in_cache_queue, out_cache_queue, cache_ids


def stop_cache(
    shared_memory_manager: typing.Any,
    in_cache_queue: Queue,
    out_cache_queue: Queue,
    cache_profiler: bool,
    cache_process: Process,
) -> None:
    """Stop the cache process and performs the necessary cleanup.

    :param shared_memory_manager: Shared memory manager.
    :param in_cache_queue: Cache messaging input queue.
    :param out_cache_queue: Cache messaging output queue.
    :param cache_profiler: If cache profiling is enabled or not.
    :param cache_process: Cache process.
    :return: None.
    """
    if cache_profiler:
        message = CacheQueueMessage(action="END_PROFILING")
        in_cache_queue.put(message)
    __destroy_cache_tracker_process__(cache_process, in_cache_queue, out_cache_queue)
    CACHE_TRACKER.stop_shared_memory_manager(shared_memory_manager)


def __get_cache_size__(cache_config: str) -> int:
    """Retrieve the cache size for the given config.

    :param cache_config: Cache configuration defined on startup.
    :return: The cache size.
    """
    if ":" in cache_config:
        _, cache_s = cache_config.split(":")
        cache_size = int(cache_s)
    else:
        cache_size = __get_default_cache_size__()
    return cache_size


def __get_default_cache_size__() -> int:
    """Return the default cache size.

    :return: The size in bytes.
    """
    # Default cache_size (bytes) = total_memory (bytes) / 4
    with open("/proc/meminfo") as meminfo_fd:
        full_meminfo = meminfo_fd.readlines()

    mem_info = dict((i.split()[0].rstrip(":"), int(i.split()[1])) for i in full_meminfo)
    cache_size = int(mem_info["MemTotal"] * 1024 / 4)
    return cache_size


def __create_cache_tracker_process__(
    process_name: str, conf: CacheTrackerConf
) -> typing.Tuple[Process, Queue, Queue]:
    """Start a new cache tracker process.

    :param process_name: Process name.
    :param conf: cache config.
    :return: Cache tracker process, in queue and out queue.
    """
    in_queue = new_queue()
    out_queue = new_queue()
    process = create_process(
        target=cache_tracker, args=(in_queue, out_queue, process_name, conf)
    )
    process.start()
    return process, in_queue, out_queue


def __destroy_cache_tracker_process__(
    cache_process: Process, in_queue: Queue, out_queue: Queue
) -> None:
    """Stop the given cache tracker process.

    :param cache_process: Cache process.
    :param in_queue: Cache messaging input queue.
    :param out_queue: Cache messaging output queue.
    :return: None.
    """
    message = CacheQueueMessage(action="QUIT")
    in_queue.put(message)  # noqa
    cache_process.join()  # noqa
    in_queue.close()  # noqa
    in_queue.join_thread()  # noqa
    out_queue.close()  # noqa
    out_queue.join_thread()  # noqa
