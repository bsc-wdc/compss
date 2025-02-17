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
PyCOMPSs Worker - Piper - Cache Setup.

This file contains the cache setup and instantiation.
IMPORTANT: Only used with python >= 3.8.
"""
import logging
import re
from typing import Union

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
from pycompss.worker.piper.cache.manager import cache_manager

# Used only for typing shortcut
Dict = typing.Dict
List = typing.List


# For calculating cache size
size_units = {
    "B": 1,
    "KB": 10**3,
    "MB": 10**6,
    "GB": 10**9,
    "TB": 10**12,
}

# RegEx for parsing ``cpu:25%`` or ``gpu:5.5GB`` ...
CACHE_SIZE_REGEX = r"(\d+(\.\d+)?)\s*([A-Za-z%]+)"


def is_cache_enabled(cache_config: str) -> bool:
    """Check if the cache is enabled.

    :param cache_config: Cache configuration defined on startup.
    :return: True if enabled, False otherwise. And size if enabled.
    """
    return cache_config.lower() not in ["false", "null"]


def start_cache(
    logger: logging.Logger,
    cache_config: str,
    cache_profiler: bool,
    analysis_dir: str,
) -> typing.Tuple[typing.Any, Process, Queue, Queue, DictProxy]:
    """Set up the cache process which keeps the consistency of the cache.

    :param logger: Logger.
    :param cache_config: Cache configuration defined on startup.
    :param cache_profiler: If cache profiling is enabled or not.
    :param analysis_dir: Directory where to store the profiling.
    :return: Shared memory manager, cache process, cache message queue and
             cache ids dictionary.
    """
    cache_config = cache_config.replace(" ", "").strip()
    main_memory_cache_size = get_cache_size("cpu", cache_config)
    gpu_cache_size = get_cache_size("gpu", cache_config)
    # Cache can be used - Create proxy dict
    cache_ids = create_proxy_dict()
    cache_hits = {}  # type: Dict[int, Dict[str, int]]
    profiler_dict = {}  # type: Dict[str, Dict[str, Dict[str, Dict[str, int]]]]
    profiler_get_struct = [[], [], []]  # type: List[List[str]]
    # profiler_get_struct structure: Filename, Parameter, Function
    smm = CACHE_TRACKER.start_shared_memory_manager()
    conf = CacheTrackerConf(
        logger,
        main_memory_cache_size,
        gpu_cache_size,
        "default",
        cache_ids,
        cache_hits,
        profiler_dict,
        profiler_get_struct,
        analysis_dir,
        cache_profiler,
    )
    (
        cache_process,
        in_cache_queue,
        out_cache_queue,
    ) = create_cache_manager_process(conf)
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
    __destroy_cache_tracker_process(
        cache_process, in_cache_queue, out_cache_queue
    )
    CACHE_TRACKER.stop_shared_memory_manager(shared_memory_manager)


def get_cache_size(device: str, cache_config: str) -> Union[float, int]:
    """Retrieve the CPU/GPU Cache size for the given config.

    :param device: `cpu` or `gpu`.
    :param cache_config: Cache configuration defined on startup.
    :return: The cache size.
    """
    pattern = device + ":" + CACHE_SIZE_REGEX

    matches = re.findall(pattern, cache_config)

    if not matches:
        cache_size = 0.25  # Default value 25% of node memory
    else:
        num, _, unit = matches[0]
        num = float(num)
        if unit == "%":
            num = num / 100
        else:
            num = size_units[unit.upper()] * num
        cache_size = num

    return cache_size


def create_cache_manager_process(
    conf: CacheTrackerConf,
) -> typing.Tuple[Process, Queue, Queue]:
    """Start a new cache tracker process.

    :param process_name: Process name.
    :param conf: cache config.
    :return: Cache tracker process, in queue and out queue.
    """
    in_queue = new_queue()
    out_queue = new_queue()
    process = create_process(
        target=cache_manager, args=(in_queue, out_queue, "cache_manager", conf)
    )
    process.start()
    return process, in_queue, out_queue


def __destroy_cache_tracker_process(
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
