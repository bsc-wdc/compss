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
PyCOMPSs Cache setup
====================
    This file contains the cache setup and instantiation.
    IMPORTANT: Only used with python >= 3.8.
"""

import typing
from multiprocessing import Process
from multiprocessing import Queue
from pycompss.util.process.manager import Process  # just typing
from pycompss.util.process.manager import Queue    # just typing
from pycompss.util.process.manager import new_queue
from pycompss.util.process.manager import new_manager
from pycompss.util.process.manager import create_process
from pycompss.worker.piper.cache.tracker import CacheTrackerConf
from pycompss.worker.piper.cache.tracker import cache_tracker
from pycompss.worker.piper.cache.tracker import start_shared_memory_manager \
    as __start_smm__
from pycompss.worker.piper.cache.tracker import stop_shared_memory_manager \
    as __stop_smm__


def is_cache_enabled(cache_config):
    # type: (str) -> bool
    """ Check if the cache is enabled.

    :param cache_config: Cache configuration defined on startup.
    :return: True if enabled, False otherwise. And size if enabled.
    """
    if ":" in cache_config:
        cache, _ = cache_config.split(":")
        cache_status = True if cache.lower() == "true" else False
    else:
        cache_status = True if cache_config.lower() == "true" else False
    return cache_status


def start_cache(logger, cache_config, cache_profiler, log_dir):
    # type: (typing.Any, str, bool, str) -> typing.Tuple[typing.Any, Process, Queue, typing.Any]
    """ Setup the cache process which keeps the consistency of the cache.

    :param logger: Logger.
    :param cache_config: Cache configuration defined on startup.
    :param cache_profiler: If cache profiling is enabled or not.
    :param log_dir: Log directory where to store the profiling.
    :return: Shared memory manager, cache process, cache message queue and
             cache ids dictionary
    """
    cache_size = __get_cache_size__(cache_config)
    # Cache can be used
    # Create a proxy dictionary to share the information across workers
    # within the same node
    manager = new_manager()
    # Proxy dictionary
    cache_ids = manager.dict()  # type: typing.Any
    profiler_dict = {}
    profiler_get_struct = [[], [], []]  # Filename, Parameter, Function
    # Start a new process to manage the cache contents.
    smm = __start_smm__()
    conf = CacheTrackerConf(logger, cache_size, "default", cache_ids,
                            profiler_dict, profiler_get_struct, log_dir,
                            cache_profiler)
    cache_process, cache_queue = \
        __create_cache_tracker_process__("cache_tracker", conf)
    return smm, cache_process, cache_queue, cache_ids


def stop_cache(shared_memory_manager, cache_queue, cache_profiler, cache_process):
    # type: (typing.Any, Queue, bool, Process) -> None
    """ Stops the cache process and performs the necessary cleanup.

    :param shared_memory_manager: Shared memory manager.
    :param cache_queue: Cache messaging queue.
    :param cache_profiler: If cache profiling is enabled or not.
    :param cache_process: Cache process
    :return: None
    """
    if cache_profiler:
        cache_queue.put("END PROFILING")
    __destroy_cache_tracker_process__(cache_process, cache_queue)
    __stop_smm__(shared_memory_manager)


def __get_cache_size__(cache_config):
    # type: (str) -> int
    """ Retrieve the cache size for the given config.

    :param cache_config: Cache configuration defined on startup.
    :return: The cache size
    """
    if ":" in cache_config:
        _, cache_s = cache_config.split(":")
        cache_size = int(cache_s)
    else:
        cache_size = __get_default_cache_size__()
    return cache_size


def __get_default_cache_size__():
    # type: () -> int
    """ Returns the default cache size.

    :return: The size in bytes.
    """
    # Default cache_size (bytes) = total_memory (bytes) / 4
    mem_info = dict((i.split()[0].rstrip(":"), int(i.split()[1]))
                    for i in open("/proc/meminfo").readlines())
    cache_size = int(mem_info["MemTotal"] * 1024 / 4)
    return cache_size


def __create_cache_tracker_process__(process_name, conf):
    # type: (str, CacheTrackerConf) -> typing.Tuple[Process, Queue]
    """ Starts a new cache tracker process.

    :param process_name: Process name.
    :param conf: cache config.
    :return: None
    """
    queue = new_queue()
    process = create_process(target=cache_tracker,
                             args=(queue, process_name, conf))
    process.start()
    return process, queue


def __destroy_cache_tracker_process__(cache_process, cache_queue):
    # type: (Process, Queue) -> None
    """ Stops the given cache tracker process.

    :param cache_process: Cache process
    :param cache_queue: Cache messaging queue.
    :return: None
    """
    cache_queue.put("QUIT")    # noqa
    cache_process.join()       # noqa
    cache_queue.close()        # noqa
    cache_queue.join_thread()  # noqa
