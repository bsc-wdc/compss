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
PyCOMPSs Cache setup
====================
    This file contains the cache setup and instantiation.
    IMPORTANT: Only used with python >= 3.8.
"""

from multiprocessing import Process
from multiprocessing import Queue

from pycompss.worker.piper.cache.tracker import CacheTrackerConf
from pycompss.worker.piper.cache.tracker import cache_tracker


def cache_enabled(config):
    # type: (PiperWorkerConfiguration) -> (bool, int)
    """ Check if the cache is enabled.

    :param config: Piper worker configuration.
    :return: True if enabled, False otherwise. And size if enabled.
    """
    if ":" in config.cache:
        cache, cache_size = config.cache.split(":")
        cache = True if cache == "true" else False
        cache_size = calculate_cache_size(cache, cache_size)
    else:
        cache = True if config.cache == "true" else False
        cache_size = calculate_cache_size(cache)
    return cache, cache_size


def calculate_cache_size(cache, provided=None):
    # type: (bool, str or None) -> int
    """ Calculates the cache size.

    :param cache: If the cache is enabled or not.
    :param provided: If the user provided a size.
    :return: The size in bytes.
    """
    if cache:
        if provided:
            return int(provided)
        else:
            # Default cache_size (bytes) = total_memory (bytes) / 4
            mem_info = dict((i.split()[0].rstrip(':'), int(i.split()[1]))
                            for i in open('/proc/meminfo').readlines())
            cache_size = int(mem_info["MemTotal"] * 1024 / 4)
            return cache_size
    else:
        return 0


def initialize_cache_process(logger, cache, cache_size, config):
    # type: (..., bool, int, PiperWorkerConfiguration) -> (..., Process, Queue, dict)
    """ Setup the cache process which keeps the consistency of the cache.

    :return: None
    """
    if cache:
        # Cache can be used
        # Create a proxy dictionary to share the information across workers
        # within the same node
        from multiprocessing import Manager
        manager = Manager()
        cache_ids = manager.dict()  # Proxy dictionary
        # Start a new process to manage the cache contents.
        from multiprocessing.managers import SharedMemoryManager
        smm = SharedMemoryManager(address=('', 50000), authkey=b'compss_cache')
        smm.start()
        conf = CacheTrackerConf(logger,
                                cache_size,
                                None,
                                cache_ids)
        cache_process, cache_queue = create_cache_tracker_process("cache_tracker", conf)  # noqa
    else:
        smm = None
        cache_process = None
        cache_queue = None
        cache_ids = None

    return smm, cache_process, cache_queue, cache_ids


def create_cache_tracker_process(process_name, conf):
    # type: (str, CacheTrackerConf) -> (Process, Queue)
    """ Starts a new cache tracker process.

    :param process_name: Process name.
    :param conf: cache config.
    :return: None
    """
    queue = Queue()
    process = Process(target=cache_tracker, args=(queue,
                                                  process_name,
                                                  conf))
    process.start()
    return process, queue
