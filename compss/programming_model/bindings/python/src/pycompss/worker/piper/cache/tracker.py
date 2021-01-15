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
PyCOMPSs Cache tracker
======================
    This file contains the cache object tracker.
    IMPORTANT: Only used with the piper_worker.py
"""

from collections import OrderedDict
try:
    from multiprocessing.shared_memory import SharedMemory
    from multiprocessing.managers import SharedMemoryManager
except ImportError:
    # Unsupported in python < 3.8
    SharedMemory = None
try:
    import numpy as np
except ImportError:
    np = None


HEADER = "*[PYTHON CACHE] "
SHARED_MEMORY_MANAGER = None


class CacheTrackerConf(object):
    """
    Cache tracker configuration
    """

    __slots__ = ['logger', 'size', 'policy', 'cache_ids']

    def __init__(self, logger, size, policy, cache_ids):
        """
        Constructs a new cache tracker configuration.

        :param logger: Main logger.
        :param size: Total cache size supported.
        :param policy: Eviction policy.
        :param cache_ids: Shared dictionary proxy where the ids and
                          (size, hits) as its value are.
        """
        self.logger = logger
        self.size = size
        self.policy = policy        # currently no policies defined.
        self.cache_ids = cache_ids  # key - (id, shape, dtype, size, hits)


def cache_tracker(queue, process_name, conf):
    # type: (..., str, CacheTrackerConf) -> None
    """ Process main body

    :param queue: Queue where to put exception messages.
    :param process_name: Process name.
    :param conf: configuration of the cache tracker.
    :return: None
    """
    # Process properties
    alive = True
    logger = conf.logger
    cache_ids = conf.cache_ids
    max_size = conf.size

    if __debug__:
        logger.debug(HEADER + "[%s] Starting Cache Tracker" %
                     str(process_name))

    # MAIN CACHE TRACKER LOOP
    used_size = 0
    while alive:
        # Check every 1 second
        msg = queue.get()
        if msg == "QUIT":
            if __debug__:
                logger.debug(HEADER + "[%s] Stopping Cache Tracker: %s" %
                             (str(process_name), str(msg)))
            alive = False
        else:
            try:
                # new_id, new_cache_id, shape, dtype, new_id_size = msg.split()
                new_id, new_cache_id, shape, dtype, new_id_size = msg
                if new_id in cache_ids:
                    # Any executor has already put the id
                    if __debug__:
                        logger.debug(HEADER + "[%s] Cache collision" %
                                     str(process_name))
                    # Increment hits
                    cache_ids[new_id][4] += 1
                else:
                    # Add new entry request
                    if __debug__:
                        logger.debug(HEADER + "[%s] Cache add entry: %s" %
                                     (str(process_name), str(msg)))
                    # Check if it is going to fit and remove if necessary
                    new_id_size = int(new_id_size)
                    if used_size + new_id_size > max_size:
                        # Cache is full, need to evict
                        used_size = check_cache_status(conf, used_size, new_id_size)
                    # Add without problems
                    used_size = used_size + new_id_size
                    cache_ids[new_id] = [new_cache_id, shape, dtype, new_id_size, 0]
            except Exception as e:
                logger.exception("%s - Exception %s" % (str(process_name),
                                                        str(e)))
                alive = False


def check_cache_status(conf, used_size, requested_size):
    # type: (CacheTrackerConf, int, int) -> int
    """ Checks the cache status looking into the shared dictionary.

    :param conf: configuration of the cache tracker.
    :param used_size: Current used size of the cache.
    :param requested_size: Size needed to fit the new object.
    :return: new used size
    """
    logger = conf.logger
    max_size = conf.size
    cache_ids = conf.cache_ids

    if __debug__:
        logger.debug(HEADER + "Checking cache status: Requested %s" %
                     str(requested_size))

    # Sort by number of hits (from lower to higher)
    sorted_cache_ids = OrderedDict(sorted(cache_ids.items(),
                                          key=lambda item: item[1][4]))

    size_to_recover = used_size + requested_size - max_size
    # Select how many to evict
    to_evict = list()
    while size_to_recover > 0:
        for k, v in sorted_cache_ids:
            to_evict.append(k)
            size_to_recover = size_to_recover - v[3]
    if __debug__:
        logger.debug(HEADER + "Evicting %d entries" % (len(to_evict)))
    # Evict
    for entry in to_evict:
        cache_id, _, _ = cache_ids.pop(entry)

    return used_size - size_to_recover


def initialize_shared_memory_manager():
    # type: () -> None
    """ Connects to the main shared memory manager initiated in piper_worker.py.

    :return: None
    """
    global SHARED_MEMORY_MANAGER
    SHARED_MEMORY_MANAGER = SharedMemoryManager(address=('127.0.0.1', 50000), authkey=b'compss_cache')
    SHARED_MEMORY_MANAGER.connect()


def retrieve_object_from_cache(logger, cache_ids, identifier):
    # type: (..., ..., str) -> ...
    """ Retrieve an object from the given cache proxy dict.

    :param logger: Logger where to push messages.
    :param cache_ids: Cache proxy dictionary.
    :param identifier: Object identifier.
    :return: The object from cache.
    """
    if __debug__:
        logger.debug(HEADER + "Retrieving: " + str(identifier))
    obj_id, obj_shape, obj_d_type, _, obj_hits = cache_ids[identifier]
    existing_shm = SharedMemory(name=obj_id)
    cache_ids[identifier][4] = obj_hits + 1
    output = np.ndarray(obj_shape, dtype=obj_d_type, buffer=existing_shm.buf)
    if __debug__:
        logger.debug(HEADER + "Retrieved: " + str(identifier))
    return output, existing_shm


def insert_object_into_cache(logger, cache_queue, obj, f_name):
    # type: (..., ..., ..., ...) -> None
    """ Put an object into cache.

    :param logger: Logger where to push messages.
    :param cache_queue: Cache notification queue.
    :param obj: Object to store.
    :param f_name: File name that corresponds to the object (used as id).
    :return: None
    """
    if __debug__:
        logger.debug(HEADER + "Inserting into cache: " + str(f_name))
    shape = obj.shape
    d_type = obj.dtype
    size = obj.nbytes
    shm = SHARED_MEMORY_MANAGER.SharedMemory(size=size)
    within_cache = np.ndarray(shape, dtype=d_type, buffer=shm.buf)
    within_cache[:] = obj[:]  # Copy contents
    new_cache_id = shm.name
    cache_queue.put((f_name, new_cache_id, shape, d_type, size))
    if __debug__:
        logger.debug(HEADER + "Inserted into cache: " + str(f_name) + " as " + str(new_cache_id))
