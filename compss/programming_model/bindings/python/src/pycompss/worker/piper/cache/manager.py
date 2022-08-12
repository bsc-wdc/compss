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
PyCOMPSs Worker - Piper - Cache Tracker Manager process.

This file contains the cache object tracker manager process.
"""


from multiprocessing import Queue

from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.tracing.helpers import emit_manual_event_explicit
from pycompss.util.tracing.helpers import EventWorkerCache
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.util.tracing.types_events_worker import TRACING_WORKER_CACHE
from pycompss.util.typing_helper import typing
from pycompss.worker.piper.cache.classes import CacheQueueMessage
from pycompss.worker.piper.cache.profiler import add_profiler_get_put
from pycompss.worker.piper.cache.profiler import add_profiler_get_struct
from pycompss.worker.piper.cache.profiler import profiler_print_message
from pycompss.worker.piper.cache.tracker import CacheTrackerConf
from pycompss.worker.piper.cache.tracker import get_file_name
from pycompss.worker.piper.cache.tracker import get_file_name_clean


CACHE_MANAGER_HEADER = "[PYTHON CACHE MANAGER]"


def cache_tracker(
    in_queue: Queue, out_queue: Queue, process_name: str, conf: CacheTrackerConf
) -> None:
    """Process main body.

    :param in_queue: Queue where to retrieve queue messages.
    :param out_queue: Queue where to put output messages.
    :param process_name: Process name.
    :param conf: configuration of the cache tracker.
    :return: None.
    """
    # First thing to do is to emit the process identifier event
    emit_manual_event_explicit(
        TRACING_WORKER.process_identifier, TRACING_WORKER.process_worker_cache_event
    )

    # Process properties
    alive = True
    logger = conf.logger
    max_size = conf.size
    cache_ids = conf.cache_ids
    cache_hits = conf.cache_hits
    profiler_dict = conf.profiler_dict
    profiler_get_struct = conf.profiler_get_struct
    log_dir = conf.log_dir
    cache_profiler = conf.cache_profiler

    if __debug__:
        logger.debug(
            "%s [%s] Starting Cache Tracker", CACHE_MANAGER_HEADER, str(process_name)
        )

    # MAIN CACHE TRACKER LOOP
    msg = CacheQueueMessage()
    used_size = 0
    locked = set()  # set containing the locked entries
    while alive:
        with EventWorkerCache(TRACING_WORKER_CACHE.cache_msg_receive_event):
            msg = in_queue.get()
        action = msg.action
        if action == "QUIT":
            with EventWorkerCache(TRACING_WORKER_CACHE.cache_msg_quit_event):
                if __debug__:
                    logger.debug(
                        "%s [%s] Stopping Cache Tracker: %s",
                        CACHE_MANAGER_HEADER,
                        str(process_name),
                        str(msg),
                    )
                    logger.debug(
                        "%s [%s] Cache hits status:",
                        CACHE_MANAGER_HEADER,
                        str(process_name),
                    )
                    used_size = 0
                    entries = 0
                    for hits, elements in cache_hits.items():
                        if elements:  # not empty entry
                            logger.debug(
                                f"{CACHE_MANAGER_HEADER} [{process_name}] {hits} hits:"
                            )
                            for obj_name, size in elements.items():
                                logger.debug(
                                    f"{CACHE_MANAGER_HEADER} [{process_name}] \t- {obj_name} {size}"
                                )
                                used_size += size
                                entries += 1
                    logger.debug(
                        "%s [%s] Entries: %s Max size: %s Used size: %s",
                        CACHE_MANAGER_HEADER,
                        str(process_name),
                        str(entries),
                        str(max_size),
                        str(used_size),
                    )
                alive = False
        elif action == "END_PROFILING":
            with EventWorkerCache(TRACING_WORKER_CACHE.cache_msg_end_profiling_event):
                if cache_profiler:
                    profiler_print_message(profiler_dict, profiler_get_struct, log_dir)
        else:
            try:
                if action == "GET":
                    with EventWorkerCache(TRACING_WORKER_CACHE.cache_msg_get_event):
                        f_name, parameter, function = msg.messages
                        if f_name not in cache_ids:
                            # The object does not exist in the Cache
                            # It does not go inside here due to we check if it
                            # is in cache before trying to get (from
                            # runtime/task/worker.py).
                            if __debug__:
                                logger.debug(
                                    "%s [%s] Cache miss",
                                    CACHE_MANAGER_HEADER,
                                    str(process_name),
                                )
                        else:
                            if cache_profiler:
                                # PROFILER GET
                                add_profiler_get_put(
                                    profiler_dict, function, parameter, f_name, "GET"
                                )
                                # PROFILER GET STRUCTURE
                                add_profiler_get_struct(
                                    profiler_get_struct, function, parameter, f_name
                                )
                            # Increment the number of hits
                            if __debug__:
                                logger.debug(
                                    "%s [%s] Cache hit",
                                    CACHE_MANAGER_HEADER,
                                    str(process_name),
                                )
                            # Increment hits
                            current = cache_ids[f_name]
                            obj_size = current[3]
                            current_hits = current[4]
                            new_hits = current_hits + 1
                            current[4] = new_hits
                            cache_ids[f_name] = current  # forces updating whole entry
                            # Keep cache_hits structure
                            try:
                                cache_hits[current_hits].pop(f_name)
                            except KeyError:
                                pass
                            if new_hits in cache_hits:
                                cache_hits[new_hits][f_name] = obj_size
                            else:
                                cache_hits[new_hits] = {f_name: obj_size}
                if action == "PUT":
                    with EventWorkerCache(TRACING_WORKER_CACHE.cache_msg_put_event):
                        (
                            f_name,
                            cache_id,
                            shared_type,
                            parameter,
                            function,
                        ) = msg.messages
                        obj_size = msg.size
                        dtype = msg.d_type
                        shape = msg.shape

                        if f_name in cache_ids:
                            # The object already exists
                            if __debug__:
                                logger.debug(
                                    "%s [%s] The object already exists NOT adding: %s",
                                    CACHE_MANAGER_HEADER,
                                    str(process_name),
                                    str(msg),
                                )
                        else:
                            # Add new entry request
                            if __debug__:
                                logger.debug(
                                    "%s [%s] Cache add entry: %s",
                                    CACHE_MANAGER_HEADER,
                                    str(process_name),
                                    str(msg),
                                )
                            if cache_profiler:
                                # PROFILER PUT
                                add_profiler_get_put(
                                    profiler_dict,
                                    function,
                                    parameter,
                                    get_file_name_clean(f_name),
                                    "PUT",
                                )
                            # Check if it is going to fit and remove if necessary
                            obj_size = int(obj_size)
                            if used_size + obj_size > max_size:
                                # Cache is full, need to evict
                                used_size = __check_cache_status__(
                                    conf, used_size, obj_size
                                )
                            # Accumulate size
                            used_size = used_size + obj_size
                            # Initial hits
                            hits = 0
                            # Add without problems
                            cache_ids[f_name] = [
                                cache_id,
                                shape,
                                dtype,
                                obj_size,
                                hits,
                                shared_type,
                            ]
                            # Register in hits dictionary
                            if hits not in cache_hits:
                                cache_hits[hits] = {}
                            cache_hits[hits][f_name] = obj_size
                elif action == "REMOVE":
                    with EventWorkerCache(TRACING_WORKER_CACHE.cache_msg_remove_event):
                        f_name_msg = msg.messages[0]
                        f_name = get_file_name(f_name_msg)
                        logger.debug(
                            "%s [%s] Removing: %s",
                            CACHE_MANAGER_HEADER,
                            str(process_name),
                            str(f_name),
                        )
                        cache_ids.pop(f_name)
                        # Remove from cache hits
                        current = cache_ids[f_name]
                        current_hits = current[4]
                        cache_hits[current_hits].pop(f_name)
                elif action == "LOCK":
                    with EventWorkerCache(TRACING_WORKER_CACHE.cache_msg_lock_event):
                        f_name_msg = msg.messages[0]
                        f_name = get_file_name(f_name_msg)
                        if f_name in locked:
                            raise PyCOMPSsException(
                                "Cache coherence issue: tried to lock an already locked file entry"
                            )
                        locked.add(f_name)
                        logger.debug(
                            "%s [%s] Locking: %s",
                            CACHE_MANAGER_HEADER,
                            str(process_name),
                            str(f_name),
                        )
                elif action == "UNLOCK":
                    with EventWorkerCache(TRACING_WORKER_CACHE.cache_msg_unlock_event):
                        f_name_msg = msg.messages[0]
                        f_name = get_file_name(f_name_msg)
                        logger.debug(
                            "%s [%s] Unlocking: %s",
                            CACHE_MANAGER_HEADER,
                            str(process_name),
                            str(f_name),
                        )
                        try:
                            locked.remove(f_name)
                        except KeyError as key_error:
                            raise PyCOMPSsException(
                                "Cache coherence issue: tried to remove locked but failed"
                            ) from key_error
                elif action == "IS_LOCKED":
                    with EventWorkerCache(
                        TRACING_WORKER_CACHE.cache_msg_is_locked_event
                    ):
                        f_name_msg = msg.messages[0]
                        f_name = get_file_name(f_name_msg)
                        is_locked = f_name in locked
                        out_queue.put(is_locked)
                        logger.debug(
                            "%s [%s] Get if is locked: %s : %s",
                            CACHE_MANAGER_HEADER,
                            str(process_name),
                            str(f_name),
                            str(is_locked),
                        )
                elif action == "IS_IN_CACHE":
                    with EventWorkerCache(
                        TRACING_WORKER_CACHE.cache_msg_is_in_cache_event
                    ):
                        f_name_msg = msg.messages[0]
                        f_name = get_file_name(f_name_msg)
                        is_in_cache = f_name in cache_ids
                        out_queue.put(is_in_cache)
                        logger.debug(
                            "%s [%s] Get if is in cache: %s : %s",
                            CACHE_MANAGER_HEADER,
                            str(process_name),
                            str(f_name),
                            str(is_in_cache),
                        )

            except Exception as general_exception:
                logger.exception(
                    "%s - Exception %s", str(process_name), str(general_exception)
                )
                alive = False


def __check_cache_status__(
    conf: CacheTrackerConf, used_size: int, requested_size: int
) -> int:
    """Check the cache status looking into the shared dictionary.

    :param conf: configuration of the cache tracker.
    :param used_size: Current used size of the cache.
    :param requested_size: Size needed to fit the new object.
    :return: New used size.
    """
    logger = conf.logger  # noqa
    max_size = conf.size
    cache_ids = conf.cache_ids
    cache_hits = conf.cache_hits
    if __debug__:
        logger.debug(
            "%s Checking cache status: Requested %s bytes",
            CACHE_MANAGER_HEADER,
            str(requested_size),
        )

    # Sort by number of hits (from lower to higher)
    sorted_hits = sorted(cache_hits.keys())
    # Calculate size to recover
    size_to_recover = used_size + requested_size - max_size
    # Select how many to evict
    to_evict, recovered_size = __evict__(sorted_hits, cache_hits, size_to_recover)
    if __debug__:
        logger.debug("%s Evicting %d entries", CACHE_MANAGER_HEADER, (len(to_evict)))
    # Evict
    for entry in to_evict:
        cache_ids.pop(entry)
    return used_size - recovered_size


def __evict__(
    sorted_hits: typing.List[int],
    cache_hits: typing.Dict[int, typing.Dict[str, int]],
    size_to_recover: int,
) -> typing.Tuple[typing.List[str], int]:
    """Select how many entries to evict.

    :param sorted_hits: List of current hits sorted from lower to higher.
    :param cache_hits: Cache hits structure.
    :param size_to_recover: Amount of size to recover.
    :return: List of f_names to evict.
    """
    to_evict = []
    total_recovered_size = 0
    for hits in sorted_hits:
        # Does not check the order by size of the objects since they have
        # the same amount of hits
        for f_name in cache_hits[hits]:
            recovered_size = cache_hits[hits].pop(f_name)
            to_evict.append(f_name)
            size_to_recover -= recovered_size
            total_recovered_size += recovered_size
            if size_to_recover <= 0:
                return to_evict, total_recovered_size
    return to_evict, total_recovered_size
