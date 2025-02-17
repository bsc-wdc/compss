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
PyCOMPSs Worker - Piper - Cache Tracker Manager process.

This file contains the cache object tracker manager process.
"""


from multiprocessing import Queue
from typing import Union
import base64

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

CP = None  # type: typing.Any


def __get_fraction_cache_size__(device: str, fraction: float) -> int:
    global CP

    if device == "cpu":
        with open("/proc/meminfo") as meminfo_fd:
            full_meminfo = meminfo_fd.readline()

        cache_size = int(full_meminfo.split()[1]) * 1024 * fraction
    else:
        if CP:
            gpu_max_mem = int(CP.cuda.Device().mem_info[1])
            cache_size = gpu_max_mem * fraction
        else:
            cache_size = 0.0

    return int(cache_size)


def __calculate_cache_size__(device: str, size: Union[int, float]) -> int:
    if size < 1:
        return __get_fraction_cache_size__(device, size)
    return int(size)


def __is_shared_type_cuda__(sharedtype):
    return "Cuda" in sharedtype


def cache_manager(
    in_queue: Queue,
    out_queue: Queue,
    process_name: str,
    conf: CacheTrackerConf,
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
        TRACING_WORKER.process_identifier,
        TRACING_WORKER.process_worker_cache_event,
    )

    global CP
    try:
        import cupy

        CP = cupy

        gpu_used_size_dict = {}
        for i in range(cupy.cuda.runtime.getDeviceCount()):
            gpu_used_size_dict[i] = 0
            with cupy.cuda.Device(i):
                cupy.cuda.set_allocator(None)
                cupy.cuda.set_pinned_memory_allocator(None)
    except ImportError:
        pass

    # Process properties
    alive = True
    logger = conf.logger
    max_size = __calculate_cache_size__("cpu", conf.size)
    gpu_max_size = __calculate_cache_size__("gpu", conf.gpu_cache_size)
    cache_ids = conf.cache_ids
    cache_hits = conf.cache_hits
    profiler_dict = conf.profiler_dict
    profiler_get_struct = conf.profiler_get_struct
    analysis_dir = conf.analysis_dir
    cache_profiler = conf.cache_profiler

    if __debug__:
        logger.debug(
            "%s [%s] Starting Cache Manager: CPU %.1fGB, GPU: %.1fGB",
            CACHE_MANAGER_HEADER,
            str(process_name),
            max_size / 1e9,
            gpu_max_size / 1e9,
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
                                f"{CACHE_MANAGER_HEADER} [{process_name}] "
                                f"{hits} hits:"
                            )
                            for obj_name, size in elements.items():
                                logger.debug(
                                    f"{CACHE_MANAGER_HEADER} [{process_name}] "
                                    f"\t- {obj_name} {size}"
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
            with EventWorkerCache(
                TRACING_WORKER_CACHE.cache_msg_end_profiling_event
            ):
                if cache_profiler:
                    profiler_print_message(
                        profiler_dict, profiler_get_struct, analysis_dir
                    )
        else:
            try:
                if action == "GET":
                    with EventWorkerCache(
                        TRACING_WORKER_CACHE.cache_msg_get_event
                    ):
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
                                    profiler_dict,
                                    function,
                                    parameter,
                                    f_name,
                                    "GET",
                                )
                                # PROFILER GET STRUCTURE
                                add_profiler_get_struct(
                                    profiler_get_struct,
                                    function,
                                    parameter,
                                    f_name,
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
                            cache_ids[f_name] = (
                                current  # forces updating whole entry
                            )
                            # Keep cache_hits structure
                            try:
                                cache_hits[current_hits].pop(f_name)
                            except KeyError:
                                pass
                            if new_hits in cache_hits:
                                cache_hits[new_hits][f_name] = obj_size
                            else:
                                cache_hits[new_hits] = {f_name: obj_size}
                elif action == "PUT":
                    with EventWorkerCache(
                        TRACING_WORKER_CACHE.cache_msg_put_event
                    ):
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
                                    "%s [%s] The object already exists "
                                    "NOT adding: %s",
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
                            # Check if it is going to fit
                            # and remove if necessary
                            obj_size = int(obj_size)
                            if used_size + obj_size > max_size:
                                # Cache is full, need to evict
                                used_size = __free_cache_space__(
                                    conf, used_size, obj_size
                                )
                            # Accumulate size
                            used_size += obj_size
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
                elif action == "PUT_GPU":
                    with EventWorkerCache(
                        TRACING_WORKER_CACHE.cache_msg_put_gpu_event
                    ):
                        (
                            f_name,
                            cache_id,
                            shared_type,
                            parameter,
                            function,
                            device_pci_bus_id,
                        ) = msg.messages

                        obj_size = msg.size
                        dtype = msg.d_type
                        shape = msg.shape

                        obj_size = int(obj_size)

                        for i in range(CP.cuda.runtime.getDeviceCount()):
                            if (
                                CP.cuda.Device(i).pci_bus_id
                                == device_pci_bus_id
                            ):
                                device_id = i

                        logger.debug(
                            "%s [%s] DEVICE ID: %s",
                            CACHE_MANAGER_HEADER,
                            str(process_name),
                            str(device_pci_bus_id),
                        )

                        if f_name in cache_ids:
                            # The object already exists
                            if __debug__:
                                logger.debug(
                                    "%s [%s] The object already exists "
                                    "NOT adding: %s",
                                    CACHE_MANAGER_HEADER,
                                    str(process_name),
                                    str(msg),
                                )
                        elif obj_size > gpu_max_size:
                            logger.debug(
                                "%s [%s] The object (%.1fMB) is bigger "
                                "than max GPU cache size(%.1fMB)",
                                CACHE_MANAGER_HEADER,
                                str(process_name),
                                obj_size / 1e6,
                                gpu_max_size / 1e6,
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
                                    "PUT_GPU",
                                )

                            gpu_used_size = gpu_used_size_dict[device_id]
                            with CP.cuda.Device(device_id):
                                if gpu_used_size + obj_size > gpu_max_size:
                                    gpu_used_size_dict[device_id] = (
                                        __free_gpu_cache_space__(
                                            conf,
                                            gpu_used_size,
                                            obj_size,
                                            device_id,
                                        )
                                    )

                                cache_mem = CP.cuda.memory.BaseMemory()
                                cache_mem.ptr = CP.cuda.runtime.malloc(
                                    obj_size
                                )

                                try:
                                    handler = base64.b64decode(cache_id)
                                    array_open = (
                                        CP.cuda.runtime.ipcOpenMemHandle(
                                            handler
                                        )
                                    )

                                    CP.cuda.runtime.memcpy(
                                        cache_mem.ptr, array_open, obj_size, 0
                                    )

                                    CP.cuda.runtime.ipcCloseMemHandle(
                                        array_open
                                    )

                                    new_handler = (
                                        CP.cuda.runtime.ipcGetMemHandle(
                                            cache_mem.ptr
                                        )
                                    )
                                    new_cache_id = base64.b64encode(
                                        new_handler
                                    ).decode("ascii")

                                    conf.gpu_arr_ptr[new_cache_id] = [
                                        cache_mem.ptr,
                                        device_id,
                                    ]
                                    gpu_used_size_dict[device_id] += obj_size

                                    hits = 0
                                    cache_ids[f_name] = [
                                        new_cache_id,
                                        shape,
                                        dtype,
                                        obj_size,
                                        hits,
                                        shared_type,
                                    ]

                                    if hits not in cache_hits:
                                        cache_hits[hits] = {}
                                    cache_hits[hits][f_name] = obj_size
                                except Exception:
                                    import traceback

                                    logger.debug(
                                        "%s [%s] PUT GPU ERROR: %s %s",
                                        CACHE_MANAGER_HEADER,
                                        str(process_name),
                                        str(device_id),
                                        str(traceback.format_exc()),
                                    )
                                    CP.cuda.runtime.free(cache_mem.ptr)
                    out_queue.put("OK")
                elif action == "REMOVE":
                    with EventWorkerCache(
                        TRACING_WORKER_CACHE.cache_msg_remove_event
                    ):
                        f_name_msg = msg.messages[0]
                        f_name = get_file_name(f_name_msg)
                        logger.debug(
                            "%s [%s] Removing: %s",
                            CACHE_MANAGER_HEADER,
                            str(process_name),
                            str(f_name),
                        )
                        (
                            shared_type,
                            obj_size,
                            device_id,
                        ) = __remove_from_cache__(
                            f_name,
                            cache_ids,
                            cache_hits,
                            gpu_arr_ptr=conf.gpu_arr_ptr,
                        )

                        if __is_shared_type_cuda__(shared_type):
                            gpu_used_size_dict[device_id] -= obj_size
                        else:
                            used_size -= obj_size
                elif action == "LOCK":
                    with EventWorkerCache(
                        TRACING_WORKER_CACHE.cache_msg_lock_event
                    ):
                        f_name_msg = msg.messages[0]
                        f_name = get_file_name(f_name_msg)
                        if f_name in locked:
                            raise PyCOMPSsException(
                                "Cache coherence issue: "
                                "tried to lock an already locked file entry"
                            )
                        locked.add(f_name)
                        logger.debug(
                            "%s [%s] Locking: %s",
                            CACHE_MANAGER_HEADER,
                            str(process_name),
                            str(f_name),
                        )
                elif action == "UNLOCK":
                    with EventWorkerCache(
                        TRACING_WORKER_CACHE.cache_msg_unlock_event
                    ):
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
                                "Cache coherence issue: "
                                "tried to remove locked but failed"
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
                    "%s - Exception %s",
                    str(process_name),
                    str(general_exception),
                )
                alive = False


def __remove_from_cache__(f_name, cache_ids, cache_hits, gpu_arr_ptr=None):
    """Remove object from cache.

    :param f_name: File name (object identifier).
    :param cache_ids: Cache identifiers.
    :param cache_hits: Cache hits structure.
    :param gpu_arr_ptr: Gpu array pointer.
    :return: Type, size and device identifier.
    """
    (cache_id, _, _, size, current_hits, shared_type) = cache_ids.pop(f_name)

    if __is_shared_type_cuda__(shared_type):
        gpu_ptr, device_id = gpu_arr_ptr.pop(cache_id)
        with CP.cuda.Device(device_id):
            CP.cuda.runtime.free(gpu_ptr)
    else:
        device_id = None

    cache_hits[current_hits].pop(f_name)

    return shared_type, size, device_id


def __free_cache_space__(
    conf: CacheTrackerConf, used_size: int, requested_size: int
) -> int:
    """Check the cache status looking into the shared dictionary.

    :param conf: configuration of the cache tracker.
    :param used_size: Current used size of the cache.
    :param requested_size: Size needed to fit the new object.
    :return: New used size.
    """
    logger = conf.logger  # noqa
    max_size = int(conf.size)
    cache_ids = conf.cache_ids
    cache_hits = conf.cache_hits

    if __debug__:
        logger.debug(
            "%s Checking cache status: Requested %s bytes",
            CACHE_MANAGER_HEADER,
            str(requested_size),
        )

    sorted_hits = sorted(cache_hits.keys())

    # Calculate size to recover
    size_to_recover = used_size + requested_size - max_size
    # Select how many to evict
    evicted, recovered_size = __cpu_evict__(
        sorted_hits, cache_ids, cache_hits, size_to_recover
    )
    if __debug__:
        logger.debug("%s Evicting %d entries", CACHE_MANAGER_HEADER, (evicted))

    return used_size - recovered_size


def __cpu_evict__(
    sorted_hits: typing.List[int],
    cache_ids,
    cache_hits: typing.Dict[int, typing.Dict[str, int]],
    size_to_recover: int,
) -> typing.Tuple[int, int]:
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
        files = list(cache_hits[hits])
        for f_name in files:
            _, recovered_size, _ = __remove_from_cache__(
                f_name, cache_ids, cache_hits
            )
            to_evict.append(f_name)
            size_to_recover -= recovered_size
            total_recovered_size += recovered_size
            if size_to_recover <= 0:
                return len(to_evict), total_recovered_size
    return len(to_evict), total_recovered_size


def __free_gpu_cache_space__(
    conf: CacheTrackerConf,
    gpu_used_size: int,
    requested_size: int,
    device_id: int,
) -> int:
    """Check the cache status looking into the shared dictionary.

    :param conf: configuration of the cache tracker.
    :param used_size: Current used size of the cache.
    :param requested_size: Size needed to fit the new object.
    :return: New used size.
    """
    logger = conf.logger  # noqa
    max_size = int(conf.size)
    cache_ids = conf.cache_ids
    cache_hits = conf.cache_hits

    if __debug__:
        logger.debug(
            "%s Checking cache status: Requested %s bytes",
            CACHE_MANAGER_HEADER,
            str(requested_size),
        )

    sorted_hits = sorted(cache_hits.keys())

    # Calculate size to recover
    size_to_recover = gpu_used_size + requested_size - max_size
    # Select how many to evict
    evicted, recovered_size = __gpu_evict__(
        sorted_hits,
        cache_ids,
        cache_hits,
        conf.gpu_arr_ptr,
        size_to_recover,
        device_id,
    )
    if __debug__:
        logger.debug(
            "%s Evicting %d entries from GPU", CACHE_MANAGER_HEADER, (evicted)
        )

    return gpu_used_size - recovered_size


def __gpu_evict__(
    sorted_hits: typing.List[int],
    cache_ids,
    cache_hits: typing.Dict[int, typing.Dict[str, int]],
    gpu_arr_ptr,
    size_to_recover: int,
    device_id: int,
) -> typing.Tuple[int, int]:
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
        files = list(cache_hits[hits])
        for f_name in files:
            obj_id, _, _, _, _, shared_type = cache_ids[f_name]

            if (
                __is_shared_type_cuda__(shared_type)
                and device_id == gpu_arr_ptr[obj_id][1]
            ):
                _, obj_size, _ = __remove_from_cache__(
                    f_name, cache_ids, cache_hits, gpu_arr_ptr=gpu_arr_ptr
                )
                to_evict.append(f_name)
                size_to_recover -= obj_size
                total_recovered_size += obj_size
                if size_to_recover <= 0:
                    return len(to_evict), total_recovered_size
    return len(to_evict), total_recovered_size
