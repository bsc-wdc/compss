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
PyCOMPSs Cache tracker
======================
    This file contains the cache object tracker.
    IMPORTANT: Only used with python >= 3.8.
"""

import os
import typing
from collections import OrderedDict

from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.objects.sizer import total_sizeof
from pycompss.util.tracing.helpers import EmitEvent
from pycompss.worker.commons.constants import RETRIEVE_OBJECT_FROM_CACHE_EVENT
from pycompss.worker.commons.constants import INSERT_OBJECT_INTO_CACHE_EVENT
from pycompss.worker.commons.constants import REMOVE_OBJECT_FROM_CACHE_EVENT
from pycompss.worker.commons.constants import TASK_EVENTS_SERIALIZE_SIZE_CACHE
from pycompss.worker.commons.constants import TASK_EVENTS_DESERIALIZE_SIZE_CACHE
from pycompss.util.tracing.helpers import emit_manual_event_explicit

from multiprocessing import Queue
try:
    from pycompss.util.process.manager import SharedMemory
    from pycompss.util.process.manager import ShareableList
    from pycompss.util.process.manager import SharedMemoryManager  # just typing
    from pycompss.util.process.manager import create_shared_memory_manager
except ImportError:
    # Unsupported in python < 3.8
    SharedMemory = None         # type: ignore
    ShareableList = None        # type: ignore
    SharedMemoryManager = None  # type: ignore
try:
    import numpy as np
except ImportError:
    np = None

from pycompss.worker.commons.constants import BINDING_SERIALIZATION_CACHE_SIZE_TYPE    # noqa: E501
from pycompss.worker.commons.constants import BINDING_DESERIALIZATION_CACHE_SIZE_TYPE  # noqa: E501
from pycompss.util.tracing.helpers import emit_manual_event_explicit

HEADER = "[PYTHON CACHE] "
SHARED_MEMORY_MANAGER = None  # type: typing.Any

# Supported shared objects (remind that nested lists are not supported).
SHARED_MEMORY_TAG = "SharedMemory"
SHAREABLE_LIST_TAG = "ShareableList"
SHAREABLE_TUPLE_TAG = "ShareableTuple"
# Currently dicts are unsupported since conversion requires nesting of lists.
# SHAREABLE_DICT_TAG = "ShareableTuple"

AUTH_KEY = b"compss_cache"
IP = "127.0.0.1"
PORT = 50000
PROFILER_LOG = "cache_profiler.json"


class CacheTrackerConf(object):
    """
    Cache tracker configuration
    """

    __slots__ = ["logger", "size", "policy", "cache_ids",
                 "profiler_dict", "profiler_get_struct", "log_dir",
                 "cache_profiler"]

    def __init__(self, logger, size, policy, cache_ids,
                 profiler_dict, profiler_get_struct, log_dir, cache_profiler):
        # type: (typing.Any, int, str, typing.Any, dict, typing.Any, str, bool) -> None
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
        self.policy = policy  # currently no policies defined.
        self.cache_ids = cache_ids  # key - (id, shape, dtype, size, hits, shared_type)
        self.profiler_dict = profiler_dict
        self.profiler_get_struct = profiler_get_struct
        self.log_dir = log_dir
        self.cache_profiler = cache_profiler


def cache_tracker(queue, process_name, conf):
    # type: (Queue, str, CacheTrackerConf) -> None
    """ Process main body

    :param queue: Queue where to put exception messages.
    :param process_name: Process name.
    :param conf: configuration of the cache tracker.
    :return: None
    """
    # Process properties
    alive = True
    logger = conf.logger
    max_size = conf.size
    cache_ids = conf.cache_ids
    profiler_dict = conf.profiler_dict
    profiler_get_struct = conf.profiler_get_struct
    log_dir = conf.log_dir
    cache_profiler = conf.cache_profiler

    if __debug__:
        logger.debug(HEADER + "[%s] Starting Cache Tracker" %
                     str(process_name))

    # MAIN CACHE TRACKER LOOP
    used_size = 0
    while alive:
        msg = queue.get()
        if msg == "QUIT":
            if __debug__:
                logger.debug(HEADER + "[%s] Stopping Cache Tracker: %s" %
                             (str(process_name), str(msg)))
            alive = False
        elif msg == "END PROFILING":
            if cache_profiler:
                profiler_print_message(profiler_dict, profiler_get_struct, log_dir)
        else:
            try:
                action, message = msg
                if action == "GET":
                    if cache_profiler:
                        filename, parameter, function = message
                        # PROFILER GET
                        add_profiler_get_put(profiler_dict, function, parameter, filename, 'GET')
                        # PROFILER GET STRUCTURE
                        add_profiler_get_struct(profiler_get_struct, function, parameter, filename)
                if action == "PUT":
                    f_name, cache_id, shape, dtype, obj_size, shared_type, parameter, function = message  # noqa: E501
                    if f_name in cache_ids:
                        if cache_profiler:
                            # PROFILER PUT
                            add_profiler_get_put(profiler_dict, function, parameter, filename_cleaned(f_name), 'PUT')
                        # Any executor has already put the id
                        if __debug__:
                            logger.debug(HEADER + "[%s] Cache hit" %
                                         str(process_name))
                        # Increment hits
                        cache_ids[f_name][4] += 1
                    else:
                        # Add new entry request
                        if __debug__:
                            logger.debug(HEADER + "[%s] Cache add entry: %s" %
                                         (str(process_name), str(msg)))
                        if cache_profiler:
                            # PROFILER PUT
                            add_profiler_get_put(profiler_dict, function, parameter, filename_cleaned(f_name), 'PUT')

                        # Check if it is going to fit and remove if necessary
                        obj_size = int(obj_size)
                        if used_size + obj_size > max_size:
                            # Cache is full, need to evict
                            used_size = check_cache_status(conf,
                                                           used_size,
                                                           obj_size)
                        # Add without problems
                        used_size = used_size + obj_size
                        cache_ids[f_name] = [cache_id,
                                             shape,
                                             dtype,
                                             obj_size,
                                             0,
                                             shared_type]
                elif action == "REMOVE":
                    f_name = __get_file_name__(message)
                    logger.debug(HEADER + "[%s] Removing: %s" %
                                 (str(process_name), str(f_name)))
                    cache_ids.pop(f_name)
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
    logger = conf.logger  # noqa
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
    position = 0
    recovered_size = 0
    keys = list(sorted_cache_ids.keys())
    while size_to_recover > 0:
        key = keys[position]
        value = sorted_cache_ids[key]
        to_evict.append(key)
        size_to_recover = size_to_recover - value[3]
        recovered_size = recovered_size + value[3]
        position = position + 1

    if __debug__:
        logger.debug(HEADER + "Evicting %d entries" % (len(to_evict)))
    # Evict
    for entry in to_evict:
        cache_ids.pop(entry)
    return used_size - recovered_size


def load_shared_memory_manager():
    # type: () -> None
    """ Connects to the main shared memory manager initiated in piper_worker.py.

    :return: None
    """
    global SHARED_MEMORY_MANAGER
    SHARED_MEMORY_MANAGER = create_shared_memory_manager(address=(IP, PORT),
                                                         authkey=AUTH_KEY)
    SHARED_MEMORY_MANAGER.connect()


def start_shared_memory_manager():
    # type: () -> SharedMemoryManager
    """ Starts the shared memory manager.

    :return: Shared memory manager instance.
    """
    smm = create_shared_memory_manager(address=("", PORT),
                                       authkey=AUTH_KEY)
    smm.start()
    return smm


def stop_shared_memory_manager(smm):
    # type: (SharedMemoryManager) -> None
    """ Stops the given shared memory manager, releasing automatically the
    objects contained in it.

    Only needed to be stopped from the main worker process. It is not
    necessary to disconnect each executor.

    :param smm: Shared memory manager.
    :return: None
    """
    smm.shutdown()


@EmitEvent(RETRIEVE_OBJECT_FROM_CACHE_EVENT, master=False, inside=True)
def retrieve_object_from_cache(logger, cache_ids, identifier, parameter_name, user_function, cache_profiler):  # noqa
    # type: (typing.Any, typing.Any, str, str, typing.Any, bool) -> typing.Any
    """ Retrieve an object from the given cache proxy dict.

    :param logger: Logger where to push messages.
    :param cache_ids: Cache proxy dictionary.
    :param identifier: Object identifier.
    :param parameter_name: Parameter name.
    :param user_function: Function name.
    :param cache_profiler: If cache profiling is enabled.
    :return: The object from cache.
    """
    emit_manual_event_explicit(BINDING_DESERIALIZATION_CACHE_SIZE_TYPE, 0)
    identifier = __get_file_name__(identifier)
    if __debug__:
        logger.debug(HEADER + "Retrieving: " + str(identifier))
    obj_id, obj_shape, obj_d_type, _, obj_hits, shared_type = cache_ids[identifier]  # noqa: E501
    size = 0
    output = None        # type: typing.Any
    existing_shm = None  # type: typing.Any
    if shared_type == SHARED_MEMORY_TAG:
        existing_shm = SharedMemory(name=obj_id)
        size = len(existing_shm.buf)
        output = np.ndarray(obj_shape, dtype=obj_d_type, buffer=existing_shm.buf)  # noqa: E501
    elif shared_type == SHAREABLE_LIST_TAG:
        existing_shm = ShareableList(name=obj_id)
        size = len(existing_shm.shm.buf)
        output = list(existing_shm)
    elif shared_type == SHAREABLE_TUPLE_TAG:
        existing_shm = ShareableList(name=obj_id)
        size = len(existing_shm.shm.buf)
        output = tuple(existing_shm)
    # Currently unsupported since conversion requires lists of lists.
    # elif shared_type == SHAREABLE_DICT_TAG:
    #     existing_shm = ShareableList(name=obj_id)
    #     output = dict(existing_shm)
    else:
        raise PyCOMPSsException("Unknown cacheable type.")
    if __debug__:
        logger.debug(HEADER + "Retrieved: " + str(identifier))
    emit_manual_event_explicit(BINDING_DESERIALIZATION_CACHE_SIZE_TYPE, size)

    filename = filename_cleaned(identifier)
    function_name = function_cleaned(user_function)
    if cache_profiler:
        cache_queue.put(("GET", (filename, parameter_name, function_name)))

    cache_ids[identifier][4] = obj_hits + 1
    return output, existing_shm


def insert_object_into_cache_wrapper(logger, cache_queue, obj, f_name, parameter, user_function):  # noqa
    # type: (typing.Any, Queue, typing.Any, str, str, typing.Any) -> None
    """ Put an object into cache filter to avoid event emission when not
    supported.

    :param logger: Logger where to push messages.
    :param cache_queue: Cache notification queue.
    :param obj: Object to store.
    :param f_name: File name that corresponds to the object (used as id).
    :param parameter: Parameter name.
    :param user_function: Function.
    :return: None
    """

    if np and cache_queue is not None and ((isinstance(obj, np.ndarray)
                                            and not obj.dtype == object)
                                           or isinstance(obj, list)
                                           or isinstance(obj, tuple)):
        # or isinstance(obj, dict)):
        insert_object_into_cache(logger, cache_queue, obj, f_name, parameter, user_function)


def insert_object_into_cache(logger, cache_queue, obj, f_name, parameter, user_function):  # noqa
    # type: (typing.Any, Queue, typing.Any, str, str, typing.Any) -> None
    """ Put an object into cache.

    :param logger: Logger where to push messages.
    :param cache_queue: Cache notification queue.
    :param obj: Object to store.
    :param f_name: File name that corresponds to the object (used as id).
    :param parameter: Parameter name.
    :param user_function: Function.
    :return: None
    """
    function = function_cleaned(user_function)
    f_name = __get_file_name__(f_name)
    if __debug__:
        logger.debug(HEADER + "Inserting into cache (%s): %s" %
                     (str(type(obj)), str(f_name)))
    try:
        inserted = True
        if isinstance(obj, np.ndarray):
            emit_manual_event_explicit(BINDING_SERIALIZATION_CACHE_SIZE_TYPE, 0)
            shape = obj.shape
            d_type = obj.dtype
            size = obj.nbytes
            shm = SHARED_MEMORY_MANAGER.SharedMemory(size=size)  # noqa
            within_cache = np.ndarray(shape, dtype=d_type, buffer=shm.buf)
            within_cache[:] = obj[:]  # Copy contents
            new_cache_id = shm.name
            cache_queue.put(("PUT", (
                f_name, new_cache_id, shape, d_type, size, SHARED_MEMORY_TAG, parameter, function)))  # noqa: E501
        elif isinstance(obj, list):
            emit_manual_event_explicit(BINDING_SERIALIZATION_CACHE_SIZE_TYPE, 0)
            sl = SHARED_MEMORY_MANAGER.ShareableList(obj)  # noqa
            new_cache_id = sl.shm.name
            size = total_sizeof(obj)
            cache_queue.put(
                ("PUT", (f_name, new_cache_id, 0, 0, size, SHAREABLE_LIST_TAG, parameter, function)))  # noqa: E501
        elif isinstance(obj, tuple):
            emit_manual_event_explicit(BINDING_SERIALIZATION_CACHE_SIZE_TYPE, 0)
            sl = SHARED_MEMORY_MANAGER.ShareableList(obj)  # noqa
            new_cache_id = sl.shm.name
            size = total_sizeof(obj)
            cache_queue.put(
                ("PUT", (f_name, new_cache_id, 0, 0, size, SHAREABLE_TUPLE_TAG, parameter, function)))  # noqa: E501
        # Unsupported dicts since they are lists of lists when converted.
        # elif isinstance(obj, dict):
        #     # Convert dict to list of tuples
        #     list_tuples = list(zip(obj.keys(), obj.values()))
        #     sl = SHARED_MEMORY_MANAGER.ShareableList(list_tuples)  # noqa
        #     new_cache_id = sl.shm.name
        #     size = total_sizeof(obj)
        #     cache_queue.put(("PUT", (f_name, new_cache_id, 0, 0, size, SHAREABLE_DICT_TAG, parameter, function)))  # noqa: E501
        else:
            inserted = False
            if __debug__:
                logger.debug(HEADER + "Can not put into cache: Not a [np.ndarray | list | tuple ] object")  # noqa: E501
        if inserted:
            emit_manual_event_explicit(BINDING_SERIALIZATION_CACHE_SIZE_TYPE, size)
        if __debug__ and inserted:
            logger.debug(HEADER + "Inserted into cache: " + str(f_name) + " as " + str(new_cache_id))  # noqa: E501
    except KeyError as e:  # noqa
        if __debug__:
            logger.debug(
                HEADER + "Can not put into cache. It may be a [np.ndarray | list | tuple ] object containing an unsupported type")  # noqa: E501
            logger.debug(str(e))


@EmitEvent(REMOVE_OBJECT_FROM_CACHE_EVENT, master=False, inside=True)
def remove_object_from_cache(logger, cache_queue, f_name):  # noqa
    # type: (typing.Any, Queue, str) -> None
    """ Removes an object from cache.

    :param logger: Logger where to push messages.
    :param cache_queue: Cache notification queue.
    :param f_name: File name that corresponds to the object (used as id).
    :return: None
    """
    f_name = __get_file_name__(f_name)
    if __debug__:
        logger.debug(HEADER + "Removing from cache: " + str(f_name))
    cache_queue.put(("REMOVE", f_name))
    if __debug__:
        logger.debug(HEADER + "Removed from cache: " + str(f_name))


def replace_object_into_cache(logger, cache_queue, obj, f_name, parameter, user_function):  # noqa
    # type: (typing.Any, Queue, typing.Any, str, str, typing.Any) -> None
    """ Put an object into cache.

    :param logger: Logger where to push messages.
    :param cache_queue: Cache notification queue.
    :param obj: Object to store.
    :param f_name: File name that corresponds to the object (used as id).
    :param parameter: Parameter name.
    :param user_function: Function.
    :return: None
    """
    f_name = __get_file_name__(f_name)
    if __debug__:
        logger.debug(HEADER + "Replacing from cache: " + str(f_name))
    remove_object_from_cache(logger, cache_queue, f_name)
    insert_object_into_cache(logger, cache_queue, obj, f_name, parameter, user_function)
    if __debug__:
        logger.debug(HEADER + "Replaced from cache: " + str(f_name))


def in_cache(f_name, cache):
    # type: (str, dict) -> bool
    """ Checks if the given file name is in the cache

    :param f_name: Absolute file name.
    :param cache: Proxy dictionary cache.
    :return: True if in. False otherwise.
    """
    if cache:
        f_name = __get_file_name__(f_name)
        return f_name in cache
    else:
        return False


def __get_file_name__(f_name):
    # type: (str) -> str
    """ Convert a full path with file name to the file name (removes the path).
    Example: /a/b/c.py -> c.py

    :param f_name: Absolute file name path
    :return: File name
    """
    return os.path.basename(f_name)


def filename_cleaned(f_name):
    return f_name.rsplit('/', 1)[-1]


def function_cleaned(function):
    return str(function)[10:].rsplit(' ', 3)[0]


def add_profiler_get_put(profiler_dict, function, parameter, filename, type):
    if function not in profiler_dict:
        profiler_dict[function] = {}
    if parameter not in profiler_dict[function]:
        profiler_dict[function][parameter] = {}
    if filename not in profiler_dict[function][parameter]:
        profiler_dict[function][parameter][filename] = {'PUT': 0, 'GET': 0}
    profiler_dict[function][parameter][filename][type] += 1


def add_profiler_get_struct(profiler_get_struct, function, parameter, filename):
    if function not in profiler_get_struct[2] and parameter not in profiler_get_struct[1]:
        profiler_get_struct[0].append(filename)
        profiler_get_struct[1].append(parameter)
        profiler_get_struct[2].append(function)


def profiler_print_message(profiler_dict, profiler_get_struct, log_dir):
    """
    for function in profiler_dict:
        f.write('\t' + "FUNCTION: " + str(function))
        logger.debug('\t' + "FUNCTION: " + str(function))
        for parameter in profiler_dict[function]:
            f.write('\t' + '\t' + '\t' + "PARAMETER: " + str(parameter))
            logger.debug('\t' + '\t' + '\t' + "PARAMETER: " + str(parameter))
            for filename in profiler_dict[function][parameter]:
                f.write('\t' + '\t' + '\t' + '\t' + "FILENAME: " + filename + '\t' + " PUT " +
                        str(profiler_dict[function][parameter][filename]['PUT']) +
                        " GET " + str(profiler_dict[function][parameter][filename]['GET']))
                logger.debug('\t' + '\t' + '\t' + '\t' + "FILENAME: " + filename + '\t' + " PUT " +
                             str(profiler_dict[function][parameter][filename]['PUT']) +
                             " GET " + str(profiler_dict[function][parameter][filename]['GET']))
    f.write("")
    logger.debug("")
    logger.debug("PROFILER GETS")
    for i in range(len(profiler_get_struct[0])):
        logger.debug('\t' + "FILENAME: " + profiler_get_struct[0][i] + ". PARAMETER: " + profiler_get_struct[1][i]
                     + ". FUNCTION: " + profiler_get_struct[2][i])
    """

    final_dict = {}
    for function in profiler_dict:
        final_dict[function] = {}
        for parameter in profiler_dict[function]:
            total_get = 0
            total_put = 0
            is_used = []
            filenames = profiler_dict[function][parameter]
            final_dict[function][parameter] = {}
            for filename in filenames:
                puts = filenames[filename]['PUT']
                if puts > 0:
                    try:
                        index = profiler_get_struct[0].index(filename)
                        is_used.append(profiler_get_struct[2][index] + '#' + profiler_get_struct[1][index])
                    except ValueError:
                        pass
                total_put += puts
                total_get += filenames[filename]['GET']
            final_dict[function][parameter]['GET'] = total_get
            final_dict[function][parameter]['PUT'] = total_put

            if len(is_used) > 0:
                final_dict[function][parameter]['USED'] = is_used
            elif total_get > 0:
                final_dict[function][parameter]['USED'] = [function+"#"+parameter]
            else:
                final_dict[function][parameter]['USED'] = []
    import json
    with open(log_dir + "/../" + PROFILER_LOG, "a") as json_file:
        json.dump(final_dict, json_file)
