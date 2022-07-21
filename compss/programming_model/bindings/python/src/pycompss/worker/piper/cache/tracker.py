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
PyCOMPSs Worker - Piper - Cache Tracker.

This file contains the cache object tracker.
IMPORTANT: Only used with python >= 3.8.
"""


import os
from multiprocessing import Queue

from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.objects.sizer import total_sizeof
from pycompss.util.tracing.helpers import emit_manual_event_explicit
from pycompss.util.tracing.helpers import EventInsideWorker
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.util.typing_helper import typing
from pycompss.worker.piper.cache.profiler import add_profiler_get_put
from pycompss.worker.piper.cache.profiler import add_profiler_get_struct
from pycompss.worker.piper.cache.profiler import profiler_print_message
from pycompss.util.process.manager import create_shared_memory_manager

try:
    from pycompss.util.process.manager import SharedMemory
    from pycompss.util.process.manager import ShareableList
    from pycompss.util.process.manager import SharedMemoryManager
except ImportError:
    # Unsupported in python < 3.8
    SharedMemory = None  # type: ignore
    ShareableList = None  # type: ignore
    SharedMemoryManager = None  # type: ignore

# Try to import numpy
NP = None  # type: typing.Any
try:
    import numpy  # noqa

    NP = numpy
except ImportError:
    pass


class SharedMemoryConfig:
    """Shared memory configuration."""

    __slots__ = ["_auth_key", "_ip_address", "_port"]

    def __init__(self):
        """Initialize a new SharedMemoryConfig instance object.

        All parameters are final.
        """
        self._auth_key = b"compss_cache"
        self._ip_address = "127.0.0.1"
        self._port = 50000

    def get_auth_key(self) -> bytes:
        """Retrieve the authentication key.

        :return: The authentication key.
        """
        return self._auth_key

    def get_ip(self) -> str:
        """Retrieve the IP address.

        :return: The IP address.
        """
        return self._ip_address

    def get_port(self) -> int:
        """Retrieve the port.

        :return: The port.
        """
        return self._port


class CacheTrackerConf:
    """Cache tracker configuration class."""

    __slots__ = [
        "logger",
        "size",
        "policy",
        "cache_ids",
        "cache_hits",
        "profiler_dict",
        "profiler_get_struct",
        "log_dir",
        "cache_profiler",
    ]

    def __init__(
        self,
        logger: typing.Any,
        size: int,
        policy: str,
        cache_ids: typing.Any,
        cache_hits: typing.Dict[int, typing.Dict[str, int]],
        profiler_dict: dict,
        profiler_get_struct: typing.Any,
        log_dir: str,
        cache_profiler: bool,
    ) -> None:
        """Construct a new cache tracker configuration.

        :param logger: Main logger.
        :param size: Total cache size supported.
        :param policy: Eviction policy.
        :param cache_ids: Shared dictionary proxy where the ids and
                          (size, hits) as its value are.
        :param cache_hits: Dictionary containing size and keys to ease management.
        :param profiler_dict: Profiling dictionary.
        :param profiler_get_struct: Profiling get structure.
        :param log_dir: Log directory.
        :param cache_profiler: Cache profiler.
        """
        self.logger = logger
        self.size = size
        self.policy = policy  # currently no policies defined.
        self.cache_ids = cache_ids  # key - (id, shape, dtype, size, hits, shared_type)
        self.cache_hits = cache_hits  # hits - {key1: size1, key2: size2, etc.}
        self.profiler_dict = profiler_dict
        self.profiler_get_struct = profiler_get_struct
        self.log_dir = log_dir
        self.cache_profiler = cache_profiler


class CacheTracker:
    """Cache Tracker manager (shared memory)."""

    __slots__ = [
        "header",
        "shared_memory_manager",
        "shared_memory_tag",
        "shareable_list_tag",
        "shareable_tuple_tag",
        "shareable_dict_tag",
        "config",
    ]

    def __init__(self):
        """Initialize a new SharedMemory instance object."""
        self.header = "[PYTHON CACHE]"
        # Shared memory manager to connect.
        self.shared_memory_manager = None  # type: typing.Any
        # Supported shared objects (remind that nested lists are not supported).
        self.shared_memory_tag = "SharedMemory"
        self.shareable_list_tag = "ShareableList"
        self.shareable_tuple_tag = "ShareableTuple"
        # Currently, dicts are unsupported since conversion requires nesting of lists.
        self.shareable_dict_tag = "ShareableTuple"
        # Configuration
        self.config = SharedMemoryConfig()

    def connect_to_shared_memory_manager(self) -> None:
        """Connect to the main shared memory manager initiated in piper_worker.py.

        :return: None.
        """
        self.shared_memory_manager = create_shared_memory_manager(
            address=(self.config.get_ip(), self.config.get_port()),
            authkey=self.config.get_auth_key(),
        )
        self.shared_memory_manager.connect()

    def start_shared_memory_manager(self) -> SharedMemoryManager:
        """Start the shared memory manager.

        :return: Shared memory manager instance.
        """
        smm = create_shared_memory_manager(
            address=("", self.config.get_port()), authkey=self.config.get_auth_key()
        )
        smm.start()  # pylint: disable=consider-using-with
        return smm

    @staticmethod
    def stop_shared_memory_manager(smm: SharedMemoryManager) -> None:
        """Stop the given shared memory manager.

        Releases automatically the objects contained in it.
        Only needed to be stopped from the main worker process.
        It is not necessary to disconnect each executor.

        :param smm: Shared memory manager.
        :return: None.
        """
        smm.shutdown()

    def check_cache_status(
        self, conf: CacheTrackerConf, used_size: int, requested_size: int
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
                self.header,
                str(requested_size),
            )

        # Sort by number of hits (from lower to higher)
        sorted_hits = sorted(cache_hits.keys())
        # Calculate size to recover
        size_to_recover = used_size + requested_size - max_size
        # Select how many to evict
        to_evict, recovered_size = self.__evict__(
            sorted_hits, cache_hits, size_to_recover
        )
        if __debug__:
            logger.debug("%s Evicting %d entries", self.header, (len(to_evict)))
        # Evict
        for entry in to_evict:
            cache_ids.pop(entry)
        return used_size - recovered_size

    @staticmethod
    def __evict__(
        sorted_hits: typing.List[int],
        cache_hits: typing.Dict[int, typing.Dict[str, int]],
        size_to_recover: int,
    ) -> typing.Tuple[list, int]:
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

    def retrieve_object_from_cache(
        self,
        logger: typing.Any,
        cache_ids: typing.Any,
        cache_queue: Queue,
        identifier: str,
        parameter_name: str,
        user_function: typing.Callable,
        cache_profiler: bool,
    ) -> typing.Any:
        """Retrieve an object from the given cache proxy dict.

        :param logger: Logger where to push messages.
        :param cache_ids: Cache proxy dictionary.
        :param cache_queue: Cache notification queue.
        :param identifier: Object identifier.
        :param parameter_name: Parameter name.
        :param user_function: Function name.
        :param cache_profiler: If cache profiling is enabled.
        :return: The object from cache.
        """
        with EventInsideWorker(TRACING_WORKER.retrieve_object_from_cache_event):
            emit_manual_event_explicit(
                TRACING_WORKER.binding_deserialization_cache_size_type, 0
            )
            identifier = __get_file_name__(identifier)
            if __debug__:
                logger.debug("%s Retrieving: %s", self.header, str(identifier))
            obj_id, obj_shape, obj_d_type, _, obj_hits, shared_type = cache_ids[
                identifier
            ]
            output = None  # type: typing.Any
            existing_shm = None  # type: typing.Any
            object_size = 0
            if shared_type == self.shared_memory_tag:
                existing_shm = SharedMemory(name=obj_id)
                output = NP.ndarray(
                    obj_shape, dtype=obj_d_type, buffer=existing_shm.buf
                )
                object_size = len(existing_shm.buf)
            elif shared_type == self.shareable_list_tag:
                existing_shm = ShareableList(name=obj_id)
                output = list(existing_shm)
                object_size = len(existing_shm.shm.buf)
            elif shared_type == self.shareable_tuple_tag:
                existing_shm = ShareableList(name=obj_id)
                output = tuple(existing_shm)
                object_size = len(existing_shm.shm.buf)
            # Currently, unsupported since conversion requires lists of lists.
            # elif shared_type == self.shareable_dict_tag:
            #     existing_shm = ShareableList(name=obj_id)
            #     output = dict(existing_shm)
            else:
                raise PyCOMPSsException("Unknown cacheable type.")
            if __debug__:
                logger.debug("%s Retrieved: %s", self.header, str(identifier))
            emit_manual_event_explicit(
                TRACING_WORKER.binding_deserialization_cache_size_type, object_size
            )

            # Profiling
            filename = __filename_cleaned__(identifier)
            function_name = __function_cleaned__(user_function)
            if cache_profiler:
                cache_queue.put(("GET", (filename, parameter_name, function_name)))

            # Add hit
            cache_ids[identifier][4] = obj_hits + 1
            return output, existing_shm

    def insert_object_into_cache_wrapper(
        self,
        logger: typing.Any,
        cache_queue: Queue,
        obj: typing.Any,
        f_name: str,
        parameter: str,
        user_function: typing.Callable,
    ) -> None:
        """Put an object into cache filtering supported types.

        :param logger: Logger where to push messages.
        :param cache_queue: Cache notification queue.
        :param obj: Object to store.
        :param f_name: File name that corresponds to the object (used as id).
        :param parameter: Parameter name.
        :param user_function: Function.
        :return: None.
        """
        if (
            NP
            and cache_queue is not None
            and (
                (isinstance(obj, NP.ndarray) and not obj.dtype == object)
                or isinstance(obj, (list, tuple))
            )
        ):
            # or isinstance(obj, dict)):
            self.insert_object_into_cache(
                logger, cache_queue, obj, f_name, parameter, user_function
            )

    def insert_object_into_cache(
        self,
        logger: typing.Any,
        cache_queue: Queue,
        obj: typing.Any,
        f_name: str,
        parameter: str,
        user_function: typing.Callable,
    ) -> None:
        """Put an object into cache.

        :param logger: Logger where to push messages.
        :param cache_queue: Cache notification queue.
        :param obj: Object to store.
        :param f_name: File name that corresponds to the object (used as id).
        :param parameter: Parameter name.
        :param user_function: Function.
        :return: None.
        """
        with EventInsideWorker(TRACING_WORKER.insert_object_into_cache_event):
            function = __function_cleaned__(user_function)
            f_name = __get_file_name__(f_name)
            if __debug__:
                logger.debug(
                    "%s Inserting into cache (%s): %s",
                    self.header,
                    str(type(obj)),
                    str(f_name),
                )
            try:
                inserted = True
                if isinstance(obj, NP.ndarray):
                    emit_manual_event_explicit(
                        TRACING_WORKER.binding_serialization_cache_size_type, 0
                    )
                    shape = obj.shape
                    d_type = obj.dtype
                    size = obj.nbytes
                    shm = self.shared_memory_manager.SharedMemory(size=size)  # noqa
                    within_cache = NP.ndarray(shape, dtype=d_type, buffer=shm.buf)
                    within_cache[:] = obj[:]  # Copy contents
                    new_cache_id = shm.name
                    cache_queue.put(
                        (
                            "PUT",
                            (
                                f_name,
                                new_cache_id,
                                shape,
                                d_type,
                                size,
                                self.shared_memory_tag,
                                parameter,
                                function,
                            ),
                        )
                    )
                elif isinstance(obj, list):
                    emit_manual_event_explicit(
                        TRACING_WORKER.binding_serialization_cache_size_type, 0
                    )
                    shareable_list = self.shared_memory_manager.ShareableList(
                        obj
                    )  # noqa
                    new_cache_id = shareable_list.shm.name
                    size = total_sizeof(obj)
                    cache_queue.put(
                        (
                            "PUT",
                            (
                                f_name,
                                new_cache_id,
                                0,
                                0,
                                size,
                                self.shareable_list_tag,
                                parameter,
                                function,
                            ),
                        )
                    )
                elif isinstance(obj, tuple):
                    emit_manual_event_explicit(
                        TRACING_WORKER.binding_serialization_cache_size_type, 0
                    )
                    shareable_list = self.shared_memory_manager.ShareableList(
                        obj
                    )  # noqa
                    new_cache_id = shareable_list.shm.name
                    size = total_sizeof(obj)
                    cache_queue.put(
                        (
                            "PUT",
                            (
                                f_name,
                                new_cache_id,
                                0,
                                0,
                                size,
                                self.shareable_tuple_tag,
                                parameter,
                                function,
                            ),
                        )
                    )
                # Unsupported dicts since they are lists of lists when converted.
                # elif isinstance(obj, dict):
                #     # Convert dict to list of tuples
                #     list_tuples = list(zip(obj.keys(), obj.values()))
                #     sl = SHARED_MEMORY_MANAGER.ShareableList(list_tuples)  # noqa
                #     new_cache_id = sl.shm.name
                #     size = total_sizeof(obj)
                #     cache_queue.put(
                #         (
                #             "PUT",
                #             (
                #                 f_name,
                #                 new_cache_id,
                #                 0,
                #                 0,
                #                 size,
                #                 self.shareable_dict_tag,
                #                 parameter,
                #                 function
                #             ),
                #         )
                #     )
                else:
                    inserted = False
                    if __debug__:
                        logger.debug(
                            "%s Can not put into cache: Not a [NP.ndarray | list | tuple ] object",
                            self.header,
                        )
                if inserted:
                    emit_manual_event_explicit(
                        TRACING_WORKER.binding_serialization_cache_size_type, size
                    )
                if __debug__ and inserted:
                    logger.debug(
                        "%s Inserted into cache: %s as %s",
                        self.header,
                        str(f_name),
                        str(new_cache_id),
                    )
            except KeyError as key_error:  # noqa
                if __debug__:
                    logger.debug(
                        "%s Can not put into cache. It may be a "
                        "[NP.ndarray | list | tuple ] object containing "
                        "an unsupported type",
                        self.header,
                    )
                    logger.debug(str(key_error))

    def remove_object_from_cache(
        self, logger: typing.Any, cache_queue: Queue, f_name: str
    ) -> None:
        """Remove an object from cache.

        :param logger: Logger where to push messages.
        :param cache_queue: Cache notification queue.
        :param f_name: File name that corresponds to the object (used as id).
        :return: None.
        """
        with EventInsideWorker(TRACING_WORKER.remove_object_from_cache_event):
            f_name = __get_file_name__(f_name)
            if __debug__:
                logger.debug("%s Removing from cache: %s", self.header, str(f_name))
            cache_queue.put(("REMOVE", f_name))
            if __debug__:
                logger.debug("%s Removed from cache: %s", self.header, str(f_name))

    def replace_object_into_cache(
        self,
        logger: typing.Any,
        cache_queue: Queue,
        obj: typing.Any,
        f_name: str,
        parameter: str,
        user_function: typing.Callable,
    ) -> None:
        """Put an object into cache.

        :param logger: Logger where to push messages.
        :param cache_queue: Cache notification queue.
        :param obj: Object to store.
        :param f_name: File name that corresponds to the object (used as id).
        :param parameter: Parameter name.
        :param user_function: Function.
        :return: None.
        """
        f_name = __get_file_name__(f_name)
        if __debug__:
            logger.debug("%s Replacing from cache: %s", self.header, str(f_name))
        self.remove_object_from_cache(logger, cache_queue, f_name)
        self.insert_object_into_cache(
            logger, cache_queue, obj, f_name, parameter, user_function
        )
        if __debug__:
            logger.debug("%s Replaced from cache: %s", self.header, str(f_name))

    @staticmethod
    def in_cache(f_name: str, cache: typing.Any) -> bool:
        """Check if the given file name is in the cache.

        :param f_name: Absolute file name.
        :param cache: Proxy dictionary cache.
        :return: True if in. False otherwise.
        """
        if cache:
            f_name = __get_file_name__(f_name)
            return f_name in cache
        return False


CACHE_TRACKER = CacheTracker()


def cache_tracker(queue: Queue, process_name: str, conf: CacheTrackerConf) -> None:
    """Process main body.

    :param queue: Queue where to put exception messages.
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
            "%s [%s] Starting Cache Tracker", CACHE_TRACKER.header, str(process_name)
        )

    # MAIN CACHE TRACKER LOOP
    used_size = 0
    while alive:
        msg = queue.get()
        if msg == "QUIT":
            if __debug__:
                logger.debug(
                    "%s [%s] Stopping Cache Tracker: %s",
                    CACHE_TRACKER.header,
                    str(process_name),
                    str(msg),
                )
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
                        add_profiler_get_put(
                            profiler_dict, function, parameter, filename, "GET"
                        )
                        # PROFILER GET STRUCTURE
                        add_profiler_get_struct(
                            profiler_get_struct, function, parameter, filename
                        )
                if action == "PUT":
                    (
                        f_name,
                        cache_id,
                        shape,
                        dtype,
                        obj_size,
                        shared_type,
                        parameter,
                        function,
                    ) = message
                    if f_name in cache_ids:
                        if cache_profiler:
                            # PROFILER PUT
                            add_profiler_get_put(
                                profiler_dict,
                                function,
                                parameter,
                                __filename_cleaned__(f_name),
                                "PUT",
                            )
                        # Any executor has already put the id
                        if __debug__:
                            logger.debug(
                                "%s [%s] Cache hit",
                                CACHE_TRACKER.header,
                                str(process_name),
                            )
                        # Increment hits
                        current = cache_ids[f_name]
                        current_hits = current[4]
                        new_hits = current_hits + 1
                        current[4] = new_hits
                        cache_ids[f_name] = current  # forces updating whole entry
                        # Keep cache_hits structure
                        cache_hits[current_hits].pop(f_name)
                        if new_hits in cache_hits:
                            cache_hits[new_hits][f_name] = obj_size
                        else:
                            cache_hits[new_hits] = {f_name: obj_size}
                    else:
                        # Add new entry request
                        if __debug__:
                            logger.debug(
                                "%s [%s] Cache add entry: %s",
                                CACHE_TRACKER.header,
                                str(process_name),
                                str(msg),
                            )
                        if cache_profiler:
                            # PROFILER PUT
                            add_profiler_get_put(
                                profiler_dict,
                                function,
                                parameter,
                                __filename_cleaned__(f_name),
                                "PUT",
                            )

                        # Check if it is going to fit and remove if necessary
                        obj_size = int(obj_size)
                        if used_size + obj_size > max_size:
                            # Cache is full, need to evict
                            used_size = CACHE_TRACKER.check_cache_status(
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
                        cache_hits[hits] = {f_name: obj_size}
                elif action == "REMOVE":
                    f_name = __get_file_name__(message)
                    logger.debug(
                        "%s [%s] Removing: %s",
                        CACHE_TRACKER.header,
                        str(process_name),
                        str(f_name),
                    )
                    cache_ids.pop(f_name)
            except Exception as general_exception:
                logger.exception(
                    "%s - Exception %s", str(process_name), str(general_exception)
                )
                alive = False


def __get_file_name__(f_name: str) -> str:
    """Convert a full path with file name to the file name (removes the path).

    Example: /a/b/c.py -> c.py

    :param f_name: Absolute file name path.
    :return: File name.
    """
    return os.path.basename(f_name)


def __filename_cleaned__(f_name: str) -> str:
    """Retrieve filename given the absolute path.

    :param f_name: Absolute file path.
    :return: File name.
    """
    return f_name.rsplit("/", 1)[-1]


def __function_cleaned__(function: typing.Callable) -> str:
    """Retrieve the clean function name.

    :param function: Function.
    :return: Function name.
    """
    return str(function)[10:].rsplit(" ", 3)[0]
