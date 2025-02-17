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
PyCOMPSs Worker - Piper - Cache Tracker.

This file contains the cache object tracker.
IMPORTANT: Only used with python >= 3.8.
"""
import base64
import logging
import os
from typing import Union

from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.objects.sizer import total_sizeof
from pycompss.util.tracing.helpers import emit_manual_event_explicit
from pycompss.util.tracing.helpers import EventInsideWorker
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.util.typing_helper import typing
from pycompss.util.process.manager import create_shared_memory_manager
from pycompss.util.process.manager import Queue
from pycompss.util.process.manager import DictProxy
from pycompss.worker.piper.cache.classes import CacheQueueMessage

try:
    from pycompss.util.process.manager import SharedMemory
    from pycompss.util.process.manager import ShareableList
    from pycompss.util.process.manager import SharedMemoryManager
except ImportError:
    # Unsupported in python < 3.8
    SharedMemory = None  # type: ignore
    ShareableList = None  # type: ignore
    SharedMemoryManager = None  # type: ignore


# Supported shared objects (remind that nested lists are not supported).
SHARED_MEMORY_TAG = "SharedMemory"
SHARED_MEMORY_CUPY_TAG = "SharedCupyCudaMemory"
SHAREABLE_LIST_TAG = "ShareableList"
SHAREABLE_TUPLE_TAG = "ShareableTuple"
SHARED_MEMORY_TORCH_TAG = "SharedTorchCudaMemory"

# Try to import numpy
NP = None  # type: typing.Any
CP = None  # type: typing.Any
TORCH = None  # type: typing.Any
try:
    import numpy  # noqa

    NP = numpy
except ImportError:
    print(
        "WARNING: No Numpy available. "
        "Cache using Numpy objects will not be available"
    )


def __set_cuda_libs__():
    global TORCH
    try:
        import torch  # noqa

        TORCH = torch
    except ImportError:
        pass

    global CP
    try:
        import cupy

        CP = cupy
    except ImportError:
        pass


class SharedMemoryConfig:
    """Shared memory configuration."""

    __slots__ = ["_auth_key", "_ip_address", "_port"]

    def __init__(self) -> None:
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
        "gpu_cache_size",
        "policy",
        "cache_ids",
        "cache_hits",
        "profiler_dict",
        "profiler_get_struct",
        "analysis_dir",
        "cache_profiler",
        "gpu_arr_ptr",
    ]

    def __init__(
        self,
        logger: logging.Logger,
        size: Union[float, int],
        gpu_cache_size: Union[float, int],
        policy: str,
        cache_ids: DictProxy,
        cache_hits: typing.Dict[int, typing.Dict[str, int]],
        profiler_dict: typing.Dict[
            str, typing.Dict[str, typing.Dict[str, typing.Dict[str, int]]]
        ],
        profiler_get_struct: typing.List[typing.List[str]],
        analysis_dir: str,
        cache_profiler: bool,
    ) -> None:
        """Construct a new cache tracker configuration.

        :param logger: Main logger.
        :param size: Total cache size supported.
        :param policy: Eviction policy.
        :param cache_ids: Shared dictionary proxy where the ids and
                          (size, hits) as its value are.
        :param cache_hits: Dictionary containing size and keys to ease
                           management.
        :param profiler_dict: Profiling dictionary.
        :param profiler_get_struct: Profiling get structure.
        :param analysis_dir: Analysis directory.
        :param cache_profiler: Cache profiler.
        """
        self.logger = logger
        self.size = size
        self.policy = policy  # currently no policies defined.
        self.cache_ids = (
            cache_ids  # key - (id, shape, dtype, size, hits, shared_type)
        )
        self.cache_hits = cache_hits  # hits - {key1: size1, key2: size2, etc.}
        self.profiler_dict = profiler_dict
        self.profiler_get_struct = profiler_get_struct
        self.analysis_dir = analysis_dir
        self.cache_profiler = cache_profiler

        self.gpu_cache_size = gpu_cache_size
        self.gpu_arr_ptr: typing.Dict[str, typing.List[int]] = (
            {}
        )  # cache_id - (cupy_data_ptr, device_id)


class CacheTracker:
    """Cache Tracker manager (shared memory)."""

    __slots__ = [
        "header",
        "shared_memory_manager",
        "config",
        "cupy_handlers",
        "lock",
    ]

    def __init__(self) -> None:
        """Initialize a new SharedMemory instance object."""
        self.header = "[PYTHON CACHE TRACKER]"
        # Shared memory manager to connect.
        self.shared_memory_manager = None  # type: typing.Any
        # Configuration
        self.config = SharedMemoryConfig()
        # Others
        self.lock = None  # type: typing.Any

        self.cupy_handlers: typing.Dict[str, bytes] = dict()

    def set_lock(self, lock: typing.Any) -> None:
        """Set lock for coherence.

        :param lock: Multiprocessing lock.
        :return: None
        """
        self.lock = lock

    def connect_to_shared_memory_manager(self) -> None:
        """Connect to the shared memory manager initiated in piper_worker.py.

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
            address=("", self.config.get_port()),
            authkey=self.config.get_auth_key(),
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

    def __get_shared_numpy(self, obj_id, obj_shape: typing.Tuple, obj_d_type):
        """Get shared numpy array.

        :param obj_id: Object identifier.
        :param obj_shape: Numpy array shape.
        :param obj_d_type: Numpy array dtype.
        :return: Shared memory segment, numpy array and its size.
        """
        existing_shm = SharedMemory(name=obj_id)
        shm_np = NP.ndarray(
            obj_shape, dtype=obj_d_type, buffer=existing_shm.buf
        )
        output = NP.empty(obj_shape, dtype=obj_d_type)
        NP.copyto(output, shm_np)
        object_size = len(existing_shm.buf)
        return existing_shm, output, object_size

    def __get_shared_cupy(self, obj_id, obj_shape: typing.Tuple, obj_d_type):
        """Get shared cupy array.

        :param obj_id: Object identifier.
        :param obj_shape: Cupy array shape.
        :param obj_d_type: Cupy array dtype.
        :return: None, cupy array and its size.
        """
        if obj_id in self.cupy_handlers:
            array_open = self.cupy_handlers[obj_id]
        else:
            handler = base64.b64decode(obj_id)
            array_open = CP.cuda.runtime.ipcOpenMemHandle(handler)
            self.cupy_handlers[obj_id] = array_open

        mem = CP.cuda.UnownedMemory(array_open, 0, obj_id)

        memptr = CP.cuda.MemoryPointer(mem, 0)
        output = CP.ndarray(shape=obj_shape, dtype=obj_d_type, memptr=memptr)

        return None, output, output.nbytes

    def __get_shared_list(self, obj_id, i_type):
        """Get iterable object.

        :param obj_id: Object identifier.
        :param i_type: Object type (can be tuple or list).
        :return: Shared memory segment, iterable object and its size.
        """
        existing_shm = ShareableList(name=obj_id)
        output = i_type(existing_shm)
        object_size = len(existing_shm.shm.buf)
        return existing_shm, output, object_size

    def close_cupy_mem_handles(self):
        """Close all memory handles from cupy arrays.

        :return: None.
        """
        for handle in self.cupy_handlers.values():
            CP.cuda.runtime.ipcCloseMemHandle(handle)
        self.cupy_handlers.clear()

    def retrieve_object_from_cache(
        self,
        logger: logging.Logger,
        cache_ids: typing.Any,
        in_cache_queue: Queue,
        out_cache_queue: Queue,
        identifier: str,
        parameter_name: str,
        user_function: typing.Callable,
        cache_profiler: bool,
    ) -> typing.Any:
        """Retrieve an object from the given cache proxy dict.

        :param logger: Logger where to push messages.
        :param cache_ids: Cache proxy dictionary.
        :param in_cache_queue: Cache notification input queue.
        :param out_cache_queue: Cache notification output queue.
        :param identifier: Object identifier.
        :param parameter_name: Parameter name.
        :param user_function: Function name.
        :param cache_profiler: If cache profiling is enabled.
        :return: The object from cache.
        """
        __set_cuda_libs__()

        emit_manual_event_explicit(
            TRACING_WORKER.deserialization_cache_size_type, 0
        )
        f_name = get_file_name(identifier)
        if __debug__:
            logger.debug("%s Retrieving: %s", self.header, str(f_name))
        obj_id, obj_shape, obj_d_type, _, _, shared_type = cache_ids[f_name]
        output = None  # type: typing.Any
        existing_shm = None  # type: typing.Any
        object_size = 0

        if shared_type == SHARED_MEMORY_TAG:
            with EventInsideWorker(
                TRACING_WORKER.retrieve_object_from_cache_event
            ):
                existing_shm, output, object_size = self.__get_shared_numpy(
                    obj_id, obj_shape, obj_d_type
                )
        elif shared_type == SHARED_MEMORY_TORCH_TAG:
            with EventInsideWorker(
                TRACING_WORKER.retrieve_object_from_gpu_cache_event
            ):
                existing_shm, output, object_size = self.__get_shared_cupy(
                    obj_id, obj_shape, obj_d_type
                )
                output = TORCH.as_tensor(output, device="cuda")
        elif shared_type == SHARED_MEMORY_CUPY_TAG:
            with EventInsideWorker(
                TRACING_WORKER.retrieve_object_from_gpu_cache_event
            ):
                existing_shm, output, object_size = self.__get_shared_cupy(
                    obj_id, obj_shape, obj_d_type
                )
        elif shared_type == SHAREABLE_LIST_TAG:
            with EventInsideWorker(
                TRACING_WORKER.retrieve_object_from_cache_event
            ):
                existing_shm, output, object_size = self.__get_shared_list(
                    obj_id, list
                )
        elif shared_type == SHAREABLE_TUPLE_TAG:
            with EventInsideWorker(
                TRACING_WORKER.retrieve_object_from_cache_event
            ):
                existing_shm, output, object_size = self.__get_shared_list(
                    obj_id, tuple
                )
        else:
            raise PyCOMPSsException("Unknown cacheable type.")
        if __debug__:
            logger.debug(
                "%s Retrieved: %s as %s", self.header, str(f_name), obj_id
            )
        emit_manual_event_explicit(
            TRACING_WORKER.deserialization_cache_size_type, object_size
        )

        # Profiling
        filename = get_file_name_clean(f_name)
        function_name = __function_clean__(user_function)

        message = CacheQueueMessage(
            action="GET", messages=[filename, parameter_name, function_name]
        )
        in_cache_queue.put(message)

        return output, existing_shm

    def insert_object_into_cache(
        self,
        logger: logging.Logger,
        in_cache_queue: Queue,
        out_cache_queue: Queue,
        obj: typing.Any,
        f_name: str,
        parameter: str,
        user_function: typing.Callable,
    ) -> None:
        """Put an object into cache filtering supported types.

        :param logger: Logger where to push messages.
        :param in_cache_queue: Cache notification input queue.
        :param out_cache_queue: Cache notification output queue.
        :param obj: Object to store.
        :param f_name: File name that corresponds to the object (used as id).
        :param parameter: Parameter name.
        :param user_function: Function.
        :return: None.
        """
        __set_cuda_libs__()

        if (
            NP
            and in_cache_queue is not None
            and out_cache_queue is not None
            and (
                (isinstance(obj, NP.ndarray) and not obj.dtype == object)
                or (
                    CP
                    and isinstance(obj, CP.ndarray)
                    and not obj.dtype == object
                )
                or (
                    TORCH
                    and isinstance(obj, TORCH.Tensor)
                    and obj.device.type == "cuda"
                )
                or isinstance(obj, (list, tuple))
            )
        ):
            self.__insert_object_into_cache(
                logger,
                in_cache_queue,
                out_cache_queue,
                obj,
                f_name,
                parameter,
                user_function,
            )

    def __insert_numpy_cache(
        self, obj, f_name, parameter, function, in_cache_queue
    ):
        emit_manual_event_explicit(
            TRACING_WORKER.serialization_cache_size_type, 0
        )
        shape = obj.shape
        d_type = obj.dtype
        size = obj.nbytes
        new_cache_id = None

        if size > 0:
            with EventInsideWorker(
                TRACING_WORKER.insert_object_into_cache_event
            ):
                # This line takes most of the time to put into cache
                shm = self.shared_memory_manager.SharedMemory(size=size)
                within_cache = NP.ndarray(shape, dtype=d_type, buffer=shm.buf)
                within_cache[:] = obj[:]  # Copy contents
                new_cache_id = shm.name
                message = CacheQueueMessage(
                    action="PUT",
                    messages=[
                        f_name,
                        new_cache_id,
                        SHARED_MEMORY_TAG,
                        parameter,
                        function,
                    ],
                    size=size,
                    d_type=d_type,
                    shape=shape,
                )
                in_cache_queue.put(message)

        return size, new_cache_id

    def __insert_cupy_cache(
        self,
        obj,
        f_name,
        parameter,
        function,
        shared_tag,
        in_cache_queue: Queue,
        out_cache_queue: Queue,
    ):
        """Insert cupy array into cache.

        :param f_name: File name that corresponds to the object (used as id).
        :param parameter: Parameter name.
        :param function: Function.
        :param shared_tag: Tag of the object (CUPY or TORCH)
        :param in_cache_queue: Cache notification input queue.
        :param out_cache_queue: Cache notification output queue.
        :return: Size and identifier.
        """
        emit_manual_event_explicit(
            TRACING_WORKER.serialization_cache_size_type, 0
        )
        shape = obj.shape
        d_type = obj.dtype
        size = obj.nbytes
        new_cache_id = None

        if size > 0:
            with EventInsideWorker(
                TRACING_WORKER.insert_object_into_gpu_cache_event
            ):
                ipc_handle = CP.cuda.runtime.ipcGetMemHandle(obj.data.ptr)

                new_cache_id = base64.b64encode(ipc_handle)
                message = CacheQueueMessage(
                    action="PUT_GPU",
                    messages=[
                        f_name,
                        new_cache_id.decode("ascii"),
                        shared_tag,
                        parameter,
                        function,
                        obj.device.pci_bus_id,
                    ],
                    size=size,
                    d_type=d_type,
                    shape=shape,
                )
                in_cache_queue.put(message)
        out_cache_queue.get()
        return size, new_cache_id

    def __insert_iterable_cache(
        self,
        obj: Union[list, tuple],
        f_name,
        parameter,
        function,
        in_cache_queue,
        type,
    ):
        """Insert iterable object into cache.

        :param f_name: File name that corresponds to the object (used as id).
        :param parameter: Parameter name.
        :param function: Function.
        :param in_cache_queue: Cache notification input queue.
        :param type: Iterable type (can be list or tuple).
        :return: Size and identifier.
        """
        size = total_sizeof(obj)
        new_cache_id = None

        if size > 0:
            with EventInsideWorker(
                TRACING_WORKER.insert_object_into_cache_event
            ):
                emit_manual_event_explicit(
                    TRACING_WORKER.serialization_cache_size_type, 0
                )
                shareable_list = self.shared_memory_manager.ShareableList(
                    obj
                )  # noqa
                new_cache_id = shareable_list.shm.name
                if isinstance(obj, list):
                    tag = SHAREABLE_LIST_TAG
                else:
                    tag = SHAREABLE_TUPLE_TAG
                message = CacheQueueMessage(
                    action="PUT",
                    messages=[
                        f_name,
                        new_cache_id,
                        tag,
                        parameter,
                        function,
                    ],
                    size=size,
                    d_type=type,
                    shape=(),  # only used with numpy
                )
                in_cache_queue.put(message)

        return size, new_cache_id

    def __insert_object_into_cache(
        self,
        logger: logging.Logger,
        in_cache_queue: Queue,
        out_cache_queue: Queue,
        obj: typing.Any,
        f_name: str,
        parameter: str,
        user_function: typing.Callable,
    ) -> None:
        """Put an object into cache.

        :param logger: Logger where to push messages.
        :param in_cache_queue: Cache notification input queue.
        :param out_cache_queue: Cache notification output queue.
        :param obj: Object to store.
        :param f_name: File name that corresponds to the object (used as id).
        :param parameter: Parameter name.
        :param user_function: Function.
        :return: None.
        """
        __set_cuda_libs__()

        function = __function_clean__(user_function)
        f_name = get_file_name(f_name)
        # Exclusion in locking to avoid multiple insertions
        # If no lock is defined may lead to unstable behaviour.
        if self.lock is not None:
            self.lock.acquire()
        message = CacheQueueMessage(action="IS_LOCKED", messages=[f_name])
        in_cache_queue.put(message)
        is_locked = out_cache_queue.get()
        message = CacheQueueMessage(action="IS_IN_CACHE", messages=[f_name])
        in_cache_queue.put(message)
        is_in_cache = out_cache_queue.get()
        if not is_locked and not is_in_cache:
            message = CacheQueueMessage(action="LOCK", messages=[f_name])
            in_cache_queue.put(message)
        if self.lock is not None:
            self.lock.release()
        if is_locked:
            if __debug__:
                logger.debug(
                    "%s Not inserting into cache due to it is being "
                    "inserted by other process: %s",
                    self.header,
                    str(f_name),
                )
        elif is_in_cache:
            if __debug__:
                logger.debug(
                    "%s Not inserting into cache due already exists "
                    "in cache: %s",
                    self.header,
                    str(f_name),
                )
        else:
            # Not locked and not in cache
            if __debug__:
                logger.debug(
                    "%s Inserting into cache (%s): %s",
                    self.header,
                    str(type(obj)),
                    str(f_name),
                )
            try:
                inserted = True
                size = 0
                if isinstance(obj, NP.ndarray):
                    size, new_cache_id = self.__insert_numpy_cache(
                        obj, f_name, parameter, function, in_cache_queue
                    )
                elif TORCH and isinstance(obj, TORCH.Tensor) and CP:
                    cupy_tensor = CP.asarray(obj)
                    shared_tag = SHARED_MEMORY_TORCH_TAG
                    size, new_cache_id = self.__insert_cupy_cache(
                        cupy_tensor,
                        f_name,
                        parameter,
                        function,
                        shared_tag,
                        in_cache_queue,
                        out_cache_queue,
                    )
                elif CP and isinstance(obj, CP.ndarray):
                    shared_tag = SHARED_MEMORY_CUPY_TAG
                    size, new_cache_id = self.__insert_cupy_cache(
                        obj,
                        f_name,
                        parameter,
                        function,
                        shared_tag,
                        in_cache_queue,
                        out_cache_queue,
                    )
                elif isinstance(obj, list) or isinstance(obj, tuple):
                    size, new_cache_id = self.__insert_iterable_cache(
                        obj,
                        f_name,
                        parameter,
                        function,
                        in_cache_queue,
                        type(obj),
                    )
                else:
                    inserted = False
                    if __debug__:
                        logger.debug(
                            "%s Can not put into cache: "
                            "Not a [NP.ndarray | list | tuple ] object",
                            self.header,
                        )

                if size == 0:
                    inserted = False

                if inserted:
                    emit_manual_event_explicit(
                        TRACING_WORKER.serialization_cache_size_type,
                        size,
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
            message = CacheQueueMessage(action="UNLOCK", messages=[f_name])
            in_cache_queue.put(message)

    def __remove_object_from_cache(
        self,
        logger: logging.Logger,
        in_cache_queue: Queue,
        out_cache_queue: Queue,
        f_name: str,
    ) -> None:
        """Remove an object from cache.

        :param logger: Logger where to push messages.
        :param in_cache_queue: Cache notification input queue.
        :param out_cache_queue: Cache notification output queue.
        :param f_name: File name that corresponds to the object (used as id).
        :return: None.
        """
        with EventInsideWorker(TRACING_WORKER.remove_object_from_cache_event):
            f_name = get_file_name(f_name)
            if __debug__:
                logger.debug(
                    "%s Removing from cache: %s", self.header, str(f_name)
                )
            message = CacheQueueMessage(action="REMOVE", messages=[f_name])
            in_cache_queue.put(message)
            if __debug__:
                logger.debug(
                    "%s Removed from cache: %s", self.header, str(f_name)
                )

    def replace_object_into_cache(
        self,
        logger: logging.Logger,
        in_cache_queue: Queue,
        out_cache_queue: Queue,
        obj: typing.Any,
        f_name: str,
        parameter: str,
        user_function: typing.Callable,
    ) -> None:
        """Put an object into cache.

        :param logger: Logger where to push messages.
        :param in_cache_queue: Cache notification input queue.
        :param out_cache_queue: Cache notification output queue.
        :param obj: Object to store.
        :param f_name: File name that corresponds to the object (used as id).
        :param parameter: Parameter name.
        :param user_function: Function.
        :return: None.
        """
        f_name = get_file_name(f_name)
        if __debug__:
            logger.debug(
                "%s Replacing from cache: %s", self.header, str(f_name)
            )
        self.__remove_object_from_cache(
            logger, in_cache_queue, out_cache_queue, f_name
        )
        self.__insert_object_into_cache(
            logger,
            in_cache_queue,
            out_cache_queue,
            obj,
            f_name,
            parameter,
            user_function,
        )
        if __debug__:
            logger.debug(
                "%s Replaced from cache: %s", self.header, str(f_name)
            )

    def in_cache(
        self, logger: logging.Logger, f_name: str, cache: typing.Any
    ) -> bool:
        """Check if the given file name is in the cache.

        :param logger: Logger where to push messages.
        :param f_name: Absolute file name.
        :param cache: Proxy dictionary cache.
        :return: True if in. False otherwise.
        """
        # It can be checked if it is locked and wait for it to be unlocked
        if cache:
            f_name = get_file_name(f_name)
            if __debug__:
                logger.debug("%s Is in cache? %s", self.header, str(f_name))

            if f_name in cache:
                obj_id, _, _, _, _, shared_type = cache[f_name]

                if shared_type == SHARED_MEMORY_CUPY_TAG:
                    with EventInsideWorker(
                        TRACING_WORKER.check_access_gpu_event
                    ):
                        is_in_gpu = self.__check_cupy_access__(logger, obj_id)
                    if is_in_gpu:
                        event = EventInsideWorker(
                            TRACING_WORKER.cache_hit_gpu_event
                        )
                    else:
                        event = EventInsideWorker(
                            TRACING_WORKER.cache_miss_gpu_event
                        )
                    with event:
                        return is_in_gpu
                return True

        return False

    def __check_cupy_access__(self, logger, obj_id):
        """Check if object accessible by cupy.

        Check if current GPU has access to the memory of the gpu where the
        handle is pointing.
        """
        if obj_id in self.cupy_handlers:
            return True

        import cupy
        from cupy_backends.cuda.api.runtime import CUDARuntimeError

        try:
            handler = base64.b64decode(obj_id)
            array_open = cupy.cuda.runtime.ipcOpenMemHandle(handler)
            cupy.cuda.runtime.ipcCloseMemHandle(array_open)
            return True
        except CUDARuntimeError as e:
            logger.debug(
                "%s CHECK CUPY ACCESS CUDARuntimeError %s", self.header, str(e)
            )
            return False


CACHE_TRACKER = CacheTracker()


def get_file_name(f_name: str) -> str:
    """Convert a full path with file name to the file name (removes the path).

    Example: /a/b/c.py -> c.py

    :param f_name: Absolute file name path.
    :return: File name.
    """
    return os.path.basename(f_name)


def get_file_name_clean(f_name: str) -> str:
    """Retrieve filename given the absolute path.

    :param f_name: Absolute file path.
    :return: File name.
    """
    return f_name.rsplit("/", 1)[-1]


def __function_clean__(function: typing.Callable) -> str:
    """Retrieve the clean function name.

    :param function: Function.
    :return: Function name.
    """
    return str(function)[10:].rsplit(" ", 3)[0]
