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
PyCOMPSs Util - Serialization - Serializer/deserializer.

This file implements the main serialization/deserialization functions.
All serialization/deserialization calls should be made using one of the
following functions:

- serialize_to_file(obj, file_name) -> dumps the object "obj" to the file
                                       "file_name"
- serialize_to_string(obj) -> dumps the object "obj" to a string
- serialize_to_handler(obj, handler) -> writes the serialized object using
                                        the specified handler it also moves
                                        the handler's pointer to the end of
                                        the dump

- deserialize_from_file(file_name) -> loads the first object from the tile
                                      "file_name"
- deserialize_from_string(serialized_content) -> loads the first object
                                                 from the given string
- deserialize_from_handler(handler) -> deserializes an object using the
                                       given handler, it also leaves the
                                       handler's pointer pointing to the
                                       end of the serialized object
"""

import gc
import json
import logging  # typing purposes
import os
import pickle
import struct
import traceback
import types

from io import BytesIO

from pycompss.util.exceptions import SerializerException
from pycompss.util.objects.properties import object_belongs_to_module
from pycompss.util.serialization.extended_support import GeneratorIndicator
from pycompss.util.serialization.extended_support import convert_to_generator
from pycompss.util.serialization.extended_support import pickle_generator
from pycompss.util.tracing.helpers import emit_manual_event_explicit
from pycompss.util.tracing.helpers import EventInsideWorker
from pycompss.util.tracing.types_events_master import TRACING_MASTER
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.util.typing_helper import typing

try:
    import dill  # noqa

    DILL_AVAILABLE = True
except ImportError:
    DILL_AVAILABLE = False

try:
    import numpy

    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False


try:
    import cupy

    CUPY_AVAILABLE = True
except ImportError:
    CUPY_AVAILABLE = False


try:
    import pyarrow

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

try:
    from pyeddl._core import Net as eddlNet
    from pyeddl.eddl import serialize_net_to_onnx_string
    from pyeddl.eddl import import_net_from_onnx_file

    PYEDDL_AVAILABLE = True
except ImportError:
    PYEDDL_AVAILABLE = False

# GLOBALS

# LIB2IDX contains as key the serializer and value its associated integer
LIB2IDX = {pickle: 0}  # type: typing.Dict[types.ModuleType, int]
if DILL_AVAILABLE:
    LIB2IDX[dill] = 1
if NUMPY_AVAILABLE:
    LIB2IDX[numpy] = 2
if PYARROW_AVAILABLE:
    LIB2IDX[pyarrow] = 3
LIB2IDX[json] = 4
if PYEDDL_AVAILABLE:
    LIB2IDX[eddlNet] = 5
if CUPY_AVAILABLE:
    LIB2IDX[cupy] = 6
# IDX2LIB contains as key the integer and the value its associated serializer
IDX2LIB = dict(
    ((v, k) for (k, v) in LIB2IDX.items())
)  # type: typing.Dict[int, types.ModuleType]
# Max integer
PLATFORM_C_MAXINT = 2 ** ((struct.Struct("i").size * 8 - 1) - 13)
# To force a specific serializer
FORCED_SERIALIZER = -1  # make a serializer the only option for serialization
# Control Garbage Collector
DISABLE_GC = False


def get_available_libraries() -> (
    typing.List[typing.Tuple[int, str, typing.Optional[str]]]
):
    """Return the available serialization libraries.

    Checks the available serializers and returns a dict of names and
    its module for the active serializers.

    :return: Dictionary of available serializers.
    """
    active_serializers = []
    for library, priority in LIB2IDX.items():
        try:
            active_serializers.append(
                (priority, library.__name__, library.__file__)
            )
        except AttributeError:
            active_serializers.append(
                (priority, library.__name__, str(library))
            )
    return active_serializers


def get_serializer_priority(
    obj: typing.Any, logger: logging.Logger
) -> typing.List[types.ModuleType]:
    """Compute the priority of the serializers.

    Returns a list with the available serializers in the most common order
    (i.e: the order that will work for almost the 90% of our objects).

    :param obj: Object to be analysed.
    :param logger: Logger to output the serialization messages.
    :return: <List> The serializers sorted by priority in descending order.
    """
    if __debug__:
        logger.debug(
            "Get serializer priority for object of type: %s" % str(type(obj))
        )
    if FORCED_SERIALIZER > -1:
        return [IDX2LIB[FORCED_SERIALIZER]]
    primitives = (int, str, bool, float)
    # primitives should be (de)serialized with for the compatibility with the
    # Runtime- only JSON objects can be deserialized in Java.
    if type(obj) in primitives:
        return [json, pickle]
    serializers = [pickle]
    if DILL_AVAILABLE:
        serializers = [pickle, dill]
    if object_belongs_to_module(obj, "numpy") and NUMPY_AVAILABLE:
        return [numpy] + serializers
    if object_belongs_to_module(obj, "cupy") and CUPY_AVAILABLE:
        return [cupy] + serializers
    if object_belongs_to_module(obj, "pyarrow") and PYARROW_AVAILABLE:
        return [pyarrow] + serializers
    if object_belongs_to_module(obj, "pyeddl") and PYEDDL_AVAILABLE:
        return [eddlNet] + serializers
    return serializers


def serialize_to_handler(
    obj: typing.Any, handler: typing.BinaryIO, logger: logging.Logger
) -> None:
    """Serialize an object to a handler.

    :param obj: Object to be serialized.
    :param handler: A handler object. It must implement methods like write,
                    writeline and similar stuff.
    :param logger: Logger to output the serialization messages.
    :return: None.
    :raises SerializerException: If something wrong happens during
                                 serialization.
    """
    emit_manual_event_explicit(
        TRACING_MASTER.binding_serialization_size_type, 0
    )
    if hasattr(handler, "name"):
        emit_manual_event_explicit(
            TRACING_MASTER.binding_serialization_object_num_type,
            (abs(hash(os.path.basename(handler.name))) % PLATFORM_C_MAXINT),
        )
    if __debug__:
        if isinstance(handler, BytesIO):
            logger.debug("Serializing to BytesIO handler: %s" % str(handler))
        else:
            logger.debug("Serializing to handler: %s" % str(handler.name))
    if DISABLE_GC:
        # Disable the garbage collector while serializing -> more performance?
        gc.disable()
    # Get the serializer priority
    serializer_priority = get_serializer_priority(obj, logger)
    if __debug__:
        logger.debug("Serializer priority: %s" % str(serializer_priority))
    i = 0
    success = False
    original_position = handler.tell()
    is_json = False
    # Lets try the serializers in the given priority
    serialization_issues = []
    while i < len(serializer_priority) and not success:
        # Reset the handlers pointer to the first position
        handler.seek(original_position)
        serializer = serializer_priority[i]
        handler.write(bytearray(f"{LIB2IDX[serializer]:04d}", "utf8"))

        if __debug__:
            logger.debug(
                "Trying to serialize using %s" % str(serializer.__name__)
            )

        # Special case: obj is a generator
        if isinstance(obj, types.GeneratorType):
            try:
                if __debug__:
                    logger.debug("Serializing a generator")
                pickle_generator(obj, handler, serializer)
                success = True
            except Exception:  # noqa
                if __debug__:
                    logger.debug("Failed serializing a generator")
                    traceback.print_exc()
        # General case
        else:
            try:
                if (
                    CUPY_AVAILABLE
                    and serializer is cupy
                    and isinstance(obj, cupy.ndarray)
                ):
                    if __debug__:
                        logger.debug("Serializing using cupy...")
                    serializer.save(handler, obj, allow_pickle=False)
                    if __debug__:
                        logger.debug("Serializing using cupy success")
                elif (
                    NUMPY_AVAILABLE
                    and serializer is numpy
                    and isinstance(obj, (numpy.ndarray, numpy.matrix))
                ):
                    if __debug__:
                        logger.debug("Serializing using numpy...")
                    serializer.save(handler, obj, allow_pickle=False)
                    if __debug__:
                        logger.debug("Serializing using numpy success")
                elif (
                    PYARROW_AVAILABLE
                    and serializer is pyarrow
                    and object_belongs_to_module(obj, "pyarrow")
                ):
                    if __debug__:
                        logger.debug("Serializing using pyarrow...")
                    writer = pyarrow.ipc.new_file(handler, obj.schema)  # noqa
                    writer.write(obj)
                    writer.close()
                    if __debug__:
                        logger.debug("Serializing using pyarrow success")
                elif (
                    PYEDDL_AVAILABLE
                    and serializer is eddlNet
                    and object_belongs_to_module(obj, "pyeddl")
                ):
                    if __debug__:
                        logger.debug("Serializing using pyeddl...")
                    handler.write(serialize_net_to_onnx_string(obj, False))
                    if __debug__:
                        logger.debug("Serializing using pyeddl success")
                elif serializer is json:
                    if __debug__:
                        logger.debug("Serializing using json...")
                    # JSON doesn't like the binary mode: close handler
                    h_name = handler.name
                    handler.close()
                    # Open the handler in normal mode
                    reopened_handler = open(
                        h_name, "w"
                    )  # pylint: disable=consider-using-with
                    reopened_handler.write(f"{LIB2IDX[serializer]:04d}")
                    serializer.dump(obj, reopened_handler)
                    is_json = True
                    if __debug__:
                        logger.debug("Serializing using json success")
                else:
                    if __debug__:
                        logger.debug(
                            "Serializing using %s..."
                            % str(serializer.__name__)
                        )
                    serializer.dump(
                        obj, handler, protocol=serializer.HIGHEST_PROTOCOL
                    )
                    if __debug__:
                        logger.debug(
                            "Serializing using %s success"
                            % str(serializer.__name__)
                        )
                success = True
                if __debug__:
                    logger.debug("Serialization accomplished")
            except Exception:  # noqa
                success = False
                traceback_exc = traceback.format_exc()
                serialization_issues.append((serializer, traceback_exc))
                if __debug__:
                    logger.debug(
                        "Could not perform serialization using %s"
                        % str(serializer.__name__)
                    )
        i += 1
    if is_json:
        serialization_size = reopened_handler.tell()
    else:
        serialization_size = handler.tell()
    emit_manual_event_explicit(
        TRACING_MASTER.binding_serialization_size_type, serialization_size
    )
    emit_manual_event_explicit(
        TRACING_MASTER.binding_serialization_object_num_type, 0
    )
    if DISABLE_GC:
        # Enable the garbage collector and force to clean the memory
        gc.enable()
        gc.collect()

    # if ret_value is None then all the serializers have failed
    if not success:
        if __debug__:
            logger.debug("Serialization TOTALLY FAILED")
        try:
            traceback.print_exc()
        except AttributeError:
            # Bug fixed in 3.5 - issue10805
            pass
        error_msg = f"Cannot serialize object {obj!r}. Reason:\n"
        for line in serialization_issues:
            error_msg += f"ERROR with:  {line[0]}\n{line[1]}\n"
        raise SerializerException(error_msg)


def serialize_to_file(
    obj: typing.Any, file_name: str, logger: logging.Logger
) -> None:
    """Serialize an object to a file.

    :param obj: Object to be serialized.
    :param file_name: File name where the object is going to be serialized.
    :param logger: Logger to output the serialization messages.
    :return: Nothing, it just serializes the object.
    """
    with EventInsideWorker(TRACING_WORKER.serialize_to_file_event):
        with open(file_name, "wb") as handler:
            serialize_to_handler(obj, handler, logger)


def serialize_to_file_mpienv(
    obj: typing.Any,
    file_name: str,
    rank_zero_reduce: bool,
    logger: logging.Logger,
) -> None:
    """Serialize an object to a file for Python MPI Tasks.

    :param obj: Object to be serialized.
    :param file_name: File name where the object is going to be serialized.
    :param rank_zero_reduce: A boolean to indicate whether objects should be
                             reduced to MPI rank zero.
                             False for INOUT objects and True otherwise.
    :param logger: Logger to output the serialization messages.
    :return: Nothing, it just serializes the object.
    """
    with EventInsideWorker(TRACING_WORKER.serialize_to_file_mpienv_event):
        if __debug__:
            logger.debug("Serializing to file mpienv")
        from mpi4py import MPI

        if rank_zero_reduce:
            nprocs = MPI.COMM_WORLD.Get_size()
            if nprocs > 1:
                obj = MPI.COMM_WORLD.reduce([obj], root=0)
            if MPI.COMM_WORLD.rank == 0:
                serialize_to_file(obj, file_name, logger)
        else:
            serialize_to_file(obj, file_name, logger)


def serialize_to_bytes(obj: typing.Any, logger: logging.Logger) -> bytes:
    """Serialize an object to a byte array.

    CAUTION: Used in redis storage integration.

    :param obj: Object to be serialized.
    :param logger: Logger to output the serialization messages.
    :return: The serialized content.
    """
    handler = BytesIO()
    serialize_to_handler(obj, handler, logger)
    ret = handler.getvalue()
    handler.close()
    return ret


def deserialize_from_handler(
    handler: typing.BinaryIO, show_exception: bool, logger: logging.Logger
) -> typing.Any:
    """Deserialize an object from a file.

    :param handler: File name from where the object is going to be
                    deserialized.
    :param show_exception: Show exception if happen (only with debug).
    :param logger: Logger to output the deserialization messages.
    :return: The object and if the handler has to be closed.
    :raises SerializerException: If deserialization can not be done.
    """
    # Retrieve the used library (if possible)
    emit_manual_event_explicit(
        TRACING_MASTER.binding_deserialization_size_type, 0
    )
    if hasattr(handler, "name"):
        emit_manual_event_explicit(
            TRACING_MASTER.binding_deserialization_object_num_type,
            (abs(hash(os.path.basename(handler.name))) % PLATFORM_C_MAXINT),
        )
    if __debug__:
        if isinstance(handler, BytesIO):
            logger.debug(
                "Deserializing from BytesIO handler: %s" % str(handler)
            )
        else:
            logger.debug("Deserializing from handler: %s" % str(handler.name))
    original_position = 0
    try:
        original_position = handler.tell()
        ser_type = handler.read(4)
        if ser_type == b"" or ser_type is None:
            return None, True
        serializer = IDX2LIB[int(ser_type)]
        if __debug__:
            logger.debug("Using deserializer: %s" % str(serializer.__name__))
    except KeyError as key_error:
        # The first 4 bytes return a value that is not within IDX2LIB
        handler.seek(original_position)
        error_message = "Handler does not refer to a valid PyCOMPSs object"
        raise SerializerException(error_message) from key_error

    close_handler = True
    try:
        if DISABLE_GC:
            # Disable the garbage collector while serializing -> performance?
            gc.disable()
        if CUPY_AVAILABLE and serializer is cupy:
            if __debug__:
                logger.debug("Cupy available")
                logger.debug("Deserializing using cupy")
            ret = serializer.load(handler, allow_pickle=False)
        elif NUMPY_AVAILABLE and serializer is numpy:
            if __debug__:
                logger.debug("Numpy available")
                logger.debug("Deserializing using numpy")
            ret = serializer.load(handler, allow_pickle=False)
        elif PYARROW_AVAILABLE and serializer is pyarrow:
            if __debug__:
                logger.debug("Pyarrow available")
                logger.debug("Deserializing using pyarrow")
            ret = pyarrow.ipc.open_file(handler)
            if isinstance(ret, pyarrow.ipc.RecordBatchFileReader):
                close_handler = False
        elif PYEDDL_AVAILABLE and serializer is eddlNet:
            if __debug__:
                logger.debug("Pyeddl available")
                logger.debug("Deserializing using pyeddl")
            h_name = handler.name
            # handler.seek(4)  # Ignore first byte?
            ret = import_net_from_onnx_file(h_name)
        elif serializer is json:
            if __debug__:
                logger.debug("Deserializing using json")
            # Deserialization of json files is not in binary: close handler
            h_name = handler.name
            handler.close()
            # Reopen handler with normal mode
            reopened_handler = open(h_name, "r")
            reopened_handler.seek(4)  # Ignore first byte
            ret = serializer.load(reopened_handler)
        else:
            if __debug__:
                logger.debug(
                    "Deserializing using %s" % str(serializer.__name__)
                )
            ret = serializer.load(handler)
        # Special case: deserialized obj wraps a generator
        if (
            isinstance(ret, tuple)
            and ret
            and isinstance(ret[0], GeneratorIndicator)
        ):
            if __debug__:
                logger.debug("Recovering a generator")
            ret = convert_to_generator(ret[1])
        if DISABLE_GC:
            # Enable the garbage collector and force to clean the memory
            gc.enable()
            gc.collect()
        if serializer is json:
            deserialization_size = reopened_handler.tell()
        else:
            deserialization_size = handler.tell()
        emit_manual_event_explicit(
            TRACING_MASTER.binding_deserialization_size_type,
            deserialization_size,
        )
        emit_manual_event_explicit(
            TRACING_MASTER.binding_deserialization_object_num_type, 0
        )
        if __debug__:
            logger.debug("Deserialization accomplished")
        return ret, close_handler
    except Exception as general_exception:
        traceback_exc = traceback.format_exc()
        if DISABLE_GC:
            gc.enable()
        if __debug__:
            logger.debug("Deserialization TOTALLY FAILED")
        if __debug__ and show_exception:
            print(f"ERROR! Deserialization with {str(serializer)} failed.")
            try:
                traceback.print_exc()
            except AttributeError:
                # Bug fixed in 3.5 - issue10805
                pass
        error_msg_head = (
            f"ERROR: Cannot deserialize object with serializer: {serializer}"
        )
        error_msg = f"{error_msg_head}\n{traceback_exc}\n"
        raise SerializerException(error_msg) from general_exception


def deserialize_from_file(
    file_name: str, logger: logging.Logger
) -> typing.Any:
    """Deserialize the contents in a given file.

    :param file_name: Name of the file with the contents to be deserialized.
    :param logger: Logger to output the deserialization messages.
    :return: A deserialized object.
    """
    with EventInsideWorker(TRACING_WORKER.deserialize_from_file_event):
        handler = open(file_name, "rb")
        ret, close_handler = deserialize_from_handler(handler, True, logger)
        if close_handler:
            handler.close()
        return ret


def deserialize_from_bytes(
    serialized_content_bytes: bytes,
    show_exception: bool,
    logger: logging.Logger,
) -> typing.Any:
    """Deserialize the contents in a given byte array.

    CAUTION: Used in redis storage integration.

    :param serialized_content_bytes: A byte array with serialized contents.
    :param show_exception: Show exception if happen (only with debug -
                           uses to be True).
    :param logger: Logger to output the deserialization messages.
    :return: A deserialized object.
    """
    with EventInsideWorker(TRACING_WORKER.deserialize_from_bytes_event):
        handler = BytesIO(serialized_content_bytes)
        ret, close_handler = deserialize_from_handler(
            handler, show_exception, logger
        )
        if close_handler:
            handler.close()
        return ret


def serialize_objects(to_serialize: list, logger: logging.Logger) -> None:
    """Serialize a list of objects to file.

    If a single object fails to be serialized, then an Exception by
    serialize_to_file will be thrown (and not caught).
    The structure of the parameter is:
         [(object1, file_name1), ... , (objectN, file_nameN)].

    :param to_serialize: List of lists to be serialized. Each sublist is a
                         pair of the form ['object','file name'].
    :param logger: Logger to output the deserialization messages.
    :return: None.
    """
    for obj, file in to_serialize:
        serialize_to_file(obj, file, logger)
