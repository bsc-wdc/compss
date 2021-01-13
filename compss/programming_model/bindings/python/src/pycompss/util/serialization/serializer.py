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
PyCOMPSs Util - Data serializer/deserializer
============================================
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
import types
import traceback

from pycompss.runtime.commons import IS_PYTHON3
from pycompss.util.serialization.extended_support import pickle_generator
from pycompss.util.serialization.extended_support import convert_to_generator
from pycompss.util.serialization.extended_support import GeneratorIndicator
from pycompss.util.objects.properties import object_belongs_to_module

from io import BytesIO

DISABLE_GC = False

if IS_PYTHON3:
    import pickle as pickle  # Uses _pickle if available
else:
    import cPickle as pickle  # noqa

try:
    import dill  # noqa
except ImportError:
    if IS_PYTHON3:
        import pickle as dill
    else:
        import cPickle as dill  # noqa

try:
    import numpy
    NUMPY_AVAILABLE = True
except ImportError:
    if IS_PYTHON3:
        import pickle as numpy
    else:
        import cPickle as numpy  # noqa
    NUMPY_AVAILABLE = False

lib2idx = {
    pickle: 0,
    numpy: 1,
    dill: 2
}

idx2lib = dict([(v, k) for (k, v) in lib2idx.items()])


class SerializerException(Exception):
    """
    Exception on serialization
    """
    pass


def get_serializer_priority(obj=()):
    # type: (object) -> list
    """ Computes the priority of the serializers.

    :param obj: Object to be analysed.
    :return: <List> The serializers sorted by priority in descending order.
    """
    if object_belongs_to_module(obj, 'numpy'):
        return [numpy, pickle, dill]
    return [pickle, dill]


def get_serializers():
    # type: () -> list
    """ Returns a list with the available serializers in the most common order
    (i.e: the order that will work for almost the 90% of our objects).

    :return: <List> the available serializer modules.
    """
    return get_serializer_priority()


def serialize_to_handler(obj, handler):
    # type: (object, ...) -> None
    """ Serialize an object to a handler.

    :param obj: Object to be serialized.
    :param handler: A handler object. It must implement methods like write,
                    writeline and similar stuff.
    :return: none
    :raises SerializerException: If something wrong happens during
                                 serialization.
    """
    if DISABLE_GC:
        # Disable the garbage collector while serializing -> more performance?
        gc.disable()
    # Get the serializer priority
    serializer_priority = get_serializer_priority(obj)
    i = 0
    success = False
    original_position = handler.tell()
    # Lets try the serializers in the given priority
    while i < len(serializer_priority) and not success:
        # Reset the handlers pointer to the first position
        handler.seek(original_position)
        serializer = serializer_priority[i]
        handler.write(bytearray('%04d' % lib2idx[serializer], 'utf8'))
        # Special case: obj is a generator
        if isinstance(obj, types.GeneratorType):
            try:
                pickle_generator(obj, handler, serializer)
                success = True
            except Exception:  # noqa
                if __debug__:
                    traceback.print_exc()
        # General case
        else:
            try:
                # If it is a numpy object then use its saving mechanism
                if serializer is numpy and \
                        NUMPY_AVAILABLE and \
                        (isinstance(obj, numpy.ndarray) or
                         isinstance(obj, numpy.matrix)):
                    serializer.save(handler, obj, allow_pickle=False)
                else:
                    serializer.dump(obj,
                                    handler,
                                    protocol=serializer.HIGHEST_PROTOCOL)
                success = True
            except Exception:  # noqa
                success = False
        i += 1

    if DISABLE_GC:
        # Enable the garbage collector and force to clean the memory
        gc.enable()
        gc.collect()

    # if ret_value is None then all the serializers have failed
    if not success:
        try:
            traceback.print_exc()
        except AttributeError:
            # Bug fixed in 3.5 - issue10805
            pass
        raise SerializerException('Cannot serialize object %s' % obj)


def serialize_to_file(obj, file_name):
    # type: (object, str) -> None
    """ Serialize an object to a file.

    :param obj: Object to be serialized.
    :param file_name: File name where the object is going to be serialized.
    :return: Nothing, it just serializes the object.
    """
    handler = open(file_name, 'wb')
    serialize_to_handler(obj, handler)
    handler.close()


def serialize_to_file_mpienv(obj, file_name, rank_zero_reduce):
    # type: (object, str, bool) -> None
    """ Serialize an object to a file for Python MPI Tasks.

    :param obj: Object to be serialized.
    :param file_name: File name where the object is going to be serialized.
    :param rank_zero_reduce: A boolean to indicate whether objects should be
                             reduced to MPI rank zero.
                             False for INOUT objects and True otherwise.
    :return: Nothing, it just serializes the object.
    """
    from mpi4py import MPI

    if rank_zero_reduce:
        nprocs = MPI.COMM_WORLD.Get_size()
        if nprocs > 1:
            obj = MPI.COMM_WORLD.reduce([obj], root=0)
        if MPI.COMM_WORLD.rank == 0:
            serialize_to_file(obj, file_name)
    else:
        serialize_to_file(obj, file_name)


def serialize_to_string(obj):
    # type: (object) -> bytes
    """ Serialize an object to a string.

    :param obj: Object to be serialized.
    :return: The serialized content
    """
    handler = BytesIO()
    serialize_to_handler(obj, handler)
    ret = handler.getvalue()
    handler.close()
    return ret


def deserialize_from_handler(handler):
    # type: (...) -> object
    """ Deserialize an object from a file.

    :param handler: File name from where the object is going to be
                    deserialized.
    :return: The object.
    :raises SerializerException: If deserialization can not be done.
    """
    # Retrieve the used library (if possible)
    original_position = None
    try:
        original_position = handler.tell()
        serializer = idx2lib[int(handler.read(4))]
    except KeyError:
        # The first 4 bytes return a value that is not within idx2lib
        handler.seek(original_position)
        error_message = 'Handler does not refer to a valid PyCOMPSs object'
        raise SerializerException(error_message)

    try:
        if DISABLE_GC:
            # Disable the garbage collector while serializing -> performance?
            gc.disable()
        if serializer is numpy and NUMPY_AVAILABLE:
            ret = serializer.load(handler, allow_pickle=False)
        else:
            ret = serializer.load(handler)
        # Special case: deserialized obj wraps a generator
        if isinstance(ret, tuple) and \
                ret and \
                isinstance(ret[0], GeneratorIndicator):
            ret = convert_to_generator(ret[1])
        if DISABLE_GC:
            # Enable the garbage collector and force to clean the memory
            gc.enable()
            gc.collect()
        return ret
    except Exception:
        if DISABLE_GC:
            gc.enable()
        if __debug__:
            print('ERROR! Deserialization with %s failed.' % str(serializer))
            try:
                traceback.print_exc()
            except AttributeError:
                # Bug fixed in 3.5 - issue10805
                pass
        raise SerializerException('Cannot deserialize object')


def deserialize_from_file(file_name):
    # type: (str) -> object
    """ Deserialize the contents in a given file.

    :param file_name: Name of the file with the contents to be deserialized
    :return: A deserialized object
    """
    handler = open(file_name, 'rb')
    ret = deserialize_from_handler(handler)
    handler.close()
    return ret


def deserialize_from_string(serialized_content):
    # type: (str) -> object
    """ Deserialize the contents in a given string.

    :param serialized_content: A string with serialized contents
    :return: A deserialized object
    """
    handler = BytesIO(serialized_content)  # noqa
    ret = deserialize_from_handler(handler)
    handler.close()
    return ret


def serialize_objects(to_serialize):
    # type: (list) -> None
    """ Serialize a list of objects to file.

    If a single object fails to be serialized, then an Exception by
    serialize_to_file will be thrown (and not caught).
    The structure of the parameter is:
         [(object1, file_name1), ... , (objectN, file_nameN)].

    :param to_serialize: List of lists to be serialized. Each sublist is a
                         pair of the form ['object','file name']
    :return: None
    """
    for obj_and_file in to_serialize:
        serialize_to_file(*obj_and_file)
