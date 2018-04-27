#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Utils: Data serializer/deserializer
This file implements the main serialization/deserialization functions.
All serialization/deserialization calls should be made using one of the following functions:

- serialize_to_file(obj, file_name) -> dumps the object "obj" to the file "file_name"
- serialize_to_string(obj) -> dumps the object "obj" to a string
- serialize_to_handler(obj, handler) -> writes the serialized object using the specified handler
                                        it also moves the handler's pointer to the end of the dump

- deserialize_from_file(file_name) -> loads the first object from the tile "file_name"
- deserialize_from_string(serialized_content) -> loads the first object from the given string
- deserialize_from_handler(handler) -> deserializes an object using the given handler, it also leaves the
                                       handler's pointer pointing to the end of the serialized object
"""
import sys
import types
import traceback
from .serialization.extended_support import pickle_generator, convert_to_generator, GeneratorIndicator
from .object_properties import object_belongs_to_module

if sys.version_info >= (3, 0):
    from io import BytesIO as StringIO
    import pickle as pickle  # Uses _pickle if available
else:
    from cStringIO import StringIO
    import cPickle as pickle

try:
    import dill
except:
    if sys.version_info >= (3, 0):
        import pickle as dill
    else:
        import cPickle as dill

try:
    import numpy
except:
    if sys.version_info >= (3, 0):
        import pickle as numpy
    else:
        import cPickle as numpy


class SerializerException(Exception):
    pass


def get_serializer_priority(obj=[]):
    """
    Computes the priority of the serializers.
    @param obj: Object to be analysed.
    @return: List -> The serializers sorted by priority in descending order
    """
    if object_belongs_to_module(obj, 'numpy'):
        return [numpy, pickle, dill]
    return [pickle, dill]


def get_serializers():
    """
    Returns a list with the available serializers in the most common order
    (i.e: the order that will work for almost the 90% of our objects)
    @return: List -> the available serializer modules
    """
    return get_serializer_priority()


def serialize_to_handler(obj, handler):
    """
    Serialize an object to a handler.
    @param obj: Object to be serialized.
    @param file_handler: A handler object. It must implement methods like
                         write, writeline and similar stuff
    """
    # get the serializer priority
    serializer_priority = get_serializer_priority(obj)
    i = 0
    success = False
    original_position = handler.tell()
    # lets try the serializers in the given priority
    while i < len(serializer_priority) and not success:
        # reset the handlers pointer to the first position
        handler.seek(original_position)
        serializer = serializer_priority[i]
        # special case: obj is a generator
        if isinstance(obj, types.GeneratorType):
            try:
                pickle_generator(obj, handler, serializer)
                success = True
            except:
                traceback.print_exc()
        # general case
        else:
            try:
                # If it is a numpy object then use its saving mechanism
                if serializer.__name__ == 'numpy' and type(obj).__module__ == numpy.__name__:
                    serializer.save(handler, obj)
                    success = True
                # elif obj it is a function (callable object) defined within the main application file
                elif callable(obj) and obj.__module__ == '__main__' and serializer.__name__ != 'dill':
                    # If this happens, it thinks that it is within launch.py __main__ with pickle.
                    # Solution, use dill in this particular case. cPickle in the rest.
                    success = False
                # otherwise, general case
                else:
                    serializer.dump(obj, handler, protocol=serializer.HIGHEST_PROTOCOL)
                    success = True
            except:
                print('WARNING! Serialization with %s failed.' % str(serializer))
                traceback.print_exc()  # No need to print all serializer exceptions
        i += 1

    # if ret_value is None then all the serializers have failed
    if not success:
        try:
            traceback.print_exc()
        except AttributeError:
            # Bug fixed in 3.5 - issue10805
            pass
        raise SerializerException('Cannot serialize object %s' % obj)


def serialize_to_file(obj, file_name):
    """
    Serialize an object to a file.
    @param obj: Object to be serialized.
    @param file_name: File name where the object is going to be serialized.
    """
    handler = open(file_name, 'wb')
    serialize_to_handler(obj, handler)
    handler.close()
    return file_name


def serialize_to_string(obj):
    """
    Serialize an object to a string.
    @param obj: Object to be serialized.
    @return: String -> the serialized content
    """
    handler = StringIO()
    serialize_to_handler(obj, handler)
    ret = handler.getvalue()
    handler.close()
    return ret


def deserialize_from_handler(handler):
    """
    Deserialize an object from a file.
    @param file_name: File name from where the object is going to
                      be deserialized.
    @return: The object deserialized.
    """
    # get the most common order of the serializers
    serializers = get_serializer_priority(numpy.zeros(1))
    original_position = handler.tell()
    # let's try to deserialize
    for serializer in serializers:
        # reset the handler in case the previous serializer has used it
        handler.seek(original_position)
        try:
            ret = serializer.load(handler)
            # special case: deserialized obj wraps a generator
            if isinstance(ret, tuple) and ret and isinstance(ret[0], GeneratorIndicator):
                ret = convert_to_generator(ret[1])
            return ret
        except:
            print('WARNING! Deserialization with %s failed.' % str(serializer))
            traceback.print_exc()  # No need to print all deserialize exceptions
    # we are not able to deserialize the contents from file_name with any of our
    # serializers
    try:
        traceback.print_exc()
    except AttributeError:
        # Bug fixed in 3.5 - issue10805
        pass
    raise SerializerException('Cannot deserialize object')


def deserialize_from_file(file_name):
    """
    Deserializes the contents in a given file
    @param file_name: Name of the file with the contents to be deserialized
    @return: A deserialized object
    """
    handler = open(file_name, 'rb')
    ret = deserialize_from_handler(handler)
    handler.close()
    return ret


def deserialize_from_string(serialized_content):
    """
    Deserializes the contents in a given string
    @param serialized_content: A string with serialized contents
    @return: A deserialized object
    """
    if isinstance(serialized_content, str):
        serialized_content = str.encode(serialized_content)
    handler = StringIO(serialized_content)
    ret = deserialize_from_handler(handler)
    handler.close()
    return ret


def serialize_objects(to_serialize):
    """
    Serialize a list of objects to file.
    If a single object fails to be serialized, then an Exception by
    serialize_to_file will be thrown (and not caught)
    The structure of the parameter is
    [(object1, file_name1), ... , (objectN, file_nameN)].
    @param to_serialize: List of lists to be serialized.
                         Each sublist is a pair of the form
                         ['object','file name'].
    """
    for obj_and_file in to_serialize:
        serialize_to_file(*obj_and_file)
