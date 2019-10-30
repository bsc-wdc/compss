#!/usr/bin/python
#
#  Copyright 2019      Cray UK Ltd., a Hewlett Packard Enterprise company
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
PyCOMPSs Util - Shared memory array serializer/deserializer
===========================================================
    This file implements the main serialization/deserialization functions based
    on the SharedArray module (from dependencies/shared-array).
    serialization/deserialization calls should be made using one of the
    following functions:

    - serialize_to_shm(obj, handler) -> Create a shared memory based numpy array
                                        array copy of "obj", indexed it as the
                                        basename of "handler.name" file name,
                                        and serialize the file into the handle
                                        using "numpy.save()" function.
    - deserialize_from_shm(handler) -> Returns a pointer to the already mapped
                                       object if it exists. Deserialize using
                                       "numpy.load()" otherwise.
    - delete_shma(name) -> Free the handle to the shared memory segment with
                           name "name". Remove the corresponding entry from the
                           dictionnary "shma_objects"
    - clear_shma(purge) -> Empty the dictionnary of shared memory segments. If
                           "purge" is True, release the memory segments from the
                           system.
"""

from os import getenv
from os.path import basename

try:
    # We rely on numpy for transfering the data.
    # Our working depends on numpy working.
    import numpy
    from SharedArray import SharedArray as shma
    SHAREDARRAY_AVAILABLE = True
except ImportError:
    SHAREDARRAY_AVAILABLE = False


shma_objects = dict()


def serialize_to_shm(obj, handler):
    """
    Serialize a named object using "numpy.save()" and store a copy in shared
    memory (system-V) and in the cache ("shma_objects" dictionnary), indexed on
    the basename of "handler.name".

    :param obj: Name of the object.
    :param handler: File handler.
    """
    obj_name = basename(handler.name + '.shm')

    shma_objects[obj_name] = shma.create_copy('shm://' + obj_name, obj)
    numpy.save(handler, obj, allow_pickle=False)


def deserialize_from_shm(handler):
    """
    Create a new numpy array which data is synchronized with the data
    shared in the file pointed by "handler". The shared array are indexed on
    the basename of "handler.name". If the key exists in the process-local
    dictionnary "shma_objects", then the object has already been deserialized
    once and a pointer to it is returned. If the key doesn't exist, then a
    shared memory segment is mapped into the process memory space and a new
    entry is added to the "shma_objects" dictionnary. If the memory mapping
    fails ("shma.attach()"), the numpy array has never been mapped system-wide
    (i.e., none of the workers have ever deserialized the array). In that case,
    the array is deserialized to memory using "numpy.load()", then a copy is
    created using "shma.create_copy()" and a new entry is added to the
    dictionnary, indexed on the basename of "handler.name".

    :param handler: File handler to the numpy-serialized array.
    :return: A new numpy array.
    """
    obj_name = basename(handler.name + '.shm')

    def try_dict():
        return shma_objects[obj_name] if obj_name in shma_objects else None
    shared_array = try_dict()

    if shared_array is None:
        try:
            # Try to attach from OS shared memory system
            def try_attach():
                return shma.attach('shm://' + obj_name)
            shared_array = try_attach()

        except (OSError, IOError):
            # Never been deserialized before
            def try_load():
                deserialized_array = numpy.load(handler, allow_pickle=False)
                return shma.create_copy('shm://' + obj_name, deserialized_array)
            shared_array = try_load()

        shma_objects[obj_name] = shared_array

    return shared_array


def deregister_arrays(names):
    """
    Delete the arrays which names are given as parameters.  Once deleted, the
    local copies of the array are still working, but it is not possible to
    deserialize (attach) the arrays anymore.

    :param names: None, or a list of key to be found in shma_objects.
    """
    for name in names:
        try:
            shma.delete('shm://' + name)
        except OSError:
            pass
        except IOError:
            pass


def delete_shma(name):
    """
    Delete the array corresponding to the name given as a parameter. Free the
    entry from the dictionnary. Other copies in use are still existing until
    the garbage collector release the memory.

    :param name: the filename of the array to free.
    """
    obj_name = basename(name + '.shm')

    try:
        shma_objects.pop(obj_name)
    except KeyError:
        pass

    deregister_arrays([obj_name])


def clear_shma(purge=False):
    """
    Empty the dictionnary entries, and, if purge equals True, also release the
    shared memory from the system.

    :param purge: Boolean, whether or not to free the shared memory. Default is
                  False.
    """
    if purge:
        deregister_arrays(shma_objects.keys())
    shma_objects.clear()
