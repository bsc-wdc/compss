"""
@author: etejedor
@author: fconejer

PyCOMPSs Utils - Serializer
===========================
    This file contains all serialization methods.
"""

import sys
import os
import mmap
import math
from cPickle import load, dump
from cPickle import loads, dumps
from cPickle import HIGHEST_PROTOCOL, UnpicklingError
import types
import marshal
from serialization.extendedSupport import pickle_generator
from serialization.extendedSupport import copy_generator
from serialization.extendedSupport import GeneratorSnapshot

# Enable or disable the use of mmap for the file read and write operations
# cross-module variable (set/modified from launch.py)
mmap_file_storage = False


class GeneratorException(Exception):
    pass


def serialize_to_file(obj, file_name, force=False):
    """
    Serialize an object to file.
    @param obj: Object to be serialized.
    @param file_name: File name where the object is going to be serialized.
    @param force: Force serialization. Values = [True, False]. Default = False.
    @return: String -> the file name (same as the parameter)
    """
    if mmap_file_storage:
        if not os.path.exists(file_name) or force:
            d = dumps(obj, HIGHEST_PROTOCOL)
            size = sys.getsizeof(d)
            if size > mmap.PAGESIZE:
                size = int(mmap.PAGESIZE * (math.ceil(size / float(mmap.PAGESIZE))))
            else:
                size = int(mmap.PAGESIZE)
            fd = os.open(file_name, os.O_CREAT | os.O_TRUNC | os.O_RDWR)
            os.write(fd, '\x00' * size)
            mm = mmap.mmap(fd, size, mmap.MAP_SHARED, mmap.PROT_WRITE)
            mm.write(d)
            mm.close()
        return file_name
    else:
        if not os.path.exists(file_name) or force:
            f = open(file_name, 'wb')
            if isinstance(obj, types.FunctionType):
                # The object is a function or a lambda
                marshal.dump(obj.func_code, f)
            elif isinstance(obj, types.GeneratorType):
                # The object is a generator - Save the state
                pickle_generator(obj, f)
            else:
                # All other objects are serialized using cPickle
                dump(obj, f, HIGHEST_PROTOCOL)
            f.close()
        return file_name


def deserialize_from_file(file_name):
    """
    Deserialize an object from a file.
    @param file_name: File name from where the object is going to
                      be deserialized.
    @return: The object deserialized.
    """
    if mmap_file_storage:
        fd = os.open(file_name, os.O_RDONLY)
        mm = mmap.mmap(fd, 0, mmap.MAP_SHARED, mmap.PROT_READ)
        content = mm.read(-1)
        l = loads(content)
        mm.close()
        return l
    else:
        l = None
        f = open(file_name, 'rb')
        try:
            l = load(f)
            if isinstance(l, GeneratorSnapshot):
                raise GeneratorException
        except (UnpicklingError, AttributeError):  # It is a function or a lambda
            f.seek(0, 0)
            func = marshal.load(f)
            l = types.FunctionType(func, globals())
        except GeneratorException:
            # It is a generator and needs to be unwrapped (from GeneratorSnapshot to generator).
            l = copy_generator(l)[0]
        f.close()
        return l


def serialize_objects(to_serialize):
    """
    Serialize a list of objects to file.
    The structure of the parameter is
    [[object1][file_name1], [object2][file_name2], ... , [objectN][file_nameN]].
    @param to_serialize: List of lists to be serialized.
                         Each sublist is composed of pairs
                         ['object','file name'].
    """
    if mmap_file_storage:
        for target in to_serialize:
            obj = target[0]
            file_name = target[1]
            d = dumps(obj, HIGHEST_PROTOCOL)
            size = sys.getsizeof(d)
            if size > mmap.PAGESIZE:
                size = int(mmap.PAGESIZE * (math.ceil(size / float(mmap.PAGESIZE))))
            else:
                size = int(mmap.PAGESIZE)
            fd = os.open(file_name, os.O_CREAT | os.O_TRUNC | os.O_RDWR)
            os.write(fd, '\x00' * size)
            mm = mmap.mmap(fd, size, mmap.MAP_SHARED, mmap.PROT_WRITE)
            mm.write(d)
            mm.close()
    else:
        for target in to_serialize:
            obj = target[0]
            file_name = target[1]
            f = open(file_name, 'wb')
            if isinstance(obj, types.FunctionType):
                # The object is a function or a lambda
                marshal.dump(obj.func_code, f, HIGHEST_PROTOCOL)
            elif isinstance(obj, types.GeneratorType):
                # The object is a generator - Save the state
                pickle_generator(obj, f)
            else:
                # All other objects are serialized using cPickle
                dump(obj, f, HIGHEST_PROTOCOL)
            f.close()