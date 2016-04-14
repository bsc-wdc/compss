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
from cPickle import HIGHEST_PROTOCOL

# Enable or disable the use of mmap for the file read and write operations
# cross-module variable (set/modified from launch.py)
mmap_file_storage = False

'''
# Provide support for serializing functions
import cPickle as pickle
import marshal, copy_reg, types

def make_cell(value):
    return(lambda: value).__closure__[0]

def make_function(*args):
    return types.FunctionType(*args)
    
copy_reg.pickle(types.CodeType, lambda code: (marshal.loads, (marshal.dumps(code),)))
copy_reg.pickle(type((lambda i=0: lambda: i)().__closure__[0]), lambda cell: (make_cell, (cell.cell_contents,)))
copy_reg.pickle(types.FunctionType, lambda fn: (make_function, (fn.__code__, {}, fn.__name__, fn.__defaults__, fn.__closure__)))
'''

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
        f = open(file_name, 'rb')
        l = load(f)
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
            dump(obj, f, HIGHEST_PROTOCOL)
            f.close()  # new
