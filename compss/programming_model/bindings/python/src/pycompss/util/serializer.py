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
import logging
from cPickle import load, dump
from cPickle import loads, dumps
from cPickle import HIGHEST_PROTOCOL, UnpicklingError, PicklingError
import types
import marshal
import thread, threading
import copy_reg
import types
from serialization.extendedSupport import pickle_generator
from serialization.extendedSupport import copy_generator
from serialization.extendedSupport import GeneratorSnapshot
from serialization.extendedSupport import pickle_module_object
from serialization.extendedSupport import unpickle_module_object 
from serialization.extendedSupport import serialize_event
from serialization.extendedSupport import serialize_lock
from serialization.extendedSupport import serialize_ellipsis
#from serialization.extendedSupport import serialize_quit 
#from serialization.extendedSupport import restorefunction, reducefunction

copy_reg.pickle(threading._Event, serialize_event)
copy_reg.pickle(thread.LockType, serialize_lock)
copy_reg.pickle(type(Ellipsis), serialize_ellipsis)
#copy_reg.pickle(type(quit), serialize_quit)
#copy_reg.pickle(type(exit), serialize_quit) 
#copy_reg.pickle(type(NotImplemented), serialize_quit)
#copy_reg.constructor(restorefunction)
#copy_reg.pickle(types.FunctionType, reducefunction)

logger = logging.getLogger(__name__)


# Enable or disable the use of mmap for the file read and write operations
# cross-module variable (set/modified from launch.py)
mmap_file_storage = False


class GeneratorException(Exception):
    pass


#class GeneratorTaskException(Exception):
#    pass
#
#class genTaskSerializer(object):
#    def __init__(self, gen, n, maxiter):
#        self.gen = gen
#        self.n = n
#        self.maxiter = maxiter


def serialize_to_file(obj, file_name, force=False):
    """
    Serialize an object to file.
    @param obj: Object to be serialized.
    @param file_name: File name where the object is going to be serialized.
    @param force: Force serialization. Values = [True, False]. Default = False.
    @return: String -> the file name (same as the parameter)
    """
    logger.debug("Serialize to file: " + str(file_name))
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
            if isinstance(obj, types.LambdaType) and obj.func_name == '<lambda>':
                # The object is a lambda function
                # The two conditions must be done since types.LambdaType equals types.FunctionType
                marshal.dump(obj.func_code, f)
            elif isinstance(obj, types.GeneratorType):
                # The object is a generator - Save the state
                pickle_generator(obj, f)
            #elif isinstance(obj, GeneratorWrapper):
            #    sg = genTaskSerializer(getPickled_generator(obj.gen), obj.n, obj.maxiter)
            #    dump(sg, f)
            else:
                # All other objects are serialized using cPickle
                try:
                    dump(obj, f, HIGHEST_PROTOCOL)
                except PicklingError, e:
                    # Could not serialize the object. Probably it is a module object.
                    if "Can't pickle <type 'module'>" in str(e):
                        pickle_module_object(obj, f, HIGHEST_PROTOCOL)
                    else:
                        raise PicklingError(str(e))
            f.close()
        return file_name


def deserialize_from_file(file_name):
    """
    Deserialize an object from a file.
    @param file_name: File name from where the object is going to
                      be deserialized.
    @return: The object deserialized.
    """
    logger.debug("Deserialize from file: " + str(file_name))
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
                logger.debug("Found a generator when deserializing.")
                raise GeneratorException
            #if isinstance(l, genTaskSerializer):
            #    logger.debug("Found a taskified generator when deserializing.")
            #    raise GeneratorTaskException
        except (UnpicklingError):  # It is a lambda function
            f.seek(0, 0)
            func = marshal.load(f)
            l = types.FunctionType(func, globals())
        except GeneratorException:
            # It is a generator and needs to be unwrapped (from GeneratorSnapshot to generator).
            l = copy_generator(l)[0]
        #except GeneratorTaskException:
        #    # Rebuild the object
        #    l = GeneratorWrapper(copy_generator(l.gen)[0], l.n, l.maxiter)
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
    logger.debug("Serialize objects:")
    for target in to_serialize:
        logger.debug("\t - " + str(target))
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
                marshal.dump(obj.func_code, f)
            elif isinstance(obj, types.GeneratorType):
                # The object is a generator - Save the state
                pickle_generator(obj, f)
            #elif isinstance(obj, GeneratorWrapper):
            #    sg = genTaskSerializer(getPickled_generator(obj.gen), obj.n, obj.maxiter)
            #    dump(sg, f)
            else:
                # All other objects are serialized using cPickle
                try:
                    dump(obj, f, HIGHEST_PROTOCOL)
                except PicklingError, e:
                    # Could not serialize the object. Probably it is a module object.
                    if "Can't pickle <type 'module'>" in str(e):
                        pickle_module_object(obj, f, HIGHEST_PROTOCOL)
                    else:
                        raise PicklingError(str(e))
            f.close()