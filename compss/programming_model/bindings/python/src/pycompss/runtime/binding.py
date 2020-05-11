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
PyCOMPSs Binding - Binding
==========================
    This file contains the Python binding auxiliary classes and methods.
"""

import pycompss.api.parameter as parameter
from pycompss.api.parameter import Parameter
from pycompss.api.parameter import TYPE
from pycompss.api.parameter import DIRECTION
from pycompss.api.parameter import JAVA_MIN_INT, JAVA_MAX_INT
from pycompss.api.parameter import JAVA_MIN_LONG, JAVA_MAX_LONG
from pycompss.api.parameter import get_compss_type
from pycompss.runtime.commons import EMPTY_STRING_KEY
from pycompss.runtime.commons import STR_ESCAPE
from pycompss.util.serialization.serializer import *
from pycompss.util.objects.sizer import total_sizeof
from pycompss.util.storages.persistent import is_psco
from pycompss.util.storages.persistent import get_id
from pycompss.util.storages.persistent import get_by_id
from pycompss.util.objects.properties import is_basic_iterable

import types
import os
import sys
import re
import uuid
import inspect
import logging
import traceback
import base64

from collections import *
from shutil import rmtree

# Import main C module extension for the communication with the runtime
# See ext/compssmodule.cc
import compss

# Types conversion dictionary from python to COMPSs
if IS_PYTHON3:
    listType = list
    dictType = dict
    _python_to_compss = {int: TYPE.INT,  # int # long
                         float: TYPE.DOUBLE,  # float
                         bool: TYPE.BOOLEAN,  # bool
                         str: TYPE.STRING,  # str
                         # The type of instances of user-defined classes
                         # types.InstanceType: TYPE.OBJECT,
                         # The type of methods of user-defined class instances
                         # types.MethodType: TYPE.OBJECT,
                         # The type of user-defined old-style classes
                         # types.ClassType: TYPE.OBJECT,
                         # The type of modules
                         # types.ModuleType: TYPE.OBJECT,
                         # The type of tuples (e.g. (1, 2, 3, 'Spam'))
                         tuple: TYPE.OBJECT,
                         # The type of lists (e.g. [0, 1, 2, 3])
                         list: TYPE.OBJECT,
                         # The type of dictionaries (e.g. {'Bacon':1,'Ham':0})
                         dict: TYPE.OBJECT,
                         # The type of generic objects
                         object: TYPE.OBJECT
                         }
else:
    listType = types.ListType
    dictType = types.DictType
    _python_to_compss = {types.IntType: TYPE.INT,          # noqa # int
                         types.LongType: TYPE.LONG,        # noqa # long
                         types.FloatType: TYPE.DOUBLE,     # noqa # float
                         types.BooleanType: TYPE.BOOLEAN,  # noqa # bool
                         types.StringType: TYPE.STRING,    # noqa # str
                         # The type of instances of user-defined classes
                         # types.InstanceType: TYPE.OBJECT,
                         # The type of methods of user-defined class instances
                         # types.MethodType: TYPE.OBJECT,
                         # The type of user-defined old-style classes
                         # types.ClassType: TYPE.OBJECT,
                         # The type of modules
                         # types.ModuleType: TYPE.OBJECT,
                         # The type of tuples (e.g. (1, 2, 3, 'Spam'))
                         types.TupleType: TYPE.OBJECT,     # noqa
                         # The type of lists (e.g. [0, 1, 2, 3])
                         types.ListType: TYPE.OBJECT,
                         # The type of dictionaries (e.g. {'Bacon':1,'Ham':0})
                         types.DictType: TYPE.OBJECT,
                         # The type of generic objects
                         types.ObjectType: TYPE.OBJECT     # noqa
                         }

# Set temporary dir
temp_dir = '.'
_temp_obj_prefix = '/compss-serialized-obj_'

# Dictionary to contain the conversion from object id to the
# filename where it is stored (mapping).
# The filename will be used for requesting an object to
# the runtime (its corresponding version).
objid_to_filename = {}

# Dictionary that contains the objects used within tasks.
pending_to_synchronize = {}

# Objects that have been accessed by the main program
_objs_written_by_mp = {}  # obj_id -> compss_file_name

# Enable or disable small objects conversion to strings
# cross-module variable (set/modified from launch.py)
object_conversion = False

# Identifier handling
_current_id = 1
# Object identifiers will be of the form _runtime_id-_current_id
# This way we avoid to have two objects from different applications having
# the same identifier
_runtime_id = str(uuid.uuid1())
# All objects are segregated according to their position in memory
# Given these positions, all objects are then reverse-mapped according
# to their filenames
_addr2id2obj = {}

content_type_format = "{}:{}"  # <module_path>:<class_name>


def calculate_identifier(obj):
    """
    Calculates the identifier for the given object.

    :param obj: Object to get the identifier
    :return: String (md5 string)
    """
    return id(obj)
    # # If we want to detect automatically IN objects modification we need
    # # to ensure uniqueness of the identifier. At this point, obj is a
    # # reference to the object that we want to compute its identifier.
    # # This means that we do not have the previous object to compare directly.
    # # So the only way would be to ensure the uniqueness by calculating
    # # an id which depends on the object.
    # # BUT THIS IS REALLY EXPENSIVE. So: Use the id and unregister the object
    # #                                   (IN) to be modified explicitly.
    # immutable_types = [bool, int, float, complex, str,
    #                    tuple, frozenset, bytes]
    # obj_type = type(obj)
    # if obj_type in immutable_types:
    #     obj_addr = id(obj)  # Only guarantees uniqueness with
    #                         # immutable objects
    # else:
    #     # For all the rest, use hash of:
    #     #  - The object id
    #     #  - The size of the object (object increase/decrease)
    #     #  - The object representation (object size is the same but has been
    #     #                               modified (e.g. list element))
    #     # WARNING: Caveat:
    #     #  - IN User defined object with parameter change without __repr__
    #     # INOUT parameters to be modified require a synchronization, so they
    #     # are not affected.
    #     import hashlib
    #     hash_id = hashlib.md5()
    #     hash_id.update(str(id(obj)).encode())            # Consider the memory pointer        # noqa: E501
    #     hash_id.update(str(total_sizeof(obj)).encode())  # Include the object size            # noqa: E501
    #     hash_id.update(repr(obj).encode())               # Include the object representation  # noqa: E501
    #     obj_addr = str(hash_id.hexdigest())
    # return obj_addr


def get_object_id(obj, assign_new_key=False, force_insertion=False):
    """
    Gets the identifier of an object. If not found or we are forced to,
    we create a new identifier for this object, deleting the old one if
    necessary. We can also query for some object without adding it in case of
    failure.

    :param obj: Object to analyse.
    :param assign_new_key: <Boolean> Assign new key.
    :param force_insertion: <Boolean> Force insertion.
    :return: Object id
    """
    global _current_id
    global _runtime_id
    # Force_insertion implies assign_new_key
    assert not force_insertion or assign_new_key

    obj_addr = calculate_identifier(obj)

    # Assign an empty dictionary (in case there is nothing there)
    _id2obj = _addr2id2obj.setdefault(obj_addr, {})

    for identifier in _id2obj:
        if _id2obj[identifier] is obj:
            if force_insertion:
                _id2obj.pop(identifier)
                break
            else:
                return identifier
    if assign_new_key:
        # This object was not in our object database or we were forced to
        # remove it, lets assign it an identifier and store it.
        # As mentioned before, identifiers are of the form
        # _runtime_id-_current_id in order to avoid having two objects from
        # different applications with the same identifier (and thus file name)
        new_id = '%s-%d' % (_runtime_id, _current_id)
        _id2obj[new_id] = obj
        _current_id += 1
        return new_id
    if len(_id2obj) == 0:
        _addr2id2obj.pop(obj_addr)
    return None


def pop_object_id(obj):
    """
    Pop an object from the nested identifier hashmap
    :param obj: Object to pop
    :return: Popped object, None if obj was not in _addr2id2obj
    """
    obj_addr = calculate_identifier(obj)
    _id2obj = _addr2id2obj.setdefault(obj_addr, {})
    for (k, v) in list(_id2obj.items()):
        _id2obj.pop(k)
    _addr2id2obj.pop(obj_addr)


# Enable or disable the management of *args parameters as a whole tuple built
# (and serialized) on the master and sent to the workers.
# When disabled, the parameters passed to a task with *args are serialized
# independently and the tuple is built on the worker.
aargs_as_tuple = False

# Setup logger
logger = logging.getLogger(__name__)


# ########################################################################### #
# ############################ CLASSES ###################################### #
# ########################################################################### #

class FunctionType(object):
    """
    Used as enum to identify the function type
    """

    FUNCTION = 1
    INSTANCE_METHOD = 2
    CLASS_METHOD = 3


class Future(object):
    """
    Future object class definition.
    """

    def __init__(self):
        # This UUID1 is for debugging purposes
        self.__hidden_id = str(uuid.uuid1())


class EmptyReturn(object):
    """
    For functions with empty return
    """

    pass


# ########################################################################### #
# ############ FUNCTIONS THAT COMMUNICATE WITH THE RUNTIME ################## #
# ########################################################################### #

def start_runtime():
    """
    Starts the runtime by calling the external python library that calls
    the bindings-common.

    :return: None
    """
    if __debug__:
        logger.info("Starting COMPSs...")
    compss.start_runtime()
    if __debug__:
        logger.info("COMPSs started")


def stop_runtime(code=0):
    """
    Stops the runtime by calling the external python library that calls
    the bindings-common.
    Also cleans objects and temporary files created during runtime.

    :return: None
    """
    if __debug__:
        logger.info("Stopping runtime...")

    if code != 0:
        if __debug__:
            logger.info("Canceling all application tasks...")
        compss.cancel_application_tasks(0)

    if __debug__:
        logger.info("Cleaning objects...")
    _clean_objects()

    if __debug__:
        logger.info("Stopping COMPSs...")
    compss.stop_runtime(code)

    if __debug__:
        logger.info("Cleaning temps...")
    _clean_temps()

    if __debug__:
        logger.info("COMPSs stopped")


def accessed_file(file_name):
    """
    Calls the external python library (that calls the bindings-common)
    in order to check if a file has been accessed.

    :param file_name: <String> File name.
    :return: True if accessed otherwise False;
    """
    if __debug__:
        logger.debug("Checking if file %s has been accessed." % file_name)
    return compss.accessed_file(file_name)


def open_file(file_name, mode):
    """
    Calls the external python library (that calls the bindings-common)
    in order to request a file.

    :param file_name: <String> File name.
    :param mode: Compss mode.
    :return: The current name of the file requested (that may have been
             renamed during runtime)
    """
    if __debug__:
        logger.debug("Getting file %s with mode %s" % (file_name, mode))
    compss_name = compss.open_file(file_name, mode)
    if __debug__:
        logger.debug("COMPSs file name is %s" % compss_name)
    return compss_name


def delete_file(file_name):
    """
    Calls the external python library (that calls the bindings-common)
    in order to request a file removal.

    :param file_name: File name to remove
    :return: True if success. False otherwise
    """
    if __debug__:
        logger.debug("Deleting file %s" % file_name)
    result = compss.delete_file(file_name, True) == 'true'
    if __debug__:
        if result:
            logger.debug("File %s successfully deleted." % file_name)
        else:
            logger.error("Failed to remove file %s." % file_name)
    return result


def get_file(file_name):
    """
    Calls the external python library (that calls the bindings-common)
    in order to request last version of file.

    :param file_name: File name to remove
    :return: None
    """
    if __debug__:
        logger.debug("Getting file %s" % file_name)

    compss.get_file(0, file_name)


def get_directory(dir_name):
    """
    Calls the external python library (that calls the bindings-common)
    in order to request last version of file.

    :param dir_name: dir name to retrieve
    :return: None
    """
    if __debug__:
        logger.debug("Getting directory %s" % dir_name)

    compss.get_directory(0, dir_name)


def delete_object(obj):
    """
    Removes a used object from the internal structures and calls the
    external python library (that calls the bindings-common)
    in order to request a its corresponding file removal.

    :param obj: Object to remove
    :return: True if success. False otherwise
    """
    obj_id = get_object_id(obj, False, False)
    if obj_id is None:
        pop_object_id(obj)
        return False

    try:
        pop_object_id(obj)
    except KeyError:
        pass
    try:
        file_name = objid_to_filename[obj_id]
        compss.delete_file(file_name, False)
    except KeyError:
        pass
    try:
        objid_to_filename.pop(obj_id)
    except KeyError:
        pass
    try:
        pending_to_synchronize.pop(obj_id)
    except KeyError:
        pass
    return True


def barrier(no_more_tasks=False):
    """
    Calls the external python library (that calls the bindings-common)
    in order to request a barrier.
    Wait for all tasks.

    :param no_more_tasks:
    :return: None
    """
    if __debug__:
        logger.debug("Barrier. No more tasks? %s" % str(no_more_tasks))

    # If noMoreFlags is set, clean up the objects
    if no_more_tasks:
        _clean_objects()

    # Call the Runtime barrier (appId 0, not needed for the signature)
    compss.barrier(0, no_more_tasks)


def barrier_group(group_name):
    """
    Calls the external python library (that calls the bindings-common)
    in order to request a barrier of a group.
    Wait for all tasks of the group.

    :param group_name: Group name
    :return: None
    """
    # Call the Runtime group barrier
    return compss.barrier_group(0, group_name)


def open_task_group(group_name, implicit_barrier):
    """
    Calls the external python library (that calls the bindings-common)
    in order to request an opening of a group.

    :param group_name: Group name
    :param implicit_barrier: <Boolean> Implicit barrier
    :return: None
    """
    compss.open_task_group(group_name, implicit_barrier, 0)


def close_task_group(group_name):
    """
    Calls the external python library (that calls the bindings-common)
    in order to request a group closure.

    :param group_name: Group name
    :return: None
    """
    compss.close_task_group(group_name, 0)


def get_log_path():
    """
    Requests the logging path to the external python library (that calls
    the bindings-common).

    :return: The path where to store the logs.
    """
    if __debug__:
        logger.debug("Requesting log path")
    log_path = compss.get_logging_path()
    if __debug__:
        logger.debug("Log path received: %s" % log_path)
    return log_path


def get_number_of_resources():
    """
    Calls the external python library (that calls the bindings-common)
    in order to request for the number of active resources.

    :return: Number of active resources
        +type: <int>
    """
    if __debug__:
        logger.debug("Request the number of active resources")

    # Call the Runtime (appId 0)
    return compss.get_number_of_resources(0)


def request_resources(num_resources, group_name):
    """
    Calls the external python library (that calls the bindings-common)
    in order to request for the creation of the given resources.

    :param num_resources: Number of resources to create.
        +type: <int>
    :param group_name: Task group to notify upon resource creation
        +type: <str> or None
    :return: None
    """
    if group_name is None:
        group_name = "NULL"

    if __debug__:
        logger.debug("Request the creation of " +
                     str(num_resources) +
                     " resources with notification to task group " +
                     str(group_name))

    # Call the Runtime (appId 0)
    compss.request_resources(0, num_resources, group_name)


def free_resources(num_resources, group_name):
    """
    Calls the external python library (that calls the bindings-common)
    in order to request for the destruction of the given resources.

    :param num_resources: Number of resources to destroy.
        +type: <int>
    :param group_name: Task group to notify upon resource creation
        +type: <str> or None
    :return: None
    """
    if group_name is None:
        group_name = "NULL"

    if __debug__:
        logger.debug("Request the destruction of " +
                     str(num_resources) +
                     " resources with notification to task group " +
                     str(group_name))

    # Call the Runtime (appId 0)
    compss.free_resources(0, num_resources, group_name)


def register_ce(core_element):
    """
    Calls the external python library (that calls the bindings-common)
    in order to notify the runtime about a core element that needs to be
    registered.
    Java Examples:

        // METHOD
        System.out.println('Registering METHOD implementation');
        String core_elementSignature = 'methodClass.methodName';
        String impl_signature = 'methodClass.methodName';
        String impl_constraints = 'ComputingUnits:2';
        String impl_type = 'METHOD';
        String[] impl_type_args = new String[] { 'methodClass', 'methodName' };
        rt.registerCoreElement(coreElementSignature, impl_signature, impl_constraints, impl_type, impl_type_args);  # noqa: E501

        // MPI
        System.out.println('Registering MPI implementation');
        core_elementSignature = 'methodClass1.methodName1';
        impl_signature = 'mpi.MPI';
        impl_constraints = 'StorageType:SSD';
        impl_type = 'MPI';
        impl_type_args = new String[] { 'mpiBinary', 'mpiWorkingDir', 'mpiRunner' };  # noqa: E501
        rt.registerCoreElement(coreElementSignature, impl_signature, impl_constraints, impl_type, impl_type_args);  # noqa: E501

        // PYTHON MPI
        System.out.println('Registering PYTHON MPI implementation');
        core_elementSignature = 'methodClass1.methodName1';
        impl_signature = 'MPI.methodClass1.methodName';
        impl_constraints = 'ComputingUnits:2';
        impl_type = 'PYTHON_MPI';
        impl_type_args = new String[] { 'methodClass', 'methodName', 'mpiWorkingDir', 'mpiRunner' };  # noqa: E501
        rt.registerCoreElement(coreElementSignature, impl_signature, impl_constraints, impl_type, impl_type_args);  # noqa: E501

        // BINARY
        System.out.println('Registering BINARY implementation');
        core_elementSignature = 'methodClass2.methodName2';
        impl_signature = 'binary.BINARY';
        impl_constraints = 'MemoryType:RAM';
        impl_type = 'BINARY';
        impl_type_args = new String[] { 'binary', 'binaryWorkingDir' };
        rt.registerCoreElement(coreElementSignature, impl_signature, impl_constraints, impl_type, impl_type_args);  # noqa: E501

        // OMPSS
        System.out.println('Registering OMPSS implementation');
        core_elementSignature = 'methodClass3.methodName3';
        impl_signature = 'ompss.OMPSS';
        impl_constraints = 'ComputingUnits:3';
        impl_type = 'OMPSS';
        impl_type_args = new String[] { 'ompssBinary', 'ompssWorkingDir' };
        rt.registerCoreElement(coreElementSignature, impl_signature, impl_constraints, impl_type, impl_type_args);  # noqa: E501

        // OPENCL
        System.out.println('Registering OPENCL implementation');
        core_elementSignature = 'methodClass4.methodName4';
        impl_signature = 'opencl.OPENCL';
        impl_constraints = 'ComputingUnits:4';
        impl_type = 'OPENCL';
        impl_type_args = new String[] { 'openclKernel', 'openclWorkingDir' };
        rt.registerCoreElement(coreElementSignature, impl_signature, impl_constraints, impl_type, impl_type_args);  # noqa: E501

        // VERSIONING
        System.out.println('Registering METHOD implementation');
        core_elementSignature = 'methodClass.methodName';
        impl_signature = 'anotherClass.anotherMethodName';
        impl_constraints = 'ComputingUnits:1';
        impl_type = 'METHOD';
        impl_type_args = new String[] { 'anotherClass', 'anotherMethodName' };
        rt.registerCoreElement(coreElementSignature, impl_signature, impl_constraints, impl_type, impl_type_args);  # noqa: E501

    ---------------------

    Core Element fields:

    ce_signature: <String> Core Element signature  (e.g.- 'methodClass.methodName')  # noqa: E501
    impl_signature: <String> Implementation signature (e.g.- 'methodClass.methodName')  # noqa: E501
    impl_constraints: <Dict> Implementation constraints (e.g.- '{ComputingUnits:2}')  # noqa: E501
    impl_type: <String> Implementation type ('METHOD' | 'MPI' | 'BINARY' | 'OMPSS' | 'OPENCL')  # noqa: E501
    impl_io: <String> IO Implementation  #noga
    impl_type_args: <List(Strings)> Implementation arguments (e.g.- ['methodClass', 'methodName'])  # noqa: E501

    :param core_element: <CE> Core Element to register
    :return: None
    """
    # Retrieve Core element fields
    ce_signature = core_element.get_ce_signature()
    impl_signature = core_element.get_impl_signature()
    impl_constraints = core_element.get_impl_constraints()
    impl_type = core_element.get_impl_type()
    impl_io = str(core_element.get_impl_io())
    impl_type_args = core_element.get_impl_type_args()

    if __debug__:
        logger.debug("Registering CE with signature: %s" % ce_signature)
        logger.debug("\t - Implementation signature: %s" % impl_signature)

    # Build constraints string from constraints dictionary
    impl_constraints_str = ''
    for key, value in impl_constraints.items():
        if isinstance(value, list):
            val = str(value).replace('\'', '')
            impl_constraints_str += key + ':' + str(val) + ';'
        else:
            impl_constraints_str += key + ':' + str(value) + ';'

    if __debug__:
        logger.debug("\t - Implementation constraints: %s" %
                     impl_constraints_str)
        logger.debug("\t - Implementation type: %s" %
                     impl_type)
        logger.debug("\t - Implementation type arguments: %s" %
                     ' '.join(impl_type_args))

    # Call runtime with the appropriate parameters
    compss.register_core_element(ce_signature,
                                 impl_signature,
                                 impl_constraints_str,
                                 impl_type,
                                 impl_io,
                                 impl_type_args)
    if __debug__:
        logger.debug("CE with signature %s registered." % ce_signature)


def wait_on(*args, **kwargs):
    """
    Waits on a set of objects defined in args with the options defined in
    kwargs.

    :param args: Objects to wait on.
    :param kwargs: Options: Write enable? [True | False] Default = True
    :return: Real value of the objects requested
    """
    ret = list(map(_compss_wait_on, args,
                   [kwargs.get("mode", "rw")] * len(args)))
    ret = ret[0] if len(ret) == 1 else ret
    # Check if there are empty elements return elements that need to be removed
    if isinstance(ret, listType):
        # Look backwards the list removing the first EmptyReturn elements
        for elem in reversed(ret):
            if isinstance(elem, EmptyReturn):
                ret.remove(elem)
    return ret


def _compss_wait_on(obj, mode):
    """
    Waits on an object.

    :param obj: Object to wait on.
    :param mode: Read or write mode
    :return: An object of 'file' type.
    """
    compss_mode = get_compss_mode(mode)

    # Private function used below (recursively)
    def wait_on_iterable(iter_obj):
        """
        Wait on an iterable object.
        Currently supports lists and dictionaries (syncs the values).
        :param iter_obj: iterable object
        :return: synchronized object
        """
        # check if the object is in our pending_to_synchronize dictionary
        from pycompss.runtime.binding import get_object_id
        obj_id = get_object_id(iter_obj)
        if obj_id in pending_to_synchronize:
            return synchronize(iter_obj, compss_mode)
        else:
            if type(iter_obj) == list:
                return [wait_on_iterable(x) for x in iter_obj]
            elif type(iter_obj) == dict:
                return {k: wait_on_iterable(v) for k, v in iter_obj.items()}
            else:
                return synchronize(iter_obj, compss_mode)

    if isinstance(obj, Future) or not (isinstance(obj, listType) or
                                       isinstance(obj, dictType)):
        return synchronize(obj, compss_mode)
    else:
        if len(obj) == 0:  # FUTURE OBJECT
            return synchronize(obj, compss_mode)
        else:
            # Will be a iterable object
            res = wait_on_iterable(obj)
            return res


def synchronize(obj, mode):
    """
    Synchronization function.
    This method retrieves the value of a future object.
    Calls the runtime in order to wait for the value and returns it when
    received.

    :param obj: Object to synchronize.
    :param mode: Direction of the object to synchronize.
    :return: The value of the object requested.
    """
    # TODO: Add a boolean to differentiate between files and object on the
    # compss.open_file call. This change pretends to obtain better traces.
    # Must be implemented first in the Runtime, then in the bindings common
    # C API and finally add the boolean here
    global _current_id

    if is_psco(obj):
        obj_id = get_id(obj)
        if obj_id not in pending_to_synchronize:
            return obj
        else:
            # file_path is of the form storage://pscoId or
            # file://sys_path_to_file
            file_path = compss.open_file("storage://" + str(obj_id), mode)
            # TODO: Add switch on protocol
            protocol, file_name = file_path.split('://')
            new_obj = get_by_id(file_name)
            return new_obj

    obj_id = get_object_id(obj)
    if obj_id not in pending_to_synchronize:
        return obj

    if __debug__:
        logger.debug("Synchronizing object %s with mode %s" % (obj_id, mode))

    file_name = objid_to_filename[obj_id]
    compss_file = compss.open_file(file_name, mode)

    # Runtime can return a path or a PSCOId
    if compss_file.startswith('/'):
        # If the real filename is null, then return None. The task that
        # produces the output file may have been ignored or cancelled, so its
        # result does not exist.
        real_file_name = compss_file.split('/')[-1]
        if real_file_name == 'null':
            print("WARNING: Could not retrieve the object " + str(file_name) +
                  " since the task that produces it may have been IGNORED or CANCELLED. Please, check the logs. Returning None.")  # noqa: E501
            return None
        new_obj = deserialize_from_file(compss_file)
        compss.close_file(file_name, mode)
    else:
        new_obj = get_by_id(compss_file)

    if mode == 'r':
        new_obj_id = get_object_id(new_obj, True, True)
        # The main program won't work with the old object anymore, update
        # mapping
        objid_to_filename[new_obj_id] = \
            objid_to_filename[obj_id].replace(obj_id, new_obj_id)
        _objs_written_by_mp[new_obj_id] = objid_to_filename[new_obj_id]

    if mode != 'r':
        compss.delete_file(objid_to_filename[obj_id], False)
        objid_to_filename.pop(obj_id)
        pending_to_synchronize.pop(obj_id)
        pop_object_id(obj)

    return new_obj


def process_task(f, module_name, class_name, ftype, f_parameters, f_returns,
                 task_kwargs, num_nodes, replicated, distributed,
                 on_failure, time_out):
    """
    Function that submits a task to the runtime.

    :param f: Function or method
    :param module_name: Name of the module containing the function/method
                        (including packages, if any)
    :param class_name: Name of the class (if method)
    :param ftype: Function type
    :param f_parameters: Function parameters (dictionary {'param1':Parameter()}
    :param f_returns: Function returns (dictionary {'*return_X':Parameter()}
    :param task_kwargs: Decorator arguments
    :param num_nodes: Number of nodes that the task must use
    :param replicated: Boolean indicating if the task must be replicated
    :param distributed: Boolean indicating if the task must be distributed
    :param on_failure: Action on failure
    :param time_out: Time for a task time out
    :return: The future object related to the task return
    """
    if __debug__:
        logger.debug("TASK: %s of type %s, in module %s, in class %s" %
                     (f.__name__, ftype, module_name, class_name))

    app_id = 0

    # Check if the function is an instance method or a class method.
    has_target = ftype == FunctionType.INSTANCE_METHOD
    fo = None
    if f_returns:
        fo = _build_return_objects(f_returns)

    num_returns = len(f_returns)

    # Get path
    if class_name == '':
        path = module_name
    else:
        path = module_name + '.' + class_name

    # Infer COMPSs types from real types, except for files
    _serialize_objects(f_parameters)

    # Build values and COMPSs types and directions
    vtdsc = _build_values_types_directions(ftype,
                                          f_parameters,
                                          f_returns,
                                          f.__code_strings__)
    values, names, compss_types, compss_directions, compss_streams, \
      compss_prefixes, content_types, weights, keep_renames = vtdsc  # noqa

    # Get priority
    has_priority = task_kwargs['priority']

    # Signature and other parameters:
    signature = '.'.join([path, f.__name__])
    # num_nodes = 1        # default due to not MPI decorator yet
    # replicated = False   # default due to not replicated tag yet
    # distributed = False  # default due to not distributed tag yet

    if __debug__:
        # Log the task submission values for debugging purposes.
        if logger.isEnabledFor(logging.DEBUG):
            values_str = ' '.join(str(v) for v in values)
            types_str = ' '.join(str(t) for t in compss_types)
            direct_str = ' '.join(str(d) for d in compss_directions)
            streams_str = ' '.join(str(s) for s in compss_streams)
            prefixes_str = ' '.join(str(p) for p in compss_prefixes)
            names_str = ' '.join(x for x in names)
            ct_str = ' '.join(str(x) for x in content_types)
            weights_str = ' '.join(str(x) for x in weights)
            keep_renames_str = ' '.join(str(x) for x in keep_renames)
            logger.debug("Processing task:")
            logger.debug("\t- App id: " + str(app_id))
            logger.debug("\t- Path: " + path)
            logger.debug("\t- Function name: " + f.__name__)
            logger.debug("\t- On failure behavior: " + on_failure)
            logger.debug("\t- Task time out: " + str(time_out))
            logger.debug("\t- Signature: " + signature)
            logger.debug("\t- Priority: " + str(has_priority))
            logger.debug("\t- Has target: " + str(has_target))
            logger.debug("\t- Num nodes: " + str(num_nodes))
            logger.debug("\t- Replicated: " + str(replicated))
            logger.debug("\t- Distributed: " + str(distributed))
            logger.debug("\t- Values: " + values_str)
            logger.debug("\t- Names: " + names_str)
            logger.debug("\t- COMPSs types: " + types_str)
            logger.debug("\t- COMPSs directions: " + direct_str)
            logger.debug("\t- COMPSs streams: " + streams_str)
            logger.debug("\t- COMPSs prefixes: " + prefixes_str)
            logger.debug("\t- Content Types: " + ct_str)
            logger.debug("\t- Weights: " + weights_str)
            logger.debug("\t- Keep_renames: " + keep_renames_str)

    # Check that there is the same amount of values as their types, as well
    # as their directions, streams and prefixes.
    assert (len(values) == len(compss_types) == len(compss_directions) ==
            len(compss_streams) == len(compss_prefixes) == len(content_types) == 
            len(weights) == len(keep_renames))

    # Submit task to the runtime (call to the C extension):
    # Parameters:
    #     0 - <Integer>   - application id (by default always 0 due to it is
    #                       not currently needed for the signature)
    #     1 - <String>    - path of the module where the task is
    #
    #     2 - <String>    - behavior if the task fails
    #
    #     3 - <String>    - function name of the task (to be called from the
    #                       worker)
    #     4 - <String>    - priority flag (true|false)
    #
    #     5 - <String>    - has target (true|false). If the task is within an
    #                       object or not.
    #     6 - [<String>]  - task parameters (basic types or file paths for
    #                       objects)
    #     7 - [<Integer>] - parameters types (number corresponding to the type
    #                       of each parameter)
    #     8 - [<Integer>] - parameters directions (number corresponding to the
    #                       direction of each parameter)
    #     9 - [<Integer>] - parameters streams (number corresponding to the
    #                       stream of each parameter)
    #     10 - [<String>] - parameters prefixes (sting corresponding to the
    #                       prefix of each parameter)

    compss.process_task(app_id,
                        signature,
                        on_failure,
                        time_out,
                        has_priority,
                        num_nodes,
                        replicated,
                        distributed,
                        has_target,
                        num_returns,
                        values,
                        names,
                        compss_types,
                        compss_directions,
                        compss_streams,
                        compss_prefixes,
                        content_types,
                        weights,
                        keep_renames)

    # Return the future object/s corresponding to the task
    # This object will substitute the user expected return from the task and
    # will be used later for synchronization or as a task parameter (then the
    # runtime will take care of the dependency.
    return fo


def get_compss_mode(pymode):
    """
    Get the direction of pymode string.

    :param pymode: String to parse and return the direction
    :return: Direction object (IN/INOUT/OUT)
    """
    if pymode.startswith('w'):
        return DIRECTION.OUT
    elif pymode.startswith('r+') or pymode.startswith('a'):
        return DIRECTION.INOUT
    elif pymode.startswith('c'):
        return DIRECTION.CONCURRENT
    elif pymode.startswith('cv'):
        return DIRECTION.COMMUTATIVE
    else:
        return DIRECTION.IN


# ########################################################################### #
# ####################### AUXILIARY FUNCTIONS ############################### #
# ########################################################################### #

def _build_return_objects(f_returns):
    """
    Build the return object from the f_returns dictionary and include their
    filename in f_returns.

    WARNING: Updates f_returns dictionary

    :param f_returns: Dictionary which contains the return objects and
                      Parameters.
    :return: future object/s
    """
    fo = None
    if len(f_returns) == 0:
        # No return
        return fo
    elif len(f_returns) == 1:
        # Simple return
        if __debug__:
            logger.debug("Simple object return found.")
        # Build the appropriate future object
        ret_value = f_returns[parameter.get_return_name(0)].object
        if type(ret_value) in \
                _python_to_compss or ret_value in _python_to_compss:
            fo = Future()  # primitives,string,dic,list,tuple
        elif inspect.isclass(ret_value):
            # For objects:
            # type of future has to be specified to allow o = func; o.func
            try:
                fo = ret_value()
            except TypeError:
                if __debug__:
                    logger.warning("Type {0} does not have an empty constructor, building generic future object".format(ret_value))  # noqa: E501
                fo = Future()
        else:
            fo = Future()  # modules, functions, methods
        obj_id = get_object_id(fo, True)
        if __debug__:
            logger.debug("Setting object %s of %s as a future" % (obj_id,
                                                                  type(fo)))
        ret_filename = temp_dir + _temp_obj_prefix + str(obj_id)
        objid_to_filename[obj_id] = ret_filename
        pending_to_synchronize[obj_id] = fo
        f_returns[parameter.get_return_name(0)] = \
            Parameter(p_type=TYPE.FILE,
                      p_direction=DIRECTION.OUT,
                      p_prefix="#")
        f_returns[parameter.get_return_name(0)].file_name = ret_filename
    else:
        # Multireturn
        fo = []
        if __debug__:
            logger.debug("Multiple objects return found.")
        for k, v in f_returns.items():
            # Build the appropriate future object
            if v.object in _python_to_compss:
                foe = Future()  # primitives, string, dic, list, tuple
            elif inspect.isclass(v.object):
                # For objects:
                # type of future has to be specified to allow o = func; o.func
                try:
                    foe = v.object()
                except TypeError:
                    if __debug__:
                        logger.warning("Type {0} does not have an empty constructor, building generic future object".format(v['Value']))  # noqa: E501
                    foe = Future()
            else:
                foe = Future()  # modules, functions, methods
            fo.append(foe)
            obj_id = get_object_id(foe, True)
            if __debug__:
                logger.debug("Setting object %s of %s as a future" %
                             (obj_id, type(foe)))
            ret_filename = temp_dir + _temp_obj_prefix + str(obj_id)
            objid_to_filename[obj_id] = ret_filename
            pending_to_synchronize[obj_id] = foe
            # Once determined the filename where the returns are going to
            # be stored, create a new Parameter object for each return object
            f_returns[k] = Parameter(p_type=TYPE.FILE,
                                     p_direction=DIRECTION.OUT,
                                     p_prefix="#")
            f_returns[k].file_name = ret_filename
    return fo


def _serialize_objects(f_parameters):
    """
    Infer COMPSs types for the task parameters and serialize them.

    WARNING: Updates f_parameters dictionary

    :param f_parameters: <Dictionary> - Function parameters
    :return: Tuple of task_kwargs updated and a dictionary containing if the
             objects are future elements.
    """
    max_obj_arg_size = 320000
    for k in f_parameters:
        # Check user annotations concerning this argument
        p = f_parameters[k]
        # Convert small objects to string if object_conversion enabled
        # Check if the object is small in order not to serialize it.
        if object_conversion:
            p, written_bytes = _convert_object_to_string(p,
                                                         max_obj_arg_size,
                                                         policy='objectSize')
            max_obj_arg_size -= written_bytes
        # Serialize objects into files
        p = _serialize_object_into_file(k, p)
        # Update k parameter's Parameter object
        f_parameters[k] = p

        if __debug__:
            logger.debug("Final type for parameter %s: %d" % (k, p.type))


def _build_values_types_directions(ftype, f_parameters, f_returns,
                                   code_strings):
    """
    Build the values list, the values types list and the values directions list

    :param ftype: task function type. If it is an instance method, the first
                  parameter will be put at the end.
    :param f_parameters: <Dictionary> Function parameters
    :param f_returns: <Dictionary> - Function returns
    :param code_strings: <Boolean> Code strings or not
    :return: <List,List,List,List,List> List of values, their types, their
             directions, their streams and their prefixes
    """
    slf = None
    values = []
    names = []
    arg_names = list(f_parameters.keys())
    result_names = list(f_returns.keys())
    compss_types = []
    compss_directions = []
    compss_streams = []
    compss_prefixes = []
    content_types = list()
    slf_name = None
    weights = []
    keep_renames = []

    # Build the range of elements
    ra = list(f_parameters.keys())
    if ftype == FunctionType.INSTANCE_METHOD or \
            ftype == FunctionType.CLASS_METHOD:
        slf = ra.pop(0)
        slf_name = arg_names.pop(0)
    # Fill the values, compss_types, compss_directions, compss_streams and
    # compss_prefixes from function parameters
    for i in ra:
        val, typ, direc, st, pre, ct, wght, kr = _extract_parameter(f_parameters[i],
                                                      code_strings)
        values.append(val)
        compss_types.append(typ)
        compss_directions.append(direc)
        compss_streams.append(st)
        compss_prefixes.append(pre)
        names.append(arg_names.pop(0))
        content_types.append(ct)
        weights.append(wght)
        keep_renames.append(kr)
    # Fill the values, compss_types, compss_directions, compss_streams and
    # compss_prefixes from self (if exist)
    if ftype == FunctionType.INSTANCE_METHOD:
        # self is always an object
        val, typ, direc, st, pre, ct, wght, kr = _extract_parameter(f_parameters[slf],
                                                      code_strings)
        values.append(val)
        compss_types.append(typ)
        compss_directions.append(direc)
        compss_streams.append(st)
        compss_prefixes.append(pre)
        names.append(slf_name)
        content_types.append(ct)
        weights.append(wght)
        keep_renames.append(kr)

    # Fill the values, compss_types, compss_directions, compss_streams and
    # compss_prefixes from function returns
    for r in f_returns:
        p = f_returns[r]
        values.append(f_returns[r].file_name)
        compss_types.append(p.type)
        compss_directions.append(p.direction)
        compss_streams.append(p.stream)
        compss_prefixes.append(p.prefix)
        names.append(result_names.pop(0))
        content_types.append(p.content_type)
        weights.append(p.weight)
        keep_renames.append(p.keep_rename)

    return values, names, compss_types, compss_directions, compss_streams,\
        compss_prefixes, content_types, weights, keep_renames


def _extract_parameter(param, code_strings, collection_depth=0):
    """
    Extract the information of a single parameter

    :param param: Parameter object
    :param code_strings: <Boolean> Encode strings
    :return: value, type, direction stream prefix and content_type of the given
    parameter
    """
    con_type = parameter.UNDEFINED_CONTENT_TYPE
    if param.type == TYPE.STRING and not param.is_future and code_strings:
        # Encode the string in order to preserve the source
        # Checks that it is not a future (which is indicated with a path)
        # Considers multiple spaces between words
        param.object = base64.b64encode(param.object.encode()).decode()
        if len(param.object) == 0:
            # Empty string - use escape string to avoid padding
            # Checked and substituted by empty string in the worker.py and
            # piper_worker.py
            param.object = base64.b64encode(EMPTY_STRING_KEY.encode()).decode()
        con_type = content_type_format.format(
            "builtins", str(param.object.__class__.__name__))

    if param.type == TYPE.FILE or param.is_future:
        # If the parameter is a file or is future, the content is in a file
        # and we register it as file
        value = param.file_name
        typ = TYPE.FILE

    elif param.type == TYPE.DIRECTORY:
        value = param.file_name
        typ = TYPE.DIRECTORY

    elif param.type == TYPE.OBJECT:
        # If the parameter is an object, its value is stored in a file and
        # we register it as file
        value = param.file_name
        typ = TYPE.FILE

        try:
            _mf = sys.modules[param.object.__class__.__module__].__file__
        except AttributeError:
            # 'builtin' modules do not have __file__ attribute!
            _mf = "builtins"

        _class_name = str(param.object.__class__.__name__)
        con_type = content_type_format.format(_mf, _class_name)

    elif param.type == TYPE.EXTERNAL_STREAM:
        # If the parameter type is stream, its value is stored in a file but
        # we keep the type
        value = param.file_name
        typ = TYPE.EXTERNAL_STREAM
    elif param.type == TYPE.COLLECTION or \
            (collection_depth > 0 and is_basic_iterable(param.obj)):
        # An object will be considered a collection if at least one of the
        # following is true:
        #     1) We said it is a collection in the task decorator
        #     2) It is part of some collection object, it is iterable and we
        #        are inside the specified depth radius
        #
        # The content of a collection is sent via JNI to the master, and the
        # format is:
        # collectionId numberOfElements collectionPyContentType
        #     type1 Id1 pyType1
        #     type2 Id2 pyType2
        #     ...
        #     typeN IdN pyTypeN
        _class_name = str(param.object.__class__.__name__)
        con_type = content_type_format.format("collection", _class_name)
        value = "{} {} {}".format(get_object_id(param.object),
                                  len(param.object), con_type)
        pop_object_id(param.object)
        typ = TYPE.COLLECTION
        for (i, x) in enumerate(param.object):
            x_value, x_type, _, _, _, x_con_type, _, _ = _extract_parameter(
                x,
                code_strings,
                param.depth - 1
            )
            value += ' %s %s %s' % (x_type, x_value, x_con_type)
    else:
        # Keep the original value and type
        value = param.object
        typ = param.type

    # Get direction, stream and prefix
    direction = param.direction

    # Get stream and prefix
    stream = param.stream
    prefix = param.prefix
    return value, typ, direction, stream, prefix, con_type, param.weight, param.keep_rename


def _convert_object_to_string(p, max_obj_arg_size, policy='objectSize'):
    """
    Convert small objects into strings that can fit into the task parameters
     call

    :param p: Object wrapper
    :param max_obj_arg_size: max size of the object to be converted
    :param policy: policy to use: 'objectSize' for considering the size of the
                   object or 'serializedSize' for considering the size of the
                    object serialized.
    :return: the object possibly converted to string
    """
    is_future = p.is_future

    if IS_PYTHON3:
        base_string = str
    else:
        base_string = basestring  # noqa

    num_bytes = 0
    if policy == 'objectSize':
        # Check if the object is small in order to serialize it.
        # This alternative evaluates the size of the object before
        # serializing the object.
        # Warning: calculate the size of a python object can be difficult
        # in terms of time and precision
        if (p.type == TYPE.OBJECT or p.type == TYPE.STRING) \
                and not is_future and p.direction == DIRECTION.IN:
            if not isinstance(p.object, base_string) and \
                    isinstance(p.object,
                               (list, dict, tuple, deque, set, frozenset)):
                # check object size - The following line does not work
                # properly with recursive objects
                # bytes = sys.getsizeof(p.object)
                num_bytes = total_sizeof(p.object)
                if __debug__:
                    megabytes = num_bytes / 1000000  # truncate
                    logger.debug("Object size %d bytes (%d Mb)." % (num_bytes,
                                                                    megabytes))

                if num_bytes < max_obj_arg_size:
                    # be careful... more than this value produces:
                    # Cannot run program '/bin/bash'...: error=7, \
                    # The arguments list is too long
                    if __debug__:
                        logger.debug("The object size is less than 320 kb.")
                    real_value = p.object
                    try:
                        v = serialize_to_string(p.object)
                        p.object = v.encode(STR_ESCAPE)
                        p.type = TYPE.STRING
                        if __debug__:
                            logger.debug("Inferred type modified (Object converted to String).")  # noqa: E501
                    except SerializerException:
                        p.object = real_value
                        p.type = TYPE.OBJECT
                        if __debug__:
                            logger.debug("The object cannot be converted due to: not serializable.")  # noqa: E501
                else:
                    p.type = TYPE.OBJECT
                    if __debug__:
                        logger.debug("Inferred type reestablished to Object.")
                        # if the parameter converts to an object, release the
                        # size to be used for converted objects?
                        # No more objects can be converted
                        # max_obj_arg_size += _bytes
                        # if max_obj_arg_size > 320000:
                        #     max_obj_arg_size = 320000
    elif policy == 'serializedSize':
        if IS_PYTHON3:
            from pickle import PicklingError
        else:
            from cPickle import PicklingError  # noqa
        # Check if the object is small in order to serialize it.
        # This alternative evaluates the size after serializing the parameter
        if (p.type == TYPE.OBJECT or p.type == TYPE.STRING) \
                and not is_future and p.direction == DIRECTION.IN:
            if not isinstance(p.object, base_string):
                real_value = p.object
                try:
                    v = serialize_to_string(p.object)
                    v = v.encode(STR_ESCAPE)
                    # check object size
                    num_bytes = sys.getsizeof(v)
                    if __debug__:
                        megabytes = num_bytes / 1000000  # truncate
                        logger.debug("Object size %d bytes (%d Mb)." %
                                     (num_bytes, megabytes))
                    if num_bytes < max_obj_arg_size:
                        # be careful... more than this value produces:
                        # Cannot run program '/bin/bash'...: error=7,
                        # arguments list too long error.
                        if __debug__:
                            logger.debug("The object size is less than 320 kb")
                        p.object = v
                        p.type = TYPE.STRING
                        if __debug__:
                            logger.debug("Inferred type modified (Object converted to String).")  # noqa: E501
                    else:
                        p.object = real_value
                        p.type = TYPE.OBJECT
                        if __debug__:
                            logger.debug("Inferred type reestablished to Object.")  # noqa: E501
                            # if the parameter converts to an object, release
                            # the size to be used for converted objects?
                            # No more objects can be converted
                            # max_obj_arg_size += _bytes
                            # if max_obj_arg_size > 320000:
                            #     max_obj_arg_size = 320000
                except PicklingError:
                    p.object = real_value
                    p.type = TYPE.OBJECT
                    if __debug__:
                        logger.debug("The object cannot be converted due to: not serializable.")  # noqa: E501
    else:
        if __debug__:
            logger.debug("[ERROR] Wrong convert_objects_to_strings policy.")
        raise Exception("Wrong convert_objects_to_strings policy.")

    return p, num_bytes


def _serialize_object_into_file(name, p):
    """
    Serialize an object into a file if necessary.

    :param name: Name of the object
    :param p: Object wrapper
    :return: p (whose type and value might be modified)
    """
    if p.type == TYPE.OBJECT or p.type == TYPE.EXTERNAL_STREAM or p.is_future:
        # 2nd condition: real type can be primitive, but now it's acting as a
        # future (object)
        try:
            val_type = type(p.object)
            if isinstance(val_type, list):
                # Is there a future object within the list?
                if any(isinstance(v, Future) for v in p.object):
                    if __debug__:
                        logger.debug("Found a list that contains future objects - synchronizing...")  # noqa: E501
                    mode = get_compss_mode('in')
                    p.object = list(map(synchronize,
                                        p.object,
                                        [mode] * len(p.object)))
            _skip_file_creation = (p.direction == DIRECTION.OUT and
                                   p.type != TYPE.EXTERNAL_STREAM)
            _turn_into_file(p, skip_creation=_skip_file_creation)
        except SerializerException:
            import sys
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type,
                                               exc_value,
                                               exc_traceback)
            logger.exception("Pickling error exception: non-serializable object found as a parameter.")  # noqa: E501
            logger.exception(''.join(line for line in lines))
            print("[ ERROR ]: Non serializable objects can not be used as parameters (e.g. methods).")  # noqa: E501
            print("[ ERROR ]: Object: %s" % p.object)
            # Raise the exception up tu launch.py in order to point where the
            # error is in the user code.
            raise
    elif p.type == TYPE.EXTERNAL_PSCO:
        _manage_persistent_object(p)
    elif p.type == TYPE.INT:
        if p.object > JAVA_MAX_INT or p.object < JAVA_MIN_INT:
            # This must go through Java as a long to prevent overflow with
            # Java integer
            p.type = TYPE.LONG
    elif p.type == TYPE.LONG:
        if p.object > JAVA_MAX_LONG or p.object < JAVA_MIN_LONG:
            # This must be serialized to prevent overflow with Java long
            p.type = TYPE.OBJECT
            _skip_file_creation = (p.direction == DIRECTION.OUT)
            _turn_into_file(p, _skip_file_creation)
    elif p.type == TYPE.STRING:
        from pycompss.api.task import prepend_strings
        if prepend_strings:
            # Strings can be empty. If a string is empty their base64 encoding
            # will be empty.
            # So we add a leading character to it to make it non empty
            p.object = '#%s' % p.object
    elif p.type == TYPE.COLLECTION:
        # Just make contents available as serialized files (or objects)
        # We will build the value field later
        # (which will be used to reconstruct the collection in the worker)
        if p.is_file_collection:
            new_object = [
                Parameter(
                    p_type=TYPE.FILE,
                    p_direction=p.direction,
                    p_object=x,
                    file_name=x,
                    depth=p.depth - 1
                )
                for x in p.object
            ]
        else:
            new_object = [
                _serialize_object_into_file(
                    name,
                    Parameter(
                        p_type=get_compss_type(x, p.depth - 1),
                        p_direction=p.direction,
                        p_object=x,
                        depth=p.depth - 1,
                        content_type=str(type(x).__name__)
                    )
                )
                for x in p.object
            ]

        p.object = new_object
        # Give this object an identifier inside the binding
        get_object_id(p.object, True, False)
    return p


def _manage_persistent_object(p):
    """
    Does the necessary actions over a persistent object used as task parameter.
    Check if the object has already been used (indexed in the objid_to_filename
    dictionary).
    In particular, saves the object id provided by the persistent storage
    (getID()) into the pending_to_synchronize dictionary.

    :param p: wrapper of the object to manage
    :return: None
    """
    p.type = TYPE.EXTERNAL_PSCO
    obj_id = get_id(p.object)
    pending_to_synchronize[obj_id] = p.object  # obj_id
    p.object = obj_id
    if __debug__:
        logger.debug("Managed persistent object: %s" % obj_id)


def _turn_into_file(p, skip_creation=False):
    """
    Write a object into a file if the object has not been already written
    (p.object).
    Consults the objid_to_filename to check if it has already been written
    (reuses it if exists). If not, the object is serialized to file and
    registered in the objid_to_filename dictionary.
    This functions stores the object into pending_to_synchronize

    :param p: Wrapper of the object to turn into file
    :return: None
    """
    # print('p           : ', p)
    # print('p.object    : ', p.object)
    # print('p.type      : ', p.type)
    # print('p.direction : ', p.direction)
    # if p.direction == DIRECTION.OUT:
    #     # If the parameter is out, infer the type and create an empty
    #     # instance of the same type as the original parameter:
    #     t = type(p.object)
    #     p.object = t()

    obj_id = get_object_id(p.object, True)
    file_name = objid_to_filename.get(obj_id)
    if file_name is None:
        # This is the first time a task accesses this object
        pending_to_synchronize[obj_id] = p.object
        file_name = temp_dir + _temp_obj_prefix + str(obj_id)
        objid_to_filename[obj_id] = file_name
        if __debug__:
            logger.debug("Mapping object %s to file %s" % (obj_id, file_name))
        if not skip_creation:
            serialize_to_file(p.object, file_name)
    elif obj_id in _objs_written_by_mp:
        if p.direction == DIRECTION.INOUT or \
                p.direction == DIRECTION.COMMUTATIVE:
            pending_to_synchronize[obj_id] = p.object
        # Main program generated the last version
        compss_file = _objs_written_by_mp.pop(obj_id)
        if __debug__:
            logger.debug("Serializing object %s to file %s" % (obj_id,
                                                               compss_file))
        if not skip_creation:
            serialize_to_file(p.object, compss_file)
    else:
        pass
    # Set file name in Parameter object
    p.file_name = file_name


def _clean_objects():
    """
    Clean the objects stored in the global dictionaries:
        * pending_to_synchronize dict
        * _addr2id2obj dict
        * objid_to_filename dict
        * _objs_written_by_mp dict

    :return: None
    """
    for filename in objid_to_filename.values():
        compss.delete_file(filename, False)
    pending_to_synchronize.clear()
    _addr2id2obj.clear()
    objid_to_filename.clear()
    _objs_written_by_mp.clear()


def _clean_temps():
    """
    Clean temporary files.
    The temporary files end with the IT extension

    :return: None
    """
    rmtree(temp_dir, True)
    cwd = os.getcwd()
    for f in os.listdir(cwd):
        if re.search(r'd\d+v\d+_\d+\.IT', f):
            os.remove(os.path.join(cwd, f))
