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
from pycompss.runtime.commons import EMPTY_STRING_KEY
from pycompss.runtime.commons import STR_ESCAPE
from pycompss.runtime.commons import IS_PYTHON3
from pycompss.util.serializer import *
from pycompss.util.sizer import total_sizeof
from pycompss.util.persistent_storage import is_psco, get_id, get_by_id

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
# See ext/compssmodule.c
import compss

# Types conversion dictionary from python to COMPSs
if IS_PYTHON3:
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
                         # The type of dictionaries (e.g. {'Bacon': 1, 'Ham': 0})
                         dict: TYPE.OBJECT,
                         # The type of generic objects
                         object: TYPE.OBJECT
                         }
else:
    _python_to_compss = {types.IntType: TYPE.INT,  # int
                         types.LongType: TYPE.LONG,  # long
                         types.FloatType: TYPE.DOUBLE,  # float
                         types.BooleanType: TYPE.BOOLEAN,  # bool
                         types.StringType: TYPE.STRING,  # str
                         # The type of instances of user-defined classes
                         # types.InstanceType: TYPE.OBJECT,
                         # The type of methods of user-defined class instances
                         # types.MethodType: TYPE.OBJECT,
                         # The type of user-defined old-style classes
                         # types.ClassType: TYPE.OBJECT,
                         # The type of modules
                         # types.ModuleType: TYPE.OBJECT,
                         # The type of tuples (e.g. (1, 2, 3, 'Spam'))
                         types.TupleType: TYPE.OBJECT,
                         # The type of lists (e.g. [0, 1, 2, 3])
                         types.ListType: TYPE.OBJECT,
                         # The type of dictionaries (e.g. {'Bacon': 1, 'Ham': 0})
                         types.DictType: TYPE.OBJECT,
                         # The type of generic objects
                         types.ObjectType: TYPE.OBJECT
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
_id2obj = {}


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
        # As mentioned before, identifiers are of the form _runtime_id-_current_id
        # in order to avoid having two objects from different applications with
        # the same identifier (and thus file name)
        new_id = '%s-%d' % (_runtime_id, _current_id)
        _id2obj[new_id] = obj
        _current_id += 1
        return new_id
    return None


# Enable or disable the management of *args parameters as a whole tuple built
# (and serialized) on the master and sent to the workers.
# When disabled, the parameters passed to a task with *args are serialized
# independently and the tuple is built on the worker.
aargs_as_tuple = False

# Setup logger
logger = logging.getLogger(__name__)


# ##############################################################################
# ############################ CLASSES #########################################
# ##############################################################################

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


# ##############################################################################
# ############ FUNCTIONS THAT COMMUNICATE WITH THE RUNTIME #####################
# ##############################################################################

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


def stop_runtime():
    """
    Stops the runtime by calling the external python library that calls
    the bindings-common.
    Also cleans objects and temporary files created during runtime.

    :return: None
    """

    if __debug__:
        logger.info("Cleaning objects...")
    _clean_objects()

    if __debug__:
        logger.info("Stopping COMPSs...")
    compss.stop_runtime()

    if __debug__:
        logger.info("Cleaning temps...")
    _clean_temps()

    if __debug__:
        logger.info("COMPSs stopped")


def get_file(file_name, mode):
    """
    Calls the external python library (that calls the bindings-common)
    in order to request a file.

    :param file_name: <String> File name.
    :param mode: Compss mode.
    :return: The current name of the file requested (that may have been renamed during runtime)
    """

    if __debug__:
        logger.debug("Getting file %s with mode %s" % (file_name, mode))
    compss_name = compss.get_file(file_name, mode)
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
    result = compss.delete_file(file_name) == 'true'
    if __debug__:
        if result:
            logger.debug("File %s successfully deleted." % file_name)
        else:
            logger.error("Failed to remove file %s." % file_name)
    return result


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
        return False

    try:
        _id2obj.pop(obj_id)
    except KeyError:
        pass
    try:
        file_name = objid_to_filename[obj_id]
        compss.delete_file(file_name)
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
        rt.registerCoreElement(coreElementSignature, impl_signature, impl_constraints, impl_type, impl_type_args);

        // MPI
        System.out.println('Registering MPI implementation');
        core_elementSignature = 'methodClass1.methodName1';
        impl_signature = 'mpi.MPI';
        impl_constraints = 'StorageType:SSD';
        impl_type = 'MPI';
        impl_type_args = new String[] { 'mpiBinary', 'mpiWorkingDir', 'mpiRunner' };
        rt.registerCoreElement(coreElementSignature, impl_signature, impl_constraints, impl_type, impl_type_args);

        // BINARY
        System.out.println('Registering BINARY implementation');
        core_elementSignature = 'methodClass2.methodName2';
        impl_signature = 'binary.BINARY';
        impl_constraints = 'MemoryType:RAM';
        impl_type = 'BINARY';
        impl_type_args = new String[] { 'binary', 'binaryWorkingDir' };
        rt.registerCoreElement(coreElementSignature, impl_signature, impl_constraints, impl_type, impl_type_args);

        // OMPSS
        System.out.println('Registering OMPSS implementation');
        core_elementSignature = 'methodClass3.methodName3';
        impl_signature = 'ompss.OMPSS';
        impl_constraints = 'ComputingUnits:3';
        impl_type = 'OMPSS';
        impl_type_args = new String[] { 'ompssBinary', 'ompssWorkingDir' };
        rt.registerCoreElement(coreElementSignature, impl_signature, impl_constraints, impl_type, impl_type_args);

        // OPENCL
        System.out.println('Registering OPENCL implementation');
        core_elementSignature = 'methodClass4.methodName4';
        impl_signature = 'opencl.OPENCL';
        impl_constraints = 'ComputingUnits:4';
        impl_type = 'OPENCL';
        impl_type_args = new String[] { 'openclKernel', 'openclWorkingDir' };
        rt.registerCoreElement(coreElementSignature, impl_signature, impl_constraints, impl_type, impl_type_args);

        // VERSIONING
        System.out.println('Registering METHOD implementation');
        core_elementSignature = 'methodClass.methodName';
        impl_signature = 'anotherClass.anotherMethodName';
        impl_constraints = 'ComputingUnits:1';
        impl_type = 'METHOD';
        impl_type_args = new String[] { 'anotherClass', 'anotherMethodName' };
        rt.registerCoreElement(coreElementSignature, impl_signature, impl_constraints, impl_type, impl_type_args);

    ---------------------

    Core Element fields:

    ce_signature: <String> Core Element signature  (e.g.- 'methodClass.methodName')
    impl_signature: <String> Implementation signature (e.g.- 'methodClass.methodName')
    impl_constraints: <Dict> Implementation constraints (e.g.- '{ComputingUnits:2}')
    impl_type: <String> Implementation type ('METHOD' | 'MPI' | 'BINARY' | 'OMPSS' | 'OPENCL')
    impl_type_args: <List(Strings)> Implementation arguments (e.g.- ['methodClass', 'methodName'])

    :param core_element: <CE> Core Element to register
    :return:
    """

    # Retrieve Core element fields
    ce_signature = core_element.get_ce_signature()
    impl_signature = core_element.get_impl_signature()
    impl_constraints = core_element.get_impl_constraints()
    impl_type = core_element.get_impl_type()
    impl_type_args = core_element.get_impl_type_args()

    if __debug__:
        logger.debug("Registering CE with signature: %s" % ce_signature)
        logger.debug("\t - Implementation signature: %s" % impl_signature)

    # Build constraints string from constraints dictionary
    impl_constraints_string = ''
    for key, value in impl_constraints.items():
        if isinstance(value, list):
            val = str(value).replace('\'', '')
            impl_constraints_string += key + ':' + str(val) + ';'
        else:
            impl_constraints_string += key + ':' + str(value) + ';'

    if __debug__:
        logger.debug("\t - Implementation constraints: %s" % impl_constraints_string)
        logger.debug("\t - Implementation type: %s" % impl_type)
    impl_type_args_string = ' '.join(impl_type_args)
    if __debug__:
        logger.debug("\t - Implementation type arguments: %s" % impl_type_args_string)

    # Call runtime with the appropriate parameters
    compss.register_core_element(ce_signature,
                                 impl_signature,
                                 impl_constraints_string,
                                 impl_type,
                                 impl_type_args)
    if __debug__:
        logger.debug("CE with signature %s registered." % ce_signature)


def synchronize(obj, mode):
    """
    Synchronization function.
    This method retrieves the value of a future object.
    Calls the runtime in order to wait for the value and returns it when received.

    :param obj: Object to synchronize.
    :param mode: Direction of the object to synchronize.
    :return: The value of the object requested.
    """

    # TODO: Add a boolean to differentiate between files and object on the compss.get_file call. This change pretends
    # to obtain better traces. Must be implemented first in the Runtime, then in the bindings common C API and
    # finally add the boolean here
    global _current_id

    if is_psco(obj):
        obj_id = get_id(obj)
        if obj_id not in pending_to_synchronize:
            return obj
        else:
            # file_path is of the form storage://pscoId or file://sys_path_to_file
            file_path = compss.get_file("storage://" + str(obj_id), mode)
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
    compss_file = compss.get_file(file_name, mode)

    # Runtime can return a path or a PSCOId
    if compss_file.startswith('/'):
        new_obj = deserialize_from_file(compss_file)
        compss.close_file(file_name, mode)
    else:
        new_obj = get_by_id(compss_file)
    new_obj_id = get_object_id(new_obj, True, True)

    # The main program won't work with the old object anymore, update mapping
    objid_to_filename[new_obj_id] = objid_to_filename[obj_id].replace(obj_id, new_obj_id)
    _objs_written_by_mp[new_obj_id] = objid_to_filename[new_obj_id]

    if __debug__:
        logger.debug("Deleting obj %s (new one is %s)" % (str(obj_id), str(new_obj_id)))
    compss.delete_file(objid_to_filename[obj_id])
    objid_to_filename.pop(obj_id)
    pending_to_synchronize.pop(obj_id)

    if __debug__:
        logger.debug("Now object with id %s and %s has mapping %s" % (new_obj_id, type(new_obj), file_name))

    return new_obj


def process_task(f, module_name, class_name, ftype, f_parameters, f_returns, task_kwargs, num_nodes, replicated,
                 distributed):
    """
    Function that submits a task to the runtime.

    :param f: Function or method
    :param module_name: Name of the module containing the function/method (including packages, if any)
    :param class_name: Name of the class (if method)
    :param ftype: Function type
    :param f_parameters: Function parameters (dictionary {'param1':Parameter()}
    :param f_returns: Function returns (dictionary {'*return_X':Parameter()}
    :param task_kwargs: Decorator arguments
    :param num_nodes: Number of nodes that the task must use
    :param replicated: Boolean indicating if the task must be replicated or not
    :param distributed: Boolean indicating if the task must be distributed or not
    :return: The future object related to the task return
    """

    if __debug__:
        logger.debug("TASK: %s of type %s, in module %s, in class %s" % (f.__name__, ftype, module_name, class_name))

    app_id = 0

    print('F RETURNS IS %s' % str(f_returns))

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
    values, names, compss_types, compss_directions, compss_streams, compss_prefixes = \
        _build_values_types_directions(ftype, f_parameters, f_returns, f.__code_strings__)

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
            values_str = ''
            types_str = ''
            direct_str = ''
            streams_str = ''
            prefixes_str = ''
            for v in values:
                values_str += str(v) + ' '
            for t in compss_types:
                types_str += str(t) + ' '
            for d in compss_directions:
                direct_str += str(d) + ' '
            for s in compss_streams:
                streams_str += str(s) + ' '
            for p in compss_prefixes:
                prefixes_str += str(p) + ' '
            # TODO: CHANGE FOR LOOPS FOR JOIN AS BELOW
            names_str = ' '.join(x for x in names)
            logger.debug("Processing task:")
            logger.debug("\t- App id: " + str(app_id))
            logger.debug("\t- Path: " + path)
            logger.debug("\t- Function name: " + f.__name__)
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

    # Check that there is the same amount of values as their types, as well as their directions, streams and prefixes.
    assert (len(values) == len(compss_types) == len(compss_directions) == len(compss_streams) == len(compss_prefixes))

    '''
    Submit task to the runtime (call to the C extension):
    Parameters:
        0 - <Integer>   - application id (by default always 0 due to it is not currently needed for the signature)
        1 - <String>    - path of the module where the task is
        2 - <String>    - function name of the task (to be called from the worker)
        3 - <String>    - priority flag (true|false)
        4 - <String>    - has target (true|false). If the task is within an object or not.
        5 - [<String>]  - task parameters (basic types or file paths for objects)
        6 - [<Integer>] - parameters types (number corresponding to the type of each parameter)
        7 - [<Integer>] - parameters directions (number corresponding to the direction of each parameter)
        8 - [<Integer>] - parameters streams (number corresponding to the stream of each parameter)
        9 - [<String>]  - parameters prefixes (sting corresponding to the prefix of each parameter)
    '''

    compss.process_task(app_id,
                        signature,
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
                        compss_prefixes)

    # Return the future object/s corresponding to the task
    # This object will substitute the user expected return from the task and will be used later for synchronization
    # or as a task parameter (then the runtime will take care of the dependency.
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
    else:
        return DIRECTION.IN


# ##############################################################################
# ####################### AUXILIARY FUNCTIONS ##################################
# ##############################################################################


def _build_return_objects(f_returns):
    """
    Build the return object from the f_returns dictionary and include their filename in f_returns.

    WARNING: Updates f_returns dictionary

    :param f_returns: Dictionary which contains the return objects and Parameters.
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
        print('RET VALUE IS %s' % ret_value)
        if type(ret_value) in _python_to_compss or ret_value in _python_to_compss:  # primitives, string, dic, list, tuple
            fo = Future()
        elif inspect.isclass(ret_value):
            # For objects:
            # type of future has to be specified to allow o = func; o.func
            try:
                fo = ret_value()
            except TypeError:
                if __debug__:
                    logger.warning(
                        "Type {0} does not have an empty constructor, building generic future object".format(ret_value))
                fo = Future()
        else:
            fo = Future()  # modules, functions, methods
        obj_id = get_object_id(fo, True)
        if __debug__:
            logger.debug("Setting object %s of %s as a future" % (obj_id, type(fo)))
        ret_filename = temp_dir + _temp_obj_prefix + str(obj_id)
        objid_to_filename[obj_id] = ret_filename
        pending_to_synchronize[obj_id] = fo
        f_returns[parameter.get_return_name(0)] = Parameter(p_type=TYPE.FILE, p_direction=DIRECTION.OUT, p_prefix="#")
        f_returns[parameter.get_return_name(0)].file_name = ret_filename
    else:
        # Multireturn
        fo = []
        if __debug__:
            logger.debug("Multiple objects return found.")
        for k, v in f_returns.items():
            # Build the appropriate future object
            if v.object in _python_to_compss:  # primitives, string, dic, list, tuple
                foe = Future()
            elif inspect.isclass(v.object):
                # For objects:
                # type of future has to be specified to allow o = func; o.func
                try:
                    foe = v.object()
                except TypeError:
                    if __debug__:
                        logger.warning(
                            "Type {0} does not have an empty constructor, building generic future object".format(
                                v['Value']))
                    foe = Future()
            else:
                foe = Future()  # modules, functions, methods
            fo.append(foe)
            obj_id = get_object_id(foe, True)
            if __debug__:
                logger.debug("Setting object %s of %s as a future" % (obj_id, type(foe)))
            ret_filename = temp_dir + _temp_obj_prefix + str(obj_id)
            objid_to_filename[obj_id] = ret_filename
            pending_to_synchronize[obj_id] = foe
            # Once determined the filename where the returns are going to
            # be stored, create a new Parameter object for each return object
            f_returns[k] = Parameter(p_type=TYPE.FILE, p_direction=DIRECTION.OUT, p_prefix="#")
            f_returns[k].file_name = ret_filename
    return fo

def _serialize_objects(f_parameters):
    '''Infer COMPSs types for the task parameters and serialize them.
    WARNING: Updates f_parameters dictionary
    :param f_parameters: <Dictionary> - Function parameters
    :return: Tuple of task_kwargs updated and a dictionary containing if the objects are future elements.
    '''
    max_obj_arg_size = 320000
    for k in f_parameters:
        # Check user annotations concerning this argument
        p = f_parameters[k]
        # Convert small objects to string if object_conversion enabled
        # Check if the object is small in order not to serialize it.
        if object_conversion:
            p, written_bytes = _convert_object_to_string(p, max_obj_arg_size, policy='objectSize')
            max_obj_arg_size -= written_bytes
        # Serialize objects into files
        p = _serialize_object_into_file(k, p)
        # Update k parameter's Parameter object
        f_parameters[k] = p

        if __debug__:
            logger.debug("Final type for parameter %s: %d" % (k, p.type))

def _build_values_types_directions(ftype, f_parameters, f_returns, code_strings):
    '''
    Build the values list, the values types list and the values directions list.
    :param ftype: task function type. If it is an instance method, the first parameter will be put at the end.
    :param f_parameters: <Dictionary> Function parameters
    :param f_returns: <Dictionary> - Function returns
    :param code_strings: <Boolean> Code strings or not
    :return: <List,List,List,List,List> List of values, their types, their directions, their streams and their prefixes
    '''
    values = []
    names = list(f_parameters.keys()) + list(f_returns.keys())
    compss_types = []
    compss_directions = []
    compss_streams = []
    compss_prefixes = []
    # Build the range of elements
    if ftype == FunctionType.INSTANCE_METHOD or ftype == FunctionType.CLASS_METHOD:
        ra = list(f_parameters.keys())
        slf = ra.pop(0)
        slf_name = names.pop(0)
    else:
        ra = list(f_parameters.keys())
    # Fill the values, compss_types, compss_directions, compss_streams and compss_prefixes from function parameters
    for i in ra:
        val, typ, direc, st, pre = _extract_parameter(f_parameters[i], code_strings)
        values.append(val)
        compss_types.append(typ)
        compss_directions.append(direc)
        compss_streams.append(st)
        compss_prefixes.append(pre)
    # Fill the values, compss_types, compss_directions, compss_streams and compss_prefixes from function returns
    for r in f_returns:
        p = f_returns[r]
        values.append(f_returns[r].file_name)
        compss_types.append(p.type)
        compss_directions.append(p.direction)
        compss_streams.append(p.stream)
        compss_prefixes.append(p.prefix)
    if ftype == FunctionType.INSTANCE_METHOD:
        # Fill the values, compss_types, compss_directions, compss_streams and compss_prefixes from self
        # self is always an object
        val, typ, direc, st, pre = _extract_parameter(f_parameters[slf], code_strings)
        values.append(val)
        compss_types.append(typ)
        compss_directions.append(direc)
        compss_streams.append(st)
        compss_prefixes.append(pre)
        names.insert(len(list(f_parameters.keys())), slf_name)
    return values, names, compss_types, compss_directions, compss_streams, compss_prefixes


def _extract_parameter(parameter, code_strings):
    '''Extract the information of a single parameter

    :param parameter: Parameter object
    :param code_strings: <Boolean> Encode strings
    :return: value, type, direction stream and prefix of the given parameter
    '''
    if parameter.type == TYPE.STRING and not parameter.is_future and code_strings:
        # Encode the string in order to preserve the source
        # Checks that it is not a future (which is indicated with a path)
        # Considers multiple spaces between words
        parameter.object = base64.b64encode(parameter.object.encode()).decode()
        if len(parameter.object) == 0:
            # Empty string - use escape string to avoid padding
            # Checked and substituted by empty string in the worker.py and piper_worker.py
            parameter.object = base64.b64encode(EMPTY_STRING_KEY.encode()).decode()
    # If the Parameter type is file, then the object must have been serialized
    # and the Parameter must have the file_name where the object is.
    if parameter.type == TYPE.FILE or parameter.type == TYPE.OBJECT or parameter.is_future:
        value = parameter.file_name
        typ = TYPE.FILE
    else:
        value = parameter.object
        typ = parameter.type
    # Get direction, stream and prefix
    direction = parameter.direction
    stream = parameter.stream
    prefix = parameter.prefix
    return value, typ, direction, stream, prefix


def _convert_object_to_string(p, max_obj_arg_size, policy='objectSize'):
    '''Convert small objects into string that can fit into the task parameters call
    :param p: Object wrapper
    :param max_obj_arg_size: max size of the object to be converted
    :param policy: policy to use: 'objectSize' for considering the size of the object or 'serializedSize' for
                   considering the size of the object serialized.
    :return: the object possibly converted to string
    '''

    is_future = p.is_future

    num_bytes = 0
    if policy == 'objectSize':
        # Check if the object is small in order to serialize it.
        # This alternative evaluates the size of the object before
        # serializing the object.
        # Warning: calculate the size of a python object can be difficult
        # in terms of time and precision
        if (p.type == TYPE.OBJECT or p.type == TYPE.STRING) and not is_future and p.direction == DIRECTION.IN:
            if not isinstance(p.object, basestring) and isinstance(p.object,
                                                                   (list, dict, tuple, deque, set, frozenset)):
                # check object size
                # bytes = sys.getsizeof(p.object)  # does not work properly with recursive object
                num_bytes = total_sizeof(p.object)
                megabytes = num_bytes / 1000000  # truncate
                if __debug__:
                    logger.debug("Object size %d bytes (%d Mb)." % (num_bytes, megabytes))

                if num_bytes < max_obj_arg_size:  # be careful... more than this value produces:
                    # Cannot run program '/bin/bash'...: error=7, La lista de argumentos es demasiado larga
                    if __debug__:
                        logger.debug("The object size is less than 320 kb.")
                    real_value = p.object
                    try:
                        v = serialize_to_string(p.object)
                        p.object = v.encode(STR_ESCAPE)
                        p.type = TYPE.STRING
                        if __debug__:
                            logger.debug("Inferred type modified (Object converted to String).")
                    except SerializerException:
                        p.object = real_value
                        p.type = TYPE.OBJECT
                        if __debug__:
                            logger.debug("The object cannot be converted due to: not serializable.")
                else:
                    p.type = TYPE.OBJECT
                    if __debug__:
                        logger.debug("Inferred type reestablished to Object.")
                        # if the parameter converts to an object, release the size to be used for converted objects?
                        # No more objects can be converted
                        # max_obj_arg_size += _bytes
                        # if max_obj_arg_size > 320000:
                        #     max_obj_arg_size = 320000
    elif policy == 'serializedSize':
        from cPickle import PicklingError
        # Check if the object is small in order to serialize it.
        # This alternative evaluates the size after serializing the parameter
        if (p.type == TYPE.OBJECT or p.type == TYPE.STRING) and not is_future and p.direction == DIRECTION.IN:
            if not isinstance(p.object, basestring):
                real_value = p.object
                try:
                    v = serialize_to_string(p.object)
                    v = v.encode(STR_ESCAPE)
                    # check object size
                    num_bytes = sys.getsizeof(v)
                    megabytes = num_bytes / 1000000  # truncate
                    if __debug__:
                        logger.debug("Object size %d bytes (%d Mb)." % (num_bytes, megabytes))
                    if num_bytes < max_obj_arg_size:
                        # be careful... more than this value produces:
                        # Cannot run program '/bin/bash'...: error=7,
                        # arguments list too long error.
                        if __debug__:
                            logger.debug("The object size is less than 320 kb.")
                        p.object = v
                        p.type = TYPE.STRING
                        if __debug__:
                            logger.debug("Inferred type modified (Object converted to String).")
                    else:
                        p.object = real_value
                        p.type = TYPE.OBJECT
                        if __debug__:
                            logger.debug("Inferred type reestablished to Object.")
                            # if the parameter converts to an object, release the
                            # size to be used for converted objects?
                            # No more objects can be converted
                            # max_obj_arg_size += _bytes
                            # if max_obj_arg_size > 320000:
                            #     max_obj_arg_size = 320000
                except PicklingError:
                    p.object = real_value
                    p.type = TYPE.OBJECT
                    if __debug__:
                        logger.debug("The object cannot be converted due to: not serializable.")
    else:
        if __debug__:
            logger.debug("[ERROR] Wrong convert_objects_to_strings policy.")
        raise Exception("Wrong convert_objects_to_strings policy.")

    return p, num_bytes


# TODO: Support for collections
# def _serialize_objects(p, is_future):
#     """
#     Serializes recursively the given object into files if necessary.
#
#     :param p: Object wrapper
#     :param is_future: Boolean indicatin whether it is a future object or not
#     :return: p (whose type and value might be modified)
#     """
#
#     # Build the range of elements
#     if ftype == FunctionType.INSTANCE_METHOD:
#         ra = list(range(1, num_pars))
#         ra.append(0)  # callee is the last
#     else:
#         ra = range(first_par, num_pars)
#     # Fill the values, compss_types and compss_directions lists
#     for i in ra:
#         spec_arg = spec_args[i]
#         p = deco_kwargs[spec_arg]
#         if type(p) is dict:
#             # The user has provided some information about a parameter within
#             # the @task parameter
#             p = _from_dict_to_parameter(p)
#         if p.type == TYPE.STRING and not is_future[i] and code_strings:
#             # Encode the string in order to preserve the source
#             # Checks that it is not a future (which is indicated with a path)
#             # Considers multiple spaces between words
#             p.object = base64.b64encode(p.object.encode()).decode()
#             if len(p.object) == 0:
#                 # Empty string - use escape string to avoid padding
#                 # Checked and substituted by empty string in the worker.py and piper_worker.py
#                 p.object = base64.b64encode(EMPTY_STRING_KEY.encode()).decode()
#
#         values.append(p.object)
#         if p.type == TYPE.OBJECT or is_future.get(i):
#             compss_types.append(TYPE.FILE)
#         else:
#             child_p_type = _python_to_compss.get(val_type)
#
#         child_parameter = Parameter(p_type=child_p_type, p_direction=p.direction, p_stream=p.stream, p_prefix=p.prefix)
#         child_parameter.object = child_value
#
#         # Recursively check
#         child_parameter = _serialize_objects(child_parameter, is_future)
#         # Update list entry
#         p.object[index] = child_parameter
#     return p


def _serialize_object_into_file(name, p):
    '''Serialize an object into a file if necessary.
    :param name: Name of the object
    :param p: Object wrapper
    :return: p (whose type and value might be modified)
    '''

    if p.type == TYPE.OBJECT or p.is_future:
        # 2nd condition: real type can be primitive, but now it's acting as a future (object)
        try:
            val_type = type(p.object)
            if isinstance(val_type, list):
                # Is there a future object within the list?
                if any(isinstance(v, Future) for v in p.object):
                    if __debug__:
                        logger.debug("Found a list that contains future objects - synchronizing...")
                    mode = get_compss_mode('in')
                    p.object = list(map(synchronize, p.object, [mode] * len(p.object)))
            _turn_into_file(name, p)
        except SerializerException:
            import sys
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.exception("Pickling error exception: non-serializable object found as a parameter.")
            logger.exception(''.join(line for line in lines))
            print("[ ERROR ]: Non serializable objects can not be used as parameters (e.g. methods).")
            print("[ ERROR ]: Object: %s" % p.object)
            # Raise the exception up tu launch.py in order to point where the error is in the user code.
            raise
    elif p.type == TYPE.EXTERNAL_PSCO:
        _manage_persistent_object(p)
    elif p.type == TYPE.INT:
        if p.object > JAVA_MAX_INT or p.object < JAVA_MIN_INT:
            # This must go through Java as a long to prevent overflow with Java int
            p.type = TYPE.LONG
    elif p.type == TYPE.LONG:
        if p.object > JAVA_MAX_LONG or p.object < JAVA_MIN_LONG:
            # This must be serialized to prevent overflow with Java long
            p.type = TYPE.OBJECT
            _turn_into_file(name, p)
    elif p.type == TYPE.STRING:
        from pycompss.api.task import prepend_strings
        if prepend_strings:
            # Strings can be empty. If a string is empty their base64 encoding will be empty
            # So we add a leading character to it to make it non empty
            p.object = '#%s' % p.object
    return p


def _manage_persistent_object(p):
    '''Does the necessary actions over a persistent object used as task parameter.
    Check if the object has already been used (indexed in the objid_to_filename
    dictionary).
    In particular, saves the object id provided by the persistent storage
    (getID()) into the pending_to_synchronize dictionary.
    :param p: wrapper of the object to manage
    '''
    p.type = TYPE.EXTERNAL_PSCO
    obj_id = get_id(p.object)
    pending_to_synchronize[obj_id] = p.object  # obj_id
    p.object = obj_id
    if __debug__:
        logger.debug("Managed persistent object: %s" % obj_id)


def _turn_into_file(name, p):
    '''Write a object into a file if the object has not been already written (p.object).
    Consults the objid_to_filename to check if it has already been written
    (reuses it if exists). If not, the object is serialized to file and
    registered in the objid_to_filename dictionary.
    This functions stores the object into pending_to_synchronize
    :param p: Wrapper of the object to turn into file
    :return: None
    '''

    # print('XXXXXXXXXXXXXXXXX')
    # print('p           : ', p)
    # print('p.object    : ', p.object)
    # print('p.type      : ', p.type)
    # print('p.direction : ', p.direction)
    # print('XXXXXXXXXXXXXXXXX')
    # if p.direction == DIRECTION.OUT:
    #     # If the parameter is out, infer the type and create an empty instance
    #     # of the same type as the original parameter:
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
        serialize_to_file(p.object, file_name)
    elif obj_id in _objs_written_by_mp:
        if p.direction == DIRECTION.INOUT:
            pending_to_synchronize[obj_id] = p.object
        # Main program generated the last version
        compss_file = _objs_written_by_mp.pop(obj_id)
        if __debug__:
            logger.debug("Serializing object %s to file %s" % (obj_id, compss_file))
        serialize_to_file(p.object, compss_file)
    else:
        pass
    # Set file name in Parameter object
    p.file_name = file_name

def _clean_objects():
    '''Clean the objects stored in the global dictionaries:
        * pending_to_synchronize dict
        * _id2obj dict
        * objid_to_filename dict
        * _objs_written_by_mp dict

    :return: None
    '''

    for filename in objid_to_filename.values():
        compss.delete_file(filename)
    pending_to_synchronize.clear()
    _id2obj.clear()
    objid_to_filename.clear()
    _objs_written_by_mp.clear()


def _clean_temps():
    '''Clean temporary files.
    The temporary files end with the IT extension
    :return: None
    '''
    rmtree(temp_dir, True)
    cwd = os.getcwd()
    for f in os.listdir(cwd):
        if re.search('d\d+v\d+_\d+\.IT', f):
            os.remove(os.path.join(cwd, f))
