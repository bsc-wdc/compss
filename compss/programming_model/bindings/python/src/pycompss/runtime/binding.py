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

import os
import re
import logging
from shutil import rmtree

import pycompss.runtime.management.COMPSs as COMPSs
from pycompss.runtime.commons import get_temporary_directory
from pycompss.runtime.management.object_tracker import OT
from pycompss.runtime.management.synchronization import wait_on_object
from pycompss.runtime.management.direction import get_compss_direction
from pycompss.runtime.management.classes import EmptyReturn
from pycompss.runtime.task.core_element import CE
from pycompss.runtime.commons import LIST_TYPE
import pycompss.util.context as context

if __debug__:
    # Setup logger
    logger = logging.getLogger(__name__)


# ########################################################################### #
# ############ FUNCTIONS THAT COMMUNICATE WITH THE RUNTIME ################## #
# ########################################################################### #

def start_runtime(log_level='off', interactive=False):
    # type: (str, bool) -> None
    """
    Starts the runtime by calling the external python library that calls
    the bindings-common.

    :param log_level: Log level [ 'trace' | 'debug' | 'info' | 'api' | 'off' ]
    :param interactive: Boolean if interactive (ipython or jupyter)
    :return: None
    """
    if __debug__:
        logger.info("Starting COMPSs...")

    if interactive and context.in_master():
        COMPSs.load_runtime(external_process=True)
    else:
        COMPSs.load_runtime(external_process=False)

    if log_level == 'trace':
        # Could also be 'debug' or True, but we only show the C extension
        # debug in the maximum tracing level.
        COMPSs.set_debug(True)

    COMPSs.start_runtime()
    if __debug__:
        logger.info("COMPSs started")


def stop_runtime(code=0):
    # type: (bool) -> None
    """
    Stops the runtime by calling the external python library that calls
    the bindings-common.
    Also cleans objects and temporary files created during runtime.
    If the code is different from 0, all running or waiting tasks will be
    cancelled.

    :parameter code: Stop code (if code != 0 ==> cancel application tasks)
    :return: None
    """
    if __debug__:
        logger.info("Stopping runtime...")

    if code != 0:
        if __debug__:
            logger.info("Canceling all application tasks...")
        COMPSs.cancel_application_tasks(0)

    if __debug__:
        logger.info("Cleaning objects...")
    _clean_objects()

    if __debug__:
        logger.info("Stopping COMPSs...")
    COMPSs.stop_runtime(code)

    if __debug__:
        logger.info("Cleaning temps...")
    _clean_temps()

    if __debug__:
        logger.info("COMPSs stopped")


def accessed_file(file_name):
    # type: (str) -> bool
    """
    Calls the external python library (that calls the bindings-common)
    in order to check if a file has been accessed.

    :param file_name: <String> File name.
    :return: True if accessed, False otherwise.
    """
    app_id = 0
    if __debug__:
        logger.debug("Checking if file %s has been accessed." % file_name)
    return COMPSs.accessed_file(app_id, file_name)


def open_file(file_name, mode):
    # type: (str, str) -> str
    """
    Calls the external python library (that calls the bindings-common)
    in order to request a file.

    :param file_name: <String> File name.
    :param mode: Open file mode ('r', 'rw', etc.).
    :return: The current name of the file requested (that may have been
             renamed during runtime)
    """
    app_id = 0
    compss_mode = get_compss_direction(mode)
    if __debug__:
        logger.debug("Getting file %s with mode %s" % (file_name, compss_mode))
    compss_name = COMPSs.open_file(app_id, file_name, compss_mode)
    if __debug__:
        logger.debug("COMPSs file name is %s" % compss_name)
    return compss_name


def delete_file(file_name):
    # type: (str) -> bool
    """
    Calls the external python library (that calls the bindings-common)
    in order to request a file removal.

    :param file_name: File name to remove
    :return: True if success. False otherwise
    """
    app_id = 0
    if __debug__:
        logger.debug("Deleting file %s" % file_name)
    result = COMPSs.delete_file(app_id, file_name, True) == 'true'
    if __debug__:
        if result:
            logger.debug("File %s successfully deleted." % file_name)
        else:
            logger.error("Failed to remove file %s." % file_name)
    return result


def get_file(file_name):
    # type: (str) -> None
    """
    Calls the external python library (that calls the bindings-common)
    in order to request last version of file.

    :param file_name: File name to remove
    :return: None
    """
    app_id = 0
    if __debug__:
        logger.debug("Getting file %s" % file_name)
    COMPSs.get_file(app_id, file_name)


def get_directory(dir_name):
    # type: (str) -> None
    """
    Calls the external python library (that calls the bindings-common)
    in order to request last version of file.

    :param dir_name: dir name to retrieve
    :return: None
    """
    app_id = 0
    if __debug__:
        logger.debug("Getting directory %s" % dir_name)
    COMPSs.get_directory(app_id, dir_name)


def delete_object(obj):
    # type: (object) -> bool
    """
    Removes a used object from the internal structures and calls the
    external python library (that calls the bindings-common)
    in order to request a its corresponding file removal.

    :param obj: Object to remove
    :return: True if success. False otherwise
    """
    app_id = 0
    obj_id = OT.get_object_id(obj, False, False)
    if obj_id is None:
        OT.pop_object_id(obj)
        return False
    try:
        OT.pop_object_id(obj)
    except KeyError:
        pass
    try:
        file_name = OT.get_filename(obj_id)
        COMPSs.delete_file(app_id, file_name, False)
    except KeyError:
        pass
    try:
        OT.pop_filename(obj_id)
    except KeyError:
        pass
    try:
        OT.pop_pending_to_synchronize(obj_id)
    except KeyError:
        pass
    return True


def barrier(no_more_tasks=False):
    # type: (bool) -> None
    """
    Calls the external python library (that calls the bindings-common)
    in order to request a barrier.
    Wait for all tasks.

    :param no_more_tasks: No more tasks
    :return: None
    """
    if __debug__:
        logger.debug("Barrier. No more tasks? %s" % str(no_more_tasks))
    # If noMoreFlags is set, clean up the objects
    if no_more_tasks:
        _clean_objects()

    app_id = 0
    # Call the Runtime barrier (appId 0, not needed for the signature)
    COMPSs.barrier(app_id, no_more_tasks)


def barrier_group(group_name):
    # type: (str) -> str
    """
    Calls the external python library (that calls the bindings-common)
    in order to request a barrier of a group.
    Wait for all tasks of the group.

    :param group_name: Group name
    :return: None or string with exception message
    """
    app_id = 0
    # Call the Runtime group barrier
    return COMPSs.barrier_group(app_id, group_name)


def open_task_group(group_name, implicit_barrier):
    # type: (str, bool) -> None
    """
    Calls the external python library (that calls the bindings-common)
    in order to request an opening of a group.

    :param group_name: Group name
    :param implicit_barrier: <Boolean> Implicit barrier
    :return: None
    """
    app_id = 0
    COMPSs.open_task_group(group_name, implicit_barrier, app_id)


def close_task_group(group_name):
    # type: (str) -> None
    """
    Calls the external python library (that calls the bindings-common)
    in order to request a group closure.

    :param group_name: Group name
    :return: None
    """
    app_id = 0
    COMPSs.close_task_group(group_name, app_id)


def get_log_path():
    # type: () -> str
    """
    Requests the logging path to the external python library (that calls
    the bindings-common).

    :return: The path where to store the logs.
    """
    if __debug__:
        logger.debug("Requesting log path")
    log_path = COMPSs.get_logging_path()
    if __debug__:
        logger.debug("Log path received: %s" % log_path)
    return log_path


def get_number_of_resources():
    # type: () -> int
    """
    Calls the external python library (that calls the bindings-common)
    in order to request for the number of active resources.

    :return: Number of active resources
    """
    app_id = 0
    if __debug__:
        logger.debug("Request the number of active resources")

    # Call the Runtime
    return COMPSs.get_number_of_resources(app_id)


def request_resources(num_resources, group_name):
    # type: (int, str) -> None
    """
    Calls the external python library (that calls the bindings-common)
    in order to request for the creation of the given resources.

    :param num_resources: Number of resources to create.
    :param group_name: Task group to notify upon resource creation
    :return: None
    """
    app_id = 0
    if group_name is None:
        group_name = "NULL"
    if __debug__:
        logger.debug("Request the creation of " +
                     str(num_resources) +
                     " resources with notification to task group " +
                     str(group_name))

    # Call the Runtime
    COMPSs.request_resources(app_id, num_resources, group_name)


def free_resources(num_resources, group_name):
    # type: (int, str) -> None
    """
    Calls the external python library (that calls the bindings-common)
    in order to request for the destruction of the given resources.

    :param num_resources: Number of resources to destroy.
    :param group_name: Task group to notify upon resource creation
    :return: None
    """
    app_id = 0

    if group_name is None:
        group_name = "NULL"
    if __debug__:
        logger.debug("Request the destruction of " +
                     str(num_resources) +
                     " resources with notification to task group " +
                     str(group_name))

    # Call the Runtime
    COMPSs.free_resources(app_id, num_resources, group_name)


def register_ce(core_element):
    # type: (CE) -> None
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
    impl_io: <String> IO Implementation
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
    COMPSs.register_core_element(ce_signature,
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
    ret = list(map(wait_on_object, args,
                   [kwargs.get("mode", "rw")] * len(args)))
    ret = ret[0] if len(ret) == 1 else ret
    # Check if there are empty elements return elements that need to be removed
    if isinstance(ret, LIST_TYPE):
        # Look backwards the list removing the first EmptyReturn elements
        for elem in reversed(ret):
            if isinstance(elem, EmptyReturn):
                ret.remove(elem)
    return ret


def process_task(function_name,         # type: str
                 path,                  # type: str
                 has_target,            # type: bool
                 module_name,           # type: str
                 class_name,            # type: str
                 f_type,                # type: int
                 values,                # type: list
                 names,                 # type: list
                 num_returns,           # type: int
                 compss_types,          # type: list
                 compss_directions,     # type: list
                 compss_streams,        # type: list
                 compss_prefixes,       # type: list
                 content_types,         # type: list
                 weights,               # type: list
                 keep_renames,          # type: list
                 has_priority,          # type: list
                 signature,             # type: list
                 num_nodes,             # type: list
                 replicated,            # type: list
                 distributed,           # type: list
                 on_failure,            # type: list
                 time_out,              # type: int
                 ):
    # type: (...) -> None
    """
    Function that submits a task to the runtime.

    :param function_name: Function name
    :param path: Module path
    :param has_target: Boolean if the task has self
    :param module_name: Name of the module containing the function/method
                        (including packages, if any)
    :param class_name: Name of the class (if method)
    :param f_type: Function type
    :param values: Task parameter values
    :param names: Task parameter names
    :param num_returns: Number of returns
    :param compss_types: List of parameter types
    :param compss_directions: List of parameter directions
    :param compss_streams: List of parameter streams
    :param compss_prefixes: List of parameter prefixes
    :param content_types: Content types
    :param weights: List of parameter weights
    :param keep_renames: Boolean keep renaming
    :param has_priority: Boolean has priority
    :param signature: Task signature
    :param num_nodes: Number of nodes that the task must use
    :param replicated: Boolean indicating if the task must be replicated
    :param distributed: Boolean indicating if the task must be distributed
    :param on_failure: Action on failure
    :param time_out: Time for a task time out
    :return: The future object related to the task return
    """
    if __debug__:
        logger.debug("TASK: %s of type %s, in module %s, in class %s" %
                     (function_name, f_type, module_name, class_name))

    app_id = 0

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
            logger.debug("\t- Function name: " + function_name)
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
    #     10 - [<String>] - parameters prefixes (string corresponding to the
    #                       prefix of each parameter)
    #     11 - [<String>] - parameters extra type (string corresponding to the
    #                       extra type of each parameter)
    #     12 - [<String>] - parameters weights (string corresponding to the
    #                       weight of each parameter
    #     13 - <String>   - Keep renames flag (true|false)
    #

    COMPSs.process_task(app_id,
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


# ########################################################################### #
# ####################### AUXILIARY FUNCTIONS ############################### #
# ########################################################################### #

def _clean_objects():
    # type: () -> None
    """
    Clean the objects stored in the global dictionaries:
        * pending_to_synchronize dict
        * _addr2id2obj dict
        * obj_id_to_filename dict
        * _objs_written_by_mp dict

    :return: None
    """
    app_id = 0
    for filename in OT.get_filenames():
        COMPSs.delete_file(app_id, filename, False)
    OT.clean_object_tracker()


def _clean_temps():
    # type: () -> None
    """
    Clean temporary files.
    The temporary files end with the IT extension

    :return: None
    """
    temp_directory = get_temporary_directory()
    rmtree(temp_directory, True)
    cwd = os.getcwd()
    for f in os.listdir(cwd):
        if re.search(r'd\d+v\d+_\d+\.IT', f):
            os.remove(os.path.join(cwd, f))
