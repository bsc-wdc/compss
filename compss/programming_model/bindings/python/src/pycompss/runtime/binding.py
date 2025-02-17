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
PyCOMPSs Binding - Binding core module.

This file contains the Python binding core classes and methods.
"""

import os
import re
import signal
from shutil import rmtree

from pycompss.runtime.management.COMPSs import COMPSs
from pycompss.util.context import CONTEXT
from pycompss.runtime.commons import GLOBALS
from pycompss.runtime.management.classes import EmptyReturn
from pycompss.runtime.management.direction import get_compss_direction
from pycompss.runtime.management.object_tracker import OT
from pycompss.runtime.management.synchronization import wait_on_object
from pycompss.runtime.task.definitions.core_element import CE
from pycompss.runtime.task.definitions.arguments import TaskArguments
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.logger.helpers import add_new_logger

# Tracing imports
from pycompss.util.tracing.helpers import enable_trace_master
from pycompss.util.tracing.helpers import EventInsideWorker
from pycompss.util.tracing.helpers import EventMaster
from pycompss.util.tracing.types_events_master import TRACING_MASTER
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.util.typing_helper import typing

if __debug__:
    import logging

    LOGGER = logging.getLogger(__name__)


# ########################################################################### #
# ############ FUNCTIONS THAT COMMUNICATE WITH THE RUNTIME ################## #
# ########################################################################### #


def start_runtime(
    log_level: str = "off",
    tracing: bool = False,
    interactive: bool = False,
    disable_external: bool = False,
) -> None:
    """Start the COMPSs runtime.

    Starts the runtime by calling the external python library that calls
    the bindings-common.

    :param log_level: Log level [ "trace" | "debug" | "info" | "api" | "off" ].
    :param tracing: Tracing level [ True | False ].
    :param interactive: Boolean if interactive (ipython or jupyter).
    :param disable_external: To avoid to load compss in external process.
    :return: None.
    """
    if __debug__:
        LOGGER.info("Starting COMPSs...")

    if tracing and not interactive:
        # Enabled only if not interactive - extrae issues within jupyter.
        enable_trace_master()

    with EventMaster(TRACING_MASTER.start_runtime_event):
        if interactive and CONTEXT.in_master() and not disable_external:
            COMPSs.load_runtime(external_process=True)
        else:
            COMPSs.load_runtime(external_process=False)

        if log_level == "trace":
            # Could also be "debug" or True, but we only show the C extension
            # debug in the maximum tracing level.
            COMPSs.set_debug(True)
            OT.enable_report()

        COMPSs.start_runtime()

    if __debug__:
        LOGGER.info("COMPSs started")


def stop_runtime(code: int = 0, hard_stop: bool = False) -> None:
    """Stop the COMPSs runtime.

    Stops the runtime by calling the external python library that calls
    the bindings-common.
    Also cleans objects and temporary files created during runtime.
    If the code is different from 0, all running or waiting tasks will be
    cancelled.

    :parameter code: Stop code (if code != 0 ==> cancel application tasks).
    :param hard_stop: Stop compss when runtime has died.
    :return: None.
    """
    with EventMaster(TRACING_MASTER.stop_runtime_event):
        app_id = 0
        if __debug__:
            LOGGER.info("Stopping runtime...")

        # Stopping a possible wall clock limit
        signal.alarm(0)

        if code != 0:
            if __debug__:
                LOGGER.info("Canceling all application tasks...")
            COMPSs.cancel_application_tasks(app_id, 0)

        if __debug__:
            LOGGER.info("Cleaning objects...")
        _clean_objects(hard_stop=hard_stop)

        if __debug__:
            reporting = OT.is_report_enabled()
            if reporting:
                LOGGER.info("Generating Object tracker report...")
                target_path = get_log_path()
                OT.generate_report(target_path)
                OT.clean_report()

        if __debug__:
            LOGGER.info("Stopping COMPSs...")
        COMPSs.stop_runtime(code)

        if __debug__:
            LOGGER.info("Cleaning temps...")
        _clean_temps()

        CONTEXT.set_out_of_scope()
        if __debug__:
            LOGGER.info("COMPSs stopped")


def file_exists(*file_name: typing.Union[list, tuple, str]) -> typing.Any:
    """Check if one or more files exists (has/have been accessed).

    :param file_name: File/s name.
    :return: True if accessed, False otherwise.
    """
    if __debug__:
        LOGGER.debug(
            "Checking if file/s: %s has/have been accessed.", file_name
        )
    return __apply_recursively_to_file(
        __file_exists, TRACING_MASTER.accessed_file_event, True, *file_name
    )


def __file_exists(app_id: int, file_name: str) -> bool:
    """Check if one files exists (has been accessed).

    Calls the external python library (that calls the bindings-common)
    in order to check if a file has been accessed.

    :param app_id: Application identifier.
    :param file_name: <String> File name.
    :return: True if accessed, False otherwise.
    """
    if os.path.exists(file_name):
        return True
    return COMPSs.accessed_file(app_id, file_name)


def open_file(file_name: str, mode: str) -> str:
    """Open a file (retrieves if necessary).

    Calls the external python library (that calls the bindings-common)
    in order to request a file.

    :param file_name: <String> File name.
    :param mode: Open file mode ('r', 'rw', etc.).
    :return: The current name of the file requested (that may have been
             renamed during runtime).
    """
    with EventMaster(TRACING_MASTER.open_file_event):
        app_id = 0
        compss_mode = get_compss_direction(mode)
        if __debug__:
            LOGGER.debug(
                "Getting file %s with mode %s", file_name, compss_mode
            )
        compss_name = COMPSs.open_file(app_id, file_name, compss_mode)
        if __debug__:
            LOGGER.debug("COMPSs file name is %s", compss_name)
        return compss_name


def delete_file(*file_name: typing.Union[list, tuple, str]) -> typing.Any:
    """Remove one or more files.

    :param file_name: File/s name to remove.
    :return: True if success. False otherwise. With the same file_name
             structure.
    """
    if __debug__:
        LOGGER.debug("Deleting file/s: %s", file_name)
    return __apply_recursively_to_file(
        __delete_file, TRACING_MASTER.delete_file_event, True, *file_name
    )


def __delete_file(app_id: int, file_name: str) -> bool:
    """Remove one or more files.

    Calls the external python library (that calls the bindings-common)
    in order to request a file removal.

    :param app_id: Application identifier.
    :param file_name: File/s name to remove.
    :return: True if success. False otherwise. With the same file_name
             structure.
    """
    result = COMPSs.delete_file(app_id, file_name, True)
    if __debug__:
        if result:
            LOGGER.debug("File %s successfully deleted.", file_name)
        else:
            LOGGER.error("Failed to remove file %s.", file_name)
    return result


def wait_on_file(*file_name: typing.Union[list, tuple, str]) -> typing.Any:
    """Retrieve one or more files.

    :param file_name: File name/s to retrieve (can contain lists and tuples
                      of strings).
    :return: The file name/s (with the same structure).
    """
    if __debug__:
        LOGGER.debug("Getting file/s: %s", file_name)
    return __apply_recursively_to_file(
        COMPSs.get_file, TRACING_MASTER.get_file_event, False, *file_name
    )


def wait_on_directory(
    *directory_name: typing.Union[list, tuple, str]
) -> typing.Any:
    """Retrieve one or more directories.

    :param directory_name: Directory name/s to retrieve (can contain lists
                           and tuples of strings).
    :return: The directory name/s (with the same structure).
    """
    if __debug__:
        LOGGER.debug("Getting directory/s: %s", directory_name)
    return __apply_recursively_to_file(
        COMPSs.get_directory,
        TRACING_MASTER.get_directory_event,
        False,
        *directory_name,
    )


def __apply_recursively_to_file(
    function: typing.Callable,
    event: int,
    get_results: bool,
    *name: typing.Union[list, tuple, str],
) -> typing.Any:
    """Apply the given function recursively over the given names.

    Calls the external python library (that calls the bindings-common)
    in order to request last version of a file.
    Iterates recursively over file_name lists and tuples.
    Emits an event per name processed.

    :param function: Function to apply.
    :param event: Event to emit.
    :param get_results: If get the results of the function, or the given name.
    :param name: File/s or directory/ies name to apply the given function.
    :return: The result of applying the given function.
    """
    app_id = 0
    ret = []  # type: typing.List[typing.Union[list, tuple, str, bool]]
    for f_name in name:
        if isinstance(f_name, str):
            with EventMaster(event):
                result = function(app_id, f_name)
            if get_results:
                ret.append(result)
            else:
                ret.append(f_name)
        elif isinstance(f_name, list):
            files_list = list(
                [
                    __apply_recursively_to_file(
                        function, event, get_results, name
                    )
                    for name in f_name
                ]
            )
            ret.append(files_list)
        elif isinstance(f_name, tuple):
            files_tuple = tuple(
                [
                    __apply_recursively_to_file(
                        function, event, get_results, name
                    )
                    for name in f_name
                ]
            )
            ret.append(files_tuple)
        else:
            raise PyCOMPSsException(
                "Unsupported type in apply_recursively. "
                "Must be str, list or tuple"
            )
    if len(ret) == 1:
        return ret[0]
    return ret


def delete_object(
    *objs: typing.Any,
) -> typing.Union[bool, typing.List[typing.Union[bool, list]]]:
    """Remove object/s.

    :param objs: Object/s to remove.
    :return: True if success. False otherwise. Keeps structure if lists or
             tuples are provided.
    """
    if __debug__:
        LOGGER.debug("Deleting object/s: %r", objs)
    app_id = 0
    ret = []  # type: typing.List[typing.Union[bool, list]]
    for obj in objs:
        with EventMaster(TRACING_MASTER.delete_object_event):
            result = __delete_object(app_id, obj)
        ret.append(result)
    if len(ret) == 1:
        return ret[0]
    return ret


def __delete_object(app_id: int, obj: typing.Any) -> bool:
    """Remove object function.

    Removes a used object from the internal structures and calls the
    external python library (that calls the bindings-common)
    in order to request its corresponding file removal.

    :param app_id: Application identifier.
    :param obj: Object to remove.
    :return: True if success. False otherwise.
    """
    obj_id = OT.is_tracked(obj)
    if obj_id is None:
        # Not being tracked
        return False
    try:
        file_name = OT.get_file_name(obj_id)
        COMPSs.delete_file(app_id, file_name, False)
        OT.stop_tracking(obj)
    except KeyError:
        pass
    return True


def barrier(no_more_tasks: bool = False) -> None:
    """Wait for all submitted tasks.

    Calls the external python library (that calls the bindings-common)
    in order to request a barrier.

    :param no_more_tasks: If no more tasks are going to be submitted, remove
                          all objects.
    :return: None.
    """
    with EventMaster(TRACING_MASTER.barrier_event):
        if __debug__:
            LOGGER.debug("Barrier. No more tasks? %s", str(no_more_tasks))
        # If noMoreFlags is set, clean up the objects
        if no_more_tasks:
            _clean_objects()
        app_id = 0
        # Call the Runtime barrier (appId 0, not needed for the signature)
        COMPSs.barrier(app_id, no_more_tasks)


def barrier_group(group_name: str) -> str:
    """Wait for all tasks of the given group.

    Calls the external python library (that calls the bindings-common)
    in order to request a barrier of a group.

    :param group_name: Group name.
    :return: None or string with exception message.
    """
    with EventMaster(TRACING_MASTER.barrier_group_event):
        app_id = 0
        # Call the Runtime group barrier
        return str(COMPSs.barrier_group(app_id, group_name))


def open_task_group(group_name: str, implicit_barrier: bool) -> None:
    """Open task group.

    Calls the external python library (that calls the bindings-common)
    in order to request an opening of a group.

    :param group_name: Group name.
    :param implicit_barrier: Perform a wait on all group tasks before closing.
    :return: None.
    """
    with EventMaster(TRACING_MASTER.open_task_group_event):
        app_id = 0
        COMPSs.open_task_group(group_name, implicit_barrier, app_id)


def close_task_group(group_name: str) -> None:
    """Close task group.

    Calls the external python library (that calls the bindings-common)
    in order to request a group closure.

    :param group_name: Group name.
    :return: None.
    """
    with EventMaster(TRACING_MASTER.close_task_group_event):
        app_id = 0
        COMPSs.close_task_group(group_name, app_id)


def cancel_task_group(group_name: str) -> str:
    """Cancel task group.

    Calls the external python library (that calls the bindings-common)
    in order to request a group cancellation.

    :param group_name: Group name.
    :return: None or string with exception message.
    """
    with EventMaster(TRACING_MASTER.cancel_task_group_event):
        app_id = 0
        return str(COMPSs.cancel_task_group(group_name, app_id))


def snapshot() -> None:
    """Make a snapshot of the tasks.

    Calls the external python library (that calls the bindings-common)
    in order to request a snapshot.

    :return: None
    """
    with EventMaster(TRACING_MASTER.snapshot_event):
        app_id = 0
        COMPSs.snapshot(app_id)


def get_log_path() -> str:
    """Get logging path.

    Requests the logging path to the external python library (that calls
    the bindings-common).

    :return: The path where to store the logs.
    """
    with EventMaster(TRACING_MASTER.get_log_path_event):
        if __debug__:
            LOGGER.debug("Requesting log path")
        log_path = COMPSs.get_logging_path()
        if __debug__:
            LOGGER.debug("Log path received: %s", log_path)
        return log_path


def get_tmp_path() -> str:
    """Get tmp path.

    Requests the master working path to the external python library (that
    calls the bindings-common).

    :return: The path where to store the master tmp files.
    """
    with EventMaster(TRACING_MASTER.get_tmp_path_event):
        if __debug__:
            LOGGER.debug("Requesting tmp path (master working dir)")
        tmp_path = COMPSs.get_master_working_path()
        if __debug__:
            LOGGER.debug(
                "Tmp path (master working dir) received: %s", tmp_path
            )
        return tmp_path


def get_number_of_resources() -> int:
    """Get the number of resources.

    Calls the external python library (that calls the bindings-common)
    in order to request for the number of active resources.

    :return: Number of active resources.
    """
    with EventMaster(TRACING_MASTER.get_number_resources_event):
        app_id = 0
        if __debug__:
            LOGGER.debug("Request the number of active resources")
        # Call the Runtime
        return COMPSs.get_number_of_resources(app_id)


def request_resources(
    num_resources: int, group_name: typing.Optional[str]
) -> None:
    """Request new resources.

    Calls the external python library (that calls the bindings-common)
    in order to request for the creation of the given resources.

    :param num_resources: Number of resources to create.
    :param group_name: Task group to notify upon resource creation.
    :return: None.
    """
    with EventMaster(TRACING_MASTER.request_resources_event):
        app_id = 0
        if group_name is None:
            group_name = "NULL"
        if __debug__:
            LOGGER.debug(
                "Request the creation of %s resources with notification to "
                "task group %s",
                str(num_resources),
                str(group_name),
            )
        # Call the Runtime
        COMPSs.request_resources(app_id, num_resources, group_name)


def free_resources(
    num_resources: int, group_name: typing.Optional[str]
) -> None:
    """Liberate resources.

    Calls the external python library (that calls the bindings-common)
    in order to request for the destruction of the given resources.

    :param num_resources: Number of resources to destroy.
    :param group_name: Task group to notify upon resource creation.
    :return: None.
    """
    with EventMaster(TRACING_MASTER.free_resources_event):
        app_id = 0
        if group_name is None:
            group_name = "NULL"
        if __debug__:
            LOGGER.debug(
                "Request the destruction of %s resources with notification to "
                "task group %s",
                str(num_resources),
                str(group_name),
            )
        # Call the Runtime
        COMPSs.free_resources(app_id, num_resources, group_name)


def set_wall_clock(wall_clock_limit: int) -> None:
    """Set the application wall clock limit.

    :param wall_clock_limit: Wall clock limit in seconds.
    :return: None.
    """
    with EventMaster(TRACING_MASTER.wall_clock_limit_event):
        app_id = 0
        if __debug__:
            LOGGER.debug("Set a wall clock limit of %s", str(wall_clock_limit))
        # Activate wall clock limit alarm
        signal.signal(signal.SIGALRM, _wall_clock_exceed)
        signal.alarm(wall_clock_limit)
        # Call the Runtime to set a timer in case wall clock is
        # reached in a synch
        COMPSs.set_wall_clock(app_id, wall_clock_limit)


def register_ce(core_element: CE) -> None:
    """Register a core element.

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
        String[] impl_type_args = new String[] { 'methodClass',
                                                 'methodName' };
        rt.registerCoreElement(coreElementSignature,
                               impl_signature,
                               impl_constraints,
                               impl_type,
                               impl_type_args);

        // MPI
        System.out.println('Registering MPI implementation');
        core_elementSignature = 'methodClass1.methodName1';
        impl_signature = 'mpi.MPI';
        impl_constraints = 'StorageType:SSD';
        impl_type = 'MPI';
        impl_type_args = new String[] { 'mpiBinary',
                                        'mpiWorkingDir',
                                        'mpiRunner' };
        rt.registerCoreElement(coreElementSignature,
                               impl_signature,
                               impl_constraints,
                               impl_type,
                               impl_type_args);

        // PYTHON MPI
        System.out.println('Registering PYTHON MPI implementation');
        core_elementSignature = 'methodClass1.methodName1';
        impl_signature = 'MPI.methodClass1.methodName';
        impl_constraints = 'ComputingUnits:2';
        impl_type = 'PYTHON_MPI';
        impl_type_args = new String[] { 'methodClass',
                                        'methodName',
                                        'mpiWorkingDir',
                                        'mpiRunner' };
        rt.registerCoreElement(coreElementSignature,
                               impl_signature,
                               impl_constraints,
                               impl_type,
                               impl_type_args);

        // BINARY
        System.out.println('Registering BINARY implementation');
        core_elementSignature = 'methodClass2.methodName2';
        impl_signature = 'binary.BINARY';
        impl_constraints = 'MemoryType:RAM';
        impl_type = 'BINARY';
        impl_type_args = new String[] { 'binary',
                                        'binaryWorkingDir' };
        rt.registerCoreElement(coreElementSignature,
                               impl_signature,
                               impl_constraints,
                               impl_type,
                               impl_type_args);

        // OMPSS
        System.out.println('Registering OMPSS implementation');
        core_elementSignature = 'methodClass3.methodName3';
        impl_signature = 'ompss.OMPSS';
        impl_constraints = 'ComputingUnits:3';
        impl_type = 'OMPSS';
        impl_type_args = new String[] { 'ompssBinary',
                                        'ompssWorkingDir' };
        rt.registerCoreElement(coreElementSignature,
                               impl_signature,
                               impl_constraints,
                               impl_type,
                               impl_type_args);

        // OPENCL
        System.out.println('Registering OPENCL implementation');
        core_elementSignature = 'methodClass4.methodName4';
        impl_signature = 'opencl.OPENCL';
        impl_constraints = 'ComputingUnits:4';
        impl_type = 'OPENCL';
        impl_type_args = new String[] { 'openclKernel',
                                        'openclWorkingDir' };
        rt.registerCoreElement(coreElementSignature,
                               impl_signature,
                               impl_constraints,
                               impl_type,
                               impl_type_args);

        // VERSIONING
        System.out.println('Registering METHOD implementation');
        core_elementSignature = 'methodClass.methodName';
        impl_signature = 'anotherClass.anotherMethodName';
        impl_constraints = 'ComputingUnits:1';
        impl_type = 'METHOD';
        impl_type_args = new String[] { 'anotherClass',
                                        'anotherMethodName' };
        rt.registerCoreElement(coreElementSignature,
                               impl_signature,
                               impl_constraints,
                               impl_type,
                               impl_type_args);

    ---------------------

    Core Element fields:

    ce_signature: <String> Core Element signature
                  (e.g.- "methodClass.methodName")
    impl_signature: <String> Implementation signature
                    (e.g.- "methodClass.methodName")
    impl_constraints: <Dict> Implementation constraints
                      (e.g.- "{ComputingUnits:2}")
    impl_type: <String> Implementation type
               ("METHOD" | "MPI" | "BINARY" | "OMPSS" | "OPENCL")
    impl_io: <String> IO Implementation
    impl_type_args: <List(Strings)> Implementation arguments
                    (e.g.- ["methodClass", "methodName"])

    :param core_element: <CE> Core Element to register.
    :return: None.
    """
    with EventMaster(TRACING_MASTER.register_core_element_event):
        # Retrieve Core element fields
        ce_signature = core_element.get_ce_signature()
        impl_signature_base = core_element.get_impl_signature()
        impl_signature = (
            None if impl_signature_base == "" else impl_signature_base
        )
        impl_constraints_base = core_element.get_impl_constraints()
        impl_constraints = None  # type: typing.Any
        if impl_constraints_base == "":
            impl_constraints = {}
        else:
            impl_constraints = impl_constraints_base
        impl_type_base = core_element.get_impl_type()
        impl_type = None if impl_type_base == "" else str(impl_type_base)
        impl_local = str(core_element.get_impl_local())
        impl_io = str(core_element.get_impl_io())
        impl_type_args = core_element.get_impl_type_args()
        prolog = core_element.get_impl_prolog()
        epilog = core_element.get_impl_epilog()
        container = core_element.get_impl_container()

        if __debug__:
            LOGGER.debug("Registering CE with signature: %s", ce_signature)
            LOGGER.debug("\t - Implementation signature: %s", impl_signature)

        # Build constraints string from constraints dictionary
        impl_constraints_lst = []
        for key, value in impl_constraints.items():
            if isinstance(value, int):
                val = str(value)
            elif isinstance(value, str):
                val = value
            elif isinstance(value, list):
                val = str(value).replace("'", "")
            else:
                raise PyCOMPSsException(
                    "Implementation constraints items must be "
                    "str, int or list."
                )
            kv_constraint = "".join((key, ":", str(val), ";"))
            impl_constraints_lst.append(kv_constraint)
        impl_constraints_str = "".join(impl_constraints_lst)

        if __debug__:
            LOGGER.debug(
                "\t - Implementation constraints: %s", impl_constraints_str
            )
            LOGGER.debug("\t - Implementation type: %s", impl_type)
            LOGGER.debug(
                "\t - Implementation type arguments: %s",
                " ".join(impl_type_args),
            )
        # import pdb; pdb.set_trace()
        # Call runtime with the appropriate parameters
        COMPSs.register_core_element(
            ce_signature,
            impl_signature,
            impl_constraints_str,
            impl_type,
            impl_local,
            impl_io,
            prolog,
            epilog,
            container,
            impl_type_args,
        )
        if __debug__:
            LOGGER.debug("CE with signature %s registered.", ce_signature)


def wait_on(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
    """Wait on a set of objects.

    Waits on a set of objects defined in args with the options defined in
    kwargs.

    :param args: Objects to wait on.
    :param kwargs: Options: Write enable? [True | False] Default = True.
        May include: master_event: Emit master event. [Default: True | False]
                                   False will emit the event inside task
                                   (for nested).
    :return: Real value of the objects requested.
    """
    master_event = True
    if "master_event" in kwargs:  # pylint: disable=consider-using-get
        master_event = kwargs["master_event"]
    if master_event:
        with EventMaster(TRACING_MASTER.wait_on_event):
            return __wait_on(*args, **kwargs)
    else:
        with EventInsideWorker(TRACING_WORKER.wait_on_event):
            return __wait_on(*args, **kwargs)


def __wait_on(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
    """Wait on a set of objects.

    Waits on a set of objects defined in args with the options defined in
    kwargs.

    :param args: Objects to wait on.
    :param kwargs: Options: Write enable? [True | False] Default = True.
    :return: Real value of the objects requested.
    """
    ret = list(
        map(wait_on_object, args, [kwargs.get("mode", "rw")] * len(args))
    )
    if len(ret) == 1:
        ret_lst = ret[0]
    else:
        ret_lst = ret
    # Check if there are empty elements return elements that need to be removed
    if isinstance(ret_lst, list):
        # Look backwards the list removing the first EmptyReturn elements
        for elem in reversed(ret_lst):
            if isinstance(elem, EmptyReturn):
                ret_lst.remove(elem)
    return ret_lst


def process_task(
    signature: str,
    has_target: bool,
    names: list,
    values: list,
    num_returns: int,
    compss_types: list,
    compss_directions: list,
    compss_streams: list,
    compss_prefixes: list,
    content_types: list,
    weights: list,
    keep_renames: list,
    decorator_arguments: TaskArguments,
    is_http: bool = False,
) -> None:
    """Submit a task to the runtime.

    :param signature: Task signature.
    :param has_target: Boolean if the task has self.
    :param names: Task parameter names.
    :param values: Task parameter values.
    :param num_returns: Number of returns.
    :param compss_types: List of parameter types.
    :param compss_directions: List of parameter directions.
    :param compss_streams: List of parameter streams.
    :param compss_prefixes: List of parameter prefixes.
    :param content_types: Content types.
    :param weights: List of parameter weights.
    :param keep_renames: Boolean keep renaming.
    :param decorator_arguments: TaskArguments object containing all information
                                contained in the @task decorator.
    :param is_http: If it is a http task (service).
    :return: The future object related to the task return.
    """
    with EventMaster(TRACING_MASTER.process_task_event):
        app_id = 0
        has_priority = decorator_arguments.priority
        num_nodes = decorator_arguments.computing_nodes
        reduction = decorator_arguments.is_reduce
        chunk_size = decorator_arguments.chunk_size
        replicated = decorator_arguments.is_replicated
        distributed = decorator_arguments.is_distributed
        on_failure = decorator_arguments.on_failure
        time_out = decorator_arguments.time_out
        if __debug__:
            # Log the task submission values for debugging purposes.
            values_str = " ".join(str(v) for v in values)
            types_str = " ".join(str(t) for t in compss_types)
            direct_str = " ".join(str(d) for d in compss_directions)
            streams_str = " ".join(str(s) for s in compss_streams)
            prefixes_str = " ".join(str(p) for p in compss_prefixes)
            names_str = " ".join(x for x in names)
            ct_str = " ".join(str(x) for x in content_types)
            weights_str = " ".join(str(x) for x in weights)
            keep_renames_str = " ".join(str(x) for x in keep_renames)
            LOGGER.debug("Processing task:")
            LOGGER.debug("\t- App id: %s", str(app_id))
            LOGGER.debug("\t- Signature: %s", signature)
            LOGGER.debug("\t- Has target: %s", str(has_target))
            LOGGER.debug("\t- Names: %s", names_str)
            LOGGER.debug("\t- Values: %s", values_str)
            LOGGER.debug("\t- COMPSs types: %s", types_str)
            LOGGER.debug("\t- COMPSs directions: %s", direct_str)
            LOGGER.debug("\t- COMPSs streams: %s", streams_str)
            LOGGER.debug("\t- COMPSs prefixes: %s", prefixes_str)
            LOGGER.debug("\t- Content Types: %s", ct_str)
            LOGGER.debug("\t- Weights: %s", weights_str)
            LOGGER.debug("\t- Keep_renames: %s", keep_renames_str)
            LOGGER.debug("\t- Priority: %s", str(has_priority))
            LOGGER.debug("\t- Num nodes: %s", str(num_nodes))
            LOGGER.debug("\t- Reduce: %s", str(reduction))
            LOGGER.debug("\t- Chunk Size: %s", str(chunk_size))
            LOGGER.debug("\t- Replicated: %s", str(replicated))
            LOGGER.debug("\t- Distributed: %s", str(distributed))
            LOGGER.debug("\t- On failure behavior: %s", on_failure)
            LOGGER.debug("\t- Task time out: %s", str(time_out))
            LOGGER.debug("\t- Is http: %s", str(is_http))

        # Check that there is the same amount of values as their types, as well
        # as their directions, streams and prefixes.
        if not (
            len(values)
            == len(compss_types)
            == len(compss_directions)
            == len(compss_streams)
            == len(compss_prefixes)
            == len(content_types)
            == len(weights)
            == len(keep_renames)
        ):
            raise PyCOMPSsException(
                "Issue with the amount of values, types, "
                "directions, streams, prefixes, etc."
            )

        # Submit task to the runtime (call to the C extension):
        # Parameters:
        #     0 - <Integer>   - application id (by default always 0 due to it
        #                       is not currently needed for the signature)
        #     1 - <String>    - path of the module where the task is
        #
        #     2 - <String>    - behavior if the task fails
        #
        #     3 - <String>    - function name of the task (to be called from
        #                       the worker)
        #     4 - <String>    - priority flag (true|false)
        #
        #     5 - <String>    - has target (true|false). If the task is within
        #                       an object or not.
        #     6 - [<String>]  - task parameters (basic types or file paths for
        #                       objects)
        #     7 - [<Integer>] - parameters types (number corresponding to the
        #                       type of each parameter)
        #     8 - [<Integer>] - parameters directions (number corresponding to
        #                       the direction of each parameter)
        #     9 - [<Integer>] - parameters streams (number corresponding to the
        #                       stream of each parameter)
        #     10 - [<String>] - parameters prefixes (string corresponding to
        #                       the prefix of each parameter)
        #     11 - [<String>] - parameters extra type (string corresponding to
        #                       the extra type of each parameter)
        #     12 - [<String>] - parameters weights (string corresponding to the
        #                       weight of each parameter
        #     13 - <String>   - Keep renames flag (true|false)
        #

        if not is_http:
            COMPSs.process_task(
                app_id,
                signature,
                on_failure,
                time_out,
                has_priority,
                num_nodes,
                reduction,
                chunk_size,
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
                keep_renames,
            )
        else:
            COMPSs.process_http_task(
                app_id,
                signature,
                on_failure,
                time_out,
                has_priority,
                num_nodes,
                reduction,
                chunk_size,
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
                keep_renames,
            )


def add_logger(logger_name: str) -> None:
    """Add a new logger for the user.

    :param logger_name: New logger name.
    :returns: None
    """
    add_new_logger(logger_name)


# ########################################################################### #
# ####################### AUXILIARY FUNCTIONS ############################### #
# ########################################################################### #


def _clean_objects(hard_stop: bool = False) -> None:
    """Clean all objects.

    Clean the objects stored in the global dictionaries from the object
    tracker.

    :param hard_stop: avoid call to delete_file when the runtime has died.
    :return: None.
    """
    OT.clean_object_tracker(hard_stop=hard_stop)


def _clean_temps() -> None:
    """Clean temporary files.

    The temporary files end with the IT extension.

    :return: None.
    """
    temp_directory = GLOBALS.get_temporary_directory()
    rmtree(temp_directory, True)
    cwd = os.getcwd()
    for temp_file in os.listdir(cwd):
        if re.search(r"d\d+v\d+_\d+\.IT", temp_file):
            os.remove(os.path.join(cwd, temp_file))


def _wall_clock_exceed(signum: int, frame: typing.Any) -> None:
    """Task wall clock exceeded action: raise PyCOMPSs exception.

    Do not remove the parameters.

    :param signum: Signal number.
    :param frame: Frame.
    :return: None.
    :raises: PyCOMPSsException exception.
    """
    raise PyCOMPSsException("Application has reached its wall clock limit")
