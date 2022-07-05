#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs runtime - Task - Master.

This file contains the task core functions when acting as master.
"""

from __future__ import print_function

import ast
import inspect
import logging
import pickle
import os
import sys
from base64 import b64encode
from collections import OrderedDict
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from typing import get_type_hints

from pycompss.api import parameter
from pycompss.runtime import binding
from pycompss.util.context import CONTEXT
from pycompss.api.commons.constants import LABELS
from pycompss.api.commons.constants import LEGACY_LABELS
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.error_msgs import cast_env_to_int_error
from pycompss.api.commons.implementation_types import IMPLEMENTATION_TYPES
from pycompss.api.parameter import DIRECTION
from pycompss.api.parameter import TYPE
from pycompss.runtime.binding import wait_on
from pycompss.runtime.commons import CONSTANTS
from pycompss.runtime.commons import GLOBALS
from pycompss.runtime.start.initialization import LAUNCH_STATUS
from pycompss.runtime.management.classes import FunctionType
from pycompss.runtime.management.classes import Future
from pycompss.runtime.management.direction import get_compss_direction
from pycompss.runtime.management.object_tracker import OT
from pycompss.runtime.task.arguments import get_kwarg_name
from pycompss.runtime.task.arguments import get_name_from_kwarg
from pycompss.runtime.task.arguments import get_return_name
from pycompss.runtime.task.arguments import get_vararg_name
from pycompss.runtime.task.arguments import is_kwarg
from pycompss.runtime.task.arguments import is_vararg
from pycompss.runtime.task.commons import get_default_direction
from pycompss.runtime.task.commons import get_varargs_direction
from pycompss.runtime.task.core_element import CE
from pycompss.runtime.task.features import TASK_FEATURES
from pycompss.runtime.task.parameter import COMPSsFile
from pycompss.runtime.task.parameter import JAVA_MAX_INT
from pycompss.runtime.task.parameter import JAVA_MAX_LONG
from pycompss.runtime.task.parameter import JAVA_MIN_INT
from pycompss.runtime.task.parameter import JAVA_MIN_LONG
from pycompss.runtime.task.parameter import Parameter
from pycompss.runtime.task.parameter import UNDEFINED_CONTENT_TYPE
from pycompss.runtime.task.parameter import get_compss_type
from pycompss.runtime.task.parameter import get_parameter_copy
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.exceptions import SerializerException
from pycompss.util.interactive.helpers import update_tasks_code_file
from pycompss.util.objects.properties import get_module_name
from pycompss.util.objects.properties import get_wrapped_source
from pycompss.util.objects.properties import is_basic_iterable
from pycompss.util.objects.properties import is_dict
from pycompss.util.objects.sizer import total_sizeof
from pycompss.util.serialization import serializer
from pycompss.util.serialization.serializer import serialize_to_file
from pycompss.util.storages.persistent import get_id
from pycompss.util.tracing.helpers import emit_manual_event_explicit
from pycompss.util.tracing.helpers import EventMaster
from pycompss.util.tracing.types_events_master import TRACING_MASTER
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.util.typing_helper import typing

logger = logging.getLogger(__name__)

# Types conversion dictionary from python to COMPSs
_PYTHON_TO_COMPSS = {
    int: TYPE.INT,  # int # long
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
    # The type of tuples (e.g. (1, 2, 3, "Spam"))
    tuple: TYPE.OBJECT,
    # The type of lists (e.g. [0, 1, 2, 3])
    list: TYPE.OBJECT,
    # The type of dictionaries (e.g. {"Bacon":1,"Ham":0})
    dict: TYPE.OBJECT,
    # The type of generic objects
    object: TYPE.OBJECT,
}

MANDATORY_ARGUMENTS = set()  # type: typing.Set[str]
# List since the parameter names are included before checking for unexpected
# arguments (the user can define a=INOUT in the task decorator and this is not
# an unexpected argument)
SUPPORTED_ARGUMENTS = {
    LABELS.returns,
    LABELS.cache_returns,
    LABELS.priority,
    LABELS.on_failure,
    LABELS.defaults,
    LABELS.time_out,
    LABELS.is_replicated,
    LABELS.is_distributed,
    LABELS.varargs_type,
    LABELS.target_direction,
    LABELS.computing_nodes,
    LABELS.is_reduce,
    LABELS.chunk_size,
    LABELS.numba,
    LABELS.numba_flags,
    LABELS.numba_signature,
    LABELS.numba_declaration,
    LABELS.tracing_hook,
}  # type: typing.Set[str]
# Deprecated arguments. Still supported but shows a message when used.
DEPRECATED_ARGUMENTS = {
    LEGACY_LABELS.is_replicated,
    LEGACY_LABELS.is_distributed,
    LEGACY_LABELS.varargs_type,
    LEGACY_LABELS.target_direction,
    LEGACY_LABELS.time_out,
}  # type: typing.Set[str]
# All supported arguments
ALL_SUPPORTED_ARGUMENTS = SUPPORTED_ARGUMENTS.union(DEPRECATED_ARGUMENTS)
# Some attributes cause memory leaks, we must delete them from memory after
# master call
ATTRIBUTES_TO_BE_REMOVED = {
    "decorator_arguments",
    "param_args",
    "param_varargs",
    "param_defaults",
    "first_arg_name",
    "parameters",
    "returns",
    "multi_return",
}

# This lock allows tasks to be launched with the Threading module while
# ensuring that no attribute is overwritten
MASTER_LOCK = Lock()
VALUE_OF = "value_of"


class TaskMaster:
    """Task class representation for the Master.

    Process the task decorator and prepare all information to call binding
    runtime.
    """

    __slots__ = [
        "param_defaults",
        "first_arg_name",
        "computing_nodes",
        "processes_per_node",
        "parameters",
        "function_name",
        "module_name",
        "function_type",
        "class_name",
        "returns",
        "multi_return",
        "core_element",
        "registered",
        "signature",
        "chunk_size",
        "is_reduce",
        "interactive",
        "module",
        "function_arguments",
        "hints",
        "on_failure",
        "defaults",
        "param_args",
        "param_varargs",
        "user_function",
        "decorator_arguments",
        "explicit_num_returns",
    ]

    def __init__(
        self,
        decorator_arguments: typing.Dict[str, typing.Any],
        user_function: typing.Callable,
        core_element: CE,
        registered: bool,
        signature: str,
        interactive: bool,
        module: typing.Any,
        function_arguments: tuple,
        function_name: str,
        module_name: str,
        function_type: int,
        class_name: str,
        hints: tuple,
        on_failure: str,
        defaults: dict,
    ) -> None:
        """Task at master constructor.

        :param decorator_arguments: Decorator arguments.
        :param user_function: User function.
        :param core_element: Core Element.
        :param registered: If it is already registered.
        :param signature: Function signature.
        :param interactive: If interactive mode.
        :param module: Module where the function belongs to.
        :param function_arguments: Function arguments.
        :param function_name: Function name.
        :param module_name: Module name.
        :param function_type: Function type.
        :param class_name: Class name.
        :param hints: Task hints.
        :param on_failure: On failure management.
        :param defaults: Default values.
        """
        # Initialize TaskCommons
        self.user_function = user_function
        self.decorator_arguments = decorator_arguments
        self.param_args = []  # type: typing.List[typing.Any]
        self.param_varargs = None  # type: typing.Any
        self.on_failure = on_failure
        self.defaults = defaults

        # Add more argument related attributes that will be useful later
        self.param_defaults = None  # type: typing.Union[None, tuple]
        # Add function related attributed that will be useful later
        self.first_arg_name = ""
        self.computing_nodes = None  # type: typing.Any
        self.processes_per_node = None  # type: typing.Any
        self.parameters = OrderedDict()  # type: OrderedDict
        self.function_name = function_name
        self.module_name = module_name
        self.function_type = function_type
        self.class_name = class_name
        # Add returns related attributes that will be useful later
        self.returns = OrderedDict()  # type: OrderedDict
        self.explicit_num_returns = None  # type: typing.Any
        self.multi_return = False
        # Task won't be registered until called from the master for the first
        # time or have a different signature
        self.core_element = core_element
        self.registered = registered
        self.signature = signature
        # Reductions
        self.chunk_size = -1
        self.is_reduce = False

        # Parameters that will come from previous tasks
        # TODO: These parameters could be within a "precalculated" parameters
        self.interactive = interactive
        self.module = module
        self.function_arguments = function_arguments
        self.hints = hints

    def call(self, args: tuple, kwargs: dict) -> tuple:
        """Run the task as master.

        This part deals with task calls in the master's side
        Also, this function must return an appropriate number of
        future objects that point to the appropriate objects/files.

        :return: A function that does "nothing" and returns futures if needed.
        """
        # This lock makes this decorator able to handle various threads
        # calling the same task concurrently
        with MASTER_LOCK:

            # Inspect the user function, get information about the arguments and
            # their names. This defines self.param_args, self.param_varargs,
            # and self.param_defaults. And gives non-None default
            # values to them if necessary
            with EventMaster(TRACING_MASTER.inspect_function_arguments):
                if not self.function_arguments:
                    self.inspect_user_function_arguments()
                # It will be easier to deal with functions if we pretend that all
                # have the signature f(positionals, *variadic, **named). This is
                # why we are substituting Nones with default stuff.
                # As long as we remember what was the users original intention with
                # the parameters we can internally mess with his signature as much
                # as we want. There is no need to add self-imposed constraints
                # here. Also, the very nature of decorators are a huge hint about
                # how we should treat user functions, as most wrappers return a
                # function f(*a, **k)
                if self.param_varargs is None:
                    self.param_varargs = LABELS.varargs_type
                if self.param_defaults is None:
                    self.param_defaults = ()

            # Compute the function path, class (if any), and name
            with EventMaster(TRACING_MASTER.get_function_information):
                self.compute_user_function_information(args)

            with EventMaster(TRACING_MASTER.get_function_signature):
                impl_signature, impl_type_args = self.get_signature()
                if __debug__:
                    logger.debug(
                        "TASK: %s of type %s, in module %s, in class %s",
                        self.function_name,
                        self.function_type,
                        self.module_name,
                        self.class_name,
                    )

            if not GLOBALS.in_tracing_task_name_to_id(impl_signature):
                GLOBALS.set_tracing_task_name_to_id(
                    impl_signature, GLOBALS.len_tracing_task_name_to_id() + 1
                )

            emit_manual_event_explicit(
                TRACING_WORKER.binding_tasks_func_type,
                GLOBALS.get_tracing_task_name_id(impl_signature),
            )

            # Check if we are in interactive mode and update if needed
            with EventMaster(TRACING_MASTER.check_interactive):
                if self.interactive:
                    self.update_if_interactive(self.module)
                else:
                    self.interactive, self.module = self.check_if_interactive()
                    if self.interactive:
                        self.update_if_interactive(self.module)

            # Extract the core element (has to be extracted before processing
            # the kwargs to avoid issues processing the parameters)
            with EventMaster(TRACING_MASTER.extract_core_element):
                cek = kwargs.pop(CORE_ELEMENT_KEY, None)
                pre_defined_ce = self.extract_core_element(cek)

            # Prepare the core element registration information
            with EventMaster(TRACING_MASTER.prepare_core_element):
                self.get_code_strings()

            # It is necessary to decide whether to register or not (the task may
            # be inherited, and in this case it has to be registered again with
            # the new implementation signature).
            if not self.registered or self.signature != impl_signature:
                with EventMaster(TRACING_MASTER.update_core_element):
                    self.update_core_element(
                        impl_signature, impl_type_args, pre_defined_ce
                    )
                    if CONTEXT.is_loading():
                        # This case will only happen with @implements since it calls
                        # explicitly to this call from his call.
                        CONTEXT.add_to_register_later((self, impl_signature))
                    else:
                        self.register_task()
                        self.registered = True
                        self.signature = impl_signature

            # Did we call this function to only register the associated core
            # element? (This can happen with @implements)
            # Do not move this import:
            if TASK_FEATURES.get_register_only():
                # Only register, no launch
                future_object = None
            else:
                # Launch task to the runtime
                # Extract task related parameters (e.g. returns, computing_nodes, etc.)
                with EventMaster(TRACING_MASTER.pop_task_parameters):
                    self.pop_task_parameters(kwargs)
                    # this is total # of processes for this task
                with EventMaster(TRACING_MASTER.process_other_arguments):
                    # Get other arguments if exist
                    if not self.hints:
                        self.hints = self.check_task_hints()
                    (
                        is_replicated,
                        is_distributed,
                        time_out,
                        has_priority,
                        has_target,
                    ) = self.hints
                    is_http = self.core_element.get_impl_type() == "HTTP"

                # Process the parameters, give them a proper direction
                with EventMaster(TRACING_MASTER.process_parameters):
                    code_strings = self.user_function.__code_strings__  # type: ignore
                    self.process_parameters(args, kwargs, code_strings=code_strings)

                # Deal with the return part.
                with EventMaster(TRACING_MASTER.process_return):
                    num_returns = self.add_return_parameters(
                        self.explicit_num_returns,
                        code_strings=code_strings,
                    )
                    if not self.returns:
                        num_returns = self.update_return_if_no_returns(
                            self.user_function
                        )

                # Build return objects
                with EventMaster(TRACING_MASTER.build_return_objects):
                    future_object = None
                    if self.returns:
                        future_object = self._build_return_objects(num_returns)

                # Infer COMPSs types from real types, except for files
                self._serialize_objects()
                # todo: should it go somewhere else?
                serializer.FORCED_SERIALIZER = -1  # reset the forced serializer

                # Build values and COMPSs types and directions
                with EventMaster(TRACING_MASTER.build_compss_types_directions):
                    vtdsc = self._build_values_types_directions()
                    (
                        values,
                        names,
                        compss_types,
                        compss_directions,
                        compss_streams,
                        compss_prefixes,
                        content_types,
                        weights,
                        keep_renames,
                    ) = vtdsc  # noqa

                if __debug__:
                    logger.debug(
                        "TASK: %s of type %s, in module %s, in class %s",
                        self.function_name,
                        self.function_type,
                        self.module_name,
                        self.class_name,
                    )

                # Process the task
                with EventMaster(TRACING_MASTER.process_task_binding):
                    binding.process_task(
                        impl_signature,
                        has_target,
                        names,
                        values,
                        num_returns,
                        compss_types,
                        compss_directions,
                        compss_streams,
                        compss_prefixes,
                        content_types,
                        weights,
                        keep_renames,
                        has_priority,
                        self.computing_nodes,
                        self.is_reduce,
                        self.chunk_size,
                        is_replicated,
                        is_distributed,
                        self.on_failure,
                        time_out,
                        is_http,
                    )

                # Remove unused attributes from the memory
                with EventMaster(TRACING_MASTER.attributes_cleanup):
                    for attribute in ATTRIBUTES_TO_BE_REMOVED:
                        if hasattr(self, attribute):
                            try:
                                delattr(self, attribute)
                            except AttributeError:
                                # Only happens when compiled
                                pass

                emit_manual_event_explicit(TRACING_WORKER.binding_tasks_func_type, 0)

        # The with statement automatically released the lock.

        # Return the future object/s corresponding to the task
        # This object will substitute the user expected return from the task
        # and will be used later for synchronization or as a task parameter
        # (then the runtime will take care of the dependency).
        # Also return if the task has been registered and its signature,
        # so that future tasks of the same function register if necessary.
        return (
            future_object,
            self.core_element,
            self.registered,
            self.signature,
            self.interactive,
            self.module,
            self.function_arguments,
            self.function_name,
            self.module_name,
            self.function_type,
            self.class_name,
            self.hints,
        )

    def check_if_interactive(self) -> typing.Tuple[bool, typing.Any]:
        """Check if running in interactive mode.

        :return: True if interactive. False otherwise.
        """
        mod = inspect.getmodule(self.user_function)  # type: typing.Any
        module_name = mod.__name__
        if CONTEXT.in_pycompss() and module_name in (
            "__main__",
            "pycompss.runtime.launch",
        ):
            # 1.- The runtime is running.
            # 2.- The module where the function is defined was run as __main__.
            return True, mod
        return False, None

    def update_if_interactive(self, mod: typing.Any) -> None:
        """Update the code for jupyter notebook.

        Update the user code if in interactive mode and the session has
        been started.

        :param mod: Source module.
        :return: None.
        """
        # We need to find out the real module name from launched
        path = LAUNCH_STATUS.get_app_path()
        # Get the file name
        file_name = os.path.splitext(os.path.basename(path))[0]
        # Do any necessary pre processing action before executing any code
        if (
            file_name.startswith(CONSTANTS.interactive_file_name)
            and not self.registered
        ):
            # If the file_name starts with "InteractiveMode" means that
            # the user is using PyCOMPSs from jupyter-notebook.
            # Convention between this file and interactive.py
            # In this case it is necessary to do a pre-processing step
            # that consists of putting all user code that may be executed
            # in the worker on a file.
            # This file has to be visible for all workers.
            update_tasks_code_file(self.user_function, path)
            print(f"Found task: {self.user_function.__name__}")

    def extract_core_element(
        self, cek: typing.Optional[CE]
    ) -> typing.Tuple[bool, bool]:
        """Get or instantiate the Task's core element.

        Extract the core element if created in a higher level decorator,
        uses an existing or creates a new one if does not.

        IMPORTANT! extract the core element from kwargs if pre-defined
                   in decorators defined on top of @task.

        :return: If previously created and if created in higher level decorator.
        """
        pre_defined_core_element = False
        upper_decorator = False
        if cek:
            # Core element has already been created in a higher level decorator
            self.core_element = cek
            pre_defined_core_element = True
            upper_decorator = True
        elif self.core_element:
            # A core element from previous task calls was saved.
            pre_defined_core_element = True
        else:
            # No decorators over @task: instantiate an empty core element.
            self.core_element = CE()
        return pre_defined_core_element, upper_decorator

    def inspect_user_function_arguments(self) -> None:
        """Get user function arguments.

        Inspect the arguments of the user function and store them.
        Read the names of the arguments and remember their order.
        We will also learn things like if the user function contained
        variadic arguments, named arguments and so on.
        This will be useful when pairing arguments with the direction
        the user has specified for them in the decorator.

        The third return value was self.param_kwargs - not used (removed).

        :return: the attributes to be reused.
        """
        try:
            arguments = self._getargspec(self.user_function)
        except TypeError:
            func_attrs = dir(self.user_function)
            if "py_func" in func_attrs:
                # This is a numba jit declared task
                py_func = self.get_user_function_py_func()
                arguments = self._getargspec(py_func)
            else:
                # This is a compiled function
                wrapped_func = self.get_user_function_wrapped()
                arguments = self._getargspec(wrapped_func)
        self.param_args, self.param_varargs, _, self.param_defaults = arguments

    def is_numba_function(self) -> bool:
        """Check if self.user_function is in reality a numba compiled function.

        :return: True if self.user_function has py_func.
        """
        return "py_func" in dir(self.user_function)

    def get_user_function_py_func(self) -> typing.Callable:
        """Retrieve py_func from self.user_function.

        WARNING!!! Only available in numba wrapped functions.

        :return: The self.user_function py_func.
        """
        return self.user_function.py_func  # type: ignore

    def user_func_py_func_glob_getter(self, field) -> typing.Any:
        """Retrieve a field from __globals__ from py_func of self.user_function.

        WARNING!!! Only available in numba wrapped functions.

        :return: __globals__ getter for the given field.
        """
        py_func = self.get_user_function_py_func()
        return py_func.__globals__.get(field)  # type: ignore

    def get_user_function_wrapped(self) -> typing.Callable:
        """Retrieve __wrapped__ from self.user_function.

        WARNING!!! Only available in compiled functions.

        :return: The user function from __wrapped__.
        """
        return self.user_function.__wrapped__  # type: ignore

    def user_func_wrapped_glob_getter(self, field) -> typing.Any:
        """Retrieve a field from __globals__ from __wrapped__ of self.user_function.

        WARNING!!! Only available in compiled functions.

        :return: __globals__ getter for the given field result.
        """
        wrapped_func = self.get_user_function_wrapped()
        return wrapped_func.__globals__.get(field)  # type: ignore

    def user_func_glob_getter(self, field: str) -> typing.Any:
        """Retrieve a field from __globals__ from py_func of self.user_function.

        WARNING!!! Only available in numba wrapped functions.

        :return: __globals__ getter for the given field result.
        """
        return self.user_function.__globals__.get(field)  # type: ignore

    @staticmethod
    def _getargspec(function: typing.Any) -> tuple:
        """Private method that retrieves the function argspec.

        :param function: Function to analyse.
        :return: args, varargs, keywords and defaults dictionaries.
        """
        full_argspec = inspect.getfullargspec(function)
        as_args = full_argspec.args
        as_varargs = full_argspec.varargs
        as_keywords = None  # type: typing.Any
        as_defaults = full_argspec.defaults
        return as_args, as_varargs, as_keywords, as_defaults

    def pop_task_parameters(self, kwargs: dict) -> None:
        """Extract all @task related parameters.

        Updates:
            - self.explicit_num_returns
            - self.cns
            - self.on_failure
            - self.defaults
            - self.is_reduce
            - self.chunk_size

        :param kwargs: Keyword arguments.
        :return: None.
        """
        # Pop returns from kwargs
        self.explicit_num_returns = kwargs.pop(LABELS.returns, None)

        # Deal with dynamic computing nodes
        # If we have an MPI, COMPSs or MultiNode decorator above us we should
        # have computing_nodes as a kwarg, we should detect it and remove it.
        # Otherwise we set it to 1
        cns = kwargs.pop(LABELS.computing_nodes, 1)
        if cns != 1:
            # Non default => parse
            self.computing_nodes = self.parse_computing_nodes(cns)
        else:
            self.computing_nodes = 1
        processes_per_node = kwargs.pop(LABELS.processes_per_node, 1)
        if processes_per_node != 1:
            # Non default => parse
            self.processes_per_node = self.parse_processes_per_node(processes_per_node)
        else:
            self.processes_per_node = 1
        # Check processes per node
        if self.processes_per_node > 1:
            self.validate_processes_per_node()
            self.computing_nodes = int(self.computing_nodes / self.processes_per_node)
        # Deal with on_failure
        if LABELS.on_failure in self.decorator_arguments:
            self.on_failure = self.decorator_arguments[LABELS.on_failure]
            # if task defines on_failure property the decorator is ignored
            kwargs.pop(LABELS.on_failure, None)
        else:
            self.on_failure = kwargs.pop(LABELS.on_failure, "RETRY")
        self.defaults = kwargs.pop(LABELS.defaults, {})
        # Deal with reductions
        is_reduce = kwargs.pop(LABELS.is_reduce, False)
        if is_reduce is not False:
            self.is_reduce = self.parse_is_reduce(is_reduce)
        else:
            self.is_reduce = False
        chunk_size = kwargs.pop(LABELS.chunk_size, 0)
        if chunk_size != 0:
            self.chunk_size = self.parse_chunk_size(chunk_size)
        else:
            self.chunk_size = 0

    def process_parameters(
        self, args: tuple, kwargs: dict, code_strings: bool = True
    ) -> None:
        """Process all the input parameters.

        Basically, processing means "build a dictionary of <name, parameter>,
        where each parameter has an associated Parameter object".
        This function also assigns default directions to parameters.

        :param args: Arguments.
        :param kwargs: Keyword arguments.
        :param code_strings: Code strings.
        :return: None, it only modifies self.parameters.
        """
        # It is important to know the name of the first argument to determine
        # if we are dealing with a class or instance method (i.e: first
        # argument is named self)
        # Process the positional arguments and fill self.parameters with
        # their corresponding Parameter object
        # Some of these positional arguments may have been not
        # explicitly defined
        num_positionals = min(len(self.param_args), len(args))
        arg_names = self.param_args[:num_positionals]
        arg_objects = args[:num_positionals]
        for (arg_name, arg_object) in zip(arg_names, arg_objects):
            self.parameters[arg_name] = self.build_parameter_object(
                arg_name, arg_object, code_strings=code_strings
            )

        # Check defaults
        if self.param_defaults:
            num_defaults = len(self.param_defaults)
            if num_defaults > 0:
                # Give default values to all the parameters that have a
                # default value and are not already set
                # As an important observation, defaults are matched as follows:
                # defaults[-1] goes with positionals[-1]
                # defaults[-2] goes with positionals[-2]
                # ...
                # Also, |defaults| <= |positionals|
                for (arg_name, default_value) in reversed(
                    list(
                        zip(
                            list(reversed(self.param_args))[:num_defaults],
                            list(reversed(self.param_defaults)),
                        )
                    )
                ):
                    if arg_name not in self.parameters:
                        real_arg_name = get_kwarg_name(arg_name)
                        self.parameters[real_arg_name] = self.build_parameter_object(
                            real_arg_name, default_value, code_strings=code_strings
                        )
        # Process variadic and keyword arguments
        # Note that they are stored with custom names
        # This will allow us to determine the class of each parameter
        # and their order in the case of the variadic ones
        # Process the variadic arguments
        supported_varargs = []
        for (i, var_arg) in enumerate(args[num_positionals:]):
            arg_name = get_vararg_name(self.param_varargs, i)
            self.parameters[arg_name] = self.build_parameter_object(
                arg_name, var_arg, code_strings=code_strings
            )
            if self.param_varargs not in supported_varargs:
                supported_varargs.append(self.param_varargs)
        # Process keyword arguments
        supported_kwargs = []
        for (name, value) in kwargs.items():
            arg_name = get_kwarg_name(name)
            self.parameters[arg_name] = self.build_parameter_object(
                arg_name, value, code_strings=code_strings
            )
            if name not in supported_kwargs:
                supported_kwargs.append(name)
        # Check the arguments - Look for mandatory and unexpected arguments
        supported_arguments = ALL_SUPPORTED_ARGUMENTS.union(self.param_args)
        supported_arguments = supported_arguments.union(supported_varargs)
        supported_arguments = supported_arguments.union(supported_kwargs)
        check_arguments(
            MANDATORY_ARGUMENTS,
            DEPRECATED_ARGUMENTS,
            supported_arguments,
            list(self.decorator_arguments.keys()),
            "@task",
        )

    def build_parameter_object(
        self, arg_name: str, arg_object: typing.Any, code_strings=True
    ) -> Parameter:
        """Create the Parameter object from an argument name and object.

        WARNING: Any modification in the param object will modify the
                 original Parameter set in the task.py __init__ constructor
                 for the rest of the task calls.

        :param arg_name: Argument name.
        :param arg_object: Argument object.
        :param code_strings: Code strings.
        :return: Parameter object.
        """
        # Is the argument a vararg? or a kwarg? Then check the direction
        # for varargs or kwargs
        if is_vararg(arg_name):
            self.param_varargs, varargs_direction = get_varargs_direction(
                self.param_varargs, self.decorator_arguments
            )
            param = get_parameter_copy(varargs_direction)
        elif is_kwarg(arg_name):
            real_name = get_name_from_kwarg(arg_name)
            default_parameter = get_default_direction(
                real_name, self.decorator_arguments, self.param_args
            )
            param = self.decorator_arguments.get(real_name, default_parameter)
        else:
            # The argument is named, check its direction
            # Default value = IN if not class or instance method and
            #                 isModifier, INOUT otherwise
            # see self.get_default_direction
            # Note that if we have something like @task(self = IN) it
            # will have priority over the default
            # direction resolution, even if this implies a contradiction
            # with the target_direction flag
            default_parameter = get_default_direction(
                arg_name, self.decorator_arguments, self.param_args
            )
            param = self.decorator_arguments.get(arg_name, default_parameter)

        # If the parameter is a FILE then its type will already be defined,
        # and get_compss_type will mislabel it as a parameter.TYPE.STRING
        if param.is_object():
            param.content_type = get_compss_type(arg_object, code_strings=code_strings)

        # Set if the object is really a future.
        if isinstance(arg_object, Future):
            param.is_future = True

        # If the parameter is a DIRECTORY or FILE update the file_name
        # or content type depending if object. Otherwise, update content.
        if param.is_file() or param.is_directory():
            if isinstance(arg_object, COMPSsFile):
                param.file_name = arg_object
            else:
                param.file_name = COMPSsFile(str(arg_object))
            # todo: beautify this
            param.extra_content_type = "FILE"
        else:
            param.extra_content_type = str(type(arg_object))
            param.content = arg_object
        return param

    def compute_user_function_information(self, args: tuple) -> None:
        """Get the user function path and name.

        Compute the function path p and the name n such that
        "from p import n" imports self.user_function.

        :return: None, it just sets self.user_function_path and
                 self.user_function_name.
        """
        self.function_name = self.user_function.__name__
        # Detect if self is present
        num_positionals = min(len(self.param_args), len(args))
        arg_names = self.param_args[:num_positionals]
        first_object = None
        if arg_names and self.first_arg_name == "":
            self.first_arg_name = arg_names[0]
            first_object = args[0]
        # Get the module name (the x part "from x import y"), except for the
        # class name
        self.compute_module_name(first_object)
        # Get the function type (function, instance method, class method)
        self.compute_function_type(first_object)

    def compute_module_name(self, first_object: typing.Any) -> None:
        """Compute the user's function module name.

        There are various cases:
            1) The user function is defined in some file. This is easy, just
               get the module returned by inspect.getmodule.
            2) The user function is in the main module. Retrieve the file and
               build the import name from it.
            3) We are in interactive mode.

        :return: None, it just modifies self.module_name.
        """
        mod = inspect.getmodule(self.user_function)  # type: typing.Any
        self.module_name = mod.__name__
        # If it is a task within a class, the module it will be where the one
        # where the class is defined, instead of the one where the task is
        # defined.
        # This avoids conflicts with task inheritance.
        if self.first_arg_name == "self":
            mod = inspect.getmodule(type(first_object))
            self.module_name = mod.__name__
        elif self.first_arg_name == "cls":
            self.module_name = first_object.__module__
        if self.module_name in ("__main__", "pycompss.runtime.launch"):
            # The module where the function is defined was run as __main__,
            # We need to find out the real module name
            path = LAUNCH_STATUS.get_app_path()
            # Get the file name
            file_name = os.path.splitext(os.path.basename(path))[0]
            # Get the module
            self.module_name = get_module_name(path, file_name)

    def compute_function_type(self, first_object: typing.Any) -> None:
        """Compute user function type.

        Compute some properties of the user function, as its name,
        its import path, and its type (module function, instance method,
         class method), etc.

        :return: None, just updates self.class_name and self.function_type.
        """
        # Check the type of the function called.
        # inspect.ismethod(f) does not work here,
        # for methods python hasn't wrapped the function as a method yet
        # Everything is still a function here, can't distinguish yet
        # with inspect.ismethod or isfunction
        self.function_type = FunctionType.FUNCTION
        self.class_name = ""
        if self.first_arg_name == "self":
            self.function_type = FunctionType.INSTANCE_METHOD
            self.class_name = type(first_object).__name__
        elif self.first_arg_name == "cls":
            self.function_type = FunctionType.CLASS_METHOD
            self.class_name = first_object.__name__
        # Finally, check if the function type is really a module function or
        # a static method.
        # Static methods are ONLY supported with Python 3 due to __qualname__
        # feature, which enables to know to which class they belong.
        # The class name is needed in order to define properly the class_name
        # for the correct registration and later invoke.
        # Since these methods don't have self, nor cls, they are considered as
        # FUNCTIONS to the runtime
        name = str(self.function_name)
        qualified_name = str(self.user_function.__qualname__)
        if name != qualified_name:
            # Then there is a class definition before the name in the
            # qualified name
            self.class_name = qualified_name[: -len(name) - 1]
            # -1 to remove the last point

    def get_code_strings(self) -> None:
        """Get if the strings must be coded or not.

        IMPORTANT! modify f adding __code_strings__ which is used in binding.

        :return: None.
        """
        ce_type = self.core_element.get_impl_type()
        default = IMPLEMENTATION_TYPES.method
        if ce_type is None or (isinstance(ce_type, str) and ce_type == ""):
            ce_type = default
        # code_strings = True if default, python_mpi or multinode
        # code_strings = False if mpi, binary and container
        code_strings = bool(
            ce_type
            in (
                default,
                IMPLEMENTATION_TYPES.python_mpi,
                IMPLEMENTATION_TYPES.multi_node,
            )
        )

        self.user_function.__code_strings__ = code_strings  # type: ignore

        if __debug__:
            logger.debug(
                "[@TASK] Task type of function %s in module %s: %s",
                self.function_name,
                self.module_name,
                str(ce_type),
            )

    def get_signature(self) -> typing.Tuple[str, list]:
        """Find out the function signature.

        The information is needed in order to compare the implementation
        signature, so that if it has been registered with a different
        signature, it can be re-registered with the new one (enable
        inheritance).

        :return: Implementation signature and implementation type arguments.
        """
        module_name = str(self.module_name)
        class_name = str(self.class_name)
        function_name = str(self.function_name)
        if self.class_name != "":
            # Within class or subclass
            impl_signature = ".".join([module_name, class_name, function_name])
            impl_type_args = [".".join([module_name, class_name]), function_name]
        else:
            # The task is defined within the main app file.
            # Not in a class or subclass
            # This case can be reached in Python 3, where particular
            # frames are included, but not class names found.
            impl_signature = ".".join([module_name, function_name])
            impl_type_args = [module_name, function_name]
        return impl_signature, impl_type_args

    def update_core_element(
        self,
        impl_signature: str,
        impl_type_args: list,
        pre_defined_ce: typing.Tuple[bool, bool],
    ) -> None:
        """Add the @task decorator information to the core element.

        CAUTION: Modifies the core_element parameter.

        :param impl_signature: Implementation signature.
        :param impl_type_args: Implementation type arguments.
        :param pre_defined_ce: Two boolean (if core element contains predefined
                               fields and if they have been predefined by
                               upper decorators).
        :return: None.
        """
        pre_defined_core_element = pre_defined_ce[0]
        upper_decorator = pre_defined_ce[1]

        # Include the registering info related to @task
        impl_type = IMPLEMENTATION_TYPES.method
        impl_constraints = {}  # type: dict
        impl_io = False

        if __debug__:
            logger.debug("Configuring core element.")

        set_ce_signature = self.core_element.set_ce_signature
        set_impl_signature = self.core_element.set_impl_signature
        set_impl_type_args = self.core_element.set_impl_type_args
        set_impl_constraints = self.core_element.set_impl_constraints
        set_impl_type = self.core_element.set_impl_type
        set_impl_io = self.core_element.set_impl_io
        if pre_defined_core_element:
            # Core element has already been created in an upper decorator
            # (e.g. @implements and @compss)
            _ce_signature = self.core_element.get_ce_signature()
            _impl_constraints = self.core_element.get_impl_constraints()
            _impl_type = self.core_element.get_impl_type()
            _impl_type_args = self.core_element.get_impl_type_args()
            _impl_io = self.core_element.get_impl_io()
            if _ce_signature == "":
                set_ce_signature(impl_signature)
                set_impl_signature(impl_signature)
            elif _ce_signature != impl_signature and not upper_decorator:
                # Specific for inheritance - not for @implements.
                set_ce_signature(impl_signature)
                set_impl_signature(impl_signature)
                set_impl_type_args(impl_type_args)
            else:
                # If we are here that means that we come from an implements
                # decorator, which means that this core element has already
                # a signature
                set_impl_signature(impl_signature)
                set_impl_type_args(impl_type_args)
            if not _impl_constraints:
                set_impl_constraints(impl_constraints)
            if not _impl_type:
                set_impl_type(impl_type)
            if not _impl_type_args:
                set_impl_type_args(impl_type_args)
            # Need to update impl_type_args if task is PYTHON_MPI and
            # if the parameter with layout exists.
            if _impl_type == IMPLEMENTATION_TYPES.python_mpi:
                self.check_layout_params(_impl_type_args)
                set_impl_signature(".".join([IMPLEMENTATION_TYPES.mpi, impl_signature]))
                if _impl_type_args:
                    set_impl_type_args(impl_type_args + _impl_type_args[1:])
                else:
                    set_impl_type_args(impl_type_args)
            if not _impl_io:
                set_impl_io(impl_io)
        else:
            # @task is in the top of the decorators stack.
            # Update the empty core_element
            self.core_element = CE(
                impl_signature,
                impl_signature,
                impl_constraints,
                impl_type,
                impl_io,
                impl_type_args,
            )

    def check_layout_params(self, impl_type_args: list) -> None:
        """Check the layout parameter format.

        :param impl_type_args: Parameter arguments.
        :return: None.
        """
        # todo: replace these INDEXES with CONSTANTS
        num_layouts = int(impl_type_args[8])
        if num_layouts > 0:
            for i in range(num_layouts):
                param_name = impl_type_args[(9 + (i * 4))].strip()
                if param_name:
                    if param_name in self.decorator_arguments:
                        if (
                            self.decorator_arguments[param_name].content_type
                            != parameter.TYPE.COLLECTION
                        ):
                            raise PyCOMPSsException(
                                f"Parameter {param_name} is not a collection!"
                            )
                    else:
                        raise PyCOMPSsException(
                            f"Parameter {param_name} does not exist!"
                        )

    def register_task(self) -> None:
        """Register the task in the runtime.

        This registration must be done only once on the task decorator
        initialization, unless there is a signature change (this will mean
        that the user has changed the implementation interactively).

        :return: None.
        """
        if __debug__:
            logger.debug(
                "[@TASK] Registering the function %s in module %s",
                self.function_name,
                self.module_name,
            )
        binding.register_ce(self.core_element)

    def validate_processes_per_node(self) -> None:
        """Check the processes per node property.

        :return: None.
        """
        if self.computing_nodes < self.processes_per_node:
            raise PyCOMPSsException("Processes is smaller than processes_per_node.")
        if (self.computing_nodes % self.processes_per_node) > 0:
            raise PyCOMPSsException(
                "Processes is not a multiple of processes_per_node."
            )

    def parse_processes_per_node(
        self, processes_per_node: typing.Union[int, str]
    ) -> int:
        """Retrieve the number of processes per node.

        This value can be defined by upper decorators and can also be defined
        dynamically defined with a global or environment variable.

        :return: The number of processes per node.
        """
        parsed_processes_per_node = 1
        if isinstance(processes_per_node, int):
            # Nothing to do
            parsed_processes_per_node = processes_per_node
        elif isinstance(processes_per_node, str):
            # Check if processes_per_node can be casted to string
            # Check if processes_per_node is an environment variable
            # Check if processes_per_node is a dynamic global variable
            try:
                # Cast string to int
                parsed_processes_per_node = int(processes_per_node)
            except ValueError:
                # Environment variable
                if processes_per_node.strip().startswith("$"):
                    # Computing nodes is an ENV variable, load it
                    env_var = processes_per_node.strip()[1:]  # Remove $
                    if env_var.startswith("{"):
                        env_var = env_var[1:-1]  # remove brackets
                    try:
                        parsed_processes_per_node = int(os.environ[env_var])
                    except ValueError as value_error:
                        raise PyCOMPSsException(
                            cast_env_to_int_error("ComputingNodes")
                        ) from value_error
                else:
                    # Dynamic global variable
                    try:
                        # Load from global variables
                        parsed_processes_per_node = self.user_func_glob_getter(
                            processes_per_node
                        )
                    except AttributeError:
                        # This is a numba jit declared task
                        try:
                            if self.is_numba_function():
                                parsed_processes_per_node = (
                                    self.user_func_py_func_glob_getter(
                                        processes_per_node
                                    )
                                )
                            else:
                                parsed_processes_per_node = (
                                    self.user_func_wrapped_glob_getter(
                                        processes_per_node
                                    )
                                )
                        except AttributeError as attribute_error:
                            # No more chances
                            # Ignore error and parsed_processes_per_node will
                            # raise the exception
                            raise PyCOMPSsException(
                                "ERROR: Wrong Computing Nodes value."
                            ) from attribute_error
        else:
            raise PyCOMPSsException(
                "Unexpected processes_per_node value. Must be str or int."
            )

        if parsed_processes_per_node <= 0:
            logger.warning(
                "Registered processes_per_node is less than 1 (%s <= 0). Automatically set it to 1",
                str(parsed_processes_per_node),
            )
            parsed_processes_per_node = 1

        return parsed_processes_per_node

    def parse_computing_nodes(self, computing_nodes: typing.Union[int, str]) -> int:
        """Retrieve the number of computing nodes.

        This value can be defined by upper decorators and can also be defined
        dynamically defined with a global or environment variable.

        :return: The number of computing nodes.
        """
        parsed_computing_nodes = 1
        if isinstance(computing_nodes, int):
            # Nothing to do
            parsed_computing_nodes = computing_nodes
        elif isinstance(computing_nodes, str):
            # Check if computing_nodes can be casted to string
            # Check if computing_nodes is an environment variable
            # Check if computing_nodes is a dynamic global variable
            try:
                # Cast string to int
                parsed_computing_nodes = int(computing_nodes)
            except ValueError:
                # Environment variable
                if computing_nodes.strip().startswith("$"):
                    # Computing nodes is an ENV variable, load it
                    env_var = computing_nodes.strip()[1:]  # Remove $
                    if env_var.startswith("{"):
                        env_var = env_var[1:-1]  # remove brackets
                    try:
                        parsed_computing_nodes = int(os.environ[env_var])
                    except ValueError as value_error:
                        raise PyCOMPSsException(
                            cast_env_to_int_error("ComputingNodes")
                        ) from value_error
                else:
                    # Dynamic global variable
                    try:
                        # Load from global variables
                        parsed_computing_nodes = self.user_func_glob_getter(
                            computing_nodes
                        )
                    except AttributeError:
                        # This is a numba jit declared task
                        try:
                            if self.is_numba_function():
                                parsed_computing_nodes = (
                                    self.user_func_py_func_glob_getter(computing_nodes)
                                )
                            else:
                                parsed_computing_nodes = (
                                    self.user_func_wrapped_glob_getter(computing_nodes)
                                )
                        except AttributeError as attribute_error:
                            # No more chances
                            # Ignore error and parsed_computing_nodes will
                            # raise the exception
                            raise PyCOMPSsException(
                                "ERROR: Wrong Computing Nodes value."
                            ) from attribute_error
        else:
            raise PyCOMPSsException(
                "Unexpected computing_nodes value. Must be str or int."
            )  # noqa: E501

        if parsed_computing_nodes <= 0:
            logger.warning(
                "Registered computing_nodes is less than 1 (%s <= 0). Automatically set it to 1",
                str(parsed_computing_nodes),
            )
            parsed_computing_nodes = 1

        return parsed_computing_nodes

    def parse_chunk_size(self, chunk_size: typing.Union[str, int]) -> int:
        """Parse the chunk size value.

        :param chunk_size: Chunk size defined in the @task decorator
        :return: Chunk size as integer.
        """
        if isinstance(chunk_size, int):
            return chunk_size
        if isinstance(chunk_size, str):
            # Check if chunk_size can be casted to string
            # Check if chunk_size is an environment variable
            # Check if chunk_size is a dynamic global variable
            try:
                # Cast string to int
                return int(chunk_size)
            except ValueError:
                # Environment variable
                if chunk_size.strip().startswith("$"):
                    # Chunk size is an ENV variable, load it
                    env_var = chunk_size.strip()[1:]  # Remove $
                    if env_var.startswith("{"):
                        env_var = env_var[1:-1]  # remove brackets
                    try:
                        return int(os.environ[env_var])
                    except ValueError as value_error:
                        raise PyCOMPSsException(
                            cast_env_to_int_error("ChunkSize")
                        ) from value_error
                else:
                    # Dynamic global variable
                    try:
                        # Load from global variables
                        return self.user_func_glob_getter(chunk_size)
                    except AttributeError:
                        # This is a numba jit declared task
                        try:
                            if self.is_numba_function():
                                return self.user_func_py_func_glob_getter(chunk_size)
                            return self.user_func_wrapped_glob_getter(chunk_size)
                        except AttributeError as attribute_error:
                            # No more chances
                            # Ignore error and parsed_chunk_size will
                            # raise the exception
                            raise PyCOMPSsException(
                                "ERROR: Wrong chunk_size value."
                            ) from attribute_error
        raise PyCOMPSsException("Unexpected chunk_size value. Must be str or int.")

    @staticmethod
    def parse_is_reduce(is_reduce: typing.Union[bool, str]) -> bool:
        """Parse the is_reduce parameter.

        :return: If it is a reduction or not.
        """
        if isinstance(is_reduce, bool):
            # Nothing to do
            return is_reduce
        if isinstance(is_reduce, str):
            # Check if is_reduce can be cast to string
            try:
                # Cast string to int
                return bool(is_reduce)
            except ValueError:
                return False
        raise PyCOMPSsException("Unexpected is_reduce value. Must be bool or str.")

    def check_task_hints(self) -> tuple:
        """Process the @task hints.

        :return: The value of all possible hints.
        """
        deco_arg_getter = self.decorator_arguments.get
        if LEGACY_LABELS.is_replicated in self.decorator_arguments:
            is_replicated = deco_arg_getter(LEGACY_LABELS.is_replicated)
            logger.warning(
                "Detected deprecated isReplicated. Please, change it to is_replicated"
            )  # noqa: E501
        else:
            is_replicated = deco_arg_getter(LABELS.is_replicated)
        # Get is distributed
        if LEGACY_LABELS.is_distributed in self.decorator_arguments:
            is_distributed = deco_arg_getter(LEGACY_LABELS.is_distributed)
            logger.warning(
                "Detected deprecated isDistributed. Please, change it to is_distributed"
            )  # noqa: E501
        else:
            is_distributed = deco_arg_getter(LABELS.is_distributed)
        # Get time out
        if LEGACY_LABELS.time_out in self.decorator_arguments:
            time_out = deco_arg_getter(LEGACY_LABELS.time_out)
            logger.warning(
                "Detected deprecated timeOut. Please, change it to time_out"
            )  # noqa: E501
        else:
            time_out = deco_arg_getter(LABELS.time_out)
        # Get priority
        has_priority = deco_arg_getter(LABELS.priority)
        # Check if the function is an instance method or a class method.
        has_target = self.function_type == FunctionType.INSTANCE_METHOD

        return (
            is_replicated,
            is_distributed,
            time_out,
            has_priority,
            has_target,
        )  # noqa: E501

    def add_return_parameters(
        self, returns: typing.Any, code_strings: bool = True
    ) -> int:
        """Modify the return parameters accordingly to the return statement.

        :return: Creates and modifies self.returns and returns the number of
                 returns.
        """
        if returns:
            _returns = returns  # type: typing.Any
        else:
            _returns = self.decorator_arguments[LABELS.returns]

        # Note that RETURNS is by default False
        if not _returns:
            return 0

        # A return statement can be the following:
        # 1) A type. This means "this task returns an object of this type"
        # 2) An integer N. This means "this task returns N objects"
        # 3) A basic iterable (tuple, list...). This means "this task
        #    returns an iterable with the indicated elements inside
        # We are returning multiple objects until otherwise proven
        # It is important to know because this will determine if we will
        # return a single object or [a single object] in some cases
        self.multi_return = True
        defined_type = False
        to_return = None  # type: typing.Any
        if isinstance(_returns, str):
            # Check if the returns statement contains an string with an
            # integer or a global variable.
            # In such case, build a list of objects of value length and
            # set it in ret_type.
            # Global variable, value_of(Parameter) or string wrapping integer
            # value (Evaluated in reverse order)
            num_rets = self.get_num_returns_from_string(_returns)
            # Construct hidden multi-return
            if num_rets > 1:
                to_return = num_rets
            else:
                to_return = 1
        elif is_basic_iterable(_returns):
            # The task returns a basic iterable with some types already defined
            to_return = _returns
            defined_type = True
        elif isinstance(_returns, int):
            # The task returns a list of N objects, defined by the int N
            to_return = _returns
        else:
            # The task returns a single object of a single type
            # This is also the only case when no multiple objects are
            # returned but only one
            self.multi_return = False
            to_return = 1
            defined_type = True

        # At this point we have a list of returns
        ret_dir = DIRECTION.OUT
        if defined_type:
            if to_return == 1:
                ret_type = get_compss_type(_returns, code_strings=code_strings)
                self.returns[get_return_name(0)] = Parameter(
                    content=_returns, content_type=ret_type, direction=ret_dir
                )
            else:
                for i, elem in enumerate(to_return):  # noqa
                    ret_type = get_compss_type(elem, code_strings=code_strings)
                    self.returns[get_return_name(i)] = Parameter(
                        content=elem, content_type=ret_type, direction=ret_dir
                    )
        else:
            ret_type = TYPE.OBJECT
            for i in range(to_return):
                self.returns[get_return_name(i)] = Parameter(
                    content=None, content_type=ret_type, direction=ret_dir
                )

        # Hopefully, an exception have been thrown if some invalid
        # stuff has been put in the returns field
        if defined_type:
            if to_return == 1:
                return to_return
            return len(to_return)  # noqa
        return to_return

    def get_num_returns_from_string(self, returns: str) -> int:
        """Convert the number of returns from string to integer.

        :param returns: Returns as string.
        :return: Number of returned parameters.
        """
        try:
            # Return is hidden by an int as a string.
            # i.e., returns="var_int"
            return int(returns)
        except ValueError as value_error:
            if returns.startswith(VALUE_OF):
                #  from "value_of ( xxx.yyy )" to [xxx, yyy]
                param_ref = (
                    returns.replace(VALUE_OF, "")
                    .replace("(", "")
                    .replace(")", "")
                    .strip()
                    .split(".")
                )  # noqa: E501
                if len(param_ref) > 0:
                    obj = self.parameters[param_ref[0]].content
                    return int(_get_object_property(param_ref, obj))
                raise PyCOMPSsException(
                    f"Incorrect value_of format in {returns}"
                ) from value_error

            # Else: return is hidden by a global variable. i.e., LT_ARGS
            try:
                num_rets = self.user_func_glob_getter(returns)
            except AttributeError:
                if self.is_numba_function():
                    # This is a numba jit declared task
                    num_rets = self.user_func_py_func_glob_getter(returns)
                else:
                    # This is a compiled task
                    num_rets = self.user_func_wrapped_glob_getter(returns)
            return int(num_rets)

    def update_return_if_no_returns(self, function: typing.Callable) -> int:
        """Look for returns if no returns is specified.

        Checks the code looking for return statements if no returns is
        specified in @task decorator.

        WARNING: Updates self.return if returns are found.

        :param function: Function to check.
        :return: The number of return elements if found.
        """
        type_hints = get_type_hints(function)
        if "return" in type_hints:
            # There is a return defined as type-hint
            ret = type_hints["return"]
            try:
                if hasattr(ret, "__len__"):
                    num_returns = len(ret)
                else:
                    num_returns = 1
            except TypeError:
                # Is not iterable, so consider just 1
                num_returns = 1
            if num_returns > 1:
                for i in range(num_returns):
                    param = Parameter(
                        content_type=parameter.TYPE.FILE,
                        direction=parameter.DIRECTION.OUT,
                    )
                    param.content = object()
                    self.returns[get_return_name(i)] = param
            else:
                param = Parameter(
                    content_type=parameter.TYPE.FILE, direction=parameter.DIRECTION.OUT
                )
                param.content = object()
                self.returns[get_return_name(0)] = param
            # Found return defined as type-hint
            return num_returns
        # else:
        #     # The user has not defined return as type-hint
        #     # So, continue searching as usual
        #     pass

        # Could not find type-hinting
        source_code = get_wrapped_source(function).strip()

        code = []  # type: list
        if self.first_arg_name == "self" or source_code.startswith("@classmethod"):
            # It is a task defined within a class (can not parse the code
            # with ast since the class does not exist yet).
            # Alternatively, the only way I see is to parse it manually
            # line by line.
            ret_mask = []
            code = source_code.split("\n")
            for line in code:
                if "return " in line:
                    ret_mask.append(True)
                else:
                    ret_mask.append(False)
        else:
            code = list(ast.walk(ast.parse(source_code)))
            ret_mask = [isinstance(node, ast.Return) for node in code]

        if any(ret_mask):
            has_multireturn = False
            lines = [i for i, li in enumerate(ret_mask) if li]
            max_num_returns = 0
            if self.first_arg_name == "self" or source_code.startswith("@classmethod"):
                # Parse code as string (it is a task defined within a class)
                def _has_multireturn(statement: str) -> bool:
                    parsed_ret = ast.parse(statement.strip())  # type: typing.Any
                    try:
                        if len(parsed_ret.body[0].value.elts) > 1:
                            return True
                        return False
                    except (KeyError, AttributeError):
                        # KeyError: "elts" means that it is a multiple return.
                        # "Ask forgiveness not permission"
                        return False

                def _get_return_elements(statement: str) -> int:
                    parsed_ret = ast.parse(statement.strip())  # type: typing.Any
                    return len(parsed_ret.body[0].value.elts)

                for i in lines:
                    if _has_multireturn(code[i]):
                        has_multireturn = True
                        num_returns = _get_return_elements(code[i])
                        if num_returns > max_num_returns:
                            max_num_returns = num_returns
            else:
                # Parse code AST (it is not a task defined within a class)
                for i in lines:
                    try:
                        if "elts" in code[i].value.__dict__:  # noqa
                            has_multireturn = True
                            num_returns = len(code[i].value.__dict__["elts"])  # noqa
                            if num_returns > max_num_returns:
                                max_num_returns = num_returns
                    except (KeyError, AttributeError):
                        # KeyError: "elts" means that it is a multiple return.
                        # "Ask forgiveness not permission"
                        pass
            if has_multireturn:
                for i in range(max_num_returns):
                    param = Parameter(
                        content_type=parameter.TYPE.FILE,
                        direction=parameter.DIRECTION.OUT,
                    )
                    param.content = object()
                    self.returns[get_return_name(i)] = param
            else:
                param = Parameter(
                    content_type=parameter.TYPE.FILE, direction=parameter.DIRECTION.OUT
                )
                param.content = object()
                self.returns[get_return_name(0)] = param
        else:
            # Return not found
            pass
        return len(self.returns)

    def _build_return_objects(self, num_returns: int) -> typing.Any:
        """Build the return objects.

        Build the return object from the self.return dictionary and include
        their filename in self.returns.
        Normally they are future objects, unless the user has defined a user
        defined class where an empty instance (needs an empty constructor)
        will be returned. This case will enable users to call tasks within
        user defined classes from future objects.

        WARNING: Updates self.returns dictionary.

        :param num_returns: Number of returned elements.
        :return: Future object/s.
        """
        future_object = None  # type: typing.Any
        if num_returns == 0:
            # No return - Return always None (as Python does)
            return future_object
        if num_returns == 1:
            # Simple return
            if __debug__:
                logger.debug("Simple object return found.")
            # Build the appropriate future object
            ret_value = self.returns[get_return_name(0)].content
            if type(ret_value) in _PYTHON_TO_COMPSS or ret_value in _PYTHON_TO_COMPSS:
                future_object = Future()  # primitives,string,dic,list,tuple
            elif inspect.isclass(ret_value):
                # For objects:
                # type of future has to be specified to allow o = func; o.func
                try:
                    future_object = ret_value()
                except TypeError:
                    logger.warning(
                        "Type %s does not have an empty constructor, "
                        "building generic future object",
                        str(ret_value),
                    )
                    future_object = Future()
            else:
                future_object = Future()  # modules, functions, methods
            _, ret_filename = OT.track(future_object)
            single_return = self.returns[get_return_name(0)]
            single_return.content_type = TYPE.FILE
            single_return.extra_content_type = "FILE"
            single_return.prefix = "#"
            single_return.file_name = COMPSsFile(ret_filename)
        else:
            # Multiple returns (the future object is a list of future objects)
            future_object = []
            if __debug__:
                logger.debug("Multiple objects return found.")
            for _, ret_v in self.returns.items():
                # Build the appropriate future object
                if ret_v.content in _PYTHON_TO_COMPSS:
                    # Primitives, string, dic, list, tuple
                    future_object_element = Future()
                elif inspect.isclass(ret_v.content):
                    # For objects: type of future has to be specified to allow:
                    # o = func; o.func
                    try:
                        future_object_element = ret_v.content()
                    except TypeError:
                        logger.warning(
                            "Type %s does not have an empty constructor, "
                            "building generic future object",
                            str(ret_v["Value"]),
                        )
                        future_object_element = Future()
                else:
                    # Modules, functions, methods
                    future_object_element = Future()
                future_object.append(future_object_element)
                _, ret_filename = OT.track(future_object_element)
                # Once determined the filename where the returns are going to
                # be stored, create a new Parameter object for each return
                # object
                return_k = ret_v
                return_k.content_type = TYPE.FILE
                return_k.extra_content_type = "FILE"
                return_k.prefix = "#"
                return_k.file_name = COMPSsFile(ret_filename)
        return future_object

    def _serialize_objects(self) -> None:
        """Infer COMPSs types for the task parameters and serialize them.

        :return: None.
        """
        # # Old school:
        # for k in self.parameters:
        #     self._serialize_object(k)
        # Allow concurrent serialization if python 3 and env. var:
        if "COMPSS_THREADED_SERIALIZATION" in os.environ:
            # Concurrent:
            with ThreadPoolExecutor() as executor:
                futures = []
                for k in self.parameters:
                    futures.append(executor.submit(self._serialize_object, k))
                wait(futures)
        else:
            # Sequential:
            for k in self.parameters:
                self._serialize_object(k)
            # Threaded: (somehow takes more time than sequential?)
            # threads = []
            # # Serialize each object in a different thread (non blocking IO)
            # for k in self.parameters:
            #     io_thread = threading.Thread(target=self._serialize_object,
            #                                  args=(k,))
            #     threads.append(io_thread)
            #     io_thread.start()
            # # Wait for all threads to finish
            # for thread in threads:
            #     thread.join()

    def _serialize_object(self, name: str) -> None:
        """Infer COMPSs types for a single task parameter and serializes it.

        WARNING: Updates self.parameters dictionary.

        :param name: Name of the element in self.parameters
        :return: None.
        """
        # 320k is usually the maximum size of all objects
        max_obj_arg_size = 320000 / 32
        with EventMaster(TRACING_MASTER.serialize_object):
            # Check user annotations concerning this argument
            param = self.parameters[name]
            if TASK_FEATURES.get_object_conversion():
                # Convert small objects to string if enabled
                # Check if the object is small in order not to serialize it.
                param = self._convert_parameter_obj_to_string(
                    name, param, max_obj_arg_size
                )
            else:
                # Serialize objects into files
                param = _serialize_object_into_file(name, param)
            # Update k parameter's Parameter object
            self.parameters[name] = param

            if __debug__:
                logger.debug(
                    "Final type for parameter %s: %d", name, param.content_type
                )

    def _build_values_types_directions(self) -> tuple:
        """Build the values, the values types and the values directions lists.

        Uses:
            - self.function_type: task function type. If it is an instance
                                  method, the first parameter will be put at
                                  the end.
            - self.parameters: <Dictionary> Function parameters.
            - self.returns: <Dictionary> - Function returns.
            - self.user_function.__code_strings__: <Boolean> Code strings
                                                   (or not).
        :return: List of values, their types, their directions, their streams
                 and their prefixes.
        """
        values = []
        names = []
        arg_names = list(self.parameters.keys())
        result_names = list(self.returns.keys())
        compss_types = []
        compss_directions = []
        compss_streams = []
        compss_prefixes = []
        extra_content_types = []
        slf_name = ""
        weights = []
        keep_renames = []
        code_strings = self.user_function.__code_strings__  # type: ignore

        # Build the range of elements
        if self.function_type in (
            FunctionType.INSTANCE_METHOD,
            FunctionType.CLASS_METHOD,
        ):
            slf_name = arg_names.pop(0)
        # Fill the values, compss_types, compss_directions, compss_streams and
        # compss_prefixes from function parameters
        for name in arg_names:
            (
                value,
                typ,
                direction,
                stream,
                prefix,
                con_type,
                weight,
                keep_rename,
            ) = _extract_parameter(self.parameters[name], code_strings)
            if isinstance(value, COMPSsFile):
                values.append(value.original_path)
            else:
                values.append(value)
            compss_types.append(typ)
            compss_directions.append(direction)
            compss_streams.append(stream)
            compss_prefixes.append(prefix)
            names.append(name)
            extra_content_types.append(con_type)
            weights.append(weight)
            keep_renames.append(keep_rename)
        # Fill the values, compss_types, compss_directions, compss_streams and
        # compss_prefixes from self (if exist)
        if self.function_type == FunctionType.INSTANCE_METHOD:
            # self is always an object
            (
                value,
                typ,
                direction,
                stream,
                prefix,
                con_type,
                weight,
                keep_rename,
            ) = _extract_parameter(self.parameters[slf_name], code_strings)
            if isinstance(value, COMPSsFile):
                values.append(value.original_path)
            else:
                values.append(value)
            compss_types.append(typ)
            compss_directions.append(direction)
            compss_streams.append(stream)
            compss_prefixes.append(prefix)
            names.append(slf_name)
            extra_content_types.append(con_type)
            weights.append(weight)
            keep_renames.append(keep_rename)

        # Fill the values, compss_types, compss_directions, compss_streams and
        # compss_prefixes from function returns
        for return_key in self.returns:
            return_param = self.returns[return_key]
            if isinstance(return_param.file_name, COMPSsFile):
                values.append(return_param.file_name.original_path)
            else:
                values.append(return_param.file_name)
            compss_types.append(return_param.content_type)
            compss_directions.append(return_param.direction)
            compss_streams.append(return_param.stream)
            compss_prefixes.append(return_param.prefix)
            names.append(result_names.pop(0))
            extra_content_types.append(return_param.extra_content_type)
            weights.append(return_param.weight)
            keep_renames.append(return_param.keep_rename)

        return (
            values,
            names,
            compss_types,
            compss_directions,
            compss_streams,
            compss_prefixes,
            extra_content_types,
            weights,
            keep_renames,
        )

    @staticmethod
    def _convert_parameter_obj_to_string(
        name: str, param: Parameter, max_obj_arg_size: float
    ) -> Parameter:
        """Convert object to string.

        Convert small objects into strings that can fit into the task
        parameters call.

        :param name: Parameter name.
        :param param: Parameter.
        :param max_obj_arg_size: Max size of the object to be converted.
        :return: The object possibly converted to string.
        """
        is_future = param.is_future
        base_string = str
        num_bytes = 0
        # Check if the object is small in order to serialize it.
        # Evaluates the size of the object before serializing the object.
        # Warning: calculate the size of a python object can be difficult
        # in terms of time and precision.
        if (
            param.content_type == TYPE.OBJECT
            and not is_future
            and param.direction == DIRECTION.IN
            and not isinstance(param.content, base_string)
        ):
            # check object size - The following line does not work
            # properly with recursive objects
            # bytes = sys.getsizeof(param.content)
            num_bytes = total_sizeof(param.content)
            if __debug__:
                megabytes = num_bytes / 1000000  # truncate
                logger.debug("Object size %d bytes (%d Mb).", num_bytes, megabytes)
            if num_bytes < max_obj_arg_size:
                # be careful... more than this value produces:
                # Cannot run program "/bin/bash"...: error=7, \
                # The arguments list is too long
                if __debug__:
                    logger.debug("The object size is less than 320 kb.")
                try:
                    value = pickle.dumps(param.content)
                    if TASK_FEATURES.get_prepend_strings():
                        # new_content = value.decode(CONSTANTS.str_escape)
                        param.content = f"#HiddenObj#{value}"  # type: ignore
                    param.content_type = TYPE.STRING_64
                    param.extra_content_type = str(type("str"))
                    if __debug__:
                        logger.debug(
                            "Inferred type modified (Object converted to String)."
                        )
                except SerializerException as serializer_exception:
                    raise PyCOMPSsException(
                        "The object cannot be converted due to: not serializable."
                    ) from serializer_exception
        else:
            if __debug__:
                logger.warning("Could not serialize object to string conversion")
            param = _serialize_object_into_file(name, param)

        return param


def _get_object_property(param_ref: list, obj: typing.Any) -> typing.Any:
    """Get the object property from the given parameter references.

    :param param_ref: List of parameter references.
    :param obj: Target object.
    :returns: The object property from the given parameters.
    """
    if len(param_ref) == 1:
        return obj
    return _get_object_property(param_ref[1:], getattr(obj, param_ref[1]))


def _manage_persistent_object(param: Parameter) -> None:
    """Manage a persistent object within a Parameter.

    Does the necessary actions over a persistent object used as task parameter.
    In particular, saves the object id provided by the persistent storage
    (getID()) into the pending_to_synchronize dictionary.

    :param param: Parameter.
    :return: None.
    """
    param.content_type = TYPE.EXTERNAL_PSCO
    obj_id = str(get_id(param.content))
    OT.set_pending_to_synchronize(obj_id)
    param.content = obj_id
    if __debug__:
        logger.debug("Managed persistent object: %s", obj_id)


def _serialize_object_into_file(
    name: str, param: Parameter, code_strings=True
) -> Parameter:
    """Serialize an object into a file if necessary.

    :param name: Name of the object.
    :param param: Parameter.
    :return: Parameter (whose type and value might be modified).
    """
    if (
        param.content_type == TYPE.OBJECT
        or param.content_type == TYPE.EXTERNAL_STREAM
        or param.is_future
    ):
        # 2nd condition: real type can be primitive, but now it's acting as a
        # future (object)
        try:
            val_type = type(param.content)
            if isinstance(val_type, list) and any(
                isinstance(value, Future) for value in param.content
            ):
                # Is there a future object within the list?
                if __debug__:
                    logger.debug(
                        "Found a list that contains future objects - synchronizing..."
                    )
                mode = get_compss_direction("in")
                param.content = list(
                    map(wait_on, param.content, [mode] * len(param.content))
                )
            _skip_file_creation = (
                param.direction == DIRECTION.OUT
                and param.content_type != TYPE.EXTERNAL_STREAM
            )
            _turn_into_file(param, name, skip_creation=_skip_file_creation)
        except SerializerException:
            import traceback

            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.exception(
                "Pickling error exception: non-serializable object found as a parameter."
            )
            logger.exception("".join(line for line in lines))
            print(
                "[ ERROR ]: Non serializable objects can not be used as parameters (e.g. methods)."
            )
            print(f"[ ERROR ]: Object: {param.content}")
            # Raise the exception up tu launch.py in order to point where the
            # error is in the user code.
            raise
    elif param.content_type == TYPE.EXTERNAL_PSCO:
        _manage_persistent_object(param)
    elif param.content_type == TYPE.INT:
        if param.content > JAVA_MAX_INT or param.content < JAVA_MIN_INT:
            # This must go through Java as a long to prevent overflow with
            # Java integer
            param.content_type = TYPE.LONG
    elif param.content_type == TYPE.LONG:
        if param.content > JAVA_MAX_LONG or param.content < JAVA_MIN_LONG:
            # This must be serialized to prevent overflow with Java long
            param.content_type = TYPE.OBJECT
            _skip_file_creation = param.direction == DIRECTION.OUT
            _turn_into_file(param, name, _skip_file_creation)
    elif param.content_type in (TYPE.STRING, TYPE.STRING_64):
        # Do not move this import to the top
        if TASK_FEATURES.get_prepend_strings():
            # Strings can be empty. If a string is empty their base64 encoding
            # will be empty.
            # So we add a leading character to it to make it non empty
            param.content = f"#{param.content}"
    elif param.content_type == TYPE.COLLECTION:
        # Just make contents available as serialized files (or objects)
        # We will build the value field later
        # (which will be used to reconstruct the collection in the worker)
        if param.is_file_collection:
            new_object_col = [
                Parameter(
                    content=x,
                    content_type=TYPE.FILE,
                    direction=param.direction,
                    file_name=COMPSsFile(x),
                    depth=param.depth - 1,
                )
                for x in param.content
            ]
        else:
            new_object_col = [
                _serialize_object_into_file(
                    name,
                    Parameter(
                        content=x,
                        content_type=get_compss_type(
                            x, param.depth - 1, code_strings=code_strings
                        ),
                        direction=param.direction,
                        depth=param.depth - 1,
                        extra_content_type=str(type(x).__name__),
                    ),
                )
                for x in param.content
            ]
        param.content = new_object_col
        # Give this object an identifier inside the binding
        if param.direction != DIRECTION.IN_DELETE:
            _, _ = OT.track(param.content, obj_name=name, collection=True)
    elif param.content_type == TYPE.DICT_COLLECTION:
        # Just make contents available as serialized files (or objects)
        # We will build the value field later
        # (which will be used to reconstruct the collection in the worker)
        new_object_dict = {}
        for p_key, p_value in param.content.items():
            key = _serialize_object_into_file(
                name,
                Parameter(
                    content=p_key,
                    content_type=get_compss_type(
                        p_key, param.depth - 1, code_strings=code_strings
                    ),
                    direction=param.direction,
                    depth=param.depth - 1,
                    extra_content_type=str(type(param).__name__),
                ),
            )
            value = _serialize_object_into_file(
                name,
                Parameter(
                    content=p_value,
                    content_type=get_compss_type(
                        p_value, param.depth - 1, code_strings=code_strings
                    ),
                    direction=param.direction,
                    depth=param.depth - 1,
                    extra_content_type=str(type(p_value).__name__),
                ),
            )
            new_object_dict[key] = value
        param.content = new_object_dict
        # Give this object an identifier inside the binding
        if param.direction != DIRECTION.IN_DELETE:
            _, _ = OT.track(param.content, obj_name=name, collection=True)
    return param


def _turn_into_file(param: Parameter, name: str, skip_creation: bool = False) -> None:
    """Write a object into a file if the object has not been already written.

    Consults the obj_id_to_filename to check if it has already been written
    (reuses it if exists). If not, the object is serialized to file and
    registered in the obj_id_to_filename dictionary.
    This functions stores the object into pending_to_synchronize.

    :param param: Wrapper of the object to turn into file.
    :param name: Name of the object.
    :param skip_creation: Skips the serialization to file.
    :return: None.
    """
    obj_id = OT.is_tracked(param.content)
    if obj_id == "":
        # This is the first time a task accesses this object
        if param.direction == DIRECTION.IN_DELETE:
            obj_id, file_name = OT.not_track()
        else:
            obj_id, file_name = OT.track(param.content, obj_name=name)
        if not skip_creation:
            serialize_to_file(param.content, file_name)
    else:
        file_name = OT.get_file_name(obj_id)
        if OT.has_been_written(obj_id):
            if param.direction in (DIRECTION.INOUT, DIRECTION.COMMUTATIVE):
                OT.set_pending_to_synchronize(obj_id)
            # Main program generated the last version
            compss_file = OT.pop_written_obj(obj_id)
            if __debug__:
                logger.debug("Serializing object %s to file %s", obj_id, compss_file)
            if not skip_creation:
                serialize_to_file(param.content, compss_file)
    # Set file name in Parameter object
    param.file_name = COMPSsFile(file_name)


def _extract_parameter(
    param: Parameter, code_strings: bool, collection_depth: int = 0
) -> tuple:
    """Extract the information of a single parameter.

    :param param: Parameter object.
    :return: value, typ, direction, stream, prefix, extra_content_type, weight,
            keep_rename of the given parameter.
    """
    con_type = UNDEFINED_CONTENT_TYPE

    if param.content_type == TYPE.STRING_64 and not param.is_future:
        # Encode the string in order to preserve the source
        # Checks that it is not a future (which is indicated with a path)
        # Considers multiple spaces between words
        param.content = b64encode(param.content.encode()).decode()
        if len(param.content) == 0:
            # Empty string - use escape string to avoid padding
            # Checked and substituted by empty string in the worker.py and
            # piper_worker.py
            param.content = b64encode(
                CONSTANTS.empty_string_key.encode()
            ).decode()  # noqa: E501
        con_type = CONSTANTS.extra_content_type_format.format(
            "builtins", str(param.content.__class__.__name__)
        )
    elif param.content_type == TYPE.STRING and not param.is_future:  # noqa: E501
        if len(param.content) == 0:
            param.content = CONSTANTS.empty_string_key
        con_type = CONSTANTS.extra_content_type_format.format(
            "builtins", str(param.content.__class__.__name__)
        )

    typ = -1  # type: int
    value = None  # type: typing.Any
    if param.content_type == TYPE.FILE or param.is_future:
        # If the parameter is a file or is future, the content is in a file
        # and we register it as file
        value = param.file_name
        # todo: make sure it works with FO
        con_type = str(Future.__name__) if param.is_future else "FILE"
        value_str = str(value)
        if isinstance(value, str):
            value_str = value
        if isinstance(value, COMPSsFile):
            value_str = value.original_path
        if value_str != "None":
            typ = TYPE.FILE
        else:
            typ = TYPE.NULL
    elif param.content_type == TYPE.DIRECTORY:
        value = param.file_name
        value_str = str(value)
        if isinstance(value, str):
            value_str = value
        if isinstance(value, COMPSsFile):
            value_str = value.original_path
        if value_str != "None":
            typ = TYPE.DIRECTORY
        else:
            typ = TYPE.NULL
    elif param.content_type == TYPE.OBJECT:
        # If the parameter is an object, its value is stored in a file and
        # we register it as file
        value = param.file_name
        typ = TYPE.FILE
        try:
            _mf = sys.modules[param.content.__class__.__module__].__file__
        except AttributeError:
            # "builtin" modules do not have __file__ attribute!
            _mf = "builtins"
        _class_name = str(param.content.__class__.__name__)
        con_type = CONSTANTS.extra_content_type_format.format(_mf, _class_name)
    elif param.content_type == TYPE.EXTERNAL_STREAM:
        # If the parameter type is stream, its value is stored in a file, but
        # we keep the type
        value = param.file_name
        typ = TYPE.EXTERNAL_STREAM
    elif param.content_type == TYPE.COLLECTION or (
        collection_depth > 0 and is_basic_iterable(param.content)
    ):
        # An object will be considered a collection if at least one of the
        # following is true:
        #     1) We said it is a collection in the task decorator
        #     2) It is part of some collections object, it is iterable, and we
        #        are inside the specified depth radius
        #
        # The content of a collection is sent via JNI to the master, and the
        # format is:
        # collectionId numberOfElements collectionPyContentType
        #     type1 Id1 pyType1
        #     type2 Id2 pyType2
        #     ...
        #     typeN IdN pyTypeN
        _class_name = str(param.content.__class__.__name__)
        con_type = CONSTANTS.extra_content_type_format.format("collection", _class_name)
        value = f"{OT.is_tracked(param.content)} {len(param.content)} {con_type}"
        OT.stop_tracking(param.content, collection=True)
        typ = TYPE.COLLECTION
        for (_, x_param) in enumerate(param.content):
            x_value, x_type, _, _, _, x_con_type, _, _ = _extract_parameter(
                x_param, code_strings, param.depth - 1
            )
            if isinstance(x_value, COMPSsFile):
                value += f" {x_type} {x_value.original_path} {x_con_type}"
            else:
                value += f" {x_type} {x_value} {x_con_type}"
    elif param.content_type == TYPE.DICT_COLLECTION or (
        collection_depth > 0 and is_dict(param.content)
    ):
        # An object will be considered a dictionary collection if at least one
        # of the following is true:
        #     1) We said it is a dictionary collection in the task decorator
        #     2) It is part of some collection object, it is dict and we
        #        are inside the specified depth radius
        #
        # The content of a dictionary collection is sent via JNI to the master,
        # and the format is:
        # dictCollectionId numberOfEntries dictCollectionPyContentType
        #     type1(key)   Id1(key)   pyType1(key)
        #     type1(value) Id1(value) pyType1(value)
        #     type2(key)   Id2(key)   pyType2(key)
        #     type2(value) Id2(value) pyType2(value)
        #     ...
        #     typeN(value) IdN(value) pyTypeN(value)
        _class_name = str(param.content.__class__.__name__)
        con_type = CONSTANTS.extra_content_type_format.format(
            "dict_collection", _class_name
        )
        value = f"{OT.is_tracked(param.content)} {len(param.content)} {con_type}"
        OT.stop_tracking(param.content, collection=True)
        typ = TYPE.DICT_COLLECTION
        for k_param, v_param in param.content.items():  # noqa
            k_value, k_type, _, _, _, k_con_type, _, _ = _extract_parameter(
                k_param, code_strings, param.depth - 1
            )
            real_k_type = k_type
            if isinstance(k_type, COMPSsFile):
                real_k_type = k_type.original_path
            real_k_value = k_value
            if isinstance(k_value, COMPSsFile):
                real_k_value = k_value.original_path
            if k_con_type != con_type:

                value = f"{value} {real_k_type} {real_k_value} {k_con_type}"
            else:
                # remove last dict_collection._classname if key is a dict_collection  # noqa: E501
                value = f"{value} {real_k_type} {real_k_value}"
            v_value, v_type, _, _, _, v_con_type, _, _ = _extract_parameter(
                v_param, code_strings, param.depth - 1
            )
            real_v_type = v_type
            if isinstance(v_type, COMPSsFile):
                real_v_type = v_type.original_path
            real_v_value = v_value
            if isinstance(v_value, COMPSsFile):
                real_v_value = v_value.original_path
            if v_con_type != con_type:
                value = f"{value} {real_v_type} {real_v_value} {v_con_type}"
            else:
                # remove last dict_collection._classname if value is a dict_collection  # noqa: E501
                value = f"{value} {real_v_type} {real_v_value}"
    else:
        # Keep the original value and type
        value = param.content
        typ = param.content_type

    # Get direction, stream and prefix
    direction = param.direction
    # Get stream and prefix
    stream = param.stream
    prefix = param.prefix
    # Get weights and keep rename
    weight = param.weight
    keep_rename = param.keep_rename

    return value, typ, direction, stream, prefix, con_type, weight, keep_rename
