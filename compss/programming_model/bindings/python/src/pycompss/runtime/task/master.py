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

from __future__ import print_function
import os
import sys
import ast
import threading
import inspect
import base64
from collections import OrderedDict
from collections import deque

from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.error_msgs import cast_env_to_int_error
from pycompss.api.parameter import TYPE
from pycompss.api.parameter import DIRECTION
from pycompss.runtime.binding import wait_on
from pycompss.runtime.commons import IS_PYTHON3  # noqa
from pycompss.runtime.commons import EMPTY_STRING_KEY
from pycompss.runtime.commons import STR_ESCAPE
from pycompss.runtime.commons import EXTRA_CONTENT_TYPE_FORMAT
from pycompss.runtime.commons import INTERACTIVE_FILE_NAME
from pycompss.runtime.commons import range
from pycompss.runtime.commons import get_object_conversion
from pycompss.runtime.constants import EXTRACT_CORE_ELEMENT
from pycompss.runtime.constants import INSPECT_FUNCTION_ARGUMENTS
from pycompss.runtime.constants import PROCESS_PARAMETERS
from pycompss.runtime.constants import GET_FUNCTION_INFORMATION
from pycompss.runtime.constants import PREPARE_CORE_ELEMENT
from pycompss.runtime.constants import GET_FUNCTION_SIGNATURE
from pycompss.runtime.constants import UPDATE_CORE_ELEMENT
from pycompss.runtime.constants import PROCESS_RETURN
from pycompss.runtime.constants import PROCESS_OTHER_ARGUMENTS
from pycompss.runtime.constants import BUILD_RETURN_OBJECTS
from pycompss.runtime.constants import SERIALIZE_OBJECTS
from pycompss.runtime.constants import BUILD_COMPSS_TYPES_DIRECTIONS
from pycompss.runtime.constants import ATTRIBUTES_CLEANUP
from pycompss.runtime.management.direction import get_compss_direction
from pycompss.runtime.management.object_tracker import OT_track
from pycompss.runtime.management.object_tracker import \
    OT_set_pending_to_synchronize
from pycompss.runtime.management.object_tracker import OT_is_tracked
from pycompss.runtime.management.object_tracker import OT_get_file_name
from pycompss.runtime.management.object_tracker import OT_has_been_written
from pycompss.runtime.management.object_tracker import OT_pop_written_obj
from pycompss.runtime.management.object_tracker import OT_stop_tracking
from pycompss.runtime.management.object_tracker import OT_not_track
from pycompss.runtime.task.commons import TaskCommons
from pycompss.runtime.task.core_element import CE
from pycompss.runtime.task.parameter import Parameter
from pycompss.runtime.task.parameter import get_parameter_copy
from pycompss.runtime.task.parameter import get_compss_type
from pycompss.runtime.task.parameter import UNDEFINED_CONTENT_TYPE
from pycompss.runtime.task.parameter import JAVA_MIN_INT
from pycompss.runtime.task.parameter import JAVA_MAX_INT
from pycompss.runtime.task.parameter import JAVA_MIN_LONG
from pycompss.runtime.task.parameter import JAVA_MAX_LONG
from pycompss.runtime.task.arguments import get_return_name
from pycompss.runtime.task.arguments import get_kwarg_name
from pycompss.runtime.task.arguments import get_vararg_name
from pycompss.runtime.task.arguments import get_name_from_kwarg
from pycompss.runtime.task.arguments import is_vararg
from pycompss.runtime.task.arguments import is_kwarg
from pycompss.runtime.management.classes import FunctionType
from pycompss.runtime.management.classes import Future
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.interactive.helpers import update_tasks_code_file
from pycompss.util.serialization.serializer import serialize_to_string
from pycompss.util.serialization.serializer import serialize_to_file
from pycompss.util.serialization.serializer import SerializerException
from pycompss.util.objects.properties import get_module_name
from pycompss.util.objects.sizer import total_sizeof
from pycompss.util.storages.persistent import get_id
from pycompss.util.objects.properties import is_basic_iterable
from pycompss.util.objects.properties import is_dict
from pycompss.util.objects.properties import get_wrapped_source
import pycompss.api.parameter as parameter
import pycompss.util.context as context
import pycompss.runtime.binding as binding
from pycompss.util.tracing.helpers import event

import logging
logger = logging.getLogger(__name__)

# Types conversion dictionary from python to COMPSs
if IS_PYTHON3:
    _PYTHON_TO_COMPSS = {int: TYPE.INT,  # int # long
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
                         object: TYPE.OBJECT
                         }
else:
    import types
    _PYTHON_TO_COMPSS = {types.IntType: TYPE.INT,          # noqa # int
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
                         # The type of tuples (e.g. (1, 2, 3, "Spam"))
                         types.TupleType: TYPE.OBJECT,     # noqa
                         # The type of lists (e.g. [0, 1, 2, 3])
                         types.ListType: TYPE.OBJECT,
                         # The type of dictionaries (e.g. {"Bacon":1,"Ham":0})
                         types.DictType: TYPE.OBJECT,
                         # The type of generic objects
                         types.ObjectType: TYPE.OBJECT     # noqa
                         }

MANDATORY_ARGUMENTS = {}
# List since the parameter names are included before checking for unexpected
# arguments (the user can define a=INOUT in the task decorator and this is not
# an unexpected argument)
SUPPORTED_ARGUMENTS = ["returns",
                       "priority",
                       "on_failure",
                       "time_out",
                       "is_replicated",
                       "is_distributed",
                       "varargs_type",
                       "target_direction",
                       "computing_nodes",
                       "is_reduce",
                       "chunk_size",
                       "numba",
                       "numba_flags",
                       "numba_signature",
                       "numba_declaration",
                       "tracing_hook"]
# Deprecated arguments. Still supported but shows a message when used.
DEPRECATED_ARGUMENTS = ["isReplicated",
                        "isDistributed",
                        "varargsType",
                        "targetDirection"]
# All supported arguments
ALL_SUPPORTED_ARGUMENTS = SUPPORTED_ARGUMENTS + DEPRECATED_ARGUMENTS
# Some attributes cause memory leaks, we must delete them from memory after
# master call
ATTRIBUTES_TO_BE_REMOVED = ["decorator_arguments",
                            "param_args",
                            "param_varargs",
                            "param_defaults",
                            "first_arg_name",
                            "parameters",
                            "returns",
                            "multi_return"]

# This lock allows tasks to be launched with the Threading module while
# ensuring that no attribute is overwritten
MASTER_LOCK = threading.Lock()


class TaskMaster(TaskCommons):
    """
    Task code for the Master:

    Process the task decorator and prepare all information to call binding
    runtime.
    """

    __slots__ = ["param_defaults",
                 "first_arg_name", "computing_nodes", "parameters",
                 "function_name", "module_name", "function_type", "class_name",
                 "returns", "multi_return",
                 "core_element", "registered", "signature",
                 "chunk_size", "is_reduce",
                 "interactive", "module", "function_arguments", "hints"]

    def __init__(self,
                 decorator_arguments,
                 user_function,
                 core_element,
                 registered,
                 signature,
                 interactive,
                 module,
                 function_arguments,
                 function_name,
                 module_name,
                 function_type,
                 class_name,
                 hints
                 ):
        # Initialize TaskCommons
        super(TaskMaster, self).__init__(decorator_arguments,
                                         user_function)
        # Add more argument related attributes that will be useful later
        self.param_defaults = None
        # Add function related attributed that will be useful later
        self.first_arg_name = None
        self.computing_nodes = None
        self.parameters = None
        self.function_name = function_name
        self.module_name = module_name
        self.function_type = function_type
        self.class_name = class_name
        # Add returns related attributes that will be useful later
        self.returns = None
        self.multi_return = False
        # Task won't be registered until called from the master for the first
        # time or have a different signature
        self.core_element = core_element
        self.registered = registered
        self.signature = signature
        # Reductions
        self.chunk_size = None
        self.is_reduce = False

        # Parameters that will come from previous tasks
        # TODO: These parameters could be within a "precalculated" parameters
        self.interactive = interactive
        self.module = module
        self.function_arguments = function_arguments
        self.hints = hints

    def call(self, *args, **kwargs):
        # type: (tuple, dict) -> (object, bool, str)
        """ Main task code at master side.

        This part deals with task calls in the master's side
        Also, this function must return an appropriate number of
        future objects that point to the appropriate objects/files.

        :return: A function that does "nothing" and returns futures if needed.
        """
        # This lock makes this decorator able to handle various threads
        # calling the same task concurrently
        MASTER_LOCK.acquire()

        # Check if we are in interactive mode and update if needed
        if not self.interactive:
            self.interactive, self.module = self.check_if_interactive()
        if self.interactive:
            self.update_if_interactive(self.module)

        # Extract the core element (has to be extracted before processing
        # the kwargs to avoid issues processing the parameters)
        with event(EXTRACT_CORE_ELEMENT, master=True):
            pre_defined_ce = self.extract_core_element(kwargs)

        # Inspect the user function, get information about the arguments and
        # their names. This defines self.param_args, self.param_varargs,
        # and self.param_defaults. And gives non-None default
        # values to them if necessary
        with event(INSPECT_FUNCTION_ARGUMENTS, master=True):
            if not self.function_arguments:
                self.function_arguments = self.inspect_user_function_arguments()                   # noqa: E501
            self.param_args, self.param_varargs, _, self.param_defaults = self.function_arguments  # noqa: E501
            # TODO: Why this fails with first python test?
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
                self.param_varargs = "varargs_type"
            if self.param_defaults is None:
                self.param_defaults = ()

        # Process the parameters, give them a proper direction
        with event(PROCESS_PARAMETERS, master=True):
            self.process_parameters(*args, **kwargs)

        # Compute the function path, class (if any), and name
        with event(GET_FUNCTION_INFORMATION, master=True):
            self.compute_user_function_information()

        # Prepare the core element registration information
        with event(PREPARE_CORE_ELEMENT, master=True):
            self.set_code_strings(self.user_function,
                                  self.core_element.get_impl_type())
        with event(GET_FUNCTION_SIGNATURE, master=True):
            impl_signature, impl_type_args = self.get_signature()

        # It is necessary to decide whether to register or not (the task may
        # be inherited, and in this case it has to be registered again with
        # the new implementation signature).
        if not self.registered or self.signature != impl_signature:
            with event(UPDATE_CORE_ELEMENT, master=True):
                self.update_core_element(impl_signature, impl_type_args,
                                         pre_defined_ce)
            self.register_task()
            self.registered = True
            self.signature = impl_signature

        # Did we call this function to only register the associated core
        # element? (This can happen with @implements)
        # Do not move this import:
        from pycompss.api.task import REGISTER_ONLY
        if REGISTER_ONLY:
            MASTER_LOCK.release()
            return (None, self.core_element, self.registered, self.signature,
                    self.interactive, self.module,
                    self.function_arguments, self.function_name,
                    self.module_name, self.function_type, self.class_name,
                    self.hints)

        with event(PROCESS_OTHER_ARGUMENTS, master=True):
            # Deal with dynamic computing nodes
            computing_nodes = self.process_computing_nodes()
            # Deal with reductions
            is_reduction, chunk_size = self.process_reduction()
            # Get other arguments if exist
            if not self.hints:
                self.hints = self.check_task_hints()
            is_replicated, is_distributed, on_failure, time_out, has_priority, has_target = self.hints  # noqa: E501

        # Deal with the return part.
        with event(PROCESS_RETURN, master=True):
            num_returns = self.add_return_parameters()
            if not self.returns:
                num_returns = self.update_return_if_no_returns(self.user_function)  # noqa: E501

        # Build return objects
        with event(BUILD_RETURN_OBJECTS, master=True):
            fo = None
            if self.returns:
                fo = self._build_return_objects(num_returns)

        # Infer COMPSs types from real types, except for files
        self._serialize_objects()

        # Build values and COMPSs types and directions
        with event(BUILD_COMPSS_TYPES_DIRECTIONS, master=True):
            vtdsc = self._build_values_types_directions()
            values, names, compss_types, compss_directions, compss_streams, \
            compss_prefixes, content_types, weights, keep_renames = vtdsc  # noqa

        # Signature and other parameters:
        # Get path
        if self.class_name == "":
            path = self.module_name
        else:
            path = ".".join((self.module_name, self.class_name))
        signature = ".".join((path, self.function_name))

        if __debug__:
            logger.debug("TASK: %s of type %s, in module %s, in class %s" %
                         (self.function_name, self.function_type,
                          self.module_name, self.class_name))

        # Process the task
        binding.process_task(
            signature,
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
            computing_nodes,
            is_reduction,
            chunk_size,
            is_replicated,
            is_distributed,
            on_failure,
            time_out
        )

        # Remove unused attributes from the memory
        with event(ATTRIBUTES_CLEANUP, master=True):
            for at in ATTRIBUTES_TO_BE_REMOVED:
                if hasattr(self, at):
                    delattr(self, at)

        # Release the lock
        MASTER_LOCK.release()

        # Return the future object/s corresponding to the task
        # This object will substitute the user expected return from the task
        # and will be used later for synchronization or as a task parameter
        # (then the runtime will take care of the dependency).
        # Also return if the task has been registered and its signature,
        # so that future tasks of the same function register if necessary.
        return (fo, self.core_element, self.registered,
                self.signature, self.interactive, self.module,
                self.function_arguments,
                self.function_name,
                self.module_name, self.function_type, self.class_name,
                self.hints)

    def check_if_interactive(self):
        # type: () -> (bool, ...)
        """ Check if running in interactive mode.

        :return: True if interactive. False otherwise.
        """
        mod = inspect.getmodule(self.user_function)
        module_name = mod.__name__
        if context.in_pycompss() and \
                (module_name == "__main__" or
                 module_name == "pycompss.runtime.launch"):
            # 1.- The runtime is running.
            # 2.- The module where the function is defined was run as __main__.
            return True, mod
        else:
            return False, None

    def update_if_interactive(self, mod):
        # type: (...) -> None
        """ Update the code for jupyter notebook.

        Update the user code if in interactive mode and the session has
        been started.

        :param mod: Source module.
        :return: None
        """
        # We need to find out the real module name
        # Get the real module name from our launch.py APP_PATH global
        # variable
        # It is guaranteed that this variable will always exist because
        # this code is only executed when we know we are in the master
        path = getattr(mod, "APP_PATH")
        # Get the file name
        file_name = os.path.splitext(os.path.basename(path))[0]
        # Do any necessary pre processing action before executing any code
        if file_name.startswith(INTERACTIVE_FILE_NAME) and not self.registered:
            # If the file_name starts with "InteractiveMode" means that
            # the user is using PyCOMPSs from jupyter-notebook.
            # Convention between this file and interactive.py
            # In this case it is necessary to do a pre-processing step
            # that consists of putting all user code that may be executed
            # in the worker on a file.
            # This file has to be visible for all workers.
            update_tasks_code_file(self.user_function, path)
            print("Found task: " + str(self.user_function.__name__))

    def extract_core_element(self, kwargs):
        # type: (dict) -> (CE, bool)
        """ Get or instantiate the Task's core element.

        Extract the core element if created in a higher level decorator,
        uses an existing or creates a new one if does not.

        IMPORTANT! extract the core element from kwargs if pre-defined
                   in decorators defined on top of @task.

        :return: If previously created and if created in higher level decorator
        """
        pre_defined_ce = False
        upper_decorator = False
        if CORE_ELEMENT_KEY in kwargs:
            # Core element has already been created in a higher level decorator
            self.core_element = kwargs[CORE_ELEMENT_KEY]
            # Remove the core element from kwargs to avoid issues processing
            # the parameters (process_parameters function).
            kwargs.pop(CORE_ELEMENT_KEY)
            pre_defined_ce = True
            upper_decorator = True
        elif self.core_element:
            # A core element from previous task calls was saved.
            pre_defined_ce = True
        else:
            # No decorators over @task: instantiate an empty core element.
            self.core_element = CE()
        return pre_defined_ce, upper_decorator

    def inspect_user_function_arguments(self):
        # type: () -> tuple
        """ Get user function arguments.

        Inspect the arguments of the user function and store them.
        Read the names of the arguments and remember their order.
        We will also learn things like if the user function contained
        variadic arguments, named arguments and so on.
        This will be useful when pairing arguments with the direction
        the user has specified for them in the decorator.

        # The third return value was self.param_kwargs - not used (removed)

        :return: the attributes to be reused
        """
        try:
            arguments = self._getargspec(self.user_function)
        except TypeError:
            # This is a numba jit declared task
            arguments = self._getargspec(self.user_function.py_func)
        return arguments

    @staticmethod
    def _getargspec(function):
        """ Private method that retrieves the function argspec.

        :param function: Function to analyse.
        :return: args, varargs, keywords and defaults dictionaries.
        """
        if IS_PYTHON3:
            full_argspec = inspect.getfullargspec(function)
            as_args = full_argspec.args
            as_varargs = full_argspec.varargs
            as_keywords = None  # full_argspec.varkw  # not needed
            as_defaults = full_argspec.defaults
            return as_args, as_varargs, as_keywords, as_defaults
        else:
            # Using getargspec in python 2 (deprecated in python 3 in favour
            # of getfullargspec).
            return inspect.getargspec(function)  # noqa

    def process_parameters(self, *args, **kwargs):
        """ Process all the input parameters.

        Basically, processing means "build a dictionary of <name, parameter>,
        where each parameter has an associated Parameter object".
        This function also assigns default directions to parameters.

        :return: None, it only modifies self.parameters.
        """
        # If we have an MPI, COMPSs or MultiNode decorator above us we should
        # have computing_nodes as a kwarg, we should detect it and remove it.
        # Otherwise we set it to 1
        self.computing_nodes = kwargs.pop("computing_nodes", 1)
        # We take the reduce and chunk size set for the reduce decorator,
        # otherwise we set them to 0.
        self.is_reduce = kwargs.pop("is_reduce", False)
        self.chunk_size = kwargs.pop("chunk_size", 0)
        # It is important to know the name of the first argument to determine
        # if we are dealing with a class or instance method (i.e: first
        # argument is named self)
        self.first_arg_name = None

        # Process the positional arguments and fill self.parameters with
        # their corresponding Parameter object
        self.parameters = OrderedDict()

        # Some of these positional arguments may have been not
        # explicitly defined
        num_positionals = min(len(self.param_args), len(args))
        arg_names = self.param_args[:num_positionals]
        arg_objects = args[:num_positionals]
        if arg_names and self.first_arg_name is None:
            self.first_arg_name = arg_names[0]
        for (arg_name, arg_object) in zip(arg_names, arg_objects):
            self.parameters[arg_name] = self.build_parameter_object(arg_name,
                                                                    arg_object)

        # Check defaults
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
                    list(zip(list(reversed(self.param_args))[:num_defaults],
                             list(reversed(self.param_defaults))))):
                if arg_name not in self.parameters:
                    real_arg_name = get_kwarg_name(arg_name)
                    is_defined = arg_name in kwargs
                    self.parameters[real_arg_name] = \
                        self.build_parameter_object(real_arg_name,
                                                    default_value,
                                                    is_defined)

        # Process variadic and keyword arguments
        # Note that they are stored with custom names
        # This will allow us to determine the class of each parameter
        # and their order in the case of the variadic ones
        # Process the variadic arguments
        for (i, var_arg) in enumerate(args[num_positionals:]):
            arg_name = get_vararg_name(self.param_varargs, i)
            self.parameters[arg_name] = self.build_parameter_object(arg_name,
                                                                    var_arg)
        # Process keyword arguments
        for (name, value) in kwargs.items():
            arg_name = get_kwarg_name(name)
            self.parameters[arg_name] = self.build_parameter_object(arg_name,
                                                                    value)
        # Check the arguments - Look for mandatory and unexpected arguments
        supported_arguments = (ALL_SUPPORTED_ARGUMENTS + self.param_args)
        check_arguments(MANDATORY_ARGUMENTS,
                        DEPRECATED_ARGUMENTS,
                        supported_arguments,
                        list(self.decorator_arguments.keys()),
                        "@task")

    def build_parameter_object(self, arg_name, arg_object, is_defined=False):
        # type: (str, object, bool) -> Parameter
        """ Creates the Parameter object from an argument name and object.
        The is_defined variable indicates that the parameter is defined in
        the kwargs dictionary. Consequently, if creating the variadic parameter
        (default parameter), we know that it will be overridden and avoid
        to set the NULL content_type.

        :param arg_name: Argument name.
        :param arg_object: Argument object.
        :param is_defined: If the parameter is defined in the kwargs dictionary.
        :return: Parameter object.
        """
        # Is the argument a vararg? or a kwarg? Then check the direction
        # for varargs or kwargs
        if is_vararg(arg_name):
            param = get_parameter_copy(self.get_varargs_direction())
        elif is_kwarg(arg_name):
            real_name = get_name_from_kwarg(arg_name)
            default_direction = self.get_default_direction(real_name)
            param = self.decorator_arguments.get(real_name,
                                                 default_direction)
        else:
            # The argument is named, check its direction
            # Default value = IN if not class or instance method and
            #                 isModifier, INOUT otherwise
            # see self.get_default_direction
            # Note that if we have something like @task(self = IN) it
            # will have priority over the default
            # direction resolution, even if this implies a contradiction
            # with the target_direction flag
            default_direction = self.get_default_direction(arg_name)
            param = self.decorator_arguments.get(arg_name,
                                                 default_direction)

        # If the parameter is a FILE then its type will already be defined,
        # and get_compss_type will misslabel it as a parameter.TYPE.STRING
        if param.is_object():
            param.content_type = get_compss_type(arg_object)

        # If the parameter is a DIRECTORY or FILE update the file_name
        # or content type depending if object. Otherwise update content.
        if param.is_file() or param.is_directory():
            if arg_object:
                param.file_name = arg_object
            elif not is_defined:
                # is None: Used None for a FILE or DIRECTORY parameter path
                param.content_type = parameter.TYPE.NULL
            else:
                # Keep the same content_type (FILE or DIRECTORY)
                pass
        else:
            param.content = arg_object
        return param

    def compute_user_function_information(self):
        # type: () -> None
        """ Get the user function path and name.

        Compute the function path p and the name n such that
        "from p import n" imports self.user_function.

        :return: None, it just sets self.user_function_path and
                 self.user_function_name.
        """
        self.function_name = self.user_function.__name__
        # Get the module name (the x part "from x import y"), except for the
        # class name
        self.compute_module_name()
        # Get the function type (function, instance method, class method)
        self.compute_function_type()

    def compute_module_name(self):
        # type: () -> None
        """ Compute the user's function module name.

        There are various cases:
            1) The user function is defined in some file. This is easy, just
               get the module returned by inspect.getmodule.
            2) The user function is in the main module. Retrieve the file and
               build the import name from it.
            3) We are in interactive mode.

        :return: None, it just modifies self.module_name.
        """
        mod = inspect.getmodule(self.user_function)
        self.module_name = mod.__name__
        # If it is a task within a class, the module it will be where the one
        # where the class is defined, instead of the one where the task is
        # defined.
        # This avoids conflicts with task inheritance.
        if self.first_arg_name == "self":
            mod = inspect.getmodule(type(self.parameters["self"].content))
            self.module_name = mod.__name__
        elif self.first_arg_name == "cls":
            self.module_name = self.parameters["cls"].content.__module__
        if self.module_name == "__main__" or \
                self.module_name == "pycompss.runtime.launch":
            # The module where the function is defined was run as __main__,
            # We need to find out the real module name
            # Get the real module name from our launch.py APP_PATH global
            # variable
            # It is guaranteed that this variable will always exist because
            # this code is only executed when we know we are in the master
            path = getattr(mod, "APP_PATH")
            # Get the file name
            file_name = os.path.splitext(os.path.basename(path))[0]
            # Get the module
            self.module_name = get_module_name(path, file_name)

    def compute_function_type(self):
        # type: () -> None
        """ Compute user function type.

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
            self.class_name = type(self.parameters["self"].content).__name__
        elif self.first_arg_name == "cls":
            self.function_type = FunctionType.CLASS_METHOD
            self.class_name = self.parameters["cls"].content.__name__
        # Finally, check if the function type is really a module function or
        # a static method.
        # Static methods are ONLY supported with Python 3 due to __qualname__
        # feature, which enables to know to which class they belong.
        # The class name is needed in order to define properly the class_name
        # for the correct registration and later invoke.
        # Since these methods don't have self, nor cls, they are considered as
        # FUNCTIONS to the runtime
        if IS_PYTHON3:
            name = self.function_name
            qualified_name = self.user_function.__qualname__
            if name != qualified_name:
                # Then there is a class definition before the name in the
                # qualified name
                self.class_name = qualified_name[:-len(name) - 1]
                # -1 to remove the last point

    def set_code_strings(self, f, ce_type):
        # type: (..., str) -> None
        """ This function is used to set if the strings must be coded or not.

        IMPORTANT! modifies f adding __code_strings__ which is used in binding.

        :param f: Function to be registered.
        :param ce_type: Core element implementation type.
        :return: None
        """
        default = "METHOD"
        if ce_type is None:
            ce_type = default

        if ce_type == default or \
                ce_type == "PYTHON_MPI" or \
                ce_type == "MULTI_NODE":
            code_strings = True
        else:
            # MPI, BINARY, CONTAINER
            code_strings = False

        f.__code_strings__ = code_strings

        if __debug__:
            logger.debug("[@TASK] Task type of function %s in module %s: %s" %
                         (self.function_name, self.module_name, str(ce_type)))

    def get_signature(self):
        # type: () -> (str, list)
        """ This function is used to find out the function signature.

        The information is needed in order to compare the implementation
        signature, so that if it has been registered with a different
        signature, it can be re-registered with the new one (enable
        inheritance).

        :return: Implementation signature and implementation type arguments.
        """
        # Get the task signature
        # To do this, we will check the frames
        # frames = inspect.getouterframes(inspect.currentframe())
        app_frames = []
        # Ignore self.get_signature and call functions from the frame.
        # Sightly faster than inspect.currentframe().
        frame = sys._getframe(2)  # noqa
        try:
            while frame:
                # This loop does like inspect.getouterframes(frame)
                # but it is more efficient since it does only get the needed
                # information.
                frame_name = frame.f_code.co_name
                if frame_name == "compss_main":
                    break
                app_frames.append(frame_name)
                frame = frame.f_back
        finally:
            # Avoid reference cycles
            del frame

        if len(app_frames) == 1:
            # The task is defined within the main app file.
            # This case is never reached with Python 3 since it includes
            # frames that are not present with Python 2.
            impl_signature = ".".join((self.module_name, self.function_name))
            impl_type_args = [self.module_name, self.function_name]
        else:
            if self.class_name:
                # Within class or subclass
                impl_signature = ".".join((self.module_name,
                                           self.class_name,
                                           self.function_name))
                impl_type_args = [".".join((self.module_name,
                                            self.class_name)),
                                  self.function_name]
            else:
                # Not in a class or subclass
                # This case can be reached in Python 3, where particular
                # frames are included, but not class names found.
                impl_signature = ".".join((self.module_name, self.function_name))
                impl_type_args = [self.module_name, self.function_name]

        return impl_signature, impl_type_args

    def update_core_element(self, impl_signature, impl_type_args,
                            pre_defined_ce):
        # type: (str, list, tuple) -> None
        """ Adds the @task decorator information to the core element.

        CAUTION: Modifies the core_element parameter.

        :param impl_signature: Implementation signature.
        :param impl_type_args: Implementation type arguments.
        :param pre_defined_ce: Two boolean (if core element contains predefined
                               fields and if they have been predefined by
                               upper decorators).
        :return: None
        """
        pre_defined_ce, upper_decorator = pre_defined_ce

        # Include the registering info related to @task
        impl_type = "METHOD"
        impl_constraints = {}
        impl_io = False

        if __debug__:
            logger.debug("Configuring core element.")

        set_ce_signature = self.core_element.set_ce_signature
        set_impl_signature = self.core_element.set_impl_signature
        set_impl_type_args = self.core_element.set_impl_type_args
        set_impl_constraints = self.core_element.set_impl_constraints
        set_impl_type = self.core_element.set_impl_type
        set_impl_io = self.core_element.set_impl_io
        if pre_defined_ce:
            # Core element has already been created in an upper decorator
            # (e.g. @implements and @compss)
            get_ce_signature = self.core_element.get_ce_signature
            get_impl_constraints = self.core_element.get_impl_constraints
            get_impl_type = self.core_element.get_impl_type
            get_impl_type_args = self.core_element.get_impl_type_args
            get_impl_io = self.core_element.get_impl_io
            if get_ce_signature() is None:
                set_ce_signature(impl_signature)
                set_impl_signature(impl_signature)
            elif get_ce_signature() != impl_signature and not upper_decorator:
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
            if get_impl_constraints() is None:
                set_impl_constraints(impl_constraints)
            if get_impl_type() is None:
                set_impl_type(impl_type)
            if get_impl_type_args() is None:
                set_impl_type_args(impl_type_args)
            # Need to update impl_type_args if task is PYTHON_MPI and
            # if the parameter with layout exists.
            if get_impl_type() == "PYTHON_MPI":
                param_name = get_impl_type_args()[-4].strip()
                if param_name:
                    if param_name in self.parameters:
                        if self.parameters[param_name].content_type != parameter.TYPE.COLLECTION:      # noqa: E501
                            raise PyCOMPSsException("Parameter %s is not a collection!" % param_name)  # noqa: E501
                    else:
                        raise PyCOMPSsException("Parameter %s does not exist!" % param_name)           # noqa: E501
                set_impl_signature(".".join(("MPI", impl_signature)))
                set_impl_type_args(impl_type_args + get_impl_type_args()[1:])
            if get_impl_io() is None:
                set_impl_io(impl_io)
        else:
            # @task is in the top of the decorators stack.
            # Update the empty core_element
            set_ce_signature(impl_signature)
            set_impl_signature(impl_signature)
            set_impl_constraints(impl_constraints)
            set_impl_type(impl_type)
            set_impl_type_args(impl_type_args)
            set_impl_io(impl_io)

    def register_task(self):
        # type: () -> None
        """ This function is used to register the task in the runtime.

        This registration must be done only once on the task decorator
        initialization, unless there is a signature change (this will mean
        that the user has changed the implementation interactively).

        :return: None
        """
        if __debug__:
            logger.debug("[@TASK] Registering the function %s in module %s" %
                         (self.function_name, self.module_name))
        binding.register_ce(self.core_element)

    def process_computing_nodes(self):
        # type: () -> int
        """ Retrieve the number of computing nodes.

        This value can be defined by upper decorators and can also be defined
        dynamically defined with a global or environment variable.

        :return: The number of computing nodes.
        """
        parsed_computing_nodes = None
        if isinstance(self.computing_nodes, int):
            # Nothing to do
            parsed_computing_nodes = self.computing_nodes
        elif isinstance(self.computing_nodes, str):
            # Check if computing_nodes can be casted to string
            # Check if computing_nodes is an environment variable
            # Check if computing_nodes is a dynamic global variable
            try:
                # Cast string to int
                parsed_computing_nodes = int(self.computing_nodes)
            except ValueError:
                # Environment variable
                if self.computing_nodes.strip().startswith('$'):
                    # Computing nodes is an ENV variable, load it
                    env_var = self.computing_nodes.strip()[1:]  # Remove $
                    if env_var.startswith('{'):
                        env_var = env_var[1:-1]  # remove brackets
                    try:
                        parsed_computing_nodes = int(os.environ[env_var])
                    except ValueError:
                        raise PyCOMPSsException(
                            cast_env_to_int_error("ComputingNodes")
                        )
                else:
                    # Dynamic global variable
                    try:
                        # Load from global variables
                        parsed_computing_nodes = \
                            self.user_function.__globals__.get(
                                self.computing_nodes
                            )
                    except AttributeError:
                        # This is a numba jit declared task
                        try:
                            parsed_computing_nodes = \
                                self.user_function.py_func.__globals__.get(
                                    self.computing_nodes
                                )
                        except AttributeError:
                            # No more chances
                            # Ignore error and parsed_computing_nodes will
                            # raise the exception
                            pass
        if parsed_computing_nodes is None:
            raise PyCOMPSsException("ERROR: Wrong Computing Nodes value.")
        if parsed_computing_nodes <= 0:
            logger.warning("Registered computing_nodes is less than 1 (%s <= 0). Automatically set it to 1" %  # noqa
                           str(parsed_computing_nodes))
            parsed_computing_nodes = 1

        return parsed_computing_nodes

    def process_reduction(self):
        # type: () -> (bool, int)
        """ Process the reduction parameter.

        :return: Is reduction and chunk size.
        """
        # Deal with chunk size
        parsed_chunk_size = None
        if isinstance(self.chunk_size, int):
            # Nothing to do
            parsed_chunk_size = self.chunk_size
        elif isinstance(self.chunk_size, str):
            # Check if chunk_size can be casted to string
            # Check if chunk_size is an environment variable
            # Check if chunk_size is a dynamic global variable
            try:
                # Cast string to int
                parsed_chunk_size = int(self.chunk_size)
            except ValueError:
                # Environment variable
                if self.chunk_size.strip().startswith('$'):
                    # Chunk size is an ENV variable, load it
                    env_var = self.chunk_size.strip()[1:]  # Remove $
                    if env_var.startswith('{'):
                        env_var = env_var[1:-1]  # remove brackets
                    try:
                        parsed_chunk_size = int(os.environ[env_var])
                    except ValueError:
                        raise PyCOMPSsException(
                            cast_env_to_int_error('ChunkSize')
                        )
                else:
                    # Dynamic global variable
                    try:
                        # Load from global variables
                        parsed_chunk_size = \
                            self.user_function.__globals__.get(
                                self.chunk_size
                            )
                    except AttributeError:
                        # This is a numba jit declared task
                        try:
                            parsed_chunk_size = \
                                self.user_function.py_func.__globals__.get(
                                    self.chunk_size
                                )
                        except AttributeError:
                            # No more chances
                            # Ignore error and parsed_chunk_size will
                            # raise the exception
                            pass
        if parsed_chunk_size is None:
            parsed_chunk_size = 0

        # Deal with chunk size
        parsed_is_reduce = False
        if isinstance(self.is_reduce, bool):
            # Nothing to do
            parsed_is_reduce = self.is_reduce
        elif isinstance(self.is_reduce, str):
            # Check if is_reduce can be casted to string
            try:
                # Cast string to int
                parsed_is_reduce = bool(self.is_reduce)
            except ValueError:
                pass
        if parsed_is_reduce is None:
            parsed_is_reduce = False

        return parsed_is_reduce, parsed_chunk_size

    def check_task_hints(self):
        # type: () -> (bool, bool, bool, int, bool, bool)
        """ Process the @task hints.

        :return: The value of all possible hints.
        """
        deco_arg_getter = self.decorator_arguments.get
        if "isReplicated" in self.decorator_arguments:
            is_replicated = deco_arg_getter("isReplicated")
            logger.warning("Detected deprecated isReplicated. Please, change it to is_replicated")  # noqa: E501
        else:
            is_replicated = deco_arg_getter("is_replicated")
        # Get is distributed
        if "isDistributed" in self.decorator_arguments:
            is_distributed = deco_arg_getter("isDistributed")
            logger.warning(
                "Detected deprecated isDistributed. Please, change it to is_distributed")  # noqa: E501
        else:
            is_distributed = deco_arg_getter("is_distributed")
        # Get on failure
        if "onFailure" in self.decorator_arguments:
            on_failure = deco_arg_getter("onFailure")
            logger.warning("Detected deprecated onFailure. Please, change it to on_failure")  # noqa: E501
        else:
            on_failure = deco_arg_getter("on_failure")
        # Get time out
        if "timeOut" in self.decorator_arguments:
            time_out = deco_arg_getter("timeOut")
            logger.warning("Detected deprecated timeOut. Please, change it to time_out")  # noqa: E501
        else:
            time_out = deco_arg_getter("time_out")
        # Get priority
        has_priority = deco_arg_getter("priority")
        # Check if the function is an instance method or a class method.
        has_target = self.function_type == FunctionType.INSTANCE_METHOD

        return is_replicated, is_distributed, on_failure, time_out, has_priority, has_target  # noqa: E501

    def add_return_parameters(self):
        # type: () -> int
        """ Modify the return parameters accordingly to the return statement.

        :return: Creates and modifies self.returns and returns the number of
                 returns.
        """
        self.returns = OrderedDict()

        _returns = self.decorator_arguments["returns"]
        # Note that "returns" is by default False
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
        if isinstance(_returns, str):
            # Check if the returns statement contains an string with an
            # integer or a global variable.
            # In such case, build a list of objects of value length and
            # set it in ret_type.
            # Global variable or string wrapping integer value
            try:
                # Return is hidden by an int as a string.
                # i.e., returns="var_int"
                num_rets = int(_returns)
            except ValueError:
                # Return is hidden by a global variable. i.e., LT_ARGS
                try:
                    num_rets = self.user_function.__globals__.get(_returns)
                except AttributeError:
                    # This is a numba jit declared task
                    num_rets = self.user_function.py_func.__globals__.get(_returns)  # noqa: E501
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
                ret_type = get_compss_type(_returns)
                self.returns[get_return_name(0)] = \
                    Parameter(content=_returns,
                              content_type=ret_type,
                              direction=ret_dir)
            else:
                for (i, elem) in enumerate(to_return):
                    ret_type = get_compss_type(elem)
                    self.returns[get_return_name(i)] = \
                        Parameter(content=elem,
                                  content_type=ret_type,
                                  direction=ret_dir)
        else:
            ret_type = TYPE.OBJECT
            self.returns = {get_return_name(i): Parameter(content=None,
                                                          content_type=ret_type,  # noqa: E501
                                                          direction=ret_dir)
                            for i in range(to_return)}
        # Hopefully, an exception have been thrown if some invalid
        # stuff has been put in the returns field
        if defined_type:
            if to_return == 1:
                return to_return
            else:
                return len(to_return)  # noqa
        else:
            return to_return

    def update_return_if_no_returns(self, f):
        # type: (...) -> int
        """ Look for returns if no returns is specified.

        Checks the code looking for return statements if no returns is
        specified in @task decorator.

        WARNING: Updates self.return if returns are found.

        :param f: Function to check.
        :return: The number of return elements if found.
        """
        # Check type-hinting in python3
        if IS_PYTHON3:
            from typing import get_type_hints
            type_hints = get_type_hints(f)
            if "return" in type_hints:
                # There is a return defined as type-hint
                ret = type_hints["return"]
                try:
                    num_returns = len(ret)
                except TypeError:
                    # Is not iterable, so consider just 1
                    num_returns = 1
                if num_returns > 1:
                    for i in range(num_returns):
                        param = Parameter(content_type=parameter.TYPE.FILE,
                                          direction=parameter.DIRECTION.OUT)
                        param.content = object()
                        self.returns[get_return_name(i)] = param
                else:
                    param = Parameter(content_type=parameter.TYPE.FILE,
                                      direction=parameter.DIRECTION.OUT)
                    param.content = object()
                    self.returns[get_return_name(0)] = param
                # Found return defined as type-hint
                return num_returns
            else:
                # The user has not defined return as type-hint
                # So, continue searching as usual
                pass

        # It is python2 or could not find type-hinting
        source_code = get_wrapped_source(f).strip()

        if self.first_arg_name == "self" or \
                source_code.startswith("@classmethod"):
            # It is a task defined within a class (can not parse the code
            # with ast since the class does not exist yet).
            # Alternatively, the only way I see is to parse it manually
            # line by line.
            ret_mask = []
            code = source_code.split('\n')
            for line in code:
                if "return " in line:
                    ret_mask.append(True)
                else:
                    ret_mask.append(False)
        else:
            code = [node for node in ast.walk(ast.parse(source_code))]
            ret_mask = [isinstance(node, ast.Return) for node in code]

        if any(ret_mask):
            has_multireturn = False
            lines = [i for i, li in enumerate(ret_mask) if li]
            max_num_returns = 0
            if self.first_arg_name == "self" or \
                    source_code.startswith("@classmethod"):
                # Parse code as string (it is a task defined within a class)
                def _has_multireturn(statement):
                    v = ast.parse(statement.strip())
                    try:
                        if len(v.body[0].value.elts) > 1:
                            return True
                        else:
                            return False
                    except (KeyError, AttributeError):
                        # KeyError: "elts" means that it is a multiple return.
                        # "Ask forgiveness not permission"
                        return False

                def _get_return_elements(statement):
                    v = ast.parse(statement.strip())
                    return len(v.body[0].value.elts)

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
                    param = Parameter(content_type=parameter.TYPE.FILE,
                                      direction=parameter.DIRECTION.OUT)
                    param.content = object()
                    self.returns[get_return_name(i)] = param
            else:
                param = Parameter(content_type=parameter.TYPE.FILE,
                                  direction=parameter.DIRECTION.OUT)
                param.content = object()
                self.returns[get_return_name(0)] = param
        else:
            # Return not found
            pass
        return len(self.returns)

    def _build_return_objects(self, num_returns):
        # type: (int) -> object
        """ Build the return objects.

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
        fo = None
        if num_returns == 0:
            # No return
            return fo
        elif num_returns == 1:
            # Simple return
            if __debug__:
                logger.debug("Simple object return found.")
            # Build the appropriate future object
            ret_value = self.returns[get_return_name(0)].content
            if type(ret_value) in _PYTHON_TO_COMPSS or \
                    ret_value in _PYTHON_TO_COMPSS:
                fo = Future()  # primitives,string,dic,list,tuple
            elif inspect.isclass(ret_value):
                # For objects:
                # type of future has to be specified to allow o = func; o.func
                try:
                    fo = ret_value()
                except TypeError:
                    logger.warning("Type %s does not have an empty constructor, building generic future object" %  # noqa: E501
                                   str(ret_value))
                    fo = Future()
            else:
                fo = Future()  # modules, functions, methods
            _, ret_filename = OT_track(fo)
            single_return = self.returns[get_return_name(0)]
            single_return.content_type = TYPE.FILE
            single_return.prefix = '#'
            single_return.file_name = ret_filename
        else:
            # Multireturn
            fo = []
            if __debug__:
                logger.debug("Multiple objects return found.")
            for k, v in self.returns.items():
                # Build the appropriate future object
                if v.content in _PYTHON_TO_COMPSS:
                    foe = Future()  # primitives, string, dic, list, tuple
                elif inspect.isclass(v.content):
                    # For objects:
                    # type of future has to be specified to allow:
                    # o = func; o.func
                    try:
                        foe = v.content()
                    except TypeError:
                        logger.warning("Type %s does not have an empty constructor, building generic future object" %  # noqa: E501
                            str(v["Value"]))
                        foe = Future()
                else:
                    foe = Future()  # modules, functions, methods
                fo.append(foe)
                _, ret_filename = OT_track(foe)
                # Once determined the filename where the returns are going to
                # be stored, create a new Parameter object for each return
                # object
                return_k = self.returns[k]
                return_k.content_type = TYPE.FILE
                return_k.prefix = '#'
                return_k.file_name = ret_filename
        return fo

    def _serialize_objects(self):
        # type: () -> None
        """ Infer COMPSs types for the task parameters and serialize them.

        WARNING: Updates self.parameters dictionary.

        :return: Tuple of task_kwargs updated and a dictionary containing
                 if the objects are future elements.
        """
        max_obj_arg_size = 320000
        for k in self.parameters:
            with event(SERIALIZE_OBJECTS, master=True):
                # Check user annotations concerning this argument
                p = self.parameters[k]
                # Convert small objects to string if OBJECT_CONVERSION enabled
                # Check if the object is small in order not to serialize it.
                if get_object_conversion():
                    p, written_bytes = self._convert_parameter_obj_to_string(p,
                                                                             max_obj_arg_size,     # noqa: E501
                                                                             policy="objectSize")  # noqa: E501
                    max_obj_arg_size -= written_bytes
                else:
                    # Serialize objects into files
                    p = _serialize_object_into_file(k, p)
                # Update k parameter's Parameter object
                self.parameters[k] = p

                if __debug__:
                    logger.debug("Final type for parameter %s: %d" % (k, p.content_type))  # noqa: E501

    def _build_values_types_directions(self):
        # type: () -> (list, list, list, list, list)
        """
        Build the values list, the values types list and the values directions
        list.

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
        slf_name = None
        weights = []
        keep_renames = []
        code_strings = self.user_function.__code_strings__

        # Build the range of elements
        if self.function_type == FunctionType.INSTANCE_METHOD or \
                self.function_type == FunctionType.CLASS_METHOD:
            slf_name = arg_names.pop(0)
        # Fill the values, compss_types, compss_directions, compss_streams and
        # compss_prefixes from function parameters
        for name in arg_names:
            val, typ, direc, st, pre, ct, wght, kr = _extract_parameter(
                self.parameters[name],
                code_strings
            )
            values.append(val)
            compss_types.append(typ)
            compss_directions.append(direc)
            compss_streams.append(st)
            compss_prefixes.append(pre)
            names.append(name)
            extra_content_types.append(ct)
            weights.append(wght)
            keep_renames.append(kr)
        # Fill the values, compss_types, compss_directions, compss_streams and
        # compss_prefixes from self (if exist)
        if self.function_type == FunctionType.INSTANCE_METHOD:
            # self is always an object
            val, typ, direc, st, pre, ct, wght, kr = _extract_parameter(
                self.parameters[slf_name],
                code_strings
            )
            values.append(val)
            compss_types.append(typ)
            compss_directions.append(direc)
            compss_streams.append(st)
            compss_prefixes.append(pre)
            names.append(slf_name)
            extra_content_types.append(ct)
            weights.append(wght)
            keep_renames.append(kr)

        # Fill the values, compss_types, compss_directions, compss_streams and
        # compss_prefixes from function returns
        for r in self.returns.keys():
            p = self.returns[r]
            values.append(p.file_name)
            compss_types.append(p.content_type)
            compss_directions.append(p.direction)
            compss_streams.append(p.stream)
            compss_prefixes.append(p.prefix)
            names.append(result_names.pop(0))
            extra_content_types.append(p.extra_content_type)
            weights.append(p.weight)
            keep_renames.append(p.keep_rename)

        return values, names, compss_types, compss_directions, \
               compss_streams, compss_prefixes, extra_content_types, \
               weights, keep_renames  # noqa

    @staticmethod
    def _convert_parameter_obj_to_string(p,
                                         max_obj_arg_size,
                                         policy="objectSize"):
        # type: (Parameter, int, str) -> (Parameter, int)
        """ Convert object to string.

        Convert small objects into strings that can fit into the task
        parameters call.

        :param p: Parameter.
        :param max_obj_arg_size: max size of the object to be converted.
        :param policy: policy to use:
                       - "objectSize" for considering the size of the object.
                       - "serializedSize" for considering the size of the
                         object serialized.
        :return: the object possibly converted to string and it size in bytes.
        """
        is_future = p.is_future

        if IS_PYTHON3:
            base_string = str
        else:
            base_string = basestring  # noqa

        num_bytes = 0
        if policy == "objectSize":
            # Check if the object is small in order to serialize it.
            # This alternative evaluates the size of the object before
            # serializing the object.
            # Warning: calculate the size of a python object can be difficult
            # in terms of time and precision
            if (p.content_type == TYPE.OBJECT or p.content_type == TYPE.STRING) \
                    and not is_future \
                    and p.direction == DIRECTION.IN \
                    and not isinstance(p.content, base_string) \
                    and isinstance(p.content, (list, dict, tuple, deque, set, frozenset)):  # noqa: E501
                # check object size - The following line does not work
                # properly with recursive objects
                # bytes = sys.getsizeof(p.content)
                num_bytes = total_sizeof(p.content)
                if __debug__:
                    megabytes = num_bytes / 1000000  # truncate
                    logger.debug("Object size %d bytes (%d Mb)." % (num_bytes, megabytes))  # noqa: E501

                if num_bytes < max_obj_arg_size:
                    # be careful... more than this value produces:
                    # Cannot run program "/bin/bash"...: error=7, \
                    # The arguments list is too long
                    if __debug__:
                        logger.debug("The object size is less than 320 kb.")  # noqa: E501
                    real_value = p.content
                    try:
                        v = serialize_to_string(p.content)
                        p.content = v.encode(STR_ESCAPE)  # noqa
                        p.content_type = TYPE.STRING
                        if __debug__:
                            logger.debug("Inferred type modified (Object converted to String).")  # noqa: E501
                    except SerializerException:
                        p.content = real_value
                        p.content_type = TYPE.OBJECT
                        if __debug__:
                            logger.debug("The object cannot be converted due to: not serializable.")  # noqa: E501
                else:
                    p.content_type = TYPE.OBJECT
                    if __debug__:
                        logger.debug("Inferred type reestablished to Object.")  # noqa: E501
                        # if the parameter converts to an object, release
                        # the size to be used for converted objects?
                        # No more objects can be converted
                        # max_obj_arg_size += _bytes
                        # if max_obj_arg_size > 320000:
                        #     max_obj_arg_size = 320000
        elif policy == "serializedSize":
            if IS_PYTHON3:
                from pickle import PicklingError
            else:
                from cPickle import PicklingError  # noqa
            # Check if the object is small in order to serialize it.
            # This alternative evaluates the size after serializing the
            # parameter
            if (p.content_type == TYPE.OBJECT or p.content_type == TYPE.STRING) \
                    and not is_future \
                    and p.direction == DIRECTION.IN \
                    and not isinstance(p.content, base_string):
                real_value = p.content
                try:
                    v = serialize_to_string(p.content)
                    v = v.encode(STR_ESCAPE)  # noqa
                    # check object size
                    num_bytes = sys.getsizeof(v)
                    if __debug__:
                        megabytes = num_bytes / 1000000  # truncate
                        logger.debug("Object size %d bytes (%d Mb)." %
                                     (num_bytes, megabytes))
                    if num_bytes < max_obj_arg_size:
                        # be careful... more than this value produces:
                        # Cannot run program "/bin/bash"...: error=7,
                        # arguments list too long error.
                        if __debug__:
                            logger.debug("The object size is less than 320 kb")  # noqa: E501
                        p.content = v
                        p.content_type = TYPE.STRING
                        if __debug__:
                            logger.debug("Inferred type modified (Object converted to String).")  # noqa: E501
                    else:
                        p.content = real_value
                        p.content_type = TYPE.OBJECT
                        if __debug__:
                            logger.debug("Inferred type reestablished to Object.")  # noqa: E501
                            # if the parameter converts to an object,
                            # release the size to be used for converted
                            # objects?
                            # No more objects can be converted
                            # max_obj_arg_size += _bytes
                            # if max_obj_arg_size > 320000:
                            #     max_obj_arg_size = 320000
                except PicklingError:
                    p.content = real_value
                    p.content_type = TYPE.OBJECT
                    if __debug__:
                        logger.debug("The object cannot be converted due to: not serializable.")  # noqa: E501
        else:
            if __debug__:
                logger.debug("[ERROR] Wrong convert_objects_to_strings policy.")  # noqa: E501
            raise PyCOMPSsException("Wrong convert_objects_to_strings policy.")

        return p, num_bytes


def _manage_persistent_object(p):
    # type: (Parameter) -> None
    """ Manage a persistent object within a Parameter.

    Does the necessary actions over a persistent object used as task parameter.
    In particular, saves the object id provided by the persistent storage
    (getID()) into the pending_to_synchronize dictionary.

    :param p: Parameter.
    :return: None
    """
    p.content_type = TYPE.EXTERNAL_PSCO
    obj_id = get_id(p.content)
    OT_set_pending_to_synchronize(obj_id)
    p.content = obj_id
    if __debug__:
        logger.debug("Managed persistent object: %s" % obj_id)


def _serialize_object_into_file(name, p):
    # type: (str, Parameter) -> Parameter
    """ Serialize an object into a file if necessary.

    :param name: Name of the object.
    :param p: Parameter.
    :return: Parameter (whose type and value might be modified).
    """
    if p.content_type == TYPE.OBJECT or \
            p.content_type == TYPE.EXTERNAL_STREAM or \
            p.is_future:
        # 2nd condition: real type can be primitive, but now it's acting as a
        # future (object)
        try:
            val_type = type(p.content)
            if isinstance(val_type, list) and any(isinstance(v, Future) for v in p.content):
                # Is there a future object within the list?
                if __debug__:
                    logger.debug("Found a list that contains future objects - synchronizing...")  # noqa: E501
                mode = get_compss_direction("in")
                p.content = list(map(wait_on,
                                     p.content,
                                     [mode] * len(p.content)))
            _skip_file_creation = (p.direction == DIRECTION.OUT and
                                   p.content_type != TYPE.EXTERNAL_STREAM)
            _turn_into_file(p, skip_creation=_skip_file_creation)
        except SerializerException:
            import traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type,
                                               exc_value,
                                               exc_traceback)
            logger.exception("Pickling error exception: non-serializable object found as a parameter.")  # noqa: E501
            logger.exception("".join(line for line in lines))
            print("[ ERROR ]: Non serializable objects can not be used as parameters (e.g. methods).")  # noqa: E501
            print("[ ERROR ]: Object: %s" % p.content)
            # Raise the exception up tu launch.py in order to point where the
            # error is in the user code.
            raise
    elif p.content_type == TYPE.EXTERNAL_PSCO:
        _manage_persistent_object(p)
    elif p.content_type == TYPE.INT:
        if p.content > JAVA_MAX_INT or p.content < JAVA_MIN_INT:
            # This must go through Java as a long to prevent overflow with
            # Java integer
            p.content_type = TYPE.LONG
    elif p.content_type == TYPE.LONG:
        if p.content > JAVA_MAX_LONG or p.content < JAVA_MIN_LONG:
            # This must be serialized to prevent overflow with Java long
            p.content_type = TYPE.OBJECT
            _skip_file_creation = (p.direction == DIRECTION.OUT)
            _turn_into_file(p, _skip_file_creation)
    elif p.content_type == TYPE.STRING:
        # Do not move this import to the top
        from pycompss.api.task import PREPEND_STRINGS  # noqa
        if PREPEND_STRINGS:
            # Strings can be empty. If a string is empty their base64 encoding
            # will be empty.
            # So we add a leading character to it to make it non empty
            p.content = "#%s" % p.content
    elif p.content_type == TYPE.COLLECTION:
        # Just make contents available as serialized files (or objects)
        # We will build the value field later
        # (which will be used to reconstruct the collection in the worker)
        if p.is_file_collection:
            new_object = [
                Parameter(
                     content=x,
                     content_type=TYPE.FILE,
                     direction=p.direction,
                     file_name=x,
                     depth=p.depth - 1
                )
                for x in p.content
            ]
        else:
            new_object = [
                _serialize_object_into_file(
                    name,
                    Parameter(
                        content=x,
                        content_type=get_compss_type(x, p.depth - 1),
                        direction=p.direction,
                        depth=p.depth - 1,
                        extra_content_type=str(type(x).__name__)
                    )
                )
                for x in p.content
            ]
        p.content = new_object
        # Give this object an identifier inside the binding
        if p.direction != DIRECTION.IN_DELETE:
            _, _ = OT_track(p.content, collection=True)
    elif p.content_type == TYPE.DICT_COLLECTION:
        # Just make contents available as serialized files (or objects)
        # We will build the value field later
        # (which will be used to reconstruct the collection in the worker)
        new_object = {}
        for k, v in p.content.items():
            key = _serialize_object_into_file(
                name,
                Parameter(
                    content=k,
                    content_type=get_compss_type(k, p.depth - 1),
                    direction=p.direction,
                    depth=p.depth - 1,
                    extra_content_type=str(type(p).__name__)
                )
            )
            value = _serialize_object_into_file(
                name,
                Parameter(
                    content=v,
                    content_type=get_compss_type(v, p.depth - 1),
                    direction=p.direction,
                    depth=p.depth - 1,
                    extra_content_type=str(type(v).__name__)
                )
            )
            new_object[key] = value
        p.content = new_object
        # Give this object an identifier inside the binding
        if p.direction != DIRECTION.IN_DELETE:
            _, _ = OT_track(p.content, collection=True)
    return p


def _turn_into_file(p, skip_creation=False):
    # type: (Parameter, bool) -> None
    """ Write a object into a file if the object has not been already written.

    Consults the obj_id_to_filename to check if it has already been written
    (reuses it if exists). If not, the object is serialized to file and
    registered in the obj_id_to_filename dictionary.
    This functions stores the object into pending_to_synchronize.

    :param p: Wrapper of the object to turn into file.
    :param skip_creation: Skips the serialization to file.
    :return: None
    """
    obj_id = OT_is_tracked(p.content)
    if obj_id is None:
        # This is the first time a task accesses this object
        if p.direction == DIRECTION.IN_DELETE:
            obj_id, file_name = OT_not_track()
        else:
            obj_id, file_name = OT_track(p.content)
        if not skip_creation:
            serialize_to_file(p.content, file_name)
    else:
        file_name = OT_get_file_name(obj_id)
        if OT_has_been_written(obj_id):
            if p.direction == DIRECTION.INOUT or \
                    p.direction == DIRECTION.COMMUTATIVE:
                OT_set_pending_to_synchronize(obj_id)
            # Main program generated the last version
            compss_file = OT_pop_written_obj(obj_id)
            if __debug__:
                logger.debug("Serializing object %s to file %s" % (obj_id,
                                                                   compss_file))  # noqa: E501
            if not skip_creation:
                serialize_to_file(p.content, compss_file)
    # Set file name in Parameter object
    p.file_name = file_name


def _extract_parameter(param, code_strings, collection_depth=0):
    # type: (Parameter, bool, int) -> (int, int, int, int, str, str, float, bool)  # noqa: E501
    """ Extract the information of a single parameter.

    :param param: Parameter object.
    :param code_strings: <Boolean> Encode strings.
    :return: value, typ, direction, stream, prefix, extra_content_type, weight,
            keep_rename of the given parameter.
    """
    con_type = UNDEFINED_CONTENT_TYPE
    if param.content_type == TYPE.STRING and not param.is_future and code_strings:  # noqa: E501
        # Encode the string in order to preserve the source
        # Checks that it is not a future (which is indicated with a path)
        # Considers multiple spaces between words
        param.content = base64.b64encode(param.content.encode()).decode()
        if len(param.content) == 0:
            # Empty string - use escape string to avoid padding
            # Checked and substituted by empty string in the worker.py and
            # piper_worker.py
            param.content = base64.b64encode(EMPTY_STRING_KEY.encode()).decode()    # noqa: E501
        con_type = EXTRA_CONTENT_TYPE_FORMAT.format(
            "builtins", str(param.content.__class__.__name__))

    if param.content_type == TYPE.FILE or param.is_future:
        # If the parameter is a file or is future, the content is in a file
        # and we register it as file
        value = param.file_name
        typ = TYPE.FILE
    elif param.content_type == TYPE.DIRECTORY:
        value = param.file_name
        typ = TYPE.DIRECTORY
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
        con_type = EXTRA_CONTENT_TYPE_FORMAT.format(_mf, _class_name)
    elif param.content_type == TYPE.EXTERNAL_STREAM:
        # If the parameter type is stream, its value is stored in a file but
        # we keep the type
        value = param.file_name
        typ = TYPE.EXTERNAL_STREAM
    elif param.content_type == TYPE.COLLECTION or \
            (collection_depth > 0 and is_basic_iterable(param.content)):
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
        _class_name = str(param.content.__class__.__name__)
        con_type = EXTRA_CONTENT_TYPE_FORMAT.format("collection", _class_name)
        value = "{} {} {}".format(OT_is_tracked(param.content),
                                  len(param.content), con_type)
        OT_stop_tracking(param.content, collection=True)
        typ = TYPE.COLLECTION
        for (i, x) in enumerate(param.content):
            x_value, x_type, _, _, _, x_con_type, _, _ = _extract_parameter(
                x,
                code_strings,
                param.depth - 1
            )
            value += ' %s %s %s' % (x_type, x_value, x_con_type)
    elif param.content_type == TYPE.DICT_COLLECTION or \
            (collection_depth > 0 and is_dict(param.content)):
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
        con_type = EXTRA_CONTENT_TYPE_FORMAT.format("dict_collection", _class_name)
        value = "{} {} {}".format(OT_is_tracked(param.content),
                                  len(param.content), con_type)
        OT_stop_tracking(param.content, collection=True)
        typ = TYPE.DICT_COLLECTION
        for k, v in param.content.items():  # noqa
            k_value, k_type, _, _, _, k_con_type, _, _ = _extract_parameter(
                k,
                code_strings,
                param.depth - 1
            )
            if k_con_type != con_type:
                value += ' %s %s %s' % (k_type, k_value, k_con_type)
            else:
                # remove last dict_collection._classname if key is a dict_collection  # noqa: E501
                value += ' %s %s' % (k_type, k_value)
            v_value, v_type, _, _, _, v_con_type, _, _ = _extract_parameter(
                v,
                code_strings,
                param.depth - 1
            )
            if v_con_type != con_type:
                value += ' %s %s %s' % (v_type, v_value, v_con_type)
            else:
                # remove last dict_collection._classname if value is a dict_collection  # noqa: E501
                value += ' %s %s' % (v_type, v_value)
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
