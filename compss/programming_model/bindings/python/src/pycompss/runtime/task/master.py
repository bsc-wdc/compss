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
import copy
import os
import threading
import inspect
from collections import OrderedDict

from pycompss.api.task import CURRENT_CORE_ELEMENT
from pycompss.api.commons.error_msgs import cast_env_to_int_error
from pycompss.runtime.commons import IS_PYTHON3
from pycompss.runtime.task.commons import TaskCommons
from pycompss.runtime.task.parameter import Parameter
from pycompss.runtime.task.parameter import get_parameter_copy
from pycompss.runtime.task.parameter import get_return_name
from pycompss.runtime.task.parameter import get_compss_type
from pycompss.runtime.task.parameter import get_kwarg_name
from pycompss.runtime.task.parameter import get_vararg_name
from pycompss.runtime.task.parameter import get_name_from_kwarg
from pycompss.runtime.task.parameter import is_vararg
from pycompss.runtime.task.parameter import is_kwarg
from pycompss.util.arguments import check_arguments
import pycompss.api.parameter as parameter

if __debug__:
    import logging
    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {}
# List since the parameter names are included before checking for unexpected
# arguments (the user can define a=INOUT in the task decorator and this is not
# an unexpected argument)
SUPPORTED_ARGUMENTS = ['_compss_tracing',  # private
                       'returns',
                       'priority',
                       'on_failure',
                       'time_out',
                       'is_replicated',
                       'is_distributed',
                       'varargs_type',
                       'target_direction',
                       'computing_nodes',
                       'numba',
                       'numba_flags',
                       'numba_signature',
                       'numba_declaration',
                       'tracing_hook']
# Deprecated arguments. Still supported but shows a message when used.
DEPRECATED_ARGUMENTS = ['isReplicated',
                        'isDistributed',
                        'varargsType',
                        'targetDirection']
# Some attributes cause memory leaks, we must delete them from memory after
# master call
ATTRIBUTES_TO_BE_REMOVED = ['decorator_arguments',
                            'parameters',
                            'param_args',
                            'param_varargs',
                            'param_kwargs',
                            'param_defaults',
                            'first_arg_name',
                            'returns',
                            'multi_return']


# This lock allows tasks to be launched with the Threading module while
# ensuring that no attribute is overwritten
MASTER_LOCK = threading.Lock()


class TaskMaster(TaskCommons):
    """
    Task code for the Master:

    Process the task decorator and prepare all information to call binding
    runtime.
    """

    def __init__(self,
                 comment,
                 decorator_arguments,
                 init_dec_args,
                 user_function):
        self.comment = comment
        # Initialize TaskCommons
        super(self.__class__, self).__init__(decorator_arguments, None, None)
        self.init_dec_args = init_dec_args
        # User function
        self.user_function = user_function
        # Add more argument related attributes that will be useful later
        self.parameters = None
        self.param_kwargs = None
        self.param_defaults = None
        self.first_arg_name = None
        # Add function related attributed that will be useful later
        self.module_name = None
        self.function_name = None
        self.function_type = None
        self.class_name = None
        self.computing_nodes = None
        # Add returns related attributes that will be useful later
        self.returns = None
        self.multi_return = False
        # Task wont be registered until called from the master for the first
        # time or have a different signature
        self.signature = None
        self.registered = False

    def call(self, *args, **kwargs):
        """
        This part deals with task calls in the master's side
        Also, this function must return an appropriate number of
        future objects that point to the appropriate objects/files.

        :return: A function that does "nothing" and returns futures if needed
        """
        # This lock makes this decorator able to handle various threads
        # calling the same task concurrently
        MASTER_LOCK.acquire()
        # IMPORTANT! recover initial decorator arguments
        self.decorator_arguments = copy.deepcopy(self.init_dec_args)

        # Inspect the user function, get information about the arguments and
        # their names. This defines self.param_args, self.param_varargs,
        # self.param_kwargs, self.param_defaults. And gives non-None default
        # values to them if necessary
        self.inspect_user_function_arguments()

        # Process the parameters, give them a proper direction
        self.process_master_parameters(*args, **kwargs)

        # Compute the function path, class (if any), and name
        self.compute_user_function_information()

        # Process the decorators to get the core element information
        # It is necessary to decide whether to register or not (the task may
        # be inherited, and in this case it has to be registered again with
        # the new implementation signature).
        impl_signature = self.prepare_core_element_information(self.user_function)  # noqa: E501
        if not self.registered or self.signature != impl_signature:
            self.register_task(self.user_function)
            self.registered = True
            self.signature = impl_signature

        # Reset the global core element to a full-None status, ready for the
        # next task! (Note that this region is locked, so no race conditions
        # will ever happen here).
        CURRENT_CORE_ELEMENT.reset()
        # Did we call this function to only register the associated core
        # element? (This can happen when trying)
        # Do not move this import:
        from pycompss.api.task import REGISTER_ONLY
        if REGISTER_ONLY:
            MASTER_LOCK.release()
            return

        # Deal with dynamic computing nodes
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
                        raise Exception(
                            cast_env_to_int_error('ComputingNodes')
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
            raise Exception("ERROR: Wrong Computing Nodes value at @mpi decorator.")  # noqa: E501
        if parsed_computing_nodes <= 0:
            logger.warning("Registered computing_nodes is less than 1 (" +
                           str(parsed_computing_nodes) +
                           " <= 0). Automatically set it to 1")
            parsed_computing_nodes = 1

        # Deal with the return part.
        self.add_return_parameters()
        if not self.returns:
            self.update_return_if_no_returns(self.user_function)
        from pycompss.runtime.binding import process_task
        # Get deprecated arguments if exist
        if 'isReplicated' in self.decorator_arguments:
            is_replicated = self.decorator_arguments['isReplicated']
        else:
            is_replicated = self.decorator_arguments['is_replicated']
        if 'isDistributed' in self.decorator_arguments:
            is_distributed = self.decorator_arguments['isDistributed']
        else:
            is_distributed = self.decorator_arguments['is_distributed']

        # Process the task
        ret = process_task(
            self.user_function,
            self.module_name,
            self.class_name,
            self.function_type,
            self.parameters,
            self.returns,
            self.decorator_arguments,
            parsed_computing_nodes,
            is_replicated,
            is_distributed,
            self.decorator_arguments['on_failure'],
            self.decorator_arguments['time_out']
        )
        # remove unused attributes from the memory
        for at in ATTRIBUTES_TO_BE_REMOVED:
            if hasattr(self, at):
                delattr(self, at)
        MASTER_LOCK.release()
        return ret

    def inspect_user_function_arguments(self):
        """
        Inspect the arguments of the user function and store them.
        Read the names of the arguments and remember their order.
        We will also learn things like if the user function contained
        variadic arguments, named arguments and so on.
        This will be useful when pairing arguments with the direction
        the user has specified for them in the decorator
        :return: None, it just adds attributes
        """
        try:
            arguments = self._getargspec(self.user_function)
            self.param_args, self.param_varargs, self.param_kwargs, self.param_defaults = arguments  # noqa: E501
        except TypeError:
            # This is a numba jit declared task
            arguments = self._getargspec(self.user_function.py_func)
            self.param_args, self.param_varargs, self.param_kwargs, self.param_defaults = arguments  # noqa: E501
        # It will be easier to deal with functions if we pretend that all have
        # the signature f(positionals, *variadic, **named). This is why we are
        # substituting
        # Nones with default stuff
        # As long as we remember what was the users original intention with
        # the parameters we can internally mess with his signature as much as
        # we want. There is no need to add self-imposed constraints here.
        # Also, the very nature of decorators are a huge hint about how we
        # should treat user functions, as most wrappers return a function
        # f(*a, **k)
        if self.param_varargs is None:
            self.param_varargs = 'varargs_type'
        if self.param_defaults is None:
            self.param_defaults = ()

    @staticmethod
    def _getargspec(function):
        if IS_PYTHON3:
            full_argspec = inspect.getfullargspec(function)
            as_args = full_argspec.args
            as_varargs = full_argspec.varargs
            as_keywords = full_argspec.varkw
            as_defaults = full_argspec.defaults
            return as_args, as_varargs, as_keywords, as_defaults
        else:
            # Using getargspec in python 2 (deprecated in python 3 in favour
            # of getfullargspec).
            return inspect.getargspec(function)  # noqa

    def process_master_parameters(self, *args, **kwargs):
        """
        Process all the input parameters.
        Basically, processing means "build a dictionary of <name, parameter>,
        where each parameter has an associated Parameter object".
        This function also assigns default directions to parameters.

        :return: None, it only modifies self.parameters
        """
        # If we have an MPI, COMPSs or MultiNode decorator above us we should
        # have computing_nodes as a kwarg, we should detect it and remove it.
        # Otherwise we set it to 1
        self.computing_nodes = kwargs.pop('computing_nodes', 1)
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
        for (arg_name, arg_object) in zip(self.param_args[:num_positionals],
                                          args[:num_positionals]):
            if self.first_arg_name is None:
                self.first_arg_name = arg_name
            self.parameters[arg_name] = self.build_parameter_object(arg_name,
                                                                    arg_object)
        num_defaults = len(self.param_defaults)
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
            if arg_name not in self.parameters.keys():
                real_arg_name = get_kwarg_name(arg_name)
                self.parameters[real_arg_name] = \
                    self.build_parameter_object(real_arg_name,
                                                default_value)
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
        supported_arguments = (SUPPORTED_ARGUMENTS +
                               DEPRECATED_ARGUMENTS +
                               self.param_args)
        check_arguments(MANDATORY_ARGUMENTS,
                        DEPRECATED_ARGUMENTS,
                        supported_arguments,
                        list(self.decorator_arguments.keys()),
                        "@task")

    def build_parameter_object(self, arg_name, arg_object):
        """
        Creates the Parameter object from an argument name and object.

        :param arg_name: Argument name
        :param arg_object: Argument object
        :return: Parameter object
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

        # TODO: add 'dir_name' to the parameter object
        if param.is_file() or param.is_directory():
            if arg_object:
                param.file_name = arg_object
            else:
                # is None: Used None for a FILE or DIRECTORY parameter path
                param.content_type = parameter.TYPE.NULL
        else:
            param.content = arg_object

        return param

    def compute_user_function_information(self):
        """
        Compute the function path p and the name n such that
        "from p import n" imports self.user_function

        :return: None, it just sets self.user_function_path and
                 self.user_function_name
        """
        self.function_name = self.user_function.__name__
        # Get the module name (the x part "from x import y"), except for the
        # class name
        self.compute_module_name()
        # Get the function type (function, instance method, class method)
        self.compute_function_type()

    def compute_module_name(self):
        """
        Compute the user's function module name.
        There are various cases:
        1) The user function is defined in some file. This is easy, just get
           the module returned by inspect.getmodule
        2) The user function is in the main module. Retrieve the file and
           build the import name from it
        3) We are in interactive mode

        :return: None, it just modifies self.module_name
        """
        import inspect
        import os
        mod = inspect.getmodule(self.user_function)
        self.module_name = mod.__name__
        # If it is a task within a class, the module it will be where the one
        # where the class is defined, instead of the one where the task is
        # defined.
        # This avoids conflicts with task inheritance.
        if self.first_arg_name == 'self':
            mod = inspect.getmodule(type(self.parameters['self'].content))
            self.module_name = mod.__name__
        elif self.first_arg_name == 'cls':
            self.module_name = self.parameters['cls'].content.__module__
        if self.module_name == '__main__' or \
                self.module_name == 'pycompss.runtime.launch':
            # The module where the function is defined was run as __main__,
            # We need to find out the real module name
            # Get the real module name from our launch.py APP_PATH global
            # variable
            # It is guaranteed that this variable will always exist because
            # this code is only executed when we know we are in the master
            path = getattr(mod, 'APP_PATH')
            # Get the file name
            file_name = os.path.splitext(os.path.basename(path))[0]
            # Get the module
            from pycompss.util.objects.properties import get_module_name
            self.module_name = get_module_name(path, file_name)

    def compute_function_type(self):
        """
        Compute some properties of the user function, as its name,
        its import path, and its type (module function, instance method,
         class method), etc.

        :return: None, just updates self.class_name and self.function_type
        """
        from pycompss.runtime.binding import FunctionType
        # Check the type of the function called.
        # inspect.ismethod(f) does not work here,
        # for methods python hasn't wrapped the function as a method yet
        # Everything is still a function here, can't distinguish yet
        # with inspect.ismethod or isfunction
        self.function_type = FunctionType.FUNCTION
        self.class_name = ''
        if self.first_arg_name == 'self':
            self.function_type = FunctionType.INSTANCE_METHOD
            self.class_name = type(self.parameters['self'].content).__name__
        elif self.first_arg_name == 'cls':
            self.function_type = FunctionType.CLASS_METHOD
            self.class_name = self.parameters['cls'].content.__name__
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

    def prepare_core_element_information(self, f):
        """
        This function is used to prepare the core element.
        The information is needed in order to compare the implementation
        signature, so that if it has been registered with a different
        signature, it can be re-registered with the new one (enable
        inheritance).

        :param f: Function to be registered
        """

        def _get_top_decorator(code, dec_keys):
            """
            Retrieves the decorator which is on top of the current task
            decorators stack.

            :param code: Tuple which contains the task code to analyse and
                         the number of lines of the code.
            :param dec_keys: Typle which contains the available decorator keys
            :return: the decorator name in the form "pycompss.api.__name__"
            """
            # Code has two fields:
            # code[0] = the entire function code.
            # code[1] = the number of lines of the function code.
            dec_func_code = code[0]
            decorators = [dfline.strip() for dfline in
                          dec_func_code if dfline.strip().startswith('@')]
            # Could be improved if it stops when the first line without @ is
            # found, but we have to be careful if a decorator is commented
            # (# before @).
            # The strip is due to the spaces that appear before functions
            # definitions, such as class methods.
            for dk in dec_keys:
                for d in decorators:
                    if d.startswith('@' + dk):
                        # check each decorator's __name__ to lower
                        return "pycompss.api." + dk.lower()
            # If no decorator is found, then the current decorator is the one
            # to register
            return __name__

        def _get_task_type(code, dec_filter, default_values):
            """
            Retrieves the type of the task based on the decorators stack.

            :param code: Tuple which contains the task code to analyse and the
                         number of lines of the code.
            :param dec_filter: Tuple which contains the filtering decorators.
                               The one used determines the type of the task.
                               If none, then it is a normal task.
            :param default_values: Default values
            :return: the type of the task
            """
            # Code has two fields:
            # code[0] = the entire function code.
            # code[1] = the number of lines of the function code.
            dec_func_code = code[0]
            full_decorators = [dfline.strip() for dfline in
                               dec_func_code if dfline.strip().startswith('@')]
            # Get only the decorators used. Remove @ and parameters.
            decorators = [fline[1:].split('(')[0] for fline in full_decorators]
            # Look for the decorator used from the filter list and return it
            # when found if @mpi and no binary then this is an python_mpi task
            index = 0
            for filt in dec_filter:
                if filt in decorators:
                    if filt == "mpi":
                        if "binary" not in full_decorators[index]:
                            filt = "PYTHON_MPI"
                    return filt
                index += 1
            # The decorator stack did not contain any of the filtering keys,
            # then return the default key.
            return default_values

        # Look for the decorator that has to do the registration
        # Since the __init__ of the decorators is independent, there is no way
        # to pass information through them.
        # However, the __call__ method of the decorators can be used.
        # The way that they are called is from bottom to top. So, the first one
        # to call its __call__ method will always be @task. Consequently, the
        # @task decorator __call__ method can detect the top decorator and pass
        # a hint to order that decorator that has to do the registration (not
        # the others).
        func_code = ''
        got_func_code = False
        func = f
        while not got_func_code:
            try:
                from pycompss.util.objects.properties import get_wrapped_sourcelines  # noqa: E501
                func_code = get_wrapped_sourcelines(func)
                got_func_code = True
            except IOError:
                # There is one or more decorators below the @task -> undecorate
                # until possible to get the func code.
                # Example of this case: test 19: @timeit decorator below the
                # @task decorator.
                func = func.__wrapped__

        decorator_keys = ("implement",
                          "constraint",
                          "task",
                          "binary",
                          "mpi",
                          "compss",
                          "decaf",
                          "ompss",
                          "opencl")

        top_decorator = _get_top_decorator(func_code, decorator_keys)
        if __debug__:
            logger.debug(
                "[@TASK] Top decorator of function %s in module %s: %s" %
                (f.__name__, self.module_name, str(top_decorator))
            )
        f.__who_registers__ = top_decorator
        # not usual tasks - handled by the runtime without invoking the
        # PyCOMPSs worker. Needed to filter in order not to code the strings
        # when using them in these type of tasks
        decorator_filter = ("binary",
                            "mpi",
                            "compss",
                            "decaf",
                            "ompss",
                            "opencl")
        default = 'task'
        task_type = _get_task_type(func_code, decorator_filter, default)

        if __debug__:
            logger.debug("[@TASK] Task type of function %s in module %s: %s" %
                         (f.__name__, self.module_name, str(task_type)))
        f.__task_type__ = task_type
        if task_type == default:
            f.__code_strings__ = True
        else:
            if task_type == "PYTHON_MPI":
                for line in func_code[0]:
                    if "@mpi" in line:
                        f.__code_strings__ = "binary" not in line
            else:
                f.__code_strings__ = False

        # Get the task signature
        # To do this, we will check the frames
        import inspect
        frames = inspect.getouterframes(inspect.currentframe())
        # Pop the __register_task and __call__ functions from the frame
        frames = frames[2:]
        # Get the application frames
        app_frames = []
        for frame in frames:
            if frame[3] == 'compss_main':
                break
            else:
                app_frames.append(frame)
        # Analise the frames
        if len(app_frames) == 1:
            # The task is defined within the main app file.
            # This case is never reached with Python 3 since it includes
            # frames that are not present with Python 2.
            ce_signature = self.module_name + "." + f.__name__
            impl_type_args = [self.module_name, f.__name__]
        else:
            if self.class_name:
                # Within class or subclass
                ce_signature = self.module_name + '.' + \
                               self.class_name + '.' + \
                               f.__name__
                impl_type_args = [self.module_name + '.' + self.class_name,
                                  f.__name__]
            else:
                # Not in a class or subclass
                # This case can be reached in Python 3, where particular
                # frames are included, but not class names found.
                ce_signature = self.module_name + "." + f.__name__
                impl_type_args = [self.module_name, f.__name__]
        # Include the registering info related to @task
        impl_signature = ce_signature
        impl_constraints = {}
        impl_type = "METHOD"

        # Maybe some top decorator has already added some parameters
        # These if statements avoid us to overwrite these already
        # existing attributes
        # For example, the constraint decorator adds things in the
        # impl_constraints field, so it would be nice to not overwrite it!

        if CURRENT_CORE_ELEMENT.get_ce_signature() is None:
            CURRENT_CORE_ELEMENT.set_ce_signature(ce_signature)
        else:
            # If we are here that means that we come from an implements
            # decorator, which means that this core element has already
            # a signature
            CURRENT_CORE_ELEMENT.set_impl_signature(ce_signature)
        if CURRENT_CORE_ELEMENT.get_impl_signature() is None:
            CURRENT_CORE_ELEMENT.set_impl_signature(impl_signature)
        if CURRENT_CORE_ELEMENT.get_impl_constraints() is None:
            CURRENT_CORE_ELEMENT.set_impl_constraints(impl_constraints)
        if CURRENT_CORE_ELEMENT.get_impl_type() is None:
            CURRENT_CORE_ELEMENT.set_impl_type(impl_type)
        if CURRENT_CORE_ELEMENT.get_impl_type_args() is None:
            CURRENT_CORE_ELEMENT.set_impl_type_args(impl_type_args)

        if CURRENT_CORE_ELEMENT.get_impl_type() == "PYTHON_MPI":
            CURRENT_CORE_ELEMENT.set_impl_signature("MPI." + impl_signature)
            CURRENT_CORE_ELEMENT.set_impl_type_args(
                impl_type_args + CURRENT_CORE_ELEMENT.get_impl_type_args()[1:])

        return impl_signature

    def register_task(self, f):  # noqa
        """
        This function is used to register the task in the runtime.
        This registration must be done only once on the task decorator
        initialization

        :param f: Function to be registered
        """
        import pycompss.runtime.binding as binding
        if __debug__:
            logger.debug(
                "[@TASK] I have to register the function %s in module %s" %
                (f.__name__, self.module_name)
            )
            logger.debug("[@TASK] %s" % str(f))
        binding.register_ce(CURRENT_CORE_ELEMENT)

    def add_return_parameters(self):
        """
        Modify the return parameters accordingly to the return statement

        :return: Nothing, it just creates and modifies self.returns
        """
        from collections import OrderedDict
        self.returns = OrderedDict()

        _returns = self.decorator_arguments['returns']
        # Note that 'returns' is by default False
        if not _returns:
            return False

        # A return statement can be the following:
        # 1) A type. This means 'this task returns an object of this type'
        # 2) An integer N. This means 'this task returns N objects'
        # 3) A basic iterable (tuple, list...). This means 'this task
        #    returns an iterable with the indicated elements inside

        # We are returning multiple objects until otherwise proven
        # It is important to know because this will determine if we will
        # return a single object or [a single object] in some cases

        from pycompss.util.objects.properties import is_basic_iterable
        self.multi_return = True
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
                to_return = [tuple([]) for _ in range(num_rets)]
            else:
                to_return = tuple([])
        elif is_basic_iterable(_returns):
            # The task returns a basic iterable with some types
            # already defined
            to_return = _returns
        elif isinstance(_returns, int):
            # The task returns a list of N objects, defined by the int N
            to_return = tuple([() for _ in range(_returns)])
        else:
            # The task returns a single object of a single type
            # This is also the only case when no multiple objects are
            # returned but only one
            self.multi_return = False
            to_return = [_returns]

        # At this point we have a list of returns
        for (i, elem) in enumerate(to_return):
            ret_type = get_compss_type(elem)
            self.returns[get_return_name(i)] = \
                Parameter(content=elem,
                          content_type=ret_type,
                          direction=parameter.OUT)
            # Hopefully, an exception have been thrown if some invalid
            # stuff has been put in the returns field

    def update_return_if_no_returns(self, f):
        """
        Checks the code looking for return statements if no returns is
         specified in @task decorator.

        WARNING: Updates self.return if returns are found.

        :param f: Function to check
        """
        from pycompss.util.objects.properties import get_wrapped_source
        import ast

        # Check type-hinting in python3
        if IS_PYTHON3:
            from typing import get_type_hints
            type_hints = get_type_hints(f)
            if 'return' in type_hints:
                # There is a return defined as type-hint
                ret = type_hints['return']
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
                return
            else:
                # The user has not defined return as type-hint
                # So, continue searching as usual
                pass

        # It is python2 or could not find type-hinting
        source_code = get_wrapped_source(f).strip()

        if self.first_arg_name == 'self' or \
                source_code.startswith('@classmethod'):
            # It is a task defined within a class (can not parse the code
            # with ast since the class does not exist yet).
            # Alternatively, the only way I see is to parse it manually
            # line by line.
            ret_mask = []
            code = source_code.split('\n')
            for line in code:
                if 'return ' in line:
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
            if self.first_arg_name == 'self' or \
                    source_code.startswith('@classmethod'):
                # Parse code as string (it is a task defined within a class)
                def _has_multireturn(statement):
                    v = ast.parse(statement.strip())
                    try:
                        if len(v.body[0].value.elts) > 1:
                            return True
                        else:
                            return False
                    except (KeyError, AttributeError):
                        # KeyError: 'elts' means that it is a multiple return.
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
                        if 'elts' in code[i].value.__dict__:
                            has_multireturn = True
                            num_returns = len(code[i].value.__dict__['elts'])
                            if num_returns > max_num_returns:
                                max_num_returns = num_returns
                    except (KeyError, AttributeError):
                        # KeyError: 'elts' means that it is a multiple return.
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
