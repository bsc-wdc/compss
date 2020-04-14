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
PyCOMPSs API - Task
===================
    This file contains the class task, needed for the task definition.
"""

from __future__ import print_function
import copy
import os
import sys
import threading
import inspect
from functools import wraps

import pycompss.api.parameter as parameter
from pycompss.api.exceptions import COMPSsException
from pycompss.runtime.core_element import CE
from pycompss.runtime.commons import IS_PYTHON3
from pycompss.runtime.commons import TRACING_HOOK_ENV_VAR
import pycompss.util.context as context
from pycompss.util.arguments import check_arguments
from pycompss.util.objects.properties import create_object_by_con_type
from pycompss.util.storages.persistent import is_psco
from pycompss.util.serialization.serializer import deserialize_from_file
from pycompss.util.serialization.serializer import serialize_to_file
from pycompss.util.serialization.serializer import serialize_to_file_mpienv
from pycompss.worker.commons.worker import build_task_parameter

if __debug__:
    import logging
    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {}
# List since the parameter names are included before checking for unexpected
# arguments (the user can define a=INOUT in the task decorator and this is not
# an unexpected argument)
SUPPORTED_ARGUMENTS = ['compss_tracing',  # private
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

# Some attributes causes memory leaks, we must delete them from memory after
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
master_lock = threading.Lock()
# Determine if strings should have a sharp symbol prepended or not
prepend_strings = True
register_only = False

current_core_element = CE()


class Task(object):
    """
    This is the Task decorator implementation.
    It is implemented as a class and consequently this implementation can be
    divided into two natural steps: decoration process and function call.

    Decoration process is what happens when the Python parser reads a decorated
    function. The actual function is not called, but the @task() triggers
    the process that stores and processes the parameters of the decorator.
    This first step corresponds to the class constructor.

    Function call is what happens when the user calls their function somewhere
    in the code. A decorator simply adds pre and post steps in this function
    call, allowing us to change and process the arguments. This second steps
    happens in the __call__ implementation.

    Also, the call itself does different things in the master than in the
    worker. We must also handle the case when the user just runs the app with
    python and no PyCOMPSs.
    The specific implementations can be found in self.master_call(),
    self.worker_call(), self.sequential_call()
    """

    @staticmethod
    def get_default_decorator_values():
        """
        Default value for decorator arguments.
        By default, do not use jit (if true -> use nopython mode,
        alternatively, the user can define a dictionary with the specific
        flags - using a dictionary will be considered as the user wants to use
        compile with jit).

        :return: A dictionary with the default values of the non-parameter
                 decorator fields
        """
        return {
            'target_direction': parameter.INOUT,
            'returns': False,
            'priority': False,
            'on_failure': 'RETRY',
            'time_out': 0,
            'is_replicated': False,
            'is_distributed': False,
            'computing_nodes': 1,
            'tracing_hook': False,
            'numba': False,  # numba mode (jit, vectorize, guvectorize)
            'numba_flags': {},  # user defined extra numba flags
            'numba_signature': None,  # vectorize and guvectorize signature
            'numba_declaration': None,  # guvectorize declaration
            'varargs_type': parameter.IN  # Here for legacy purposes
        }

    def __init__(self, comment=None, **kwargs):
        """
        This part is called in the decoration process, not as an
        explicit function call.

        We do two things here:
        a) Assign default values to unspecified fields
           (see get_default_decorator_values )
        b) Transform the parameters from user friendly types
           (i.e Parameter.IN, etc) to a more convenient internal representation

        :param comment: Hidden to the user (non-documented).
        :param kwargs: Decorator parameters. A task decorator has no positional
                       arguments.
        """
        self.comment = comment
        self.decorator_arguments = kwargs
        # Set missing values to their default ones (step a)
        for (key, value) in self.get_default_decorator_values().items():
            if key not in self.decorator_arguments:
                self.decorator_arguments[key] = value
        # Give all parameters a unique instance for them (step b)
        # Note that when a user defines a task like
        # @task(a = IN, b = IN, c = INOUT)
        # both a and b point to the same IN object (the one from parameter.py)
        # Giving them a unique instance makes life easier in further steps
        for (key, value) in self.decorator_arguments.items():
            # Not all decorator arguments are necessarily parameters
            # (see self.get_default_decorator_values)
            if parameter.is_parameter(value):
                self.decorator_arguments[key] = \
                    parameter.get_parameter_copy(value)
            # Specific case when value is a dictionary
            # Use case example:
            # @binary(binary="ls")
            # @task(hide={Type: FILE_IN, Prefix: "--hide="},
            #       sort={Type: IN, Prefix: "--sort="})
            # def myLs(flag, hide, sort):
            #   pass
            # Transform this dictionary to a Parameter object
            if parameter.is_dict_specifier(value):
                if key not in ['numba', 'numba_flags',
                               'numba_signature', 'numba_declaration']:
                    # Perform user -> instance substitution
                    # param = self.decorator_arguments[key][parameter.Type]
                    # Replace the whole dict by a single parameter object
                    self.decorator_arguments[key] = \
                        parameter.get_parameter_from_dictionary(
                            self.decorator_arguments[key]
                    )
                    # self.decorator_arguments[key].update(
                    #     {parameter.Type: parameter.get_parameter_copy(param)}
                    # )
                else:
                    # It is a reserved word that we need to keep the user
                    # defined value (not a Parameter object)
                    self.decorator_arguments[key] = value

        # initial decorator arguments must be saved and passed to the following
        # 'calls' of the 'task'
        self.init_dec_args = copy.deepcopy(self.decorator_arguments)
        # Add more argument related attributes that will be useful later
        self.parameters = None
        self.param_args = None
        self.param_varargs = None
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

    def __call__(self, user_function):
        """
        This part is called in all explicit function calls.
        Note that in PyCOMPSs a single function call will be transformed into
        two calls, as both master and worker need to call the function.

        The work to do in the master part is totally different
        from the job to do in the worker part. This is why there are
        some other functions like master_call, worker_call, and
        sequential_call

        There is also a third case that happens when the user runs a PyCOMPSs
        code without PyCOMPSs. This case is straightforward: just call the
        user function with the user parameters and return whatever the user
        code returned. Therefore, we can just return the user function.

        :param user_function: Function to decorate
        :return: The function to be executed
        """
        self.user_function = user_function

        self.update_if_interactive()

        @wraps(user_function)
        def task_decorator(*args, **kwargs):
            # Determine the context and decide what to do
            if context.in_master():
                return self.master_call(*args, **kwargs)
            elif context.in_worker():
                if 'compss_key' in kwargs.keys():
                    return self.worker_call(*args, **kwargs)
                else:
                    # Called from another task within the worker
                    # Ignore the @task decorator and run it sequentially
                    message = "WARNING: Calling task: "
                    message += str(user_function.__name__)
                    message += " from this task.\n"
                    message += "         It will be executed sequentially "
                    message += "within the caller task."
                    print(message, file=sys.stderr)
                    return self.sequential_call(*args, **kwargs)
            # We are neither in master nor in the worker, or the user has
            # stopped the interactive session.
            # Therefore, the user code is being executed with no
            # launch_compss/enqueue_compss/runcompss/interactive session
            return self.sequential_call(*args, **kwargs)

        return task_decorator

    def update_if_interactive(self):
        """
        Update the user code if in interactive mode and the session has
        been started.

        :return: None
        """
        import inspect
        mod = inspect.getmodule(self.user_function)
        self.module_name = mod.__name__
        if context.in_pycompss() and \
                (self.module_name == '__main__' or
                 self.module_name == 'pycompss.runtime.launch'):
            # 1.- The runtime is running.
            # 2.- The module where the function is defined was run as __main__,
            # We need to find out the real module name
            # Get the real module name from our launch.py APP_PATH global
            # variable
            # It is guaranteed that this variable will always exist because
            # this code is only executed when we know we are in the master
            path = getattr(mod, 'APP_PATH')
            # Get the file name
            file_name = os.path.splitext(os.path.basename(path))[0]
            # Do any necessary pre processing action before executing any code
            if file_name.startswith('InteractiveMode') and not self.registered:
                # If the file_name starts with 'InteractiveMode' means that
                # the user is using PyCOMPSs from jupyter-notebook.
                # Convention between this file and interactive.py
                # In this case it is necessary to do a pre-processing step
                # that consists of putting all user code that may be executed
                # in the worker on a file.
                # This file has to be visible for all workers.
                from pycompss.util.interactive.helpers import update_tasks_code_file  # noqa
                update_tasks_code_file(self.user_function, path)
                print("Found task: " + str(self.user_function.__name__))
        else:
            pass

    def update_return_if_no_returns(self, f):
        """
        Checks the code looking for return statements if no returns is
         specified in @task decorator.

        WARNING: Updates self.return if returns are found.

        :param f: Function to check
        """
        from pycompss.api.parameter import Parameter
        from pycompss.api.parameter import DIRECTION
        from pycompss.api.parameter import TYPE
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
                        param = Parameter(p_type=TYPE.FILE,
                                          p_direction=DIRECTION.OUT)
                        param.object = object()
                        self.returns[parameter.get_return_name(i)] = param
                else:
                    param = Parameter(p_type=TYPE.FILE,
                                      p_direction=DIRECTION.OUT)
                    param.object = object()
                    self.returns[parameter.get_return_name(0)] = param
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
            # TODO: WHAT IF IS CLASSMETHOD FROM BOOLEAN?
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
                    param = Parameter(p_type=TYPE.FILE,
                                      p_direction=DIRECTION.OUT)
                    param.object = object()
                    self.returns[parameter.get_return_name(i)] = param
            else:
                param = Parameter(p_type=TYPE.FILE,
                                  p_direction=DIRECTION.OUT)
                param.object = object()
                self.returns[parameter.get_return_name(0)] = param
        else:
            # Return not found
            pass

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
            decorators = [l.strip() for l in
                          dec_func_code if l.strip().startswith('@')]
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
            full_decorators = [l.strip() for l in
                               dec_func_code if l.strip().startswith('@')]
            # Get only the decorators used. Remove @ and parameters.
            decorators = [l[1:].split('(')[0] for l in full_decorators]
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
                from pycompss.util.objects.properties import get_wrapped_sourcelines  # noqa
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

        if current_core_element.get_ce_signature() is None:
            current_core_element.set_ce_signature(ce_signature)
        else:
            # If we are here that means that we come from an implements
            # decorator, which means that this core element has already
            # a signature
            current_core_element.set_impl_signature(ce_signature)
        if current_core_element.get_impl_signature() is None:
            current_core_element.set_impl_signature(impl_signature)
        if current_core_element.get_impl_constraints() is None:
            current_core_element.set_impl_constraints(impl_constraints)
        if current_core_element.get_impl_type() is None:
            current_core_element.set_impl_type(impl_type)
        if current_core_element.get_impl_type_args() is None:
            current_core_element.set_impl_type_args(impl_type_args)

        if current_core_element.get_impl_type() == "PYTHON_MPI":
            current_core_element.set_impl_signature("MPI." + impl_signature)
            current_core_element.set_impl_type_args(
                impl_type_args+current_core_element.get_impl_type_args()[1:])

        return impl_signature

    def register_task(self, f):
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
        binding.register_ce(current_core_element)

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
            return inspect.getargspec(function)

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
            self.param_args, self.param_varargs, self.param_kwargs, self.param_defaults = arguments  # noqa
        except TypeError:
            # This is a numba jit declared task
            arguments = self._getargspec(self.user_function.py_func)
            self.param_args, self.param_varargs, self.param_kwargs, self.param_defaults = arguments  # noqa
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
            mod = inspect.getmodule(type(self.parameters['self'].object))
            self.module_name = mod.__name__
        elif self.first_arg_name == 'cls':
            self.module_name = self.parameters['cls'].object.__module__
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
            self.class_name = type(self.parameters['self'].object).__name__
        elif self.first_arg_name == 'cls':
            self.function_type = FunctionType.CLASS_METHOD
            self.class_name = self.parameters['cls'].object.__name__
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
                    num_rets = self.user_function.py_func.__globals__.get(_returns)
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
            ret_type = parameter.get_compss_type(elem)
            self.returns[parameter.get_return_name(i)] = \
                parameter.Parameter(p_type=ret_type,
                                    p_object=elem,
                                    p_direction=parameter.OUT)
            # Hopefully, an exception have been thrown if some invalid
            # stuff has been put in the returns field

    def master_call(self, *args, **kwargs):
        """
        This part deals with task calls in the master's side
        Also, this function must return an appropriate number of
        future objects that point to the appropriate objects/files.

        :return: A function that does "nothing" and returns futures if needed
        """
        # This lock makes this decorator able to handle various threads
        # calling the same task concurrently
        master_lock.acquire()
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
        impl_signature = self.prepare_core_element_information(
            self.user_function)
        if not self.registered or self.signature != impl_signature:
            self.register_task(self.user_function)
            self.registered = True
            self.signature = impl_signature
        # Reset the global core element to a full-None status, ready for the
        # next task! (Note that this region is locked, so no race conditions
        # will ever happen here).
        current_core_element.reset()
        # Did we call this function to only register the associated core
        # element? (This can happen when trying)
        if register_only:
            master_lock.release()
            return
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
            self.computing_nodes,
            is_replicated,
            is_distributed,
            self.decorator_arguments['on_failure'],
            self.decorator_arguments['time_out']
        )
        # remove unused attributes from the memory
        for at in ATTRIBUTES_TO_BE_REMOVED:
            if hasattr(self, at):
                delattr(self, at)
        master_lock.release()
        return ret

    def get_varargs_direction(self):
        """
        Returns the direction of the varargs arguments.
        Can be defined in the decorator in two ways:
        args = dir, where args is the name of the variadic args tuple, or
        varargs_type = dir (for legacy reasons)

        :return: Direction of the varargs arguments.
        """
        if self.param_varargs not in self.decorator_arguments:
            if 'varargsType' in self.decorator_arguments:
                self.param_varargs = 'varargsType'
                return self.decorator_arguments['varargsType']
            else:
                return self.decorator_arguments['varargs_type']
        return self.decorator_arguments[self.param_varargs]

    def get_default_direction(self, var_name):
        """
        Returns the default direction for a given parameter

        :return: An identifier of the direction
        """
        # We are the 'self' or 'cls' in an instance or classmethod that
        # modifies the given class, so we are an INOUT, CONCURRENT or
        # COMMUTATIVE
        self_dirs = [parameter.DIRECTION.INOUT,
                     parameter.DIRECTION.CONCURRENT,
                     parameter.DIRECTION.COMMUTATIVE]
        if 'targetDirection' in self.decorator_arguments:
            target_label = 'targetDirection'
        else:
            target_label = 'target_direction'
        if self.decorator_arguments[target_label].direction in self_dirs and \
                var_name in ['self', 'cls'] and \
                self.param_args and \
                self.param_args[0] == var_name:
            return self.decorator_arguments[target_label]
        return parameter.get_new_parameter('IN')

    def process_master_parameters(self, *args, **kwargs):
        """
        Process all the input parameters.
        Basically, processing means "build a dictionary of <name, parameter>,
        where each parameter has an associated Parameter object".
        This function also assigns default directions to parameters.

        :return: None, it only modifies self.parameters
        """
        from collections import OrderedDict
        parameter_values = OrderedDict()
        # If we have an MPI, COMPSs or MultiNode decorator above us we should
        # have computing_nodes as a kwarg, we should detect it and remove it.
        # Otherwise we set it to 1
        self.computing_nodes = kwargs.pop('computing_nodes', 1)
        # It is important to know the name of the first argument to determine
        # if we are dealing with a class or instance method (i.e: first
        # argument is named self)
        self.first_arg_name = None
        # Process the positional arguments
        # Some of these positional arguments may have been not
        # explicitly defined
        num_positionals = min(len(self.param_args), len(args))
        for (var_name, var_value) in zip(self.param_args[:num_positionals],
                                         args[:num_positionals]):
            if self.first_arg_name is None:
                self.first_arg_name = var_name
            parameter_values[var_name] = var_value
        num_defaults = len(self.param_defaults)
        # Give default values to all the parameters that have a
        # default value and are not already set
        # As an important observation, defaults are matched as follows:
        # defaults[-1] goes with positionals[-1]
        # defaults[-2] goes with positionals[-2]
        # ...
        # Also, |defaults| <= |positionals|
        for (var_name, default_value) in reversed(list(zip(list(reversed(self.param_args))[:num_defaults],  # noqa
                                                           list(reversed(self.param_defaults))))):  # noqa
            if var_name not in parameter_values:
                real_var_name = parameter.get_kwarg_name(var_name)
                parameter_values[real_var_name] = default_value
        # Process variadic and keyword arguments
        # Note that they are stored with custom names
        # This will allow us to determine the class of each parameter
        # and their order in the case of the variadic ones
        # Process the variadic arguments
        for (i, var_arg) in enumerate(args[num_positionals:]):
            parameter_values[parameter.get_vararg_name(self.param_varargs, i)] = var_arg  # noqa
        # Process keyword arguments
        for (name, value) in kwargs.items():
            parameter_values[parameter.get_kwarg_name(name)] = value
        # Build a dictionary of parameters
        self.parameters = OrderedDict()
        # Assign directions to parameters
        for var_name in parameter_values.keys():
            # Is the argument a vararg? or a kwarg? Then check the direction
            # for varargs or kwargs
            if parameter.is_vararg(var_name):
                self.parameters[var_name] = parameter.get_parameter_copy(self.get_varargs_direction())  # noqa
            elif parameter.is_kwarg(var_name):
                real_name = parameter.get_name_from_kwarg(var_name)
                self.parameters[var_name] = self.decorator_arguments.get(real_name,  # noqa
                                                                         self.get_default_direction(real_name))  # noqa
            else:
                # The argument is named, check its direction
                # Default value = IN if not class or instance method and
                #                 isModifier, INOUT otherwise
                # see self.get_default_direction
                # Note that if we have something like @task(self = IN) it
                # will have priority over the default
                # direction resolution, even if this implies a contradiction
                # with the target_direction flag
                self.parameters[var_name] = self.decorator_arguments.get(var_name,  # noqa
                                                                         self.get_default_direction(var_name))  # noqa

            # If the parameter is a FILE then its type will already be defined,
            # and get_compss_type will misslabel it as a TYPE.STRING
            if self.parameters[var_name].type is None:
                self.parameters[var_name].type = parameter.get_compss_type(parameter_values[var_name])  # noqa

            # TODO: add 'dir_name' to the parameter object
            if parameter.is_file(self.parameters[var_name]) or \
               parameter.is_directory(self.parameters[var_name]):
                if parameter_values[var_name]:
                    self.parameters[var_name].file_name = parameter_values[var_name]  # noqa
                else:
                    # is None: Used None for a FILE or DIRECTORY parameter path
                    self.parameters[var_name].type = parameter.TYPE.NULL
            else:
                self.parameters[var_name].object = parameter_values[var_name]

        # Check the arguments - Look for mandatory and unexpected arguments
        supported_args = SUPPORTED_ARGUMENTS + DEPRECATED_ARGUMENTS + self.param_args  # noqa
        check_arguments(MANDATORY_ARGUMENTS,
                        DEPRECATED_ARGUMENTS,
                        supported_args,
                        list(self.decorator_arguments.keys()),
                        "@task")

    def get_parameter_direction(self, name):
        """
        Returns the direction of any parameter

        :param name: Name of the parameter
        :return: Its direction inside this task
        """
        if parameter.is_vararg(name):
            return self.get_varargs_direction()
        elif parameter.is_return(name):
            return parameter.get_new_parameter('OUT')
        orig_name = parameter.get_name_from_kwarg(name)
        if orig_name in self.decorator_arguments:
            return self.decorator_arguments[orig_name]
        return self.get_default_direction(orig_name)

    def update_direction_of_worker_parameters(self, args):
        """
        Update worker parameter directions, will be useful to determine if
         files should be written later

        :param args: List of arguments
        :return: None. Modifies args
        """
        for arg in args:
            arg.direction = self.get_parameter_direction(arg.name)

    def is_parameter_an_object(self, name):
        """
        Given the name of a parameter, determine if it is an object or not

        :param name: Name of the parameter
        :return: True if the parameter is a (serializable) object
        """
        original_name = parameter.get_original_name(name)
        # Get the args parameter object
        if parameter.is_vararg(original_name):
            return self.get_varargs_direction().type is None
        # Is this parameter annotated in the decorator?
        if original_name in self.decorator_arguments:
            annotated = [parameter.TYPE.COLLECTION,
                         parameter.TYPE.EXTERNAL_STREAM,
                         None]
            return self.decorator_arguments[original_name].type in annotated
        # The parameter is not annotated in the decorator, so (by default)
        # return True
        return True

    def is_parameter_file_collection (self, name):
        """
        Given the name of a parameter, determine if it is an file collection or not

        :param name: Name of the parameter
        :return: True if the parameter is a file collection
        """
        original_name = parameter.get_original_name(name)
        # Get the args parameter object
        if parameter.is_vararg(original_name):
            return self.get_varargs_direction().is_file_collection
        # Is this parameter annotated in the decorator?
        if original_name in self.decorator_arguments:
            return self.decorator_arguments[original_name].is_file_collection
        # The parameter is not annotated in the decorator, so (by default)
        # return False
        return False

    def reveal_objects(self, args):
        """
        This function takes the arguments passed from the persistent worker
        and treats them to get the proper parameters for the user function.

        :param args: Arguments
        :return: None
        """

        def storage_supports_pipelining():
            # Some storage implementations use pipelining
            # Pipelining means "accumulate the getByID queries and perform them
            # in a single megaquery".
            # If this feature is not available (storage does not support it)
            # getByID operations will be performed one after the other
            try:
                import storage.api
                return storage.api.__pipelining__
            except Exception:
                return False

        def retrieve_content(arg, name_prefix, depth=0):
            # This case is special, as a FILE can actually mean a FILE or an
            # object that is serialized in a file
            if parameter.is_vararg(arg.name):
                self.param_varargs = arg.name
            if arg.type == parameter.TYPE.FILE:
                if self.is_parameter_an_object(arg.name):
                    # The object is stored in some file, load and deserialize
                    arg.content = deserialize_from_file(
                        arg.file_name.split(':')[-1]
                    )
                else:
                    # The object is a FILE, just forward the path of the file
                    # as a string parameter
                    arg.content = arg.file_name.split(':')[-1]
            elif arg.type == parameter.TYPE.DIRECTORY:
                arg.content = arg.file_name.split(":")[-1]
            elif arg.type == parameter.TYPE.EXTERNAL_STREAM:
                arg.content = deserialize_from_file(arg.file_name)
            elif arg.type == parameter.TYPE.COLLECTION:
                arg.content = []
                # This field is exclusive for COLLECTION_T parameters, so make
                # sure you have checked this parameter is a collection before
                # consulting it
                arg.collection_content = []
                col_f_name = arg.file_name.split(':')[-1]

                # maybe it is an inner-collection..
                _dec_arg = self.decorator_arguments.get(arg.name, None)
                _col_dir = _dec_arg.direction if _dec_arg else None
                _col_dep = _dec_arg.depth if _dec_arg else depth

                for (i, line) in enumerate(open(col_f_name, 'r')):
                    data_type, content_file, content_type = line.strip().split()
                    # Same naming convention as in COMPSsRuntimeImpl.java
                    sub_name = "%s.%d" % (arg.name, i)
                    if name_prefix:
                        sub_name = "%s.%s" % (name_prefix, arg.name)
                    else:
                        sub_name = "@%s" % sub_name

                    if not self.is_parameter_file_collection(arg.name):
                        sub_arg, _ = build_task_parameter(int(data_type),
                                                          None,
                                                          "",
                                                          sub_name,
                                                          content_file,
                                                          arg.content_type)

                        # if direction of the collection is 'out', it means we
                        # haven't received serialized objects from the Master
                        # (even though parameters have 'file_name', those files
                        # haven't been created yet). plus, inner collections of
                        # col_out params do NOT have 'direction', we identify
                        # them by 'depth'..
                        if _col_dir == parameter.DIRECTION.OUT or \
                                ((_col_dir is None) and _col_dep > 0):

                            # if we are at the last level of COL_OUT param,
                            # create 'empty' instances of elements
                            if _col_dep == 1:
                                temp = create_object_by_con_type(content_type)
                                sub_arg.content = temp
                                arg.content.append(sub_arg.content)
                                arg.collection_content.append(sub_arg)
                            else:
                                retrieve_content(sub_arg, sub_name,
                                                 depth=_col_dep-1)
                                arg.content.append(sub_arg.content)
                                arg.collection_content.append(sub_arg)
                        else:
                            # Recursively call the retrieve method, fill the
                            # content field in our new taskParameter object
                            retrieve_content(sub_arg, sub_name)
                            arg.content.append(sub_arg.content)
                            arg.collection_content.append(sub_arg)
                    else:
                        arg.content.append(content_file)
                        arg.collection_content.append(content_file)

            elif not storage_supports_pipelining() and \
                    arg.type == parameter.TYPE.EXTERNAL_PSCO:
                # The object is a PSCO and the storage does not support
                # pipelining, do a single getByID of the PSCO
                from storage.api import getByID
                arg.content = getByID(arg.key)
                # If we have not entered in any of these cases we will assume
                # that the object was a basic type and the content is already
                # available and properly casted by the python worker

        if storage_supports_pipelining():
            # Perform the pipelined getByID operation
            pscos = [x for x in args if x.type == parameter.TYPE.EXTERNAL_PSCO]
            identifiers = [x.key for x in pscos]
            from storage.api import getByID
            objects = getByID(*identifiers)
            # Just update the TaskParameter object with its content
            for (obj, value) in zip(objects, pscos):
                obj.content = value

        # Deal with all the parameters that are NOT returns
        for arg in [x for x in args if isinstance(x, parameter.TaskParameter) and not parameter.is_return(x.name)]:  # noqa
            retrieve_content(arg, "")

    def worker_call(self, *args, **kwargs):
        """
        This part deals with task calls in the worker's side
        Note that the call to the user function is made by the worker,
        not by the user code.

        :return: A function that calls the user function with the given
                 parameters and does the proper serializations and updates
                 the affected objects.
        """
        # Self definition (only used when defined in the task)
        self_type = None
        self_value = None
        compss_exception = None
        # All parameters are in the same args list. At the moment we only know
        # the type, the name and the "value" of the parameter. This value may
        # be treated to get the actual object (e.g: deserialize it, query the
        # database in case of persistent objects, etc.)
        self.reveal_objects(args)
        # After this line all the objects in arg have a "content" field, now
        # we will segregate them in User positional and variadic args
        user_args = []
        # User named args (kwargs)
        user_kwargs = {}
        # Return parameters, save them apart to match the user returns with
        # the internal parameters
        ret_params = []

        for arg in args:
            # Just fill the three data structures declared above
            # Deal with the self parameter (if any)
            if not isinstance(arg, parameter.TaskParameter):
                user_args.append(arg)
            # All these other cases are all about regular parameters
            elif parameter.is_return(arg.name):
                ret_params.append(arg)
            elif parameter.is_kwarg(arg.name):
                user_kwargs[parameter.get_name_from_kwarg(arg.name)] = \
                    arg.content
            else:
                if parameter.is_vararg(arg.name):
                    self.param_varargs = parameter.get_varargs_name(arg.name)
                # Apart from the names we preserve the original order, so it
                # is guaranteed that named positional arguments will never be
                # swapped with variadic ones or anything similar
                user_args.append(arg.content)

        num_returns = len(ret_params)

        # Save the self object type and value before executing the task
        # (it could be persisted inside if its a persistent object)
        has_self = False
        if args and not isinstance(args[0], parameter.TaskParameter):
            # Then the first arg is self
            has_self = True
            self_type = parameter.get_compss_type(args[0])
            if self_type == parameter.TYPE.EXTERNAL_PSCO:
                self_value = args[0].getID()
            else:
                # Since we are checking the type of the deserialized self
                # parameter, get_compss_type will return that its type is
                # parameter.TYPE.OBJECT, which although it is an object, self
                # is always a file for the runtime. So we must force its type
                # to avoid that the return message notifies that it has a new
                # type "object" which is not supported for python objects in
                # the runtime.
                self_type = parameter.TYPE.FILE
                self_value = 'null'

        # Tracing hook is disabled by default during the user code of the task.
        # The user can enable it with tracing_hook=True in @task decorator for
        # specific tasks or globally with the COMPSS_TRACING_HOOK=true
        # environment variable.
        restore_hook = False
        pro_f = None
        if kwargs['compss_tracing']:
            global_tracing_hook = False
            if TRACING_HOOK_ENV_VAR in os.environ:
                hook_enabled = os.environ[TRACING_HOOK_ENV_VAR] == "true"
                global_tracing_hook = hook_enabled
            if self.decorator_arguments['tracing_hook'] or global_tracing_hook:
                # The user wants to keep the tracing hook
                pass
            else:
                # When Extrae library implements the function to disable,
                # use it, as:
                #     import pyextrae
                #     pro_f = pyextrae.shutdown()
                # Since it is not available yet, we manage the tracing hook
                # by ourselves
                pro_f = sys.getprofile()
                sys.setprofile(None)
                restore_hook = True

        # Call the user function with all the reconstructed parameters and
        # get the return values
        if self.decorator_arguments['numba']:
            # Import all supported functionalities
            from numba import jit
            from numba import njit
            from numba import generated_jit
            from numba import vectorize
            from numba import guvectorize
            from numba import stencil
            from numba import cfunc
            numba_mode = self.decorator_arguments['numba']
            numba_flags = self.decorator_arguments['numba_flags']
            if type(numba_mode) is dict:
                # Use the flags defined by the user
                numba_flags['cache'] = True  # Always force cache
                user_returns = \
                    jit(self.user_function,
                        **numba_flags)(*user_args, **user_kwargs)
            elif numba_mode is True or numba_mode == 'jit':
                numba_flags['cache'] = True  # Always force cache
                user_returns = jit(self.user_function,
                                   **numba_flags)(*user_args,
                                                  **user_kwargs)
                # Alternative way of calling:
                # user_returns = jit(cache=True)(self.user_function) \
                #                   (*user_args, **user_kwargs)
            elif numba_mode == 'generated_jit':
                user_returns = generated_jit(self.user_function,
                                             **numba_flags)(*user_args,
                                                            **user_kwargs)
            elif numba_mode == 'njit':
                numba_flags['cache'] = True  # Always force cache
                user_returns = njit(self.user_function,
                                    **numba_flags)(*user_args, **user_kwargs)
            elif numba_mode == 'vectorize':
                numba_signature = self.decorator_arguments['numba_signature']
                user_returns = vectorize(
                    numba_signature,
                    **numba_flags
                )(self.user_function)(*user_args, **user_kwargs)
            elif numba_mode == 'guvectorize':
                numba_signature = self.decorator_arguments['numba_signature']
                numba_decl = self.decorator_arguments['numba_declaration']
                user_returns = guvectorize(
                    numba_signature,
                    numba_decl,
                    **numba_flags
                )(self.user_function)(*user_args, **user_kwargs)
            elif numba_mode == 'stencil':
                user_returns = stencil(
                    **numba_flags
                )(self.user_function)(*user_args, **user_kwargs)
            elif numba_mode == 'cfunc':
                numba_signature = self.decorator_arguments['numba_signature']
                user_returns = cfunc(
                                   numba_signature
                               )(self.user_function).ctypes(*user_args,
                                                            **user_kwargs)
            else:
                raise Exception("Unsupported numba mode.")
        else:
            try:
                # Normal task execution
                user_returns = self.user_function(*user_args, **user_kwargs)
            except COMPSsException as ce :
                compss_exception = ce
                # Check old targetDirection
                if 'targetDirection' in self.decorator_arguments:
                    target_label = 'targetDirection'
                else:
                    target_label = 'target_direction'
                compss_exception.target_direction = self.decorator_arguments[target_label]

        # Reestablish the hook if it was disabled
        if restore_hook:
            sys.setprofile(pro_f)

        # Manage all the possible outputs of the task and build the return new
        # types and values
        def get_file_name(file_path):
            return file_path.split(':')[-1]

        python_mpi = False
        if kwargs["python_MPI"]:
            python_mpi = True

        # Deal with INOUTs and COL_OUTs

        def get_collection_objects(_content, _arg):
            """ Retrieve collection objects recursively
            """
            if _arg.type == parameter.TYPE.COLLECTION:
                for (new_con, _elem) in zip(_arg.content,
                                            _arg.collection_content):
                    for sub_el in get_collection_objects(new_con, _elem):
                        yield sub_el
            else:
                yield (_content, _arg)

        for arg in args:
            # handle only task parameters that are objects

            # skip files and non-task-parameters
            if not isinstance(arg, parameter.TaskParameter) or \
                    not self.is_parameter_an_object(arg.name):
                continue

            # file collections are objects, but must be skipped as well
            if self.is_parameter_file_collection(arg.name):
                continue

            # skip psco
            # since param.type has the old type, we can not use:
            #     param.type != parameter.TYPE.EXTERNAL_PSCO
            _is_psco_true = (arg.type == parameter.TYPE.EXTERNAL_PSCO or
                             is_psco(arg.content))
            if _is_psco_true:
                continue

            original_name = parameter.get_original_name(arg.name)
            param = self.decorator_arguments.get(
                original_name, self.get_default_direction(original_name))

            # skip non-inouts or non-col_outs
            _is_col_out = (arg.type == parameter.TYPE.COLLECTION and
                           param.direction == parameter.DIRECTION.OUT)

            _is_inout = (param.direction == parameter.DIRECTION.INOUT or
                         param.direction == parameter.DIRECTION.COMMUTATIVE)

            if not (_is_inout or _is_col_out):
                continue

            # Now it's 'INOUT' or 'COL_OUT' object param, serialize to a file
            if arg.type == parameter.TYPE.COLLECTION:
                # handle collections recursively
                for (content, elem) in get_collection_objects(arg.content, arg):
                    f_name = get_file_name(elem.file_name)
                    if python_mpi:
                        serialize_to_file_mpienv(content, f_name, False)
                    else:
                        serialize_to_file(content, f_name)
            else:
                f_name = get_file_name(arg.file_name)
                if python_mpi:
                    serialize_to_file_mpienv(arg.content, f_name, False)
                else:
                    serialize_to_file(arg.content, f_name)

        if compss_exception is None:
            raise compss_exception

        # Deal with returns (if any)
        if num_returns > 0:
            if num_returns == 1:
                # Generalize the return case to multi-return to simplify the
                # code
                user_returns = [user_returns]
            elif num_returns > 1 and python_mpi:

                def get_ret_rank(ret_params):
                    from mpi4py import MPI
                    return [ret_params[MPI.COMM_WORLD.rank]]

                user_returns = [user_returns]
                ret_params = get_ret_rank(ret_params)
            # Note that we are implicitly assuming that the length of the user
            # returns matches the number of return parameters
            for (obj, param) in zip(user_returns, ret_params):
                # If the object is a PSCO, do not serialize to file
                if param.type == parameter.TYPE.EXTERNAL_PSCO or is_psco(obj):
                    continue
                # Serialize the object
                # Note that there is no "command line optimization" in the
                # returns, as we always pass them as files.
                # This is due to the asymmetry in worker-master communications
                # and because it also makes it easier for us to deal with
                # returns in that format
                f_name = get_file_name(param.file_name)
                if python_mpi:
                    if num_returns > 1:
                        rank_zero_reduce = False
                    else:
                        rank_zero_reduce = True

                    serialize_to_file_mpienv(obj, f_name, rank_zero_reduce)
                else:
                    serialize_to_file(obj, f_name)

        # We must notify COMPSs when types are updated
        # Potential update candidates are returns and INOUTs
        # But the whole types and values list must be returned
        # new_types and new_values correspond to "parameters self returns"
        new_types, new_values = [], []

        # Add parameter types and value
        params_start = 1 if has_self else 0
        params_end = len(args) - num_returns + 1
        # Update new_types and new_values with the args list
        # The results parameter is a boolean to distinguish the error message.
        for arg in args[params_start:params_end - 1]:
            # Loop through the arguments and update new_types and new_values
            if not isinstance(arg, parameter.TaskParameter):
                raise Exception('ERROR: A task parameter arrived as an' +
                                ' object instead as a TaskParameter' +
                                ' when building the task result message.')
            else:
                original_name = parameter.get_original_name(arg.name)
                param = self.decorator_arguments.get(original_name,
                                                     self.get_default_direction(original_name))  # noqa
                if arg.type == parameter.TYPE.EXTERNAL_PSCO:
                    # It was originally a persistent object
                    new_types.append(parameter.TYPE.EXTERNAL_PSCO)
                    new_values.append(arg.key)
                elif is_psco(arg.content) and \
                        param.direction != parameter.DIRECTION.IN:
                    # It was persisted in the task
                    new_types.append(parameter.TYPE.EXTERNAL_PSCO)
                    new_values.append(arg.content.getID())
                else:
                    # Any other return object: same type and null value
                    new_types.append(arg.type)
                    new_values.append('null')

        # Check old targetDirection
        if 'targetDirection' in self.decorator_arguments:
            target_label = 'targetDirection'
        else:
            target_label = 'target_direction'

        # Add self type and value if exist
        if has_self:
            if self.decorator_arguments[target_label].direction == parameter.DIRECTION.INOUT:  # noqa
                # Check if self is a PSCO that has been persisted inside the
                # task and target_direction.
                # Update self type and value
                self_type = parameter.get_compss_type(args[0])
                if self_type == parameter.TYPE.EXTERNAL_PSCO:
                    self_value = args[0].getID()
                else:
                    # Self can only be of type FILE, so avoid the last update
                    # of self_type
                    self_type = parameter.TYPE.FILE
                    self_value = 'null'
            new_types.append(self_type)
            new_values.append(self_value)

        # Add return types and values
        # Loop through the rest of the arguments and update new_types and
        #  new_values.
        # assert len(args[params_end - 1:]) == len(user_returns)
        # add_parameter_new_types_and_values(args[params_end - 1:], True)
        if num_returns > 0:
            for ret in user_returns:
                ret_type = parameter.get_compss_type(ret)
                if ret_type == parameter.TYPE.EXTERNAL_PSCO:
                    ret_value = ret.getID()
                else:
                    # Returns can only be of type FILE, so avoid the last
                    # update of ret_type
                    ret_type = parameter.TYPE.FILE
                    ret_value = 'null'
                new_types.append(ret_type)
                new_values.append(ret_value)

        return new_types, new_values, self.decorator_arguments[target_label]

    def sequential_call(self, *args, **kwargs):
        """
        The easiest case: just call the user function and return whatever it
        returns.

        :return: The user function
        """
        # Inspect the user function, get information about the arguments and
        # their names
        # This defines self.param_args, self.param_varargs,
        # self.param_kwargs and self.param_defaults
        # And gives non-None default values to them if necessary
        from pycompss.api.dummy.task import task as dummy_task
        d_t = dummy_task(args, kwargs)
        return d_t.__call__(self.user_function)(*args, **kwargs)


# task can be also typed as Task
task = Task
