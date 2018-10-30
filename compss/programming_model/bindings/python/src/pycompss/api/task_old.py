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
PyCOMPSs API - Task
===================
    This file contains the class task, needed for the task definition and the
    reveal_objects function.
"""

import inspect
import os
import logging
import ast
import copy

from collections import OrderedDict
from functools import wraps

from pycompss.runtime.commons import IS_PYTHON3
from pycompss.api.parameter import *

if IS_PYTHON3:
    # Shadow long with int
    long = int

if __debug__:
    logger = logging.getLogger('pycompss.api.task')
    # logger = logging.getLogger()   # for jupyter logging  # TODO: detect if jupyter and set logger
    # logger.setLevel(logging.DEBUG)


class Task(object):

    def __init__(self, *args, **kwargs):
        """
        If there are decorator arguments, the function to be decorated is
        not passed to the constructor!
        """

        from pycompss.util.location import i_am_within_scope

        # Check if under the PyCOMPSs scope
        if i_am_within_scope():
            from pycompss.util.location import i_am_at_master
            from pycompss.api.parameter import is_parameter, get_new_parameter

            self.scope = True

            if __debug__:
                logger.debug("Init @task decorator...")

            # Defaults
            self.args = args  # Not used
            self.kwargs = kwargs  # The only ones actually used: (decorators)

            # Pre-process decorator arguments

            # Reserved PyCOMPSs keywords and default values
            reserved_keywords = {
                'isModifier': True,
                'returns': False,
                'priority': False,
                'isReplicated': False,
                'isDistributed': False,
                'varargsType': IN
            }

            # Set reserved keyword default values in self.kwargs
            for (reserved_keyword, default_value) in reserved_keywords.items():
                if reserved_keyword not in self.kwargs:
                    self.kwargs[reserved_keyword] = default_value

            # Instantiate the parameters as new Parameter objects into self.kwargs
            # taking into consideration the key of the arg_value.
            # See parameter.py conversion dict.
            for arg_name, arg_value in self.kwargs.items():
                # Common parameter definition
                if is_parameter(arg_value):
                    key = arg_value.key
                    self.kwargs[arg_name] = get_new_parameter(key)
                # Parameter defined as dictionary with fields
                if isinstance(arg_value, dict) and 'type' in arg_value:
                    key = arg_value['type'].key
                    arg_value['type'] = get_new_parameter(key)
                    self.kwargs[arg_name] = _from_dict_to_parameter(arg_value)

            if __debug__:
                logger.debug("Init @task decorator finished.")
        else:
            # Not under the PyCOMPSs scope
            self.scope = False
            # Defaults
            self.args = args
            self.kwargs = kwargs

    def __call__(self, f):
        """
        If there are decorator arguments, __call__() is only called
        once, as part of the decoration process! You can only give
        it a single argument, which is the function object.
        """

        # Check if under the PyCOMPSs scope
        if not self.scope:
            return self.__not_under_pycompss_scope(f)

        # Imports
        from pycompss.util.interactive_helpers import update_tasks_code_file
        from pycompss.util.location import i_am_at_master

        if __debug__:
            logger.debug("Call in @task decorator...")

        # Assume it is an instance method if the first parameter of the
        # function is called 'self'
        # "I would rely on the convention that functions that will become
        # methods have a first argument named self, and other functions don't.
        # Fragile, but then, there's no really solid way."
        self.f_argspec = inspect.getargspec(f)

        # Set default variables
        self.has_self_parameter = False
        self.has_cls_parameter = False
        self.has_varargs = False
        self.has_keywords = False
        self.has_defaults = False
        self.parameters = OrderedDict()
        self.returns = OrderedDict()
        self.is_replicated = False
        self.is_distributed = False
        self.module_name = ''

        # Step 1.- Check if it is an instance method.
        # Question: Will the first condition evaluate to false? spec_args will
        # always be a named tuple, so it will always return true if evaluated
        # as a bool
        # Answer: The first condition evaluates if args exists (a list) and is
        # not empty in the spec_args. The second checks if the first argument
        # in that list is 'self'. In case that the args list exists and its
        # first element is self, then the function is considered as an instance
        # function (task defined within a class).
        direction = DIRECTION.INOUT  # default 'self' direction
        if self.f_argspec.args and self.f_argspec.args[0] == 'self':
            self.has_self_parameter = True
            if self.kwargs['isModifier']:
                direction = DIRECTION.INOUT
            else:
                direction = DIRECTION.IN

        # Step 2.- Check if it is a class method.
        # The check of 'cls' may be weak but it is PEP8 style agreements.
        if self.f_argspec.args and self.f_argspec.args[0] == 'cls':
            self.has_cls_parameter = True
            direction = DIRECTION.IN

        # Step 1 or 2 b - Add class object parameter
        if self.has_self_parameter or self.has_cls_parameter:
            self.kwargs['self'] = Parameter(p_type=TYPE.OBJECT,
                                            p_direction=direction)

        # Step 3.- Check if it has varargs (contains *args?)
        # Check if contains *args
        if self.f_argspec.varargs is not None:
            self.has_varargs = True

        # Step 4.- Check if it has keyword arguments (contains **kwargs?)
        # Check if contains **kwargs
        if self.f_argspec.keywords is not None:
            self.has_keywords = True

        # Step 5.- Check if it has default values
        # Check if has default values
        if self.f_argspec.defaults is not None:
            self.has_defaults = True

        # Step 6.- Check if the keyword returns has been specified by the user.
        # Check if the keyword returns has been specified by the user.
        if self.kwargs['returns']:
            self.__update_return_type()
        else:
            # If no returns statement found, double check to see if the user has specified a return statement.
            self.__update_return_if_no_returns(f)

        # Step 7.- Check if the keyword isReplicated has been specified by the user.
        if self.kwargs['isReplicated']:
            self.is_replicated = True

        # Step 8.- Check if the keyword isDistributed has been specified by the user.
        if self.kwargs['isDistributed']:
            self.is_distributed = True

        # Step 9.- Get module (for invocation purposes in the worker)
        mod = inspect.getmodule(f)
        self.module_name = mod.__name__

        if self.module_name == '__main__' or self.module_name == 'pycompss.runtime.launch':
            # The module where the function is defined was run as __main__,
            # We need to find out the real module name:

            # Get the real module name from our launch.py app_path global variable
            try:
                path = getattr(mod, "app_path")
            except AttributeError:
                # This exception is raised when the runtime is not running and the @task decorator is used.
                # The runtime has not been started yet.
                return self.__not_under_pycompss_scope(f)

            # Get the file name
            file_name = os.path.splitext(os.path.basename(path))[0]

            # Do any necessary pre processing action before executing any code
            if file_name.startswith('InteractiveMode'):
                # If the file_name starts with 'InteractiveMode' means that
                # the user is using PyCOMPSs from jupyter-notebook.
                # Convention between this file and interactive.py
                # In this case it is necessary to do a pre-processing step
                # that consists of putting all user code that may be executed
                # in the worker on a file.
                # This file has to be visible for all workers.
                update_tasks_code_file(f, path)
            else:
                # work as always
                pass

            # Get the module
            self.module_name = _get_module_name(path, file_name)

        # The registration needs to be done only in the master node
        if i_am_at_master():
            self.__register_task(f)

        # Modified variables until now that will be used later:
        #   - self.f_argspec          : Function argspect (Named tuple)
        #                               e.g. ArgSpec(args=['a', 'b', 'compss_retvalue'], varargs=None,
        #                               keywords=None, defaults=None)
        #   - self.has_self_parameter : Boolean - if the function is an instance (contains self in the f_argspec)
        #   - self.has_cls_parameter  : Boolean - if the function is a class method (contains cls in the f_argspec)
        #   - self.has_varargs        : Boolean - if the function has *args
        #   - self.has_keywords       : Boolean - if the function has **kwargs
        #   - self.has_defaults       : Boolean - if the function has default values
        #   - self.parameters         : OrderedDict of function's Parameters
        #   - self.returns            : OrderedDict of return's Parameters
        #   - self.is_replicated      : Boolean - if the task is replicated
        #   - self.is_distributed     : Boolean - if the task is distributed
        #   - self.module_name        : String  - Module name (e.g. test.kmeans)
        # Other variables that will be used:
        #   - f                 : Decorated function
        #   - self.args         : Decorator args tuple (usually empty)
        #   - self.kwargs       : Decorator keywords dictionary.
        #                         e.g. {'priority': True, 'isModifier': True, 'returns': <type 'dict'>,
        #                               'self': <pycompss.api.parameter.Parameter instance at 0xXXXXXXXXX>,
        #                               'compss_retvalue': <pycompss.api.parameter.Parameter instance at 0xXXXXXXXX>}

        if __debug__:
            logger.debug("Call in @task decorator finished.")

        @wraps(f)
        def wrapped_f(*args, **kwargs):
            # args   - <Tuple>      - Contains the objects that the function has been called with (positional).
            # kwargs - <Dictionary> - Contains the named objects that the function has been called with.

            # By default, each task is set to use one core.
            computing_nodes = 1
            if 'computingNodes' in kwargs:
                # There is a @mpi decorator over task that overrides the
                # default value of computing nodes
                computing_nodes = kwargs['computingNodes']
                del kwargs['computingNodes']

            # Check if this call is nested using the launch_pycompss_module
            # function from launch.py.
            is_nested = False
            for i_s in inspect.stack():
                if i_s[3] in ['launch_pycompss_module', 'launch_pycompss_application']:
                    is_nested = True

            # Prepare parameters and returns.
            # Update self.parameters and self.returns with the appropriate Parameter objects.
            self.__build_parameters_and_return_dicts(f, args, kwargs)

            if not i_am_at_master() and not is_nested:
                # Task decorator worker body code.
                new_types, new_values = self.worker_code(f, args, kwargs)
                return new_types, new_values
            else:
                # Task decorator master body code.
                # Returns the future object that will be used instead of the
                # actual function return.
                fo = self.master_code(f, computing_nodes, args)
                return fo

        return wrapped_f

    # ############################################################################ #
    # ############### TASK DECORATOR PROLOG PRIVATE METHODS ###################### #
    # ############################################################################ #

    def __not_under_pycompss_scope(self, f, show_message=False):
        """
        Prints the warning message when running a PyCOMPSs application without the PyCOMPSs module and retrieves
        the dummy task decorator.

        :return: dummy task decorator
        """

        if show_message:
            print("WARNING!!! You're trying to execute a python application with PyCOMPSs decorators without \
                    the COMPSs Runtime running.")
            print("Please, start the COMPSs Runtime before using task decorated functions in order to avoid this \
                    message and exploit the parallelization.")
            print("Suggestion: Use the 'runcompss' command or the 'start' function from pycompss.interactive module \
                    depending on your needs.")
            print("            Alternatively, use python -m pycompss run/enqueue [flags] application [parameters]")

        from pycompss.api.dummy.task import task as dummy_task
        d_t = dummy_task(self.args, self.kwargs)
        return d_t.__call__(f)

    def __update_return_type(self):
        """
        Updates the return types within self.returns ordered dict.

        :return: None
        """

        from pycompss.api.parameter import Parameter
        from pycompss.api.parameter import DIRECTION

        # Manage Simple returns specified by the user
        # This condition is interesting, because a user can write returns=list, lists have attribute
        # __len__ but an exception is raised. Consequently, if users do not specify the length
        # it will be managed as a single return
        # When the user specifies the length, it is possible to manage the elements independently.
        # Also, users can use a global variable to refer the arguments size (string)

        ret_kwarg = self.kwargs['returns']
        if not hasattr(ret_kwarg, '__len__') or type(ret_kwarg) is type:
            # Simple return
            ret_type = _get_compss_type(ret_kwarg)
            param = Parameter(p_type=ret_type, p_direction=DIRECTION.OUT)
            param.object = ret_kwarg
            self.returns['compss_retvalue'] = param
        elif isinstance(ret_kwarg, str):
            # Multi-return in a global variable or a string wrapping an int
            ret_type = _get_compss_type(ret_kwarg)
            param = Parameter(p_type=ret_type, p_direction=DIRECTION.OUT)
            param.object = ret_kwarg
            self.returns['compss_retvalue'] = param
        else:
            # Multi-return (list)
            for index, ret in enumerate(ret_kwarg):
                ret_type = _get_compss_type(ret)
                param = Parameter(p_type=ret_type, p_direction=DIRECTION.OUT)
                param.object = ret
                self.returns['compss_retvalue' + str(index)] = param

    def __update_return_if_no_returns(self, f):
        """
        Checks the code looking for return statements if no returns is specified in @task decorator.

        WARNING: Updates self.return if returns are found.

        :param f: Function to check
        """

        from pycompss.api.parameter import Parameter
        from pycompss.api.parameter import DIRECTION
        from pycompss.api.parameter import TYPE

        source_code = _get_wrapped_source(f).strip()

        if self.has_self_parameter or source_code.startswith(
                '@classmethod'):  # TODO: WHAT IF IS CLASSMETHOD FROM BOOLEAN?
            # It is a task defined within a class (can not parse the code with ast since the class does not
            # exist yet. Alternatively, the only way I see is to parse it manually line by line.
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
            print("INFO! Return found in function " + f.__name__ + " without 'returns' statement at task definition.")
            has_multireturn = False
            lines = [i for i, li in enumerate(ret_mask) if li]
            max_num_returns = 0
            if self.has_self_parameter or source_code.startswith('@classmethod'):
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
                if __debug__:
                    logger.debug("Multireturn found: %s" % str(max_num_returns))
                i = 0
                for _ in range(max_num_returns):
                    param = Parameter(p_type=TYPE.FILE, p_direction=DIRECTION.OUT)
                    param.object = object()
                    self.returns['compss_retvalue' + str(i)] = param
                    i += 1
            else:
                if __debug__:
                    logger.debug("Return found")
                param = Parameter(p_type=TYPE.FILE, p_direction=DIRECTION.OUT)
                param.object = object()
                self.returns['compss_retvalue'] = param
        else:
            # Return not found
            pass

    def __register_task(self, f):
        """
        This function is used to register the task in the runtime.
        This registration must be done only once on the task decorator
        initialization

        :param f: Function to be registered
        """

        from pycompss.runtime.core_element import CE
        import pycompss.runtime.binding as binding

        # Look for the decorator that has to do the registration
        # Since the __init__ of the decorators is independent, there is no way
        # to pass information through them.
        # However, the __call__ method of the decorators can be used.
        # The way that they are called is from bottom to top. So, the first one to
        # call its __call__ method will always be @task. Consequently, the @task
        # decorator __call__ method can detect the top decorator and pass a hint
        # to order that decorator that has to do the registration (not the others).
        func_code = ''
        got_func_code = False
        func = f
        while not got_func_code:
            try:
                func_code = _get_wrapped_sourcelines(func)
                got_func_code = True
            except IOError:
                # There is one or more decorators below the @task --> undecorate
                # until possible to get the func code.
                # Example of this case: test 19: @timeit decorator below the
                # @task decorator.
                func = func.__wrapped__

        decorator_keys = ("implement", "constraint", "decaf", "mpi", "ompss", "binary", "opencl", "task")

        top_decorator = _get_top_decorator(func_code, decorator_keys)
        if __debug__:
            logger.debug(
                "[@TASK] Top decorator of function %s in module %s: %s" % (f.__name__,
                                                                           self.module_name,
                                                                           str(top_decorator)))
        f.__who_registers__ = top_decorator

        # not usual tasks - handled by the runtime without invoking the PyCOMPSs
        # worker. Needed to filter in order not to code the strings when using
        # them in these type of tasks
        decorator_filter = ("decaf", "mpi", "ompss", "binary", "opencl")
        default = 'task'
        task_type = _get_task_type(func_code, decorator_filter, default)
        if __debug__:
            logger.debug("[@TASK] Task type of function %s in module %s: %s" % (f.__name__,
                                                                                self.module_name,
                                                                                str(task_type)))
        f.__task_type__ = task_type
        if task_type == default:
            f.__code_strings__ = True
        else:
            f.__code_strings__ = False

        # Get the task signature
        # To do this, we will check the frames
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
            # This case is never reached with Python 3 since it includes frames that are not present with Python 2.
            ce_signature = self.module_name + "." + f.__name__
            impl_type_args = [self.module_name, f.__name__]
        else:
            # There is more than one frame
            # Discover if the task is defined within a class or subclass
            # Construct the qualified class name
            class_name = []
            # Weak way, but I see no other way compatible with both 2 and 3.
            for app_frame in app_frames:
                if (app_frame[3] != '<module>' and app_frame[4] is not None and
                        (app_frame[4][0].strip().startswith('@') or app_frame[4][0].strip().startswith('def'))):
                    # app_frame[3] != <module> ==> functions and classes
                    # app_frame[4] is not None ==> functions injected by the interpreter
                    # (app_frame[4][0].strip().startswith('@') or app_frame[4][0].strip().startswith('def')) ==> Ignores functions injected by wrappers (e.g. autoparallel), but keep the classes.
                    class_name.append(app_frame[3])
            class_name = '.'.join(class_name)
            if class_name:
                # Within class or subclass
                ce_signature = self.module_name + '.' + class_name + '.' + f.__name__
                impl_type_args = [self.module_name + '.' + class_name, f.__name__]
            else:
                # Not in a class or subclass
                # This case can be reached in Python 3, where particular frames are included, but not class names found.
                ce_signature = self.module_name + "." + f.__name__
                impl_type_args = [self.module_name, f.__name__]
        # Include the registering info related to @task
        impl_signature = ce_signature
        impl_constraints = {}
        impl_type = "METHOD"
        core_element = CE(ce_signature,
                          impl_signature,
                          impl_constraints,
                          impl_type,
                          impl_type_args)
        f.__to_register__ = core_element

        # Do the task register if I am the top decorator
        if f.__who_registers__ == __name__:
            if __debug__:
                logger.debug(
                    "[@TASK] I have to do the register of function %s in module %s" % (f.__name__, self.module_name))
                logger.debug("[@TASK] %s" % str(f.__to_register__))
            binding.register_ce(core_element)

    def __build_parameters_and_return_dicts(self, f, args, kwargs):
        """
        Build parameters and return dictionaries

        WARNING: Updates self.parameters dictionary
        WARNING: Updates self.returns dictionary

        :param f: Function
        :param args: Function args
        :param kwargs: Function kwargs
        """
        import pycompss.runtime.binding as binding

        param_keys = self.f_argspec.args
        param_values = args

        # Step 0.- Grab the 'compss_types' information if available.
        #          This only happens during the call function of the task decorator in the worker.
        at_worker = False
        worker_rets = None
        worker_kwargs = None
        if 'compss_types' in kwargs:
            # Then we are being called within the worker, and the args and kwargs differ.
            at_worker = True
            # For instance, args contains all parameters and instead of objects, their path
            # Also, kwargs contains worker defined keywords instead of the real dict
            # (e.g. compss_types, compss_tracing, compss_return_length, compss_storage_conf and compss_process_name
            param_values = list(param_values)  # Copy to list in order to use pop since it does not contain objects
            num_rets = kwargs['compss_return_length']
            if num_rets > 0:
                worker_rets = param_values[-num_rets:]
                param_values = param_values[:-num_rets]
            if self.has_keywords:
                worker_kwargs = param_values.pop()

        # Step 1.- Check self
        if self.has_self_parameter or self.has_cls_parameter:
            f_self = dict()
            self_name = param_keys[0]
            f_self[self_name] = self.kwargs['self']
            f_self[self_name].object = param_values[0]
            if self.kwargs['isModifier']:
                d = DIRECTION.INOUT
            else:
                d = DIRECTION.IN
            f_self[self_name].direction = d
            # Include in the first position
            self.parameters.update(f_self)

        # Step 2.- Build the parameters names and values lists.
        # Be very careful with parameter position.
        # The included are sorted by position. The rest may not.

        # Check how many parameters are defined in the function
        num_parameters = len(self.f_argspec.args)

        # Check if the user has defined default values and include them
        if self.has_defaults:
            # There are default parameters
            # Get the variable names and values that have been defined by default (_get_default_args(f)).
            # default_params will have a list of pairs of the form (argument, default_value)
            # Default values have to be always defined after undefined value parameters.
            default_params = _get_default_args(f)
            args_list = list(param_values)  # Given values
            # Default parameter addition
            for p in self.f_argspec.args[len(param_values):num_parameters]:
                if p in kwargs:
                    args_list.append(kwargs[p])
                    kwargs.pop(p)
                else:
                    for dp in default_params:
                        if p == dp[0]:
                            args_list.append(dp[1])
            param_values = tuple(args_list)

        # List of parameter names
        vals_names = list(self.f_argspec.args[:num_parameters])
        # List of values of each parameter
        vals_values = list(param_values[:num_parameters])  # first values of args are the parameters

        # Check if there are *args or **kwargs
        args_names = []
        args_vals = []
        if self.has_varargs:  # *args
            aargs = '*' + self.f_argspec.varargs
            if binding.aargs_as_tuple:
                # If the *args are expected to be managed as a tuple:
                args_names.append(aargs)  # Name used for the *args
                # last values will compose the *args parameter
                args_vals.append(param_values[num_parameters:])
            else:
                # If the *args are expected to be managed as individual elements:
                pos = 0
                for i in range(len(param_values[num_parameters:])):
                    args_names.append(aargs + str(pos))  # Name used for the *args
                    self.kwargs[aargs + str(pos)] = copy.copy(self.kwargs['varargsType'])
                    pos += 1
                args_vals = args_vals + list(param_values[num_parameters:])
        if self.has_keywords:  # **kwargs
            aakwargs = '**' + self.f_argspec.keywords  # Name used for the **kwargs
            args_names.append(aakwargs)
            # Check if some of the **kwargs are used as vals_values
            if len(vals_names) > len(vals_values):
                for i in range(len(vals_values), len(vals_names)):
                    vals_values.append(kwargs[vals_names[i]])
                    kwargs.pop(vals_names[i])
            if at_worker:
                # The **kwargs dictionary is a path to the object that contains it
                args_vals.append(worker_kwargs)
            else:
                # The **kwargs dictionary is considered as a single dictionary object.
                args_vals.append(kwargs)

        # Build the final list of parameter names
        parameter_names = vals_names + args_names
        # Build the final list of parameter_values for each parameter
        parameter_values = vals_values + args_vals

        # Step 3.- Fill self.returns structure
        if at_worker:
            # WORKER SIDE: Retrieve returns from worker_rets
            if worker_rets:
                if len(worker_rets) == 1:
                    self.returns['compss_retvalue'].file_name = worker_rets[0]
                else:
                    self.returns = OrderedDict()  # Empty any previous content
                    for i, ret in enumerate(worker_rets):
                        self.returns['compss_retvalue' + str(i)] = ret
        else:
            # MASTER SIDE: Backup original expression and discover hidden returns
            if self.returns:
                self.returns_bkp = copy.deepcopy(self.returns)
                self.__discover_hidden_returns(f)

        # Step 4.- Fill self.parameters structure
        for i, p in enumerate(parameter_names):
            if p in self.kwargs:
                parameter = copy.copy(self.kwargs[p])  # copy Parameter reference to self.parameters
                #                                      # Keep the original parameter in self.kwargs
                #                                      # It does not contain any object nor file_name.
                if isinstance(parameter, dict):
                    # The user has given some information about the parameter as dict
                    self.parameters[p] = _from_dict_to_parameter(parameter)
                else:
                    self.parameters[p] = parameter
            else:
                self.parameters[p] = Parameter()

            obj = parameter_values[i]
            if self.parameters[p].type != TYPE.FILE:
                self.parameters[p].object = obj
                # Then it is a primitive, get its real type
                self.parameters[p].type = _get_compss_type(obj)
            else:
                self.parameters[p].file_name = obj

        # Step 5.- Update the Parameter type with the worker information
        if at_worker:
            for i, param in enumerate(self.parameters):
                t = kwargs['compss_types']
                self.parameters[param].type = t[i]

        # Step 6.- Clean elements that are not defined in parameter_names
        for p in list(self.parameters.keys()):
            # This way of iteration avoids OrderedDict mutation during iteration
            if p not in parameter_names:
                self.parameters.pop(p)

        # Step 7.- Extract *args and **kwargs
        aargs = OrderedDict()
        if '*args0' in self.parameters:
            # Extract the *args
            for i in list(self.parameters.keys()):
                # This way of iteration avoids OrderedDict mutation during iteration
                if i.startswith('*args'):
                    aargs[i] = self.parameters.pop(i)
        akwargs = OrderedDict()
        if '**kwargs' in self.parameters:
            # Extract the **kwargs
            akwargs['**kwargs'] = self.parameters.pop('**kwargs')

        # Last step: Place the args and kwargs at the end of the parameters.
        self.parameters.update(aargs)
        self.parameters.update(akwargs)

    def __discover_hidden_returns(self, f):
        """
        Discover hidden returns.
        For example, if the user defines returns=2 or returns="2"

        WARNING: Updates self.returns dictionary

        :param f: Original function
        :param at_worker: <Boolean> At worker. The return, if single, will be a str instead of an int.
        """

        # Only one return defined.
        # May hide a "int", int or type.
        if len(self.returns) == 1:
            num_rets = 1
            hidden_multireturn = False
            ret_value = self.returns['compss_retvalue'].object
            if isinstance(ret_value, str):
                # Check if the returns statement contains an string with an integer or a global variable
                # In such case, build a list of objects of value length and set it in ret_type.

                # Global variable or string wrapping integer value
                try:
                    # Return is hidden by an int as a string. i.e., returns="var_int"
                    num_rets = int(ret_value)
                except ValueError:
                    # Return is hidden by a global variable. i.e., LT_ARGS
                    num_rets = f.__globals__.get(ret_value)
                # Construct hidden multireturn
                hidden_multireturn = True
                if num_rets > 1:
                    ret_v = [object for _ in range(num_rets)]
                else:
                    ret_v = object
            elif isinstance(ret_value, int):
                # Check if the returns statement contains an integer value.
                # In such case, build a list of objects of value length and set it in ret_type.
                num_rets = ret_value
                # Assume all as objects (generic type).
                # It will not work properly when using user defined classes, since
                # the future object built will not be of the same type as expected
                # and may cause "AttributeError" since the 'object' does not have
                # the attributes of the class
                # Hidden multireturn with returns=int
                hidden_multireturn = True
                if num_rets > 1:
                    ret_v = [object for _ in range(num_rets)]
                else:
                    ret_v = object
            elif isinstance(ret_value, list) or isinstance(ret_value, tuple):
                # Check if returns=[] or returns=()
                hidden_multireturn = True
                num_rets = len(ret_value)
                ret_v = self.returns['compss_retvalue'].object
            else:
                ret_v = ret_value

            # Update self.returns
            if hidden_multireturn:
                if num_rets > 1:
                    parameter = self.returns['compss_retvalue']
                    self.returns.pop('compss_retvalue')
                    num_ret = 0
                    for i in ret_v:
                        if isinstance(parameter, list):
                            # when returns=[..., ..., etc.] use the specific parameter
                            self.returns['compss_retvalue' + str(num_ret)] = parameter[num_ret]
                        else:
                            # otherwise all are the kept the same
                            self.returns['compss_retvalue' + str(num_ret)] = parameter
                        self.returns['compss_retvalue' + str(num_ret)].object = i
                        num_ret += 1
                else:
                    self.returns['compss_retvalue'].object = ret_v

    # ############################################################################ #
    # #################### TASK DECORATOR WORKER CODE ############################ #
    # ############################################################################ #

    def worker_code(self, f, args, kwargs):
        """
        Task decorator body executed in the workers.
        Its main function is to execute to execute the function decorated as task.
        Prior to the execution, the worker needs to retrieve the necessary parameters.
        These parameters will be used for the invocation of the function.
        When the function has been executed, stores in a file the result and finishes the worker execution.
        Then, the runtime grabs this files and does all management among tasks that involve them.

        :param f: <Function> - Function to execute
        :param args: <Tuple> - Contains the objects that the function has been called with (positional).
        :param kwargs: <Dictionary> - Contains the named objects that the function has been called with.
        :return: Two lists: new_types and new_values.
        """

        from pycompss.util.serializer import serialize_objects
        import pycompss.runtime.binding as binding

        # Retrieve internal parameters from worker.py.
        # tracing = kwargs.get('compss_tracing')  # Not used here, but kept informatively

        # Discover hidden objects passed as files
        real_values, to_serialize = self.__reveal_objects()

        if binding.aargs_as_tuple:
            # Check if there is *arg parameter in the task, so the last element (*arg tuple) has to be flattened
            if self.has_varargs:
                if self.has_keywords:
                    real_values = real_values[:-2] + list(real_values[-2]) + [real_values[-1]]
                else:
                    real_values = real_values[:-1] + list(real_values[-1])
        else:
            pass

        kargs = {}
        # Check if there is **kwarg parameter in the task, so the last element (kwarg dict) has to be flattened
        if self.has_keywords:
            kargs = real_values[-1]  # kwargs dict
            real_values = real_values[:-1]  # remove kwargs from real_values
        else:
            pass

        ret = f(*real_values, **kargs)  # Real call to f function

        # This will contain the same as to_serialize but we will store the whole
        # file identifier string instead of simply the file_name
        _output_objects = []

        if self.returns:
            # If there is multi-return then serialize each one on a different file
            # Multi-return example: a,b,c = fun() , where fun() has a return x,y,z
            if len(self.returns) > 1:
                try:
                    # Check that output has len function
                    _ = len(ret)
                    # Assign it to an iterable return
                    aux = ret
                except TypeError:
                    # The output has not len function
                    aux = [ret]

                # Fill the extra positions (if the user has declared more than used)
                while len(aux) < len(self.returns):
                    print("WARN: Filling extra return positions with Empty Objects")
                    aux.append(binding.EmptyReturn())

                # Build the return file names
                rets = args[-len(self.returns):]
                for index, ret_filename in enumerate(rets):
                    _output_objects.append((aux[index], ret_filename))
                    ret_filename = ret_filename.split(':')[-1]
                    to_serialize.append((aux[index], ret_filename))
            else:
                # Simple return
                ret_filename = args[-1]
                _output_objects.append((ret, ret_filename))
                ret_filename = ret_filename.split(':')[-1]
                to_serialize.append((ret, ret_filename))

        # Check if the values and types have changed after the task execution:
        # I.e.- an object that has been made persistent within the task may be
        # detected here, and the type change done within the outputTypes list.
        new_types, new_values, to_serialize = _check_value_changes(kwargs['compss_types'],
                                                                   list(args),
                                                                   to_serialize)

        if len(to_serialize) > 0:
            serialize_objects(to_serialize)

        return new_types, new_values

    def __reveal_objects(self):
        """
        Function that goes through all parameters (self.parameters) in order to
        find and open the files.

        :return: a list with the real values and
                 another list of objects to serialize after the task execution (INOUT/OUT)
        """

        from pycompss.api.parameter import DIRECTION
        from pycompss.api.parameter import TYPE
        from pycompss.util.serializer import deserialize_from_file

        real_values = []
        to_serialize = []

        for param in self.parameters:
            parameter = self.parameters[param]

            if inspect.isclass(parameter.object):  # class (it's a class method)
                real_values.append(parameter.object)
                continue
            else:
                pass

            if parameter.type == TYPE.FILE:
                if parameter.file_name:
                    # The parameter is a File (defined as FILE in the decorator)
                    file_name = self.__extract_file_name(parameter.file_name)
                    real_values.append(file_name)
                else:
                    # The parameter is a hidden object
                    file_name = self.__extract_file_name(parameter.object)
                    obj = deserialize_from_file(file_name)
                    real_values.append(obj)
                    if parameter.direction != DIRECTION.IN:
                        to_serialize.append((obj, file_name))
            elif parameter.type == TYPE.OBJECT and parameter.file_name:
                # The parameter is actually an object serialized into a file
                file_name = self.__extract_file_name(parameter.file_name)
                obj = deserialize_from_file(file_name)
                real_values.append(obj)
                if parameter.direction != DIRECTION.IN:
                    to_serialize.append((obj, file_name))
            else:
                # Otherwise, the parameter is a primitive
                real_values.append(parameter.object)

        return real_values, to_serialize

    @staticmethod
    def __extract_file_name(pattern):
        """
        Process the pattern string sent through NIO or GAT which points to a file.
        Getting ids and file names from passed files and objects pattern is:
            - NIO:
                "originalDataID:destinationDataID;flagToPreserveOriginalData:flagToWrite:PathToFile"
            - GAT:
                "PathToFile"

        :param pattern: String to process following the NIO and GAT formats
        :return: <String> The file name hidden in the pattern
        """
        complete_file_name = pattern.split(':')
        if len(complete_file_name) > 1:
            # In NIO we get more information
            # forig = complete_f_name[0]        # Not used yet
            # fdest = complete_f_name[1]        # Not used yet
            # preserve = complete_f_name[2]     # Not used yet
            # write_final = complete_f_name[3]  # Not used yet
            file_name = complete_file_name[4]
            # preserve, write_final = list(map(lambda x: x == "true", [preserve, write_final]))  # Not used yet
            # suffix_name = forig               # Not used yet
        else:
            # In GAT we only get the name
            file_name = complete_file_name[0]
        return file_name

    # ############################################################################ #
    # #################### TASK DECORATOR MASTER CODE ############################ #
    # ############################################################################ #

    def master_code(self, f, num_nodes, args):
        """
        Task decorator body executed in the master.

        :param f: <Function> - Function to execute
        :param num_nodes: <Integer> - Number of computing nodes
        :param args: <Tuple> - Contains the objects that the function has been called with (positional)
        :return: Future object that fakes the real return of the task (for its delegated execution)
        """

        from pycompss.runtime.binding import process_task

        f_type, class_name = self.__check_function_type_and_class_name(f, args)

        fo = process_task(f,
                          self.module_name,
                          class_name,
                          f_type,
                          self.parameters,
                          self.returns,
                          self.kwargs,
                          num_nodes,
                          self.is_replicated,
                          self.is_distributed)

        # Restore returns object
        if self.returns:
            self.returns = self.returns_bkp

        # Starts the asynchronous creation of the task.
        # First calling the PyCOMPSs library and then C library (bindings-commons).
        return fo

    def __check_function_type_and_class_name(self, f, args):
        """
        Check function type and class name in the same loop

        :param f: Function
        :param args: Function args
        :return: <FunctionType> Function type, <String> Class name
        """
        from pycompss.runtime.binding import FunctionType
        # Check the type of the function called.
        # inspect.ismethod(f) does not work here,
        # for methods python hasn't wrapped the function as a method yet
        # Everything is still a function here, can't distinguish yet
        # with inspect.ismethod or isfunction
        f_type = FunctionType.FUNCTION
        class_name = ''
        if self.has_self_parameter:
            f_type = FunctionType.INSTANCE_METHOD
            class_name = type(args[0]).__name__
        if args and inspect.isclass(args[0]):
            for n, _ in inspect.getmembers(args[0], inspect.ismethod):
                if n == f.__name__:
                    f_type = FunctionType.CLASS_METHOD
                    class_name = args[0].__name__
        return f_type, class_name


# ############################################################################# #
# ################### TASK DECORATOR ALTERNATIVE NAME ######################### #
# ############################################################################# #

task = Task


# ############################################################################# #
# ####################### AUXILIARY FUNCTIONS ################################# #
# ############################################################################# #

def _get_module_name(path, file_name):
    """
    Get the module name considering its path and filename.

    Example: runcompss -d src/kmeans.py
             path = "test/kmeans.py"
             file_name = "kmeans" (without py extension)
             return mod_name = "test.kmeans"

    :param path: relative path until the file.py from where the runcompss has been executed
    :param file_name: python file to be executed name (without the py extension)
    :return: the module name
    """

    dirs = path.split(os.path.sep)
    mod_name = file_name
    i = len(dirs) - 1
    while i > 0:
        new_l = len(path) - (len(dirs[i]) + 1)
        path = path[0:new_l]
        if "__init__.py" in os.listdir(path):
            # directory is a package
            i -= 1
            mod_name = dirs[i] + '.' + mod_name
        else:
            break
    return mod_name


def _get_top_decorator(code, decorator_keys):
    """
    Retrieves the decorator which is on top of the current task decorators stack.

    :param code: Tuple which contains the task code to analyse and the number of lines of the code.
    :param decorator_keys: Typle which contains the available decorator keys
    :return: the decorator name in the form "pycompss.api.__name__"
    """

    # Code has two fields:
    # code[0] = the entire function code.
    # code[1] = the number of lines of the function code.
    func_code = code[0]
    decorators = [l.strip() for l in func_code if l.strip().startswith('@')]
    # Could be improved if it stops when the first line without @ is found,
    # but we have to be care if a decorator is commented (# before @)
    # The strip is due to the spaces that appear before functions definitions,
    # such as class methods.
    for dk in decorator_keys:
        for d in decorators:
            if d.startswith('@' + dk):
                return "pycompss.api." + dk.lower()  # each decorator __name__

    # If no decorator is found, then the current decorator is the one to register
    return __name__


def _get_task_type(code, decorator_filter, default):
    """
    Retrieves the type of the task based on the decorators stack.

    :param code: Tuple which contains the task code to analyse and the number of lines of the code.
    :param decorator_filter: Tuple which contains the filtering decorators. The one
                             used determines the type of the task. If none, then it is a normal task.
    :param default: Default values
    :return: the type of the task
    """

    # Code has two fields:
    # code[0] = the entire function code.
    # code[1] = the number of lines of the function code.
    func_code = code[0]
    full_decorators = [l.strip() for l in func_code if l.strip().startswith('@')]
    # Get only the decorators used. Remove @ and parameters.
    decorators = [l[1:].split('(')[0] for l in full_decorators]
    # Look for the decorator used from the filter list and return it when found
    for f in decorator_filter:
        if f in decorators:
            return f
    # The decorator stack did not contain any of the filtering keys, then
    # return the default key.
    return default


def _check_value_changes(types, values, to_serialize):
    """
    Check if the input values have changed and adapt its types accordingly.
    Considers also changes that may affect to the to_serialize list.

    NOTE: This function can also return the real_to_serialize list, which
    contains the objects that should be serialized after checking the changes.
    For example, if a return is a simple type (int), it can be considered
    within the new_types and new_values, popped from the to_serialize list, and
    returned on the task return pipe.
    However, the runtime does not support getting values from the return pipe.
    For this reason, we continue using the to_serialize list to serialize the
    return object into the return file. Consequently, the real_to_serialize
    variable is not currently used, but should be considered when the runtime
    provides support for returning simple objects through the pipe.

    WARNING: Due to the runtime does not support gathering values from the
    output pipe at worker, all values will be set to null but the PSCOs that
    may have changed.

    :param types: List of types of the values list
    :param values: List of values used as task input
    :param to_serialize: List of objects to be serialized
    :return: Three lists, the new types, new values and new to_serialize list.
    """

    assert len(types) == len(values), "Inconsistent state: type-value length mismatch."

    from pycompss.api.parameter import TYPE
    from pycompss.util.persistent_storage import get_id

    # Update the existing PSCOS with their id.
    for i in range(len(types)):
        if types[i] == TYPE.EXTERNAL_PSCO:
            values[i] = get_id(values[i])
    real_to_serialize = []
    # Analise only PSCOS from to_serialize objects list
    for ts in to_serialize:
        # ts[0] == real object to serialize
        # ts[1] == file path where to serialize
        pos = 0
        changed = False
        for i in values:
            if isinstance(i, str) and _get_compss_type(ts[0]) == TYPE.EXTERNAL_PSCO and ts[1] in i:
                # Include the PSCO id in the values list
                values[pos] = get_id(ts[0])
                types[pos] = TYPE.EXTERNAL_PSCO
                changed = True
            pos += 1
        if not changed:
            real_to_serialize.append(ts)
    # Put all values that do not match the EXTERNAL_PSCO type to null
    for i in range(len(types)):
        if not types[i] == TYPE.EXTERNAL_PSCO:
            values[i] = 'null'
    return types, values, real_to_serialize


def _get_compss_type(value):
    """
    Retrieve the value type mapped to COMPSs types.

    :param value: Value to analyse
    :return: The Type of the value
    """

    from pycompss.api.parameter import TYPE
    from pycompss.util.persistent_storage import has_id, get_id

    if type(value) is bool:
        return TYPE.BOOLEAN
    elif type(value) is str and len(value) == 1:
        # Char does not exist as char. Only for strings of length 1.
        return TYPE.CHAR
    elif type(value) is str and len(value) > 1:
        # Any file will be detected as string, since it is a path.
        # The difference among them is defined by the parameter decoration as FILE.
        return TYPE.STRING
    # elif type(value) is byte:
    #    # byte does not exist in python (instead bytes is an str alias)
    #    return TYPE.BYTE
    # elif type(value) is short:
    #    # short does not exist in python... they are integers.
    #    return TYPE.SHORT
    elif type(value) is int:
        return TYPE.INT
    elif type(value) is long:
        return TYPE.LONG
    elif type(value) is float:
        return TYPE.DOUBLE
    # elif type(value) is double:  # In python, floats are doubles.
    #     return TYPE.DOUBLE
    elif type(value) is str:
        return TYPE.STRING
    # elif type(value) is :  # Unavailable - The runtime does not support python objects
    #     return TYPE.OBJECT
    # elif type(value) is :  # Unavailable - PSCOs not persisted will be handled as objects (as files)
    #     return TYPE.PSCO
    elif has_id(value):
        # If has method getID maybe is a PSCO
        try:
            if get_id(value) not in [None, 'None']:  # the 'getID' + id == criteria for persistent object
                return TYPE.EXTERNAL_PSCO
            else:
                return TYPE.OBJECT
        except TypeError:
            # A PSCO class has been used to check its type (when checking
            # the return). Since we still don't know if it is going to be
            # persistent inside, we assume that it is not. It will be checked
            # later on the worker side when the task finishes.
            return TYPE.OBJECT
    else:
        # Default type
        return TYPE.OBJECT


def _get_default_args(f):
    """
    Returns a dictionary of arg_name:default_values for the input function.

    :param f: Function to inspect for default parameters.
    """

    a = inspect.getargspec(f)
    num_params = len(a.args) - len(a.defaults)
    return list(zip(a.args[num_params:], a.defaults))


def _get_wrapped_source(f):
    """
    Gets the text of the source code for the given function.

    :param f: Input function
    :return: Source
    """

    if hasattr(f, "__wrapped__"):
        # has __wrapped__, going deep
        return _get_wrapped_source(f.__wrapped__)
    else:
        # Returning getsource
        return inspect.getsource(f)


def _get_wrapped_sourcelines(f):
    """
    Gets a list of source lines and starting line number for the given function.

    :param f: Input function
    :return: Source lines
    """

    if hasattr(f, "__wrapped__"):
        # has __wrapped__, going deep
        return _get_wrapped_sourcelines(f.__wrapped__)
    else:
        # Returning getsourcelines
        return inspect.getsourcelines(f)


def _from_dict_to_parameter(d):
    """
    Convert a Dict defined by a user for a parameter into a real Parameter object.

    :param d: Dictionary (mandatory to have 'Type' key).
    :return:  Parameter object.
    """

    from pycompss.api.parameter import Parameter
    from pycompss.api.parameter import Type
    from pycompss.api.parameter import Direction
    from pycompss.api.parameter import Stream
    from pycompss.api.parameter import Prefix
    if Type not in d:  # If no Type specified => IN
        d[Type] = Parameter()
    p = d[Type]
    if Direction in d:
        p.direction = d[Direction]
    if Stream in d:
        p.stream = d[Stream]
    if Prefix in d:
        p.prefix = d[Prefix]
    return p
