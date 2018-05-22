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
import sys
import logging
import ast
import copy
from functools import wraps
from pycompss.runtime.commons import IS_PYTHON3

# Tracing Events and Codes -> Should be equal to Tracer.java definitions
SYNC_EVENTS = 8000666
TASK_EVENTS = 8000010
TASK_EXECUTION = 120
SERIALIZATION = 121

if IS_PYTHON3:
    # Shadow long with int
    long = int

if __debug__:
    logger = logging.getLogger('pycompss.api.task')
    # logger = logging.getLogger()   # for jupyter logging
    # logger.setLevel(logging.DEBUG)


class task(object):

    def __init__(self, *args, **kwargs):
        """
        If there are decorator arguments, the function to be decorated is
        not passed to the constructor!
        """

        from pycompss.util.location import i_am_within_scope

        # Check if under the PyCOMPSs scope
        if i_am_within_scope():
            from pycompss.util.location import i_am_at_master
            from pycompss.api.parameter import IN

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

            # Remove old args
            for old_vararg in [x for x in self.kwargs.keys() if x.startswith('*args')]:
                self.kwargs.pop(old_vararg)

            if i_am_at_master():
                for arg_name in self.kwargs.keys():
                    if arg_name not in reserved_keywords.keys():
                        # Prevent p.value from being overwritten later by ensuring
                        # each Parameter is a separate object
                        p = self.kwargs[arg_name]
                        pcopy = copy.copy(p)  # shallow copy
                        self.kwargs[arg_name] = pcopy

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
        from pycompss.api.parameter import Parameter
        from pycompss.api.parameter import TYPE
        from pycompss.api.parameter import DIRECTION
        from pycompss.util.interactive_helpers import updateTasksCodeFile
        from pycompss.util.location import i_am_at_master

        if __debug__:
            logger.debug("Call in @task decorator...")

        # Assume it is an instance method if the first parameter of the
        # function is called 'self'
        # "I would rely on the convention that functions that will become
        # methods have a first argument named self, and other functions don't.
        # Fragile, but then, there's no really solid way."
        self.f_argspec = inspect.getargspec(f)

        # Set default booleans
        self.is_instance = False
        self.is_classmethod = False
        self.has_varargs = False
        self.has_keywords = False
        self.has_defaults = False
        self.has_return = False
        self.has_multireturn = False

        # Step 1.- Check if it is an instance method.
        # Question: Will the first condition evaluate to false? spec_args will
        # always be a named tuple, so it will always return true if evaluated
        # as a bool
        # Answer: The first condition evaluates if args exists (a list) and is
        # not empty in the spec_args. The second checks if the first argument
        # in that list is 'self'. In case that the args list exists and its
        # first element is self, then the function is considered as an instance
        # function (task defined within a class).
        if self.f_argspec.args and self.f_argspec.args[0] == 'self':
            self.is_instance = True
            if self.kwargs['isModifier']:
                direction = DIRECTION.INOUT
            else:
                direction = DIRECTION.IN
            # Add callee object parameter
            self.kwargs['self'] = Parameter(p_type=TYPE.OBJECT,
                                            p_direction=direction)

        # Step 2.- Check if it is a class method.
        # The check of 'cls' may be weak but it is PEP8 style agreements.
        if self.f_argspec.args and self.f_argspec.args[0] == 'cls':
            self.is_classmethod = True
            # Add class object parameter
            self.kwargs['self'] = Parameter(p_type=TYPE.OBJECT,
                                            p_direction=DIRECTION.IN)

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
            self.has_return = True
            # TODO: WHY THIS VARIABLE? THE INFORMATION IS IN SELF.KWARGS['COMPSS_RETVALUE']
            self.f_argspec.args.append('compss_retvalue')
            self.__update_return_type()
        else:
            # If no returns statement found, double check to see if the user has specified a return statement.
            self.__update_return_if_no_returns(f)

        # Get module (for invocation purposes in the worker)
        mod = inspect.getmodule(f)
        self.module = mod.__name__

        if self.module == '__main__' or self.module == 'pycompss.runtime.launch':
            # the module where the function is defined was run as __main__,
            # we need to find out the real module name

            # Get the real module name from our launch.py app_path global variable
            try:
                path = getattr(mod, "app_path")
            except AttributeError:
                # This exception is raised when the runtime is not running and the @task decorator is used.
                # The runtime has not been started yet.
                return self.__not_under_pycompss_scope(f)

            # Get the file name
            file_name = os.path.splitext(os.path.basename(path))[0]

            # Do any necessary preprocessing action before executing any code
            if file_name.startswith('InteractiveMode'):
                # If the file_name starts with 'InteractiveMode' means that
                # the user is using PyCOMPSs from jupyter-notebook.
                # Convention between this file and interactive.py
                # In this case it is necessary to do a pre-processing step
                # that consists of putting all user code that may be executed
                # in the worker on a file.
                # This file has to be visible for all workers.
                updateTasksCodeFile(f, path)
            else:
                # work as always
                pass

            # Get the module
            self.module = get_module_name(path, file_name)

        # The registration needs to be done only in the master node
        if i_am_at_master():
            self.__register_task(f)

        # Modified variables until now that will be used later:
        #   - self.f_argspec        : Function argspect (Named tuple)
        #                             e.g. ArgSpec(args=['a', 'b', 'compss_retvalue'], varargs=None,
        #                             keywords=None, defaults=None)
        #   - self.is_instance      : Boolean - if the function is an instance (contains self in the f_argspec)
        #   - self.is_classmethod   : Boolean - if the function is a classmethod (contains cls in the f_argspec)
        #   - self.has_varargs      : Boolean - if the function has *args
        #   - self.has_keywords     : Boolean - if the function has **kwargs
        #   - self.has_defaults     : Boolean - if the function has default values
        #   - self.has_return       : Boolean - if the function has return
        #   - self.has_multireturn  : Boolean - if the function has multireturn
        #   - self.module_name      : String  - Module name (e.g. test.kmeans)
        #   - is_replicated         : Boolean - if the task is replicated
        #   - is_distributed    : Boolean - if the task is distributed
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

            is_replicated = self.kwargs['isReplicated']
            is_distributed = self.kwargs['isDistributed']
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
            istack = inspect.stack()
            for i_s in istack:
                if i_s[3] == 'launch_pycompss_module':
                    is_nested = True
                if i_s[3] == 'launch_pycompss_application':
                    is_nested = True

            if not i_am_at_master() and (not is_nested):
                # Task decorator worker body code.
                new_types, new_values = self.worker_code(f, args, kwargs)
                return new_types, new_values
            else:
                # Task decorator master body code.
                # Returns the future object that will be used instead of the
                # actual function return.
                fo = self.master_code(f, is_replicated, is_distributed, computing_nodes, args, kwargs)
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
        Updates the return types within self.kwargs['compss_retvalue']
        """

        from pycompss.api.parameter import Parameter
        from pycompss.api.parameter import DIRECTION

        # This condition is interesting, because a user can write returns=list
        # However, lists have the attribute __len__ but raise an exception.
        # Since the user does not indicate the length, it will be managed
        # as a single return.
        # When the user specifies the length, it is possible to manage the
        # elements independently.
        if not hasattr(self.kwargs['returns'], '__len__') or type(self.kwargs['returns']) is type:
            # Simple return
            ret_type = get_COMPSs_type(self.kwargs['returns'])
            self.kwargs['compss_retvalue'] = Parameter(p_type=ret_type, p_direction=DIRECTION.OUT)
        else:
            # Multi return
            self.has_multireturn = True
            returns = []
            for r in self.kwargs['returns']:
                ret_type = get_COMPSs_type(r)
                returns.append(Parameter(p_type=ret_type, p_direction=DIRECTION.OUT))
            self.kwargs['compss_retvalue'] = tuple(returns)

    def __update_return_if_no_returns(self, f):
        """
        Checks the code looking for return statements if no returns is specified in @task decorator.
        :param f: Function to check
        """

        from pycompss.api.parameter import Parameter
        from pycompss.api.parameter import DIRECTION
        from pycompss.api.parameter import TYPE

        source_code = get_wrapped_source(f).strip()
        if self.is_instance or source_code.startswith('@classmethod'):  # TODO: WHAT IF IS CLASSMETHOD FROM BOOLEAN?
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
            self.has_return = True
            lines = [i for i, li in enumerate(ret_mask) if li]
            max_num_returns = 0
            if self.is_instance or source_code.startswith('@classmethod'):
                # Parse code as string (it is a task defined within a class)
                def _has_multireturn(statement):
                    v = ast.parse(statement.strip())
                    try:
                        if len(v.body[0].value.elts) > 1:
                            return True
                        else:
                            return False
                    except Exception:
                        # KeyError: 'elts' means that it is a multiple return.
                        # "Ask forgiveness not permission"
                        return False

                def _get_return_elements(statement):
                    v = ast.parse(statement.strip())
                    return len(v.body[0].value.elts)

                for i in lines:
                    if _has_multireturn(code[i]):
                        self.has_multireturn = True
                        num_returns = _get_return_elements(code[i])
                        if num_returns > max_num_returns:
                            max_num_returns = num_returns
            else:
                # Parse code AST (it is not a task defined within a class)
                for i in lines:
                    try:
                        if 'elts' in code[i].value.__dict__:
                            self.has_multireturn = True
                            num_returns = len(code[i].value.__dict__['elts'])
                            if num_returns > max_num_returns:
                                max_num_returns = num_returns
                    except Exception:
                        # KeyError: 'elts' means that it is a multiple return.
                        # "Ask forgiveness not permission"
                        pass
            if self.has_multireturn:
                if __debug__:
                    logger.debug("Multireturn found: %s" % str(max_num_returns))
                self.kwargs['returns'] = []
                returns = []
                for _ in range(max_num_returns):
                    self.kwargs['returns'].append(object())
                    returns.append(Parameter(p_type=TYPE.FILE, p_direction=DIRECTION.OUT))
                self.kwargs['compss_retvalue'] = tuple(returns)
            else:
                if __debug__:
                    logger.debug("Return found")
                self.kwargs['returns'] = object()
                self.kwargs['compss_retvalue'] = Parameter(p_type=TYPE.FILE, p_direction=DIRECTION.OUT)
            self.f_argspec.args.append('compss_retvalue')

    def __register_task(self, f):
        """
        This function is used to register the task in the runtime.
        This registration must be done only once on the task decorator
        initialization.
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
        got_func_code = False
        func = f
        while not got_func_code:
            try:
                func_code = get_wrapped_sourcelines(func)
                got_func_code = True
            except IOError:
                # There is one or more decorators below the @task --> undecorate
                # until possible to get the func code.
                # Example of this case: test 19: @timeit decorator below the
                # @task decorator.
                func = func.__wrapped__

        decorator_keys = ("implement", "constraint", "decaf", "mpi", "ompss", "binary", "opencl", "task")

        top_decorator = get_top_decorator(func_code, decorator_keys)
        if __debug__:
            logger.debug(
                "[@TASK] Top decorator of function %s in module %s: %s" % (f.__name__, self.module, str(top_decorator)))
        f.__who_registers__ = top_decorator

        # not usual tasks - handled by the runtime without invoking the PyCOMPSs
        # worker. Needed to filter in order not to code the strings when using
        # them in these type of tasks
        decorator_filter = ("decaf", "mpi", "ompss", "binary", "opencl")
        default = 'task'
        task_type = get_task_type(func_code, decorator_filter, default)
        if __debug__:
            logger.debug(
                "[@TASK] Task type of function %s in module %s: %s" % (f.__name__, self.module, str(task_type)))
        f.__task_type__ = task_type
        if task_type == default:
            f.__code_strings__ = True
        else:
            f.__code_strings__ = False

        # Include the registering info related to @task
        ins = inspect.getouterframes(inspect.currentframe())
        # I know that this is ugly, but I see no other way to get the class name
        class_name = ins[2][3]
        # I know that this is ugly, but I see no other way to check if it is a class method.
        is_classmethod = class_name != '<module>'
        if self.is_instance or is_classmethod:
            ce_signature = self.module + "." + class_name + '.' + f.__name__
            impl_type_args = [self.module + "." + class_name, f.__name__]
        else:
            ce_signature = self.module + "." + f.__name__
            impl_type_args = [self.module, f.__name__]
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
                    "[@TASK] I have to do the register of function %s in module %s" % (f.__name__, self.module))
                logger.debug("[@TASK] %s" % str(f.__to_register__))
            binding.register_ce(core_element)

    # ############################################################################ #
    # #################### TASK DECORATOR WORKER CODE ############################ #
    # ############################################################################ #

    def worker_code(self, f, args, kwargs):
        """ Task decorator body executed in the workers.
        Its main function is to execute to execute the function decorated as task.
        Prior to the execution, the worker needs to retrieve the necessary parameters.
        These parameters will be used for the invocation of the function.
        When the function has been executed, stores in a file the result and finishes the worker execution.
        Then, the runtime grabs this files and does all management among tasks that involve them.
        :param f: <Function> - Function to execute
        :param args: <Tuple> - Contains the objects that the function has been called with (positional).
        :param kwargs: <Dictionary> - Contains the named objects that the function has been called with.
        :return: Two lists: newTypes and newValues.
        """

        from pycompss.util.serializer import serialize_objects
        import pycompss.runtime.binding as binding

        # Retrieve internal parameters from worker.py.
        tracing = kwargs.get('compss_tracing')

        if tracing:
            import pyextrae
            pyextrae.eventandcounters(TASK_EVENTS, 0)
            pyextrae.eventandcounters(TASK_EVENTS, SERIALIZATION)

        spec_args = self.f_argspec.args

        returns = self.kwargs['returns']
        is_multi_return = False
        num_return = 0

        if self.has_return:
            # Check if there is multireturn
            if isinstance(returns, list) or isinstance(returns, tuple) or isinstance(returns, int):
                if isinstance(returns, int):
                    num_return = returns
                else:
                    num_return = len(returns)
                if num_return > 1:
                    is_multi_return = True
                else:
                    is_multi_return = False
                # If there is a multireturn, we need to append as many arguments as returns
                # are to the spec_args.
                spec_args = spec_args[:-1] + [spec_args[-1] + str(i) for i in
                                              range(num_return)]
            else:
                # The spec_args already has the compss_retvalue
                num_return = 1

        toadd = []

        # Check if there is *arg parameter in the task
        if self.has_varargs:
            if binding.aargs_as_tuple:
                # If the *args are expected to be managed as a tuple:
                toadd.append(self.f_argspec.varargs)
            else:
                # If the *args are expected to be managed as individual elements:
                num_aargs = len(args) - len(spec_args)
                if self.has_keywords:
                    num_aargs -= 1
                for i in range(num_aargs):
                    toadd.append('*' + self.f_argspec.varargs + str(i))

        # Check if there is **kwarg parameters in the task
        if self.has_keywords:
            toadd.append(self.f_argspec.keywords)

        if self.has_return:
            # Include to add between the arguments and the returns
            if is_multi_return:
                spec_args = spec_args[:-num_return] + toadd + spec_args[-num_return:]
            else:
                spec_args = spec_args[:-1] + toadd + [spec_args[-1]]
        else:
            spec_args = spec_args + toadd

        # Discover hidden objects passed as files
        real_values, to_serialize = self.__reveal_objects(args, spec_args, kwargs['compss_types'], num_return)

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

        if tracing:
            pyextrae.eventandcounters(TASK_EVENTS, 0)
            pyextrae.eventandcounters(TASK_EVENTS, TASK_EXECUTION)

        ret = f(*real_values, **kargs)  # Real call to f function

        if tracing:
            pyextrae.eventandcounters(TASK_EVENTS, 0)
            pyextrae.eventandcounters(TASK_EVENTS, SERIALIZATION)

        # This will contain the same as to_serialize but we will store the whole
        # file identifier string instead of simply the file_name
        _output_objects = []

        if returns:
            # If there is multi-return then serialize each one on a different file
            # Multi-return example: a,b,c = fun() , where fun() has a return x,y,z
            if is_multi_return:
                aux = []
                try:
                    num_ret = len(ret)
                    aux = ret
                except Exception:
                    aux.append(ret)
                    while len(aux) < num_return:
                        # The user declared more than used.
                        aux.append(binding.EmptyReturn())
                total_rets = len(args) - num_return
                rets = args[total_rets:]
                i = 0
                for ret_filename in rets:
                    _output_objects.append((aux[i], ret_filename))
                    ret_filename = ret_filename.split(':')[-1]
                    to_serialize.append((aux[i], ret_filename))
                    i += 1
            else:  # simple return
                ret_filename = args[-1]
                _output_objects.append((ret, ret_filename))
                ret_filename = ret_filename.split(':')[-1]
                to_serialize.append((ret, ret_filename))

        # Check if the values and types have changed after the task execution:
        # I.e.- an object that has been made persistent within the task may be
        # detected here, and the type change done within the outputTypes list.
        new_types, new_values, to_serialize = check_value_changes(kwargs['compss_types'], list(args), to_serialize)
        if len(to_serialize) > 0:
            serialize_objects(to_serialize)

        return new_types, new_values

    def __reveal_objects(self, values, spec_args, compss_types, num_return):
        """
        Function that goes through all parameters in order to
        find and open the files.
        :param values: <List> - The value of each parameter.
        :param spec_args: <List> - Specific arguments.
        :param compss_types: <List> - The types of the values.
        :param num_return: <Int> - Number of returns
        :return: a list with the real values
        """

        from pycompss.api.parameter import Parameter
        from pycompss.api.parameter import DIRECTION
        from pycompss.api.parameter import TYPE
        from pycompss.util.serializer import deserialize_from_file

        num_pars = len(spec_args)
        real_values = []
        to_serialize = []

        if self.has_return:
            num_pars -= num_return  # return value must not be passed to the function call

        for i in range(num_pars):
            spec_arg = spec_args[i]
            compss_type = compss_types[i]
            value = values[i]
            if i == 0:
                if spec_arg == 'self':  # callee object
                    if self.kwargs['isModifier']:
                        d = DIRECTION.INOUT
                    else:
                        d = DIRECTION.IN
                    self.kwargs[spec_arg] = Parameter(p_type=TYPE.OBJECT, p_direction=d)
                elif inspect.isclass(value):  # class (it's a class method)
                    real_values.append(value)
                    continue

            p = self.kwargs.get(spec_arg)
            if p is None:  # decoration not present, using default
                p = self.kwargs['varargsType'] if spec_arg.startswith('*args') else Parameter()

            if compss_type == TYPE.FILE and p.type != TYPE.FILE:
                # Getting ids and file names from passed files and objects pattern
                # is: "originalDataID:destinationDataID;flagToPreserveOriginalData:flagToWrite:PathToFile"
                complete_fname = value.split(':')
                if len(complete_fname) > 1:
                    # In NIO we get more information
                    # forig = complete_fname[0]        # Not used yet
                    # fdest = complete_fname[1]        # Not used yet
                    # preserve = complete_fname[2]     # Not used yet
                    # write_final = complete_fname[3]  # Not used yet
                    fname = complete_fname[4]
                    # preserve, write_final = list(map(lambda x: x == "true", [preserve, write_final]))  # Not used yet
                    # suffix_name = forig              # Not used yet
                else:
                    # In GAT we only get the name
                    fname = complete_fname[0]

                value = fname
                # For COMPSs it is a file, but it is actually a Python object
                if __debug__:
                    logger.debug("Processing a hidden object in parameter %d", i)
                obj = deserialize_from_file(value)
                real_values.append(obj)
                if p.direction != DIRECTION.IN:
                    to_serialize.append((obj, value))
            else:
                if compss_type == TYPE.FILE:
                    complete_fname = value.split(':')
                    if len(complete_fname) > 1:
                        # In NIO we get more information
                        # forig = complete_fname[0]        # Not used yet
                        # fdest = complete_fname[1]        # Not used yet
                        # preserve = complete_fname[2]     # Not used yet
                        # write_final = complete_fname[3]  # Not used yet
                        fname = complete_fname[4]
                    else:
                        # In GAT we only get the name
                        fname = complete_fname[0]
                    value = fname
                real_values.append(value)
        return real_values, to_serialize

    # ############################################################################ #
    # #################### TASK DECORATOR MASTER CODE ############################ #
    # ############################################################################ #

    def master_code(self, f, is_replicated, is_distributed, num_nodes, args, kwargs):
        """
        Task decorator body executed in the master
        :param f: <Function> - Function to execute
        :param is_replicated: <Boolean> - If the function is replicated
        :param is_distributed: <Boolean> - If the function is distributed
        :param num_nodes: <Integer> - Number of computing nodes
        :param args: <Tuple> - Contains the objects that the function has been called with (positional).
        :param kwargs: <Dictionary> - Contains the named objects that the function has been called with.
        :return: Future object that fakes the real return of the task (for its delegated execution)
        """

        from pycompss.runtime.binding import process_task
        from pycompss.runtime.binding import FunctionType
        import pycompss.runtime.binding as binding

        # Check the type of the function called.
        # inspect.ismethod(f) does not work here,
        # for methods python hasn't wrapped the function as a method yet
        # Everything is still a function here, can't distinguish yet
        # with inspect.ismethod or isfunction
        ftype = FunctionType.FUNCTION
        class_name = ''
        if self.is_instance:
            ftype = FunctionType.INSTANCE_METHOD
            class_name = type(args[0]).__name__
        if args and inspect.isclass(args[0]):
            for n, _ in inspect.getmembers(args[0], inspect.ismethod):
                if n == f.__name__:
                    ftype = FunctionType.CLASS_METHOD
                    class_name = args[0].__name__

        # Build the arguments list
        # Be very careful with parameter position.
        # The included are sorted by position. The rest may not.

        # Check how many parameters are defined in the function
        num_params = len(self.f_argspec.args)
        if self.has_return:
            num_params -= 1

        # Check if the user has defined default values and include them
        if self.has_defaults:
            # There are default parameters
            # Get the variable names and values that have been defined by default (get_default_args(f)).
            # default_params will have a list of pairs of the form (argument, default_value)
            # Default values have to be always defined after undefined value parameters.
            default_params = get_default_args(f)
            args_list = list(args)  # Given values
            # Default parameter addition
            for p in self.f_argspec.args[len(args):num_params]:
                if p in kwargs:
                    args_list.append(kwargs[p])
                    kwargs.pop(p)
                else:
                    for dp in default_params:
                        if p == dp[0]:
                            args_list.append(dp[1])
            args = tuple(args_list)

        # List of parameter names
        vals_names = list(self.f_argspec.args[:num_params])
        # List of values of each parameter
        vals = list(args[:num_params])  # first values of args are the parameters

        # Check if there are *args or **kwargs
        args_names = []
        args_vals = []
        if self.has_varargs:  # *args
            aargs = '*' + self.f_argspec.varargs
            if binding.aargs_as_tuple:
                # If the *args are expected to be managed as a tuple:
                args_names.append(aargs)  # Name used for the *args
                # last values will compose the *args parameter
                args_vals.append(args[num_params:])
            else:
                # If the *args are expected to be managed as individual elements:
                pos = 0
                for i in range(len(args[num_params:])):
                    args_names.append(aargs + str(pos))  # Name used for the *args
                    self.kwargs[aargs + str(pos)] = copy.copy(self.kwargs['varargsType'])
                    pos += 1
                args_vals = args_vals + list(args[num_params:])
        if self.has_keywords:  # **kwargs
            aakwargs = '**' + self.f_argspec.keywords  # Name used for the **kwargs
            args_names.append(aakwargs)
            # Check if some of the **kwargs are used as vals
            if len(vals_names) > len(vals):
                for i in range(len(vals), len(vals_names)):
                    vals.append(kwargs[vals_names[i]])
                    kwargs.pop(vals_names[i])
            # The **kwargs dictionary is considered as a single dictionary object.
            args_vals.append(kwargs)

        # Build the final list of parameter names
        spec_args = vals_names + args_names
        if self.has_return:
            spec_args += ['compss_retvalue']
        # Build the final list of values for each parameter
        values = tuple(vals + args_vals)

        fo = process_task(f, self.module, class_name, ftype, self.has_return, spec_args, values, kwargs, self.kwargs,
                          num_nodes,
                          is_replicated, is_distributed)
        # Starts the asynchronous creation of the task.
        # First calling the PyCOMPSs library and then C library (bindings-commons).
        return fo


# ############################################################################# #
# ####################### AUXILIARY FUNCTIONS ################################# #
# ############################################################################# #

def get_module_name(path, file_name):
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


def get_top_decorator(code, decoratorKeys):
    """
    Retrieves the decorator which is on top of the current task decorators stack.
    :param code: Tuple which contains the task code to analyse and the number of lines of the code.
    :param decoratorKeys: Typle which contains the available decorator keys
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
    for dk in decoratorKeys:
        for d in decorators:
            if d.startswith('@' + dk):
                return "pycompss.api." + dk.lower()  # each decorator __name__

    # If no decorator is found, then the current decorator is the one to register
    return __name__


def get_task_type(code, decorator_filter, default):
    """
    Retrieves the type of the task based on the decorators stack.
    :param code: Tuple which contains the task code to analyse and the number of lines of the code.
    :param decorator_filter: Typle which contains the filtering decorators. The one
    used determines the type of the task. If none, then it is a normal task.
    :param default: Default values
    :return: the type of the task
    """

    # Code has two fields:
    # code[0] = the entire function code.
    # code[1] = the number of lines of the function code.
    func_code = code[0]
    fulldecorators = [l.strip() for l in func_code if l.strip().startswith('@')]
    # Get only the decorators used. Remove @ and parameters.
    decorators = [l[1:].split('(')[0] for l in fulldecorators]
    # Look for the decorator used from the filter list and return it when found
    for f in decorator_filter:
        if f in decorators:
            return f
    # The decorator stack did not contain any of the filtering keys, then
    # return the default key.
    return default


def check_value_changes(types, values, to_serialize):
    """
    Check if the input values have changed and adapt its types accordingly.
    Considers also changes that may affect to the to_serialize list.
    NOTE: This function can also return the real_to_serialize list, which
    contains the objects that should be serialized after checking the changes.
    For example, if a return is a simple type (int), it can be considered
    within the newTypes and newValues, poped from the to_serialize list, and
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

    assert len(types) == len(values), 'Inconsistent state: type-value length mismatch.'

    from pycompss.api.parameter import TYPE
    from pycompss.util.persistent_storage import get_ID

    # Update the existing PSCOS with their id.
    for i in range(len(types)):
        if types[i] == TYPE.EXTERNAL_PSCO:
            values[i] = get_ID(values[i])
    real_to_serialize = []
    # Analise only PSCOS from to_serialize objects list
    for ts in to_serialize:
        # ts[0] == real object to serialize
        # ts[1] == file path where to serialize
        pos = 0
        changed = False
        for i in values:
            if isinstance(i, str) and get_COMPSs_type(ts[0]) == TYPE.EXTERNAL_PSCO and ts[1] in i:
                # Include the PSCO id in the values list
                values[pos] = get_ID(ts[0])
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


def get_COMPSs_type(value):
    """
    Retrieve the value type mapped to COMPSs types.
    :param value: Value to analyse
    :return: The Type of the value
    """

    from pycompss.api.parameter import TYPE
    from pycompss.util.persistent_storage import has_ID, get_ID

    if type(value) is bool:
        return TYPE.BOOLEAN
    elif type(value) is str and len(value) == 1:
        # Char does not exist as char. Only for strings of length 1.
        return TYPE.CHAR
    elif type(value) is str and len(value) > 1:
        return TYPE.STRING
    # elif type(value) is byte:
    # byte does not exist in python (instead bytes is an str alias)
    #    return TYPE.BYTE
    # elif type(value) is short:
    # short does not exist in python... they are integers.
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
    elif has_ID(value):
        # If has method getID maybe is a PSCO
        try:
            if get_ID(value) not in [None, 'None']:  # the 'getID' + id == criteria for persistent object
                return TYPE.EXTERNAL_PSCO
        except TypeError:
            # A PSCO class has been used to check its type (when checking
            # the return). Since we still don't know if it is going to be
            # persistent inside, we assume that it is not. It will be checked
            # later on the worker side when the task finishes.
            return TYPE.FILE
    else:
        # Default type
        return TYPE.FILE


def get_default_args(f):
    """
    Returns a dictionary of arg_name:default_values for the input function
    @param f: Function to inspect for default parameters.
    """

    a = inspect.getargspec(f)
    num_params = len(a.args) - len(a.defaults)
    return list(zip(a.args[num_params:], a.defaults))


def get_wrapped_source(f):
    """
    Gets the text of the source code for the given function
    :param f: Input function
    :return: Source
    """
    if hasattr(f, "__wrapped__"):
        # has __wrapped__, going deep
        return get_wrapped_source(f.__wrapped__)
    else:
        # Returning getsource
        return inspect.getsource(f)


def get_wrapped_sourcelines(f):
    """
    Gets a list of source lines and starting line number for the given function
    :param f: Input function
    :return: Source lines
    """
    if hasattr(f, "__wrapped__"):
        # has __wrapped__, going deep
        return get_wrapped_sourcelines(f.__wrapped__)
    else:
        # Returning getsourcelines
        return inspect.getsourcelines(f)
