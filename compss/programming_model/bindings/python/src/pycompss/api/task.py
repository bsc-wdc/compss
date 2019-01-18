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
This file contains the class task, needed for the task definition.
"""

import threading

import pycompss.api.parameter as parameter
from pycompss.runtime.core_element import CE

if __debug__:
    import logging

    logger = logging.getLogger('pycompss.api.task')

# This lock allows tasks to be launched with the Threading module while ensuring
# that no attribute is overwritten
master_lock = threading.Lock()
# Determine if strings should have a sharp symbol prepended or not
prepend_strings = True
register_only = False

current_core_element = CE()


class task(object):
    """
    This is the Task decorator implementation.
    It is implemented as a class and consequently this implementation can be
    divided into two natural steps: decoration process and function call.

    Decoration process is what happens when the Python parser reads a decorated
    function. The actual function is not called, but the @task() triggers
    the process that stores and processes the parameters of the decorator.
    This first step corresponds to the class constructor.

    Function call is what happens when the user calls their function somewhere
    in the code. A decorator simply adds pre and post steps in this function call,
    allowing us to change and process the arguments. This second steps happens in the
    __call__ implementation.

    Also, the call itself does different things in the master than in the worker.
    We must also handle the case when the user just runs the app with python and
    no PyCOMPSs.
    The specific implementations can be found in self.master_call(),
    self.worker_call(), self.sequential_call()
    """

    def get_default_decorator_values(self):
        """
        Default value for decorator arguments.
        :return: A dictionary with the default values of the non-parameter decorator fields
        """
        return {
            'isModifier': True,  # Irrelevant if direction of self is explicitly defined
            'returns': False,
            'priority': False,
            'isReplicated': False,
            'isDistributed': False,
            'computingNodes': 1,
            'varargsType': parameter.IN  # Here for legacy purposes
        }

    def __init__(self, comment=None, **kwargs):
        """
        This part is called in the decoration process, not as an
        explicit function call.

        We do two things here:
        a) Assign default values to unspecified fields (see get_default_decorator_values )
        b) Transform the parameters from user friendly types (i.e Parameter.IN, etc) to
           a more convenient internal representation

        :param comment: Hidden to the user (non-documented).
        :param kwargs: Decorator parameters. A task decorator has no positional arguments.
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
                self.decorator_arguments[key] = parameter.get_parameter_copy(value)
            # Specific case when value is a dictionary
            # Use case example:
            # @binary(binary="ls")
            # @task(hide={Type: FILE_IN, Prefix: "--hide="}, sort={Type: IN, Prefix: "--sort="})
            # def myLs(flag, hide, sort):
            #   pass
            # Transform this dictionary to a Parameter object
            if parameter.is_dict_specifier(value):
                # Perform user -> instance substitution
                # param = self.decorator_arguments[key][parameter.Type]
                # Replace the whole dict by a single parameter object
                self.decorator_arguments[key] = parameter.get_parameter_from_dictionary(
                    self.decorator_arguments[key]
                )
                # self.decorator_arguments[key].update({parameter.Type: parameter.get_parameter_copy(param)})

        # Add more argument related attributes that will be useful later
        self.parameters = None
        self.param_args = None
        self.param_varargs = None
        self.param_kwargs = None
        self.param_defaults = None
        self.first_arg_name = None
        # Add functon related attributed that will be useful later
        self.module_name = None
        self.function_name = None
        self.function_type = None
        self.class_name = None
        self.computing_nodes = None
        # Add returns related attributes that will be useful later
        self.returns = None
        self.multi_return = False

        # Task wont be registered until called from the master for the first time or
        # have a different signature
        self.signature = None
        self.registered = False

    def add_return_parameters(self):
        """
        Modify the return parameters accordingly to the return statement
        :return: Nothing, it just creates and modifies self.returns
        """
        from collections import OrderedDict
        self.returns = OrderedDict()
        # Note that returns is by default False
        if self.decorator_arguments['returns']:
            # A return statement can be the following:
            # 1) A type. This means 'this task returns an object of this type'
            # 2) An integer N. This means 'this task returns N objects'
            # 3) A basic iterable (tuple, list...). This means 'this task returns an iterable
            #    with the indicated elements inside
            from pycompss.util.object_properties import is_basic_iterable
            # We are returning multiple objects until otherwise proven
            # It is important to know because this will determine if we will return
            # a single object or [a single object] in some cases
            self.multi_return = True
            if isinstance(self.decorator_arguments['returns'], str):
                # Check if the returns statement contains an string with an integer or a global variable
                # In such case, build a list of objects of value length and set it in ret_type.
                # Global variable or string wrapping integer value
                try:
                    # Return is hidden by an int as a string. i.e., returns="var_int"
                    num_rets = int(self.decorator_arguments['returns'])
                except ValueError:
                    # Return is hidden by a global variable. i.e., LT_ARGS
                    num_rets = self.user_function.__globals__.get(self.decorator_arguments['returns'])
                # Construct hidden multireturn
                if num_rets > 1:
                    to_return = [tuple([]) for _ in range(num_rets)]
                else:
                    to_return = tuple([])
            elif is_basic_iterable(self.decorator_arguments['returns']):
                # The task returns a basic iterable with some types already defined
                to_return = self.decorator_arguments['returns']
            elif isinstance(self.decorator_arguments['returns'], int):
                # The task returns a list of N objects, defined by the integer N
                to_return = tuple([() for _ in range(self.decorator_arguments['returns'])])
            else:
                # The task returns a single object of a single type
                # This is also the only case when no multiple objects are returned but only one
                self.multi_return = False
                to_return = [self.decorator_arguments['returns']]
            # At this point we have a list of returns
            for (i, elem) in enumerate(to_return):
                ret_type = parameter.get_compss_type(elem)
                self.returns[parameter.get_return_name(i)] = parameter.Parameter(p_type=ret_type,
                                                                                 p_object=elem,
                                                                                 p_direction=parameter.OUT)
                # Hopefully, an exception have been thrown if some invalid stuff has been put
                # in the returns field

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

        def task_decorator(*args, **kwargs):
            # Determine the context and decide what to do
            import pycompss.util.context as context
            if context.in_master():
                return self.master_call(*args, **kwargs)
            elif context.in_worker():
                return self.worker_call(*args, **kwargs)
            # We are neither in master nor in the worker
            # Therefore, the user code is being executed with no
            # launch_compss/enqueue_compss/runcompss, etc etc
            return self.sequential_call(*args, **kwargs)

        return task_decorator

    def update_return_if_no_returns(self, f):
        """
        Checks the code looking for return statements if no returns is specified in @task decorator.

        WARNING: Updates self.return if returns are found.

        :param f: Function to check
        """

        from pycompss.api.parameter import Parameter
        from pycompss.api.parameter import DIRECTION
        from pycompss.api.parameter import TYPE
        from pycompss.util.object_properties import get_wrapped_source
        import ast
        source_code = get_wrapped_source(f).strip()

        if self.first_arg_name == 'self' or source_code.startswith('@classmethod'):
            # TODO: WHAT IF IS CLASSMETHOD FROM BOOLEAN?
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
            has_multireturn = False
            lines = [i for i, li in enumerate(ret_mask) if li]
            max_num_returns = 0
            if self.first_arg_name == 'self' or source_code.startswith('@classmethod'):
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
                    param = Parameter(p_type=TYPE.FILE, p_direction=DIRECTION.OUT)
                    param.object = object()
                    self.returns[parameter.get_return_name(i)] = param
            else:
                param = Parameter(p_type=TYPE.FILE, p_direction=DIRECTION.OUT)
                param.object = object()
                self.returns[parameter.get_return_name(0)] = param
        else:
            # Return not found
            pass

    def prepare_core_element_information(self, f):
        """
        This function is used to prepare the core element.
        The information is needed in order to compare the implementation signature,
        so that if it has been registered with a different signature, it can be
        re-registered with the new one (enable inheritance).

        :param f: Function to be registered
        """

        def _get_top_decorator(code, dec_keys):
            """
            Retrieves the decorator which is on top of the current task decorators stack.

            :param code: Tuple which contains the task code to analyse and the number of lines of the code.
            :param dec_keys: Typle which contains the available decorator keys
            :return: the decorator name in the form "pycompss.api.__name__"
            """

            # Code has two fields:
            # code[0] = the entire function code.
            # code[1] = the number of lines of the function code.
            dec_func_code = code[0]
            decorators = [l.strip() for l in dec_func_code if l.strip().startswith('@')]
            # Could be improved if it stops when the first line without @ is found,
            # but we have to be care if a decorator is commented (# before @)
            # The strip is due to the spaces that appear before functions definitions,
            # such as class methods.
            for dk in dec_keys:
                for d in decorators:
                    if d.startswith('@' + dk):
                        return "pycompss.api." + dk.lower()  # each decorator __name__
            # If no decorator is found, then the current decorator is the one to register
            return __name__

        def _get_task_type(code, dec_filter, default_values):
            """
            Retrieves the type of the task based on the decorators stack.

            :param code: Tuple which contains the task code to analyse and the number of lines of the code.
            :param dec_filter: Tuple which contains the filtering decorators. The one
                                     used determines the type of the task. If none, then it is a normal task.
            :param default_values: Default values
            :return: the type of the task
            """

            # Code has two fields:
            # code[0] = the entire function code.
            # code[1] = the number of lines of the function code.
            dec_func_code = code[0]
            full_decorators = [l.strip() for l in dec_func_code if l.strip().startswith('@')]
            # Get only the decorators used. Remove @ and parameters.
            decorators = [l[1:].split('(')[0] for l in full_decorators]
            # Look for the decorator used from the filter list and return it when found
            for filt in dec_filter:
                if filt in decorators:
                    return filt
            # The decorator stack did not contain any of the filtering keys, then
            # return the default key.
            return default_values

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
                from pycompss.util.object_properties import get_wrapped_sourcelines
                func_code = get_wrapped_sourcelines(func)
                got_func_code = True
            except IOError:
                # There is one or more decorators below the @task --> undecorate
                # until possible to get the func code.
                # Example of this case: test 19: @timeit decorator below the
                # @task decorator.
                func = func.__wrapped__

        decorator_keys = ("implement", "constraint", "task", "binary", "mpi", "compss", "decaf", "ompss", "opencl")

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
        decorator_filter = ("binary", "mpi", "compss", "decaf", "ompss", "opencl")
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
            # This case is never reached with Python 3 since it includes frames that are not present with Python 2.
            ce_signature = self.module_name + "." + f.__name__
            impl_type_args = [self.module_name, f.__name__]
        else:
            if self.class_name:
                # Within class or subclass
                ce_signature = self.module_name + '.' + self.class_name + '.' + f.__name__
                impl_type_args = [self.module_name + '.' + self.class_name, f.__name__]
            else:
                # Not in a class or subclass
                # This case can be reached in Python 3, where particular frames are included, but not class names found.
                ce_signature = self.module_name + "." + f.__name__
                impl_type_args = [self.module_name, f.__name__]
        # Include the registering info related to @task
        impl_signature = ce_signature
        impl_constraints = {}
        impl_type = "METHOD"

        # Maybe some top decorator has already added some parameters
        # These if statements avoid us to overwrite these already
        # existing attributes
        # For example, the constraint decorator adds things in the impl_constraints
        # field, so it would be nice to not overwrite it!

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
                "[@TASK] I have to do the register of function %s in module %s" % (f.__name__, self.module_name))
            logger.debug("[@TASK] %s" % str(f))
        binding.register_ce(current_core_element)

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
        import inspect
        self.param_args, self.param_varargs, self.param_kwargs, self.param_defaults = \
            inspect.getargspec(self.user_function)
        # It will be easier to deal with functions if we pretend that all have the
        # signature f(positionals, *variadic, **named). This is why we are substituting
        # Nones with default stuff
        # As long as we remember what was the users original intention with the parameters
        # we can internally mess with his signature as much as we want. There is no need to add
        # self-imposed constraints here.
        # Also, the very nature of decorators are a huge hint about how we should treat user
        # functions, as most wrappers return a function f(*a, **k)
        if self.param_varargs is None:
            self.param_varargs = 'varargsType'
        if self.param_defaults is None:
            self.param_defaults = ()

    def compute_module_name(self):
        """
        Compute the user's function module name.
        There are various cases:
        1) The user function is defined in some file. This is easy, just get the module returned by inspect.getmodule
        2) The user function is in the main module. Retrieve the file and build the import name from it
        3) We are in interactive mode

        This function was taken from the old task.py and only some minor modifications were applied to it
        :return: Nothing, it just modifies self.module_name
        """
        import inspect
        import os
        mod = inspect.getmodule(self.user_function)
        self.module_name = mod.__name__
        # If it is a task within a class, the module it will be where the one
        # where the class is defined, instead of the one where the task is defined.
        # This avoids conflicts with task inheritance.
        if self.first_arg_name == 'self':
            mod = inspect.getmodule(type(self.parameters['self'].object))
            self.module_name = mod.__name__
        elif self.first_arg_name == 'cls':
            self.module_name = self.parameters['cls'].object.__module__
        if self.module_name == '__main__' or self.module_name == 'pycompss.runtime.launch':
            # The module where the function is defined was run as __main__,
            # We need to find out the real module name
            # Get the real module name from our launch.py app_path global variable
            # It is guaranteed that this variable will always exist because this code is only executed
            # when we know we are in the master
            path = getattr(mod, 'app_path')
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
                from pycompss.util.interactive_helpers import update_tasks_code_file
                update_tasks_code_file(self.user_function, path)
            # Get the module
            from pycompss.util.object_properties import get_module_name
            self.module_name = get_module_name(path, file_name)

    def compute_function_type(self):
        """
        Compute some properties of the user function, as its name,
        its import path, and its type (module function, instance method, class method),
        etc...
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

    def compute_user_function_information(self):
        """
        Compute the function path p and the name n such that
        "from p import n" imports self.user_function
        :return: None, it just sets self.user_function_path and self.user_function_name
        """
        # Get the module name (the x part "from x import y"), except for the class name
        self.compute_module_name()
        self.compute_function_type()
        self.function_name = self.user_function.__name__

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
        # Inspect the user function, get information about the arguments and their names
        # This defines self.param_args, self.param_varargs, self.param_kwargs, self.param_defaults
        # And gives non-None default values to them if necessary
        self.inspect_user_function_arguments()
        # Process the parameters, give them a proper direction
        self.process_master_parameters(*args, **kwargs)
        # Compute the function path, class (if any), and name
        self.compute_user_function_information()
        # Process the decorators to get the core element information
        # It is necessary to decide whether to register or not (the task may be inherited,
        # and in this case it has to be registered again with the new implementation signature).
        impl_signature = self.prepare_core_element_information(self.user_function)
        if not self.registered or self.signature != impl_signature:
            self.register_task(self.user_function)
            self.registered = True
            self.signature = impl_signature
        # Reset the global core element to a full-None status, ready for the next task!
        # (Note that this region is locked, so no race conditions will ever happen here).
        current_core_element.reset()
        # Did we call this function to only register the associated core element?
        # (This can happen when trying)
        if register_only:
            master_lock.release()
            return
        # Deal with the return part.
        self.add_return_parameters()
        if not self.returns:
            self.update_return_if_no_returns(self.user_function)
        from pycompss.runtime.binding import process_task
        ret = process_task(
            self.user_function,
            self.module_name,
            self.class_name,
            self.function_type,
            self.parameters,
            self.returns,
            self.decorator_arguments,
            self.computing_nodes,
            self.decorator_arguments['isReplicated'],
            self.decorator_arguments['isDistributed']
        )
        master_lock.release()
        return ret

    def get_varargs_direction(self):
        """
        Returns the direction of the varargs arguments.
        Can be defined in the decorator in two ways:
        args = dir, where args is the name of the variadic args tuple, or
        varargsType = dir (for legacy reasons)
        """
        if self.param_varargs not in self.decorator_arguments:
            return self.decorator_arguments['varargsType']
        return self.decorator_arguments[self.param_varargs]

    def get_default_direction(self, var_name):
        """
        Returns the default direction for a given parameter
        :return: An identifier of the direction
        """
        # We are the 'self' or 'cls' in an instance or classmethod that modifies the given class
        # so we are an INOUT
        if self.decorator_arguments['isModifier'] and var_name in ['self', 'cls'] and \
                self.param_args and self.param_args[0] == var_name:
            return parameter.get_new_parameter('INOUT')
        # Default, safest direction = IN
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
        # If we have an MPI, COMPSs or MultiNode decorator above us we should have computingNodes
        # as a kwarg, we should detect it and remove it. Otherwise we set it to 1
        self.computing_nodes = kwargs.pop('computingNodes', 1)
        # It is important to know the name of the first argument to determine if we
        # are dealing with a class or instance method (i.e: first argument is named self)
        self.first_arg_name = None
        # Process the positional arguments
        # Some of these positional arguments may have been not
        # explicitly defined
        num_positionals = min(len(self.param_args), len(args))
        for (var_name, var_value) in zip(self.param_args[:num_positionals], args[:num_positionals]):
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
        for (var_name, default_value) in reversed(list(zip(list(reversed(self.param_args))[:num_defaults],
                                                           list(reversed(self.param_defaults))))):
            if var_name not in parameter_values:
                parameter_values[parameter.get_kwarg_name(var_name)] = default_value
        # Process variadic and keyword arguments
        # Note that they are stored with custom names
        # This will allow us to determine the class of each parameter
        # and their order in the case of the variadic ones
        # Process the variadic arguments
        for (i, var_arg) in enumerate(args[num_positionals:]):
            parameter_values[parameter.get_vararg_name(self.param_varargs, i)] = var_arg
        # Process keyword arguments
        for (name, value) in kwargs.items():
            parameter_values[parameter.get_kwarg_name(name)] = value
        # Build a dictionary of parameters
        self.parameters = OrderedDict()
        # Assign directions to parameters
        for var_name in parameter_values.keys():
            # Is the argument a vararg? or a kwarg? Then check the direction for varargs or kwargs
            if parameter.is_vararg(var_name):
                self.parameters[var_name] = parameter.get_parameter_copy(self.get_varargs_direction())
            elif parameter.is_kwarg(var_name):
                real_name = parameter.get_name_from_kwarg(var_name)
                self.parameters[var_name] = self.decorator_arguments.get(real_name, self.get_default_direction(real_name))
            else:
                # The argument is named, check its direction
                # Default value = IN if not class or instance method and isModifier, INOUT otherwise
                # see self.get_default_direction
                # Note that if we have something like @task(self = IN) it will have priority over the default
                # direction resolution, even if this implies a contradiction with the isModifier flag
                self.parameters[var_name] = self.decorator_arguments.get(var_name, self.get_default_direction(var_name))
            # If the parameter is a FILE then its type will already be defined, and get_compss_type will misslabel it
            # as a TYPE.STRING
            if self.parameters[var_name].type is None:
                self.parameters[var_name].type = parameter.get_compss_type(parameter_values[var_name])
            if self.parameters[var_name].type == parameter.TYPE.FILE:
                self.parameters[var_name].file_name = parameter_values[var_name]
            else:
                self.parameters[var_name].object = parameter_values[var_name]

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
        Update worker parameter directions, will be useful to determine if files should be written later
        :param args: List of arguments
        """
        for arg in args:
            arg.direction = self.get_parameter_direction(arg.name)

    def is_parameter_object(self, name):
        """
        Given the name of a parameter, determine if it is an object or not
        :param name: Name of the parameter
        :return: True iff the parameter is a (serializable) object
        """
        original_name = parameter.get_original_name(name)
        # Get the args parameter object
        if parameter.is_vararg(original_name):
            return self.get_varargs_direction().type is None
        # Is this parameter annotated in the decorator?
        if original_name in self.decorator_arguments:
            return self.decorator_arguments[original_name].type is None
        # The parameter is not annotated in the decorator, so (by default) return True
        return True

    def reveal_objects(self, args):
        """
        (The name seemed funny to me so I kept it intact from the original version)
        This function takes the arguments passed from the persistent worker and treats them
        to get the proper parameters for the user function.
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
        for arg in [x for x in args if isinstance(x, parameter.TaskParameter) and not parameter.is_return(x.name)]:
            # This case is special, as a FILE can actually mean a FILE or an
            # object that is serialized in a file
            if parameter.is_vararg(arg.name):
                self.param_varargs = arg.name

            if arg.type == parameter.TYPE.FILE:
                if self.is_parameter_object(arg.name):
                    # The object is stored in some file, load and deserialize it
                    from pycompss.util.serializer import deserialize_from_file
                    arg.content = deserialize_from_file(arg.file_name.split(':')[-1])
                else:
                    # The object is a FILE, just forward the path of the file as a string parameter
                    arg.content = arg.file_name.split(':')[-1]
            elif not storage_supports_pipelining() and arg.type == parameter.TYPE.EXTERNAL_PSCO:
                # The object is a PSCO and the storage does not support pipelining, do a single getByID
                # of the PSCO
                from storage.api import getByID
                arg.content = getByID(arg.key)
                # If we have not entered in any of these cases we will assume that the object was a basic type
                # and the content is already available and properly casted by the python worker

    def worker_call(self, *args, **kwargs):
        """
        This part deals with task calls in the worker's side
        Note that the call to the user function is made by the worker,
        not by the user code.
        :return: A function that calls the user function with the given
        parameters and does the proper serializations and updates
        the affected objects.
        """
        # All parameters are in the same args list. At the moment we only know the type, the name and the
        # "value" of the parameter. This value may be treated to get the actual object (e.g: deserialize it,
        # query the database in case of persistent objects, etc...)
        self.reveal_objects(args)
        # After this line all the objects in arg have a "content" field, now we will segregate them in
        # User positional and variadic args
        user_args = []
        # User named args (kwargs)
        user_kwargs = {}
        # Return parameters, save them apart to match the user returns with the internal parameters
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
                user_kwargs[parameter.get_name_from_kwarg(arg.name)] = arg.content
            else:
                if parameter.is_vararg(arg.name):
                    self.param_varargs = parameter.get_varargs_name(arg.name)
                # Apart from the names we preserve the original order, so it is guaranteed that named positional
                # arguments will never be swapped with variadic ones or anything similar
                user_args.append(arg.content)

        num_returns = len(ret_params)

        # Call the user function with all the reconstructed parameters, get the return values
        user_returns = self.user_function(*user_args, **user_kwargs)

        def get_file_name(file_path):
            return file_path.split(':')[-1]

        # Deal with INOUTs
        for arg in [x for x in args if isinstance(x, parameter.TaskParameter) and self.is_parameter_object(x.name)]:
            original_name = parameter.get_original_name(arg.name)
            param = self.decorator_arguments.get(original_name, self.get_default_direction(original_name))
            if param.direction == parameter.DIRECTION.INOUT:
                from pycompss.util.serializer import serialize_to_file
                serialize_to_file(arg.content, get_file_name(arg.file_name))

        # Deal with returns (if any)
        if num_returns > 0:
            if num_returns == 1:
                # Generalize the return case to multi-return to simplify the code
                user_returns = [user_returns]
            # Note that we are implicitly assuming that the length of the user returns matches the number
            # of return parameters
            for (obj, param) in zip(user_returns, ret_params):
                # The object is a PSCO. Set its content (which is supposedly already persisted)
                # as its key
                if param.type == parameter.TYPE.EXTERNAL_PSCO:
                    from pycompss.util.persistent_storage import get_id
                    param.content = get_id(obj)
                # Serialize the object
                # Note that there is no "command line optimization" in the returns, as we always pass them as files
                # This is due to the asymmetry in worker-master communications and because it also makes it easier
                # for us to deal with returns in that format
                from pycompss.util.serializer import serialize_to_file
                serialize_to_file(obj, get_file_name(param.file_name))

        # We must notify COMPSs when types are updated
        # Potential update candidates are returns and INOUTs
        # But the whole types and values list must be returned
        new_types, new_values = [], []

        def get_object_information(obj):
            """ Returns a pair (t, v) with the new type and value of
            some object
            :param obj:
            :return:
            """
            ret_type = parameter.get_compss_type(obj)
            return ret_type, obj.getID() if ret_type == parameter.TYPE.EXTERNAL_PSCO else 'null'

        pending_to_swap = False

        for arg in args:
            is_task_param = isinstance(arg, parameter.TaskParameter)
            obj = arg.content if is_task_param else arg
            # Do we have a self? Then well have to swap these two lists in the future
            pending_to_swap |= is_task_param
            t, v = get_object_information(obj)
            new_types.append(t)
            new_values.append(v)

        if pending_to_swap:
            # We have a self parameter, we should respect the original order of the parameters as in
            # the Java COMPSs implementation
            for L in [new_types, new_values]:
                L.append(L.pop(0))

        return new_types, new_values, self.decorator_arguments['isModifier']

    def sequential_call(self, *args, **kwargs):
        """
        The easiest case: just call the user function and return whatever it
        returns.
        :return: The user function
        """
        # Inspect the user function, get information about the arguments and their names
        # This defines self.param_args, self.param_varargs, self.param_kwargs, self.param_defaults
        # And gives non-None default values to them if necessary
        return self.user_function(*args, **kwargs)


# task can be also typed as Task
Task = task
