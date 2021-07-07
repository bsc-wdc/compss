#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
import sys
from functools import wraps

import pycompss.api.parameter as parameter
import pycompss.util.context as context
from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.runtime.constants import TASK_INSTANTIATION
from pycompss.worker.commons.constants import WORKER_TASK_INSTANTIATION
from pycompss.runtime.task.master import TaskMaster
from pycompss.runtime.task.worker import TaskWorker
from pycompss.runtime.task.parameter import is_param
from pycompss.runtime.task.parameter import get_new_parameter
from pycompss.runtime.task.parameter import get_parameter_from_dictionary
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.util.tracing.helpers import event

if __debug__:
    import logging
    logger = logging.getLogger(__name__)

# Determine if strings should have a sharp symbol prepended or not
PREPEND_STRINGS = True
# Only register the task
REGISTER_ONLY = False


class Task(PyCOMPSsDecorator):
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
    The specific implementations can be found in TaskMaster.call(),
    TaskWorker.call() and self._sequential_call()
    """

    __slots__ = ["task_type", "decorator_arguments", "user_function",
                 "registered", "signature",
                 "interactive", "module", "function_arguments",
                 "function_name", "module_name", "function_type", "class_name",
                 "hints", "on_failure", "defaults"]

    @staticmethod
    def _get_default_decorator_values():
        """ Default value for decorator arguments.

        By default, do not use jit (if true -> use nopython mode,
        alternatively, the user can define a dictionary with the specific
        flags - using a dictionary will be considered as the user wants to use
        compile with jit).

        :return: A dictionary with the default values of the non-parameter
                 decorator fields.
        """
        return {
            "target_direction": parameter.INOUT,
            "returns": False,
            "priority": False,
            "defaults": {},
            "time_out": 0,
            "is_replicated": False,
            "is_distributed": False,
            "computing_nodes": 1,
            "is_reduce": False,
            "chunk_size": 0,
            "tracing_hook": False,
            "numba": False,  # numba mode (jit, vectorize, guvectorize)
            "numba_flags": {},  # user defined extra numba flags
            "numba_signature": None,  # vectorize and guvectorize signature
            "numba_declaration": None,  # guvectorize declaration
            "varargs_type": parameter.IN,  # Here for legacy purposes
        }

    def __init__(self, *args, **kwargs):  # noqa
        """ Task constructor.

        This part is called in the decoration process, not as an
        explicit function call.

        We do two things here:
            a) Assign default values to unspecified fields
               (see _get_default_decorator_values).
            b) Transform the parameters from user friendly types
               (i.e Parameter.IN, etc) to a more convenient internal
               representation.

        :param kwargs: Decorator parameters. A task decorator has no positional
                       arguments.
        :param user_function: User function to execute.
        :param core_element: Core element for the task (only used in master,
                             but needed here to keep it between task
                             invocations).
        :param registered: If the core element has already been registered.
        :param signature: The user function signature.
        """
        self.task_type = "METHOD"
        decorator_name = "".join(('@', Task.__name__.lower()))
        super(Task, self).__init__(decorator_name, *args, **kwargs)

        self.decorator_arguments = kwargs
        # Set missing values to their default ones (step a)
        for (key, value) in self._get_default_decorator_values().items():
            if key not in self.decorator_arguments:
                self.decorator_arguments[key] = value
        # Give all parameters a unique instance for them (step b)
        # Note that when a user defines a task like
        # @task(a = IN, b = IN, c = INOUT)
        # both a and b point to the same IN object (the one from parameter.py)
        # Giving them a unique instance makes life easier in further steps
        for (key, value) in self.decorator_arguments.items():
            # Not all decorator arguments are necessarily parameters
            # (see self._get_default_decorator_values)
            if is_param(value):
                self.decorator_arguments[key] = get_new_parameter(value.key)
            # Specific case when value is a dictionary
            # Use case example:
            # @binary(binary="ls")
            # @task(hide={Type: FILE_IN, Prefix: "--hide="},
            #       sort={Type: IN, Prefix: "--sort="})
            # def myLs(flag, hide, sort):
            #   pass
            # Transform this dictionary to a Parameter object
            if isinstance(value, dict):
                if key not in ["numba",
                               "numba_flags",
                               "numba_signature",
                               "numba_declaration"]:
                    # Perform user -> instance substitution
                    # param = self.decorator_arguments[key][parameter.Type]
                    # Replace the whole dict by a single parameter object
                    self.decorator_arguments[key] = \
                        get_parameter_from_dictionary(
                            self.decorator_arguments[key]
                        )
                else:
                    # It is a reserved word that we need to keep the user
                    # defined value (not a Parameter object)
                    self.decorator_arguments[key] = value

        # Function to execute as task
        self.user_function = None
        # Global variables common for all tasks of this kind
        self.registered = None
        self.signature = None
        # Saved from the initial task
        self.interactive = None
        self.module = None
        self.function_arguments = None
        self.function_name = None
        self.module_name = None
        self.function_type = None
        self.class_name = None
        self.hints = None
        self.on_failure = None
        self.defaults = None

    def __call__(self, user_function):
        """ This function is called in all explicit function calls.

        Note that in PyCOMPSs a single function call will be transformed into
        two calls, as both master and worker need to call the function.

        The work to do in the master part is totally different
        from the job to do in the worker part. This is why there are
        some other functions like master_call, worker_call, and
        _sequential_call.

        There is also a third case that happens when the user runs a PyCOMPSs
        code without PyCOMPSs. This case is straightforward: just call the
        user function with the user parameters and return whatever the user
        code returned. Therefore, we can just return the user function.

        :param user_function: Function to decorate.
        :return: The function to be executed.
        """
        global logger
        self.user_function = user_function

        @wraps(user_function)
        def task_decorator(*args, **kwargs):
            # global logger
            # if "compss_logger" in kwargs.keys():
            #     # if invoked from a worker, then take the provided
            #     # Otherwise, continue with default logger
            #     logger = kwargs["compss_logger"]
            return self.__decorator_body__(user_function, args, kwargs)

        return task_decorator

    def __decorator_body__(self, user_function, args, kwargs):
        # Determine the context and decide what to do
        if context.in_master():
            # @task being executed in the master
            # Each task will have a TaskMaster, so its content will
            # not be shared.
            self.__check_core_element__(kwargs, user_function)
            with event(TASK_INSTANTIATION, master=True):
                master = TaskMaster(self.decorator_arguments,
                                    self.user_function,
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
                                    self.on_failure,
                                    self.defaults)
            result = master.call(*args, **kwargs)
            fo, self.core_element, self.registered, self.signature, self.interactive, self.module, self.function_arguments, self.function_name, self.module_name, self.function_type, self.class_name, self.hints = result  # noqa: E501
            del master
            return fo
        elif context.in_worker():
            if "compss_key" in kwargs.keys():
                # @task being executed in the worker
                with event(WORKER_TASK_INSTANTIATION,
                           master=False, inside=True):
                    worker = TaskWorker(self.decorator_arguments,
                                        self.user_function,
                                        self.on_failure,
                                        self.defaults)
                result = worker.call(*args, **kwargs)
                del worker
                if context.is_nesting_enabled():
                    from pycompss.runtime.binding import nested_barrier
                    nested_barrier()
                return result
            else:
                if context.is_nesting_enabled():
                    # nested @task executed in the worker
                    # Each task will have a TaskMaster, so its content will
                    # not be shared.
                    with event(TASK_INSTANTIATION, master=True):
                        master = TaskMaster(self.decorator_arguments,
                                            self.user_function,
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
                                            self.on_failure,
                                            self.defaults)
                        result = master.call(*args, **kwargs)
                        fo, self.core_element, self.registered, self.signature, self.interactive, self.module, self.function_arguments, self.function_name, self.module_name, self.function_type, self.class_name, self.hints = result  # noqa: E501
                    del master
                    return fo
                else:
                    # Called from another task within the worker
                    # Ignore the @task decorator and run it sequentially
                    message = "".join(("WARNING: Calling task: ",
                                       str(user_function.__name__),
                                       " from this task.\n",
                                       "         It will be executed sequentially ",  # noqa: E501
                                       "within the caller task."))
                    print(message, file=sys.stderr)
                    return self._sequential_call(*args, **kwargs)
        # We are neither in master nor in the worker, or the user has
        # stopped the interactive session.
        # Therefore, the user code is being executed with no
        # launch_compss/enqueue_compss/runcompss/interactive session
        return self._sequential_call(*args, **kwargs)

    def _sequential_call(self, *args, **kwargs):
        """ Sequential task execution.

        The easiest case: just call the user function and return whatever it
        returns.

        :return: The user function return.
        """
        # Inspect the user function, get information about the arguments and
        # their names
        # This defines self.param_args, self.param_varargs,
        # self.param_kwargs and self.param_defaults
        # And gives non-None default values to them if necessary
        from pycompss.api.dummy.task import task as dummy_task
        d_t = dummy_task(args, kwargs)
        return d_t.__call__(self.user_function)(*args, **kwargs)

    @staticmethod
    def __check_core_element__(kwargs, user_function):
        """ Check Core Element for containers.

        :param kwargs: Keyword arguments
        :param user_function: User function
        :return: None (updates the Core Element of the given kwargs)
        """
        if CORE_ELEMENT_KEY in kwargs and \
                kwargs[CORE_ELEMENT_KEY].get_impl_type() == "CONTAINER":
            # The task is using a container
            impl_args = kwargs[CORE_ELEMENT_KEY].get_impl_type_args()
            _type = impl_args[2]
            unassigned = "[unassigned]"
            if _type == unassigned:
                # The task is not invoking a binary
                _engine = impl_args[0]
                _image = impl_args[1]
                _type = "CET_PYTHON"
                _func_complete = "%s&%s" % (str(user_function.__module__),
                                            str(user_function.__name__))
                impl_args = [_engine,         # engine
                             _image,          # image
                             _type,           # internal_type
                             unassigned,      # internal_binary
                             _func_complete,  # internal_func
                             unassigned,      # working_dir
                             unassigned]      # fail_by_ev
                kwargs[CORE_ELEMENT_KEY].set_impl_type_args(impl_args)


# task can be also typed as Task
task = Task
TASK = Task
