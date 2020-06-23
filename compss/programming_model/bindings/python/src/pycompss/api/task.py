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
import inspect
from functools import wraps

import pycompss.api.parameter as parameter
import pycompss.util.context as context
from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.runtime.task.core_element import CE
from pycompss.runtime.task.parameter import is_param
from pycompss.runtime.task.parameter import get_parameter_copy
from pycompss.runtime.task.parameter import is_dict_specifier
from pycompss.runtime.task.parameter import get_parameter_from_dictionary

if __debug__:
    import logging
    logger = logging.getLogger(__name__)

# Determine if strings should have a sharp symbol prepended or not
PREPEND_STRINGS = True
# Only register the task
REGISTER_ONLY = False
# Current core element for the registration (if necessary)
CURRENT_CORE_ELEMENT = CE()


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
    TaskWorker.call() and self.sequential_call()
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
            'numba': False,          # numba mode (jit, vectorize, guvectorize)
            'numba_flags': {},          # user defined extra numba flags
            'numba_signature': None,    # vectorize and guvectorize signature
            'numba_declaration': None,  # guvectorize declaration
            'varargs_type': parameter.IN  # Here for legacy purposes
        }

    def __init__(self, comment=None, **kwargs):  # noqa
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
            if is_param(value):
                self.decorator_arguments[key] = get_parameter_copy(value)
            # Specific case when value is a dictionary
            # Use case example:
            # @binary(binary="ls")
            # @task(hide={Type: FILE_IN, Prefix: "--hide="},
            #       sort={Type: IN, Prefix: "--sort="})
            # def myLs(flag, hide, sort):
            #   pass
            # Transform this dictionary to a Parameter object
            if is_dict_specifier(value):
                if key not in ['numba',
                               'numba_flags',
                               'numba_signature',
                               'numba_declaration']:
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

        # initial decorator arguments must be saved and passed to the following
        # 'calls' of the 'task'
        self.init_dec_args = copy.deepcopy(self.decorator_arguments)
        # Function to execute as task
        self.user_function = None

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
                from pycompss.runtime.task.master import TaskMaster
                master = TaskMaster(self.comment,
                                    self.decorator_arguments,
                                    self.init_dec_args,
                                    self.user_function)
                return master.call(*args, **kwargs)
            elif context.in_worker():
                if 'compss_key' in kwargs.keys():
                    from pycompss.runtime.task.worker import TaskWorker
                    worker = TaskWorker(self.decorator_arguments,
                                        self.user_function)
                    return worker.call(*args, **kwargs)
                else:
                    # Called from another task within the worker
                    # Ignore the @task decorator and run it sequentially
                    message = "WARNING: Calling task: "
                    message += str(user_function.__name__)
                    message += " from this task.\n"
                    message += "         It will be executed sequentially "
                    message += "within the caller task."
                    print(message, file=sys.stderr, flush=True)
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
        mod = inspect.getmodule(self.user_function)
        module_name = mod.__name__
        if context.in_pycompss() and \
                (module_name == '__main__' or
                 module_name == 'pycompss.runtime.launch'):
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
                from pycompss.util.interactive.helpers import update_tasks_code_file  # noqa: E501
                update_tasks_code_file(self.user_function, path)
                print("Found task: " + str(self.user_function.__name__))
        else:
            # No need to update anything
            pass

    def sequential_call(self, *args, **kwargs):
        """
        The easiest case: just call the user function and return whatever it
        returns.

        :return: The user function return
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
