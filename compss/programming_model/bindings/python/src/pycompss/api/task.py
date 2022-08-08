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
PyCOMPSs API - Task decorator.

This file contains the Task class, needed for the task definition.
"""

from __future__ import print_function

import inspect
import os
import sys
from functools import wraps

from pycompss.api import parameter
from pycompss.util.context import CONTEXT
from pycompss.api.commons.constants import LABELS
from pycompss.api.commons.constants import LEGACY_LABELS
from pycompss.api.commons.constants import INTERNAL_LABELS
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.implementation_types import IMPLEMENTATION_TYPES
from pycompss.api.dummy.task import task as dummy_task
from pycompss.runtime.binding import nested_barrier
from pycompss.runtime.start.initialization import LAUNCH_STATUS
from pycompss.runtime.task.core_element import CE
from pycompss.runtime.task.master import TaskMaster
from pycompss.runtime.task.parameter import Parameter
from pycompss.runtime.task.parameter import get_new_parameter
from pycompss.runtime.task.parameter import get_parameter_from_dictionary
from pycompss.runtime.task.parameter import is_param
from pycompss.runtime.task.worker import TaskWorker
from pycompss.util.logger.helpers import update_logger_handlers
from pycompss.util.objects.properties import get_module_name
from pycompss.util.tracing.helpers import EventInsideWorker
from pycompss.util.tracing.helpers import EventMaster
from pycompss.util.tracing.types_events_master import TRACING_MASTER
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.util.typing_helper import dummy_function
from pycompss.util.typing_helper import typing
from pycompss.util.exceptions import PyCOMPSsException

if __debug__:
    import logging

    logger = logging.getLogger(__name__)


class TaskArguments:
    """Task decorator arguments class definition.

    This class represents the supported task decorator arguments.
    """

    __slots__ = [
        # Supported keywords
        "target_direction",
        "returns",
        "cache_returns",
        "priority",
        "defaults",
        "time_out",
        "is_replicated",
        "is_distributed",
        "computing_nodes",
        "processes_per_node",
        "is_reduce",
        "chunk_size",
        "on_failure",
        "tracing_hook",
        "numba",
        "numba_flags",
        "numba_signature",
        "numba_declaration",
        # varargs_type kept for legacy purposes:
        "varargs_type",
        # Function parameters:
        "parameters",
    ]

    def __init__(self):
        """Set the default task decorator arguments values."""
        # Supported arguments in the @task decorator
        self.target_direction = parameter.INOUT.key
        self.returns = False
        self.cache_returns = True
        self.priority = False
        self.defaults = {}
        self.time_out = 0
        self.is_replicated = False
        self.is_distributed = False
        self.computing_nodes = 1
        self.processes_per_node = 1
        self.is_reduce = False
        self.chunk_size = 0
        self.on_failure = "RETRY"
        self.tracing_hook = False
        self.numba = False  # numba mode (jit, vectorize, guvectorize)
        self.numba_flags = {}  # user defined extra numba flags
        self.numba_signature = None  # vectorize and guvectorize signature
        self.numba_declaration = None  # guvectorize declaration
        # varargs_type kept for legacy purposes:
        self.varargs_type = parameter.IN.key
        # Function parameters:
        self.parameters = {}

    @staticmethod
    def __deprecation_warning__(old_keyword: str, new_keyword: str):
        """Log the deprecation warning message for the given keyword.

        :param old_keyword: Deprecated keyword.
        :param new_keyword: Keyword that should be used.
        :return: None
        """
        logger.warning(
            "Detected deprecated %s. Please, change it to %s", old_keyword, new_keyword
        )

    def update_arguments(self, kwargs: typing.Dict[str, typing.Any]) -> None:
        """Update the task arguments with the given kwargs.

        :param kwargs: Task keyword arguments.
        :returns: None
        """
        if LABELS.target_direction in kwargs:
            target_direction = kwargs.pop(LABELS.target_direction)
            if is_param(target_direction):
                self.target_direction = target_direction.key
            else:
                raise PyCOMPSsException(
                    f"Unexpected {LABELS.target_direction} type. Must be a direction."
                )
        elif LEGACY_LABELS.target_direction in kwargs:
            target_direction = kwargs.pop(LEGACY_LABELS.target_direction)
            if is_param(target_direction):
                self.target_direction = target_direction.key
                self.__deprecation_warning__(
                    LEGACY_LABELS.target_direction, LABELS.target_direction
                )
            else:
                raise PyCOMPSsException(
                    f"Unexpected {LEGACY_LABELS.target_direction} type. Must be a direction."
                )
        if LABELS.returns in kwargs:
            self.returns = kwargs.pop(LABELS.returns)
        if LABELS.cache_returns in kwargs:
            self.cache_returns = kwargs.pop(LABELS.cache_returns)
        if LABELS.priority in kwargs:
            self.priority = kwargs.pop(LABELS.priority)
        if LABELS.defaults in kwargs:
            self.defaults = kwargs.pop(LABELS.defaults)
        if LABELS.time_out in kwargs:
            self.time_out = kwargs.pop(LABELS.time_out)
        elif LEGACY_LABELS.time_out in kwargs:
            self.time_out = kwargs.pop(LEGACY_LABELS.time_out)
            self.__deprecation_warning__(LEGACY_LABELS.time_out, LABELS.time_out)
        if LABELS.is_replicated in kwargs:
            self.is_replicated = kwargs.pop(LABELS.is_replicated)
        elif LEGACY_LABELS.is_replicated in kwargs:
            self.is_replicated = kwargs.pop(LEGACY_LABELS.is_replicated)
            self.__deprecation_warning__(
                LEGACY_LABELS.is_replicated, LABELS.is_replicated
            )
        if LABELS.is_distributed in kwargs:
            self.is_distributed = kwargs.pop(LABELS.is_distributed)
        elif LEGACY_LABELS.is_distributed in kwargs:
            self.is_distributed = kwargs.pop(LEGACY_LABELS.is_distributed)
            self.__deprecation_warning__(
                LEGACY_LABELS.is_distributed, LABELS.is_distributed
            )
        if LABELS.computing_nodes in kwargs:
            self.computing_nodes = kwargs.pop(LABELS.computing_nodes)
        elif LEGACY_LABELS.computing_nodes in kwargs:
            self.computing_nodes = kwargs.pop(LEGACY_LABELS.computing_nodes)
            self.__deprecation_warning__(
                LEGACY_LABELS.computing_nodes, LABELS.computing_nodes
            )
        if LABELS.processes_per_node in kwargs:
            self.processes_per_node = kwargs.pop(LABELS.processes_per_node)
        if LABELS.is_reduce in kwargs:
            self.is_reduce = kwargs.pop(LABELS.is_reduce)
        if LABELS.chunk_size in kwargs:
            self.chunk_size = kwargs.pop(LABELS.chunk_size)
        if LABELS.on_failure in kwargs:
            self.on_failure = kwargs.pop(LABELS.on_failure)
        if LABELS.tracing_hook in kwargs:
            self.tracing_hook = kwargs.pop(LABELS.tracing_hook)
        if LABELS.numba in kwargs:
            self.numba = kwargs.pop(LABELS.numba)
        if LABELS.numba_flags in kwargs:
            self.numba_flags = kwargs.pop(LABELS.numba_flags)
        if LABELS.numba_signature in kwargs:
            self.numba_signature = kwargs.pop(LABELS.numba_signature)
        if LABELS.numba_declaration in kwargs:
            self.numba_declaration = kwargs.pop(LABELS.numba_declaration)
        if LABELS.varargs_type in kwargs:
            varargs_type = kwargs.pop(LABELS.varargs_type)
            if is_param(varargs_type):
                self.varargs_type = varargs_type.key
            else:
                raise PyCOMPSsException(
                    "Unexpected varargs_type type. Must be a direction."
                )
        elif LEGACY_LABELS.varargs_type in kwargs:
            varargs_type = kwargs.pop(LEGACY_LABELS.varargs_type)
            if is_param(varargs_type):
                self.varargs_type = varargs_type.key
                self.__deprecation_warning__(
                    LEGACY_LABELS.varargs_type, LABELS.varargs_type
                )
            else:
                raise PyCOMPSsException(
                    "Unexpected varargsType type. Must be a direction."
                )
        # The rest of the arguments are expected to be only the function
        # parameter related information
        for (key, value) in kwargs.items():
            if isinstance(value, dict):
                # It is a dictionary for the given parameter (e.g. param={Direction: IN})
                self.parameters[key] = get_parameter_from_dictionary(value)
            else:
                # It is a keyword for the given parameter (e.g. param=IN)
                try:
                    self.parameters[key] = get_new_parameter(value.key)
                except AttributeError:  # no .key in value.
                    # Non defined parameters: usually mistaken parameters.
                    self.parameters[key] = value

    def __repr__(self) -> str:
        """Representation of the task arguments definition.

        :return: String representing the task arguments.
        """
        task_arguments = "Task arguments:\n"
        task_arguments += f"- Target direction: {self.target_direction}\n"
        task_arguments += f"- Returns: {self.returns}\n"
        task_arguments += f"- Cache returns: {self.cache_returns}\n"
        task_arguments += f"- Priority: {self.priority}\n"
        task_arguments += f"- Defaults: {self.defaults}\n"
        task_arguments += f"- Time out: {self.time_out}\n"
        task_arguments += f"- Is replicated: {self.is_replicated}\n"
        task_arguments += f"- Is distributed: {self.is_distributed}\n"
        task_arguments += f"- Computing nodes: {self.computing_nodes}\n"
        task_arguments += f"- Processes per node: {self.processes_per_node}\n"
        task_arguments += f"- Is reduce: {self.is_reduce}\n"
        task_arguments += f"- Chunk size: {self.chunk_size}\n"
        task_arguments += f"- On failure: {self.on_failure}\n"
        task_arguments += f"- Tracing hook: {self.tracing_hook}\n"
        task_arguments += f"- Numba: {self.numba}\n"
        task_arguments += f"- Numba flags: {self.numba_flags}\n"
        task_arguments += f"- Numba signature: {self.numba_signature}\n"
        task_arguments += f"- Numba declaration: {self.numba_declaration}\n"
        task_arguments += f"- Varargs type: {self.varargs_type}\n"
        task_arguments += f"- Parameters: {self.parameters}"
        return task_arguments

    def get_keys(self):  # TODO: avoid this function.
        """Return defined arguments.

        :return: List of strings
        """
        arguments = [
            "target_direction",
            "returns",
            "cache_returns",
            "priority",
            "defaults",
            "time_out",
            "is_replicated",
            "is_distributed",
            "computing_nodes",
            "processes_per_node",
            "is_reduce",
            "chunk_size",
            "on_failure",
            "tracing_hook",
            "numba",
            "numba_flags",
            "numba_signature",
            "numba_declaration",
            "varargs_type",
        ]
        return arguments

    def get_parameter(self, name: str, default_direction: str) -> Parameter:
        """Retrieve the Parameter object associated to name.

        If not found, retrieves a new Parameter object with the default
        direction.

        :param name: Parameter name.
        :param default_direction: Default direction if not found.
        :return: The Parameter object
        """
        if name in self.parameters:
            return self.parameters[name]
        # Not found:
        default_parameter = get_new_parameter(default_direction)
        return default_parameter

    def get_parameter_or_none(self, name: str) -> typing.Optional[Parameter]:
        """Retrieve the Parameter object associated to name.

        If not found, retrieves None.

        :param name: Parameter name.
        :return: The Parameter object or None
        """
        if name in self.parameters:
            return self.parameters[name]
        return None


class Task:  # pylint: disable=too-few-public-methods, too-many-instance-attributes
    """This is the Task decorator implementation.

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

    __slots__ = [
        "task_type",
        "decorator_name",
        "scope",
        "core_element",
        "core_element_configured",
        "decorator_arguments",
        "user_function",
        "registered",
        "signature",
        "interactive",
        "module",
        "function_arguments",
        "function_name",
        "module_name",
        "function_type",
        "class_name",
        "hints",
        "on_failure",
        "defaults",
    ]

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Task constructor.

        This part is called in the decoration process, not as an
        explicit function call.

        We do two things here:
            a) Assign default values to unspecified fields.
            b) Transform the parameters from user friendly types
               (i.e Parameter.IN, etc) to a more convenient internal
               representation.

        :param args: Decorator positional parameters (ignored).
        :param kwargs: Decorator parameters. A task decorator has no positional
                       arguments.
        """
        self.task_type = IMPLEMENTATION_TYPES.method
        self.decorator_name = "".join(("@", Task.__name__.lower()))
        self.scope = CONTEXT.in_pycompss()
        self.core_element = CE()
        self.core_element_configured = False

        # Create TaskArguments object from kwargs
        task_decorator_arguments = TaskArguments()
        task_decorator_arguments.update_arguments(kwargs)
        self.decorator_arguments = task_decorator_arguments

        self.user_function = dummy_function  # type: typing.Callable
        # Global variables common for all tasks of this kind
        self.registered = False
        self.signature = ""
        # Saved from the initial task
        self.interactive = False
        self.module = None  # type: typing.Any
        self.function_arguments = tuple()  # type: tuple
        self.function_name = ""
        self.module_name = ""
        self.function_type = -1
        self.class_name = ""
        self.hints = tuple()  # type: tuple
        self.on_failure = ""
        self.defaults = {}  # type: dict

    def __call__(self, user_function: typing.Callable) -> typing.Callable:
        """Perform the task processing.

        This function is called in all explicit function calls.
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
        self.user_function = user_function

        @wraps(user_function)
        def task_decorator(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            return self.__decorator_body__(user_function, args, kwargs)

        return task_decorator

    def __decorator_body__(
        self, user_function: typing.Callable, args: tuple, kwargs: dict
    ) -> typing.Any:
        """Body of the task decorator.

        :param user_function: Decorated function.
        :param args: Function arguments.
        :param kwargs: Function keyword arguments.
        :returns: Result of executing the user_function with the given args and kwargs.
        """
        # Determine the context and decide what to do
        if CONTEXT.in_master():
            # @task being executed in the master
            # Each task will have a TaskMaster, so its content will
            # not be shared.
            self.__check_core_element__(kwargs, user_function)
            with EventMaster(TRACING_MASTER.task_instantiation):
                master = TaskMaster(
                    self.decorator_arguments,
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
                    self.defaults,
                )
            result = master.call(args, kwargs)
            (
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
            ) = result
            del master
            return future_object
        if CONTEXT.in_worker():
            if "compss_key" in kwargs.keys():
                is_nesting_enabled = CONTEXT.is_nesting_enabled()
                if is_nesting_enabled:
                    if __debug__:
                        # Update the whole logger since it will be in job out/err
                        update_logger_handlers(
                            kwargs["compss_log_cfg"],
                            kwargs["compss_log_files"][0],
                            kwargs["compss_log_files"][1],
                        )
                # @task being executed in the worker
                with EventInsideWorker(TRACING_WORKER.worker_task_instantiation):
                    worker = TaskWorker(
                        self.decorator_arguments,
                        self.user_function,
                        self.on_failure,
                        self.defaults,
                    )
                result = worker.call(*args, **kwargs)
                # Force flush stdout and stderr
                sys.stdout.flush()
                sys.stderr.flush()
                # Remove worker
                del worker
                if is_nesting_enabled:
                    # Wait for all nested tasks to finish
                    nested_barrier()
                    if __debug__:
                        # Reestablish logger handlers
                        update_logger_handlers(kwargs["compss_log_cfg"])
                return result

            # There is no compss_key in kwargs.keys() => task invocation within task:
            #  - submit the task to the runtime if nesting is enabled.
            #  - execute sequentially if nested is not enabled.
            if CONTEXT.is_nesting_enabled():
                # Each task will have a TaskMaster, so its content will
                # not be shared.
                with EventMaster(TRACING_MASTER.task_instantiation):
                    master = TaskMaster(
                        self.decorator_arguments,
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
                        self.defaults,
                    )
                result = master.call(args, kwargs)
                (
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
                ) = result
                del master
                return future_object
            # Called from another task within the worker
            # Ignore the @task decorator and run it sequentially
            message = (
                f"WARNING: Calling task: {str(user_function.__name__)} from this task.\n"
                f"         It will be executed sequentially within the caller task."
            )
            print(message, file=sys.stderr)
            return self._sequential_call(*args, **kwargs)
        # We are neither in master nor in the worker, or the user has
        # stopped the interactive session.
        # Therefore, the user code is being executed with no
        # launch_compss/enqueue_compss/runcompss/interactive session
        return self._sequential_call(*args, **kwargs)

    def _sequential_call(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        """Sequential task execution.

        The easiest case: just call the user function and return whatever it
        returns.

        :return: The user function return.
        """
        # Inspect the user function, get information about the arguments and
        # their names
        # This defines self.param_args, self.param_varargs,
        # self.param_kwargs and self.param_defaults
        # And gives non-None default values to them if necessary
        d_t = dummy_task(args, kwargs)
        return d_t.__call__(self.user_function)(*args, **kwargs)

    def __check_core_element__(
        self, kwargs: dict, user_function: typing.Callable
    ) -> None:
        """Check Core Element for containers.

        :param kwargs: Keyword arguments.
        :param user_function: User function.
        :return: None (updates the Core Element of the given kwargs).
        """
        if (
            CORE_ELEMENT_KEY in kwargs
            and kwargs[CORE_ELEMENT_KEY].get_impl_type()
            == IMPLEMENTATION_TYPES.container
        ):
            # The task is using a container
            impl_args = kwargs[CORE_ELEMENT_KEY].get_impl_type_args()
            _type = impl_args[2]
            if _type == INTERNAL_LABELS.unassigned:
                # The task is not invoking a binary
                _engine = impl_args[0]
                _image = impl_args[1]
                _type = "CET_PYTHON"
                _module_name = str(self.__get_module_name__(user_function))
                _function_name = str(user_function.__name__)
                _func_complete = f"{_module_name}&{_function_name}"
                impl_args = [
                    _engine,  # engine
                    _image,  # image
                    _type,  # internal_type
                    INTERNAL_LABELS.unassigned,  # internal_binary
                    _func_complete,  # internal_func
                    INTERNAL_LABELS.unassigned,  # working_dir
                    INTERNAL_LABELS.unassigned,
                ]  # fail_by_ev
                kwargs[CORE_ELEMENT_KEY].set_impl_type_args(impl_args)

    @staticmethod
    def __get_module_name__(user_function: typing.Callable) -> str:
        """Get the module name from the user function.

        :param user_function: User function.
        :returns: The module name where the user function is defined.
        """
        mod = inspect.getmodule(user_function)  # type: typing.Any
        module_name = mod.__name__
        if module_name == "__main__":
            # The module where the function is defined was run as __main__,
            # We need to find out the real module name
            # Get the real module name from our launch.py APP_PATH global
            # variable
            # It is guaranteed that this variable will always exist because
            # this code is only executed when we know we are in the master
            path = LAUNCH_STATUS.get_app_path()
            # Get the file name
            file_name = os.path.splitext(os.path.basename(path))[0]
            # Get the module
            module_name = get_module_name(path, file_name)
        return module_name


# task can be also typed as Task
task = Task  # pylint: disable=invalid-name
