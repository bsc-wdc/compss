#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs API - Software decorator.

This file contains the Software class, needed for the software task definition
through the decorator. Software decorator can be used to move the definition of
the multiple decorators to a JSON file.

WARNING: CAN NOT BE COMPILED WITH MYPY SINCE THE SOFTWARE DECORATOR
         INHERITS FROM TASK DECORATOR.
"""
import builtins
import json
import sys
from functools import wraps
from pycompss.runtime.task.master import TaskMaster

from pycompss.util.context import CONTEXT
from pycompss.api import binary
from pycompss.api import mpi
from pycompss.api import mpmd_mpi
from pycompss.api import multinode
from pycompss.api import http
from pycompss.api import compss
from pycompss.api import task
from pycompss.api import parameter

from pycompss.api.commons.constants import INTERNAL_LABELS
from pycompss.api.commons.constants import LABELS
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import resolve_fail_by_exit_value
from pycompss.api.commons.implementation_types import IMPLEMENTATION_TYPES
from pycompss.runtime.task.definitions.core_element import CE
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.typing_helper import typing
from pycompss.runtime.task.worker import TaskWorker


if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {LABELS.config_file}
SUPPORTED_ARGUMENTS = {LABELS.config_file}
DEPRECATED_ARGUMENTS = set()  # type: typing.Set[str]

SUPPORTED_DECORATORS = {
    LABELS.mpi: (mpi, mpi.mpi),
    LABELS.task: (task, task.task),
    LABELS.binary: (binary, binary.binary),
    LABELS.mpmd_mpi: (mpmd_mpi, mpmd_mpi.mpmd_mpi),
    LABELS.http: (http, http.http),
    LABELS.compss: (compss, compss.compss),
    LABELS.multinode: (multinode, multinode.multinode),
}


class Software(
    task.task
):  # pylint: disable=too-few-public-methods, too-many-instance-attributes
    """Software decorator class.

    When provided with a config file, it can replicate any existing python
    decorator by wrapping the user function with the decorator defined in
    the config file. Arguments of the decorator should be defined in the
    config file which is in JSON format.
    """

    __slots__ = [
        "args",
        "kwargs",
        "config_args",
        "decor",
        "constraints",
        "container",
        "prolog",
        "epilog",
        "parameters",
        "file_path",
        "is_workflow",
    ]

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Initialize the software decorator.

        Only when in the Master, parses the config file and generates
        the decorators. Otherwise, just an "__init__".

        :param args: not used (maybe should be?).
        :param kwargs: so far contains only the JSON configuration file path.

        """
        super().__init__(*args, **kwargs)
        decorator_name = "".join(("@", Software.__name__.lower()))
        self.task_type = None  # type: typing.Any
        self.config_args = None  # type: typing.Any
        self.decor = None  # type: typing.Any
        self.constraints = None  # type: typing.Any
        self.container = None  # type: typing.Any
        self.prolog = None  # type: typing.Any
        self.epilog = None  # type: typing.Any
        self.parameters = {}  # type: typing.Dict
        self.is_workflow = False  # type: bool
        self.file_path = None

        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = CONTEXT.in_pycompss()
        self.core_element = None  # type: typing.Any
        self.core_element_configured = False

        self.registered_signatures = {}
        self.constraint_args = {}

        # no need to parse the config file in the worker. all the @task params
        # are passed inside "kwargs"
        if self.scope and CONTEXT.in_master():
            if __debug__:
                logger.debug("Init @software decorator..")
            # Check the arguments
            check_arguments(
                MANDATORY_ARGUMENTS,
                DEPRECATED_ARGUMENTS,
                SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                list(kwargs.keys()),
                decorator_name,
            )

    def __call__(  # pylint: disable=R0915
        self, user_function: typing.Callable
    ) -> typing.Callable:
        # disable=too-many-statements
        """Parse and set the software parameters within the task core element.

        When called, @software decorator basically wraps the user function
        into the "real" decorator and passes the args and kwargs.

        :param user_function: User function to be decorated.
        :return: User function decorated with the decor type defined by the
                 user.
        """

        @wraps(user_function)
        def software_f(  # pylint: disable=R0914,R0911,R0912,R0915
            *args: typing.Any, **kwargs: typing.Any
        ) -> typing.Any:
            # disable=too-many-locals, too-many-return-statements
            # disable=too-many-branches, too-many-statements
            if not self.scope:
                # Execute the software as with PyCOMPSs so that sequential
                # execution performs as parallel.
                # To disable: raise Exception(not_in_pycompss(LABELS.binary))
                return user_function(*args, **kwargs)

            self.decorated_function.function = user_function

            updated_args = self.pop_file_path(args)
            self.parse_config_file()

            if CONTEXT.in_worker():
                self.decorator_arguments.update_arguments(self.parameters)
                worker = TaskWorker(
                    self.decorator_arguments, self.decorated_function
                )
                m_result = worker.call(*updated_args, **kwargs)
                # Force flush stdout and stderr
                sys.stdout.flush()
                sys.stderr.flush()
                # Remove worker
                del worker
                return m_result

            if self.is_workflow:
                # no need to do anything, just run the user code
                return user_function(*args, **kwargs)

            if __debug__:
                logger.debug("Executing software_f wrapper.")

            if self.constraints is not None:
                core_element = CE()
                core_element.set_impl_constraints(self.constraints)
                kwargs[CORE_ELEMENT_KEY] = core_element

            if self.prolog is not None:
                resolve_fail_by_exit_value(self.prolog, "True")
                prolog_binary = self.prolog[LABELS.binary]
                prolog_params = self.prolog.get(
                    LABELS.args, INTERNAL_LABELS.unassigned
                )
                prolog_fail_by = self.prolog.get(LABELS.fail_by_exit_value)
                _prolog = [prolog_binary, prolog_params, prolog_fail_by]

                prolog_core_element = kwargs.get(CORE_ELEMENT_KEY, CE())
                prolog_core_element.set_impl_prolog(_prolog)
                kwargs[CORE_ELEMENT_KEY] = prolog_core_element

            if self.epilog is not None:
                resolve_fail_by_exit_value(self.epilog, "False")
                epilog_binary = self.epilog[LABELS.binary]
                epilog_params = self.epilog.get(
                    LABELS.args, INTERNAL_LABELS.unassigned
                )
                epilog_fail_by = self.epilog.get(LABELS.fail_by_exit_value)
                _epilog = [epilog_binary, epilog_params, epilog_fail_by]

                epilog_core_element = kwargs.get(CORE_ELEMENT_KEY, CE())
                epilog_core_element.set_impl_epilog(_epilog)
                kwargs[CORE_ELEMENT_KEY] = epilog_core_element

            if self.container is not None:
                _func = str(user_function.__name__)
                impl_type = IMPLEMENTATION_TYPES.container
                impl_signature = ".".join((impl_type, _func))

                core_element = kwargs.get(CORE_ELEMENT_KEY, CE())
                impl_args = [
                    self.container[LABELS.engine],  # engine
                    self.container[LABELS.image],  # image
                    self.container.get(
                        LABELS.options, INTERNAL_LABELS.unassigned
                    ),
                    INTERNAL_LABELS.unassigned,  # internal_type
                    INTERNAL_LABELS.unassigned,  # internal_binary
                    INTERNAL_LABELS.unassigned,  # internal_func
                    INTERNAL_LABELS.unassigned,  # working_dir
                    INTERNAL_LABELS.unassigned,
                ]  # fail_by_ev
                core_element.set_impl_type(impl_type)
                core_element.set_impl_signature(impl_signature)
                core_element.set_impl_type_args(impl_args)
                kwargs[CORE_ELEMENT_KEY] = core_element

            if CORE_ELEMENT_KEY in kwargs:
                self.core_element_configured = True

            if not self.decor:
                # It's a PyCOMPSs task with only @task and @software
                # decorators, so everything from the config file is already
                # in the CE.
                return user_function(*args, **kwargs)

            decorator = self.decor

            # if the function is meant to be called on the worker, we must send
            # the config file as a FILE_IN param
            if decorator in [task.task, multinode.multinode]:
                self.decorator_arguments.update_arguments(self.parameters)
                kwargs[LABELS.software_config_file] = self.kwargs.get(
                    LABELS.config_file
                )

            if not self.parameters:
                # @task definition is not in the config file, call the user
                # function which includes @task
                def decor_f():
                    def function():
                        ret = decorator(**self.config_args)
                        return ret(user_function)(*args, **kwargs)

                    return function()

                decor_f.__doc__ = user_function.__doc__
                return decor_f()

            if self.decor is not task.task:
                # it is not a regular @task, build the decorator with
                # "execution" key-values and the @task decorator with
                # "parameters" key-values
                def decor_f_nonregular():
                    def function(*_, **__):
                        t_t = task.task(**self.parameters)
                        return t_t(user_function)(*_, **__)

                    dec = decorator(**self.config_args)
                    return dec(function)(*args, **kwargs)

                return decor_f_nonregular()

            # regular task definition inside a config file
            self.__check_core_element__(kwargs, user_function)
            master = TaskMaster(
                user_function,
                self.core_element,
                self.decorator_arguments,
                self.decorated_function,
                self.registered_signatures,
                self.constraint_args,
            )
            (
                future_object,
                self.core_element,
                self.decorated_function,
                self.registered_signatures,
                self.constraint_args,
            ) = master.call(args, kwargs)

            del master
            return future_object

        software_f.__doc__ = user_function.__doc__
        return software_f

    def pop_file_path(self, *args) -> typing.Tuple:
        """Pop JSON configuration file path from the args.

        :param args: args of the task function
        :return: args without JSON config file path
        """
        if CONTEXT.in_master():
            if not self.file_path:
                self.file_path = self.kwargs.get(LABELS.config_file)
            return args
        if CONTEXT.in_worker():
            tmp = list(*args)  # type: typing.List[typing.Any]
            for i, value in enumerate(tmp):
                if value.name == "#kwarg_software_config_file":
                    self.file_path = value.file_name.source_path
                    break
            else:
                return args
            tmp.pop(i)
            return tuple(tmp)
        raise PyCOMPSsException("Unexpected context!")

    def parse_config_file(self) -> None:
        """
        Parse the config file and set self's task_type, decor, and config args.

        :return: None
        """
        if not self.file_path:
            raise PyCOMPSsException(" ERROR: Incorrect file_path.")
        with open(  # pylint: disable=unspecified-encoding
            self.file_path, "r"
        ) as file_path_descriptor:
            config = json.load(file_path_descriptor)

        execution = config.get(LABELS.execution, {})
        self.parameters = config.get(LABELS.parameters, {})
        exec_type = execution.pop(LABELS.type, None)
        if exec_type is None:
            print("WARN: Execution type not provided for @software task")
        elif exec_type == LABELS.task:
            self.task_type, self.decor = SUPPORTED_DECORATORS[exec_type]
            if __debug__:
                logger.debug("Executing Software as task function ...")
        elif exec_type == "workflow":
            if __debug__:
                logger.debug("Executing Software as Workflow ...")
            self.is_workflow = True
            return
        elif exec_type.lower() not in SUPPORTED_DECORATORS:
            msg = (
                f"Error: Executor Type {exec_type} is not supported "
                f"for software task."
            )
            raise PyCOMPSsException(msg)
        else:
            exec_type = exec_type.lower()
            self.task_type, self.decor = SUPPORTED_DECORATORS[exec_type]
            mand_args = self.task_type.MANDATORY_ARGUMENTS
            if not all(arg in execution for arg in mand_args):
                msg = f"Error: Missing arguments for '{self.task_type}'."
                raise PyCOMPSsException(msg)

        self.replace_param_types()

        # send the config file to the worker as well
        if CONTEXT.in_master() and self.decor in [
            task.task,
            multinode.multinode,
        ]:
            self.parameters[LABELS.software_config_file] = parameter.FILE_IN

        self.config_args = execution
        self.constraints = config.get("constraints", None)
        self.container = config.get("container", None)
        self.prolog = config.get("prolog", None)
        self.epilog = config.get("epilog", None)

    def replace_param_types(self):
        """Replace the strings with Parameters form the API.

        :return:
        """
        # replace python param types if any
        for key, value in self.parameters.items():
            if isinstance(value, str) and hasattr(parameter, value):
                self.parameters[key] = getattr(parameter, value)

        # convert the "returns" value
        rets = self.parameters.get("returns", None)
        if rets and isinstance(rets, str) and hasattr(builtins, rets):
            self.parameters["returns"] = getattr(builtins, rets)


# ########################################################################### #
# ##################### Software DECORATOR ALTERNATIVE NAME ################# #
# ########################################################################### #


software = Software  # pylint: disable=invalid-name
