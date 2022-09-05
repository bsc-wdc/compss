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
PyCOMPSs API - Software decorator.

This file contains the Software class, needed for the software task definition
through the decorator.
"""
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
from pycompss.api.soft_converter import conv
from pycompss.api import parameter

from pycompss.api.commons.constants import INTERNAL_LABELS
from pycompss.api.commons.constants import LABELS
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import resolve_fail_by_exit_value
from pycompss.api.commons.implementation_types import IMPLEMENTATION_TYPES
from pycompss.runtime.task.core_element import CE
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.typing_helper import typing
from pycompss.util.logger.helpers import update_logger_handlers
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


class SoftwareTask(task.task):  # pylint: disable=too-few-public-methods, too-many-instance-attributes
    """Software decorator class.

    When provided with a config file, it can replicate any existing python
    decorator by wrapping the user function with the decorator defined in
    the config file. Arguments of the decorator should be defined in the
    config file which is in JSON format.
    """

    __slots__ = [
        "decorator_name",
        "args",
        "kwargs",
        "scope",
        "core_element",
        "core_element_configured",
        "task_type",
        "config_args",
        "decor",
        "constraints",
        "container",
        "prolog",
        "epilog",
        "parameters",
        "is_workflow"
    ]

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Parse the config file and store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given @software parameter (config_file).

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        super().__init__(*args, **kwargs)
        decorator_name = "".join(("@", SoftwareTask.__name__.lower()))
        # super(Software, self).__init__(decorator_name, *args, **kwargs)
        self.task_type = None  # type: typing.Any
        self.config_args = None  # type: typing.Any
        self.decor = None  # type: typing.Any
        self.constraints = None  # type: typing.Any
        self.container = None  # type: typing.Any
        self.prolog = None  # type: typing.Any
        self.epilog = None  # type: typing.Any
        self.parameters = dict()  # type: typing.Dict
        self.is_workflow = False  # type: bool

        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = CONTEXT.in_pycompss()
        self.core_element = None  # type: typing.Any
        self.core_element_configured = False

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
            self.parse_config_file()

    def __call__(self, user_function: typing.Callable) -> typing.Callable:
        """Parse and set the software parameters within the task core element.

        When called, @software decorator basically wraps the user function
        into the "real" decorator and passes the args and kwargs.

        :param user_function: User function to be decorated.
        :return: User function decorated with the decor type defined by the user.
        """

        @wraps(user_function)
        def software_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:

            self.user_function = user_function

            if CONTEXT.in_worker():
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
                return result

            if not self.scope or self.is_workflow:
                # Execute the software as with PyCOMPSs so that sequential
                # execution performs as parallel.
                # To disable: raise Exception(not_in_pycompss(LABELS.binary))
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

            # todo: add comments
            # move out if not decor
            if self.decor:
                decorator = self.decor
                if not self.parameters:
                    def decor_f():
                        def function():
                            ret = decorator(**self.config_args)
                            return ret(user_function)(*args, **kwargs)
                        return function()
                    decor_f.__doc__ = user_function.__doc__
                    return decor_f()
                else:
                    if self.decor is task.task:
                        self.__check_core_element__(kwargs, user_function)
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
                    else:
                        def decor_f():
                            def function(*_, **__):
                                tt = task.task(**self.parameters)
                                return tt(user_function)(*_, **__)
                            dec = decorator(**self.config_args)
                            return dec(function)(*args, **kwargs)

                    return decor_f()

            # It's a PyCOMPSs task with only @task and @software decorators
            return user_function(*args, **kwargs)

        software_f.__doc__ = user_function.__doc__
        return software_f

    def parse_config_file(self) -> None:
        """Parse the config file and set self's task_type, decor, and config args.

        :return: None
        """
        file_path = self.kwargs[LABELS.config_file]
        with open(  # pylint: disable=unspecified-encoding
            file_path, "r"
        ) as file_path_descriptor:
            config = json.load(file_path_descriptor)

        execution = config.get(LABELS.execution, {})
        self.parameters = config.get(LABELS.parameters, dict())
        exec_type = execution.pop(LABELS.type, None)
        if exec_type is None:
            print("Execution type not provided for @software task")
        elif exec_type == LABELS.task:
            self.task_type, self.decor = SUPPORTED_DECORATORS[exec_type]
            print("Executing task function..")
        elif exec_type == "workflow":
            print("Executing workflow..")
            self.is_workflow = True
            return
        elif exec_type.lower() not in SUPPORTED_DECORATORS:
            msg = f"Error: Executor Type {exec_type} is not supported for software task."
            raise PyCOMPSsException(msg)
        else:
            exec_type = exec_type.lower()
            self.task_type, self.decor = SUPPORTED_DECORATORS[exec_type]
            mand_args = self.task_type.MANDATORY_ARGUMENTS
            if not all(arg in execution for arg in mand_args):
                msg = f"Error: Missing arguments for '{self.task_type}'."
                raise PyCOMPSsException(msg)

        self.replace_param_types()
        self.config_args = execution
        self.constraints = config.get("constraints", None)
        self.container = config.get("container", None)
        self.prolog = config.get("prolog", None)
        self.epilog = config.get("epilog", None)

    def replace_param_types(self):
        if not self.parameters:
            return
        for k, v in self.parameters.items():
            if isinstance(v, str) and hasattr(parameter, v):
                self.parameters[k] = getattr(parameter, v)


def task_f(user_function, parameters, *args, **kwargs):
    import inspect
    tc = inspect.getsource(user_function)
    lines = tc.split("\n")
    lines = lines[1:]
    tbi = "@task(" + str(format_json(parameters)) + ")"
    lines.insert(0, tbi)
    lines = "\n".join(lines)
    orig_name = inspect.getsourcefile(user_function)
    fp = "dt_" + orig_name
    imports = get_imports(orig_name)
    with open(fp, "a") as helper:
        helper.writelines(imports)
        helper.write("\n")
        helper.write(lines)
        helper.write("\n")

    import importlib
    mod = importlib.import_module(fp[:-3])
    func = getattr(mod, str(user_function.__name__))
    return func(*args, **kwargs)


@conv()
@task.task(compss_task_file=parameter.FILE_IN)
def converted(*args, compss_task_file="dt_test.py", **kwargs):
    pass


def format_json(jeyson):
    ret = ""
    for k in jeyson:
        ret += k + "=" + str(jeyson[k]) + ", "
    return ret[:-2]


def get_imports(file_name) -> list:
    imports = []

    with open(file_name, "r") as fayl:
        lines = fayl.read().split("\n")

    for line in lines:
        if (
            line.startswith("from") or line.startswith("import")
        ) and "pycompss.interactive" not in line:
            imports.append(line + "\n")
    return imports


# ########################################################################### #
# ##################### Software DECORATOR ALTERNATIVE NAME ################# #
# ########################################################################### #


software = SoftwareTask  # pylint: disable=invalid-name
