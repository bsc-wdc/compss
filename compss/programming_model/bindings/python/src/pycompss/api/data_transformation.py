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
PyCOMPSs API - Data Transformation decorator.

This file contains DT decorator class and its helper DTO class.
"""

import inspect

from functools import wraps

from pycompss.util.context import CONTEXT
from pycompss.api.commons.constants import LABELS
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.private_tasks import transform as _transform
from pycompss.api.commons.private_tasks import col_to_obj as _col_to_obj
from pycompss.api.commons.private_tasks import col_to_file as _col_to_file
from pycompss.api.commons.private_tasks import col_to_dir as _col_to_dir
from pycompss.api.commons.private_tasks import (
    object_to_file as _object_to_file,
)
from pycompss.api.commons.private_tasks import (
    object_to_dir as _object_to_dir,
)
from pycompss.api.commons.private_tasks import file_to_col as _file_to_col
from pycompss.api.commons.private_tasks import (
    file_to_object as _file_to_object,
)
from pycompss.api.commons.private_tasks import (
    dir_to_object as _dir_to_object,
)
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.typing_helper import typing

# from pycompss.runtime.task.definitions.core_element import CE

if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = set()  # type: typing.Set[str]
SUPPORTED_ARGUMENTS = set()  # type: typing.Set[str]
DEPRECATED_ARGUMENTS = set()  # type: typing.Set[str]

OBJECT_TO_FILE = 0
FILE_TO_OBJECT = 1
FILE_TO_COLLECTION = 2
COLLECTION_TO_OBJECT = 3
COLLECTION_TO_FILE = 4
OBJECT_TO_DIRECTORY = 5
DIRECTORY_TO_OBJECT = 6
COLLECTION_TO_DIRECTORY = 7


class DataTransformation:  # pylint: disable=R0902,R0903
    # disable=too-many-instance-attributes, too-few-public-methods
    """Data Transformation decorator for PyCOMPSs tasks."""

    __slots__ = [
        "decorator_name",
        "args",
        "kwargs",
        "scope",
        "core_element",
        "user_function",
        "core_element_configured",
        "dt_function",
        "type",
        "target",
        "destination",
    ]

    def __init__(self, *args, **kwargs):
        """Store arguments passed to the decorator.

        If the args are empty, it will mean that the decorator should get the
        list of the DTO's from the call method.

        :param args: should contain only the <parameter_name> & <user_function>
        :param kwargs: kwargs of the user DT function.
        """
        decorator_name = "".join(("@", DataTransformation.__name__.lower()))
        # super(DataTransformation, self).__init__(
        #     decorator_name, *args, **kwargs
        # )
        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = CONTEXT.in_pycompss()
        # self.core_element = None  # type: typing.Optional[CE]
        self.core_element_configured = False
        self.user_function = None
        self.target = self.kwargs.pop("target", None)
        self.dt_function = self.kwargs.pop("function", None)
        self.type = self.kwargs.pop("type", None)
        # TODO: so far we use default unique file name for all DTs,
        #       should it be replaced?
        self.destination = self.kwargs.pop("destination", "dt_file_out")

    def __call__(self, user_function: typing.Callable) -> typing.Callable:
        """Call to the decorated task function.

        Call is mainly meant to generate DT (task) functions. However, if
        the __init__ wasn't provided with any args, it also should extract the
        DTO's from the kwargs.

        :param user_function: User function to be decorated.
        :return: Decorated dummy user function.
        """

        @wraps(user_function)
        def dt_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            if not self.scope:
                raise NotImplementedError

            if __debug__:
                logger.debug("Executing DT wrapper.")
            tmp = list(args)
            if (
                CONTEXT.in_master() or CONTEXT.is_nesting_enabled()
            ) and not self.core_element_configured:
                self.__call_dt__(user_function, tmp, kwargs)
            with keep_arguments(tuple(tmp), kwargs, prepend_strings=True):
                # no need to do anything on the worker side
                ret = user_function(*tmp, **kwargs)

            return ret

        dt_f.__doc__ = user_function.__doc__
        _transform.__doc__ = user_function.__doc__
        return dt_f

    def __call_dt__(  # pylint: disable=too-many-branches
        self, user_function, args: list, kwargs: dict
    ) -> None:
        """Extract and call the DT functions.

        :param kwargs: Keyword arguments received from call.
        :return: None
        """
        dts = []
        self.user_function = user_function
        dt_kwargs = self.kwargs.copy()
        if __debug__:
            logger.debug("Configuring DT core element.")
        if "dt" in kwargs:
            tmp = kwargs.pop("dt")
            if isinstance(tmp, DTObject):
                dts.append(tmp.extract())
            elif isinstance(tmp, list):
                dts = [obj.extract() for obj in tmp]
        elif len(self.args) == 2:
            dts.append((self.args[0], self.args[1], dt_kwargs))
        elif self.type is OBJECT_TO_FILE:
            dts.append(
                (self.target, self.user_function, dt_kwargs, args, kwargs)
            )
        elif self.type is OBJECT_TO_DIRECTORY:
            dts.append(
                (self.target, self.user_function, dt_kwargs, args, kwargs)
            )
        elif self.type is FILE_TO_OBJECT:
            dts.append(
                (self.target, self.user_function, dt_kwargs, args, kwargs)
            )
        elif self.type is DIRECTORY_TO_OBJECT:
            dts.append(
                (self.target, self.user_function, dt_kwargs, args, kwargs)
            )
        elif self.type is FILE_TO_COLLECTION:
            dts.append(
                (self.target, self.user_function, dt_kwargs, args, kwargs)
            )
        elif self.type is COLLECTION_TO_OBJECT:
            dts.append(
                (self.target, self.user_function, dt_kwargs, args, kwargs)
            )
        elif self.type is COLLECTION_TO_FILE:
            dts.append(
                (self.target, self.user_function, dt_kwargs, args, kwargs)
            )
        elif self.type is COLLECTION_TO_DIRECTORY:
            dts.append(
                (self.target, self.user_function, dt_kwargs, args, kwargs)
            )
        if __debug__:
            logger.debug("Applying %s DTs.", str(len(dts)))
        for _dt in dts:
            self._apply_dt(_dt[0], _dt[1], _dt[2], args, kwargs)

    def _apply_dt(
        self, param_name, func, func_kwargs, args, kwargs
    ):  # pylint: disable=R0913,R0912,R0903
        # disable=too-many-arguments, too-many-branches, too-few-public-methods
        """Call the data transformation function for the given parameter.

        :param param_name: parameter that DT will be applied to
        :param func: DT function
        :param func_kwargs: args and kwargs values of the original DT function
        :param args:
        :param kwargs:
        :return:
        """
        is_workflow = False
        if LABELS.is_workflow in func_kwargs:
            is_workflow = func_kwargs.pop(LABELS.is_workflow)
            is_workflow = is_workflow in [True, "True", "true", 1, "1"]

        p_value = None
        is_kwarg = param_name in kwargs
        if is_kwarg:
            p_value = kwargs.get(param_name)
        else:
            all_params = inspect.signature(self.user_function)  # type: ignore
            keyz = all_params.parameters.keys()
            if param_name not in keyz:
                raise PyCOMPSsException(
                    f"Wrong Param {param_name} in data transformation"
                )
            i = list(keyz).index(param_name)
            if i < len(args):
                p_value = args[i]
            else:
                p_value = all_params.parameters.get(
                    param_name
                ).default  # type: ignore

        new_value = None
        func_kwargs = _replace_func_kwargs(
            func_kwargs, kwargs, args, self.user_function
        )
        if is_workflow:
            # no need to create a task if it's a workflow
            new_value = func(p_value, **func_kwargs)
        elif self.type is OBJECT_TO_FILE:
            _object_to_file(
                p_value, self.destination, self.dt_function, **func_kwargs
            )
            new_value = self.destination
        elif self.type is OBJECT_TO_DIRECTORY:
            _object_to_dir(
                p_value, self.destination, self.dt_function, **func_kwargs
            )
            new_value = self.destination
        elif self.type is FILE_TO_OBJECT:
            new_value = _file_to_object(
                p_value, self.dt_function, **func_kwargs
            )
        elif self.type is DIRECTORY_TO_OBJECT:
            new_value = _dir_to_object(
                p_value, self.dt_function, **func_kwargs
            )
        elif self.type is FILE_TO_COLLECTION:
            size = int(func_kwargs.pop("size"))
            new_value = _file_to_col(  # pylint: disable=unexpected-keyword-arg
                p_value, self.dt_function, returns=size, **func_kwargs
            )
        elif self.type is COLLECTION_TO_OBJECT:
            new_value = _col_to_obj(p_value, self.dt_function, **func_kwargs)
        elif self.type is COLLECTION_TO_FILE:
            _col_to_file(
                p_value, self.destination, self.dt_function, **func_kwargs
            )
            new_value = self.destination
        elif self.type is COLLECTION_TO_DIRECTORY:
            _col_to_dir(
                p_value, self.destination, self.dt_function, **func_kwargs
            )
            new_value = self.destination
        else:
            new_value = _transform(p_value, func, **func_kwargs)

        if is_kwarg or i >= len(args):
            kwargs[param_name] = new_value
        else:
            args[i] = new_value


def _replace_func_kwargs(dt_f_kwargs, f_kwargs, f_args, func):
    for key, value in dt_f_kwargs.items():
        if (
            isinstance(value, str)
            and value.startswith("{{")
            and value.endswith("}}")
        ):
            name = value[2:-2]
            if name in f_kwargs:
                dt_f_kwargs[key] = f_kwargs[name]
            else:
                dt_f_kwargs[key] = _get_param_from_signature(
                    func, name, f_args
                )
    return dt_f_kwargs


def _get_param_from_signature(function, param_name, args):
    all_params = inspect.signature(function)  # type: ignore
    keyz = all_params.parameters.keys()
    if param_name not in keyz:
        raise PyCOMPSsException(
            f"Wrong Param {param_name} in data transformation"
        )
    i = list(keyz).index(param_name)
    if i < len(args):
        p_value = args[i]
    else:
        p_value = all_params.parameters.get(param_name).default  # type: ignore
    return p_value


class DTObject:  # pylint: disable=too-few-public-methods
    """Data Transformation Object is a replacement for DT decorator definition.

    Data Transformation Object is a helper class to avoid stack of
    decorators or to simplify the definition inside the user code. Arguments of
    the object creation of the class is the same as Data Transformation
    decorator. It always expects the parameter name as the first element, then
     dt_function and the rest of the dt_function kwargs if any.

    """

    def __init__(self, param_name, func, **func_kwargs):
        """Initialize the DTO object with the given arguments.

        :param args: should contain only the <parameter_name> & <user_function>
        :param kwargs: kwargs of the user DT function.
        """
        self.param_name = param_name
        self.func = func
        self.func_kwargs = func_kwargs

    def extract(self) -> tuple:
        """Extract the DTO object attributes in a tuple.

        :return: tuple of the param name, user function and its kwargs dict.
        """
        return self.param_name, self.func, self.func_kwargs


# ########################################################################### #
# ############################# ALTERNATIVE NAMES ########################### #
# ########################################################################### #


dt = DataTransformation  # pylint: disable=invalid-name
data_transformation = DataTransformation  # pylint: disable=invalid-name
dto = DTObject  # pylint: disable=invalid-name
