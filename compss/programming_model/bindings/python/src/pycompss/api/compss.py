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
PyCOMPSs API - COMPSs decorator.

This file contains the COMPSs class, needed for the COMPSs task definition
through the decorator.
"""

from functools import wraps

from pycompss.util.context import CONTEXT
from pycompss.api.commons.constants import INTERNAL_LABELS
from pycompss.api.commons.constants import LABELS
from pycompss.api.commons.constants import LEGACY_LABELS
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import process_computing_nodes
from pycompss.api.commons.decorator import resolve_fail_by_exit_value
from pycompss.api.commons.decorator import resolve_working_dir
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.api.commons.implementation_types import IMPLEMENTATION_TYPES
from pycompss.runtime.task.definitions.core_element import CE
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import NotInPyCOMPSsException
from pycompss.util.typing_helper import typing

if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {LABELS.app_name}
SUPPORTED_ARGUMENTS = {
    LABELS.computing_nodes,
    LABELS.runcompss,
    LABELS.flags,
    LABELS.worker_in_master,
    LABELS.app_name,
    LABELS.args,
    LABELS.working_dir,
    LABELS.fail_by_exit_value,
}
DEPRECATED_ARGUMENTS = {
    LEGACY_LABELS.computing_nodes,
    LEGACY_LABELS.worker_in_master,
    LEGACY_LABELS.app_name,
    LEGACY_LABELS.working_dir,
}


class COMPSs:  # pylint: disable=too-few-public-methods
    """COMPSs decorator class.

    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on compss task creation.
    """

    __slots__ = [
        "decorator_name",
        "args",
        "kwargs",
        "scope",
        "core_element",
        "core_element_configured",
    ]

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given constraints.

        :param args: Arguments.
        :param kwargs: Keyword arguments.
        """
        decorator_name = "".join(("@", COMPSs.__name__.lower()))
        # super(COMPSs, self).__init__(decorator_name, *args, **kwargs)
        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = CONTEXT.in_pycompss()
        self.core_element = None  # type: typing.Optional[CE]
        self.core_element_configured = False
        if self.scope:
            # Check the arguments
            check_arguments(
                MANDATORY_ARGUMENTS,
                DEPRECATED_ARGUMENTS,
                SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                list(kwargs.keys()),
                decorator_name,
            )

            # Get the computing nodes
            process_computing_nodes(decorator_name, self.kwargs)

    def __call__(self, user_function: typing.Callable) -> typing.Callable:
        """Parse and set the compss parameters within the task core element.

        :param user_function: Function to decorate.
        :return: Decorated function.
        """

        @wraps(user_function)
        def compss_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            if not self.scope:
                raise NotInPyCOMPSsException(not_in_pycompss("compss"))

            if __debug__:
                logger.debug("Executing compss_f wrapper.")

            if (
                CONTEXT.in_master() or CONTEXT.is_nesting_enabled()
            ) and not self.core_element_configured:
                # master code - or worker with nesting enabled
                self.__configure_core_element__(kwargs)

            # Set the computing_nodes variable in kwargs for its usage
            # in @task decorator
            kwargs[LABELS.computing_nodes] = self.kwargs[
                LABELS.computing_nodes
            ]

            with keep_arguments(args, kwargs, prepend_strings=False):
                # Call the method
                ret = user_function(*args, **kwargs)

            return ret

        compss_f.__doc__ = user_function.__doc__
        return compss_f

    def __configure_core_element__(self, kwargs: dict) -> None:
        """Include the registering info related to @compss.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @compss core element.")

        # Resolve @compss specific parameters
        if LABELS.runcompss in self.kwargs:
            runcompss = self.kwargs[LABELS.runcompss]
        else:
            runcompss = (
                INTERNAL_LABELS.unassigned
            )  # Empty or INTERNAL_LABELS.unassigned

        if LABELS.flags in self.kwargs:
            flags = self.kwargs[LABELS.flags]
        else:
            flags = (
                INTERNAL_LABELS.unassigned
            )  # Empty or INTERNAL_LABELS.unassigned

        if LABELS.worker_in_master in self.kwargs:
            worker_in_master = self.kwargs[LABELS.worker_in_master]
        elif LEGACY_LABELS.worker_in_master in self.kwargs:
            worker_in_master = self.kwargs[LEGACY_LABELS.worker_in_master]
        else:
            worker_in_master = "true"  # Empty or INTERNAL_LABELS.unassigned

        if LEGACY_LABELS.app_name in self.kwargs:
            app_name = self.kwargs[LEGACY_LABELS.app_name]
        else:
            app_name = self.kwargs[LABELS.app_name]
        # Resolve args
        _args = self.kwargs.get(LABELS.args, INTERNAL_LABELS.unassigned)

        # Resolve the working directory
        resolve_working_dir(self.kwargs)
        # Resolve the fail by exit value
        resolve_fail_by_exit_value(self.kwargs)

        impl_type = IMPLEMENTATION_TYPES.compss
        impl_signature = ".".join((impl_type, app_name))
        impl_args = [
            runcompss,
            flags,
            app_name,
            _args,
            worker_in_master,
            self.kwargs[LABELS.working_dir],
            self.kwargs[LABELS.fail_by_exit_value],
        ]

        if CORE_ELEMENT_KEY in kwargs:
            # Core element has already been created in a higher level decorator
            # (e.g. @constraint)
            kwargs[CORE_ELEMENT_KEY].set_impl_type(impl_type)
            kwargs[CORE_ELEMENT_KEY].set_impl_signature(impl_signature)
            kwargs[CORE_ELEMENT_KEY].set_impl_type_args(impl_args)
        else:
            # @binary is in the top of the decorators stack.
            # Instantiate a new core element object, update it and include
            # it into kwarg
            core_element = CE()
            core_element.set_impl_type(impl_type)
            core_element.set_impl_signature(impl_signature)
            core_element.set_impl_type_args(impl_args)
            kwargs[CORE_ELEMENT_KEY] = core_element

        # Set as configured
        self.core_element_configured = True


# ########################################################################### #
# #################### COMPSs DECORATOR ALTERNATIVE NAME #################### #
# ########################################################################### #

compss = COMPSs  # pylint: disable=invalid-name
