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
PyCOMPSs API - on_failure decorator.

This file contains the on_failure class, needed for the on failure management
definition through the decorator.
"""

from functools import wraps

from pycompss.util.context import CONTEXT
from pycompss.api.commons.constants import LABELS
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.dummy.on_failure import on_failure as dummy_on_failure
from pycompss.runtime.task.definitions.core_element import CE
from pycompss.util.arguments import check_mandatory_arguments
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.typing_helper import typing

if __debug__:
    import logging

    logger = logging.getLogger(__name__)


MANDATORY_ARGUMENTS = {LABELS.management}
SUPPORTED_ARGUMENTS = {
    LABELS.management_ignore,
    LABELS.management_retry,
    LABELS.management_cancel_successor,
    LABELS.management_fail,
}


class OnFailure:  # pylint: disable=R0903,R0902
    # disable=too-few-public-methods, too-many-instance-attributes
    """OnFailure decorator class.

    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on task on_failure task management definition.
    """

    __slots__ = [
        "on_failure_action",
        "defaults",
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
        kwargs = dictionary with the given on_failure.

        :param args: Arguments.
        :param kwargs: Keyword arguments.
        """
        decorator_name = "".join(("@", OnFailure.__name__.lower()))
        # super(OnFailure, self).__init__(decorator_name, *args, **kwargs)
        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = CONTEXT.in_pycompss()
        self.core_element = None  # type: typing.Optional[CE]
        self.core_element_configured = False
        if self.scope:
            # Check the arguments
            check_mandatory_arguments(
                MANDATORY_ARGUMENTS, list(kwargs.keys()), decorator_name
            )

            # Save the parameters into self so that they can be accessed when
            # the task fails and the action needs to be taken
            self.on_failure_action = kwargs.pop(LABELS.management)
            # Check supported management values
            if self.on_failure_action not in SUPPORTED_ARGUMENTS:
                raise PyCOMPSsException(
                    f"ERROR: Unsupported on failure action: "
                    f"{self.on_failure_action}"
                )
            # Keep all defaults in a dictionary
            self.defaults = kwargs

    def __call__(self, user_function: typing.Callable) -> typing.Callable:
        """Parse and set the on_failure within the task core element.

        :param user_function: Function to decorate.
        :return: Decorated function.
        """

        @wraps(user_function)
        def constrained_f(
            *args: typing.Any, **kwargs: typing.Any
        ) -> typing.Any:
            if not self.scope:
                d_c = dummy_on_failure(self.args, self.kwargs)
                return d_c.__call__(user_function)(*args, **kwargs)

            if __debug__:
                logger.debug("Executing on_failure_f wrapper.")

            if (
                CONTEXT.in_master() or CONTEXT.is_nesting_enabled()
            ) and not self.core_element_configured:
                # master code - or worker with nesting enabled
                self.__configure_core_element__(kwargs)

            # Set the on failure management action and default variables in
            # kwargs for its usage in @task decorator
            kwargs["on_failure"] = self.on_failure_action
            kwargs["defaults"] = self.defaults

            with keep_arguments(args, kwargs, prepend_strings=True):
                # Call the method
                ret = user_function(*args, **kwargs)

            return ret

        constrained_f.__doc__ = user_function.__doc__
        return constrained_f

    def __configure_core_element__(self, kwargs: dict) -> None:
        """Include the registering info related to @on_failure.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Current keyword arguments to be updated with the core
                       element information.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @on_failure core element.")

        if CORE_ELEMENT_KEY not in kwargs:
            # @on_failure is in the top of the decorators stack.
            # Instantiate a new core element object, update it and include
            # it into kwarg
            kwargs[CORE_ELEMENT_KEY] = CE()

        # Set as configured
        self.core_element_configured = True


# ########################################################################### #
# ################### ON FAILURE DECORATOR ALTERNATIVE NAME ################# #
# ########################################################################### #

on_failure = OnFailure  # pylint: disable=invalid-name
onFailure = OnFailure  # noqa: N816 # pylint: disable=invalid-name
