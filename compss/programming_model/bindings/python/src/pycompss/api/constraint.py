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
PyCOMPSs API - Constraint decorator.

This file contains the Constraint class, needed for the task constraint
definition through the decorator.
"""

from functools import wraps

from pycompss.util.context import CONTEXT
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.dummy.constraint import constraint as dummy_constraint
from pycompss.runtime.task.definitions.core_element import CE
from pycompss.util.typing_helper import typing
from pycompss.util.exceptions import PyCOMPSsException

if __debug__:
    import logging

    logger = logging.getLogger(__name__)


class Constraint:  # pylint: disable=too-few-public-methods
    """Constraint decorator class.

    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on task constraint creation.
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
        decorator_name = "".join(("@", Constraint.__name__.lower()))
        # super(Constraint, self).__init__(decorator_name, *args, **kwargs)
        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = CONTEXT.in_pycompss()
        self.core_element = None  # type: typing.Optional[CE]
        self.core_element_configured = False

    def __call__(self, user_function: typing.Callable) -> typing.Callable:
        """Parse and set the constraints within the task core element.

        :param user_function: Function to decorate.
        :return: Decorated function.
        """

        @wraps(user_function)
        def constrained_f(
            *args: typing.Any, **kwargs: typing.Any
        ) -> typing.Any:
            if not self.scope:
                d_c = dummy_constraint(self.args, self.kwargs)
                return d_c.__call__(user_function)(*args, **kwargs)

            if __debug__:
                logger.debug("Executing constrained_f wrapper.")

            if (
                CONTEXT.in_master() or CONTEXT.is_nesting_enabled()
            ) and not self.core_element_configured:
                # master code - or worker with nesting enabled
                self.__configure_core_element__(kwargs)

            with keep_arguments(args, kwargs, prepend_strings=True):
                # Call the method
                ret = user_function(*args, **kwargs)

            return ret

        constrained_f.__doc__ = user_function.__doc__
        return constrained_f

    def __configure_core_element__(self, kwargs: dict) -> None:
        """Include the registering info related to @constraint.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Current keyword arguments to be updated with the core
                       element information.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @constraint core element.")

        is_local_key = "is_local"
        is_local = False
        if is_local_key in self.kwargs:
            is_local = self.kwargs.pop(is_local_key)
            if not isinstance(is_local, bool):
                raise PyCOMPSsException(
                    "is_local constraint can only be defined with boolean"
                )

        if CORE_ELEMENT_KEY in kwargs:
            # Core element has already been created in a higher level decorator
            # (e.g. @implements and @compss)
            kwargs[CORE_ELEMENT_KEY].set_impl_constraints(self.kwargs)
            kwargs[CORE_ELEMENT_KEY].set_impl_local(is_local)
        else:
            # @constraint is in the top of the decorators stack.
            # Instantiate a new core element object, update it and include
            # it into kwarg
            core_element = CE()
            core_element.set_impl_constraints(self.kwargs)
            core_element.set_impl_local(is_local)
            kwargs[CORE_ELEMENT_KEY] = core_element

        # Set as configured
        self.core_element_configured = True


# ########################################################################### #
# ################### CONSTRAINT DECORATOR ALTERNATIVE NAME ################# #
# ########################################################################### #

constraint = Constraint  # pylint: disable=invalid-name
