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
PyCOMPSs API - Implement (Versioning)
=====================================
    This file contains the class constraint, needed for the implement
    definition through the decorator.
"""

from functools import wraps

import pycompss.util.context as context
from pycompss.api.commons.constants import LEGACY_SOURCE_CLASS
from pycompss.api.commons.constants import METHOD
from pycompss.api.commons.constants import SOURCE_CLASS
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.api.commons.implementation_types import IMPL_METHOD
from pycompss.runtime.task.core_element import CE
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import NotInPyCOMPSsException
from pycompss.util.typing_helper import typing

if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {SOURCE_CLASS, METHOD}
SUPPORTED_ARGUMENTS = {SOURCE_CLASS, METHOD}
DEPRECATED_ARGUMENTS = {LEGACY_SOURCE_CLASS}


class Implement(object):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on mpi task creation.
    """

    __slots__ = [
        "first_register",
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
        kwargs = dictionary with the given implement parameters.

        :param args: Arguments.
        :param kwargs: Keyword arguments.
        """
        self.first_register = False
        decorator_name = "".join(("@", Implement.__name__.lower()))
        # super(Implement, self).__init__(decorator_name, *args, **kwargs)
        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = context.in_pycompss()
        self.core_element = None  # type: typing.Any
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

    def __call__(self, user_function: typing.Callable) -> typing.Callable:
        """Parse and set the implement parameters within the task core element.

        :param user_function: Function to decorate.
        :return: Decorated function.
        """

        @wraps(user_function)
        def implement_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            # This is executed only when called.
            if not self.scope:
                raise NotInPyCOMPSsException(not_in_pycompss("implement"))

            if __debug__:
                logger.debug("Executing implement_f wrapper.")

            if (
                context.in_master() or context.is_nesting_enabled()
            ) and not self.core_element_configured:
                # master code - or worker with nesting enabled
                self.__configure_core_element__(kwargs)

            with keep_arguments(args, kwargs, prepend_strings=True):
                # Call the method
                ret = user_function(*args, **kwargs)

            return ret

        implement_f.__doc__ = user_function.__doc__

        if context.in_master() and not self.first_register:
            import pycompss.api.task as t

            self.first_register = True
            t.REGISTER_ONLY = True
            self.__call__(user_function)(self)
            t.REGISTER_ONLY = False

        return implement_f

    def __configure_core_element__(self, kwargs: dict) -> None:
        """Include the registering info related to @implement.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @implement core element.")

        # Resolve @implement specific parameters
        if LEGACY_SOURCE_CLASS in self.kwargs:
            another_class = self.kwargs[LEGACY_SOURCE_CLASS]
            self.kwargs[SOURCE_CLASS] = self.kwargs.pop(LEGACY_SOURCE_CLASS)
        else:
            another_class = self.kwargs[SOURCE_CLASS]
        another_method = self.kwargs[METHOD]
        ce_signature = ".".join((another_class, another_method))
        impl_type = IMPL_METHOD
        # impl_args = [another_class, another_method] - set by @task

        if CORE_ELEMENT_KEY in kwargs:
            # Core element has already been created in a higher level decorator
            # (e.g. @constraint)
            kwargs[CORE_ELEMENT_KEY].set_ce_signature(ce_signature)
            kwargs[CORE_ELEMENT_KEY].set_impl_type(impl_type)
            # @task sets the implementation type arguments
            # kwargs[CORE_ELEMENT_KEY].set_impl_type_args(impl_args)
        else:
            # @implement is in the top of the decorators stack.
            # Instantiate a new core element object, update it and include
            # it into kwarg
            core_element = CE()
            core_element.set_ce_signature(ce_signature)
            core_element.set_impl_type(impl_type)
            # @task sets the implementation type arguments
            # core_element.set_impl_type_args(impl_args)
            kwargs[CORE_ELEMENT_KEY] = core_element

        # Set as configured
        self.core_element_configured = True


# ########################################################################### #
# ################## IMPLEMENT DECORATOR ALTERNATIVE NAME ################### #
# ########################################################################### #

implement = Implement
