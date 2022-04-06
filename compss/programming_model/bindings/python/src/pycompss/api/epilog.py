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
PyCOMPSs API - Epilog
==================
todo: write a proper description
"""
import typing
from functools import wraps

from pycompss.api.commons.constants import INTERNAL_LABELS
from pycompss.api.commons.constants import LABELS
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import resolve_fail_by_exit_value
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.runtime.task.core_element import CE
from pycompss.util.arguments import check_arguments

import pycompss.util.context as context


if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {LABELS.binary}
SUPPORTED_ARGUMENTS = {
    LABELS.binary,
    LABELS.params,
    LABELS.fail_by_exit_value,
}
DEPRECATED_ARGUMENTS = set()  # type: typing.Set[str]


class Epilog(object):
    """
    todo: write comments
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
        kwargs = dictionary with the given binary and params strings.

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        decorator_name = "".join(("@", Epilog.__name__.lower()))
        # super(Epilog, self).__init__(decorator_name, *args, **kwargs)
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
        """
        todo: write
        :param user_function: User function to be decorated.
        :return: Decorated dummy user function.
        """

        @wraps(user_function)
        def epilog_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            return self.__decorator_body__(user_function, args, kwargs)

        epilog_f.__doc__ = user_function.__doc__
        return epilog_f

    def __decorator_body__(
        self, user_function: typing.Callable, args: tuple, kwargs: dict
    ) -> typing.Any:
        if not self.scope:
            raise NotImplementedError

        if __debug__:
            logger.debug("Executing epilog wrapper.")

        if (
            context.in_master() or context.is_nesting_enabled()
        ) and not self.core_element_configured:
            self.__configure_core_element__(kwargs)

        with keep_arguments(args, kwargs, prepend_strings=True):
            # Call the method
            ret = user_function(*args, **kwargs)

        return ret

    def __configure_core_element__(self, kwargs: dict) -> None:
        """Include the registering info related to @epilog.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @epilog core element.")

        # Resolve the fail by exit value
        resolve_fail_by_exit_value(self.kwargs, def_val="false")

        binary = self.kwargs[LABELS.binary]
        params = self.kwargs.get(LABELS.params, INTERNAL_LABELS.unassigned)
        fail_by = self.kwargs.get(LABELS.fail_by_exit_value)
        _epilog = [binary, params, fail_by]

        ce = kwargs.get(CORE_ELEMENT_KEY, CE())
        ce.set_impl_epilog(_epilog)
        kwargs[CORE_ELEMENT_KEY] = ce
        # Set as configured
        self.core_element_configured = True


# ########################################################################### #
# ################### EPILOG DECORATOR ALTERNATIVE NAME ##################### #
# ########################################################################### #

epilog = Epilog
