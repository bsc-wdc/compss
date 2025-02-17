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
PyCOMPSs API - IO decorator.

This file contains the IO class, needed for the IO task definition through
the decorator.
"""

from functools import wraps

from pycompss.util.context import CONTEXT
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.runtime.task.definitions.core_element import CE
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import NotInPyCOMPSsException
from pycompss.util.typing_helper import typing

if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = set()  # type: typing.Set[str]
SUPPORTED_ARGUMENTS = set()  # type: typing.Set[str]
DEPRECATED_ARGUMENTS = set()  # type: typing.Set[str]


class IO:  # pylint: disable=too-few-public-methods
    """IO decorator class.

    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on IO task creation.
    """

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given constraints.

        :param args: Arguments.
        :param kwargs: Keyword arguments.
        """
        decorator_name = "".join(("@", IO.__name__.lower()))
        # super(IO, self).__init__(decorator_name, *args, **kwargs)
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

    def __call__(self, user_function: typing.Callable) -> typing.Callable:
        """Parse and set the IO parameters within the task core element.

        :param user_function: Function to decorate.
        :return: Decorated function.
        """

        @wraps(user_function)
        def io_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            if not self.scope:
                raise NotInPyCOMPSsException(not_in_pycompss("IO"))

            if __debug__:
                logger.debug("Executing IO_f wrapper.")

            if (
                CONTEXT.in_master() or CONTEXT.is_nesting_enabled()
            ) and not self.core_element_configured:
                # master code - or worker with nesting enabled
                self.__configure_core_element__(kwargs)

            with keep_arguments(args, kwargs, prepend_strings=True):
                # Call the method
                ret = user_function(*args, **kwargs)

            return ret

        io_f.__doc__ = user_function.__doc__
        return io_f

    @staticmethod
    def __configure_core_element__(
        kwargs: typing.Dict[str, typing.Any]
    ) -> None:
        """Include the registering info related to @IO.

        IMPORTANT! Updates kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @IO core element.")

        # Resolve @io specific parameters

        if CORE_ELEMENT_KEY in kwargs:
            # Core element has already been created in a higher level decorator
            # (e.g. @constraint)
            kwargs[CORE_ELEMENT_KEY].set_impl_io(True)
        else:
            # @binary is in the top of the decorators stack.
            # Instantiate a new core element object, update it and include
            # it into kwarg
            core_element = CE()
            core_element.set_impl_io(True)
            kwargs[CORE_ELEMENT_KEY] = core_element


# ########################################################################### #
# ###################### IO DECORATOR ALTERNATIVE NAME ###################### #
# ########################################################################### #

io = IO  # pylint: disable=invalid-name
