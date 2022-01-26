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

from functools import wraps

from pycompss.api.commons.constants import RUNNER

import pycompss.util.context as context
from pycompss.api.commons.constants import *
from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.runtime.task.core_element import CE
from pycompss.util.arguments import check_arguments


if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {BINARY}
SUPPORTED_ARGUMENTS = {PARAMS}
DEPRECATED_ARGUMENTS = set()


class Epilog(PyCOMPSsDecorator):
    """
    todo: write comments
    """

    __slots__ = []

    def __init__(self, *args, **kwargs):
        """ Store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given binary and params strgins.

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        self.decorator_name = "".join(('@', Epilog.__name__.lower()))

        super(Epilog, self).__init__(self.decorator_name, *args, **kwargs)
        if self.scope:
            if __debug__:
                logger.debug("Init @epilog decorator...")

            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            self.decorator_name)

    def __call__(self, user_function):
        # type: (typing.Callable) -> typing.Callable
        """
        todo: write
        :param user_function: User function to be decorated.
        :return: Decorated dummy user function.
        """

        @wraps(user_function)
        def epilog_f(*args, **kwargs):
            return self.__decorator_body__(user_function, args, kwargs)

        epilog_f.__doc__ = user_function.__doc__
        return epilog_f

    def __decorator_body__(self, user_function, args, kwargs):
        if not self.scope:
            raise NotImplementedError

        if __debug__:
            logger.debug("Executing epilog wrapper.")

        if (context.in_master() or context.is_nesting_enabled()) \
                and not self.core_element_configured:
            self.__configure_core_element__(kwargs, user_function)

        with keep_arguments(args, kwargs, prepend_strings=False):
            # Call the method
            ret = user_function(*args, **kwargs)

        return ret

    def __configure_core_element__(self, kwargs, user_function):
        # type: (dict, ...) -> None
        """ Include the registering info related to @epilog.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :param user_function: Decorated function.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @epilog core element.")

        binary = self.kwargs[BINARY]
        params = self.kwargs[PARAMS]
        _epilog = [binary, params]

        ce = kwargs.get(CORE_ELEMENT_KEY, CE())
        ce.set_epilog(_epilog)
        kwargs[CORE_ELEMENT_KEY] = ce
        # Set as configured
        self.core_element_configured = True


# ########################################################################### #
# ##################### MPI DECORATOR ALTERNATIVE NAME ###################### #
# ########################################################################### #

epilog = Epilog
