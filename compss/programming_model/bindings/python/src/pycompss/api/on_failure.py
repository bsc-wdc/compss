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
PyCOMPSs API - ON_FAILURE
=========================
    This file contains the class on_failure, needed for the on failure
    management definition through the decorator.
"""

import typing
from functools import wraps

import pycompss.util.context as context
from pycompss.api.commons.constants import MANAGEMENT
from pycompss.api.commons.constants import MANAGEMENT_IGNORE
from pycompss.api.commons.constants import MANAGEMENT_RETRY
from pycompss.api.commons.constants import MANAGEMENT_CANCEL_SUCCESSOR
from pycompss.api.commons.constants import MANAGEMENT_FAIL
from pycompss.util.arguments import check_mandatory_arguments
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.runtime.task.core_element import CE
from pycompss.util.exceptions import PyCOMPSsException

if __debug__:
    import logging
    logger = logging.getLogger(__name__)


MANDATORY_ARGUMENTS = {MANAGEMENT}
SUPPORTED_MANAGEMENT = {MANAGEMENT_IGNORE,
                        MANAGEMENT_RETRY,
                        MANAGEMENT_CANCEL_SUCCESSOR,
                        MANAGEMENT_FAIL}


class OnFailure(object):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on task on_failure creation.
    """

    # __slots__ = ["on_failure_action", "defaults"]

    def __init__(self, *args, **kwargs):
        # type: (*typing.Any, **typing.Any) -> None
        """ Store arguments passed to the decorator.

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
        self.scope = context.in_pycompss()
        self.core_element = None  # type: typing.Any
        self.core_element_configured = False
        if self.scope:
            # Check the arguments
            check_mandatory_arguments(MANDATORY_ARGUMENTS,
                                      list(kwargs.keys()),
                                      decorator_name)

            # Save the parameters into self so that they can be accessed when
            # the task fails and the action needs to be taken
            self.on_failure_action = kwargs.pop(MANAGEMENT)
            # Check supported management values
            if self.on_failure_action not in SUPPORTED_MANAGEMENT:
                raise PyCOMPSsException(
                    "ERROR: Unsupported on failure action: %s" %
                    self.on_failure_action)
            # Keep all defaults in a dictionary
            self.defaults = kwargs

    def __call__(self, user_function):
        # type: (typing.Any) -> typing.Any
        """ Parse and set the on_failure within the task core element.

        :param user_function: Function to decorate.
        :return: Decorated function.
        """
        @wraps(user_function)
        def constrained_f(*args, **kwargs):
            # type: (*typing.Any, **typing.Any) -> typing.Any
            if not self.scope:
                from pycompss.api.dummy.on_failure import on_failure \
                    as dummy_on_failure
                d_c = dummy_on_failure(self.args, self.kwargs)
                return d_c.__call__(user_function)(*args, **kwargs)

            if __debug__:
                logger.debug("Executing on_failure_f wrapper.")

            if (context.in_master() or context.is_nesting_enabled()) \
                    and not self.core_element_configured:
                # master code - or worker with nesting enabled
                self.__configure_core_element__(kwargs, user_function)

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

    def __configure_core_element__(self, kwargs, user_function):
        # type: (dict, typing.Any) -> None
        """ Include the registering info related to @on_failure.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Current keyword arguments to be updated with the core
                       element information.
        :param user_function: Decorated function.
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

on_failure = OnFailure
onFailure = OnFailure
