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
PyCOMPSs API - OPENCL
=====================
    This file contains the class constraint, needed for the opencl task
    definition through the decorator.
"""

from functools import wraps

import pycompss.util.context as context
from pycompss.api.commons.constants import KERNEL
from pycompss.api.commons.constants import LEGACY_WORKING_DIR
from pycompss.api.commons.constants import WORKING_DIR
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import resolve_working_dir
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.api.commons.implementation_types import IMPL_OPENCL
from pycompss.runtime.task.core_element import CE
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import NotInPyCOMPSsException
from pycompss.util.typing_helper import typing

if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {KERNEL}
SUPPORTED_ARGUMENTS = {KERNEL, WORKING_DIR}
DEPRECATED_ARGUMENTS = {LEGACY_WORKING_DIR}


class OpenCL(object):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on opencl task creation.
    """

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given constraints.

        :param args: Arguments.
        :param kwargs: Keyword arguments.
        """
        decorator_name = "".join(("@", OpenCL.__name__.lower()))
        # super(OpenCL, self).__init__(decorator_name, *args, **kwargs)
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
        """Parse and set the opencl parameters within the task core element.

        :param user_function: Function to decorate.
        :return: Decorated function.
        """

        @wraps(user_function)
        def opencl_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            if not self.scope:
                raise NotInPyCOMPSsException(not_in_pycompss("opencl"))

            if __debug__:
                logger.debug("Executing opencl_f wrapper.")

            if (
                context.in_master() or context.is_nesting_enabled()
            ) and not self.core_element_configured:
                # master code - or worker with nesting enabled
                self.__configure_core_element__(kwargs)

            with keep_arguments(args, kwargs, prepend_strings=False):
                # Call the method
                ret = user_function(*args, **kwargs)

            return ret

        opencl_f.__doc__ = user_function.__doc__
        return opencl_f

    def __configure_core_element__(self, kwargs: dict) -> None:
        """Include the registering info related to @opencl.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @opencl core element.")

        # Resolve @opencl specific parameters
        kernel = self.kwargs[KERNEL]

        # Resolve the working directory
        resolve_working_dir(self.kwargs)

        impl_type = IMPL_OPENCL
        impl_signature = ".".join((impl_type, kernel))
        impl_args = [kernel, self.kwargs[WORKING_DIR]]

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
# ################### OPENCL DECORATOR ALTERNATIVE NAME ##################### #
# ########################################################################### #

opencl = OpenCL
