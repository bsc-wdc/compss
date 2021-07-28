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
PyCOMPSs API - HTTP
==================
    HTTP Task decorator class.
"""

from functools import wraps
import pycompss.util.context as context
from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import run_command
from pycompss.runtime.task.core_element import CE
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import PyCOMPSsException


if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {'method_type', 'base_url'}
SUPPORTED_ARGUMENTS = {'json_payload', 'produces'}
DEPRECATED_ARGUMENTS = set()


class HTTP(PyCOMPSsDecorator):
    """
    """

    __slots__ = ['task_type']

    def __init__(self, *args, **kwargs):
        """ Store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given mpi parameters.

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        self.task_type = "http"
        decorator_name = "".join(('@', HTTP.__name__.lower()))
        super(HTTP, self).__init__(decorator_name, *args, **kwargs)
        if self.scope:
            if __debug__:
                logger.debug("Init @http decorator..")

            # noqa TODO: Maybe add here the collection layout to avoid iterate twice per elements

            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            decorator_name)

    def __call__(self, user_function):
        """ Parse and set the mpi parameters within the task core element.

        :param user_function: Function to decorate.
        :return: Decorated function.
        """

        @wraps(user_function)
        def http_f(*args, **kwargs):
            return self.__decorator_body__(user_function, args, kwargs)

        http_f.__doc__ = user_function.__doc__
        return http_f

    def __decorator_body__(self, user_function, args, kwargs):
        if not self.scope:
            # run http
            self.__run_http__(args, kwargs)
            pass

        if __debug__:
            logger.debug("Executing http_f wrapper.")

        if (context.in_master() or context.is_nesting_enabled()) \
                and not self.core_element_configured:
            # master code - or worker with nesting enabled
            self.__configure_core_element__(kwargs, user_function)

        with keep_arguments(args, kwargs):
            # Call the method
            ret = user_function(*args, **kwargs)

        return ret

    def __run_http__(self, *args, **kwargs):
        # type: (..., dict) -> int
        """ Runs the mpi binary defined in the decorator when used as dummy.

        :param args: Arguments received from call.
        :param kwargs: Keyword arguments received from call.
        :return: Execution return code.
        """
        print("running http")
        return 200

    def __configure_core_element__(self, kwargs, user_function):
        # type: (dict, ...) -> None
        """ Include the registering info related to @http.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :param user_function: Decorated function.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @http core element.")
        impl_type = "HTTP"
        # todo: nm: beautify this..
        impl_args = [self.kwargs['method_type'], self.kwargs['base_url'],
                     self.kwargs.get('json_payload', "#"),
                     self.kwargs.get('produces', "#")]

        if CORE_ELEMENT_KEY in kwargs:
            # Core element has already been created in a higher level decorator
            # (e.g. @constraint)
            kwargs[CORE_ELEMENT_KEY].set_impl_type(impl_type)
            kwargs[CORE_ELEMENT_KEY].set_impl_type_args(impl_args)
        else:
            # @binary is in the top of the decorators stack.
            # Instantiate a new core element object, update it and include
            # it into kwarg
            core_element = CE()
            core_element.set_impl_type(impl_type)
            core_element.set_impl_type_args(impl_args)
            kwargs[CORE_ELEMENT_KEY] = core_element

        # Set as configured
        self.core_element_configured = True


# ########################################################################### #
# ##################### HTTP DECORATOR ALTERNATIVE NAME ###################### #
# ########################################################################### #

http = HTTP
